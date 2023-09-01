package core

import (
	"fmt"
	"math/rand"

	api "github.com/kocubinski/costor-api"
	"github.com/kocubinski/iavl-bench/core/metrics"
)

type ChangesetIterator interface {
	Next() error
	Valid() bool
	GetChangeset() *Changeset
}

type Changeset struct {
	Version int64
	Nodes   []*api.Node
}

type ChangesetGenerator struct {
	StoreKey         string
	Seed             int64
	KeyMean          int
	KeyStdDev        int
	ValueMean        int
	ValueStdDev      int
	InitialSize      int
	FinalSize        int
	Versions         int64
	ChangePerVersion int
	DeleteFraction   float64
}

func (c ChangesetGenerator) Iterator() (ChangesetIterator, error) {
	if c.FinalSize < c.InitialSize {
		return nil, fmt.Errorf("final size must be greater than initial size")
	}

	itr := &ChangesetItr{
		gen:               c,
		rand:              rand.New(rand.NewSource(c.Seed)),
		createsPerVersion: float64(c.FinalSize-c.InitialSize) / float64(c.Versions-1),
		keys:              make([][]byte, c.FinalSize),
		freeList:          make(chan int, c.FinalSize),
		deleteMiss:        metrics.Default.NewCounter("iterator.delete_miss"),
		updateMiss:        metrics.Default.NewCounter("iterator.update_miss"),
	}

	for i := 0; i < c.FinalSize; i++ {
		itr.freeList <- i
	}

	err := itr.Next()
	return itr, err
}

type ChangesetItr struct {
	Changeset *Changeset

	version           int64
	rand              *rand.Rand
	gen               ChangesetGenerator
	keys              [][]byte
	freeList          chan int
	createsPerVersion float64
	createAccumulator float64
	deleteMiss        *metrics.Counter
	updateMiss        *metrics.Counter
}

func (itr *ChangesetItr) nextVersion() {
	itr.version++
	itr.Changeset = &Changeset{Version: itr.version}
	var versionNodes []*api.Node

	deletes := int(itr.gen.DeleteFraction * float64(itr.gen.ChangePerVersion))
	updates := itr.gen.ChangePerVersion - deletes

	// only delete past version 1
	if itr.version > 1 {
		for i := 0; i < deletes; i++ {
			j := itr.rand.Intn(itr.gen.FinalSize)
			if itr.keys[j] == nil {
				//itr.deleteMiss.Inc()
				i--
				continue
			}
			node := &api.Node{
				StoreKey: itr.gen.StoreKey,
				Block:    itr.version,
				Key:      itr.keys[j],
				Delete:   true,
			}

			if fmt.Sprintf("%x", node.Key) == "836a27c1c92316b921a61628448b57074c3c8ec3937243ff8754bd190c0d8f536e3e3027349626e74934776b01c3941e97944081719855526f09425d" {
				fmt.Printf("found in delete gen; block %d\n", node.Block)
			}

			itr.freeList <- j
			itr.keys[j] = nil
			versionNodes = append(versionNodes, node)
		}
	}

	if itr.version > 1 {
		for i := 0; i < updates; i++ {
			j := itr.rand.Intn(itr.gen.FinalSize)
			if itr.keys[j] == nil {
				//itr.updateMiss.Inc()
				i--
				continue
			}
			versionNodes = append(versionNodes, &api.Node{
				StoreKey: itr.gen.StoreKey,
				Block:    itr.version,
				Key:      itr.keys[j],
				Value:    itr.genBytes(itr.gen.ValueMean, itr.gen.ValueStdDev),
			})
		}
	}

	var creates int
	if itr.version == 1 {
		creates = itr.gen.InitialSize
	} else {
		itr.createAccumulator += itr.createsPerVersion
		clamped := int(itr.createAccumulator)
		creates = clamped + deletes
		itr.createAccumulator -= float64(clamped)
	}
	for i := 0; i < creates; i++ {
		node := &api.Node{
			StoreKey: itr.gen.StoreKey,
			Key:      itr.genBytes(itr.gen.KeyMean, itr.gen.KeyStdDev),
			Value:    itr.genBytes(itr.gen.ValueMean, itr.gen.ValueStdDev),
			Block:    itr.version,
		}

		j := <-itr.freeList
		itr.keys[j] = node.Key
		versionNodes = append(versionNodes, node)
	}

	itr.rand.Shuffle(len(versionNodes), func(i, j int) {
		versionNodes[i], versionNodes[j] = versionNodes[j], versionNodes[i]
	})
	itr.Changeset.Nodes = versionNodes
}

func (itr *ChangesetItr) Next() error {
	if itr.version == itr.gen.Versions {
		itr.Changeset = nil
		return nil
	}

	itr.nextVersion()
	return nil
}

func (itr *ChangesetItr) Valid() bool {
	return itr.Changeset != nil
}

func (itr *ChangesetItr) GetChangeset() *Changeset {
	return itr.Changeset
}

func (itr *ChangesetItr) genBytes(mean, stdDev int) []byte {
	length := int(itr.rand.NormFloat64()*float64(stdDev) + float64(mean))
	// length must be at least 1
	// explanation: normal distribution is a poor approximation of certain data sets where std dev is skewed
	// by outliers on the upper bound.  mean - std dev can be negative, which is not a valid length.
	// we could just clamp length at 1, but that would skew the distribution of lengths towards 0 which is
	// not realistic.  instead we just generate again closer to the mean with a std dev of mean / 3.
	// this is not perfect but good enough for test sets.
	if length < 1 {
		length = int(itr.rand.NormFloat64()*float64(mean/3) + float64(mean))
		// much lower probability of this happening twice, but just in case
		if length < 1 {
			length = 1
		}
	}
	b := make([]byte, length)
	itr.rand.Read(b)
	return b
}

type ChangesetIterators struct {
	iterators    []ChangesetIterator
	version      int64
	versionSkips int
	idx          int
	Changeset    *Changeset
}

func NewChangesetIterators(gens []ChangesetGenerator) (ChangesetIterator, error) {
	if len(gens) == 0 {
		return nil, fmt.Errorf("must provide at least one generator")
	}
	var iterators []ChangesetIterator
	version := gens[0].Versions
	firstChangeset := &Changeset{Version: version}
	for _, gen := range gens {
		if gen.Versions != version {
			return nil, fmt.Errorf("all generators must have the same number of versions")
		}
		itr, err := gen.Iterator()
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, itr)
		firstChangeset.Nodes = append(firstChangeset.Nodes, itr.GetChangeset().Nodes...)
	}

	itr := &ChangesetIterators{
		iterators: iterators,
	}
	itr.version = version
	itr.Changeset = firstChangeset

	return itr, nil
}

func (itr *ChangesetIterators) Next() error {
	changeset := &Changeset{}
	for _, i := range itr.iterators {
		err := i.Next()
		if err != nil {
			return err
		}
		if !i.Valid() {
			itr.Changeset = nil
			return nil
		}
		cs := i.GetChangeset()
		changeset.Nodes = append(changeset.Nodes, cs.Nodes...)
		if changeset.Version != 0 && changeset.Version != cs.Version {
			return fmt.Errorf("version mismatch: %d != %d", changeset.Version, cs.Version)
		}
		changeset.Version = cs.Version
	}
	itr.Changeset = changeset
	return nil
}

func (itr *ChangesetIterators) Valid() bool {
	return itr.Changeset != nil
}

func (itr *ChangesetIterators) GetChangeset() *Changeset {
	return itr.Changeset
}
