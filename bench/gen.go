package bench

import (
	"fmt"
	"math/rand"

	"github.com/cosmos/iavl-bench/bench/metrics"
	api "github.com/kocubinski/costor-api"
)

type ChangesetIterator interface {
	Next() error
	Valid() bool
	GetChangeset() *api.Changeset
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

const byteChunkSize = 512
const byteChunkCount = 500_000

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
		byteChunks:        make([][]byte, byteChunkCount),
	}

	for i := 0; i < c.FinalSize; i++ {
		itr.freeList <- i
	}

	for i := 0; i < len(itr.byteChunks); i++ {
		itr.rand.Read(itr.byteChunks[i][:])
	}

	err := itr.Next()
	return itr, err
}

type ChangesetItr struct {
	Changeset *api.Changeset

	version           int64
	rand              *rand.Rand
	gen               ChangesetGenerator
	keys              [][]byte
	freeList          chan int
	createsPerVersion float64
	createAccumulator float64
	deleteMiss        *metrics.Counter
	updateMiss        *metrics.Counter
	byteChunks        [][]byte
}

func (itr *ChangesetItr) nextVersion() {
	itr.version++
	itr.Changeset = &api.Changeset{Version: itr.version}
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

func (itr *ChangesetItr) GetChangeset() *api.Changeset {
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
	rem := length % byteChunkSize
	for i := 0; i < length-rem; i += byteChunkSize {
		copy(b[i:], itr.byteChunks[itr.rand.Intn(byteChunkCount)])
	}
	// fill remainder (entire buffer if length < byteChunkSize)
	itr.rand.Read(b[length-rem:])
	return b
}

type ChangesetIterators struct {
	*api.Changeset
	iterators    []ChangesetIterator
	versionSkips int
	idx          int
}

func NewChangesetIterators(gens []ChangesetGenerator) (ChangesetIterator, error) {
	if len(gens) == 0 {
		return nil, fmt.Errorf("must provide at least one generator")
	}
	itr := &ChangesetIterators{Changeset: &api.Changeset{}}
	versions := gens[0].Versions
	for _, gen := range gens {
		if gen.Versions != versions {
			return nil, fmt.Errorf("all generators must have the same number of versions")
		}
		i, err := gen.Iterator()
		if err != nil {
			return nil, err
		}
		itr.iterators = append(itr.iterators, i)
		itr.Nodes = append(itr.Nodes, i.GetChangeset().Nodes...)
		itr.Version = i.GetChangeset().Version
	}

	return itr, nil
}

func (itr *ChangesetIterators) Next() error {
	changeset := &api.Changeset{}
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

func (itr *ChangesetIterators) GetChangeset() *api.Changeset {
	return itr.Changeset
}
