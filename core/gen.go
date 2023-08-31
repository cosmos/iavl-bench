package core

import (
	"fmt"
	"math/rand"

	api "github.com/kocubinski/costor-api"
)

func BankLikeGenerator(seed int64, versions int) ChangesetGenerator {
	return ChangesetGenerator{
		StoreKey:         "bank",
		Seed:             seed,
		KeyMean:          56,
		KeyStdDev:        3,
		ValueMean:        100,
		ValueStdDev:      1200,
		InitialSize:      35_000,
		FinalSize:        2_200_200,
		Versions:         versions,
		ChangePerVersion: 368_000_000 / versions,
		DeleteFraction:   0.06,
	}
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
	Versions         int
	ChangePerVersion int
	DeleteFraction   float64
}

func (c ChangesetGenerator) Iterator() (*ChangesetIterator, error) {
	if c.FinalSize < c.InitialSize {
		return nil, fmt.Errorf("final size must be greater than initial size")
	}

	itr := &ChangesetIterator{
		gen:  c,
		rand: rand.New(rand.NewSource(c.Seed)),
		// TODO
		// this approximation must be padded with the expected number of deletes per version
		// createsPerVersion needs to be flaot to account for avg creates per version < 1
		// accumulate creates and only write when >= 1, then subtract what was created
		createsPerVersion: (c.FinalSize - c.InitialSize) / (c.Versions - 1),
		keysHashes:        map[[16]byte]struct{}{},
	}

	err := itr.Next()
	return itr, err
}

type ChangesetIterator struct {
	Node    *api.Node
	Version int

	rand              *rand.Rand
	gen               ChangesetGenerator
	keys              [][]byte
	keysHashes        map[[16]byte]struct{}
	createsPerVersion int
	versionNodes      []*api.Node
	versionIndex      int
	deletable         [][]byte
}

func (itr *ChangesetIterator) nextVersion() {
	itr.Version++
	itr.versionIndex = 0
	itr.versionNodes = nil

	deletes := int(itr.gen.DeleteFraction * float64(itr.gen.ChangePerVersion))
	updates := itr.gen.ChangePerVersion - deletes

	// only delete past version 1
	if itr.Version > 1 {
		for i := 0; i < deletes; i++ {
			j := itr.rand.Intn(len(itr.keys))
			node := &api.Node{
				StoreKey: itr.gen.StoreKey,
				Block:    int64(itr.Version),
				Key:      itr.keys[j],
				Delete:   true,
			}

			itr.keys = append(itr.keys[:j], itr.keys[j+1:]...)
			itr.versionNodes = append(itr.versionNodes, node)
		}
	}

	if itr.Version > 1 {
		for i := 0; i < updates; i++ {
			j := itr.rand.Intn(len(itr.keys))
			itr.versionNodes = append(itr.versionNodes, &api.Node{
				StoreKey: itr.gen.StoreKey,
				Block:    int64(itr.Version),
				Key:      itr.keys[j],
				Value:    itr.genBytes(itr.gen.ValueMean, itr.gen.ValueStdDev),
			})
		}
	}

	var (
		creates int
		//createCollisions int
	)
	if itr.Version == 1 {
		creates = itr.gen.InitialSize
	} else {
		creates = itr.createsPerVersion + deletes
	}
	for i := 0; i < creates; i++ {
		node := &api.Node{
			StoreKey: itr.gen.StoreKey,
			Key:      itr.genBytes(itr.gen.KeyMean, itr.gen.KeyStdDev),
			Value:    itr.genBytes(itr.gen.ValueMean, itr.gen.ValueStdDev),
			Block:    int64(itr.Version),
		}
		itr.versionNodes = append(itr.versionNodes, node)
		itr.keys = append(itr.keys, node.Key)
	}

	itr.rand.Shuffle(len(itr.versionNodes), func(i, j int) {
		itr.versionNodes[i], itr.versionNodes[j] = itr.versionNodes[j], itr.versionNodes[i]
	})
}

func (itr *ChangesetIterator) Next() error {
	if itr.versionIndex >= len(itr.versionNodes) {
		if itr.Version == itr.gen.Versions {
			itr.Node = nil
			return nil
		}
		itr.nextVersion()
	}

	itr.Node = itr.versionNodes[itr.versionIndex]
	itr.versionIndex++
	return nil
}

func (itr *ChangesetIterator) Valid() bool {
	return itr.Node != nil
}

func (itr *ChangesetIterator) genBytes(mean, stdDev int) []byte {
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

func (itr *ChangesetIterator) GetNode() *api.Node {
	return itr.Node
}

type ChangesetIterators struct {
	iterators   []ChangesetIterator
	lastVersion int
	idx         int
	Node        *api.Node
}

func NewChangesetIterators(gens []ChangesetGenerator) (*ChangesetIterators, error) {
	if len(gens) == 0 {
		return nil, fmt.Errorf("must provide at least one generator")
	}

	var iterators []ChangesetIterator
	version := gens[0].Versions
	for _, gen := range gens {
		if gen.Versions != version {
			return nil, fmt.Errorf("all generators must have the same number of versions")
		}
		itr, err := gen.Iterator()
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, *itr)
	}

	itr := &ChangesetIterators{
		iterators: iterators,
	}
	err := itr.Next()
	if err != nil {
		return nil, err
	}
	return itr, nil
}

func (itr *ChangesetIterators) Next() error {
	// terminal condition
	if len(itr.iterators) == 0 {
		itr.Node = nil
		return nil
	}

	// reset index if we've reached the end of the list
	if itr.idx >= len(itr.iterators) {
		itr.idx = 0
	}

	curItr := itr.iterators[itr.idx]
	err := curItr.Next()
	if err != nil {
		return err
	}
	// when we reach the end of an iterator, remove it from the list
	if !curItr.Valid() {
		itr.iterators = append(itr.iterators[:itr.idx], itr.iterators[itr.idx+1:]...)
		return itr.Next()
	}

	// nominal case
	itr.Node = curItr.Node
	itr.idx++
	return nil
}

func (itr *ChangesetIterators) Valid() bool {
	return itr.Node != nil
}

func (itr *ChangesetIterators) GetNode() *api.Node {
	return itr.Node
}
