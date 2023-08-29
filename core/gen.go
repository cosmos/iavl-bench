package core

import (
	"fmt"
	"math/rand"

	api "github.com/kocubinski/costor-api"
)

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

func (c ChangesetGenerator) Iterator() (*ChangesetGenIterator, error) {
	if c.FinalSize < c.InitialSize {
		return nil, fmt.Errorf("final size must be greater than initial size")
	}

	itr := &ChangesetGenIterator{
		gen:               c,
		rand:              rand.New(rand.NewSource(c.Seed)),
		createsPerVersion: (c.FinalSize - c.InitialSize) / (c.Versions - 1),
	}

	err := itr.Next()
	return itr, err
}

type ChangesetGenIterator struct {
	Node    *api.Node
	Version int

	rand              *rand.Rand
	gen               ChangesetGenerator
	keys              [][]byte
	createsPerVersion int
	versionNodes      []*api.Node
	versionIndex      int
}

func (itr *ChangesetGenIterator) nextVersion() {
	itr.Version++
	itr.versionIndex = 0
	itr.versionNodes = nil

	deletes := int(itr.gen.DeleteFraction * float64(itr.gen.ChangePerVersion))
	updates := itr.gen.ChangePerVersion - deletes

	// only delete past version 1
	if itr.Version > 1 {
		for i := 0; i < deletes; i++ {
			j := itr.rand.Intn(len(itr.keys))
			itr.versionNodes = append(itr.versionNodes, &api.Node{
				Key:    itr.keys[j],
				Delete: true,
			})
			itr.keys = append(itr.keys[:j], itr.keys[j+1:]...)
		}
	}

	if itr.Version > 1 {
		for i := 0; i < updates; i++ {
			j := itr.rand.Intn(len(itr.keys))
			itr.versionNodes = append(itr.versionNodes, &api.Node{
				Key:   itr.keys[j],
				Value: itr.genBytes(itr.gen.ValueMean, itr.gen.ValueStdDev),
			})
		}
	}

	var creates int
	if itr.Version == 1 {
		creates = itr.gen.InitialSize
	} else {
		creates = itr.createsPerVersion + deletes
	}
	for i := 0; i < creates; i++ {
		node := &api.Node{
			Key:   itr.genBytes(itr.gen.KeyMean, itr.gen.KeyStdDev),
			Value: itr.genBytes(itr.gen.ValueMean, itr.gen.ValueStdDev),
		}
		itr.versionNodes = append(itr.versionNodes, node)
		itr.keys = append(itr.keys, node.Key)
	}

	rand.Shuffle(len(itr.versionNodes), func(i, j int) {
		itr.versionNodes[i], itr.versionNodes[j] = itr.versionNodes[j], itr.versionNodes[i]
	})
}

func (itr *ChangesetGenIterator) Next() error {
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

func (itr *ChangesetGenIterator) Valid() bool {
	return itr.Node != nil
}

func (itr *ChangesetGenIterator) genBytes(mean, stdDev int) []byte {
	length := int(itr.rand.NormFloat64()*float64(stdDev) + float64(mean))
	if length < 1 {
		length = 1
	}
	b := make([]byte, length)
	itr.rand.Read(b)
	return b
}
