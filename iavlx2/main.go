package main

import (
	"fmt"
	"sync"

	"github.com/cosmos/cosmos-sdk/iavl"

	"github.com/cosmos/iavl-bench/bench"
)

type multiTree struct {
	version int64
	trees   map[string]iavl.CommitTree
}

func (m *multiTree) Version() int64 {
	return m.version
}

func (m *multiTree) Commit(updates bench.MultiStoreUpdates) error {
	var wg sync.WaitGroup
	for storeName, treeUpdates := range updates {
		tree, exists := m.trees[storeName]
		if !exists {
			return fmt.Errorf("store not found: %s", storeName)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := tree.Commit(treeUpdates)
			if err != nil {
				panic(err)
			}
		}()
	}
	wg.Wait()
	m.version++
	return nil
}

func (m *multiTree) Tree(storeName string) bench.TreeReader {
	//TODO implement me
	panic("implement me")
}

func (m *multiTree) Close() error {
	return nil
}

var _ bench.MultiTree = (*multiTree)(nil)
