package main

import (
	"github.com/cosmos/iavl-bench/bench"
	"iavlx"
)

type MemMultiTree struct {
	trees   map[string]iavlx.Tree
	version int64
}

func NewMemMultiTree() *MemMultiTree {
	return &MemMultiTree{
		trees: make(map[string]iavlx.Tree),
	}
}

func (m *MemMultiTree) Version() int64 {
	return m.version
}

func (m *MemMultiTree) ApplyUpdate(storeKey string, key, value []byte, delete bool) error {
	tree, ok := m.trees[storeKey]
	if !ok {
		tree = *iavlx.NewMemTree()
		m.trees[storeKey] = tree
	}
	if delete {
		return tree.Delete(key)
	} else {
		return tree.Set(key, value)
	}
}

func (m *MemMultiTree) Commit() error {
	m.version++
	return nil
}

var _ bench.Tree = (*MemMultiTree)(nil)

func main() {
	bench.Run("iavlx", bench.RunConfig{
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			return NewMemMultiTree(), nil
		},
	})
}
