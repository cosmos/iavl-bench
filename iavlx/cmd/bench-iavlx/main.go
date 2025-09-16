package main

import (
	"log/slog"

	"github.com/cosmos/iavl-bench/bench"
	"iavlx"
)

type MemMultiTree struct {
	logger  *slog.Logger
	trees   map[string]iavlx.CommitTree
	version int64
}

func NewMemMultiTree(logger *slog.Logger) *MemMultiTree {
	return &MemMultiTree{
		logger: logger,
		trees:  make(map[string]iavlx.CommitTree),
	}
}

func (m *MemMultiTree) Version() int64 {
	return m.version
}

func (m *MemMultiTree) ApplyUpdate(storeKey string, key, value []byte, delete bool) error {
	tree, ok := m.trees[storeKey]
	if !ok {
		tree = *iavlx.NewCommitTree(iavlx.MemStore{})
		m.trees[storeKey] = tree
	}
	if delete {
		return tree.Delete(key)
	} else {
		return tree.Set(key, value)
	}
}

func (m *MemMultiTree) Commit() error {
	for _, tree := range m.trees {
		hash, err := tree.Commit()
		if err != nil {
			return err
		}
		m.logger.Info("committed", "hash", hash, "version", tree.Version())
	}
	m.version++
	return nil
}

var _ bench.Tree = (*MemMultiTree)(nil)

func main() {
	bench.Run("iavlx", bench.RunConfig{
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			return NewMemMultiTree(params.Logger), nil
		},
	})
}
