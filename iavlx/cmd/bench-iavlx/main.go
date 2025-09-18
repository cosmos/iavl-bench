package main

import (
	"log/slog"
	"path/filepath"

	"github.com/cosmos/iavl-bench/bench"
	"iavlx"
)

type MemMultiTree struct {
	logger  *slog.Logger
	dbDir   string
	trees   map[string]*iavlx.CommitTree
	version int64
}

func NewMemMultiTree(logger *slog.Logger, dbDir string) *MemMultiTree {
	return &MemMultiTree{
		logger: logger,
		trees:  make(map[string]*iavlx.CommitTree),
		dbDir:  dbDir,
	}
}

func (m *MemMultiTree) Version() int64 {
	return m.version
}

func (m *MemMultiTree) ApplyUpdate(storeKey string, key, value []byte, delete bool) error {
	tree, ok := m.trees[storeKey]
	if !ok {
		//leafDb, err := dbm.NewGoLevelDB(fmt.Sprintf("%s.leaves", storeKey), m.dbDir, nil)
		//if err != nil {
		//	return err
		//}
		//branchDb, err := dbm.NewGoLevelDB(fmt.Sprintf("%s.branches", storeKey), m.dbDir, nil)
		//if err != nil {
		//	return err
		//}
		dbDir := filepath.Join(m.dbDir, storeKey)
		tree, err := iavlx.NewCommitTree(dbDir)
		if err != nil {
			return err
		}
		m.trees[storeKey] = tree
	}
	batch := tree.Branch()
	var err error
	if delete {
		err = batch.Remove(key)
	} else {
		err = batch.Set(key, value)
	}
	if err != nil {
		return err
	}
	return tree.ApplyBatch(batch)
}

func (m *MemMultiTree) Commit() error {
	for storeName, tree := range m.trees {
		hash, err := tree.Commit()
		if err != nil {
			return err
		}
		m.logger.Info("committed", "hash", hash, "version", tree.Version(), "store", storeName)
	}
	m.version++
	return nil
}

var _ bench.Tree = (*MemMultiTree)(nil)

func main() {
	bench.Run("iavlx", bench.RunConfig{
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			return NewMemMultiTree(params.Logger, params.TreeDir), nil
		},
	})
}
