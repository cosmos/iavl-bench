package main

import (
	"fmt"

	"cosmossdk.io/log"
	"github.com/cosmos/iavl"
	"github.com/cosmos/iavl/db"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/bench/util"
)

type MultiTreeWrapper struct {
	dbDir   string
	version int64
	trees   map[string]*iavl.MutableTree
}

func (m *MultiTreeWrapper) Close() error {
	for _, tree := range m.trees {
		if err := tree.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (m *MultiTreeWrapper) Version() int64 {
	return m.version
}

func (m *MultiTreeWrapper) ApplyUpdate(storeKey string, key, value []byte, delete bool) error {
	tree, ok := m.trees[storeKey]
	if !ok {
		return fmt.Errorf("store key %s not found", storeKey)
	}
	if delete {
		_, _, err := tree.Remove(key)
		return err
	}
	_, err := tree.Set(key, value)
	return err
}

func (m *MultiTreeWrapper) Commit() error {
	for _, tree := range m.trees {
		if _, _, err := tree.SaveVersion(); err != nil {
			return err
		}
	}
	m.version++
	return util.SaveVersion(m.dbDir, m.version)
}

var _ bench.Tree = &MultiTreeWrapper{}

type Options struct {
	CacheSize              int  `json:"cache_size"`
	SkipFastStorageUpgrade bool `json:"skip_fast_storage_upgrade"`
}

func main() {
	bench.Run("iavl/v1-memdb", bench.RunConfig{
		OptionsType: &Options{},
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			opts := params.TreeOptions.(*Options)
			dbDir := params.TreeDir
			version, err := util.LoadVersion(dbDir)
			if err != nil {
				return nil, err
			}

			trees := make(map[string]*iavl.MutableTree)
			logger := log.NewNopLogger()
			for _, storeName := range params.StoreNames {
				memdb := db.NewMemDB()
				tree := iavl.NewMutableTree(memdb, opts.CacheSize, opts.SkipFastStorageUpgrade, logger)
				if version != 0 {
					if _, err := tree.LoadVersion(version); err != nil {
						return nil, fmt.Errorf("loading store %s at version %d: %w", storeName, version, err)
					}
				}
				trees[storeName] = tree
			}

			return &MultiTreeWrapper{
				trees:   trees,
				version: version,
				dbDir:   dbDir,
			}, nil
		},
	})
}
