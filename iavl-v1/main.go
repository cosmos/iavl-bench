package main

import (
	"fmt"

	"cosmossdk.io/log"
	"github.com/cosmos/iavl"
	"github.com/cosmos/iavl/db"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/bench/util"
)

type MultiTreeWrapper struct {
	dbDir   string
	version int64
	trees   map[string]*iavl.MutableTree
}

func (m *MultiTreeWrapper) Commit(updates bench.MultiStoreUpdates) error {
	// for now just do this sequentially since that's what we were doing before
	for storeKey, treeUpdates := range updates {
		tree, ok := m.trees[storeKey]
		if !ok {
			return fmt.Errorf("store key %s not found", storeKey)
		}
		for _, update := range treeUpdates.Updates {
			if update.Delete {
				_, _, err := tree.Remove(update.Key)
				if err != nil {
					return err
				}
			} else {
				_, err := tree.Set(update.Key, update.Value)
				if err != nil {
					return err
				}
			}
		}

		_, _, err := tree.SaveVersion()
		if err != nil {
			return err
		}
	}
	m.version++

	return util.SaveVersion(m.dbDir, m.version)
}

func (m *MultiTreeWrapper) Tree(storeName string) bench.TreeReader {
	store, ok := m.trees[storeName]
	if !ok {
		return nil
	}
	return treeReader{
		store: store,
	}
}

type treeReader struct {
	store *iavl.MutableTree
}

func (t treeReader) Get(key []byte) ([]byte, error) {
	return t.store.Get(key)
}

func (t treeReader) Size() int64 {
	return t.store.Size()
}

func (m *MultiTreeWrapper) ForceToDisk() error {
	return fmt.Errorf("not implemented")
}

func (m *MultiTreeWrapper) Close() error {
	for _, tree := range m.trees {
		err := tree.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MultiTreeWrapper) Version() int64 {
	return m.version
}

var _ bench.MultiTree = &MultiTreeWrapper{}

type Options struct {
	SkipFastStorageUpgrade bool `json:"skip_fast_storage_upgrade"`
	CacheSize              int  `json:"cache_size"`
}

func main() {
	bench.Run("iavl/v1", bench.RunConfig{
		OptionsType: &Options{},
		TreeLoader: func(params bench.LoaderParams) (bench.MultiTree, error) {
			opts := params.TreeOptions.(*Options)
			dbDir := params.TreeDir
			version, err := util.LoadVersion(dbDir)
			if err != nil {
				return nil, err
			}
			trees := make(map[string]*iavl.MutableTree)
			//logger := util.NewSlogWrapper(params.Logger)
			// logging is very noisy, use a nop logger
			logger := log.NewNopLogger()
			for _, storeName := range params.StoreNames {
				d, err := db.NewGoLevelDBWithOpts(storeName, dbDir, &opt.Options{})
				if err != nil {
					return nil, err
				}
				tree := iavl.NewMutableTree(d, opts.CacheSize, opts.SkipFastStorageUpgrade, logger)
				if version != 0 {
					_, err := tree.LoadVersion(version)
					if err != nil {
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
