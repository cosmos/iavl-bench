package iavl_v2

import (
	"fmt"

	"github.com/cosmos/iavl/v2"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/bench/multitreeutil"
)

type MultiTreeWrapper struct {
	dbDir   string
	version int64
	trees   map[string]*iavl.Tree
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
	} else {
		_, err := tree.Set(key, value)
		return err
	}
}

func (m *MultiTreeWrapper) Commit() error {
	for _, tree := range m.trees {
		_, _, err := tree.SaveVersion()
		if err != nil {
			return err
		}
	}

	m.version++

	return multitreeutil.SaveVersion(m.dbDir, m.version)
}

var _ bench.Tree = &MultiTreeWrapper{}

type Options struct {
	CheckpointInterval int64 `json:"checkpoint_interval"`
	EvictionDepth      int8  `json:"eviction_depth"`
}

func Run(treeType string) {
	bench.Run(treeType, bench.RunConfig{
		OptionsType: &Options{},
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			opts := params.TreeOptions.(*Options)
			dbDir := params.TreeDir
			version, err := multitreeutil.LoadVersion(dbDir)
			if err != nil {
				return nil, err
			}
			trees := make(map[string]*iavl.Tree)
			nodePool := iavl.NewNodePool()
			for _, storeName := range params.StoreNames {
				sqliteOpts := iavl.SqliteDbOptions{
					Path: fmt.Sprintf("%s/%s", dbDir, storeName),
				}
				sqlite, err := iavl.NewSqliteDb(nodePool, sqliteOpts)
				if err != nil {
					return nil, err
				}
				treeOpts := iavl.DefaultTreeOptions()
				if opts.CheckpointInterval != 0 {
					treeOpts.CheckpointInterval = opts.CheckpointInterval
				}
				if opts.EvictionDepth != 0 {
					treeOpts.EvictionDepth = opts.EvictionDepth
				}
				tree := iavl.NewTree(sqlite, nodePool, treeOpts)
				if version != 0 {
					err = tree.LoadVersion(version)
					if err != nil {
						return nil, fmt.Errorf("loading version %d for store %s: %w", version, storeName, err)
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
