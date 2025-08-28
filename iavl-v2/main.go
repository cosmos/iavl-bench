package main

import (
	"fmt"

	"github.com/cosmos/iavl/v2"

	"github.com/cosmos/iavl-bench/bench"
)

//func main() {
//	root, err := bench.RootCommand()
//	if err != nil {
//		os.Exit(1)
//	}
//
//	root.AddCommand(cmd.TreeCommand(&cmd.Context{
//		TreeContext: bench.TreeContext{
//			Context: context.Background(),
//		},
//	}))
//
//	if err := root.Execute(); err != nil {
//		fmt.Printf("Error: %s\n", err.Error())
//		os.Exit(1)
//	}
//}

type MultiTreeWrapper struct {
	version int64
	trees   map[string]*iavl.Tree
}

func (m MultiTreeWrapper) Version() int64 {
	return m.version
}

func (m MultiTreeWrapper) ApplyUpdate(storeKey string, key, value []byte, delete bool) error {
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

func (m MultiTreeWrapper) Commit() error {
	for _, tree := range m.trees {
		_, _, err := tree.SaveVersion()
		if err != nil {
			return err
		}
	}
	m.version++
	return nil
}

var _ bench.Tree = &MultiTreeWrapper{}

func main() {
	bench.Run(bench.RunConfig{
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			dbDir := params.TreeDir
			// TODO allow loading existing version
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
				opts := iavl.DefaultTreeOptions()
				tree := iavl.NewTree(sqlite, nodePool, opts)
				trees[storeName] = tree
			}
			return MultiTreeWrapper{trees: trees}, nil
		},
	})
}
