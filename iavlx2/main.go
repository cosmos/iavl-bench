package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/cosmos/cosmos-sdk/iavl"
	"github.com/cosmos/cosmos-sdk/telemetry"

	"github.com/cosmos/iavl-bench/bench"
)

type multiTree struct {
	version int64
	trees   map[string]*iavl.CommitTree
}

func NewMultiTree(storeNames []string, dir string, opts iavl.Options) (bench.MultiTree, error) {
	trees := make(map[string]*iavl.CommitTree)
	for _, name := range storeNames {
		var err error
		treeDir := filepath.Join(dir, name)
		trees[name], err = iavl.NewCommitTree(treeDir, opts)
		if err != nil {
			return nil, fmt.Errorf("failed to create tree for store %s: %w", name, err)
		}
	}
	return &multiTree{
		trees: trees,
	}, nil
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
			_, err := tree.Commit(context.Background(), treeUpdates.Updates, int(treeUpdates.TotalOps))
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
	return m.trees[storeName].Latest()
}

func (m *multiTree) Close() error {
	return errors.Join(
		// TODO add closing individual trees if needed
		telemetry.Shutdown(context.Background()),
	)
}

var _ bench.MultiTree = (*multiTree)(nil)

func main() {
	bench.Run("iavlx2", bench.RunConfig{
		OptionsType: &iavl.Options{},
		TreeLoader: func(params bench.LoaderParams) (bench.MultiTree, error) {
			opts := params.TreeOptions.(*iavl.Options)
			if opts == nil {
				opts = &iavl.Options{}
			}

			return NewMultiTree(params.StoreNames, params.TreeDir, *opts)
		},
	})
}
