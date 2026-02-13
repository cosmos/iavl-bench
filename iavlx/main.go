package main

import (
	"context"
	"errors"
	"fmt"

	"cosmossdk.io/store/pruning/types"
	storetypes "cosmossdk.io/store/types"
	"github.com/cosmos/cosmos-sdk/iavl"
	"github.com/cosmos/cosmos-sdk/telemetry"

	"github.com/cosmos/iavl-bench/bench"
)

type multiTree struct {
	mt *iavl.CommitMultiTree
}

func NewMultiTree(storeNames []string, dir string, opts iavl.Options) (bench.MultiTree, error) {
	mt, err := iavl.LoadCommitMultiTree(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to load commit multi tree: %w", err)
	}
	for _, name := range storeNames {
		mt.MountStoreWithDB(storetypes.NewKVStoreKey(name), storetypes.StoreTypeIAVL, nil)
	}
	err = mt.LoadLatestVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to load latest version: %w", err)
	}
	return &multiTree{
		mt: mt,
	}, nil
}

func (m *multiTree) Version() int64 {
	return m.mt.LatestVersion()
}

func (m *multiTree) Commit(updates bench.MultiStoreUpdates) error {
	panic("TODO")
}

func (m *multiTree) Tree(storeName string) bench.TreeReader {
	panic("TODO")
}

func (m *multiTree) Close() error {
	return errors.Join(
		// TODO add closing individual trees if needed
		telemetry.Shutdown(context.Background()),
	)
}

var _ bench.MultiTree = (*multiTree)(nil)

type Options struct {
	DB      iavl.Options         `json:"db"`
	Pruning types.PruningOptions `json:"pruning"`
}

func main() {
	bench.Run("iavlx", bench.RunConfig{
		OptionsType: &Options{},
		TreeLoader: func(params bench.LoaderParams) (bench.MultiTree, error) {
			opts := params.TreeOptions.(*Options)
			if opts == nil {
				opts = &Options{}
			}

			mt, err := NewMultiTree(params.StoreNames, params.TreeDir, opts.DB)
			if err != nil {
				return nil, fmt.Errorf("failed to create multi tree: %w", err)
			}
			return mt, nil
		},
	})
}
