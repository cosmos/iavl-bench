package main

import (
	"context"
	"errors"
	"fmt"

	"cosmossdk.io/store/pruning/types"
	storetypes "cosmossdk.io/store/types"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cosmos/cosmos-sdk/iavl"
	"github.com/cosmos/cosmos-sdk/telemetry"

	"github.com/cosmos/iavl-bench/bench"
)

type multiTree struct {
	mt *iavl.CommitMultiTree
}

func NewMultiTree(storeKeys []*storetypes.KVStoreKey, dir string, opts Options) (bench.RootMultiTree, error) {
	mt, err := iavl.LoadCommitMultiTree(dir, opts.DB)
	if err != nil {
		return nil, fmt.Errorf("failed to load commit multi tree: %w", err)
	}
	mt.SetPruning(opts.Pruning)
	for _, key := range storeKeys {
		mt.MountStoreWithDB(key, storetypes.StoreTypeIAVL, nil)
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

func (m *multiTree) CacheMultiTree() bench.MultiTree {
	return m.mt.CacheMultiStore()
}

func (m *multiTree) Commit(multiTree bench.MultiTree) error {
	finalizer, err := m.mt.StartCommit(context.Background(), multiTree.(storetypes.MultiStore), cmtproto.Header{})
	if err != nil {
		return fmt.Errorf("failed to start commit: %w", err)
	}
	_, err = finalizer.Finalize()
	return err
}

func (m *multiTree) Close() error {
	return errors.Join(
		m.mt.Close(),
		telemetry.Shutdown(context.Background()),
	)
}

type Options struct {
	DB      iavl.Options         `json:"db"`
	Pruning types.PruningOptions `json:"pruning"`
}

func main() {
	bench.Run("iavlx", bench.RunConfig{
		OptionsType: &Options{},
		TreeLoader: func(params bench.LoaderParams) (bench.RootMultiTree, error) {
			opts := params.TreeOptions.(*Options)
			if opts == nil {
				opts = &Options{}
			}

			mt, err := NewMultiTree(params.StoreKeys, params.TreeDir, opts.DB)
			if err != nil {
				return nil, fmt.Errorf("failed to create multi tree: %w", err)
			}
			return mt, nil
		},
	})
}
