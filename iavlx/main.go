package main

import (
	"context"
	"errors"
	"fmt"

	"cosmossdk.io/log/v2"
	"cosmossdk.io/log/v2/slog"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cosmos/cosmos-sdk/iavlx"
	pruningtypes "github.com/cosmos/cosmos-sdk/store/v2/pruning/types"
	storetypes "github.com/cosmos/cosmos-sdk/store/v2/types"
	"github.com/cosmos/cosmos-sdk/telemetry"

	"github.com/cosmos/iavl-bench/bench"
)

type multiTree struct {
	mt *iavlx.CommitMultiTree
}

func NewMultiTree(storeKeys []*storetypes.KVStoreKey, dir string, opts iavlx.Options, logger log.Logger) (bench.RootMultiTree, error) {
	mt, err := iavlx.LoadCommitMultiTree(dir, opts, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to load commit multi tree: %w", err)
	}
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

func (m *multiTree) SetPruning(pruning pruningtypes.PruningOptions) {
	m.mt.SetPruning(pruning)
}

func (m *multiTree) CacheMultiTree() bench.MultiTree {
	return m.mt.CommitBranch()
}

func (m *multiTree) Commit(multiTree bench.MultiTree) (storetypes.CommitID, error) {
	cb, ok := multiTree.(*iavlx.CommitBranch)
	if !ok {
		return storetypes.CommitID{}, fmt.Errorf("expected iavlx.CommitBranch, got %T", multiTree)
	}
	finalizer, err := cb.StartCommit(context.Background(), cmtproto.Header{})
	if err != nil {
		return storetypes.CommitID{}, fmt.Errorf("failed to start commit: %w", err)
	}
	return finalizer.Finalize()
}

func (m *multiTree) Close() error {
	return errors.Join(
		m.mt.Close(),
		telemetry.Shutdown(context.Background()),
	)
}

func main() {
	bench.Run("iavlx", bench.RunConfig{
		OptionsType: &iavlx.Options{},
		TreeLoader: func(params bench.LoaderParams) (bench.RootMultiTree, error) {
			opts := params.TreeOptions.(*iavlx.Options)
			if opts == nil {
				opts = &iavlx.Options{}
			}

			mt, err := NewMultiTree(params.StoreKeys, params.TreeDir, *opts, slog.NewCustomLogger(params.Logger))
			if err != nil {
				return nil, fmt.Errorf("failed to create multi tree: %w", err)
			}
			return mt, nil
		},
	})
}
