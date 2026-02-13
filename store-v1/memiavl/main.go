package main

import (
	"cosmossdk.io/log/slog" // TODO switch to "cosmossdk.io/log/v2" which fixes the slog wrapper
	"cosmossdk.io/store/cronos/rootmulti"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/store-v1"
)

func main() {
	bench.Run("store-memiavl", bench.RunConfig{
		TreeLoader: func(params bench.LoaderParams) (bench.RootMultiTree, error) {
			store := rootmulti.NewStore(
				params.TreeDir,
				slog.NewCustomLogger(params.Logger),
				false,
				false,
			)
			return store_v1.NewCommitMultiStoreWrapper(store, params.StoreKeys)
		},
	})
}
