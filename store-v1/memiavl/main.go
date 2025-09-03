package main

import (
	"cosmossdk.io/store/cronos/rootmulti"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/bench/util"
	"github.com/cosmos/iavl-bench/store-v1"
)

func main() {
	bench.Run("store-memiavl", bench.RunConfig{
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			store := rootmulti.NewStore(
				params.TreeDir,
				util.NewSlogWrapper(params.Logger),
				false,
				false,
			)
			return store_v1.NewCommitMultiStoreWrapper(store, params.StoreNames)
		},
	})
}
