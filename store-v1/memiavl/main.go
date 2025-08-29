package main

import (
	"cosmossdk.io/log"
	"cosmossdk.io/store/cronos/rootmulti"
	"
)

func main() {
	bench.Run("store-memiavl", bench.RunConfig{
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			store := rootmulti.NewStore(params.TreeDir, log.NewNopLogger(), false, false)
			return store_v1.NewCommitMultiStoreWrapper(store, params.StoreNames)
		},
	})
}
