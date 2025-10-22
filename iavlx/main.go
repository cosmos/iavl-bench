package main

import (
	"cosmossdk.io/log/slog"
	iavlx "github.com/cosmos/cosmos-sdk/iavl"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/store-v1"
)

func main() {
	bench.Run("iavlx", bench.RunConfig{
		OptionsType: &iavlx.Options{},
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			opts := params.TreeOptions.(*iavlx.Options)
			if opts == nil {
				opts = &iavlx.Options{}
			}
			store, err := iavlx.LoadDB(
				params.TreeDir,
				opts,
				slog.NewCustomLogger(params.Logger),
			)
			if err != nil {
				return nil, err
			}
			return store_v1.NewCommitMultiStoreWrapper(store, params.StoreNames)
		},
	})
}
