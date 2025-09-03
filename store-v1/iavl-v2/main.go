package main

import (
	"fmt"

	"cosmossdk.io/log/slog"
	"cosmossdk.io/store/iavl2"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	db "github.com/cosmos/cosmos-db"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/store-v1"
)

func main() {
	bench.Run("store-v1", bench.RunConfig{
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			d, err := db.NewGoLevelDB("not-used", params.TreeDir, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create db: %w", err)
			}
			store := rootmulti.NewStore(d, slog.NewCustomLogger(params.Logger), metrics.NewNoOpMetrics())
			store.EnableIAVLV2(&iavl2.Config{
				Path: params.TreeDir,
			})
			return store_v1.NewCommitMultiStoreWrapper(store, params.StoreNames)
		},
	})
}
