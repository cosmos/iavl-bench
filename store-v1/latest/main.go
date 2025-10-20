package main

import (
	"fmt"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	db "github.com/cosmos/cosmos-db"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/store-v1"
)

func main() {
	bench.Run("store-v1", bench.RunConfig{
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			d, err := db.NewGoLevelDB("bench-store", params.TreeDir, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create db: %w", err)
			}
			// use a no-op logger because logging is very noisy
			store := rootmulti.NewStore(d, log.NewNopLogger(), metrics.NewNoOpMetrics())
			return store_v1.NewCommitMultiStoreWrapper(store, params.StoreNames)
		},
	})
}
