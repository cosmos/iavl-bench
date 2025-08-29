package store_v1

import (
	"fmt"

	"cosmossdk.io/log"
	"cosmossdk.io/store/metrics"
	"cosmossdk.io/store/rootmulti"
	"cosmossdk.io/store/types"
	db "github.com/cosmos/cosmos-db"

	"github.com/cosmos/iavl-bench/bench"
)

type storeWrapper struct {
	storeKeys map[string]types.StoreKey
	store     *rootmulti.Store
}

func (s *storeWrapper) Version() int64 {
	return s.store.LatestVersion()
}

func (s *storeWrapper) ApplyUpdate(storeKey string, key, value []byte, delete bool) error {
	sk, ok := s.storeKeys[storeKey]
	if !ok {
		return fmt.Errorf("store key %s not found", storeKey)
	}
	store := s.store.GetKVStore(sk)
	if delete {
		store.Delete(key)
	} else {
		store.Set(key, value)
	}
	return nil
}

func (s *storeWrapper) Commit() error {
	_ = s.store.Commit()
	return nil
}

var _ bench.Tree = &storeWrapper{}

func Run() {
	bench.Run("store-v1", bench.RunConfig{
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			dbDir := params.TreeDir

			storeKeys := make(map[string]types.StoreKey)
			for _, name := range params.StoreNames {
				storeKeys[name] = types.NewKVStoreKey(name)
			}

			d, err := db.NewGoLevelDB("bench-store", dbDir, nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create db: %w", err)
			}
			store := rootmulti.NewStore(d, log.NewNopLogger(), metrics.NewNoOpMetrics())

			for _, sk := range storeKeys {
				store.MountStoreWithDB(sk, types.StoreTypeIAVL, nil)
			}

			err = store.LoadLatestVersion()
			if err != nil {
				return nil, fmt.Errorf("failed to load latest version: %w", err)
			}

			return &storeWrapper{
				storeKeys: storeKeys,
				store:     store,
			}, nil
		},
	})
}
