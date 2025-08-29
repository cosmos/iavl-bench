package store_v1

import (
	"fmt"

	"cosmossdk.io/store/types"

	"github.com/cosmos/iavl-bench/bench"
)

type CommitMultiStoreWrapper struct {
	storeKeys map[string]types.StoreKey
	store     types.CommitMultiStore
}

func NewCommitMultiStoreWrapper(store types.CommitMultiStore, storeNames []string) (*CommitMultiStoreWrapper, error) {
	storeKeys := make(map[string]types.StoreKey)
	for _, name := range storeNames {
		if _, exists := storeKeys[name]; exists {
			return nil, fmt.Errorf("duplicate store name: %s", name)
		}
		storeKeys[name] = types.NewKVStoreKey(name)
	}

	err := store.LoadLatestVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to load latest version: %w", err)
	}

	return &CommitMultiStoreWrapper{store: store, storeKeys: storeKeys}, nil
}

func (s *CommitMultiStoreWrapper) Version() int64 {
	return s.store.LatestVersion()
}

func (s *CommitMultiStoreWrapper) ApplyUpdate(storeKey string, key, value []byte, delete bool) error {
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

func (s *CommitMultiStoreWrapper) Commit() error {
	_ = s.store.Commit()
	return nil
}

var _ bench.Tree = &CommitMultiStoreWrapper{}
