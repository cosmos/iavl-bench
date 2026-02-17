package store_v1

import (
	"fmt"
	"io"

	"cosmossdk.io/store/types"

	"github.com/cosmos/iavl-bench/bench"
)

type CommitMultiStoreWrapper struct {
	store types.CommitMultiStore
}

func NewCommitMultiStoreWrapper(store types.CommitMultiStore, storeKeys []*types.KVStoreKey) (*CommitMultiStoreWrapper, error) {
	for _, key := range storeKeys {
		store.MountStoreWithDB(key, types.StoreTypeIAVL, nil)
	}

	err := store.LoadLatestVersion()
	if err != nil {
		return nil, fmt.Errorf("failed to load latest version: %w", err)
	}

	return &CommitMultiStoreWrapper{store: store}, nil
}

func (s *CommitMultiStoreWrapper) Version() int64 {
	return s.store.LatestVersion()
}

func (s *CommitMultiStoreWrapper) CacheMultiTree() bench.MultiTree {
	return s.store.CacheMultiStore()
}

func (s *CommitMultiStoreWrapper) Commit(multiTree bench.MultiTree) (types.CommitID, error) {
	multiTree.(types.CacheMultiStore).Write()
	cid := s.store.Commit()
	return cid, nil
}

func (s *CommitMultiStoreWrapper) Close() error {
	if closer, ok := s.store.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

var _ bench.RootMultiTree = &CommitMultiStoreWrapper{}
