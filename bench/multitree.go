package bench

import (
	"io"

	storetypes "cosmossdk.io/store/types"
)

type RootMultiTree interface {
	// Version should return the last committed version. If no version has been committed, it should return 0.
	Version() int64
	// CacheMultiTree should return a new MultiTree instance that allows cached writes that get committed when Commit gets called.
	CacheMultiTree() MultiTree
	// Commit should persist all changes made since the last commit and return the new version's hash.
	Commit(multiTree MultiTree) (storetypes.CommitID, error)
	io.Closer
}

type MultiTree interface {
	GetKVStore(storetypes.StoreKey) storetypes.KVStore
}
