package bench

import (
	"io"

	pruningtypes "github.com/cosmos/cosmos-sdk/store/v2/pruning/types"
	storetypes "github.com/cosmos/cosmos-sdk/store/v2/types"
)

type RootMultiTree interface {
	// SetPruning should set the pruning options for the multi tree. This should be called before any commits are made.
	SetPruning(pruning pruningtypes.PruningOptions)
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
