package main

import (
	"github.com/crypto-org-chain/cronos/memiavl"

	"github.com/cosmos/iavl-bench/bench"
)

type DBWrapper struct {
	db *memiavl.DB
}

func (d DBWrapper) Version() int64 {
	return d.db.Version()
}

func (d DBWrapper) ApplyUpdate(storeKey string, key, value []byte, delete bool) error {
	changeSet := memiavl.ChangeSet{
		Pairs: []*memiavl.KVPair{
			{
				Key:    key,
				Value:  value,
				Delete: delete,
			},
		},
	}
	return d.db.ApplyChangeSet(storeKey, changeSet)
}

func (d DBWrapper) Commit() error {
	_, err := d.db.Commit()
	return err
}

var _ bench.Tree = &DBWrapper{}

type Options struct {
	SnapshotKeepRecent uint32 `json:"snapshot-keep-recent"`
	SnapshotInterval   uint32 `json:"snapshot-interval"`
	// Buffer size for the asynchronous commit queue, -1 means synchronous commit,
	// default to 0.
	AsyncCommitBuffer int `json:"async-commit-buffer"`
	// ZeroCopy if true, the get and iterator methods could return a slice pointing to mmaped blob files.
	ZeroCopy bool `json:"zero-copy"`
	// CacheSize defines the cache's max entry size for each memiavl store.
	CacheSize int `json:"cache-size"`
}

func main() {
	bench.Run("memiavl", bench.RunConfig{
		OptionsType: &Options{},
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			benchmarkOpts := params.TreeOptions.(*Options)
			opts := memiavl.Options{
				CreateIfMissing:    true,
				InitialStores:      params.StoreNames,
				SnapshotKeepRecent: benchmarkOpts.SnapshotKeepRecent,
				SnapshotInterval:   benchmarkOpts.SnapshotInterval,
				AsyncCommitBuffer:  benchmarkOpts.AsyncCommitBuffer,
				ZeroCopy:           benchmarkOpts.ZeroCopy,
				CacheSize:          benchmarkOpts.CacheSize,
			}

			db, err := memiavl.Load(params.TreeDir, opts)
			if err != nil {
				return nil, err
			}
			return DBWrapper{db: db}, nil
		},
	})
}
