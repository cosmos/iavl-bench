package runner

import (
	"fmt"

	"github.com/crypto-org-chain/cronos/memiavl"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/bench/util"
)

type DBWrapper struct {
	db *memiavl.DB
}

func (d DBWrapper) Version() int64 {
	return d.db.Version()
}

func (d DBWrapper) Commit(updates bench.MultiStoreUpdates) error {
	// for now just do this sequentially since that's what we were doing before
	for storeKey, treeUpdates := range updates {
		var changeSet memiavl.ChangeSet
		for update := range treeUpdates.Updates {
			changeSet.Pairs = append(changeSet.Pairs, &memiavl.KVPair{
				Key:    update.Key,
				Value:  update.Value,
				Delete: update.Delete,
			})
		}
		if err := d.db.ApplyChangeSet(storeKey, changeSet); err != nil {
			return err
		}
	}
	_, err := d.db.Commit()
	return err
}

func (d DBWrapper) Tree(storeName string) bench.TreeReader {
	panic("not implemented")
}

func (d DBWrapper) ForceToDisk() error {
	return fmt.Errorf("not implemented")
}

func (d DBWrapper) Close() error {
	return d.db.Close()
}

var _ bench.MultiTree = &DBWrapper{}

type Options struct {
	SnapshotKeepRecent uint32 `json:"snapshot_keep_recent"`
	SnapshotInterval   uint32 `json:"snapshot_interval"`
	// Buffer size for the asynchronous commit queue, -1 means synchronous commit,
	// default to 0.
	AsyncCommitBuffer int `json:"async_commit_buffer"`
	// ZeroCopy if true, the get and iterator methods could return a slice pointing to mmaped blob files.
	ZeroCopy bool `json:"zero_copy"`
	// CacheSize defines the cache's max entry size for each memiavl store.
	CacheSize int `json:"cache_size"`
}

func Run() {
	Runner().Run()
}

func Runner() bench.Runner {
	return bench.NewRunner("memiavl", bench.RunConfig{
		OptionsType: &Options{},
		TreeLoader: func(params bench.LoaderParams) (bench.MultiTree, error) {
			benchmarkOpts := params.TreeOptions.(*Options)
			opts := memiavl.Options{
				CreateIfMissing:    true,
				InitialStores:      params.StoreNames,
				SnapshotKeepRecent: benchmarkOpts.SnapshotKeepRecent,
				SnapshotInterval:   benchmarkOpts.SnapshotInterval,
				AsyncCommitBuffer:  benchmarkOpts.AsyncCommitBuffer,
				ZeroCopy:           benchmarkOpts.ZeroCopy,
				CacheSize:          benchmarkOpts.CacheSize,
				Logger:             util.NewSlogWrapper(params.Logger),
			}

			db, err := memiavl.Load(params.TreeDir, opts)
			if err != nil {
				return nil, err
			}
			return &DBWrapper{db: db}, nil
		},
	})
}
