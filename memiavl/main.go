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
	SnapshotKeepRecent uint32
	SnapshotInterval   uint32
	AsyncCommitBuffer  int
	ZeroCopy           bool
	CacheSize          int
}

func main() {
	bench.Run("memiavl", bench.RunConfig{
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			opts := memiavl.Options{
				CreateIfMissing: true,
				InitialStores:   params.StoreNames,
			}

			db, err := memiavl.Load(params.TreeDir, opts)
			if err != nil {
				return nil, err
			}
			return DBWrapper{db: db}, nil
		},
	})
}
