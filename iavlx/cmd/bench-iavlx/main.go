package main

import (
	"github.com/cosmos/iavl-bench/bench"
	"iavlx/internal"
)

type dbWrapper struct {
	db *internal.DB
}

func (t *dbWrapper) Version() int64 {
	return int64(t.db.LatestVersion())
}

func (t *dbWrapper) ApplyUpdate(storeKey string, key, value []byte, delete bool) error {
	tree := t.db.Branch().TreeByName(storeKey)
	if delete {
		return tree.Remove(key)
	} else {
		return tree.Set(key, value)
	}
}

func (t *dbWrapper) Commit() error {
	return t.db.Commit()
}

var _ bench.Tree = &dbWrapper{}

func main() {
	bench.Run("iavlx", bench.RunConfig{
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			db, err := internal.LoadDB(internal.DBOptions{
				Path:      params.TreeDir,
				TreeNames: params.StoreNames,
			})
			if err != nil {
				return nil, err
			}
			return &dbWrapper{
				db,
			}, nil
		},
	})
}
