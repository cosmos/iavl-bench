package main

import (
	"log/slog"

	"github.com/cosmos/iavl-bench/bench"
	"iavlx/internal"
)

type dbWrapper struct {
	logger *slog.Logger
	db     *internal.DB
}

func (t *dbWrapper) Close() error {
	return t.db.Close()
}

func (t *dbWrapper) Version() int64 {
	return int64(t.db.LatestVersion())
}

func (t *dbWrapper) ApplyUpdate(storeKey string, key, value []byte, delete bool) error {
	branch := t.db.Branch()
	tree := branch.TreeByName(storeKey)
	var err error
	if delete {
		err = tree.Remove(key)
	} else {
		err = tree.Set(key, value)
	}
	if err != nil {
		return nil
	}
	return t.db.Apply(branch)
}

func (t *dbWrapper) Commit() error {
	ci, err := t.db.Commit(t.logger)
	if err != nil {
		return err
	}
	t.logger.Info("committed", "version", ci.Version, "info", ci.StoreInfos)
	return err
}

var _ bench.Tree = &dbWrapper{}

func main() {
	bench.Run("iavlx", bench.RunConfig{
		OptionsType: &internal.Options{},
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			db, err := internal.LoadDB(params.TreeDir, params.StoreNames, params.TreeOptions.(*internal.Options), params.Logger)
			if err != nil {
				return nil, err
			}
			return &dbWrapper{
				db:     db,
				logger: params.Logger,
			}, nil
		},
	})
}
