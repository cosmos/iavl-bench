package main

import (
	"context"
	"errors"

	logslog "log/slog"

	"github.com/cosmos/cosmos-sdk/iavlx"
	"github.com/cosmos/cosmos-sdk/telemetry"
	"github.com/samber/slog-multi"
	"go.opentelemetry.io/contrib/bridges/otelslog"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/store-v1"
)

type telemetryWrapper struct {
	bench.Tree
}

func (w *telemetryWrapper) Close() error {
	return errors.Join(
		w.Tree.Close(),
		telemetry.Shutdown(context.Background()),
	)
}

func main() {
	bench.Run("iavlx", bench.RunConfig{
		OptionsType: &iavlx.Options{},
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			opts := params.TreeOptions.(*iavlx.Options)
			if opts == nil {
				opts = &iavlx.Options{}
			}

			store, err := iavlx.LoadDB(
				params.TreeDir,
				opts,
				logslog.New(slogmulti.Fanout(
					params.Logger.Handler(),
					otelslog.NewHandler("iavlx"),
				)),
			)
			if err != nil {
				return nil, err
			}
			tree, err := store_v1.NewCommitMultiStoreWrapper(store, params.StoreNames)
			if err != nil {
				return nil, err
			}
			return &telemetryWrapper{Tree: tree}, nil
		},
	})
}
