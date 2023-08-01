package main

import (
	"context"
	"fmt"
	"os"

	clog "cosmossdk.io/log"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/iavl"
	"github.com/kocubinski/costor-api/logz"
	"github.com/kocubinski/iavl-bench/core"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/spf13/cobra"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func main() {
	root, err := core.RootCommand()
	if err != nil {
		os.Exit(1)
	}

	root.AddCommand(treeCommand(context.Background()))

	if err := root.Execute(); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		os.Exit(1)
	}
}

var log = logz.Logger.With().Str("bench", "iavl-v1").Logger()

func treeCommand(c context.Context) *cobra.Command {
	var (
		levelDbName string
	)
	ctx := &core.TreeContext{
		Context: c,
		Log:     log,
	}
	cmd := &cobra.Command{
		Use:   "tree",
		Short: "rebuild the tree from changesets",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx.IndexDir = cmd.Flag("index-dir").Value.String()
			levelDb, err := dbm.NewGoLevelDBWithOpts(levelDbName, ctx.IndexDir, &opt.Options{})
			if err != nil {
				return err
			}
			prefix := fmt.Sprintf("s/k:%s/", "bank")
			prefixDb := dbm.NewPrefixDB(levelDb, []byte(prefix))

			labels := map[string]string{}
			tree := iavl.NewMutableTree(prefixDb, 1_000_000, true, clog.NewNopLogger())
			labels["backend"] = "leveldb"
			labels["key_format"] = "v1"

			ctx.MetricLeafCount = promauto.NewCounter(prometheus.CounterOpts{
				Name:        "costor_index_tree_leaf_count",
				Help:        "number of leaf nodes procesed into the tree",
				ConstLabels: labels,
			})

			return ctx.BuildLegacyIAVL(tree)
		},
	}
	cmd.Flags().StringVar(&levelDbName, "leveldb-name", "legacy", "name to give the new leveldb instance")
	cmd.Flags().StringVar(&ctx.LogDir, "log-dir", "logs", "directory containing the compressed changeset logs")
	if err := cmd.MarkFlagRequired("log-dir"); err != nil {
		panic(err)
	}

	return cmd
}
