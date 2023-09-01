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
func newIavlTree(levelDb dbm.DB, storeKey string) (core.Tree, error) {
	prefix := fmt.Sprintf("s/k:%s/", storeKey)
	prefixDb := dbm.NewPrefixDB(levelDb, []byte(prefix))

	return iavl.NewMutableTree(prefixDb, 1_000_000, true, clog.NewNopLogger()), nil
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

			ctx.Generators = []core.ChangesetGenerator{
				core.BankLikeGenerator(0, 10_000_000),
				core.LockupLikeGenerator(0, 10_000_000),
			}

			hashLog, err := os.Create(fmt.Sprintf("%s/iavl-v1-hash.log", ctx.IndexDir))
			if err != nil {
				return err
			}
			defer hashLog.Close()
			ctx.HashLog = hashLog

			levelDb, err := dbm.NewGoLevelDBWithOpts(levelDbName, ctx.IndexDir, &opt.Options{})
			if err != nil {
				return err
			}

			multiTree := core.NewMultiTree()
			multiTree.Trees["bank"], err = newIavlTree(levelDb, "bank")
			if err != nil {
				return err
			}
			multiTree.Trees["lockup"], err = newIavlTree(levelDb, "lockup")
			if err != nil {
				return err
			}

			labels := map[string]string{}
			labels["backend"] = "leveldb"
			labels["key_format"] = "v1"

			//tree.MetricTreeHeight = promauto.NewGauge(prometheus.GaugeOpts{
			//	Name:        "iavl_tree_height",
			//	ConstLabels: labels,
			//})
			//tree.MetricTreeSize = promauto.NewGauge(prometheus.GaugeOpts{
			//	Name:        "iavl_tree_size",
			//	ConstLabels: labels,
			//})

			ctx.MetricLeafCount = promauto.NewCounter(prometheus.CounterOpts{
				Name:        "costor_index_tree_leaf_count",
				Help:        "number of leaf nodes procesed into the tree",
				ConstLabels: labels,
			})

			return ctx.BuildLegacyIAVL(multiTree)
		},
	}
	cmd.Flags().StringVar(&levelDbName, "leveldb-name", "iavl-v1", "name to give the new leveldb instance")
	cmd.Flags().StringVar(&ctx.LogDir, "log-dir", "", "directory containing the compressed changeset logs")

	return cmd
}
