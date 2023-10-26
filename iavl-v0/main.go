package main

import (
	"context"
	"fmt"
	"os"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/iavl"
	"github.com/cosmos/iavl-bench/bench"
	"github.com/kocubinski/costor-api/logz"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/spf13/cobra"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func main() {
	root, err := bench.RootCommand()
	if err != nil {
		os.Exit(1)
	}

	root.AddCommand(treeCommand(context.Background()))

	if err := root.Execute(); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		os.Exit(1)
	}
}

var log = logz.Logger.With().Str("bench", "iavl-v0").Logger()

func newIavlTree(levelDb dbm.DB, storeKey string) (bench.Tree, error) {
	prefix := fmt.Sprintf("s/k:%s/", storeKey)
	prefixDb := dbm.NewPrefixDB(levelDb, []byte(prefix))

	return iavl.NewMutableTree(prefixDb, 1_000_000, true)
}

func treeCommand(c context.Context) *cobra.Command {
	var (
		levelDbName string
		seed        int64
	)
	ctx := &bench.TreeContext{
		Context: c,
		Log:     log,
	}
	cmd := &cobra.Command{
		Use:   "tree",
		Short: "rebuild the tree from changesets",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx.IndexDir = cmd.Flag("index-dir").Value.String()

			hashLog, err := os.Create(fmt.Sprintf("%s/iavl-v0-hash.log", ctx.IndexDir))
			if err != nil {
				return err
			}
			defer hashLog.Close()
			ctx.HashLog = hashLog

			levelDb, err := dbm.NewGoLevelDBWithOpts(levelDbName, ctx.IndexDir, &opt.Options{})
			if err != nil {
				return err
			}

			multiTree := bench.NewMultiTree()
			multiTree.Trees["wasm"], err = newIavlTree(levelDb, "wasm")
			if err != nil {
				return err
			}
			multiTree.Trees["ibc"], err = newIavlTree(levelDb, "ibc")
			if err != nil {
				return err
			}
			multiTree.Trees["upgrade"], err = newIavlTree(levelDb, "upgrade")
			if err != nil {
				return err
			}
			multiTree.Trees["icahost"], err = newIavlTree(levelDb, "icahost")
			if err != nil {
				return err
			}
			multiTree.Trees["concentratedliquidity"], err = newIavlTree(levelDb, "concentratedliquidity")
			if err != nil {
				return err
			}

			ctx.Iterator = OsmoLikeManyTrees()

			labels := map[string]string{}
			labels["backend"] = "leveldb"
			labels["key_format"] = "v0"

			ctx.MetricTreeSize = promauto.NewGauge(prometheus.GaugeOpts{
				Name:        "iavl_tree_size",
				ConstLabels: labels,
			})
			ctx.MetricsTreeHeight = promauto.NewGauge(prometheus.GaugeOpts{
				Name:        "iavl_tree_height",
				ConstLabels: labels,
			})
			ctx.MetricLeafCount = promauto.NewCounter(prometheus.CounterOpts{
				Name:        "costor_index_tree_leaf_count",
				Help:        "number of leaf nodes procesed into the tree",
				ConstLabels: labels,
			})

			ctx.VersionLimit = 100

			return ctx.BuildLegacyIAVL(multiTree)
		},
	}
	cmd.Flags().StringVar(&levelDbName, "leveldb-name", "iavl-v0", "name to give the new leveldb instance")
	cmd.Flags().StringVar(&ctx.LogDir, "log-dir", "", "directory containing the compressed changeset logs")
	cmd.Flags().Int64Var(&seed, "seed", 0, "seed for the data generator")

	return cmd
}

func OsmoLikeManyTrees() bench.ChangesetIterator {
	seed := int64(1234)
	versions := int64(100_000)
	changes := int(versions / 100)
	deleteFrac := 0.2

	wasm := bench.ChangesetGenerator{
		StoreKey:         "wasm",
		Seed:             seed,
		KeyMean:          79,
		KeyStdDev:        23,
		ValueMean:        170,
		ValueStdDev:      202,
		InitialSize:      8_500_000,
		FinalSize:        8_600_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	ibc := bench.ChangesetGenerator{
		StoreKey:         "ibc",
		Seed:             seed,
		KeyMean:          58,
		KeyStdDev:        4,
		ValueMean:        22,
		ValueStdDev:      29,
		InitialSize:      23_400_000,
		FinalSize:        23_500_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	upgrade := bench.ChangesetGenerator{
		StoreKey:         "upgrade",
		Seed:             seed,
		KeyMean:          8,
		KeyStdDev:        1,
		ValueMean:        8,
		ValueStdDev:      0,
		InitialSize:      60,
		FinalSize:        62,
		Versions:         versions,
		ChangePerVersion: 1,
		DeleteFraction:   0,
	}
	concentratedliquidity := bench.ChangesetGenerator{
		StoreKey:         "concentratedliquidity",
		Seed:             seed,
		KeyMean:          25,
		KeyStdDev:        11,
		ValueMean:        44,
		ValueStdDev:      48,
		InitialSize:      600_000,
		FinalSize:        610_000,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	icahost := bench.ChangesetGenerator{
		StoreKey:         "icahost",
		Seed:             seed,
		KeyMean:          103,
		KeyStdDev:        11,
		ValueMean:        37,
		ValueStdDev:      25,
		InitialSize:      1_500,
		FinalSize:        1_600,
		Versions:         versions,
		ChangePerVersion: changes,
		DeleteFraction:   deleteFrac,
	}
	itr, err := bench.NewChangesetIterators([]bench.ChangesetGenerator{
		wasm,
		ibc,
		upgrade,
		concentratedliquidity,
		icahost,
	})
	if err != nil {
		panic(err)
	}
	return itr
}
