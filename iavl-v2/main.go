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

var log = logz.Logger.With().Str("bench", "iavl-v2").Logger()

func treeCommand(c context.Context) *cobra.Command {
	var (
		sqliteDbName string
		sqlite       bool
		mapDb        bool
		nodeBackend  bool
		nopBackend   bool
		levelDbName  string
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
			hashLog, err := os.Create(fmt.Sprintf("%s/iavl-v2-hash.log", ctx.IndexDir))
			if err != nil {
				return err
			}
			defer hashLog.Close()
			ctx.HashLog = hashLog
			levelDb, err := dbm.NewGoLevelDBWithOpts(levelDbName, ctx.IndexDir, &opt.Options{})
			if err != nil {
				return err
			}
			prefix := fmt.Sprintf("s/k:%s/", "bank")
			prefixDb := dbm.NewPrefixDB(levelDb, []byte(prefix))

			var (
				tree   *iavl.MutableTree
				labels = map[string]string{}
			)

			switch {
			case sqlite:
				sqlDb, err := iavl.NewSqliteDb(cmd.Context(), fmt.Sprintf("%s/iavl.sqlite", ctx.IndexDir))
				if err != nil {
					return err
				}
				tree = iavl.NewMutableTreeWithOpts(prefixDb, 1000, &iavl.Options{NodeBackend: sqlDb},
					true, clog.NewNopLogger())
				labels["backend"] = "sqlite"
			case nopBackend:
				labels["backend"] = "mapdb"
				tree = iavl.NewMutableTreeWithOpts(prefixDb, 0,
					&iavl.Options{NodeBackend: &iavl.NopBackend{}},
					true, clog.NewNopLogger())
				tree.MetricTreeHeight = promauto.NewGauge(prometheus.GaugeOpts{
					Name:        "iavl_tree_height",
					ConstLabels: labels,
				})
				tree.MetricTreeSize = promauto.NewGauge(prometheus.GaugeOpts{
					Name:        "iavl_tree_size",
					ConstLabels: labels,
				})
			case mapDb:
				labels["backend"] = "mapdb"
				backend := iavl.NewMapDB()
				tree = iavl.NewMutableTreeWithOpts(prefixDb, 300_000,
					&iavl.Options{NodeBackend: backend},
					true, clog.NewNopLogger())
				tree.MetricTreeHeight = promauto.NewGauge(prometheus.GaugeOpts{
					Name:        "iavl_tree_height",
					ConstLabels: labels,
				})
				tree.MetricTreeSize = promauto.NewGauge(prometheus.GaugeOpts{
					Name:        "iavl_tree_size",
					ConstLabels: labels,
				})
			case nodeBackend:
				labels["backend"] = "node"
				sqlDb, err := iavl.NewSqliteDb(cmd.Context(), fmt.Sprintf("%s/iavl.sqlite", ctx.IndexDir))
				if err != nil {
					return err
				}

				walog, err := iavl.NewTidwalLog(ctx.IndexDir)
				if err != nil {
					return err
				}

				wal := iavl.NewWal(walog, prefixDb, sqlDb)
				wal.MetricNodesRead = promauto.NewCounter(prometheus.CounterOpts{
					Name: "iavl_wal_nodes_read",
				})
				wal.MetricWalSize = promauto.NewGauge(prometheus.GaugeOpts{
					Name: "iavl_wal_size",
				})
				wal.MetricCacheMiss = promauto.NewCounter(prometheus.CounterOpts{
					Name: "iavl_wal_cache_miss",
				})
				wal.MetricCacheHit = promauto.NewCounter(prometheus.CounterOpts{
					Name: "iavl_wal_cache_hit",
				})
				wal.MetricCacheSize = promauto.NewGauge(prometheus.GaugeOpts{
					Name: "iavl_wal_cache_size",
				})

				go func() {
					err = wal.CheckpointRunner(cmd.Context())
					if err != nil {
						log.Fatal().Err(err).Msg("wal reader failed")
					}
				}()

				kvBackend, err := iavl.NewKeyValueBackend(prefixDb, 1_000_000, wal)
				if err != nil {
					return err
				}
				kvBackend.MetricBlockCount = promauto.NewCounter(prometheus.CounterOpts{
					Name: "iavl_backend_block_count",
				})
				kvBackend.MetricCacheSize = promauto.NewGauge(prometheus.GaugeOpts{
					Name: "iavl_backend_cache_size",
				})
				kvBackend.MetricCacheMiss = promauto.NewCounter(prometheus.CounterOpts{
					Name: "iavl_backend_cache_miss",
				})
				kvBackend.MetricCacheHit = promauto.NewCounter(prometheus.CounterOpts{
					Name: "iavl_backend_cache_hit",
				})
				kvBackend.MetricDbFetch = promauto.NewCounter(prometheus.CounterOpts{
					Name: "iavl_backend_db_fetch",
				})
				kvBackend.MetricDbFetchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
					Name: "iavl_backend_db_fetch_duration",
				})

				opts := &iavl.Options{NodeBackend: kvBackend}
				tree = iavl.NewMutableTreeWithOpts(prefixDb, 0, opts, true, clog.NewNopLogger())
				tree.MetricTreeHeight = promauto.NewGauge(prometheus.GaugeOpts{
					Name:        "iavl_tree_height",
					ConstLabels: labels,
				})
				tree.MetricTreeSize = promauto.NewGauge(prometheus.GaugeOpts{
					Name:        "iavl_tree_size",
					ConstLabels: labels,
				})
			default:
				tree = iavl.NewMutableTree(prefixDb, 1_000_000, true, clog.NewNopLogger())
				labels["backend"] = "leveldb"
			}

			ctx.MetricLeafCount = promauto.NewCounter(prometheus.CounterOpts{
				Name:        "costor_index_tree_leaf_count",
				Help:        "Number of leaves in the iavl tree",
				ConstLabels: labels,
			})

			return ctx.BuildLegacyIAVL(tree)
		},
	}
	cmd.Flags().StringVar(&sqliteDbName, "sqlite-db-name", "sqlite.db", "path to sqlite db")
	cmd.Flags().BoolVar(&sqlite, "sqlite", false, "use sqlite")
	cmd.Flags().BoolVar(&mapDb, "mapdb", false, "use mapdb")
	cmd.Flags().BoolVar(&nopBackend, "nop", false, "use no-op backend")
	cmd.Flags().BoolVar(&nodeBackend, "node-backend", false, "use node backend")
	cmd.Flags().StringVar(&levelDbName, "leveldb-name", "legacy", "name to give the new leveldb instance")
	cmd.Flags().StringVar(&ctx.LogDir, "log-dir", "logs", "directory containing the compressed changeset logs")
	if err := cmd.MarkFlagRequired("log-dir"); err != nil {
		panic(err)
	}

	return cmd
}
