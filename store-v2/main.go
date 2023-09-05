package main

import (
	"context"
	"fmt"
	"os"

	clog "cosmossdk.io/log"
	"cosmossdk.io/store/v2/commitment"
	store_iavl "cosmossdk.io/store/v2/commitment/iavl"
	"cosmossdk.io/store/v2/multistore"
	"cosmossdk.io/store/v2/storage/leveldb"
	dbm "github.com/cosmos/cosmos-db"
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

var _ core.MultiTree = &MultiStore{}

type MultiStore struct {
	*multistore.Store
}

func (m MultiStore) SaveVersions() ([]byte, error) {
	return m.Commit()
}

func (m MultiStore) GetTree(key string) (core.Tree, error) {
	commitmentDb := m.GetSCStore(key)
	iavlTree, ok := commitmentDb.(*store_iavl.Tree)
	if !ok {
		return nil, fmt.Errorf("commitment database is not an IAVL tree")
	}
	return iavlTree, nil
}

func newIavlTree(levelDb dbm.DB, storeKey string) (commitment.Database, error) {
	prefix := fmt.Sprintf("s/k:%s/", storeKey)
	prefixDb := dbm.NewPrefixDB(levelDb, []byte(prefix))

	cfg := &store_iavl.Config{
		CacheSize:              100_000,
		SkipFastStorageUpgrade: true,
	}
	tree := store_iavl.NewIavlTree(prefixDb, clog.NewNopLogger(), cfg)
	return tree, nil
}

var log = logz.Logger.With().Str("bench", "store-v2").Logger()

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

			ssDb, err := dbm.NewGoLevelDBWithOpts(
				fmt.Sprintf("%s.ss", levelDbName), ctx.IndexDir, &opt.Options{})
			if err != nil {
				return err
			}
			stateStorage := &leveldb.DB{DB: ssDb}
			storeV2MultiStore, err := multistore.New(stateStorage)
			if err != nil {
				return err
			}
			ms := &MultiStore{storeV2MultiStore}
			if err != nil {
				return err
			}

			scDb, err := dbm.NewGoLevelDBWithOpts(
				fmt.Sprintf("%s.sc", levelDbName), ctx.IndexDir, &opt.Options{})
			if err != nil {
				return err
			}

			bankTree, err := newIavlTree(scDb, "bank")
			if err != nil {
				return err
			}
			err = ms.MountSCStore("bank", bankTree)
			if err != nil {
				return err
			}

			lockupTree, err := newIavlTree(scDb, "lockup")
			if err != nil {
				return err
			}
			err = ms.MountSCStore("lockup", lockupTree)
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

			return ctx.BuildLegacyIAVL(ms)
		},
	}
	cmd.Flags().StringVar(&levelDbName, "leveldb-name", "store-v2", "name to give the new leveldb instance")
	cmd.Flags().StringVar(&ctx.LogDir, "log-dir", "", "directory containing the compressed changeset logs")

	return cmd
}
