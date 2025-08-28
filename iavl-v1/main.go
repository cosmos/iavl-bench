package main

import (
	"fmt"

	"github.com/cosmos/iavl"
	db "github.com/cosmos/iavl/db"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/bench/multitreeutil"
)

//func main() {
//	root, err := bench.RootCommand()
//	if err != nil {
//		os.Exit(1)
//	}
//
//	root.AddCommand(treeCommand(), initCommand(), rootNodeCommand())
//
//	if err := root.Execute(); err != nil {
//		fmt.Printf("Error: %s\n", err.Error())
//		os.Exit(1)
//	}
//}
//func newIavlTree(levelDb dbm.DB, storeKey string, from int64) (bench.Tree, error) {
//	prefix := fmt.Sprintf("s/k:%s/", storeKey)
//	prefixDb := dbm.NewPrefixDB(levelDb, []byte(prefix))
//
//	var err error
//	tree, err := iavl.NewMutableTree(prefixDb, 1_000_000, true, clog.NewNopLogger()), nil
//	if err != nil {
//		return nil, err
//	}
//	if from > 0 {
//		_, err = tree.LoadVersion(from)
//		if err != nil {
//			return nil, err
//		}
//	}
//	return tree, nil
//}
//
//var log = logz.Logger.With().Str("bench", "iavl-v1").Logger()
//
//func treeCommand() *cobra.Command {
//	var (
//		levelDbName string
//		seed        int64
//		from        int64
//		logDir      string
//	)
//	cmd := &cobra.Command{
//		Use:   "tree",
//		Short: "rebuild the tree from changesets",
//		RunE: func(cmd *cobra.Command, args []string) error {
//			indexDir := cmd.Flag("index-dir").Value.String()
//
//			itr, err := compact.NewChangesetIterator(logDir)
//			if err != nil {
//				return err
//			}
//
//			hashLog, err := os.Create(fmt.Sprintf("%s/iavl-v1-hash.log", indexDir))
//			if err != nil {
//				return err
//			}
//			defer hashLog.Close()
//
//			levelDb, err := dbm.NewGoLevelDBWithOpts(levelDbName, indexDir, &opt.Options{})
//			if err != nil {
//				return err
//			}
//			tree, err := newIavlTree(levelDb, "bank", from)
//
//			var cnt int64
//			var lastVersion int64
//			var lastHash []byte
//
//			since := time.Now()
//			for ; itr.Valid(); err = itr.Next() {
//				if err != nil {
//					return err
//				}
//				changeset := itr.Nodes()
//				for ; changeset.Valid(); err = changeset.Next() {
//					if err != nil {
//						return err
//					}
//					node := changeset.GetNode()
//					if node.Delete {
//						_, _, err = tree.Remove(node.Key)
//						if err != nil {
//							return err
//						}
//					} else {
//						_, err = tree.Set(node.Key, node.Value)
//						if err != nil {
//							return err
//						}
//					}
//
//					cnt++
//					if cnt%100_000 == 0 {
//						log.Info().Msgf("version=%d nodes=%s n/s=%s",
//							itr.Version(),
//							humanize.Comma(cnt),
//							humanize.Comma(int64(100_000/time.Since(since).Seconds())),
//						)
//						since = time.Now()
//					}
//				}
//				lastHash, lastVersion, err = tree.SaveVersion()
//				if err != nil {
//					return err
//				}
//				_, err = hashLog.WriteString(fmt.Sprintf("%d,%x\n", lastVersion, lastHash))
//				if err != nil {
//					return err
//				}
//			}
//
//			log.Info().Msgf("done; last version=%d hash=%x", lastVersion, lastHash)
//			return nil
//		},
//	}
//	cmd.Flags().StringVar(&levelDbName, "leveldb-name", "iavl-v1", "name to give the new leveldb instance")
//	cmd.Flags().StringVar(&logDir, "log-dir", "", "directory containing the compressed changeset logs")
//	cmd.Flags().Int64Var(&seed, "seed", 1234, "seed for the random number generator")
//	cmd.Flags().Int64Var(&from, "from", -1, "version to start from")
//
//	return cmd
//}
//
//func rootNodeCommand() *cobra.Command {
//	var (
//		levelDbName string
//		version     int64
//		storeKey    string
//	)
//	cmd := &cobra.Command{
//		Use:   "root",
//		Short: "load the tree at version n and print the root node",
//		RunE: func(cmd *cobra.Command, args []string) error {
//			indexDir := cmd.Flag("index-dir").Value.String()
//			levelDb, err := dbm.NewGoLevelDBWithOpts(levelDbName, indexDir, &opt.Options{})
//			if err != nil {
//				return err
//			}
//
//			prefix := fmt.Sprintf("s/k:%s/", storeKey)
//			prefixDb := dbm.NewPrefixDB(levelDb, []byte(prefix))
//
//			tree, err := iavl.NewMutableTree(prefixDb, 1_000_000, true, clog.NewNopLogger()), nil
//			if err != nil {
//				return err
//			}
//			_, err = tree.LoadVersion(version)
//			if err != nil {
//				return err
//			}
//
//			log.Info().Msgf("loaded root hash=%x height=%d size=%s",
//				tree.Hash(), tree.Height(), humanize.Comma(tree.Size()))
//
//			return nil
//		},
//	}
//	cmd.Flags().StringVar(&levelDbName, "leveldb-name", "iavl-v1", "name of leveldb instance")
//	cmd.Flags().Int64Var(&version, "version", 0, "version to load")
//	cmd.Flags().StringVar(&storeKey, "store-key", "", "store key to load")
//	cmd.MarkFlagRequired("leveldb-name")
//	cmd.MarkFlagRequired("version")
//	cmd.MarkFlagRequired("store-key")
//
//	return cmd
//}
//
//func initCommand() *cobra.Command {
//	var (
//		levelDbName string
//	)
//	cmd := &cobra.Command{
//		Use:   "init",
//		Short: "build an osmosis scale (80M nodes) tree at version 1 and save",
//		RunE: func(cmd *cobra.Command, args []string) error {
//			dbDir := cmd.Flag("index-dir").Value.String()
//			levelDb, err := dbm.NewGoLevelDBWithOpts(levelDbName, dbDir, &opt.Options{})
//			if err != nil {
//				return err
//			}
//			tree, err := newIavlTree(levelDb, "bank", -1)
//			if err != nil {
//				return err
//			}
//
//			var cnt int64
//			since := time.Now()
//			itr := bench.OsmoLikeIterator()
//			v1 := itr.Nodes()
//			for ; v1.Valid(); err = v1.Next() {
//				if err != nil {
//					return err
//				}
//				node := v1.GetNode()
//				if node.Delete {
//					return fmt.Errorf("unexpected delete in version 1")
//				}
//				_, err = tree.Set(node.Key, node.Value)
//				if err != nil {
//					return err
//				}
//				cnt++
//				if cnt%100_000 == 0 {
//					log.Info().Msgf("version=%d nodes=%s n/s=%s",
//						itr.Version(),
//						humanize.Comma(cnt),
//						humanize.Comma(int64(100_000/time.Since(since).Seconds())),
//					)
//					since = time.Now()
//				}
//			}
//
//			since = time.Now()
//			log.Info().Msg("saving version 1")
//			h, v, err := tree.SaveVersion()
//			if err != nil {
//				return err
//			}
//			log.Info().Msgf("saving took=%s version=%d hash=%x", time.Since(since), v, h)
//			return nil
//		},
//	}
//	cmd.Flags().StringVar(&levelDbName, "leveldb-name", "iavl-v1", "name to give the new leveldb instance")
//	return cmd
//}

type MultiTreeWrapper struct {
	dbDir   string
	version int64
	trees   map[string]*iavl.MutableTree
}

func (m *MultiTreeWrapper) Version() int64 {
	return m.version
}

func (m *MultiTreeWrapper) ApplyUpdate(storeKey string, key, value []byte, delete bool) error {
	tree, ok := m.trees[storeKey]
	if !ok {
		return fmt.Errorf("store key %s not found", storeKey)
	}
	if delete {
		_, _, err := tree.Remove(key)
		return err
	} else {
		_, err := tree.Set(key, value)
		return err
	}
}

func (m *MultiTreeWrapper) Commit() error {
	for _, tree := range m.trees {
		_, _, err := tree.SaveVersion()
		if err != nil {
			return err
		}
	}

	m.version++

	return multitreeutil.SaveVersion(m.dbDir, m.version)
}

var _ bench.Tree = &MultiTreeWrapper{}

func main() {
	bench.Run(bench.RunConfig{
		TreeLoader: func(params bench.LoaderParams) (bench.Tree, error) {
			dbDir := params.TreeDir
			version, err := multitreeutil.LoadVersion(dbDir)
			if err != nil {
				return nil, err
			}
			trees := make(map[string]*iavl.MutableTree)
			logger := iavl.NewNopLogger()
			for _, storeName := range params.StoreNames {
				d, err := db.NewGoLevelDBWithOpts(storeName, dbDir, &opt.Options{})
				if err != nil {
					return nil, err
				}
				tree := iavl.NewMutableTree(d, 10_000, true, logger)
				if version != 0 {
					_, err := tree.LoadVersion(version)
					if err != nil {
						return nil, fmt.Errorf("loading store %s at version %d: %w", storeName, version, err)
					}
				}
				trees[storeName] = tree
			}
			return &MultiTreeWrapper{
				trees:   trees,
				version: version,
				dbDir:   dbDir,
			}, nil
		},
	})
}
