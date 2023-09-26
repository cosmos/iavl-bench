package cmd

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/cosmos/iavl-bench/bench"
	iavl_proto "github.com/cosmos/iavl/proto"
	"github.com/crypto-org-chain/cronos/memiavl"
	"github.com/dustin/go-humanize"
	"github.com/kocubinski/costor-api/compact"
	"github.com/kocubinski/costor-api/logz"
	"github.com/spf13/cobra"
	log2 "github.com/tendermint/tendermint/libs/log"
)

var log = logz.Logger.With().Str("module", "memiavl").Logger()

func RunCommand() *cobra.Command {
	var (
		logDir       string
		indexDir     string
		versionLimit int64
	)
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Build a MemIAVL index from the nodes directory",
		RunE: func(_ *cobra.Command, _ []string) error {
			memIavlOpts := memiavl.Options{
				CreateIfMissing:    false,
				InitialStores:      []string{"bank"},
				SnapshotKeepRecent: 1000,
				SnapshotInterval:   15000,
				AsyncCommitBuffer:  -1,
				Logger:             log2.NewTMLogger(log2.NewSyncWriter(os.Stdout)),
			}
			dir := fmt.Sprintf("%s/memiavl", indexDir)
			//if err := os.RemoveAll(dir); err != nil {
			//	return err
			//}
			miavl, err := memiavl.Load(dir, memIavlOpts)
			if err != nil {
				return err
			}

			namedChangeset := &memiavl.NamedChangeSet{
				Name:      "bank",
				Changeset: iavl_proto.ChangeSet{},
			}

			cnt := 1
			since := time.Now()
			lastVersion := int64(1)
			hashLog, err := os.Create(fmt.Sprintf("%s/memiavl-hash.log", indexDir))
			if err != nil {
				return err
			}
			defer hashLog.Close()

			itr, err := compact.NewChangesetIterator(logDir)
			if err != nil {
				return err
			}
			for ; itr.Valid(); err = itr.Next() {
				if err != nil {
					return err
				}
				changeset := itr.Nodes()
				for ; changeset.Valid(); err = changeset.Next() {
					n := changeset.GetNode()
					// continue building changeset
					namedChangeset.Changeset.Pairs = append(namedChangeset.Changeset.Pairs, &iavl_proto.KVPair{
						Key:    n.Key,
						Value:  n.Value,
						Delete: n.Delete,
					})

					if cnt%100_000 == 0 {
						var m runtime.MemStats
						runtime.ReadMemStats(&m)
						log.Info().Msgf("version=%d leaves=%s dur=%s leaves/s=%s alloc=%s gc=%s",
							itr.Version(),
							humanize.Comma(int64(cnt)),
							time.Since(since),
							humanize.Comma(int64(100_000/time.Since(since).Seconds())),
							humanize.Bytes(m.Alloc),
							humanize.Comma(int64(m.NumGC)),
						)
						since = time.Now()
					}
					cnt++
				}

				// block height advanced; flush.
				_, v, err := miavl.Commit([]*memiavl.NamedChangeSet{namedChangeset})
				if err != nil {
					return err
				}
				if v%100 == 0 {
					_, err = fmt.Fprintf(hashLog, "%d|%x\n", v, miavl.TreeByName("bank").RootHash())
					if err != nil {
						return err
					}
				}

				namedChangeset = &memiavl.NamedChangeSet{
					Name:      "bank",
					Changeset: iavl_proto.ChangeSet{},
				}
				lastVersion = v

				if versionLimit != -1 && lastVersion > versionLimit {
					break
				}
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&indexDir, "index-dir", fmt.Sprintf("%s/.costor", os.Getenv("HOME")),
		"the directory to store the index in")
	cmd.Flags().StringVar(&logDir, "log-dir", "", "path to compacted changelogs")
	cmd.MarkFlagRequired("log-dir")
	cmd.Flags().Int64Var(&versionLimit, "limit", -1, "the maximum version to process")

	return cmd
}

func BuildCommand() *cobra.Command {
	var (
		logDir       string
		indexDir     string
		versionLimit int64
	)
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Build a MemIAVL snapshot at version 1",
		RunE: func(_ *cobra.Command, _ []string) error {
			memIavlOpts := memiavl.Options{
				CreateIfMissing:    true,
				InitialStores:      []string{"bank"},
				SnapshotKeepRecent: 1000,
				SnapshotInterval:   1000,
				AsyncCommitBuffer:  -1,
				Logger:             log2.NewTMLogger(log2.NewSyncWriter(os.Stdout)),
			}
			dir := fmt.Sprintf("%s/memiavl", indexDir)
			if err := os.RemoveAll(dir); err != nil {
				return err
			}
			miavl, err := memiavl.Load(dir, memIavlOpts)
			if err != nil {
				return err
			}

			namedChangeset := &memiavl.NamedChangeSet{
				Name:      "bank",
				Changeset: iavl_proto.ChangeSet{},
			}

			itr := bench.OsmoLikeIterator()
			if err != nil {
				return err
			}
			version1 := itr.Nodes()
			for ; version1.Valid(); err = version1.Next() {
				n := version1.GetNode()
				// continue building changeset
				namedChangeset.Changeset.Pairs = append(namedChangeset.Changeset.Pairs, &iavl_proto.KVPair{
					Key:    n.Key,
					Value:  n.Value,
					Delete: n.Delete,
				})
			}

			h, v, err := miavl.Commit([]*memiavl.NamedChangeSet{namedChangeset})
			fmt.Printf("version=%d hash=%x\n", v, h)

			log.Info().Msgf("writing snapshot")
			if err = miavl.RewriteSnapshot(); err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&indexDir, "index-dir", fmt.Sprintf("%s/.costor", os.Getenv("HOME")),
		"the directory to store the index in")
	cmd.Flags().StringVar(&logDir, "log-dir", "", "path to compacted changelogs")
	cmd.Flags().Int64Var(&versionLimit, "limit", -1, "the maximum version to process")

	return cmd
}
