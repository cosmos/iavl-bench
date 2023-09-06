package cmd

import (
	"fmt"
	"os"
	"time"

	iavl_proto "github.com/cosmos/iavl/proto"
	"github.com/crypto-org-chain/cronos/memiavl"
	"github.com/dustin/go-humanize"
	"github.com/kocubinski/costor-api/compact"
	"github.com/kocubinski/costor-api/logz"
	"github.com/spf13/cobra"
	log2 "github.com/tendermint/tendermint/libs/log"
)

var log = logz.Logger.With().Str("module", "memiavl").Logger()

type context struct {
	indexDir     string
	logDir       string
	versionLimit int64
}

func Command() *cobra.Command {
	c := &context{}
	cmd := &cobra.Command{
		Use:   "memiavl",
		Short: "benchmark memiavl",
	}
	cmd.PersistentFlags().StringVar(&c.indexDir, "index-dir", fmt.Sprintf("%s/.costor", os.Getenv("HOME")),
		"the directory to store the index in")
	cmd.Flags().StringVar(&c.logDir, "log-dir", "", "path to compacted changelogs")
	if err := cmd.MarkFlagRequired("log-dir"); err != nil {
		panic(err)
	}

	cmd.AddCommand(buildCommand(c))
	return cmd
}

func buildCommand(c *context) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "Build a MemIAVL index from the nodes directory",
		RunE: func(_ *cobra.Command, _ []string) error {
			memIavlOpts := memiavl.Options{
				CreateIfMissing:    true,
				InitialStores:      []string{"bank"},
				SnapshotKeepRecent: 1000,
				SnapshotInterval:   1000,
				AsyncCommitBuffer:  -1,
				Logger:             log2.TestingLogger(),
			}
			dir := fmt.Sprintf("%s/memiavl", c.indexDir)
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

			cnt := 1
			since := time.Now()
			lastVersion := int64(1)
			hashLog, err := os.Create(fmt.Sprintf("%s/memiavl-hash.log", c.indexDir))
			if err != nil {
				return err
			}
			defer hashLog.Close()

			//commitResult := make(chan error)
			stream := &compact.StreamingContext{}
			itr, err := stream.NewIterator(c.logDir)
			if err != nil {
				return err
			}
			for ; itr.Valid(); err = itr.Next() {
				if err != nil {
					return err
				}
				n := itr.Node

				// continue building changeset
				namedChangeset.Changeset.Pairs = append(namedChangeset.Changeset.Pairs, &iavl_proto.KVPair{
					Key:    n.Key,
					Value:  n.Value,
					Delete: n.Delete,
				})

				// block height advanced; flush.
				if n.Block > lastVersion {
					_, v, err := miavl.Commit([]*memiavl.NamedChangeSet{namedChangeset})
					if err != nil {
						return err
					}
					if v%20_000 == 0 {
						_, err = fmt.Fprintf(hashLog, "%d|%x\n", v, miavl.TreeByName("bank").RootHash())
						if err != nil {
							return err
						}
					}

					namedChangeset = &memiavl.NamedChangeSet{
						Name:      "bank",
						Changeset: iavl_proto.ChangeSet{},
					}
					lastVersion = n.Block
				}

				if c.versionLimit > 0 && lastVersion > c.versionLimit {
					break
				}

				if cnt%100_000 == 0 {
					log.Info().Msgf("processed %s leaves in %s; %s leaves/s",
						humanize.Comma(int64(cnt)),
						time.Since(since),
						humanize.Comma(int64(100_000/time.Since(since).Seconds())))
					since = time.Now()
				}
				cnt++
			}

			return nil
		},
	}

	return cmd
}