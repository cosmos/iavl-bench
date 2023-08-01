package main

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
)

var log = logz.Logger.With().Str("module", "memiavl").Logger()

type context struct {
	indexDir string
	nodesDir string
}

func Command() *cobra.Command {
	c := &context{}
	cmd := &cobra.Command{
		Use:   "memiavl",
		Short: "benchmark memiavl",
	}
	cmd.PersistentFlags().StringVar(&c.indexDir, "index-dir", fmt.Sprintf("%s/.costor", os.Getenv("HOME")),
		"the directory to store the index in")
	cmd.AddCommand(buildCommand(c))
	return cmd
}

func buildCommand(c *context) *cobra.Command {
	var logDir string
	cmd := &cobra.Command{
		Use:   "index",
		Short: "Build a MemIAVL index from the nodes directory",
		RunE: func(cmd *cobra.Command, args []string) error {
			memIavlOpts := memiavl.Options{
				CreateIfMissing: true,
				InitialStores:   []string{"bank"},
			}
			miavl, err := memiavl.Load(fmt.Sprintf("%s/memiavl", c.indexDir), memIavlOpts)
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

			commitResult := make(chan error)
			stream := &compact.StreamingContext{}
			itr, err := stream.NewIterator(logDir)
			for ; itr.Valid(); err = itr.Next() {
				if err != nil {
					return err
				}
				n := itr.Node

				// block height advanced; flush.
				if n.Block > lastVersion {
					go func() {
						_, _, commitErr := miavl.Commit([]*memiavl.NamedChangeSet{namedChangeset})
						commitResult <- commitErr
					}()
					select {
					case <-time.After(1 * time.Minute):
						log.Fatal().Msgf("commit took longer than 1 minute at block %d", n.Block)
					case err := <-commitResult:
						if err != nil {
							log.Error().Err(err).Msgf("failed to commit changeset at block %d", n.Block)
							return err
						}
					}

					namedChangeset = &memiavl.NamedChangeSet{
						Name:      "bank",
						Changeset: iavl_proto.ChangeSet{},
					}
					lastVersion = n.Block
				}

				// continue building changeset
				namedChangeset.Changeset.Pairs = append(namedChangeset.Changeset.Pairs, &iavl_proto.KVPair{
					Key:    n.Key,
					Value:  n.Value,
					Delete: n.Delete,
				})

				if cnt%50_000 == 0 {
					log.Info().Msgf("processed %s leaves in %s; %s leaves/s",
						humanize.Comma(int64(cnt)),
						time.Since(since),
						humanize.Comma(int64(50_000/time.Since(since).Seconds())))
					since = time.Now()
				}
				cnt++
			}

			return nil
		},
	}
	cmd.Flags().StringVar(&logDir, "log-dir", "", "path to compacted changelogs")
	if err := cmd.MarkFlagRequired("log-dir"); err != nil {
		panic(err)
	}
	return cmd
}
