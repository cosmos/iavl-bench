package core

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/kocubinski/costor-api/compact"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

type TreeContext struct {
	context.Context

	Log               zerolog.Logger
	IndexDir          string
	LogDir            string
	Generators        []ChangesetGenerator
	VersionLimit      int64
	MetricLeafCount   prometheus.Counter
	MetricTreeSize    prometheus.Gauge
	MetricsTreeHeight prometheus.Gauge
	HashLog           *os.File
}

func (c *TreeContext) BuildLegacyIAVL(multiTree MultiTree) error {
	cnt := 1
	since := time.Now()
	var (
		itr         ChangesetIterator
		err         error
		iavlVersion int64
	)

	if c.LogDir != "" {
		itr, err = compact.NewMultiChangesetIterator(c.LogDir)
		if err != nil {
			path := strings.Split(c.LogDir, "/")
			itr, err = compact.NewChangesetIterator(c.LogDir, path[len(path)-1])
			if err != nil {
				return err
			}
		}
	} else {
		itr, err = NewChangesetIterators(c.Generators)
		if err != nil {
			return err
		}
	}

	for ; itr.Valid(); err = itr.Next() {
		if err != nil {
			return err
		}
		changeset := itr.GetChangeset()

		if c.VersionLimit > 0 && changeset.Version > c.VersionLimit {
			break
		}

		for _, n := range changeset.Nodes {
			cnt++
			if cnt%100_000 == 0 {
				c.Log.Info().Msgf("processed %s leaves in %s; %s leaves/s",
					humanize.Comma(int64(cnt)),
					time.Since(since),
					humanize.Comma(int64(100_000/time.Since(since).Seconds())))
				since = time.Now()
			}
			c.MetricLeafCount.Inc()

			if n.Block != changeset.Version {
				return fmt.Errorf("expected block %d; got %d", changeset.Version, n.Block)
			}
			storeTree, err := multiTree.GetTree(n.StoreKey)
			if err != nil {
				return err
			}
			if !n.Delete {
				if _, err := storeTree.Set(n.Key, n.Value); err != nil {
					return err
				}
			} else {
				_, ok, err := storeTree.Remove(n.Key)
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("failed to remove key %x; version %d", n.Key, n.Block)
				}
			}
		}

		var hashes []byte
		hashes, err = multiTree.SaveVersions()
		if err != nil {
			return err
		}
		if changeset.Version%20000 == 0 && c.HashLog != nil {
			h := sha256.Sum256(hashes)
			_, err = fmt.Fprintf(c.HashLog, "%d|%x\n", iavlVersion, h)
			if err != nil {
				return err
			}
		}
	}

	//if c.MetricTreeSize != nil {
	//	c.MetricTreeSize.Set(float64(tree.Size()))
	//}
	//if c.MetricsTreeHeight != nil {
	//	c.MetricsTreeHeight.Set(float64(tree.Height()))
	//}

	return nil
}
