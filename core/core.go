package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"time"

	"github.com/dustin/go-humanize"
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

func (c *TreeContext) BuildLegacyIAVL(multiTree *MultiTree) error {
	cnt := 1
	since := time.Now()
	var (
		itr ChangesetIterator
		err error
	)

	if c.LogDir != "" {
		//stream := &compact.StreamingContext{}
		//itr, err = stream.NewIterator(c.LogDir)
		//if err != nil {
		//	return err
		//}
		return fmt.Errorf("not implemented")
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
		nodes := itr.GetChangeset().Nodes
		version := nodes[0].Block

		if c.VersionLimit > 0 && version > c.VersionLimit {
			break
		}

		for _, n := range itr.GetChangeset().Nodes {
			cnt++
			if cnt%100_000 == 0 {
				c.Log.Info().Msgf("processed %s leaves in %s; %s leaves/s",
					humanize.Comma(int64(cnt)),
					time.Since(since),
					humanize.Comma(int64(100_000/time.Since(since).Seconds())))
				since = time.Now()
			}
			c.MetricLeafCount.Inc()

			if n.Block != version {
				return fmt.Errorf("expected block %d; got %d", version, n.Block)
			}
			if !n.Delete {
				if _, err := multiTree.Trees[n.StoreKey].Set(n.Key, n.Value); err != nil {
					return err
				}
			} else {
				_, ok, err := multiTree.Trees[n.StoreKey].Remove(n.Key)
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("failed to remove key %x; version %d", n.Key, n.Block)
				}
			}
		}

		var hashes bytes.Buffer
		for _, tree := range multiTree.Trees {
			h, v, err := tree.SaveVersion()
			if err != nil {
				return err
			}
			version = v
			hashes.Write(h)
		}
		if version%20000 == 0 && c.HashLog != nil {
			h := sha256.Sum256(hashes.Bytes())
			_, err = fmt.Fprintf(c.HashLog, "%d|%x\n", version, h)
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
