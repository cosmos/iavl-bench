package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"time"

	"github.com/dustin/go-humanize"
	api "github.com/kocubinski/costor-api"
	"github.com/kocubinski/costor-api/compact"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

type TreeContext struct {
	context.Context

	Log               zerolog.Logger
	IndexDir          string
	LogDir            string
	Generator         ChangesetGenerator
	VersionLimit      int64
	MetricLeafCount   prometheus.Counter
	MetricTreeSize    prometheus.Gauge
	MetricsTreeHeight prometheus.Gauge
	HashLog           *os.File
}

type NodeIterator interface {
	Valid() bool
	Next() error
	GetNode() *api.Node
}

type kvChange struct {
	store  string
	key    []byte
	value  []byte
	delete bool
}

func (c *TreeContext) BuildLegacyIAVL(multiTree *MultiTree) error {
	cnt := 1
	since := time.Now()
	lastVersion := int64(1)
	var (
		itr     NodeIterator
		err     error
		changes []kvChange
	)

	if c.LogDir != "" {
		stream := &compact.StreamingContext{}
		itr, err = stream.NewIterator(c.LogDir)
		if err != nil {
			return err
		}
	} else {
		//itr, err = NewChangesetIterators(c.Generators)
		//if err != nil {
		//	return err
		//}
		itr, err = c.Generator.Iterator()
		if err != nil {
			return err
		}
	}

	for ; itr.Valid(); err = itr.Next() {
		if err != nil {
			return err
		}
		n := itr.GetNode()

		sk := n.StoreKey
		if sk == "" {
			sk = "bank"
		}
		changes = append(changes, kvChange{
			store:  sk,
			key:    n.Key,
			value:  n.Value,
			delete: n.Delete,
		})

		if n.Block > lastVersion {
			for _, change := range changes {
				if !change.delete {
					if _, err := multiTree.Trees[change.store].Set(change.key, change.value); err != nil {
						return err
					}
				} else {
					_, ok, err := multiTree.Trees[change.store].Remove(change.key)
					if err != nil {
						return err
					}
					if !ok {
						return fmt.Errorf("failed to remove key %x", n.Key)
					}
				}
			}
			var hashes bytes.Buffer
			var version int64
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
			lastVersion = n.Block
			changes = nil
		}

		if c.VersionLimit > 0 && lastVersion > c.VersionLimit {
			break
		}

		//if c.MetricTreeSize != nil {
		//	c.MetricTreeSize.Set(float64(tree.Size()))
		//}
		//if c.MetricsTreeHeight != nil {
		//	c.MetricsTreeHeight.Set(float64(tree.Height()))
		//}

		if cnt%100_000 == 0 {
			c.Log.Info().Msgf("processed %s leaves in %s; %s leaves/s",
				humanize.Comma(int64(cnt)),
				time.Since(since),
				humanize.Comma(int64(100_000/time.Since(since).Seconds())))
			since = time.Now()
		}
		c.MetricLeafCount.Inc()
		cnt++
	}

	return nil
}
