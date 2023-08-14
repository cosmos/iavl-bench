package core

import (
	"context"
	"fmt"
	"os"
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
	VersionLimit      int64
	MetricLeafCount   prometheus.Counter
	MetricTreeSize    prometheus.Gauge
	MetricsTreeHeight prometheus.Gauge
	HashLog           *os.File
}

type Tree interface {
	Set(key, value []byte) (bool, error)
	Get(key []byte) ([]byte, error)
	Remove(key []byte) ([]byte, bool, error)
	SaveVersion() ([]byte, int64, error)
	Size() int64
	Height() int8
}

type kvChange struct {
	key    []byte
	value  []byte
	delete bool
}

func (c *TreeContext) BuildLegacyIAVL(tree Tree) error {
	cnt := 1
	since := time.Now()
	lastVersion := int64(1)

	stream := &compact.StreamingContext{}
	itr, err := stream.NewIterator(c.LogDir)
	var changes []kvChange
	if err != nil {
		return err
	}
	for ; itr.Valid(); err = itr.Next() {
		if err != nil {
			return err
		}
		n := itr.Node

		changes = append(changes, kvChange{
			key:    n.Key,
			value:  n.Value,
			delete: n.Delete,
		})

		if n.Block > lastVersion {
			for _, change := range changes {
				if !change.delete {
					if _, err := tree.Set(change.key, change.value); err != nil {
						return err
					}
				} else {
					_, ok, err := tree.Remove(change.key)
					if err != nil {
						return err
					}
					if !ok {
						return fmt.Errorf("failed to remove key %s", string(n.Key))
					}
				}
			}
			h, v, err := tree.SaveVersion()
			if err != nil {
				return err
			}
			if v%20000 == 0 && c.HashLog != nil {
				_, err = fmt.Fprintf(c.HashLog, "%d|%x\n", v, h)
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

		if c.MetricTreeSize != nil {
			c.MetricTreeSize.Set(float64(tree.Size()))
		}
		if c.MetricsTreeHeight != nil {
			c.MetricsTreeHeight.Set(float64(tree.Height()))
		}

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
