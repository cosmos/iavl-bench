package bench

import (
	"bytes"
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

	// hack to use a single tree instead of per storekey
	OneTree string
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
		changeset := itr.Nodes()

		if c.VersionLimit > 0 && itr.Version() > c.VersionLimit {
			break
		}
		var (
			storekey string
			key      []byte
		)

		for ; changeset.Valid(); err = changeset.Next() {
			if err != nil {
				return err
			}
			cnt++
			if cnt%100_000 == 0 {
				c.Log.Info().Msgf("processed %s leaves in %s; %s leaves/s; version=%d",
					humanize.Comma(int64(cnt)),
					time.Since(since),
					humanize.Comma(int64(100_000/time.Since(since).Seconds())),
					itr.Version())
				since = time.Now()
			}
			c.MetricLeafCount.Inc()

			n := changeset.GetNode()
			if n.Block != itr.Version() {
				return fmt.Errorf("expected block %d; got %d", itr.Version(), n.Block)
			}
			if c.OneTree != "" {
				storekey = c.OneTree
				var keyBz bytes.Buffer
				keyBz.Write([]byte(n.StoreKey))
				keyBz.Write(n.Key)
				key = keyBz.Bytes()
			} else {
				storekey = n.StoreKey
				key = n.Key
			}
			storeTree, err := multiTree.GetTree(storekey)
			if err != nil {
				return err
			}
			if !n.Delete {
				if _, err := storeTree.Set(key, n.Value); err != nil {
					return err
				}
			} else {
				_, ok, err := storeTree.Remove(key)
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("failed to remove key %x; version %d", n.Key, n.Block)
				}
			}
		}

		var hash [32]byte
		if c.OneTree == "" {
			var hashes []byte
			hashes, err = multiTree.SaveVersions()
			if err != nil {
				return err
			}
			hash = sha256.Sum256(hashes)
		} else {
			storeTree, err := multiTree.GetTree(storekey)
			if err != nil {
				return err
			}
			var h []byte
			h, iavlVersion, err = storeTree.SaveVersion()
			if err != nil {
				return err
			}
			copy(hash[:], h)
		}

		if itr.Version()%1000 == 0 && c.HashLog != nil {
			_, err = fmt.Fprintf(c.HashLog, "%d|%x\n", iavlVersion, hash)
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
