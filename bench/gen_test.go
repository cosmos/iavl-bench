package bench_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"testing"
	"time"

	"github.com/cosmos/iavl-bench/bench"
	"github.com/cosmos/iavl-bench/bench/metrics"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func Test_ChangesetGenerator(t *testing.T) {
	bg := context.Background()
	bg, cancel := context.WithCancel(bg)
	go metrics.Default.Run(bg)
	defer func() {
		cancel()
	}()

	gen := bench.LockupLikeGenerator(0, 10_000_000)
	//gen := bench.BankLikeGenerator(0, 10_000_000)
	//gen := bench.StakingLikeGenerator(0, 10_000_000)
	itr, err := gen.Iterator()
	require.NoError(t, err)

	nodes := map[[16]byte]struct{}{}
	var cnt int64
	var lastCnt int64
	since := time.Now()
	for ; itr.Valid(); err = itr.Next() {
		require.NoError(t, err)
		changeset := itr.Nodes()
		exit := false
		for ; changeset.Valid(); err = changeset.Next() {
			require.NoError(t, err)
			node := changeset.GetNode()
			cnt++
			require.NotNil(t, node)
			keyHash := md5.Sum(node.Key)

			if node.Delete {
				_, exists := nodes[keyHash]
				require.True(t, exists, fmt.Sprintf("key %x not found; version %d",
					node.Key, itr.Version()))
				delete(nodes, keyHash)
			} else {
				nodes[keyHash] = struct{}{}
			}

			if cnt%1_000_000 == 0 {
				fmt.Printf("version %d; count %s; len %s; node/ms: %s\n",
					itr.Version(),
					humanize.Comma(cnt),
					humanize.Comma(int64(len(nodes))),
					humanize.Comma((cnt-lastCnt)/time.Since(since).Milliseconds()))
				lastCnt = cnt
				since = time.Now()
			}

			if cnt == 3_000_000 {
				exit = true
				break
			}
		}
		if exit {
			break
		}
	}
	require.NotEqual(t, 0, cnt)
	//require.True(t, gen.FinalSize == len(nodes) || gen.FinalSize == len(nodes)+1,
	//	fmt.Sprintf("final size %d != %d", gen.FinalSize, len(nodes)))
}

func Test_ChangesetGenerator_Determinism(t *testing.T) {
	cases := []struct {
		seed int64
		hash string
	}{
		{2, "d4aa2835373a2e344814f058b069e0e0"},
		{100, "0db33bf3684c62fb03ec6eaf49157592"},
		{777, "107ae635fa046002d21ad40723b30609"},
		{-43, "4c1c7187e45794129dafe77557a83caa"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("seed %d", tc.seed), func(t *testing.T) {
			gen := bench.ChangesetGenerator{
				StoreKey:         "test",
				Seed:             tc.seed,
				KeyMean:          10,
				KeyStdDev:        2,
				ValueMean:        100,
				ValueStdDev:      1000,
				InitialSize:      1000,
				FinalSize:        10000,
				Versions:         10,
				ChangePerVersion: 500,
				DeleteFraction:   0.1,
			}
			itr, err := gen.Iterator()
			require.NoError(t, err)

			nodes := map[[16]byte]struct{}{}
			var h [16]byte
			for ; itr.Valid(); err = itr.Next() {
				require.NoError(t, err)
				changeset := itr.Nodes()
				for ; changeset.Valid(); err = changeset.Next() {
					require.NoError(t, err)
					node := changeset.GetNode()
					require.NotNil(t, node)

					keyHash := md5.Sum(node.Key)
					if node.Delete {
						_, exists := nodes[keyHash]
						require.True(t, exists, fmt.Sprintf("key %x not found", node.Key))
						delete(nodes, keyHash)
					} else {
						nodes[keyHash] = struct{}{}
					}

					var buf bytes.Buffer
					buf.Write(h[:])
					buf.Write(node.Key)
					buf.Write(node.Value)
					h = md5.Sum(buf.Bytes())

				}
			}
			fmt.Printf("hash: %x\n", h)
			require.Equal(t, tc.hash, fmt.Sprintf("%x", h))
			require.Equal(t, gen.FinalSize, len(nodes))
		})
	}
}

func Test_ChangesetIterators(t *testing.T) {
	gen1 := &bench.ChangesetGenerator{
		StoreKey:         "test",
		Seed:             1,
		KeyMean:          10,
		KeyStdDev:        2,
		ValueMean:        100,
		ValueStdDev:      1000,
		InitialSize:      1000,
		FinalSize:        10000,
		Versions:         10,
		ChangePerVersion: 500,
		DeleteFraction:   0.1,
	}
	gen2 := *gen1
	gen2.Seed = 2
	gen3 := *gen1
	gen3.Seed = 3

	itr, err := bench.NewChangesetIterators([]bench.ChangesetGenerator{*gen1, gen2, gen3})
	require.NoError(t, err)

	nodes := map[[16]byte]struct{}{}
	var h [16]byte
	for ; itr.Valid(); err = itr.Next() {
		require.NoError(t, err)
		changeset := itr.Nodes()
		for ; changeset.Valid(); err = changeset.Next() {
			require.NoError(t, err)
			node := changeset.GetNode()
			require.NotNil(t, node)

			keyHash := md5.Sum(node.Key)
			if node.Delete {
				_, exists := nodes[keyHash]
				require.True(t, exists, fmt.Sprintf("key %x not found", node.Key))
				delete(nodes, keyHash)
			} else {
				nodes[keyHash] = struct{}{}
			}

			var buf bytes.Buffer
			buf.Write(h[:])
			buf.Write([]byte(node.StoreKey))
			buf.Write(node.Key)
			buf.Write(node.Value)
			h = md5.Sum(buf.Bytes())
		}
	}
	require.Equal(t, gen1.FinalSize+gen2.FinalSize+gen3.FinalSize, len(nodes))
}
