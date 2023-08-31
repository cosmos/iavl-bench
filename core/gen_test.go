package core_test

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/kocubinski/iavl-bench/core"
	"github.com/stretchr/testify/require"
)

func Test_ChangesetGenerator(t *testing.T) {
	//gen := core.ChangesetGenerator{
	//	StoreKey:         "test",
	//	Seed:             2,
	//	KeyMean:          10,
	//	KeyStdDev:        2,
	//	ValueMean:        100,
	//	ValueStdDev:      2000,
	//	InitialSize:      1000,
	//	FinalSize:        10000,
	//	Versions:         10,
	//	ChangePerVersion: 500,
	//	DeleteFraction:   0.1,
	//}
	gen := core.BankLikeGenerator(0, 10_000_000)
	itr, err := gen.Iterator()
	require.NoError(t, err)

	nodes := map[[16]byte]struct{}{}
	var cnt int64
	var lastCnt int64
	since := time.Now()
	version := itr.Version
	for ; itr.Valid(); err = itr.Next() {
		if itr.Version != version {
			//fmt.Printf("version %d; count %d; len %d\n", version, cnt, len(nodes))
			version = itr.Version
		}
		cnt++
		//if fmt.Sprintf("%x", itr.Node.Key) == "49" {
		//	n := itr.Node
		//	if !n.Delete {
		//		vh := md5.Sum(n.Value)
		//		n.Value = vh[:]
		//	}
		//	fmt.Printf("seen key 49 %v\n", n)
		//}
		require.NoError(t, err)
		require.NotNil(t, itr.Node)
		keyHash := md5.Sum(itr.Node.Key)
		if itr.Node.Delete {
			_, exists := nodes[keyHash]
			require.True(t, exists, fmt.Sprintf("key %x not found; version %d", itr.Node.Key, version))
			delete(nodes, keyHash)
		} else {
			nodes[keyHash] = struct{}{}
		}
		if cnt%1_000_000 == 0 {
			fmt.Printf("version %d; count %s; len %s; node/ms: %s\n",
				version, humanize.Comma(cnt),
				humanize.Comma(int64(len(nodes))),
				humanize.Comma((cnt-lastCnt)/time.Since(since).Milliseconds()))
			lastCnt = cnt
			since = time.Now()
		}
	}
	require.NotEqual(t, 0, cnt)
	require.Equal(t, gen.FinalSize, len(nodes))
}

func Test_ChangesetGenerator_Determinism(t *testing.T) {
	cases := []struct {
		seed int64
		hash string
	}{
		{2, "08589a9d7583b598552f2dd328b9f087"},
		{100, "6e00828663122181dbd185a3120b00d9"},
		{777, "cf66266c99122410110b6885b0e72589"},
		{-43, "3eba060775aebf83a7edb304377a84f4"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("seed %d", tc.seed), func(t *testing.T) {
			gen := core.ChangesetGenerator{
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
				require.NotNil(t, itr.Node)

				keyHash := md5.Sum(itr.Node.Key)
				if itr.Node.Delete {
					_, exists := nodes[keyHash]
					require.True(t, exists, fmt.Sprintf("key %x not found", itr.Node.Key))
					delete(nodes, keyHash)
				} else {
					nodes[keyHash] = struct{}{}
				}

				var buf bytes.Buffer
				buf.Write(h[:])
				buf.Write(itr.Node.Key)
				buf.Write(itr.Node.Value)
				h = md5.Sum(buf.Bytes())
			}
			fmt.Printf("hash: %x\n", h)
			require.Equal(t, tc.hash, fmt.Sprintf("%x", h))
			require.Equal(t, gen.FinalSize, len(nodes))
		})
	}
}
