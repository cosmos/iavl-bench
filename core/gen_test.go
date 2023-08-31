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
	gen := core.BankLikeGenerator(0, 100_000)
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
			if version%1000 == 0 {
				fmt.Printf("version %d; count %s; len %d; node/s: %s\n",
					version, humanize.Comma(cnt), len(nodes),
					humanize.Comma(int64(cnt-lastCnt)/int64(time.Since(since).Seconds())))
				lastCnt = cnt
				since = time.Now()
			}
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
	}
	require.NotEqual(t, 0, cnt)
	require.Equal(t, gen.FinalSize, len(nodes))
}

func Test_ChangesetGenerator_Determinism(t *testing.T) {
	cases := []struct {
		seed int64
		hash string
	}{
		{2, "6b1016b9e3ae4518176be598b92c3756"},
		{100, "02d8c8b77136a8b515f12fa44d1457ec"},
		{777, "903a2b9a4507fc61a2e4f1c5ba54a616"},
		{-43, "49ff48a37ed02a0d9ec87acad273a1e5"},
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

			var h [16]byte
			for ; itr.Valid(); err = itr.Next() {
				require.NoError(t, err)
				require.NotNil(t, itr.Node)
				var buf bytes.Buffer
				buf.Write(h[:])
				buf.Write(itr.Node.Key)
				buf.Write(itr.Node.Value)
				h = md5.Sum(buf.Bytes())
			}
			fmt.Printf("hash: %x\n", h)
			require.Equal(t, tc.hash, fmt.Sprintf("%x", h))
		})
	}
}
