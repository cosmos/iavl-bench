package core_test

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"testing"

	api "github.com/kocubinski/costor-api"
	"github.com/kocubinski/iavl-bench/core"
	"github.com/stretchr/testify/require"
)

func Test_ChangesetGenerator(t *testing.T) {
	gen := core.ChangesetGenerator{
		StoreKey:         "test",
		Seed:             2,
		KeyMean:          10,
		KeyStdDev:        2,
		ValueMean:        100,
		ValueStdDev:      20,
		InitialSize:      1000,
		FinalSize:        10000,
		Versions:         10,
		ChangePerVersion: 500,
		DeleteFraction:   0.1,
	}
	itr, err := gen.Iterator()
	require.NoError(t, err)

	nodes := map[string]*api.Node{}
	cnt := 0
	version := itr.Version
	for ; itr.Valid(); err = itr.Next() {
		if itr.Version != version {
			fmt.Printf("version %d; count %d; len %d\n", version, cnt, len(nodes))
			version = itr.Version
			cnt = 0
		}
		cnt++
		require.NoError(t, err)
		require.NotNil(t, itr.Node)
		if itr.Node.Delete {
			delete(nodes, string(itr.Node.Key))
		} else {
			nodes[string(itr.Node.Key)] = itr.Node
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
		{2, "9b3d25249f2fbc3d93967a42360c7daf"},
		{100, "5554d39f506382dac4bbe9038ed7d715"},
		{777, "2bcb9c53a46a24006583478d669dc29a"},
		{-43, "390132d9deda4c88325bcfd4f2bc3d8c"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("seed %d", tc.seed), func(t *testing.T) {
			gen := core.ChangesetGenerator{
				StoreKey:         "test",
				Seed:             tc.seed,
				KeyMean:          10,
				KeyStdDev:        2,
				ValueMean:        100,
				ValueStdDev:      20,
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
