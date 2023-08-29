package core_test

import (
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
