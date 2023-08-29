package cmd

import (
	"testing"

	"github.com/kocubinski/iavl-bench/core"
	"github.com/stretchr/testify/require"
)

func Test_BuildTree(t *testing.T) {
	cmd := TreeCommand(&Context{
		TreeContext: core.TreeContext{
			IndexDir:     "/Users/mattk/.costor",
			VersionLimit: 1_000_000,
		},
	})
	require.NoError(t, cmd.Flags().Set("log-dir", "/Users/mattk/src/scratch/osmosis-hist/bank-ordered"))
	require.NoError(t, cmd.Flags().Set("nop", "true"))
	require.NoError(t, cmd.RunE(nil, nil))
}
