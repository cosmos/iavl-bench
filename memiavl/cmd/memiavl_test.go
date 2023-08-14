package cmd

import "testing"

func Test_BuildTree(t *testing.T) {
	cmd := buildCommand(&context{
		indexDir:     "/Users/mattk/.costor",
		logDir:       "/Users/mattk/src/scratch/osmosis-hist/bank-ordered/",
		versionLimit: 1_000_000,
	})
	cmd.RunE(nil, nil)
}
