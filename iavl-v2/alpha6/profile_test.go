package main

import (
	"os"
	"testing"

	"github.com/cosmos/iavl-bench/iavl-v2"
)

func TestProfile(t *testing.T) {
	runner := iavl_v2.Runner("iavl/v2-alpha6")
	dir, err := os.MkdirTemp("", "iavl-bench-profile")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	runner.SetArgs([]string{
		"bench",
		"--changeset-dir=/Users/arc/iavl-bench-data/testdata",
		"--db-dir=" + dir,
	})
	runner.Run()
}
