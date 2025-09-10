package main

import (
	"os"
	"testing"

	memiavl "bench-memiavl/runner"
)

func TestProfile(t *testing.T) {
	runner := memiavl.Runner()
	dir, err := os.MkdirTemp("", "iavl-bench-profile")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	runner.SetArgs([]string{
		"bench",
		"--changeset-dir=/Users/arc/iavl-bench-data/testdata",
		"--db-dir=" + dir,
		`--db-options={"snapshot_interval":10}`,
	})
	runner.Run()
}
