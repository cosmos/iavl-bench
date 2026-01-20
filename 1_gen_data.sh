#!/bin/bash
set -e

# Default values
VERSIONS=${1:-50}
SCALE=${2:-0.05}

echo "Generating changesets with VERSIONS=$VERSIONS and SCALE=$SCALE..."
echo "Bench backends: treedb (iavl v0), treedb-v1 (iavl v1), iavl/v1 (leveldb), iavl/v1-memdb"

# Cleanup
echo "Cleaning up old changesets..."
rm -rf changesets

# Run generator
cd bench
go run ./cmd/gen-changesets/main.go --versions "$VERSIONS" --scale "$SCALE" ../changesets

echo "Done! Changeset size:"
du -sh ../changesets
