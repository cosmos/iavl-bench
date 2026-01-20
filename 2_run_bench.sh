#!/bin/bash
set -e

echo "Cleaning up previous benchmark data..."
rm -rf data-treedb-v0 data-treedb-v1 data-leveldb-v1 data-memdb-v1 results
mkdir -p data-treedb-v0 data-treedb-v1 data-leveldb-v1 data-memdb-v1 results

echo "Building benchmarks..."
(cd treedb && go build -o treedb-v0-bench .)
(cd treedb-v1 && go build -o treedb-v1-bench .)
(cd iavl-v1 && go build -o iavl-v1-bench .)
(cd iavl-v1-memdb && go build -o iavl-v1-memdb-bench .)

#echo "Running TreeDB (IAVL v0.21.x) Benchmark..."
#./treedb/treedb-v0-bench bench \
#    --db-dir ./data-treedb-v0 \
#    --changeset-dir ./changesets \
#    --log-file ./results/treedb-v0.jsonl \
#    --log-type json
#
echo "Running TreeDB (IAVL v1) Benchmark..."
./treedb-v1/treedb-v1-bench bench \
    --db-dir ./data-treedb-v1 \
    --changeset-dir ./changesets \
    --log-file ./results/treedb-v1.jsonl \
    --log-type json

echo "Running IAVL v1 LevelDB Benchmark..."
./iavl-v1/iavl-v1-bench bench \
    --db-dir ./data-leveldb-v1 \
    --changeset-dir ./changesets \
    --log-file ./results/iavl-v1-leveldb.jsonl \
    --log-type json

echo "Running iavl/v1 MemDB Benchmark..."
./iavl-v1-memdb/iavl-v1-memdb-bench bench \
    --db-dir ./data-memdb-v1 \
    --changeset-dir ./changesets \
    --log-file ./results/iavl-v1-memdb.jsonl \
    --log-type json

echo "Done! Results saved to ./results/"
ls -lh results/
