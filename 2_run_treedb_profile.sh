#!/bin/bash
set -e

echo "Cleaning up previous benchmark data..."
rm -rf data-treedb-v1-profile results-profile
mkdir -p data-treedb-v1-profile results-profile

# Ensure bench binary is built
(cd treedb-v1 && go build -o treedb-v1-bench .)

echo "Running TreeDB (IAVL v1) Benchmark with Profiling..."
# Using --mutexprofile and --blockprofile to catch contention
TREEDB_BENCH_DISABLE_WAL=1 ./treedb-v1/treedb-v1-bench bench \
    --db-dir ./data-treedb-v1-profile \
    --changeset-dir ./changesets \
    --log-file ./results-profile/treedb-v1.jsonl \
    --log-type json \
    --mutexprofile ./results-profile/mutex.prof \
    --blockprofile ./results-profile/block.prof \
    --cpuprofile ./results-profile/cpu.prof \
    --target-version 20 # Run shorter version for profiling

echo "Done! Profiles saved to ./results-profile/"
ls -lh results-profile/
