#!/bin/bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "$ROOT_DIR"

RESULTS_DIR=${RESULTS_DIR:-./results}
CHANGESET_DIR=${CHANGESET_DIR:-./changesets}
WARM_CHANGESETS=${WARM_CHANGESETS:-0}
DISABLE_BG=${DISABLE_BG:-0}

if [[ ! -d "$CHANGESET_DIR" ]]; then
  echo "changeset dir not found: $CHANGESET_DIR" >&2
  exit 1
fi

echo "Cleaning up previous benchmark data..."
rm -rf data-leveldb-v1 data-treedb-v1 data-memdb-v1 "$RESULTS_DIR"
mkdir -p data-leveldb-v1 data-treedb-v1 data-memdb-v1 "$RESULTS_DIR"

echo "Building benchmarks..."
(cd treedb-v1 && go build -o treedb-v1-bench .)
(cd iavl-v1-memdb && go build -o iavl-v1-memdb-bench .)

warm_changesets() {
  if [[ "$WARM_CHANGESETS" != "1" ]]; then
    return
  fi
  echo "Warming changesets (WARM_CHANGESETS=1)..."
  if [[ -f "$CHANGESET_DIR/changeset_info.json" ]]; then
    cat "$CHANGESET_DIR/changeset_info.json" >/dev/null
  fi
  find "$CHANGESET_DIR" -type f -name '*.delimpb' -print0 | xargs -0 cat >/dev/null
}

#TREEDB_LEAF_PREFIX_COMPRESSION=1 

echo "Running IAVL v1 TreeDB Benchmark..."
flag="fast"
warm_changesets
db_dir="data-treedb-v1/${flag}"
log_file="${RESULTS_DIR}/treedb-v1-${flag}.jsonl"
env \
  TREEDB_BENCH_PROFILE="${TREEDB_BENCH_PROFILE:-fast}" \
  TREEDB_BENCH_MODE="${TREEDB_BENCH_MODE:-cached}" \
  TREEDB_BENCH_DISABLE_BG="$DISABLE_BG" \
  TREEDB_BENCH_DISABLE_WAL="${TREEDB_BENCH_DISABLE_WAL:-1}" \
  TREEDB_BENCH_DISABLE_VALUE_LOG="${TREEDB_BENCH_DISABLE_VALUE_LOG:-0}" \
  TREEDB_BENCH_RELAXED_SYNC="${TREEDB_BENCH_RELAXED_SYNC:-1}" \
  TREEDB_BENCH_DISABLE_READ_CHECKSUM="${TREEDB_BENCH_DISABLE_READ_CHECKSUM:-1}" \
  TREEDB_BENCH_ALLOW_UNSAFE="${TREEDB_BENCH_ALLOW_UNSAFE:-1}" \
  TREEDB_BENCH_ALLOW_VIEW="${TREEDB_BENCH_ALLOW_VIEW:-0}" \
  TREEDB_BENCH_ALLOW_UNSAFE_READS="${TREEDB_BENCH_ALLOW_UNSAFE_READS:-0}" \
  TREEDB_BENCH_IAVL_SYNC="${TREEDB_BENCH_IAVL_SYNC:-0}" \
  TREEDB_BENCH_REUSE_READS="${TREEDB_BENCH_REUSE_READS:-1}" \
  TREEDB_SLAB_COMPRESSION="${TREEDB_SLAB_COMPRESSION:-zstd}" \
  TREEDB_LEAF_PREFIX_COMPRESSION="${TREEDB_LEAF_PREFIX_COMPRESSION:-1}" \
  TREEDB_ZIPPER_DEBUG_SEPARATORS="${TREEDB_ZIPPER_DEBUG_SEPARATORS:-1}" \
  TREEDB_FORCE_VALUE_POINTERS="${TREEDB_FORCE_VALUE_POINTERS:-1}" \
  ./treedb-v1/treedb-v1-bench bench \
    --db-dir "$db_dir" \
    --changeset-dir "$CHANGESET_DIR" \
    --log-file "$log_file" \
    --log-type json

#warm_changesets
#echo "Running IAVL v1 LevelDB Benchmark..."
#./iavl-v1/iavl-v1-bench bench \
#    --db-dir ./data-leveldb-v1 \
#    --changeset-dir ./changesets \
#    --log-file ./results/iavl-v1-leveldb.jsonl \
#    --log-type json
#

echo "Running iavl/v1 MemDB Benchmark..."
rm -rf data-memdb-v1
mkdir -p data-memdb-v1
warm_changesets
./iavl-v1-memdb/iavl-v1-memdb-bench bench   --db-dir ./data-memdb-v1   --changeset-dir "$CHANGESET_DIR"   --log-file "$RESULTS_DIR/iavl-v1-memdb.jsonl"   --log-type json

echo "Done! Results saved to $RESULTS_DIR"
ls -lh "$RESULTS_DIR"/
