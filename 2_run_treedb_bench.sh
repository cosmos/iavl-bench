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
rm -rf data-treedb-v1 data-memdb-v1 "$RESULTS_DIR"
mkdir -p data-treedb-v1 data-memdb-v1 "$RESULTS_DIR"

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

run_treedb_case() {
  local name="$1"
  local mode="$2"
  shift 2
  local db_dir="data-treedb-v1/${mode}-${name}"
  local log_file="${RESULTS_DIR}/treedb-v1-${mode}-${name}.jsonl"

  echo "Running TreeDB (mode=${mode}, case=${name})..."
  rm -rf "$db_dir"
  mkdir -p "$db_dir"
  warm_changesets

  env     TREEDB_BENCH_MODE="$mode"     TREEDB_BENCH_DISABLE_BG="$DISABLE_BG"     "$@"     ./treedb-v1/treedb-v1-bench bench       --db-dir "$db_dir"       --changeset-dir "$CHANGESET_DIR"       --log-file "$log_file"       --log-type json
}

run_treedb_case safe_strict cached   TREEDB_BENCH_DISABLE_WAL=0   TREEDB_BENCH_RELAXED_SYNC=0   TREEDB_BENCH_DISABLE_READ_CHECKSUM=0   TREEDB_BENCH_ALLOW_UNSAFE=0   TREEDB_BENCH_IAVL_SYNC=1

run_treedb_case safe_nonsync cached   TREEDB_BENCH_DISABLE_WAL=0   TREEDB_BENCH_RELAXED_SYNC=0   TREEDB_BENCH_DISABLE_READ_CHECKSUM=0   TREEDB_BENCH_ALLOW_UNSAFE=0   TREEDB_BENCH_IAVL_SYNC=0

run_treedb_case unsafe_fast cached   TREEDB_BENCH_DISABLE_WAL=1   TREEDB_BENCH_RELAXED_SYNC=1   TREEDB_BENCH_DISABLE_READ_CHECKSUM=1   TREEDB_BENCH_ALLOW_UNSAFE=1   TREEDB_BENCH_IAVL_SYNC=0

#run_treedb_case safe_strict backend   TREEDB_BENCH_DISABLE_WAL=0   TREEDB_BENCH_RELAXED_SYNC=0   TREEDB_BENCH_DISABLE_READ_CHECKSUM=0   TREEDB_BENCH_ALLOW_UNSAFE=0   TREEDB_BENCH_IAVL_SYNC=1
#
#run_treedb_case safe_nonsync backend   TREEDB_BENCH_DISABLE_WAL=0   TREEDB_BENCH_RELAXED_SYNC=0   TREEDB_BENCH_DISABLE_READ_CHECKSUM=0   TREEDB_BENCH_ALLOW_UNSAFE=0   TREEDB_BENCH_IAVL_SYNC=0
#
#run_treedb_case unsafe_fast backend   TREEDB_BENCH_DISABLE_WAL=1   TREEDB_BENCH_RELAXED_SYNC=1   TREEDB_BENCH_DISABLE_READ_CHECKSUM=1   TREEDB_BENCH_ALLOW_UNSAFE=1   TREEDB_BENCH_IAVL_SYNC=0

echo "Running iavl/v1 MemDB Benchmark..."
rm -rf data-memdb-v1
mkdir -p data-memdb-v1
warm_changesets
./iavl-v1-memdb/iavl-v1-memdb-bench bench   --db-dir ./data-memdb-v1   --changeset-dir "$CHANGESET_DIR"   --log-file "$RESULTS_DIR/iavl-v1-memdb.jsonl"   --log-type json

echo "Done! Results saved to $RESULTS_DIR"
ls -lh "$RESULTS_DIR"/
