#!/bin/bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "$ROOT_DIR"

RESULTS_DIR=${RESULTS_DIR:-./results}
CHANGESET_DIR=${CHANGESET_DIR:-./changesets}
TARGET_VERSION=${TARGET_VERSION:-0}
WARM_CHANGESETS=${WARM_CHANGESETS:-0}
DISABLE_BG=${DISABLE_BG:-0}
CLEAN_RESULTS=${CLEAN_RESULTS:-1}
KEEP_DB=${KEEP_DB:-0}
SWEEP_VALUE_LOG=${SWEEP_VALUE_LOG:-1}
IAVL_SYNC=${IAVL_SYNC:-0}
INCLUDE_MEMDB=${INCLUDE_MEMDB:-0}

if [[ ! -d "$CHANGESET_DIR" ]]; then
  echo "changeset dir not found: $CHANGESET_DIR" >&2
  exit 1
fi

if [[ "$CLEAN_RESULTS" == "1" ]]; then
  echo "Cleaning up previous benchmark data..."
  rm -rf data-treedb-v1 "$RESULTS_DIR"
fi
mkdir -p data-treedb-v1 "$RESULTS_DIR"

if [[ "$INCLUDE_MEMDB" == "1" ]]; then
  rm -rf data-memdb-v1
  mkdir -p data-memdb-v1
fi

echo "Building benchmarks..."
(cd treedb-v1 && go build -o treedb-v1-bench .)
if [[ "$INCLUDE_MEMDB" == "1" ]]; then
  (cd iavl-v1-memdb && go build -o iavl-v1-memdb-bench .)
fi

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

target_args=()
if [[ "$TARGET_VERSION" != "0" ]]; then
  target_args=(--target-version "$TARGET_VERSION")
fi

run_case() {
  local mode="$1"
  local wal="$2"
  local relaxed="$3"
  local crc="$4"
  local vlog="$5"

  local name="wal${wal}_relaxed${relaxed}_crc${crc}_vlog${vlog}"
  local db_dir="data-treedb-v1/${mode}-${name}"
  local log_file="${RESULTS_DIR}/treedb-v1-${mode}-${name}.jsonl"

  local allow_unsafe=0
  if [[ "$wal" == "1" || "$relaxed" == "1" || "$crc" == "1" ]]; then
    allow_unsafe=1
  fi

  echo "Running TreeDB (mode=${mode}, ${name})..."
  rm -rf "$db_dir"
  mkdir -p "$db_dir"
  warm_changesets

  env     TREEDB_BENCH_MODE="$mode"     TREEDB_BENCH_DISABLE_WAL="$wal"     TREEDB_BENCH_RELAXED_SYNC="$relaxed"     TREEDB_BENCH_DISABLE_READ_CHECKSUM="$crc"     TREEDB_BENCH_DISABLE_VALUE_LOG="$vlog"     TREEDB_BENCH_ALLOW_UNSAFE="$allow_unsafe"     TREEDB_BENCH_DISABLE_BG="$DISABLE_BG"     TREEDB_BENCH_IAVL_SYNC="$IAVL_SYNC"     ./treedb-v1/treedb-v1-bench bench       --db-dir "$db_dir"       --changeset-dir "$CHANGESET_DIR"       --log-file "$log_file"       --log-type json       ${target_args[@]+"${target_args[@]}"}

  if [[ "$KEEP_DB" != "1" ]]; then
    rm -rf "$db_dir"
  fi
}

modes=(cached backend)
disable_wal_values=(0 1)
relaxed_sync_values=(0 1)
disable_crc_values=(0 1)

disable_vlog_values=(0)
if [[ "$SWEEP_VALUE_LOG" == "1" ]]; then
  disable_vlog_values=(0 1)
fi

for mode in "${modes[@]}"; do
  for wal in "${disable_wal_values[@]}"; do
    for relaxed in "${relaxed_sync_values[@]}"; do
      for crc in "${disable_crc_values[@]}"; do
        for vlog in "${disable_vlog_values[@]}"; do
          run_case "$mode" "$wal" "$relaxed" "$crc" "$vlog"
        done
      done
    done
  done
done

if [[ "$INCLUDE_MEMDB" == "1" ]]; then
  echo "Running iavl/v1 MemDB Benchmark..."
  rm -rf data-memdb-v1
  mkdir -p data-memdb-v1
  warm_changesets
  ./iavl-v1-memdb/iavl-v1-memdb-bench bench     --db-dir ./data-memdb-v1     --changeset-dir "$CHANGESET_DIR"     --log-file "$RESULTS_DIR/iavl-v1-memdb.jsonl"     --log-type json     ${target_args[@]+"${target_args[@]}"}
fi

echo "Done! Results saved to $RESULTS_DIR"
ls -lh "$RESULTS_DIR"/
