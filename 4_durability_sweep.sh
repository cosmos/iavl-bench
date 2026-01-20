#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Durability/profile sweep for TreeDB's iavl-bench (treedb-v1).
#
# Usage:
#   ./4_durability_sweep.sh
#
# Optional knobs:
#   TARGET_VERSION=40         # default 40
#   OUT_DIR=results-durability/2025-12-26T12-00-00
#   KEEP_DB=0|1               # default 0 (delete db dirs after each run)
#   DISABLE_BG=0|1            # default 1 (disable background workers for cleaner profiles)
#   ENABLE_BLOCK=0|1          # default 0 (block profiles are noisy/overhead)
#   SWEEP_VALUE_LOG=0|1       # default 0 (run both DisableValueLog=0/1)
#   SWEEP_CHECKSUM=0|1        # default 0 (run both DisableReadChecksum=0/1)

TARGET_VERSION="${TARGET_VERSION:-40}"
OUT_DIR="${OUT_DIR:-results-durability/$(date +%Y-%m-%dT%H-%M-%S)}"
KEEP_DB="${KEEP_DB:-0}"
DISABLE_BG="${DISABLE_BG:-1}"
ALLOW_UNSAFE="${ALLOW_UNSAFE:-1}"
ENABLE_BLOCK="${ENABLE_BLOCK:-0}"
SWEEP_VALUE_LOG="${SWEEP_VALUE_LOG:-0}"
SWEEP_CHECKSUM="${SWEEP_CHECKSUM:-0}"
GOMAP_DIR="${GOMAP_DIR:-${ROOT_DIR}/../gomap}"

disable_value_log_values=(0)
disable_read_checksum_values=(1)
if [[ "${SWEEP_VALUE_LOG}" == "1" ]]; then
  disable_value_log_values=(0 1)
fi
if [[ "${SWEEP_CHECKSUM}" == "1" ]]; then
  disable_read_checksum_values=(0 1)
fi

mkdir -p "$OUT_DIR"

echo "Building treedb-v1 bench..."
(cd treedb-v1 && go build -o treedb-v1-bench .)

run_case() {
  local name="$1"; shift
  local db_dir="data-durability-${name}"
  local case_dir="${OUT_DIR}/${name}"

  echo
  echo "=== ${name} ==="
  echo "db_dir=${db_dir}"
  echo "out_dir=${case_dir}"

  rm -rf "${db_dir}"
  mkdir -p "${db_dir}"
  mkdir -p "${case_dir}"

  local block_cmd=""
  if [[ "${ENABLE_BLOCK}" == "1" ]]; then
    block_cmd="--blockprofile ${case_dir}/block.pprof --blockprofile-rate 1000000"
  fi

  # Record configuration for later triage.
  {
    echo "TARGET_VERSION=${TARGET_VERSION}"
    echo "KEEP_DB=${KEEP_DB}"
    echo "DISABLE_BG=${DISABLE_BG}"
    echo "ENABLE_BLOCK=${ENABLE_BLOCK}"
    echo
    echo "git:"
    echo "  iavl-bench=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
    if [[ -d "${GOMAP_DIR}/.git" ]]; then
      echo "  gomap=$(git -C \"${GOMAP_DIR}\" rev-parse --short HEAD 2>/dev/null || echo unknown)"
      echo "  gomap-branch=$(git -C \"${GOMAP_DIR}\" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
    fi
    echo
    echo "case_env:"
    for kv in "$@"; do
      echo "  ${kv}"
    done
    echo "  TREEDB_BENCH_DISABLE_BG=${DISABLE_BG}"
    echo "  TREEDB_BENCH_ALLOW_UNSAFE=${ALLOW_UNSAFE}"
    echo
    echo "cmd:"
    echo "./treedb-v1/treedb-v1-bench bench --db-dir ${db_dir} --changeset-dir ./changesets --log-file ${case_dir}/run.jsonl --log-type json --cpuprofile ${case_dir}/cpu.pprof --mutexprofile ${case_dir}/mutex.pprof --mutexprofile-fraction 1 ${block_cmd} --target-version ${TARGET_VERSION}"
  } > "${case_dir}/config.txt"

  # Run benchmark + profiles.
  if [[ "${ENABLE_BLOCK}" == "1" ]]; then
    env "$@" \
      TREEDB_BENCH_DISABLE_BG="${DISABLE_BG}" \
      TREEDB_BENCH_ALLOW_UNSAFE="${ALLOW_UNSAFE}" \
      ./treedb-v1/treedb-v1-bench bench \
        --db-dir "./${db_dir}" \
        --changeset-dir ./changesets \
        --log-file "${case_dir}/run.jsonl" \
        --log-type json \
        --cpuprofile "${case_dir}/cpu.pprof" \
        --mutexprofile "${case_dir}/mutex.pprof" \
        --mutexprofile-fraction 1 \
        --blockprofile "${case_dir}/block.pprof" \
        --blockprofile-rate 1000000 \
        --target-version "${TARGET_VERSION}"
  else
    env "$@" \
      TREEDB_BENCH_DISABLE_BG="${DISABLE_BG}" \
      TREEDB_BENCH_ALLOW_UNSAFE="${ALLOW_UNSAFE}" \
      ./treedb-v1/treedb-v1-bench bench \
        --db-dir "./${db_dir}" \
        --changeset-dir ./changesets \
        --log-file "${case_dir}/run.jsonl" \
        --log-type json \
        --cpuprofile "${case_dir}/cpu.pprof" \
        --mutexprofile "${case_dir}/mutex.pprof" \
        --mutexprofile-fraction 1 \
        --target-version "${TARGET_VERSION}"
  fi

  # Summaries.
  go tool pprof -top -nodecount=50 ./treedb-v1/treedb-v1-bench "${case_dir}/cpu.pprof" > "${case_dir}/cpu.top.txt" || true
  go tool pprof -top -nodecount=50 ./treedb-v1/treedb-v1-bench "${case_dir}/mutex.pprof" > "${case_dir}/mutex.top.txt" || true
  if [[ "${ENABLE_BLOCK}" == "1" ]]; then
    go tool pprof -top -nodecount=50 ./treedb-v1/treedb-v1-bench "${case_dir}/block.pprof" > "${case_dir}/block.top.txt" || true
  fi

  if [[ "${KEEP_DB}" != "1" ]]; then
    rm -rf "${db_dir}"
  fi
}

run_tier() {
  local base="$1"; shift

  local disable_value_log
  local disable_read_checksum

  for disable_value_log in "${disable_value_log_values[@]}"; do
    for disable_read_checksum in "${disable_read_checksum_values[@]}"; do
      run_case \
        "${base}_vlog_${disable_value_log}_crc_${disable_read_checksum}" \
        "$@" \
        "TREEDB_BENCH_DISABLE_VALUE_LOG=${disable_value_log}" \
        "TREEDB_BENCH_DISABLE_READ_CHECKSUM=${disable_read_checksum}"
    done
  done
}

# Lowest durability: WAL disabled.
run_tier \
  "wal_off" \
  TREEDB_BENCH_DISABLE_WAL=1 \
  TREEDB_BENCH_IAVL_SYNC=0 \
  TREEDB_BENCH_CHECKPOINT_EVERY=0

# WAL disabled, but checkpoint every version (durable backend boundary; no WAL recovery).
run_tier \
  "wal_off_checkpoint_1" \
  TREEDB_BENCH_DISABLE_WAL=1 \
  TREEDB_BENCH_IAVL_SYNC=0 \
  TREEDB_BENCH_CHECKPOINT_EVERY=1

# WAL enabled, non-sync commits (best-effort; no fsync).
run_tier \
  "wal_on_nonsync" \
  TREEDB_BENCH_DISABLE_WAL=0 \
  TREEDB_BENCH_IAVL_SYNC=0 \
  TREEDB_BENCH_CHECKPOINT_EVERY=0

# WAL enabled, non-sync commits, but checkpoint every version (durable backend boundary + WAL trim).
run_tier \
  "wal_on_nonsync_checkpoint_1" \
  TREEDB_BENCH_DISABLE_WAL=0 \
  TREEDB_BENCH_IAVL_SYNC=0 \
  TREEDB_BENCH_CHECKPOINT_EVERY=1

# WAL enabled, sync commits but relaxed (WriteSync does not fsync).
run_tier \
  "wal_on_sync_relaxed" \
  TREEDB_BENCH_DISABLE_WAL=0 \
  TREEDB_BENCH_IAVL_SYNC=1 \
  TREEDB_BENCH_RELAXED_SYNC=1 \
  TREEDB_BENCH_CHECKPOINT_EVERY=0

# Highest durability: WAL enabled, sync commits, strict sync (fsync).
run_tier \
  "wal_on_sync_strict" \
  TREEDB_BENCH_DISABLE_WAL=0 \
  TREEDB_BENCH_IAVL_SYNC=1 \
  TREEDB_BENCH_RELAXED_SYNC=0 \
  TREEDB_BENCH_CHECKPOINT_EVERY=0

# Highest durability + backend boundary: strict sync plus checkpoint every version.
run_tier \
  "wal_on_sync_strict_checkpoint_1" \
  TREEDB_BENCH_DISABLE_WAL=0 \
  TREEDB_BENCH_IAVL_SYNC=1 \
  TREEDB_BENCH_RELAXED_SYNC=0 \
  TREEDB_BENCH_CHECKPOINT_EVERY=1

echo
echo "Done. Outputs in ${OUT_DIR}"
