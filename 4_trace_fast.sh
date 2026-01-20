#!/usr/bin/env bash
set -euo pipefail

# Run fast-mode bench with fs_usage tracing for dec26 and main.
# Usage: sudo ./4_trace_fast.sh
#
# Env:
#   TRACE_SECS=120
#   TRACE_MODE=filesys
#   TRACE_ATTACH_MODE=pid|command
#   CHANGESET_DIR=/path/to/changesets
#   RESULTS_DIR=/path/to/results
#   DISABLE_BG=1
#   TRACE_TARGETS="dec26 main"

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run with sudo: sudo $0" >&2
  exit 1
fi

RUN_USER="${SUDO_USER:-}"
if [[ -z "${RUN_USER}" ]]; then
  echo "SUDO_USER not set; run with sudo so the bench can run as your user." >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHANGESET_DIR="${CHANGESET_DIR:-${ROOT_DIR}/changesets}"
RESULTS_DIR="${RESULTS_DIR:-${ROOT_DIR}/results}"
TRACE_SECS="${TRACE_SECS:-120}"
TRACE_MODE="${TRACE_MODE:-filesys}"
TRACE_ATTACH_MODE="${TRACE_ATTACH_MODE:-}"
DISABLE_BG="${DISABLE_BG:-1}"
TRACE_TARGETS="${TRACE_TARGETS:-dec26 main}"
GOMAP_DIR="${GOMAP_DIR:-${ROOT_DIR}/../gomap}"
GOMAP_MAIN_DIR="${GOMAP_MAIN_DIR:-${ROOT_DIR}/../gomap-main}"
RUN_HOME="$(eval echo "~${RUN_USER}")"

if [[ -z "${TRACE_ATTACH_MODE}" ]]; then
  if [[ "${TRACE_MODE}" == "pathname" ]]; then
    TRACE_ATTACH_MODE="command"
  else
    TRACE_ATTACH_MODE="pid"
  fi
fi

if [[ ! -d "${CHANGESET_DIR}" ]]; then
  echo "changeset dir not found: ${CHANGESET_DIR}" >&2
  exit 1
fi

if ! command -v fs_usage >/dev/null 2>&1; then
  echo "fs_usage not found on PATH." >&2
  exit 1
fi

run_as_user() {
  sudo -u "${RUN_USER}" -E env PATH="${PATH}" HOME="${RUN_HOME}" "$@"
}

clean_data() {
  rm -rf "${ROOT_DIR}"/data-*
}

prepare_target() {
  local target="$1"
  if [[ "${target}" == "dec26" ]]; then
    run_as_user bash -c "cd \"${ROOT_DIR}/treedb-v1\" && go mod edit -replace github.com/snissn/gomap=${GOMAP_DIR}"
  else
    run_as_user bash -c "cd \"${ROOT_DIR}/treedb-v1\" && go mod edit -replace github.com/snissn/gomap=${GOMAP_MAIN_DIR}"
  fi
  run_as_user bash -c "cd \"${ROOT_DIR}/treedb-v1\" && go build -o treedb-v1-bench ."
}

run_trace() {
  local target="$1"
  local ts
  ts="$(date +%Y%m%d%H%M%S)"

  prepare_target "${target}"

  local db_dir="${ROOT_DIR}/data-treedb-v1/${target}-fast-trace-${ts}"
  local log_file="${RESULTS_DIR}/treedb-v1-cached-${target}-fast-trace-${ts}.jsonl"
  local trace_file="${RESULTS_DIR}/fs_usage-${TRACE_MODE}-${target}-fast-${ts}.txt"
  local pid_file="${RESULTS_DIR}/treedb-v1-${target}-fast-${ts}.pid"

  run_as_user mkdir -p "${ROOT_DIR}/data-treedb-v1" "${RESULTS_DIR}"
  run_as_user rm -rf "${db_dir}"
  run_as_user mkdir -p "${db_dir}"

  local fs_pid=""
  if [[ "${TRACE_ATTACH_MODE}" == "command" ]]; then
    fs_usage -w -f "${TRACE_MODE}" -t "${TRACE_SECS}" treedb-v1-bench > "${trace_file}" &
    fs_pid=$!
  fi

  local bench_cmd
  bench_cmd="cd \"${ROOT_DIR}\"; echo \\$\\$ > \"${pid_file}\"; exec env \
TREEDB_BENCH_MODE=cached \
TREEDB_BENCH_DISABLE_BG=${DISABLE_BG} \
TREEDB_BENCH_DISABLE_WAL=1 \
TREEDB_BENCH_RELAXED_SYNC=1 \
TREEDB_BENCH_DISABLE_READ_CHECKSUM=1 \
TREEDB_BENCH_ALLOW_UNSAFE=1 \
TREEDB_BENCH_IAVL_SYNC=0 \
\"${ROOT_DIR}/treedb-v1/treedb-v1-bench\" bench \
  --db-dir \"${db_dir}\" \
  --changeset-dir \"${CHANGESET_DIR}\" \
  --log-file \"${log_file}\" \
  --log-type json"

  run_as_user bash -c "${bench_cmd}" &
  local bench_runner_pid=$!

  local bench_pid=""
  for _ in $(seq 1 200); do
    if [[ -s "${pid_file}" ]]; then
      bench_pid="$(cat "${pid_file}")"
      break
    fi
    sleep 0.1
  done

  if [[ -z "${bench_pid}" ]]; then
    echo "Failed to capture bench PID for ${target}." >&2
    wait "${bench_runner_pid}" || true
    return 1
  fi

  if [[ "${TRACE_ATTACH_MODE}" == "pid" ]]; then
    fs_usage -w -f "${TRACE_MODE}" -t "${TRACE_SECS}" "${bench_pid}" > "${trace_file}" &
    fs_pid=$!
  fi

  wait "${bench_runner_pid}"

  if [[ -n "${fs_pid}" ]]; then
    if kill -0 "${fs_pid}" 2>/dev/null; then
      kill "${fs_pid}" || true
    fi
    wait "${fs_pid}" || true
  fi

  run_as_user rm -f "${pid_file}"
  run_as_user rm -rf "${db_dir}"

  chown "${RUN_USER}" "${trace_file}" || true

  if ! grep -q "treedb-v1-bench" "${trace_file}"; then
    echo "Warning: no treedb-v1-bench lines in ${trace_file}" >&2
  fi
}

clean_data
for target in ${TRACE_TARGETS}; do
  run_trace "${target}"
done
clean_data

echo "fs_usage traces complete. Outputs in ${RESULTS_DIR}."
