#!/usr/bin/env bash
set -euo pipefail

# Bisect helper for TreeDB iavl-bench (WAL disabled).
# Usage:
#   THRESH=10.8 RUNS=2 ./bisect_nowal.sh
#
# Returns:
#   0 = good (fast enough)
#   1 = bad (regression)
#   125 = skip (setup failed)

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNS="${RUNS:-2}"
THRESH="${THRESH:-10.8}"
TARGET_VERSION="${TARGET_VERSION:-20}"
PROFILE="${PROFILE:-0}"
STAT="${STAT:-median}"

DBDIR="$ROOT/data-treedb-v1-bisect"
OUTDIR="$ROOT/results-profile/bisect-tmp"

clean_db() {
  rm -rf "$DBDIR"
}

clean_all() {
  rm -rf "$DBDIR" "$OUTDIR"
}

build() {
  (cd "$ROOT/treedb-v1" && go build -o treedb-v1-bench .)
}

run_once() {
  mkdir -p "$OUTDIR"
  if [ "$PROFILE" = "1" ]; then
    /usr/bin/time -p env TREEDB_BENCH_DISABLE_WAL=1 TREEDB_BENCH_DISABLE_BG=1 \
      "$ROOT/treedb-v1/treedb-v1-bench" bench \
        --db-dir "$DBDIR" \
        --changeset-dir "$ROOT/changesets" \
        --log-file "$OUTDIR/treedb-v1.jsonl" \
        --log-type json \
        --target-version "$TARGET_VERSION" \
        --mutexprofile "$OUTDIR/mutex.prof" \
        --blockprofile "$OUTDIR/block.prof" \
        --cpuprofile "$OUTDIR/cpu.prof" 2> "$OUTDIR/time.txt"
  else
    /usr/bin/time -p env TREEDB_BENCH_DISABLE_WAL=1 TREEDB_BENCH_DISABLE_BG=1 \
      "$ROOT/treedb-v1/treedb-v1-bench" bench \
        --db-dir "$DBDIR" \
        --changeset-dir "$ROOT/changesets" \
        --log-file "$OUTDIR/treedb-v1.jsonl" \
        --log-type json \
        --target-version "$TARGET_VERSION" 2> "$OUTDIR/time.txt"
  fi
  awk '/^real /{print $2}' "$OUTDIR/time.txt"
}

main() {
  clean_all
  build || { clean_all; exit 125; }

  mkdir -p "$OUTDIR"
  : > "$OUTDIR/times.txt"
  for i in $(seq 1 "$RUNS"); do
    clean_db
    t=$(run_once) || { clean_all; exit 125; }
    echo "$t" >> "$OUTDIR/times.txt"
  done

  stat=$(python3 - "$OUTDIR/times.txt" "$STAT" <<'PY'
import sys
from statistics import median, mean
path = sys.argv[1]
mode = sys.argv[2]
vals = []
with open(path, "r") as fh:
    for line in fh:
        line = line.strip()
        if line:
            vals.append(float(line))
if not vals:
    print("nan")
    sys.exit(0)
if mode == "mean":
    print(mean(vals))
else:
    print(median(vals))
PY
  )

  echo "${STAT}=${stat}s (threshold=${THRESH}s)"

  clean_all

  python3 - "$stat" "$THRESH" <<'PY'
import sys
val=float(sys.argv[1]); thresh=float(sys.argv[2])
sys.exit(0 if val <= thresh else 1)
PY
}

main "$@"
