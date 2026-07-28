#!/usr/bin/env bash
# Run a perf/measurement command inside a memory-capped transient cgroup scope.
#
# Why (issue #3068, 2026-07-28): an UNCONTAINED cold scan of an 8.0 GiB
# (8,620,456,540 B) mmap'd Data.db on a swapless box hard-hung the host for 75
# minutes. The kernel never OOM-killed anything -- with %commit at 105% and no
# swap it livelocked in direct reclaim, load 62.7, every task (sshd included) in
# D state. A memory cap makes the *offending process* die instead of the machine.
#
# WHEN to use it: any measurement that reads a multi-GB corpus (cold scan, warm
# scan, sstable-to-parquet export, a Flight scan against the #3068 perf corpus).
# See docs/development/perf-corpus-and-containment.md.
#
# Usage: perf-run-contained.sh [--mem 8G] [--swap 2G] -- <cmd> [args...]
#        perf-run-contained.sh --check-args ...   # validate only, run nothing
set -euo pipefail

CHECK_ARGS=0

usage() {
  echo "usage: $0 [--mem 8G] [--swap 2G] -- <cmd> [args...]" >&2
  echo "  --mem/--swap take a systemd memory value: a byte count with an" >&2
  echo "  optional K/M/G/T[i] suffix, a percentage, 'max' or 'infinity'." >&2
  echo "  --check-args validates and prints the resolved caps, running nothing." >&2
}

# systemd MemoryMax/MemorySwapMax syntax. A malformed value must be rejected
# HERE: systemd-run would otherwise reject it *after* sudo, or (worse) a typo
# like "8" could be read as 8 bytes and make every run look like an OOM.
valid_mem_value() {
  [[ "$1" =~ ^[0-9]+(\.[0-9]+)?([KMGTkmgt]i?)?B?$ || "$1" =~ ^[0-9]+(\.[0-9]+)?%$ \
     || "$1" == "max" || "$1" == "infinity" ]]
}

MEM="8G"; SWAP="2G"
while [ $# -gt 0 ]; do
  case "$1" in
    --mem|--swap)
      [ $# -ge 2 ] || { echo "$1 requires a value" >&2; usage; exit 2; }
      valid_mem_value "$2" || {
        echo "invalid $1 value: '$2'" >&2; usage; exit 2
      }
      if [ "$1" = "--mem" ]; then MEM="$2"; else SWAP="$2"; fi
      shift 2 ;;
    # Argument-validation self-test hook (scripts/tests/test_perf_run_contained.sh):
    # parse + validate, print the resolved caps, execute NOTHING.
    --check-args) CHECK_ARGS=1; shift ;;
    -h|--help) usage; exit 0 ;;
    --)     shift; break ;;
    *)      echo "unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done
[ $# -gt 0 ] || { echo "missing command after --" >&2; usage; exit 2; }

if [ "$CHECK_ARGS" = 1 ]; then
  echo "ARGS-OK mem=${MEM} swap=${SWAP} cmd=$*"
  exit 0
fi

# Refuse to run when the system is already over-committed -- that is the exact
# precondition that wedged the box.
commit_pct="$(awk '/CommitLimit:/{cl=$2} /Committed_AS:/{ca=$2} END{ if (cl>0) printf "%d", ca*100/cl; else print 0 }' /proc/meminfo)"
if [ "${commit_pct}" -ge 95 ]; then
  echo "REFUSING: Committed_AS is ${commit_pct}% of CommitLimit (>=95%). Free memory first." >&2
  exit 3
fi

echo "[contained] MemoryMax=${MEM} MemorySwapMax=${SWAP} commit=${commit_pct}% :: $*" >&2
exec sudo -n systemd-run --scope --collect --quiet \
  --uid="$(id -u)" --gid="$(id -g)" \
  -p MemoryMax="${MEM}" -p MemorySwapMax="${SWAP}" -p OOMPolicy=kill \
  --working-directory="$PWD" \
  -- "$@"
