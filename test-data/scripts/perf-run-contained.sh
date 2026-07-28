#!/usr/bin/env bash
# Run a perf/measurement command inside a memory-capped transient cgroup scope.
#
# Why (issue #3068, 2026-07-28): an UNCONTAINED cold scan of an 8.1 GB mmap'd
# Data.db on a swapless box hard-hung the host for 75 minutes. The kernel never
# OOM-killed anything -- with %commit at 105% and no swap it livelocked in
# reclaim with every task in D state. A memory cap makes the *offending process*
# die instead of the machine.
#
# Usage: perf-run-contained.sh [--mem 8G] [--swap 2G] -- <cmd> [args...]
set -euo pipefail

MEM="8G"; SWAP="2G"
while [ $# -gt 0 ]; do
  case "$1" in
    --mem)  MEM="$2"; shift 2 ;;
    --swap) SWAP="$2"; shift 2 ;;
    --)     shift; break ;;
    *)      echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
[ $# -gt 0 ] || { echo "usage: $0 [--mem 8G] [--swap 2G] -- <cmd>" >&2; exit 2; }

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
