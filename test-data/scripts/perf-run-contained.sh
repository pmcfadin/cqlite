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
  echo "  optional K/M/G/T[i] suffix, or a percentage (>0%, <=100%)." >&2
  echo "  The cap must be FINITE: 'max' and 'infinity' (any case) are REFUSED --" >&2
  echo "  a 'contained' run with no cap is not contained. --mem may not be zero;" >&2
  echo "  --swap 0 IS allowed and means 'no swap'." >&2
  echo "  A SUFFIXLESS number is a BYTE count to systemd ('8' = 8 bytes), so a" >&2
  echo "  suffixless value below 1 MiB is refused and a larger one prints the" >&2
  echo "  byte reading it resolved to." >&2
  echo "  --check-args validates and prints the resolved caps, running nothing." >&2
}

# Smallest suffixless byte count accepted: below this a "cap" is useless and is
# almost certainly a missing suffix (the `--mem 8` footgun).
MIN_SUFFIXLESS_BYTES=1048576

# Classify a systemd MemoryMax/MemorySwapMax value. Sets VAL_KIND
# (bytes|percent|unbounded), VAL_NUM (numeric part), and for bytes VAL_UNIT
# (k|m|g|t or empty) + VAL_BYTES (resolved byte count). Returns 1 on a syntax
# error. Case-insensitive, like systemd.
classify_mem_value() {
  local low
  VAL_KIND=""; VAL_NUM=""; VAL_UNIT=""; VAL_BYTES=""
  low="$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')"
  case "$low" in
    max|infinity) VAL_KIND=unbounded; return 0 ;;
  esac
  if [[ "$low" =~ ^([0-9]+(\.[0-9]+)?)%$ ]]; then
    VAL_KIND=percent; VAL_NUM="${BASH_REMATCH[1]}"; return 0
  fi
  if [[ "$low" =~ ^([0-9]+(\.[0-9]+)?)(([kmgt])i?)?b?$ ]]; then
    VAL_KIND=bytes; VAL_NUM="${BASH_REMATCH[1]}"; VAL_UNIT="${BASH_REMATCH[4]}"
    VAL_BYTES=$(awk -v n="${BASH_REMATCH[1]}" -v u="${BASH_REMATCH[4]}" 'BEGIN{
      m = 1
      if (u == "k") m = 1024
      else if (u == "m") m = 1024 * 1024
      else if (u == "g") m = 1024 * 1024 * 1024
      else if (u == "t") m = 1024 * 1024 * 1024 * 1024
      printf "%.0f", n * m }')
    return 0
  fi
  return 1
}

# A malformed OR UNBOUNDED value must be rejected HERE: systemd-run would
# otherwise reject a typo *after* sudo, and it would happily ACCEPT
# MemoryMax=max -- i.e. run the "contained" workload with no cap at all, which
# is the exact state that livelocked a swapless host for 75 minutes (#3068).
validate_cap() {  # $1 = --mem|--swap, $2 = value
  local flag="$1" val="$2"
  classify_mem_value "$val" || { echo "invalid $flag value: '$val'" >&2; usage; exit 2; }
  case "$VAL_KIND" in
    unbounded)
      echo "invalid $flag value: '$val' -- an unbounded cap is refused." >&2
      echo "  'max'/'infinity' DISABLE the limit, so the run would not be contained:" >&2
      echo "  an uncontained multi-GB read livelocked a swapless host for 75 min (#3068)." >&2
      echo "  Pass a finite, nonzero cap instead, e.g. $flag 8G." >&2
      exit 2 ;;
    percent)
      # >0% for --mem (a 0% cap is nonsense); >=0% for --swap; never over 100%
      # of physical RAM, which is effectively no cap at all.
      local strict=0 lower_bound="at least 0%"
      if [ "$flag" = "--mem" ]; then strict=1; lower_bound="greater than 0%"; fi
      awk -v p="$VAL_NUM" -v strict="$strict" 'BEGIN{
        exit !((strict ? p > 0 : p >= 0) && p <= 100) }' || {
        echo "invalid $flag value: '$val' -- a percentage cap must be" >&2
        echo "  $lower_bound and at most 100% of physical RAM." >&2
        exit 2
      } ;;
    bytes)
      if [ "$VAL_BYTES" = "0" ]; then
        # `--swap 0` is the legitimate "no swap at all" cap; `--mem 0` is not.
        [ "$flag" = "--swap" ] && return 0
        echo "invalid --mem value: '$val' -- a zero memory cap is refused" >&2
        echo "  (it would kill the workload immediately, not contain it)." >&2
        exit 2
      fi
      if [ -z "$VAL_UNIT" ] && [ "$VAL_BYTES" -lt "$MIN_SUFFIXLESS_BYTES" ]; then
        echo "invalid $flag value: '$val' -- systemd reads a SUFFIXLESS number as" >&2
        echo "  BYTES, so this is a ${VAL_BYTES}-byte cap (< 1 MiB): every run would" >&2
        echo "  look like an instant OOM and hide the real result. Add a suffix" >&2
        echo "  (e.g. $flag ${val}G) or pass an explicit byte count >= 1 MiB." >&2
        exit 2
      fi
      if [ -z "$VAL_UNIT" ]; then
        awk -v f="$flag" -v v="$val" -v b="$VAL_BYTES" 'BEGIN{
          printf "note: %s %s has no suffix; systemd reads it as BYTES = %s B (%.2f GiB).\n",
                 f, v, b, b / (1024 * 1024 * 1024) }' >&2
      fi ;;
  esac
}

MEM="8G"; SWAP="2G"
while [ $# -gt 0 ]; do
  case "$1" in
    --mem|--swap)
      [ $# -ge 2 ] || { echo "$1 requires a value" >&2; usage; exit 2; }
      validate_cap "$1" "$2"
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
