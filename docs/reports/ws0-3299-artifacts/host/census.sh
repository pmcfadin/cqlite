#!/usr/bin/env bash
# #3299 Step 1 — PMU census on THIS box, with a POSITIVE CONTROL.
#
# WHY THIS EXISTS. #3217 was lost to the silent-instrument class: on a virtualized
# host `cache-references` did not report `<not supported>` — it programmed cleanly,
# reported `100.00%` enabled, and returned a hard 0. A hard 0 from a working counter
# and a hard 0 from an absent one are TEXTUALLY IDENTICAL, so "is it non-zero?" is
# not a census. This script therefore drives each event over a workload whose memory
# behaviour is KNOWN BEFORE it is measured (#3224's `cache-hostile`, pointer-chasing
# a 2 GiB buffer many times the LLC through a serial dependency, so essentially every
# load is an LLC miss), and classifies each event against that prediction:
#
#   REAL          value > 0 at 100.00% enabled                — instrument works
#   HARD-ZERO     value == 0 at 100.00% enabled, on a workload
#                 that CANNOT have zero of this quantity      — INSTRUMENT UNAVAILABLE
#   NOT-SUPPORTED perf printed <not supported>                — instrument unavailable
#   NOT-COUNTED   perf printed <not counted>                  — instrument unavailable
#   UNKNOWN-EVENT perf refused the event name (exit != 0)     — instrument unavailable
#   MULTIPLEXED   enabled < 100.00%                           — scaled estimate, not a count
#
# "HARD-ZERO" is the whole point: it is reported as UNAVAILABLE, never published as a
# measurement of zero. A prior session's census ran on a DIFFERENT instance and is not
# transferable evidence, which is why this re-runs here.
#
# Hermetic w.r.t. the repo: reads only #3224's committed cache-hostile.c and writes
# only into this directory.
set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../../../.." && pwd)"
SRC="$REPO/docs/reports/ws0-3224-artifacts/cache-hostile.c"
WORK="${WS0_3299_CENSUS_WORK:-/data/ws0-3299/census}"
OUT="$HERE"

[[ -r "$SRC" ]] || { echo "FATAL: missing $SRC" >&2; exit 2; }
mkdir -p "$WORK"
BIN="$WORK/cache-hostile"
cc -O2 -std=c99 -pthread -o "$BIN" "$SRC"

# The census workload. `--delay-ms 0` deliberately counts the WHOLE process (buffer
# init + permutation build + chase): for a census the question is only "does this
# instrument move at all", and including init makes the LLC-miss prediction stronger,
# not weaker (2 GiB of first-touch page faults miss every level of cache).
HOSTILE=("$BIN" chase --buffer-mib 2048 --accesses 20000000 --delay-ms 0 --arm census)

# Every event spelling the issue names, in the order it names them.
EVENTS=(
  instructions cycles
  L1-dcache-loads L1-dcache-load-misses
  LLC-loads LLC-load-misses
  cache-references cache-misses
  mem_load_retired.l3_miss mem_load_retired.l3_hit
  longest_lat_cache.miss longest_lat_cache.reference
  r4f2e r412e
)
# Events for which a zero on THIS workload is impossible, so a hard zero proves the
# instrument is unavailable rather than the quantity absent. (r412e/r4f2e are the raw
# encodings of longest_lat_cache.reference/.miss on Intel.)
declare -A ZERO_IMPOSSIBLE=(
  [instructions]=1 [cycles]=1 [L1-dcache-loads]=1 [L1-dcache-load-misses]=1
  [LLC-loads]=1 [LLC-load-misses]=1 [cache-references]=1 [cache-misses]=1
  [mem_load_retired.l3_miss]=1 [mem_load_retired.l3_hit]=1
  [longest_lat_cache.miss]=1 [longest_lat_cache.reference]=1
  [r4f2e]=1 [r412e]=1
)

exec > >(tee "$OUT/pmu-census.txt") 2>&1
echo "==== #3299 PMU CENSUS ===="
echo "date_utc:        $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "host:            $(hostname)"
echo "instance_id:     $(curl -s --max-time 2 -H "X-aws-ec2-metadata-token: $(curl -s --max-time 2 -X PUT http://169.254.169.254/latest/api/token -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' || true)" http://169.254.169.254/latest/meta-data/instance-id 2>/dev/null || echo UNAVAILABLE)"
echo "kernel:          $(uname -r)"
echo "perf_version:    $(perf --version 2>&1 | head -1)"
echo "paranoid:        $(cat /proc/sys/kernel/perf_event_paranoid)"
echo "kptr_restrict:   $(cat /proc/sys/kernel/kptr_restrict 2>/dev/null || echo UNAVAILABLE)"
echo "nmi_watchdog:    $(cat /proc/sys/kernel/nmi_watchdog 2>/dev/null || echo UNAVAILABLE)"
echo "nproc_logical:   $(nproc)"
echo "cache_hostile:   $SRC (2 GiB pointer chase, 20M serial-dependent accesses)"
echo
printf '%-32s %-14s %20s %10s  %s\n' EVENT VERDICT VALUE ENABLED% RAW
printf '%s\n' "--------------------------------------------------------------------------------------------------"

for ev in "${EVENTS[@]}"; do
  csv="$WORK/ev-${ev//[^A-Za-z0-9]/_}.csv"
  rc=0
  perf stat -x, -e "$ev" -o "$csv" -- "${HOSTILE[@]}" >/dev/null 2>"$csv.err" || rc=$?
  raw=""
  if [[ -s "$csv" ]]; then
    raw="$(grep -v '^#' "$csv" | grep -v '^[[:space:]]*$' | head -1 || true)"
  fi
  val="$(cut -d, -f1 <<<"$raw")"
  # `perf stat -x, -o <file> -e EV` row layout:
  #   1=value 2=unit 3=event 4=run_time_ns 5=pct_running
  pct="$(cut -d, -f5 <<<"$raw")"
  verdict=UNCLASSIFIED
  if (( rc != 0 )) && [[ -z "$raw" ]]; then
    verdict=UNKNOWN-EVENT; raw="perf rc=$rc: $(head -2 "$csv.err" | tr '\n' ' ')"
  elif [[ "$raw" == *"<not supported>"* ]]; then
    verdict=NOT-SUPPORTED
  elif [[ "$raw" == *"<not counted>"* ]]; then
    verdict=NOT-COUNTED
  elif [[ ! "$val" =~ ^[0-9]+$ ]]; then
    verdict=UNPARSEABLE
  elif [[ -n "$pct" ]] && awk -v p="$pct" 'BEGIN{exit !(p+0 < 99.99)}'; then
    verdict=MULTIPLEXED
  elif (( val == 0 )); then
    verdict=$([[ -n "${ZERO_IMPOSSIBLE[$ev]:-}" ]] && echo HARD-ZERO || echo ZERO)
  else
    verdict=REAL
  fi
  printf '%-32s %-14s %20s %10s  %s\n' "$ev" "$verdict" "${val:-?}" "${pct:-?}" "$raw"
done

echo
cat <<'NOTE'
LEGEND — the distinction this census exists to draw:
  REAL          the instrument counted. Usable.
  HARD-ZERO     the instrument programmed cleanly, reported 100.00% enabled, and
                returned 0 on a workload that CANNOT have zero of this quantity.
                This is an UNAVAILABLE INSTRUMENT, not a measurement of zero, and it
                is the exact silent-instrument failure #3217 published and #3224
                catalogued. It MUST NOT be reported as a measured value.
  NOT-SUPPORTED / NOT-COUNTED / UNKNOWN-EVENT / UNPARSEABLE / MULTIPLEXED
                unavailable, or an estimate rather than a count. Not usable.
NOTE
