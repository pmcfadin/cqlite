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
#   REAL          value > 0 at EXACTLY 100.00% enabled, after
#                 the control workload exited 0               — instrument works
#   HARD-ZERO     value == 0 at 100.00% enabled, on a workload
#                 that CANNOT have zero of this quantity      — INSTRUMENT UNAVAILABLE
#   NOT-SUPPORTED perf printed <not supported>                — instrument unavailable
#   NOT-COUNTED   perf printed <not counted>                  — instrument unavailable
#   UNKNOWN-EVENT perf refused the event name (exit != 0,
#                 no counter row at all)                      — instrument unavailable
#   CONTROL-FAILED perf/the workload exited != 0 THOUGH a row
#                 was written                                 — VERDICT NOT ESTABLISHED
#   NO-ENABLED-PCT the enabled% field is absent or unparseable — VERDICT NOT ESTABLISHED
#   MULTIPLEXED   enabled < 100.00%                           — scaled estimate, not a count
#   ENABLED-IMPLAUSIBLE enabled > 100.00%                     — VERDICT NOT ESTABLISHED
#
# The rule itself lives in `classify-event.sh` (sourced below), so the harness
# self-test can drive every branch without perf. A `REAL` verdict requires three
# AFFIRMATIVE facts — control workload exited 0, enabled% present and parseable,
# enabled% EXACTLY 100.00 — and that file records why each one is required.
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

# The verdict rule and its zero-impossible table live in ONE sourceable file, so
# `../harness/selftest.sh` can drive every branch — including the ones a healthy
# box never produces — without perf, a 2 GiB buffer or root.
# shellcheck source=classify-event.sh
. "$HERE/classify-event.sh"

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

undecided=""
for ev in "${EVENTS[@]}"; do
  csv="$WORK/ev-${ev//[^A-Za-z0-9]/_}.csv"
  rc=0
  perf stat -x, -e "$ev" -o "$csv" -- "${HOSTILE[@]}" >/dev/null 2>"$csv.err" || rc=$?
  raw=""
  if [[ -s "$csv" ]]; then
    raw="$(grep -v '^#' "$csv" | grep -v '^[[:space:]]*$' | head -1 || true)"
  fi
  # `perf stat -x, -o <file> -e EV` row layout:
  #   1=value 2=unit 3=event 4=run_time_ns 5=pct_running
  val="$(cut -d, -f1 <<<"$raw")"
  pct="$(cut -d, -f5 <<<"$raw")"
  verdict="$(classify_event "$ev" "$rc" "$raw")"
  # perf's stderr is the only record of WHY a run that produced no usable verdict
  # failed, so it is carried into the printed row for those two verdicts.
  case "$verdict" in
    UNKNOWN-EVENT)  raw="perf rc=$rc: $(head -2 "$csv.err" | tr '\n' ' ')" ;;
    CONTROL-FAILED) raw="perf/workload rc=$rc: $(head -2 "$csv.err" | tr '\n' ' ') | row: $raw" ;;
  esac
  # A verdict that is not a statement about the INSTRUMENT means this run could
  # not say — it must not be read as a finding, and a census that produced any of
  # them did not complete.
  verdict_established "$verdict" || undecided="$undecided $ev=$verdict"
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
  CONTROL-FAILED / NO-ENABLED-PCT / ENABLED-IMPLAUSIBLE
                NO VERDICT WAS ESTABLISHED. The control workload exited non-zero
                (so the memory behaviour this census classifies against was never
                driven), or the enabled% — the only field separating a count from
                a scaled estimate — was absent, unparseable, or not exactly
                100.00. These are NOT "unavailable instrument" findings and they
                are NOT usable: they mean this run cannot say. Re-run; if it
                persists, the census itself is broken and nothing here may be
                cited about that event.
NOTE

# THE CENSUS REPORTS ITS OWN COMPLETENESS, and a run that could not decide an
# event EXITS NON-ZERO. Printing "no verdict established" in a column and then
# exiting 0 is the silent-success shape this whole script exists to refuse: the
# operator's next step is to cite these verdicts in `guards.py`'s forbidden-event
# list, and a census that could not say must not be read as one that did.
if [[ -n "$undecided" ]]; then
  echo
  echo "CENSUS INCOMPLETE — no verdict was established for:$undecided"
  echo "Nothing may be cited about those events from this run. Re-run; if it persists, the"
  echo "census itself is broken (a failed control workload, or perf reporting no enabled%)."
  exit 1
fi
echo
echo "CENSUS COMPLETE — every event produced a verdict about the instrument."
