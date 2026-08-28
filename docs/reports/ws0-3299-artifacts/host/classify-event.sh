#!/usr/bin/env bash
# #3299 — the PMU census's event CLASSIFIER, in one sourceable place.
#
# WHY IT IS ITS OWN FILE. This rule decides whether an instrument may be used at
# all, and it is the rule #3299 round 7 found could certify a counter off its own
# short failure path. A rule that decides that must be TESTABLE without perf, a
# 2 GiB buffer or root — so it is sourced by `census.sh` (which drives real
# hardware) and by `../harness/selftest.sh` (which drives synthetic `perf stat`
# rows through every branch, including the ones no healthy box produces). One
# implementation, exercised two ways; a second copy would only be knowable to
# agree with this one by differential testing (CLAUDE.md, #3283).
#
# Sourced, never executed: it defines a function and a table and runs nothing.

# Events for which a zero on the census workload (#3224's `cache-hostile`: a 2 GiB
# pointer chase, essentially every load an LLC miss) is IMPOSSIBLE, so a hard zero
# proves the instrument unavailable rather than the quantity absent. (r412e/r4f2e
# are the raw encodings of longest_lat_cache.reference/.miss on Intel.) It lives
# beside the classifier because it IS part of the classification rule.
declare -A ZERO_IMPOSSIBLE=(
  [instructions]=1 [cycles]=1 [L1-dcache-loads]=1 [L1-dcache-load-misses]=1
  [LLC-loads]=1 [LLC-load-misses]=1 [cache-references]=1 [cache-misses]=1
  [mem_load_retired.l3_miss]=1 [mem_load_retired.l3_hit]=1
  [longest_lat_cache.miss]=1 [longest_lat_cache.reference]=1
  [r4f2e]=1 [r412e]=1
)

# classify_event <event> <perf-exit-status> <perf-csv-row> -> echoes ONE verdict.
#
# A `REAL` VERDICT REQUIRES THREE AFFIRMATIVE FACTS (#3299 round 7): the control
# workload exited 0, the enabled% field is present and parseable, and it reads
# EXACTLY 100.00. None of the three used to be required — a nonzero exit was
# ignored whenever a CSV row existed, and an ABSENT enabled% skipped the
# multiplexing test and fell through to REAL — so the census could mark an event
# usable on the strength of its own failure path. Every missing precondition now
# has its OWN verdict naming what could not be established, because "the run
# failed" and "the counter is dead" are different facts and only the second one
# is a finding about the hardware.
classify_event() {
  local ev="$1" rc="$2" raw="$3" val pct
  # `perf stat -x, -o <file> -e EV` row layout:
  #   1=value 2=unit 3=event 4=run_time_ns 5=pct_running
  val="$(cut -d, -f1 <<<"$raw")"
  pct="$(cut -d, -f5 <<<"$raw")"
  local verdict=UNCLASSIFIED
  if (( rc != 0 )) && [[ -z "$raw" ]]; then
    verdict=UNKNOWN-EVENT
  elif [[ "$raw" == *"<not supported>"* ]]; then
    # Ahead of the exit-status check DELIBERATELY: perf refusing to program the
    # event is a fact about the INSTRUMENT and does not depend on the workload
    # having completed. Both verdicts are non-REAL, so this ordering cannot
    # certify anything — it only reports the more specific of two true things.
    verdict=NOT-SUPPORTED
  elif [[ "$raw" == *"<not counted>"* ]]; then
    verdict=NOT-COUNTED
  elif (( rc != 0 )); then
    # THE POSITIVE CONTROL DID NOT COMPLETE, so the memory behaviour this census
    # classifies against was never driven. A counter row written by a run that
    # died — the 2 GiB allocation refused, the chase killed part-way — describes
    # some other, unknown workload, and a value from it is evidence about the
    # instrument in neither direction. This branch is what stops a short failure
    # path reaching `REAL`.
    verdict=CONTROL-FAILED
  elif [[ ! "$val" =~ ^[0-9]+$ ]]; then
    verdict=UNPARSEABLE
  elif [[ ! "$pct" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
    # An ABSENT or unparseable enabled% used to skip the multiplexing test and
    # land on REAL: the one field that separates a count from a scaled estimate
    # was allowed to be missing, and its absence read as compliance. The regex
    # is what keeps `nan`/`inf`/`-1` out of the numeric comparisons below, where
    # they would parse to 0 or compare false against every bound.
    verdict=NO-ENABLED-PCT
  elif awk -v p="$pct" 'BEGIN{exit !(p+0 < 100)}'; then
    verdict=MULTIPLEXED
  elif ! awk -v p="$pct" 'BEGIN{exit !(p+0 == 100)}'; then
    # Above 100.00% is not a stricter pass, it is a field this script does not
    # understand. The gate to REAL is the AFFIRMATIVE `== 100`, never "not less
    # than 100" — an unplanned value must not inherit the permissive branch.
    verdict=ENABLED-IMPLAUSIBLE
  elif (( val == 0 )); then
    verdict=$([[ -n "${ZERO_IMPOSSIBLE[$ev]:-}" ]] && echo HARD-ZERO || echo ZERO)
  else
    verdict=REAL
  fi
  echo "$verdict"
}

# verdict_established <verdict> -> exit 0 if the verdict is a STATEMENT ABOUT THE
# INSTRUMENT, 1 if this run simply could not say.
#
# Keyed on the AFFIRMATIVE set and matched EXACTLY, never on "is it one of the bad
# ones": an unplanned or misspelled verdict must inherit the fail-closed branch,
# not the permissive one, and a prefix match would accept `REALLY` as `REAL`.
# `CONTROL-FAILED`, `NO-ENABLED-PCT`, `ENABLED-IMPLAUSIBLE` and `UNPARSEABLE` all
# mean the RUN failed, which is not a finding about the hardware and may not be
# cited as one.
verdict_established() {
  case "$1" in
    REAL|HARD-ZERO|ZERO|NOT-SUPPORTED|NOT-COUNTED|UNKNOWN-EVENT|MULTIPLEXED) return 0 ;;
    *) return 1 ;;
  esac
}
