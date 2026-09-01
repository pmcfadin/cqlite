#!/usr/bin/env bash
# record-scan.sh — take ONE perf observation of a WARM, PINNED, steady-state bare scan
# (issue #3445).
#
# It does not measure anything itself. Its whole job is to place a `perf` window entirely
# inside the #3299 scan worker's post-barrier steady state, so that no page-cache
# first-touch, no ingestion/schema setup and no process teardown lands in the samples.
#
# The worker (`docs/reports/ws0-3299-artifacts/harness/scan-worker`) is REUSED rather than
# reimplemented: it already drives `Database::execute_streaming` over the pinned corpus with
# `ws0_corpus_gen::scan_scope::verify_exact_scope`, already treats a 0-row pass as a failure
# rather than a measurement, and already prewarms before signalling ready. This script adds
# only the perf window and the pinning.
#
# WARM is structural here, not a convention: the worker writes `ready-0` only after
# `--prewarm-passes` full untimed passes, and this script does not start `perf` until it has
# seen that file AND released the barrier AND waited out `--settle`.
#
# Usage:
#   record-scan.sh --out DIR --binary PATH [--event EV] [--period N] [--secs N]
#                  [--cpu N] [--settle N] [--stat-events LIST]
#
# Two modes, selected by --mode:
#   record  perf record (sampling) -> perf.data, for annotate/srcline attribution (AC1)
#   stat    perf stat (counting)   -> counters.csv with pct_running, for AC2
set -euo pipefail

CORPUS=/data/ws0-3096
OUT=; BINARY=; EVENT=cycles; PERIOD=500009; SECS=40; CPU=2; SETTLE=5; MODE=record
# Quiescence bound. A rep is publishable only if the box was quiet across the WHOLE rep,
# not merely at its start, so the load is SAMPLED THROUGHOUT and the maximum is checked.
# 0 disables the check (and says so in the verdict file) -- it never silently disables it.
MAX_LOAD=3.0; LOAD_SAMPLE_SECS=5
STAT_EVENTS='cycles,instructions,cycle_activity.stalls_total,cycle_activity.stalls_l1d_miss,idq_uops_not_delivered.core,int_misc.recovery_cycles'
while [ $# -gt 0 ]; do
  case "$1" in
    --out) OUT=$2; shift 2;;
    --binary) BINARY=$2; shift 2;;
    --event) EVENT=$2; shift 2;;
    --period) PERIOD=$2; shift 2;;
    --secs) SECS=$2; shift 2;;
    --cpu) CPU=$2; shift 2;;
    --settle) SETTLE=$2; shift 2;;
    --mode) MODE=$2; shift 2;;
    --stat-events) STAT_EVENTS=$2; shift 2;;
    --max-load) MAX_LOAD=$2; shift 2;;
    --load-sample-secs) LOAD_SAMPLE_SECS=$2; shift 2;;
    --corpus) CORPUS=$2; shift 2;;
    *) echo "record-scan.sh: unknown argument: $1" >&2; exit 2;;
  esac
done
[ -n "$OUT" ] && [ -n "$BINARY" ] || { echo "record-scan.sh: --out and --binary are required" >&2; exit 2; }
# A malformed bound must REFUSE, never take the "disabled" branch: `awk 'm+0==0'` treats
# `--max-load abc` as 0 and so as "check off", i.e. a typo would silently buy a pass.
# Must contain at least one DIGIT: the character-class test alone accepts "." and "..",
# which awk then reads as 0, silently taking the "check disabled" branch and reporting
# UNCHECKED for what is actually a malformed bound. A malformed bound must REFUSE.
# SWEEP (a), roborev r6: ONE validator, used by every numeric argument. `--period`, `--secs`,
# `--cpu` and `--settle` had NO validation at all -- `--secs abc` reached `sleep abc`, and a
# negative `--period` reached perf. Sharing one function means a new numeric argument cannot be
# added without validation, which is the standard the --stat-events derivation set.
require_uint() {  # name value [min]
  local n=$1 v=$2 min=${3:-0}
  case "$v" in
    ''|*[!0-9]*) echo "record-scan.sh: $n must be a non-negative integer: '$v'" >&2; exit 2;;
  esac
  if [ "$v" -lt "$min" ]; then
    echo "record-scan.sh: $n must be >= $min: '$v'" >&2; exit 2
  fi
}
require_uint --period "$PERIOD" 1
require_uint --secs "$SECS" 1
require_uint --settle "$SETTLE" 0
require_uint --cpu "$CPU" 0
if [ "$CPU" -ge "$(nproc)" ]; then
  echo "record-scan.sh: --cpu ($CPU) is not a cpu on this box (nproc=$(nproc))" >&2; exit 2
fi

case "$MAX_LOAD" in
  ''|*[!0-9.]*|*.*.*) echo "record-scan.sh: --max-load must be a number (0 disables): '$MAX_LOAD'" >&2; exit 2;;
  *[0-9]*) ;;
  *) echo "record-scan.sh: --max-load must contain a digit (0 disables): '$MAX_LOAD'" >&2; exit 2;;
esac
case "$LOAD_SAMPLE_SECS" in
  ''|*[!0-9]*|0) echo "record-scan.sh: --load-sample-secs must be a positive integer" >&2; exit 2;;
esac
# MODE is validated against a CLOSED set here, not tested for equality at each use. The
# measurement branched on `= record` while the pct_running validation branched on
# `= stat`, so `--mode stats` would run `perf stat` and SKIP the multiplexing check --
# a typo silently disabling a validity guard.
case "$MODE" in
  record|stat) ;;
  *) echo "record-scan.sh: --mode must be 'record' or 'stat': '$MODE'" >&2; exit 2;;
esac
# The sampling interval must be short enough to observe the INTERIOR of the window. At
# --load-sample-secs >= --secs the sampler produces only a start observation plus the closing
# one, which is precisely the endpoint-only read the across-window sampler exists to replace --
# and it would still have satisfied a bare "at least 2 samples" rule. Require room for at least
# 3 (start + >=1 interior + close), i.e. an interval strictly under half the duration.
if [ $((LOAD_SAMPLE_SECS * 2)) -ge "$SECS" ]; then
  echo "record-scan.sh: --load-sample-secs ($LOAD_SAMPLE_SECS) must be under half of --secs" \
       "($SECS) so the window's INTERIOR is sampled, not just its endpoints" >&2
  exit 2
fi
[ -x "$BINARY" ] || { echo "record-scan.sh: not executable: $BINARY" >&2; exit 2; }
[ -d "$CORPUS/ws0/events" ] || { echo "record-scan.sh: no corpus at $CORPUS/ws0/events" >&2; exit 2; }

mkdir -p "$OUT"
RUNDIR=$(mktemp -d /tmp/ws0-3445-rep.XXXXXX)
# The sampler is a background child, so it is reaped here too: an early exit must not
# leave a loadavg loop running on a metered box after the rep it belonged to is gone.
cleanup() {
  touch "$RUNDIR/stop" 2>/dev/null || true; sleep 0.5
  kill "${WPID:-}" 2>/dev/null || true
  kill "${SAMPLER:-}" 2>/dev/null || true
}
trap cleanup EXIT

# Co-tenancy is RECORDED, never assumed away: other lanes share this box, and a rep taken
# beside a peer's gate is a rep whose validity has to be judged, not hidden.
#
# WHY A BEFORE/AFTER PAIR IS NOT ENOUGH. The gate semaphore
# (CQLITE_GATE_MAX_CONCURRENCY=1) serialises GATE against GATE; a perf run holds no slot,
# so a peer's gate can start, run and finish entirely INSIDE this rep's window while both
# endpoint samples look quiet. loadavg is also a decaying average, so its value at t=0
# describes the minute BEFORE the rep. Hence a sampler across the whole window, and a
# verdict taken from the MAXIMUM rather than from either endpoint.
{ echo "loadavg_before=$(cut -d' ' -f1-3 /proc/loadavg)"
  echo "nproc=$(nproc)"
  echo "peer_cargo_or_gate_procs=$(pgrep -c -f 'cargo|agent-gate' || :)"
} > "$OUT/cotenancy-before.txt"

# WORKER DEADLINE, DERIVED (roborev r6 finding 1). It was a fixed 900 s while the measurement
# is SETTLE + SECS, so a long configuration could let the worker hit its own deadline and exit
# SUCCESSFULLY in the middle of the perf window -- perf and this script would both still exit 0
# and a partly idle observation would publish as a valid rep. Derived with generous headroom for
# the untimed prewarm pass, which is not bounded by SECS.
WORKER_MAX_SECS=$(( SETTLE + SECS + 1800 ))
taskset -c "$CPU" "$BINARY" \
  --corpus "$CORPUS" --rundir "$RUNDIR" --worker-id 0 \
  --prewarm-passes 1 --max-secs "$WORKER_MAX_SECS" --progress-ms 250 \
  > "$OUT/worker.stdout" 2> "$OUT/worker.stderr" &
WPID=$!

# Wait for the worker's own ready signal. Its absence is a FAILURE, never a short window:
# starting perf without it would put ingestion + the cold first pass inside the samples.
for _ in $(seq 1 1800); do [ -f "$RUNDIR/ready-0" ] && break; sleep 1
  kill -0 "$WPID" 2>/dev/null || { echo "record-scan.sh: worker died before ready" >&2; cat "$OUT/worker.stderr" >&2; exit 1; }
done
[ -f "$RUNDIR/ready-0" ] || { echo "record-scan.sh: worker never signalled ready" >&2; exit 1; }

touch "$RUNDIR/go"
sleep "$SETTLE"          # steady state, after the barrier release transient

# Affinity is READ BACK from the kernel rather than trusted to taskset's argument -- and
# the read is REQUIRED to have worked, because the validity ledger asserts "pinned (kernel
# read-back)" for every rep. An empty file would make that column an unbacked claim.
if ! tr -d '\0' < "/proc/$WPID/status" | grep -E 'Cpus_allowed_list' > "$OUT/affinity-observed.txt"; then
  echo "record-scan.sh: could not read back affinity for pid $WPID" >&2
  exit 1
fi
if ! grep -qE "Cpus_allowed_list:[[:space:]]*${CPU}\$" "$OUT/affinity-observed.txt"; then
  echo "record-scan.sh: affinity read-back does not equal the requested cpu ${CPU}:" >&2
  cat "$OUT/affinity-observed.txt" >&2
  exit 1
fi

# --- load sampler across the measured window ------------------------------------
: > "$OUT/load-samples.txt"
( while :; do
    # `pgrep -c` PRINTS 0 and exits non-zero when nothing matches, so `|| echo 0` appended a
    # SECOND line -- corrupting load-samples.txt and inflating the sample count, which let a
    # one-sample run pass the `quiescence-unmeasured` refusal that requires two. `|| :`
    # swallows the exit status only. It fires exactly when the box is QUIET, i.e. in the
    # condition this check exists to certify.
    NPEER=$(pgrep -c -f 'cargo|agent-gate|maturin|rustc' || :)
    echo "$(date -u +%H:%M:%S) $(cut -d' ' -f1 /proc/loadavg) ${NPEER:-0}"
    sleep "$LOAD_SAMPLE_SECS"
  done ) >> "$OUT/load-samples.txt" 2>/dev/null &
SAMPLER=$!
stop_sampler() { kill "$SAMPLER" 2>/dev/null || true; }

# Window boundaries read from the SAME clock source the worker timestamps with (that is what
# the worker's --print-monotonic-ns flag exists for), so the span check below compares like
# with like rather than two unrelated clocks.
T0=$("$BINARY" --print-monotonic-ns 2>/dev/null || echo 0)
PERF_RC=0
if [ "$MODE" = record ]; then
  # `|| true` here would report success for a perf that never recorded anything.
  perf record -e "$EVENT" -c "$PERIOD" -p "$WPID" -o "$OUT/perf.data" \
    -- sleep "$SECS" > "$OUT/perf-record.log" 2>&1 || PERF_RC=$?
else
  # -x, gives the machine-readable form whose 5th field is pct_running: the validity rule
  # is checked from THAT field, not from the absence of a warning in the human-readable form.
  # `perf stat -x,` writes its CSV to STDERR, not stdout: sending stdout to counters.csv
  # yields an EMPTY counters file and a log that happens to hold the data, which is how a
  # validity check ends up reading nothing and reporting nothing wrong. Capture stderr.
  perf stat -x, -e "$STAT_EVENTS" -p "$WPID" \
    -- sleep "$SECS" 2> "$OUT/counters.csv" > "$OUT/perf-stat.stdout" || PERF_RC=$?
fi

# FINDING (roborev r3): the sampler was killed WITHOUT a final sample and `loadavg_after` was
# excluded from the peak, so a contention spike in the last sampling interval was invisible --
# undercutting the very "max across the WHOLE window, not endpoints" property this check exists
# to establish. Take one synchronous, well-formed sample AFTER the measurement completes and
# BEFORE stopping the sampler, so the closing interval is inside the adjudicated set.
T1=$("$BINARY" --print-monotonic-ns 2>/dev/null || echo 0)
NPEER_END=$(pgrep -c -f 'cargo|agent-gate|maturin|rustc' || :)
echo "$(date -u +%H:%M:%S) $(cut -d' ' -f1 /proc/loadavg) ${NPEER_END:-0}" >> "$OUT/load-samples.txt"
stop_sampler
{ echo "loadavg_after=$(cut -d' ' -f1-3 /proc/loadavg)"
  echo "peer_cargo_or_gate_procs=$(pgrep -c -f 'cargo|agent-gate' || :)"
} > "$OUT/cotenancy-after.txt"

# --- quiescence verdict: REFUSE loudly, never silently re-roll -------------------
# The verdict is written to a file in the rep directory whatever it says, so a REFUSED
# rep leaves a durable record that can be reported as a refusal. A rep quietly re-rolled
# until it looked clean is the worse outcome (#3299 AC5), so this script does not retry.
# Both figures count only WELL-FORMED sample lines (3 fields), so a malformed line can
# neither hide a peak nor inflate the sample count past the unmeasured-quiescence refusal.
PEAK=$(awk 'NF==3 { if ($2+0 > m) m = $2+0 } END { printf "%.2f", m }' "$OUT/load-samples.txt")
# COVERAGE, NOT A COUNT (roborev r6 finding 1 / SWEEP (c)).
#
# A count cannot distinguish "sampled throughout the window" from "sampled at the start and once
# at the end" -- which is the ONE distinction this sampler exists to make. The previous floor was
# half the expected count, so with defaults a sampler recording at 0, 5 and 10 s and then DYING
# still passed once the synchronous closing sample landed at 40 s, leaving three quarters of the
# window unobserved. Raising the number would not have fixed it; the predicate was wrong.
#
# So the verdict is taken on the observed TIMESTAMPS: the samples must SPAN the window and no
# GAP between consecutive samples may exceed a small multiple of the requested interval. That is
# the property "the box was quiet across the whole rep" actually requires.
#
# Timestamps are HH:MM:SS UTC from the sampler; converted to seconds, with midnight rollover
# handled by adding a day to any negative delta.
COV=$(awk 'NF==3 {
             split($1, t, ":"); s = t[1]*3600 + t[2]*60 + t[3]
             if (n > 0) { d = s - prev; if (d < 0) d += 86400; if (d > maxgap) maxgap = d; span += d }
             prev = s; n++
           }
           END { printf "%d %d %d", n+0, span+0, maxgap+0 }' "$OUT/load-samples.txt")
NSAMP=$(printf '%s' "$COV" | cut -d' ' -f1)
SPAN=$(printf '%s' "$COV" | cut -d' ' -f2)
MAXGAP=$(printf '%s' "$COV" | cut -d' ' -f3)
# The window the samples must cover is the measurement itself. Allow the span to fall short by
# one interval (the first sample lands after the loop's first read) and a gap of up to 3
# intervals (scheduling under load), but no more.
MIN_SPAN=$(( SECS - LOAD_SAMPLE_SECS - 1 ))
if [ "$MIN_SPAN" -lt 1 ]; then MIN_SPAN=1; fi
MAX_ALLOWED_GAP=$(( LOAD_SAMPLE_SECS * 3 ))
if [ "$NSAMP" -lt 3 ]; then
  printf 'verdict=REFUSED\nreason=quiescence-undersampled\nsamples=%s\npeak_load=%s\nmax_load=%s\n' \
    "$NSAMP" "$PEAK" "$MAX_LOAD" > "$OUT/quiescence-verdict.txt"
elif [ "$SPAN" -lt "$MIN_SPAN" ] || [ "$MAXGAP" -gt "$MAX_ALLOWED_GAP" ]; then
  printf 'verdict=REFUSED\nreason=quiescence-coverage-gap\nsamples=%s\nspan_secs=%s\nmin_span_secs=%s\nmax_gap_secs=%s\nallowed_gap_secs=%s\npeak_load=%s\nmax_load=%s\n' \
    "$NSAMP" "$SPAN" "$MIN_SPAN" "$MAXGAP" "$MAX_ALLOWED_GAP" "$PEAK" "$MAX_LOAD" > "$OUT/quiescence-verdict.txt"
elif [ "$(awk -v p="$PEAK" -v m="$MAX_LOAD" 'BEGIN{print (m+0==0) ? "off" : ((p+0>m+0)?"bad":"ok")}')" = bad ]; then
  printf 'verdict=REFUSED\nreason=box-not-quiet-across-rep\nsamples=%s\npeak_load=%s\nmax_load=%s\n' \
    "$NSAMP" "$PEAK" "$MAX_LOAD" > "$OUT/quiescence-verdict.txt"
elif [ "$(awk -v m="$MAX_LOAD" 'BEGIN{print (m+0==0)?"off":"on"}')" = off ]; then
  printf 'verdict=UNCHECKED\nreason=max-load-check-disabled\nsamples=%s\npeak_load=%s\n' \
    "$NSAMP" "$PEAK" > "$OUT/quiescence-verdict.txt"
else
  printf 'verdict=OK\nsamples=%s\nspan_secs=%s\nmax_gap_secs=%s\npeak_load=%s\nmax_load=%s\n' \
    "$NSAMP" "$SPAN" "$MAXGAP" "$PEAK" "$MAX_LOAD" > "$OUT/quiescence-verdict.txt"
fi

touch "$RUNDIR/stop"
# The worker's OWN verdict is load-bearing and must not be discarded: it exits non-zero on
# a zero-row pass, which is the "0 rows is a failure, never a measurement" rule this rig
# inherits. `wait || true` threw that away, so a worker that died 3 s into a 40 s window
# left `perf record -- sleep 40` exiting 0 and the rep looking valid.
WORKER_RC=0
wait "$WPID" || WORKER_RC=$?
# Its summary is REQUIRED, not optional: the validity ledger's "rows measured" column has
# no mechanical backing without it.
# NOTE ON `rows_total` IN THE COPIED SUMMARY (roborev r3 finding): it counts rows from BARRIER
# RELEASE, which includes the `--settle` interval that precedes the perf window. It is therefore
# "total post-barrier rows", NOT "rows measured during the perf window", and nothing here claims
# otherwise. Deriving the windowed count would mean intersecting the worker's progress records
# with the perf window boundaries; until that exists, the field is LABELLED rather than
# reinterpreted -- an unsupported column is the same "claims a control it does not have" class
# this rig has already had to correct twice.
if ! cp "$RUNDIR/worker-0.summary.json" "$OUT/worker-summary.json" 2>/dev/null; then
  echo "record-scan.sh: worker wrote no summary — refusing to call this a rep" >&2
  if [ "$WORKER_RC" -eq 0 ]; then WORKER_RC=1; fi
fi
trap - EXIT; rm -rf "$RUNDIR"

# --- the rep is only a rep if EVERY verdict says so ------------------------------
# BLOCKER (review): previously the quiescence verdict was WRITTEN and then ignored, the
# script printed "rep written" and exited 0, so a caller checking $? published a rep taken
# at peak load 18. A recorded refusal that changes nothing observable is not a refusal.
# LOST SAMPLES, ENFORCED (roborev r6 finding 2). `perf record` exiting 0 does not establish
# that the ring buffer never overflowed, and buffer loss biases attribution silently. The
# published reps' "0 lost samples" was an AD-HOC read taken during the validity audit, not a
# control this harness implemented -- so it is implemented now, and the value is required to be
# explicitly zero rather than merely absent from the output.
LOST=
if [ "$MODE" = record ] && [ -f "$OUT/perf.data" ]; then
  LOST=$(perf report -i "$OUT/perf.data" --stdio 2>/dev/null \
           | sed -n 's/^# Total Lost Samples: *\([0-9]*\).*/\1/p' | head -1)
  printf 'lost_samples=%s\n' "${LOST:-UNREADABLE}" > "$OUT/lost-samples.txt"
fi

QV=$(sed -n 's/^verdict=//p' "$OUT/quiescence-verdict.txt" 2>/dev/null)
# NB: written as `if` blocks, not `[ x ] && y`. Under `set -e` a bare `[ test ] && assign`
# whose test is FALSE returns non-zero as a statement and kills the script -- so the
# healthy path would have exited here.
FAIL=
if [ "$WORKER_RC" -ne 0 ]; then FAIL="$FAIL worker-exit=$WORKER_RC"; fi
if [ "$PERF_RC" -ne 0 ]; then FAIL="$FAIL perf-exit=$PERF_RC"; fi
# Explicitly zero, or refuse. An unreadable count is NOT zero.
if [ "$MODE" = record ]; then
  case "$LOST" in
    0) ;;
    '') FAIL="$FAIL lost-samples=unreadable" ;;
    *)  FAIL="$FAIL lost-samples=$LOST" ;;
  esac
fi
# F1's real property: the worker must have been in steady state across the WHOLE window, which
# is a fact about the worker, not a bound on the arguments. Its own summary records the interval.
if [ -f "$OUT/worker-summary.json" ] && [ "${T0:-0}" -gt 0 ] && [ "${T1:-0}" -gt 0 ]; then
  WS=$(sed -n 's/.*"t_start_ns":\([0-9]*\).*/\1/p' "$OUT/worker-summary.json" | head -1)
  WE=$(sed -n 's/.*"t_end_ns":\([0-9]*\).*/\1/p' "$OUT/worker-summary.json" | head -1)
  if [ -n "$WS" ] && [ -n "$WE" ]; then
    if [ "$WS" -gt "$T0" ] || [ "$WE" -lt "$T1" ]; then
      FAIL="$FAIL worker-did-not-span-window(worker=[$WS,$WE] window=[$T0,$T1])"
    fi
    printf 'worker_start_ns=%s\nworker_end_ns=%s\nwindow_start_ns=%s\nwindow_end_ns=%s\n' \
      "$WS" "$WE" "$T0" "$T1" > "$OUT/window-span.txt"
  else
    FAIL="$FAIL worker-window-unmeasured"
  fi
fi
# pct_running is the issue's own validity rule, so it is CHECKED here rather than left for
# a human to eyeball in the CSV. Field 5 of `perf stat -x,`; any event below 100.00 means
# the counters were multiplexed and the rep is not publishable.
# COUNTER VALIDATION. This guards a claim the report actually leans on -- "all counting reps at
# 100.00% pct_running" is cited as a reason to trust data taken on a contended box -- so it is
# validated event by event rather than by a spot check.
#
# The previous version failed OPEN in four separate ways (roborev r4): an EMPTY counters.csv
# skipped validation entirely; a MISSING event was never noticed; a zero or `<not supported>` /
# `<not counted>` count passed; and because it only counted rows whose field 5 was below 100, a
# file with ONE good row and any number of unparseable ones passed. Same shape as the false zero:
# the observed values were fine, and the guard could not have told us if they were not.
if [ "$MODE" = stat ]; then
  if [ ! -s "$OUT/counters.csv" ]; then
    FAIL="$FAIL counters=absent-or-empty"
  else
    # Every requested event must be PRESENT. Derived from the request, not a hard-coded list, so
    # changing --stat-events cannot silently shrink what is checked.
    MISSING=
    for ev in $(printf '%s' "$STAT_EVENTS" | tr ',' ' '); do
      if ! awk -F, -v e="$ev" 'NF>=3 && $3==e { found=1 } END { exit !found }' "$OUT/counters.csv"; then
        MISSING="$MISSING$ev,"
      fi
    done
    [ -n "$MISSING" ] && FAIL="$FAIL counters-missing-events=${MISSING%,}"
    # Every row: a finite POSITIVE count in field 1, and pct_running EXACTLY 100.00 in field 5.
    # `<not supported>` / `<not counted>` are non-numeric and so fail the count test.
    BAD=$(awk -F, '
      NF < 5 { bad++; next }
      $1 !~ /^[0-9]+(\.[0-9]+)?$/ { bad++; next }       # non-numeric or <not supported>
      $1+0 <= 0                    { bad++; next }       # a zero count measured nothing
      $5 !~ /^[0-9]+\.[0-9]+$/     { bad++; next }       # pct_running not numeric
      $5+0 != 100                  { bad++; next }       # multiplexed
      { ok++ }
      END { printf "%d/%d", bad+0, bad+ok+0 }' "$OUT/counters.csv")
    BADN=${BAD%%/*}
    if [ "${BAD#*/}" -eq 0 ]; then
      FAIL="$FAIL counters=no-parsable-rows"
    elif [ "$BADN" -ne 0 ]; then
      FAIL="$FAIL counters-invalid-rows=$BAD"
    fi
  fi
fi
case "$QV" in
  OK|UNCHECKED) ;;
  '') FAIL="$FAIL quiescence-verdict=absent" ;;
  *)  FAIL="$FAIL quiescence=$QV" ;;
esac
if [ -n "$FAIL" ]; then
  echo "record-scan.sh: REP REFUSED —$FAIL (artifacts left in $OUT for the record)" >&2
  exit 3
fi
echo "record-scan.sh: rep written to $OUT"
