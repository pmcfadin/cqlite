#!/usr/bin/env bash
# ws0-baseline.sh — the committed, clean-checkout-runnable Arrow-encode
# measurement rig for CQLite issue #3096 (spec R1/R2).
#
# Measures BOTH arms in ONE session, over the SAME bytes, on the SAME verified
# physical-core sibling pair:
#
#   arm A  bare scan      cqlite_core::Database::execute_streaming  (ws0-scan-bench)
#   arm B  Flight do_get  the real gRPC RPC over loopback           (cqlite-flight + flight-loadgen)
#
# and reports each as rows/s AND cycles/row, warm and cold as SEPARATE claims,
# median of N with the observed spread, setup subtracted from the cycles/row
# denominator, and the row denominator printed beside every figure.
#
# ---------------------------------------------------------------------------
# The traps this rig is built around (spec R2) — do not "simplify" these away
# ---------------------------------------------------------------------------
#
#  1. CPU-WIDE COUNTERS ONLY. Every measurement goes through the single
#     `perf_stat_c` wrapper, which counts CPU-wide. Per-process counting measured
#     >2x observer cost on this workload and appears NOWHERE in this rig. Three
#     layers enforce that (see scripts/perf/lib-perf-lint.sh): an ALLOWLIST over
#     this file's source (perf is invoked in ONE place, everything else must be
#     marked), a per-TOKEN option check, and a RUNTIME argv check in the wrapper.
#     It stopped being a deny-list grep because five ordinary bash spellings
#     bypassed two successive versions of one.
#  2. VERIFIED SIBLING PINNING. The pinned pair is read from
#     `thread_siblings_list` and the run FAILS CLOSED if it is not one physical
#     core's siblings (`lib-cpu.sh`). Never assumed from CPU numbers.
#  3. WARM AND COLD ARE SEPARATE CLAIMS. Never averaged together. Cold does
#     `sync; echo 3 > /proc/sys/vm/drop_caches` before EVERY rep — and, since
#     the drop happens once per REP while `--scan-passes N` runs N passes inside
#     ONE bench process, a cold run with N>1 is REFUSED below rather than
#     reporting pass 1 (cold) blended with passes 2..N (warm) as one "cold"
#     number. Symmetrically, EVERY warm rep of BOTH arms runs an untimed prewarm
#     before its perf window (`prewarm_status`, recorded per rep in results.json)
#     so a "warm" figure is never a partly-cold one. Arm A fails closed on a
#     prewarm failure; arm B records and continues — see the bias argument at each.
#  4. SETUP IS SUBTRACTED, AND SAID SO. Arm A runs `--setup-only` under its own
#     `perf stat` and the driver reports `(cycles_total - cycles_setup) / rows`.
#     Arm B starts and prewarms the server BEFORE the perf window opens, so its
#     setup is outside the window by construction.
#  5. ZERO ROWS IS A FAILURE. Any rep that observes zero rows exits non-zero
#     rather than reporting a measurement.
#  6. NEVER A CPU-SHARE CLAIM. This rig emits rows/s and cycles/row only.
#
# Usage:
#   scripts/perf/ws0-baseline.sh --corpus /data/ws0-3096 [options]
#
# Generate the corpus first:
#   cargo run --release -p ws0-corpus-gen --bin ws0-corpus-gen -- --out /data/ws0-3096
#
# Full method, caveats and the recorded pinning: docs/reports/ws0-3096-artifacts/measurement-method.md

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
# shellcheck source=scripts/perf/lib-cpu.sh
source "$HERE/lib-cpu.sh"
# shellcheck source=scripts/perf/lib-perf-lint.sh
source "$HERE/lib-perf-lint.sh"
# shellcheck source=scripts/perf/lib-host-state.sh
source "$HERE/lib-host-state.sh"

CORPUS=""
SERVER_CPUS="2,10"
CLIENT_CPUS="4,12,5,13,6,14,7,15"
REPS=3
TEMPS="warm cold"
ARMS="bypass"
STEP_DURATION="45s"
# A COLD rep must contain exactly ONE full scan. The load generator stops issuing
# at its deadline but lets the in-flight request finish, so a short step yields a
# single request — while a long cold step would run one COLD scan followed by
# WARM ones and average them into a single "cold" number, which is precisely the
# blending spec R2 forbids.
COLD_STEP_DURATION="1s"
SCAN_PASSES=1
PORT=18815
OUT_DIR=""
DO_BUILD=1
EVENTS="cycles,instructions"

usage() {
  cat <<EOF
ws0-baseline.sh — issue #3096 same-session Arrow-encode baseline

  --corpus DIR         Corpus root from ws0-corpus-gen (holds ws0/events/). REQUIRED.
  --server-cpus LIST   Pinned physical-core sibling pair for BOTH arms (default $SERVER_CPUS).
  --client-cpus LIST   CPUs for the Flight load generator; must not overlap (default $CLIENT_CPUS).
  --reps N             Reps per (arm, temperature). Median reported, spread printed (default $REPS).
  --temp WHICH         warm | cold | both (default both).
  --arm WHICH          bypass | merge | both (default bypass).
  --step-duration D    Flight loadgen step hold for WARM reps (default $STEP_DURATION).
  --cold-step-duration D
                       Step hold for COLD reps (default $COLD_STEP_DURATION). Deliberately
                       short: the loadgen finishes its in-flight request, so this yields
                       exactly ONE cold scan. A long cold step would blend one cold scan
                       with warm ones inside a single "cold" claim, so a value above
                       5000ms is REFUSED when --temp includes cold — and ws0_report.py
                       independently rejects any cold rep whose observed requests_ok != 1.
  --scan-passes N      Timed passes per bare-scan rep (default $SCAN_PASSES). REFUSED with
                       N>1 when --temp includes cold: caches are dropped once per rep, so
                       pass 1 would be cold and passes 2..N warm, blended into one "cold"
                       number (spec R2/AC5 forbids blending).
  --port N             Loopback port for the Flight server (default $PORT).
  --out DIR            Results dir (default \$REPO/target/perf-ws0-3096/<timestamp>).
  --no-build           Skip the release build; use the binaries already in target/release.
  -h, --help           This text.

Physical-core sibling pairs on this box:
$(list_sibling_pairs)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --corpus) CORPUS="$2"; shift 2 ;;
    --server-cpus) SERVER_CPUS="$2"; shift 2 ;;
    --client-cpus) CLIENT_CPUS="$2"; shift 2 ;;
    --reps) REPS="$2"; shift 2 ;;
    --temp)
      case "$2" in
        warm) TEMPS="warm" ;;
        cold) TEMPS="cold" ;;
        both) TEMPS="warm cold" ;;
        *) echo "FATAL: --temp must be warm|cold|both" >&2; exit 2 ;;
      esac; shift 2 ;;
    --arm)
      case "$2" in
        bypass) ARMS="bypass" ;;
        merge) ARMS="merge" ;;
        both) ARMS="bypass merge" ;;
        *) echo "FATAL: --arm must be bypass|merge|both" >&2; exit 2 ;;
      esac; shift 2 ;;
    --step-duration) STEP_DURATION="$2"; shift 2 ;;
    --cold-step-duration) COLD_STEP_DURATION="$2"; shift 2 ;;
    --scan-passes) SCAN_PASSES="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --out) OUT_DIR="$2"; shift 2 ;;
    --no-build) DO_BUILD=0; shift ;;
    -h|--help) usage; exit 0 ;;
    # Every unrecognized argument is an ERROR, never ignored: a typo'd flag that
    # is silently dropped produces a measurement of something other than what
    # was asked for, and nothing in the output would say so.
    *) echo "FATAL: unrecognized argument '$1'" >&2; usage >&2; exit 2 ;;
  esac
done

# --- trap 1: this file contains NO per-process perf invocation ----------------
# Runs unconditionally at startup, over THIS FILE, so an edit that reaches for the
# per-process option — or that invokes perf anywhere other than the single
# `perf_stat_c` wrapper — cannot run at all. The mechanism, the three bypasses review
# round 1 found in its predecessor, and why it is an ALLOWLIST rather than a deny-list
# grep: scripts/perf/lib-perf-lint.sh. Its LAYER 3 (the runtime argv check) lives in
# `perf_stat_c` below, because only the wrapper sees the argv.
_perf_lint_out="$(perf_invocation_lint "${BASH_SOURCE[0]}")"
if [[ -n "$_perf_lint_out" ]]; then
  echo "FATAL: this script contains a per-process perf invocation, or invokes perf" >&2  # perf-lint-allow
  echo "       outside its single wrapper:" >&2
  printf '       %s\n' "$_perf_lint_out" >&2
  echo "       Per-process counting (${_PP_SHORT} / ${_PP_LONG}) measured >2x observer cost on" >&2
  echo "       this workload; CPU-wide counting is mandatory (issue #3096 spec R2), and" >&2
  echo "       every invocation must go through perf_stat_c so ONE place enforces it." >&2  # perf-lint-allow
  exit 2
fi
unset _perf_lint_out

# --- trap 3 enforcement: COLD is ONE pass, or it is not a cold claim ----------
# `--scan-passes N` runs N timed passes INSIDE ONE ws0-scan-bench process, and
# `drop_caches_if_cold` runs ONCE per rep — before that process starts. So at
# N>1 the reported "cold" figure is pass 1 (genuinely cold) folded together with
# passes 2..N (already warm, reading the page cache pass 1 just filled): a
# BLENDED number presented as a separate claim, which is exactly what spec
# R2/AC5 forbids ("warm and cold SHALL be reported as SEPARATE claims, never
# blended"). Dropping caches BETWEEN passes is not available to us — the passes
# run inside the bench process, which is unprivileged by design — so the only
# honest options are one pass per cold rep or no cold measurement, and this
# fails closed on the blend rather than reporting it (issue #3096 review).
#
# The WARM arm is unaffected: N>1 there is a legitimate way to amortize process
# start, and every pass is warm by construction.
# Every numeric option is validated POSITIVE, up front, before any build, cache drop
# or measurement (issue #3272, finding 5). `--reps` had no validation at all: at
# `--reps 0` every `for rep in $(seq 1 0)` loop body was skipped, so the driver ran
# to completion having measured NOTHING and handed the reporter an empty session —
# which (pre-fix) also exited zero. A non-numeric `--reps abc` made `seq` emit
# nothing and print its own diagnostic into the middle of a "successful" run. The
# vacuous-green class: a run that measured nothing must not look like one that did.
require_positive_int() { # require_positive_int <flag> <value> [max]
  local flag="$1" value="$2" max="${3:-}"
  case "$value" in
    ''|*[!0-9]*)
      echo "FATAL: --$flag must be a positive integer (got '$value')" >&2; exit 2 ;;
  esac
  # DIGIT COUNT first, before any arithmetic — bash arithmetic is signed 64-bit and
  # WRAPS SILENTLY, so a 20-digit value becomes an arbitrary (possibly small,
  # possibly negative) number and the range checks below would be comparing
  # something other than what the caller wrote. Measured: `99999999999999999999`
  # evaluates to 7766279631452241919. 9 digits is past any legitimate rep count or
  # port and cannot wrap.
  if [[ "${#value}" -gt 9 ]]; then
    echo "FATAL: --$flag is absurdly large (got '$value', ${#value} digits)." >&2
    echo "       Refused before arithmetic: bash arithmetic wraps at 64 bits, so a" >&2
    echo "       value this size would be range-checked as some other number." >&2
    exit 2
  fi
  # 10# for the same reason parse_duration_ms uses it: `08` is not octal here.
  if (( 10#$value < 1 )); then
    echo "FATAL: --$flag must be at least 1 (got '$value')." >&2
    echo "       A run with --$flag below 1 measures nothing, and a report over" >&2
    echo "       nothing is not a smaller version of the requested claim — it is a" >&2
    echo "       vacuous success (issue #3272)." >&2
    exit 2
  fi
  if [[ -n "$max" ]] && (( 10#$value > max )); then
    echo "FATAL: --$flag must be at most $max (got '$value')" >&2
    exit 2
  fi
}
require_positive_int scan-passes "$SCAN_PASSES"
require_positive_int reps "$REPS"
require_positive_int port "$PORT" 65535

# --- trap 3 enforcement, arm B: a COLD rep is ONE request, or it is not cold ---
# `--cold-step-duration` is the one option a caller can raise to silently turn a
# cold rep into a blended one (issue #3096 review, finding 2). The loadgen holds a
# step for the given duration, starting a NEW request whenever the previous one
# finishes before the deadline; only the FIRST request after the cache drop is
# cold, so requests 2..N contribute WARM rows to a figure reported as "cold".
#
# Two independent guards, because neither alone is sufficient:
#   (a) HERE, up front: reject a cold step long enough to admit a second request
#       on this corpus, before any build, cache drop or measurement happens. The
#       ceiling is 5s — 4x below the 20.2s cold full-corpus scan this rig's
#       recorded session measured — so a single in-flight request is structural.
#   (b) In ws0_report.py: require the OBSERVED `requests_ok` of every cold rep to
#       be exactly 1, whatever the duration was. That is the ground truth, and it
#       holds on a corpus whose scan is faster than any duration ceiling could
#       anticipate. A ceiling alone would be a guess; the observed count is not.
COLD_STEP_MAX_MS=5000

# parse_duration_ms <value> — echo milliseconds, non-zero on a malformed value.
# Accepts the loadgen's `<n>ms` / `<n>s` / `<n>m` forms only: a bare `45` is
# REJECTED rather than guessed at, since guessing seconds-vs-millis would silently
# measure a step 1000x from the one requested.
#
# EVERY component enters arithmetic as `10#$n` — DECIMAL, explicitly (issue #3272,
# finding 7). Bash's `$((...))` reads a leading-zero literal as OCTAL, which made
# this function silently wrong for a whole class of ordinary spellings:
#
#   `010s`     -> $((010 * 1000))   = 8000 ms.  A caller asking for 10s measured 8s.
#   `030ms`    -> $((030))          = 24 ms.
#   `08s`      -> a HARD bash error, "08: value too great for base (error token is
#                 08)", which the `case` then reported as "must be <n>ms, <n>s or
#                 <n>m" — a complaint about the FORMAT of a value whose format is
#                 fine, sending the reader after the wrong thing.
#   `010000ms` -> 4096 ms, i.e. UNDER the 5000ms cold ceiling while really being
#                 10s — so the octal parse could smuggle a BLENDED cold step past
#                 the guard added for #3096 finding 2.
#
# The regex already restricts `$n` to digits, so `10#` cannot fail on a value that
# reaches it; it only fixes the base. Note the `*ms` case must stay FIRST — `*s`
# would otherwise match `45ms` and leave a trailing `m`.
#
# The DIGIT-LENGTH CAP closes the OTHER half of the same defect class, and it is a
# bypass of exactly the same shape as the octal one. Bash arithmetic is signed
# 64-bit and WRAPS SILENTLY, so a large-but-well-formed value multiplied by 1000 or
# 60000 lands on a small positive number:
#
#   `2305843009213693956s` -> $((… * 1000)) wraps to **4000** ms, UNDER the 5000ms
#   cold ceiling. A caller could therefore smuggle a blended cold step past the
#   guard of #3096 finding 2 with an absurd duration, exactly as `010000ms` could
#   via the octal parse. Verified against this driver before the fix: the value ran
#   straight through the ceiling to the corpus check.
#
# A duration is capped at 9 digits — ~11.5 days in ms, ~31 years in seconds, far
# past any legitimate step — which keeps the largest product (999999999 * 60000)
# near 6e13, four orders of magnitude inside 2^63. So the multiply cannot wrap and
# the ceiling comparison is on the number the caller actually wrote. REJECTED rather
# than clamped: a clamp would measure a step other than the one requested without
# saying so, which is the failure mode this whole function is being hardened against.
DURATION_MAX_DIGITS=9
parse_duration_ms() {
  local v="$1" n
  case "$v" in
    *ms) n="${v%ms}" ;;
    *s)  n="${v%s}" ;;
    *m)  n="${v%m}" ;;
    *)   return 1 ;;
  esac
  [[ "$n" =~ ^[0-9]+$ ]] || return 1
  # The DIGIT COUNT is compared before any arithmetic touches the value: a numeric
  # bound would itself be evaluated by the arithmetic that wraps.
  [[ "${#n}" -le "$DURATION_MAX_DIGITS" ]] || return 1
  case "$v" in
    *ms) echo "$((10#$n))" ;;
    *s)  echo "$((10#$n * 1000))" ;;
    *m)  echo "$((10#$n * 60000))" ;;
  esac
}

for _spec in "step-duration:$STEP_DURATION" "cold-step-duration:$COLD_STEP_DURATION"; do
  _name="${_spec%%:*}"; _val="${_spec#*:}"
  if ! _ms="$(parse_duration_ms "$_val")"; then
    echo "FATAL: --$_name must be <n>ms, <n>s or <n>m (got '$_val')" >&2
    echo "       A bare number is refused rather than guessed at: seconds-vs-millis" >&2
    echo "       would silently measure a step 1000x from the one requested." >&2
    exit 2
  fi
  if [[ "$_ms" -le 0 ]]; then
    echo "FATAL: --$_name must be greater than zero (got '$_val')" >&2
    exit 2
  fi
done

if [[ " $TEMPS " == *" cold "* ]]; then
  COLD_STEP_MS="$(parse_duration_ms "$COLD_STEP_DURATION")"
  if [[ "$COLD_STEP_MS" -gt "$COLD_STEP_MAX_MS" ]]; then
    echo "FATAL: --cold-step-duration $COLD_STEP_DURATION (${COLD_STEP_MS}ms) exceeds the" >&2
    echo "       ${COLD_STEP_MAX_MS}ms ceiling for a run that includes a COLD temperature." >&2
    echo "       The loadgen starts a NEW request whenever the previous one finishes before" >&2
    echo "       the step deadline, and only the FIRST request after the cache drop is cold." >&2
    echo "       A longer cold step therefore folds WARM requests into a figure reported as" >&2
    echo "       'cold' — the blending spec R2/AC5 forbids." >&2
    echo "       Use --cold-step-duration 1s (the default), or --temp warm to hold a step" >&2
    echo "       open for as long as you like. ws0_report.py independently REJECTS any cold" >&2
    echo "       rep whose observed requests_ok != 1, so this is a fast failure, not the" >&2
    echo "       only one." >&2
    exit 2
  fi
fi
if [[ " $TEMPS " == *" cold "* && "$SCAN_PASSES" -gt 1 ]]; then
  echo "FATAL: --temp ${TEMPS// /+} with --scan-passes $SCAN_PASSES would BLEND one cold pass" >&2
  echo "       with $((SCAN_PASSES - 1)) already-warm pass(es) into a single number reported as" >&2
  echo "       'cold'. Caches are dropped once per REP, before the bench process starts, and" >&2
  echo "       the bench sums rows and seconds over all its timed passes." >&2
  echo "       Spec R2/AC5: warm and cold are SEPARATE claims, never blended." >&2
  echo "       Use --scan-passes 1 for any run that includes a cold temperature, or" >&2
  echo "       --temp warm to keep multi-pass amortization." >&2
  exit 2
fi

[[ -n "$CORPUS" ]] || { echo "FATAL: --corpus is required" >&2; usage >&2; exit 2; }
CORPUS="$(cd "$CORPUS" && pwd)"
TABLE_DIR="$CORPUS/ws0/events"
if ! ls "$TABLE_DIR"/*-Data.db >/dev/null 2>&1; then
  echo "FATAL: $TABLE_DIR holds no *-Data.db." >&2
  echo "       Generate it: cargo run --release -p ws0-corpus-gen --bin ws0-corpus-gen -- --out $CORPUS" >&2
  exit 2
fi
TICKET_TEMPLATE="$CORPUS/ticket-template.json"

for tool in perf taskset python3; do  # perf-lint-allow: a presence PROBE (command -v), not an invocation
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: $tool is not installed" >&2; exit 2; }
done

# The sibling check must read the REAL host topology before it can vouch for
# anything (issue #3272, item 10). `lib-cpu.sh` exposes an injectable topology root
# so `scripts/tests/test_ws0_cpu_pinning_guards.sh` can prove the check REJECTS a
# non-sibling set without needing a particular CPU layout; that override would
# otherwise be a way to satisfy the pinning guarantee with a fabricated
# `thread_siblings_list`, so a measurement run refuses it here, before it measures.
assert_real_cpu_topology || exit 2
verify_sibling_pair "$SERVER_CPUS" "server"
verify_sibling_pair "$CLIENT_CPUS" "client" 2>/dev/null \
  || echo "client CPUs: $CLIENT_CPUS (a multi-core set — only the SERVER set must be one physical core)"
verify_disjoint "$SERVER_CPUS" "$CLIENT_CPUS"

# ---------------------------------------------------------------------------
# Server lifecycle — ONLY the process THIS script started (issue #3096 review)
# ---------------------------------------------------------------------------
# This rig used to open each Flight rep with `pkill -x cqlite-flight`, which kills
# EVERY matching process on the box — including a PEER LANE's Flight server on a
# shared fleet machine (one worker per machine is the convention, but the fleet
# runs concurrent gates, e2e tiers and loadgen lanes that start their own
# servers). Clearing the box to make room for a measurement is a destructive
# cross-lane action, and it is silent: the peer just dies.
#
# Instead: remember the PID we launched, kill only that, and treat an occupied
# port as a FAILURE to be reported rather than an obstacle to be removed.
SERVER_PID=""

# Host sysctl capture/mutate/restore lives in scripts/perf/lib-host-state.sh — the
# only part of this rig that changes state outside its own process tree. The
# driver composes `restore_sysctls` into its single `on_exit` handler below and
# calls `relax_perf_sysctls` once, before the results dir exists.


stop_server() {
  [[ -n "$SERVER_PID" ]] || return 0
  local pid="$SERVER_PID"
  SERVER_PID=""
  kill "$pid" 2>/dev/null || true
  local i
  for i in $(seq 1 20); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.5
  done
  kill -9 "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# Runs on EVERY exit path — success, a FATAL, or a Ctrl-C — so no rep can leave an
# orphaned server holding the port (which used to be what the next run's `pkill` was
# cleaning up) OR the host's perf hardening weakened (issue #3272, finding 3).
#
# ONE handler, ONE registration, deliberately. A second top-level `trap ... EXIT`
# would SILENTLY DISCARD this one — bash keeps a single handler per signal — so the
# server-stop and the sysctl restore are composed inside `on_exit` rather than
# registered separately. The signal list is explicit because `EXIT` does not fire on
# SIGINT/SIGTERM/SIGHUP while a foreground child (a `perf stat` leg) is running, and
# `exit` at the end of the handler is what makes the signal path terminate rather
# than resume.
on_exit() {
  local rc=$?
  trap - EXIT INT TERM HUP
  stop_server
  restore_sysctls
  exit "$rc"
}
trap on_exit EXIT INT TERM HUP

# Is $PORT free? Fail closed if not: an occupied port means either an orphan of
# ours (report it, do not silently reap something that might not be ours) or
# another lane's server (never ours to kill). `--port` is the remedy.
require_port_free() {
  local where="$1" i
  for i in $(seq 1 10); do
    (echo >"/dev/tcp/127.0.0.1/$PORT") >/dev/null 2>&1 || return 0
    sleep 1
  done
  echo "FATAL: 127.0.0.1:$PORT is already accepting connections ($where)." >&2
  echo "       This rig will NOT clear the box: a matching process may be another" >&2
  echo "       lane's Flight server on a shared machine, and killing it is a" >&2
  echo "       destructive cross-lane action (issue #3096 review)." >&2
  echo "       Pick a free port with --port N, or stop the listener yourself after" >&2
  echo "       confirming whose it is (e.g. 'ss -ltnp \"sport = :$PORT\"')." >&2
  exit 2
}

# Fail BEFORE the release build, not after it.
require_port_free "preflight"

# Weaken the host knobs CPU-wide counting needs — AFTER `trap on_exit` above, never
# before: the interval between the write and the trap being armed is the one window in
# which a signal could leave the host relaxed. Every knob is enrolled for restore
# BEFORE it is written, and a knob whose prior could not be read is not written at all
# (scripts/perf/lib-host-state.sh).
relax_perf_sysctls

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/target/perf-ws0-3096/$TS}"
mkdir -p "$OUT_DIR"
BIN="$REPO_ROOT/target/release"

if [[ "$DO_BUILD" == "1" ]]; then
  echo "building release binaries…"
  (cd "$REPO_ROOT" && cargo build --release -p ws0-corpus-gen -p cqlite-flight -p flight-loadgen) \
    > "$OUT_DIR/build.log" 2>&1 \
    || { echo "FATAL: release build failed — see $OUT_DIR/build.log" >&2; exit 2; }
fi
for b in ws0-scan-bench cqlite-flight flight-loadgen; do
  [[ -x "$BIN/$b" ]] || { echo "FATAL: $BIN/$b missing (drop --no-build, or build it)" >&2; exit 2; }
done

# The Flight ticket is derived from the DDL the corpus was WRITTEN with (the
# generator emits it beside the data), so both arms provably read one schema.
DDL_FILE="$CORPUS/ws0-events.cql"
[[ -r "$DDL_FILE" ]] || { echo "FATAL: $DDL_FILE missing — regenerate the corpus" >&2; exit 2; }
python3 - "$DDL_FILE" "$TICKET_TEMPLATE" <<'PY'
import json, sys
ddl = open(sys.argv[1]).read().strip().rstrip(';')
json.dump({"version": 2, "keyspace": "ws0", "table": "events", "ddl": ddl,
           "snapshot": None, "token_start": None, "token_end": None,
           "wraparound": False, "columns": None, "predicates": [],
           "filter": None, "aggregation": None, "limit": None},
          open(sys.argv[2], "w"), indent=1)
PY

drop_caches_if_cold() {
  [[ "$1" == "cold" ]] || return 0
  sync
  echo 3 | sudo -n tee /proc/sys/vm/drop_caches >/dev/null \
    || { echo "FATAL: cannot drop caches (sudo -n) — a 'cold' claim would be a lie" >&2; exit 2; }
}

# THE ONLY PLACE THIS FILE INVOKES perf. CPU-WIDE (`-C <cpu-list>`), never
# per-process (trap 1). The source-level allowlist above is anchored on exactly that
# fact: every other line mentioning a perf/stat WORD must be marked, so a new
# invocation elsewhere cannot run however it is spelled.
#
# LAYER 3 of the same guard, at RUNTIME on the argv (#3272 review B4). By the time a
# value reaches here bash has done word-splitting and QUOTE REMOVAL, so `-p'1234'`,
# `-p1234`, `-p "$pid"` and `--pid=1234` are all just tokens — the spelling problem a
# source-text deny-list can never close does not exist at this layer. It catches what
# no scan of this file could see: a caller passing a COMPUTED option, or one built by
# an `eval`.
perf_stat_c() {
  local outfile="$1"; shift
  local a
  for a in "$@"; do
    case "$a" in
      "$_PP_SHORT"|"$_PP_SHORT"*|"$_PP_LONG"|"$_PP_LONG"*)
        echo "FATAL: perf_stat_c was passed the per-process option '$a'." >&2
        echo "       Per-process counting measured >2x observer cost on this workload;" >&2
        echo "       this wrapper counts CPU-WIDE only (issue #3096 spec R2). The" >&2
        echo "       argument list was: $*" >&2
        exit 2 ;;
    esac
  done
  perf stat -x, -e "$EVENTS" -C "$SERVER_CPUS" -o "$outfile" -- "$@"
}

# ---------------------------------------------------------------------------
# Arm A — the bare scan
# ---------------------------------------------------------------------------
measure_scan() {
  local temp="$1" rep="$2" tag="scan-$temp-$rep"
  drop_caches_if_cold "$temp"

  # --- untimed PREWARM (warm arm only) -----------------------------------------
  # A full scan OUTSIDE every perf window, before the measured legs, so the warm
  # arm measures warm work (issue #3096 review, finding 1).
  #
  # Why this is not optional. `--setup-only` opens the corpus and ingests the
  # schema; it does NOT read the `Data.db` pages the scan streams. So on a
  # genuinely cold page cache — a fresh box, or a `--temp cold` rep earlier in the
  # same session having dropped the caches — the FIRST "warm" rep faulted those
  # pages in from disk and was measured partly cold. At `--reps 1` that partly-cold
  # rep IS the reported median, and nothing in the output said so: the warm/cold
  # separation spec R2/AC5 requires had silently broken. The Flight arm has always
  # prewarmed (below); this arm did not, and it is the DENOMINATOR of the 1.3x
  # ratio.
  #
  # FAIL CLOSED here, unlike the Flight arm's record-and-continue. The bias
  # direction is what differs: a partly-cold BARE SCAN reads SLOWER, which SHRINKS
  # `bare/flight` and makes the 1.3x target EASIER to hit — a degradation that can
  # manufacture a win. (A degraded Flight prewarm biases against do_get, so
  # continuing with a recorded label is honest there.) A prewarm scan that fails
  # while the timed scan would succeed is also not a thing: same binary, same
  # arguments, same corpus.
  #
  # Skipped on the cold arm BY DESIGN — a prewarm there would make "cold"
  # meaningless.
  local prewarm_status="skipped-cold-arm"
  if [[ "$temp" == "warm" ]]; then
    if taskset -c "$SERVER_CPUS" "$BIN/ws0-scan-bench" \
        --corpus "$CORPUS" --passes 1 \
        > "$OUT_DIR/$tag.prewarm.json" 2> "$OUT_DIR/$tag.prewarm.err"; then
      prewarm_status="ok"
    else
      prewarm_status="FAILED-exit-$?"
      printf '%s\n' "$prewarm_status" > "$OUT_DIR/$tag.prewarm.status"
      echo "FATAL: bare-scan PREWARM failed for $tag ($prewarm_status)." >&2
      echo "       Without it this 'warm' rep is partly cold, which makes the bare scan" >&2
      echo "       read SLOWER and the 1.3x bare/flight target EASIER — a degradation" >&2
      echo "       that can manufacture a win, so it is refused rather than labelled." >&2
      echo "       See $OUT_DIR/$tag.prewarm.err" >&2
      exit 1
    fi
  fi
  printf '%s\n' "$prewarm_status" > "$OUT_DIR/$tag.prewarm.status"

  # Setup-only leg: the corpus open + schema ingest, under its OWN perf window,
  # so its cycles can be SUBTRACTED from the full run (spec R2).
  perf_stat_c "$OUT_DIR/perf-$tag-setup.csv" \
    taskset -c "$SERVER_CPUS" "$BIN/ws0-scan-bench" \
      --corpus "$CORPUS" --setup-only \
    > "$OUT_DIR/$tag-setup.json" 2> "$OUT_DIR/$tag-setup.err"

  drop_caches_if_cold "$temp"
  perf_stat_c "$OUT_DIR/perf-$tag.csv" \
    taskset -c "$SERVER_CPUS" "$BIN/ws0-scan-bench" \
      --corpus "$CORPUS" --passes "$SCAN_PASSES" \
    > "$OUT_DIR/$tag.json" 2> "$OUT_DIR/$tag.err" \
    || { echo "FATAL: bare-scan rep $tag failed — see $OUT_DIR/$tag.err" >&2; exit 1; }
  echo "  $tag done"
}

# ---------------------------------------------------------------------------
# Arm B — Flight do_get over a real loopback transport
# ---------------------------------------------------------------------------
measure_flight() {
  local temp="$1" rep="$2" arm="$3" tag="flight-$arm-$temp-$rep"
  local step="$STEP_DURATION"
  [[ "$temp" == "cold" ]] && step="$COLD_STEP_DURATION"
  # Only the previous rep's own server — never a `pkill` by name.
  stop_server
  require_port_free "before $tag"
  drop_caches_if_cold "$temp"

  CQLITE_FLIGHT_MERGE_PATH="$arm" taskset -c "$SERVER_CPUS" "$BIN/cqlite-flight" \
    --data-dir "$CORPUS" --listen "127.0.0.1:$PORT" \
    > "$OUT_DIR/$tag.server.log" 2>&1 &
  SERVER_PID=$!
  local i
  for i in $(seq 1 120); do
    (echo >"/dev/tcp/127.0.0.1/$PORT") >/dev/null 2>&1 && break
    sleep 1
  done

  # Prewarm OUTSIDE the perf window (warm arm only): opens the readers and fills
  # the warm-handle registry, so the measured window is steady-state scan work
  # and not one-off setup. On the COLD arm this is deliberately skipped — a
  # prewarm would make "cold" meaningless.
  #
  # The outcome is RECORDED, not swallowed (issue #3096 review). A silently failed
  # prewarm downgrades a "warm" claim to a partly-cold one, and the old `|| true`
  # left nothing in results.json or summary.txt to say so. The bias runs AGAINST
  # the Flight arm (a cold-ish arm measures slower), so it cannot manufacture a
  # win — but an unrecorded degradation is still an unrecorded degradation. The
  # run continues rather than aborting: a rep that is honestly labelled
  # `prewarm-failed` is more useful than no rep, and ws0_report.py surfaces the
  # label in every report it writes.
  local prewarm_status="skipped-cold-arm"
  if [[ "$temp" == "warm" ]]; then
    if taskset -c "$CLIENT_CPUS" "$BIN/flight-loadgen" \
        --endpoint "http://127.0.0.1:$PORT" --ticket-template "$TICKET_TEMPLATE" \
        --shape full --ramp 1 --step-duration 20s --round prewarm --out /dev/null \
        > "$OUT_DIR/$tag.prewarm.log" 2>&1; then
      prewarm_status="ok"
    else
      prewarm_status="FAILED-exit-$?"
      echo "  WARNING: prewarm FAILED for $tag ($prewarm_status) — this 'warm' rep is" >&2
      echo "           partly cold. Recorded in results.json and summary.txt; see" >&2
      echo "           $OUT_DIR/$tag.prewarm.log" >&2
    fi
  fi
  printf '%s\n' "$prewarm_status" > "$OUT_DIR/$tag.prewarm.status"

  perf_stat_c "$OUT_DIR/perf-$tag.csv" \
    taskset -c "$CLIENT_CPUS" "$BIN/flight-loadgen" \
      --endpoint "http://127.0.0.1:$PORT" --ticket-template "$TICKET_TEMPLATE" \
      --shape full --ramp 1 --step-duration "$step" \
      --round "$tag" --out "$OUT_DIR/$tag.jsonl" \
    > "$OUT_DIR/$tag.log" 2>&1 \
    || { stop_server; echo "FATAL: flight rep $tag failed — see $OUT_DIR/$tag.log" >&2; exit 1; }

  stop_server
  echo "  $tag done"
}

echo
echo "=== issue #3096 same-session baseline (rig hardened by #3272) ==="
echo "corpus:      $CORPUS"
echo "server CPUs: $SERVER_CPUS (verified physical-core siblings)"
echo "client CPUs: $CLIENT_CPUS"
echo "reps:        $REPS   temps: $TEMPS   arms: $ARMS"
echo "out:         $OUT_DIR"
# The SELECTION, stated up front as well as in the report (issue #3272, finding 6).
# Completeness is judged against WHAT WAS SELECTED — an unselected temperature or arm
# is legitimately absent, a selected one that is absent is fatal — so the selection
# has to be visible, or a narrow session reads exactly like a full matrix that
# happened to print fewer rows. ws0_report.py records it in `results.json .selection`
# and prints a PARTIAL MATRIX banner; this line is the same fact at the top of the
# transcript, before any measurement exists to be misread.
if [[ "$TEMPS" != "warm cold" || "$ARMS" != "bypass merge" ]]; then
  echo "selection:   PARTIAL MATRIX — temps [$TEMPS] x arms [$ARMS] only."
  echo "             The full matrix is temps [warm cold] x arms [bypass merge];"
  echo "             absent combinations will NOT be measured by this session and"
  echo "             the report will say nothing about them."
else
  echo "selection:   FULL MATRIX — temps [$TEMPS] x arms [$ARMS]"
fi
echo

for temp in $TEMPS; do
  echo "-- bare scan ($temp) --"
  for rep in $(seq 1 "$REPS"); do measure_scan "$temp" "$rep"; done
  for arm in $ARMS; do
    echo "-- flight do_get / $arm ($temp) --"
    for rep in $(seq 1 "$REPS"); do measure_flight "$temp" "$rep" "$arm"; done
  done
done

python3 "$HERE/ws0_report.py" \
  --dir "$OUT_DIR" --corpus "$CORPUS" --server-cpus "$SERVER_CPUS" \
  --client-cpus "$CLIENT_CPUS" --reps "$REPS" --temps "$TEMPS" --arms "$ARMS" \
  --step-duration "$STEP_DURATION/$COLD_STEP_DURATION" --scan-passes "$SCAN_PASSES" \
  | tee "$OUT_DIR/summary.txt"

echo
echo "machine-readable: $OUT_DIR/results.json"
echo "human summary:    $OUT_DIR/summary.txt"
