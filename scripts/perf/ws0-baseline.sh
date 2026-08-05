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
#     >2x observer cost on this workload, and per-THREAD counting is the same thing
#     under another option, so neither appears anywhere in this rig. Three layers
#     enforce that (see scripts/perf/lib-perf-lint.sh): an ALLOWLIST over the source
#     of EVERY `scripts/perf/*.sh` — this driver and all four libraries it sources,
#     discovered by glob, not enumerated (perf is invoked in ONE place, everything
#     else must be marked); a per-TOKEN OPTION ALLOWLIST (only `-x -e -C -o --`, so
#     an option nobody anticipated fails closed); and a RUNTIME argv check in the
#     wrapper that refuses ANY caller-supplied option. All three are allowlists
#     because five ordinary bash spellings bypassed two successive deny-list greps,
#     and then `-t`/`--tid` bypassed the deny-list of OPTIONS that replaced them.
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
# shellcheck source=scripts/perf/lib-args.sh
source "$HERE/lib-args.sh"

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
# `--validate-args-only`: run every ARGUMENT check, print a stamp, and exit 0 having
# touched NOTHING outside this process (issue #3272 review R1). See the exit point below
# for why the alternative — asserting acceptance by running the real driver until it
# happens to fail on something later — was a hermeticity defect rather than a shortcut.
VALIDATE_ONLY=0

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
  --validate-args-only Run every ARGUMENT check and exit 0 with 'ARGUMENTS OK' — no
                       sysctl write, no build, no cache drop, no perf, no measurement.
                       Exists so the self-tests can assert the ACCEPT direction of
                       argument validation without executing anything (#3272 R1).
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
    --validate-args-only) VALIDATE_ONLY=1; shift ;;
    -h|--help) usage; exit 0 ;;
    # Every unrecognized argument is an ERROR, never ignored: a typo'd flag that
    # is silently dropped produces a measurement of something other than what
    # was asked for, and nothing in the output would say so.
    *) echo "FATAL: unrecognized argument '$1'" >&2; usage >&2; exit 2 ;;
  esac
done

# --- trap 1: the WHOLE RIG contains NO per-process perf invocation -------------
# Runs unconditionally at startup, over EVERY `scripts/perf/*.sh` — this driver AND the
# four libraries it sources — so an edit that reaches for a counting-domain option, or
# that invokes perf anywhere other than the single `perf_stat_c` wrapper, cannot run at
# all. The mechanism, the five bypasses review round 1 found in its predecessor, and why
# BOTH the invocation check and the option check are ALLOWLISTS rather than deny-list
# greps: scripts/perf/lib-perf-lint.sh. Its LAYER 3 (the runtime argv check) lives in
# `perf_stat_c` below, because only the wrapper sees the argv.
#
# THE SUBJECT IS THE DIRECTORY, not `${BASH_SOURCE[0]}` (issue #3272 review round 2, R2).
# This used to lint ITSELF only, which put `lib-cpu.sh`, `lib-host-state.sh`,
# `lib-args.sh` and `lib-perf-lint.sh` inside the rig and outside all three layers: a
# `perf stat -p "$SERVER_PID"` added to any of them fired nothing. The set is DISCOVERED
# by glob rather than enumerated, so adding a library cannot silently add an unlinted
# file, and the tree lint reports its own vacuity (an empty subject, no wrapper, or two
# wrappers) rather than printing nothing and reading as clean.
_perf_lint_out="$(perf_invocation_lint_tree "$HERE")"
if [[ -n "$_perf_lint_out" ]]; then
  echo "FATAL: this rig contains a per-process/per-thread perf invocation, invokes perf" >&2  # perf-lint-allow
  echo "       outside its single wrapper, or carries an option outside the allowlist:" >&2
  printf '       %s\n' "$_perf_lint_out" >&2
  echo "       Per-process (${_PP_SHORT} / ${_PP_LONG}) and per-thread (${_PT_SHORT} / ${_PT_LONG}) counting" >&2
  echo "       measured >2x observer cost on this workload; CPU-wide counting is mandatory" >&2
  echo "       (issue #3096 spec R2), and every invocation must go through perf_stat_c so" >&2  # perf-lint-allow
  echo "       ONE place enforces it. Permitted options: ${PERF_ALLOWED_OPTS}." >&2
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
# Numeric-option and duration validation live in scripts/perf/lib-args.sh; the
# CALL SITES stay here, so what this driver actually validates is visible at its
# top level rather than buried in a library.

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
#
# `COLD_STEP_MAX_MS` is defined in `lib-args.sh`, beside the `duration_reject` diagnostic
# that quotes it — it used to be defined HERE and interpolated THERE, which made that
# library non-self-contained: under `set -u` any other caller died on an unbound variable
# instead of printing the diagnostic (#3272 review round 2 nit).


for _spec in "step-duration:$STEP_DURATION" "cold-step-duration:$COLD_STEP_DURATION"; do
  _name="${_spec%%:*}"; _val="${_spec#*:}"
  # `_rc` is captured on its OWN statement. `if ! cmd; then … $? …` reads 0 in the body
  # — `!` REPLACES the status with the inverted one — so the cause code was lost and
  # `duration_reject` always took its malformed branch. Measured: a 20-digit duration
  # reported "must be <n>ms, <n>s or <n>m", which is the exact misleading
  # format-complaint this split exists to remove.
  _ms="$(parse_duration_ms "$_val")" && _rc=0 || _rc=$?
  if [[ "$_rc" -ne 0 ]]; then
    duration_reject "$_name" "$_val" "$_rc"
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

# --- the ARGUMENT-VALIDATION boundary (#3272 review R1) -----------------------
# Everything above this line is a decision about the ARGUMENTS: pure string/integer
# checks plus this file's own source lint. Everything BELOW it touches the world —
# it stats the corpus, reads the host's CPU topology, probes the port, WRITES HOST
# SYSCTLS via `sudo -n`, and runs `cargo build --release`.
#
# `--validate-args-only` stops exactly here, which is what makes the ACCEPT direction
# of argument validation ASSERTABLE without executing anything. The previous approach
# was to run the real driver and accept "it failed somewhere later" as proof the
# arguments were fine — and that inverted the hermeticity of the self-tests: on a LINUX
# host (where the gate's `tooling-tests` runs) the accept cases sailed past validation
# into `relax_perf_sysctls` (a host sysctl mutation) and a full `cargo build --release`,
# six times over. It was invisible only because macOS exits earlier at
# `perf is not installed`. A test suite whose hermeticity depends on the host LACKING a
# tool is not hermetic; it is untested on the platform that matters.
#
# The stamp is a fixed string so the assertion is affirmative — the caller checks for
# `ARGUMENTS OK`, not for the absence of a complaint.
#
# SCOPE, stated so the mode is not read as more than it is: the checks BELOW the boundary
# (`--corpus` resolvability, the sibling/disjointness verification of `--server-cpus` and
# `--client-cpus`, port availability) are HOST-DEPENDENT and are deliberately NOT covered
# by this mode. They cannot be — verifying a CPU set needs a real `thread_siblings_list`,
# which is exactly what `scripts/tests/test_ws0_cpu_pinning_guards.sh` drives directly
# against an injected topology root instead.
if [[ "$VALIDATE_ONLY" == "1" ]]; then
  echo "ARGUMENTS OK (--validate-args-only): reps=$REPS temps=[$TEMPS] arms=[$ARMS]" \
       "port=$PORT scan-passes=$SCAN_PASSES step=$STEP_DURATION cold-step=$COLD_STEP_DURATION"
  echo "  nothing was executed: no sysctl write, no build, no cache drop, no perf, no measurement."
  exit 0
fi

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
#
# TWO checks, with DIFFERENT postures, and the reason each has the posture it has is
# recorded HERE at the branch rather than left to be re-derived (#3272 review round 2,
# R4b — it used to enumerate `-p`/`--pid` only, which let `-t`/`--tid` through: per-THREAD
# counting, equally per-process in effect, same observer cost):
#
#  (a) the argv PREFIX — every token before the COMMAND WORD — is an ALLOWLIST OF
#      NOTHING. This wrapper supplies every perf option itself, so a caller-supplied
#      option here is refused WHATEVER IT IS. That closes the unknown-future-spelling
#      hole: `--per-thread`, `-a`, `--cgroup` and whatever perf ships next fail without
#      this file having to know they exist.
#  (b) the COMMAND and its arguments cannot be allowlisted, and that is a fact about the
#      domain, not an omission: `$@` legitimately carries `taskset -c 1`,
#      `--shape full`, `--step-duration 45s`, `--corpus …`. So there the check is
#      necessarily an enumeration — the counting-DOMAIN option families — and it is
#      retained because a domain option appearing after the command word is a caller who
#      built the argv wrong, which is worth catching even though the token would land
#      past perf's `--` and never be read by perf as an option at all.
# `PERF_DOMAIN_OPTS` comes from lib-perf-lint.sh, beside the option names it is built
# from — never defined here, because this function is EXTRACTED and driven directly by
# scripts/tests/test_ws0_cpu_pinning_guards.sh, and a constant it could only get from the
# driver would make the extracted copy die on an unbound variable instead of diagnosing.
perf_stat_c() {
  local outfile="$1"; shift
  local a name opt in_prefix=1
  for a in "$@"; do
    # The option NAME with any attached value dropped: `-p1234`, `-p"$x"`, `--pid=1234`
    # all reduce to the option itself. Bash has already done quote removal, so the
    # spelling problem a source-text scan cannot close does not exist here.
    case "$a" in
      --*) name="${a%%=*}" ;;
      -?*) name="${a:0:2}" ;;
      *)   name="" ;;
    esac
    if [[ -z "$name" ]]; then
      in_prefix=0     # the COMMAND word: everything after it belongs to the command
      continue
    fi
    if [[ "$in_prefix" == "1" ]]; then
      echo "FATAL: perf_stat_c was passed the perf option '$a'." >&2
      echo "       This wrapper supplies every perf option itself and counts CPU-WIDE only" >&2
      echo "       (issue #3096 spec R2). Before the command word the check is an ALLOWLIST" >&2
      echo "       OF NOTHING: a caller-supplied option is refused whatever it is, so an" >&2
      echo "       option perf has not shipped yet fails CLOSED rather than passing because" >&2
      echo "       no deny-list entry matched it (#3272 R4b)." >&2
      echo "       The argument list was: $*" >&2
      exit 2
    fi
    for opt in $PERF_DOMAIN_OPTS; do
      [[ "$name" == "$opt" ]] || continue
      echo "FATAL: perf_stat_c was passed the counting-domain option '$a'." >&2
      echo "       Per-process (${_PP_SHORT}/${_PP_LONG}) and per-thread (${_PT_SHORT}/${_PT_LONG})" >&2
      echo "       counting measured >2x observer cost on this workload; this wrapper counts" >&2
      echo "       CPU-WIDE only (issue #3096 spec R2). A domain option after the command" >&2
      echo "       word is an argv built wrong: it would land past perf's '--' and never be" >&2
      echo "       read by perf at all, so the measurement would silently not be the one" >&2
      echo "       asked for. The argument list was: $*" >&2
      exit 2
    done
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

# ---------------------------------------------------------------------------
# The measurement loop is INTERLEAVED — one rep per arm per round, arm order
# rotated (issue #3272 review B5)
# ---------------------------------------------------------------------------
# This loop used to run ALL `$REPS` bare-scan reps, then all Flight reps of arm 1, then
# all of arm 2. That makes each arm's median a measurement of a DIFFERENT TIME WINDOW,
# and this rig's own recorded evidence says those windows are not comparable:
#
#   `docs/reports/ws0-3096-artifacts/measurement-method.md` §3b — "**THE RULE, binding
#   on every future use of this rig: same-session interleaved A/B/C with a drift control
#   that is code-identical across arms, or NO COMPARISON.**" It then states the shape
#   explicitly: (1) "run **one rep at a time**, never all reps of an arm back to back",
#   (2) "**rotate the arm order every round** so no arm holds a fixed position",
#   (4) "**difference within a round**".
#
# The rule exists because it was PAID FOR: on the delivery box, in one session, the
# UNTOUCHED warm bare scan read 370,134 rows/s at 05:06 UTC and 333,206 rows/s at 06:05
# — a ~10% drift with nothing changed on the measured path. And the failure is not
# hypothetical for THIS driver: the whole claim it exists to produce is the
# `bare/flight` RATIO, so a drift between the bare-scan block and the Flight block
# lands DIRECTLY on the reported ratio and on the 1.3x PASS/BELOW-TARGET verdict, in
# whichever direction the box happened to drift. The sequential order made a
# same-session run a cross-window comparison wearing a same-session label.
#
# So: ROUNDS on the outside, arms on the inside, order rotated by round index. Every
# round measures the bare scan and each Flight arm within a few minutes of each other,
# and no arm holds a fixed position across rounds. The per-rep artifacts are named
# exactly as before (`scan-<temp>-<rep>`, `flight-<arm>-<temp>-<rep>`), so the reporter
# and every existing artifact reader are unaffected — this changes WHEN each rep runs,
# not what is written.
#
# THE BARE SCAN IS ONE OF THE ROTATED ARMS (issue #3272 review round 2, R4a). It used to
# lead EVERY round with only the Flight arms rotating among themselves — so with the
# DEFAULT single Flight arm (`--arm bypass`) NO ROTATION OCCURRED AT ALL: round 1 was
# `scan, flight-bypass`, and so was every round after it. The round-1 fix for the drift
# hazard therefore did not close it. And the bare scan is the one arm where a fixed
# position matters most: it is the DENOMINATOR of the reported ratio, so a systematic
# within-round effect that always lands on it (a page cache left by the previous round's
# Flight rep, a thermal ramp early in each round) moves the ratio in one direction every
# time — a bias the per-round direction count cannot see, because it is present in every
# round equally.
#
# So the rotated list is `scan` PLUS each Flight arm, and each entry is DISPATCHED to
# `measure_scan` or `measure_flight` by name. For the 2-arm default that is a genuine
# alternation (`scan,bypass` / `bypass,scan` / `scan,bypass` / …) rather than a fixed
# order — a "rotation" that degenerates to a fixed order at n=2 would be the same defect.
#
# The ROUND AND THE ARM'S POSITION WITHIN IT are recorded per rep (`<tag>.round`), and
# ws0_report.py REQUIRES and VERIFIES them: it pairs by the OBSERVED round and refuses a
# session whose interleaving cannot be established, instead of printing an interleaving
# claim unconditionally (which is what it used to do while pairing by rep index and
# reading none of these files — #3272 R3). `position` is what makes the rotation checkable
# at all: a round index alone cannot distinguish an interleaved session from an arm-major
# one, since both have a rep index.
#
# At the spreads this rig measures (5-10% per arm) a couple of percent of median
# difference is not readable, and the recorded #3096 session is the case in point — a
# +2.3% median difference measured at ZERO (median −0.03%, 4 of 8 rounds positive) when
# re-measured on 8 interleaved rounds.
#
# `rotate_arms <round> <arms…>` — the arm list left-rotated by `(round-1) % n`, so over
# n rounds every arm occupies every position.
rotate_arms() {
  local round="$1"; shift
  local -a all=("$@")
  local n="${#all[@]}" i shift_by
  shift_by=$(( (round - 1) % n ))
  for ((i = 0; i < n; i++)); do
    printf '%s ' "${all[$(( (i + shift_by) % n ))]}"
  done
}

# record_round <tag> <round> <position> <arms-in-round> — the interleaving metadata the
# reporter REQUIRES. Written after the rep, so a rep that died leaves no metadata and the
# report refuses it rather than attributing it to a round.
#
# `monotonic_ns` IS THE OBSERVATION; the other three are LABELS (#3272 review round 3, B3).
# `round`, `position` and `arms_in_round` are numbers THIS LOOP COMPUTES, and the reporter
# additionally forces `round == rep`, so `round` carries no independent information at all.
# An arm-major loop keeping the same rotation arithmetic emits BYTE-IDENTICAL metadata for a
# NON-interleaved session — and the report then printed "the reps were INTERLEAVED … this is
# OBSERVED, not asserted", which was a re-statement of a label rather than an observation.
#
# A monotonic timestamp per rep is the thing a forgery cannot reproduce: round-major
# ordering (every arm of round r finishing before any arm of round r+1) is a FACT ABOUT THE
# CLOCK, and an arm-major session violates it in a way no relabelling can hide. So the
# reporter derives the interleaving claim from these instants and refuses a session whose
# timestamps say arm-major, whatever the labels say.
#
# `time.monotonic_ns()` and not `date`: it is monotonic (immune to an NTP step or a DST
# change mid-session, either of which could otherwise reorder two reps), it is
# nanosecond-resolution, and python3 is already a HARD requirement of this rig. The cost is
# one interpreter start per rep — tens of milliseconds against a 45-second step.
#
# The instant recorded is the rep's COMPLETION. The loop is strictly sequential — one rep
# runs to completion before the next starts — so completion order IS the order the reps ran,
# and using the later instant means a rep that died leaves no metadata at all rather than a
# start time for a measurement that never finished.
record_round() {
  local now
  now="$(python3 -c 'import time; print(time.monotonic_ns())')" || {
    echo "FATAL: could not read a monotonic clock for $1 — the interleaving of this" >&2
    echo "       session would be UNOBSERVABLE, and the report refuses to claim an" >&2
    echo "       interleaving it cannot establish (#3272 B3). This rig requires python3." >&2
    exit 1
  }
  printf 'round=%s\nposition=%s\narms_in_round=%s\nmonotonic_ns=%s\n' \
    "$2" "$3" "$4" "$now" > "$OUT_DIR/$1.round"
}

# The rotated arm list: the bare scan and every selected Flight arm, as PEERS.
# shellcheck disable=SC2206  # word-splitting $ARMS into an array is intended
_ARM_LIST=(scan $ARMS)
_N_ARMS="${#_ARM_LIST[@]}"
for temp in $TEMPS; do
  for rep in $(seq 1 "$REPS"); do
    echo "-- round $rep/$REPS ($temp) — one rep of each of the $_N_ARMS arms, interleaved --"
    _pos=0
    for arm in $(rotate_arms "$rep" "${_ARM_LIST[@]}"); do
      _pos=$((_pos + 1))
      case "$arm" in
        scan)
          measure_scan "$temp" "$rep"
          record_round "scan-$temp-$rep" "$rep" "$_pos" "$_N_ARMS" ;;
        *)
          measure_flight "$temp" "$rep" "$arm"
          record_round "flight-$arm-$temp-$rep" "$rep" "$_pos" "$_N_ARMS" ;;
      esac
    done
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
