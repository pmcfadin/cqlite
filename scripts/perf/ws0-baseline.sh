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
#
# ---------------------------------------------------------------------------
# FILE SIZE, and the eight libraries this driver has been split into (epic #1116)
# ---------------------------------------------------------------------------
# The gate's `file-size` ratchet is `.rs`-ONLY, so a shell file crosses the ~800-line
# campsite-rule target SILENTLY — this is checked with `wc -l` rather than left to the gate. Round
# 9's guard fixes took this file to 1008 lines, and the MEASUREMENT LEGS were split out in
# response (see `lib-measure.sh`); round 10's M2 provenance record took it to 986 and the BUILD +
# BINARY IDENTITY went out the same way (`lib-binaries.sh`). It is ~900 now.
#
# Eight libraries, each owning ONE question about whether a measurement means what it says:
#
#     lib-cpu.sh          are the pinned CPUs one physical core?
#     lib-host-state.sh   is the host's state put back?
#     lib-args.sh         are the arguments values this rig can measure?
#     lib-perf-lint.sh    is the counting domain CPU-wide?
#     lib-server.sh       which program did the Flight arm actually measure?
#     lib-outdir.sh       do the artifacts being read all come from ONE session?
#     lib-measure.sh      how is ONE rep of an arm executed, prewarmed and counted?
#     lib-binaries.sh     WHICH PROGRAMS are measured, and are they this revision's?
#
# What remains here is deliberately the part that must stay legible in ONE file: the ORDER of
# operations, which is itself a correctness property (arguments before creation, verification
# before measurement, the pin before the first rep), the round/rotation loop, and `perf_stat_c`.
#
# `perf_stat_c` did NOT move with the legs, and that is load-bearing rather than a preference:
# `perf_invocation_lint_tree` DISCOVERS which file owns the single wrapper and lints every OTHER
# `scripts/perf/*.sh` in `library` mode, where DEFINING `perf_stat_c` is itself a finding ("the rig
# has exactly ONE"). Moving it into a library would flip the owner and make this driver a library
# that must not define it — inverting layer 1 of the three-layer perf guard — and
# `test_ws0_cpu_pinning_guards.sh` text-extracts it from THIS file by name. The next seam, if one
# is needed, is the session-pin python heredocs (~100 lines) — tracked under epic #1116.

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
# shellcheck source=scripts/perf/lib-server.sh
source "$HERE/lib-server.sh"
# shellcheck source=scripts/perf/lib-outdir.sh
source "$HERE/lib-outdir.sh"
# shellcheck source=scripts/perf/lib-binaries.sh
source "$HERE/lib-binaries.sh"
# shellcheck source=scripts/perf/lib-inputs.sh
source "$HERE/lib-inputs.sh"
# LAST, because the sourcing order is the DEPENDENCY order: the measurement legs call
# `stop_server`/`require_port_free`/`await_server_ready` from lib-server.sh above, plus this
# driver's own `perf_stat_c` and `drop_caches_if_cold` (both defined below — a function body is
# resolved at CALL time, and the legs are called only from the measurement loop, which is after
# both definitions).
# shellcheck source=scripts/perf/lib-measure.sh
source "$HERE/lib-measure.sh"

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
# WHETHER THIS RUN CLAIMS TO BE A WS0 BASELINE (#3272 round 13, F3). Defaults to `baseline`,
# which REQUIRES the canonical measurement corpus: the pre-measurement pin used to snapshot
# whatever corpus it was handed and compare it against nothing, so a smoke-sized or
# differently-seeded corpus was self-consistent all the way through the reporter and published as
# a baseline. `--non-baseline` runs it anyway and LABELS the session and the report — a smoke
# corpus must still run, and this issue has already broken three documented commands by forbidding
# an input instead of labelling it. The two words come from `ws0_canonical_corpus.MODE_*`.
BASELINE_MODE="baseline"
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
  --out DIR            Results dir (default \$REPO/target/perf-ws0-3096/<ts>-<pid>, created
                       atomically). REFUSED if it exists and is non-empty: measuring into a
                       used dir mixes artifacts from different sessions into one report.
  --no-build           Skip the release build; use the binaries already in target/release.
  --non-baseline       Measure a corpus that is NOT the canonical measurement corpus. By
                       DEFAULT the corpus is checked against the canonical pin in
                       tools/ws0-corpus-gen/src/measurement_corpus.rs before the first rep
                       and a divergent one is REFUSED: a smoke-sized or differently-seeded
                       corpus is self-consistent through every other check, so it used to be
                       published as a WS0 baseline. This flag runs it anyway — the smoke path
                       — and the session manifest and the printed report are LABELLED
                       'NOT A WS0 BASELINE' in words. It never makes a run a baseline.
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
    --non-baseline) BASELINE_MODE="non-baseline"; shift ;;
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
# --- AN EXPLICIT `--out` MUST NOT BE A USED DIRECTORY (#3272 round 6, R1) -------------
# ABOVE the argument boundary, and CREATION stays below it. That split is deliberate: this is a
# pure ARGUMENT check (it needs no perf, no topology, no corpus), so calling it here makes it
# reachable — and therefore OBSERVABLE by the hermetic self-tests — through
# `--validate-args-only`, while `--validate-args-only` still creates nothing. The reason a used
# dir is refused at all, and why it is refused rather than auto-suffixed: scripts/perf/lib-outdir.sh.
require_unused_out_dir "${OUT_DIR:-}"

if [[ "$VALIDATE_ONLY" == "1" ]]; then
  # `baseline-mode` is in the stamp so the hermetic self-tests can observe WHICH claim the run
  # makes without executing anything. The canonical-corpus COMPARISON itself is necessarily below
  # this boundary (it reads the corpus's recorded identity off disk), like the schema check.
  echo "ARGUMENTS OK (--validate-args-only): reps=$REPS temps=[$TEMPS] arms=[$ARMS]" \
       "port=$PORT scan-passes=$SCAN_PASSES step=$STEP_DURATION cold-step=$COLD_STEP_DURATION" \
       "baseline-mode=$BASELINE_MODE"
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
# CAPTURED, not merely printed (#3272 review round 9, F6). `verify_sibling_pair` echoes the
# EXPANDED sibling set it verified (`2,10 -> verified siblings of one physical core (2 10)`), and
# that expansion is the substance of the verification — the sysfs answer, not a restatement of the
# argument. It is recorded into the session dir below so the REPORT's "verified physical-core
# siblings" claim rests on an observation instead of on trust. The status is still checked (the
# function fails closed; `set -e` plus this `||` makes that explicit rather than implicit).
WS0_SERVER_SIBLINGS="$(verify_sibling_pair "$SERVER_CPUS" "server")" || exit 2
echo "$WS0_SERVER_SIBLINGS"
verify_sibling_pair "$CLIENT_CPUS" "client" 2>/dev/null \
  || echo "client CPUs: $CLIENT_CPUS (a multi-core set — only the SERVER set must be one physical core)"
verify_disjoint "$SERVER_CPUS" "$CLIENT_CPUS"

# ---------------------------------------------------------------------------
# Server lifecycle — ONLY the process THIS script started (issue #3096 review)
# ---------------------------------------------------------------------------
# `stop_server`, `require_port_free`, `require_socket_prober` and `await_server_ready`
# live in scripts/perf/lib-server.sh: the rig's one responsibility that is a PROCESS AND A
# SOCKET rather than a number — which program the Flight arm actually measured. That file
# carries the full argument for each, including why a port that ACCEPTS is not evidence
# that our server is the one serving it (#3272 review round 3, B4).
#
# The driver keeps `on_exit` and its single `trap` registration below, because composing
# them is a decision about THIS file's exit paths: bash keeps one handler per signal, so a
# second top-level `trap ... EXIT` would silently discard the first.


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


# Fail BEFORE the release build, not after it.
require_port_free "preflight"
# ...and establish, ONCE, that the ownership prober every rep depends on actually works
# (#3272 B4). It binds a KERNEL-ASSIGNED ephemeral port (`bind(…, 0)`), so no argument is
# passed and no collision is possible.
#
# It used to be called with `$(( PORT + 1 ))` — a port NOTHING had checked free
# (`require_port_free` covers `$PORT` only), which made `--port 65535` ask for port 65536 and,
# when `PORT+1` happened to be occupied, made a CORRECT run die with "the prober cannot answer"
# whose stated causes were all wrong (#3272 review round 4 nit). See `require_socket_prober`.
require_socket_prober

# Weaken the host knobs CPU-wide counting needs — AFTER `trap on_exit` above, never
# before: the interval between the write and the trap being armed is the one window in
# which a signal could leave the host relaxed. Every knob is enrolled for restore
# BEFORE it is written, and a knob whose prior could not be read is not written at all
# (scripts/perf/lib-host-state.sh).
relax_perf_sysctls

# --- THE OUTPUT DIR IS CREATED EXCLUSIVELY AND CLAIMED (#3272 R1 + round 7 F3) --------
# The whole lifecycle — refuse a used dir, create it, CLAIM it against a concurrent peer —
# lives in scripts/perf/lib-outdir.sh, which carries the full argument for each half. The CALL
# SITES stay here so what this driver actually does to the filesystem remains visible at its top
# level, and so the ARGUMENT/CREATION boundary is legible in one file: the refusal is called far
# above, `--validate-args-only` exits between them, and creation happens only here.
BIN="$REPO_ROOT/target/release"
# The status is checked EXPLICITLY rather than left to `set -e`. `create_out_dir` runs in a
# COMMAND SUBSTITUTION (it must echo the default name it chose), so its `exit 2` terminates only
# that subshell; the driver survives on `set -e` alone. That works — and a fail-closed refusal
# whose enforcement depends on an implicit shell option is one `set +e` from being decorative,
# which is the class of defect this issue exists to remove. `|| exit` states it.
OUT_DIR="$(create_out_dir "${OUT_DIR:-}" "$REPO_ROOT/target/perf-ws0-3096")" || exit 2
# ...and the result must be a directory that now exists. An empty `$OUT_DIR` here would send
# every artifact to `/…` paths rooted at nothing, so it is refused rather than measured into.
[[ -n "$OUT_DIR" && -d "$OUT_DIR" ]] || {
  echo "FATAL: the session output directory was not created (got '${OUT_DIR:-}')." >&2
  exit 2
}

# --- BUILD, AND RECORD WHICH BINARIES ARE MEASURED (#3272 round 10, M2) ---------------
# Both live in scripts/perf/lib-binaries.sh, which carries the full argument for each: the release
# build plus the existence loop, and the provenance record that closes `--no-build`'s silence about
# WHICH programs produced the reported ratio. The CALL SITES stay here so the ORDER remains legible
# at the driver's top level — binaries before the corpus pin, the pin before the first rep.
#
# Status checked EXPLICITLY: neither runs in a command substitution, so `|| exit 2` is what
# terminates the run (a refusal resting on `set -e` alone is one `set +e` from decorative).
build_release_binaries || exit 2
record_measured_binaries || exit 2

# --- THE SCHEMA, THEN THE REQUEST DERIVED FROM IT (#3272 round 6 R2 + round 10 M1) ----
# Both live in scripts/perf/lib-inputs.sh, which carries the full argument for each: the DDL is a
# MEASUREMENT INPUT the two arms read ASYMMETRICALLY (the bare scan ingests it per invocation, the
# Flight ticket is generated from it once), and `ticket-template.json` IS THE REQUEST every Flight rep
# re-reads. `write_session_ticket` sets `$TICKET_TEMPLATE` beside the write that creates the file —
# inside `$OUT_DIR`, never the shared corpus (#3272 round 13, F2).
#
# The CALL SITES stay here so the ORDER remains legible at the driver's top level, and because two
# suites assert exactly this order BY LINE NUMBER: the schema before the ticket derived from it, the
# ticket before the pin that records its digest, the pin before the first rep.
#
# Status checked EXPLICITLY: neither runs in a command substitution, so `|| exit 2` is what
# terminates the run (a refusal resting on `set -e` alone is one `set +e` from decorative).
verify_corpus_schema_input || exit 2
write_ticket_template_for_session || exit 2

# --- IS THIS THE CORPUS THE BASELINE IS DEFINED AS? (#3272 round 13, F3) --------------
# Also in scripts/perf/lib-inputs.sh, which carries the full argument. In one line: the pin below
# records the identity of whatever corpus it is handed and compares it against NOTHING, so a
# smoke-sized or differently-seeded corpus is self-consistent through every downstream check and
# was published as a WS0 BASELINE. This compares the corpus against the canonical pin in
# tools/ws0-corpus-gen/src/measurement_corpus.rs BEFORE the first rep. `--non-baseline` runs a
# noncanonical corpus anyway and LABELS the session and the report.
#
# BEFORE the pin, deliberately: a refusal must cost seconds, not a multi-minute measurement.
#
# Status checked EXPLICITLY: it does not run in a command substitution, so `|| exit 2` is what
# terminates the run (a refusal resting on `set -e` alone is one `set +e` from decorative).
verify_corpus_is_canonical_or_declared || exit 2

# --- PIN WHICH CORPUS THIS SESSION IS ABOUT TO MEASURE (#3272 review round 4) --------
# Stamped into the RESULTS DIR, BEFORE the first rep, and REQUIRED by ws0_report.py.
# ...and, since round 10's M1, WHICH REQUEST: the pin records the Flight ticket's digest, which is
# why the template is written immediately above rather than below.
#
# The corpus digest used to be verified only against the corpus present AT REPORT TIME, with
# no identity captured in the session dir beforehand. Two real sequences attribute figures to
# bytes nobody measured, and BOTH are self-consistent at report time — so the report-time
# check cannot see either:
#
#   * re-reporting an OLD result dir against a DIFFERENT corpus (`--dir <old> --corpus
#     <other>`): the reporter re-derives `<other>`'s digest, finds it consistent, and prints
#     it as the identity of figures measured over something else;
#   * a corpus REGENERATED (or written by another lane) BETWEEN reps: report time verifies the
#     corpus's LAST state while the earlier reps measured the earlier bytes.
#
# The pin records the corpus path, row count, Data.db size and recorded sha256 — it does NOT
# re-hash, because this is on the measurement's critical path and a 2.8 GB hash per session
# would be paid by every run. The digest RE-DERIVATION stays at report time; what the pin adds
# is that the identity being re-derived is the one the session STARTED with.
# The CONFIGURATION is stamped with it (#3272 F1): the reporter READS its reps,
# temperatures, arms, scan-passes and CPU pins from here rather than taking them as
# arguments, because taking them from the reporting command line let a re-report with
# fewer reps or a narrower arm set ignore measured artifacts and still claim the
# replacement configuration had been verified. The component set is recorded too
# (#3272 F3), so the pre-measurement identity covers everything a scan reads.
# The configuration reaches python through the ENVIRONMENT rather than a positional argument
# list, and that is not a style choice: a continuation line whose first token is a bare `"$VAR"`
# is treated as an INVOCATION by `perf_invocation_lint`'s fail-closed layer 1 (an unresolvable
# command word could be anything, including perf), so a multi-line positional argv here FAILS
# the rig's own startup lint. Named env vars are also self-describing at the call site.
WS0_CFG_REPS="$REPS" \
WS0_CFG_TEMPS="$TEMPS" \
WS0_CFG_ARMS="$ARMS" \
WS0_CFG_SCAN_PASSES="$SCAN_PASSES" \
WS0_CFG_SERVER_CPUS="$SERVER_CPUS" \
WS0_CFG_CLIENT_CPUS="$CLIENT_CPUS" \
WS0_CFG_STEP_DURATION="$STEP_DURATION/$COLD_STEP_DURATION" \
WS0_CFG_BASELINE_MODE="$BASELINE_MODE" \
python3 -c '
import os, pathlib, sys
sys.path.insert(0, sys.argv[1])
from ws0_canonical_corpus import require_canonical_or_declared
from ws0_validate import Invalid, load_corpus_identity
from ws0_session import MANIFEST_CONFIG_FIELDS, write_session_corpus_pin
corpus, out = pathlib.Path(sys.argv[2]), pathlib.Path(sys.argv[3])
repo_root = pathlib.Path(sys.argv[4])
# Every field the manifest requires, read from the environment by NAME. A field the driver
# failed to export is an ERROR here rather than an absent key the reporter would refuse later,
# so the diagnostic names the driver rather than the session dir.
config = {}
for field in MANIFEST_CONFIG_FIELDS:
    var = "WS0_CFG_" + field.upper()
    value = os.environ.get(var)
    if value is None or value == "":
        print(f"FATAL: {var} was not exported, so the session manifest cannot record"
              f" {field!r} — the reporter READS its configuration from the manifest (#3272 F1)"
              " and would refuse this session.", file=sys.stderr)
        raise SystemExit(1)
    config[field] = value
try:
    identity = load_corpus_identity(corpus)
    # THE CANONICAL COMPARISON (#3272 round 13, F3), re-derived HERE so the record the pin carries
    # is one this process OBSERVED rather than a verdict passed in as a string. The comparison
    # already ran (and already refused a divergent corpus in `baseline` mode) in
    # `verify_corpus_is_canonical_or_declared` above; running it again is cheap (a source parse and
    # a few comparisons) and it is what makes a caller-supplied verdict impossible.
    canonical = require_canonical_or_declared(
        repo_root, identity, config["baseline_mode"], corpus
    )
    pin = write_session_corpus_pin(out, corpus, identity, config, canonical)
except Invalid as exc:
    print(f"FATAL: {exc}", file=sys.stderr)
    raise SystemExit(1)
print(f"corpus pin:   {pin[\"data_db_sha256\"]} ({pin[\"rows\"]} rows / {pin[\"data_db_bytes\"]} B,"
      f" {len(pin[\"components\"])} components)"
      " recorded in session-corpus-pin.json BEFORE the first rep")
print(f"config pin:   reps={config[\"reps\"]} temps=[{config[\"temps\"]}] arms=[{config[\"arms\"]}]"
      f" scan-passes={config[\"scan_passes\"]} — the reporter READS these, never its own argv")
print(f"canonical pin: {canonical[\"label\"]} — recorded in session-corpus-pin.json"
      " (canonical_corpus), which the reporter REQUIRES and re-derives the verdict from")
' "$HERE" "$CORPUS" "$OUT_DIR" "$REPO_ROOT" \
  || { echo "FATAL: could not pin this session's corpus identity — the report REQUIRES it," >&2
       echo "       because a session dir that does not record WHICH corpus it measured can" >&2
       echo "       be re-reported against any other corpus (#3272 round 4)." >&2
       exit 2; }

# --- RECORD THE SIBLING VERIFICATION THIS DRIVER PERFORMED (#3272 round 9, F6) --------
# The report prints `server <list> (verified physical-core siblings)`. That claim was backed by
# NOTHING readable: the CPU lists reach the reporter through the manifest, whose reader
# deliberately declines to re-check them, while the check that DID run was against this script's
# argv — and nothing tied the two together. MEASURED: a manifest hand-edited to
# `config.server_cpus = "99,99"` made the report exit 0 printing
# `pinning : server 99,99 (verified physical-core siblings)`.
#
# So the verification is written down WHERE IT WAS MADE. This driver read the real
# `thread_siblings_list` on the real measuring host and failed closed; it now records WHICH lists
# it checked and WHAT sysfs answered, and `ws0_report.py` requires the record and requires the
# manifest's lists to EQUAL the verified ones.
#
# NOT re-derived at report time, deliberately: a results dir is routinely reviewed on a different
# host, whose topology describes a machine that never ran the measurement — a check whose verdict
# depends on where the report is generated is not evidence about the session.
WS0_PIN_SERVER_CPUS="$SERVER_CPUS" \
WS0_PIN_CLIENT_CPUS="$CLIENT_CPUS" \
WS0_PIN_SIBLINGS="$WS0_SERVER_SIBLINGS" \
WS0_PIN_TOPOLOGY_ROOT="$CPU_TOPOLOGY_ROOT" \
python3 -c '
import json, os, pathlib, socket, sys
sys.path.insert(0, sys.argv[1])
from ws0_pinning import PINNING_RECORD_FIELDS, pinning_record_path
rec = {
    "server_cpus": os.environ["WS0_PIN_SERVER_CPUS"],
    "client_cpus": os.environ["WS0_PIN_CLIENT_CPUS"],
    # The sysfs ANSWER, which is the substance of the verification rather than a restatement of
    # the argument: `verify_sibling_pair`s own output line, carrying the expanded sibling set it
    # read out of thread_siblings_list.
    "server_siblings_expanded": os.environ["WS0_PIN_SIBLINGS"],
    "topology_root": os.environ["WS0_PIN_TOPOLOGY_ROOT"],
    "host": socket.gethostname() or "unknown",
    "verified_by": "scripts/perf/lib-cpu.sh verify_sibling_pair + verify_disjoint, fail-closed,"
                   " against the real thread_siblings_list BEFORE the first rep",
    # The LIMIT of the record, carried in the record — the same posture
    # `recorded_round_metadata` takes about itself, so results.json tells ONE story about its
    # own artifacts instead of two contradictory ones (#3272 F6).
    "provenance": "written BY THE DRIVER that performed the verification, so it establishes what"
                  " that driver observed on the measuring host — not an independent truth about"
                  " the host. What it closes is the SUBSTITUTION: a manifest CPU list the driver"
                  " never verified can no longer be printed as verified.",
}
absent = [f for f in PINNING_RECORD_FIELDS if f not in rec]
if absent:
    # A field the reader requires and the writer does not produce would surface as an absent-record
    # refusal at report time, blaming the session dir for a driver defect. Named here instead.
    print(f"FATAL: the pinning record is missing {absent} — the writer and"
          " ws0_pinning.PINNING_RECORD_FIELDS disagree.", file=sys.stderr)
    raise SystemExit(1)
p = pinning_record_path(pathlib.Path(sys.argv[2]))
p.write_text(json.dumps(rec, indent=1) + "\n")
print(f"pinning pin:  {rec[\"server_cpus\"]} verified against"
      f" {rec[\"topology_root\"]} on {rec[\"host\"]} — recorded in {p.name} so the report cites an"
      " OBSERVATION, not lib-cpu.sh by name")
' "$HERE" "$OUT_DIR" \
  || { echo "FATAL: could not record this session's CPU-pin verification — the report REQUIRES" >&2
       echo "       it, because otherwise 'verified physical-core siblings' is printed about a" >&2
       echo "       manifest string nothing ever checked (#3272 F6)." >&2
       exit 2; }
# (The Flight ticket template used to be written HERE, after the pin and after the CPU-pin record.
# It moved ABOVE the pin in #3272 round 10's M1, because the pin now records its digest — see the
# block that writes it for why an unpinned request is invisible to every corpus check.)

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
# The two MEASUREMENT LEGS live in scripts/perf/lib-measure.sh (#3272 round 9)
# ---------------------------------------------------------------------------
# `measure_scan` (arm A, the bare scan) and `measure_flight` (arm B, do_get over a real
# loopback transport) were split out under the campsite rule — this file was 1008 lines
# against the ~800 source target, and the gate's `file-size` ratchet is `.rs`-only so a shell
# file crosses it silently. That library carries the full argument for each leg (the prewarm
# postures and why they differ per arm, the setup-only leg, the per-rep server lifecycle) and
# the reason `perf_stat_c` deliberately did NOT move with them: the tree lint DISCOVERS the
# single wrapper's owner and lints every OTHER scripts/perf/*.sh in `library` mode, where
# defining it is a FINDING — so moving it would invert layer 1 of the perf guard.
#
# Sourced at the TOP of this file, after lib-server.sh, because the sourcing order is the
# dependency order: these legs call stop_server/require_port_free/await_server_ready. The
# call sites are the measurement loop below.

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
# The measurement loop: ROUNDS outside, arms inside, order rotated by round
# ---------------------------------------------------------------------------
# This loop used to run ALL `$REPS` bare-scan reps, then all Flight reps of arm 1, then
# all of arm 2. That makes each arm's median a measurement of a DIFFERENT TIME WINDOW,
# and this rig's own recorded evidence says those windows are not comparable: on the
# delivery box, in one session, the UNTOUCHED warm bare scan read 370,134 rows/s at
# 05:06 UTC and 333,206 rows/s at 06:05 — a ~10% drift with nothing changed on the
# measured path. The whole claim this driver exists to produce is the `bare/flight`
# RATIO, so a drift between the bare-scan block and the Flight block lands DIRECTLY on
# the reported ratio and on the 1.3x PASS/BELOW-TARGET verdict.
#
# So: ROUNDS on the outside, arms on the inside, order rotated by round index, with the
# BARE SCAN AS ONE OF THE ROTATED ARMS rather than leading every round (it is the
# DENOMINATOR of the ratio, so a fixed position would put any within-round systematic
# effect on it every time). For the 2-arm default that is a genuine alternation
# (`scan,bypass` / `bypass,scan` / …) rather than a fixed order. The per-rep artifacts
# are named exactly as before (`scan-<temp>-<rep>`, `flight-<arm>-<temp>-<rep>`).
#
# WHAT THIS LOOP DOES *NOT* BUY, STATED BECAUSE THE DIFFERENCE MATTERS (#3272 round 4).
# Ordering the loop this way is a REASONABLE THING TO DO; it is NOT a DRIFT CONTROL, and
# nothing downstream verifies that it happened. `measurement-method.md` §3b specifies a
# same-session interleaved control ("or NO COMPARISON") — that control is **NOT
# IMPLEMENTED OR ENFORCED** by this rig, and the reporter accordingly makes NO
# interleaving claim. An earlier round of #3272 did print one ("the reps were INTERLEAVED
# … OBSERVED FROM THE CLOCK"), and it was DELETED: at the default `--reps 1` there is one
# round, so ZERO orderings were compared while the verdict field still said `True`. A
# positive verdict from zero comparisons is the exact defect this issue exists to remove.
# Re-adding an OBSERVED control on real hardware is tracked by **#3287/#3299**. Until
# then, treat any cross-arm difference this rig prints as UNCONTROLLED FOR DRIFT.
#
# The ROUND, the ARM'S POSITION WITHIN IT and the rep's COMPLETION INSTANT are recorded
# per rep (`<tag>.round`). `ws0_report.py` REQUIRES all four fields, uses `round` to pair
# the per-round comparison, INTEGRITY-CHECKS them against each other (same round set per
# arm, positions 1..n exactly once, arms_in_round matching, no duplicate instants, labels
# not contradicting instants) and passes them through to `results.json` as INERT RECORDED
# DATA. It derives no ordering property from them.
#
# At the spreads this rig measures (5-10% per arm) a couple of percent of median
# difference is not readable, and the recorded #3096 session is the case in point — a
# +2.3% median difference measured at ZERO (median −0.03%, 4 of 8 rounds positive) when
# re-measured on 8 rounds.
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

# record_round <tag> <round> <position> <arms-in-round> — the per-rep round metadata the
# reporter REQUIRES. Written after the rep, so a rep that died leaves no metadata and the
# report refuses it rather than attributing it to a round.
#
# THIS IS RECORDED DATA, NOT EVIDENCE OF A PROPERTY (#3272 round 4). `round`, `position` and
# `arms_in_round` are numbers THIS LOOP COMPUTES, and the reporter additionally forces
# `round == rep`, so `round` carries no independent information at all. `monotonic_ns` is the
# only field that records something the loop did not choose — WHEN the rep completed.
#
# What the reporter does with all four: it pairs the per-round comparison on `round`,
# INTEGRITY-CHECKS the four fields against each other and against the other arms (a
# duplicate instant means a copied file; labels contradicting instants means neither can
# attribute a figure), and passes the values through to `results.json` verbatim. It derives
# NO ordering or interleaving claim from them — that claim was deleted because it reported a
# positive verdict at one round having compared nothing. Re-adding an OBSERVED drift control
# is #3287/#3299. Recording the metadata is kept because an operator (or that later work)
# needs the raw per-rep timeline, and because the integrity refusals catch a partial or
# forged session.
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
    echo "FATAL: could not read a monotonic clock for $1 — this rep's completion instant" >&2
    echo "       cannot be recorded, and the reporter REQUIRES all four round-metadata" >&2
    echo "       fields (they are integrity-checked against each other). This rig" >&2
    echo "       requires python3." >&2
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
    echo "-- round $rep/$REPS ($temp) — one rep of each of the $_N_ARMS arms, rotated --"
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

# The reporter takes ONLY the two paths: everything else is read from the session manifest
# stamped above (#3272 F1). Passing `--reps`/`--temps`/`--arms`/`--scan-passes`/the CPU pins
# here would be the substitution the manifest exists to prevent, so those flags no longer
# exist — an accepted-but-ignored flag is a silent lie to whoever passed it.
python3 "$HERE/ws0_report.py" --dir "$OUT_DIR" --corpus "$CORPUS" \
  | tee "$OUT_DIR/summary.txt"

echo
echo "machine-readable: $OUT_DIR/results.json"
echo "human summary:    $OUT_DIR/summary.txt"
