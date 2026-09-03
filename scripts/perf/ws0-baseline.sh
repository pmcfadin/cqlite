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
# FILE SIZE, and the ten libraries this driver has been split into (epic #1116)
# ---------------------------------------------------------------------------
# The gate's `file-size` ratchet is `.rs`-ONLY, so a shell file crosses the ~800-line
# campsite-rule target SILENTLY — this is checked with `wc -l` rather than left to the gate. Every
# guard round since round 9 has pushed this file over and been answered by a SPLIT rather than by
# growth: the MEASUREMENT LEGS (`lib-measure.sh`), the BUILD + BINARY IDENTITY
# (`lib-binaries.sh`), the SCHEMA + REQUEST (`lib-inputs.sh`) and round 22's BOUNDARY CHECK
# (`lib-corpus-boundary.sh`), which landed NET-NEUTRAL — one call line in, its argument out.
#
# NO LINE COUNT IS WRITTEN HERE, deliberately — this issue's own lesson applied to its own prose. A
# number in a comment is a RECORDED CLAIM NOTHING VERIFIES and drifts exactly as `_PERF_STATE="ok"`
# did (#3249): an earlier draft of THIS paragraph asserted a count already false when written. Run
# `wc -l`, the only statement of this file's size that cannot be stale.
#
# Eleven libraries, each owning ONE question about whether a measurement means what it says:
#
#     lib-cpu.sh             are the pinned CPUs one physical core?
#     lib-host-state.sh      is the host's state put back?
#     lib-args.sh            are the arguments values this rig can measure?
#     lib-perf-lint.sh       is the counting domain CPU-wide?
#     lib-server.sh          which program did the Flight arm actually measure?
#     lib-outdir.sh          do the artifacts being read all come from ONE session?
#     lib-measure.sh         how is ONE rep of an arm executed, prewarmed and counted?
#     lib-binaries.sh        WHICH PROGRAMS are measured, and are they this revision's?
#     lib-inputs.sh          WHICH SCHEMA are the bytes read with, and WHICH REQUEST is asked?
#     lib-corpus-boundary.sh are the bytes still the PINNED bytes, MID-RUN?
#     lib-flight-arm.sh      the two arms no longer run the same way — what differs, and was
#                            the difference VERIFIED rather than requested? (#3551)
#
# What remains here is deliberately the part that must stay legible in ONE file: the ORDER of
# operations, which is itself a correctness property (arguments before creation, verification
# before measurement, the pin before the first rep), the round/rotation loop, and `perf_stat_c`.
#
# `perf_stat_c` did NOT move with the legs, and that is load-bearing rather than a preference:
# `perf_invocation_lint_tree` DISCOVERS which file owns the single wrapper and lints every OTHER
# `scripts/perf/*.sh` in `library` mode, where DEFINING `perf_stat_c` is itself a finding ("the rig
# has exactly ONE"). Moving it would flip the owner and invert layer 1 of the three-layer perf
# guard, and `test_ws0_cpu_pinning_guards.sh` text-extracts it from THIS file by name. The next
# seam is the session-pin python heredocs (~100 lines) — tracked under epic #1116.

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
# shellcheck source=scripts/perf/lib-corpus-boundary.sh
source "$HERE/lib-corpus-boundary.sh"
# AFTER lib-cpu.sh, which its verification calls (#3551): the sourcing order is the dependency
# order.
# shellcheck source=scripts/perf/lib-flight-arm.sh
source "$HERE/lib-flight-arm.sh"
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
# --- THE FLIGHT ARM'S OWN PIN AND ALLOCATOR (#3551) --------------------------------------
# The rig has never had §3b step 3's DRIFT CONTROL: a leg that is code-identical AND
# pin-identical across the arms being compared. These three variables are what create one.
# Everything about the bare-scan arm stays on `$SERVER_CPUS`, so when only the FLIGHT pin (or
# only the FLIGHT allocator) moves, the bare scan is the same program on the same cores in the
# same session — the control the comparison needs — and the difference between arms is the ONE
# property that changed.
#
# `FLIGHT_SERVER_CPUS` is EMPTY here and defaults to `$SERVER_CPUS` after the argument loop, so
# EVERY EXISTING INVOCATION BEHAVES BYTE-IDENTICALLY: same taskset list, same perf counting
# domain, same recorded manifest value. The default is resolved after the loop rather than here
# because `--server-cpus` may be given AFTER `--flight-server-cpus` and the loop is
# order-independent.
FLIGHT_SERVER_CPUS=""
# WHICH PROPERTY the flight pin must satisfy — `siblings` (one physical core's hyperthreads, the
# #3096 default) or `distinct-cores` (one thread per physical core, the SMT-unpin arm). NOT a
# relaxation: each value selects an EQUALLY AFFIRMATIVE assertion read from the real
# `thread_siblings_list`, and an unknown value is a usage error rather than a default (see the
# argument loop).
FLIGHT_PIN_MODE="siblings"
# Arm C: the Flight SERVER PROCESS ONLY runs under `LD_PRELOAD=<libjemalloc>`. The binary is
# byte-identical across arms — that is the point of doing it with a preload rather than a build
# flag — so nothing else in the rig changes.
FLIGHT_ALLOCATOR="system"
# An explicit library path for a host whose libjemalloc is somewhere else. EMPTY = DISCOVER from
# the standard paths below, and a failed discovery is a NAMED REFUSAL rather than a silent
# fall-through to the system allocator: a run labelled `jemalloc` that measured system malloc is
# the instrument-reports-success-without-measuring shape this rig exists to refuse.
FLIGHT_ALLOCATOR_LIB=""
# --- ARM C, GENERALISED: THE MECHANISM UNDER TEST IS ARENA CONTENTION (#3551, #3217 partC F1) ---
# `docs/reports/ws0-3217-artifacts/partC/PROPOSED-FOLLOWUPS.md` F1 (strength STRONGEST)
# pre-registers this experiment as its AC2: "a controlled arena experiment at the same points:
# MALLOC_ARENA_MAX = 1, 2, 4, default ... If capping arenas does not move the -24%, the allocator
# hypothesis is falsified and that is a passing outcome to be reported as such."
#
# EMPTY means INJECT NOTHING, and that is not the same as injecting 0: glibc's handling of an
# empty or zero value is not something this rig may assume, so the variable is simply absent from
# the server's environment unless a value was asked for (the launch has two forms for exactly
# that reason). Independent of `--flight-allocator`: the two knobs are recordable together or
# separately.
FLIGHT_MALLOC_ARENA_MAX=""
# The candidate PATHS live beside the three-valued probe that consumes them, in
# `scripts/perf/lib-flight-arm.sh`: the list and the probe are one decision (what counts as
# "this host has no jemalloc"), and splitting them would put half of it two files away.
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
# The counted events. CONFIGURABLE since #3248, whose AC4 clock basis needs
# msr/aperf,msr/mperf,msr/tsc,ref-cycles, which this two-event default cannot supply. The
# DEFAULT IS UNCHANGED so an AC0 reproduction run is configured byte-identically to the
# #3096 session it reproduces. Validated by ws0_validate.perf_event_list (non-empty,
# charset-allowlisted, DUPLICATE-FREE) and recorded in the session manifest, because
# cycles/row and IPC are claims about specific counters.
#
# GROWING THIS SET PROVOKES PMU MULTIPLEXING, which perf handles by time-sharing counters and
# SCALING the counts — an estimate reported as an ordinary integer. read_perf_counters refuses
# a scaled count (it now reads perf's enabled-percentage, which nothing read before #3248), so
# an over-large set fails closed rather than reporting quietly wrong numbers.
EVENTS="cycles,instructions"
# Where the measured binaries come from. Default unchanged; #3248 needs target/perfsym because
# [profile.release] sets strip = true, so a release binary carries NO symbols and cannot be
# attributed per-function at all. Implies --no-build (the build writes only to target/release).
BIN_DIR=""
# AC1 sampling profile (#3248). Empty = no profiling, which is the default: AC0 must run on an
# UNPERTURBED instrument, and a sampling session costs measurable observer overhead (measured on
# this box: ~5% on cycles with a concurrent record). When set, every timed counting window is
# ALSO covered by a CPU-wide `perf record` on the same pinned CPUs.
#
# The two sessions do NOT multiplex — MEASURED, not assumed: `cycles` and `instructions` both
# reported 100.00% enabled with a concurrent `perf record -e cycles -C` on the same CPUs, and the
# sampling session collected samples with zero lost. Had they multiplexed, perf would have SCALED
# the counts and `read_perf_counters` would now refuse them (#3248's multiplex guard).
#
# A profiled run is DISTINGUISHABLE FROM A BASELINE IN THE ARTIFACT without a new manifest field,
# because a profile needs symbols and `[profile.release]` strips: such a run is necessarily
# `--bin-dir target/perfsym`, which the manifest records. So `results.json` already says which
# binaries produced any given figure.
PROFILE_OUT=""
# A PRIME sample frequency, deliberately. A round number risks lock-step with a periodic activity
# in the workload (a batch boundary, a flush interval), which aliases the profile toward or away
# from whatever shares its period.
PROFILE_FREQ=499
# THE ACTIVE SAMPLER PID, AT FILE SCOPE SO `on_exit` CAN SEE IT.
#
# `perf_stat_c` cleans up its own sampler on the NORMAL path, but a TERM/HUP delivered to the
# DRIVER mid-measurement runs `on_exit` and exits without ever returning into `perf_stat_c` --
# so the sampler and its `sleep 86400` were orphaned for 24 hours. A `local` in the function
# is invisible to the trap handler, which is why this is a global.
_ACTIVE_PROFILER_PID=""
# The external box-load timeseries to judge this session's quiescence against (#3248).
# OPT-IN, and its ABSENCE IS RECORDED rather than silent -- see the wiring below the
# measurement loop for why it is not mandatory.
QUIESCENCE_TIMESERIES=""
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
  --flight-server-cpus LIST
                       Pin the FLIGHT SERVER to LIST instead of --server-cpus. Defaults to
                       --server-cpus, so omitting it changes nothing: same taskset list and
                       the same CPU-wide counting domain as today. Verified before the first
                       rep — every CPU present and ONLINE, disjoint from --client-cpus, and
                       satisfying --flight-pin-mode. The BARE-SCAN arm always stays on
                       --server-cpus, which is what makes it a pin-identical drift control
                       across arms that differ only in the flight pin (#3551).
  --flight-pin-mode WHICH
                       siblings | distinct-cores (default $FLIGHT_PIN_MODE). NOT a relaxation
                       of the sibling guard: both are read from the real thread_siblings_list
                       and both fail closed. `siblings` REFUSES a distinct-core set;
                       `distinct-cores` REFUSES a sibling pair, and REFUSES a single-CPU list
                       (pairwise-distinct over one CPU compares nothing). An unknown value is
                       a usage error, never a default.
  --flight-allocator WHICH
                       system | jemalloc (default $FLIGHT_ALLOCATOR). On jemalloc the Flight
                       SERVER PROCESS ONLY is launched with LD_PRELOAD=<lib>; the binary is
                       byte-identical across arms. VERIFIED AFTER START from
                       /proc/<pid>/maps, per rep: glibc prints 'cannot be preloaded ...
                       ignored' and CONTINUES with system malloc, so without that read arm C
                       would be a byte-identical duplicate of arm B under a label saying
                       otherwise. On `system` the NEGATIVE is asserted too (no jemalloc
                       mapping) and any inherited LD_PRELOAD is emptied for the launch.
  --flight-malloc-arena-max N
                       Set MALLOC_ARENA_MAX=N for the FLIGHT SERVER PROCESS ONLY (same seam as
                       --flight-allocator, and independent of it). Unset = inject nothing,
                       which is NOT the same as 0. Positive integer, validated up front.
                       VERIFIED per rep from /proc/<pid>/environ as a whole NUL-separated
                       entry — an arena cap leaves no mapping, so `maps` cannot see it at all,
                       and a substring match would confuse =1 with =16. This is #3217 partC
                       F1's pre-registered AC2: if capping arenas does not move the delta the
                       allocator hypothesis is FALSIFIED, which is a result to report, not a
                       failure.
  --jemalloc-lib PATH  The preloaded library, for a host where it is not one of the standard
                       paths. Must be an existing, readable, regular file. Without it the
                       path is DISCOVERED and a failed discovery REFUSES (remedy named) —
                       never a silent fall-through to system malloc.
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
  --events LIST        Comma-separated hardware events to count (default $EVENTS).
                       Validated non-empty, charset-allowlisted and DUPLICATE-FREE:
                       read_perf_counters SUMS lines by event name, so a repeated event would
                       report DOUBLE its true count as an ordinary integer. Recorded in the
                       session manifest. NOTE a larger set can provoke PMU multiplexing, which
                       SCALES the counts; a scaled count is refused at report time, never
                       published.
  --bin-dir DIR        Take the measured binaries from DIR instead of target/release, and
                       IMPLY --no-build (the build writes only to target/release, so building
                       would populate a directory nobody measures). Exists because
                       [profile.release] sets strip = true: a release binary carries no symbols,
                       so per-function attribution against it is impossible (#3248). The reps
                       run FROZEN COPIES either way, so the digests still describe the bytes
                       that ran — this field records WHICH BUILD they came from, which those
                       digests cannot say.
  --profile-out DIR    ALSO take a CPU-wide sampling profile over every timed counting window,
                       written to DIR as profile-<tag>.data (#3248 AC1). Off by default: a
                       sampling session costs observer overhead, so a baseline must not carry
                       one. Needs symbol-bearing binaries, so pair it with --bin-dir; a
                       stripped binary yields a profile of raw addresses and says nothing.
  --profile-freq N     Sampling frequency for --profile-out (default $PROFILE_FREQ, a prime:
                       a round number risks lock-step with a periodic activity in the
                       workload, which aliases the profile).
  --quiescence-timeseries FILE
                       Judge this session against an EXTERNAL box-load timeseries (the
                       sampler in scripts/perf/, one JSON line per 10s with a competing-
                       process census). Boundary samples are taken around the measurement
                       loop and ws0_quiescence.py REFUSES the run if any in-window sample
                       shows a competing process. Opt-in, because the timeseries is produced
                       outside the rig and demanding one would fail every box without it --
                       but its ABSENCE IS RECORDED in the manifest as
                       `quiescence: NOT VERIFIED`, so a run can never silently look verified.
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
    --flight-server-cpus) FLIGHT_SERVER_CPUS="$2"; shift 2 ;;
    # A CLOSED SET, like --temp/--arm above: an unrecognised mode must not fall back to the
    # default, because the whole point of the flag is WHICH property was asserted, and a run
    # that silently asserted the other one is a measurement of something nobody asked for.
    --flight-pin-mode)
      case "$2" in
        siblings|distinct-cores) FLIGHT_PIN_MODE="$2" ;;
        *) echo "FATAL: --flight-pin-mode must be siblings|distinct-cores (got '$2')" >&2; exit 2 ;;
      esac; shift 2 ;;
    --flight-allocator)
      case "$2" in
        system|jemalloc) FLIGHT_ALLOCATOR="$2" ;;
        *) echo "FATAL: --flight-allocator must be system|jemalloc (got '$2')" >&2; exit 2 ;;
      esac; shift 2 ;;
    --flight-malloc-arena-max) FLIGHT_MALLOC_ARENA_MAX="$2"; shift 2 ;;
    --jemalloc-lib) FLIGHT_ALLOCATOR_LIB="$2"; shift 2 ;;
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
    --events) EVENTS="$2"; shift 2 ;;
    --bin-dir) BIN_DIR="$2"; DO_BUILD=0; shift 2 ;;
    --profile-out) PROFILE_OUT="$2"; shift 2 ;;
    --quiescence-timeseries) QUIESCENCE_TIMESERIES="$2"; shift 2 ;;
    --profile-freq) PROFILE_FREQ="$2"; shift 2 ;;
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

# Through the SHARED validator (`lib-args.sh`), never a new numeric check: a second
# implementation of "is this a positive integer" is a second thing to drift, and this one is
# already wrap-proof (#3272 F2). Only when a value was given — empty means inject nothing.
if [[ -n "$FLIGHT_MALLOC_ARENA_MAX" ]]; then
  require_positive_int flight-malloc-arena-max "$FLIGHT_MALLOC_ARENA_MAX"
fi
require_positive_int scan-passes "$SCAN_PASSES"
require_positive_int reps "$REPS"
require_positive_int port "$PORT" 65535

# --- THE MEASURED SERVER, SPELLED ONCE (#3272 round 14, F2) --------------------
# Derived HERE from the validated port because only this script knows which server it launched; it is
# BOTH what the reps are pointed at (lib-measure.sh) AND what the manifest pins before rep 1. TWO
# spellings — a composed argv and a separately-stamped pin — would make every rep of a correct run
# compare unequal to its own pin. `endpoint` was IGNORED; see SESSION_BOUND_INPUTS for what that let through.
FLIGHT_ENDPOINT="http://127.0.0.1:$PORT"

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

# --events and --bin-dir are validated HERE, ABOVE the --validate-args-only boundary, because
# "refusing a value after acting on it is not refusing it" (#3272 round 1, finding 10: `--reps
# 200000` passed the driver's own check and was refused only by the REPORT, after 200,000
# full-corpus reps). The first version of these two flags had exactly that defect: the event list
# was checked by the manifest writer and the bin dir at BIN assignment, both BELOW this line, so a
# duplicated event or a missing directory would have been caught only after a cargo build, a cache
# drop and a server start.
#
# The event list goes through THE SAME validator the manifest reader applies
# (ws0_validate.perf_event_list) rather than a bash re-implementation, because a second
# implementation of a rule is a second thing to drift: the driver would accept what the reporter
# refuses, or worse the reverse. Pure computation — no build, no sysctl, no perf, no measurement.
if ! _events_err="$(python3 -c '
import sys
sys.path.insert(0, sys.argv[1])
from ws0_validate import Invalid, perf_event_list
from ws0_collect import REQUIRED_EVENTS
try:
    chosen = perf_event_list("--events", sys.argv[2])
except Invalid as exc:
    print(exc, file=sys.stderr)
    raise SystemExit(1)
# THE COLLECTORS REQUIRE THESE UNCONDITIONALLY, so an event list without them completes the
# WHOLE measurement and only then fails at report time -- "refusing a value after acting on
# it is not refusing it", the rule this rig states for itself (#3272 round 1 finding 10). The
# documented clock-only set for the AC4 characterisation is exactly such a list, so this is a
# live footgun rather than a hypothetical one. Read from ws0_collect.REQUIRED_EVENTS rather
# than re-listed here: two copies of one requirement is the drift this rig keeps finding.
# NO APOSTROPHES IN THIS COMMENT -- it sits inside a shell single-quoted python program, so
# one apostrophe terminates the string. The first version of this comment did exactly that.
missing = [e for e in REQUIRED_EVENTS if e not in chosen]
if missing:
    print("--events omits " + ", ".join(repr(m) for m in missing)
          + ", which every collector requires. cycles/row and IPC are derived from them, so a"
          " list without them measures for minutes and then fails at REPORT time. Add them:"
          " they cost nothing alongside any other events.", file=sys.stderr)
    raise SystemExit(1)
' "$HERE" "$EVENTS" 2>&1)"; then
  echo "FATAL: --events is not a usable event list." >&2
  echo "       $_events_err" >&2
  exit 2
fi

if [[ -n "$PROFILE_OUT" ]]; then
  # NON-MUTATING above the boundary. `--validate-args-only` promises in words that "nothing
  # was executed ... no state", and the first version of this check ran `mkdir -p` HERE, so a
  # validation-only invocation CREATED a directory. Creation moved below the boundary; what is
  # checked here is only that the path could be created -- its parent exists and is writable.
  _prof_parent="$(dirname -- "$PROFILE_OUT")"
  if [[ -e "$PROFILE_OUT" && ! -d "$PROFILE_OUT" ]]; then
    echo "FATAL: --profile-out '$PROFILE_OUT' exists and is not a directory." >&2
    exit 2
  fi
  if [[ ! -d "$PROFILE_OUT" && ( ! -d "$_prof_parent" || ! -w "$_prof_parent" ) ]]; then
    echo "FATAL: --profile-out '$PROFILE_OUT' does not exist and its parent" >&2
    echo "       '$_prof_parent' is not a writable directory, so it cannot be created." >&2
    exit 2
  fi
  if ! [[ "$PROFILE_FREQ" =~ ^[1-9][0-9]{0,4}$ ]]; then
    echo "FATAL: --profile-freq '$PROFILE_FREQ' is not a positive integer below 100000." >&2
    echo "       A frequency of 0 samples nothing and reads exactly like a quiet profile." >&2
    exit 2
  fi
fi

# --profile-out WITHOUT --bin-dir CAN NEVER SUCCEED, so it is refused HERE rather than after a
# build (#3248, roborev job 69 finding 3).
#
# `[profile.release]` sets `strip = true`, so the default build ALWAYS produces symbol-free
# binaries and the post-build frozen-binary check must ALWAYS fail. The previous shape passed
# argument validation, CLAIMED both output directories, ran a full release build, and only then
# refused -- leaving claimed directories behind for a configuration that had no reachable
# success. Refusing an impossible configuration after acting on it is the same defect as
# refusing a bad VALUE after acting on it.
if [[ -n "$PROFILE_OUT" && -z "$BIN_DIR" ]]; then
  echo "FATAL: --profile-out requires --bin-dir, and this is not a preference." >&2
  echo "       [profile.release] sets strip = true, so the default build produces binaries with" >&2
  echo "       NO symbols and a sampling profile of them attributes nothing. This combination" >&2
  echo "       has no reachable success, so it is refused now rather than after a build." >&2
  echo "         cargo build --profile perfsym -p ws0-corpus-gen -p cqlite-flight -p flight-loadgen" >&2
  echo "         ... --bin-dir \$PWD/target/perfsym --profile-out <dir>" >&2
  exit 2
fi

# A PROFILE OF A STRIPPED BINARY IS THE SILENT FAILURE THIS FEATURE EXISTS TO AVOID.
# `[profile.release]` sets `strip = true`, so the default binaries carry ZERO symbols and
# `perf record` against them exits 0 and yields a confident table of raw addresses. Accepting
# `--profile-out` with them would produce exactly the #3217-class artifact this issue was
# funded to stop producing. Checked here rather than trusted: the symbol table is READ.
if [[ -n "$PROFILE_OUT" ]]; then
  _prof_bin="${BIN_DIR:-$REPO_ROOT/target/release}"
  for _b in ws0-scan-bench cqlite-flight flight-loadgen; do
    # `grep -c`, NOT `grep -q`, AND THE STATUS IS NOT READ FROM THE PIPELINE.
    #
    # This driver runs under `set -o pipefail`, and `grep -q` EXITS AS SOON AS IT MATCHES,
    # which closes the pipe and gives `nm` a SIGPIPE -- so the pipeline reports FAILURE on the
    # SUCCESS case. The first version of this guard therefore refused every CORRECT input: a
    # perfsym binary with 2,997 Rust symbols was reported as carrying none, which would have
    # blocked every legitimate profiling run. A guard that fails closed on correct input is
    # still a defect, and it is the same family as the rest of this issue -- the observer
    # (grep exiting early) changed the thing being measured (the producer exit status).
    # A MISSING BINARY IS FATAL HERE, NOT SKIPPED (roborev job 73 finding 1). The `-e` guard
    # below used to mean an ABSENT binary passed this loop silently: combined with a --bin-dir
    # check that verified only that the DIRECTORY existed, an empty or partial --bin-dir passed
    # `--validate-args-only` outright, and a real run then relaxed host sysctls and claimed
    # output directories before `build_release_binaries` rejected it. `--profile-out` requires
    # `--bin-dir` (above) and `--bin-dir` implies `--no-build`, so nothing will ever create it --
    # the configuration has NO reachable success and belongs in argument validation. This is the
    # preflight half of a fail-open I already fixed on the post-build side in round 1; the same
    # `[[ -e ]]` skip was still here.
    if [[ ! -f "$_prof_bin/$_b" ]]; then
      echo "FATAL: --profile-out was given with --bin-dir '$_prof_bin', but '$_b' is not a file" >&2
      echo "       there. --bin-dir implies --no-build, so nothing will create it and this run" >&2
      echo "       cannot succeed. Build the profile first, e.g." >&2
      echo "         cargo build --profile perfsym -p ws0-corpus-gen -p cqlite-flight -p flight-loadgen" >&2
      exit 2
    fi
    _syms=$(nm "$_prof_bin/$_b" 2>/dev/null | grep -c '_RN' || true)
    if [[ "${_syms:-0}" -eq 0 ]]; then
      echo "FATAL: --profile-out was given, but $_prof_bin/$_b carries NO Rust symbols." >&2
      echo "       A sampling profile of a stripped binary reports raw addresses and attributes" >&2
      echo "       nothing -- the profiler exits 0 and the failure is silent, which is the" >&2
      echo "       exact class this issue was funded to stop producing." >&2
      echo "       Build a symbol-bearing profile and point --bin-dir at it:" >&2
      echo "         cargo build --profile perfsym -p ws0-corpus-gen -p cqlite-flight -p flight-loadgen" >&2
      echo "         ... --bin-dir \$PWD/target/perfsym --profile-out <dir>" >&2
      exit 2
    fi
  done
fi

# `-f` AS WELL AS `-r`, BECAUSE THE MESSAGE ALREADY CLAIMED "file" (roborev job 71 finding 3).
# `! -r` alone passes a readable DIRECTORY and a FIFO: a directory then fails much later inside
# ws0_quiescence.py, and a FIFO can BLOCK the reader indefinitely -- in both cases AFTER the full
# measurement has run, which is exactly what an up-front argument check exists to prevent. `-f`
# follows symlinks, so a symlink to a real record still passes.
if [[ -n "$QUIESCENCE_TIMESERIES" ]] && { [[ ! -f "$QUIESCENCE_TIMESERIES" ]] || [[ ! -r "$QUIESCENCE_TIMESERIES" ]]; }; then
  echo "FATAL: --quiescence-timeseries '$QUIESCENCE_TIMESERIES' is not a readable regular file." >&2
  echo "       It is the external box-load record this session is judged against; an" >&2
  echo "       unreadable one cannot establish anything, so it is refused up front rather" >&2
  echo "       than after the measurement. A directory or FIFO is refused HERE for the same" >&2
  echo "       reason: a FIFO would block the reader after the whole measurement had run." >&2
  exit 2
fi

if [[ -n "$BIN_DIR" && ! -d "$BIN_DIR" ]]; then
  echo "FATAL: --bin-dir '$BIN_DIR' is not a directory." >&2
  echo "       --bin-dir implies --no-build (the cargo build writes only to target/release), so" >&2
  echo "       nothing will create it. Build it first, e.g." >&2
  echo "         cargo build --profile perfsym -p cqlite-flight -p flight-loadgen" >&2
  exit 2
fi

# ...AND THE DIRECTORY IS NOT THE BINARIES (roborev job 73 finding 1). An EMPTY but existing
# --bin-dir satisfied the check above, so `--validate-args-only` reported ARGUMENTS OK for a run
# that could not possibly execute: --bin-dir implies --no-build, so a missing measured binary is
# never created. Checked here, before any side effect, for the same reason as every other
# argument check -- refusing an impossible configuration AFTER acting on it is the defect.
if [[ -n "$BIN_DIR" ]]; then
  for _rb in ws0-scan-bench cqlite-flight flight-loadgen; do
    if [[ ! -f "$BIN_DIR/$_rb" ]]; then
      echo "FATAL: --bin-dir '$BIN_DIR' does not hold '$_rb'." >&2
      echo "       --bin-dir implies --no-build, so nothing will create it and this run cannot" >&2
      echo "       succeed. Build all three into that directory first, e.g." >&2
      echo "         cargo build --profile perfsym -p ws0-corpus-gen -p cqlite-flight -p flight-loadgen" >&2
      exit 2
    fi
    if [[ ! -x "$BIN_DIR/$_rb" ]]; then
      echo "FATAL: --bin-dir '$BIN_DIR/$_rb' exists but is not executable." >&2
      echo "       The reps execute it directly, so this run cannot succeed." >&2
      exit 2
    fi
  done
fi

# --- THE FLIGHT PIN DEFAULTS TO THE SERVER PIN (#3551) -------------------------------------
# AFTER the argument loop, because the loop is order-independent: resolving it at declaration
# would make `--flight-server-cpus` + a later `--server-cpus` silently disagree with the flag the
# operator wrote last. The equality is what makes this whole feature a NO-OP by default.
if [[ -z "$FLIGHT_SERVER_CPUS" ]]; then
  FLIGHT_SERVER_CPUS="$SERVER_CPUS"
fi

# --- THE FLIGHT ARM'S ALLOCATOR, RESOLVED BEFORE ANY MEASUREMENT (#3551) -------------------
# `scripts/perf/lib-flight-arm.sh` owns the three-valued library probe (present /
# verified-absent / a NAMED could-not-measure state, each a refusal) and the four values the
# manifest, the pin record and the per-rep check all read. Called HERE, above the argument
# boundary, because it reads nothing but file metadata and because "refusing a value after
# acting on it is not refusing it" — the rule `--bin-dir` and `--profile-out` follow a few lines
# up. Exit 2, like every other argument refusal.
record_flight_allocator_facts || exit 2

# --- THE ENVIRONMENT IS PART OF THE MEASUREMENT (#3551 item 8) ------------------------------
# Two records, deliberately separate: AMBIENT (as measured in this driver's own environment) and
# INJECTED (what the rig sets, per arm). "The operator had a stray LD_PRELOAD" and "the rig set
# one on purpose" are different facts and only one of them is a defect. Without them, arm A and
# arm C are indistinguishable in every recorded field — one binary set across all arms is
# deliberate, so the ENVIRONMENT is the only thing that differs, and it was written down nowhere.
# `docs/reports/ws0-3552-report.md` §4 is the governing rule: state RUSTFLAGS and
# CARGO_ENCODED_RUSTFLAGS AS MEASURED, because a reproduction only corroborates if its
# environment differs — not just its tree, box, or operator.
WS0_ENV_AMBIENT="$(ws0_ambient_env_record)"
# ...and an ambient ALLOCATOR setting is REFUSED rather than merely recorded, because
# `ws0-scan-bench` would inherit it and the bare scan is the drift control. Above the boundary
# because it is an environment read: `--validate-args-only` reaches it, so the refusal is
# hermetically observable and costs nothing.
refuse_ambient_allocator_env || exit 2
WS0_ENV_INJECTED="flight server process ONLY: LD_PRELOAD=${FLIGHT_ALLOCATOR_LIB:-<empty>}, $FLIGHT_ARENA_RECORDED; bare scan (the drift control): NOTHING is injected, asserted per rep against the environment its bench inherits (<tag>.scan-env.status)"

if [[ "$VALIDATE_ONLY" == "1" ]]; then
  # `baseline-mode` is in the stamp so the hermetic self-tests can observe WHICH claim the run
  # makes without executing anything. The canonical-corpus COMPARISON itself is necessarily below
  # this boundary (it reads the corpus's recorded identity off disk), like the schema check.
  echo "ARGUMENTS OK (--validate-args-only): reps=$REPS temps=[$TEMPS] arms=[$ARMS]" \
       "port=$PORT scan-passes=$SCAN_PASSES step=$STEP_DURATION cold-step=$COLD_STEP_DURATION" \
       "baseline-mode=$BASELINE_MODE events=[$EVENTS] bin-dir=[${BIN_DIR:-<default target/release>}]" \
       "flight-cpus=$FLIGHT_SERVER_CPUS flight-pin-mode=$FLIGHT_PIN_MODE" \
       "flight-allocator=$FLIGHT_ALLOCATOR jemalloc-lib=[$FLIGHT_ALLOCATOR_LIB_RECORDED]" \
       "flight-malloc-arena-max=[${FLIGHT_MALLOC_ARENA_MAX:-<not injected>}]" \
       "env-ambient=[$WS0_ENV_AMBIENT]"
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
# NOT `verify_sibling_pair … || echo` (#3272 round 21) — that `||` swallowed every failure, so an offline/absent CPU was accepted, affinity silently reduced, manifest wrong: see verify_cpus_online.
verify_cpus_online "$CLIENT_CPUS" "client" || exit 2
verify_disjoint "$SERVER_CPUS" "$CLIENT_CPUS"

# --- THE FLIGHT ARM'S PIN, VERIFIED WITH THE SAME RIGOUR AS THE SERVER'S (#3551) -----------
# Three checks, all fail-closed and all BEFORE the first rep: every CPU present and ONLINE, the
# requested PIN MODE (two affirmative assertions, never a relaxation), and disjointness from the
# client set. `scripts/perf/lib-flight-arm.sh` carries the argument for each; the CALL is here so
# the ORDER stays visible in one file — after the server set is verified, before anything is
# measured. It captures the sysfs echo into `$WS0_FLIGHT_PIN_VERIFIED`, which the pin record
# below carries, so the report's claim rests on an observation.
verify_flight_arm_pin || exit 2


# --- THE COUNTING DOMAIN FOLLOWS THE ARM (#3551) -------------------------------------------
# `perf_stat_c`/`perf_record_c` count CPU-WIDE over `$PERF_COUNT_CPUS`, and the two arms no
# longer necessarily run on the same CPUs. Counting the SERVER set while the Flight server ran on
# a DIFFERENT set would collect cycles from cores that served nothing and divide them by this
# rep's rows — a cycles/row figure of the wrong CPUs, silently, which is the exact defect class
# the sibling check exists to prevent one level down. So each measurement leg sets this to the
# CPUs ITS server actually ran on (`lib-measure.sh`), and it is initialised here so no perf
# invocation can ever see it unset. With the flight pin at its default the value is identical in
# both arms and every argv is byte-for-byte what it is today.
PERF_COUNT_CPUS="$SERVER_CPUS"
# ...AND THE COUNTING DOMAIN IS CHECKED AGAINST A CLOSED SET, NOT LEFT TO CONVENTION (#3551).
#
# `perf stat -C <list>` counting a list the measured work did not run on is a FABRICATED number
# IN THE FLATTERING DIRECTION: pin the Flight server to `2,3` while counting `2,10` and the
# window collects cpu10's IDLE and misses cpu3's WORK entirely, so the same rows cost fewer
# cycles and the arm looks like a large win. Nothing in the output would say so. Getting that
# right by convention is exactly what this rig refuses to rely on, so the wrapper VALIDATES its
# own counting domain against the two pairings this session actually verified — and refuses
# anything else, naming both lists.
#
# The table is `<counted>|<affinity of the process inside the perf window>`, one per line, and
# it is DERIVED from the verified lists rather than written out, so it cannot describe a pin
# nobody checked. Exactly two entries are legitimate, and the second one is why a simple
# "counted == taskset list" rule would be WRONG:
#
#   * BARE SCAN — the window brackets `ws0-scan-bench` on the server set and counts the server
#     set: the measured process runs on the counted CPUs.
#   * FLIGHT — the window brackets the LOAD GENERATOR on the CLIENT set while counting the
#     SERVER's CPUs, deliberately (that is the whole design: the client's cost must stay outside
#     the counted domain). So here the counted list and the argv's `taskset -c` list MUST
#     differ, and requiring them equal would red every correct Flight rep.
#
# With the flight pin at its default the two entries collapse to the pre-#3551 behaviour.
# ONE LINE, with the separator spelled `$'\n'`, deliberately: a continuation line whose first
# token is a bare `"$VAR"` is classified a POSSIBLE perf invocation by `lib-perf-lint.sh`'s
# fail-closed layer 1 (an unresolvable command word could be anything, including perf), so the
# two-line form FAILED the rig's own startup lint — measured, at this very line. Same trap
# `lib-measure.sh` records for its prewarm call.
WS0_PERF_COUNT_PAIRINGS="$SERVER_CPUS|$SERVER_CPUS"$'\n'"$FLIGHT_SERVER_CPUS|$CLIENT_CPUS"

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
  # THE SAMPLER FIRST. A TERM/HUP during a measurement never returns into `perf_stat_c`, so
  # its own cleanup does not run and `perf record` -- which waits on `sleep 86400` -- would be
  # orphaned for 24 hours, still sampling, on a box a later lane will try to measure on.
  # SIGINT rather than SIGKILL so perf finalises its output; `|| true` throughout because a
  # cleanup handler may not fail the run, and `wait` is bounded because the process is a
  # direct child.
  if [[ -n "${_ACTIVE_PROFILER_PID:-}" ]]; then
    echo "cleanup: stopping the active sampling profile (pid $_ACTIVE_PROFILER_PID)" >&2
    kill -INT "$_ACTIVE_PROFILER_PID" 2>/dev/null || true
    wait "$_ACTIVE_PROFILER_PID" 2>/dev/null || true
    _ACTIVE_PROFILER_PID=""
  fi
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
BIN="${BIN_DIR:-$REPO_ROOT/target/release}"
# THE SOURCE DIRECTORY, captured HERE and recorded in the session manifest (#3248).
#
# It must be captured at this line and not later, because `record_measured_binaries` REASSIGNS
# `BIN` to `$OUT_DIR/measured-bin/` (lib-binaries.sh:177) once it has frozen copies of the three
# executables. That freeze is what makes the digests describe the bytes that actually ran — but
# it also means that after it, `$BIN` no longer says which BUILD they came from, and a perfsym
# run and a release run become indistinguishable in results.json. This variable is the only
# record of that distinction, so it is taken before the reassignment can occur.
# The profile directory is CREATED here, below the --validate-args-only boundary, so a
# validation-only run leaves no state behind (its path was already checked above).
# --profile-out IS CLAIMED, NOT JUST CREATED (#3248, roborev job 66 finding 2).
#
# Profile filenames are DETERMINISTIC (`profile-<tag>.data`), so two sessions pointed at one
# directory silently overwrite each other, and a session can validate or attribute another
# session's capture. The rig already refuses a reused `--out` for exactly this reason
# (`require_unused_out_dir`); `--profile-out` had no equivalent, which made the weaker
# half of the pair the one an operator would reach for.
#
# `mkdir` (not `mkdir -p`) on a marker directory is the claim: it is atomic, so of two
# concurrent sessions exactly one wins.
if [[ -n "$PROFILE_OUT" ]]; then
  if ! mkdir -p "$PROFILE_OUT"; then
    echo "FATAL: could not create --profile-out '$PROFILE_OUT'." >&2
    exit 2
  fi
  if ! mkdir "$PROFILE_OUT/.ws0-profile-claim" 2>/dev/null; then
    echo "FATAL: --profile-out '$PROFILE_OUT' is ALREADY CLAIMED by another measurement" >&2
    echo "       session (its .ws0-profile-claim marker exists). Profile filenames are" >&2
    echo "       deterministic, so sharing the directory would overwrite one session's" >&2
    echo "       captures with another's — and a profile attributed to the wrong session is" >&2
    echo "       worse than a missing one. Name an unused directory." >&2
    exit 2
  fi
  if ! find "$PROFILE_OUT" -maxdepth 1 -name 'profile-*.data' -print -quit | grep -q .; then
    :
  else
    echo "FATAL: --profile-out '$PROFILE_OUT' already holds profile-*.data from an earlier" >&2
    echo "       run. Measuring into it would mix two sessions' captures under one name." >&2
    exit 2
  fi
fi
BIN_DIR_RECORDED="$BIN"
# WHETHER A SAMPLING PROFILE WAS ATTACHED, and at what frequency (#3248, roborev job 60
# finding 1). Recorded because `bin_dir` CANNOT establish it: the same symbol-bearing build
# runs with and without `--profile-out`, so a committed artifact claiming bin_dir
# distinguishes a profiled run was WRONG. It matters because a profiled run pays measurable
# observer overhead (measured: 1.6-4.3% on rows/s), so its throughput figures must never be
# read as a baseline -- and results.json is where a reader looks to find that out.
if [[ -n "$PROFILE_OUT" ]]; then
  PROFILE_RECORDED="on freq=$PROFILE_FREQ"
else
  PROFILE_RECORDED="off"
fi
# WHETHER THIS SESSION IS JUDGED FOR QUIESCENCE AT ALL. Recorded either way: a run with no
# timeseries is not "quiet", it is UNVERIFIED, and the difference has to survive into
# results.json or a reader cannot tell a checked run from an unchecked one.
if [[ -n "$QUIESCENCE_TIMESERIES" ]]; then
  QUIESCENCE_RECORDED="judged against $QUIESCENCE_TIMESERIES"
else
  QUIESCENCE_RECORDED="NOT VERIFIED (no timeseries supplied)"
fi
# Existence was already refused above the --validate-args-only boundary; this only reports it.
if [[ -n "$BIN_DIR" ]]; then
  echo "measured bin source: $BIN_DIR (--bin-dir; implies --no-build)"
fi
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

# THE SYMBOL CHECK, AGAIN, ON THE BINARIES THAT WILL ACTUALLY RUN (#3248, roborev job 64
# finding 1 — High).
#
# The pre-boundary check above is necessary but NOT SUFFICIENT, and its insufficiency is the
# exact failure it was written to prevent. It skips a binary that does not exist yet
# (`[[ -e ... ]]`), so on a CLEAN CHECKOUT with `--profile-out` and no `--bin-dir`:
# validation passed (nothing to check), `build_release_binaries` then produced STRIPPED
# binaries because `[profile.release]` sets `strip = true`, and profiling recorded a file
# full of raw addresses — silently, with perf exiting 0. A guard that only fires when the
# subject already exists cannot protect the path that creates the subject.
#
# Checked here on `$BIN`, which `record_measured_binaries` has just repointed at the FROZEN
# copies under measured-bin/ — the bytes this session actually executes, not the ones that
# happened to be in target/ when the arguments were parsed.
if [[ -n "$PROFILE_OUT" ]]; then
  for _b in "${WS0_MEASURED_BINARIES[@]}"; do
    _fsyms=$(nm "$BIN/$_b" 2>/dev/null | grep -c '_RN' || true)
    if [[ "${_fsyms:-0}" -eq 0 ]]; then
      echo "FATAL: --profile-out was given, but the FROZEN binary $BIN/$_b carries no Rust" >&2
      echo "       symbols, so a sampling profile of it would attribute nothing." >&2
      echo "       [profile.release] sets strip = true, so the default build cannot be" >&2
      echo "       profiled. Build a symbol-bearing profile and pass --bin-dir:" >&2
      echo "         cargo build --profile perfsym -p ws0-corpus-gen -p cqlite-flight -p flight-loadgen" >&2
      exit 2
    fi
  done
  echo "profile symbols: verified on all ${#WS0_MEASURED_BINARIES[@]} frozen binaries"
fi

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
WS0_CFG_FLIGHT_SERVER_CPUS="$FLIGHT_SERVER_CPUS" \
WS0_CFG_ENV_AMBIENT="$WS0_ENV_AMBIENT" \
WS0_CFG_ENV_INJECTED="$WS0_ENV_INJECTED" \
WS0_CFG_STEP_DURATION="$STEP_DURATION/$COLD_STEP_DURATION" \
WS0_CFG_FLIGHT_ENDPOINT="$FLIGHT_ENDPOINT" \
WS0_CFG_BASELINE_MODE="$BASELINE_MODE" \
WS0_CFG_EVENTS="$EVENTS" \
WS0_CFG_BIN_DIR="$BIN_DIR_RECORDED" \
WS0_CFG_PROFILE="$PROFILE_RECORDED" \
WS0_CFG_QUIESCENCE="$QUIESCENCE_RECORDED" \
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
# NO BACKSLASH-ESCAPED QUOTES INSIDE AN f-STRING EXPRESSION, and no nested same-type quotes
# either. Both are traps here, for different reasons, and the first one was a LIVE BUG that
# made this whole step raise SyntaxError before it could pin anything (#3248):
#   * `f"{pin[\"k\"]}"` — a backslash inside the expression part is a SyntaxError on EVERY
#     CPython to date, including 3.12: the tokenizer reads the backslash as a line
#     continuation ("unexpected character after line continuation character").
#   * `f"{pin["k"]}"` — nested same-type quotes are legal only from 3.12 (PEP 701), so using
#     them would silently move the failure onto older interpreters instead of removing it.
# Binding the values to locals first sidesteps both and works on any version. This step is
# FATAL when it fails, so a syntax error here blocked the entire measurement path.
_sha = pin["data_db_sha256"]
_rows = pin["rows"]
_bytes = pin["data_db_bytes"]
_ncomp = len(pin["components"])
_reps = config["reps"]
_temps = config["temps"]
_arms = config["arms"]
_passes = config["scan_passes"]
_canon = canonical["label"]
print(f"corpus pin:   {_sha} ({_rows} rows / {_bytes} B, {_ncomp} components)"
      " recorded in session-corpus-pin.json BEFORE the first rep")
print(f"config pin:   reps={_reps} temps=[{_temps}] arms=[{_arms}]"
      f" scan-passes={_passes} — the reporter READS these, never its own argv")
print(f"canonical pin: {_canon} — recorded in session-corpus-pin.json"
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
WS0_PIN_FLIGHT_SERVER_CPUS="$FLIGHT_SERVER_CPUS" \
WS0_PIN_FLIGHT_PIN_MODE="$FLIGHT_PIN_MODE" \
WS0_PIN_FLIGHT_PIN_VERIFIED="$WS0_FLIGHT_PIN_VERIFIED" \
WS0_PIN_FLIGHT_ALLOCATOR="$FLIGHT_ALLOCATOR" \
WS0_PIN_FLIGHT_ALLOCATOR_LIB="$FLIGHT_ALLOCATOR_LIB_RECORDED" \
WS0_PIN_FLIGHT_MALLOC_ARENA_MAX="$FLIGHT_ARENA_RECORDED" \
WS0_PIN_FLIGHT_ALLOCATOR_VERIFICATION="$FLIGHT_ALLOCATOR_VERIFICATION" \
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
    # THE FLIGHT ARM (#3551). Recorded because the report prints a claim about each of them, and
    # because a value that exists in no artifact is a claim resting on the operators memory of
    # what they typed. `flight_server_cpus` is additionally compared against the MANIFEST by
    # `ws0_pinning.verify_pinning_record`, which is the F6 substitution check extended to the
    # new pin: a manifest edited to name CPUs no verification ran against is refused rather than
    # printed as verified.
    "flight_server_cpus": os.environ["WS0_PIN_FLIGHT_SERVER_CPUS"],
    "flight_pin_mode": os.environ["WS0_PIN_FLIGHT_PIN_MODE"],
    # The sysfs ANSWER for the flight pin — `verify_sibling_pair`s or `verify_distinct_cores`s
    # own output line, carrying the expanded sibling sets it read. The substance of the
    # verification, not a restatement of the argument.
    "flight_pin_verified": os.environ["WS0_PIN_FLIGHT_PIN_VERIFIED"],
    "flight_allocator": os.environ["WS0_PIN_FLIGHT_ALLOCATOR"],
    "flight_allocator_lib": os.environ["WS0_PIN_FLIGHT_ALLOCATOR_LIB"],
    "flight_malloc_arena_max": os.environ["WS0_PIN_FLIGHT_MALLOC_ARENA_MAX"],
    "flight_allocator_verification": os.environ["WS0_PIN_FLIGHT_ALLOCATOR_VERIFICATION"],
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
# Locals first — same trap as the corpus-pin print above: a backslash inside an f-string
# expression is a SyntaxError on every CPython, and this step is fatal when it fails.
_scpus = rec["server_cpus"]
_troot = rec["topology_root"]
_host = rec["host"]
print(f"pinning pin:  {_scpus} verified against"
      f" {_troot} on {_host} — recorded in {p.name} so the report cites an"
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
# perf_record_c <outfile> — THE SECOND SANCTIONED perf INVOCATION (#3248 AC1).
#
# It lives in THIS file, beside `perf_stat_c`, for the reason lib-measure.sh records about the
# counting wrapper: `perf_invocation_lint_tree` discovers wrapper ownership by grepping this
# file, lints exactly the owner in `owner` mode and every other `scripts/perf/*.sh` in
# `library` mode — where DEFINING a wrapper is itself a finding. A record wrapper in a library
# would invert layer 1 of the guard.
#
# CPU-WIDE ONLY, on the same verified sibling pair the counting window uses. `-C` is supplied
# here and is separately ASSERTED by the lint's END checks: a sampling profile pinned to
# nothing samples whichever CPUs the scheduler happened to use, which is not the measured arm.
# No caller-supplied options are accepted, exactly as in `perf_stat_c`.
#
# It samples until SIGINT, which `perf_stat_c` sends when the counting window closes, so the
# profile covers EXACTLY the counted window rather than a guessed duration. perf finalises the
# file on SIGINT; a guessed `sleep` would either truncate the profile or run past the window
# and sample the teardown.
perf_record_c() {
  local outfile="$1"
  # `exec`, NOT a plain call, AND THIS IS THE WHOLE CORRECTNESS OF THE HOOK.
  #
  # The caller runs this function BACKGROUNDED, so `$!` is the PID of the SUBSHELL bash forked
  # for it — not of perf. Signalling that subshell kills the subshell and orphans perf, which
  # then never finalises its output: the first version of this hook produced a 31 MB file whose
  # `data size` header field was ZERO, and `perf report` refused it with "Was the 'perf record'
  # command properly terminated?". Every profile of that run was unusable, and nothing in the
  # measurement said so — the rig exited fine and the files existed at a plausible size.
  # `exec` REPLACES the subshell with perf, so `$!` IS perf and the caller's SIGINT reaches it.
  #
  # NO `-g`. Call-graph collection is deliberately absent, because this rig's profiling profile
  # is `perfsym` — symbols WITHOUT frame pointers, chosen so codegen matches `release`. Frame
  # pointers are the ONLY call-graph mechanism that works on this host (dwarf unwinding hangs
  # past 120s on a binary this size; LBR is unavailable on this KVM guest, measured), so asking
  # for stacks from a build that has no frame pointers yields unreliable ones. AC1's headline
  # figures are FLAT SELF-TIME, which needs the sample IP and nothing else. Call-graph evidence
  # comes from a separate, explicitly PERTURBED frame-pointer build and is reported as structural
  # only. See docs/reports/ws0-3248-artifacts/raw/callgraph-capability-census.md.
  #
  # Defaults on the driver globals, same standalone-extraction rule as perf_stat_c below: two
  # suites text-extract these functions and run them under `set -u`.
  exec perf record -e cycles -F "${PROFILE_FREQ:-499}" -C "$PERF_COUNT_CPUS" -o "$outfile" -- sleep 86400
}

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
  # --- THE COUNTING DOMAIN, VALIDATED HERE (#3551) ------------------------------------------
  # After the option allowlist above, so an argv-guard case still gets the argv diagnostic, and
  # BEFORE the sampler starts, so `perf record` inherits a domain that has been checked (that is
  # the answer to "does the --profile-out path need the same treatment": it reads the SAME
  # variable and is started below this point, so one validation covers both invocations).
  #
  # `${VAR:-}` here is NOT a permissive default: an empty value is REFUSED two lines down. It
  # exists so an unset variable produces this NAMED diagnostic instead of bash's
  # unbound-variable error, and there is deliberately no fall-back to `$SERVER_CPUS` — a silent
  # default is precisely how this defect would survive its own fix.
  local _counted="${PERF_COUNT_CPUS:-}" _pairings="${WS0_PERF_COUNT_PAIRINGS:-}"
  local _aff="" _want_c=0 _tok _pair
  if [[ -z "$_counted" ]]; then
    echo "FATAL: perf_stat_c was called with no counting domain (\$PERF_COUNT_CPUS is empty or" >&2
    echo "       unset). Each measurement leg sets it to the CPUs ITS OWN server runs on" >&2
    echo "       immediately before this call; there is no default, because counting the wrong" >&2
    echo "       CPUs fabricates a number in the flattering direction rather than failing" >&2
    echo "       (#3551)." >&2
    exit 2
  fi
  if [[ -z "$_pairings" ]]; then
    echo "FATAL: perf_stat_c has no verified counting-domain table (\$WS0_PERF_COUNT_PAIRINGS is" >&2
    echo "       empty or unset), so '$_counted' cannot be checked against the pins this session" >&2
    echo "       VERIFIED. The driver derives that table from the verified lists before the first" >&2
    echo "       rep. Refused rather than assumed: an unchecked domain is the defect (#3551)." >&2
    exit 2
  fi
  # The affinity of the process this window brackets, read out of THIS call's argv — never from a
  # global, because the pairing being checked is between the counted list and what is about to
  # RUN. Bash has already done word-splitting and quote removal, so the spelling problem a
  # source scan would have does not exist here.
  for _tok in "$@"; do
    if [[ "$_want_c" == "2" ]]; then _aff="$_tok"; break; fi
    if [[ "$_want_c" == "1" && "$_tok" == "-c" ]]; then _want_c=2; continue; fi
    case "$_tok" in */taskset|taskset) _want_c=1 ;; esac
  done
  if [[ -z "$_aff" ]]; then
    echo "FATAL: perf_stat_c cannot tell WHERE the command it is about to measure will run: its" >&2
    echo "       argv carries no 'taskset -c <list>'. The counting domain ('$_counted') is" >&2
    echo "       therefore unverifiable against it, and an unverifiable pairing is refused" >&2
    echo "       rather than assumed correct (#3551). Every leg in this rig pins its command." >&2
    echo "       The argument list was: $*" >&2
    exit 2
  fi
  local _ok=0
  while IFS= read -r _pair; do
    [[ -n "$_pair" ]] || continue
    [[ "$_pair" == "$_counted|$_aff" ]] && { _ok=1; break; }
  done <<<"$_pairings"
  if [[ "$_ok" != "1" ]]; then
    echo "FATAL: perf stat would COUNT cpus '$_counted' while the command it brackets is pinned" >&2  # perf-lint-allow: a diagnostic STRING
    echo "       to '$_aff', and that pairing is not one this session verified." >&2
    echo "       The verified pairings (<counted>|<measured-process affinity>) are:" >&2
    printf '         %s\n' $_pairings >&2
    echo "       This is the #3551 defect and it fails CLOSED because it CANNOT be seen in the" >&2
    echo "       output: counting a list the work did not run on collects the other CPUs' IDLE" >&2
    echo "       and misses the work entirely, so the same rows cost FEWER cycles and the arm" >&2
    echo "       reads as a large win. Fix the leg that set \$PERF_COUNT_CPUS, not this check." >&2
    exit 2
  fi

  # THE SAMPLING SESSION BRACKETS EXACTLY THIS WINDOW (#3248 AC1). Started here rather than in
  # the measurement legs because this function already IS the timed window: any other insertion
  # point would need to guess the window's duration, and a guess either truncates the profile or
  # samples the teardown.
  #
  # The setup-only leg is deliberately NOT profiled: its cycles are SUBTRACTED from the reported
  # figure, so including it in the profile would attribute corpus-open and schema-ingest work to
  # the per-row region the profile exists to describe.
  # `${PROFILE_OUT:-}`, NOT `$PROFILE_OUT`, and this is not defensive style — it is required.
  # `scripts/tests/test_ws0_cpu_pinning_guards.sh` and the invocation-lint self-test EXTRACT this
  # function by text (`awk '/^perf_stat_c\(\)/,/^}/'`) and run it standalone under `set -u`, so a
  # bare reference to a DRIVER global dies with an unbound-variable error instead of producing the
  # function's diagnostic. That is the exact cross-file coupling this rig already documents for
  # `$COLD_STEP_MAX_MS` in lib-args.sh — and the first version of this profiling hook
  # reintroduced it, breaking two argv-guard cases that had nothing to do with profiling.
  local _prof_pid=""
  if [[ -n "${PROFILE_OUT:-}" && "$outfile" != *-setup.csv ]]; then
    local _ptag
    _ptag="$(basename "$outfile" .csv)"
    perf_record_c "$PROFILE_OUT/profile-$_ptag.data" >"$PROFILE_OUT/profile-$_ptag.stderr" 2>&1 &
    _prof_pid=$!
    # Published at file scope BEFORE the window opens, so a signal arriving at any point
    # during the measurement finds a PID to clean up.
    _ACTIVE_PROFILER_PID="$_prof_pid"
    # DEFERRED DEFECT, MEASURED: THE PROFILE WINDOW AND THE cyc/row DENOMINATOR COVER DIFFERENT
  # REGIONS. (#3248 roborev job 84 F1 + job 86 F1; follow-up
  # https://github.com/pmcfadin/cqlite/issues/3469 family 3.)
  #
  # The profiler attaches to the FULL counted window -- the guard below skips only `*-setup.csv`
  # -- and it opens 300 ms BEFORE the window (the arming delay documented immediately below).
  # The reported cycles/row is setup-subtracted scan (`cycles_scan = cycles_total - cycles_setup`,
  # ws0_collect.py). So profile SHARES are fractions of a window that includes 300 ms of
  # pre-window capture plus the setup leg, and they are multiplied by a setup-EXCLUSIVE total.
  # Buckets are therefore UNDERSTATED by the contaminated fraction of their own arm.
  #
  # BOTH TERMS, MEASURED. An earlier version of this comment carried only the setup term and
  # therefore understated the defect by ~150x -- the arming delay is the dominant term and it is
  # ASYMMETRIC between the two arms being differenced:
  #
  #                          bare_scan          flight_bypass
  #   setup leg             0.0164-0.0171%      n/a (no setup subtraction on this arm)
  #   300 ms arming         2.47-2.52%          0.48-0.51%
  #   combined              ~2.49%              ~0.49%      => ~2.0 pp differential bias
  #
  # WHICH PUBLISHED FIGURES THIS CAN MOVE -- checked per result, not asserted (#3469 family 3
  # carries the arithmetic). Every conclusion survives; ONE figure moves in its second digit:
  #   * the +21.5% shared-bucket excess -> ~+19.0% (-2.4 pp). Direction and rough size hold.
  #   * everything counter-derived is IMMUNE: the +6,707 gap, the 5.19x L2 bytes-touched result
  #     (l2_lines_in x 64B / rows, different events, different run), and all of AC0, which was
  #     run with NO profiler attached.
  #   * the HashMap-lever result is robust because its denominators come from the UNPROFILED
  #     control: gains shift <0.2 pp and the ratio INVERSION -- which is the actual conclusion --
  #     holds either way (1.4107x -> 1.4430x published, -> 1.4439x corrected).
  #   * the lever ceiling shifts by 0.0009x against a claim with ~0.2x of slack.
  #
  # A fix converts shares against `cycles_total` and subtracts setup from the buckets, or brackets
  # the profile to the counted window exactly -- but the arming delay makes exact bracketing
  # impossible in one direction or the other, which is why the delay is documented below rather
  # than eliminated.
  #
  # ARMING DELAY, AND THE COST IS STATED RATHER THAN CLAIMED AWAY (#3248, roborev job 69
    # finding 1). The sampler needs a moment to arm, so without this the first fraction of the
    # window is UNSAMPLED and the profile under-represents whatever runs first. With it, the
    # profile instead includes ~300 ms of samples from BEFORE the counting window opens.
    #
    # So the earlier claim that the profile "brackets EXACTLY the counted window" was WRONG in
    # one direction or the other, and there is no setting that makes it true: one side of the
    # boundary is always slightly off. 300 ms against this rig's 5-45 s windows is 0.7-6% of the
    # capture, and it lands on server startup/steady-state rather than on the encode region.
    # The direction chosen is the one that cannot silently DROP the region under study.
    sleep 0.3
  fi
  # `|| _rc=$?`, NOT a bare call followed by `$?`, AND THIS IS A RESOURCE-LEAK FIX.
  #
  # This driver runs `set -euo pipefail`, and every `perf_stat_c` call site in
  # lib-measure.sh (:150, :156, :250) is BARE -- not in a condition, not followed by `||`.
  # So `set -e` is live inside this function, and before the profiling hook existed that was
  # harmless: `perf stat` was the LAST command, so its status simply became the function
  # status. Adding cleanup after it changed that: a failing `perf stat` now exits the shell
  # AT THAT LINE, so the SIGINT below never runs and `perf record` is ORPHANED -- still
  # sampling, against `sleep 86400`, for 24 hours. VERIFIED by execution: with a bare call
  # under `set -e` the cleanup line does not run at all.
  #
  # Testing the status with `||` suppresses `set -e` for this command only, so the cleanup
  # always runs and the measured status is still propagated by `return $_rc` below.
  local _rc=0
  perf stat -x, -e "$EVENTS" -C "$PERF_COUNT_CPUS" -o "$outfile" -- "$@" || _rc=$?
  if [[ -n "$_prof_pid" ]]; then
    # SIGINT, not SIGKILL: perf finalises perf.data on INT and leaves an unreadable stub on KILL.
    # THE PROFILER MUST STILL HAVE BEEN RUNNING, AND ITS EXIT STATUS IS READ (#3248, roborev
    # job 68 finding 3).
    #
    # The previous version signalled and waited with `|| true` on both, so a `perf record`
    # that DIED EARLY was indistinguishable from one that covered the whole window: it had
    # written some valid data, so `data.size` was nonzero and the header check passed, and a
    # TRUNCATED profile was accepted as complete. `data.size != 0` establishes that perf
    # finalised SOMETHING, not that it sampled the window it was asked to.
    #
    # `kill -0` first: if the process is already gone, the signal would have been a no-op and
    # the "we stopped it cleanly" story is false.
    local _prof_was_alive=1
    kill -0 "$_prof_pid" 2>/dev/null || _prof_was_alive=0
    kill -INT "$_prof_pid" 2>/dev/null || true
    local _prof_status=0
    wait "$_prof_pid" || _prof_status=$?
    _ACTIVE_PROFILER_PID=""
    if [[ "$_prof_was_alive" -eq 0 ]]; then
      echo "FATAL: the sampling profiler had ALREADY EXITED before the counting window closed," >&2
      echo "       so its profile covers only part of the window it was asked to sample -- and" >&2
      echo "       a partial profile is indistinguishable from a complete one by file size or" >&2
      echo "       by the perf.data header alone. Its stderr:" >&2
      sed 's/^/         /' "$PROFILE_OUT/profile-$_ptag.stderr" >&2 2>/dev/null || true
      return 2
    fi
    # THE GATE IS perf's OWN SUCCESS LINE, NOT AN ENUMERATION OF EXIT STATUSES.
    #
    # The first version of this check accepted {0, 130} as the affirmative set, reasoning that
    # SIGINT termination reports 128+2. THAT REASONING WAS WRONG AND IT FAILED A CORRECT RUN:
    # perf actually exits **143** here (128+SIGTERM), because on SIGINT it stops the session,
    # writes its data, then terminates its own child (`sleep`) with SIGTERM. Measured directly:
    # status 143 alongside `Captured and wrote 2.278 MB ... (11704 samples)` — a complete,
    # successful capture. I derived an "affirmative set" by reasoning instead of measuring it,
    # which is the error this issue exists to catch, committed inside a check written to catch it.
    #
    # So the gate is now the AFFIRMATIVE EVIDENCE perf itself prints on success, and the exit
    # status is RECORDED rather than adjudicated — because I have just demonstrated that I
    # cannot reliably enumerate its values, and a check keyed on an enumeration I get wrong
    # fails correct runs. `Captured and wrote` appears only after perf has flushed its data.
    if ! grep -q 'Captured and wrote' "$PROFILE_OUT/profile-$_ptag.stderr" 2>/dev/null; then
      echo "FATAL: the sampling profiler never reported a completed capture (its stderr has no" >&2
      echo "       'Captured and wrote' line), so nothing establishes that it flushed the data" >&2
      echo "       it collected. Exit status was $_prof_status. Its stderr:" >&2
      sed 's/^/         /' "$PROFILE_OUT/profile-$_ptag.stderr" >&2 2>/dev/null || true
      return 2
    fi
    # `>&2`, LIKE EVERY OTHER DIAGNOSTIC IN THIS FUNCTION. `perf_stat_c`'s STDOUT IS THE
    # MEASURED COMMAND'S OUTPUT CHANNEL -- the rep's payload JSON is written through it -- so
    # an informational line on stdout is APPENDED TO THE DATA. The first version of this echo
    # did exactly that and corrupted scan-warm-1.json into two JSON documents, which surfaced
    # as `JSONDecodeError: Extra data` in the reporter, three layers away from the cause.
    # A diagnostic and a data stream must not share a channel.
    echo "profile: $_ptag captured (profiler exit status $_prof_status, capture confirmed)" >&2
    # AND VERIFY IT FINALISED. An unterminated `perf record` leaves a file of plausible SIZE
    # whose `data size` header is 0, which `perf report` refuses — so the failure is invisible
    # at the filesystem level and only appears when someone tries to read the profile, possibly
    # days later. Checked here, where the run can still be repeated.
    # VERIFY IT FINALISED, by reading the perf.data header's `data.size` field directly.
    #
    # TWO THINGS HERE WERE ESTABLISHED BY POSITIVE CONTROL, not by reasoning, and the first
    # version of this check would have failed both ways:
    #
    #  1. `perf report --header-only` ACCEPTS AN UNFINALISED FILE. Measured against a
    #     deliberately SIGKILLed capture: it exits 0 on a file whose data is unreadable, so a
    #     readback through perf would have passed the exact case it was written to catch.
    #  2. `data.size` at byte offset 48 of the header IS the discriminating field: SIGKILLed
    #     capture -> 0; the same capture ended with SIGINT -> 114,600. That is the field perf's
    #     own warning names ("the file's data size field is 0 which is unexpected").
    #
    # So the check is a direct header read, and no third perf subcommand is introduced — which
    # also keeps the invocation allowlist to the two subcommands this rig actually needs.
    local _pf="$PROFILE_OUT/profile-$_ptag.data"
    if ! python3 -c '
import struct, sys
path = sys.argv[1]
try:
    with open(path, "rb") as fh:
        head = fh.read(56)
except OSError as exc:
    print(f"cannot read {path}: {exc}", file=sys.stderr); raise SystemExit(1)
if len(head) < 56 or head[:8] != b"PERFILE2":
    print(f"{path} is not a perf.data file", file=sys.stderr); raise SystemExit(1)
if struct.unpack_from("<Q", head, 48)[0] == 0:
    print(f"{path} data.size == 0", file=sys.stderr); raise SystemExit(1)
' "$_pf" 2>/dev/null; then
      echo "FATAL: the sampling profile $_pf did not finalise — its perf.data header records" >&2
      echo "       data.size == 0, so perf was terminated before it could write the data it" >&2
      echo "       had collected. The file exists at a plausible SIZE, which is why this is" >&2
      echo "       checked rather than assumed: the failure is invisible on the filesystem" >&2
      echo "       and surfaces only when someone tries to read the profile." >&2
      echo "       This is a profiling-hook defect, not a measurement result." >&2
      # If the MEASUREMENT also failed, propagate THAT status rather than masking it with
      # this one: the measurement failure is the more actionable of the two, and returning a
      # fixed 2 would hide it.
      if [[ "$_rc" -ne 0 ]]; then
        echo "       (the measured command ALSO failed, status $_rc — reporting that)" >&2
        return "$_rc"
      fi
      return 2
    fi
  fi
  return $_rc
}

# ---------------------------------------------------------------------------
# The two MEASUREMENT LEGS live in scripts/perf/lib-measure.sh (#3272 round 9)
# ---------------------------------------------------------------------------
# `measure_scan` (arm A) and `measure_flight` (arm B, do_get over a real loopback transport)
# were split out under the campsite rule; that library carries the full argument for each leg
# and for why `perf_stat_c` deliberately did NOT move with them. Sourced at the TOP of this
# file, after lib-server.sh, because the sourcing order is the dependency order: these legs
# call stop_server/require_port_free/await_server_ready. Call sites: the loop below.

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
# all of arm 2 — making each arm's median a measurement of a DIFFERENT TIME WINDOW, which
# this rig's own recorded evidence says are not comparable: on the delivery box, in one
# session, the UNTOUCHED warm bare scan read 370,134 rows/s at 05:06 UTC and 333,206 at
# 06:05, a ~10% drift with nothing changed on the measured path. The whole claim this
# driver produces is the `bare/flight` RATIO, so a drift between the bare-scan block and
# the Flight block lands DIRECTLY on it and on the 1.3x PASS/BELOW-TARGET verdict.
#
# So: ROUNDS on the outside, arms on the inside, order rotated by round index, with the
# BARE SCAN AS ONE OF THE ROTATED ARMS rather than leading every round (it is the
# DENOMINATOR of the ratio, so a fixed position would put any within-round systematic
# effect on it every time). For the 2-arm default that is a genuine alternation rather
# than a fixed order. Per-rep artifact names are unchanged.
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
# At the spreads this rig measures (5-10% per arm) a couple of percent of median difference
# is not readable: the recorded #3096 session's +2.3% median difference re-measured at ZERO
# (median −0.03%, 4 of 8 rounds positive) over 8 rounds.
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
# one interpreter start per rep — tens of milliseconds against a 45-second step. The instant
# recorded is the rep's COMPLETION, and the loop is strictly sequential, so completion order
# IS the order the reps ran — and a rep that died leaves NO metadata rather than a start time
# for a measurement that never finished.
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

# THE QUIESCENCE BOUNDARY, OPENING SIDE (#3248). Taken here rather than at startup so it
# brackets the MEASUREMENT rather than the build: a cargo build before rep 1 is this session's
# own load and would read as contamination of its own window.
#
# DELIBERATELY ABOVE `_ARM_LIST=`, not between it and the loop. `test_ws0_round_metadata.sh`
# extracts the rotation loop by TEXT -- `awk '/^_ARM_LIST=/,/^done$/'` -- and evals it in a
# harness where this driver-scoped state does not exist, so code placed inside that range
# breaks four rotation checks with an empty `order:`. That is the same text-extraction coupling
# this rig records for `perf_stat_c` and `$COLD_STEP_MAX_MS`, and the first version of this
# block hit it.
_QUIESCENCE_WINDOW_START=""
if [[ -n "$QUIESCENCE_TIMESERIES" ]]; then
  _QUIESCENCE_WINDOW_START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  python3 "$HERE/ws0_quiescence.py" sample --out "$OUT_DIR/quiescence-before.json" \
    || { echo "FATAL: could not take the opening quiescence sample." >&2; exit 2; }
fi

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
      # THE MEASUREMENT BOUNDARY (#3272 round 22), per ARM-rep. Status checked EXPLICITLY, so
      # `|| exit 1` refuses the run. Full argument: lib-corpus-boundary.sh.
      verify_corpus_boundary_or_refuse "$temp-$rep-after-$arm" || exit 1
    done
  done
done

# THE QUIESCENCE BOUNDARY, CLOSING SIDE, AND THE JUDGEMENT (#3248).
#
# WIRED HERE because a gate nothing calls is not a gate: roborev job 62 finding 2 caught that
# ws0_quiescence.py shipped with no caller, so ordinary runs could still publish results from a
# contaminated window while the tool sat in the tree looking like protection. It runs BEFORE
# the report, so a contaminated session produces no report at all rather than a report with a
# caveat -- the numbers from a contaminated window are not worth publishing with a footnote.
if [[ -n "$QUIESCENCE_TIMESERIES" ]]; then
  _quiescence_window_end="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  python3 "$HERE/ws0_quiescence.py" sample --out "$OUT_DIR/quiescence-after.json" \
    || { echo "FATAL: could not take the closing quiescence sample." >&2; exit 2; }
  if ! python3 "$HERE/ws0_quiescence.py" judge \
        --before "$OUT_DIR/quiescence-before.json" \
        --after "$OUT_DIR/quiescence-after.json" \
        --timeseries "$QUIESCENCE_TIMESERIES" \
        --window-start "$_QUIESCENCE_WINDOW_START" \
        --window-end "$_quiescence_window_end" \
        --out "$OUT_DIR/quiescence-verdict.json"; then
    echo "FATAL: this session is NOT certified quiescent, so no report is produced." >&2
    echo "       The measured windows overlapped competing load, which moves frequency by up" >&2
    echo "       to 25% (measured, #3299) — the figures would be plausible and wrong." >&2
    echo "       Re-run on a quiet box; the verdict cause above says what was seen." >&2
    exit 2
  fi
  echo "quiescence:   CERTIFIED — see $OUT_DIR/quiescence-verdict.json"
else
  echo "quiescence:   NOT VERIFIED — no --quiescence-timeseries was supplied, so nothing"
  echo "              establishes this session did not overlap competing load. Recorded as"
  echo "              such in the session manifest; it is not a claim of quietness."
fi

# The reporter takes ONLY the two paths: everything else is read from the session manifest
# stamped above (#3272 F1). Passing `--reps`/`--temps`/`--arms`/`--scan-passes`/the CPU pins
# here would be the substitution the manifest exists to prevent, so those flags no longer
# exist — an accepted-but-ignored flag is a silent lie to whoever passed it.
python3 "$HERE/ws0_report.py" --dir "$OUT_DIR" --corpus "$CORPUS" \
  | tee "$OUT_DIR/summary.txt"

echo
echo "machine-readable: $OUT_DIR/results.json"
echo "human summary:    $OUT_DIR/summary.txt"
