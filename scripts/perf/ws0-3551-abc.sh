#!/usr/bin/env bash
# ws0-3551-abc.sh — the interleaved A/B/C(/C0) driver for issue #3551.
#
# WHY THIS EXISTS. `ws0-baseline.sh` measures ONE configuration per invocation, so an A/B/C
# comparison is a SET of its sessions and the interleaving is a property of HOW they are
# ordered. `docs/reports/ws0-3096-artifacts/measurement-method.md` §3b requires, verbatim:
# one rep at a time, never all reps of an arm back to back (step 1); the arm order rotated
# every round (step 2); the drift control carried in EVERY run (step 3); differences taken
# WITHIN a round and the direction count reported (step 4); rows/s AND cycles/row AND IPC per
# run (step 5). §3b.1 states plainly that the committed rig implements NONE of that and makes
# no interleaving claim. This script is that operator obligation, written down and runnable
# instead of performed by hand and asserted afterwards.
#
# WHAT IT CLAIMS AND WHAT IT DOES NOT. It claims the ORDER IT EXECUTED, because it executed
# it: the rotation is computed here and every session's position is recorded here. It does NOT
# claim the box was quiet — that is `ws0_quiescence.py`'s job, passed through per session — and
# it does not claim the arms differ only as labelled; each session's own recorded pinning is
# the authority for that, which is why the aggregator reads configuration back OUT of the
# artifacts rather than restating this file's table.
#
# THE CONTROL, which is the whole reason the arms are shaped this way. Only `--flight-server-cpus`
# and the allocator knobs vary; `--server-cpus` is IDENTICAL in every arm, so the bare-scan leg
# is code-identical AND pin-identical everywhere and its movement across arms is drift plus
# contamination and nothing else. That is §3b step 3's control. Vary `--server-cpus` per arm and
# you lose it — the bare scan becomes a second treatment and there is nothing left to read the
# first one against.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CORPUS=""
BIN_DIR=""
OUT=""
ROUNDS=3
STEP_DURATION="45s"
QUIESCENCE_TS=""
JEMALLOC_LIB=""
ARENA_MAX=2
PORT=18815
# The pins. Arm A is the #3096/#3248 configuration verbatim; B/C0/C move ONE cpu of the flight
# pin off its sibling and onto a second physical core. Both are TWO logical CPUs, so the
# admission ceiling `clamp(2 x available_parallelism, 2, 64)` is unmoved — asserted from each
# server's own log by ws0-baseline.sh, never assumed here.
PIN_A="2,10"
PIN_B="2,3"

usage() {
  cat <<EOF
ws0-3551-abc.sh — issue #3551 interleaved SMT-unpin + allocator trial

  --corpus DIR       ws0-corpus-gen corpus root. REQUIRED.
  --bin-dir DIR      ONE frozen binary set measured by EVERY arm. REQUIRED, and required to be
                     one directory: the arms must not differ in their binaries (#3248 withdrew a
                     machine-code claim for exactly that reason), so this is deliberately not
                     per-arm.
  --out DIR          Where the r<N>-<arm>/ session dirs go. REQUIRED. A (round, arm) that
                     already holds a results.json is SKIPPED, so an interrupted set resumes
                     instead of starting over — which matters on a shared box.
  --rounds N         Rounds; each round runs every arm once, order rotated (default $ROUNDS).
  --step-duration D  Flight loadgen step hold per rep (default $STEP_DURATION).
  --arena-max N      MALLOC_ARENA_MAX for arm C0 (default $ARENA_MAX).
  --jemalloc-lib P   Passed through for arm C on a host with a non-standard path.
  --quiescence-timeseries F
                     Passed to every session. Its ABSENCE is recorded by ws0-baseline.sh as
                     'quiescence: NOT VERIFIED', so omitting it cannot look verified.
  --port N           Loopback port (default $PORT).
  -h, --help         This text.

Arms: A=$PIN_A siblings/system · B=$PIN_B distinct-cores/system
      C0=$PIN_B distinct-cores/system + MALLOC_ARENA_MAX · C=$PIN_B distinct-cores/jemalloc
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --corpus) CORPUS="${2:-}"; shift 2 ;;
    --bin-dir) BIN_DIR="${2:-}"; shift 2 ;;
    --out) OUT="${2:-}"; shift 2 ;;
    --rounds) ROUNDS="${2:-}"; shift 2 ;;
    --step-duration) STEP_DURATION="${2:-}"; shift 2 ;;
    --arena-max) ARENA_MAX="${2:-}"; shift 2 ;;
    --jemalloc-lib) JEMALLOC_LIB="${2:-}"; shift 2 ;;
    --quiescence-timeseries) QUIESCENCE_TS="${2:-}"; shift 2 ;;
    --port) PORT="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "FATAL: unknown argument $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$CORPUS" ]]  || { echo "FATAL: --corpus is required" >&2; exit 2; }
[[ -n "$BIN_DIR" ]] || { echo "FATAL: --bin-dir is required" >&2; exit 2; }
[[ -n "$OUT" ]]     || { echo "FATAL: --out is required" >&2; exit 2; }
[[ "$ROUNDS" =~ ^[1-9][0-9]*$ ]] || { echo "FATAL: --rounds must be a positive integer, got '$ROUNDS'" >&2; exit 2; }
[[ "$ARENA_MAX" =~ ^[1-9][0-9]*$ ]] || { echo "FATAL: --arena-max must be a positive integer, got '$ARENA_MAX'" >&2; exit 2; }
[[ -d "$CORPUS" ]]  || { echo "FATAL: --corpus '$CORPUS' is not a directory" >&2; exit 2; }
[[ -d "$BIN_DIR" ]] || { echo "FATAL: --bin-dir '$BIN_DIR' is not a directory" >&2; exit 2; }

mkdir -p "$OUT"

ARMS=(A B C0 C)

arm_flags() {
  # The one place an arm's identity is defined. Printed into the run record below AND read back
  # out of each session's own recorded pinning by the aggregator, so a divergence between what
  # this table says and what was measured is detectable rather than assumed away.
  case "$1" in
    A)  printf '%s\n' --flight-server-cpus "$PIN_A" --flight-pin-mode siblings --flight-allocator system ;;
    B)  printf '%s\n' --flight-server-cpus "$PIN_B" --flight-pin-mode distinct-cores --flight-allocator system ;;
    C0) printf '%s\n' --flight-server-cpus "$PIN_B" --flight-pin-mode distinct-cores --flight-allocator system --flight-malloc-arena-max "$ARENA_MAX" ;;
    C)  printf '%s\n' --flight-server-cpus "$PIN_B" --flight-pin-mode distinct-cores --flight-allocator jemalloc ;;
    *)  echo "FATAL: unknown arm '$1'" >&2; return 2 ;;
  esac
}

echo "== #3551 interleaved A/B/C =="
echo "corpus:   $CORPUS"
echo "bins:     $BIN_DIR"
echo "out:      $OUT"
echo "rounds:   $ROUNDS   arms: ${ARMS[*]}"
echo "control:  bare scan pinned to --server-cpus (IDENTICAL in every arm) — method §3b step 3"
echo

n=${#ARMS[@]}
for ((r = 1; r <= ROUNDS; r++)); do
  # STEP 2: rotate. Round r starts at arm (r-1) mod n, so no arm holds a fixed position and no
  # arm is ever measured twice in a row at the same point in the box's own drift.
  order=()
  for ((i = 0; i < n; i++)); do
    order+=("${ARMS[$(((r - 1 + i) % n))]}")
  done
  echo "-- round $r/$ROUNDS  order: ${order[*]}"
  pos=0
  for arm in "${order[@]}"; do
    pos=$((pos + 1))
    dir="$OUT/r$r-$arm"
    if [[ -f "$dir/results.json" ]]; then
      echo "   [$pos/$n] $arm  SKIP (already measured: $dir/results.json)"
      continue
    fi
    mapfile -t extra < <(arm_flags "$arm")
    started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "   [$pos/$n] $arm  start $started -> $dir"
    # The argv is BUILT AS AN ARRAY rather than assembled with `${VAR:+...}` expansions at the
    # call site. Two reasons, and the first is not stylistic: `lib-perf-lint.sh`'s
    # `is_var_command` correctly flags a command line whose leading word is a variable
    # expansion, because it cannot know the variable does not hold `perf` — so the conditional
    # form tripped the rig's own perf-invocation lint and FATALed the shipped driver's
    # self-check (MEASURED: `ws0-3551-abc.sh:148: perf/stat invocation outside the single
    # perf_stat_c wrapper, unmarked`, which then cascaded into 5 hermeticity failures). Marking
    # the line `perf-lint-allow` would have silenced a lint that was reasoning correctly; the
    # array makes the leading word the literal `bash` instead. Second, an empty optional value
    # cannot become an empty positional argument this way.
    local_args=(--corpus "$CORPUS" --bin-dir "$BIN_DIR" --out "$dir"
                --reps 1 --temp warm --arm bypass
                --step-duration "$STEP_DURATION" --port "$PORT")
    if [[ -n "$QUIESCENCE_TS" ]]; then
      local_args+=(--quiescence-timeseries "$QUIESCENCE_TS")
    fi
    if [[ -n "$JEMALLOC_LIB" ]]; then
      local_args+=(--jemalloc-lib "$JEMALLOC_LIB")
    fi
    local_args+=("${extra[@]}")
    set +e
    bash "$HERE/ws0-baseline.sh" "${local_args[@]}" > "$OUT/r$r-$arm.log" 2>&1
    rc=$?
    set -e
    ended="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    # The window is recorded whether the session passed or failed. A FAILED session's window is
    # what lets its failure be correlated against the box-load timeseries afterwards, which is
    # the whole reason the timeseries is kept outside the worktree.
    mkdir -p "$dir"
    # Assembled into a variable rather than a multi-line `printf`, for the same lint reason as
    # the argv array above: a CONTINUATION line whose first word is `"$r"` is, to a line-oriented
    # lint, a command held in a variable — and `is_var_command` cannot see the backslash on the
    # line before. MEASURED: `ws0-3551-abc.sh:174: perf/stat invocation outside the single
    # perf_stat_c wrapper, unmarked`. Every line below starts with either an assignment prefix
    # or a literal command word, so the lint reads what is actually happening.
    window_json="{\"round\":$r,\"position_in_round\":$pos,\"arms_in_round\":$n"
    window_json="$window_json,\"arm\":\"$arm\",\"started\":\"$started\",\"ended\":\"$ended\""
    window_json="$window_json,\"exit\":$rc,\"order\":\"${order[*]}\"}"
    printf '%s\n' "$window_json" > "$dir/abc-window.json"
    if [[ $rc -ne 0 ]]; then
      echo "FATAL: round $r arm $arm exited $rc — see $OUT/r$r-$arm.log" >&2
      echo "       Earlier rounds are intact; re-running with the same --out RESUMES." >&2
      exit "$rc"
    fi
    echo "        done $ended"
  done
done

echo
echo "all rounds complete. aggregate with:"
echo "  python3 $HERE/ws0_abc_aggregate.py --root $OUT --arms A,B,C0,C --baseline A"
