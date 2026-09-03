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

# The binaries this set MEASURES, and therefore the ones whose BYTES the arms must share. The
# same list `ws0_binaries.MEASURED_BINARIES` uses; it is restated here rather than imported
# because this file's use of it is a FINGERPRINT over one directory, not the session's
# provenance record, and a shell driver importing a python constant to build a shell array
# would buy nothing but a second failure mode.
MEASURED_BINARIES=(ws0-scan-bench cqlite-flight flight-loadgen)

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
                     instead of starting over — which matters on a shared box. The resume is
                     CHECKED, not assumed: see abc-run.json below.
  --rounds N         Rounds; each round runs every arm once, order rotated (default $ROUNDS).
                     Deliberately NOT part of the run fingerprint — extending a set from 3
                     rounds to 5 over the same --out is a legitimate resume.
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

\$OUT/abc-run.json is this set's RUN FINGERPRINT: the corpus path AND its recorded Data.db
sha256 + row count, the --bin-dir path AND a digest of every measured binary in it, the arm set
and each arm's exact flag list, --step-duration, --arena-max, --jemalloc-lib and --port. It is
WRITTEN on the first invocation and VERIFIED on every later one; a differing field is a REFUSAL
naming the field and both values, because two sessions measured under different treatments are
not a paired experiment however much the directory layout says they are.
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

# sha256_of <file> — THREE-VALUED, and the third value is a REFUSAL.
#
# present  -> the digest on stdout, rc 0
# missing/unreadable, or NO DIGEST TOOL ON THIS BOX -> a named FATAL, rc 2
#
# The last case is why this is not a one-liner: an absent `sha256sum` returning an EMPTY digest
# would compare equal to another run's empty digest and report two different binary sets as
# identical — a comparison that could not be made, reported as a comparison that passed.
sha256_of() {
  local f="$1" out=""
  if [[ ! -f "$f" ]]; then
    echo "FATAL: '$f' does not exist, so the bytes this set measures cannot be identified." >&2
    echo "       Build the binaries into --bin-dir before starting the set." >&2
    return 2
  fi
  if command -v sha256sum >/dev/null 2>&1; then
    out="$(sha256sum "$f")" || out=""
  elif command -v shasum >/dev/null 2>&1; then
    out="$(shasum -a 256 "$f")" || out=""
  else
    echo "FATAL: neither sha256sum nor shasum is installed, so '$f' cannot be DIGESTED and two" >&2
    echo "       binary sets cannot be told apart. This is refused rather than skipped: an" >&2
    echo "       unmeasurable digest compared equal would report a changed binary as unchanged." >&2
    return 2
  fi
  out="${out%% *}"
  if [[ ! "$out" =~ ^[0-9a-f]{64}$ ]]; then
    echo "FATAL: could not read a sha256 for '$f' (digest tool produced '$out')." >&2
    return 2
  fi
  printf '%s\n' "$out"
}

echo "== #3551 interleaved A/B/C =="
echo "corpus:   $CORPUS"
echo "bins:     $BIN_DIR"
echo "out:      $OUT"
echo "rounds:   $ROUNDS   arms: ${ARMS[*]}"
echo "control:  bare scan pinned to --server-cpus (IDENTICAL in every arm) — method §3b step 3"

# ===========================================================================
# THE RUN FINGERPRINT — what makes a RESUME a resume rather than two experiments
# ===========================================================================
# The SKIP in the loop below is deliberate and STAYS: this box is shared with nine other lanes
# and a set that has to start over loses its window. But a skip is only sound if the sessions
# being kept were produced by THIS configuration, and nothing checked that. Point --out at an
# earlier set's directory, or change the corpus, a pin, the allocator or the binaries between
# invocations, and the aggregator receives a supposedly PAIRED experiment whose rounds were
# measured under different treatments — every downstream check reads the artifacts as one set,
# because that is what the directory says they are.
#
# So the first invocation WRITES `abc-run.json` and every later one VERIFIES it field by field,
# refusing with the differing field NAMED and both values printed. Covered: everything that
# would make two sessions incomparable.
#
#   * the corpus PATH *and* its recorded `Data.db` sha256 + row count — a path can be
#     repopulated with a different corpus, so the path alone would not notice;
#   * the --bin-dir PATH *and* a digest of every measured binary in it: the arms must measure
#     IDENTICAL BYTES, which is the whole reason --bin-dir is not per-arm (#3248 withdrew a
#     machine-code claim for exactly that reason);
#   * the arm SET and, per arm, the EXACT flag list `arm_flags` emits, so a changed pin, pin
#     mode, allocator or arena cap is caught at the flag level rather than inferred from it;
#   * --step-duration, --arena-max, --jemalloc-lib and --port.
#
# `--rounds` IS DELIBERATELY EXCLUDED. Extending a set from 3 rounds to 5 over the same --out is
# a legitimate resume — the same experiment with more pairs — so refusing it would red CORRECT
# INPUT, and a guard that reds on correct input is the guard an operator learns to work around.
# `--arena-max` is included even though it reaches only arm C0, because it is part of that arm's
# flag list and a changed cap changes that treatment.
#
# Every probe here is THREE-VALUED — present / verified-absent / could-not-measure — and
# could-not-measure is a REFUSAL, never "compatible": a comparison that could not be made has
# not been made. An absent or unreadable `corpus-identity.json`, an unreadable or ungrammatical
# `abc-run.json`, a missing binary and a box with no digest tool are each their own named
# refusal carrying its own remedy.
# Read through a COMMAND SUBSTITUTION and not `mapfile < <(...)`: a process substitution's exit
# status is not the reading builtin's, so `mapfile … || exit` would have DISCARDED every refusal
# below and continued with an empty identity — a could-not-measure silently taking the
# permissive branch, which is the exact shape this block exists to remove.
identity_rc=0
identity_out="$(python3 - "$CORPUS/corpus-identity.json" <<'PY'
import json
import pathlib
import sys

p = pathlib.Path(sys.argv[1])
if not p.exists():
    sys.stderr.write(
        f"FATAL: no corpus identity at {p} — this set's corpus cannot be IDENTIFIED, so a\n"
        "       resume could not tell one corpus from another sitting at the same path.\n"
        "       Regenerate the corpus with tools/ws0-corpus-gen, which writes this file\n"
        "       beside the data.\n"
    )
    raise SystemExit(2)
try:
    raw = p.read_text()
except OSError as exc:
    sys.stderr.write(
        f"FATAL: {p} EXISTS but could not be READ ({exc}). This is refused rather than\n"
        "       skipped: an unreadable identity is UNMEASURED, not compatible.\n"
    )
    raise SystemExit(2)
try:
    identity = json.loads(raw)
except ValueError as exc:
    sys.stderr.write(f"FATAL: {p} is not readable JSON ({exc}) — the corpus is UNIDENTIFIED.\n")
    raise SystemExit(2)
if not isinstance(identity, dict):
    sys.stderr.write(f"FATAL: {p} must hold a JSON object, got {type(identity).__name__}.\n")
    raise SystemExit(2)
sha = identity.get("data_db_sha256")
rows = identity.get("rows")
if not isinstance(sha, str) or not sha:
    sys.stderr.write(
        f"FATAL: {p} carries no usable 'data_db_sha256' (got {sha!r}) — the corpus BYTES\n"
        "       cannot be pinned, so a repopulated corpus path would resume silently.\n"
    )
    raise SystemExit(2)
if isinstance(rows, bool) or not isinstance(rows, int) or rows <= 0:
    sys.stderr.write(
        f"FATAL: {p} carries no usable 'rows' (got {rows!r}) — a corpus with no row count\n"
        "       is not a measurable corpus.\n"
    )
    raise SystemExit(2)
print(sha)
print(rows)
PY
)" || identity_rc=$?
[[ $identity_rc -eq 0 ]] || exit "$identity_rc"
mapfile -t corpus_identity <<<"$identity_out"
[[ ${#corpus_identity[@]} -eq 2 && -n "${corpus_identity[0]}" && -n "${corpus_identity[1]}" ]] \
  || { echo "FATAL: could not read the corpus identity at $CORPUS/corpus-identity.json" >&2; exit 2; }

fp=("corpus_path=$CORPUS"
    "corpus_data_db_sha256=${corpus_identity[0]}"
    "corpus_rows=${corpus_identity[1]}"
    "bin_dir=$BIN_DIR"
    "step_duration=$STEP_DURATION"
    "arena_max=$ARENA_MAX"
    "jemalloc_lib=$JEMALLOC_LIB"
    "port=$PORT"
    "arms=${ARMS[*]}")
for b in "${MEASURED_BINARIES[@]}"; do
  digest="$(sha256_of "$BIN_DIR/$b")" || exit 2
  fp+=("binary_sha256.$b=$digest")
done
for arm in "${ARMS[@]}"; do
  mapfile -t af < <(arm_flags "$arm")
  fp+=("arm_flags.$arm=${af[*]}")
done

rc=0
python3 - "$OUT/abc-run.json" "${fp[@]}" <<'PY' || rc=$?
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
fields = {}
for arg in sys.argv[2:]:
    key, _, value = arg.partition("=")
    if not key:
        sys.stderr.write(f"FATAL: internal: fingerprint argument {arg!r} has no field name.\n")
        raise SystemExit(2)
    fields[key] = value

REMEDY = (
    "       This --out belongs to a DIFFERENT experiment. Either point --out at a FRESH\n"
    "       directory, or re-run with the configuration the recorded values above name.\n"
    "       --rounds is deliberately NOT fingerprinted, so extending a set is not this.\n"
)

if path.exists():
    try:
        raw = path.read_text()
    except OSError as exc:
        sys.stderr.write(
            f"FATAL: {path} EXISTS but could not be READ ({exc}), so this resume could not be\n"
            "       CHECKED. Refused rather than accepted: an unverifiable resume is not a\n"
            "       compatible one. Fix the permissions, or start the set in a fresh --out.\n"
        )
        raise SystemExit(2)
    try:
        record = json.loads(raw)
    except ValueError as exc:
        sys.stderr.write(
            f"FATAL: {path} is not readable JSON ({exc}), so this resume could not be CHECKED.\n"
            "       Refused rather than accepted. Start the set in a fresh --out.\n"
        )
        raise SystemExit(2)
    recorded = record.get("fields") if isinstance(record, dict) else None
    if not isinstance(recorded, dict):
        sys.stderr.write(
            f"FATAL: {path} carries no `fields` object, so this resume could not be CHECKED\n"
            "       against anything. Refused rather than accepted; start in a fresh --out.\n"
        )
        raise SystemExit(2)
    problems = []
    for key in sorted(set(recorded) | set(fields)):
        if key not in recorded:
            problems.append(
                f"{key}: NOT RECORDED by the existing run record; this invocation has"
                f" {fields[key]!r}"
            )
        elif key not in fields:
            problems.append(
                f"{key}: recorded as {recorded[key]!r}; this invocation does not supply it"
            )
        elif str(recorded[key]) != fields[key]:
            problems.append(
                f"{key}: recorded {str(recorded[key])!r}, this invocation {fields[key]!r}"
            )
    if problems:
        sys.stderr.write(
            f"FATAL: {path} records an INCOMPATIBLE run — resuming would combine sessions\n"
            "       measured under different treatments into one supposedly paired set.\n"
        )
        for problem in problems:
            sys.stderr.write(f"       DIFFERS {problem}\n")
        sys.stderr.write(REMEDY)
        raise SystemExit(2)
    print(
        f"resume:   VERIFIED against {path} — all {len(fields)} fingerprint field(s) identical"
    )
else:
    body = {
        "issue": "#3551",
        "fields": fields,
        "note": (
            "the RUN FINGERPRINT of this A/B/C set, written by the first invocation and"
            " VERIFIED field-by-field by every later one. A differing field is a REFUSAL:"
            " sessions measured under different treatments are not a paired experiment."
        ),
        "rounds_excluded": (
            "--rounds is deliberately NOT a field here. Extending a set from 3 rounds to 5"
            " over the same --out is a legitimate resume — the same experiment with more"
            " pairs — and refusing it would red correct input."
        ),
    }
    try:
        path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")
    except OSError as exc:
        sys.stderr.write(
            f"FATAL: could not WRITE the run record {path} ({exc}). Refused rather than run"
            " unfingerprinted: an unrecorded set cannot be resumed safely.\n"
        )
        raise SystemExit(2)
    print(f"resume:   run record WRITTEN to {path} ({len(fields)} fingerprint field(s))")
PY
[[ $rc -eq 0 ]] || exit "$rc"
echo

# verify_measured_session <dir> <round> <arm> — a session may only be SKIPPED once its own
# window record says it is the session this (round, arm) slot expects.
#
# `results.json` alone establishes NOTHING about provenance: it is the reporter's output and
# carries no round, no position and no arm LABEL of this set's vocabulary. A directory holding
# one but no `abc-window.json` was produced by something other than this driver (or by a driver
# run whose window write failed), and a window describing a DIFFERENT arm means the directory
# was moved or renamed — in both cases the pairing the aggregator will perform is a fiction.
# A recorded non-zero `exit` is the third case: the window is written for FAILED sessions on
# purpose (so the failure can be correlated against the box-load timeseries), so a failed
# session's leftover `results.json` must never be silently adopted as a measurement.
#
# Each refusal NAMES THE DIRECTORY, because the operator's next action is on that directory and
# a set is 12 to 20 of them.
verify_measured_session() {
  local dir="$1" want_round="$2" want_arm="$3" vrc=0
  python3 - "$dir" "$want_round" "$want_arm" <<'PY' || vrc=$?
import json
import pathlib
import sys

d = pathlib.Path(sys.argv[1])
want_round, want_arm = sys.argv[2], sys.argv[3]
w = d / "abc-window.json"
if not w.exists():
    sys.stderr.write(
        f"FATAL: {d} holds a results.json but NO abc-window.json, so NOTHING establishes which\n"
        f"       arm or which round produced it — it cannot be adopted as this set's ({want_round},\n"
        f"       {want_arm}) session. Refused rather than skipped. Remedy: remove {d} and re-run,\n"
        "       which re-measures that (round, arm).\n"
    )
    raise SystemExit(2)
try:
    record = json.loads(w.read_text())
except (OSError, ValueError) as exc:
    sys.stderr.write(
        f"FATAL: {d} holds a results.json but its abc-window.json could not be READ ({exc}), so\n"
        "       this session's provenance is UNMEASURED — which is refused, never treated as\n"
        f"       compatible. Remedy: remove {d} and re-run.\n"
    )
    raise SystemExit(2)
if not isinstance(record, dict):
    sys.stderr.write(
        f"FATAL: {d}: abc-window.json must hold a JSON object, got"
        f" {type(record).__name__} — this session's provenance is UNMEASURED.\n"
    )
    raise SystemExit(2)
for field in ("arm", "round", "exit"):
    if field not in record:
        sys.stderr.write(
            f"FATAL: {d}: abc-window.json carries no {field!r}, so this session cannot be\n"
            f"       attributed. Refused rather than skipped. Remedy: remove {d} and re-run.\n"
        )
        raise SystemExit(2)
if str(record["arm"]) != want_arm:
    sys.stderr.write(
        f"FATAL: {d}: abc-window.json records arm {str(record['arm'])!r} but the directory name\n"
        f"       says arm {want_arm!r}. A session measured under one arm cannot stand in for\n"
        f"       another — that is the treatment itself. Remedy: remove {d} and re-run.\n"
    )
    raise SystemExit(2)
if str(record["round"]) != want_round:
    sys.stderr.write(
        f"FATAL: {d}: abc-window.json records round {str(record['round'])!r} but the directory\n"
        f"       name says round {want_round!r}. The pairing is BY ROUND, so a mislabelled round\n"
        f"       pairs the wrong sessions. Remedy: remove {d} and re-run.\n"
    )
    raise SystemExit(2)
if record["exit"] != 0:
    sys.stderr.write(
        f"FATAL: {d}: abc-window.json records exit {record['exit']!r} — that session FAILED, so\n"
        "       its leftover results.json is not a measurement this set may adopt. The window is\n"
        "       written for failed sessions on purpose (so the failure can be correlated against\n"
        f"       the box-load timeseries). Remedy: remove {d} and re-run.\n"
    )
    raise SystemExit(2)
PY
  [[ $vrc -eq 0 ]] || exit "$vrc"
}

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
      # The run fingerprint above establishes that THIS INVOCATION's configuration matches the
      # one the set was started with; this establishes that THIS DIRECTORY holds the session
      # the slot expects. Two different questions, and the first cannot answer the second.
      verify_measured_session "$dir" "$r" "$arm"
      echo "   [$pos/$n] $arm  SKIP (measured, window VERIFIED: $dir/results.json)"
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
