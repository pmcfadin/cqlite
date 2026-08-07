#!/usr/bin/env bash
# CQLite issue #3225 §2 — the C(N) x width sweep driver.
#
# Adapted from docs/reports/ws0-3217-artifacts/partA-run/run-partA.sh. What changed
# and why (design.md D7):
#   * widths S in {1,2,3,4,6}; S=3 is NEW and goes through sweep.sh's LITERAL
#     cpu-list form (0-2,8-10) — sweep.sh needs no code change for it.
#   * N ramp extended to 1,2,4,8,16,24,32,64. 64 is the SHIPPED default
#     (DEFAULT_MAX_CONCURRENT_SCANS, cqlite-flight/src/admission.rs), and AC5
#     cannot be evaluated against a point nobody measured. #3217 stopped at 16.
#   * the admission ceiling is raised to 64 (see WS0_MAX_CONCURRENT_SCANS below):
#     at the harness default of 16 every N>16 point would measure the ADMISSION
#     GATE shedding, not the concurrency curve.
#   * the merge-path reference arms are dropped (#3217 answered that; this round
#     measures a curve, bypass only).
#   * NOT run: partB-run/, profile-*, classify-offcpu, runqlat. That also removes
#     the perf_event_paranoid/kptr_restrict symbolization dependency.
#   * RESTARTABLE PER ARM (see below) — #3217's driver was not.
#
# Usage:
#   bash run-3225.sh [options]
#     --root <dir>        WS0 root (default $WS0_ROOT, else /data/ws0)
#     --stage <dir>       staged SSTable dir (default <root>/ws0-h2h/datasets/sstables)
#     --worktree <dir>    repo checkout holding target/release (default: this file's repo)
#     --arms "a b c"      subset of arm labels to run (default: all five). Matched
#                         against the EFFECTIVE labels, i.e. AFTER --label-suffix; an
#                         unknown label is a fatal usage error, never an empty run
#     --ramp <list>       override the N ramp (dry runs only)
#     --step <secs>       override the per-point hold (dry runs only)
#     --reps <n>          override reps (dry runs only)
#     --label-suffix <s>  append to every arm label (use for dry runs: -dryrun)
#     --force             re-run arms that already have a complete summary.json
#     --list              print the planned arms and exit
#     --help
#
# RESTART / RESUME AFTER A CRASH
#   Every point is written to <root>/results/<arm>/points.jsonl by emit-point.py the
#   moment it completes, and each arm writes summary.json when it finishes. Re-running
#   this script with the SAME arguments:
#     - SKIPS every arm that already has a complete summary.json (COMPLETED arms are
#       never redone and never lost);
#     - QUARANTINES a partial arm (points.jsonl but no summary.json) to
#       <arm>.partial-<utc> and re-runs that arm from rep 1.
#   The quarantine is deliberate: sweep.sh always starts at rep 1 and APPENDS, so
#   resuming into an existing points.jsonl would silently mix a truncated first
#   attempt with a second one and corrupt every per-N median. A partial arm costs
#   ~1 h to redo; a silently doubled arm costs the whole result's credibility.
#
# LONG RUNNING: ~1 h per arm, ~5-6 h for all five. Launch detached:
#   nohup bash run-3225.sh > /data/ws0/logs/run-3225.log 2>&1 < /dev/null &
set -uo pipefail

WS0_ROOT="${WS0_ROOT:-/data/ws0}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKTREE="$(cd "$HERE/../../../.." && pwd)"
STAGE=""
ARMS_REQ=""
RAMP="1,2,4,8,16,24,32,64"
STEP_SECS=120
REPS=3
LABEL_SUFFIX=""
FORCE=0
LIST_ONLY=0

while [ $# -gt 0 ]; do
  case "$1" in
    --root)         [ $# -ge 2 ] || { echo "ERROR: --root needs a value" >&2; exit 2; }; WS0_ROOT="$2"; shift 2 ;;
    --stage)        [ $# -ge 2 ] || { echo "ERROR: --stage needs a value" >&2; exit 2; }; STAGE="$2"; shift 2 ;;
    --worktree)     [ $# -ge 2 ] || { echo "ERROR: --worktree needs a value" >&2; exit 2; }; WORKTREE="$2"; shift 2 ;;
    --arms)         [ $# -ge 2 ] || { echo "ERROR: --arms needs a value" >&2; exit 2; }; ARMS_REQ="$2"; shift 2 ;;
    --ramp)         [ $# -ge 2 ] || { echo "ERROR: --ramp needs a value" >&2; exit 2; }; RAMP="$2"; shift 2 ;;
    --step)         [ $# -ge 2 ] || { echo "ERROR: --step needs a value" >&2; exit 2; }; STEP_SECS="$2"; shift 2 ;;
    --reps)         [ $# -ge 2 ] || { echo "ERROR: --reps needs a value" >&2; exit 2; }; REPS="$2"; shift 2 ;;
    --label-suffix) [ $# -ge 2 ] || { echo "ERROR: --label-suffix needs a value" >&2; exit 2; }; LABEL_SUFFIX="$2"; shift 2 ;;
    --force)        FORCE=1; shift ;;
    --list)         LIST_ONLY=1; shift ;;
    # 2..50 is the header comment block; keep this in step with it (the last header
    # line is the one immediately above `set -uo pipefail`) or --help truncates.
    -h|--help)      sed -n '2,50p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "ERROR: unrecognized argument '$1'" >&2; exit 2 ;;
  esac
done

HARNESS="$WORKTREE/docs/reports/ws0-3217-artifacts/harness"
[ -d "$HARNESS" ] || { echo "ERROR: harness dir not found: $HARNESS" >&2; exit 1; }
[ -f "$HARNESS/sweep.sh" ] || { echo "ERROR: sweep.sh not found under $HARNESS" >&2; exit 1; }
[ -z "$STAGE" ] && STAGE="$WS0_ROOT/ws0-h2h/datasets/sstables"

# ---- the environment sweep.sh reads. Fail closed on every one of them. -------
export WS0_ROOT
export WS0_STAGE="$STAGE"
export WS0_FLIGHT_BIN="${WS0_FLIGHT_BIN:-$WORKTREE/target/release/cqlite-flight}"
export WS0_LOADGEN_BIN="${WS0_LOADGEN_BIN:-$WORKTREE/target/release/flight-loadgen}"
export WS0_TICKET_TPL="${WS0_TICKET_TPL:-$WORKTREE/docs/reports/ws0-3100-artifacts/ws0-h2h/ws0-events-template.json}"

# THE knob this whole issue is about. The harness default is 16, which would make
# every N>16 point measure admission shedding instead of the concurrency curve.
# 64 is the SHIPPED default (admission.rs DEFAULT_MAX_CONCURRENT_SCANS) and the top
# of this ramp, so the ceiling never binds and C(N) is measured cleanly at every N.
export WS0_MAX_CONCURRENT_SCANS="${WS0_MAX_CONCURRENT_SCANS:-64}"

# Unchanged from #3217 so the S in {1,2,4,6} arms stay directly comparable.
export WS0_BATCH_SIZE="${WS0_BATCH_SIZE:-8192}"
export WS0_MAX_BATCH_BYTES="${WS0_MAX_BATCH_BYTES:-4194304}"
export WS0_MAX_INFLIGHT_EGRESS_BYTES="${WS0_MAX_INFLIGHT_EGRESS_BYTES:-12582912}"
export WS0_ADMISSION_WAIT_TIMEOUT_MS="${WS0_ADMISSION_WAIT_TIMEOUT_MS:-30000}"
export WS0_SEED="${WS0_SEED:-42}"
export WS0_WARM_SECS="${WS0_WARM_SECS:-45}"
export WS0_SETTLE_SECS="${WS0_SETTLE_SECS:-5}"
export WS0_CLIENT_SAT_THRESHOLD="${WS0_CLIENT_SAT_THRESHOLD:-0.70}"
export WS0_RESULTS="${WS0_RESULTS:-$WS0_ROOT/results}"
export WS0_LOGS="${WS0_LOGS:-$WS0_ROOT/logs}"
export WS0_ARTIFACTS="${WS0_ARTIFACTS:-$WS0_ROOT/artifacts}"

CLIENT_CPUS="${WS0_CLIENT_CPUS:-6,7,14,15}"

# ---- the arm table: label | server-cpu-spec ---------------------------------
# S=1,2,4,6 use sweep.sh's shorthands (which also stamp server_physical_cores_S).
# S=3 has no shorthand and uses the LITERAL list — its points therefore carry
# server_physical_cores_S=null, and analyze-3225.py re-derives S from the arm's
# own cpu-topology.json sibling groups rather than trusting a label.
ARM_LABELS=(cn3225-s1 cn3225-s2 cn3225-s3 cn3225-s4 cn3225-s6)
ARM_SPECS=(s1        s2        0-2,8-10  s4        s6)

log()  { printf '[%s] %s\n' "$(date -u +%FT%TZ)" "$*"; }
die()  { printf '[%s] ERROR: %s\n' "$(date -u +%FT%TZ)" "$*" >&2; exit 1; }

# ---- the admission ceiling MUST cover the top of the ramp (checked, not hoped) --
# WS0_MAX_CONCURRENT_SCANS is INHERITED whenever the caller exports it, so the ":-64"
# default above guarantees nothing: an environment carrying the harness default of 16
# would silently cap every N in {24,32,64} and the sweep would publish a THROTTLED
# curve as C(N).
#
# And the rejection counter CANNOT detect that. WS0_ADMISSION_WAIT_TIMEOUT_MS is
# 30000, so a request arriving over the ceiling does not fail — it WAITS for a permit
# and then SUCCEEDS. `requests_unavailable` therefore stays 0 while every point above
# the ceiling measures queueing at the gate. "0 rejections" is corroborating evidence
# at best; it is NOT evidence that the ceiling did not bind.
#
# The only sound check is this one: read the ramp, read the ceiling, compare the two
# numbers, and abort BEFORE the first arm starts. It costs a millisecond here and
# saved a forensic reconstruction of six arms' server_flags afterwards.
RAMP_MAX=0
_ramp_count=0
IFS=',' read -r -a _ramp_tokens <<< "$RAMP"
for _tok in "${_ramp_tokens[@]}"; do
  _tok="${_tok//[[:space:]]/}"
  [ -n "$_tok" ] || continue
  case "$_tok" in
    *[!0-9]*) printf "ERROR: --ramp '%s' holds a non-integer N: '%s'\n" "$RAMP" "$_tok" >&2; exit 2 ;;
  esac
  [ "$_tok" -ge 1 ] || {
    printf "ERROR: --ramp '%s' holds N=%s; every N must be >= 1\n" "$RAMP" "$_tok" >&2; exit 2; }
  _ramp_count=$((_ramp_count + 1))
  [ "$_tok" -gt "$RAMP_MAX" ] && RAMP_MAX="$_tok"
done
[ "$_ramp_count" -ge 1 ] || {
  printf "ERROR: --ramp '%s' names no N at all — there is no sweep to run\n" "$RAMP" >&2; exit 2; }

case "$WS0_MAX_CONCURRENT_SCANS" in
  ''|*[!0-9]*)
    printf "ERROR: WS0_MAX_CONCURRENT_SCANS='%s' is not a non-negative integer.\n" \
      "$WS0_MAX_CONCURRENT_SCANS" >&2
    printf "       The admission ceiling is the knob this whole issue is about; an unparseable\n" >&2
    printf "       value cannot be compared against the ramp, so the sweep would be uncertified.\n" >&2
    exit 2 ;;
esac
if [ "$WS0_MAX_CONCURRENT_SCANS" -lt "$RAMP_MAX" ]; then
  printf "ERROR: admission ceiling %s < max(ramp) %s — REFUSING to sweep.\n" \
    "$WS0_MAX_CONCURRENT_SCANS" "$RAMP_MAX" >&2
  printf "       ramp=%s  WS0_MAX_CONCURRENT_SCANS=%s (inherited from the environment)\n" \
    "$RAMP" "$WS0_MAX_CONCURRENT_SCANS" >&2
  printf "       Every N above %s would measure the ADMISSION GATE, not the concurrency curve,\n" \
    "$WS0_MAX_CONCURRENT_SCANS" >&2
  printf "       and with WS0_ADMISSION_WAIT_TIMEOUT_MS=%s those requests WAIT and then SUCCEED,\n" \
    "$WS0_ADMISSION_WAIT_TIMEOUT_MS" >&2
  printf "       so requests_unavailable stays 0 and NOTHING downstream would notice.\n" >&2
  printf "       Fix: unset WS0_MAX_CONCURRENT_SCANS (defaults to 64), export one >= %s,\n" "$RAMP_MAX" >&2
  printf "       or lower the ramp with --ramp.\n" >&2
  exit 2
fi

# ---- the EFFECTIVE arm labels, and --arms validated against them -------------
# The label a caller must name is the one --label-suffix produces, not the one in
# ARM_LABELS. That distinction is the whole defect: `--arms cn3225-s3` with
# `--label-suffix -dryrun` in play matched NOTHING, so both the run loop and the
# arm-status loop skipped every arm, FAILED stayed 0, and the driver printed
# "SWEEP COMPLETE" and exited 0 having measured nothing. A benchmark that reports
# success on zero measurements is worse than one that crashes — the crash gets fixed.
# So: an unrecognised label is a fatal usage error naming what is valid, and an empty
# selection is fatal too (never a vacuous success). Validated BEFORE --list as well as
# before the run, so a typo costs a second rather than a six-hour window.
EFFECTIVE_LABELS=()
for i in "${!ARM_LABELS[@]}"; do
  EFFECTIVE_LABELS+=("${ARM_LABELS[$i]}${LABEL_SUFFIX}")
done

# The ONE membership test every loop below asks. Three hand-rolled copies of this
# `case` is how --list drifted out of step with the run loop in the first place.
arm_requested() { # effective-label
  [ -z "$ARMS_REQ" ] && return 0
  local lab
  for lab in $ARMS_REQ; do
    [ "$lab" = "$1" ] && return 0
  done
  return 1
}

if [ -n "$ARMS_REQ" ]; then
  UNKNOWN_ARMS=()
  SELECTED_ARMS=()
  for req in $ARMS_REQ; do
    matched=0
    for lab in "${EFFECTIVE_LABELS[@]}"; do
      [ "$req" = "$lab" ] && { matched=1; break; }
    done
    if [ "$matched" -eq 1 ]; then SELECTED_ARMS+=("$req"); else UNKNOWN_ARMS+=("$req"); fi
  done
  if [ "${#UNKNOWN_ARMS[@]}" -gt 0 ]; then
    printf 'ERROR: --arms names %d label(s) that no arm has: %s\n' \
      "${#UNKNOWN_ARMS[@]}" "${UNKNOWN_ARMS[*]}" >&2
    printf '       valid arm labels%s: %s\n' \
      "$([ -n "$LABEL_SUFFIX" ] && printf ' (with --label-suffix %s applied)' "$LABEL_SUFFIX")" \
      "${EFFECTIVE_LABELS[*]}" >&2
    [ -n "$LABEL_SUFFIX" ] && printf '       NOTE: --label-suffix %s is in effect, so --arms must name the SUFFIXED label.\n' \
      "$LABEL_SUFFIX" >&2
    printf '       Refusing to run: an unmatched label selects no arm, and the sweep would then report itself complete over zero measurements.\n' >&2
    exit 2
  fi
  if [ "${#SELECTED_ARMS[@]}" -eq 0 ]; then
    printf 'ERROR: --arms %q selected NO arm (empty/whitespace-only selection).\n' "$ARMS_REQ" >&2
    printf '       valid arm labels: %s\n' "${EFFECTIVE_LABELS[*]}" >&2
    exit 2
  fi
fi

if [ "$LIST_ONLY" -eq 1 ]; then
  printf 'planned arms (ramp=%s step=%ss reps=%s client=%s max_concurrent_scans=%s):\n' \
    "$RAMP" "$STEP_SECS" "$REPS" "$CLIENT_CPUS" "$WS0_MAX_CONCURRENT_SCANS"
  printf '  admission ceiling %s >= max(ramp) %s: OK (checked above; a lower ceiling aborts)\n' \
    "$WS0_MAX_CONCURRENT_SCANS" "$RAMP_MAX"
  listed=0
  for i in "${!ARM_LABELS[@]}"; do
    label="${EFFECTIVE_LABELS[$i]}"
    # --list MUST apply the same --arms filter the run loop applies. Printing the
    # unfiltered table while execution silently skips most of it makes the one
    # affordance whose whole job is "show me the plan before the 6-hour run" lie.
    arm_requested "$label" || continue
    printf '  %-20s server_cpus=%s\n' "$label" "${ARM_SPECS[$i]}"
    listed=$((listed + 1))
  done
  # Affirmative: a plan of zero arms is not a plan. Unreachable given the --arms
  # validation above, so reaching it means the filter and the validator disagree.
  if [ "$listed" -eq 0 ]; then
    printf 'ERROR: the plan is EMPTY — no arm survived the --arms filter (arms=%q suffix=%q).\n' \
      "$ARMS_REQ" "$LABEL_SUFFIX" >&2
    exit 2
  fi
  printf '  (%d arm(s) planned)\n' "$listed"
  exit 0
fi

[ -d "$WS0_STAGE" ]      || die "WS0_STAGE=$WS0_STAGE is not a directory (regenerate the corpus first: docs/reports/ws0-3225-artifacts/corpus/regen-corpus.sh)"
[ -x "$WS0_FLIGHT_BIN" ] || die "WS0_FLIGHT_BIN=$WS0_FLIGHT_BIN is not executable (cargo build --release -p cqlite-flight)"
[ -x "$WS0_LOADGEN_BIN" ]|| die "WS0_LOADGEN_BIN=$WS0_LOADGEN_BIN is not executable (cargo build --release -p flight-loadgen)"
[ -f "$WS0_TICKET_TPL" ] || die "WS0_TICKET_TPL=$WS0_TICKET_TPL not found"
# Depth-agnostic on purpose: the staged layout is <stage>/<keyspace>/<table-dir>/*-Data.db,
# which a fixed-depth glob gets wrong by one level (common.sh's own check only WARNs on
# that, so it cannot be relied on to catch an unstaged corpus).
STAGED_DATA_DB_COUNT="$(find "$WS0_STAGE" -name '*-Data.db' -type f 2>/dev/null | wc -l)"
[ "$STAGED_DATA_DB_COUNT" -ge 1 ] \
  || die "no *-Data.db anywhere under $WS0_STAGE — the corpus is not staged"
log "staged corpus: $STAGED_DATA_DB_COUNT *-Data.db file(s) under $WS0_STAGE"

# A live Cassandra (or a stray flight server) would compete for the pinned cores
# and silently spoil every point. Refuse rather than measure noise.
if pgrep -f 'org.apache.cassandra.service.CassandraDaemon' >/dev/null 2>&1; then
  die "a Cassandra daemon is RUNNING — stop it before sweeping (it competes for the pinned cores)"
fi
if pgrep -f "$WS0_FLIGHT_BIN" >/dev/null 2>&1; then
  die "a cqlite-flight from $WS0_FLIGHT_BIN is already running — stop it first"
fi

DRIVER_LOGS="$WS0_LOGS/driver"
mkdir -p "$DRIVER_LOGS" "$WS0_RESULTS"
PROG="$DRIVER_LOGS/run-3225-progress.txt"
touch "$PROG"

log "worktree=$WORKTREE"
log "stage=$WS0_STAGE  flight=$WS0_FLIGHT_BIN  loadgen=$WS0_LOADGEN_BIN"
log "ramp=$RAMP step=${STEP_SECS}s reps=$REPS warm=${WS0_WARM_SECS}s settle=${WS0_SETTLE_SECS}s path=bypass"
log "client_cpus=$CLIENT_CPUS  max_concurrent_scans=$WS0_MAX_CONCURRENT_SCANS  client_sat_gate=$WS0_CLIENT_SAT_THRESHOLD"
log "results -> $WS0_RESULTS   progress ledger -> $PROG"

# ---- per-arm corpus digest + effective-ceiling stamp -------------------------
# Two facts an arm's own artifacts did NOT record, and which cost forensics to
# reconstruct afterwards:
#   1. the EFFECTIVE admission ceiling. sweep.sh buries it inside the server_flags
#      STRING of every point; nothing states it as a field, and run-config.json —
#      the file whose whole job is "what was this arm configured with" — omitted it.
#   2. a DIGEST of the bytes the arm actually read. corpus-basis.json records the
#      stage path, the file count and the byte sizes; a different file of the same
#      size at the same path is indistinguishable from the right one, so "all arms
#      agree" was agreement about metadata, not about content.
# Both are stamped here, per arm, from what is on disk AT THAT ARM'S TIME.
corpus_digest_manifest() { # -> "<sha256>  <relpath>" per staged Data.db, sorted
  ( cd "$WS0_STAGE" 2>/dev/null &&
    find . -name '*-Data.db' -type f -print0 | LC_ALL=C sort -z | xargs -0 -r sha256sum )
}

stamp_arm_provenance() { # arm-dir  digest-manifest-before  digest-manifest-after
  python3 - "$1" "$2" "$3" \
    "$WS0_MAX_CONCURRENT_SCANS" "$RAMP" "$RAMP_MAX" "$WS0_ADMISSION_WAIT_TIMEOUT_MS" <<'PY'
import datetime, hashlib, json, os, sys

arm_dir, before, after, ceiling, ramp, ramp_max, wait_ms = sys.argv[1:8]
now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
errs = []


def patch(name, fields):
    path = os.path.join(arm_dir, name)
    try:
        with open(path) as fh:
            doc = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        errs.append("%s: %s: %s" % (path, type(exc).__name__, exc))
        return
    doc.update(fields)
    with open(path, "w") as fh:
        fh.write(json.dumps(doc, indent=1) + "\n")


patch("run-config.json", {
    "max_concurrent_scans_effective": int(ceiling),
    "admission_wait_timeout_ms_effective": int(wait_ms),
    "ramp_max_N": int(ramp_max),
    "admission_ceiling_covers_ramp": int(ceiling) >= int(ramp_max),
    "provenance_stamped_by": "run-3225.sh (effective ceiling; sweep.sh records it only "
                             "inside the server_flags string)",
})

# The digest is only a digest of THIS arm's bytes if the corpus did not move under
# it. Measured before AND after; a disagreement is recorded as an error and NO
# digest is written, because a single value would then name neither state.
def parse(man):
    out = {}
    for line in man.splitlines():
        parts = line.split(None, 1)
        if len(parts) == 2:
            out[parts[1].strip()] = parts[0]
    return out


b, a = parse(before), parse(after)
if not b:
    errs.append("no *-Data.db digest could be measured before the arm (empty manifest)")
elif b != a:
    patch("corpus-basis.json", {
        "data_db_sha256_error":
            "the staged corpus CHANGED during this arm: before=%r after=%r — no digest "
            "is recorded, because neither value describes the whole arm" % (b, a),
        "data_db_sha256_measured_utc": now,
    })
    errs.append("staged corpus changed DURING the arm (before != after)")
else:
    manifest = "".join("%s  %s\n" % (b[k], k) for k in sorted(b))
    fields = {
        "data_db_sha256_files": len(b),
        "data_db_sha256_manifest": hashlib.sha256(manifest.encode()).hexdigest(),
        "data_db_sha256_manifest_lines": [l for l in manifest.splitlines()],
        "data_db_sha256_basis": "sha256 measured by run-3225.sh over every staged "
                                "*-Data.db immediately BEFORE and AFTER this arm; the two "
                                "measurements agreed",
        "data_db_sha256_measured_utc": now,
    }
    if len(b) == 1:
        fields["data_db_sha256"] = next(iter(b.values()))
    patch("corpus-basis.json", fields)

if errs:
    for e in errs:
        print("STAMP ERROR: %s" % e, file=sys.stderr)
    sys.exit(1)
PY
}

# An arm is COMPLETE when its summary.json exists and parses. summarize-sweep.py
# writes it only after the last rep, so its presence is the completion marker.
arm_complete() {
  local dir="$1"
  [ -s "$dir/summary.json" ] || return 1
  python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$dir/summary.json" >/dev/null 2>&1
}

run_arm() { # label  server-cpu-spec
  local label="$1" spec="$2"
  local dir="$WS0_RESULTS/$label"

  if arm_complete "$dir" && [ "$FORCE" -eq 0 ]; then
    log "SKIP $label — already complete ($(wc -l < "$dir/points.jsonl" 2>/dev/null || echo 0) points)"
    echo "$(date -u +%FT%TZ) SKIP  $label (complete)" >> "$PROG"
    return 0
  fi
  if [ -e "$dir" ]; then
    local quarantine="$dir.partial-$(date -u +%Y%m%dT%H%M%SZ)"
    mv "$dir" "$quarantine"
    log "QUARANTINED partial/forced arm $label -> $quarantine (sweep.sh restarts at rep 1 and appends; mixing attempts would corrupt every median)"
    echo "$(date -u +%FT%TZ) QUAR  $label -> $quarantine" >> "$PROG"
  fi

  log "START $label (server_cpus=$spec)"
  echo "$(date -u +%FT%TZ) START $label" >> "$PROG"
  local digest_before
  digest_before="$(corpus_digest_manifest)"
  ( cd "$HARNESS" && bash ./sweep.sh "$label" "$spec" "$CLIENT_CPUS" "$RAMP" "$STEP_SECS" "$REPS" bypass ) \
    > "$DRIVER_LOGS/$label.out" 2>&1 < /dev/null
  # rc MUST be captured before ANY other command substitution: $(date ...) spawns a
  # subshell and overwrites $?. That exact bug shipped in #3217's driver ledger and
  # made a failed arm indistinguishable from a clean one.
  local rc=$?
  echo "$(date -u +%FT%TZ) END   $label rc=$rc" >> "$PROG"
  if [ "$rc" -ne 0 ]; then
    log "FAIL  $label rc=$rc — see $DRIVER_LOGS/$label.out (continuing to the next arm; re-run this script to retry it)"
    return 0
  fi
  log "DONE  $label ($(wc -l < "$dir/points.jsonl" 2>/dev/null || echo 0) points)"

  # Stamp AFTER the arm, with the digest measured on both sides of it. A stamp that
  # cannot be written is reported and counted: the analysis fails closed on a missing
  # digest, so a silent skip here would surface much later as an unexplained refusal.
  local digest_after
  digest_after="$(corpus_digest_manifest)"
  if stamp_arm_provenance "$dir" "$digest_before" "$digest_after"; then
    log "STAMPED $label — max_concurrent_scans=$WS0_MAX_CONCURRENT_SCANS (>= max ramp $RAMP_MAX), corpus digest recorded"
  else
    STAMP_FAILED=$((STAMP_FAILED + 1))
    log "STAMP FAIL $label — run-config/corpus-basis provenance NOT recorded (see above); the analysis will refuse this arm"
    echo "$(date -u +%FT%TZ) STAMP-FAIL $label" >> "$PROG"
  fi
  return 0
}

FAILED=0
ATTEMPTED=0
STAMP_FAILED=0
for i in "${!ARM_LABELS[@]}"; do
  label="${EFFECTIVE_LABELS[$i]}"
  spec="${ARM_SPECS[$i]}"
  arm_requested "$label" || continue
  run_arm "$label" "$spec"
  ATTEMPTED=$((ATTEMPTED + 1))
done

# The verdict below must never be reached vacuously. If the selection somehow ran
# nothing, "SWEEP COMPLETE" would be a report about zero measurements.
if [ "$ATTEMPTED" -eq 0 ]; then
  echo "$(date -u +%FT%TZ) NO-ARM-ATTEMPTED arms=$ARMS_REQ suffix=$LABEL_SUFFIX" >> "$PROG"
  die "no arm was attempted (arms='$ARMS_REQ' suffix='$LABEL_SUFFIX') — refusing to report a sweep verdict over zero arms"
fi

# Report, don't guess: an arm with no complete summary.json did not produce a curve.
log "---- arm status ----"
for i in "${!ARM_LABELS[@]}"; do
  label="${EFFECTIVE_LABELS[$i]}"
  arm_requested "$label" || continue
  if arm_complete "$WS0_RESULTS/$label"; then
    log "  COMPLETE   $label  ($(wc -l < "$WS0_RESULTS/$label/points.jsonl") points)"
  else
    log "  INCOMPLETE $label  <- re-run this script to retry it"
    FAILED=1
  fi
done

echo "$(date -u +%FT%TZ) ALL-ARMS-ATTEMPTED failed=$FAILED stamp_failed=$STAMP_FAILED" >> "$PROG"
if [ "$FAILED" -eq 1 ]; then
  log "SWEEP INCOMPLETE — at least one arm has no summary.json. Re-run this script (completed arms are skipped)."
  exit 1
fi
if [ "$STAMP_FAILED" -gt 0 ]; then
  log "SWEEP MEASURED BUT UNCERTIFIED — $STAMP_FAILED arm(s) carry no ceiling/corpus-digest stamp."
  log "  The points are on disk; analyze-3225.py will refuse them until the stamp is repaired."
  exit 1
fi
log "SWEEP COMPLETE. Analyse with:"
log "  python3 $HERE/analyze-3225.py $WS0_RESULTS"
