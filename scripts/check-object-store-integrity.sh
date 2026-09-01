#!/usr/bin/env bash
#
# check-object-store-integrity.sh — the #3749 SHARED-OBJECT-STORE integrity sweep.
#
# WHAT QUESTION THIS ANSWERS
# --------------------------
# On this fleet EVERY LANE ON A BOX IS A `git worktree` OF ONE SHARED `.git`
# (measured: `/data/lanes/repo/.git/objects` for lane-3544, lane-3473 and lane-3629
# alike). Git does NOT rehash a packed or loose object against the id it was asked
# for on an ordinary read: it verifies the pack CRC and the zlib stream, which catch
# bit rot and a truncated or torn write, but a whole object whose CONTENT does not
# hash to its own name is returned without complaint. So every consumer of that
# store — including the gate's component-set pre-flight, which reads `origin/main`'s
# committed manifest and HEAD's committed component declaration THROUGH it
# (`scripts/agent-gate.sh`, the `_CS_BASE_OBJ=reused` fast path) — is trusting
# content it never verified.
#
# THE SCOPE OF THIS SCRIPT IS ACCIDENTAL CORRUPTION, AND THAT IS AN OWNER RULING
# (#3749, 2026-09-01), NOT AN OVERSIGHT
# -----------------------------------------------------------------------------
# DELIBERATE peer forgery is INVOKER-CLASS and OUT OF MODEL. Per the #3312 triage
# rule recorded in CLAUDE.md — *same-host actors able to write these scripts are
# invoker-class, not third parties* — a peer lane that wants a false gate PASS can
# simply edit `scripts/agent-gate.sh`, which is cheaper than forging pack data. The
# ruling REJECTED all three hardening alternatives, and the reasons are recorded so
# they are not re-derived:
#   * per-lane full clones — a permanent multi-GB tax on every lane for a threat
#     that is out of model;
#   * per-read rehashing — the fourth carve into one pre-flight, and a permanent
#     cost on every `--lite` round;
#   * removing the object-reuse fast path — a HALF-closure: the ancestry walk and
#     the provenance leg must still read HEAD's COMMITTED content, which has no
#     source other than this store.
# What IS in model is corruption nobody intended: bit rot, a torn pack write, a
# full disk mid-write, a SIGKILLed `git gc`. That is what this sweep closes, and
# `git fsck` is the only thing that answers it, because it REHASHES.
#
# NOT `--connectivity-only`, EVER. `--connectivity-only` walks the reachability
# graph and does NOT rehash object content, so it cannot detect the corruption this
# script exists to find. It would make the sweep fast and vacuous. Do not
# "optimise" it in.
#
# NOT `--strict` either, and for the opposite reason: `--strict` promotes legitimate
# historical warnings (a malformed committer line, a zero-padded file mode) to
# errors, so it would report CORRUPT on a healthy store — the guard operators learn
# to waive.
#
# THE VOCABULARY IS CHOSEN SO THIS CANNOT BE READ AS A CERTIFICATION
# ------------------------------------------------------------------
# The house idiom is `scripts/flow/base-staleness.sh`, and the properties are the
# same ones (CLAUDE.md documents why each is load-bearing):
#   (a) EVERY output line, stdout AND stderr, begins with `OBJECT-STORE: `.
#   (b) Every dynamic field is CONTROL-CHARACTER SANITIZED (newline, CR, other C0,
#       DEL -> a visible escape). GIT PERMITS NEWLINES IN PATHS and an fsck
#       diagnostic quotes paths verbatim, so an unsanitized field emits a line with
#       NO PREFIX AT ALL, breaking the one invariant every consumer and every test
#       rests on. Fields are otherwise kept VERBATIM: an object id or a path that
#       has been masked is useless to the operator who has to act on it.
#   (c) The verdict appears ONLY on an `OBJECT-STORE: verdict ` line, and its token
#       is from the CLOSED set {VERIFIED, CORRUPT, UNMEASURED}. Continuation prose
#       goes on `verdict-detail` lines, so the verdict line's token position can
#       never hold a word.
#   (d) This script's own STATIC TEMPLATE TEXT contains no other verdict
#       vocabulary — asserted STRUCTURALLY over the source file by
#       `scripts/tests/test_check_object_store_integrity.sh`, because that is a
#       provable property while a claim about one sample run is not.
#
# EXIT CODES, AND THE CONSUMER CONTRACT
# -------------------------------------
#   0   VERIFIED   — the sweep RAN TO COMPLETION and reported no corruption.
#   4   CORRUPT    — fsck reported corruption. The affected object ids are named.
#   5   UNMEASURED — the answer was not obtained: no git, no resolvable object
#                    store, no usable timeout binary, the bound expired, or an fsck
#                    failure this script cannot classify.
#   2   usage error — and `--help` exits 2 as well, deliberately: exit 0 MEANS
#                    VERIFIED here, so a run that measured nothing must never
#                    produce it.
#
# *** A CONSUMER MUST NOT READ `UNMEASURED` AS CLEAN. ***
# That is CLAUDE.md's standing rule: never derive a pass from the absence of a bad
# signal; where the sole oracle could not be consulted the verdict is non-passing
# and its text names what was unverifiable. It is stated here and asserted by a
# test, because the shape that keeps recurring in this repo is a multi-state signal
# whose unmeasured state inherits the permissive branch.
#
# THERE IS DELIBERATELY NO KNOB THAT CAN PRODUCE `VERIFIED`. `--timeout` can only
# make the bound tighter or looser, and a tighter bound can only yield UNMEASURED;
# nothing here can be set to manufacture a clean verdict (#3312: an override is
# settable by the party it constrains).
#
# IT MUTATES NOTHING. `git fsck` is read-only, and this script writes no file, no
# ref and no config. Callers that need a throttle keep their own stamp file
# (`scripts/local/worker-supervisor.sh` does).
#
# USAGE
#   scripts/check-object-store-integrity.sh [--repo <path>] [--timeout <secs>]
#   scripts/check-object-store-integrity.sh --help
#
# CALLERS (both go through THIS script — a second implementation would be a second
# place for the verdict to drift):
#   * scripts/bootstrap-agent-machine.sh          — once, at machine onboarding
#   * scripts/local/worker-supervisor.sh          — throttled, per-iteration
#
# macOS bash 3.2 compatible, shellcheck-clean.
set -uo pipefail

# Ambient git state that would otherwise bend this measurement, pinned in one
# place (the idiom and the rationale are base-staleness.sh's):
#   GIT_NO_LAZY_FETCH=1     — in a partial/promisor clone an object read fetches
#                             over the network and WRITES a packfile into the store
#                             this script is auditing. Honoured from git 2.36.
#   GIT_NO_REPLACE_OBJECTS=1 — `refs/replace/*` substitutes objects, so a single
#                             local replacement ref could change which objects the
#                             sweep visits.
export GIT_NO_LAZY_FETCH=1
export GIT_NO_REPLACE_OBJECTS=1

P='OBJECT-STORE:'

# sane <string> -> the string with every C0 control character and DEL replaced by a
# VISIBLE escape, on stdout. Applied to EVERY dynamic field (property (b)).
sane() {
  local s="$1" out c i n
  s="${s//$'\r'/'\r'}"
  s="${s//$'\n'/'\n'}"
  s="${s//$'\t'/'\t'}"
  case "$s" in
    *[[:cntrl:]]*) ;;
    *)
      printf '%s' "$s"
      return 0
      ;;
  esac
  out=""
  n=${#s}
  i=0
  while [ "$i" -lt "$n" ]; do
    c="${s:i:1}"
    case "$c" in
      [[:cntrl:]]) out=$(printf '%s\\x%02x' "$out" "'$c") ;;
      *) out="$out$c" ;;
    esac
    i=$((i + 1))
  done
  printf '%s' "$out"
}

# EVERY line here is prefixed too: under property (a) the prefix is THE
# load-bearing invariant, so an unprefixed usage line is a hole in it. `${0##*/}`
# rather than `basename` — an external command whose stderr is not captured here
# would emit an unprefixed diagnostic from the one function whose job is to be
# readable when the call was wrong.
usage() {
  printf '%s USAGE - the call is wrong (this is NOT a measurement verdict)\n' "$P" >&2
  printf '%s USAGE usage: %s [--repo <path>] [--timeout <secs>]\n' \
    "$P" "$(sane "${0##*/}")" >&2
  printf '%s USAGE Rehashes the SHARED git object store behind <path> with git fsck\n' "$P" >&2
  printf '%s USAGE and reports whether it is intact (#3749). Read-only; mutates nothing.\n' "$P" >&2
  printf '%s USAGE Exits 0 verified / 4 corrupt / 5 unmeasured / 2 usage.\n' "$P" >&2
  printf '%s USAGE A CONSUMER MUST NOT READ EXIT 5 AS CLEAN (nothing was measured).\n' "$P" >&2
  printf '%s USAGE Scope is ACCIDENTAL corruption. Deliberate peer forgery is\n' "$P" >&2
  printf '%s USAGE invoker-class and OUT OF MODEL (#3749 owner ruling, #3312 triage).\n' "$P" >&2
}

# unmeasured <cause...> — exit 5. Prints NO clean signal of any kind, so it can
# never be misread as a completed sweep.
unmeasured() {
  while [ "$#" -gt 0 ]; do
    printf '%s unmeasured-cause %s\n' "$P" "$(sane "$1")"
    shift
  done
  printf '%s verdict UNMEASURED\n' "$P"
  printf '%s verdict-detail the sweep could not be performed. A CONSUMER MUST NOT READ THIS\n' "$P"
  printf '%s verdict-detail AS CLEAN (#3749); it is not a certification.\n' "$P"
  exit 5
}

# --- argument parsing: every unrecognised argument is refused ----------------
REPO="."
BOUND_SECS=300
repo_set=0
bound_set=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    -h | --help)
      usage
      exit 2
      ;;
    --repo)
      [ "$#" -ge 2 ] || { usage; exit 2; }
      [ "$repo_set" -eq 0 ] || { usage; exit 2; }
      REPO="$2"
      repo_set=1
      shift 2
      ;;
    --timeout)
      [ "$#" -ge 2 ] || { usage; exit 2; }
      [ "$bound_set" -eq 0 ] || { usage; exit 2; }
      # Validated as a POSITIVE integer, never coerced: a bare word would evaluate
      # to 0 in the bound and kill the sweep instantly, which under the
      # classification below is UNMEASURED — a silently self-disabling bound.
      case "$2" in
        '' | *[!0-9]*) usage; exit 2 ;;
      esac
      [ "$2" -ge 1 ] || { usage; exit 2; }
      BOUND_SECS="$2"
      bound_set=1
      shift 2
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

# --- the bound: resolve it BEFORE running anything ---------------------------
#
# THE SWEEP IS BOUNDED, AND AN UNBOUNDABLE HOST DOES NOT GET TO RUN IT. Both
# callers are hang-sensitive: an unbounded fsck can wedge machine onboarding, and
# in the supervisor it sits in the per-iteration preflight path. Refusing is
# UNMEASURED, which is non-passing, so nothing is certified on the strength of a
# probe we declined to run.
#
# The candidate is PROBED for `--kill-after` rather than sniffed by name (the
# idiom, and the reasoning, are bootstrap-agent-machine.sh's): BusyBox and older
# implementations reject the flag, and a selected binary that rejects it would make
# every bounded call fail. SIGTERM-only is ACCEPTED here — unlike a credential
# helper, `git fsck` does not trap or ignore SIGTERM — and the degradation is
# NAMED in the output rather than left silent.
TIMEOUT_BIN=""
TIMEOUT_KILL_AFTER=0
for _tb_name in timeout gtimeout; do
  _tb_path="$(command -v "$_tb_name" 2>/dev/null || true)"
  [ -n "$_tb_path" ] || continue
  if "$_tb_path" --kill-after=1 1 true >/dev/null 2>&1; then
    TIMEOUT_BIN="$_tb_path"
    TIMEOUT_KILL_AFTER=1
    break
  fi
  [ -n "$TIMEOUT_BIN" ] || TIMEOUT_BIN="$_tb_path"
done
unset _tb_name _tb_path
BOUND_KILL_GRACE=5

command -v git >/dev/null 2>&1 ||
  unmeasured "git is not on PATH, so the object store cannot be rehashed at all"

if [ -z "$TIMEOUT_BIN" ]; then
  unmeasured "no timeout/gtimeout on PATH - refusing to run an UNBOUNDED fsck: both" \
    "callers are hang-sensitive (machine onboarding, and the supervisor's" \
    "per-iteration preflight). Install GNU coreutils and re-run."
fi

# --- resolve the SHARED object store ----------------------------------------
#
# `--git-common-dir`, NEVER `--git-dir`: in a linked worktree `--git-dir` answers
# `<repo>/.git/worktrees/<lane>`, which is the LANE's private administrative
# directory, while the objects every lane on the box shares live under the COMMON
# dir (measured on this fleet: toplevel /data/lanes/lane-NNNN, common dir
# /data/lanes/repo/.git). Sweeping the per-worktree dir would audit the wrong
# thing and report VERIFIED about a store it never read.
if ! GIT_COMMON_DIR_RAW=$(git -C "$REPO" rev-parse --git-common-dir 2>/dev/null) ||
  [ -z "$GIT_COMMON_DIR_RAW" ]; then
  unmeasured "git -C $(sane "$REPO") rev-parse --git-common-dir failed: not a git" \
    "repository, or the repository is unreadable"
fi
# The value may be RELATIVE (a plain `.git`), and it is relative to the REPO, not
# to this script's cwd.
if ! GIT_COMMON_DIR=$(cd "$REPO" 2>/dev/null && cd "$GIT_COMMON_DIR_RAW" 2>/dev/null && pwd -P); then
  GIT_COMMON_DIR=""
fi
if [ -z "$GIT_COMMON_DIR" ]; then
  unmeasured "the git common directory ($(sane "$GIT_COMMON_DIR_RAW")) could not be" \
    "canonicalized from $(sane "$REPO") - absent, unreadable, or not a directory"
fi
OBJ_DIR="$GIT_COMMON_DIR/objects"
if [ ! -d "$OBJ_DIR" ] || [ ! -r "$OBJ_DIR" ]; then
  unmeasured "the object store $(sane "$OBJ_DIR") is absent or unreadable, so there is" \
    "nothing this run can rehash"
fi

# --- scratch space (outside the repository: this script writes nothing in it) --
if ! TMPD=$(mktemp -d "${TMPDIR:-/tmp}/object-store-integrity.XXXXXX" 2>/dev/null) ||
  [ -z "$TMPD" ] || [ ! -d "$TMPD" ]; then
  unmeasured "could not create a scratch dir under $(sane "${TMPDIR:-/tmp}")"
fi
trap 'rm -rf "$TMPD" 2>/dev/null' EXIT

printf '%s store %s\n' "$P" "$(sane "$OBJ_DIR")"
printf '%s subject %s (resolved via git rev-parse --git-common-dir, NOT --git-dir)\n' \
  "$P" "$(sane "$REPO")"
if [ "$TIMEOUT_KILL_AFTER" -eq 1 ]; then
  printf '%s bound %ss (hard: SIGTERM then SIGKILL after %ss)\n' "$P" "$BOUND_SECS" "$BOUND_KILL_GRACE"
else
  printf '%s bound %ss (SIGTERM-only: %s does not accept --kill-after; git fsck does not trap SIGTERM)\n' \
    "$P" "$BOUND_SECS" "$(sane "$TIMEOUT_BIN")"
fi

# --- THE SWEEP --------------------------------------------------------------
#
# Full fsck: it REHASHES object content, which is the whole point (see the header
# on why `--connectivity-only` would be vacuous here and `--strict` would be a
# false positive). `--no-dangling` because an unreachable object is ordinary in a
# store that has held reset branches, not corruption. `--no-progress` because
# progress output is not a finding and would pollute the anchored stream.
#
# `nice`d: this is a hygiene sweep on a box that runs up to 4 gates. Measured on
# this fleet's 331M shared store (one 219M pack): 19.83s elapsed, 64% cpu, 426MB
# maxrss. The 300s default bound is ~15x that headroom — generous on purpose,
# because the bound exists to stop a HANG, not to police duration: a cold cache
# under four concurrent gates can legitimately be several times slower, and a
# bound that expires on a healthy-but-busy box produces UNMEASURED noise nobody
# acts on.
START_TS=$(date +%s 2>/dev/null || echo 0)
fsck_rc=0
if [ "$TIMEOUT_KILL_AFTER" -eq 1 ]; then
  nice -n 19 "$TIMEOUT_BIN" --kill-after="$BOUND_KILL_GRACE" "$BOUND_SECS" \
    git --git-dir="$GIT_COMMON_DIR" fsck --no-progress --no-dangling \
    >"$TMPD/fsck.out" 2>"$TMPD/fsck.err" || fsck_rc=$?
else
  nice -n 19 "$TIMEOUT_BIN" "$BOUND_SECS" \
    git --git-dir="$GIT_COMMON_DIR" fsck --no-progress --no-dangling \
    >"$TMPD/fsck.out" 2>"$TMPD/fsck.err" || fsck_rc=$?
fi
END_TS=$(date +%s 2>/dev/null || echo 0)
ELAPSED=$((END_TS - START_TS))
[ "$ELAPSED" -ge 0 ] || ELAPSED=0

cat "$TMPD/fsck.out" "$TMPD/fsck.err" >"$TMPD/fsck.all" 2>/dev/null || : >"$TMPD/fsck.all"

# --- CLASSIFY ---------------------------------------------------------------
#
# CORRUPTION IS RECOGNISED FROM fsck's OWN DIAGNOSTIC SHAPES, not from "the exit
# status was non-zero" — because a non-zero fsck also covers `fatal: not a git
# repository` and every other way the invocation can fail, and calling that CORRUPT
# would send an operator hunting for damage that is not there. The recognised
# shapes (verified against git 2.43.0 on planted fixtures, both of which the test
# suite plants):
#   error: inflate: data stream error ...                 (a torn/rotted object)
#   error: <sha>: object corrupt or missing: <path>
#   error: <sha>: hash-path mismatch, found at: <path>    (content != its own name)
#   missing blob|tree|commit|tag <sha>
#   broken link from ... to ...
# Anything containing `corrupt` is included too, wherever git puts it.
#
# WARNINGS ARE NOT CORRUPTION and are deliberately not matched: `warning in commit
# <sha>: missingSpaceBeforeEmail` is legitimate historical sloppiness, git exits 0
# on it, and matching it would report CORRUPT on a healthy store.
: >"$TMPD/findings"
sed -n -e '/^error/p' -e '/^missing /p' -e '/^broken link/p' -e '/corrupt/p' \
  "$TMPD/fsck.all" >"$TMPD/findings" 2>/dev/null || : >"$TMPD/findings"
n_findings=$(grep -c . "$TMPD/findings" 2>/dev/null | tr -d ' ')
case "$n_findings" in '' | *[!0-9]*) n_findings=0 ;; esac

# The affected object ids: every 40-hex token in the findings, deduped. Extracted
# from the findings themselves so an id can never be reported without the
# diagnostic that named it; the diagnostics are printed verbatim as well, because
# a shape this extractor does not recognise must still reach the operator.
: >"$TMPD/ids"
tr -c '0-9a-f' '\n' <"$TMPD/findings" 2>/dev/null |
  awk 'length($0) == 40 { print }' | sort -u >"$TMPD/ids" 2>/dev/null || : >"$TMPD/ids"

FINDING_LIST_LIMIT=40

if [ "$n_findings" -gt 0 ]; then
  n=0
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    n=$((n + 1))
    [ "$n" -le "$FINDING_LIST_LIMIT" ] && printf '%s finding %s\n' "$P" "$(sane "$line")"
  done <"$TMPD/findings"
  [ "$n" -gt "$FINDING_LIST_LIMIT" ] &&
    printf '%s finding (+%s further fsck diagnostics, not listed)\n' "$P" "$((n - FINDING_LIST_LIMIT))"
  while IFS= read -r oid; do
    [ -n "$oid" ] || continue
    printf '%s object %s\n' "$P" "$(sane "$oid")"
  done <"$TMPD/ids"
  printf '%s measured fsck rc=%s in %ss over %s\n' "$P" "$fsck_rc" "$ELAPSED" "$(sane "$OBJ_DIR")"
  printf '%s verdict CORRUPT\n' "$P"
  printf '%s verdict-detail %s fsck diagnostic(s) name damaged or unhashable objects in the\n' "$P" "$n_findings"
  printf '%s verdict-detail SHARED store. Every lane on this box reads it, so it can change ANY\n' "$P"
  printf '%s verdict-detail gate verdict here: do NOT certify anything against this checkout.\n' "$P"
  printf '%s verdict-detail REMEDY: stop the lanes on this box, then re-obtain the objects from the\n' "$P"
  printf '%s verdict-detail canonical remote (a fresh clone of pmcfadin/cqlite, or\n' "$P"
  printf '%s verdict-detail `git fetch --force origin` if the damage is confined to fetched packs).\n' "$P"
  printf '%s verdict-detail A LOCAL `git gc`/`git repack` CANNOT REPAIR THIS - it rewrites the same\n' "$P"
  printf '%s verdict-detail damaged content, or refuses. Escalate rather than improvising (#3749).\n' "$P"
  exit 4
fi

# 124 = SIGTERM'd at the bound; 137 = it ignored SIGTERM and --kill-after
# escalated to SIGKILL. A KILLED SWEEP IS UNMEASURED, NEVER VERIFIED: it exited
# without having rehashed the rest of the store, and its silence up to that point
# is the absence of a bad signal, not a clean answer.
if [ "$fsck_rc" -eq 124 ] || [ "$fsck_rc" -eq 137 ]; then
  unmeasured "the fsck exceeded its ${BOUND_SECS}s bound and was killed (rc=$fsck_rc) after" \
    "${ELAPSED}s - it never finished rehashing the store, so its silence is NOT a" \
    "clean result. Re-run with a larger --timeout on an idle box."
fi

# ANY OTHER NON-ZERO STATUS IS UNMEASURED, NOT CLEAN AND NOT CORRUPT. fsck failed
# in a way this script does not recognise (an unreadable pack directory, a git too
# old for a flag, a `fatal:` about the repository itself). Guessing CORRUPT would
# send an operator after damage that may not exist; guessing VERIFIED would be the
# permissive-unknown branch this whole file refuses.
if [ "$fsck_rc" -ne 0 ]; then
  unmeasured "git fsck exited $fsck_rc after ${ELAPSED}s with no recognised corruption" \
    "diagnostic, so this run can classify it as neither clean nor corrupt." \
    "fsck said: $(head -c 400 "$TMPD/fsck.all" 2>/dev/null)"
fi

# --- THE ONE AFFIRMATIVE BRANCH --------------------------------------------
#
# VERIFIED REQUIRES EVIDENCE THE SWEEP RAN AND COMPLETED, not merely that nothing
# bad was printed. The evidence is the fsck PROCESS's OWN exit status 0, and it is
# affirmative for a stated reason: the process ran under a bound whose kill
# statuses (124/137) are distinguishable and are routed to UNMEASURED above, and an
# fsck that could not start, could not read the store or died on a signal cannot
# produce 0. `git fsck` exits 0 only after walking and REHASHING every object it
# was asked to check. So `rc == 0` here means "it finished, and it found nothing" —
# two facts, not one — while every state in which the first fact is unknown has
# already been routed to UNMEASURED.
printf '%s measured fsck rc=0 in %ss over %s (full rehash: not --connectivity-only)\n' \
  "$P" "$ELAPSED" "$(sane "$OBJ_DIR")"
printf '%s verdict VERIFIED\n' "$P"
printf '%s verdict-detail git fsck ran to completion and reported no damaged objects.\n' "$P"
printf '%s verdict-detail SCOPE: this is a POINT-IN-TIME sweep of ACCIDENTAL corruption, not a\n' "$P"
printf '%s verdict-detail per-read guarantee and not a defence against deliberate forgery, which\n' "$P"
printf '%s verdict-detail is invoker-class and out of model (#3749 owner ruling, #3312 triage).\n' "$P"
exit 0
