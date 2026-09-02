#!/usr/bin/env bash
#
# Regression tests for scripts/check-object-store-integrity.sh (issue #3749).
#
# Fast + HERMETIC: every case builds a synthetic git repo under one `mktemp -d`, and
# nothing here reads THIS box's shared object store — a suite whose verdict depended on
# the health of the machine running it would be untestable and unattributable. No
# network, no `gh`, no cargo.
#
# The suite carries five things beyond ordinary cases, because a guard here can be
# SATISFIED AND WRONG:
#
#   1. RED-ARM DISCIPLINE, EXPLICIT (CLAUDE.md: "a bare red is not evidence"). Every
#      corruption case differs from a CLEAN TWIN built by the same code path in EXACTLY
#      ONE property — the planted damage — and the construction is ASSERTED with git
#      before the subject is run, so a case cannot pass against a fixture that never had
#      the property under test. An unrelated breakage produces an identical exit code.
#   2. A PLANTED MUTANT proving the FULL REHASH is load-bearing (Case 6). A copy of the
#      script with `--connectivity-only` added must (a) genuinely carry that defect and
#      nothing else, and (b) report VERIFIED on a store the real script calls CORRUPT.
#      Measured on git 2.43.0: `git fsck --connectivity-only` exits 0 on a hash-path
#      mismatch. That is why the script's header forbids the flag, and this is the case
#      that stops someone "optimising" it in.
#   3. THE ANCHORED OUTPUT GUARANTEE, whole-suite: every nonempty line of EVERY run,
#      stdout AND stderr, begins with `OBJECT-STORE: `, and every `verdict ` line carries
#      a token from the CLOSED set. Violations ACCUMULATE to files and are reported once,
#      from the EXIT trap — never from a position in this file, which is maintained by
#      hand and would silently shrink as cases are appended (the #3650 R6 F3 lesson).
#   4. THE STATIC-TEMPLATE ASSERTION (Case 13), structural over the source: the script's
#      own literal text carries no FOREIGN verdict vocabulary (`PASS`, `OK`, `RESULT:`) so
#      its output can never be mistaken for an AGENT-GATE/ROBOREV/PREMERGE block, and its
#      OWN verdict tokens appear only on `verdict ` templates. Provable, unlike a claim
#      about one sample run.
#   5. A CASE FLOOR. A span-replacing edit that silently deletes cases while the suite
#      reports green is a recorded incident in this repo (#3544 deleted four).
#
# Run standalone:   bash scripts/tests/test_check_object_store_integrity.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SUBJECT="$SCRIPT_DIR/../check-object-store-integrity.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

if [ ! -r "$SUBJECT" ]; then
  printf 'FAIL - the subject %s is not readable\n' "$SUBJECT" >&2
  exit 1
fi

# THE SCRATCH DIR IS VALIDATED BEFORE ANY PATH IS BUILT FROM IT. An unchecked `mktemp`
# leaves $T empty, after which every "$T/..." resolves at the filesystem ROOT — and the
# trap would run `rm -rf ""`.
if ! T=$(mktemp -d "${TMPDIR:-/tmp}/object-store-integrity-test.XXXXXX" 2>/dev/null) ||
  [ -z "$T" ] || [ ! -d "$T" ]; then
  printf 'FAIL - could not create a scratch directory under %s\n' "${TMPDIR:-/tmp}" >&2
  exit 1
fi

ALL_OUT="$T/all-output.txt"
ANCHOR_BAD="$T/anchor-violations.txt"
VERDICT_BAD="$T/verdict-violations.txt"
: >"$ALL_OUT"
: >"$ANCHOR_BAD"
: >"$VERDICT_BAD"

RECORD_CALLS=0
INSPECTED_RECORDS=-1
WHOLE_SUITE_RUNS=0
FINISHED=0

# record_out <tag> — accumulate $OUT and check the anchored invariants on it. Called from
# run() so no case can forget it.
record_out() {
  local tag="$1" line tok
  RECORD_CALLS=$((RECORD_CALLS + 1))
  printf '%s\n' "$OUT" >>"$ALL_OUT"
  while IFS= read -r line; do
    [ -n "$line" ] || continue
    case "$line" in
      'OBJECT-STORE: '*) ;;
      *) printf '%s\t%s\n' "$tag" "$line" >>"$ANCHOR_BAD" ;;
    esac
    case "$line" in
      'OBJECT-STORE: verdict '*)
        tok=${line#'OBJECT-STORE: verdict '}
        tok=${tok%% *}
        case "$tok" in
          VERIFIED | CORRUPT | UNMEASURED) ;;
          *) printf '%s\t%s\n' "$tag" "$line" >>"$VERDICT_BAD" ;;
        esac
        ;;
    esac
  done <<RECORD_OUT
$OUT
RECORD_OUT
}

# verdict_of — the token on $OUT's single `verdict ` line, or the empty string.
verdict_of() {
  local v
  v=$(printf '%s\n' "$OUT" | grep '^OBJECT-STORE: verdict ' | head -1)
  v=${v#'OBJECT-STORE: verdict '}
  printf '%s' "${v%% *}"
}
verdict_lines() { printf '%s\n' "$OUT" | grep -c '^OBJECT-STORE: verdict ' | tr -d ' '; }

# whole_suite_checks — invoked ONLY from finish (the EXIT trap), so a case appended
# anywhere in this file is inspected. DO NOT CALL IT ANYWHERE ELSE: the count
# reconciliation below reds the suite if it runs more than once or inspects fewer runs
# than the suite recorded.
whole_suite_checks() {
  local nonempty cov_missing needle
  WHOLE_SUITE_RUNS=$((WHOLE_SUITE_RUNS + 1))

  nonempty=$(grep -c . "$ALL_OUT" | tr -d ' ')
  if [ "$nonempty" -lt 80 ]; then
    bad "anchor: only $nonempty accumulated lines — the whole-suite assertion would be weak"
  else
    ok "anchor: the whole-suite assertion inspects $nonempty output lines from every run"
  fi
  cov_missing=""
  for needle in 'verdict VERIFIED' 'verdict CORRUPT' 'verdict UNMEASURED' 'USAGE'; do
    grep -q "$needle" "$ALL_OUT" || cov_missing="$cov_missing '$needle'"
  done
  if [ -z "$cov_missing" ]; then
    ok "anchor: the accumulated output covers all THREE verdicts AND the usage path"
  else
    bad "anchor: accumulated output missing:$cov_missing — narrower than the suite claims"
  fi
  if [ -s "$ANCHOR_BAD" ]; then
    bad "anchor: $(grep -c . "$ANCHOR_BAD" | tr -d ' ') line(s) lack the 'OBJECT-STORE: ' prefix; first: $(head -1 "$ANCHOR_BAD")"
  else
    ok "anchor: EVERY nonempty line of EVERY run, stdout AND stderr, begins with 'OBJECT-STORE: '"
  fi
  if [ -s "$VERDICT_BAD" ]; then
    bad "anchor: a 'verdict ' line carries a token outside the closed set; first: $(head -1 "$VERDICT_BAD")"
  else
    ok "anchor: every 'verdict ' token is from {VERIFIED, CORRUPT, UNMEASURED}"
  fi
  [ "$INSPECTED_RECORDS" -lt 0 ] && INSPECTED_RECORDS=$RECORD_CALLS
}

# THE CASE FLOOR is a MINIMUM, not an equality: adding cases must not require editing it,
# while a span-replacing edit that DELETES cases reds the suite instead of reporting a
# green tally over a shrunken suite (#3544's own subject, inside its own test file).
#
# IT IS SET TO THE EXACT CURRENT COUNT, and the slack it used to carry was itself a
# defect (#3749 review round 3, item 4): a floor of 34 against 74 actual meant HALF the
# suite could vanish — or be cut short by a signal — and still report green. Every
# host-dependent branch in this file emits a `bad` rather than silently running fewer
# assertions (the two `command -v` guards and the fixture-construction asserts), so a
# green run hits exactly the number below on any host that can run it at all; a green run
# BELOW it means cases were removed or the run was truncated. The count is written ONCE,
# as the value: a second copy of it in this comment went stale for three rounds and a
# restated constant is a magic number wearing a relation's clothes. RAISE IT when you add
# cases; LOWER IT when you deliberately remove them, never "for safety" — a floor above
# the real count is a permanently red suite.
CASE_FLOOR=120

finish() {
  local rc=$?
  if [ "$FINISHED" -eq 1 ]; then
    rm -rf "$T"
    return
  fi
  FINISHED=1
  whole_suite_checks
  if [ "$WHOLE_SUITE_RUNS" -ne 1 ]; then
    bad "whole-suite: the accumulated-output assertions ran $WHOLE_SUITE_RUNS times, not once — they belong to finish() alone"
  elif [ "$INSPECTED_RECORDS" -ne "$RECORD_CALLS" ]; then
    bad "whole-suite: they inspected $INSPECTED_RECORDS recorded runs but the suite recorded $RECORD_CALLS — do NOT reposition the check, it must run from finish()"
  else
    ok "whole-suite: the assertions inspected EVERY one of the $RECORD_CALLS recorded runs"
  fi
  if [ "$PASS" -lt "$CASE_FLOOR" ] && [ "$FAIL" -eq 0 ]; then
    printf 'FAIL - case-floor: %d cases ran but this suite declares a floor of %d — cases were REMOVED (or are skipping) without the floor being lowered deliberately.\n' "$PASS" "$CASE_FLOOR"
    FAIL=$((FAIL + 1))
  fi
  printf '\n=== object-store-integrity: %d passed, %d failed (floor %d) ===\n' "$PASS" "$FAIL" "$CASE_FLOOR"
  rm -rf "$T"
  if [ "$FAIL" -ne 0 ] || [ "$rc" -ne 0 ]; then
    exit 1
  fi
  exit 0
}
# EXIT *and* the signals: bash runs no EXIT trap for a signal left at its default
# disposition, so an interrupted run would strand the scratch tree.
# THE SIGNAL TRAPS SET AN EXPLICIT NONZERO STATUS AND LET **EXIT** DO THE CLEANUP
# (#3749 review round 3, item 4). They used to call `finish` directly, and `finish`
# derives its exit status from `$?` — which at trap time is the status of whatever
# command was interrupted, routinely **0**. So a signal arriving mid-suite ran the
# cleanup, reported the cases that had passed SO FAR, and **exited 0** with every later
# case never run: a green tally over a shrunken suite, which is this repository's own
# recorded incident class (#3544) inside a file whose header claims to guard against it.
# `exit <128+signo>` makes the EXIT trap run with a nonzero `$?`, so `finish` reports
# FAIL, and the shell's conventional status for the signal is preserved.
trap finish EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# --- fixtures ---------------------------------------------------------------
g() { local r="$1"; shift; git -C "$r" "$@"; }

# THE FIXTURE CLOCK IS PINNED, AND THE SUITE'S VALIDITY RESTS ON IT — DO NOT REMOVE.
# A commit's sha is a function of its author/committer DATES as well as its tree, so an
# unpinned builder makes every fixture's object ids a function of THE WALL-CLOCK SECOND
# THE FIXTURE WAS BUILT IN. Two `newrepo` calls landing either side of a second boundary
# then produce DIFFERENT commit shas, which is a wall-clock race in a correctness test
# (CLAUDE.md pre-roborev self-check; scripts/tests/check-no-wallclock-asserts.sh, #2642).
#
# It is not hypothetical and it is not cosmetic: Case 18's POSITIVE CONTROL runs a plain
# git with `GIT_OBJECT_DIRECTORY=<clean>/.git/objects --git-dir=<mismatch>/.git` and
# requires exit 0 — which holds ONLY while the two fixtures share object ids, because
# otherwise <mismatch>'s refs name objects absent from <clean>'s store and fsck raises
# ERROR_REACHABLE (exit 2). Measured with this exact recipe: same second =>
# b1d2aeee4db43bcb41f647c78430314a2d0a5a58 twice; 1.2s later =>
# 7efdf0e1453622b5bc6ff818e508f4857ce89455. Observed intermittently as
# "env-plant: a plain git was not redirected by GIT_OBJECT_DIRECTORY (rc=2)" on a loaded
# box, which would red `tooling-tests` in the gate of record for every lane, forever.
#
# The control was RIGHT to fail-close there; the fixture construction was what was wrong.
# The fixed epoch is arbitrary (matching the 1700000000 used elsewhere in this file) and
# carries an explicit `+0000` so the value does not depend on the box's TZ either.
FIXTURE_DATE='1700000000 +0000'

# newrepo <name> -> path. Two blobs, one commit. THE ONE CODE PATH every fixture is
# built by, so a corruption case's CLEAN TWIN is identical but for the planted damage —
# BYTE-IDENTICAL, object ids included, which is what the pin above buys.
newrepo() {
  local r="$T/$1"
  mkdir -p "$r"
  git init -q "$r" >/dev/null 2>&1
  g "$r" config user.email t@t
  g "$r" config user.name t
  printf 'content aaa\n' >"$r/f1"
  printf 'content bbb\n' >"$r/f2"
  g "$r" add f1 f2 >/dev/null
  GIT_AUTHOR_DATE="$FIXTURE_DATE" GIT_COMMITTER_DATE="$FIXTURE_DATE" \
    g "$r" -c user.email=t@t -c user.name=t commit -q -m c1 >/dev/null
  printf '%s' "$r"
}

loose_path() {
  local r="$1" sha="$2"
  printf '%s' "$r/.git/objects/${sha:0:2}/${sha:2}"
}

# run <expected-exit> <desc> [args...] — run the subject, set $OUT/$RC, accumulate.
run() {
  local want="$1" desc="$2"
  shift 2
  # The subject is resolved from THIS FILE'S OWN location, with no settable path
  # variable: a case needing a different script SUBSTITUTES the artifact (the mutant
  # case writes its own copy and calls it directly), because a test-only seam is one
  # more thing a real invoker can set (CLAUDE.md #3312 corollary).
  OUT=$(bash "$SUBJECT" "$@" 2>&1)
  RC=$?
  record_out "$desc"
  if [ "$RC" -eq "$want" ]; then
    return 0
  fi
  bad "$desc (exit $RC, wanted $want)"
  printf '%s\n' "$OUT" | head -6
  return 1
}

# --- Case 1: FIXTURE SELF-CONSISTENCY ---------------------------------------
# Asserted with git, not with the subject: a case that used the subject to validate its
# own fixture could not distinguish a broken fixture from a broken subject.
R_CLEAN=$(newrepo clean)
if git -C "$R_CLEAN" rev-parse HEAD >/dev/null 2>&1 &&
  [ -n "$(git -C "$R_CLEAN" rev-parse HEAD:f1 2>/dev/null)" ] &&
  git -C "$R_CLEAN" fsck --no-progress --no-dangling >/dev/null 2>&1; then
  ok "fixture: the clean repo really is a repo with objects, and git itself calls it intact"
else
  bad "fixture: the clean repo is not the shape this suite claims"
fi

# --- Case 2: a clean store is VERIFIED, exit 0 ------------------------------
if run 0 "clean: VERIFIED" --repo "$R_CLEAN"; then
  if [ "$(verdict_of)" = VERIFIED ] && [ "$(verdict_lines)" -eq 1 ]; then
    ok "clean: a clean store yields exactly one 'verdict VERIFIED' line and exit 0"
  else
    bad "clean: verdict was '$(verdict_of)' on $(verdict_lines) verdict line(s)"
  fi
  if printf '%s\n' "$OUT" | grep -q '^OBJECT-STORE: measured fsck rc=0 '; then
    ok "clean: the affirmative branch reports its MEASUREMENT (fsck rc=0), not just a verdict"
  else
    bad "clean: no 'measured' line — VERIFIED must be an affirmative measurement"
  fi
  if printf '%s\n' "$OUT" | grep -q '^OBJECT-STORE: store .*/\.git/objects$'; then
    ok "clean: the run NAMES the object store it swept"
  else
    bad "clean: the run does not name the store it swept"
  fi
fi

# --- Case 3: a ZLIB-DAMAGED loose object is CORRUPT, exit 4 -----------------
# RED ARM, ONE PROPERTY: built by the same `newrepo` as the clean twin above, then ONE
# loose object's bytes are overwritten. The construction is asserted with git first —
# `cat-file` must FAIL on that object — so this case cannot pass against an intact
# fixture, and the exit code alone is never taken as evidence.
R_ROT=$(newrepo rotted)
ROT_SHA=$(git -C "$R_ROT" rev-parse HEAD:f1)
ROT_PATH=$(loose_path "$R_ROT" "$ROT_SHA")
chmod 644 "$ROT_PATH" 2>/dev/null
printf 'not a zlib stream at all' >"$ROT_PATH"
if [ -n "$ROT_SHA" ] && ! git -C "$R_ROT" cat-file -p "$ROT_SHA" >/dev/null 2>&1 &&
  git -C "$R_CLEAN" cat-file -p "$(git -C "$R_CLEAN" rev-parse HEAD:f1)" >/dev/null 2>&1; then
  ok "rot-plant: the plant IS the defect described (that object is unreadable here, readable in the clean twin)"
else
  bad "rot-plant: the fixture is not corrupt (or the clean twin is) — the case below would prove nothing"
fi
if run 4 "rotted: CORRUPT" --repo "$R_ROT"; then
  if [ "$(verdict_of)" = CORRUPT ]; then
    ok "rotted: a zlib-damaged loose object yields 'verdict CORRUPT' and exit 4"
  else
    bad "rotted: verdict was '$(verdict_of)', wanted CORRUPT"
  fi
  if printf '%s\n' "$OUT" | grep -q "^OBJECT-STORE: object $ROT_SHA$"; then
    ok "rotted: the output NAMES the affected object id on its own 'object' line"
  else
    bad "rotted: the affected object id $ROT_SHA is not named on an 'object' line"
  fi
  if printf '%s\n' "$OUT" | grep -q '^OBJECT-STORE: finding '; then
    ok "rotted: fsck's own diagnostic is quoted verbatim on a 'finding' line (not re-worded)"
  else
    bad "rotted: no 'finding' line — the operator gets a verdict with no evidence"
  fi
  if printf '%s\n' "$OUT" | grep -q '^OBJECT-STORE: verdict-detail REMEDY'; then
    ok "rotted: the CORRUPT verdict carries an operator REMEDY"
  else
    bad "rotted: CORRUPT with no remedy line"
  fi
fi

# --- Case 4: a HASH-PATH MISMATCH is CORRUPT --------------------------------
# THE CLASS THIS SCRIPT EXISTS FOR: a whole, well-formed, zlib-valid object whose CONTENT
# does not hash to its own name. An ordinary git read does not notice (no rehash); fsck
# does. One property again: f1's loose object file is replaced by f2's, byte for byte.
R_MIS=$(newrepo mismatch)
MIS_A=$(git -C "$R_MIS" rev-parse HEAD:f1)
MIS_B=$(git -C "$R_MIS" rev-parse HEAD:f2)
MIS_PA=$(loose_path "$R_MIS" "$MIS_A")
MIS_PB=$(loose_path "$R_MIS" "$MIS_B")
chmod 644 "$MIS_PA" 2>/dev/null
cp "$MIS_PB" "$MIS_PA"
if [ -n "$MIS_A" ] && [ -n "$MIS_B" ] && [ "$MIS_A" != "$MIS_B" ] &&
  cmp -s "$MIS_PA" "$MIS_PB" &&
  [ "$(git -C "$R_MIS" cat-file -p "$MIS_A" 2>/dev/null)" = "content bbb" ]; then
  # `cat-file` HANDS BACK THE WRONG CONTENT WITHOUT COMPLAINT — that is the measurement
  # the whole trust boundary rests on, made here rather than asserted from prose.
  ok "mismatch-plant: the plant IS the defect described (git returns f2's content for f1's sha, no error)"
else
  bad "mismatch-plant: the fixture does not carry a hash-path mismatch — the case below would prove nothing"
fi
if run 4 "mismatch: CORRUPT" --repo "$R_MIS"; then
  if [ "$(verdict_of)" = CORRUPT ]; then
    ok "mismatch: a content/name mismatch (the class an ordinary read cannot see) is CORRUPT, exit 4"
  else
    bad "mismatch: verdict was '$(verdict_of)', wanted CORRUPT"
  fi
  if printf '%s\n' "$OUT" | grep -q "^OBJECT-STORE: object $MIS_A$" &&
    printf '%s\n' "$OUT" | grep -q "^OBJECT-STORE: object $MIS_B$"; then
    ok "mismatch: BOTH object ids fsck named are reported"
  else
    bad "mismatch: the reported ids do not include both $MIS_A and $MIS_B"
  fi
fi

# --- Case 5: the CLEAN TWIN of the corruption cases is still VERIFIED -------
# The other half of the one-property discipline: the fixture builder does not itself
# produce something fsck dislikes, so CORRUPT above is attributable to the plant.
R_TWIN=$(newrepo twin)
if run 0 "twin: VERIFIED" --repo "$R_TWIN"; then
  [ "$(verdict_of)" = VERIFIED ] &&
    ok "twin: an UNplanted repo from the same builder is VERIFIED — the plants are what CORRUPT reports" ||
    bad "twin: an unplanted repo reported '$(verdict_of)'"
fi

# --- Case 6 (PLANTED MUTANT): the FULL REHASH is load-bearing --------------
# A copy of the script with `--connectivity-only` added to the fsck call. Measured on git
# 2.43.0: that flag walks reachability WITHOUT rehashing content, so it exits 0 on the
# Case 4 fixture. Two halves: the plant IS the defect described, and it gets Case 4 wrong.
#
# THE PLANT IS ON THE ARGV ARRAY, which is where the flags live since round 4 gave the
# third walk its own mode. The previous plant sed'ed the literal `fsck --no-progress
# --no-dangling`; when that text moved into `fargs=(...)` the sed matched NOTHING, the
# mutant became a byte-identical copy of the subject, and the construction assert caught
# it — which is what a construction assert is for.
MUT="$T/mutant-connectivity-only.sh"
sed 's/^  fargs=(--no-progress --no-dangling)$/  fargs=(--no-progress --no-dangling --connectivity-only)/' \
  "$SUBJECT" >"$MUT"
if bash -n "$MUT" 2>/dev/null &&
  grep -q -- 'fargs=(--no-progress --no-dangling --connectivity-only)' "$MUT" &&
  ! grep -q -- 'fargs=(--no-progress --no-dangling --connectivity-only)' "$SUBJECT" &&
  [ "$(grep -c -- '--connectivity-only' "$MUT")" -gt "$(grep -c -- '--connectivity-only' "$SUBJECT")" ]; then
  ok "connectivity-mutant: the plant IS the defect described (--connectivity-only on the fsck call, absent from the subject)"
else
  bad "connectivity-mutant: the plant is not the defect described"
fi
MUT_OUT=$(bash "$MUT" --repo "$R_MIS" 2>&1)
MUT_RC=$?
# DELIBERATELY NOT recorded into $ALL_OUT: it is the violation the suite exists to forbid.
if [ "$MUT_RC" -eq 0 ] && printf '%s\n' "$MUT_OUT" | grep -q '^OBJECT-STORE: verdict VERIFIED'; then
  ok "connectivity-mutant: WITHOUT the full rehash the SAME corrupt store reports VERIFIED — the flag would make this vacuous"
else
  bad "connectivity-mutant: expected a vacuous VERIFIED from the mutant (rc=$MUT_RC) — the case proves nothing otherwise"
fi

# --- Case 7: a linked WORKTREE reports the SHARED (common) store ------------
# `--git-common-dir`, not `--git-dir`: in a worktree the latter names the lane's private
# administrative directory, and sweeping that would audit the wrong thing while reporting
# a verdict about "the store".
R_WT_MAIN=$(newrepo wtmain)
# CANONICALISED, NOT THE LITERAL PATH: the subject reports `pwd -P`, and on macOS
# `${TMPDIR}` resolves through `/private`, so pinning `$R_WT_MAIN/.git/objects`
# false-REDS on correct input on a platform this script's own header claims to
# support (#3749 review NIT 9). A guard that reds on correct input is the guard
# agents learn to waive.
R_WT_MAIN_P=$(cd "$R_WT_MAIN" && pwd -P)
R_WT="$T/wt-linked"
if g "$R_WT_MAIN" worktree add -q --detach "$R_WT" >/dev/null 2>&1 && [ -d "$R_WT" ]; then
  WT_PRIVATE=$(git -C "$R_WT" rev-parse --absolute-git-dir 2>/dev/null)
  if run 0 "worktree: VERIFIED" --repo "$R_WT"; then
    if printf '%s\n' "$OUT" | grep -q "^OBJECT-STORE: store $R_WT_MAIN_P/\.git/objects$"; then
      ok "worktree: a linked worktree sweeps the SHARED common store ($R_WT_MAIN_P/.git/objects)"
    else
      bad "worktree: the swept store is not the shared one: $(printf '%s\n' "$OUT" | grep '^OBJECT-STORE: store ')"
    fi
    if [ -n "$WT_PRIVATE" ] && ! printf '%s\n' "$OUT" | grep -q "^OBJECT-STORE: store $WT_PRIVATE/objects$"; then
      ok "worktree: it does NOT sweep the worktree's private git dir ($WT_PRIVATE)"
    else
      bad "worktree: the private per-worktree dir was swept instead of the shared store"
    fi
  fi
else
  bad "worktree: could not create a linked worktree fixture (git worktree add failed)"
fi

# --- Case 8: NOT A GIT REPOSITORY is UNMEASURED, and never VERIFIED --------
mkdir -p "$T/plain-dir"
if run 5 "not-a-repo: UNMEASURED" --repo "$T/plain-dir"; then
  if [ "$(verdict_of)" = UNMEASURED ] &&
    ! printf '%s\n' "$OUT" | grep -q 'verdict VERIFIED'; then
    ok "not-a-repo: an unresolvable store is UNMEASURED (exit 5) and emits NO clean signal"
  else
    bad "not-a-repo: verdict was '$(verdict_of)'"
  fi
  if printf '%s\n' "$OUT" | grep -q '^OBJECT-STORE: unmeasured-cause '; then
    ok "not-a-repo: the run NAMES what could not be measured"
  else
    bad "not-a-repo: UNMEASURED with no cause line"
  fi
  if printf '%s\n' "$OUT" | grep -q 'MUST NOT READ THIS'; then
    ok "not-a-repo: the verdict detail states the consumer contract (unmeasured is not clean)"
  else
    bad "not-a-repo: the consumer contract is not stated in the output"
  fi
fi

# --- hermetic PATH dirs for the tool-absence cases -------------------------
# Symlinked coreutils only; each case adds exactly the tools it intends to be present, so
# the verdict cannot depend on what this host happens to have installed.
mk_bin() {
  local dir="$1" t p
  shift
  mkdir -p "$dir"
  for t in bash env printf mktemp rm cat sed awk grep tr sort head wc date nice chmod mkdir cmp "$@"; do
    p=$(type -P "$t" 2>/dev/null) || continue
    [ -n "$p" ] && ln -sf "$p" "$dir/$t" 2>/dev/null
  done
}

# --- Case 9: NO GIT is UNMEASURED ------------------------------------------
BIN_NOGIT="$T/bin-nogit"
mk_bin "$BIN_NOGIT" timeout
OUT=$(PATH="$BIN_NOGIT" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
RC=$?
record_out "no-git"
if [ "$RC" -eq 5 ] && [ "$(verdict_of)" = UNMEASURED ] &&
  printf '%s\n' "$OUT" | grep -q 'git is not on PATH'; then
  ok "no-git: without git the sweep is UNMEASURED (exit 5) naming the missing tool"
else
  bad "no-git: rc=$RC verdict='$(verdict_of)' (wanted 5/UNMEASURED)"
fi

# --- Case 10: NO TIMEOUT BINARY refuses to run UNBOUNDED -------------------
# The RED ARM is one property against Case 11's control: the same hermetic PATH, with and
# without a `timeout`.
BIN_NOTO="$T/bin-notimeout"
mk_bin "$BIN_NOTO" git
rm -f "$BIN_NOTO/timeout" "$BIN_NOTO/gtimeout"
if [ ! -e "$BIN_NOTO/timeout" ] && [ ! -e "$BIN_NOTO/gtimeout" ] && [ -e "$BIN_NOTO/git" ]; then
  ok "no-timeout-plant: the plant IS the property described (git present, neither timeout nor gtimeout)"
else
  bad "no-timeout-plant: the hermetic PATH is not the shape the case claims"
fi
OUT=$(PATH="$BIN_NOTO" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
RC=$?
record_out "no-timeout"
if [ "$RC" -eq 5 ] && [ "$(verdict_of)" = UNMEASURED ] &&
  printf '%s\n' "$OUT" | grep -q 'UNBOUNDED'; then
  ok "no-timeout: an unboundable host REFUSES to sweep and is UNMEASURED, never VERIFIED"
else
  bad "no-timeout: rc=$RC verdict='$(verdict_of)' (wanted 5/UNMEASURED)"
fi

# --- Case 11: the CONTROL for Case 10 --------------------------------------
BIN_TO="$T/bin-timeout"
mk_bin "$BIN_TO" git timeout gtimeout
OUT=$(PATH="$BIN_TO" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
RC=$?
record_out "hermetic-control"
if [ "$RC" -eq 0 ] && [ "$(verdict_of)" = VERIFIED ]; then
  ok "hermetic-control: the SAME hermetic PATH plus a timeout binary reaches VERIFIED — Case 10's refusal is the bound, not the sandbox"
else
  bad "hermetic-control: rc=$RC verdict='$(verdict_of)' — Case 10 proves nothing without this"
fi

# --- Case 12: an EXPIRED BOUND is UNMEASURED, never VERIFIED ---------------
# A git shim that SLEEPS on `fsck` and delegates everything else to the real git, so the
# only difference from the control below is the sleep. Two halves: the shim records its
# invocations (so the degrade is attributed to a call that really happened), and the same
# fixture with a NON-sleeping shim still reaches VERIFIED.
REAL_GIT=$(command -v git 2>/dev/null) || REAL_GIT=""
if [ -z "$REAL_GIT" ]; then
  bad "bound-expired: no git on PATH — the bound cannot be exercised"
else
  SHIM_SLOW="$T/shim-slow"
  SHIM_FAST="$T/shim-fast"
  SHIM_LOG="$T/shim-calls.txt"
  : >"$SHIM_LOG"
  mk_bin "$SHIM_SLOW" timeout gtimeout sleep
  mk_bin "$SHIM_FAST" timeout gtimeout sleep
  for _pair in "$SHIM_SLOW:yes" "$SHIM_FAST:no"; do
    _d=${_pair%:*}
    _slow=${_pair#*:}
    rm -f "$_d/git"
    {
      printf '#!/usr/bin/env bash\n'
      printf '# Test shim: log fsck calls; %s; delegate everything else to the real git.\n' \
        "$([ "$_slow" = yes ] && echo 'SLEEP past the bound on fsck' || echo 'run fsck normally')"
      printf 'for a in "$@"; do if [ "$a" = fsck ]; then printf %%s\\\\n "$*" >>"%s"; ' "$SHIM_LOG"
      if [ "$_slow" = yes ]; then printf 'sleep 30; exit 0; '; fi
      printf 'break; fi; done\n'
      printf 'exec %s "$@"\n' "$REAL_GIT"
    } >"$_d/git"
    chmod +x "$_d/git"
  done
  if grep -q 'sleep 30' "$SHIM_SLOW/git" && ! grep -q 'sleep 30' "$SHIM_FAST/git"; then
    ok "bound-plant: the two shims differ in EXACTLY one property (the sleep past the bound)"
  else
    bad "bound-plant: the shims are not the pair described"
  fi
  OUT=$(PATH="$SHIM_SLOW:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" --timeout 1 2>&1)
  RC=$?
  record_out "bound-expired"
  if [ "$RC" -eq 5 ] && [ "$(verdict_of)" = UNMEASURED ] &&
    printf '%s\n' "$OUT" | grep -q 'exceeded its 1s bound' &&
    [ -s "$SHIM_LOG" ]; then
    ok "bound-expired: a sweep killed at its bound is UNMEASURED (exit 5), and the fsck really was invoked"
  else
    bad "bound-expired: rc=$RC verdict='$(verdict_of)' shim-invoked=$([ -s "$SHIM_LOG" ] && echo yes || echo no)"
  fi
  OUT=$(PATH="$SHIM_FAST:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" --timeout 1 2>&1)
  RC=$?
  record_out "bound-control"
  if [ "$RC" -eq 0 ] && [ "$(verdict_of)" = VERIFIED ]; then
    ok "bound-control: the NON-sleeping shim at the SAME 1s bound reaches VERIFIED — the sleep is what expired it"
  else
    bad "bound-control: rc=$RC verdict='$(verdict_of)' — the expired-bound case proves nothing without this"
  fi
fi

# --- Case 13: usage errors are exit 2 and emit NO verdict ------------------
usage_case() {
  local desc="$1"
  shift
  OUT=$(bash "$SUBJECT" "$@" 2>&1)
  RC=$?
  record_out "usage $desc"
  if [ "$RC" -eq 2 ] && [ "$(verdict_lines)" -eq 0 ]; then
    ok "usage: $desc -> exit 2 with NO verdict line (exit 0 means VERIFIED here)"
  else
    bad "usage: $desc -> rc=$RC with $(verdict_lines) verdict line(s), wanted 2 and 0"
  fi
}
usage_case "an unrecognised flag" --bogus
usage_case "a bare positional" somewhere
usage_case "--timeout with a non-numeric value" --timeout abc
usage_case "--timeout 0 (which would kill every sweep instantly)" --timeout 0
usage_case "--repo with no value" --repo
usage_case "a repeated --repo" --repo "$R_CLEAN" --repo "$R_TWIN"
usage_case "--help (a run that measured nothing must not exit 0)" --help

# --- Case 14: the STATIC TEMPLATE TEXT, structurally ----------------------
# Only WHOLE-LINE comments are stripped: this file's `#` characters live inside printf
# formats (`(#3749)`) and parameter expansions (`${s//...}`), so a trailing-comment strip
# could TRUNCATE a template and HIDE a token. Keeping too much text can only produce a
# false FAIL, never a false PASS.
grep -v '^[[:space:]]*#' "$SUBJECT" >"$T/subject-code.txt"
code_lines=$(grep -c . "$T/subject-code.txt" | tr -d ' ')
all_lines=$(grep -c . "$SUBJECT" | tr -d ' ')
if [ "$code_lines" -lt "$all_lines" ] && [ "$code_lines" -gt 60 ] &&
  grep -q 'verdict VERIFIED' "$T/subject-code.txt" &&
  grep -q 'verdict CORRUPT' "$T/subject-code.txt" &&
  grep -q 'verdict UNMEASURED' "$T/subject-code.txt"; then
  ok "template: the comment-stripped source ($code_lines of $all_lines lines) still holds the output templates"
else
  bad "template: the comment strip left no usable template text ($code_lines of $all_lines) — the case would be vacuous"
fi
tmpl_bad=0
for tok in PASS 'RESULT:' OK; do
  if grep -q -- "$tok" "$T/subject-code.txt"; then
    bad "template: the script's own static text contains the FOREIGN verdict token '$tok': $(grep -m1 -- "$tok" "$T/subject-code.txt")"
    tmpl_bad=1
  fi
done
[ "$tmpl_bad" -eq 0 ] &&
  ok "template: the static text carries none of PASS, OK, RESULT: — its output can never be pasted as a gate/roborev block"
own_bad=$(grep -nE 'VERIFIED|CORRUPT|UNMEASURED' "$T/subject-code.txt" | grep -v 'verdict ' | head -1)
if [ -z "$own_bad" ]; then
  ok "template: its OWN verdict tokens appear only on 'verdict ' templates (structural)"
else
  bad "template: a verdict token appears off the verdict line: $own_bad"
fi

# --- Case 15: it MUTATES NOTHING ------------------------------------------
# A verifier with a side effect is a worse verifier, and this one is run from a hygiene
# path on a box other lanes share. Compared as a sorted listing of the whole repo
# (paths + sizes), which catches a new ref, a new pack and a rewritten object alike.
snap() { (cd "$1" && find . -type f -exec ls -ld {} + 2>/dev/null | awk '{print $5, $NF}' | sort); }
before=$(snap "$R_CLEAN")
run 0 "no-mutation sweep" --repo "$R_CLEAN" >/dev/null
after=$(snap "$R_CLEAN")
if [ "$before" = "$after" ] && [ -n "$before" ]; then
  ok "no-mutation: a full sweep leaves the repository byte-identical (no ref, no pack, no rewrite)"
else
  bad "no-mutation: the sweep changed the repository"
fi

# --- Case 16: REACHABILITY IS NOT CORRUPT (the #3749 review's BLOCKER B) ----
# THE DEFECT THIS CASE EXISTS FOR, MEASURED ON THE REAL FLEET STORE: `git fsck`
# prints `error: <ref>: invalid reflog entry <sha>` when a reflog names an object a
# peer lane's gc has pruned, and on a store eight lanes are concurrently writing that
# happened on roughly a quarter to a half of all runs — on a store nothing is wrong
# with. The first classifier recognised damage from `/^error/p`, so every one of
# those was a CORRUPT that pages high, stops the supervisor and fails `--strict`
# bootstrap. The class now comes from fsck's exit BITMASK (1/4 = object/pack damage,
# 2/8/16 = reachability/refs/commit-graph).
#
# IT IS NOT DEMOTED TO CLEAN EITHER, and that is the other half: a genuinely MISSING
# object reports the same ERROR_REACHABLE bit, so this lands on its own NON-PASSING
# state with its own cause.
R_REFLOG=$(newrepo reflog)
RL_BR=$(git -C "$R_REFLOG" symbolic-ref --short HEAD 2>/dev/null)
RL_LOG="$R_REFLOG/.git/logs/refs/heads/$RL_BR"
if [ -n "$RL_BR" ] && [ -f "$RL_LOG" ]; then
  printf '%s %s t <t@t> 1700000000 +0000\tbogus\n' \
    "$(git -C "$R_REFLOG" rev-parse HEAD)" \
    "1111111111111111111111111111111111111111" >>"$RL_LOG"
fi
RL_RC=0
git -C "$R_REFLOG" fsck --no-progress --no-dangling >/dev/null 2>"$T/reflog-fsck.err" || RL_RC=$?
if [ "$RL_RC" -eq 2 ] && grep -q 'invalid reflog entry' "$T/reflog-fsck.err" &&
  git -C "$R_REFLOG" cat-file -p "$(git -C "$R_REFLOG" rev-parse HEAD:f1)" >/dev/null 2>&1; then
  # ONE property: the fixture differs from its clean twin only by a reflog line, and
  # the assertion is on the BITMASK (2 = ERROR_REACHABLE, no 1/4) rather than on the
  # message — that is the signal the subject now classifies on.
  ok "reflog-plant: the plant IS the defect described (fsck exits 2 = ERROR_REACHABLE with an 'invalid reflog entry', objects readable)"
else
  bad "reflog-plant: fsck rc=$RL_RC on the reflog fixture (wanted 2) — the case below would prove nothing"
fi
if run 5 "reflog: UNMEASURED not CORRUPT" --repo "$R_REFLOG"; then
  if [ "$(verdict_of)" = UNMEASURED ]; then
    ok "reflog: a stale reflog entry on a busy shared store is UNMEASURED, NOT CORRUPT (it stops no supervisor)"
  else
    bad "reflog: verdict was '$(verdict_of)', wanted UNMEASURED — a healthy store must not read as corrupt"
  fi
  if printf '%s\n' "$OUT" | grep -q 'REFLOG-SCOPED' &&
    printf '%s\n' "$OUT" | grep -q 'reflog expire' &&
    printf '%s\n' "$OUT" | grep -q 'pass 3: fsck --no-reflogs'; then
    ok "reflog: the cause ATTRIBUTES the complaint (a third --no-reflogs walk cleared it => REFLOG-SCOPED) and gives that class's remedy, not the re-clone one"
  else
    bad "reflog: the UNMEASURED cause does not attribute the reachability complaint: $(printf '%s\n' "$OUT" | grep 'unmeasured-cause' | tail -2 | tr '\n' ' ')"
  fi
  if ! printf '%s\n' "$OUT" | grep -q '^OBJECT-STORE: object '; then
    ok "reflog: NO 'object' lines — the 40-hex tokens in a reflog diagnostic name INTACT objects, not damaged ones"
  else
    bad "reflog: intact object ids were reported as affected objects"
  fi
fi

# --- Case 17: THE DISCRIMINATOR — a non-clean walk must REPRODUCE ----------
# The store this sweep audits is mutated by up to 8 peer lanes WHILE fsck walks it,
# so a diagnostic can be a concurrency artefact. No fixture can hold a concurrent
# writer, so the discriminator is exercised with a git shim whose FIRST fsck reports
# and whose SECOND does not — the sequence a transient produces.
#
# THREE ARMS, EACH ONE PROPERTY APART: report-once vs report-always (the condition),
# and reachability vs damage (the exit bits).
mk_fsck_shim() {
  # mk_fsck_shim <dir> <always|once> <rc> <message> <log> [rc-when---no-reflogs]
  #
  # THE SIXTH ARGUMENT IS WHAT MAKES THE REACHABILITY-CAUSE ARMS POSSIBLE (#3749 review
  # round 4). The subject's third walk carries `--no-reflogs`, and the whole question it
  # asks is whether the complaint SURVIVES that flag — so a shim that answered the same
  # way to both configurations could not stage either outcome. It defaults to <rc>, which
  # is what every pre-round-4 arm wants (a shim that has no opinion about reflogs).
  local d="$1" when="$2" rc="$3" msg="$4" log="$5" nrl_rc="${6:-$3}"
  mk_bin "$d" timeout gtimeout
  rm -f "$d/git"
  {
    printf '#!/usr/bin/env bash\n'
    printf '# Test shim: on `fsck`, report %s and exit %s (%s with --no-reflogs); delegate everything else.\n' "$when" "$rc" "$nrl_rc"
    printf 'nrl=0\n'
    printf 'for a in "$@"; do [ "$a" = --no-reflogs ] && nrl=1; done\n'
    printf 'for a in "$@"; do\n'
    printf '  if [ "$a" = fsck ]; then\n'
    printf '    printf "call\\n" >>%s\n' "$(printf '%q' "$log")"
    printf '    n=$(grep -c . %s 2>/dev/null || printf 0)\n' "$(printf '%q' "$log")"
    printf '    if [ "$nrl" = 1 ]; then\n'
    printf '      if [ %s -ne 0 ]; then printf "%%s\\n" %s >&2; fi\n' "$(printf '%q' "$nrl_rc")" "$(printf '%q' "$msg")"
    printf '      exit %s\n' "$nrl_rc"
    printf '    fi\n'
    if [ "$when" = always ]; then
      printf '    if [ 1 -eq 1 ]; then\n'
    else
      printf '    if [ "$n" -le 1 ]; then\n'
    fi
    printf '      printf "%%s\\n" %s >&2\n' "$(printf '%q' "$msg")"
    printf '      exit %s\n' "$rc"
    printf '    fi\n'
    printf '    break\n'
    printf '  fi\n'
    printf 'done\n'
    printf 'exec %s "$@"\n' "$(printf '%q' "$REAL_GIT")"
  } >"$d/git"
  chmod +x "$d/git"
}
if [ -z "${REAL_GIT:-}" ]; then
  bad "discriminator: no real git on PATH — the shim arms cannot be built"
else
  RL_MSG='error: refs/heads/x: invalid reflog entry 1111111111111111111111111111111111111111'
  DMG_MSG='error: f761ec192d9f0dca3329044b96ebdb12839dbff6: hash-path mismatch, found at: /somewhere'
  # (a) CONSTRUCTION, asserted before the subject runs: the once-shim really does
  #     report on its first fsck and not on its second.
  SH_ONCE="$T/shim-once"
  LOG_ONCE="$T/shim-once-calls.txt"
  : >"$LOG_ONCE"
  mk_fsck_shim "$SH_ONCE" once 2 "$RL_MSG" "$LOG_ONCE"
  c1=0; PATH="$SH_ONCE:$PATH" "$SH_ONCE/git" -C "$R_CLEAN" fsck --no-progress >/dev/null 2>&1 || c1=$?
  c2=0; PATH="$SH_ONCE:$PATH" "$SH_ONCE/git" -C "$R_CLEAN" fsck --no-progress >/dev/null 2>&1 || c2=$?
  if [ "$c1" -eq 2 ] && [ "$c2" -eq 0 ] && [ "$(grep -c . "$LOG_ONCE" | tr -d ' ')" -eq 2 ]; then
    ok "discriminator-plant: the once-shim IS the sequence described (first fsck rc=2, second rc=0, both logged)"
  else
    bad "discriminator-plant: first=$c1 second=$c2 calls=$(grep -c . "$LOG_ONCE" | tr -d ' ') — the cases below would prove nothing"
  fi
  : >"$LOG_ONCE"
  OUT=$(PATH="$SH_ONCE:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
  RC=$?
  record_out "discriminator-transient"
  if [ "$RC" -eq 0 ] && [ "$(verdict_of)" = VERIFIED ] &&
    [ "$(grep -c . "$LOG_ONCE" | tr -d ' ')" -eq 2 ] &&
    printf '%s\n' "$OUT" | grep -q 'did NOT reproduce'; then
    ok "discriminator: a diagnostic that does not survive a SECOND walk is VERIFIED — and the run says so, having really walked twice"
  else
    bad "discriminator(transient): rc=$RC verdict='$(verdict_of)' walks=$(grep -c . "$LOG_ONCE" | tr -d ' ') (wanted 0/VERIFIED/2)"
  fi
  # (b) ONE PROPERTY APART: the same message on EVERY walk. It reproduces, so it is
  #     non-passing — and still not CORRUPT, because with the reflogs EXCLUDED it clears
  #     (the shim's 6th argument), which is what makes it reflog-scoped rather than
  #     damage. THREE walks now: sweep, reproduction, attribution.
  SH_ALWAYS="$T/shim-always"
  LOG_ALWAYS="$T/shim-always-calls.txt"
  : >"$LOG_ALWAYS"
  mk_fsck_shim "$SH_ALWAYS" always 2 "$RL_MSG" "$LOG_ALWAYS" 0
  OUT=$(PATH="$SH_ALWAYS:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
  RC=$?
  record_out "discriminator-persistent"
  if [ "$RC" -eq 5 ] && [ "$(verdict_of)" = UNMEASURED ] &&
    [ "$(grep -c . "$LOG_ALWAYS" | tr -d ' ')" -eq 3 ] &&
    printf '%s\n' "$OUT" | grep -q 'REFLOG-SCOPED'; then
    ok "discriminator: the SAME reachability diagnostic on BOTH walks does not reach VERIFIED — and having cleared with --no-reflogs it is attributed REFLOG-SCOPED, not damage (3 walks)"
  else
    bad "discriminator(persistent): rc=$RC verdict='$(verdict_of)' walks=$(grep -c . "$LOG_ALWAYS" | tr -d ' ') (wanted 5/UNMEASURED/3 naming REFLOG-SCOPED)"
  fi
  # (b2) THE ITEM-1 DEFECT, STAGED: one property from (b) — the complaint SURVIVES the
  #      reflog-excluded walk. Before round 4 this reached UNMEASURED, which the
  #      supervisor deliberately continues past, so workers kept running against a store
  #      with an object missing from under a live ref.
  SH_LIVE="$T/shim-live-reachable"
  LOG_LIVE="$T/shim-live-reachable-calls.txt"
  : >"$LOG_LIVE"
  mk_fsck_shim "$SH_LIVE" always 2 'missing blob 1111111111111111111111111111111111111111' "$LOG_LIVE" 2
  OUT=$(PATH="$SH_LIVE:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
  RC=$?
  record_out "discriminator-live-reachable"
  if [ "$RC" -eq 4 ] && [ "$(verdict_of)" = CORRUPT ] &&
    [ "$(grep -c . "$LOG_LIVE" | tr -d ' ')" -eq 3 ]; then
    ok "reachability-attribution: a reachability complaint that SURVIVES --no-reflogs is CORRUPT (exit 4) — the false negative round 4 fixed, staged one property from (b)"
  else
    bad "reachability-attribution(live): rc=$RC verdict='$(verdict_of)' walks=$(grep -c . "$LOG_LIVE" | tr -d ' ') (wanted 4/CORRUPT/3)"
  fi
  if printf '%s\n' "$OUT" | grep -q 'is the remedy for the OTHER cause' &&
    ! printf '%s\n' "$OUT" | grep -q '^OBJECT-STORE: object '; then
    ok "reachability-attribution: the CORRUPT remedy says the reflog remedy does NOT apply, and no 40-hex token is labelled an affected 'object' (a broken-link diagnostic names the intact source too)"
  else
    bad "reachability-attribution: the CORRUPT branch's remedy/labelling is wrong: $(printf '%s\n' "$OUT" | grep 'verdict-detail' | head -3 | tr '\n' ' ')"
  fi
  # (b3) THE ATTRIBUTION ITSELF FAILING is not a licence to pick either verdict: a third
  #      walk that returns a status this script cannot read leaves the reproduced
  #      complaint UNATTRIBUTED, which is neither CORRUPT nor clean. One property from
  #      (b2): the third walk's status.
  SH_UNATTR="$T/shim-unattributable"
  LOG_UNATTR="$T/shim-unattributable-calls.txt"
  : >"$LOG_UNATTR"
  mk_fsck_shim "$SH_UNATTR" always 2 "$RL_MSG" "$LOG_UNATTR" 128
  OUT=$(PATH="$SH_UNATTR:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
  RC=$?
  record_out "discriminator-unattributable"
  if [ "$RC" -eq 5 ] && [ "$(verdict_of)" = UNMEASURED ] &&
    printf '%s\n' "$OUT" | grep -q 'could NOT be attributed'; then
    ok "reachability-attribution: a third walk that produces no usable answer leaves the complaint UNATTRIBUTED and non-passing — never CORRUPT on an unreadable status, never clean"
  else
    bad "reachability-attribution(unattributable): rc=$RC verdict='$(verdict_of)' (wanted 5/UNMEASURED naming the failed attribution)"
  fi
  # (b4) AND A DAMAGE BIT SEEN ONLY IN THE THIRD WALK has not reproduced across the two
  #      sweep walks, so it is UNMEASURED too — the third walk can strengthen the
  #      reachability verdict, it cannot introduce a damage verdict of its own.
  SH_L8="$T/shim-late-damage"
  LOG_L8="$T/shim-late-damage-calls.txt"
  : >"$LOG_L8"
  mk_fsck_shim "$SH_L8" always 2 "$RL_MSG" "$LOG_L8" 1
  OUT=$(PATH="$SH_L8:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
  RC=$?
  record_out "discriminator-late-damage"
  if [ "$RC" -eq 5 ] && [ "$(verdict_of)" = UNMEASURED ] &&
    printf '%s\n' "$OUT" | grep -q 'appeared ONLY in the'; then
    ok "reachability-attribution: a damage bit appearing ONLY in the third walk is UNMEASURED — the attribution walk cannot manufacture a damage verdict the sweep walks never saw"
  else
    bad "reachability-attribution(late-damage): rc=$RC verdict='$(verdict_of)' (wanted 5/UNMEASURED naming the third-walk-only damage)"
  fi
  # (c) A DAMAGE class (fsck exit bit 1) seen ONCE and not the second time is
  #     UNMEASURED: neither established damage nor a clean store. One property apart
  #     from (a) — the exit bits.
  SH_FLICK="$T/shim-flicker"
  LOG_FLICK="$T/shim-flicker-calls.txt"
  : >"$LOG_FLICK"
  mk_fsck_shim "$SH_FLICK" once 3 "$DMG_MSG" "$LOG_FLICK"
  OUT=$(PATH="$SH_FLICK:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
  RC=$?
  record_out "discriminator-flicker"
  if [ "$RC" -eq 5 ] && [ "$(verdict_of)" = UNMEASURED ] &&
    printf '%s\n' "$OUT" | grep -q 'did not reproduce'; then
    ok "discriminator: a DAMAGE class seen once and not twice is UNMEASURED — a flickering corruption signal is certified as neither"
  else
    bad "discriminator(flicker): rc=$RC verdict='$(verdict_of)' (wanted 5/UNMEASURED)"
  fi
  # (d) AND THE FATAL BRANCH STILL FIRES when it reproduces: same shim, always-arm,
  #     damage bits. Without this the three arms above could all be a subject that
  #     simply never reports CORRUPT.
  SH_DMG="$T/shim-damage"
  LOG_DMG="$T/shim-damage-calls.txt"
  : >"$LOG_DMG"
  mk_fsck_shim "$SH_DMG" always 3 "$DMG_MSG" "$LOG_DMG"
  OUT=$(PATH="$SH_DMG:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
  RC=$?
  record_out "discriminator-reproduced-damage"
  if [ "$RC" -eq 4 ] && [ "$(verdict_of)" = CORRUPT ] &&
    [ "$(grep -c . "$LOG_DMG" | tr -d ' ')" -eq 2 ]; then
    ok "discriminator: a damage class on BOTH walks IS CORRUPT (exit 4) — the discriminator did not defang the fatal branch"
  else
    bad "discriminator(reproduced): rc=$RC verdict='$(verdict_of)' walks=$(grep -c . "$LOG_DMG" | tr -d ' ') (wanted 4/CORRUPT/2)"
  fi
fi

# --- Case 18: THE INHERITED GIT ENVIRONMENT CANNOT BEND THE VERDICT --------
# Reproduced against the first version of this script: `GIT_OBJECT_DIRECTORY=<good>`
# with `--repo <bad>` printed `store <bad>/.git/objects` and `verdict VERIFIED`,
# exit 0 — every emitted line affirmatively false, with no signal to either consumer.
# The script pinned two ambient variables and inherited the rest of the family.
#
# THE CONSTRUCTION IS ASSERTED FIRST, and it is what makes this case non-vacuous: a
# PLAIN (non-isolated) git really is redirected by the variable, so a green below is
# the isolation and not an inert variable.
# THE PRECONDITION OF THE CONTROL BELOW, MEASURED RATHER THAN ASSUMED. The redirect can
# only make <mismatch>'s refs resolve inside <clean>'s object store while the two
# fixtures share object ids; if they do not, fsck raises ERROR_REACHABLE and the control
# fails for a reason that has nothing to do with the injection. That identity is what
# FIXTURE_DATE buys (see the builder's header) — unpinned it held only when both builds
# happened to land in the same wall-clock second. Asserted HERE, at the one case that
# depends on it, so removing the pin reds with the cause named instead of intermittently.
CLEAN_HEAD=$(git -C "$R_CLEAN" rev-parse HEAD 2>/dev/null)
MIS_HEAD=$(git -C "$R_MIS" rev-parse HEAD 2>/dev/null)
if [ -n "$CLEAN_HEAD" ] && [ "$CLEAN_HEAD" = "$MIS_HEAD" ]; then
  ok "env-plant: the clean and mismatch fixtures share a HEAD sha — the builder's clock is pinned, so the control below cannot turn on when the suite ran"
else
  bad "env-plant: clean HEAD '$CLEAN_HEAD' != mismatch HEAD '$MIS_HEAD' — the fixture clock is NOT pinned and the control below is a wall-clock race"
fi
plain_rc=0
GIT_OBJECT_DIRECTORY="$R_CLEAN/.git/objects" \
  git --git-dir="$R_MIS/.git" fsck --no-progress --no-dangling >/dev/null 2>&1 || plain_rc=$?
if [ "$plain_rc" -eq 0 ]; then
  ok "env-plant: the injection IS effective against a non-isolated git (GIT_OBJECT_DIRECTORY makes the CORRUPT store fsck clean)"
else
  bad "env-plant: a plain git was not redirected by GIT_OBJECT_DIRECTORY (rc=$plain_rc) — the cases below would prove nothing"
fi
OUT=$(GIT_OBJECT_DIRECTORY="$R_CLEAN/.git/objects" bash "$SUBJECT" --repo "$R_MIS" 2>&1)
RC=$?
record_out "env-object-directory"
if [ "$RC" -eq 4 ] && [ "$(verdict_of)" = CORRUPT ] &&
  printf '%s\n' "$OUT" | grep -q "^OBJECT-STORE: store $R_MIS/\.git/objects$"; then
  ok "env: an inherited GIT_OBJECT_DIRECTORY cannot make the sweep read a store OTHER than the one it names"
else
  bad "env(GIT_OBJECT_DIRECTORY): rc=$RC verdict='$(verdict_of)' — a false verdict about the named store"
fi
OUT=$(GIT_DIR="$R_CLEAN/.git" bash "$SUBJECT" --repo "$R_MIS" 2>&1)
RC=$?
record_out "env-git-dir"
if [ "$RC" -eq 4 ] && [ "$(verdict_of)" = CORRUPT ] &&
  printf '%s\n' "$OUT" | grep -q "^OBJECT-STORE: store $R_MIS/\.git/objects$"; then
  ok "env: an inherited GIT_DIR does not repoint the sweep (the subject is --repo, resolved under isolation)"
else
  bad "env(GIT_DIR): rc=$RC verdict='$(verdict_of)' store=$(printf '%s\n' "$OUT" | grep '^OBJECT-STORE: store ')"
fi
OUT=$(GIT_ALTERNATE_OBJECT_DIRECTORIES="$R_MIS/.git/objects" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
RC=$?
record_out "env-alternates"
if [ "$RC" -eq 0 ] && [ "$(verdict_of)" = VERIFIED ]; then
  ok "env: an inherited GIT_ALTERNATE_OBJECT_DIRECTORIES cannot import a CORRUPT store into a healthy one's verdict"
else
  bad "env(alternates): rc=$RC verdict='$(verdict_of)' — a false verdict about the named store"
fi

# --- Case 19: A MULTI-LINE fsck DIAGNOSTIC SURVIVES WHOLE, AND sane() RUNS -
# git PERMITS NEWLINES IN PATHS and fsck quotes the path verbatim, so a diagnostic
# can be two physical lines. The first version split findings with `sed`, so the
# CONTINUATION — which carries the rest of the path the operator has to act on —
# matched no pattern and was DROPPED SILENTLY: the anchor invariant held, but by
# truncation, and the header's "fields are otherwise kept VERBATIM" was false.
#
# This is also the ONLY case that puts a control character into a field, so it is
# what exercises sane()'s escape loop at all.
NL=$'\n'
R_NLDIR="$T/nl${NL}dir"
if mkdir -p "$R_NLDIR" 2>/dev/null && [ -d "$R_NLDIR" ]; then
  R_NL="$R_NLDIR/repo"
  mkdir -p "$R_NL"
  git init -q "$R_NL" >/dev/null 2>&1
  g "$R_NL" config user.email t@t
  g "$R_NL" config user.name t
  printf 'content aaa\n' >"$R_NL/f1"
  printf 'content bbb\n' >"$R_NL/f2"
  g "$R_NL" add f1 f2 >/dev/null 2>&1
  g "$R_NL" -c user.email=t@t -c user.name=t commit -q -m c1 >/dev/null 2>&1
  NL_A=$(git -C "$R_NL" rev-parse HEAD:f1 2>/dev/null)
  NL_B=$(git -C "$R_NL" rev-parse HEAD:f2 2>/dev/null)
  if [ -n "$NL_A" ] && [ -n "$NL_B" ] && [ "$NL_A" != "$NL_B" ]; then
    chmod 644 "$(loose_path "$R_NL" "$NL_A")" 2>/dev/null
    cp "$(loose_path "$R_NL" "$NL_B")" "$(loose_path "$R_NL" "$NL_A")"
  fi
  # ABSOLUTE --git-dir, deliberately: with `-C <repo>` git prints the object path
  # RELATIVE to the repo, which does not contain the newline-bearing directory at all
  # — so the construction would be asserted against a diagnostic of a different shape
  # than the one the subject (which passes an absolute --git-dir) actually receives.
  nl_lines=$(git --git-dir="$R_NL/.git" fsck --no-progress --no-dangling 2>&1 | grep -c . | tr -d ' ')
  if [ "$(git -C "$R_NL" cat-file -p "$NL_A" 2>/dev/null)" = "content bbb" ] && [ "$nl_lines" -ge 3 ]; then
    ok "newline-plant: the plant IS the shape described (a hash-path mismatch whose quoted path contains a NEWLINE, so fsck emits a multi-line diagnostic)"
  else
    bad "newline-plant: content='$(git -C "$R_NL" cat-file -p "$NL_A" 2>/dev/null | head -1)' fsck-lines=$nl_lines — the case below would prove nothing"
  fi
  OUT=$(bash "$SUBJECT" --repo "$R_NL" 2>&1)
  RC=$?
  record_out "newline-path"
  if [ "$RC" -eq 4 ] && [ "$(verdict_of)" = CORRUPT ]; then
    ok "newline-path: a store under a newline-bearing path is still classified (exit 4)"
  else
    bad "newline-path: rc=$RC verdict='$(verdict_of)' (wanted 4/CORRUPT)"
  fi
  # THE TRUNCATION HALF: the continuation must be PRESENT, on the SAME anchored line,
  # with the newline rendered as a visible escape. `nl${NL}dir` is the containing
  # directory, so `dir/repo/.git/objects` is exactly the text `sed` used to drop.
  if printf '%s\n' "$OUT" | grep -q 'finding .*hash-path mismatch' &&
    printf '%s\n' "$OUT" | grep -q 'nl\\ndir/repo/\.git/objects'; then
    ok "newline-path: the CONTINUATION of a multi-line diagnostic survives on the same anchored line, newline escaped as \\n (sane()'s escape loop, unexercised before)"
  else
    bad "newline-path: the diagnostic was truncated — the operator is handed a path that does not exist: $(printf '%s\n' "$OUT" | grep 'finding ' | head -1)"
  fi
else
  bad "newline-path: could not create a newline-bearing directory (the case cannot run on this filesystem)"
fi

# --- Case 20: NO `env` REFUSES rather than measuring un-isolated ------------
# `env -i` is how every git call here gets its allowlisted environment (Case 18), so a
# host without `env` cannot isolate — and the alternative to refusing is running fsck
# under the caller's environment, which is exactly the false-VERIFIED Case 18 covers.
# ONE property against Case 11's control: the same hermetic PATH, minus `env`.
BIN_NOENV="$T/bin-noenv"
mk_bin "$BIN_NOENV" git timeout gtimeout
rm -f "$BIN_NOENV/env"
if [ ! -e "$BIN_NOENV/env" ] && [ -e "$BIN_NOENV/git" ] && [ -e "$BIN_NOENV/timeout" ]; then
  ok "no-env-plant: the plant IS the property described (git and timeout present, env absent)"
else
  bad "no-env-plant: the hermetic PATH is not the shape the case claims"
fi
OUT=$(PATH="$BIN_NOENV" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
RC=$?
record_out "no-env"
if [ "$RC" -eq 5 ] && [ "$(verdict_of)" = UNMEASURED ] &&
  printf '%s\n' "$OUT" | grep -q 'cannot ISOLATE'; then
  ok "no-env: a host that cannot isolate git's environment is UNMEASURED, never a measurement taken un-isolated"
else
  bad "no-env: rc=$RC verdict='$(verdict_of)' (wanted 5/UNMEASURED)"
fi

# --- Case 21: THE BITMASK DOES NOT END AT 31 (real fixtures) ----------------
# THE DEFECT THIS CASE EXISTS FOR (#3749 review round 2, BLOCKER 3). The first bitmask
# classifier only bit-tested statuses in 1..31, on the reasoning that 128 is git's
# `die()` and `127 & 1` would read as object damage. But git's own mask is wider: 2.43
# defines 32 ERROR_MULTI_PACK_INDEX. So a store with BOTH a multi-pack-index complaint
# and real object damage exits 33/35/36, fell outside the range, was called
# `unclassified` and became UNMEASURED — a FALSE NEGATIVE on genuine object corruption,
# which is the one direction this control exists to prevent.
#
# TWO REAL FIXTURES, ONE PROPERTY APART: both carry a truncated `multi-pack-index`; only
# the second also carries a corrupted loose object. The exit statuses are MEASURED with
# git before the subject runs, and the assertion is on the BITS rather than on a literal
# number, so a git that numbers its bits differently fails the construction assert
# (attributable) instead of silently passing the case (not).
mk_midx_repo() {
  local r="$T/$1"
  mkdir -p "$r"
  git init -q "$r" >/dev/null 2>&1
  g "$r" config user.email t@t
  g "$r" config user.name t
  printf 'content aaa\n' >"$r/f1"
  g "$r" add f1 >/dev/null
  g "$r" -c user.email=t@t -c user.name=t commit -q -m c1 >/dev/null
  g "$r" repack -q -ad >/dev/null 2>&1
  g "$r" multi-pack-index write >/dev/null 2>&1
  # A second commit AFTER the repack, so the store also holds loose objects to damage.
  printf 'content bbb\n' >"$r/f2"
  g "$r" add f2 >/dev/null
  g "$r" -c user.email=t@t -c user.name=t commit -q -m c2 >/dev/null
  # The plant: a multi-pack-index too short to parse.
  printf 'MIDX\001' >"$r/.git/objects/pack/multi-pack-index"
  printf '%s' "$r"
}
fsck_status() {
  local rc=0
  git -C "$1" fsck --no-progress --no-dangling >/dev/null 2>&1 || rc=$?
  printf '%s' "$rc"
}
R_MIDX=$(mk_midx_repo midx)
R_MIDX_DMG=$(mk_midx_repo midx-damaged)
# The BLOB of the post-repack commit, named explicitly rather than "whatever `find`
# returns first": corrupting the COMMIT object instead makes git `die()` (exit 128, a
# NON-bitmask status), which is Case 23's subject and not this one. Measured both ways on
# git 2.43.0 — blob 35, commit 128 — so the choice is load-bearing, not incidental.
MIDX_BLOB=$(g "$R_MIDX_DMG" rev-parse HEAD:f2 2>/dev/null)
MIDX_LOOSE=""
if [ -n "$MIDX_BLOB" ]; then
  MIDX_LOOSE=$(loose_path "$R_MIDX_DMG" "$MIDX_BLOB")
  chmod u+w "$MIDX_LOOSE" 2>/dev/null
  printf 'garbagegarbagegarbage' >"$MIDX_LOOSE"
fi
S_MIDX=$(fsck_status "$R_MIDX")
S_MIDX_DMG=$(fsck_status "$R_MIDX_DMG")
if [ "$((S_MIDX & 32))" -ne 0 ] && [ "$((S_MIDX & 5))" -eq 0 ] &&
  [ "$((S_MIDX_DMG & 32))" -ne 0 ] && [ "$((S_MIDX_DMG & 1))" -ne 0 ]; then
  ok "midx-plant: the fixtures ARE what the case claims (git 2.43 exits $S_MIDX for the midx alone = bit 32 and no damage bit, $S_MIDX_DMG with a corrupted loose object = 32 PLUS bit 1) — the status the old 1..31 range check dropped"
else
  bad "midx-plant: fsck exited $S_MIDX (midx only) and $S_MIDX_DMG (midx+damage) — not the 32/32|1 shapes the case is about; the cases below would prove nothing"
fi
if run 4 "midx+damage: CORRUPT, not UNMEASURED" --repo "$R_MIDX_DMG"; then
  if [ "$(verdict_of)" = CORRUPT ]; then
    ok "midx+damage: object damage ALONGSIDE a multi-pack-index complaint (exit $S_MIDX_DMG) is CORRUPT — an unrelated high bit cannot mask the damage bits"
  else
    bad "midx+damage: verdict='$(verdict_of)', wanted CORRUPT — real object corruption was dropped because of an unrelated bit"
  fi
fi
# ONE PROPERTY APART: the same fixture WITHOUT the damaged object. Non-passing, and NOT
# the fatal branch — so the CORRUPT above is attributable to the damage bit and not to
# the multi-pack-index complaint the two fixtures share.
if run 5 "midx alone: UNMEASURED, not CORRUPT" --repo "$R_MIDX"; then
  if [ "$(verdict_of)" = UNMEASURED ] &&
    printf '%s\n' "$OUT" | grep -q 'multi-pack-index'; then
    ok "midx alone: a multi-pack-index complaint with NO damage bit is UNMEASURED and the cause names the class — never CORRUPT, and never clean"
  else
    bad "midx alone: verdict='$(verdict_of)' — wanted UNMEASURED naming the multi-pack-index class"
  fi
fi

# --- Case 22: THE TWO STATUSES THE FINDING NAMES, EXACTLY -------------------
# 33 (32|1) and 36 (32|4) are the statuses the review named. They are staged with the
# shim rather than with a fixture, because a real store cannot be made to exit a chosen
# status on demand — and 36 in particular needs pack damage that does not also set other
# bits. Each arm is ONE PROPERTY from the reproduced-damage arm of Case 17 (rc=3): the
# added bit 32.
if [ -n "${REAL_GIT:-}" ]; then
  DMG_MSG2='error: f761ec192d9f0dca3329044b96ebdb12839dbff6: object corrupt or missing: /somewhere'
  for _bm in 33 36; do
    SH_BM="$T/shim-bit$_bm"
    LOG_BM="$T/shim-bit$_bm-calls.txt"
    : >"$LOG_BM"
    mk_fsck_shim "$SH_BM" always "$_bm" "$DMG_MSG2" "$LOG_BM"
    bm_c=0
    PATH="$SH_BM:$PATH" "$SH_BM/git" -C "$R_CLEAN" fsck --no-progress >/dev/null 2>&1 || bm_c=$?
    if [ "$bm_c" -eq "$_bm" ]; then
      ok "bitmask-plant($_bm): the shim really exits $_bm, so the case below measures the classifier and not the shim"
    else
      bad "bitmask-plant($_bm): the shim exited $bm_c — the case below would prove nothing"
    fi
    : >"$LOG_BM"
    OUT=$(PATH="$SH_BM:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
    RC=$?
    record_out "bitmask-$_bm"
    if [ "$RC" -eq 4 ] && [ "$(verdict_of)" = CORRUPT ]; then
      ok "bitmask($_bm): a damage bit travelling with ERROR_MULTI_PACK_INDEX is CORRUPT — the status the 1..31 range check turned into a false UNMEASURED"
    else
      bad "bitmask($_bm): rc=$RC verdict='$(verdict_of)' (wanted 4/CORRUPT)"
    fi
  done

  # --- Case 23: AND THE NON-BITMASK PATH STILL REFUSES ---------------------
  # The control for Case 22, and the half of the round-1 reasoning that was RIGHT: 128 is
  # git's `die()` and 127 a missing binary, and `127 & 1` is 1. Neither may be bit-tested.
  # ONE property from the arms above: the status, which is at or above the floor where
  # shell and `die()` conventions live.
  for _nb in 127 128; do
    SH_NB="$T/shim-nonmask$_nb"
    LOG_NB="$T/shim-nonmask$_nb-calls.txt"
    : >"$LOG_NB"
    mk_fsck_shim "$SH_NB" always "$_nb" "$DMG_MSG2" "$LOG_NB"
    OUT=$(PATH="$SH_NB:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
    RC=$?
    record_out "nonmask-$_nb"
    if [ "$RC" -eq 5 ] && [ "$(verdict_of)" = UNMEASURED ] &&
      printf '%s\n' "$OUT" | grep -q 'cannot read as its error'; then
      ok "non-bitmask($_nb): a status outside the mask is UNMEASURED with its OWN cause — never bit-tested into CORRUPT, and never described as a reachability problem it never reported"
    else
      bad "non-bitmask($_nb): rc=$RC verdict='$(verdict_of)' (wanted 5/UNMEASURED naming the unreadable status)"
    fi
  done
fi

# --- Case 24: `--print-store` IS THE ONE ISOLATED RESOLVER ------------------
# WHY IT EXISTS (#3749 review round 2, BLOCKER 2). A caller that throttles or latches on
# the shared store has to NAME it, and naming it means resolving `--git-common-dir`.
# `scripts/local/worker-supervisor.sh` was doing that with a BARE `git`, inheriting the
# caller's environment, while this script had just moved every one of its own git calls
# under `env -i` + one allowlist — so an inherited `GIT_DIR` keyed the supervisor's stamp
# on ANOTHER repository. This mode is the shared resolver that removes the second
# implementation, so it must be isolated exactly like the sweep is.
if run 0 "print-store: resolves and exits without sweeping" --repo "$R_CLEAN" --print-store; then
  if [ "$(printf '%s\n' "$OUT" | grep -c '^OBJECT-STORE: store ')" -eq 1 ] &&
    printf '%s\n' "$OUT" | grep -q "^OBJECT-STORE: store $R_CLEAN/\.git/objects$" &&
    ! printf '%s\n' "$OUT" | grep -q '^OBJECT-STORE: verdict '; then
    ok "print-store: prints exactly one anchored 'store' line naming the resolved store, and NO verdict — it measures nothing and must not look like it did"
  else
    bad "print-store: output was [$(printf '%s\n' "$OUT" | tr '\n' '|')]"
  fi
  # AND THE KEY LINE, which is what a caller actually keys its throttle and CORRUPT latch
  # on (#3749 review round 5, item 3). Exactly one, and of the shape the caller validates:
  # a readable tail plus 16 hex of sha256 over the RAW path. Case 29 below is the property;
  # this is the CONTRACT — a caller that stopped receiving it would silently lose its
  # box-wide key.
  if [ "$(printf '%s\n' "$OUT" | grep -c '^OBJECT-STORE: store-key ')" -eq 1 ] &&
    printf '%s\n' "$OUT" | grep -qE '^OBJECT-STORE: store-key [A-Za-z0-9._-]{1,64}\.[0-9a-f]{16}$'; then
    ok "print-store: prints exactly one anchored 'store-key' line of the shape the caller validates (readable tail + 16 hex digest)"
  else
    bad "print-store: no usable store-key line in [$(printf '%s\n' "$OUT" | tr '\n' '|')]"
  fi
fi
# THE ISOLATION, with its construction asserted first: a PLAIN git IS redirected by
# GIT_COMMON_DIR, so a green here is the allowlist and not an inert variable. This is the
# exact injection that mis-keyed the supervisor's stamp.
plain_common=$(GIT_COMMON_DIR="$R_MIS/.git" git -C "$R_CLEAN" rev-parse --git-common-dir 2>/dev/null || true)
if [ "$plain_common" = "$R_MIS/.git" ]; then
  ok "print-store-plant: the injection IS effective against a non-isolated git (GIT_COMMON_DIR repoints rev-parse at another repository)"
else
  bad "print-store-plant: a plain git was not repointed by GIT_COMMON_DIR (got '$plain_common') — the case below would prove nothing"
fi
OUT=$(GIT_COMMON_DIR="$R_MIS/.git" bash "$SUBJECT" --repo "$R_CLEAN" --print-store 2>&1)
RC=$?
record_out "print-store-env"
if [ "$RC" -eq 0 ] &&
  printf '%s\n' "$OUT" | grep -q "^OBJECT-STORE: store $R_CLEAN/\.git/objects$"; then
  ok "print-store: an inherited GIT_COMMON_DIR cannot make the resolver name a DIFFERENT store — a caller keying a throttle on it cannot be pointed at another repository"
else
  bad "print-store: rc=$RC out=[$(printf '%s\n' "$OUT" | tr '\n' '|')] — the resolver was repointed by the caller's environment"
fi
# A HOST WITHOUT `timeout` can still be ASKED THE QUESTION: this mode runs one rev-parse,
# not an fsck, so refusing for want of a bound would leave a caller keying its throttle on
# nothing exactly where the sweep is already UNMEASURED. ONE property from Case 11.
BIN_NOTO="$T/bin-notimeout-print"
mk_bin "$BIN_NOTO" git
rm -f "$BIN_NOTO/timeout" "$BIN_NOTO/gtimeout"
OUT=$(PATH="$BIN_NOTO" bash "$SUBJECT" --repo "$R_CLEAN" --print-store 2>&1)
RC=$?
record_out "print-store-no-timeout"
if [ "$RC" -eq 0 ] && printf '%s\n' "$OUT" | grep -q '^OBJECT-STORE: store '; then
  ok "print-store: a host with no timeout binary can still NAME the store (the bound is the sweep's requirement, not the resolver's)"
else
  bad "print-store(no-timeout): rc=$RC out=[$(printf '%s\n' "$OUT" | tr '\n' '|')]"
fi

# --- Case 25: A FAILED LAUNCH/CAPTURE IS UNMEASURED, NEVER CORRUPT ----------
# THE DEFECT (#3749 review round 3, item 2). The two capture redirections are part of
# the fsck command, so if opening the scratch output file FAILS bash never execs
# anything and the status is 1 — which is also fsck's ERROR_OBJECT bit. Both passes
# then fail identically, both "reproduce", and the sweep emitted **CORRUPT** about a
# store it never opened. That is a false CORRUPT on a healthy box: the same class as
# round 1's BLOCKER B, arriving through the shell instead of through a concurrent
# writer, and it pages high + stops the supervisor + fails --strict bootstrap.
#
# THE LEVER IS A `mktemp` SHIM, and it is WHITE-BOX ON PURPOSE. The failure has to be
# staged at the exact place the subject writes, so the shim plants a DIRECTORY at the
# capture paths the subject uses for its two passes: `> <dir>` fails with EISDIR for
# EVERY uid, so this case does not depend on permission bits (a chmod-based plant is
# inert as root, and this suite must not silently skip there). The coupling to the
# `p1.out`/`p2.out` names is deliberate and is asserted: if fsck_pass renames its
# capture files the CONSTRUCTION assert below reds, which is attributable, rather than
# the case passing against a plant that no longer plants anything.
if [ -z "${REAL_GIT:-}" ]; then
  bad "launch-capture: no real git on PATH — the shim arms cannot be built"
else
  REAL_MKTEMP=$(command -v mktemp 2>/dev/null) || REAL_MKTEMP=""
  # mk_mktemp_shim <dir> <entry...> — a `mktemp` that delegates, then creates <entry...>
  # inside the directory it just made. With no entries it is a pure pass-through, which
  # is the CONTROL arm.
  mk_mktemp_shim() {
    local d="$1"
    shift
    mkdir -p "$d"
    {
      printf '#!/usr/bin/env bash\n'
      printf '# Test shim: delegate to the real mktemp, then plant entries in the result.\n'
      printf 'out=$(%s "$@") || exit $?\n' "$(printf '%q' "$REAL_MKTEMP")"
      printf 'if [ -d "$out" ]; then\n'
      local e
      for e in "$@"; do
        printf '  mkdir -p "$out"/%s 2>/dev/null || true\n' "$(printf '%q' "$e")"
      done
      printf 'fi\n'
      printf 'printf "%%s\\n" "$out"\n'
    } >"$d/mktemp"
    chmod +x "$d/mktemp"
  }
  if [ -z "$REAL_MKTEMP" ]; then
    bad "launch-capture: no mktemp on PATH — the shim arms cannot be built"
  else
    # (a) CONSTRUCTION, asserted before the subject runs, in THREE parts. The shim must
    #     really plant the directory; a redirection into it must really FAIL; and that
    #     failure's status must really be 1 — the value that carries ERROR_OBJECT. If
    #     the third stopped being true the whole case would be about nothing.
    SH_CAP="$T/shim-capture"
    mk_mktemp_shim "$SH_CAP" p1.out p2.out
    PROBE_D=$(PATH="$SH_CAP:$PATH" mktemp -d "$T/capture-probe.XXXXXX" 2>/dev/null || true)
    probe_rc=0
    if [ -n "$PROBE_D" ] && [ -d "$PROBE_D" ]; then
      (true >"$PROBE_D/p1.out") 2>/dev/null || probe_rc=$?
    fi
    if [ -n "$PROBE_D" ] && [ -d "$PROBE_D/p1.out" ] && [ -d "$PROBE_D/p2.out" ] &&
      [ "$probe_rc" -eq 1 ]; then
      ok "launch-capture-plant: the shim IS the defect described (a DIRECTORY at both capture paths; redirecting into it fails with status 1 — fsck's ERROR_OBJECT bit)"
    else
      bad "launch-capture-plant: dir='$PROBE_D' p1=$([ -d "$PROBE_D/p1.out" ] && echo dir || echo no) redirect-rc=$probe_rc — the case below would prove nothing"
    fi
    # (b) THE SUBJECT: a CLEAN store, an unopenable capture path. UNMEASURED (exit 5),
    #     and the cause must say the fsck was never launched/captured rather than
    #     describing the store.
    OUT=$(PATH="$SH_CAP:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
    RC=$?
    record_out "launch-capture-failed"
    if [ "$RC" -eq 5 ] && [ "$(verdict_of)" = UNMEASURED ]; then
      ok "launch-capture: a failed capture redirection is UNMEASURED (exit 5) — the shell's status 1 is NOT read as fsck's ERROR_OBJECT bit"
    else
      bad "launch-capture: rc=$RC verdict='$(verdict_of)' (wanted 5/UNMEASURED) — a launch failure is being classified as object damage"
    fi
    if [ "$(verdict_of)" != CORRUPT ] && [ "$RC" -ne 4 ]; then
      ok "launch-capture: the healthy store is NOT reported CORRUPT — the false-CORRUPT class (round 1 BLOCKER B) does not return through the shell"
    else
      bad "launch-capture: a HEALTHY store was reported CORRUPT because the capture failed"
    fi
    if printf '%s\n' "$OUT" | grep -q '^OBJECT-STORE: unmeasured-cause .*could not be LAUNCHED'; then
      ok "launch-capture: the cause NAMES the launch/capture failure, so an operator is sent to the scratch filesystem and not to a re-clone"
    else
      bad "launch-capture: no launch/capture cause line — the operator gets UNMEASURED with the wrong or no explanation"
    fi
    if ! printf '%s\n' "$OUT" | grep -q '^OBJECT-STORE: object '; then
      ok "launch-capture: NO 'object' lines — nothing was rehashed, so naming object ids would be a fabricated finding"
    else
      bad "launch-capture: object ids reported for a walk that never ran"
    fi
    # (c) THE CONTROL, ONE PROPERTY APART: the same shim mechanism, planting a name the
    #     subject never opens. Without this, (b) could be passing because ANY mktemp
    #     shim breaks the run.
    SH_CAP_OK="$T/shim-capture-control"
    mk_mktemp_shim "$SH_CAP_OK" unused-by-the-subject
    OUT=$(PATH="$SH_CAP_OK:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
    RC=$?
    record_out "launch-capture-control"
    if [ "$RC" -eq 0 ] && [ "$(verdict_of)" = VERIFIED ]; then
      ok "launch-capture-control: the SAME shim planting a name the subject never opens is VERIFIED — the UNMEASURED above is the capture failure, not the shim"
    else
      bad "launch-capture-control: rc=$RC verdict='$(verdict_of)' (wanted 0/VERIFIED) — the shim itself is breaking the run"
    fi
    # (d) THE CONFIRMING PASS has the same rule. Plant ONLY the second pass's capture
    #     path and make the first walk non-clean (the reflog shim), so pass 1 runs and
    #     pass 2 cannot be launched: neither confirmed nor dismissed => UNMEASURED, and
    #     never the fatal branch on a status the shell produced.
    SH_CAP_P2="$T/shim-capture-p2"
    mk_mktemp_shim "$SH_CAP_P2" p2.out
    LOG_CAP="$T/shim-capture-calls.txt"
    : >"$LOG_CAP"
    mk_fsck_shim "$T/shim-capture-git" always 2 "$RL_MSG" "$LOG_CAP"
    # mk_bin populated that dir with SYMLINKS to the real tools, so the shim must
    # REPLACE the link rather than be copied through it (a `cp` onto a symlink writes
    # the TARGET — /usr/bin/mktemp — which fails, loudly and unanchored).
    rm -f "$T/shim-capture-git/mktemp"
    cp "$SH_CAP_P2/mktemp" "$T/shim-capture-git/mktemp"
    OUT=$(PATH="$T/shim-capture-git:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
    RC=$?
    record_out "launch-capture-pass2"
    if [ "$RC" -eq 5 ] && [ "$(verdict_of)" = UNMEASURED ] &&
      printf '%s\n' "$OUT" | grep -q 'confirming pass could not'; then
      ok "launch-capture: an unlaunchable CONFIRMING pass is UNMEASURED naming itself — the discriminator cannot be satisfied by a walk that never ran"
    else
      bad "launch-capture(pass2): rc=$RC verdict='$(verdict_of)' (wanted 5/UNMEASURED naming the confirming pass)"
    fi
  fi
fi

# --- Case 26: A SIGNALLED RUN OF **THIS SUITE** CANNOT EXIT GREEN -----------
# THE DEFECT (#3749 review round 3, item 4). The signal traps used to be
# `trap 'finish' INT TERM HUP`. `finish` takes its exit status from `$?`, and at trap
# time `$?` is the status of whatever was interrupted — routinely **0** — so a signal
# arriving mid-suite ran the cleanup, printed the tally of the cases that had run SO
# FAR, and **exited 0**, with every later case never executed. Combined with a case
# floor of 34 against 74 actual, half this suite could be silently skipped and the run
# still read as a pass. That is precisely the "green tally over a shrunken suite" class
# this file's own header claims to guard against.
#
# THE TRAP DECLARATIONS ARE EXTRACTED FROM THIS FILE AT RUN TIME, never restated: a
# case that re-typed them would go on passing after someone changed the real ones. The
# harness supplies only the `$?`-derived exit rule that `finish` itself applies, and
# busy-waits on a command whose status is 0 so the trap fires with `$? == 0` — the
# condition under which the defect is silent.
TRAP_SRC="$T/trap-lines.txt"
grep -n '^trap ' "${BASH_SOURCE[0]}" | sed 's/^[0-9]*://' >"$TRAP_SRC"
trap_n=$(grep -c . "$TRAP_SRC" | tr -d ' ')
if [ "$trap_n" -ge 4 ] && grep -qx 'trap finish EXIT' "$TRAP_SRC" &&
  ! grep -q "^trap 'finish'" "$TRAP_SRC"; then
  ok "signal-trap-plant: this file declares $trap_n top-level traps including 'trap finish EXIT', and none delegates a SIGNAL straight to finish"
else
  bad "signal-trap-plant: extracted $trap_n trap line(s) [$(tr '\n' ';' <"$TRAP_SRC")] — the case below would prove nothing"
fi
# mk_trap_harness <path> <trap-file> <ready-file> — the real `finish`'s status rule plus
# <trap-file>'s declarations, a READINESS STAMP, then a loop whose last command exits 0.
mk_trap_harness() {
  local path="$1" traps="$2" ready="$3"
  {
    printf '#!/usr/bin/env bash\n'
    printf 'finish() { local rc=$?; if [ "$rc" -ne 0 ]; then exit 1; fi; exit 0; }\n'
    cat "$traps"
    # THE SIGNAL IS ORDERED BY THIS STAMP, NEVER BY A SLEEP. A fixed delay would be a
    # wall-clock race of exactly the class this file's fixture pin removes: on a loaded
    # box bash can take longer than the delay to start, parse and INSTALL ITS TRAPS, and
    # a SIGTERM arriving before them is handled by the DEFAULT disposition (exit 143).
    # That makes the RED arm below — which requires the old form to exit 0 — fail-close
    # for a reason that has nothing to do with the trap form under test, while the GREEN
    # arm would still pass, for the wrong reason. The stamp is written only after every
    # trap declaration has executed, so the kill below cannot outrun them.
    printf 'printf r >%q\n' "$ready"
    printf 'while :; do :; done\n'
  } >"$path"
  chmod +x "$path"
}
# term_status <harness> <ready-file> -> the exit status after a SIGTERM delivered once the
# harness has AFFIRMATIVELY installed its traps, or `not-ready` if it never got there
# (bounded: a wedged harness fails the run rather than hanging it).
term_status() {
  local h="$1" ready="$2" pid st=0 waited=0
  rm -f "$ready"
  bash "$h" >/dev/null 2>&1 &
  pid=$!
  while [ ! -e "$ready" ] && [ "$waited" -lt 200 ]; do
    sleep 0.05
    waited=$((waited + 1))
  done
  if [ ! -e "$ready" ]; then
    kill -KILL "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null
    printf 'not-ready'
    return
  fi
  kill -TERM "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null || st=$?
  printf '%s' "$st"
}
mk_trap_harness "$T/trap-green.sh" "$TRAP_SRC" "$T/trap-green.ready"
printf 'trap finish EXIT\ntrap %s INT TERM HUP\n' "'finish'" >"$T/trap-red-lines.txt"
mk_trap_harness "$T/trap-red.sh" "$T/trap-red-lines.txt" "$T/trap-red.ready"
green_st=$(term_status "$T/trap-green.sh" "$T/trap-green.ready")
red_st=$(term_status "$T/trap-red.sh" "$T/trap-red.ready")
# A NON-NUMERIC STATUS IS ITS OWN REFUSAL, never fed to `[ ... -eq ]`: an unmeasured
# harness must not be read as either verdict.
trap_measured=yes
case "$green_st" in '' | *[!0-9]*) trap_measured=no ;; esac
case "$red_st" in '' | *[!0-9]*) trap_measured=no ;; esac
if [ "$trap_measured" = no ]; then
  bad "signal-trap-harness: a harness never reported its traps installed (green='$green_st' red='$red_st') — the two cases here would prove nothing"
elif [ "$red_st" -eq 0 ]; then
  ok "signal-trap-control: the OLD form (a signal delegating straight to finish) really does exit 0 on SIGTERM — the harness can see the defect"
else
  bad "signal-trap-control: the old form exited $red_st, not 0 — a green below would prove nothing"
fi
if [ "$trap_measured" = yes ] && [ "$green_st" -ne 0 ]; then
  ok "signal-trap: THIS suite's own trap declarations exit NONZERO ($green_st) on SIGTERM — a signalled run can never be read as a pass with cases unrun"
elif [ "$trap_measured" = yes ]; then
  bad "signal-trap: this suite's traps exit 0 on SIGTERM — a truncated run reports green"
fi

# --- Case 27: A MISSING LIVE-REACHABLE OBJECT IS CORRUPT (real fixtures) ----
# THE DEFECT THIS CASE EXISTS FOR (#3749 review round 4, item 1). fsck raises
# ERROR_REACHABLE (exit bit 2) for BOTH a stale reflog entry — routine on a store eight
# lanes write — and an object that is genuinely ABSENT while a live ref still needs it.
# The round-2/3 classifier put both on UNMEASURED, which is deliberately NON-FATAL to the
# supervisor's loop, so a demonstrably damaged store went on spawning workers and the
# journal called it "not measured". That is a false negative on real corruption, the one
# direction this whole control exists to prevent.
#
# TWO REAL FIXTURES, ONE PROPERTY APART, AND THE PROPERTY IS *WHAT STILL NEEDS THE
# OBJECT*. Both are built by the same code path and both have exactly one loose object
# deleted; in the first, HEAD's tree names it, and in the second only the reflog does.
# No shim: the round-4 arms in Case 17 stage the same two outcomes with a shim, which
# proves the classifier reacts to the flag, while these prove that REAL GIT answers the
# two configurations differently in the way the classifier depends on.
mk_missing_obj_repo() {
  # mk_missing_obj_repo <name> <live|reflog> -> path
  local r="$T/$1" mode="$2" victim=""
  mkdir -p "$r"
  git init -q "$r" >/dev/null 2>&1
  g "$r" config user.email t@t
  g "$r" config user.name t
  printf 'content aaa\n' >"$r/f1"
  g "$r" add f1 >/dev/null
  g "$r" -c user.email=t@t -c user.name=t commit -q -m c1 >/dev/null
  printf 'content bbb\n' >"$r/f2"
  g "$r" add f2 >/dev/null
  g "$r" -c user.email=t@t -c user.name=t commit -q -m c2 >/dev/null
  if [ "$mode" = live ]; then
    # The blob HEAD's tree still names.
    victim=$(g "$r" rev-parse HEAD:f2 2>/dev/null)
  else
    # A commit that ONLY the reflog names: created, then reset away.
    victim=$(g "$r" rev-parse HEAD 2>/dev/null)
    g "$r" reset -q --hard HEAD~1 >/dev/null 2>&1
  fi
  if [ -n "$victim" ]; then
    chmod u+w "$(loose_path "$r" "$victim")" 2>/dev/null
    rm -f "$(loose_path "$r" "$victim")"
  fi
  printf '%s' "$r"
}
# fsck_status_mode <repo> [--no-reflogs] -> the exit status
fsck_status_mode() {
  local r="$1" rc=0
  if [ "${2:-}" = --no-reflogs ]; then
    git -C "$r" fsck --no-progress --no-dangling --no-reflogs >/dev/null 2>&1 || rc=$?
  else
    git -C "$r" fsck --no-progress --no-dangling >/dev/null 2>&1 || rc=$?
  fi
  printf '%s' "$rc"
}
R_LIVE_MISS=$(mk_missing_obj_repo missing-live live)
R_REFLOG_MISS=$(mk_missing_obj_repo missing-reflog reflog)
LM_WITH=$(fsck_status_mode "$R_LIVE_MISS")
LM_WITHOUT=$(fsck_status_mode "$R_LIVE_MISS" --no-reflogs)
RM_WITH=$(fsck_status_mode "$R_REFLOG_MISS")
RM_WITHOUT=$(fsck_status_mode "$R_REFLOG_MISS" --no-reflogs)
# THE CONSTRUCTION ASSERT IS ON THE BITS, not on literal numbers, and it asserts the
# DISCRIMINATING OBSERVABLE: this is what makes the fixtures the two things the case
# claims. A git that answered the two configurations the same way would red HERE
# (attributable) rather than passing the cases below (not).
if [ "$((LM_WITH & 2))" -ne 0 ] && [ "$((LM_WITH & 5))" -eq 0 ] &&
  [ "$((LM_WITHOUT & 2))" -ne 0 ] &&
  [ "$((RM_WITH & 2))" -ne 0 ] && [ "$((RM_WITH & 5))" -eq 0 ] &&
  [ "$RM_WITHOUT" -eq 0 ]; then
  ok "missing-object-plant: the two fixtures ARE what the case claims (live-reachable: fsck $LM_WITH with reflogs and $LM_WITHOUT without, both ERROR_REACHABLE and no damage bit; reflog-only: $RM_WITH with reflogs and $RM_WITHOUT without)"
else
  bad "missing-object-plant: live=$LM_WITH/$LM_WITHOUT reflog-only=$RM_WITH/$RM_WITHOUT — not the two shapes the case is about; the cases below would prove nothing"
fi
if run 4 "missing live-reachable object: CORRUPT" --repo "$R_LIVE_MISS"; then
  if [ "$(verdict_of)" = CORRUPT ]; then
    ok "missing-live: an object MISSING while HEAD's tree still names it is CORRUPT (exit 4) — it stops the supervisor, where the pre-round-4 UNMEASURED did not"
  else
    bad "missing-live: verdict='$(verdict_of)', wanted CORRUPT — real corruption reported as something a consumer continues past"
  fi
  if printf '%s\n' "$OUT" | grep -q 'pass 3: fsck --no-reflogs' &&
    printf '%s\n' "$OUT" | grep -q 'live ref, the index or HEAD' &&
    printf '%s\n' "$OUT" | grep -q '^OBJECT-STORE: finding missing '; then
    ok "missing-live: the verdict is attributed by a THIRD --no-reflogs walk, says which roots still need the object, and names the diagnostic"
  else
    bad "missing-live: the CORRUPT verdict does not show the attribution: $(printf '%s\n' "$OUT" | grep -E 'measured pass|verdict-detail' | head -4 | tr '\n' ' ')"
  fi
fi
# ONE PROPERTY APART: the same deletion, reachable only from the reflog. Non-passing and
# NOT the fatal branch — so the CORRUPT above is attributable to the LIVE reachability and
# not merely to "an object is missing".
if run 5 "missing reflog-only object: UNMEASURED" --repo "$R_REFLOG_MISS"; then
  if [ "$(verdict_of)" = UNMEASURED ] &&
    printf '%s\n' "$OUT" | grep -q 'REFLOG-SCOPED'; then
    ok "missing-reflog-only: an object only the REFLOG names clears with --no-reflogs, so it stays REFLOG-SCOPED and non-passing — never the fatal branch, and never clean"
  else
    bad "missing-reflog-only: verdict='$(verdict_of)' — wanted UNMEASURED attributed REFLOG-SCOPED"
  fi
fi

# --- Case 28: THE SWEEP WALKS NEVER CARRY `--no-reflogs` (structural) -------
# `--no-reflogs` belongs to the reachability-CAUSE discriminator and to nothing else.
# Round 1 proposed it as a way to SUPPRESS the intermittent reflog false positive, the
# lead measured it and rejected it, and the two uses are one edit apart: put it on the
# sweep and a real missing object on a store with reflogs stops being reported at all.
# Behaviour cannot see the difference (both configurations exit 0 on a healthy store), so
# the guard is structural over the shipped call sites.
sweep_calls=$(grep -n '^[[:space:]]*fsck_pass ' "$SUBJECT")
p12_bad=$(printf '%s\n' "$sweep_calls" | grep -E 'fsck_pass p[12]\b' | grep -- '--no-reflogs' | head -1)
p3_ok=$(printf '%s\n' "$sweep_calls" | grep -cE 'fsck_pass p3 --no-reflogs$' | tr -d ' ')
n_calls=$(printf '%s\n' "$sweep_calls" | grep -c . | tr -d ' ')
if [ "$n_calls" -ge 3 ] && [ "$p3_ok" -eq 1 ]; then
  ok "no-reflogs-plant: the shipped script has $n_calls fsck_pass call sites and exactly one is the p3 --no-reflogs attribution walk — the assert below has a subject"
else
  bad "no-reflogs-plant: $n_calls fsck_pass call site(s), p3-with-flag=$p3_ok — the call sites moved; the assert below would be vacuous"
fi
if [ -z "$p12_bad" ]; then
  ok "no-reflogs: passes 1 and 2 — the sweep proper — carry no --no-reflogs, so the flag cannot become a suppressor of the very class it exists to attribute (structural)"
else
  bad "no-reflogs: a SWEEP walk carries --no-reflogs: $p12_bad"
fi

# --- Case 29: THE STORE KEY IS INJECTIVE OVER THE **RAW** PATH --------------
# THE DEFECT (#3749 review round 5, item 3). Round 4 made the caller's throttle/latch key
# injective over the FLATTENING and then computed the digest from the value this script had
# already passed through `sane()` — a DISPLAY encoding, and a lossy one. A store path
# holding a REAL newline and one holding the two literal characters `\n` render to the same
# text, so two different stores shared a throttle stamp AND a CORRUPT latch: one suppressing
# the other's sweep for the whole interval, or one store's damage stopping every lane
# working on the other. The identity now comes from the raw bytes, in this script, and the
# caller receives a finished key.
#
# The construction assert measures the COLLISION FIRST — the two values really do render
# identically — so a green below is injectivity and not two arbitrary distinct strings.
KEYFN="$T/keyfn.sh"
sed -n '/^sane() {/,/^}/p;/^store_digest() {/,/^}/p;/^store_key() {/,/^}/p' "$SUBJECT" >"$KEYFN"
if grep -q '^store_key() {' "$KEYFN" && grep -q '^store_digest() {' "$KEYFN" &&
  grep -q '^sane() {' "$KEYFN"; then
  ok "store-key-plant: sane/store_digest/store_key were extracted from the shipped script at run time — the case measures the shipped derivation, not a restatement of it"
else
  bad "store-key-plant: the extraction from $SUBJECT did not yield the three functions — everything below would be vacuous"
fi
RAW_NL=$(printf '/tmp/objstore-a\nb/objects')
RAW_LIT='/tmp/objstore-a\nb/objects'
SANE_NL=$(bash -c '. "$1"; sane "$2"' _ "$KEYFN" "$RAW_NL" 2>/dev/null)
SANE_LIT=$(bash -c '. "$1"; sane "$2"' _ "$KEYFN" "$RAW_LIT" 2>/dev/null)
if [ -n "$SANE_NL" ] && [ "$SANE_NL" = "$SANE_LIT" ]; then
  ok "store-key-plant: the two store paths RENDER identically through sane() ('$SANE_NL') — the collision this case is about is really available"
else
  bad "store-key-plant: the two paths render differently ('$SANE_NL' vs '$SANE_LIT') — the assert below would prove nothing"
fi
KEY_NL=$(bash -c '. "$1"; store_key "$2"' _ "$KEYFN" "$RAW_NL" 2>/dev/null)
KEY_LIT=$(bash -c '. "$1"; store_key "$2"' _ "$KEYFN" "$RAW_LIT" 2>/dev/null)
if [ -n "$KEY_NL" ] && [ -n "$KEY_LIT" ] && [ "$KEY_NL" != "$KEY_LIT" ]; then
  ok "store-key: two stores whose paths RENDER identically get DIFFERENT keys — the identity is the raw bytes, not the display encoding"
else
  bad "store-key: nl='$KEY_NL' lit='$KEY_LIT' — the key is computed from the rendering, so two different stores share a throttle stamp and a CORRUPT latch"
fi
# ROUND 4'S PROPERTY, PRESERVED: the flattening collision is still separated.
KEY_A=$(bash -c '. "$1"; store_key "$2"' _ "$KEYFN" '/tmp/objstore-collide/a/b/objects' 2>/dev/null)
KEY_B=$(bash -c '. "$1"; store_key "$2"' _ "$KEYFN" '/tmp/objstore-collide/a_b/objects' 2>/dev/null)
KEY_A2=$(bash -c '. "$1"; store_key "$2"' _ "$KEYFN" '/tmp/objstore-collide/a/b/objects' 2>/dev/null)
if [ -n "$KEY_A" ] && [ "$KEY_A" != "$KEY_B" ] && [ "$KEY_A" = "$KEY_A2" ]; then
  ok "store-key: two paths that FLATTEN to one name still get different keys, and the same path gets the SAME key twice (a digest, not a nonce)"
else
  bad "store-key: a='$KEY_A' b='$KEY_B' a2='$KEY_A2'"
fi
case "$KEY_A" in
  *objects.[0-9a-f]*) ok "store-key: the key keeps a readable tail naming the store and ends in the digest, so an operator reading 'ls /tmp' can still tell which store a stamp belongs to" ;;
  *) bad "store-key: '$KEY_A' is not operator-readable" ;;
esac
# END TO END, and this is the arm that pins the WIRING rather than the function: a real
# repository under a newline-bearing path. The printed key must NOT equal the key of the
# printed `store` line — which is exactly what it would equal if the caller (or this mode)
# digested the rendering.
NLDIR=$(printf '%s/nl-a\nb' "$T")
if mkdir -p "$NLDIR" 2>/dev/null && git init -q "$NLDIR/repo" >/dev/null 2>&1; then
  ok "store-key-plant: a repository really can be created under a newline-bearing path on this filesystem — the end-to-end arm below has a subject"
else
  bad "store-key-plant: could not create a repository under a newline-bearing path; the end-to-end arm below would prove nothing"
fi
OUT=$(bash "$SUBJECT" --repo "$NLDIR/repo" --print-store 2>&1)
RC=$?
record_out "print-store-newline"
PRINTED_STORE=$(printf '%s\n' "$OUT" | sed -n 's/^OBJECT-STORE: store //p' | head -1)
PRINTED_KEY=$(printf '%s\n' "$OUT" | sed -n 's/^OBJECT-STORE: store-key //p' | head -1)
KEY_OF_DISPLAY=$(bash -c '. "$1"; store_key "$2"' _ "$KEYFN" "$PRINTED_STORE" 2>/dev/null)
if [ "$RC" -eq 0 ] && [ -n "$PRINTED_KEY" ] && [ -n "$KEY_OF_DISPLAY" ] &&
  [ "$PRINTED_KEY" != "$KEY_OF_DISPLAY" ]; then
  ok "store-key: --print-store on a repository under a newline-bearing path prints a key that is NOT the key of its own rendered 'store' line — the digest is taken from the raw canonical path (rc=$RC)"
else
  bad "store-key(end-to-end): rc=$RC printed-key='$PRINTED_KEY' key-of-display='$KEY_OF_DISPLAY' store='$PRINTED_STORE' — the key is derived from the display rendering"
fi
# AND THE RENDERED LINE IS STILL SAFE: one line, with the newline escaped, so the anchored
# output invariant survives a path that could otherwise inject a prefix-less line.
if [ "$(printf '%s\n' "$OUT" | grep -c '^OBJECT-STORE: ')" = "$(printf '%s\n' "$OUT" | grep -c .)" ]; then
  ok "store-key: every line of the newline-path run is still anchored — sane() keeps doing the job it exists for, and the key does the job sane() cannot"
else
  bad "store-key: an unanchored line appeared for a newline-bearing store path: [$(printf '%s\n' "$OUT" | tr '\n' '|')]"
fi

# --- Case 30: THE LINKED-WORKTREE ROOTS A COMMON-DIR fsck **DOES** WALK ------
# THE REVIEW FINDING THIS CASE ANSWERS (#3749 review round 7) claimed that running fsck
# with `--git-dir=<common>` discards linked worktrees' private administrative context, so
# an object needed only by a lane's private HEAD or index could be overlooked and the
# store reported VERIFIED. MEASURED ON git 2.43.0, THAT IS FALSE — and the reason this
# case exists is that "false today" is not a property anybody had checked: the coverage
# was BELIEVED, and a future git that narrowed fsck's worktree enumeration would have
# shrunk this control silently. Now it reds a case.
#
# THREE ROOTS, EACH WITH A ONE-PROPERTY CONTROL. Each fixture is built by ONE code path
# and the arms differ only in whether the object is deleted, and the construction is
# asserted with git before the subject runs: the object really is gone, and the ONLY
# thing that names it is the linked worktree.
mk_wt_root_repo() {
  # mk_wt_root_repo <name> <head|index|prunable> <delete|keep> -> path
  # The victim object id is published to "$T/<name>.victim" and NOT to a variable: the
  # caller uses $( ), which runs this in a SUBSHELL, so an assignment would never be seen
  # (measured the hard way — it read as an unbound variable under set -u).
  local r="$T/$1" mode="$2" act="$3" wt="$T/$1-wt" WTR_VICTIM=""
  : >"$T/$1.victim"
  mkdir -p "$r"
  git init -q "$r" >/dev/null 2>&1
  g "$r" config user.email t@t
  g "$r" config user.name t
  printf 'content aaa\n' >"$r/f1"
  g "$r" add f1 >/dev/null
  GIT_AUTHOR_DATE="$FIXTURE_DATE" GIT_COMMITTER_DATE="$FIXTURE_DATE" \
    g "$r" -c user.email=t@t -c user.name=t commit -q -m c1 >/dev/null
  g "$r" worktree add -q --detach "$wt" HEAD >/dev/null 2>&1 || { printf '%s' "$r"; return 0; }
  case "$mode" in
    head | prunable)
      # A commit that exists ONLY as this linked worktree's detached HEAD.
      printf 'lane-private\n' >"$wt/u"
      g "$wt" add u >/dev/null
      GIT_AUTHOR_DATE="$FIXTURE_DATE" GIT_COMMITTER_DATE="$FIXTURE_DATE" \
        g "$wt" -c user.email=t@t -c user.name=t commit -q -m lane-only >/dev/null
      WTR_VICTIM=$(g "$wt" rev-parse HEAD 2>/dev/null)
      [ "$mode" = prunable ] && rm -rf "$wt"
      ;;
    index)
      # A blob STAGED in the linked worktree and in no commit anywhere.
      printf 'staged-only\n' >"$wt/s"
      g "$wt" add s >/dev/null
      WTR_VICTIM=$(g "$wt" rev-parse :s 2>/dev/null)
      ;;
  esac
  printf '%s' "$WTR_VICTIM" >"$T/$1.victim"
  if [ "$act" = delete ] && [ -n "$WTR_VICTIM" ]; then
    chmod u+w "$(loose_path "$r" "$WTR_VICTIM")" 2>/dev/null
    rm -f "$(loose_path "$r" "$WTR_VICTIM")"
  fi
  printf '%s' "$r"
}
for WTR_MODE in head index prunable; do
  R_WTR=$(mk_wt_root_repo "wtroot-$WTR_MODE" "$WTR_MODE" delete)
  WTR_DEAD=$(cat "$T/wtroot-$WTR_MODE.victim" 2>/dev/null || true)
  R_WTR_OK=$(mk_wt_root_repo "wtroot-$WTR_MODE-ctl" "$WTR_MODE" keep)
  WTR_ALIVE=$(cat "$T/wtroot-$WTR_MODE-ctl.victim" 2>/dev/null || true)
  # CONSTRUCTION, ASSERTED WITH git AND NOT WITH THE SUBJECT: the two arms name the SAME
  # object (so they differ in one property), the subject arm really has lost it, and the
  # control really still has it.
  if [ -n "$WTR_DEAD" ] && [ "$WTR_DEAD" = "$WTR_ALIVE" ] &&
    ! git -C "$R_WTR" cat-file -e "$WTR_DEAD" 2>/dev/null &&
    git -C "$R_WTR_OK" cat-file -e "$WTR_ALIVE" 2>/dev/null; then
    ok "wt-root-plant($WTR_MODE): both arms name the same object $WTR_DEAD, the subject has lost it and the one-property control still holds it"
  else
    bad "wt-root-plant($WTR_MODE): dead='$WTR_DEAD' alive='$WTR_ALIVE' — the arms are not one property apart; the two cases below would prove nothing"
  fi
  # THE SUBJECT: non-passing, and never VERIFIED. `head` and `prunable` reach the fatal
  # branch (the complaint survives --no-reflogs); `index` is asserted only as non-clean,
  # because WHICH non-passing verdict it earns is git's business and not this control's
  # claim — pinning the stronger one would red on a git that words it differently.
  OUT=$(bash "$SUBJECT" --repo "$R_WTR" 2>&1)
  RC=$?
  record_out "wt-root-$WTR_MODE"
  if [ "$RC" -ne 0 ] && [ "$(verdict_of)" != VERIFIED ]; then
    ok "wt-root($WTR_MODE): a common-dir fsck DOES walk a linked worktree's private $WTR_MODE root — an object needed only by it is not reported clean (exit $RC, verdict $(verdict_of))"
  else
    bad "wt-root($WTR_MODE): exit $RC verdict='$(verdict_of)' — the sweep called a store VERIFIED while an object a linked worktree's $WTR_MODE root needs is absent"
  fi
  OUT=$(bash "$SUBJECT" --repo "$R_WTR_OK" 2>&1)
  RC=$?
  record_out "wt-root-$WTR_MODE-control"
  if [ "$RC" -eq 0 ] && [ "$(verdict_of)" = VERIFIED ]; then
    ok "wt-root($WTR_MODE-control): the same fixture with the object PRESENT is VERIFIED, so the non-pass above is the missing object and not the worktree"
  else
    bad "wt-root($WTR_MODE-control): exit $RC verdict='$(verdict_of)' — the control does not pass, so the subject arm attributes nothing"
  fi
done

# --- Case 31: THE GAP IS DECLARED, ON EVERY RUN, IN THE OUTPUT --------------
# WHAT THIS CASE IS FOR (#3749 review round 8). A common-dir fsck does NOT use a LINKED
# worktree's per-worktree refs (`refs/worktree/*`, `refs/bisect/*`, `refs/rewritten/*`)
# as reachability roots, so an object named ONLY by one of them is not detected. A probe
# for that WAS built and REMOVED: it produced three false-clean routes of its own (a
# missing CHILD of a present root; a per-worktree ref whose NAME collides with a common
# one; a failed `awk`/`sort`/`comm` degrading to a zero-root census), and CLAUDE.md's
# ruling is that a guard with known false-PASSes is worse than no guard. What replaces it
# is a DECLARATION — and a declaration nobody can see is worth nothing, so the property
# under test is that it is EMITTED, on every verdict class, in the anchored stream.
#
# THIS CASE AND THE GAP ARE ONE FACT. If the gap is ever genuinely CLOSED, this case must
# change in the SAME diff as the declaration it pins: a stale declaration claiming a hole
# that no longer exists is as misleading as a silent hole.
dg_lines() { printf '%s\n' "$OUT" | grep '^OBJECT-STORE: declared-gap '; }
for DG_ARM in "clean:0:$R_CLEAN" "corrupt:4:$R_ROT" "unmeasured:5:$R_REFLOG_MISS"; do
  DG_NAME=${DG_ARM%%:*}
  DG_REST=${DG_ARM#*:}
  DG_WANT=${DG_REST%%:*}
  DG_REPO=${DG_REST#*:}
  OUT=$(bash "$SUBJECT" --repo "$DG_REPO" 2>&1)
  RC=$?
  record_out "declared-gap-$DG_NAME"
  # THE POINT OF RUNNING ALL THREE CLASSES: what a run did NOT walk does not depend on
  # what it found, so a declaration printed only beside the affirmative verdict would be
  # absent from exactly the logs an operator reads most carefully.
  if [ "$RC" -eq "$DG_WANT" ] && [ -n "$(dg_lines)" ]; then
    ok "declared-gap($DG_NAME): the run-time declaration is present on a $DG_NAME run (exit $RC, verdict $(verdict_of)) — it is not conditional on the verdict"
  else
    bad "declared-gap($DG_NAME): exit $RC (wanted $DG_WANT) verdict='$(verdict_of)' declaration-lines=$(dg_lines | grep -c .) — a run that omits the declaration is indistinguishable from one that covers the gap"
  fi
done
# THE COUNT CARRIES `RECOGNISED`, NEVER A BARE NUMBER: a bare `1` in a log reads as a
# complete census, and this list states its own non-exhaustiveness. Asserted in both
# directions — the affirmative form is present, and no `declared-gap <digits>` line
# exists without it.
OUT=$(bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
RC=$?
record_out "declared-gap-form"
DG_BARE=$(dg_lines | grep -E '^OBJECT-STORE: declared-gap [0-9]+$' | head -1)
if dg_lines | grep -q '^OBJECT-STORE: declared-gap 1 RECOGNISED' && [ -z "$DG_BARE" ]; then
  ok "declared-gap(form): the count is emitted as '1 RECOGNISED' and no bare-count line exists — the declaration cannot be read as a completed census"
else
  bad "declared-gap(form): recognised-line='$(dg_lines | grep -m1 RECOGNISED)' bare='$DG_BARE'"
fi
# IT MUST NAME WHAT IS NOT WALKED, WHAT *IS* WALKED, AND THE MEASUREMENT WITH ITS DATE.
# The middle one is what keeps the declaration honest: "an fsck misses some worktree
# state" would be true and useless, and the round-7 measurement (private HEAD, INDEX and
# prunable HEADs ARE walked — pinned by Case 30 above) is what narrows it to one
# namespace. The date is required because "zero instances" is a measurement that expires.
DG_MISS=""
for DG_TOK in 'refs/worktree/\*' 'refs/bisect/\*' 'refs/rewritten/\*' 'NOT in the gap' \
  'private HEAD and INDEX' 'prunable' '2026-09-02' '0 such refs' '#3749'; do
  dg_lines | grep -q -- "$DG_TOK" || DG_MISS="$DG_MISS [$DG_TOK]"
done
if [ -z "$DG_MISS" ]; then
  ok "declared-gap(content): the declaration names all three un-walked namespaces, the coverage that is NOT in the gap, the fleet measurement and its date, and the issue"
else
  bad "declared-gap(content): the declaration is missing$DG_MISS — a declaration that does not say what IS covered cannot be checked by a reader"
fi
# ANCHORED, AND BEFORE THE VERDICT. Property (a) of this script's output is that every
# line carries the prefix; and the declaration is printed BEFORE the walk, so it survives
# in a log truncated at the point a long fsck was killed.
DG_TOTAL=$(printf '%s\n' "$OUT" | grep -c 'declared-gap')
DG_ANCH=$(dg_lines | grep -c .)
DG_FIRST=$(printf '%s\n' "$OUT" | grep -nE '^OBJECT-STORE: (declared-gap|verdict) ' | head -1)
if [ "$DG_TOTAL" -eq "$DG_ANCH" ] && [ "$DG_ANCH" -gt 1 ] &&
  printf '%s' "$DG_FIRST" | grep -q 'declared-gap'; then
  ok "declared-gap(anchoring): all $DG_ANCH declaration lines are anchored and the declaration precedes the verdict — it survives a log truncated mid-walk"
else
  bad "declared-gap(anchoring): total=$DG_TOTAL anchored=$DG_ANCH first-of(declared-gap|verdict)='$DG_FIRST'"
fi
# AND THE REMOVAL IS COMPLETE: no vestigial mode, flag or census key anywhere in the
# shipped script or its two consumers. A half-removed probe is the worst of both — the
# declaration would say the gap is open while a dead code path claimed to check it.
DG_VEST=""
for DG_F in "$SUBJECT" "$(dirname "$SUBJECT")/../local/worker-supervisor.sh" \
  "$(dirname "$SUBJECT")/../bootstrap-agent-machine.sh"; do
  [ -r "$DG_F" ] || continue
  grep -qE 'probe-private-roots|PROBE_PRIVATE_ROOTS|private-root' "$DG_F" &&
    DG_VEST="$DG_VEST $(basename "$DG_F")"
done
if [ -z "$DG_VEST" ]; then
  ok "declared-gap(no-vestige): neither the sweep nor its two consumers carry a private-root probe mode, flag or census key — the gap is declared in ONE place and claimed nowhere"
else
  bad "declared-gap(no-vestige): probe remnants in$DG_VEST — if the gap has been CLOSED, change this case and the declaration together; if not, the remnant is dead code claiming coverage the declaration denies"
fi
