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
CASE_FLOOR=34

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
trap finish EXIT
trap 'finish' INT TERM HUP

# --- fixtures ---------------------------------------------------------------
g() { local r="$1"; shift; git -C "$r" "$@"; }

# newrepo <name> -> path. Two blobs, one commit. THE ONE CODE PATH every fixture is
# built by, so a corruption case's CLEAN TWIN is identical but for the planted damage.
newrepo() {
  local r="$T/$1"
  mkdir -p "$r"
  git init -q "$r" >/dev/null 2>&1
  g "$r" config user.email t@t
  g "$r" config user.name t
  printf 'content aaa\n' >"$r/f1"
  printf 'content bbb\n' >"$r/f2"
  g "$r" add f1 f2 >/dev/null
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
MUT="$T/mutant-connectivity-only.sh"
sed 's/fsck --no-progress --no-dangling/fsck --no-progress --no-dangling --connectivity-only/' \
  "$SUBJECT" >"$MUT"
if bash -n "$MUT" 2>/dev/null &&
  grep -q -- '--connectivity-only' "$MUT" &&
  ! grep -q -- 'fsck --no-progress --no-dangling --connectivity-only' "$SUBJECT" &&
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
  if printf '%s\n' "$OUT" | grep -q 'reachability/ref/commit-graph' &&
    printf '%s\n' "$OUT" | grep -q 'reflog expire'; then
    ok "reflog: the cause NAMES the class and gives the remedy for it (not the re-clone remedy, which is for damage)"
  else
    bad "reflog: the UNMEASURED cause does not name the reachability class"
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
  # mk_fsck_shim <dir> <always|once> <rc> <message> <log>
  local d="$1" when="$2" rc="$3" msg="$4" log="$5"
  mk_bin "$d" timeout gtimeout
  rm -f "$d/git"
  {
    printf '#!/usr/bin/env bash\n'
    printf '# Test shim: on `fsck`, report %s and exit %s; delegate everything else.\n' "$when" "$rc"
    printf 'for a in "$@"; do\n'
    printf '  if [ "$a" = fsck ]; then\n'
    printf '    printf "call\\n" >>%s\n' "$(printf '%q' "$log")"
    printf '    n=$(grep -c . %s 2>/dev/null || printf 0)\n' "$(printf '%q' "$log")"
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
  #     non-passing — and still not CORRUPT, because it is the reachability class.
  SH_ALWAYS="$T/shim-always"
  LOG_ALWAYS="$T/shim-always-calls.txt"
  : >"$LOG_ALWAYS"
  mk_fsck_shim "$SH_ALWAYS" always 2 "$RL_MSG" "$LOG_ALWAYS"
  OUT=$(PATH="$SH_ALWAYS:$PATH" bash "$SUBJECT" --repo "$R_CLEAN" 2>&1)
  RC=$?
  record_out "discriminator-persistent"
  if [ "$RC" -eq 5 ] && [ "$(verdict_of)" = UNMEASURED ] &&
    [ "$(grep -c . "$LOG_ALWAYS" | tr -d ' ')" -eq 2 ]; then
    ok "discriminator: the SAME diagnostic on BOTH walks does not reach VERIFIED — the re-run is a discriminator, not a retry-until-clean"
  else
    bad "discriminator(persistent): rc=$RC verdict='$(verdict_of)' walks=$(grep -c . "$LOG_ALWAYS" | tr -d ' ') (wanted 5/UNMEASURED/2)"
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
