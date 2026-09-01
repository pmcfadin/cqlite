#!/usr/bin/env bash
#
# Regression tests for scripts/flow/review-stage.sh (issue #3751).
#
# FAST + HERMETIC: no network, no datasets, no cargo, no gh, and NOTHING is read from the
# surrounding checkout. Every case builds a SYNTHETIC git repository under `mktemp -d` (a
# `git init` plus a `.gitignore` — never a commit, so no git identity is needed) and runs the
# SHIPPED script with that repository as cwd. The script's only environmental inputs are
# `git rev-parse --show-toplevel` and `git check-ignore`, both of which then answer about the
# scratch repo. A suite header claiming "hermetic" while its green cases measure the
# surrounding repository is the defect class #3650 review R5 F2 records one file over, so the
# claim is made true by construction here rather than asserted.
#
# WHY A POSITIVE CONTROL IS THE FIRST REQUIREMENT OF THIS FILE
# -----------------------------------------------------------
# The subject is a mechanism whose safe answer is `NOT-RUN`. An implementation that answered
# `NOT-RUN` for EVERY input would satisfy every negative case in this suite and be useless —
# a guard that cannot green vacuously is the standing requirement (#1699/#3544). So case 2
# writes a REAL report and requires `PASS` with exit 0, and the FINDINGS case requires exit
# 4: the three non-NOT-RUN tokens are each proved REACHABLE before any refusal is asserted.
#
# WHY EVERY NOT-RUN CAUSE IS ASSERTED BY NAME
# -------------------------------------------
# SIX states share one token and one exit code, and the operator action differs per state
# ("the agent produced nothing" / "someone deleted the file" / "you never opened a stage" are
# three different next moves). Asserting exit 5 alone would pass on a script that collapsed
# all six into one message — which is the collapse this issue exists to remove. So each
# cause is matched as TEXT.
#
# Run standalone:   bash scripts/tests/test_review_stage.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RS="$SCRIPT_DIR/../flow/review-stage.sh"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

if [ ! -x "$RS" ]; then
  printf 'FAIL - %s is missing or not executable\n' "$RS" >&2
  exit 1
fi

# THE SCRATCH DIR IS VALIDATED BEFORE ANY PATH IS BUILT FROM IT (test_premerge_assert.sh's
# #3650 B5 lesson). An unchecked `mktemp` leaves $T EMPTY, after which every "$T/..." in this
# suite resolves to an ABSOLUTE path at the ROOT — synthetic git repos directly under / —
# which a privileged run would really create. Aborting BEFORE the trap is installed also
# keeps the trap from ever running `rm -rf ""`.
if ! T=$(mktemp -d "${TMPDIR:-/tmp}/review-stage-test.XXXXXX" 2>/dev/null) ||
  [ -z "$T" ] || [ ! -d "$T" ]; then
  printf 'FAIL - could not create a scratch directory under %s: refusing to run, because\n' \
    "${TMPDIR:-/tmp}" >&2
  printf 'FAIL - every path in this suite would resolve under / instead.\n' >&2
  exit 1
fi
trap 'rm -rf "$T"' EXIT

# newrepo [gitignore-body] — a synthetic worktree. With no argument it ignores
# `.review-stage/` exactly as the shipped .gitignore does; a caller passes a DIFFERENT body
# to exercise the fail-closed path verification. `git init` is enough: check-ignore and
# rev-parse --show-toplevel both work with no commits and no configured identity.
#
# EACH REPOSITORY IS UNIQUE BY CONSTRUCTION (`mktemp -d`), not by a counter. Every call site
# is `R=$(newrepo)`, i.e. COMMAND SUBSTITUTION, which runs the function in a SUBSHELL — so a
# `REPO_SEQ=$((REPO_SEQ + 1))` inside it incremented a variable in the subshell and the parent
# never saw it. Every case therefore shared ONE directory and leaked state into the next: the
# gitignore-body case rewrote the shared `.gitignore` to `unrelated-pattern`, after which the
# outside-the-repo case refused at the STAGE-RECORD half instead of the report path it names —
# a case passing for a reason that was not its own. The counter is REMOVED rather than worked
# around (an `export`/tempfile shim would leave the same trap for the next helper); `mktemp -d`
# cannot be defeated by a subshell because uniqueness comes from the filesystem, not from
# state this script has to carry.
newrepo() {
  local d
  d=$(mktemp -d "$T/repo.XXXXXX" 2>/dev/null) || return 1
  [ -n "$d" ] && [ -d "$d" ] || return 1
  git -C "$d" init -q >/dev/null 2>&1 || return 1
  if [ $# -ge 1 ]; then printf '%s\n' "$1" >"$d/.gitignore"; else printf '.review-stage/\n' >"$d/.gitignore"; fi
  printf '%s\n' "$d"
}

# rs <repo> <args...> — run the SHIPPED script inside <repo>, capturing stdout+stderr in OUT
# and the status in RC. stderr is merged because usage refusals go there and several cases
# assert their text.
OUT=""; RC=0
rs() {
  local repo="$1"; shift
  OUT="$(cd "$repo" && bash "$RS" "$@" 2>&1)"
  RC=$?
}

# has <needle> <label> — OUT must contain <needle> as a literal substring.
has() {
  case "$OUT" in
    *"$1"*) ok "$2" ;;
    *) bad "$2 (got: $OUT)" ;;
  esac
}
hasnt() {
  case "$OUT" in
    *"$1"*) bad "$2 — found the forbidden text '$1' (got: $OUT)" ;;
    *) ok "$2" ;;
  esac
}
rc_is() {
  if [ "$RC" -eq "$1" ]; then ok "$2"; else bad "$2 (expected rc=$1, got rc=$RC; out: $OUT)"; fi
}

REPORT_OF() { printf '%s/.review-stage/issue-%s/%s.md\n' "$1" "$2" "$3"; }

# --- 1. AC1: a stage that produced nothing is NOT-RUN, non-zero -----------------
# THE CASE THE ISSUE EXISTS FOR. Open a stage, spawn nothing, write nothing.
R="$(newrepo)" || { bad "could not create a scratch git repo"; printf '\n=== review-stage: %d passed, %d failed ===\n' "$PASS" "$FAIL"; exit 1; }
rs "$R" open c --issue 3751 --agent spec-auditor --deadline-secs 1800
rc_is 0 "AC1: open exits 0"
has "OPEN-OK" "AC1: open reports OPEN-OK"
has "$(REPORT_OF "$R" 3751 c)" "AC1: open prints the ABSOLUTE report path"
has "REPORT OF RECORD (mandatory)" "AC1: open prints the paste-ready spawn clause"
has "an absent review is not a clean one" "AC1: the clause states the consequence of silence"
if [ -f "$(REPORT_OF "$R" 3751 c)" ]; then
  ok "AC1: the report file EXISTS before any agent runs (pre-stamped sentinel)"
else
  bad "AC1: open did not create the report file"
fi
OUT="$(cat "$(REPORT_OF "$R" 3751 c)")"; RC=0
has "result: NOT-RUN (no report written)" "AC1: the pre-stamp records a NON-VERDICT sentinel"
has "spawned-at:" "AC1: the sentinel records spawned-at"
has "agent: spec-auditor" "AC1: the sentinel records the agent"
has "deadline-secs: 1800" "AC1: the sentinel records the deadline"

rs "$R" verdict c --issue 3751
rc_is 5 "AC1: the verdict of a silent stage is exit 5 (non-zero)"
has "RESULT: NOT-RUN (no report written)" "AC1: the verdict token is NOT-RUN, cause 'no report written'"
hasnt "RESULT: PASS" "AC1: the verdict is not readable as a pass"
has "elapsed=" "AC1: the verdict names the elapsed time"
has "agent=spec-auditor" "AC1: the verdict names the agent whose silence it reports"
if [ "$(printf '%s\n' "$OUT" | wc -l | tr -d ' ')" = "1" ]; then
  ok "AC1: verdict emits EXACTLY ONE line"
else
  bad "AC1: verdict must emit exactly one line (got: $OUT)"
fi

# --- 2. POSITIVE CONTROL: a real report reads PASS ------------------------------
# Without this case an always-NOT-RUN implementation passes the whole suite.
printf 'result: PASS\n\nReviewed the diff; no blocking finding.\n' >"$(REPORT_OF "$R" 3751 c)"
rs "$R" verdict c --issue 3751
rc_is 0 "POSITIVE CONTROL: a real report reads PASS with exit 0"
has "RESULT: PASS " "POSITIVE CONTROL: the token is exactly PASS"

printf 'result: FINDINGS\n\n1. blocking: the guard greens vacuously.\n' >"$(REPORT_OF "$R" 3751 c)"
rs "$R" verdict c --issue 3751
rc_is 4 "POSITIVE CONTROL: a findings report reads FINDINGS with exit 4"
has "RESULT: FINDINGS " "POSITIVE CONTROL: the token is exactly FINDINGS"

# --- 3. the six NOT-RUN causes, each BY NAME ------------------------------------
# (a) no report written — asserted in case 1 above, re-asserted here on a fresh stage so the
#     six causes are visible as one set (the sixth, `report unreadable`, is section 11b).
R2="$(newrepo)"
rs "$R2" open c --issue 100 --agent rust-reviewer --deadline-secs 60
rc_is 0 "causes: fresh stage opens"
rs "$R2" verdict c --issue 100
has "NOT-RUN (no report written)" "cause 1/6 BY NAME: no report written"

# (b) report absent — the stage is open and its report file is GONE.
rm -f "$(REPORT_OF "$R2" 100 c)"
rs "$R2" verdict c --issue 100
rc_is 5 "causes: an absent report is still exit 5"
has "NOT-RUN (report absent)" "cause 2/6 BY NAME: report absent"
has "agent=rust-reviewer" "causes: an absent report still reports the stage's agent (the stage record survives it)"

# (c) report empty — exists, holds nothing recordable.
printf '\n   \n\t\n' >"$(REPORT_OF "$R2" 100 c)"
rs "$R2" verdict c --issue 100
has "NOT-RUN (report empty)" "cause 3/6 BY NAME: report empty"

# (d) report ungrammatical — a body with no result line at all.
printf '# my review\n\nlooks fine to me\n' >"$(REPORT_OF "$R2" 100 c)"
rs "$R2" verdict c --issue 100
has "NOT-RUN (report ungrammatical: no 'result:' line)" "cause 4/6 BY NAME: report ungrammatical (no result line)"

# (e) stage never opened — no stage record for this kind at all.
rs "$R2" verdict coverage --issue 100
rc_is 5 "causes: a never-opened stage is exit 5"
has "NOT-RUN (stage never opened)" "cause 5/6 BY NAME: stage never opened"
hasnt "RESULT: PASS" "causes: a never-opened stage is not a pass"

# --- 4. the grammar is CLOSED, not prefix-tested --------------------------------
# THE #3544 LESSON: a `PASS*` test accepts a value that asserts its own unmeasuredness.
printf 'result: PASS-BUT-UNMEASURED\n' >"$(REPORT_OF "$R2" 100 c)"
rs "$R2" verdict c --issue 100
rc_is 5 "closed grammar: PASS-BUT-UNMEASURED does NOT satisfy PASS (exit 5)"
has "NOT-RUN (report ungrammatical: unrecognised result token 'PASS-BUT-UNMEASURED')" \
  "closed grammar: the unrecognised token is NAMED in the cause"
hasnt "RESULT: PASS " "closed grammar: PASS-BUT-UNMEASURED is not reported as PASS"

printf 'result: PASSED\n' >"$(REPORT_OF "$R2" 100 c)"
rs "$R2" verdict c --issue 100
rc_is 5 "closed grammar: PASSED is not PASS"
has "unrecognised result token 'PASSED'" "closed grammar: PASSED is named as unrecognised"

# Case matters, and the direction is fail-closed on purpose: a lowercase `pass` is refused
# rather than guessed at, and the refusal names the token the author actually wrote.
printf 'result: pass\n' >"$(REPORT_OF "$R2" 100 c)"
rs "$R2" verdict c --issue 100
rc_is 5 "closed grammar: a lowercase 'pass' is refused (fail-closed), not coerced"
has "unrecognised result token 'pass'" "closed grammar: the lowercase token is named"

# An empty result VALUE is ungrammatical, not empty — the file has content.
printf 'result:\n\nbody\n' >"$(REPORT_OF "$R2" 100 c)"
rs "$R2" verdict c --issue 100
has "NOT-RUN (report ungrammatical: empty 'result:' value)" "closed grammar: an empty result value is named separately from an empty report"

# A report may record its OWN NOT-RUN cause, and it is preferred over the default: an agent
# saying WHY it could not review is more informative than "no report written".
printf 'result: NOT-RUN (could not read the diff)\n' >"$(REPORT_OF "$R2" 100 c)"
rs "$R2" verdict c --issue 100
rc_is 5 "closed grammar: a self-recorded NOT-RUN stays NOT-RUN"
has "NOT-RUN (could not read the diff)" "closed grammar: a self-recorded cause is reported verbatim"

# A CAUSE CANNOT FORGE ONE OF THE LINE'S OWN key=value FIELDS (#3312's rule, applied to this
# grammar). Part of the cause is REPORT-DERIVED, and the report is written by the very agent
# whose stage is being judged, so a cause carrying `agent=peer` would put a second, EARLIER
# `agent=` pair on the line a consumer scans. Neutralised at the emit boundary, display-only:
# the token and the exit code are decided on the raw value before the line is built.
printf 'result: NOT-RUN (nothing ran agent=peer elapsed=999)\n' >"$(REPORT_OF "$R2" 100 c)"
rs "$R2" verdict c --issue 100
rc_is 5 "injection: a cause carrying key=value is still exit 5"
hasnt "agent=peer" "injection: a report-supplied 'agent=' cannot appear as a field on the verdict line"
has "agent=rust-reviewer" "injection: the MEASURED agent is the only agent= pair on the line"
has "nothing ran agent~peer elapsed~999" "injection: the cause is still readable, with '=' neutralised rather than dropped"
printf "result: PASS=really\n" >"$(REPORT_OF "$R2" 100 c)"
rs "$R2" verdict c --issue 100
rc_is 5 "injection: an unrecognised token carrying '=' is NOT-RUN"
hasnt "RESULT: PASS " "injection: 'PASS=really' is not reported as PASS"

# --- 4b. THE RESULT LINE IS READ AT COLUMN ZERO, AND THE TEMPLATE IS INERT (round 2, B1) --
# The sentinel `open` writes CONTAINS example `result: PASS` / `result: FINDINGS` lines, by
# design: the agent has to be told the exact spelling. While the parser allowed leading
# whitespace those examples were GRAMMATICALLY VALID verdict lines, and the only thing
# keeping them from being read was ORDER (`grep -m1` took the real line first). Order is not
# inertness: an agent that DELETES the sentinel line at column zero and appends its own
# verdict at the end got the TEMPLATE's PASS. That is #3312's family — "an artifact that
# DESCRIBED the escape hatch BECAME it" — reproduced inside the mechanism built to close it.
# Two independent properties are asserted, because either alone would leave a route.
R4B="$(newrepo)"
rs "$R4B" open c --issue 700 --agent spec-auditor
rc_is 0 "column-zero: a fresh stage opens"
R4B_REPORT="$(REPORT_OF "$R4B" 700 c)"

# (a) THE TEMPLATE CARRIES EXACTLY ONE candidate result line — the sentinel — even under the
#     OLD, loosened `^[[:space:]]*result:` pattern. This is the inertness half: it holds even
#     if the anchoring below were ever relaxed again.
N_LOOSE=$(LC_ALL=C grep -c -i '^[[:space:]]*result:' "$R4B_REPORT" 2>/dev/null || true)
if [ "$N_LOOSE" = "1" ]; then
  ok "column-zero: the pre-stamped template holds exactly ONE whitespace-tolerant 'result:' candidate (the sentinel), so its own examples cannot pose as the record"
else
  bad "column-zero: the template holds $N_LOOSE whitespace-tolerant 'result:' candidates; its examples are grammatically valid verdict lines and the only protection is grep -m1 ORDER (out: $(LC_ALL=C grep -i -n '^[[:space:]]*result:' "$R4B_REPORT"))"
fi

# (b) THE REVIEWER'S EXACT REPRODUCTION. Delete the column-zero sentinel (leaving the
#     template intact, which is what `sed -i '/^result:/d'` does) and append FINDINGS.
#     A portable equivalent of that sed is used: same effect, no BSD/GNU -i divergence.
LC_ALL=C grep -v '^result:' "$R4B_REPORT" >"$R4B_REPORT.tmp" && mv -f "$R4B_REPORT.tmp" "$R4B_REPORT"
printf 'result: FINDINGS\n' >>"$R4B_REPORT"
rs "$R4B" verdict c --issue 700
rc_is 4 "column-zero (B1 reproduction): a deleted sentinel + an APPENDED FINDINGS reads FINDINGS, not the template's PASS"
has "RESULT: FINDINGS " "column-zero (B1 reproduction): the token is the AGENT's, not the template's"
hasnt "RESULT: PASS " "column-zero (B1 reproduction): the indented template example is NOT read as the record"

# (c) AN INDENTED result line ALONE is not a record. "cannot tell" must not take the
#     permissive branch: an indented copy is DATA, so there is no result line at all.
printf '# report\n\n    result: PASS\n\nbody\n' >"$R4B_REPORT"
rs "$R4B" verdict c --issue 700
rc_is 5 "column-zero: an INDENTED 'result: PASS' alone is NOT-RUN, never a pass"
has "NOT-RUN (report ungrammatical: no 'result:' line)" "column-zero: the cause names the absent result line rather than inventing a verdict"
hasnt "RESULT: PASS " "column-zero: an indented result line cannot be read as PASS"

# (d) POSITIVE CONTROL for the anchor — without this, a parser that read NOTHING would pass
#     (b) and (c). An ordinary column-zero record still works, and so does the documented
#     case-insensitivity at column zero.
printf 'result: PASS\n\nreviewed, no blocking finding\n' >"$R4B_REPORT"
rs "$R4B" verdict c --issue 700
rc_is 0 "column-zero CONTROL: an ordinary column-zero 'result: PASS' still reads PASS"
has "RESULT: PASS " "column-zero CONTROL: the token is exactly PASS"
printf 'Result: FINDINGS\n' >"$R4B_REPORT"
rs "$R4B" verdict c --issue 700
rc_is 4 "column-zero CONTROL: 'Result:' at column zero is still recognised (case-insensitive, anchored)"

# --- 4c. THE TOKEN REDUCTION DOES NOT GLOB (round 2, B5) -------------------------
# `set -- $value` was an UNQUOTED expansion, so the value went through PATHNAME EXPANSION as
# well as word splitting: a report recording `result: *`, read from a directory holding a file
# named `PASS`, expanded to that filename and read PASS — a FALSE PASS produced by globbing,
# in the one function whose entire job is a closed grammar. The reduction is now a parameter
# expansion, which neither splits nor globs and needs no positional clobber.
R4C="$(newrepo)"
rs "$R4C" open c --issue 710 --agent spec-auditor
rc_is 0 "noglob: the stage opens"
printf 'result: *\n' >"$(REPORT_OF "$R4C" 710 c)"
: >"$R4C/PASS"        # the cwd of every `rs` call is the repo root
: >"$R4C/FINDINGS"    # both tokens present, so the glob has more than one candidate to sort
rs "$R4C" verdict c --issue 710
rc_is 5 "noglob: 'result: *' with files named PASS/FINDINGS in the cwd is NOT-RUN, not a glob-expanded pass"
hasnt "RESULT: PASS " "noglob: pathname expansion cannot produce a passing token"
has "unrecognised result token '*'" "noglob: the refusal names the token the author actually wrote, VERBATIM"
rm -f "$R4C/PASS" "$R4C/FINDINGS"

# CONTROL: the reduction still reduces. A first-word reduction that stopped working would
# make every multi-word value ungrammatical, so the documented behaviour is pinned here.
printf 'result: PASS reviewed the whole diff\n' >"$(REPORT_OF "$R4C" 710 c)"
rs "$R4C" verdict c --issue 710
rc_is 0 "noglob CONTROL: a multi-word value still reduces to its FIRST WORD (PASS)"
has "RESULT: PASS " "noglob CONTROL: the reduced token is PASS"

# --- 5. the path is verified gitignored, fail-closed ----------------------------
# (a) an explicit --report that git does NOT confirm ignored.
R3="$(newrepo)"
rs "$R3" open c --issue 200 --agent spec-auditor --report tracked-report.md
rc_is 2 "check-ignore: a non-ignored --report is REFUSED (exit 2)"
has "OPEN-REFUSED reason=path-not-gitignored" "check-ignore: the refusal names the reason"
has "tracked-report.md" "check-ignore: the refusal names the path"
has "#2926" "check-ignore: the refusal explains the mid-run tree-mutation hazard it prevents"
if [ -f "$R3/tracked-report.md" ]; then
  bad "check-ignore: the refusal must not write the file it refused"
else
  ok "check-ignore: nothing was written at the refused path"
fi

# (b) the STAGE RECORD's own path is verified too — checking only the report would leave the
#     other write dirtying a running gate. A repo whose .gitignore does NOT cover
#     .review-stage/ must refuse, naming that half.
R4="$(newrepo 'unrelated-pattern')"
rs "$R4" open c --issue 201 --agent spec-auditor
rc_is 2 "check-ignore: an unignored .review-stage/ is REFUSED"
has "what=stage-record" "check-ignore: the refusal names the stage-record half"
if [ -f "$R4/.review-stage/issue-201/c.stage" ]; then
  bad "check-ignore: the refusal must not write the stage record"
else
  ok "check-ignore: no stage record was written at the refused path"
fi

# (c) a path OUTSIDE the repository cannot be confirmed, so it is refused — "cannot tell"
#     must never take the permissive branch.
#
#     THE REFUSAL IS ASSERTED TO NAME **report-of-record** AND THE REQUESTED PATH, so this case
#     can only pass for its OWN reason. It could not, before: every `newrepo` ran in command
#     substitution, so the `REPO_SEQ` increment happened in a SUBSHELL and was lost — every case
#     shared ONE directory, R4 above rewrote its `.gitignore` to `unrelated-pattern`, and this
#     case then refused at the STAGE-RECORD half (a real refusal, for the wrong reason) without
#     ever reaching the report path it names. Each repo is now unique BY CONSTRUCTION.
rs "$R3" open c --issue 202 --agent spec-auditor --report "$T/outside-the-repo.md"
rc_is 2 "check-ignore: a path outside the repository is REFUSED, not exempted"
has "path-not-gitignored" "check-ignore: the outside-the-repo refusal names the same reason"
has "what=report-of-record" "check-ignore: the outside-the-repo refusal names the REPORT half, not the stage record"
has "path=$T/outside-the-repo.md" "check-ignore: the refusal names the REQUESTED path verbatim"
hasnt "what=stage-record" "check-ignore: this case reached the report path, not a stage-record refusal"

# --- 6. re-opening does not silently reset the clock ----------------------------
R5="$(newrepo)"
rs "$R5" open c --issue 300 --agent spec-auditor --deadline-secs 900
rc_is 0 "re-open: the first open succeeds"
FIRST_SPAWNED="$(grep -m1 '^spawned-at:' "$R5/.review-stage/issue-300/c.stage" | sed -e 's/^spawned-at:[[:space:]]*//')"
if [ -n "$FIRST_SPAWNED" ]; then ok "re-open: the first open recorded a spawned-at"; else bad "re-open: no spawned-at recorded"; fi
sleep 1
rs "$R5" open c --issue 300 --agent spec-auditor
rc_is 2 "re-open: a second open without --force is REFUSED (exit 2)"
has "OPEN-REFUSED reason=already-open" "re-open: the refusal names already-open"
SECOND_SPAWNED="$(grep -m1 '^spawned-at:' "$R5/.review-stage/issue-300/c.stage" | sed -e 's/^spawned-at:[[:space:]]*//')"
if [ "$SECOND_SPAWNED" = "$FIRST_SPAWNED" ]; then
  ok "re-open: the refusal PRESERVED the original spawned-at"
else
  bad "re-open: the refusal changed spawned-at ($FIRST_SPAWNED -> $SECOND_SPAWNED)"
fi
rs "$R5" open c --issue 300 --agent spec-auditor --force
rc_is 0 "re-open: --force is accepted"
FORCED_SPAWNED="$(grep -m1 '^spawned-at:' "$R5/.review-stage/issue-300/c.stage" | sed -e 's/^spawned-at:[[:space:]]*//')"
if [ "$FORCED_SPAWNED" = "$FIRST_SPAWNED" ]; then
  ok "re-open: --force PRESERVES spawned-at too — a re-spawn must not restart a clock a reader is using"
else
  bad "re-open: --force reset spawned-at ($FIRST_SPAWNED -> $FORCED_SPAWNED)"
fi
has "reopen-count=1" "re-open: --force records the re-open count"

# --- 7. status is visible and ADVISORY -----------------------------------------
R6="$(newrepo)"
rs "$R6" open c --issue 400 --agent flow-closer --deadline-secs 0
rc_is 0 "status: stage with a 0s deadline opens"
sleep 1
rs "$R6" status c --issue 400
rc_is 0 "status: exits 0 even past the deadline (advisory only — no caller may branch on it)"
has "state=sentinel-only" "status: names the sentinel-only state"
has "past-deadline=yes" "status: names that the deadline has passed"
has "PAST DEADLINE" "status: says so in prose, so a waiting stage is not mistaken for a hung one"
has "NOTHING has been produced" "status: names that nothing was produced"
case "$OUT" in
  *"elapsed=1"*|*"elapsed=2"*|*"elapsed=3"*) ok "status: names the measured elapsed time" ;;
  *) bad "status: did not name a plausible elapsed time (got: $OUT)" ;;
esac
has "the deadline never changes the verdict" "status: declares itself advisory"

# A LATE REPORT IS STILL A REPORT: the verdict comes from CONTENT, not the clock.
printf 'result: PASS\n\nSlow but real.\n' >"$(REPORT_OF "$R6" 400 c)"
rs "$R6" verdict c --issue 400
rc_is 0 "late report: a real report written PAST the deadline reads PASS (content, not clock)"
has "RESULT: PASS " "late report: the token is PASS"
rs "$R6" status c --issue 400
rc_is 0 "late report: status still exits 0"
has "state=reported" "late report: status reports the state as reported"

# status on a never-opened stage is informative and still exit 0.
rs "$R6" status coverage --issue 400
rc_is 0 "status: a never-opened stage is exit 0 (advisory)"
has "state=never-opened" "status: names the never-opened state"
has "no stage was ever opened" "status: says what to do about a never-opened stage"

# --- 8. record-author-performed: the full form ---------------------------------
R7="$(newrepo)"
rs "$R7" open c --issue 500 --agent spec-auditor --deadline-secs 600
rc_is 0 "author-performed: the stage opens"
rs "$R7" record-author-performed c --issue 500 \
  --reason 'no peer agent available on this box; C performed by hand against the spec deltas' \
  --evidence 'docs/round-artifacts/issue-500-hand-c.md' \
  --performed-by author
rc_is 0 "author-performed: the FULL form is accepted"
has "RECORD-OK" "author-performed: reports RECORD-OK"
has "result=AUTHOR-PERFORMED" "author-performed: records the AUTHOR-PERFORMED result"
has "an author's hand audit is not an independent one; weight it accordingly" \
  "author-performed: the required disclosure is emitted verbatim"
OUT="$(cat "$(REPORT_OF "$R7" 500 c)")"; RC=0
has "an author's hand audit is not an independent one; weight it accordingly" \
  "author-performed: the required disclosure is recorded verbatim IN THE REPORT"
has "performed-by: author" "author-performed: the report records who performed it"
has "evidence: docs/round-artifacts/issue-500-hand-c.md" "author-performed: the report records the evidence artifact"

rs "$R7" verdict c --issue 500
rc_is 6 "author-performed: the verdict is exit 6, its own code"
has "RESULT: AUTHOR-PERFORMED " "author-performed: the verdict token is AUTHOR-PERFORMED"
hasnt "RESULT: PASS" "author-performed: a reader grepping the passing token does NOT match it"

# A hand-written AUTHOR-PERFORMED token WITHOUT the working is not a disclosed substitute.
printf 'result: AUTHOR-PERFORMED\n\nI checked it.\n' >"$(REPORT_OF "$R7" 500 c)"
rs "$R7" verdict c --issue 500
rc_is 5 "author-performed: the token WITHOUT the disclosure is refused (exit 5, fail-closed)"
has "AUTHOR-PERFORMED without the required disclosure" "author-performed: the refusal names the missing disclosure"

# THE CLASSIFIER IS AS STRONG AS THE WRITER (#3751 round 1, F3). `verdict` reads reports the
# WRITER never produced — a hand-written one — and it used to accept any NON-EMPTY
# performed-by/reason/evidence, so `performed-by: nobody`, `reason: x`, `evidence: tbd` reached
# the token that PROCEEDS at the merge point while `record-author-performed` would have refused
# all three. The same fact must not be checked in two places with two strengths.
#
# hand_ap <report-path> <performed-by> <reason> <evidence> — a HAND-WRITTEN
# AUTHOR-PERFORMED report carrying the required disclosure verbatim, so every case below fails
# for the reason it names and not for a missing disclosure.
hand_ap() {
  { printf 'result: AUTHOR-PERFORMED\n\n'
    printf 'performed-by: %s\n' "$2"
    printf 'reason: %s\n' "$3"
    printf 'evidence: %s\n' "$4"
    printf '\n%s\n' "an author's hand audit is not an independent one; weight it accordingly"
  } >"$1"
}
AP_R="$(REPORT_OF "$R7" 500 c)"

# POSITIVE CONTROL FIRST: a hand-written report WITH real working still reads AUTHOR-PERFORMED.
# Without this, a classifier that refused every hand-written report would satisfy every case
# below — and the sanctioned fallback would be unreachable outside the writer.
hand_ap "$AP_R" author 'no peer agent available on this box; hand C against the spec deltas' \
  'docs/round-artifacts/issue-500-hand-c.md'
rs "$R7" verdict c --issue 500
rc_is 6 "hand-written AP: a hand report WITH its working still reads AUTHOR-PERFORMED (exit 6)"
has "RESULT: AUTHOR-PERFORMED " "hand-written AP: the positive control reports the token"

# THE FINDING'S OWN TRIPLE. Every field is present and non-empty, and every one is unusable.
hand_ap "$AP_R" nobody x tbd
rs "$R7" verdict c --issue 500
rc_is 5 "hand-written AP: performed-by=nobody / reason=x / evidence=tbd is NOT-RUN, never AUTHOR-PERFORMED"
hasnt "RESULT: AUTHOR-PERFORMED" "hand-written AP: the unusable triple does not reach the proceeding token"
has "report ungrammatical: AUTHOR-PERFORMED" "hand-written AP: reported as ungrammatical, naming the token"

# EACH FIELD, BY NAME — the operator action differs per field, exactly as it does for the six
# NOT-RUN causes.
hand_ap "$AP_R" nobody 'no peer agent available; hand C against the spec deltas' 'docs/x.md'
rs "$R7" verdict c --issue 500
rc_is 5 "hand-written AP: an out-of-set performed-by is refused by the CLASSIFIER too"
has "performed-by" "hand-written AP: the refusal names performed-by"
has "not 'author' or 'peer'" "hand-written AP: the refusal names the closed performer set"

hand_ap "$AP_R" author 'x' 'docs/x.md'
rs "$R7" verdict c --issue 500
rc_is 5 "hand-written AP: a reason with fewer than 3 recordable characters is refused"
has "recordable characters" "hand-written AP: the refusal names the recordable-characters rule"

hand_ap "$AP_R" author 'tbd' 'docs/x.md'
rs "$R7" verdict c --issue 500
rc_is 5 "hand-written AP: a PLACEHOLDER reason is refused by the classifier, as by the writer"
has "PLACEHOLDER" "hand-written AP: the refusal names it as a placeholder"

hand_ap "$AP_R" author 'hand-c-audit:<slug>' 'docs/x.md'
rs "$R7" verdict c --issue 500
rc_is 5 "hand-written AP: an UNSUBSTITUTED <…> in the reason is refused by the classifier"
has "UNSUBSTITUTED" "hand-written AP: the refusal names the unsubstituted template"

hand_ap "$AP_R" author 'no peer agent available; hand C against the spec deltas' 'tbd'
rs "$R7" verdict c --issue 500
rc_is 5 "hand-written AP: a PLACEHOLDER evidence is refused (the working must be NAMED)"
has "evidence" "hand-written AP: the refusal names the evidence field"

# --- 9. record-author-performed: the refusal matrix ----------------------------
R8="$(newrepo)"
rs "$R8" open c --issue 600 --agent spec-auditor
rc_is 0 "author-refusals: the stage opens"
GOOD_REASON='no peer agent available; hand C against the spec deltas'
GOOD_EV='docs/round-artifacts/issue-600-hand-c.md'

rs "$R8" record-author-performed c --issue 600 --evidence "$GOOD_EV" --performed-by author
rc_is 64 "author-refusals: a MISSING --reason is a usage error"
has "--reason <why> is required" "author-refusals: names the missing reason"

rs "$R8" record-author-performed c --issue 600 --reason "$GOOD_REASON" --performed-by author
rc_is 64 "author-refusals: a MISSING --evidence is a usage error"
has "--evidence <artifact> is required" "author-refusals: names the missing evidence"

rs "$R8" record-author-performed c --issue 600 --reason "$GOOD_REASON" --evidence "$GOOD_EV"
rc_is 64 "author-refusals: a MISSING --performed-by is a usage error"
has "--performed-by author|peer is required" "author-refusals: names the missing performer"

rs "$R8" record-author-performed c --issue 600 --reason "$GOOD_REASON" --evidence "$GOOD_EV" --performed-by nobody
rc_is 64 "author-refusals: an out-of-set --performed-by is a usage error"
has "must be exactly 'author' or 'peer'" "author-refusals: names the closed performer set"

rs "$R8" record-author-performed c --issue 600 --reason "$GOOD_REASON" --evidence "$GOOD_EV" --performed-by peer
rc_is 0 "author-refusals: --performed-by peer is accepted (peer-C is the PREFERRED form)"

for ph in why todo tbd TODO n/a placeholder; do
  rs "$R8" record-author-performed c --issue 600 --reason "$ph" --evidence "$GOOD_EV" --performed-by author
  rc_is 64 "author-refusals: placeholder --reason '$ph' is refused"
  has "PLACEHOLDER" "author-refusals: the '$ph' refusal names it as a placeholder"
done

rs "$R8" record-author-performed c --issue 600 --reason 'hand-c-audit:<slug>' --evidence "$GOOD_EV" --performed-by author
rc_is 64 "author-refusals: an UNSUBSTITUTED <…> in --reason is refused (before sanitization)"
has "UNSUBSTITUTED placeholder" "author-refusals: names the unsubstituted template"

rs "$R8" record-author-performed c --issue 600 --reason '' --evidence "$GOOD_EV" --performed-by author
rc_is 64 "author-refusals: a SUPPLIED-but-empty --reason is refused"

rs "$R8" record-author-performed c --issue 600 --reason '   ' --evidence "$GOOD_EV" --performed-by author
rc_is 64 "author-refusals: a --reason with nothing recordable in it is refused"
has "recordable characters" "author-refusals: names the recordable-characters requirement"

rs "$R8" record-author-performed c --issue 600 --reason "$GOOD_REASON" --evidence 'tbd' --performed-by author
rc_is 64 "author-refusals: a placeholder --evidence is refused too (the working must be named)"

rs "$R8" record-author-performed c --issue 600 --reason "$GOOD_REASON" --evidence '<artifact>' --performed-by author
rc_is 64 "author-refusals: an UNSUBSTITUTED <…> in --evidence is refused"

# A substitute recorded for a stage nobody opened has no subject: refused, not auto-opened.
rs "$R8" record-author-performed coverage --issue 600 --reason "$GOOD_REASON" --evidence "$GOOD_EV" --performed-by author
rc_is 2 "author-refusals: recording against a NEVER-OPENED stage is refused (exit 2)"
has "AUTHOR-REFUSED reason=stage-never-opened" "author-refusals: names the never-opened cause"

# --- 9b. record-author-performed NEVER SILENTLY REPLACES A RECORDED VERDICT (round 2, B2) --
# THE ASYMMETRY THAT MADE THIS A DEFECT: `open` refuses to re-stamp an already-open stage
# without --force ("re-opening would restart a clock a reader is using"), while
# record-author-performed overwrote a recorded BLOCKING verdict with a PROCEEDING one — the
# worse clobber under the weaker guard, with no flag, no warning and no trace of what was
# destroyed. An overwrite that leaves no trace is the audit-trail failure this issue is about.
R9B="$(newrepo)"
rs "$R9B" open c --issue 620 --agent spec-auditor
rc_is 0 "clobber: the stage opens"
R9B_REPORT="$(REPORT_OF "$R9B" 620 c)"
AP_REASON='no peer agent available on this box; hand C against the spec deltas'
AP_EV='docs/round-artifacts/issue-620-hand-c.md'

# (a) A RECORDED FINDINGS IS NOT REPLACEABLE WITHOUT --force.
printf 'result: FINDINGS\n\n### [BLOCKER] a real gap\n' >"$R9B_REPORT"
rs "$R9B" verdict c --issue 620
rc_is 4 "clobber CONTROL: the recorded FINDINGS reads FINDINGS before the attempt"
rs "$R9B" record-author-performed c --issue 620 --reason "$AP_REASON" --evidence "$AP_EV" --performed-by author
rc_is 2 "clobber: replacing a recorded FINDINGS is REFUSED without --force (exit 2)"
has "AUTHOR-REFUSED reason=verdict-already-recorded" "clobber: the refusal names the cause"
has "recorded-verdict=FINDINGS" "clobber: the refusal names the PRIOR token it would have destroyed"
rs "$R9B" verdict c --issue 620
rc_is 4 "clobber: the refused attempt left the recorded FINDINGS intact"
has "RESULT: FINDINGS " "clobber: FINDINGS still blocks after the refusal"

# (b) A RECORDED PASS is equally not replaceable — the destroyed verdict does not have to be
#     a blocking one for its erasure to be untraceable.
printf 'result: PASS\n\nreviewed, no blocking finding\n' >"$R9B_REPORT"
rs "$R9B" record-author-performed c --issue 620 --reason "$AP_REASON" --evidence "$AP_EV" --performed-by author
rc_is 2 "clobber: replacing a recorded PASS is REFUSED without --force"
has "recorded-verdict=PASS" "clobber: the refusal names PASS as the prior token"

# (c) FORCED, IT RECORDS WHAT IT REPLACED. A --force that erased the prior token silently
#     would move the hole rather than close it.
printf 'result: FINDINGS\n\n### [BLOCKER] a real gap\n' >"$R9B_REPORT"
rs "$R9B" record-author-performed c --issue 620 --reason "$AP_REASON" --evidence "$AP_EV" --performed-by author --force
rc_is 0 "clobber: --force is accepted"
has "replaced-verdict=FINDINGS" "clobber: the forced RECORD-OK line names the token it replaced"
OUT="$(cat "$R9B_REPORT")"; RC=0
has "replaced-verdict: FINDINGS" "clobber: the REPORT itself records the replaced token, so the substitution is auditable"
rs "$R9B" verdict c --issue 620
rc_is 6 "clobber: after a forced replacement the verdict is AUTHOR-PERFORMED (exit 6)"

# (d) CONTROL — the NORMAL path needs no flag. A guard that reds on correct input is the
#     guard agents learn to waive, so a sentinel-only report stays freely replaceable.
R9C="$(newrepo)"
rs "$R9C" open c --issue 630 --agent spec-auditor
rc_is 0 "clobber CONTROL: a second stage opens"
rs "$R9C" record-author-performed c --issue 630 --reason "$AP_REASON" --evidence "$AP_EV" --performed-by author
rc_is 0 "clobber CONTROL: a SENTINEL-ONLY report is replaced with NO --force (the normal path is unaffected)"
has "RECORD-OK" "clobber CONTROL: the normal path still reports RECORD-OK"
hasnt "replaced-verdict" "clobber CONTROL: nothing was replaced, so no replacement is claimed"

# --- 10. usage discipline ------------------------------------------------------
R9="$(newrepo)"
rs "$R9" open c --issue 700
rc_is 64 "usage: open without --agent is a usage error"
has "--agent <type> is required" "usage: names the missing agent"

rs "$R9" open 'c/../../etc/passwd' --issue 700 --agent spec-auditor
rc_is 64 "usage: a path-traversal <kind> is refused, not sanitized"
has "invalid <kind>" "usage: names the invalid kind"

rs "$R9" open c --issue abc --agent spec-auditor
rc_is 64 "usage: a non-numeric --issue is a usage error"

rs "$R9" open c --issue 700 --agent spec-auditor --deadline-secs -5
rc_is 64 "usage: a non-numeric --deadline-secs is a usage error"

rs "$R9" verdict
rc_is 64 "usage: verdict with no kind is a usage error"

rs "$R9" frobnicate c --issue 700
rc_is 64 "usage: an unknown subcommand is a usage error"
has "unknown subcommand" "usage: names the unknown subcommand"

rs "$R9"
rc_is 64 "usage: no subcommand is a usage error"

rs "$R9" --help
rc_is 0 "usage: --help exits 0"
has "REVIEW-STAGE:" "usage: --help renders the header contract"

# --- 11. a symlink is REFUSED, never followed -----------------------------------
# `git check-ignore` answers about a LEXICAL path; a WRITE follows symlinks. So an ignored
# `.review-stage/issue-<N>/c.md` that is a SYMLINK puts the write wherever the link points —
# a TRACKED file, or outside the repository — which falsifies the whole claim the gitignore
# verification exists to make: that a stage opened mid-run cannot dirty a running gate
# (#2926) or make premerge-assert refuse on `dirty: yes` (#3648).
#
# EVERY CASE ASSERTS THE VICTIM IS UNTOUCHED, not merely that the exit code was 2: a refusal
# that had already written through the link would satisfy an exit-code-only test.

# (a) the REPORT path is a symlink to a tracked, non-ignored file in the same repo.
R10="$(newrepo)"
printf 'the original tracked content\n' >"$R10/victim.md"
git -C "$R10" add victim.md >/dev/null 2>&1
mkdir -p "$R10/.review-stage/issue-800"
ln -s "$R10/victim.md" "$R10/.review-stage/issue-800/c.md"
rs "$R10" open c --issue 800 --agent spec-auditor
rc_is 2 "symlink: a REPORT path that is a symlink is REFUSED (exit 2)"
has "reason=path-is-symlink" "symlink: the refusal names the symlink reason"
has "what=report-of-record" "symlink: the refusal names which half was refused"
if [ "$(cat "$R10/victim.md")" = "the original tracked content" ]; then
  ok "symlink: the tracked file the link pointed at is UNTOUCHED"
else
  bad "symlink: the write FOLLOWED the link and clobbered a tracked file"
fi

# (b) the STAGE RECORD path is a symlink — checked too, for the same reason both paths are
#     checked for ignore status: refusing only the report leaves the other write following a link.
R11="$(newrepo)"
printf 'stage victim\n' >"$R11/victim.stage"
mkdir -p "$R11/.review-stage/issue-801"
ln -s "$R11/victim.stage" "$R11/.review-stage/issue-801/c.stage"
rs "$R11" open c --issue 801 --agent spec-auditor
rc_is 2 "symlink: a STAGE RECORD path that is a symlink is REFUSED"
has "what=stage-record" "symlink: the refusal names the stage-record half"
if [ "$(cat "$R11/victim.stage")" = "stage victim" ]; then
  ok "symlink: the stage-record link target is UNTOUCHED"
else
  bad "symlink: the stage-record write followed the link"
fi

# (c) an intermediate COMPONENT under .review-stage/ is a symlink — a symlinked
#     `.review-stage/issue-<N>` redirects BOTH writes just as effectively as a symlinked leaf,
#     and here it points OUTSIDE the repository entirely.
R12="$(newrepo)"
OUTSIDE="$T/outside-tree-$$"
mkdir -p "$OUTSIDE"
mkdir -p "$R12/.review-stage"
ln -s "$OUTSIDE" "$R12/.review-stage/issue-802"
rs "$R12" open c --issue 802 --agent spec-auditor
rc_is 2 "symlink: a symlinked PATH COMPONENT is REFUSED (not just the leaf)"
has "reason=path-is-symlink" "symlink: the component refusal names the symlink reason"
has "component=" "symlink: the refusal NAMES the offending component"
if [ -z "$(ls -A "$OUTSIDE" 2>/dev/null)" ]; then
  ok "symlink: nothing was written outside the repository through the component link"
else
  bad "symlink: the write escaped the repository through a symlinked component"
fi

# (d) a symlinked .review-stage/ ITSELF.
R13="$(newrepo)"
OUTSIDE2="$T/outside-review-stage-$$"
mkdir -p "$OUTSIDE2"
ln -s "$OUTSIDE2" "$R13/.review-stage"
rs "$R13" open c --issue 803 --agent spec-auditor
rc_is 2 "symlink: a symlinked .review-stage/ is REFUSED"
has "reason=path-is-symlink" "symlink: the .review-stage/ refusal names the symlink reason"

# (e) record-author-performed writes the report too, so it is held to the same rule.
R14="$(newrepo)"
rs "$R14" open c --issue 804 --agent spec-auditor
rc_is 0 "symlink: a normal stage opens (the control for the case below)"
printf 'author victim\n' >"$R14/victim-author.md"
rm -f "$(REPORT_OF "$R14" 804 c)"
ln -s "$R14/victim-author.md" "$(REPORT_OF "$R14" 804 c)"
rs "$R14" record-author-performed c --issue 804 \
  --reason 'no peer agent available; hand C against the spec deltas' \
  --evidence 'docs/round-artifacts/issue-804-hand-c.md' --performed-by author
rc_is 2 "symlink: record-author-performed REFUSES a symlinked report path"
has "path-is-symlink" "symlink: the author-performed refusal names the symlink reason"
if [ "$(cat "$R14/victim-author.md")" = "author victim" ]; then
  ok "symlink: the author-performed write did not follow the link"
else
  bad "symlink: record-author-performed followed the link"
fi

# (f) POSITIVE CONTROL: the ordinary path is unaffected, the report is a REGULAR file, and the
#     write is atomic-by-rename rather than in-place. Without this, a check that refused every
#     write would satisfy every case above.
R15="$(newrepo)"
rs "$R15" open c --issue 805 --agent spec-auditor
rc_is 0 "symlink control: an ordinary open still succeeds"
AP15="$(REPORT_OF "$R15" 805 c)"
if [ -f "$AP15" ] && [ ! -L "$AP15" ]; then
  ok "symlink control: the report is a REGULAR file, not a link"
else
  bad "symlink control: the report is missing or is a link"
fi
if [ -f "$R15/.review-stage/issue-805/c.stage" ] && [ ! -L "$R15/.review-stage/issue-805/c.stage" ]; then
  ok "symlink control: the stage record is a REGULAR file"
else
  bad "symlink control: the stage record is missing or is a link"
fi
# NO TEMPORARY FILE SURVIVES a successful write: a leftover `.c.md.tmp.<pid>` is an untracked
# file in the tree, which is the very thing the ignore verification exists to prevent — and it
# would be indistinguishable from a crashed write.
LEFTOVER="$(ls -A "$R15/.review-stage/issue-805" | grep -c 'tmp' || true)"
if [ "$LEFTOVER" = "0" ]; then
  ok "symlink control: no temporary file is left behind by a successful write"
else
  bad "symlink control: $LEFTOVER temporary file(s) survived the write"
fi
rs "$R15" verdict c --issue 805
rc_is 5 "symlink control: the atomically-written sentinel reads NOT-RUN as usual"
has "RESULT: NOT-RUN (no report written)" "symlink control: the sentinel content survived the atomic write"
printf 'result: PASS\n\nreviewed.\n' >"$AP15"
rs "$R15" verdict c --issue 805
rc_is 0 "symlink control: a real report over an atomically-written sentinel still reads PASS"

# (g) A DECLARED CONSEQUENCE of writing through a temporary file: a `--report` in a directory
#     ignored only by EXTENSION is refused, because the temp name is not matched by that pattern
#     and WOULD dirty a running gate. Pinned so it is a KNOWN, EXPLAINED refusal rather than a
#     surprise, and so the diagnostic keeps explaining the path the caller never named.
#     `.review-stage/` — the default, and the only path the pipeline uses — is ignored as a
#     DIRECTORY, so this never fires there (asserted by every green case above).
R16="$(newrepo '.review-stage/
*.md')"
mkdir -p "$R16/logs"
rs "$R16" open c --issue 806 --agent spec-auditor --report logs/mine.md
rc_is 2 "tempfile: a --report ignored only by EXTENSION is REFUSED (its temp would dirty the tree)"
has "what=report-of-record-tempfile" "tempfile: the refusal names the TEMPORARY half"
has "TEMPORARY file the write goes through" "tempfile: the refusal explains the path the caller never named"
has "ignore the DIRECTORY instead" "tempfile: the refusal names the remedy"
if [ -f "$R16/logs/mine.md" ]; then
  bad "tempfile: the refusal must not write the report it refused"
else
  ok "tempfile: nothing was written at the refused path"
fi
# And the DIRECTORY-ignored form of the same thing is ACCEPTED — the refusal above is about the
# pattern, not about --report itself, and without this the case above could pass on a blanket
# refusal of every custom report path.
R17="$(newrepo '.review-stage/
mylogs/')"
mkdir -p "$R17/mylogs"
rs "$R17" open c --issue 807 --agent spec-auditor --report mylogs/mine.md
rc_is 0 "tempfile control: a --report under a DIRECTORY-ignored path is accepted"
if [ -f "$R17/mylogs/mine.md" ] && [ ! -L "$R17/mylogs/mine.md" ]; then
  ok "tempfile control: the custom report was written as a regular file"
else
  bad "tempfile control: the custom report was not written"
fi

# --- 11d. A REFUSAL REPORTS ITS OWN SUBCOMMAND'S MARKER (round 2, S2) ---------------------
# `assert_ignored` / `assert_no_symlink` / the write helpers are shared by `open` and
# `record-author-performed`, and they hard-coded `OPEN-REFUSED` — so a record-author-performed
# refusal was reported under the WRONG subcommand's marker, while every refusal raised in
# `cmd_record_author_performed` itself said `AUTHOR-REFUSED`. One subcommand, two markers, is a
# grep that answers about the wrong thing.
R11D="$(newrepo)"
rs "$R11D" open c --issue 820 --agent spec-auditor
rc_is 0 "refuse-marker: the stage opens while .review-stage/ IS ignored"
# Now make the path NOT ignored — the same fail-closed check `open` passed a moment ago.
printf 'unrelated-pattern\n' >"$R11D/.gitignore"
rs "$R11D" record-author-performed c --issue 820 \
  --reason 'no peer agent available on this box; hand audit against the spec deltas' \
  --evidence 'docs/round-artifacts/issue-820-hand.md' --performed-by author
rc_is 2 "refuse-marker: a non-ignored report path is refused (exit 2)"
has "AUTHOR-REFUSED reason=path-not-gitignored" "refuse-marker: the refusal carries THIS subcommand's marker"
hasnt "OPEN-REFUSED" "refuse-marker: no 'OPEN-REFUSED' marker appears in a record-author-performed refusal"

# And the SYMLINK refusal, from the other shared helper.
printf '.review-stage/\n' >"$R11D/.gitignore"
rm -f "$(REPORT_OF "$R11D" 820 c)"
ln -s "$R11D/.gitignore" "$(REPORT_OF "$R11D" 820 c)"
rs "$R11D" record-author-performed c --issue 820 \
  --reason 'no peer agent available on this box; hand audit against the spec deltas' \
  --evidence 'docs/round-artifacts/issue-820-hand.md' --performed-by author
rc_is 2 "refuse-marker: a SYMLINKED report path is refused (exit 2)"
has "AUTHOR-REFUSED reason=path-is-symlink" "refuse-marker: the symlink refusal also carries THIS subcommand's marker"
hasnt "OPEN-REFUSED" "refuse-marker: the symlink refusal does not borrow open's marker either"

# CONTROL: `open`'s OWN refusals still say OPEN-REFUSED — a fix that renamed the marker
# globally, instead of making it per-subcommand, would pass both cases above.
R11E="$(newrepo 'unrelated-pattern')"
rs "$R11E" open c --issue 830 --agent spec-auditor
rc_is 2 "refuse-marker CONTROL: open still refuses a non-ignored path"
has "OPEN-REFUSED reason=path-not-gitignored" "refuse-marker CONTROL: open's own refusal still says OPEN-REFUSED"
hasnt "AUTHOR-REFUSED" "refuse-marker CONTROL: open does not borrow the author marker"

# --- 11c. THE report= FIELD GOES THROUGH THE SAME EMIT BOUNDARY (round 2, S1) -------------
# The cause is neutralised at the emit boundary because a report-supplied `agent=peer` would put
# a second, earlier `agent=` pair on a line consumers scan. `report=` is interpolated into that
# SAME line, is CALLER-INFLUENCED (`--report`), and was the one emitted value left raw — so the
# comment stating "ONE emit boundary" was not what the code did. Same treatment, one function.
R11C="$(newrepo)"
INJ_REPORT="$R11C/.review-stage/issue-810/a=b elapsed=999.md"
mkdir -p "$R11C/.review-stage/issue-810"
rs "$R11C" open t2 --issue 810 --agent spec-auditor --report "$INJ_REPORT"
rc_is 0 "report-field: open accepts a path carrying '=' (it is a legal filename, so refusing would red correct input)"
# The OPEN-OK LINE only. `open` also prints the path RAW on a line of its own (a caller consumes
# it) and inside the paste-ready clause — deliberate, and stated in the code: those are whole
# lines with no `key=value` fields for a pair to be injected into, so there is nothing to anchor.
OPEN_OK_LINE="$(printf '%s\n' "$OUT" | LC_ALL=C grep 'OPEN-OK' || true)"
case "$OPEN_OK_LINE" in
  *"elapsed=999"*) bad "report-field: the OPEN-OK line carries the path's injected 'elapsed=' pair (got: $OPEN_OK_LINE)" ;;
  *"a~b elapsed~999.md"*) ok "report-field: the OPEN-OK line neutralises the injected pair and keeps the path readable" ;;
  *) bad "report-field: could not read an OPEN-OK line to check (got: $OUT)" ;;
esac

rs "$R11C" verdict t2 --issue 810
rc_is 5 "report-field: the verdict of the sentinel-only stage is NOT-RUN"
hasnt "elapsed=999" "report-field: the VERDICT line does not carry the path's injected 'elapsed=' pair"
N_ELAPSED=$(printf '%s\n' "$OUT" | LC_ALL=C tr ' ' '\n' | LC_ALL=C grep -c '^elapsed=' || true)
if [ "$N_ELAPSED" = "1" ]; then
  ok "report-field: EXACTLY ONE 'elapsed=' field on the verdict line, so a first-match consumer reads the MEASURED value"
else
  bad "report-field: $N_ELAPSED 'elapsed=' fields on the verdict line (out: $OUT)"
fi
has "a~b elapsed~999.md" "report-field: the path is still READABLE, with '=' neutralised rather than dropped (display-only, like the cause)"

rs "$R11C" status t2 --issue 810
rc_is 0 "report-field: status is advisory (exit 0)"
hasnt "elapsed=999" "report-field: the STATUS line does not carry the injected pair either"

rs "$R11C" record-author-performed t2 --issue 810 \
  --reason 'no peer agent available on this box; hand audit against the spec deltas' \
  --evidence 'docs/round-artifacts/issue-810-hand.md' --performed-by author
rc_is 0 "report-field: record-author-performed accepts the stage"
hasnt "elapsed=999" "report-field: the RECORD-OK line does not carry the injected pair either"

# --- 11b. AN UNREADABLE REPORT IS ITS OWN CAUSE, NOT "report empty" (round 2, B7) ---------
# The cause list's entire justification is that THE OPERATOR ACTION DIFFERS PER CAUSE: "the file
# is empty" sends the operator to the agent, "I cannot read the file" sends them to `chmod`. An
# unreadable report was reported as `report empty`, and bash's own redirection error ("Permission
# denied") leaked to stderr beside the verdict line — a raw error is not a named refusal.
R11B="$(newrepo)"
rs "$R11B" open c --issue 800 --agent spec-auditor
rc_is 0 "unreadable: the stage opens"
R11B_REPORT="$(REPORT_OF "$R11B" 800 c)"
printf 'result: PASS\n\nreviewed.\n' >"$R11B_REPORT"
chmod 000 "$R11B_REPORT" 2>/dev/null || true

# THE PRECONDITION IS MEASURED BY ATTEMPTING THE READ, not by `[ -r ]` (which answers TRUE for
# root). Both branches execute the SAME NUMBER of assertions, so the suite's EXACT case floor
# stays host-independent, and the branch that cannot measure the property DECLARES that rather
# than passing silently.
if ( : <"$R11B_REPORT" ) 2>/dev/null; then
  ok "unreadable: DECLARED GAP — this host still reads a mode-000 file (running as root, or a filesystem ignoring mode bits), so the unreadable cause has NO SUBJECT here; the case below asserts what IS true on such a host instead of passing silently"
  rs "$R11B" verdict c --issue 800
  rc_is 0 "unreadable (no-subject host): the report IS readable, so its content decides — PASS"
  has "RESULT: PASS " "unreadable (no-subject host): the token is the report's own"
  hasnt "Permission denied" "unreadable (no-subject host): no raw shell error is emitted"
else
  ok "unreadable: the precondition holds — this host cannot read the mode-000 report (MEASURED by attempting the read, not by [ -r ], which answers TRUE for root)"
  rs "$R11B" verdict c --issue 800
  rc_is 5 "unreadable: an unreadable report is NOT-RUN (exit 5)"
  has "NOT-RUN (report unreadable)" "unreadable: cause 6/6 BY NAME — 'report unreadable', whose operator action is chmod, NOT 'report empty', whose operator action is the agent"
  hasnt "Permission denied" "unreadable: bash's raw redirection error does not leak beside the verdict line"
fi

# CONTROL, on every host: restored permissions read the content, so the cause above is about
# READABILITY and not about the file being rejected for some other reason.
chmod 644 "$R11B_REPORT" 2>/dev/null || true
rs "$R11B" verdict c --issue 800
rc_is 0 "unreadable CONTROL: restoring read permission reads the report again (PASS)"
has "RESULT: PASS " "unreadable CONTROL: the token is the report's own"

# AND THE EMPTY CAUSE IS STILL DISTINCT — a fix that folded unreadable into empty, or empty into
# unreadable, would pass one of the two cases above on its own.
: >"$R11B_REPORT"
rs "$R11B" verdict c --issue 800
rc_is 5 "unreadable: an EMPTY (but readable) report is still exit 5"
has "NOT-RUN (report empty)" "unreadable: 'report empty' still names the empty state, distinctly"
hasnt "report unreadable" "unreadable: an empty report is not reported as unreadable"

# --- 12. OUTSIDE A GIT WORKTREE: the documented exit fires, and no path is fabricated ------
# `repo_root` used to `die_usage` itself, and its only caller was `$(repo_root)` inside a
# COMMAND SUBSTITUTION — so `exit 64` terminated the SUBSHELL, the diagnostic printed once per
# substitution, and the script carried on with an EMPTY root: `verdict` emitted
# `report=/.review-stage/issue-1/c.md`, a FABRICATED absolute path, on the line that is
# otherwise the authority, while exiting 5 instead of the 64 the header documents. A `die` that
# cannot reach the top level is not a die.
NOGIT="$T/nogit"
mkdir -p "$NOGIT"
if git -C "$NOGIT" rev-parse --show-toplevel >/dev/null 2>&1; then
  # The scratch dir is inside a repository (an unusual TMPDIR), so this section cannot measure
  # what it claims. SAY SO rather than pass: a case that silently cannot run is the vacuous
  # green this suite exists to refuse.
  bad "outside-a-worktree: $NOGIT is inside a git repository, so this section could not measure anything — set TMPDIR to a non-repository path"
else
  ok "outside-a-worktree: the scratch dir is outside any git worktree (the precondition is MEASURED, not assumed)"

  rs "$NOGIT" verdict c --issue 1
  rc_is 64 "outside-a-worktree: verdict exits 64, the DOCUMENTED usage-error code"
  has "not inside a git worktree" "outside-a-worktree: the refusal names the cause"
  hasnt "=/.review-stage" "outside-a-worktree: NO '/'-rooted report path is emitted (the old code published a fabricated one)"
  hasnt "RESULT:" "outside-a-worktree: no verdict line at all — a refusal is not a verdict"
  N_DIAG=$(printf '%s\n' "$OUT" | LC_ALL=C grep -c 'not inside a git worktree' || true)
  if [ "$N_DIAG" = "1" ]; then
    ok "outside-a-worktree: the diagnostic prints ONCE (it printed once per command substitution while the die was trapped in a subshell)"
  else
    bad "outside-a-worktree: the diagnostic printed $N_DIAG times, so it is still being raised inside a substitution (out: $OUT)"
  fi

  rs "$NOGIT" open c --issue 1 --agent spec-auditor
  rc_is 64 "outside-a-worktree: open exits 64 too, instead of failing later inside mkdir"
  has "not inside a git worktree" "outside-a-worktree: open names the same cause"
  hasnt "mkdir" "outside-a-worktree: the failure is a named refusal, not a raw mkdir error"

  rs "$NOGIT" status c --issue 1
  rc_is 64 "outside-a-worktree: status exits 64 as well (all three readers agree)"

  # CONTROL: --help must NOT require a worktree. Resolving the root at the head of every
  # subcommand would be wrong if it also gated the usage text — a guard that reds on correct
  # input is the guard agents learn to waive.
  rs "$NOGIT" --help
  rc_is 0 "outside-a-worktree CONTROL: --help still works with no worktree"
  has "EXIT CODES" "outside-a-worktree CONTROL: --help prints the usage text"
fi

# --- case floor ---------------------------------------------------------------
# A CASE FLOOR (#3544). A span-replacing edit once silently deleted FOUR cases from a suite
# that then reported `failed: 0` at 102 instead of 105 — a green tally over a shrunken suite,
# which is this issue's own subject inside a test file.
#
# THE FLOOR EQUALS THE MEASURED COUNT, AND THAT IS DERIVED, NOT A HAIR-TRIGGER. Elsewhere in
# this repo a floor carries a margin because a legitimately-configured host can DISPLACE
# assertions (a node-less box makes `node-bindings` SKIP), so an exact floor would red on a
# correct machine. This suite has NO such branch: its only requirements are bash and git,
# every case runs on every host, no assertion is host-conditional, and a missing git is a hard
# failure because the subject IS a git-worktree tool. So the largest legitimate displacement
# is 0, and `measured - 0` is the strongest detector available — a floor with slack is a floor
# with a hole in it.
#
# TWO SECTIONS ADDED IN ROUND 2 BRANCH ON THE HOST, AND NEITHER WEAKENS THE EXACT FLOOR —
# checked rather than assumed, because an exact floor is only correct while the count is
# invariant on every PASSING run. (1) Section 11b's mode-000 case: both branches emit the SAME
# NUMBER of assertions by construction, so the count does not move. (2) Section 12's
# outside-a-worktree case: if the scratch dir turns out to be INSIDE a repository the section
# calls `bad`, so the run FAILS anyway — the floor could then only add a second red to an
# already-red run, never a false red to a green one. Any future host-conditional block must
# satisfy one of those two shapes or the floor needs a derived margin, as
# test_premerge_assert.sh's does.
# that stops noticing a silently-dying section. Adding cases never reds it (it is a lower
# bound); REMOVING one does, which is the point. Move it consciously, in the same diff as the
# shrink it accounts for.
ASSERT_FLOOR=261
EXECUTED=$((PASS + FAIL))
if [ "$EXECUTED" -lt "$ASSERT_FLOOR" ]; then
  bad "CASE FLOOR: only $EXECUTED assertions executed, below the committed floor of $ASSERT_FLOOR — a section died silently, and 'failed: 0' over a shrunken suite is not a pass"
fi

printf '\n=== review-stage: %d passed, %d failed (executed %d, floor %d) ===\n' \
  "$PASS" "$FAIL" "$((PASS + FAIL))" "$ASSERT_FLOOR"
[ "$FAIL" -eq 0 ]
