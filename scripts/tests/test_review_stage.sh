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

# The descriptor number prepare_write holds. Declared here, once, because two structural
# assertions below name it and a bare literal in each would drift apart.
WRITE_FD_PIN=9

REPORT_OF() { printf '%s/.review-stage/issue-%s/%s.md\n' "$1" "$2" "$3"; }
# REPORT_GEN_OF <repo> <issue> <kind> <generation> — the same path, GENERATION-BOUND (round 5,
# J1). Generation 0 keeps the bare `<kind>.md` name (so `REPORT_OF` above is generation 0 and every
# quoted document example stays true); generation N>0 is `<kind>.<N>.md`.
REPORT_GEN_OF() {
  if [ "$4" = 0 ]; then printf '%s/.review-stage/issue-%s/%s.md\n' "$1" "$2" "$3"
  else printf '%s/.review-stage/issue-%s/%s.%s.md\n' "$1" "$2" "$3" "$4"; fi
}
# The path `open` PRINTS on its own line — the one a caller pastes into a spawn prompt. Read from
# the output rather than reconstructed, because "the clause hands the fresh agent the right file"
# is one of the properties under test.
printed_report_path() { printf '%s\n' "$OUT" | LC_ALL=C sed -n 's|^\(/.*\.md\)$|\1|p' | LC_ALL=C head -1; }

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

# --- 4d. EXACTLY ONE column-zero `result:` line (round 3, G2) --------------------
# Round 2 anchored `classify_report` at column zero and stopped there, leaving `grep -m1`:
# the FIRST of several anchored lines won. So a stale `result: PASS` followed by an APPENDED
# `result: FINDINGS` classified as PASS, and a merge proceeded over recorded blocking
# findings. Order is not a rule — it is whichever line happened to come first.
#
# THIS IS THE SECOND FINDING AT THIS SEAM IN TWO ROUNDS, and the fix is a CONSOLIDATION:
# `premerge-assert.sh`'s `_c_verdict_awk` already COUNTED its anchored lines and refused
# several as AMBIGUOUS while this reader took the first. Two readers of one shape holding two
# opinions is the divergence this repo pins, so they are now held to the same rule and
# `scripts/tests/test_premerge_assert.sh`'s DIFFERENTIAL section drives BOTH over one shared
# table of adversarial inputs — a second implementation's agreement is only knowable by
# testing it, never by care.
R4D="$(newrepo)"
rs "$R4D" open c --issue 470 --agent spec-auditor
rc_is 0 "several: the stage opens (the fixture)"
AP4D="$(REPORT_OF "$R4D" 470 c)"
# THE REVIEWER'S SCENARIO, VERBATIM: a stale PASS, then an appended FINDINGS.
printf 'result: PASS\n\nan earlier round found nothing.\n\nresult: FINDINGS\n\nthe later round found a blocker.\n' >"$AP4D"
rs "$R4D" verdict c --issue 470
rc_is 5 "several: a stale PASS followed by an appended FINDINGS is NOT-RUN, never PASS"
has "column-zero 'result:' lines" "several: the cause names that there were SEVERAL, not that there were none"
hasnt "RESULT: PASS " "several: the stale first line does NOT win"
hasnt "RESULT: FINDINGS " "several: nor does the last one — several candidates is refused, not resolved"
# AND THE OTHER ORDER, because a LAST-WINS reader would pass the case above. Neither order may
# resolve: the refusal comes from the COUNT.
printf 'result: FINDINGS\n\nblocker.\n\nresult: PASS\n\nsomeone appended a pass.\n' >"$AP4D"
rs "$R4D" verdict c --issue 470
rc_is 5 "several: the REVERSE order is refused too (a last-wins reader would have passed)"
has "column-zero 'result:' lines" "several: the reverse order names the same cause"
# ZERO AND SEVERAL STAY DISTINCT CAUSES. The operator action differs ("your agent wrote no
# verdict" / "this report records two"), and a fix folding either into the other would pass one
# of these two cases alone.
printf '# a report with prose only\n\nnothing recordable here.\n' >"$AP4D"
rs "$R4D" verdict c --issue 470
rc_is 5 "several: ZERO anchored lines is still its own cause"
has "no 'result:' line" "several: zero is reported as 'no result: line', not as a count"
hasnt "column-zero 'result:' lines" "several: the zero cause does not borrow the several cause's text"
# THE CONTROL, and it is the load-bearing half: an INDENTED copy is DATA, so a report with one
# column-zero line and any number of quoted/indented/fenced copies still reads its verdict.
# Without this, a fix that refused every report holding the word twice would pass the negatives.
printf 'result: PASS\n\nquoting another stage for context:\n\n    result: FINDINGS\n\n> result: NOT-RUN\n' >"$AP4D"
rs "$R4D" verdict c --issue 470
rc_is 0 "several CONTROL: one column-zero line plus indented and quoted copies still reads PASS"
has "RESULT: PASS " "several CONTROL: the single record is read"

# --- 5. the path is verified gitignored, fail-closed ----------------------------
# (a) THE REPORT HALF: a repository whose `.gitignore` covers the stage RECORD but not the
#     REPORT. Since round 4 the path is DERIVED (there is no `--report`), so the way to reach
#     this refusal is a repository that ignores one of the two files and not the other — which
#     is also the real-world shape (a hand-written pattern instead of the shipped
#     `.review-stage/` directory rule).
R3="$(newrepo '.review-stage/**/*.stage')"
rs "$R3" open c --issue 200 --agent spec-auditor
rc_is 2 "check-ignore: a report path git does not confirm ignored is REFUSED (exit 2)"
has "OPEN-REFUSED reason=path-not-gitignored" "check-ignore: the refusal names the reason"
has "what=report-of-record" "check-ignore: the refusal names the REPORT half"
has "path=$R3/.review-stage/issue-200/c.md" "check-ignore: the refusal names the DERIVED path verbatim"
has "#2926" "check-ignore: the refusal explains the mid-run tree-mutation hazard it prevents"
if [ -f "$R3/.review-stage/issue-200/c.md" ]; then
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

# (c) A PATH OUTSIDE THE REPOSITORY IS NO LONGER EXPRESSIBLE, which is why the case that used to
#     stand here is GONE rather than weakened. It passed `--report "$T/outside-the-repo.md"` and
#     asserted that `check-ignore`'s "cannot tell" (rc 128) took the refusing branch. With the
#     path DERIVED from the repo root plus a validated kind and issue (round 4, H2/H3) no
#     invocation can name a path outside the checkout, so the case would have been asserting
#     against an unreachable state — and section 13(b) pins the reachable half instead: the
#     removed flag is a usage error and creates NOTHING outside the checkout. `assert_ignored`
#     KEEPS its fail-closed shape (every non-zero `check-ignore` answer refuses, not just rc 1),
#     because it is the property the function is for; it is simply no longer reachable from here.
#     The refusals above still name the half they are about (`what=report-of-record` /
#     `what=stage-record`), which is what stopped an earlier version of these cases passing for
#     the wrong reason.

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

# --- 7b. EVERY NOT-RUN CAUSE MAPS TO ITS OWN `state=` (round 4, H4) --------------
# THE FINDING: `status` mapped `report unreadable` onto `state=report-ungrammatical` — the wrong
# REMEDIATION SIGNAL (`chmod`, not "your agent wrote a bad verdict line"), and a claim about
# CONTENT THAT WAS NEVER OBSERVED. `classify_report` already distinguishes the cause, so the
# collapse was purely in the status mapper.
#
# AND THE CLASS IS "ONE STATE COLLAPSING SEVERAL CAUSES", NOT THIS ONE INSTANCE, so all SEVEN
# reachable NOT-RUN causes are asserted here, each against its own state AND against the ABSENCE
# of the state it used to be confused with. Two were wrong: `report unreadable` (the named one)
# and a SELF-RECORDED cause (`result: NOT-RUN (ran out of context)`), which also fell through to
# `report-ungrammatical` — affirmatively false, since such a report is perfectly grammatical and
# is the one case where the AGENT told us why. `report ungrammatical: <what>` keeps ONE state for
# all its variants deliberately: the operator action is the same for every one of them.
R7B="$(newrepo)"

# (1) sentinel-only
rs "$R7B" open c --issue 480 --agent spec-auditor
rc_is 0 "status-map: the stage opened"
rs "$R7B" status c --issue 480
has "state=sentinel-only" "status-map: 'no report written' -> sentinel-only"

# (2) report absent
rm -f "$(REPORT_OF "$R7B" 480 c)"
rs "$R7B" status c --issue 480
has "state=report-absent" "status-map: 'report absent' -> report-absent"

# (3) report empty
: >"$(REPORT_OF "$R7B" 480 c)"
rs "$R7B" status c --issue 480
has "state=report-empty" "status-map: 'report empty' -> report-empty"

# (4) report UNREADABLE — the named finding. Mode 000 is not effective for root, so the case
#     asserts the mapping only where the read really is refused, and says which branch it took;
#     BOTH branches emit the same number of assertions, so the exact case floor does not move.
UNREAD="$(REPORT_OF "$R7B" 480 c)"
printf 'result: PASS\n\nreviewed.\n' >"$UNREAD"
chmod 000 "$UNREAD" 2>/dev/null || true
if ( : <"$UNREAD" ) 2>/dev/null; then
  ok "status-map: SKIPPED the unreadable mapping — this user can read a mode-000 file (root); nothing is asserted about a state that was not reached"
  ok "status-map: (the same, second half — the case emits a fixed number of assertions either way)"
else
  rs "$R7B" status c --issue 480
  has "state=report-unreadable" "status-map: 'report unreadable' -> report-unreadable (NOT report-ungrammatical: the fix is chmod)"
  hasnt "state=report-ungrammatical" "status-map: an unreadable report is no longer reported as ungrammatical — a claim about content never observed"
fi
chmod 644 "$UNREAD" 2>/dev/null || true

# (5) report ungrammatical — ONE state for every variant, which is correct: same operator action.
printf 'reviewed, but no verdict line.\n' >"$UNREAD"
rs "$R7B" status c --issue 480
has "state=report-ungrammatical" "status-map: 'report ungrammatical: no result line' -> report-ungrammatical"
printf 'result: PASS\nresult: FINDINGS\n' >"$UNREAD"
rs "$R7B" status c --issue 480
has "state=report-ungrammatical" "status-map: the AMBIGUOUS (several records) variant maps to the same state"
printf 'result: MAYBE\n' >"$UNREAD"
rs "$R7B" status c --issue 480
has "state=report-ungrammatical" "status-map: the unrecognised-token variant maps to the same state"

# (6) A SELF-RECORDED NOT-RUN CAUSE: the report is GRAMMATICAL and the agent said why. This used
#     to fall through to report-ungrammatical, which is false about the file and points the
#     operator at the wrong thing.
printf 'result: NOT-RUN (ran out of context before reading the diff)\n\nsee above.\n' >"$UNREAD"
rs "$R7B" status c --issue 480
rc_is 0 "status-map: a self-recorded NOT-RUN is advisory like every other state"
has "state=not-run-self-reported" "status-map: a self-recorded cause -> not-run-self-reported"
hasnt "state=report-ungrammatical" "status-map: a grammatical self-recorded NOT-RUN is NOT reported as ungrammatical"
has "ran out of context" "status-map: and the STATUS-NOTE passes the agent's own cause through, since that is the actionable part"

# (7) never opened
rs "$R7B" status coverage --issue 480
has "state=never-opened" "status-map: 'stage never opened' -> never-opened"

# STRUCTURAL DRIFT GUARD, DERIVED FROM THE SOURCE RATHER THAN CURATED. The mapper's fall-through
# means "this cause came from the report itself", which is true only while every BUILT-IN cause is
# enumerated: a new one added to `classify_report` and not to the mapper would be mislabelled
# `not-run-self-reported` — a false statement about where the cause came from, and exactly the
# class this item is about, one round later. So the built-in cause literals are EXTRACTED from the
# shipped script and each is required to be matched by an arm of the mapper block. Derived, so a
# new cause adds an assertion instead of needing this list edited.
MAPPER_BLOCK="$(LC_ALL=C sed -n '/STATUS-CAUSE-MAP-BEGIN/,/STATUS-CAUSE-MAP-END/p' "$RS")"
if [ -n "$MAPPER_BLOCK" ]; then
  ok "status-map/drift: the mapper block was located in the shipped script"
else
  bad "status-map/drift: could not locate the mapper block — the assertions below would be vacuous"
fi
BUILTIN_CAUSES="$(LC_ALL=C grep -o "NOT-RUN|[a-z][a-z' -]*" "$RS" \
  | LC_ALL=C sed -e "s/^NOT-RUN|//" -e "s/[ ]*$//" | LC_ALL=C sort -u)"
NCAUSES="$(printf '%s\n' "$BUILTIN_CAUSES" | LC_ALL=C grep -c . || true)"
if [ "${NCAUSES:-0}" -ge 5 ]; then
  ok "status-map/drift: $NCAUSES built-in cause literals were extracted (>= the 5 known ones, so the extraction is not empty)"
else
  bad "status-map/drift: only ${NCAUSES:-0} cause literal(s) extracted — the extraction broke and the guard is vacuous"
fi
printf '%s\n' "$BUILTIN_CAUSES" | while IFS= read -r CAUSE_LIT; do
  [ -n "$CAUSE_LIT" ] || continue
  case "$MAPPER_BLOCK" in
    *"\"$CAUSE_LIT\""*) printf 'ok   - status-map/drift: the mapper enumerates the built-in cause %s\n' "'$CAUSE_LIT'" ;;
    *) printf 'FAIL - status-map/drift: the built-in cause %s is NOT enumerated by the status mapper, so it would be mislabelled as self-reported\n' "'$CAUSE_LIT'" ;;
  esac
done >"$T/drift.out"
DRIFT_OK="$(LC_ALL=C grep -c '^ok   - ' "$T/drift.out" || true)"
DRIFT_BAD="$(LC_ALL=C grep -c '^FAIL - ' "$T/drift.out" || true)"
LC_ALL=C cat "$T/drift.out"
PASS=$((PASS + DRIFT_OK))
FAIL=$((FAIL + DRIFT_BAD))

# CONTROL: a real verdict is still `state=reported`, so the mapping did not turn every state into
# a named NOT-RUN one.
printf 'result: PASS\n\nreviewed.\n' >"$UNREAD"
rs "$R7B" status c --issue 480
has "state=reported" "status-map CONTROL: a recorded verdict is still state=reported"

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

# (g) A DECLARED CONSEQUENCE of writing through a temporary file: a repository that ignores the
#     records by EXTENSION rather than by DIRECTORY is refused, because the temp name matches no
#     such pattern and WOULD dirty a running gate. Pinned so it is a KNOWN, EXPLAINED refusal
#     rather than a surprise, and so the diagnostic keeps explaining the path the caller never
#     named. The SHIPPED `.gitignore` ignores `.review-stage/` as a DIRECTORY, so this never
#     fires here (asserted by every green case above). Round 4 note: this used to be reached with
#     a custom `--report` in an extension-ignored directory; with the path DERIVED the same state
#     is reached through the repository's OWN `.gitignore`, which is the shape that can actually
#     occur in the field.
R16="$(newrepo '.review-stage/**/*.md
.review-stage/**/*.stage')"
rs "$R16" open c --issue 806 --agent spec-auditor
rc_is 2 "tempfile: records ignored only by EXTENSION are REFUSED (the temp would dirty the tree)"
has "what=report-of-record-tempfile" "tempfile: the refusal names the TEMPORARY half"
has "TEMPORARY file the write goes through" "tempfile: the refusal explains the path the caller never named"
has "ignore the DIRECTORY instead" "tempfile: the refusal names the remedy"
if [ -f "$R16/.review-stage/issue-806/c.md" ]; then
  bad "tempfile: the refusal must not write the report it refused"
else
  ok "tempfile: nothing was written at the refused path"
fi
# And the DIRECTORY-ignored form of the same thing is ACCEPTED — the refusal above is about the
# PATTERN, not about writing at all, and without this control the case above could pass on a
# script that refused every write.
R17="$(newrepo '.review-stage/')"
rs "$R17" open c --issue 807 --agent spec-auditor
rc_is 0 "tempfile control: a DIRECTORY-ignored .review-stage/ is accepted"
if [ -f "$R17/.review-stage/issue-807/c.md" ] && [ ! -L "$R17/.review-stage/issue-807/c.md" ]; then
  ok "tempfile control: the report was written as a regular file"
else
  bad "tempfile control: the report was not written"
fi

# --- 11e. THE TEMPORARY FILE IS UNPREDICTABLE AND CREATED EXCLUSIVELY (round 3, G3) -------
# THE DEFECT: the temp path was `<dir>/.<basename>.tmp.$$` — DERIVABLE from the report path plus
# a pid — and it was VALIDATED and then REOPENED with shell redirection. A peer lane can write
# into this box's filesystem (every lane runs as one user under a shared HOME), so a symlink
# planted in that window made the write clobber its target, and the following `mv` could install
# that link as the report while reporting success. That is a NON-INVOKER route, not an
# invoker-class one.
#
# THE WINDOW IS REMOVED, NOT NARROWED: the name comes from `mktemp -u` (unpredictable, so there
# is nothing to pre-plant AT) and the file is created and opened in ONE step under `set -C`,
# which makes bash use `O_CREAT|O_EXCL` — measured to refuse an existing file, an existing
# symlink AND a DANGLING symlink without creating its target. The body then writes to the
# ALREADY-OPEN descriptor, so no path is re-resolved between validation and writing.
#
# THE IGNORE CHECK IS LEXICAL AND IS TAKEN ON THE EXACT NAME ABOUT TO BE CREATED, so it has no
# window of its own: `git check-ignore` answers about a path STRING, and the string checked is
# the string created.
R11E="$(newrepo '.review-stage/**/*.md
.review-stage/**/*.stage')"
# The extension-only-ignored REPOSITORY is used deliberately: its `-tempfile` refusal is the ONE
# place the temporary path is NAMED in the output, so it is how a test can observe a name that
# otherwise never leaves the process. (That refusal is round 1's declared consequence, pinned in
# its own right by section 11(g) below — this case reuses it as an oracle, it does not replace it.)
TMPNAME_OF() {
  printf '%s' "$1" | LC_ALL=C tr ' ' '\n' |
    LC_ALL=C grep -A0 '^path=' | LC_ALL=C sed -e 's/^path=//' | LC_ALL=C head -1
}
rs "$R11E" open c --issue 810 --agent spec-auditor
rc_is 2 "tempname: the extension-only-ignored repository still refuses (the oracle for this case)"
has "what=report-of-record-tempfile" "tempname: and the refusal is about the TEMPORARY half"
T1="$(TMPNAME_OF "$OUT")"
rs "$R11E" open c --issue 811 --agent spec-auditor
T2="$(TMPNAME_OF "$OUT")"
if [ -n "$T1" ] && [ -n "$T2" ]; then
  ok "tempname: the temporary path was observable in both runs (the case is not vacuous)"
else
  bad "tempname: could not observe the temporary path — the assertions below would be vacuous (T1='$T1' T2='$T2')"
fi
if [ -n "$T1" ] && [ -n "$T2" ]; then
  if [ "$T1" != "$T2" ]; then
    ok "tempname: two consecutive runs use DIFFERENT temporary names"
  else
    bad "tempname: two runs used the SAME temporary name ('$T1') — it is predictable from the report path"
  fi
  # AND NOT PREDICTABLE FROM (report path + pid), which "they differ" alone does NOT establish:
  # two runs are two processes, so the OLD `.tmp.$$` form also differed between them. A pid is at
  # most 7 digits on Linux (pid_max 4194304), so requiring >= 10 characters of suffix EXCLUDES
  # the pre-fix form DETERMINISTICALLY — no probabilistic assertion, which would flake.
  SUF1="${T1##*.tmp.}"
  if [ "$SUF1" != "$T1" ] && [ "${#SUF1}" -ge 10 ]; then
    ok "tempname: the random component is >= 10 chars, so it is not a pid (a pid is <= 7 digits)"
  else
    bad "tempname: the temporary name '$T1' has no >=10-char random component — it looks derivable from the report path plus a pid"
  fi
fi
# THE MECHANISM ITSELF IS ASSERTED STRUCTURALLY, AND IS LABELLED AS SUCH. The race is a race:
# planting a symlink inside a window that no longer exists, at a name that is no longer
# predictable, is not something a test can arrange — and pid reuse and scheduling are not
# controllable, the same reasoning round 2's S3 assert records. What IS decidable from source is
# that the three properties are present: an unpredictable name, an O_EXCL create-and-open, and a
# write through the descriptor rather than through the path.
PW_BODY=$(LC_ALL=C awk '
  /^prepare_write\(\) \{/ { inf = 1 }
  inf { print }
  inf && /^\}/ { exit }
' "$RS")
case "$PW_BODY" in
  "") bad "G3-structural: could not extract prepare_write from the shipped script — no subject" ;;
  *) ok "G3-structural: prepare_write was located in the shipped script" ;;
esac
case "$PW_BODY" in
  *'mktemp -u'*) ok "G3-structural: the temporary NAME comes from mktemp, not from the report path plus a pid" ;;
  *) bad "G3-structural: prepare_write does not use mktemp for the temporary name (body: $PW_BODY)" ;;
esac
case "$PW_BODY" in
  *'tmp.$$'* | *'tmp.$BASHPID'*)
    bad "G3-structural: the temporary name is still derived from the pid, so a peer can pre-plant at it (body: $PW_BODY)" ;;
  *) ok "G3-structural: the temporary name is NOT derived from the pid" ;;
esac
case "$PW_BODY" in
  *'set -C'*) ok "G3-structural: noclobber is set around the create, so bash opens with O_CREAT|O_EXCL" ;;
  *) bad "G3-structural: prepare_write does not set noclobber, so the create can follow a planted symlink (body: $PW_BODY)" ;;
esac
case "$PW_BODY" in
  *"exec $WRITE_FD_PIN>"*) ok "G3-structural: the file is CREATED AND OPENED in one step (a held descriptor)" ;;
  *) bad "G3-structural: prepare_write does not open descriptor $WRITE_FD_PIN on the temporary file, so the path is re-resolved at write time (body: $PW_BODY)" ;;
esac
# AND THE WRITE BODIES GO THROUGH THE DESCRIPTOR, NOT THE PATH. Without this the descriptor could
# be opened and then ignored — the window would be back, with a held fd as decoration.
if LC_ALL=C grep -q '} >"\$WRITE_TMP"' "$RS"; then
  bad "G3-structural: a write body still redirects to the temporary PATH, so the path is re-resolved between validation and writing"
else
  ok "G3-structural: no write body redirects to the temporary PATH"
fi
if [ "$(LC_ALL=C grep -c '} >&'"$WRITE_FD_PIN" "$RS" 2>/dev/null || true)" -ge 2 ]; then
  ok "G3-structural: both write bodies redirect to the held descriptor"
else
  bad "G3-structural: fewer than 2 write bodies redirect to the held descriptor — one of them still re-resolves a path"
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

# --- 11c. EVERY DATA VALUE GOES THROUGH THE ONE EMIT BOUNDARY (round 2 S1, round 4 H2) ------
# The rule: a value INTERPOLATED into one of this tool's `key=value` control lines is DATA, and a
# consumer scans those lines, so an injected `elapsed=`/`agent=` pair could put a SECOND, EARLIER
# pair on the line and be read instead of the measured one. `field_value` is the ONE boundary;
# `sanitize_field` is the stronger one for a value that becomes a RECORD field.
#
# ROUND 4 CHANGED WHICH VALUES CAN CARRY THE RESERVED CHARACTER, not the rule. The `report=` path
# used to be caller-controlled through `--report` (a path like `a=b elapsed=999.md` is a LEGAL
# filename, so it could not be refused); it is now DERIVED from a strictly-validated kind and
# issue, so it cannot carry `=` at all. The vectors that REMAIN are asserted here instead: the
# CAUSE (which comes from the report the tool is judging) and the flag values a caller supplies.
R11C="$(newrepo)"
rs "$R11C" open t2 --issue 810 --agent 'spec-auditor elapsed=999'
rc_is 0 "emit-boundary: open accepts an --agent carrying '=' (it is sanitized, not refused)"
OPEN_OK_LINE="$(printf '%s\n' "$OUT" | LC_ALL=C grep 'OPEN-OK' || true)"
case "$OPEN_OK_LINE" in
  *"elapsed=999"*) bad "emit-boundary: the OPEN-OK line carries the agent's injected 'elapsed=' pair (got: $OPEN_OK_LINE)" ;;
  *"agent=spec-auditor-elapsed-999"*) ok "emit-boundary: the OPEN-OK line records the agent with '=' neutralised and still readable" ;;
  *) bad "emit-boundary: could not read an OPEN-OK line to check (got: $OUT)" ;;
esac
N_ELAPSED=$(printf '%s\n' "$OPEN_OK_LINE" | LC_ALL=C tr ' ' '\n' | LC_ALL=C grep -c '^elapsed=' || true)
if [ "$N_ELAPSED" = "0" ]; then
  ok "emit-boundary: no 'elapsed=' pair reached the OPEN-OK line at all"
else
  bad "emit-boundary: $N_ELAPSED 'elapsed=' field(s) on the OPEN-OK line (out: $OPEN_OK_LINE)"
fi

# THE CAUSE: written by the very agent whose stage is being judged, and rendered INSIDE the
# verdict line's field list. A self-recorded NOT-RUN cause is the vector.
printf 'result: NOT-RUN (elapsed=999 agent=peer deadline=0)\n\nran out of context.\n' \
  >"$(REPORT_OF "$R11C" 810 t2)"
rs "$R11C" verdict t2 --issue 810
rc_is 5 "emit-boundary: a self-recorded NOT-RUN cause is reported"
hasnt "elapsed=999" "emit-boundary: the VERDICT line does not carry the cause's injected 'elapsed=' pair"
hasnt "agent=peer" "emit-boundary: nor its injected 'agent=' pair"
for FIELD in elapsed agent deadline report; do
  N=$(printf '%s\n' "$OUT" | LC_ALL=C tr ' ' '\n' | LC_ALL=C grep -c "^$FIELD=" || true)
  if [ "$N" = "1" ]; then
    ok "emit-boundary: EXACTLY ONE '$FIELD=' field on the verdict line, so a first-match consumer reads the MEASURED value"
  else
    bad "emit-boundary: $N '$FIELD=' fields on the verdict line (out: $OUT)"
  fi
done
has "elapsed~999" "emit-boundary: the cause is still READABLE, with '=' neutralised rather than dropped (display-only)"

# AND THE UNRECOGNISED-TOKEN CAUSE, which quotes the report's own token VERBATIM.
printf 'result: PASS=elapsed=999\n\nnot a token.\n' >"$(REPORT_OF "$R11C" 810 t2)"
rs "$R11C" verdict t2 --issue 810
rc_is 5 "emit-boundary: an unrecognised token is NOT-RUN"
hasnt "elapsed=999" "emit-boundary: the token quoted into the cause cannot inject a pair either"

rs "$R11C" status t2 --issue 810
rc_is 0 "emit-boundary: status is advisory (exit 0)"
hasnt "elapsed=999" "emit-boundary: the STATUS line does not carry an injected pair either"

# AND A FLAG VALUE THAT BECOMES A RECORD FIELD: --reason/--evidence go through sanitize_field,
# which is stricter still ('=' is not in its keep set).
rs "$R11C" record-author-performed t2 --issue 810 \
  --reason 'no peer agent available on this box; elapsed=999 hand audit against the spec deltas' \
  --evidence 'docs/round-artifacts/issue-810-hand.md' --performed-by author
rc_is 0 "emit-boundary: record-author-performed accepts the stage"
hasnt "elapsed=999" "emit-boundary: the RECORD-OK line does not carry the reason's injected pair either"

# AND EVERY NON-PRINTABLE CONTROL CHARACTER, NOT JUST THE THREE WHITESPACE ONES (round 5, J3).
# `one_line` mapped `\n`/`\r`/`\t` and deleted NUL, while its comment asserted that "no control
# character can break the one-line contract" — so ESC, BEL, backspace, VT, FF and DEL passed
# through into the verdict line and into `premerge-assert.sh`'s diagnostics, where a
# report-supplied cause could emit terminal escape sequences. The CLAIM being broader than the
# MECHANISM is the defect, independently of what a sequence can do.
#
# ASSERTED AS A BYTE CENSUS OVER THE WHOLE OUTPUT, not as "does it contain ESC": a case that
# checked one byte would pass on a fix that handled that byte alone, which is how the original
# gap survived (`\t` was handled and `\v` was not).
CTRL_CAUSE="$(printf 'ran out \033[2Jof \007context \010here \177and \013more')"
printf 'result: NOT-RUN (%s)\n\nsee above.\n' "$CTRL_CAUSE" >"$(REPORT_OF "$R11C" 810 t2)"
rs "$R11C" verdict t2 --issue 810
rc_is 5 "emit-boundary/controls: a control-bearing cause is still reported (not refused)"
CTRL_LEFT="$(printf '%s' "$OUT" | LC_ALL=C tr -dc '\001-\010\013\014\016-\037\177' | LC_ALL=C wc -c | LC_ALL=C tr -d ' ')"
if [ "$CTRL_LEFT" = "0" ]; then
  ok "emit-boundary/controls: NO C0 or DEL byte survives into the verdict line (ESC, BEL, backspace, VT and DEL all planted)"
else
  bad "emit-boundary/controls: $CTRL_LEFT control byte(s) survived into the verdict line (out: $(printf '%s' "$OUT" | LC_ALL=C tr -d '\001-\010\013\014\016-\037\177'))"
fi
N_LINES="$(printf '%s\n' "$OUT" | LC_ALL=C grep -c . || true)"
if [ "$N_LINES" = "1" ]; then
  ok "emit-boundary/controls: and the verdict is still EXACTLY ONE line"
else
  bad "emit-boundary/controls: the verdict became $N_LINES lines"
fi
has "ran out" "emit-boundary/controls: the readable prose either side of a control byte is PRESERVED"
has "and" "emit-boundary/controls: including the text after the DEL byte, so the cause is not truncated at the first control"
rs "$R11C" status t2 --issue 810
STATUS_CTRL="$(printf '%s' "$OUT" | LC_ALL=C tr -dc '\001-\010\013\014\016-\037\177' | LC_ALL=C wc -c | LC_ALL=C tr -d ' ')"
if [ "$STATUS_CTRL" = "0" ]; then
  ok "emit-boundary/controls: the STATUS-NOTE that passes the agent's own cause through is neutralised too"
else
  bad "emit-boundary/controls: $STATUS_CTRL control byte(s) reached the STATUS output"
fi

# CONTROL: ORDINARY PUNCTUATION AND NON-ASCII PROSE PASS THROUGH READABLE. Without this, a fix
# that dropped everything outside [A-Za-z0-9 ] would satisfy every assertion above — and a guard
# that mangles legitimate text is a guard people route around. The em dash also pins the
# `LC_ALL=C` requirement: a locale-sensitive `tr` aborts on it (BSD: "Illegal byte sequence"),
# which under `set -euo pipefail` would print no verdict line at all.
printf 'result: NOT-RUN (ran out of context %s 100%% of the "budget", (see notes) [ref: #3751])\n' \
  "$(printf '\342\200\224')" >"$(REPORT_OF "$R11C" 810 t2)"
rs "$R11C" verdict t2 --issue 810
rc_is 5 "emit-boundary/controls CONTROL: an ordinary prose cause is reported"
has "$(printf '\342\200\224')" "emit-boundary/controls CONTROL: a non-ASCII em dash survives byte-for-byte"
has '100% of the "budget"' "emit-boundary/controls CONTROL: punctuation and quotes survive verbatim"
has '[ref: #3751]' "emit-boundary/controls CONTROL: brackets and a colon survive verbatim"

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

# --- 11f. THE STAGE RECORD IS THE PUBLICATION MARKER (round 4, H1) ----------------
# THE DEFECT: `open --force` wrote the NEW stage record (carrying the NEW `head-sha:`) BEFORE it
# replaced the previous report with the sentinel. In that window — or PERMANENTLY, if the second
# write failed or the process was killed between them — a previous `result: PASS` was paired with
# the NEW commit, which is exactly the pair `premerge-assert.sh` accepts: it checks the recorded
# head-sha against the certified sha and reads the report's verdict, and both answers were then
# "yes". Round 3's own `head-sha` binding thus inherited a resource-lifetime bug.
#
# THE FIX IS AN ORDER, NOT A CHECK: reset the REPORT to the sentinel FIRST and write the STAGE
# RECORD LAST, so the record is the PUBLICATION MARKER and every partial state fails closed (no
# record ⇒ `stage never opened`; a record whose report is the sentinel ⇒ `no report written`).
#
# HOW IT IS OBSERVED. The order is not visible from the outside once both writes have landed, so
# these cases run an INSTRUMENTED copy of the shipped script whose `commit_write` appends the
# ON-DISK state after every successful write, and can `exit 90` at the first one to simulate a
# kill. The probe is inserted at the ONE post-`mv` success point by an ANCHORED substitution
# whose match count is asserted — an instrumentation that silently matched nothing would make
# every assertion below vacuous. It is order-AGNOSTIC (it fires after whichever write is first),
# so it detects the defect rather than assuming the fixed order.
RS_PROBE="$T/review-stage-probe.sh"
PROBE_HITS="$(LC_ALL=C grep -c '^  WRITE_TMP=""$' "$RS" || true)"
if [ "$PROBE_HITS" = "1" ]; then
  ok "marker-order: the probe anchor matches EXACTLY ONE line of the shipped script"
else
  bad "marker-order: the probe anchor matched $PROBE_HITS lines — the instrumentation below would be vacuous or misplaced"
fi
awk '
  { print }
  /^  WRITE_TMP=""$/ && !done {
    done = 1
    print "  if [ -n \"${PROBE_OUT:-}\" ]; then"
    print "    _pr_head=\"$( (LC_ALL=C grep -m1 \"^head-sha:\" \"${PROBE_SFILE:-/nonexistent}\" 2>/dev/null || printf \"head-sha: none\\n\") | LC_ALL=C sed -e \"s/^head-sha:[[:space:]]*//\" )\""
    print "    _pr_tok=\"$( (LC_ALL=C grep -m1 \"^result:\" \"${PROBE_RPATH:-/nonexistent}\" 2>/dev/null || printf \"result: none\\n\") | LC_ALL=C sed -e \"s/^result:[[:space:]]*//\" -e \"s/[[:space:]].*$//\" )\""
    print "    printf \"PROBE after=%s record-head=%s report-token=%s\\n\" \"$what\" \"$_pr_head\" \"$_pr_tok\" >>\"$PROBE_OUT\""
    print "    if [ -n \"${PROBE_KILL:-}\" ]; then exit 90; fi"
    print "  fi"
  }
' "$RS" >"$RS_PROBE"
if bash -n "$RS_PROBE" 2>/dev/null; then
  ok "marker-order: the instrumented copy is syntactically valid"
else
  bad "marker-order: the instrumented copy does not parse — the assertions below are vacuous"
fi

# rsp <repo> <probe-log> <sfile> <rpath> <kill:0|1> <args...> — the instrumented run.
rsp() {
  local repo="$1" log="$2" sf="$3" rp="$4" kill="$5"; shift 5
  : >"$log"
  if [ "$kill" = "1" ]; then
    OUT="$(cd "$repo" && PROBE_OUT="$log" PROBE_SFILE="$sf" PROBE_RPATH="$rp" PROBE_KILL=1 bash "$RS_PROBE" "$@" 2>&1)"
  else
    OUT="$(cd "$repo" && PROBE_OUT="$log" PROBE_SFILE="$sf" PROBE_RPATH="$rp" bash "$RS_PROBE" "$@" 2>&1)"
  fi
  RC=$?
}
# nth_probe <log> <n> — the n'th recorded on-disk state, or empty.
nth_probe() { LC_ALL=C sed -n "${2}p" "$1" 2>/dev/null || true; }

# (a) THE ORDER, on a first open: the report is published FIRST and the record LAST.
R11F="$(newrepo)"
printf 'seed\n' >"$R11F/seed.txt"
git -C "$R11F" add seed.txt >/dev/null 2>&1
git -C "$R11F" -c user.email=t@example.invalid -c user.name=t commit -q -m A >/dev/null 2>&1
SHA_A="$(git -C "$R11F" rev-parse HEAD 2>/dev/null || true)"
if [ -n "$SHA_A" ]; then
  ok "marker-order: the scratch repo has a resolvable HEAD (the head-sha binding is measurable)"
else
  bad "marker-order: could not commit in the scratch repo — the head-sha assertions would be vacuous"
fi
SF_A="$R11F/.review-stage/issue-900/c.stage"
RP_A="$(REPORT_OF "$R11F" 900 c)"
LOG_A="$T/probe-a.log"
rsp "$R11F" "$LOG_A" "$SF_A" "$RP_A" 0 open c --issue 900 --agent spec-auditor
rc_is 0 "marker-order: the instrumented first open succeeds"
NPROBE="$(LC_ALL=C grep -c . "$LOG_A" 2>/dev/null || true)"
if [ "$NPROBE" = "2" ]; then
  ok "marker-order: both writes were observed (2 probe records)"
else
  bad "marker-order: $NPROBE probe record(s) observed, expected 2 (log: $(cat "$LOG_A" 2>/dev/null))"
fi
P1="$(nth_probe "$LOG_A" 1)"
P2="$(nth_probe "$LOG_A" 2)"
case "$P1" in
  *"after=report-of-record"*) ok "marker-order: the REPORT is written FIRST" ;;
  *) bad "marker-order: the first write was not the report (got: $P1)" ;;
esac
case "$P2" in
  *"after=stage-record"*) ok "marker-order: the STAGE RECORD is written LAST — it is the publication marker" ;;
  *) bad "marker-order: the last write was not the stage record (got: $P2)" ;;
esac
case "$P1" in
  *"record-head=none"*) ok "marker-order: no stage record exists yet while the sentinel is published (a partial state reads 'stage never opened')" ;;
  *) bad "marker-order: a stage record already existed at the first write (got: $P1)" ;;
esac

# (b) THE PAIRING THE DEFECT PRODUCED: a forced re-open over a report recording PASS, at a
#     commit NEWER than the one the stage was opened at. The forbidden on-disk state is
#     (record head-sha == the NEW commit) AND (report reads PASS) — the pair premerge-assert
#     accepts. It must not exist at ANY write boundary.
rs "$R11F" open c --issue 901 --agent spec-auditor
rc_is 0 "marker-order: the stage under test opened at commit A"
printf 'result: PASS\n\naudited at A.\n' >"$(REPORT_OF "$R11F" 901 c)"
printf 'more\n' >>"$R11F/seed.txt"
git -C "$R11F" add seed.txt >/dev/null 2>&1
git -C "$R11F" -c user.email=t@example.invalid -c user.name=t commit -q -m B >/dev/null 2>&1
SHA_B="$(git -C "$R11F" rev-parse HEAD 2>/dev/null || true)"
if [ -n "$SHA_B" ] && [ "$SHA_B" != "$SHA_A" ]; then
  ok "marker-order: a second commit B exists and differs from A"
else
  bad "marker-order: could not create a distinct commit B (A=$SHA_A B=$SHA_B)"
fi
SF_B="$R11F/.review-stage/issue-901/c.stage"
# THE PROBE WATCHES THE GENERATION THE RE-OPEN PUBLISHES (round 5, J1). The re-opened stage's
# report is generation 1 (`c.1.md`), not the generation-0 file the previous agent holds — so the
# on-disk pair a reader would act on is (this record's head-sha, generation 1's verdict). Reading
# generation 0 here would measure a file nothing consults and report the H1 property as broken on
# a script that has it.
RP_B="$(REPORT_GEN_OF "$R11F" 901 c 1)"
RP_B_OLD="$(REPORT_OF "$R11F" 901 c)"
LOG_B="$T/probe-b.log"
rsp "$R11F" "$LOG_B" "$SF_B" "$RP_B" 0 open c --issue 901 --agent spec-auditor --force
rc_is 0 "marker-order: the forced re-open succeeds"
if [ -f "$RP_B" ]; then
  ok "marker-order: the re-open published generation 1's report, so the probe measured the file a reader consults"
else
  bad "marker-order: generation 1's report ($RP_B) does not exist — the assertions below would measure the wrong file"
fi
FORBIDDEN="$(LC_ALL=C grep -c "record-head=$SHA_B report-token=PASS" "$LOG_B" 2>/dev/null || true)"
if [ "$FORBIDDEN" = "0" ]; then
  ok "marker-order: at NO write boundary is the NEW head-sha paired with the OLD PASS report (the H1 defect)"
else
  bad "marker-order: $FORBIDDEN boundary/boundaries paired the new head-sha with the stale PASS (log: $(cat "$LOG_B" 2>/dev/null))"
fi
PB1="$(nth_probe "$LOG_B" 1)"
case "$PB1" in
  *"report-token=NOT-RUN"*) ok "marker-order: the new generation's report is the SENTINEL at the FIRST boundary" ;;
  *) bad "marker-order: the report was not yet the sentinel at the first boundary (got: $PB1)" ;;
esac
case "$PB1" in
  *"record-head=$SHA_A"*) ok "marker-order: the record still names the OLD commit while the new report is being published" ;;
  *) bad "marker-order: the record was already re-stamped before the report was published (got: $PB1)" ;;
esac
# AND THE PREVIOUS GENERATION'S PASS IS STILL ON DISK, UNREAD (round 5, J1). H1 used to CLOBBER it
# with the sentinel, which was the only thing keeping the stale verdict out of the reader's way;
# now the reader simply does not look there. Both properties are asserted, because the ORDER still
# has to hold for the generation the re-open publishes.
if LC_ALL=C grep -q '^result: PASS$' "$RP_B_OLD" 2>/dev/null; then
  ok "marker-order: the previous generation's PASS survives as history"
else
  bad "marker-order: the previous generation's report was clobbered instead of superseded"
fi
rs "$R11F" verdict c --issue 901
rc_is 5 "marker-order: and the re-opened stage reads its OWN generation, so the stale PASS is not a verdict"

# (c) THE INTERRUPTED STATE, PERMANENTLY: the same forced re-open, killed at the first write.
#     Whatever is on disk afterwards must NOT read as a verdict for the new tree. This is the
#     half a check could not deliver — a check placed after the harmful write could only report
#     it — so the property is that the harmful pairing is never REACHED.
rs "$R11F" open c --issue 902 --agent spec-auditor
rc_is 0 "marker-order: the kill case's stage opened at commit B"
printf 'result: PASS\n\naudited at B.\n' >"$(REPORT_OF "$R11F" 902 c)"
printf 'more still\n' >>"$R11F/seed.txt"
git -C "$R11F" add seed.txt >/dev/null 2>&1
git -C "$R11F" -c user.email=t@example.invalid -c user.name=t commit -q -m C >/dev/null 2>&1
SHA_C="$(git -C "$R11F" rev-parse HEAD 2>/dev/null || true)"
SF_C="$R11F/.review-stage/issue-902/c.stage"
RP_C="$(REPORT_OF "$R11F" 902 c)"
RP_C_NEW="$(REPORT_GEN_OF "$R11F" 902 c 1)"
LOG_C="$T/probe-c.log"
rsp "$R11F" "$LOG_C" "$SF_C" "$RP_C_NEW" 1 open c --issue 902 --agent spec-auditor --force
rc_is 90 "marker-order: the simulated kill fired at the first write"
NPC="$(LC_ALL=C grep -c . "$LOG_C" 2>/dev/null || true)"
if [ "$NPC" = "1" ]; then
  ok "marker-order: exactly ONE write completed before the kill"
else
  bad "marker-order: $NPC write(s) completed before the kill, expected 1"
fi
SENT="$(LC_ALL=C grep -c '^result: NOT-RUN' "$RP_C_NEW" 2>/dev/null || true)"
if [ "$SENT" = "1" ]; then
  ok "marker-order: the interrupted state leaves the new generation's SENTINEL on disk"
else
  bad "marker-order: the new generation's report is not the sentinel after the kill ($(LC_ALL=C grep -m1 '^result:' "$RP_C_NEW" 2>/dev/null))"
fi
# WHAT THE INTERRUPTED STATE IS, AND WHY IT IS STILL THE FAIL-CLOSED ONE (round 5, J1 changes the
# OBSERVABLE here, not the property). The RECORD is the publication marker and it was not written,
# so the stage is EXACTLY what it was before the re-open: generation 0, opened at B, recording the
# audit that was really made at B. That pair is TRUTHFUL — and it is now published ATOMICALLY,
# because `head-sha:` and `report-generation:` are two fields of ONE record write, so no partial
# state can pair a new commit with an older generation's verdict at all. Before generations, the
# same interruption left the ONE report clobbered to the sentinel; that was fail-closed too, but it
# DESTROYED the audit it had, and the H1 pairing was kept out only by the write order.
DISKHEAD="$(LC_ALL=C sed -n 's/^head-sha:[[:space:]]*//p' "$SF_C" 2>/dev/null | LC_ALL=C head -1 || true)"
if [ "$DISKHEAD" = "$SHA_B" ]; then
  ok "marker-order: the record still names the commit the audit was actually made at, so the merge point refuses on the sha too"
else
  bad "marker-order: the record's head-sha is '$DISKHEAD', expected the pre-kill '$SHA_B'"
fi
DISKGEN="$(LC_ALL=C sed -n 's/^report-generation:[[:space:]]*//p' "$SF_C" 2>/dev/null | LC_ALL=C head -1 || true)"
if [ "$DISKGEN" = "0" ]; then
  ok "marker-order: and it still names the generation that head-sha was stamped WITH — the pair is atomic, so it cannot be split by an interruption"
else
  bad "marker-order: the record's report-generation is '$DISKGEN', expected the pre-kill '0'"
fi
rs "$R11F" verdict c --issue 902
rc_is 0 "marker-order: the interrupted re-open published NOTHING, so the stage is still the previous generation"
has "report=$RP_C" "marker-order: the verdict is read from the generation the RECORD names, not the half-published one"
hasnt "report=$RP_C_NEW" "marker-order: the unpublished generation's sentinel is not read"

# (d) POSITIVE CONTROL: the UNINTERRUPTED forced re-open leaves a USABLE stage — otherwise (c)
#     could pass on a script that broke --force altogether, and a guard that reds on correct
#     input is the guard agents learn to waive.
rs "$R11F" open c --issue 902 --agent spec-auditor --force
rc_is 0 "marker-order CONTROL: a complete forced re-open succeeds"
RP_C_LIVE="$(printed_report_path)"
if [ -n "$RP_C_LIVE" ] && [ "$RP_C_LIVE" != "$RP_C" ]; then
  ok "marker-order CONTROL: the completed re-open published a NEW generation and printed its path"
else
  bad "marker-order CONTROL: the re-open printed '$RP_C_LIVE', which is not a fresh generation"
fi
printf 'result: PASS\n\nre-audited at C.\n' >"$RP_C_LIVE"
rs "$R11F" verdict c --issue 902
rc_is 0 "marker-order CONTROL: the re-opened stage can record a PASS again"
DISKHEAD2="$(LC_ALL=C sed -n 's/^head-sha:[[:space:]]*//p' "$SF_C" 2>/dev/null | LC_ALL=C head -1 || true)"
if [ "$DISKHEAD2" = "$SHA_C" ]; then
  ok "marker-order CONTROL: the completed re-open re-stamped head-sha to the CURRENT commit"
else
  bad "marker-order CONTROL: head-sha is '$DISKHEAD2' after a complete re-open, expected '$SHA_C'"
fi

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

# --- 13. THE REPORT PATH IS DERIVED — NO CALLER-CONTROLLED COMPONENT (round 4, H2/H3) -----
# THE FINDINGS, both of which were properties of the `--report` OVERRIDE:
#   H2 the caller's path was written RAW into the LINE-ORIENTED stage record, so a LEGAL
#      filename containing a NEWLINE split across lines and the reader (`load_stage`, via
#      `read_field`) took only the PREFIX — which could name a DIFFERENT, pre-existing report
#      recording PASS while the sentinel had gone to the newline-bearing name;
#   H3 `open` created the report's PARENT DIRECTORY before verifying repository containment and
#      ignore status, so a REFUSED outside-the-repository path still created directories outside
#      the checkout.
#
# BOTH ARE CLOSED BY CONSTRUCTION, NOT BY A CHECK: `--report` is REMOVED, so the path is always
# `<repo-root>/.review-stage/issue-<N>/<kind>.md`. `<kind>` and `<issue>` are then the WHOLE path
# input surface, and both are validated strictly (kind `[A-Za-z0-9][A-Za-z0-9_-]*`, issue digits
# only), so there is no newline to split on and no containment question to answer. The removal is
# a DELIBERATE NARROWING of the approved design surface: measured, `--report` was mandated by no
# spec requirement and used by nothing (no agent definition, no skill, no script, no call site),
# and it was the common source of a finding CLUSTER across four review rounds.
R13="$(newrepo)"

# (a) THE FLAG IS GONE, and its absence is a USAGE ERROR rather than a silently-ignored argument:
#     a caller still passing it must be told, not obeyed by accident.
rs "$R13" open c --issue 900 --agent spec-auditor --report other.md
rc_is 64 "derived: --report is no longer accepted (unknown argument, exit 64)"
has "unknown argument" "derived: the usage error names it as an unknown argument"
if [ -e "$R13/other.md" ]; then
  bad "derived: the refused run must not create the path the removed flag named"
else
  ok "derived: nothing was created at the path the removed flag named"
fi

# (b) H3 DIRECTLY: the old code ran `mkdir -p "$(dirname "$rpath")"` BEFORE the containment and
#     ignore verification, so this exact invocation created a directory OUTSIDE the checkout and
#     THEN refused. With the flag gone the argument is refused before anything is created.
H3DIR="$T/h3-outside-the-repo"
rm -rf "$H3DIR"
rs "$R13" open c --issue 901 --agent spec-auditor --report "$H3DIR/x.md"
rc_is 64 "derived/H3: an outside-the-repository path cannot even be REQUESTED"
if [ -d "$H3DIR" ]; then
  bad "derived/H3: a directory was created OUTSIDE the checkout ($H3DIR) before the refusal"
else
  ok "derived/H3: no directory was created outside the checkout"
fi

# (c) THE DERIVED PATH IS ANCHORED AT THE REPO ROOT, not at the caller's cwd — so it does not
#     move with the directory the agent happens to be spawned in, and the reader and the writer
#     cannot disagree about which file the stage means.
mkdir -p "$R13/sub/deeper"
OUT="$(cd "$R13/sub/deeper" && bash "$RS" open c --issue 902 --agent spec-auditor 2>&1)"; RC=$?
rc_is 0 "derived: open from a SUBDIRECTORY succeeds"
has "$R13/.review-stage/issue-902/c.md" "derived: the path is anchored at the repo ROOT, not at cwd"
if [ -e "$R13/sub/deeper/.review-stage" ]; then
  bad "derived: a .review-stage/ tree was created relative to the caller's cwd"
else
  ok "derived: nothing was created relative to the caller's cwd"
fi

# (d) H2's READER HALF: the stage record's `report:` field is no longer READ AS A LOCATION. Plant
#     a record naming a DIFFERENT, pre-existing report that records PASS — the exact outcome H2
#     describes — and the verdict must still come from the DERIVED path (the sentinel).
rs "$R13" open c --issue 903 --agent spec-auditor
rc_is 0 "derived/H2: the stage under test opened"
printf 'result: PASS\n\na different report entirely.\n' >"$R13/.review-stage/issue-903/other.md"
SF13="$R13/.review-stage/issue-903/c.stage"
LC_ALL=C sed -e "s|^report: .*|report: $R13/.review-stage/issue-903/other.md|" "$SF13" >"$SF13.new" && mv "$SF13.new" "$SF13"
rs "$R13" verdict c --issue 903
rc_is 5 "derived/H2: a planted 'report:' naming another PASS report does NOT select it"
has "RESULT: NOT-RUN (no report written)" "derived/H2: the verdict comes from the DERIVED path's sentinel"
has "report=$R13/.review-stage/issue-903/c.md" "derived/H2: and the emitted report= names the DERIVED path"
rs "$R13" status c --issue 903
has "state=sentinel-only" "derived/H2: status reads the derived path too, so the two cannot disagree"

# (e) H2's NEWLINE MECHANISM, spelled out: a value SPLIT ACROSS LINES, whose FIRST line is a
#     complete path to another report. `read_field` returns that prefix, so this is precisely the
#     mis-selection a newline-bearing `--report` produced. Nothing reads the field now.
rs "$R13" open c --issue 904 --agent spec-auditor
printf 'result: PASS\n\nanother pre-existing report.\n' >"$R13/.review-stage/issue-904/other.md"
SF14="$R13/.review-stage/issue-904/c.stage"
{
  LC_ALL=C grep -v '^report:' "$SF14"
  printf 'report: %s\n' "$R13/.review-stage/issue-904/other.md"
  printf 'and-the-rest-of-the-filename.md\n'
} >"$SF14.new" && mv "$SF14.new" "$SF14"
rs "$R13" verdict c --issue 904
rc_is 5 "derived/H2: a 'report:' value split across LINES cannot select another report either"
hasnt "RESULT: PASS" "derived/H2: the prefix of a split value is not read as the report location"

# (f) THE WHOLE REMAINING PATH-INPUT SURFACE: <kind>. Conservative on purpose — `.` is refused as
#     well as `/`, because a kind is a FILENAME component and `[A-Za-z0-9][A-Za-z0-9_-]*` covers
#     every kind this pipeline uses (`c`, `rust-review`, `fix`, `coverage`).
for BADKIND in 'c.x' '.c' '-c' 'c d' 'c/x' 'c..' '' ; do
  rs "$R13" open "$BADKIND" --issue 905 --agent spec-auditor
  rc_is 64 "derived/kind: '$BADKIND' is refused as a usage error"
done
rs "$R13" open "$(printf 'c\nd')" --issue 905 --agent spec-auditor
rc_is 64 "derived/kind: a kind carrying a NEWLINE is refused"
rs "$R13" open "$(printf 'c\rd')" --issue 905 --agent spec-auditor
rc_is 64 "derived/kind: a kind carrying a CR is refused"
# POSITIVE CONTROL: the kinds the pipeline actually uses are still accepted, or this narrowing
# would red on correct input.
for OKKIND in c rust-review coverage fix stage_2 A1 ; do
  rs "$R13" open "$OKKIND" --issue 906 --agent spec-auditor
  rc_is 0 "derived/kind CONTROL: '$OKKIND' is accepted"
done

# (g) AND <issue>: decimal digits only, so no separator and no traversal can enter the directory
#     component either.
for BADISSUE in '9 9' '9/9' '9.9' '-9' '9a' '' ; do
  rs "$R13" open c --issue "$BADISSUE" --agent spec-auditor
  rc_is 64 "derived/issue: '$BADISSUE' is refused as a usage error"
done
rs "$R13" open c --issue "$(printf '907\n908')" --agent spec-auditor
rc_is 64 "derived/issue: an issue carrying a NEWLINE is refused"

# --- 14. THE REPORT PATH IS GENERATION-BOUND (round 5, J1) ------------------------
# THE FINDING. `open --force` reset the report to the sentinel and re-stamped `head-sha:`, but
# the report PATH was unchanged — so the PREVIOUS, idle agent could wake up afterwards and write
# its OLD-TREE verdict into that same path, where it was now paired with the NEWLY stamped
# `head-sha:`. A commit nobody audited then passed `premerge-assert.sh`. This is not an exotic
# race: #3751 exists BECAUSE delegated agents go idle and return late, so "the late agent wakes
# up and writes" is the expected behaviour of the population this mechanism serves.
#
# THE FIX IS STRUCTURAL, NOT A CHECK. Every open records a `report-generation:` in the stage
# record and the report path INCLUDES it, so a resumed old agent holds a STALE PATH and cannot
# write into the current generation's report at all. A check could only notice the write
# afterwards, and the harm is the write.
#
# TWO SHAPES, ONE DERIVATION, and the reason the first open keeps the bare name: generation 0 is
# `<kind>.md` and generation N>0 is `<kind>.<N>.md`. They cannot collide because `<kind>` may not
# contain a dot (round 4's narrowing) and a generation is digits only. Keeping generation 0's
# name bare is what makes every quoted `.review-stage/issue-<N>/<kind>.md` example in the agent
# definitions, the skills, CLAUDE.md and both website pages still TRUE for a first open, and what
# keeps a record written before this field existed readable instead of reported as
# `report absent` — a guard that reds on correct input is the guard agents learn to waive.

R14="$(newrepo)"
printf 'seed\n' >"$R14/seed.txt"
git -C "$R14" add seed.txt >/dev/null 2>&1
git -C "$R14" -c user.email=t@example.invalid -c user.name=t commit -q -m A >/dev/null 2>&1
G_A="$(git -C "$R14" rev-parse HEAD 2>/dev/null || true)"
if [ -n "$G_A" ]; then
  ok "generation: the scratch repo has a resolvable HEAD (the head-sha half is measurable)"
else
  bad "generation: could not commit in the scratch repo — the assertions below would be vacuous"
fi

# (a) THE FIRST OPEN KEEPS THE BARE NAME, and the printed path is the OPEN-OK line's path. Pinned
#     so a change to the compatibility rule has to be deliberate: several committed documents
#     quote this exact shape.
rs "$R14" open c --issue 950 --agent spec-auditor
rc_is 0 "generation: the first open succeeds"
G_P0="$(printed_report_path)"
if [ "$G_P0" = "$(REPORT_GEN_OF "$R14" 950 c 0)" ]; then
  ok "generation: generation 0's report keeps the bare <kind>.md name (every quoted doc example stays true)"
else
  bad "generation: the first open printed '$G_P0', expected $(REPORT_GEN_OF "$R14" 950 c 0)"
fi
has "report=$G_P0" "generation: the OPEN-OK line's report= field is the SAME path the clause prints"
has "report-generation=0" "generation: the OPEN-OK line names the generation"
if LC_ALL=C grep -q '^report-generation: 0$' "$R14/.review-stage/issue-950/c.stage" 2>/dev/null; then
  ok "generation: the stage record records the generation, so a reader has ONE source of truth for which report counts"
else
  bad "generation: the stage record does not record report-generation: 0 (record: $(cat "$R14/.review-stage/issue-950/c.stage" 2>/dev/null))"
fi

# (b) THE J1 SCENARIO, END TO END. The stage records a PASS at A; a further commit lands; the
#     stage is re-opened with --force; and THEN the previous agent wakes up and writes its
#     old-tree PASS into the path it was originally given.
printf 'result: PASS\n\naudited at A.\n' >"$G_P0"
rs "$R14" verdict c --issue 950
rc_is 0 "generation: the first-generation audit is readable while it is current"
printf 'more\n' >>"$R14/seed.txt"
git -C "$R14" add seed.txt >/dev/null 2>&1
git -C "$R14" -c user.email=t@example.invalid -c user.name=t commit -q -m B >/dev/null 2>&1
G_B="$(git -C "$R14" rev-parse HEAD 2>/dev/null || true)"
if [ -n "$G_B" ] && [ "$G_B" != "$G_A" ]; then
  ok "generation: a distinct commit B exists"
else
  bad "generation: could not create a distinct commit B (A=$G_A B=$G_B)"
fi
rs "$R14" open c --issue 950 --agent spec-auditor --force
rc_is 0 "generation: the forced re-open succeeds"
G_P1="$(printed_report_path)"
if [ "$G_P1" != "$G_P0" ]; then
  ok "generation: the re-opened stage's report path DIFFERS from the one the idle agent holds"
else
  bad "generation: --force reused the report path ($G_P1) — the resumed agent can still write into the current report"
fi
if [ "$G_P1" = "$(REPORT_GEN_OF "$R14" 950 c 1)" ]; then
  ok "generation: and it is generation 1's path"
else
  bad "generation: the forced re-open printed '$G_P1', expected $(REPORT_GEN_OF "$R14" 950 c 1)"
fi
has "report-generation=1" "generation: the OPEN-OK line names the new generation"
# THE OLD AGENT WAKES UP AND WRITES. This is the whole finding.
printf 'result: PASS\n\naudited at A, reported late.\n' >"$G_P0"
rs "$R14" verdict c --issue 950
rc_is 5 "generation: a resumed agent's write into its OLD path is NOT a verdict for the new tree"
has "NOT-RUN (no report written)" "generation: the current generation reads as the sentinel it is"
hasnt "RESULT: PASS" "generation: no PASS is reported for a tree nobody audited"
has "report=$G_P1" "generation: and the emitted report= names the CURRENT generation"
# NOTHING IS DELETED. Old generations stay on disk as history; the property is that nothing
# READS them, not that they are removed — an audit trail is the point of this whole issue.
if LC_ALL=C grep -q '^result: PASS$' "$G_P0" 2>/dev/null; then
  ok "generation: the previous generation's report is left INTACT on disk as history"
else
  bad "generation: the previous generation's report was destroyed — the audit trail is the point of this issue"
fi
# AND THE RECORD PAIRS THE NEW GENERATION WITH THE NEW COMMIT, in ONE atomic write.
if LC_ALL=C grep -q "^head-sha: $G_B\$" "$R14/.review-stage/issue-950/c.stage" 2>/dev/null; then
  ok "generation: the record re-stamped head-sha to B beside the new generation (one atomic pair)"
else
  bad "generation: the record does not name B (record: $(cat "$R14/.review-stage/issue-950/c.stage" 2>/dev/null))"
fi

# (c) POSITIVE CONTROL: the RE-SPAWNED agent, writing into the path the clause printed, reaches a
#     PASS. Without this the section would pass on a script that had simply broken --force, and a
#     guard that reds on correct input is the guard agents learn to waive.
printf 'result: PASS\n\nre-audited at B.\n' >"$G_P1"
rs "$R14" verdict c --issue 950
rc_is 0 "generation CONTROL: the fresh agent's report at the printed path IS the verdict"
has "report=$G_P1" "generation CONTROL: read from the current generation's path"

# (d) MONOTONIC ACROSS A SECOND RE-OPEN, and generation 1's report is left alone too.
rs "$R14" open c --issue 950 --agent spec-auditor --force
rc_is 0 "generation: a second forced re-open succeeds"
G_P2="$(printed_report_path)"
if [ "$G_P2" = "$(REPORT_GEN_OF "$R14" 950 c 2)" ]; then
  ok "generation: the generation ADVANCES (2), so no two opens of one stage ever share a path"
else
  bad "generation: the second re-open printed '$G_P2', expected $(REPORT_GEN_OF "$R14" 950 c 2)"
fi
if LC_ALL=C grep -q '^result: PASS$' "$G_P1" 2>/dev/null; then
  ok "generation: generation 1's report is history too, not clobbered"
else
  bad "generation: generation 1's report was destroyed by the next re-open"
fi
rs "$R14" verdict c --issue 950
rc_is 5 "generation: and the new generation starts as a non-verdict, whatever the older ones say"

# (e) THE ADVANCE BELT: a monotonic counter read from the record cannot help when the RECORD is
#     gone and the REPORT is not — the count would restart at 0 and hand a new agent the path an
#     old one still holds. So the generation also ADVANCES PAST any generation whose report file
#     already exists.
rs "$R14" open c --issue 951 --agent spec-auditor
rc_is 0 "generation/belt: a stage opens at generation 0"
G_B0="$(printed_report_path)"
printf 'result: PASS\n\naudited by the agent that is still running.\n' >"$G_B0"
rm -f "$R14/.review-stage/issue-951/c.stage"
rs "$R14" open c --issue 951 --agent spec-auditor
rc_is 0 "generation/belt: with the record gone, a fresh open succeeds"
G_B1="$(printed_report_path)"
if [ "$G_B1" != "$G_B0" ]; then
  ok "generation/belt: it ADVANCES past the surviving report rather than reusing its path"
else
  bad "generation/belt: the fresh open reused $G_B0, which an earlier agent still holds"
fi
if LC_ALL=C grep -q '^result: PASS$' "$G_B0" 2>/dev/null; then
  ok "generation/belt: and the surviving report is untouched"
else
  bad "generation/belt: the surviving report was clobbered"
fi
rs "$R14" verdict c --issue 951
rc_is 5 "generation/belt: the re-opened stage reads its OWN generation, not the survivor's PASS"

# (f) A RECORD WHOSE GENERATION CANNOT BE READ NAMES ITS OWN CAUSE AND FABRICATES NO PATH. The
#     field decides WHICH ARTIFACT COUNTS, so "cannot tell" may not take the permissive branch by
#     falling back to generation 0 — that is how a stale generation-0 PASS would be read as the
#     current verdict.
rs "$R14" open c --issue 952 --agent spec-auditor
G_SF="$R14/.review-stage/issue-952/c.stage"
printf 'result: PASS\n\nstale, from generation 0.\n' >"$(REPORT_GEN_OF "$R14" 952 c 0)"
LC_ALL=C sed -e 's|^report-generation: .*|report-generation: nope|' "$G_SF" >"$G_SF.new" && mv "$G_SF.new" "$G_SF"
rs "$R14" verdict c --issue 952
rc_is 5 "generation/defect: an unreadable report-generation is a NON-VERDICT"
has "stage record unreadable" "generation/defect: and it names the STAGE RECORD, not the report (a different operator action)"
hasnt "RESULT: PASS" "generation/defect: the generation-0 report is NOT read as the current verdict"
has "report=unresolved" "generation/defect: no path is fabricated on the line that is otherwise the authority"
rs "$R14" status c --issue 952
has "state=stage-record-unreadable" "generation/defect: status gives it its own state, per the one-state-per-cause rule"
rs "$R14" record-author-performed c --issue 952 \
  --reason 'no peer auditor was available on this box' --evidence docs/development/review-stage-reporting.md --performed-by author
rc_is 2 "generation/defect: record-author-performed REFUSES rather than write to a guessed path"
# NAMED, so this case cannot pass on the NEIGHBOURING refusal: a generation-0 report holding a
# recorded PASS would refuse as `verdict-already-recorded` whether or not the record is readable.
has "AUTHOR-REFUSED reason=stage-record-unreadable" "generation/defect: naming the record defect, not the neighbouring already-recorded refusal"

# (g) SEVERAL generation lines is AMBIGUOUS, refused by the COUNT and not resolved by order —
#     the same rule the `result:` reader follows, for the same reason.
rs "$R14" open c --issue 953 --agent spec-auditor
G_SF2="$R14/.review-stage/issue-953/c.stage"
printf 'result: PASS\n\nstale, from generation 0.\n' >"$(REPORT_GEN_OF "$R14" 953 c 0)"
printf 'report-generation: 7\n' >>"$G_SF2"
rs "$R14" verdict c --issue 953
rc_is 5 "generation/defect: TWO report-generation lines is a NON-VERDICT"
has "stage record unreadable" "generation/defect: named as a record defect"
hasnt "RESULT: PASS" "generation/defect: and the first line does not win"

# (h) A RECORD WRITTEN BEFORE THE FIELD EXISTED still reads its report. Every prior version wrote
#     exactly ONE report, at `<kind>.md`, so ABSENT is an affirmative measurement of that shape —
#     not a "cannot tell". Reading generation 0 there is the TRUE answer, and reporting
#     `report absent` instead would red on correct input.
rs "$R14" open c --issue 954 --agent spec-auditor
G_SF3="$R14/.review-stage/issue-954/c.stage"
printf 'result: PASS\n\naudited by the previous version of this tool.\n' >"$(REPORT_GEN_OF "$R14" 954 c 0)"
LC_ALL=C grep -v '^report-generation:' "$G_SF3" >"$G_SF3.new" && mv "$G_SF3.new" "$G_SF3"
rs "$R14" verdict c --issue 954
rc_is 0 "generation/legacy: a record with no report-generation reads generation 0's report"
has "report=$(REPORT_GEN_OF "$R14" 954 c 0)" "generation/legacy: and names the bare path that version wrote"

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
#
# ROUND 3 ADDED 24 HOST-INDEPENDENT ASSERTIONS (261 -> 285): section 4d's 12 (exactly one
# column-zero `result:` record, both orders, both distinct causes, and the indented-copies
# control) and section 11e's 12 (the temporary file's unpredictability plus the structural asserts
# of the O_EXCL create-and-open mechanism). Neither section branches on the host — 4d needs only
# git and bash like every other case, and 11e's `mktemp -u`/`set -C` requirements are the SUBJECT
# of the assertions rather than a precondition for running them, so a box lacking either FAILS the
# case rather than displacing it. The EXACT floor therefore still holds by the two shapes recorded
# above, and it moves to the new measured count.
#
# ROUND 4 ADDED 25 HOST-INDEPENDENT ASSERTIONS (285 -> 310) in section 11f (the stage record is
# the publication marker: the write ORDER, the forbidden new-sha/stale-verdict pairing at every
# write boundary, the interrupted state, and the uninterrupted positive control). EVERY assertion
# in it is UNCONDITIONAL — each `if` calls exactly one of `ok`/`bad`, and its extra requirements
# (git commits in the scratch repo, `awk` for the instrumented copy) are the SUBJECT of asserted
# preconditions rather than a precondition for running: a box that cannot commit, or an `awk`
# that produces no instrumented copy, FAILS the case rather than displacing it. So the EXACT
# floor still holds by the two shapes recorded above.
#
# ROUND 4's SECOND ITEM MOVED THE COUNT AGAIN, IN BOTH DIRECTIONS (310 -> 350). `--report` was
# REMOVED, so the cases that could only be reached through it were REPLACED rather than deleted:
# section 5(a) now reaches the report-half ignore refusal through a repository `.gitignore` that
# covers one file and not the other; 5(c)'s outside-the-repository case is GONE (unreachable by
# construction — section 13(b) pins the reachable half, that the removed flag is a usage error and
# creates nothing outside the checkout); 11(g)/(h) and 11e's temp-name oracle use an
# extension-ignoring repository instead of a custom path; and 11c's `report=` injection vector is
# replaced by the vectors that REMAIN (the report-supplied cause, and the flag values a caller
# passes), verified to still RED when the emit boundary is removed. Section 13 then adds the
# derived-path and strict kind/issue cases. Every assertion added is unconditional, so the EXACT
# floor still holds by the two shapes recorded above.
#
# ROUND 4's THIRD ITEM (H4) ADDS 22 (350 -> 372): section 7b maps all SEVEN reachable NOT-RUN
# causes to their own `state=`, asserts the ABSENCE of the state each was confused with, and adds
# the DERIVED drift guard (the built-in cause literals are extracted from the shipped script, so a
# new cause adds an assertion here rather than needing this list edited — which can only RAISE the
# count, while a broken extraction is caught by its own >= 5 floor). One case is host-conditional
# and takes the FIRST shape the two rules above allow: the mode-000 unreadable case emits the SAME
# NUMBER of assertions whether or not this user can read a mode-000 file (root can), so the count
# does not move.
#
# ROUND 5 (J1) MOVES IT TO 419, IN BOTH DIRECTIONS AGAIN. Section 14 adds 40 assertions (the
# report path is GENERATION-BOUND: the first open's bare name, the J1 scenario end to end, the
# re-spawn positive control, monotonicity, the existence belt, the two record-defect shapes, and
# the legacy-record control), and section 11f gains 7 (the previous generation's report survives as
# history and is not read; the interrupted re-open leaves the record's head-sha AND generation
# paired; the completed re-open prints a fresh generation) — 372 -> 419. Section 11f's own count
# rose rather than fell: nothing was deleted, three assertions were RE-EXPRESSED against the
# generation the re-open publishes (the probe now watches that file, because reading the previous
# generation would measure a file no reader consults) and one changed its expected outcome, which
# is recorded in place with the reasoning. Every added assertion is UNCONDITIONAL — section 14's
# extra requirements (git commits in the scratch repo) are the SUBJECT of an asserted precondition
# rather than a precondition for running, exactly as 11f's are — so the EXACT floor still holds by
# the two shapes recorded above.
#
# ROUND 5's SECOND ITEM (J3) ADDS 10 (419 -> 429): section 11c gains the control-character census
# over the verdict line and the STATUS-NOTE (ESC, BEL, backspace, VT and DEL all planted, asserted
# as a BYTE COUNT over the whole output rather than as "does it contain ESC" — a one-byte check
# would pass on a one-byte fix, which is how the original gap survived) plus the CONTROL that
# ordinary punctuation and a non-ASCII em dash still pass through readable. Both are
# host-independent: they need only bash, git and `tr`, and `tr` is a subject of the assertions
# rather than a precondition for running them.
ASSERT_FLOOR=429
EXECUTED=$((PASS + FAIL))
if [ "$EXECUTED" -lt "$ASSERT_FLOOR" ]; then
  bad "CASE FLOOR: only $EXECUTED assertions executed, below the committed floor of $ASSERT_FLOOR — a section died silently, and 'failed: 0' over a shrunken suite is not a pass"
fi

printf '\n=== review-stage: %d passed, %d failed (executed %d, floor %d) ===\n' \
  "$PASS" "$FAIL" "$((PASS + FAIL))" "$ASSERT_FLOOR"
[ "$FAIL" -eq 0 ]
