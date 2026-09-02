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

# REPORT_OF <repo> <issue> <kind> — THE REPORT THE STAGE RECORD CURRENTLY NAMES, derived the way
# the tool derives it (round 6, K2). The report path carries a per-open NONCE, so it is NOT
# predictable and NOTHING may reconstruct it from a shape: this helper reads `report-nonce:` out of
# the stage record, exactly as `load_stage` does. With no record, or a record with no nonce, it
# yields the LEGACY bare `<kind>.md` — which is the path a never-opened stage reports and the path
# every pre-nonce record's single report lives at.
REPORT_OF() {
  local d="$1/.review-stage/issue-$2" n
  n="$(LC_ALL=C sed -n 's/^report-nonce:[[:space:]]*//p' "$d/$3.stage" 2>/dev/null | LC_ALL=C head -1 || true)"
  if [ -n "$n" ]; then printf '%s/%s.%s.md\n' "$d" "$3" "$n"; else printf '%s/%s.md\n' "$d" "$3"; fi
}
# LEGACY_REPORT_OF <repo> <issue> <kind> — the BARE `<kind>.md`. Read but never written by this
# version: it is what a pre-nonce record names and what a never-opened stage reports.
LEGACY_REPORT_OF() { printf '%s/.review-stage/issue-%s/%s.md\n' "$1" "$2" "$3"; }
# NONCE_PATH_OK <path> <dir> <kind> — the path IS `<dir>/<kind>.<alnum>.md`. Used where the
# nonce is UNKNOWABLE to the test (a refusal writes no record, so there is nothing to read it
# from), which is the point: the shape is asserted, the value is not predicted.
NONCE_PATH_OK() {
  local q="$1" dir="$2" kind="$3" mid
  case "$q" in "$dir/$kind."*.md) ;; *) return 1 ;; esac
  mid="${q#"$dir/$kind."}"; mid="${mid%.md}"
  case "$mid" in "" | *[!A-Za-z0-9]* ) return 1 ;; esac
  return 0
}
# RECORD_NONCE <repo> <issue> <kind> — the record's nonce token, or empty.
RECORD_NONCE() {
  LC_ALL=C sed -n 's/^report-nonce:[[:space:]]*//p' "$1/.review-stage/issue-$2/$3.stage" 2>/dev/null \
    | LC_ALL=C head -1 || true
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
REFUSED_PATH="$(printf '%s\n' "$OUT" | LC_ALL=C sed -n 's/.*[ ]path=\([^ ]*\).*/\1/p' | LC_ALL=C head -1)"
if NONCE_PATH_OK "$REFUSED_PATH" "$R3/.review-stage/issue-200" c; then
  ok "check-ignore: the refusal names the DERIVED path it was about to write"
else
  bad "check-ignore: the refusal named '$REFUSED_PATH', which is not \$repo/.review-stage/issue-200/c.<nonce>.md"
fi
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
has "not 'author'" "hand-written AP: the refusal names the closed performer set"
# AND `peer` IS OUT OF THAT SET SINCE ROUND 6 (K3). It was ACCEPTED and then reported under the
# token `AUTHOR-PERFORMED`, i.e. a PEER audit — the more independent of the two — was stated to be
# the diff AUTHOR's. A verdict that misstates WHO audited is a false statement in the one line a
# human reads, and the affordance bought nothing: a peer who can perform the audit can write the
# report of record and produce a genuine `PASS`. So the token set is not widened; `peer` is gone.
hand_ap "$AP_R" peer 'a peer on this box audited the spec deltas by hand' 'docs/x.md'
rs "$R7" verdict c --issue 500
rc_is 5 "hand-written AP: performed-by: peer is refused by the CLASSIFIER too — never reported as an AUTHOR audit"
hasnt "RESULT: AUTHOR-PERFORMED" "hand-written AP: a peer-performed report does NOT reach the merge-proceeding token"
has "not 'author'" "hand-written AP: and the refusal names the one-value performer set"

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
has "--performed-by author is required" "author-refusals: names the missing performer"

rs "$R8" record-author-performed c --issue 600 --reason "$GOOD_REASON" --evidence "$GOOD_EV" --performed-by nobody
rc_is 64 "author-refusals: an out-of-set --performed-by is a usage error"
has "must be exactly 'author'" "author-refusals: names the closed performer set"

# `peer` IS REFUSED BY NAME (round 6, K3), not merely absent from a list. It used to be ACCEPTED
# and then reported under the token `AUTHOR-PERFORMED`, so a peer audit was stated to be the diff
# AUTHOR's — a false statement in the one line a human reads. The token set is deliberately NOT
# widened with a `PEER-PERFORMED`: the grammar is enumerated in `premerge-assert.sh`, CLAUDE.md,
# `docs/development/review-stage-reporting.md`, six agent definitions, two skills, the OpenSpec
# delta and both website pages, and a token nobody needs is a maintenance tax at every one of
# them. `record-author-performed` exists for the case where NO independent audit can be obtained;
# a peer who CAN audit writes the report of record and produces a genuine `PASS`.
rs "$R8" record-author-performed c --issue 600 --reason "$GOOD_REASON" --evidence "$GOOD_EV" --performed-by peer
rc_is 64 "author-refusals: --performed-by peer is REFUSED — this tool records an AUTHOR audit and nothing else"
has "must be exactly 'author'" "author-refusals: the refusal names the one-value set"
has "report of record" "author-refusals: and points at the PRIMARY path, which is what a peer should use"
if [ -f "$(REPORT_OF "$R8" 600 c)" ] && LC_ALL=C grep -q '^result: NOT-RUN' "$(REPORT_OF "$R8" 600 c)" 2>/dev/null; then
  ok "author-refusals: the refused peer recording wrote NOTHING — the report is still the sentinel"
else
  bad "author-refusals: the refused peer recording changed the report ($(LC_ALL=C grep -m1 '^result:' "$(REPORT_OF "$R8" 600 c)" 2>/dev/null))"
fi

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

# (a) A LINK PLANTED AT THE GUESSABLE REPORT NAME IS NOW INERT, WHICH IS STRONGER THAN REFUSED
#     (round 6, K2). This case used to plant a symlink at `.review-stage/issue-<N>/c.md` and
#     assert `open` REFUSED it. Since the report name carries a per-open NONCE there IS no
#     guessable report name: `open` writes `c.<nonce>.md` and never touches the planted link, so
#     the refusal is UNREACHABLE FROM HERE and the case asserts the property that replaced it —
#     the victim is untouched because the write was never AT it. The report-half leaf refusal
#     itself is still real code and still REACHED, by case (e) below (`record-author-performed`
#     writes the path the RECORD names, which IS knowable), and the report path's PARENT
#     components are covered by (c) and (d). This is the same disposition round 4 gave section
#     5(c): a case whose state became unreachable by construction is REPLACED, never weakened.
R10="$(newrepo)"
printf 'the original tracked content\n' >"$R10/victim.md"
git -C "$R10" add victim.md >/dev/null 2>&1
mkdir -p "$R10/.review-stage/issue-800"
ln -s "$R10/victim.md" "$R10/.review-stage/issue-800/c.md"
rs "$R10" open c --issue 800 --agent spec-auditor
rc_is 0 "symlink: a link at the OLD guessable report name does not even take part in the open"
R10_RP="$(printed_report_path)"
if NONCE_PATH_OK "$R10_RP" "$R10/.review-stage/issue-800" c && [ -f "$R10_RP" ] && [ ! -L "$R10_RP" ]; then
  ok "symlink: the report went to a nonce-named REGULAR file, not to the planted name"
else
  bad "symlink: the open wrote '$R10_RP', which is not a nonce-named regular file"
fi
if [ -L "$R10/.review-stage/issue-800/c.md" ]; then
  ok "symlink: the planted link is still a link — nothing wrote through it or replaced it"
else
  bad "symlink: the planted link was written through or replaced"
fi
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
if [ -n "$(ls -A "$R16/.review-stage/issue-806" 2>/dev/null)" ]; then
  bad "tempfile: the refusal must not write anything in the stage directory ($(ls -A "$R16/.review-stage/issue-806" 2>/dev/null))"
else
  ok "tempfile: nothing was written at the refused path"
fi
# And the DIRECTORY-ignored form of the same thing is ACCEPTED — the refusal above is about the
# PATTERN, not about writing at all, and without this control the case above could pass on a
# script that refused every write.
R17="$(newrepo '.review-stage/')"
rs "$R17" open c --issue 807 --agent spec-auditor
rc_is 0 "tempfile control: a DIRECTORY-ignored .review-stage/ is accepted"
CTRL807="$(REPORT_OF "$R17" 807 c)"
if [ -f "$CTRL807" ] && [ ! -L "$CTRL807" ]; then
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
    print "    _pr_nonce=\"$( (LC_ALL=C grep -m1 \"^report-nonce:\" \"${PROBE_SFILE:-/nonexistent}\" 2>/dev/null || true) | LC_ALL=C sed -e \"s/^report-nonce:[[:space:]]*//\" )\""
    print "    _pr_rp=\"${PROBE_RPREFIX:-/nonexistent}.md\""
    print "    if [ -n \"$_pr_nonce\" ]; then _pr_rp=\"${PROBE_RPREFIX:-/nonexistent}.$_pr_nonce.md\"; fi"
    print "    _pr_tok=\"$( (LC_ALL=C grep -m1 \"^result:\" \"$_pr_rp\" 2>/dev/null || printf \"result: none\\n\") | LC_ALL=C sed -e \"s/^result:[[:space:]]*//\" -e \"s/[[:space:]].*$//\" )\""
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

# rsp <repo> <probe-log> <sfile> <report-prefix> <kill:0|1> <args...> — the instrumented run.
#
# THE PROBE READS THE REPORT THE RECORD NAMES, NOT A PATH THE TEST PREDICTED (round 6, K2). The
# report name carries a per-open NONCE, so no caller can know it in advance — and that is the
# FAITHFUL observable anyway: the pair `premerge-assert.sh` acts on is (this record's `head-sha:`,
# the verdict of the report THIS RECORD names), so the probe derives the second from the first
# exactly as `load_stage` does. The fourth argument is therefore the `<dir>/<kind>` PREFIX, and the
# probe appends `.<nonce>.md` (or `.md` for a pre-nonce record). One consequence is recorded in
# case (b) below: at the FIRST write boundary the record is still the OLD one, so the token
# observed there is the OLD report's — which is TRUTHFUL, because the head-sha beside it is the
# old one too.
rsp() {
  local repo="$1" log="$2" sf="$3" rp="$4" kill="$5"; shift 5
  : >"$log"
  if [ "$kill" = "1" ]; then
    OUT="$(cd "$repo" && PROBE_OUT="$log" PROBE_SFILE="$sf" PROBE_RPREFIX="$rp" PROBE_KILL=1 bash "$RS_PROBE" "$@" 2>&1)"
  else
    OUT="$(cd "$repo" && PROBE_OUT="$log" PROBE_SFILE="$sf" PROBE_RPREFIX="$rp" bash "$RS_PROBE" "$@" 2>&1)"
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
RP_A="$R11F/.review-stage/issue-900/c"
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
# THE PROBE WATCHES THE REPORT THE RECORD NAMES AT EACH BOUNDARY (round 5 J1, round 6 K2) — the
# pair a reader would act on, derived rather than predicted. The previous agent's report is
# captured BEFORE the re-open, because after it the record names a different file.
RP_B_OLD="$(REPORT_OF "$R11F" 901 c)"
RPFX_B="$R11F/.review-stage/issue-901/c"
LOG_B="$T/probe-b.log"
rsp "$R11F" "$LOG_B" "$SF_B" "$RPFX_B" 0 open c --issue 901 --agent spec-auditor --force
rc_is 0 "marker-order: the forced re-open succeeds"
RP_B="$(REPORT_OF "$R11F" 901 c)"
if [ -f "$RP_B" ] && [ "$RP_B" != "$RP_B_OLD" ]; then
  ok "marker-order: the re-open published a NEW report and the record names it"
else
  bad "marker-order: the re-open's report ('$RP_B') is missing or is the previous one ('$RP_B_OLD')"
fi
FORBIDDEN="$(LC_ALL=C grep -c "record-head=$SHA_B report-token=PASS" "$LOG_B" 2>/dev/null || true)"
if [ "$FORBIDDEN" = "0" ]; then
  ok "marker-order: at NO write boundary is the NEW head-sha paired with the OLD PASS report (the H1 defect)"
else
  bad "marker-order: $FORBIDDEN boundary/boundaries paired the new head-sha with the stale PASS (log: $(cat "$LOG_B" 2>/dev/null))"
fi
PB1="$(nth_probe "$LOG_B" 1)"
# WHAT THE FIRST BOUNDARY SHOWS, AND WHY IT IS THE TRUTHFUL PAIR (the observable moved in round 6,
# the property did not). The RECORD is the publication marker and it has not been written yet, so
# at this boundary the record still names the OLD commit AND the OLD report — a CONSISTENT pair,
# describing the audit that really happened. Round 5's version of this assertion measured the
# NEW file directly (it could, because the path was a predictable generation) and read NOT-RUN;
# with the path nonce-bound, predicting it is exactly what a reader cannot do, so the probe asks
# the record. The forbidden pair (new head-sha + PASS) is asserted above and is what H1 is about.
case "$PB1" in
  *"report-token=PASS"*) ok "marker-order: at the FIRST boundary the record still names the OLD report, so its PASS is what a reader would see" ;;
  *) bad "marker-order: the first boundary did not read the record-named report (got: $PB1)" ;;
esac
case "$PB1" in
  *"record-head=$SHA_A"*) ok "marker-order: and it names the OLD commit, so the pair a reader acts on is CONSISTENT, not a false certification" ;;
  *) bad "marker-order: the record was already re-stamped before the report was published (got: $PB1)" ;;
esac
PB2="$(nth_probe "$LOG_B" 2)"
case "$PB2" in
  *"record-head=$SHA_B report-token=NOT-RUN"*) ok "marker-order: at the SECOND boundary the new commit is paired with the new report's SENTINEL, in ONE atomic record write" ;;
  *) bad "marker-order: the second boundary is not (new head-sha, sentinel) (got: $PB2)" ;;
esac
# AND THE PREVIOUS REPORT'S PASS IS STILL ON DISK, UNREAD (round 5, J1). H1 used to CLOBBER it
# with the sentinel, which was the only thing keeping the stale verdict out of the reader's way;
# now the reader simply does not look there. Both properties are asserted, because the ORDER still
# has to hold for the report the re-open publishes.
if LC_ALL=C grep -q '^result: PASS$' "$RP_B_OLD" 2>/dev/null; then
  ok "marker-order: the superseded report's PASS survives as history"
else
  bad "marker-order: the superseded report was clobbered instead of superseded"
fi
rs "$R11F" verdict c --issue 901
rc_is 5 "marker-order: and the re-opened stage reads the report its RECORD names, so the stale PASS is not a verdict"

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
RPFX_C="$R11F/.review-stage/issue-902/c"
LOG_C="$T/probe-c.log"
rsp "$R11F" "$LOG_C" "$SF_C" "$RPFX_C" 1 open c --issue 902 --agent spec-auditor --force
rc_is 90 "marker-order: the simulated kill fired at the first write"
NPC="$(LC_ALL=C grep -c . "$LOG_C" 2>/dev/null || true)"
if [ "$NPC" = "1" ]; then
  ok "marker-order: exactly ONE write completed before the kill"
else
  bad "marker-order: $NPC write(s) completed before the kill, expected 1"
fi
# THE ORPHANED REPORT IS FOUND, NOT PREDICTED (round 6, K2). The record was never written, so
# NOTHING names the report the killed open had published — which is the whole point: an
# unpublished report is a file no reader can reach. The test locates it as the report in this
# stage directory that the record does NOT name.
RP_C_NEW=""
for CAND in "$R11F/.review-stage/issue-902/"c.*.md; do
  [ -f "$CAND" ] || continue
  [ "$CAND" != "$RP_C" ] || continue
  RP_C_NEW="$CAND"
done
SENT="$(LC_ALL=C grep -c '^result: NOT-RUN' "$RP_C_NEW" 2>/dev/null || true)"
if [ -n "$RP_C_NEW" ] && [ "$SENT" = "1" ]; then
  ok "marker-order: the interrupted state leaves the unpublished report's SENTINEL on disk"
else
  bad "marker-order: the unpublished report is missing or is not the sentinel after the kill (path='$RP_C_NEW')"
fi
# WHAT THE INTERRUPTED STATE IS, AND WHY IT IS STILL THE FAIL-CLOSED ONE (round 5, J1 changes the
# OBSERVABLE here, not the property). The RECORD is the publication marker and it was not written,
# so the stage is EXACTLY what it was before the re-open: the SAME report, opened at B, recording
# the audit that was really made at B. That pair is TRUTHFUL — and it is published ATOMICALLY,
# because `head-sha:` and `report-nonce:` are two fields of ONE record write, so no partial
# state can pair a new commit with an older report's verdict at all. Before the nonce, the
# same interruption left the ONE report clobbered to the sentinel; that was fail-closed too, but it
# DESTROYED the audit it had, and the H1 pairing was kept out only by the write order.
DISKHEAD="$(LC_ALL=C sed -n 's/^head-sha:[[:space:]]*//p' "$SF_C" 2>/dev/null | LC_ALL=C head -1 || true)"
if [ "$DISKHEAD" = "$SHA_B" ]; then
  ok "marker-order: the record still names the commit the audit was actually made at, so the merge point refuses on the sha too"
else
  bad "marker-order: the record's head-sha is '$DISKHEAD', expected the pre-kill '$SHA_B'"
fi
DISKNONCE="$(RECORD_NONCE "$R11F" 902 c)"
if [ -n "$DISKNONCE" ] && [ "$RP_C" = "$R11F/.review-stage/issue-902/c.$DISKNONCE.md" ]; then
  ok "marker-order: and it still names the report that head-sha was stamped WITH — the pair is atomic, so it cannot be split by an interruption"
else
  bad "marker-order: the record's report-nonce is '$DISKNONCE', which does not name the pre-kill report '$RP_C'"
fi
rs "$R11F" verdict c --issue 902
rc_is 0 "marker-order: the interrupted re-open published NOTHING, so the stage is still the previous report"
has "report=$RP_C" "marker-order: the verdict is read from the report the RECORD names, not the half-published one"
hasnt "report=$RP_C_NEW" "marker-order: the unpublished report's sentinel is not read"

# (d) POSITIVE CONTROL: the UNINTERRUPTED forced re-open leaves a USABLE stage — otherwise (c)
#     could pass on a script that broke --force altogether, and a guard that reds on correct
#     input is the guard agents learn to waive.
rs "$R11F" open c --issue 902 --agent spec-auditor --force
rc_is 0 "marker-order CONTROL: a complete forced re-open succeeds"
RP_C_LIVE="$(printed_report_path)"
if [ -n "$RP_C_LIVE" ] && [ "$RP_C_LIVE" != "$RP_C" ] && [ "$RP_C_LIVE" != "$RP_C_NEW" ]; then
  ok "marker-order CONTROL: the completed re-open published a FRESH report and printed its path"
else
  bad "marker-order CONTROL: the re-open printed '$RP_C_LIVE', which is not a fresh report"
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
has "$(REPORT_OF "$R13" 902 c)" "derived: the path is anchored at the repo ROOT, not at cwd"
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
has "report=$(REPORT_OF "$R13" 903 c)" "derived/H2: and the emitted report= names the DERIVED path"
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

# --- 14. THE REPORT PATH IS NONCE-BOUND (round 5 J1, round 6 K2) ------------------
# THE J1 FINDING. `open --force` reset the report to the sentinel and re-stamped `head-sha:`, but
# the report PATH was unchanged — so the PREVIOUS, idle agent could wake up afterwards and write
# its OLD-TREE verdict into that same path, where it was now paired with the NEWLY stamped
# `head-sha:`. A commit nobody audited then passed `premerge-assert.sh`. This is not an exotic
# race: #3751 exists BECAUSE delegated agents go idle and return late, so "the late agent wakes
# up and writes" is the expected behaviour of the population this mechanism serves.
#
# THE FIX IS STRUCTURAL, NOT A CHECK. Every open records a `report-nonce:` in the stage record and
# the report path INCLUDES it, so a resumed old agent holds a STALE PATH and cannot write into the
# current report at all. A check could only notice the write afterwards, and the harm is the write.
#
# ROUND 6 (K2) CHANGED THE VALUE, NOT THE PROPERTY: the path component was a SCANNED generation
# number and is now a GENERATED nonce, because a value chosen by looking at what is on disk can be
# chosen twice (section 16). So the shapes here are `<kind>.<nonce>.md` for every open this version
# performs and the bare `<kind>.md` for a LEGACY record only — which is why (a) below asserts the
# SHAPE and reads the VALUE from the record, exactly as a reader must.

R14="$(newrepo)"
printf 'seed\n' >"$R14/seed.txt"
git -C "$R14" add seed.txt >/dev/null 2>&1
git -C "$R14" -c user.email=t@example.invalid -c user.name=t commit -q -m A >/dev/null 2>&1
G_A="$(git -C "$R14" rev-parse HEAD 2>/dev/null || true)"
if [ -n "$G_A" ]; then
  ok "nonce: the scratch repo has a resolvable HEAD (the head-sha half is measurable)"
else
  bad "nonce: could not commit in the scratch repo — the assertions below would be vacuous"
fi

# (a) EVERY OPEN — INCLUDING THE FIRST — CARRIES A NONCE, and the record is the one place that
#     names it. The bare `<kind>.md` is READ but never WRITTEN by this version (case (h)), so a
#     reader that reconstructed the path from a shape rather than from the record would be reading
#     a file nobody wrote.
rs "$R14" open c --issue 950 --agent spec-auditor
rc_is 0 "nonce: the first open succeeds"
G_P0="$(printed_report_path)"
if NONCE_PATH_OK "$G_P0" "$R14/.review-stage/issue-950" c; then
  ok "nonce: the first open's report is <kind>.<nonce>.md — an UNPREDICTABLE name, not the bare one"
else
  bad "nonce: the first open printed '$G_P0', which is not \$dir/c.<nonce>.md"
fi
if [ "$G_P0" != "$(LEGACY_REPORT_OF "$R14" 950 c)" ]; then
  ok "nonce: and it is NOT the legacy bare name (which this version never writes)"
else
  bad "nonce: the first open wrote the legacy bare name, so a re-open could collide with it"
fi
has "report=$G_P0" "nonce: the OPEN-OK line's report= field is the SAME path the clause prints"
has "report-nonce=$(RECORD_NONCE "$R14" 950 c)" "nonce: the OPEN-OK line names the nonce the record carries"
if [ "$G_P0" = "$(REPORT_OF "$R14" 950 c)" ]; then
  ok "nonce: the stage record NAMES this report, so a reader has ONE source of truth for which report counts"
else
  bad "nonce: the record names '$(REPORT_OF "$R14" 950 c)', not the printed '$G_P0' (record: $(cat "$R14/.review-stage/issue-950/c.stage" 2>/dev/null))"
fi

# (b) THE J1 SCENARIO, END TO END. The stage records a PASS at A; a further commit lands; the
#     stage is re-opened with --force; and THEN the previous agent wakes up and writes its
#     old-tree PASS into the path it was originally given.
printf 'result: PASS\n\naudited at A.\n' >"$G_P0"
rs "$R14" verdict c --issue 950
rc_is 0 "nonce: the first report is readable while it is current"
printf 'more\n' >>"$R14/seed.txt"
git -C "$R14" add seed.txt >/dev/null 2>&1
git -C "$R14" -c user.email=t@example.invalid -c user.name=t commit -q -m B >/dev/null 2>&1
G_B="$(git -C "$R14" rev-parse HEAD 2>/dev/null || true)"
if [ -n "$G_B" ] && [ "$G_B" != "$G_A" ]; then
  ok "nonce: a distinct commit B exists"
else
  bad "nonce: could not create a distinct commit B (A=$G_A B=$G_B)"
fi
rs "$R14" open c --issue 950 --agent spec-auditor --force
rc_is 0 "nonce: the forced re-open succeeds"
G_P1="$(printed_report_path)"
if [ "$G_P1" != "$G_P0" ]; then
  ok "nonce: the re-opened stage's report path DIFFERS from the one the idle agent holds"
else
  bad "nonce: --force reused the report path ($G_P1) — the resumed agent can still write into the current report"
fi
if NONCE_PATH_OK "$G_P1" "$R14/.review-stage/issue-950" c; then
  ok "nonce: and it is a fresh nonce-named report"
else
  bad "nonce: the forced re-open printed '$G_P1', which is not \$dir/c.<nonce>.md"
fi
has "report-nonce=$(RECORD_NONCE "$R14" 950 c)" "nonce: the OPEN-OK line names the NEW nonce, and the record carries it"
# THE OLD AGENT WAKES UP AND WRITES. This is the whole finding.
printf 'result: PASS\n\naudited at A, reported late.\n' >"$G_P0"
rs "$R14" verdict c --issue 950
rc_is 5 "nonce: a resumed agent's write into its OLD path is NOT a verdict for the new tree"
has "NOT-RUN (no report written)" "nonce: the current report reads as the sentinel it is"
hasnt "RESULT: PASS" "nonce: no PASS is reported for a tree nobody audited"
has "report=$G_P1" "nonce: and the emitted report= names the CURRENT report"
# NOTHING IS DELETED. Superseded reports stay on disk as history; the property is that nothing
# READS them, not that they are removed — an audit trail is the point of this whole issue.
if LC_ALL=C grep -q '^result: PASS$' "$G_P0" 2>/dev/null; then
  ok "nonce: the superseded report is left INTACT on disk as history"
else
  bad "nonce: the superseded report was destroyed — the audit trail is the point of this issue"
fi
# AND THE RECORD PAIRS THE NEW REPORT WITH THE NEW COMMIT, in ONE atomic write.
if LC_ALL=C grep -q "^head-sha: $G_B\$" "$R14/.review-stage/issue-950/c.stage" 2>/dev/null; then
  ok "nonce: the record re-stamped head-sha to B beside the new nonce (one atomic pair)"
else
  bad "nonce: the record does not name B (record: $(cat "$R14/.review-stage/issue-950/c.stage" 2>/dev/null))"
fi

# (c) POSITIVE CONTROL: the RE-SPAWNED agent, writing into the path the clause printed, reaches a
#     PASS. Without this the section would pass on a script that had simply broken --force, and a
#     guard that reds on correct input is the guard agents learn to waive.
printf 'result: PASS\n\nre-audited at B.\n' >"$G_P1"
rs "$R14" verdict c --issue 950
rc_is 0 "nonce CONTROL: the fresh agent's report at the printed path IS the verdict"
has "report=$G_P1" "nonce CONTROL: read from the current report's path"

# (d) A SECOND RE-OPEN IS A THIRD DISTINCT PATH, and the previous two are left alone. "Distinct",
#     not "one greater": nothing counts any more (section 16).
rs "$R14" open c --issue 950 --agent spec-auditor --force
rc_is 0 "nonce: a second forced re-open succeeds"
G_P2="$(printed_report_path)"
if [ -n "$G_P2" ] && [ "$G_P2" != "$G_P1" ] && [ "$G_P2" != "$G_P0" ]; then
  ok "nonce: the path is DISTINCT from both earlier ones, so no two opens of one stage share a path"
else
  bad "nonce: the second re-open printed '$G_P2', which collides with an earlier report"
fi
if LC_ALL=C grep -q '^result: PASS$' "$G_P1" 2>/dev/null; then
  ok "nonce: the previous report is history too, not clobbered"
else
  bad "nonce: the previous report was destroyed by the next re-open"
fi
rs "$R14" verdict c --issue 950
rc_is 5 "nonce: and the new report starts as a non-verdict, whatever the older ones say"

# (e) THE PROPERTY THE DELETED EXISTENCE BELT WAS FOR, NOW STRUCTURAL (round 6, K2). Round 5's
#     counter had to be defended with a scan: delete the RECORD while a REPORT survives and the
#     count restarted at 0, handing a new agent the path an old, still-running agent holds. The
#     scan is GONE and the property is stronger, because a generated value cannot land on an
#     existing name whether or not anything looked.
rs "$R14" open c --issue 951 --agent spec-auditor
rc_is 0 "nonce/orphan: a stage opens"
G_B0="$(printed_report_path)"
printf 'result: PASS\n\naudited by the agent that is still running.\n' >"$G_B0"
rm -f "$R14/.review-stage/issue-951/c.stage"
rs "$R14" open c --issue 951 --agent spec-auditor
rc_is 0 "nonce/orphan: with the record gone, a fresh open succeeds"
G_B1="$(printed_report_path)"
if [ "$G_B1" != "$G_B0" ]; then
  ok "nonce/orphan: it picks a path the surviving report does not occupy — with no probe of what exists"
else
  bad "nonce/orphan: the fresh open reused $G_B0, which an earlier agent still holds"
fi
if LC_ALL=C grep -q '^result: PASS$' "$G_B0" 2>/dev/null; then
  ok "nonce/orphan: and the surviving report is untouched"
else
  bad "nonce/orphan: the surviving report was clobbered"
fi
rs "$R14" verdict c --issue 951
rc_is 5 "nonce/orphan: the re-opened stage reads its OWN report, not the survivor's PASS"

# (f) A RECORD WHOSE NONCE CANNOT BE READ NAMES ITS OWN CAUSE AND FABRICATES NO PATH. The field
#     decides WHICH ARTIFACT COUNTS, so "cannot tell" may not take the permissive branch by
#     falling back to the bare name — that is how a stale report's PASS would be read as the
#     current verdict.
rs "$R14" open c --issue 952 --agent spec-auditor
G_SF="$R14/.review-stage/issue-952/c.stage"
printf 'result: PASS\n\nstale, at the legacy name.\n' >"$(LEGACY_REPORT_OF "$R14" 952 c)"
LC_ALL=C sed -e 's|^report-nonce: .*|report-nonce: ../../nope|' "$G_SF" >"$G_SF.new" && mv "$G_SF.new" "$G_SF"
rs "$R14" verdict c --issue 952
rc_is 5 "nonce/defect: an unreadable report-nonce is a NON-VERDICT"
has "stage record unreadable" "nonce/defect: and it names the STAGE RECORD, not the report (a different operator action)"
hasnt "RESULT: PASS" "nonce/defect: the legacy-named report is NOT read as the current verdict"
has "report=unresolved" "nonce/defect: no path is fabricated on the line that is otherwise the authority"
rs "$R14" status c --issue 952
has "state=stage-record-unreadable" "nonce/defect: status gives it its own state, per the one-state-per-cause rule"
rs "$R14" record-author-performed c --issue 952 \
  --reason 'no peer auditor was available on this box' --evidence docs/development/review-stage-reporting.md --performed-by author
rc_is 2 "nonce/defect: record-author-performed REFUSES rather than write to a guessed path"
# NAMED, so this case cannot pass on the NEIGHBOURING refusal: a legacy-named report holding a
# recorded PASS would refuse as `verdict-already-recorded` whether or not the record is readable.
has "AUTHOR-REFUSED reason=stage-record-unreadable" "nonce/defect: naming the record defect, not the neighbouring already-recorded refusal"
# AND A TOO-SHORT TOKEN IS REFUSED TOO: the value is alphanumeric, so charset alone would accept
# it, and a one-character "nonce" is a value a second open could plausibly land on.
rs "$R14" open c --issue 955 --agent spec-auditor
G_SF5="$R14/.review-stage/issue-955/c.stage"
LC_ALL=C sed -e 's|^report-nonce: .*|report-nonce: a|' "$G_SF5" >"$G_SF5.new" && mv "$G_SF5.new" "$G_SF5"
rs "$R14" verdict c --issue 955
rc_is 5 "nonce/defect: an alphanumeric but too-SHORT token is a NON-VERDICT too (charset is not the whole rule)"
has "stage record unreadable" "nonce/defect: named as a record defect"

# (g) SEVERAL nonce lines is AMBIGUOUS, refused by the COUNT and not resolved by order —
#     the same rule the `result:` reader follows, for the same reason.
rs "$R14" open c --issue 953 --agent spec-auditor
G_SF2="$R14/.review-stage/issue-953/c.stage"
G_P953="$(printed_report_path)"
printf 'result: PASS\n\na real audit, at the report the record named.\n' >"$G_P953"
printf 'report-nonce: aaaaaaaaaa\n' >>"$G_SF2"
rs "$R14" verdict c --issue 953
rc_is 5 "nonce/defect: TWO report-nonce lines is a NON-VERDICT"
has "stage record unreadable" "nonce/defect: named as a record defect"
hasnt "RESULT: PASS" "nonce/defect: and the first line does not win"

# (h) A RECORD WRITTEN BEFORE THE FIELD EXISTED still reads its report. Every prior version wrote
#     exactly ONE report, at `<kind>.md`, so ABSENT is an affirmative measurement of that shape —
#     not a "cannot tell". Reading the bare name there is the TRUE answer, and reporting
#     `report absent` instead would red on correct input.
rs "$R14" open c --issue 954 --agent spec-auditor
G_SF3="$R14/.review-stage/issue-954/c.stage"
printf 'result: PASS\n\naudited by the previous version of this tool.\n' >"$(LEGACY_REPORT_OF "$R14" 954 c)"
LC_ALL=C grep -v '^report-nonce:' "$G_SF3" >"$G_SF3.new" && mv "$G_SF3.new" "$G_SF3"
rs "$R14" verdict c --issue 954
rc_is 0 "nonce/legacy: a record with no report-nonce reads the bare report that version wrote"
has "report=$(LEGACY_REPORT_OF "$R14" 954 c)" "nonce/legacy: and names the bare path that version wrote"

# --- 15. AN UNREADABLE STAGE RECORD IS NOT "no nonce field" (round 6, K1) ----------
# THE FINDING. The field naming this stage's report was counted with
#   n="$(grep -c ... "$sfile" 2>/dev/null || true)"; case "$n" in ""|*[!0-9]*) n=0
# and `grep` uses its EXIT STATUS to separate the two facts this reader depends on: 1 means "the
# file was READ and holds no such line", >=2 means "the file could NOT BE READ". `|| true` threw
# that away, so an unreadable record was INDISTINGUISHABLE from a record with no such field
# and took the LEGACY reading — the bare `<kind>.md` — so an OLD report recording
# PASS was reported as the current verdict while which report is current was UNKNOWN. That is the
# shape this repo names repeatedly: the unmeasured state inheriting the permissive branch.
#
# *read failed* and *read fine, field absent* are different facts, and only the SECOND one is
# legitimately permissive (every earlier version of this tool wrote exactly one report, at the
# bare name, so ABSENT is an affirmative measurement of that shape — see section 14(h)). A read
# FAILURE is the existing `stage record unreadable` non-verdict, with no path derived.
R15K="$(newrepo)"
rs "$R15K" open c --issue 960 --agent spec-auditor
rc_is 0 "record-read: the stage opened"
# THE STALE PASS GOES AT THE LEGACY BARE PATH — the file a "no nonce field" reading consults.
# It is the artifact whose PASS must not be reported while the record cannot be read.
K1_RP="$(printed_report_path)"
K1_LEGACY="$(LEGACY_REPORT_OF "$R15K" 960 c)"
printf 'result: PASS\n\naudited long ago, at a tree nobody can now name.\n' >"$K1_LEGACY"
K1_SF="$R15K/.review-stage/issue-960/c.stage"
if [ -f "$K1_SF" ] && [ -f "$K1_LEGACY" ]; then
  ok "record-read: the record and a stale legacy report both exist (the assertions below have a subject)"
else
  bad "record-read: missing precondition (record=$K1_SF legacy=$K1_LEGACY) — the assertions below would be vacuous"
fi
chmod 000 "$K1_SF" 2>/dev/null || true
# MODE 000 IS NOT EFFECTIVE FOR ROOT, so the case asserts the mapping only where the read really
# is refused, and says which branch it took; BOTH branches emit the SAME NUMBER of assertions, so
# the exact case floor does not move (the shape section 7b(4) already uses).
if ( : <"$K1_SF" ) 2>/dev/null; then
  ok "record-read: SKIPPED the unreadable-record assertions — this user can read a mode-000 file (root); nothing is claimed about a state that was not reached"
  ok "record-read: (fixed assertion count, 2 of 7)"
  ok "record-read: (fixed assertion count, 3 of 7)"
  ok "record-read: (fixed assertion count, 4 of 7)"
  ok "record-read: (fixed assertion count, 5 of 7)"
  ok "record-read: (fixed assertion count, 6 of 7)"
  ok "record-read: (fixed assertion count, 7 of 7)"
else
  rs "$R15K" verdict c --issue 960
  rc_is 5 "record-read: an UNREADABLE record is a NON-VERDICT, not the legacy reading"
  has "stage record unreadable" "record-read: named as a RECORD defect (the operator action is chmod, not 'your agent wrote a bad line')"
  hasnt "RESULT: PASS" "record-read: the stale legacy report is NOT reported as the current verdict"
  has "report=unresolved" "record-read: and no path is fabricated on the line that is otherwise the authority"
  rs "$R15K" status c --issue 960
  has "state=stage-record-unreadable" "record-read: status gives it the record-defect state, per the one-state-per-cause rule"
  # AND THE WRITE SIDE REFUSES TOO: `open` read the same count with the same `|| true`, so it
  # would have treated an unreadable record as generation 0 and handed a re-spawned agent the
  # path an earlier agent may still hold.
  rs "$R15K" open c --issue 960 --agent spec-auditor --force
  rc_is 2 "record-read: open REFUSES on a record it cannot read rather than guessing the legacy reading"
  has "reason=stage-record-unreadable" "record-read: and the refusal names the record, by its own reason"
fi
chmod 644 "$K1_SF" 2>/dev/null || true
# CONTROL: readable again, and the stage reads its verdict — so the case cannot pass on a tool
# that simply refuses everything. The PASS is written to the path `open` PRINTED, which is the
# file a caller was handed.
printf 'result: PASS\n\nre-audited.\n' >"$K1_RP"
rs "$R15K" verdict c --issue 960
rc_is 0 "record-read CONTROL: a READABLE record reads its report and reports the verdict"
has "RESULT: PASS" "record-read CONTROL: the recorded PASS is reported once the record can be read"

# --- 16. THE REPORT PATH IS GENERATED, NEVER SELECTED (round 6, K2) ----------------
# THE FINDING. Round 5 chose the report's generation by SCANNING the stage directory for an unused
# `<kind>.<gen>.md` — a bounded walk with its own exhaustion refusal. A value chosen by looking at
# what is already on disk is a value TWO CONCURRENT CALLERS CAN BOTH CHOOSE: two `open --force`
# runs read the same record, probe the same directory BEFORE either has written, pick the same
# generation and hand ONE report path to TWO agents — so the superseded agent's write lands on the
# current report and replaces FINDINGS with PASS.
#
# THE FIX IS SUBTRACTION: the counter, the scan, the 4096-attempt bound and the exhaustion refusal
# are DELETED and the path component is a GENERATED nonce. Nothing is selected, so nothing races —
# and a lock would have been the worse answer, serialising a race a nonce removes while adding a
# mechanism (a stale lock file, a box without `flock`, a holder killed mid-open) to a script whose
# whole subject is not taking the permissive branch when something cannot be measured.

# (a) THE RACE, SIMULATED DETERMINISTICALLY. This is a SIMULATION OF THE INTERLEAVING, not a real
#     concurrent run: the two opens are sequential, and the stage directory is RESTORED to the
#     state both of them observed between them, because in the interleaving being modelled B's
#     read (and, under the scanned counter, B's existence probe) happens BEFORE either agent
#     writes. No timing is involved, so the case cannot flake — and it reds on the scanned counter
#     every time (measured: both calls printed `c.1.md`, and A's `result: FINDINGS` was replaced by
#     B's `result: PASS`).
R16="$(newrepo)"
printf 'seed\n' >"$R16/seed.txt"
git -C "$R16" add seed.txt >/dev/null 2>&1
git -C "$R16" -c user.email=t@example.invalid -c user.name=t commit -q -m A >/dev/null 2>&1
rs "$R16" open c --issue 970 --agent spec-auditor
rc_is 0 "race: the stage both concurrent calls will re-open is open"
K2_SD="$R16/.review-stage/issue-970"
K2_SNAP="$T/k2-snap-$$"
rm -rf "$K2_SNAP"
if cp -a "$K2_SD" "$K2_SNAP" 2>/dev/null; then
  ok "race: the state both calls observe was snapshotted (the simulation has a subject)"
else
  bad "race: could not snapshot the stage directory — the assertions below would be vacuous"
fi
rs "$R16" open c --issue 970 --agent spec-auditor --force
rc_is 0 "race: call A's forced re-open succeeds"
K2_PA="$(printed_report_path)"
# B ran CONCURRENTLY: it observed the pre-A state, so that is the state it is given.
rm -rf "$K2_SD"; cp -a "$K2_SNAP" "$K2_SD"
rs "$R16" open c --issue 970 --agent spec-auditor --force
rc_is 0 "race: call B's forced re-open succeeds"
K2_PB="$(printed_report_path)"
if [ -n "$K2_PA" ] && [ "$K2_PA" != "$K2_PB" ]; then
  ok "race: the two calls were handed DIFFERENT report paths, though both observed the same prior state"
else
  bad "race: both calls were handed '$K2_PA' — one report path for two agents, so either agent can overwrite the other's verdict"
fi
# NOW BOTH AGENTS REPORT. A found a blocking finding; B found nothing.
printf 'result: FINDINGS\n\nagent A: one blocking finding.\n' >"$K2_PA"
printf 'result: PASS\n\nagent B: nothing found.\n' >"$K2_PB"
if LC_ALL=C grep -q '^result: FINDINGS$' "$K2_PA" 2>/dev/null; then
  ok "race: A's FINDINGS is still A's FINDINGS — the superseded agent cannot overwrite it"
else
  bad "race: A's report now reads '$(LC_ALL=C grep -m1 '^result:' "$K2_PA" 2>/dev/null)' — a FINDINGS was replaced by a concurrent open's PASS"
fi
# AND THE PUBLISHED VERDICT IS THE ONE THE RECORD NAMES — exactly one of the two, never a mixture.
rs "$R16" verdict c --issue 970
if [ "$(REPORT_OF "$R16" 970 c)" = "$K2_PB" ]; then
  ok "race: the record names B's report (the LAST record write published), so the verdict is unambiguous"
else
  bad "race: the record names '$(REPORT_OF "$R16" 970 c)', which is neither call's published report"
fi
has "report=$K2_PB" "race: and the emitted report= is that one"

# (b) NOTHING IS SELECTED — asserted STRUCTURALLY over the shipped script, because the point of
#     this item is a DELETION and a behavioural case cannot see that a mechanism is gone. Three
#     literals from the removed machinery must be absent, and the generator must contain no
#     filesystem existence probe (which is what made the old value collidable). This half PINS THE
#     DELETION; case (a) is the behavioural guard, because a reintroduced counter under new names
#     would slip past a literal scan and still red (a).
for GONE in 'gen_attempts' 'report-generation' '-exhausted'; do
  if LC_ALL=C grep -q -- "$GONE" "$RS"; then
    bad "race/structural: the shipped script still contains '$GONE' — the selection machinery was not removed"
  else
    ok "race/structural: '$GONE' is gone from the shipped script"
  fi
done
GENBODY="$(LC_ALL=C sed -n '/^new_report_nonce() {$/,/^}$/p' "$RS")"
if [ -n "$GENBODY" ]; then
  ok "race/structural: the nonce generator was located in the shipped script"
else
  bad "race/structural: could not locate new_report_nonce() — the assertion below would be vacuous"
fi
case "$GENBODY" in
  *'[ -f '* | *'[ -e '* | *'[ -L '* | *' ls '*)
    bad "race/structural: the nonce generator probes the filesystem — a value derived from what exists is a value two callers can both choose" ;;
  *) ok "race/structural: the nonce generator makes no filesystem probe, so its value cannot be a function of what exists" ;;
esac

# (c) THE GENERATOR IS ACTUALLY UNIQUE ACROSS REAL OPENS. The structural case says nothing is
#     selected; this one measures that what IS produced does not repeat. Twelve opens of twelve
#     stages, through the shipped script, must yield twelve distinct valid tokens.
K2_TOKENS="$T/k2-tokens.txt"
: >"$K2_TOKENS"
K2_N=0
while [ "$K2_N" -lt 12 ]; do
  K2_N=$((K2_N + 1))
  rs "$R16" open c --issue "98$K2_N" --agent spec-auditor >/dev/null 2>&1
  RECORD_NONCE "$R16" "98$K2_N" c >>"$K2_TOKENS"
done
K2_TOTAL="$(LC_ALL=C grep -c . "$K2_TOKENS" || true)"
K2_UNIQ="$(LC_ALL=C sort -u "$K2_TOKENS" | LC_ALL=C grep -c . || true)"
if [ "$K2_TOTAL" = "12" ]; then
  ok "race/unique: all twelve opens recorded a nonce"
else
  bad "race/unique: only $K2_TOTAL of 12 opens recorded a nonce"
fi
if [ "$K2_UNIQ" = "$K2_TOTAL" ]; then
  ok "race/unique: all $K2_TOTAL tokens are DISTINCT"
else
  bad "race/unique: $K2_TOTAL tokens collapsed to $K2_UNIQ distinct values"
fi
K2_BAD=0
while IFS= read -r TOKV; do
  [ -n "$TOKV" ] || continue
  case "$TOKV" in
    *[!A-Za-z0-9]* ) K2_BAD=$((K2_BAD + 1)) ;;
    *) [ "${#TOKV}" -ge 6 ] || K2_BAD=$((K2_BAD + 1)) ;;
  esac
done <"$K2_TOKENS"
if [ "$K2_BAD" = "0" ]; then
  ok "race/unique: every token is alphanumeric and at least 6 characters (the shape report_path requires)"
else
  bad "race/unique: $K2_BAD token(s) are not a valid nonce shape"
fi

# (d) NO FALLBACK GENERATOR. The nonce comes from `mktemp -u`; a box that cannot produce one is
#     REFUSED, not given a predictable substitute (a pid, a timestamp, a counter) — which is
#     exactly the collidable value this replaces. Reached by SUBSTITUTING THE ARTIFACT the script
#     calls (a `mktemp` earlier on PATH), never a test-only seam: a seam is one more thing a real
#     invoker can set.
K2_BIN="$T/k2-fakebin"
mkdir -p "$K2_BIN"
printf '#!/bin/sh\nexit 1\n' >"$K2_BIN/mktemp"
chmod +x "$K2_BIN/mktemp"
OUT="$(cd "$R16" && PATH="$K2_BIN:$PATH" bash "$RS" open c --issue 990 --agent spec-auditor 2>&1)"; RC=$?
rc_is 2 "race/no-fallback: an open that cannot generate a nonce is REFUSED"
has "reason=report-nonce-not-generated" "race/no-fallback: the refusal names the cause"
has "no fallback to a predictable token" "race/no-fallback: and says why there is no substitute"
if [ -z "$(ls -A "$R16/.review-stage/issue-990" 2>/dev/null | LC_ALL=C grep -v '^c\.stage$' || true)" ]; then
  ok "race/no-fallback: no report was written at any path"
else
  bad "race/no-fallback: the refused open wrote $(ls -A "$R16/.review-stage/issue-990" 2>/dev/null)"
fi
# CONTROL: with mktemp back, the same open succeeds — so the case above is about the generator and
# not about a script that refuses everything.
rs "$R16" open c --issue 991 --agent spec-auditor
rc_is 0 "race/no-fallback CONTROL: with a working mktemp the same open succeeds"

# --- 17. EVERY DATA VALUE ON AN EMITTED LINE GOES THROUGH THE ONE BOUNDARY (round 7, L1) ---
# THE FINDING, one directory over from where round 5 left it. `read_field` routes every value it
# reads out of the stage record through `one_line`, which neutralises control characters — and
# deliberately does NOT map the ONE reserved character of these `key=value` lines, '='. That is
# `field_value`'s job, and three fields never called it: `deadline=`, `agent=` (verdict AND status)
# and `spawned-at=` (status). So a record whose `agent:` reads `spec-auditor deadline=0` put a
# SECOND `deadline=` pair on the verdict line, AHEAD of the measured one, for any consumer that
# scans field by field — which is exactly what `premerge-assert.sh`'s field census does.
#
# It is the same class as round 2's S1 (the cause) and round 5's J3 (the control bytes), found for
# the third time at a new site, which is why round 7 also lands a STRUCTURAL guard
# (`scripts/tests/lib/emit-boundary-scan.sh`, exercised in section 18) instead of only the fix.
#
# THE READ SIDE IS THE SIDE THAT HAS TO BE STRONG: `open` validates `--agent` through
# `sanitize_field` and `--deadline-secs` as digits, so this shape cannot be WRITTEN by this tool.
# It arrives from a HAND-EDITED record — and reading hand-written records is what this tool does.
R17="$(newrepo)"
rs "$R17" open c --issue 980 --agent spec-auditor
rc_is 0 "boundary/record: the stage opened"
L1_RP="$(printed_report_path)"
L1_SF="$R17/.review-stage/issue-980/c.stage"
printf 'result: PASS\n' >"$L1_RP" 2>/dev/null || true
# The three fields are rewritten IN THE RECORD, each carrying a `key=value` pair of its own. The
# planted pairs name fields a consumer reads (`deadline=`, `agent=`, `elapsed=`), because the harm
# is a SECOND answer to a question the line already answers.
if [ -f "$L1_SF" ] && [ -f "$L1_RP" ]; then
  LC_ALL=C sed -e 's|^agent:.*|agent: spec-auditor deadline=0|' \
    -e 's|^deadline-secs:.*|deadline-secs: 1800 agent=forged|' \
    -e 's|^spawned-at:.*|spawned-at: 2026-01-01T00:00:00Z elapsed=999|' "$L1_SF" >"$L1_SF.new" &&
    mv "$L1_SF.new" "$L1_SF"
  ok "boundary/record: the record and its report exist and were re-written (the assertions below have a subject)"
else
  bad "boundary/record: missing precondition (record=$L1_SF report=$L1_RP) — the assertions below would be vacuous"
fi
# COUNTED, NOT MATCHED. `deadline=` appearing twice is the defect; asserting the line "contains
# deadline=1800" would pass on the broken script too, because it does.
FIELD_COUNT() { printf '%s' "$1" | LC_ALL=C tr ' ' '\n' | LC_ALL=C grep -c "^$2" || true; }
rs "$R17" verdict c --issue 980
rc_is 0 "boundary/record: the verdict is still PASS — a display boundary decides nothing"
for f in deadline= agent=; do
  n="$(FIELD_COUNT "$OUT" "$f")"
  if [ "$n" = "1" ]; then
    ok "boundary/record: the verdict line carries EXACTLY ONE '$f' pair"
  else
    bad "boundary/record: the verdict line carries $n '$f' pairs — a record value forged a field (got: $OUT)"
  fi
done
# The '=' is mapped to '~' rather than dropped, and the surrounding text is untouched, so the
# audit trail still shows what the record actually said. (Note the SPACE survives: `field_value`
# preserves prose, and a space is not a reserved character of this grammar — it merely stops the
# smuggled text being read as a key.)
has "deadline=1800 agent~forged" "boundary/record: the smuggled '=' is rendered as '~' — neutralised, not dropped, so the audit trail still shows what the record said"
rs "$R17" status c --issue 980
rc_is 0 "boundary/record: status still reports"
has "past-deadline=unknown" "boundary/record: an unmeasurable deadline yields past-deadline=unknown, never a permissive 'no' from a comparison that never ran"
hasnt "integer expression expected" "boundary/record: and no raw bash diagnostic escapes into the REVIEW-STAGE: block"
for f in deadline= agent= elapsed= spawned-at=; do
  n="$(FIELD_COUNT "$OUT" "$f")"
  if [ "$n" = "1" ]; then
    ok "boundary/record: the STATUS line carries EXACTLY ONE '$f' pair"
  else
    bad "boundary/record: the STATUS line carries $n '$f' pairs — a record value forged a field (got: $OUT)"
  fi
done
# THE CONTROL, without which every assertion above is satisfiable by a script that dropped the
# fields altogether: an ORDINARY record still reports its own agent and deadline verbatim.
R17B="$(newrepo)"
rs "$R17B" open c --issue 981 --agent spec-auditor --deadline-secs 1234
rc_is 0 "boundary/record CONTROL: an ordinary stage opened"
printf 'result: PASS\n' >"$(printed_report_path)" 2>/dev/null || true
rs "$R17B" verdict c --issue 981
has "deadline=1234 agent=spec-auditor" "boundary/record CONTROL: an ordinary record's values pass through the boundary UNCHANGED"

# --- 19. THE RENAME MUST REPLACE THE EXACT DESTINATION NAME (round 7, L2) --------
# THE FINDING. `commit_write` ended in `mv -f "$WRITE_TMP" "$dest"`, and a plain `mv` does NOT
# promise to replace the NAME `dest`: if `dest` is — or BECOMES — a directory, or a symlink to one,
# `mv` puts the temporary file INSIDE it and EXITS 0. The write then lands outside the path this
# script verified while the tool reports success. `mv -T` (`--no-target-directory`) makes that an
# error, and `rename(2)` does not follow a symlink for the destination, so `-T` closes the LEAF.
#
# THE NARROWEST TRUE CLAIM ABOUT THIS COVERAGE, stated rather than implied. `-T` is DEFENCE IN
# DEPTH for a TOCTOU WINDOW: the pre-existing checks (`assert_no_symlink`, and the
# `path-not-a-regular-file` refusal) already refuse a destination that is a directory or a symlink
# AT THE TIME THEY RUN, so a PRE-PLANTED one never reaches `mv` at all, and the window in which it
# could — between those checks and the rename — is not inducible from outside this process without
# a timing race, which would be a flaky test. So the coverage here is deliberately of three
# different kinds, and none of them pretends to be the fourth:
#   (a) the END-TO-END OUTCOME for both plants — REFUSE, and nothing lands inside the plant. Which
#       LAYER refuses is named in the assertions, so the case does not claim to exercise `mv -T`.
#   (b) the HOST PROPERTY `-T` relies on, MEASURED on this box rather than assumed.
#   (c) a STRUCTURAL pin that `commit_write` uses `-T` and that no un-`-T`'d `mv` survives in the
#       script — which is the only expressible assertion about the window itself.
# This is a LEAF-level property and stays inside the boundary of the escalated J2 residual (the
# parent-DIRECTORY-component substitution): nothing here asserts anything about the parents.

# (a) DESTINATION PRE-REPLACED BY A DIRECTORY. The subject is the REPORT half, reached through
#     `record-author-performed`, because that is the write whose destination the RECORD names.
R19="$(newrepo)"
rs "$R19" open c --issue 990 --agent spec-auditor
rc_is 0 "rename: the stage opened"
L2_RP="$(printed_report_path)"
rm -f "$L2_RP" 2>/dev/null || true
mkdir -p "$L2_RP" 2>/dev/null || true
if [ -d "$L2_RP" ]; then
  ok "rename: a DIRECTORY now stands at the report's exact destination name (the case has a subject)"
else
  bad "rename: could not plant a directory at $L2_RP — the assertions below would be vacuous"
fi
rs "$R19" record-author-performed c --issue 990 --reason no-independent-auditor-available \
  --evidence docs/round-artifacts/issue-3751-l2.md --performed-by author
rc_is 2 "rename: the write REFUSES rather than landing somewhere else"
has "AUTHOR-REFUSED reason=path-not-a-regular-file" "rename: and it refuses by NAME — today at the pre-rename check, which is the layer that sees a PRE-planted directory (mv -T covers the window AFTER it)"
L2_INSIDE="$(find "$L2_RP" -type f 2>/dev/null | LC_ALL=C wc -l | LC_ALL=C tr -d ' ')"
if [ "$L2_INSIDE" = "0" ]; then
  ok "rename: NOTHING was written INSIDE the planted directory"
else
  bad "rename: $L2_INSIDE file(s) landed inside the planted directory — the write escaped the verified path"
fi

# (b) DESTINATION PRE-REPLACED BY A SYMLINK TO A DIRECTORY.
R19B="$(newrepo)"
rs "$R19B" open c --issue 991 --agent spec-auditor
rc_is 0 "rename/symlink: the stage opened"
L2_RP2="$(printed_report_path)"
mkdir -p "$R19B/elsewhere" 2>/dev/null || true
rm -f "$L2_RP2" 2>/dev/null || true
ln -s "$R19B/elsewhere" "$L2_RP2" 2>/dev/null || true
if [ -L "$L2_RP2" ] && [ -d "$L2_RP2" ]; then
  ok "rename/symlink: a symlink TO A DIRECTORY now stands at the report's destination name"
else
  bad "rename/symlink: could not plant a symlink-to-directory at $L2_RP2 — the assertions below would be vacuous"
fi
rs "$R19B" record-author-performed c --issue 991 --reason no-independent-auditor-available \
  --evidence docs/round-artifacts/issue-3751-l2.md --performed-by author
rc_is 2 "rename/symlink: the write REFUSES"
has "AUTHOR-REFUSED reason=path-is-symlink" "rename/symlink: and by NAME, at the symlink check"
L2_INSIDE2="$(find "$R19B/elsewhere" -type f 2>/dev/null | LC_ALL=C wc -l | LC_ALL=C tr -d ' ')"
if [ "$L2_INSIDE2" = "0" ]; then
  ok "rename/symlink: NOTHING was written through the link"
else
  bad "rename/symlink: $L2_INSIDE2 file(s) landed through the link — the write escaped the verified path"
fi

# (c) THE CONTROL. An ordinary replacement still succeeds, and it REPLACES rather than appending or
#     leaving a stale file beside it — without this every assertion above is satisfiable by a script
#     that refuses every write.
R19C="$(newrepo)"
rs "$R19C" open c --issue 992 --agent spec-auditor
rc_is 0 "rename CONTROL: the stage opened"
L2_RP3="$(printed_report_path)"
if [ -f "$L2_RP3" ] && LC_ALL=C grep -q '^result: NOT-RUN' "$L2_RP3"; then
  ok "rename CONTROL: the sentinel report was written at the exact destination name (a regular file, not a directory)"
else
  bad "rename CONTROL: the sentinel was not written at $L2_RP3"
fi
rs "$R19C" record-author-performed c --issue 992 --reason no-independent-auditor-available \
  --evidence docs/round-artifacts/issue-3751-l2.md --performed-by author
rc_is 0 "rename CONTROL: an ordinary recording succeeds"
if [ -f "$L2_RP3" ] && LC_ALL=C grep -q '^result: AUTHOR-PERFORMED' "$L2_RP3" &&
  ! LC_ALL=C grep -q '^result: NOT-RUN' "$L2_RP3"; then
  ok "rename CONTROL: the destination was REPLACED atomically — the new record is there and no line of the old one survives"
else
  bad "rename CONTROL: the destination was not cleanly replaced (content: $(LC_ALL=C head -3 "$L2_RP3" 2>/dev/null))"
fi
L2_LEFTOVER="$(find "$(dirname "$L2_RP3")" -name '.rs-*' -o -name '*.tmp*' 2>/dev/null | LC_ALL=C wc -l | LC_ALL=C tr -d ' ')"
if [ "$L2_LEFTOVER" = "0" ]; then
  ok "rename CONTROL: and no temporary file was left behind"
else
  bad "rename CONTROL: $L2_LEFTOVER temporary file(s) left behind"
fi

# (d) THE HOST PROPERTY `-T` RELIES ON, MEASURED HERE rather than assumed — because the whole fix
#     rests on it, and a claim about a tool's behaviour is exactly the kind that decays silently.
L2_D="$T/mvt"; mkdir -p "$L2_D/dir" 2>/dev/null || true
printf 'payload\n' >"$L2_D/src" 2>/dev/null || true
if mv -f -T "$L2_D/src" "$L2_D/dir" 2>/dev/null; then
  bad "rename/host: 'mv -f -T <file> <dir>' SUCCEEDED on this host — -T does not have the property commit_write relies on"
else
  ok "rename/host: 'mv -f -T <file> <dir>' FAILS on this host, which is the property commit_write relies on"
fi
if [ -f "$L2_D/src" ] && [ ! -f "$L2_D/dir/src" ]; then
  ok "rename/host: and it leaves the source in place rather than moving it INSIDE the directory"
else
  bad "rename/host: -T moved the file into the directory (or lost it) — the fix's premise does not hold here"
fi

# (f) A HOST WITHOUT `-T` GETS A NAMED REFUSAL, NOT A SILENT FALLBACK. Simulated with a
#     PATH-shadowed `mv` that rejects the option exactly as a BSD/macOS `mv` does — the ARTIFACT is
#     substituted, not a settable seam in the script. This is the assertion behind "REQUIRED, not
#     attempted": the write must fail, name the missing option, and write NOTHING.
L2_BIN="$T/l2bin"; mkdir -p "$L2_BIN"
cat >"$L2_BIN/mv" <<'L2SHIM'
#!/usr/bin/env bash
# A `mv` with no -T, the way a stock BSD/macOS one behaves: the option parse fails and nothing moves.
for a in "$@"; do
  case "$a" in
    -T | --no-target-directory) printf 'mv: illegal option -- T\n' >&2; exit 64 ;;
    -*T*) printf 'mv: illegal option -- T\n' >&2; exit 64 ;;
  esac
done
exec /bin/mv "$@"
L2SHIM
chmod +x "$L2_BIN/mv" 2>/dev/null || true
R19D="$(newrepo)"
if [ -x "$L2_BIN/mv" ] && ! PATH="$L2_BIN:$PATH" mv -T /dev/null /dev/null 2>/dev/null; then
  ok "rename/no-T: the shimmed mv rejects -T (the simulated host is real, not assumed)"
else
  bad "rename/no-T: the shim did not take effect — the assertions below would be vacuous"
fi
OUT="$(cd "$R19D" && PATH="$L2_BIN:$PATH" bash "$RS" open c --issue 993 --agent spec-auditor 2>&1)"; RC=$?
rc_is 2 "rename/no-T: a host whose mv has no -T REFUSES the write"
has "reason=write-failed" "rename/no-T: under the existing write-failed marker"
has "NO -T / --no-target-directory" "rename/no-T: and the detail NAMES the missing option, so the operator is not sent to look at permissions"
if [ ! -e "$R19D/.review-stage/issue-993/c.stage" ]; then
  ok "rename/no-T: and NOTHING was recorded — no fallback to a plain mv"
else
  bad "rename/no-T: a stage record was written on a host whose mv has no -T"
fi

# (e) STRUCTURAL: the only expressible assertion about the TOCTOU window itself.
if LC_ALL=C grep -q 'mv -f -T "\$WRITE_TMP" "\$dest"' "$RS"; then
  ok "rename/structural: commit_write renames with 'mv -f -T'"
else
  bad "rename/structural: commit_write no longer uses 'mv -f -T' — the L2 property is gone"
fi
# No `mv` anywhere in the script may lack -T. Counted over EXECUTABLE lines (comments quote the
# option by name, and a comment is not a call), because a silent fallback is the one outcome L2
# forbids.
L2_BAREMV="$(LC_ALL=C grep -n '^[^#]*[^A-Za-z_-]mv ' "$RS" | LC_ALL=C grep -vc -- '-T' || true)"
if [ "$L2_BAREMV" = "0" ]; then
  ok "rename/structural: no executable line runs an 'mv' without -T — there is no fallback to reintroduce the defect"
else
  bad "rename/structural: $L2_BAREMV executable line(s) run 'mv' without -T: $(LC_ALL=C grep -n '^[^#]*[^A-Za-z_-]mv ' "$RS" | LC_ALL=C grep -v -- '-T')"
fi
# THE THREE-VALUED PROBE exists and is three-valued: `unknown` is not `no`, and only the probe's
# `no` arm may say this host lacks the option.
for L2_TOK in "printf 'yes" "printf 'no" "printf 'unknown"; do
  if LC_ALL=C grep -q "$L2_TOK" "$RS"; then
    ok "rename/structural: mv_T_supported can answer \"${L2_TOK#printf \'}\" — a could-not-measure is not a not-supported"
  else
    bad "rename/structural: mv_T_supported cannot answer \"${L2_TOK#printf \'}\""
  fi
done

# --- 18. THE STRUCTURAL EMIT-BOUNDARY GUARD (round 7, L1b) ----------------------
# WHY A GUARD AND NOT A FOURTH PATCH. The boundary was bypassed at a NEW site in three consecutive
# review rounds (round 2's S1, round 5's J3, round 7's L1). Every fix was correct and the class kept
# regenerating, which is this repository's standing signal to mechanize rather than to carve the same
# place again — and CLAUDE.md's rule is explicit: neutralise at ONE boundary, NEVER per interpolation
# site, "because a per-site escape is a list to keep complete".
#
# THE GUARD IS `scripts/tests/lib/emit-boundary-scan.sh`, and it DECLARES ITS OWN SCOPE on every run.
# Both suites exercise it, each for its own script, and each with a POSITIVE CONTROL — a bare
# `exit 0` from a scanner proves nothing, because a scanner that flagged nothing would emit exactly
# the same status.
EBS="$SCRIPT_DIR/lib/emit-boundary-scan.sh"
if [ ! -f "$EBS" ]; then
  bad "emit-guard: $EBS is missing — the structural guard did not run (1/6)"
  bad "emit-guard: the same absence (2/6)"
  bad "emit-guard: the same absence (3/6)"
  bad "emit-guard: the same absence (4/6)"
  bad "emit-guard: the same absence (5/6)"
  bad "emit-guard: the same absence (6/6)"
else
  EBS_OUT="$(bash "$EBS" "$RS" 2>&1)"; EBS_RC=$?
  if [ "$EBS_RC" -eq 0 ]; then
    ok "emit-guard: the SHIPPED review-stage.sh is CLEAN — every value on an emitted line is routed or allowlisted"
  else
    bad "emit-guard: the shipped review-stage.sh has an emit-boundary BYPASS: $EBS_OUT"
  fi
  # THE SCOPE MUST BE DECLARED IN THE OUTPUT, not only in a comment: a guard whose coverage is
  # invisible is one a reader over-reads.
  case "$EBS_OUT" in
    *"NOT COVERED"*) ok "emit-guard: the scan DECLARES what it does not cover, on every run" ;;
    *) bad "emit-guard: the scan did not declare its scope (got: $EBS_OUT)" ;;
  esac
  case "$EBS_OUT" in
    *"in-scope emit site(s)"*) ok "emit-guard: and it reports HOW MANY sites it examined — a count, not an adjective" ;;
    *) bad "emit-guard: the scan did not report its subject count (got: $EBS_OUT)" ;;
  esac
  # (a) THE POSITIVE CONTROL. A bypassing emit is planted in a THROWAWAY COPY — the artifact is
  #     substituted, never a settable seam in the shipped script (#3312's corollary for tests) — and
  #     the guard must red AND NAME the planted symbol. A bare non-zero exit is not evidence: an
  #     unrelated breakage produces an identical status.
  EBS_D="$T/ebs"; mkdir -p "$EBS_D"
  cp "$RS" "$EBS_D/review-stage.sh" 2>/dev/null || true
  # A name no allowlist entry mentions, planted on a REAL emit line so the plant is in scope.
  LC_ALL=C sed -e 's|^  emit "RECORD-OK kind=|  emit "RECORD-OK smuggled=$PLANTED_BYPASS_VALUE kind=|' \
    "$EBS_D/review-stage.sh" >"$EBS_D/planted.sh" 2>/dev/null || true
  if [ -f "$EBS_D/planted.sh" ] && LC_ALL=C grep -q 'PLANTED_BYPASS_VALUE' "$EBS_D/planted.sh"; then
    ok "emit-guard/control: the plant landed in the scratch copy (asserted, not assumed — a plant that missed would make the control vacuous)"
  else
    bad "emit-guard/control: the plant did NOT land, so the control below proves nothing"
  fi
  mv "$EBS_D/planted.sh" "$EBS_D/review-stage.sh" 2>/dev/null || true
  EBS_POUT="$(bash "$EBS" "$EBS_D/review-stage.sh" 2>&1)"; EBS_PRC=$?
  if [ "$EBS_PRC" -ne 0 ]; then
    ok "emit-guard/control: the guard REDS on a planted bypass"
  else
    bad "emit-guard/control: the guard reported CLEAN on a planted bypass — it proves nothing (got: $EBS_POUT)"
  fi
  case "$EBS_POUT" in
    *PLANTED_BYPASS_VALUE*) ok "emit-guard/control: and it NAMES the offending value, so the red is attributable" ;;
    *) bad "emit-guard/control: the guard red without naming the planted value (got: $EBS_POUT)" ;;
  esac
fi


# --- 20. A VALIDATED-AS-DIGITS VALUE IS NOT A COMPARABLE ONE (round 8) ----------
# THE FINDING (roborev job 379). `validate_secs` accepted an arbitrarily long digit string, and
# `status` then handed it to bash's FIXED-WIDTH `[ -gt ]`. Measured on the shipped script at
# 69ea2f273:
#
#   $ review-stage.sh open   c --issue 900 --agent spec-auditor --deadline-secs 9999999999999999999999999
#   REVIEW-STAGE: OPEN-OK ... deadline-secs=9999999999999999999999999 ...        (accepted, rc 0)
#   $ review-stage.sh status c --issue 900
#   REVIEW-STAGE: STATUS ... deadline=9999999999999999999999999 past-deadline=no ...
#   review-stage.sh: line 1726: [: 9999999999999999999999999: integer expression expected
#
# Two defects on one line. A RAW BASH DIAGNOSTIC outside the `REVIEW-STAGE: ` anchor every line of
# that block carries (round 7's L1 fixed the sibling instance of exactly that leak), and
# `past-deadline=no` — a PERMISSIVE answer derived from a comparison that never happened, which is
# this repository's standing prohibition: never derive a pass from the absence of a bad signal.
#
# THE CLASS IS WIDER THAN THE FLAG, and the siblings are WORSE because `$(( ))` does not fail at
# all -- it WRAPS SILENTLY. Measured, same script, same box:
#
#   spawned-epoch: 18446744073709551616  ->  elapsed=1788315330 past-deadline=yes + a PAST DEADLINE
#                                            note, for a stage opened ONE SECOND earlier: 56 years
#                                            of fabricated elapsed time, and no diagnostic anywhere
#   spawned-epoch: 01756000000           ->  elapsed=1524598466  (`$(( 010 ))` is OCTAL in bash
#                                            arithmetic while `[ 010 -gt 9 ]` is DECIMAL -- ONE
#                                            value with TWO readings inside one script)
#   reopen-count:  99999999999999999999  ->  reopen-count=7766279631452241920 WRITTEN BACK into the
#                                            record and printed on OPEN-OK
#
# So the fix is ONE predicate, int_is_comparable, applied at every boundary where a value from
# argv or from the record reaches a fixed-width operation -- and the bound is AFFIRMATIVE (at most
# MAX_INT_DIGITS digits, no leading zero) rather than a test for the values that happen to break.
#
# FIELD_IS <needle> — an EXACT `key=value` field, not a substring. `reopen-count=1` is a prefix of
# `reopen-count=16`, so a `has` assertion would have PASSED on the octal defect this section is
# about. Every emitted field is space-delimited, so bracketing the line with spaces makes the test
# exact without a regex.
FIELD_IS() {
  case " $OUT " in
    *" $1 "*) ok "$2" ;;
    *) bad "$2 (got: $OUT)" ;;
  esac
}
# PLANT_FIELD <record> <key> <value> <label> — rewrite one record field and ASSERT the rewrite
# landed. A `sed` that missed would make every assertion after it vacuous, which is the harness
# defect round 7 recorded twice. Same `> .new && mv` idiom as section 17 (no `-i`, whose suffix
# handling is GNU-vs-BSD incompatible).
PLANT_FIELD() {
  local f="$1" k="$2" v="$3" label="$4"
  if [ -f "$f" ] &&
    LC_ALL=C sed -e "s|^$k:.*|$k: $v|" "$f" >"$f.new" 2>/dev/null &&
    mv "$f.new" "$f" 2>/dev/null &&
    LC_ALL=C grep -q "^$k: $v\$" "$f"; then
    ok "$label"
  else
    bad "$label — the plant did NOT land, so the assertions after it prove nothing"
  fi
}
R20="$(newrepo)"

# (a) THE INVOKER ROUTE: refused AT THE BOUNDARY, by name, exit 64 — never surfaced two
#     subcommands later as somebody else's shell error.
rs "$R20" open c --issue 910 --agent spec-auditor --deadline-secs 9999999999999999999999999
rc_is 64 "secs/bound: a 25-digit --deadline-secs is a USAGE refusal (exit 64), not an accepted value"
has "--deadline-secs" "secs/bound: the refusal NAMES the flag"
has "digits" "secs/bound: and states the bound, so the operator knows what to pass instead"
hasnt "integer expression expected" "secs/bound: no raw bash diagnostic — the refusal is this tool's own"
if [ -e "$R20/.review-stage/issue-910/c.stage" ]; then
  bad "secs/bound: a stage record was written for a refused --deadline-secs"
else
  ok "secs/bound: and NOTHING was written — a refused value never reaches the record"
fi
# THE BOUNDARY VALUE, BOTH SIDES. `MAX_INT_DIGITS` is 10, so 9999999999 is the widest ACCEPTED
# value and 10000000000 the narrowest refused one. Pinning both sides is what makes this a bound
# rather than a direction.
rs "$R20" open c --issue 911 --agent spec-auditor --deadline-secs 9999999999
rc_is 0 "secs/bound: the widest in-bound value (10 digits) is ACCEPTED"
FIELD_IS "deadline-secs=9999999999" "secs/bound: and is recorded verbatim, not clamped"
rs "$R20" open c --issue 912 --agent spec-auditor --deadline-secs 10000000000
rc_is 64 "secs/bound: one digit wider (11 digits) is REFUSED"

# (b) THE CONTROL, without which every assertion here is satisfiable by a validator that refuses
#     everything: the values the emitter really produces still pass. `0` in particular — round 7's
#     L3 records `deadline=0` as a legitimate emitter state, so a bound that refused a leading zero
#     WITHOUT excepting `0` itself would red on correct input.
for d in 0 1 1800; do
  rs "$R20" open c --issue "92$d" --agent spec-auditor --deadline-secs "$d"
  rc_is 0 "secs/bound CONTROL: --deadline-secs $d is accepted"
done

# (c) LEADING ZEROS ARE REFUSED, because they are read TWO WAYS in one script (octal to `$(( ))`,
#     decimal to `[ ]`). A value with two readings is refused rather than normalised.
rs "$R20" open c --issue 930 --agent spec-auditor --deadline-secs 0800
rc_is 64 "secs/bound: a zero-padded '0800' is REFUSED — one value must not have two readings"
has "leading zero" "secs/bound: and the refusal says WHY, so it does not read as an arbitrary rejection"

# (d) THE RECORD READ-BACK ROUTE. `deadline-secs` is read out of the record and is deliberately
#     NOT validated on the read side (round 7's disposition: the record's own text is DISPLAYED,
#     routed through `field_value`, so a hand edit stays visible in the audit trail). What has to
#     be affirmative is the COMPARISON.
R20D="$(newrepo)"
rs "$R20D" open c --issue 940 --agent spec-auditor
rc_is 0 "secs/record: the stage opened"
PLANT_FIELD "$R20D/.review-stage/issue-940/c.stage" deadline-secs 9999999999999999999999999 \
  "secs/record: the overflowing deadline landed in the record (asserted, not assumed)"
rs "$R20D" status c --issue 940
rc_is 0 "secs/record: status still reports (advisory — it decides nothing, and it must not DIE either)"
FIELD_IS "past-deadline=unknown" "secs/record: an INCOMPARABLE deadline yields past-deadline=unknown, never a permissive 'no' from a comparison that never ran"
hasnt "past-deadline=no" "secs/record: and specifically NOT 'no' — the exact permissive answer the finding reported"
hasnt "integer expression expected" "secs/record: no raw bash diagnostic escapes the REVIEW-STAGE: block"
# EVERY LINE, not merely the absence of one known string: the property is that nothing UNANCHORED
# escapes, and a future shell error would carry different text. `status` emits only
# `REVIEW-STAGE: ` lines, which is what makes this assertion expressible here (`open` also prints
# the bare report path and the paste-ready clause, by design).
N20_UNANCHORED="$(printf '%s\n' "$OUT" | LC_ALL=C grep -cv '^REVIEW-STAGE: ' || true)"
if [ "$N20_UNANCHORED" = "0" ]; then
  ok "secs/record: EVERY line of status output carries the REVIEW-STAGE: anchor — 0 unanchored lines"
else
  bad "secs/record: $N20_UNANCHORED unanchored line(s) escaped the status block (got: $OUT)"
fi
FIELD_IS "deadline=9999999999999999999999999" "secs/record: the record's own text is still DISPLAYED — a hand edit stays visible in the audit trail"
rs "$R20D" verdict c --issue 940
rc_is 5 "secs/record: and the VERDICT is unchanged — a display/measurement boundary decides nothing"

# (e) THE SILENT-WRAP ROUTE, the worse half: `$(( now - epoch ))` never fails, so the old code
#     reported a FABRICATED elapsed with no diagnostic at all.
R20E="$(newrepo)"
rs "$R20E" open c --issue 950 --agent spec-auditor --deadline-secs 1800
rc_is 0 "secs/epoch: the stage opened"
for plant in 18446744073709551616 01756000000; do
  PLANT_FIELD "$R20E/.review-stage/issue-950/c.stage" spawned-epoch "$plant" \
    "secs/epoch: spawned-epoch=$plant landed in the record (asserted, not assumed)"
  rs "$R20E" status c --issue 950
  rc_is 0 "secs/epoch($plant): status still reports"
  FIELD_IS "elapsed=unknown" "secs/epoch($plant): an INCOMPARABLE spawned-epoch yields elapsed=unknown — never a wrapped or octal number presented as a measurement"
  FIELD_IS "past-deadline=unknown" "secs/epoch($plant): and past-deadline is unknown, because there is no elapsed time to compare"
  hasnt "PAST DEADLINE" "secs/epoch($plant): no PAST DEADLINE note fires off a fabricated clock"
  rs "$R20E" verdict c --issue 950
  FIELD_IS "elapsed=unknown" "secs/epoch($plant): the VERDICT line's elapsed= is unknown too — premerge-assert.sh reads that field, and a fabricated number there is digits and would pass its grammar"
done
# THE CONTROL: an ordinary record still MEASURES. Without it every assertion above is satisfiable
# by a `load_stage` that answered `unknown` for everything.
R20F="$(newrepo)"
rs "$R20F" open c --issue 960 --agent spec-auditor --deadline-secs 1800
rc_is 0 "secs/epoch CONTROL: an ordinary stage opened"
rs "$R20F" status c --issue 960
hasnt "elapsed=unknown" "secs/epoch CONTROL: an ordinary record's elapsed IS measured — the bound did not turn the clock off"
FIELD_IS "past-deadline=no" "secs/epoch CONTROL: and a real comparison still yields a real answer"

# (f) THE ARITHMETIC-ON-A-RECORD-VALUE ROUTE: `reopen-count` is READ from the record and fed to
#     `$(( prior_count + 1 ))`, whose wrap was WRITTEN BACK — so the fabrication was durable.
R20G="$(newrepo)"
rs "$R20G" open c --issue 970 --agent spec-auditor
rc_is 0 "secs/reopen: the stage opened"
SF20G="$R20G/.review-stage/issue-970/c.stage"
for plant in 99999999999999999999 017; do
  PLANT_FIELD "$SF20G" reopen-count "$plant" \
    "secs/reopen: reopen-count=$plant landed in the record (asserted, not assumed)"
  rs "$R20G" open c --issue 970 --agent spec-auditor --force
  rc_is 0 "secs/reopen($plant): the re-open still succeeds — an unusable counter is not a reason to refuse a spawn"
  FIELD_IS "reopen-count=1" "secs/reopen($plant): the counter falls back to the SAME value an absent/non-numeric one gets, never a wrapped or octal number"
  N20G="$(LC_ALL=C sed -n 's/^reopen-count:[[:space:]]*//p' "$SF20G" 2>/dev/null | LC_ALL=C head -1)"
  if [ "$N20G" = "1" ]; then
    ok "secs/reopen($plant): and the record itself holds 1 — the fabrication is not made durable"
  else
    bad "secs/reopen($plant): the record now holds reopen-count '$N20G' — a wrapped/octal value was written back"
  fi
done

# (g) THE ADOPTION ROUTE: `--force` keeps the FIRST spawn's clock by copying `spawned-epoch`
#     forward, so an unusable value used to be re-written into the fresh record and outlive the
#     edit that introduced it.
R20H="$(newrepo)"
rs "$R20H" open c --issue 980 --agent spec-auditor
rc_is 0 "secs/adopt: the stage opened"
SF20H="$R20H/.review-stage/issue-980/c.stage"
PLANT_FIELD "$SF20H" spawned-epoch 18446744073709551616 \
  "secs/adopt: the unusable prior epoch landed in the record (asserted, not assumed)"
rs "$R20H" open c --issue 980 --agent spec-auditor --force
rc_is 0 "secs/adopt: --force still re-opens the stage"
has "the clock restarts from now" "secs/adopt: and it SAYS the clock restarted — a silently reset clock is what the --force path exists not to do"
E20H="$(LC_ALL=C sed -n 's/^spawned-epoch:[[:space:]]*//p' "$SF20H" 2>/dev/null | LC_ALL=C head -1)"
case "$E20H" in
  18446744073709551616) bad "secs/adopt: the unusable epoch was ADOPTED into the fresh record — the fabrication outlives the edit" ;;
  "" | *[!0-9]* ) bad "secs/adopt: the fresh record's spawned-epoch is not a number ('$E20H')" ;;
  *) ok "secs/adopt: the fresh record carries a real clock reading, not the unusable one" ;;
esac
rs "$R20H" status c --issue 980
FIELD_IS "past-deadline=no" "secs/adopt: and status measures a restarted clock rather than 56 years of fabricated elapsed time"

# (h) STRUCTURAL. The point of round 8 is ONE predicate at every such boundary, not four local
#     patches — so the shipped script is asserted to HAVE that predicate, to declare its bound
#     ONCE, and to route every fixed-width consumer of a record/argv value through it. Counted on
#     EXECUTABLE lines only (`^[^#]*`), because the comments quote the symbol by name and a comment
#     is not a call — the shape section 19 already uses.
N20_DEF="$(LC_ALL=C grep -c '^int_is_comparable()' "$RS" || true)"
if [ "$N20_DEF" = "1" ]; then
  ok "secs/structural: int_is_comparable is defined EXACTLY once — one predicate, not a family"
else
  bad "secs/structural: int_is_comparable is defined $N20_DEF times (want 1)"
fi
N20_BOUND="$(LC_ALL=C grep -c '^MAX_INT_DIGITS=' "$RS" || true)"
if [ "$N20_BOUND" = "1" ]; then
  ok "secs/structural: the bound is declared ONCE, as a named constant — never a literal at each site"
else
  bad "secs/structural: MAX_INT_DIGITS is declared $N20_BOUND times (want 1)"
fi
# COUNTED AS OCCURRENCES, NOT LINES: two of the gates test BOTH operands on one `&&` line, and a
# gate that dropped one operand would leave the line count unchanged — which is the whole defect
# class one level up (`now_epoch`'s own output was the unvalidated operand). SEVEN is the
# enumeration: argv (1), both operands of the elapsed subtraction (2), both operands of the
# past-deadline comparison (2), the adopted prior epoch (1), the reopen counter (1).
N20_CALLS="$(LC_ALL=C grep '^[^#]*int_is_comparable ' "$RS" | LC_ALL=C grep -o 'int_is_comparable ' | LC_ALL=C grep -c . || true)"
if [ "$N20_CALLS" -ge 7 ]; then
  ok "secs/structural: $N20_CALLS executable call(s) route through it — argv, both subtraction operands, both comparison operands, the adopted epoch and the counter"
else
  bad "secs/structural: only $N20_CALLS executable int_is_comparable call(s) — the boundaries this round enumerated are 7"
fi
# AND THE SUBJECTS ARE STILL THERE, i.e. each fixed-width operation this round gated still exists.
# Named per value, so a red says WHICH boundary's subject moved and the guard above went vacuous.
for pair in 'STAGE_ELAPSED" -gt "$STAGE_DEADLINE:the past-deadline comparison' \
  'now - epoch:the elapsed-time subtraction' \
  'prior_count + 1:the reopen-count increment'; do
  needle="${pair%%:*}"; label="${pair#*:}"
  if LC_ALL=C grep -qF "$needle" "$RS"; then
    ok "secs/structural: $label is still present, so the routing assertion above has a subject"
  else
    bad "secs/structural: $label is GONE from the script — this round's subject moved and the guard above is now vacuous"
  fi
done
# --- case floor ---------------------------------------------------------------
# --- 21. THE CLOBBER GUARD MUST PREVENT, NOT REPORT (round 9, N1) ----------------
# THE FINDING (roborev job 382, N1). Round 2's B2 made `record-author-performed` refuse to
# replace a RECORDED verdict without `--force`. It checked the CURRENT verdict, then prepared a
# temporary file, wrote the substitute into it and renamed it into place — so a late reviewer
# landing `result: FINDINGS` ANYWHERE IN THAT WINDOW was silently overwritten by the
# merge-proceeding AUTHOR-PERFORMED token, with no `--force` and no `replaced-verdict:` trace.
# The guard REPORTED the state it found; it did not PREVENT the state it exists to stop, and
# CLAUDE.md's rule is that a check placed before the act it guards, with a window in between,
# only reports — the control has to be that the bad state cannot be REACHED.
#
# THE WINDOW IS SIMULATED, NOT RACED. Both cases below run a SCRATCH COPY of the shipped script
# with ONE line injected INSIDE the window — at its EARLIEST point (before the symlink assert)
# and at its LATEST point (immediately before the rename) — so the interleaving is deterministic,
# cannot flake, and covers both ends of the span rather than one convenient instant. The
# ARTIFACT is substituted; there is no settable seam in the shipped script (#3312's corollary for
# tests). It is a SIMULATION of the race: nothing here is concurrent, and the case makes no claim
# about timing.
N1_D="$T/n1"; mkdir -p "$N1_D"
# `awk -v` PERFORMS ESCAPE PROCESSING on its value, so an injected line containing `\n` would
# arrive carrying REAL NEWLINES and be planted as several broken lines (round 7's measured
# harness defect). Every value travels through ENVIRON, which does no such processing.
n1_build() {
  local dest="$1" anchor="$2" inj="$3"
  N1_ANCHOR="$anchor" N1_INJ="$inj" LC_ALL=C awk '
    BEGIN { a = ENVIRON["N1_ANCHOR"]; inj = ENVIRON["N1_INJ"]; done = 0 }
    index($0, a) > 0 && done == 0 { print inj; done = 1 }
    { print }
  ' "$RS" >"$dest" 2>/dev/null || return 1
  [ -s "$dest" ] || return 1
  LC_ALL=C grep -q 'N1_LATE_REVIEWER' "$dest" || return 1
  return 0
}
# The interleaving itself: a late reviewer replacing the report with a BLOCKING verdict. Single
# quotes so the `$STAGE_REPORT` reference is resolved by the SCRATCH SCRIPT at run time, not here.
N1_INJECTION='  printf '"'"'result: FINDINGS\n\n### [BLOCKER] N1_LATE_REVIEWER landed this inside the window\n'"'"' >"$STAGE_REPORT"'
N1_REASON='no peer agent available on this box; hand C against the spec deltas'
N1_EV='docs/round-artifacts/issue-3751-hand-c.md'

n1_case() {
  # <label> <anchor> — build the scratch script, open a stage with the SHIPPED script, then run
  # `record-author-performed` with the scratch one. The stage is opened by the shipped script on
  # purpose: only the recording path is under test.
  local label="$1" anchor="$2" prog="$N1_D/$3.sh" repo issue="$4" rep
  if ! n1_build "$prog" "$anchor" "$N1_INJECTION"; then
    bad "n1/$label: the interleaving plant did NOT land, so this case proves nothing"
    bad "n1/$label: (the same absence, 2/5)"
    bad "n1/$label: (the same absence, 3/5)"
    bad "n1/$label: (the same absence, 4/5)"
    bad "n1/$label: (the same absence, 5/5)"
    return 0
  fi
  ok "n1/$label: the interleaving plant landed in the scratch copy (asserted, not assumed)"
  repo="$(newrepo)"
  rs "$repo" open c --issue "$issue" --agent spec-auditor
  rep="$(REPORT_OF "$repo" "$issue" c)"
  OUT="$(cd "$repo" && bash "$prog" record-author-performed c --issue "$issue" \
    --reason "$N1_REASON" --evidence "$N1_EV" --performed-by author 2>&1)"; RC=$?
  rc_is 2 "n1/$label: a verdict recorded INSIDE the window is REFUSED, not overwritten"
  has "reason=report-changed-mid-write" "n1/$label: the refusal names the cause"
  OUT="$(cat "$rep" 2>/dev/null || printf '<absent>\n')"; RC=0
  has "result: FINDINGS" "n1/$label: the late reviewer's FINDINGS SURVIVES"
  hasnt "result: AUTHOR-PERFORMED" "n1/$label: and the merge-proceeding token was NOT installed over it"
}

# (a) the EARLIEST point in the window — immediately after the B2 check, before the path asserts.
n1_case early 'assert_no_symlink "$STAGE_REPORT" report-of-record' early 640
# (b) the LATEST point THE CHECK CAN COVER — after the substitute is fully written to the
#     temporary file, immediately before the re-observation that guards the rename. This is the
#     instant a check placed anywhere earlier cannot see, and the anchor is deliberately the
#     re-observation itself: delete that line and this case cannot plant, which fails closed (5
#     bads) rather than passing vacuously. The span AFTER the re-observation is the DECLARED
#     RESIDUAL WINDOW named in the script — one `mv` wide, irreducible in a shell — and it is
#     deliberately NOT asserted here: a case requiring the clobber to happen would red the day
#     someone closes it.
n1_case late 'now_obs="$(report_bytes "$STAGE_REPORT")"' late 641

# (c) FORCED IS NOT A BLANKET AUTHORIZATION. `--force` authorizes replacing the verdict the
#     operator READ; a DIFFERENT verdict arriving afterwards was never authorized, so the
#     interleaving is refused under `--force` too.
if n1_build "$N1_D/forced.sh" 'now_obs="$(report_bytes "$STAGE_REPORT")"' "$N1_INJECTION"; then
  ok "n1/forced: the plant landed"
else
  bad "n1/forced: the plant did NOT land"
fi
R21F="$(newrepo)"
rs "$R21F" open c --issue 642 --agent spec-auditor
R21F_REP="$(REPORT_OF "$R21F" 642 c)"
printf 'result: PASS\n\nreviewed, no blocking finding\n' >"$R21F_REP"
OUT="$(cd "$R21F" && bash "$N1_D/forced.sh" record-author-performed c --issue 642 \
  --reason "$N1_REASON" --evidence "$N1_EV" --performed-by author --force 2>&1)"; RC=$?
rc_is 2 "n1/forced: --force does NOT authorize replacing a verdict that arrived after the check"
has "reason=report-changed-mid-write" "n1/forced: and it names the same cause"
OUT="$(cat "$R21F_REP" 2>/dev/null || printf '<absent>\n')"; RC=0
has "result: FINDINGS" "n1/forced: the verdict that arrived in the window survives"

# (d) CONTROL — THE SAME SCRATCH MACHINERY, WITH A NO-OP INJECTION, STILL RECORDS. Without this
#     the refusals above are satisfiable by a scratch copy that is simply broken, or by a check
#     that refuses every recording.
if n1_build "$N1_D/noop.sh" 'now_obs="$(report_bytes "$STAGE_REPORT")"' '  : N1_LATE_REVIEWER no-op'; then
  ok "n1/CONTROL: the no-op plant landed at the same anchor"
else
  bad "n1/CONTROL: the no-op plant did NOT land"
fi
R21C="$(newrepo)"
rs "$R21C" open c --issue 643 --agent spec-auditor
R21C_REP="$(REPORT_OF "$R21C" 643 c)"
OUT="$(cd "$R21C" && bash "$N1_D/noop.sh" record-author-performed c --issue 643 \
  --reason "$N1_REASON" --evidence "$N1_EV" --performed-by author 2>&1)"; RC=$?
rc_is 0 "n1/CONTROL: an UNDISTURBED report is still recorded (the refusal comes from the interleaving, not from the scratch copy)"
has "RECORD-OK" "n1/CONTROL: the normal path still reports RECORD-OK"
hasnt "report-changed-mid-write" "n1/CONTROL: and claims no interleaving that did not happen"
OUT="$(cat "$R21C_REP" 2>/dev/null || printf '<absent>\n')"; RC=0
has "result: AUTHOR-PERFORMED" "n1/CONTROL: the substitute really was installed"

# (e) CONTROL — the SHIPPED script's forced replacement still works end to end, so the new
#     re-verification did not red the one path B2 deliberately leaves open.
R21S="$(newrepo)"
rs "$R21S" open c --issue 644 --agent spec-auditor
R21S_REP="$(REPORT_OF "$R21S" 644 c)"
printf 'result: FINDINGS\n\n### [BLOCKER] a real gap\n' >"$R21S_REP"
rs "$R21S" record-author-performed c --issue 644 --reason "$N1_REASON" --evidence "$N1_EV" \
  --performed-by author --force
rc_is 0 "n1/CONTROL: the SHIPPED forced replacement is unaffected"
has "replaced-verdict=FINDINGS" "n1/CONTROL: and still records what it replaced"

# (f) STRUCTURAL — THE CHECK IS INSIDE THE WINDOW IT CERTIFIES. A re-verification that drifted
#     back above `prepare_write` would restore the reported-not-prevented shape while every
#     behavioural case above still passed (the injection anchors would move with it), so the
#     ORDER is pinned from source: the second observation must be taken AFTER the substitute is
#     written and BEFORE the rename.
N1_PREP_LN="$(LC_ALL=C grep -n 'prepare_write "\$STAGE_REPORT" report-of-record' "$RS" | LC_ALL=C head -1 | cut -d: -f1)"
N1_COMMIT_LN="$(LC_ALL=C grep -n 'commit_write "\$STAGE_REPORT" report-of-record' "$RS" | LC_ALL=C head -1 | cut -d: -f1)"
N1_RECHECK_LN="$(LC_ALL=C grep -n 'report_bytes "\$STAGE_REPORT"' "$RS" | LC_ALL=C tail -1 | cut -d: -f1)"
if [ -n "$N1_PREP_LN" ] && [ -n "$N1_COMMIT_LN" ] && [ -n "$N1_RECHECK_LN" ] &&
  [ "$N1_RECHECK_LN" -gt "$N1_PREP_LN" ] && [ "$N1_RECHECK_LN" -lt "$N1_COMMIT_LN" ]; then
  ok "n1/structural: the re-observation is taken after the write and BEFORE the rename (lines $N1_PREP_LN < $N1_RECHECK_LN < $N1_COMMIT_LN)"
else
  bad "n1/structural: the re-observation is NOT between the write and the rename (prepare=$N1_PREP_LN recheck=$N1_RECHECK_LN commit=$N1_COMMIT_LN)"
fi
# AND THE RESIDUAL WINDOW IS DECLARED IN THE CODE, because it cannot be removed: there is no
# compare-and-swap rename reachable from a shell. A comment naming it is what stops the next
# reader believing the check is atomic.
if LC_ALL=C grep -q 'RESIDUAL WINDOW' "$RS"; then
  ok "n1/structural: the irreducible residual window is DECLARED in the source, not left implicit"
else
  bad "n1/structural: nothing in the source declares the residual window"
fi

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
# ROUND 6's FIRST ITEM (K1) ADDS 11 (429 -> 440): section 15 pins that an UNREADABLE stage record
# is a NON-VERDICT rather than "no generation field" — on the READ side (the verdict, its cause,
# the absent path, the status state) and on the WRITE side (`open` refuses instead of guessing the
# legacy generation) — plus the two preconditions and the readable-again CONTROL. One case is
# host-conditional and takes the FIRST shape the two rules above allow: the mode-000 branch emits
# the SAME NUMBER of assertions whether or not this user can read a mode-000 file (root can), so
# the count does not move.
# ROUND 6's SECOND ITEM (K2) MOVES IT TO 465, IN BOTH DIRECTIONS. Section 16 adds 21 (the report
# path is GENERATED, never SELECTED: the race simulation end to end, the structural pins on the
# DELETED selection machinery, the uniqueness measurement over twelve real opens, and the
# no-fallback refusal with its control). Section 14 was RE-EXPRESSED rather than shrunk — the
# scanned generation became a nonce, so (a) asserts the SHAPE and reads the VALUE from the record,
# (d) asserts DISTINCTNESS instead of monotonicity, (e) keeps the property the deleted existence
# belt was for, and (f) gains the too-SHORT-token case (charset is not the whole rule): 40 -> 44.
# Section 11(a) also changed OUTCOME rather than disappearing: a symlink planted at the old
# guessable report name is now INERT rather than refused (the name is unpredictable), so the case
# asserts that stronger property and grew from 4 assertions to 5, with the reachable report-half
# leaf refusal still covered by 11(e). 440 -> 465. Every added assertion is UNCONDITIONAL —
# section 16's extra requirements (git commits, `cp -a`, a PATH-shadowed `mktemp`) are the SUBJECT
# of asserted preconditions rather than preconditions for running — so the EXACT floor still holds
# by the two shapes recorded above.
# ROUND 6's THIRD ITEM (K3) MOVES IT TO 471, IN BOTH DIRECTIONS. `--performed-by peer` was ACCEPTED
# and then reported under the token `AUTHOR-PERFORMED`, so a PEER audit was stated to be the diff
# AUTHOR's. `peer` is REMOVED rather than given a token of its own, so the ONE case that asserted it
# was accepted (2 assertions) is REPLACED by the case that it is refused by name and writes nothing
# (4), and the classifier gains the matching hand-written case (3): 465 -> 471, with the two prose
# assertions that quoted the two-value set corrected in place. Every assertion is unconditional.
# ROUND 7 MOVES IT TO 519, AND EVERY ADDED ASSERTION IS UNCONDITIONAL.
# L1(a) — section 17 adds 15: the three values READ FROM THE STAGE RECORD (`deadline=`, `agent=`,
# `spawned-at=`) reached the verdict and STATUS lines through `one_line` alone, which does not map
# the ONE reserved character of a `key=value` line, so a hand-edited record put a SECOND
# `deadline=`/`agent=` pair on a line consumers scan field by field. COUNTED, not matched — the
# broken script prints `deadline=1800` too — plus the `past-deadline` defect the case surfaced (a
# two-valued guard testing for the literal `unknown` let any other non-numeric value reach
# `[ ... -gt ... ]`, printing a raw bash diagnostic into the REVIEW-STAGE: block and then taking the
# permissive branch), and a CONTROL that an ordinary record's values pass through UNCHANGED.
# L1(b) — section 18 adds 6: the STRUCTURAL emit-boundary guard
# (scripts/tests/lib/emit-boundary-scan.sh) must be CLEAN on the shipped script, must DECLARE its
# scope and its subject count at run time, and must RED on a planted bypass AND NAME it — with the
# plant itself asserted to have landed, since a plant that missed would make the control vacuous.
# L2 — section 19 adds 27: the rename must replace the EXACT destination name (`mv -f -T`), covered
# in five deliberately different ways because the window `-T` closes is not inducible from outside
# the process — the end-to-end OUTCOME for a planted directory and a planted symlink-to-directory
# (with the refusing LAYER named, so the case does not claim to exercise `mv -T`), the atomic
# replacement CONTROL, the HOST property measured on this box, a no-`-T` host simulated with a
# PATH-shadowed `mv` (the "REQUIRED, not attempted" assertion), and the structural pins.
# Every branch of every conditional in all three sections emits the SAME NUMBER of assertions
# (the `[ ! -f "$EBS" ]` fallback emits 6 bads against 6 oks; each precondition emits exactly one
# either way), so the EXACT floor holds by the two shapes recorded above.
#
# ROUND 8 (roborev job 379) MOVES IT TO 578. Section 20 adds 59: a validated-as-digits value is
# not a COMPARABLE one, so the bound is affirmative at every boundary where a value from argv or
# from the stage record reaches a fixed-width operation. Five routes, each with the plant asserted
# to have landed — the invoker flag (refused at the boundary, exit 64, nothing written) with BOTH
# sides of the 10-digit bound pinned; the record's `deadline-secs` read back (`past-deadline`
# reads `unknown`, and EVERY line of the status block is asserted to carry the `REVIEW-STAGE: `
# anchor, so a future shell error with different text is caught too); `spawned-epoch`'s silent
# `$(( ))` WRAP and its OCTAL reading (`elapsed=unknown`, on the status AND verdict lines);
# `reopen-count`'s wrap, which was WRITTEN BACK into the record; and the `--force` adoption path,
# where an unusable epoch used to outlive the edit that introduced it. Plus three CONTROLS (`0`,
# `1`, `1800` still accepted; an ordinary record still MEASURES; a real comparison still answers)
# and six structural pins. `FIELD_IS` compares an EXACT space-delimited field rather than a
# substring, because `reopen-count=1` is a prefix of `reopen-count=16` and a `has` assertion would
# have passed on the octal defect this section is about. Every assertion is unconditional — each
# `if`/`case` calls exactly one of `ok`/`bad` — so the EXACT floor holds by the two shapes above.
#
# ROUND 9 (roborev job 382) MOVES IT TO 601. Section 21 adds 23: the clobber guard must PREVENT,
# not report. Round 2's B2 checked the recorded verdict and then spent a `mktemp`, an `O_EXCL`
# create, a `date` and a dozen `printf`s before installing its replacement, so a verdict landing
# in that window was overwritten by the merge-proceeding AUTHOR-PERFORMED token with no `--force`
# and no trace. The interleaving is SIMULATED deterministically — one line injected into a SCRATCH
# COPY of the script at the EARLIEST point of the window and at the LATEST point the check can
# cover — plus the `--force` case (which authorizes replacing the verdict the operator READ, never
# one that arrives afterwards), three CONTROLS (the same scratch machinery with a NO-OP injection
# still records; the shipped forced replacement is unaffected; the substitute really is installed)
# and two STRUCTURAL pins (the re-observation sits between the write and the rename, by line
# number, and the irreducible residual window is DECLARED in the source). Every branch emits the
# same number of assertions (`n1_case`'s plant-failed arm emits 5 bads against 5 oks), so the
# EXACT floor holds.
ASSERT_FLOOR=601
EXECUTED=$((PASS + FAIL))
if [ "$EXECUTED" -lt "$ASSERT_FLOOR" ]; then
  bad "CASE FLOOR: only $EXECUTED assertions executed, below the committed floor of $ASSERT_FLOOR — a section died silently, and 'failed: 0' over a shrunken suite is not a pass"
fi

printf '\n=== review-stage: %d passed, %d failed (executed %d, floor %d) ===\n' \
  "$PASS" "$FAIL" "$((PASS + FAIL))" "$ASSERT_FLOOR"
[ "$FAIL" -eq 0 ]
