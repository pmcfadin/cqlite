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
has "supersedes-report-nonce=" "clobber: and names the GENERATION it took over from, so the replaced report can be read"
# THE SUBSTITUTE IS A FRESH GENERATION (round 15, U1), so the current report is the one the
# RECORD now names — not the path this test opened with, which is the SUPERSEDED one.
R9B_SUPERSEDED="$R9B_REPORT"
R9B_REPORT="$(REPORT_OF "$R9B" 620 c)"
if [ "$R9B_REPORT" != "$R9B_SUPERSEDED" ]; then
  ok "clobber: the recording published a DIFFERENT report path — the prior generation was not written over"
else
  bad "clobber: the recording re-used the prior report path (want a fresh generation): $R9B_REPORT"
fi
OUT="$(cat "$R9B_REPORT")"; RC=0
has "replaced-verdict: FINDINGS" "clobber: the REPORT itself records the replaced token, so the substitution is auditable"
has "supersedes-report-nonce: " "clobber: and the report names the generation it superseded"
# AND THE SUPERSEDED VERDICT IS STILL ON DISK. This is the whole of U1: a recorded verdict is
# SUPERSEDED, never DESTROYED, so the audit trail survives the substitution.
OUT="$(cat "$R9B_SUPERSEDED" 2>/dev/null || printf '<absent>\n')"; RC=0
has "result: FINDINGS" "clobber: the SUPERSEDED report still records the FINDINGS it recorded — nothing was destroyed"
has "[BLOCKER] a real gap" "clobber: including the reviewer's own prose, readable in its own generation"
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
# THE DESTINATION IS THE FRESH GENERATION (round 15, U1), read from the RECORD rather than
# predicted: `record-author-performed` no longer renames over the report it read, so the
# atomic-replacement property is asserted about the name it DID write.
L2_RP4="$(REPORT_OF "$R19C" 992 c)"
if [ "$L2_RP4" != "$L2_RP3" ]; then
  ok "rename CONTROL: the recording published a FRESH generation, so the sentinel's name was never a rename destination"
else
  bad "rename CONTROL: the recording re-used the sentinel's path $L2_RP3"
fi
if [ -f "$L2_RP4" ] && LC_ALL=C grep -q '^result: AUTHOR-PERFORMED' "$L2_RP4" &&
  ! LC_ALL=C grep -q '^result: NOT-RUN' "$L2_RP4"; then
  ok "rename CONTROL: the destination holds exactly the new record — the temporary file was renamed onto the exact name, whole, with no line of a previous document in it"
else
  bad "rename CONTROL: the destination was not cleanly written (content: $(LC_ALL=C head -3 "$L2_RP4" 2>/dev/null))"
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
  # TEN, matching the ten assertions the else-branch emits, so the EXACT case floor holds either
  # way (round 9 added the four compound-statement control assertions below).
  bad "emit-guard: $EBS is missing — the structural guard did not run (1/10)"
  bad "emit-guard: the same absence (2/10)"
  bad "emit-guard: the same absence (3/10)"
  bad "emit-guard: the same absence (4/10)"
  bad "emit-guard: the same absence (5/10)"
  bad "emit-guard: the same absence (6/10)"
  bad "emit-guard: the same absence (7/10)"
  bad "emit-guard: the same absence (8/10)"
  bad "emit-guard: the same absence (9/10)"
  bad "emit-guard: the same absence (10/10)"
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
  # (b) THE COMPOUND-STATEMENT POSITIVE CONTROL (#3751 round 9, N3). The plant above is at the
  #     START of a line, which the FIRST version of this guard could see. Its blind spot was every
  #     COMPOUND statement — its scope was anchored `^[[:space:]]*(emit|note)[[:space:]]` — and two
  #     REAL bypasses sat behind it in this very script (`$extra` behind a `[ -z … ] ||`, `$token`
  #     in a one-line `case` arm). So this control REPRODUCES one of those instances: the routing is
  #     removed from the `[ -z "$extra" ] ||` line and the planted name put in its place. A control
  #     that only plants at a line start could not tell the widened guard from the blind one.
  EBS_C="$T/ebs-compound"; mkdir -p "$EBS_C"
  LC_ALL=C sed -e 's|emit "$REFUSE_MARKER detail=$(field_value "$extra")"|emit "$REFUSE_MARKER detail=$PLANTED_COMPOUND_BYPASS"|' \
    "$RS" >"$EBS_C/review-stage.sh" 2>/dev/null || true
  EBS_CLINE="$(LC_ALL=C grep -n 'PLANTED_COMPOUND_BYPASS' "$EBS_C/review-stage.sh" 2>/dev/null | LC_ALL=C head -1 || true)"
  if [ -n "$EBS_CLINE" ]; then
    ok "emit-guard/compound: the compound plant landed in the scratch copy (asserted, not assumed)"
  else
    bad "emit-guard/compound: the compound plant did NOT land, so this control proves nothing"
  fi
  # THE PLANT MUST REALLY BE COMPOUND, or this control is a duplicate of (a): the statement must
  # NOT begin the line. Measured from the planted text itself rather than assumed from the sed.
  case "$(printf '%s\n' "${EBS_CLINE#*:}" | LC_ALL=C sed -e 's/^[[:space:]]*//' -e 's/[[:space:]].*//')" in
    emit | note)
      bad "emit-guard/compound: the planted statement BEGINS its line, so a line-anchored scope would have seen it too — this control does not test compound recognition (line: $EBS_CLINE)" ;;
    "")
      bad "emit-guard/compound: could not read the planted line's first word" ;;
    *)
      ok "emit-guard/compound: the planted statement does NOT begin its line (it is behind a [ … ] ||), which is exactly what the line-anchored scope could not see" ;;
  esac
  EBS_COUT="$(bash "$EBS" "$EBS_C/review-stage.sh" 2>&1)"; EBS_CRC=$?
  if [ "$EBS_CRC" -ne 0 ]; then
    ok "emit-guard/compound: the guard REDS on a bypass inside a COMPOUND statement"
  else
    bad "emit-guard/compound: the guard reported CLEAN on a compound-statement bypass — the round-7 blind spot is back (got: $EBS_COUT)"
  fi
  case "$EBS_COUT" in
    *PLANTED_COMPOUND_BYPASS*) ok "emit-guard/compound: and it NAMES the offending value, so the red is attributable" ;;
    *) bad "emit-guard/compound: the guard red without naming the planted value (got: $EBS_COUT)" ;;
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

# (a) the EARLIEST point in the window — immediately after the B2 check, where this call takes the
#     record text it is going to republish. The anchor moved in round 15 (U1) because the path
#     asserts it used to sit on became a READ-side guard placed BEFORE the check, and again in
#     round 17 (W1) because that record text is no longer READ here: it comes from the ONE
#     observation, so the anchor is the line that consumes it. The case is unchanged in meaning —
#     a verdict landing at the earliest point of the window is REFUSED, not overwritten.
n1_case early 'rec_text="$STAGE_RECORD_TEXT"' early 640
# (b) the LATEST point THE CHECK CAN COVER — after the substitute is fully written AND committed
#     at its fresh generation, immediately before the re-observation that guards the publication.
#     This is the instant a check placed anywhere earlier cannot see, and the anchor is
#     deliberately the re-observation itself: delete that line and this case cannot plant, which
#     fails closed (5 bads) rather than passing vacuously. The span AFTER the re-observation is the
#     remaining window, and since round 15 (U1) what lands in it is SUPERSEDED rather than
#     DESTROYED — asserted positively in (g) below, which is the case round 9 said could not be
#     written.
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
# THE SUBSTITUTE IS AT THE FRESH GENERATION THE RECORD NOW NAMES (round 15, U1) — read from the
# record, never from the path the stage was opened at, which is the SUPERSEDED one.
OUT="$(cat "$(REPORT_OF "$R21C" 643 c)" 2>/dev/null || printf '<absent>\n')"; RC=0
has "result: AUTHOR-PERFORMED" "n1/CONTROL: the substitute really was installed"
OUT="$(cat "$R21C_REP" 2>/dev/null || printf '<absent>\n')"; RC=0
has "result: NOT-RUN" "n1/CONTROL: and the sentinel it superseded is still on disk, untouched"

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

# (g) THE WINDOW ROUND 9 DECLARED IRREDUCIBLE — DRIVEN, AND NO LONGER DESTRUCTIVE (round 15, U1).
#     Round 9 narrowed the clobber window to the span between the re-observation and the
#     `rename(2)` inside one `mv`, then DECLARED the remainder, arguing that a shell has no
#     compare-and-swap rename. That was true about the shell and wrong about the harm: the party
#     who loses a verdict in that span is A SLOW REVIEWER — the population #3751 exists for — so
#     the loss was caused by this system's own normal behaviour, and what was lost was a recorded
#     review verdict. `record-author-performed` therefore no longer writes to `$STAGE_REPORT` at
#     all: the substitute lands in a FRESHLY RESERVED generation and the stage record publishes
#     it. The window is still there; DESTRUCTION is not.
#
#     THE INJECTION IS AT THE LAST INSTANT BEFORE PUBLICATION — inside that remaining window,
#     AFTER the re-observation, which is precisely the case round 9 said could not be written
#     ("a case requiring the clobber to happen would red the day someone closes it"). It can be
#     written now because the assertion is no longer that a clobber happens: it is that the late
#     verdict SURVIVES. SIMULATED, NOT RACED — one injected line at a fixed point in a scratch
#     copy of the shipped script, nothing concurrent, no timing dependence.
#
#     The anchor is the LAST occurrence of the record commit, because `cmd_open` holds the first.
n1_build_last() {
  local dest="$1" anchor="$2" inj="$3"
  N1_ANCHOR="$anchor" N1_INJ="$inj" LC_ALL=C awk '
    BEGIN { a = ENVIRON["N1_ANCHOR"]; inj = ENVIRON["N1_INJ"] }
    NR == FNR { if (index($0, a) > 0) last = FNR; next }
    FNR == last { print inj }
    { print }
  ' "$RS" "$RS" >"$dest" 2>/dev/null || return 1
  [ -s "$dest" ] || return 1
  LC_ALL=C grep -q 'N1_LATE_REVIEWER' "$dest" || return 1
  return 0
}
if n1_build_last "$N1_D/window.sh" 'commit_write "$sfile" stage-record' "$N1_INJECTION"; then
  ok "n1/window: the plant landed at the LAST instant before publication (inside the remaining window)"
else
  bad "n1/window: the plant did NOT land, so this case proves nothing"
fi
R21W="$(newrepo)"
rs "$R21W" open c --issue 645 --agent spec-auditor
R21W_PRIOR="$(REPORT_OF "$R21W" 645 c)"
OUT="$(cd "$R21W" && bash "$N1_D/window.sh" record-author-performed c --issue 645 \
  --reason "$N1_REASON" --evidence "$N1_EV" --performed-by author 2>&1)"; RC=$?
rc_is 0 "n1/window: the recording completes (the interleaving lands after every check, by construction)"
R21W_NEW="$(REPORT_OF "$R21W" 645 c)"
if [ "$R21W_NEW" != "$R21W_PRIOR" ]; then
  ok "n1/window: and the published report is a DIFFERENT generation from the one the reviewer wrote into"
else
  bad "n1/window: the recording published the SAME path the reviewer wrote into: $R21W_NEW"
fi
OUT="$(cat "$R21W_PRIOR" 2>/dev/null || printf '<absent>\n')"; RC=0
has "result: FINDINGS" "n1/window: THE LATE REVIEWER'S BLOCKING VERDICT IS STILL ON DISK — superseded, never destroyed"
has "N1_LATE_REVIEWER" "n1/window: including the reviewer's own prose, readable in its own generation"
OUT="$(cat "$R21W_NEW" 2>/dev/null || printf '<absent>\n')"; RC=0
has "result: AUTHOR-PERFORMED" "n1/window: the published generation holds the substitute"
has "supersedes-report-nonce: " "n1/window: which NAMES the generation it took over from, so the surviving verdict is findable"
# THE RED THIS REPLACES, ASSERTED RATHER THAN REMEMBERED: the pre-U1 shape wrote the substitute AT
# `$STAGE_REPORT`, so the same interleaving DESTROYED the verdict. Reconstructed by planting the
# same injection at the pre-U1 anchor in a copy whose write destination is forced back to
# `$STAGE_REPORT` — without this control the assertions above are satisfiable by a script that
# simply refuses, and a "superseded, not destroyed" claim about a tool that writes nowhere is
# vacuous.
if n1_build_last "$N1_D/preu1.sh" 'commit_write "$new_rpath" report-of-record' "$N1_INJECTION" &&
  LC_ALL=C sed -e 's|commit_write "\$new_rpath" report-of-record|commit_write "$STAGE_REPORT" report-of-record|' \
      -e 's|prepare_write "\$new_rpath" report-of-record|prepare_write "$STAGE_REPORT" report-of-record|' \
      "$N1_D/preu1.sh" >"$N1_D/preu1b.sh" &&
  LC_ALL=C grep -q 'commit_write "\$STAGE_REPORT" report-of-record' "$N1_D/preu1b.sh"; then
  ok "n1/window RED-CONTROL: the pre-U1 write destination was reconstructed (the substitute goes back to \$STAGE_REPORT)"
else
  bad "n1/window RED-CONTROL: could not reconstruct the pre-U1 write destination"
fi
R21P="$(newrepo)"
rs "$R21P" open c --issue 646 --agent spec-auditor
R21P_REP="$(REPORT_OF "$R21P" 646 c)"
OUT="$(cd "$R21P" && bash "$N1_D/preu1b.sh" record-author-performed c --issue 646 \
  --reason "$N1_REASON" --evidence "$N1_EV" --performed-by author 2>&1)"; RC=$?
N1_SURVIVES="$(LC_ALL=C grep -rl 'N1_LATE_REVIEWER' "$R21P/.review-stage" 2>/dev/null | LC_ALL=C wc -l | LC_ALL=C tr -d ' ')"
if [ "$N1_SURVIVES" = "0" ]; then
  ok "n1/window RED-CONTROL: with the pre-U1 destination the late verdict is GONE FROM DISK ENTIRELY — the differential can see the defect"
else
  bad "n1/window RED-CONTROL: the late verdict survived the pre-U1 destination too ($N1_SURVIVES file(s)), so the case above proves nothing"
fi

# (f) STRUCTURAL — THE CHECK IS INSIDE THE WINDOW IT CERTIFIES. A re-verification that drifted
#     back above the substitute's write would restore the reported-not-prevented shape while every
#     behavioural case above still passed (the injection anchors would move with it), so the
#     ORDER is pinned from source: the second observation must be taken AFTER the substitute is
#     committed at its fresh generation and BEFORE the stage record is published.
N1_SUBCOMMIT_LN="$(LC_ALL=C grep -n 'commit_write "\$new_rpath" report-of-record' "$RS" | LC_ALL=C head -1 | cut -d: -f1)"
N1_PUBLISH_LN="$(LC_ALL=C grep -n 'commit_write "\$sfile" stage-record' "$RS" | LC_ALL=C tail -1 | cut -d: -f1)"
N1_RECHECK_LN="$(LC_ALL=C grep -n 'report_bytes "\$STAGE_REPORT"' "$RS" | LC_ALL=C tail -1 | cut -d: -f1)"
if [ -n "$N1_SUBCOMMIT_LN" ] && [ -n "$N1_PUBLISH_LN" ] && [ -n "$N1_RECHECK_LN" ] &&
  [ "$N1_RECHECK_LN" -gt "$N1_SUBCOMMIT_LN" ] && [ "$N1_RECHECK_LN" -lt "$N1_PUBLISH_LN" ]; then
  ok "n1/structural: the re-observation is taken after the substitute is committed and BEFORE the publication (lines $N1_SUBCOMMIT_LN < $N1_RECHECK_LN < $N1_PUBLISH_LN)"
else
  bad "n1/structural: the re-observation is NOT between the substitute's commit and the publication (subcommit=$N1_SUBCOMMIT_LN recheck=$N1_RECHECK_LN publish=$N1_PUBLISH_LN)"
fi
# THE OVERWRITE MUST BE UNEXPRESSIBLE, NOT MERELY UNTAKEN (round 15, U1). The whole of U1 is that
# no write in this script has `$STAGE_REPORT` as its destination; a single line reintroducing one
# would restore the destructive shape while (g) above still passed, because (g) asserts about the
# generation the RECORD names.
N1_CLOBBER="$(LC_ALL=C grep -c -E '^[^#]*(prepare_write|commit_write) "\$STAGE_REPORT"' "$RS" || true)"
if [ "$N1_CLOBBER" = "0" ]; then
  ok "n1/structural: NO write in the script targets \$STAGE_REPORT — the report of record is never a rename destination"
else
  bad "n1/structural: $N1_CLOBBER write(s) target \$STAGE_REPORT: $(LC_ALL=C grep -n -E '^[^#]*(prepare_write|commit_write) "\$STAGE_REPORT"' "$RS")"
fi
# AND THE SUBSTITUTE'S DESTINATION IS *RESERVED*, never merely generated (round 12, R1): a
# predictable fresh name could land on a HISTORICAL report of this stage, destroying the audit
# trail through the other door.
if LC_ALL=C grep -q '^[^#]*reserve_report_path "\$issue" "\$kind" "\$dir"' "$RS" &&
  [ "$(LC_ALL=C grep -c '^[^#]*reserve_report_path "\$issue" "\$kind" "\$dir"' "$RS" || true)" = "2" ]; then
  ok "n1/structural: BOTH writers of a report path (open and record-author-performed) claim it through reserve_report_path"
else
  bad "n1/structural: record-author-performed does not claim its report path through reserve_report_path (found $(LC_ALL=C grep -c '^[^#]*reserve_report_path "\$issue" "\$kind" "\$dir"' "$RS" || true) call(s), want 2)"
fi
# AND THE REMAINING WINDOW IS DECLARED IN THE CODE, with its ACTUAL consequence. A comment naming
# it is what stops the next reader believing the publication is atomic.
if LC_ALL=C grep -q 'THE REMAINING WINDOW IS DECLARED' "$RS"; then
  ok "n1/structural: the remaining window is DECLARED in the source, not left implicit"
else
  bad "n1/structural: nothing in the source declares the remaining window"
fi
# AND ROUND 9's WITHDRAWN CLAIM MAY NOT SURVIVE ANYWHERE. It said a recorded verdict could be LOST
# in that span and that this was irreducible; it is neither, and a stale declaration is what stops
# the next person looking. Swept over the script, both boundary scanners' subjects, the suite and
# the doctrine sites.
# THE NEEDLES ARE SPLIT so this guard cannot match its own source line — the idiom
# `test_roborev_review_guard.sh` uses for the same reason: a self-matching scan reds on a healthy
# tree and is the guard agents learn to waive.
N1_STALE=0
N1_STALE_WHERE=""
N1_N1="one \`m""v\` wide"
N1_N2="one m""v wide"
N1_N3="RESIDUAL WINDOW, DECLARED BECAUSE IT ""CANNOT BE REMOVED"
n1_carries_withdrawn() { LC_ALL=C grep -qiF -e "$N1_N1" -e "$N1_N2" -e "$N1_N3" "$1"; }
N1_SWEPT=0
for N1_F in "$RS" "$SCRIPT_DIR/../flow/premerge-assert.sh" "${BASH_SOURCE[0]}" \
  "$SCRIPT_DIR/../../CLAUDE.md" "$SCRIPT_DIR/../../docs/development/review-stage-reporting.md"; do
  [ -f "$N1_F" ] || continue
  N1_SWEPT=$((N1_SWEPT + 1))
  if n1_carries_withdrawn "$N1_F"; then
    N1_STALE=$((N1_STALE + 1)); N1_STALE_WHERE="$N1_STALE_WHERE $N1_F"
  fi
done
if [ "$N1_STALE" = "0" ] && [ "$N1_SWEPT" -eq 5 ]; then
  ok "n1/structural: round 9's WITHDRAWN residual declaration (a narrow irreducible rename span in which a recorded verdict could be lost) survives nowhere ($N1_SWEPT site(s) swept)"
else
  bad "n1/structural: $N1_STALE of $N1_SWEPT swept site(s) still carry round 9's withdrawn residual declaration (want 0 of 5):$N1_STALE_WHERE"
fi
# A POSITIVE CONTROL, because a sweep that matches nothing is indistinguishable from a sweep that
# cannot match: a searcher needs one, and this repository has the incident where a scan built to
# close one blind spot shipped with its own and reported CLEAN on four real sites.
N1_PLANT="$T/n1-withdrawn-plant.md"
{
  printf 'prose that mentions the check, then the withdrawn claim:\n'
  printf 'the irreducible residual is %s and is declared at the check\n' "$N1_N1"
} >"$N1_PLANT" 2>/dev/null || true
if [ -f "$N1_PLANT" ] && n1_carries_withdrawn "$N1_PLANT"; then
  ok "n1/structural CONTROL: the sweep DOES find the withdrawn declaration when it is present"
else
  bad "n1/structural CONTROL: the sweep did not find a PLANTED copy of the withdrawn declaration — the clean result above proves nothing"
fi

# --- 22. AN AUDIT COUNTER AT ITS CEILING MUST NOT RESTART (round 9, N4) -----------
# THE FINDING (roborev job 382, N4). Round 8 bounded every value bash has to compare or add at ten
# decimal digits. `reopen-count` goes through `$(( prior + 1 ))`, so the MAXIMUM ACCEPTED value —
# `9999999999` — incremented to an ELEVEN-digit one, which `int_is_comparable` then rejects on the
# NEXT re-open, and the counter silently RESTARTED AT 1. An audit counter that resets is a false
# audit trail, which is this issue's own subject: `reopen-count` exists to be read beside
# `reopened-at:` when correlating a surviving report with a re-spawn.
#
# THE DISPOSITION IS SATURATION, NOT REFUSAL, and the reason is round 8's own ruling one bullet up:
# "an unusable counter falls back to the value an absent one gets; it is never a reason to refuse a
# spawn". Refusing a re-open because a COSMETIC audit field is at its ceiling would block real work
# over a number, and a guard that reds on correct input is the guard agents learn to waive. Held at
# the ceiling the value means "at least this many", it can never decrease, and both `open` and
# `status` RENDER it with a `+` so the reader is told which of the two it is.
R22="$(newrepo)"
rs "$R22" open c --issue 950 --agent spec-auditor
rc_is 0 "reopen/ceiling: the stage opens"
R22_REC="$R22/.review-stage/issue-950/c.stage"

# (a) THE DEFECT, at the boundary: a counter AT the maximum accepted value.
PLANT_FIELD "$R22_REC" reopen-count 9999999999 "reopen/ceiling: a counter at the 10-digit ceiling is planted"
rs "$R22" open c --issue 950 --agent spec-auditor --force
rc_is 0 "reopen/ceiling: the re-open still SUCCEEDS (a cosmetic counter is not a reason to refuse a spawn)"
FIELD_IS "reopen-count=9999999999+" "reopen/ceiling: OPEN-OK reports the value HELD at the ceiling, rendered '+' so it reads as AT LEAST"
has "AT ITS CEILING" "reopen/ceiling: and a note NAMES the ceiling rather than letting the number change silently"
if LC_ALL=C grep -q '^reopen-count: 9999999999$' "$R22_REC" 2>/dev/null; then
  ok "reopen/ceiling: the RECORD holds 9999999999 — neither an 11-digit value nor a restart"
else
  bad "reopen/ceiling: the record holds '$(LC_ALL=C sed -n 's/^reopen-count: //p' "$R22_REC" 2>/dev/null)' (want 9999999999)"
fi

# (b) THE HARM ITSELF: a SECOND re-open. Pre-fix the first force wrote an 11-digit value and this
#     one read it as incomparable and RESTARTED THE COUNTER AT 1 — the false audit trail.
rs "$R22" open c --issue 950 --agent spec-auditor --force
rc_is 0 "reopen/ceiling: a second re-open also succeeds"
FIELD_IS "reopen-count=9999999999+" "reopen/ceiling: and the counter is STILL at the ceiling, not restarted at 1"
hasnt "reopen-count=1 " "reopen/ceiling: the exact restart the finding names does not happen"
if LC_ALL=C grep -q '^reopen-count: 9999999999$' "$R22_REC" 2>/dev/null; then
  ok "reopen/ceiling: and the record still holds it after two re-opens"
else
  bad "reopen/ceiling: the record holds '$(LC_ALL=C sed -n 's/^reopen-count: //p' "$R22_REC" 2>/dev/null)' after two re-opens (want 9999999999)"
fi

# (c) `status` REPORTS WHAT THE RECORD HOLDS — the round's requirement that the chosen behaviour is
#     what both surfaces say. Before this round `status` did not report the counter at all.
rs "$R22" status c --issue 950
rc_is 0 "reopen/ceiling: status is readable"
FIELD_IS "reopen-count=9999999999+" "reopen/ceiling: STATUS reports the counter, rendered AT-LEAST at the ceiling"

# (d) CONTROL — ONE BELOW THE CEILING STILL COUNTS UP. Without this the assertions above are
#     satisfiable by a counter that is pinned for every input.
R22B="$(newrepo)"
rs "$R22B" open c --issue 951 --agent spec-auditor
R22B_REC="$R22B/.review-stage/issue-951/c.stage"
PLANT_FIELD "$R22B_REC" reopen-count 9999999998 "reopen/CONTROL: a counter ONE BELOW the ceiling is planted"
rs "$R22B" open c --issue 951 --agent spec-auditor --force
FIELD_IS "reopen-count=9999999999+" "reopen/CONTROL: it INCREMENTS to the ceiling (the counter still counts)"
# The note claims something SPECIFIC — that a counter was HELD rather than restarted — and nothing
# was held on this transition, so its absence is part of the property: a note that fired here would
# claim an event that did not happen (round 2's "an absence is not a claim", inverted).
hasnt "AT ITS CEILING" "reopen/CONTROL: and no HOLD is claimed for a transition that simply incremented"

# (e) CONTROL — AN ORDINARY COUNTER IS UNTOUCHED, and carries NO `+`: the marker must mean
#     something, so it may not appear on a value that can still increase.
R22C="$(newrepo)"
rs "$R22C" open c --issue 952 --agent spec-auditor
rs "$R22C" open c --issue 952 --agent spec-auditor --force
FIELD_IS "reopen-count=1" "reopen/CONTROL: an ordinary first re-open reports 1, with no at-least marker"
rs "$R22C" open c --issue 952 --agent spec-auditor --force
FIELD_IS "reopen-count=2" "reopen/CONTROL: and the second reports 2 — the ordinary path is unaffected"
rs "$R22C" status c --issue 952
FIELD_IS "reopen-count=2" "reopen/CONTROL: status agrees with the record on an ordinary counter"
hasnt "AT ITS CEILING" "reopen/CONTROL: and says nothing about a ceiling that was not reached"

# (f) AN INCOMPARABLE COUNTER: displayed VERBATIM, never compared, and never marked at-least —
#     round 8's disposition (the record's own text stays visible in the audit trail) unchanged.
PLANT_FIELD "$R22C/.review-stage/issue-952/c.stage" reopen-count 99999999999999999999 \
  "reopen/incomparable: a 20-digit counter is planted"
rs "$R22C" status c --issue 952
FIELD_IS "reopen-count=99999999999999999999" "reopen/incomparable: status DISPLAYS the record's own text, so a hand edit stays visible"
hasnt "99999999999999999999+" "reopen/incomparable: and does NOT mark it at-least, which would assert a comparison that never ran"

# (g) STRUCTURAL — ONE LITERAL FOR THE BOUND. The ceiling and the digit width are the same fact;
#     two literals is two places for it to drift, and a drift here would make the saturation
#     boundary and the acceptance boundary disagree.
if LC_ALL=C grep -q '^MAX_INT_VALUE=9999999999$' "$RS" &&
  LC_ALL=C grep -q '^MAX_INT_DIGITS="\?\${#MAX_INT_VALUE}"\?$' "$RS"; then
  ok "reopen/structural: the digit width is DERIVED from the ceiling value, so the two cannot drift"
else
  bad "reopen/structural: MAX_INT_DIGITS is not derived from MAX_INT_VALUE (grep found: $(LC_ALL=C grep -n '^MAX_INT_' "$RS" | LC_ALL=C tr '\n' ' '))"
fi

# --- 23. THE REPORT NONCE MUST BE RESERVED, NOT MERELY GENERATED (round 12, R1) ----
# THE FINDING (roborev job 386, R1). Round 6 replaced the SCANNED generation with a random nonce
# and, along with the scan, DELETED the existence belt. Deleting the scan was right — it raced.
# Deleting the reservation was not: `mktemp -u` invents a NAME and creates NOTHING, so a nonce that
# repeats a report already on disk (a historical report of the same stage) sends `open`'s
# `mv -f -T` straight over that report and REPUBLISHES its path in the record. The sentinel
# replaces a recorded verdict, and the superseded agent still holding that path can then write the
# CURRENT verdict — the exact property round 5's generation binding exists to prevent, reached with
# no concurrency at all.
#
# THE FIX IS AN ATOMIC CLAIM, NOT THE OLD SCAN. The old loop SELECTED a name by TESTING EXISTENCE
# and wrote to it later: two steps with a window between them. `reserve_report_path` creates the
# name under `set -C` (`O_CREAT|O_EXCL`), so the decision and the claim are ONE operation; a
# collision yields a FRESH random nonce, never the "next" one, and exhausting the bounded attempts
# is a NAMED refusal rather than a fallback to an unreserved name.
#
# THE COLLISION IS FORCED, NOT WAITED FOR. A real nonce repeat is astronomically unlikely, so the
# generator is driven from a FEED FILE in a SCRATCH COPY of the shipped script — the ARTIFACT is
# substituted, there is no settable seam in the shipped script (#3312's corollary for tests). Each
# feed line supplies one `mktemp -u`-shaped candidate; once the feed is empty the scratch copy
# falls back to the real generator, which is what makes the retry case observable.
R1_D="$T/r1"; mkdir -p "$R1_D"
R1_TAKEN=TAKENNONCE1
# `awk -v` PERFORMS ESCAPE PROCESSING on its value (round 7's measured harness defect), so both
# the anchor and the replacement travel through ENVIRON.
r1_build() {
  local dest="$1" feed="$2"
  R1_ANCHOR='cand="$(mktemp -u "$dir/.nonce.XXXXXXXXXX"' \
  R1_REPL='  cand="$(LC_ALL=C sed -n 1p "'"$feed"'" 2>/dev/null || true)"; if [ -n "$cand" ]; then LC_ALL=C sed -i 1d "'"$feed"'" 2>/dev/null || true; else cand="$(mktemp -u "$dir/.nonce.XXXXXXXXXX" 2>/dev/null || true)"; fi # R1_FEED_GENERATOR' \
  LC_ALL=C awk '
    BEGIN { a = ENVIRON["R1_ANCHOR"]; r = ENVIRON["R1_REPL"]; done = 0 }
    index($0, a) > 0 && done == 0 { print r; done = 1; next }
    { print }
  ' "$RS" >"$dest" 2>/dev/null || return 1
  [ -s "$dest" ] || return 1
  LC_ALL=C grep -q 'R1_FEED_GENERATOR' "$dest" || return 1
  # THE ORIGINAL GENERATOR LINE MUST BE GONE, or the feed would be ignored and every case below
  # would pass for the wrong reason (a scratch copy identical to the shipped script).
  LC_ALL=C grep -q 'cand="\$(mktemp -u "\$dir/\.nonce\.XXXXXXXXXX" 2>/dev/null || true)"$' "$dest" && return 1
  bash -n "$dest" 2>/dev/null || return 1
  return 0
}
# r1_feed <path> <n> — n identical TAKEN candidates, so the generator collides n times.
r1_feed() {
  local f="$1" n="$2" i=0
  : >"$f"
  while [ "$i" -lt "$n" ]; do
    i=$((i + 1))
    printf '/unused/.nonce.%s\n' "$R1_TAKEN" >>"$f"
  done
}

# (a) A NONCE THAT REPEATS AN EXISTING REPORT IS REFUSED, AND THE HISTORICAL REPORT SURVIVES.
#     The feed collides on every attempt, so the reservation can never succeed. This is the branch
#     that must NAME its refusal rather than fall back to an unreserved name.
R1_FEED_A="$R1_D/feed-a.txt"
r1_feed "$R1_FEED_A" 32
if r1_build "$R1_D/collide.sh" "$R1_FEED_A"; then
  ok "r1/collide: the feed-driven generator landed in the scratch copy (asserted, not assumed)"
else
  bad "r1/collide: the feed-driven generator did NOT land — the assertions below would be vacuous"
fi
R1A="$(newrepo)"
R1A_DIR="$R1A/.review-stage/issue-820"
mkdir -p "$R1A_DIR"
R1A_HIST="$R1A_DIR/c.$R1_TAKEN.md"
printf 'result: FINDINGS\n\n### [BLOCKER] the historical agent found this\n' >"$R1A_HIST"
OUT="$(cd "$R1A" && bash "$R1_D/collide.sh" open c --issue 820 --agent spec-auditor 2>&1)"; RC=$?
rc_is 2 "r1/collide: an open whose every nonce is already taken REFUSES"
has "reason=report-nonce-not-reserved" "r1/collide: the refusal NAMES the cause (a claim that could not be made, not a token that could not be generated)"
hasnt "reason=report-nonce-not-generated" "r1/collide: and does not report the generator failing, which is a different operator action"
OUT="$(cat "$R1A_HIST" 2>/dev/null || printf '<absent>\n')"; RC=0
has "### [BLOCKER] the historical agent found this" "r1/collide: the historical report is BYTE-INTACT — nothing was written over it"
hasnt "no report written" "r1/collide: and its recorded verdict was not replaced by a sentinel"
if [ ! -f "$R1A_DIR/c.stage" ]; then
  ok "r1/collide: no stage record was published, so no reader derives the historical path as current"
else
  bad "r1/collide: a stage record was published by a refused open (it names $(RECORD_NONCE "$R1A" 820 c))"
fi

# (b) A COLLISION RETRIES TO A FRESH NONCE. One TAKEN candidate, then the real generator — so the
#     first attempt collides and the second succeeds. This is the branch that must NOT refuse: a
#     guard that reds on a recoverable collision is the guard agents learn to waive.
R1_FEED_B="$R1_D/feed-b.txt"
r1_feed "$R1_FEED_B" 1
if r1_build "$R1_D/retry.sh" "$R1_FEED_B"; then
  ok "r1/retry: the feed-driven generator landed for the retry case"
else
  bad "r1/retry: the feed-driven generator did NOT land for the retry case"
fi
R1B="$(newrepo)"
R1B_DIR="$R1B/.review-stage/issue-821"
mkdir -p "$R1B_DIR"
R1B_HIST="$R1B_DIR/c.$R1_TAKEN.md"
printf 'result: FINDINGS\n\n### [BLOCKER] the historical agent found this too\n' >"$R1B_HIST"
OUT="$(cd "$R1B" && bash "$R1_D/retry.sh" open c --issue 821 --agent spec-auditor 2>&1)"; RC=$?
rc_is 0 "r1/retry: the open SUCCEEDS — one collision is retried, not refused"
R1B_PRINTED="$(printed_report_path)"
if [ -n "$R1B_PRINTED" ] && [ "$R1B_PRINTED" != "$R1B_HIST" ]; then
  ok "r1/retry: the published report is a FRESH path, not the taken one"
else
  bad "r1/retry: the open republished '$R1B_PRINTED', which IS the historical report — a superseded agent holding it can write the current verdict"
fi
if [ "$(REPORT_OF "$R1B" 821 c)" = "$R1B_PRINTED" ]; then
  ok "r1/retry: and the stage record names that same fresh report"
else
  bad "r1/retry: the record names '$(REPORT_OF "$R1B" 821 c)' but the clause printed '$R1B_PRINTED'"
fi
OUT="$(cat "$R1B_HIST" 2>/dev/null || printf '<absent>\n')"; RC=0
has "### [BLOCKER] the historical agent found this too" "r1/retry: the historical report is untouched by the retry"
hasnt "no report written" "r1/retry: and was not overwritten with a sentinel"

# (c) CONTROL — THE SAME SCRATCH MACHINERY WITH AN EMPTY FEED STILL OPENS AND RE-OPENS. Without
#     this the refusal in (a) is satisfiable by a scratch copy that is simply broken, or by a
#     reservation that refuses every input.
R1_FEED_C="$R1_D/feed-c.txt"
: >"$R1_FEED_C"
if r1_build "$R1_D/control.sh" "$R1_FEED_C"; then
  ok "r1/CONTROL: the scratch copy built with an empty feed"
else
  bad "r1/CONTROL: the scratch copy did NOT build with an empty feed"
fi
R1C="$(newrepo)"
OUT="$(cd "$R1C" && bash "$R1_D/control.sh" open c --issue 822 --agent spec-auditor 2>&1)"; RC=$?
rc_is 0 "r1/CONTROL: an ordinary open still succeeds through the scratch copy"
R1C_P1="$(printed_report_path)"
OUT="$(cd "$R1C" && bash "$R1_D/control.sh" open c --issue 822 --agent spec-auditor --force 2>&1)"; RC=$?
rc_is 0 "r1/CONTROL: and an ordinary --force re-open still succeeds"
R1C_P2="$(printed_report_path)"
if [ -n "$R1C_P1" ] && [ -n "$R1C_P2" ] && [ "$R1C_P1" != "$R1C_P2" ]; then
  ok "r1/CONTROL: the re-open is handed a DIFFERENT path, so round 5's generation binding is intact"
else
  bad "r1/CONTROL: open and re-open both reported '$R1C_P1'"
fi

# (d) CONTROL — A SUPERSEDED AGENT'S HELD PATH IS STILL DEAD, through the SHIPPED script. The
#     reservation must not have turned a fresh path back into a reused one: the whole reason
#     `open --force` moves the path is that the previous, idle agent returns LATE and writes its
#     old-tree verdict wherever it was told to.
R1E="$(newrepo)"
rs "$R1E" open c --issue 823 --agent spec-auditor
rc_is 0 "r1/superseded: the first stage opens"
R1E_OLD="$(printed_report_path)"
rs "$R1E" open c --issue 823 --agent spec-auditor --force
rc_is 0 "r1/superseded: the forced re-open succeeds"
R1E_NEW="$(printed_report_path)"
if [ -n "$R1E_OLD" ] && [ "$R1E_OLD" != "$R1E_NEW" ]; then
  ok "r1/superseded: the re-open moved the report path"
else
  bad "r1/superseded: the re-open reported the same path '$R1E_OLD'"
fi
printf 'result: PASS\n\nthe superseded agent, waking up late\n' >"$R1E_OLD"
rs "$R1E" verdict c --issue 823
rc_is 5 "r1/superseded: the current verdict is the fresh stage's NON-VERDICT, not the late PASS"
has "no report written" "r1/superseded: and it names the sentinel cause"
hasnt "RESULT: PASS" "r1/superseded: the superseded agent's PASS is not reported as the current verdict"

# (e) STRUCTURAL — THE CLAIM IS ATOMIC, AND IT HAPPENS BEFORE ANYTHING IS WRITTEN. A behavioural
#     case cannot see WHICH operation made the claim, and a reservation that drifted after
#     `prepare_write` would restore the clobber while (a) and (b) still passed.
R1_RES_BODY="$(LC_ALL=C sed -n '/^reserve_report_path() {$/,/^}$/p' "$RS")"
if [ -n "$R1_RES_BODY" ]; then
  ok "r1/structural: the reservation function was located in the shipped script"
else
  bad "r1/structural: could not locate reserve_report_path() — the assertions below would be vacuous"
fi
case "$R1_RES_BODY" in
  *'set -C'*) ok "r1/structural: the claim is made under set -C, i.e. O_CREAT|O_EXCL — one operation, so there is no window between deciding and claiming" ;;
  *) bad "r1/structural: the reservation does not create under set -C, so the name is not claimed atomically" ;;
esac
case "$R1_RES_BODY" in
  *'[ -f '* | *'[ -e '* | *'[ -L '* | *' ls '*)
    bad "r1/structural: the reservation TESTS EXISTENCE — that is round 6's scan, which selected a name in one step and wrote it in another" ;;
  *) ok "r1/structural: the reservation makes no existence TEST, so it is a claim and not a selection" ;;
esac
case "$R1_RES_BODY" in
  *'RESERVE_ATTEMPTS'*) ok "r1/structural: the retry is BOUNDED, so an unwritable directory refuses instead of spinning" ;;
  *) bad "r1/structural: the retry has no declared bound" ;;
esac
if LC_ALL=C grep -q '^RESERVE_ATTEMPTS=[0-9][0-9]*$' "$RS"; then
  ok "r1/structural: the bound is ONE literal at the top level, so the loop and the refusal cannot name two different numbers"
else
  bad "r1/structural: RESERVE_ATTEMPTS is not a single top-level literal (grep: $(LC_ALL=C grep -n 'RESERVE_ATTEMPTS=' "$RS" | LC_ALL=C tr '\n' ' '))"
fi
R1_RESERVE_LN="$(LC_ALL=C grep -n 'reserve_report_path "\$issue" "\$kind" "\$dir"' "$RS" | LC_ALL=C head -1 | cut -d: -f1)"
R1_PREP_LN="$(LC_ALL=C grep -n 'prepare_write "\$rpath" report-of-record' "$RS" | LC_ALL=C head -1 | cut -d: -f1)"
if [ -n "$R1_RESERVE_LN" ] && [ -n "$R1_PREP_LN" ] && [ "$R1_RESERVE_LN" -lt "$R1_PREP_LN" ]; then
  ok "r1/structural: the name is claimed BEFORE the report is written (lines $R1_RESERVE_LN < $R1_PREP_LN)"
else
  bad "r1/structural: the reservation is not ahead of the write (reserve=$R1_RESERVE_LN prepare=$R1_PREP_LN)"
fi
# AND THE COMMENT SAYS WHY THIS IS NOT THE ROUND-6 SCAN RETURNING. Round 6's doctrine recorded the
# existence belt as DELETED; that claim is now false, and a reader who finds a create-if-absent
# loop here without the distinction will read it as a regression and remove it again.
if LC_ALL=C grep -q 'WHY THIS IS NOT THE ROUND-6 SCAN COMING BACK' "$RS"; then
  ok "r1/structural: the source states why an atomic claim is not the deleted scan"
else
  bad "r1/structural: nothing in the source distinguishes this claim from round 6's deleted scan"
fi
# (f) THE RESERVATION IS AN OWNED RESOURCE, SO IT IS REGISTERED WITH THE CLEANUP PATH. Round 17 of
#     #3544's review recorded the third instance of one family — a fix that adds a resource
#     inherits that resource's lifetime bugs — so the pairing is pinned rather than trusted: the
#     name is registered the moment it exists, de-registered the moment real content holds it, and
#     reaped by the SAME `trap` that reaps the temporary file (two handlers behind ONE
#     registration, because bash keeps only the last `trap … EXIT`).
case "$R1_RES_BODY" in
  *'RESERVED_PATH="$cand"'*) ok "r1/lifetime: the claimed name is registered for cleanup inside the reservation itself" ;;
  *) bad "r1/lifetime: the reservation does not register the name it created, so a refused open leaks it" ;;
esac
if LC_ALL=C grep -q "^trap 'cleanup_write_tmp; cleanup_reserved_path' EXIT\$" "$RS"; then
  ok "r1/lifetime: ONE trap reaps BOTH owned artifacts, so neither registration can silently replace the other"
else
  bad "r1/lifetime: the reservation is not reaped by the same trap as the temporary file (trap lines: $(LC_ALL=C grep -n '^trap ' "$RS" | LC_ALL=C tr '\n' ' '))"
fi
R1_DEREG_LN="$(LC_ALL=C grep -n '\[ "\$dest" != "\$RESERVED_PATH" \]' "$RS" | LC_ALL=C head -1 | cut -d: -f1)"
if [ -n "$R1_DEREG_LN" ]; then
  ok "r1/lifetime: and it is de-registered on fulfilment, in commit_write, so the cleanup cannot delete the PUBLISHED report"
else
  bad "r1/lifetime: nothing de-registers the reservation once the report is written — the EXIT trap would delete it"
fi
# BEHAVIOURAL, and it is section 11(g)'s case doing the measuring: an open that reserves and then
# REFUSES (a repository that ignores the records by EXTENSION, so the report path is ignored and
# the temporary file beside it is not) must leave the stage directory EMPTY. Without the cleanup
# above that case reds with an orphaned `c.<nonce>.md`, which is why it is named here rather than
# duplicated: an empty file at a report path nothing published is indistinguishable from a crashed
# write, the same reason `commit_write` removes its own temporary file.
R1F="$(newrepo '.review-stage/**/*.md
.review-stage/**/*.stage')"
rs "$R1F" open c --issue 824 --agent spec-auditor
rc_is 2 "r1/lifetime: an open that reserves and then refuses is still a refusal"
if [ -z "$(ls -A "$R1F/.review-stage/issue-824" 2>/dev/null)" ]; then
  ok "r1/lifetime: and it leaves NO reserved name behind — the tree is as it was found"
else
  bad "r1/lifetime: the refused open leaked $(ls -A "$R1F/.review-stage/issue-824" 2>/dev/null)"
fi


# --- 24. THE VERDICT MUST DESCRIBE A STATE THAT EXISTED (round 12, R2) -------------
# THE FINDING (roborev job 386, R2). `classify_report` read the report EIGHT times — existence, a
# readability probe, the body for emptiness, the `result:` census, the disclosure, then
# `performed-by`, `reason` and `evidence` each through their own `read_field` — so a report
# REPLACED between two of those reads let the classifier combine fields drawn from DIFFERENT,
# INDIVIDUALLY INVALID versions and emit `AUTHOR-PERFORMED` even though NO SINGLE SNAPSHOT of the
# file ever contained valid working. A verdict is a statement about a document; assembled from two
# documents it is a statement about neither.
#
# THE FIX IS ONE OBSERVATION, which is round 9's N2 property (`premerge-assert.sh` reads the stage
# record once and parses every field from that capture) applied one level down to the REPORT.
#
# IT IS A SIMULATED INTERLEAVE, NOT A RACE. Nothing below is concurrent: one line injected into a
# SCRATCH COPY of the shipped script swaps the file at a NAMED field read, so the ordering is
# deterministic, the case cannot flake, and it makes no claim about timing. The ARTIFACT is
# substituted — there is no settable seam in the shipped script (#3312's corollary for tests).
R2_D="$T/r2"; mkdir -p "$R2_D"
# The injected line fires for ONE field key, chosen per case through the environment of the SCRATCH
# copy. `awk -v` performs escape processing on its value (round 7's measured harness defect), so
# both the anchor and the replacement travel through ENVIRON.
#
# THE ANCHOR IS THE FIELD-GRAMMAR ENTRY (`key="$2"`, FIRST occurrence), which is deliberately the
# ONE line present in BOTH the pre-fix and post-fix scripts — pre-fix that is `read_field`, which
# re-read the FILE per field, and post-fix it is `read_field_from`, which reads the caller's
# SNAPSHOT. So the same plant lands either way and the difference it produces is the property: the
# swap changes the file, and after the fix the file is not what the fields come from.
r2_build() {
  local dest="$1"
  R2_ANCHOR='key="$2"' \
  R2_INJ='  if [ "$key" = "${R2_SWAP_KEY:-}" ] && [ -n "${R2_SWAP_SRC:-}" ] && [ -n "${R2_SWAP_DST:-}" ]; then cp -f "$R2_SWAP_SRC" "$R2_SWAP_DST" 2>/dev/null || true; fi # R2_INTERLEAVE' \
  LC_ALL=C awk '
    BEGIN { a = ENVIRON["R2_ANCHOR"]; inj = ENVIRON["R2_INJ"]; done = 0 }
    { print }
    index($0, a) > 0 && done == 0 { print inj; done = 1 }
  ' "$RS" >"$dest" 2>/dev/null || return 1
  [ -s "$dest" ] || return 1
  LC_ALL=C grep -q 'R2_INTERLEAVE' "$dest" || return 1
  bash -n "$dest" 2>/dev/null || return 1
  return 0
}
R2_PROG="$R2_D/interleave.sh"
if r2_build "$R2_PROG"; then
  ok "r2: the interleave plant landed in the scratch copy (asserted, not assumed)"
else
  bad "r2: the interleave plant did NOT land, so the assertions below would be vacuous"
fi
R2_GOOD_REASON='no peer agent available on this box; hand C against the spec deltas'
R2_GOOD_EV='docs/round-artifacts/issue-3751-hand-c.md'
# The disclosure VERBATIM. A literal here rather than a read of the shipped script: the classifier
# must require the exact sentence, and a fixture derived from the artifact under test would agree
# with it however that sentence changed (an oracle sharing a source with its subject).
R2_DISCLOSURE="an author's hand audit is not an independent one; weight it accordingly"

# THE TWO VERSIONS. Each carries the verbatim disclosure and a column-zero `result:` line, and each
# is INDIVIDUALLY INVALID — which is asserted below, not assumed, because a refusal that came from
# a broken fixture would prove nothing.
R2_VA="$R2_D/version-a.md"
{
  printf 'result: AUTHOR-PERFORMED\n\n'
  printf 'performed-by: author\n'
  printf 'reason: %s\n' "$R2_GOOD_REASON"
  printf 'evidence: tbd\n\n'
  printf '%s\n' "$R2_DISCLOSURE"
} >"$R2_VA"
R2_VB="$R2_D/version-b.md"
{
  printf 'result: AUTHOR-PERFORMED\n\n'
  printf 'performed-by: author\n'
  printf 'reason: x\n'
  printf 'evidence: %s\n\n' "$R2_GOOD_EV"
  printf '%s\n' "$R2_DISCLOSURE"
} >"$R2_VB"
# A THIRD version, for case (b): it records a NON-VERDICT and carries usable working, so the token
# and the working provably come from different documents.
R2_VC="$R2_D/version-c.md"
{
  printf 'result: NOT-RUN (the auditor could not read the diff)\n\n'
  printf 'performed-by: author\n'
  printf 'reason: %s\n' "$R2_GOOD_REASON"
  printf 'evidence: %s\n\n' "$R2_GOOD_EV"
  printf '%s\n' "$R2_DISCLOSURE"
} >"$R2_VC"

# r2_alone <version-file> <issue> <label> — the SHIPPED script's verdict for ONE version, standing
# alone. This is the premise of every interleave case: if a version alone already reached the
# merge-proceeding token, the interleave would be measuring nothing.
r2_alone() {
  local ver="$1" issue="$2" label="$3" repo rep
  repo="$(newrepo)"
  rs "$repo" open c --issue "$issue" --agent spec-auditor
  rep="$(printed_report_path)"
  cp -f "$ver" "$rep"
  rs "$repo" verdict c --issue "$issue"
  if [ "$RC" -ne 6 ]; then
    ok "r2/premise: $label ALONE does not reach AUTHOR-PERFORMED (rc=$RC)"
  else
    bad "r2/premise: $label ALONE already reaches AUTHOR-PERFORMED, so the interleave case proves nothing"
  fi
}
r2_alone "$R2_VA" 830 "version A (good reason, placeholder evidence)"
r2_alone "$R2_VB" 831 "version B (placeholder reason, good evidence)"
r2_alone "$R2_VC" 832 "version C (a recorded NON-VERDICT with usable working)"

# (a) THE WORKING ASSEMBLED FROM TWO VERSIONS. A is installed; the swap to B fires at the
#     `evidence` read, so `performed-by` and `reason` come from A and `evidence` from B — a
#     complete set of valid working that NO SINGLE SNAPSHOT ever held.
R2A="$(newrepo)"
rs "$R2A" open c --issue 833 --agent spec-auditor
rc_is 0 "r2/assembled: the stage opens"
R2A_REP="$(printed_report_path)"
cp -f "$R2_VA" "$R2A_REP"
OUT="$(cd "$R2A" && R2_SWAP_KEY=evidence R2_SWAP_SRC="$R2_VB" R2_SWAP_DST="$R2A_REP" \
  bash "$R2_PROG" verdict c --issue 833 2>&1)"; RC=$?
if [ "$RC" -ne 6 ]; then
  ok "r2/assembled: a verdict is NOT assembled from fields of two different versions (rc=$RC)"
else
  bad "r2/assembled: AUTHOR-PERFORMED was reported from working no single snapshot held (out: $OUT)"
fi
hasnt "RESULT: AUTHOR-PERFORMED" "r2/assembled: the merge-proceeding token is not reported"
has "AUTHOR-PERFORMED" "r2/assembled: and the cause still NAMES the token the report asserted, so the operator knows which field to fix"

# (b) THE TOKEN AND THE WORKING FROM DIFFERENT DOCUMENTS. A is installed (AUTHOR-PERFORMED with
#     unusable working); the swap to C fires at the FIRST field read, so the token and the
#     disclosure come from A and every field from C — a document that recorded a NON-VERDICT.
R2B="$(newrepo)"
rs "$R2B" open c --issue 834 --agent spec-auditor
rc_is 0 "r2/two-documents: the stage opens"
R2B_REP="$(printed_report_path)"
cp -f "$R2_VA" "$R2B_REP"
OUT="$(cd "$R2B" && R2_SWAP_KEY=performed-by R2_SWAP_SRC="$R2_VC" R2_SWAP_DST="$R2B_REP" \
  bash "$R2_PROG" verdict c --issue 834 2>&1)"; RC=$?
if [ "$RC" -ne 6 ]; then
  ok "r2/two-documents: a token read from one version is not validated by another version's working (rc=$RC)"
else
  bad "r2/two-documents: AUTHOR-PERFORMED was reported for a document that recorded a NON-VERDICT (out: $OUT)"
fi
hasnt "RESULT: AUTHOR-PERFORMED" "r2/two-documents: the merge-proceeding token is not reported"

# (c) CONTROL — THE SAME SCRATCH MACHINERY, NO SWAP, STILL REACHES THE TOKEN. Without this, (a)
#     and (b) are satisfiable by a scratch copy that is simply broken, or by a classifier that
#     refuses every AUTHOR-PERFORMED report.
R2_VALID="$R2_D/version-valid.md"
{
  printf 'result: AUTHOR-PERFORMED\n\n'
  printf 'performed-by: author\n'
  printf 'reason: %s\n' "$R2_GOOD_REASON"
  printf 'evidence: %s\n\n' "$R2_GOOD_EV"
  printf '%s\n' "$R2_DISCLOSURE"
} >"$R2_VALID"
R2C="$(newrepo)"
rs "$R2C" open c --issue 835 --agent spec-auditor
R2C_REP="$(printed_report_path)"
cp -f "$R2_VALID" "$R2C_REP"
OUT="$(cd "$R2C" && bash "$R2_PROG" verdict c --issue 835 2>&1)"; RC=$?
rc_is 6 "r2/CONTROL: an UNDISTURBED valid substitute still reaches AUTHOR-PERFORMED through the scratch copy"
has "RESULT: AUTHOR-PERFORMED" "r2/CONTROL: the token really is reachable, so the refusals above are about the interleave"
# AND THROUGH THE SHIPPED SCRIPT, so the single-observation read did not red a correct report.
R2S="$(newrepo)"
rs "$R2S" open c --issue 836 --agent spec-auditor
cp -f "$R2_VALID" "$(printed_report_path)"
rs "$R2S" verdict c --issue 836
rc_is 6 "r2/CONTROL: and the SHIPPED script reads the same valid substitute as AUTHOR-PERFORMED"

# (d) EVERY OTHER CAUSE STILL COMES FROM THE ONE SNAPSHOT — the reads that were CONSOLIDATED, not
#     just the AUTHOR-PERFORMED ones. Each is asserted through the SHIPPED script, because
#     replacing eight reads with one is exactly the change that could silently move a cause.
R2E="$(newrepo)"
rs "$R2E" open c --issue 837 --agent spec-auditor
R2E_REP="$(printed_report_path)"
rm -f "$R2E_REP"
rs "$R2E" verdict c --issue 837
rc_is 5 "r2/causes: a deleted report is still NOT-RUN"
has "report absent" "r2/causes: and still names 'report absent', not a state derived from an empty read"
printf '   \n\n\t\n' >"$R2E_REP"
rs "$R2E" verdict c --issue 837
has "report empty" "r2/causes: a whitespace-only report is still 'report empty'"
printf 'no verdict line here at all\n' >"$R2E_REP"
rs "$R2E" verdict c --issue 837
has "no 'result:' line" "r2/causes: a report with no record still names the missing line"
printf 'result: PASS\nresult: FINDINGS\n' >"$R2E_REP"
rs "$R2E" verdict c --issue 837
has "2 column-zero 'result:' lines" "r2/causes: two records are still AMBIGUOUS, counted from the snapshot"
printf 'result: PASS\n\nreviewed.\n' >"$R2E_REP"
rs "$R2E" verdict c --issue 837
rc_is 0 "r2/causes: and a real PASS is still a PASS (the positive control for the consolidated read)"
# --- 25. AN UNREADABLE PRIOR VERDICT IS *UNKNOWN*, NOT *REPLACEABLE* (round 13, S1) ----
# THE FINDING (roborev job 387, S1). Round 12's R2 correctly gave the classifier an UNREADABLE
# observation state — but `record-author-performed`'s clobber guard branches on the TOKEN, and an
# unreadable report classifies as `NOT-RUN`, which is the guard's REPLACEABLE side. So a report
# whose recorded verdict was UNKNOWN — possibly a blocking `FINDINGS` — was overwritten by the
# merge-proceeding `AUTHOR-PERFORMED` token with NO `--force` and NO `replaced-verdict:` trace.
# Measured against the shipped script at 5e3b51a74: a mode-000 report holding `result: FINDINGS`
# yielded `RECORD-OK ... result=AUTHOR-PERFORMED`, exit 0, and the findings text was gone.
#
# That is this repository's central rule violated INSIDE its own mechanism: "cannot tell" must
# never take the permissive branch, and *unknown* is not *absent*. The guard's permissive side is
# now keyed AFFIRMATIVELY on the two states that were MEASURED — `absent` (verified-absent: there
# is no recorded verdict to destroy) and `present` (readable: the token decides) — so any state
# that does not affirmatively say the report was read refuses, and a state added later joins the
# refusing side by construction rather than by someone remembering to add an arm.
#
# `--force` DELIBERATELY DOES NOT COVER IT, and the reason is the same one round 9's
# re-verification gives: `--force` authorizes replacing THE VERDICT THE OPERATOR READ, and nobody
# read this one. Refusing strands nobody — `open <kind> --force` moves the stage to a fresh report
# at a fresh nonce and leaves the unreadable file on disk as history — which is asserted in
# section 26, on a subject every host has.
R25="$(newrepo)"
rs "$R25" open c --issue 950 --agent spec-auditor
rc_is 0 "s1: the stage opens"
R25_REPORT="$(REPORT_OF "$R25" 950 c)"
S1_REASON='no peer agent available on this box; hand C against the spec deltas'
S1_EV='docs/round-artifacts/issue-950-hand-c.md'
printf 'result: FINDINGS\n\n### [BLOCKER] a real gap the substitute would have erased\n' >"$R25_REPORT"
# THE BAIT IS ASSERTED VALID FIRST (round 9's lesson): a refusal from a broken fixture proves
# nothing, so the recorded blocking verdict is READ before it is made unreadable.
rs "$R25" verdict c --issue 950
rc_is 4 "s1 PREMISE: the report records a real, readable, BLOCKING verdict before the attempt"
chmod 000 "$R25_REPORT" 2>/dev/null || true

# THE PRECONDITION IS MEASURED BY ATTEMPTING THE READ, not by `[ -r ]` (which answers TRUE for
# root) — section 11b's idiom. BOTH branches execute ELEVEN assertions, so the suite's EXACT case
# floor stays host-independent, and the branch with no subject asserts what IS true there rather
# than passing silently.
if ( : <"$R25_REPORT" ) 2>/dev/null; then
  ok "s1: DECLARED GAP — this host still reads a mode-000 file (running as root, or a filesystem ignoring mode bits), so the UNREADABLE prior state has NO SUBJECT here; the eleven assertions below assert what IS true on such a host, namely that the recorded-verdict guard covers this file instead"
  rs "$R25" verdict c --issue 950
  rc_is 4 "s1 (no-subject host): the report IS readable, so its own content decides — FINDINGS"
  has "RESULT: FINDINGS " "s1 (no-subject host): the token is the report's own"
  rs "$R25" record-author-performed c --issue 950 --reason "$S1_REASON" --evidence "$S1_EV" --performed-by author
  rc_is 2 "s1 (no-subject host): the recording is REFUSED without --force"
  has "reason=verdict-already-recorded" "s1 (no-subject host): refused by the RECORDED-VERDICT guard, which is the one with a subject here"
  has "recorded-verdict=FINDINGS" "s1 (no-subject host): the refusal names the prior token"
  hasnt "RECORD-OK" "s1 (no-subject host): nothing was recorded"
  rs "$R25" record-author-performed c --issue 950 --reason "$S1_REASON" --evidence "$S1_EV" --performed-by author --force
  rc_is 0 "s1 (no-subject host): --force IS legitimate over a verdict that WAS read"
  has "replaced-verdict=FINDINGS" "s1 (no-subject host): and the replacement is traced"
  OUT="$(cat "$R25_REPORT" 2>&1)"; RC=0
  has "result: AUTHOR-PERFORMED" "s1 (no-subject host): the report now records the substitute"
  has "replaced-verdict: FINDINGS" "s1 (no-subject host): with the destroyed token recorded in it"
else
  ok "s1: the precondition holds — this host cannot read the mode-000 report (MEASURED by attempting the read, not by [ -r ], which answers TRUE for root)"
  rs "$R25" verdict c --issue 950
  rc_is 5 "s1 PREMISE: the prior verdict is now UNKNOWN — the classifier reports it unreadable"
  has "NOT-RUN (report unreadable)" "s1 PREMISE: and names that state, not 'no report written'"
  rs "$R25" record-author-performed c --issue 950 --reason "$S1_REASON" --evidence "$S1_EV" --performed-by author
  rc_is 2 "s1: a recording over an UNREADABLE prior report is REFUSED (exit 2), not treated as an ordinary NOT-RUN"
  has "AUTHOR-REFUSED reason=prior-verdict-unreadable" "s1: refused BY NAME, distinctly from verdict-already-recorded — the operator action differs (make the file readable, or open a fresh stage)"
  has "prior-state=unreadable" "s1: and the refusal names the STATE that could not be read"
  hasnt "RECORD-OK" "s1: nothing was recorded"
  rs "$R25" record-author-performed c --issue 950 --reason "$S1_REASON" --evidence "$S1_EV" --performed-by author --force
  rc_is 2 "s1: --force does NOT cover it — it authorizes replacing the verdict you READ, and nobody read this one"
  has "AUTHOR-REFUSED reason=prior-verdict-unreadable" "s1: the forced attempt is refused under the SAME cause"
  chmod 644 "$R25_REPORT" 2>/dev/null || true
  OUT="$(cat "$R25_REPORT" 2>&1)"; RC=0
  has "result: FINDINGS" "s1: the report is INTACT after both attempts — the blocking verdict was not destroyed"
  hasnt "AUTHOR-PERFORMED" "s1: and the merge-proceeding token was never written into it"
fi

# CONTROL, ON EVERY HOST: a VERIFIED-ABSENT report is still freely replaceable. `absent` is an
# affirmative measurement — there is no recorded verdict to destroy — so the permissive side is
# not "everything that is not present", it is a named state. A guard that reds here would red on
# correct input, which is the guard agents learn to waive.
R25B="$(newrepo)"
rs "$R25B" open c --issue 951 --agent spec-auditor
rc_is 0 "s1 CONTROL: a second stage opens"
rm -f "$(REPORT_OF "$R25B" 951 c)"
rs "$R25B" verdict c --issue 951
rc_is 5 "s1 CONTROL: the deleted report is measured ABSENT"
has "NOT-RUN (report absent)" "s1 CONTROL: and named absent, not unreadable"
rs "$R25B" record-author-performed c --issue 951 --reason "$S1_REASON" --evidence "$S1_EV" --performed-by author
rc_is 0 "s1 CONTROL: recording over an ABSENT report needs no --force — nothing is destroyed"
has "RECORD-OK" "s1 CONTROL: the normal path is unaffected"
hasnt "replaced-verdict" "s1 CONTROL: nothing was replaced, so no replacement is claimed"

# STRUCTURAL: ONE READER OF THE OBSERVATION GRAMMAR, AND THE PERMISSIVE SET IS AFFIRMATIVE.
# The state word is what both the classifier and the clobber guard branch on, and they had no
# shared reader of it — the classifier matched `report_bytes`' prefixes itself while the guard did
# not look at the state at all. A second reader of that grammar is a second opinion about whether
# a report was READ, which is exactly the divergence this finding is.
S25_SRC="$(cat "$RS" 2>/dev/null || true)"
case "$S25_SRC" in
  *"report_state() {"*) ok "s1/structural: report_state() is the named reader of the observation state" ;;
  *) bad "s1/structural: could not locate report_state() — the assertions below would be vacuous" ;;
esac
S25_CLS="$(LC_ALL=C sed -n '/^classify_report() {/,/^}$/p' "$RS" 2>/dev/null || true)"
case "$S25_CLS" in
  *'report_state "$obs"'*) ok "s1/structural: the classifier reads the state THROUGH that helper" ;;
  *) bad "s1/structural: the classifier matches the observation grammar itself, so it and the clobber guard can form two opinions about whether the report was read" ;;
esac
S25_RAP="$(LC_ALL=C sed -n '/^cmd_record_author_performed() {/,/^}$/p' "$RS" 2>/dev/null || true)"
# RETARGETED IN ROUND 17 (W1), AND STRICTLY STRONGER. This pinned that the clobber guard called
# `report_state "$prior_obs"` itself, i.e. that it consulted the STATE at all. Since W1 the guard
# reads nothing: both the bytes and their state come from the ONE `observe_stage` observation, so
# the property to pin is that BINDING — a guard that re-derived the state from a read of its own
# would be the second observation W1 removes, and one that ignored the state would be round 13's S1
# again. Both halves are asserted, and the observer is pinned as the caller that derives it through
# the named reader.
case "$S25_RAP" in
  *'prior_state="$STAGE_REPORT_STATE"'*) ok "s1/structural: the clobber guard takes the observation STATE from the ONE observation, not from a read of its own" ;;
  *) bad "s1/structural: the clobber guard does not consult the observation STATE, so 'could not read it' is indistinguishable from 'nothing is recorded'" ;;
esac
S25_OBS="$(LC_ALL=C sed -n '/^observe_stage() {/,/^}$/p' "$RS" 2>/dev/null || true)"
case "$S25_OBS" in
  *'report_state "$STAGE_REPORT_OBS"'*) ok "s1/structural: and the observation derives it through report_state, the ONE named reader of that grammar" ;;
  *) bad "s1/structural: the observation does not derive the report state through report_state, so two readers of that grammar can form two opinions" ;;
esac
case "$S25_RAP" in
  *'absent | present)'*) ok "s1/structural: and its permissive set is keyed AFFIRMATIVELY on the two measured states, so a state added later refuses by construction" ;;
  *) bad "s1/structural: the guard's permissive branch is not an affirmative match on absent|present — a '!= unreadable' test lets every future state through" ;;
esac

# --- 26. A CAPTURE THAT NORMALISES ITS INPUT CANNOT BE THE THING THAT VALIDATES IT (round 13, S2) --
# THE FINDING (roborev job 387, S2). Every read of an untrusted file in this tool goes through a
# COMMAND SUBSTITUTION, and bash SILENTLY DISCARDS NUL bytes there (bash 5.2 emits a warning on
# stderr, which every call site here redirects to /dev/null, so it is silent in practice). So the
# capture did not merely LOSE information — it MANUFACTURED grammar the file does not contain:
#
#   $ printf 'res\0ult: PASS\n' > "$report"      # the file holds NO column-zero result: line
#   $ LC_ALL=C grep -c '^result:' "$report"       # -> rc 1, no match: measured, not argued
#   $ review-stage.sh verdict c --issue 901       # -> RESULT: PASS, exit 0
#
# and one file over, the same idiom in `read_field` REDIRECTED A READER: a stage record whose
# `report-nonce:` value was `STALE<NUL>PASS1` is NOT a valid nonce token, yet the capture read it
# as the valid `STALEPASS1`, so `verdict` reported a STALE report's `PASS` for a stage whose own
# current report held the sentinel — round 4's H2 defect (a data file redirecting a reader)
# reached through the capture instead of through `--report`.
#
# THE FIX IS IN THE READ, NOT IN A PROBE. A separate `grep -q`/`wc -c` probe of the same path is a
# SECOND observation, and one direction of its disagreement is a FALSE PASS (the capture reads the
# NUL-bearing version while the probe reads a clean one) — round 12's R2 lesson exactly. So the ONE
# read maps NUL to SOH IN THE STREAM: nothing is lost (the length is preserved), the forged grammar
# is never created (`res<SOH>ult:` is not a record), and the byte's PRESENCE is observable, so the
# refusal can NAME it instead of silently judging a transformed document.
R26="$(newrepo)"
rs "$R26" open c --issue 960 --agent spec-auditor
rc_is 0 "s2: the stage opens"
R26_REPORT="$(REPORT_OF "$R26" 960 c)"
# CONTROL FIRST: the same bytes WITHOUT the NUL are a real PASS, so the refusal below is about the
# NUL and not about the fixture being broken some other way.
printf 'result: PASS\n' >"$R26_REPORT"
rs "$R26" verdict c --issue 960
rc_is 0 "s2 CONTROL: the same report without a NUL is a genuine PASS (the fixture is otherwise valid)"
printf 'res\000ult: PASS\n' >"$R26_REPORT"
# THE PRECONDITION IS MEASURED: the file really does NOT hold a column-zero `result:` line, so a
# PASS from it could only have been manufactured by the reader.
if LC_ALL=C grep -q '^result:' "$R26_REPORT" 2>/dev/null; then
  bad "s2 PREMISE: the fixture DOES hold a column-zero result: line, so the case below proves nothing"
else
  ok "s2 PREMISE: the fixture holds NO column-zero 'result:' line (MEASURED with grep on the FILE, not inferred)"
fi
# The size is MEASURED here, not predicted: it is what "byte-intact" is compared against below.
R26_SIZE="$(LC_ALL=C wc -c <"$R26_REPORT" | LC_ALL=C tr -d ' ')"
rs "$R26" verdict c --issue 960
rc_is 5 "s2: a NUL-bearing report is a NON-VERDICT (exit 5), not the PASS the capture manufactured"
hasnt "RESULT: PASS" "s2: the merge-proceeding token is NOT reported for a document the file does not contain"
has "NUL" "s2: and the cause NAMES the byte, so an operator knows what is wrong with their file"
rs "$R26" status c --issue 960
has "state=report-ungrammatical" "s2: status maps it to the ungrammatical state — deliberately NOT its own, because the operator action is the same as every other variant (rewrite the report as text)"

# AND IT IS REFUSED AT THE WRITE SIDE TOO, WHICH IS S1's PROPERTY ON A SUBJECT EVERY HOST HAS.
# A NUL-bearing report is one whose recorded verdict could not be READ, so `record-author-performed`
# may not replace it — and unlike a mode-000 file this holds for root as well.
S2_REASON='no peer agent available on this box; hand C against the spec deltas'
S2_EV='docs/round-artifacts/issue-960-hand-c.md'
rs "$R26" record-author-performed c --issue 960 --reason "$S2_REASON" --evidence "$S2_EV" --performed-by author
rc_is 2 "s2/s1: a recording over an UNREADABLE-CLASS report is REFUSED on every host"
has "AUTHOR-REFUSED reason=prior-verdict-unreadable" "s2/s1: under S1's cause"
has "prior-state=unrepresentable" "s2/s1: and the state names WHY it could not be read — a byte the capture cannot carry, not a permission"
rs "$R26" record-author-performed c --issue 960 --reason "$S2_REASON" --evidence "$S2_EV" --performed-by author --force
rc_is 2 "s2/s1: --force does not cover it either"
if [ "$(LC_ALL=C wc -c <"$R26_REPORT" | LC_ALL=C tr -d ' ')" = "$R26_SIZE" ]; then
  ok "s2/s1: the report is BYTE-INTACT after both attempts ($R26_SIZE bytes, the NUL included)"
else
  bad "s2/s1: the report was modified — $(LC_ALL=C wc -c <"$R26_REPORT") byte(s), expected $R26_SIZE"
fi
# AND THE RECOVERY PATH WORKS, so the refusal strands nobody — S1's `--force` ruling rests on it.
rs "$R26" open c --issue 960 --agent spec-auditor --force
rc_is 0 "s2/s1 RECOVERY: open --force supersedes the stage with a fresh report"
rs "$R26" record-author-performed c --issue 960 --reason "$S2_REASON" --evidence "$S2_EV" --performed-by author
rc_is 0 "s2/s1 RECOVERY: and the substitute is then recorded, so the refusal strands nobody"
if [ "$(LC_ALL=C wc -c <"$R26_REPORT" | LC_ALL=C tr -d ' ')" = "$R26_SIZE" ]; then
  ok "s2/s1 RECOVERY: the unreadable report is still on disk as history, untouched"
else
  bad "s2/s1 RECOVERY: the superseded report was modified — $(LC_ALL=C wc -c <"$R26_REPORT") byte(s), expected $R26_SIZE"
fi

# THE SECOND SITE: the STAGE RECORD's own reader. `read_field` is the other capture of untrusted
# file content in this script, and a NUL in the record's nonce field forged a VALID token out of an
# invalid one, redirecting the reader to a STALE report.
R26B="$(newrepo)"
rs "$R26B" open c --issue 961 --agent spec-auditor
rc_is 0 "s2/record: a second stage opens"
R26B_DIR="$R26B/.review-stage/issue-961"
printf 'result: PASS\n\nstale, from a superseded generation\n' >"$R26B_DIR/c.STALEPASS1.md"
R26B_NONCE="$(RECORD_NONCE "$R26B" 961 c)"
if [ -n "$R26B_NONCE" ]; then
  ok "s2/record: the record's real nonce was read, so the forgery below has a subject"
else
  bad "s2/record: could not read the record's nonce — the case below would prove nothing"
fi
# The record is rewritten with a NUL INSIDE the nonce value. `python3` is not required: the value
# is built with printf and the record is rebuilt line by line with the shell.
{
  while IFS= read -r RLINE; do
    case "$RLINE" in
      report-nonce:*) printf 'report-nonce: STALE\000PASS1\n' ;;
      *) printf '%s\n' "$RLINE" ;;
    esac
  done <"$R26B_DIR/c.stage"
} >"$R26B_DIR/c.stage.new" && mv -f "$R26B_DIR/c.stage.new" "$R26B_DIR/c.stage"
if LC_ALL=C grep -q 'report-nonce: STALEPASS1' "$R26B_DIR/c.stage" 2>/dev/null; then
  bad "s2/record PREMISE: the record holds the LITERAL token STALEPASS1, so the forgery is not the capture's doing"
else
  ok "s2/record PREMISE: the record's nonce value is NOT the literal token STALEPASS1 (MEASURED on the FILE) — only a NUL-dropping capture could read it as one"
fi
rs "$R26B" verdict c --issue 961
rc_is 5 "s2/record: the record is a NON-VERDICT (exit 5), not the STALE report's PASS"
hasnt "RESULT: PASS" "s2/record: no reader is redirected to a report the record does not name"
has "stage record unreadable" "s2/record: refused as a RECORD defect, which derives no path at all"
hasnt "c.STALEPASS1.md" "s2/record: and the stale report's path is never even published"

# STRUCTURAL: ONE MAPPING, ONE LITERAL, AND EVERY CAPTURE OF FILE CONTENT GOES THROUGH IT.
# A second spelling of the marker byte is a second place for it to diverge, and a divergence means
# the DETECTOR looks for a byte the MAPPER never writes — a silent false PASS. So the tr spelling
# is the one literal and the byte is DERIVED from it.
S26_SRC="$(cat "$RS" 2>/dev/null || true)"
case "$S26_SRC" in
  *"capture_map_nul() {"*) ok "s2/structural: capture_map_nul() is the ONE mapping implementation" ;;
  *) bad "s2/structural: could not locate capture_map_nul() — the assertions below would be vacuous" ;;
esac
S26_NRAW="$(LC_ALL=C grep -c "tr '\\\\000'" "$RS" 2>/dev/null || true)"
case "$S26_NRAW" in
  1) ok "s2/structural: the NUL translation appears EXACTLY ONCE in the script, so no reader can drift from it" ;;
  *) bad "s2/structural: the NUL translation appears ${S26_NRAW:-0} time(s) — a second copy is a second place for the mapper and the detector to disagree" ;;
esac
case "$S26_SRC" in
  *'CAPTURE_NUL_BYTE="$(printf'*) ok "s2/structural: the marker BYTE is DERIVED from the tr spelling, not written a second time" ;;
  *) bad "s2/structural: the marker byte is spelled independently of the translation, so the detector can look for a byte the mapper never writes" ;;
esac
S26_RB="$(LC_ALL=C sed -n '/^report_bytes() {/,/^}$/p' "$RS" 2>/dev/null || true)"
case "$S26_RB" in
  *'capture_map_nul "$p"'*) ok "s2/structural: report_bytes reads through the mapping" ;;
  *) bad "s2/structural: report_bytes still captures the file raw, so a NUL is silently dropped from the verdict's subject" ;;
esac
# RETARGETED IN ROUND 17 (W1). The subject was `read_field`, the FILE-reading field reader; it is
# DELETED, because every field of the stage record now comes from the ONE capture `observe_record`
# takes. The property is unchanged and its subject moved to the reader that survived — and it is
# stronger, because there is now exactly ONE record-file reader to route rather than two.
S26_RF="$(LC_ALL=C sed -n '/^stage_record_text() {/,/^}$/p' "$RS" 2>/dev/null || true)"
case "$S26_RF" in
  *'capture_map_nul "$file"'*) ok "s2/structural: the ONE stage-record file reader (stage_record_text) reads through the same mapping" ;;
  *) bad "s2/structural: the record file reader still captures raw, so a NUL can forge a field value" ;;
esac
if [ "$(LC_ALL=C grep -c '^read_field() {' "$RS" || true)" -eq 0 ]; then
  ok "s2/structural: and no FILE-reading field reader remains at all — the second read is unexpressible, not merely unused"
else
  bad "s2/structural: a file-reading read_field is back, so a caller can read the record per field again"
fi
S26_CAT="$(LC_ALL=C grep -c 'cat -- "\$' "$RS" 2>/dev/null || true)"
case "$S26_CAT" in
  0) ok "s2/structural: NO capture of file content bypasses the mapping (zero raw \`cat -- \"\$…\"\` reads remain)" ;;
  *) bad "s2/structural: ${S26_CAT:-?} raw file capture(s) remain, and a capture that normalises its input cannot be the thing that validates it" ;;
esac
# AND THE COMPLETE READ IS ASSERTED BY *TWO* SIGNALS, because either alone is defeatable. The
# sentinel `E` survives a refactor that folds the assignment into its `local` declaration (where
# the status would become `local`'s); the STATUS catches a truncated read whose last delivered byte
# happens to BE an `E`, which the sentinel cannot tell from a complete one.
case "$S26_RB" in
  *'|| rc=$?'*) ok "s2/structural: report_bytes captures the read's STATUS as well as its sentinel — a truncated read ending in the sentinel byte cannot pass as complete" ;;
  *) bad "s2/structural: report_bytes relies on the sentinel alone, so a partial read whose last byte is the sentinel is accepted as a complete one" ;;
esac
# EVERY CONSUMER OF THE SNAPSHOT IS LOCALE-PINNED — MEASURED, NOT PREDICTED FROM SOURCE SHAPE.
# `$( )` is byte-faithful apart from NUL and trailing newlines, but the TOOLS that read the snapshot
# are not locale-independent: GNU `grep` handles input it considers binary differently, and BSD `tr`
# ABORTS on an invalid multibyte sequence under a UTF-8 locale (`one_line`'s own comment records
# that, which under `set -euo pipefail` would kill the script inside a substitution and print no
# verdict line at all). An unpinned consumer would therefore make the verdict a function of the
# CALLER's environment.
#
# A SOURCE SCAN FOR UNPINNED INVOCATIONS WAS WRITTEN FIRST AND DISCARDED: it fired on four INDENTED
# comments, a heredoc opener and the `--help` renderer — none of which reads untrusted content — and
# a guard that reds on correct input is the guard agents learn to waive (#3229's ruling). What runs
# instead is the real thing, twice, over a report carrying non-ASCII text AND an invalid UTF-8 byte.
R26C="$(newrepo)"
rs "$R26C" open c --issue 962 --agent spec-auditor
rc_is 0 "s2/locale: a third stage opens"
printf 'result: FINDINGS (a caus\303\251 with an em dash \342\200\224 and a lone \377 byte)\n' \
  >"$(REPORT_OF "$R26C" 962 c)"
# THE WALL-CLOCK FIELD IS NEUTRALISED BEFORE THE COMPARISON, and this is not cosmetic: the two
# runs are seconds apart, so `elapsed=` legitimately differs and a raw comparison FLAKED (measured:
# 1 failure in 5, `elapsed=0` vs `elapsed=1`). That is the wall-clock-race-in-a-test class CLAUDE.md
# lints for, and `elapsed` is not the property under test — the TOKEN, the CAUSE and the report path
# are, and those are compared byte-for-byte.
R26C_STABLE() { printf '%s\n' "$1" | LC_ALL=C sed -e 's/elapsed=[0-9]*/elapsed=<n>/g'; }
rs "$R26C" verdict c --issue 962
rc_is 4 "s2/locale: the report reads FINDINGS under the suite's own locale"
R26C_BASE="$(R26C_STABLE "$OUT")"
# THE NAME IS TAKEN FROM `locale -a`'s OWN OUTPUT, never from a canonical spelling. glibc PRINTS
# `C.utf8`/`en_US.utf8` while `setlocale` also accepts `C.UTF-8`, so a fixed candidate list matched
# NOTHING on this fleet and the case took the no-subject branch on a host that HAS the subject — a
# test passing for the wrong reason. Selection normalises (lowercase, `-`/`_` removed) and then uses
# the printed spelling, which is the one guaranteed to be accepted.
R26C_LOC=""
while IFS= read -r R26C_CAND; do
  case "$(printf '%s' "$R26C_CAND" | LC_ALL=C tr 'A-Z' 'a-z' | LC_ALL=C tr -d '_-')" in
    *utf8) R26C_LOC="$R26C_CAND"; break ;;
  esac
done <<EOF_R26C
$(locale -a 2>/dev/null || true)
EOF_R26C
# BOTH branches execute THREE assertions, so the EXACT case floor stays host-independent.
if [ -n "$R26C_LOC" ]; then
  ok "s2/locale: a UTF-8 locale is installed on this host ($R26C_LOC), so locale invariance HAS a subject"
  OUT="$(cd "$R26C" && LC_ALL="$R26C_LOC" bash "$RS" verdict c --issue 962 2>&1)"; RC=$?
  rc_is 4 "s2/locale: the SAME report reads FINDINGS under $R26C_LOC too"
  if [ "$(R26C_STABLE "$OUT")" = "$R26C_BASE" ]; then
    ok "s2/locale: and the verdict line is BYTE-IDENTICAL under both locales — no consumer of the snapshot is locale-sensitive"
  else
    bad "s2/locale: the verdict DIFFERS by locale, so some consumer of the snapshot is not pinned (C: $R26C_BASE / $R26C_LOC: $(R26C_STABLE "$OUT"))"
  fi
else
  ok "s2/locale: DECLARED GAP — no UTF-8 locale is installed on this host, so cross-locale invariance has NO SUBJECT here; the two assertions below assert what IS true instead of passing silently"
  OUT="$(cd "$R26C" && bash "$RS" verdict c --issue 962 2>&1)"; RC=$?
  rc_is 4 "s2/locale (no-subject host): the non-ASCII report still reads FINDINGS"
  if [ "$(R26C_STABLE "$OUT")" = "$R26C_BASE" ]; then
    ok "s2/locale (no-subject host): and the verdict line is reproducible byte-for-byte"
  else
    bad "s2/locale (no-subject host): the verdict is not reproducible across two runs of one locale ($R26C_BASE / $(R26C_STABLE "$OUT"))"
  fi
fi


# (e) STRUCTURAL — ONE READ, PINNED. A behavioural case cannot see that the classifier reads the
#     file once; a refactor that reintroduced a second read would pass (a) and (b) as long as the
#     new read happened to sit outside the swap point.
R2_BODY="$(LC_ALL=C sed -n '/^classify_report() {$/,/^}$/p' "$RS")"
if [ -n "$R2_BODY" ]; then
  ok "r2/structural: the classifier was located in the shipped script"
else
  bad "r2/structural: could not locate classify_report() — the assertions below would be vacuous"
fi
# RETARGETED IN ROUND 17 (W1), AND STRICTLY STRONGER. These pinned that the classifier used the
# report PATH exactly once and that the one use was `report_bytes` — i.e. that it took at most ONE
# observation of its own. W1 removed the path parameter altogether and made the observation
# REQUIRED, so the property is no longer "one read" but "NO read": a classifier that cannot name a
# path cannot take a second observation, and a caller that supplies none gets a NAMED non-verdict
# rather than a fresh read. A parameter a function does not use is an invitation to read again,
# which is why it was removed rather than left standing.
R2_NRP="$(printf '%s\n' "$R2_BODY" | LC_ALL=C grep -o 'rpath' | LC_ALL=C grep -c . || true)"
if [ "$R2_NRP" = "0" ]; then
  ok "r2/structural: the classifier names NO report path at all — a second observation is unexpressible, not merely untaken"
else
  bad "r2/structural: the classifier still names a report path $R2_NRP time(s), so it can read the file for itself"
fi
# JUDGED OVER CODE, NOT THE PROSE BESIDE IT: this function's comments legitimately NAME
# `report_bytes` (they explain whose grammar the observation is in), so a whole-body match would
# red on a correct script — the shape this repository calls a guard that reds on correct input.
R2_CODE="$(printf '%s\n' "$R2_BODY" | LC_ALL=C grep -v '^[[:space:]]*#' || true)"
case "$R2_CODE" in
  *'report_bytes'*) bad "r2/structural: the classifier still calls report_bytes, so it can take an observation of its own" ;;
  *) ok "r2/structural: and it calls no file reader, so its token always describes the caller's own observation" ;;
esac
case "$R2_BODY" in
  *"NOT-RUN|stage not observed"*)
    ok "r2/structural: an unobserved caller is a NAMED non-verdict, never a fresh read" ;;
  *) bad "r2/structural: an empty observation has no named refusal, so it could fall back to reading the file" ;;
esac
# THE FIELD GRAMMAR IS ONE IMPLEMENTATION. A snapshot reader written beside a file reader would be
# a SECOND implementation of `<key>: <value>`, and a second implementation's agreement is only
# knowable by testing it. Round 12 satisfied that by having the file-reading `read_field` DELEGATE
# to `read_field_from`; round 17 (W1) satisfies it by SUBTRACTION — the file-reading sibling is
# gone, so `read_field_from` is the only implementation there is, and the same now holds for the
# LINE COUNTER (`count_field_lines_from`), which had a second spelling inline at the record-rewrite
# verification.
if [ "$(LC_ALL=C grep -c '^read_field_from() {' "$RS" || true)" -eq 1 ] &&
  [ "$(LC_ALL=C grep -c '^count_field_lines_from() {' "$RS" || true)" -eq 1 ]; then
  ok "r2/structural: the field grammar and the line counter each have EXACTLY ONE implementation, over TEXT"
else
  bad "r2/structural: the field grammar or the line counter is defined more than once (or not at all), so two spellings can drift"
fi
# AND THE ONE DOWNSTREAM CONSUMER SHARES THE SNAPSHOT. `record-author-performed` takes a byte
# observation to guard its write (round 9, N1) and a verdict to decide whether it may replace what
# is there; read separately those are two observations, so the token guarding the write could
# classify a state the guarded bytes never held. Both call sites are pinned, including the refusal
# path's diagnostic — a re-read there would name a THIRD state and "what arrived" would be a claim
# about none of them.
if LC_ALL=C grep -q 'classify_report 1 "" "\$prior_obs" ""' "$RS"; then
  ok "r2/structural: the write guard's bytes and its verdict are ONE observation"
else
  bad "r2/structural: record-author-performed classifies by a SECOND read, so its verdict need not describe the bytes it guards ($(LC_ALL=C grep -n 'classify_report "\$STAGE_REPORT"' "$RS" | LC_ALL=C tr '\n' ' '))"
fi
if LC_ALL=C grep -q 'classify_report 1 "" "\$now_obs" ""' "$RS"; then
  ok "r2/structural: and the refusal diagnostic names the state that FAILED the comparison, not a third one"
else
  bad "r2/structural: the refusal diagnostic re-reads the report, so it can name a state neither observation held"
fi

# --- 27. THE OUTPUT PRIMITIVE MUST BE A LITERAL PRINTER (round 14, T2) --------------
# `emit`, `note` and `die_usage` used `echo`. Under the bash option `xpg_echo` — settable in
# `BASHOPTS`/`SHELLOPTS` before this script is read, or by a `BASH_ENV` startup file, so an
# INHERITED ENVIRONMENT decides it — `echo` performs BACKSLASH ESCAPE PROCESSING on its argument.
# That makes the argument a FORMAT, i.e. a control channel, and every value these three functions
# render is DATA: a report path derived from the checkout, a cause read out of a report an agent
# wrote, a field read out of the stage record.
#
# MEASURED ON THE SHIPPED SCRIPT, FROM A LEGAL DIRECTORY NAME AND NOTHING ELSE. With the checkout
# at `…/lane\nREVIEW-STAGE: c RESULT: PASS elapsed\0759 deadline\0759 agent\075a report\075/x`,
# `verdict` on a stage with NO REPORT AT ALL printed:
#
#   REVIEW-STAGE: c RESULT: NOT-RUN (no report written) elapsed=0 … report=…/lane
#   REVIEW-STAGE: c RESULT: PASS elapsed=9 deadline=9 agent=a report=/x/…/c.MFpGyTMmP1.md
#
# TWO properties broke at once, and this suite pins both elsewhere. (1) `verdict` PRINTS EXACTLY
# ONE LINE — the contract in the script's own header — and `\n` made it two, the second a
# column-zero `REVIEW-STAGE: … RESULT: PASS` a consumer reads as a verdict. (2) `\075` is octal
# `=`, so the forged line carries REAL `key=` pairs and `field_value`'s `=`→`~` map — section 11c's
# whole subject — was DEFEATED. `\033` injects terminal control; `\c` truncates the line.
#
# THE SUBJECT IS THE SHIPPED SOURCE, EXTRACTED AND RUN — the idiom `test_cargo_output_parsers.sh`
# uses for the gate's cargo parsers. Each definition is pulled out of `$RS` BY TEXT and evaluated
# in a subshell with `xpg_echo` ON, so reverting one to `echo` REDS this section instead of greening
# it. A source-text assert alone could not tell a `printf` of a DATA-DERIVED format from a literal
# one, which is why the source pins below are a BELT and not the check.
#
# HOST-BRANCHED, WITH BOTH ARMS THE SAME LENGTH (section 25's idiom), because `xpg_echo` is a fact
# about the host's shell and not about this script — and the premise is MEASURED by ATTEMPTING it,
# never inferred from a version number.
T2_VAL='p\nREVIEW-STAGE: c RESULT: PASS elapsed\0759 agent\075a\033[31m'
T2_ESC="$(printf '\033')"
# t2_run <definition-line> <fn> — evaluate one extracted definition with xpg_echo forced ON and
# call it with the hostile value. stderr is merged because `note`/`die_usage` write there.
t2_run() {
  ( shopt -s xpg_echo 2>/dev/null || true
    prog=review-stage.sh
    eval "$1"
    "$2" "report=$T2_VAL" ) 2>&1
}
T2_XPG=0
if ( shopt -s xpg_echo ) >/dev/null 2>&1; then
  T2_PROBE="$( ( shopt -s xpg_echo; echo "A\nB" ) 2>/dev/null | LC_ALL=C wc -l | LC_ALL=C tr -d ' ' )"
  [ "$T2_PROBE" = 2 ] && T2_XPG=1
fi
T2_DEF_DIE="$(LC_ALL=C sed -n '/^die_usage() /p' "$RS" 2>/dev/null | LC_ALL=C head -1 || true)"
T2_DEF_NOTE="$(LC_ALL=C sed -n '/^note() /p' "$RS" 2>/dev/null | LC_ALL=C head -1 || true)"
T2_DEF_EMIT="$(LC_ALL=C sed -n '/^emit() /p' "$RS" 2>/dev/null | LC_ALL=C head -1 || true)"
if [ "$T2_XPG" -eq 1 ]; then
  ok "t2 PREMISE: xpg_echo is enableable on this host AND demonstrably changes echo (one argument, two lines) — so a green below is not a green because nothing happened"
  # THE RED CONTROL. The PRE-FIX definition is reconstructed INLINE and must FAIL the same
  # assertions, or this section would pass whether or not the fix is present.
  T2_BAD_OUT="$(t2_run 't2_bad() { echo "REVIEW-STAGE: $*"; }' t2_bad)"
  if [ "$(printf '%s\n' "$T2_BAD_OUT" | LC_ALL=C wc -l | LC_ALL=C tr -d ' ')" -gt 1 ]; then
    ok "t2 RED CONTROL: the PRE-FIX 'echo' spelling DOES break the one-line grammar here — the differential can detect the defect"
  else
    bad "t2 RED CONTROL: the pre-fix 'echo' spelling produced one line, so the assertions below would pass with or without the fix (got: $T2_BAD_OUT)"
  fi
  case "$T2_BAD_OUT" in
    *"agent=a"*) ok "t2 RED CONTROL: and it manufactured a REAL '=' out of octal \\075, which is the '=' neutralisation being defeated" ;;
    *) bad "t2 RED CONTROL: the pre-fix spelling did not manufacture an '=' (got: $T2_BAD_OUT)" ;;
  esac
  for T2_FN in die_usage note emit; do
    case "$T2_FN" in
      die_usage) T2_DEF="$T2_DEF_DIE" ;;
      note)      T2_DEF="$T2_DEF_NOTE" ;;
      *)         T2_DEF="$T2_DEF_EMIT" ;;
    esac
    T2_OUT="$(t2_run "$T2_DEF" "$T2_FN")"
    if [ "$(printf '%s\n' "$T2_OUT" | LC_ALL=C wc -l | LC_ALL=C tr -d ' ')" = 1 ]; then
      ok "t2/$T2_FN: EXACTLY ONE line under xpg_echo, so a '\\n' in a legal path cannot split the grammar"
    else
      bad "t2/$T2_FN: the value's '\\n' was interpreted and the output is not one line (got: $T2_OUT)"
    fi
    case "$T2_OUT" in
      *"$T2_ESC"*) bad "t2/$T2_FN: the value's '\\033' became a real ESC byte — terminal control reached the operator channel (got: $T2_OUT)" ;;
      *) ok "t2/$T2_FN: '\\033' is INERT — no ESC byte reached the output" ;;
    esac
    case "$T2_OUT" in
      *'p\nREVIEW-STAGE:'*) ok "t2/$T2_FN: the backslash sequence is carried VERBATIM, so the neutralisation stays display-only and the value is still readable" ;;
      *) bad "t2/$T2_FN: the value was not carried verbatim (got: $T2_OUT)" ;;
    esac
    case "$T2_OUT" in
      *"agent=a"*) bad "t2/$T2_FN: octal \\075 manufactured a real '=' — field_value's '=' neutralisation is defeated (got: $T2_OUT)" ;;
      *'agent\075a'*) ok "t2/$T2_FN: '\\075' is INERT — no 'key=' pair can be manufactured out of an octal escape" ;;
      *) bad "t2/$T2_FN: neither the inert nor the interpreted form of \\075 is present (got: $T2_OUT)" ;;
    esac
  done
else
  # THE NO-SUBJECT ARM, DECLARED RATHER THAN SKIPPED. This host's bash cannot be made to interpret
  # escapes in `echo`, so the defect is not reproducible here and this arm says so. It runs the
  # SAME extraction over the SAME three definitions — establishing that each is a literal printer
  # under this host's own `echo` semantics — and emits the SAME COUNT, so the EXACT floor is
  # host-independent.
  ok "t2 PREMISE: this host's bash does not honour xpg_echo, so the hostile-environment differential has NO SUBJECT here — declared, and the same three definitions are exercised below under this host's own echo semantics"
  T2_BAD_OUT="$(t2_run 't2_bad() { echo "REVIEW-STAGE: $*"; }' t2_bad)"
  if [ "$(printf '%s\n' "$T2_BAD_OUT" | LC_ALL=C wc -l | LC_ALL=C tr -d ' ')" = 1 ]; then
    ok "t2 RED CONTROL (no subject): the pre-fix 'echo' spelling is indistinguishable here, which is exactly why this arm claims nothing about xpg_echo"
  else
    bad "t2 RED CONTROL (no subject): the pre-fix spelling split the line on a host that reported no xpg_echo — the premise probe is wrong (got: $T2_BAD_OUT)"
  fi
  case "$T2_BAD_OUT" in
    *'agent\075a'*) ok "t2 RED CONTROL (no subject): and \\075 stayed inert for it too — the difference this section measures is unavailable on this host" ;;
    *) bad "t2 RED CONTROL (no subject): \\075 did not stay inert on a host that reported no xpg_echo (got: $T2_BAD_OUT)" ;;
  esac
  for T2_FN in die_usage note emit; do
    case "$T2_FN" in
      die_usage) T2_DEF="$T2_DEF_DIE" ;;
      note)      T2_DEF="$T2_DEF_NOTE" ;;
      *)         T2_DEF="$T2_DEF_EMIT" ;;
    esac
    T2_OUT="$(t2_run "$T2_DEF" "$T2_FN")"
    if [ "$(printf '%s\n' "$T2_OUT" | LC_ALL=C wc -l | LC_ALL=C tr -d ' ')" = 1 ]; then
      ok "t2/$T2_FN: EXACTLY ONE line (this host's echo semantics; the xpg_echo case is declared unavailable)"
    else
      bad "t2/$T2_FN: the output is not one line (got: $T2_OUT)"
    fi
    case "$T2_OUT" in
      *"$T2_ESC"*) bad "t2/$T2_FN: an ESC byte reached the output (got: $T2_OUT)" ;;
      *) ok "t2/$T2_FN: no ESC byte reached the output" ;;
    esac
    case "$T2_OUT" in
      *'p\nREVIEW-STAGE:'*) ok "t2/$T2_FN: the backslash sequence is carried VERBATIM" ;;
      *) bad "t2/$T2_FN: the value was not carried verbatim (got: $T2_OUT)" ;;
    esac
    case "$T2_OUT" in
      *"agent=a"*) bad "t2/$T2_FN: a real '=' was manufactured (got: $T2_OUT)" ;;
      *'agent\075a'*) ok "t2/$T2_FN: '\\075' is INERT" ;;
      *) bad "t2/$T2_FN: neither form of \\075 is present (got: $T2_OUT)" ;;
    esac
  done
fi

# (b) THE STRUCTURAL GUARD OVER THE PRIMITIVE (round 14, T2). `emit-boundary-scan.sh` already
#     asserts that every VALUE on an emitted line is routed; it now also asserts that the printing
#     COMMAND is a literal printer — `echo` is refused outright, with no allowlist, and every
#     `printf` FORMAT must be a script-authored literal. A routed value is no protection if the
#     primitive re-interprets what the boundary just neutralised, which is why this is a second
#     check and not a second allowlist entry.
if [ ! -f "$EBS" ]; then
  # TEN, matching the ten assertions the else-branch emits, so the EXACT floor holds either way.
  bad "primitive-guard: $EBS is missing — the structural guard did not run (1/10)"
  bad "primitive-guard: the same absence (2/10)"
  bad "primitive-guard: the same absence (3/10)"
  bad "primitive-guard: the same absence (4/10)"
  bad "primitive-guard: the same absence (5/10)"
  bad "primitive-guard: the same absence (6/10)"
  bad "primitive-guard: the same absence (7/10)"
  bad "primitive-guard: the same absence (8/10)"
  bad "primitive-guard: the same absence (9/10)"
  bad "primitive-guard: the same absence (10/10)"
else
  T2_SOUT="$(bash "$EBS" "$RS" 2>&1)"; T2_SRC=$?
  if [ "$T2_SRC" -eq 0 ]; then
    ok "primitive-guard: the SHIPPED review-stage.sh is CLEAN — no echo, and every printf FORMAT is a literal"
  else
    bad "primitive-guard: the shipped review-stage.sh FAILS the guard: $T2_SOUT"
  fi
  case "$T2_SOUT" in
    *"printf statement(s)"*) ok "primitive-guard: the check REPORTS how many printf statements it examined — a count, not an adjective" ;;
    *) bad "primitive-guard: the primitive check reported no subject count, so it may not have run at all (got: $T2_SOUT)" ;;
  esac
  case "$T2_SOUT" in
    *"NOT COVERED (output primitive)"*) ok "primitive-guard: and it DECLARES what the primitive check does not cover, on every run" ;;
    *) bad "primitive-guard: the primitive check did not declare its own scope (got: $T2_SOUT)" ;;
  esac
  # THE POSITIVE CONTROL, on a THROWAWAY COPY (the artifact is substituted, never a settable seam
  # — #3312's corollary for tests). The plant is deliberately COMPOUND, so it also proves the
  # primitive walker is POSITIONAL rather than line-anchored (round 9's N3 blind spot).
  T2_ED="$T/t2-echo"; mkdir -p "$T2_ED"
  LC_ALL=C sed -e '/^emit() /s|.*|emit()      { [ -n "$*" ] \&\& echo "REVIEW-STAGE: $PLANTED_ECHO_PRIMITIVE"; }|' \
    "$RS" >"$T2_ED/review-stage.sh" 2>/dev/null || true
  T2_ELINE="$(LC_ALL=C grep -n 'PLANTED_ECHO_PRIMITIVE' "$T2_ED/review-stage.sh" 2>/dev/null | LC_ALL=C head -1 || true)"
  if [ -n "$T2_ELINE" ]; then
    ok "primitive-guard/control: the echo plant landed in the scratch copy (asserted, not assumed)"
  else
    bad "primitive-guard/control: the echo plant did NOT land, so the control below proves nothing"
  fi
  case "$(printf '%s\n' "${T2_ELINE#*:}" | LC_ALL=C sed -e 's/^[[:space:]]*//' -e 's/[[:space:]].*//')" in
    echo)
      bad "primitive-guard/control: the planted echo BEGINS its statement position at the line start, so this control does not test positional recognition (line: $T2_ELINE)" ;;
    "")
      bad "primitive-guard/control: could not read the planted line's first word" ;;
    *)
      ok "primitive-guard/control: the planted echo does NOT begin its line (it is behind a [ … ] &&), so the control also tests POSITIONAL recognition" ;;
  esac
  T2_EOUT="$(bash "$EBS" "$T2_ED/review-stage.sh" 2>&1)"; T2_ERC=$?
  if [ "$T2_ERC" -ne 0 ]; then
    ok "primitive-guard/control: the guard REDS on a planted echo"
  else
    bad "primitive-guard/control: the guard reported CLEAN on a planted echo — it proves nothing (got: $T2_EOUT)"
  fi
  case "$T2_EOUT" in
    *"output-primitive bypass"*) ok "primitive-guard/control: and it NAMES the check that failed, so the red is attributable to the primitive rather than to a value" ;;
    *) bad "primitive-guard/control: the guard red without naming the output-primitive check (got: $T2_EOUT)" ;;
  esac
  # THE SECOND PLANT: a printf whose FORMAT is data-derived. The same channel, one step in — a
  # format carrying '%' or a backslash from data is interpreted exactly as echo's argument was.
  T2_FD="$T/t2-fmt"; mkdir -p "$T2_FD"
  LC_ALL=C sed -e '/^emit() /s|.*|emit()      { printf "$PLANTED_FORMAT_PRIMITIVE" "REVIEW-STAGE: $*"; }|' \
    "$RS" >"$T2_FD/review-stage.sh" 2>/dev/null || true
  if LC_ALL=C grep -q 'PLANTED_FORMAT_PRIMITIVE' "$T2_FD/review-stage.sh" 2>/dev/null; then
    ok "primitive-guard/format: the data-derived-format plant landed in the scratch copy"
  else
    bad "primitive-guard/format: the format plant did NOT land, so the control below proves nothing"
  fi
  T2_FOUT="$(bash "$EBS" "$T2_FD/review-stage.sh" 2>&1)"; T2_FRC=$?
  if [ "$T2_FRC" -ne 0 ]; then
    ok "primitive-guard/format: the guard REDS on a printf whose FORMAT came from a variable"
  else
    bad "primitive-guard/format: the guard reported CLEAN on a data-derived printf format (got: $T2_FOUT)"
  fi
  case "$T2_FOUT" in
    *PLANTED_FORMAT_PRIMITIVE*) ok "primitive-guard/format: and it NAMES the offending format, so the red is attributable" ;;
    *) bad "primitive-guard/format: the guard red without naming the planted format (got: $T2_FOUT)" ;;
  esac
fi

# (c) THE SOURCE PINS — a BELT beside the behavioural differential, not the check. They name the
#     exact spelling required, so a reviewer reading the diff sees the rule as well as the guard.
for T2_FN in die_usage note emit; do
  case "$T2_FN" in
    die_usage) T2_DEF="$T2_DEF_DIE" ;;
    note)      T2_DEF="$T2_DEF_NOTE" ;;
    *)         T2_DEF="$T2_DEF_EMIT" ;;
  esac
  case "$T2_DEF" in
    *"printf '%s\\n'"*) ok "t2/structural: $T2_FN prints through a printf of the literal format '%s\\n'" ;;
    "") bad "t2/structural: could not locate the definition of $T2_FN in the shipped script" ;;
    *) bad "t2/structural: $T2_FN does not print through a literal printf (got: $T2_DEF)" ;;
  esac
done

# --- 28. A FAITHFUL READER IS NOT A FAITHFUL ANSWER (round 14, T1) ------------------
# ROUND 13's S2 GAVE THIS SCRIPT ONE FAITHFUL-READ BOUNDARY AND LEFT ONE PATH BYPASSING IT — the
# THIRD round in a row with that shape (round 7's emit sites, round 13's record reads, this).
# `count_field_lines` still read the stage record with `grep -c` on the FILE, and `grep` is a
# perfectly faithful reader; it is the ANSWER that is not. A record whose key is spelt
# `report-<NUL>nonce: CURRENTX1` holds NO `report-nonce:` line, so the count was a TRUTHFUL `0` —
# and `0` is precisely the value that means "a pre-nonce record, whose single report is the LEGACY
# bare `<kind>.md`". Measured on the shipped script, with a stale legacy report still on disk:
#
#   $ od -c c.stage | grep -A1 report      ->  r e p o r t - \0 n o n c e :   C U R R E N T X 1
#   $ grep -c '^report-nonce:' c.stage     ->  0   (rc 1 — the record really is not there)
#   $ review-stage.sh verdict c --issue 900
#     REVIEW-STAGE: c RESULT: PASS … report=…/.review-stage/issue-900/c.md     rc=0
#
# The current report (`c.CURRENTX1.md`) held the SENTINEL. The `PASS` came from a STALE `c.md` an
# earlier version would have written — which is round 4's H2 defect (a data file redirecting a
# reader) reached through yet another door, and it is exactly what the nonce exists to prevent.
# So the byte never has to defeat the COUNTER to defeat the READER: it only has to make the current
# record unparseable while a stale artifact is still on disk. `0` is not a safe reading of a
# document we could not read as text.
#
# THE STATUS IS THREE-VALUED NOW (0 faithful / 1 read failed / 2 unrepresentable) and both callers
# spell the permissive set AFFIRMATIVELY as `0`, so a status added later refuses by construction.
# Status 2 gets its OWN refusal because the operator action differs — rewrite the record or re-open
# the stage, never a chmod — and a refusal saying "permission or I/O" about a file whose permissions
# are fine is a false rationale, which round 2's B7 records as worse than none.
T1R="$(newrepo)" || bad "t1: could not create the fixture repo"
T1D="$T1R/.review-stage/issue-900"
mkdir -p "$T1D"
# THE STALE LEGACY ARTIFACT: what a pre-nonce version of this tool wrote, still on disk, recording a
# PASS. It is READ but never WRITTEN by this version, which is what makes the `0` branch dangerous.
printf '# stale legacy report\nresult: PASS\n\nan old agent audited an OLD tree\n' >"$T1D/c.md"
# THE CURRENT RECORD, whose `report-nonce:` KEY carries the byte.
printf 'kind: c\nissue: 900\nagent: spec-auditor\nspawned-at: 2026-09-01T00:00:00Z\nspawned-epoch: 1756684800\ndeadline-secs: 3600\nreopen-count: 0\nreport-\000nonce: CURRENTX1\nhead-sha: unresolved\n' >"$T1D/c.stage"
# THE CURRENT REPORT, holding the sentinel — i.e. the agent produced NOTHING.
printf '# review stage: c\nresult: NOT-RUN (no report written)\n' >"$T1D/c.CURRENTX1.md"
# PREMISE, MEASURED ON THE FILES — three of them, because a case built on a broken fixture proves
# nothing: the record really lacks a column-zero `report-nonce:` line (so a faithful reader's `0` is
# CORRECT and the defect is in what `0` MEANS), it really holds the byte, and the stale artifact
# really records the merge-proceeding verdict.
if LC_ALL=C grep -q '^report-nonce:' "$T1D/c.stage" 2>/dev/null; then
  bad "t1 PREMISE: the record DOES carry a column-zero report-nonce: line, so the case below is not about the byte"
else
  ok "t1 PREMISE: the record holds NO column-zero 'report-nonce:' line (MEASURED with grep on the FILE) — a faithful reader legitimately counts ZERO"
fi
if LC_ALL=C tr -d '\000' <"$T1D/c.stage" 2>/dev/null | LC_ALL=C cmp -s - "$T1D/c.stage"; then
  bad "t1 PREMISE: the record holds NO NUL byte, so this host did not build the fixture and the case proves nothing"
else
  ok "t1 PREMISE: the record really holds a NUL byte (MEASURED by deleting it and comparing)"
fi
if LC_ALL=C grep -q '^result: PASS$' "$T1D/c.md" 2>/dev/null; then
  ok "t1 PREMISE: the STALE legacy report really records the merge-proceeding 'result: PASS' — the bait is real"
else
  bad "t1 PREMISE: the stale legacy report does not record a PASS, so nothing dangerous was on offer"
fi
rs "$T1R" verdict c --issue 900
rc_is 5 "t1: a record holding the byte is a NON-VERDICT (exit 5), not the stale legacy report's PASS"
hasnt "RESULT: PASS" "t1: the merge-proceeding token is NOT reported for a record that could not be read as text"
has "stage record unreadable" "t1: and the cause is the stage-RECORD one, so the operator is not sent to look at a report"
has "NUL 0x00 or SOH 0x01" "t1: the cause NAMES the byte, so an operator knows what is wrong with their file"
has "NOT a chmod" "t1: and it names the NEXT ACTION, which differs from a permission failure's"
has "report=unresolved" "t1: no report path is published at all — neither the stale legacy one nor a fabricated current one"
hasnt "c.md " "t1: the stale legacy path is never even named"
# `status` reports the same fact on its own surface, with the derived state.
rs "$T1R" status c --issue 900
rc_is 0 "t1/status: status is advisory (exit 0)"
has "state=stage-record-unreadable" "t1/status: the derived state names the record, not the report"
has "NUL 0x00 or SOH 0x01" "t1/status: and the STATUS-NOTE carries the byte in its detail"
# THE WRITE SIDE REFUSES UNDER ITS OWN NAME, distinct from the read-failed refusal: `open --force`
# over such a record would have to copy `spawned-at`/`reopen-count` out of a document it cannot read.
rs "$T1R" open c --issue 900 --agent spec-auditor --force
rc_is 2 "t1/open: a forced re-open over a record holding the byte is REFUSED (exit 2)"
has "reason=stage-record-unrepresentable" "t1/open: refused under its OWN reason token, distinctly from stage-record-unreadable — the operator action differs"
has "do not chmod it" "t1/open: and the refusal says so explicitly, because a permission diagnosis would be a false rationale"
hasnt "OPEN-OK" "t1/open: nothing was written"
if LC_ALL=C grep -q '^result: PASS$' "$T1D/c.md" 2>/dev/null; then
  ok "t1/open: the stale artifact is INTACT — a refusal destroys nothing"
else
  bad "t1/open: the refused re-open modified the stale report"
fi
# CONTROL: the SAME record WITHOUT the byte reads the CURRENT report, not the legacy one. Without
# this the case above could pass on a script that refused every record.
printf 'kind: c\nissue: 900\nagent: spec-auditor\nspawned-at: 2026-09-01T00:00:00Z\nspawned-epoch: 1756684800\ndeadline-secs: 3600\nreopen-count: 0\nreport-nonce: CURRENTX1\nhead-sha: unresolved\n' >"$T1D/c.stage"
rs "$T1R" verdict c --issue 900
rc_is 5 "t1 CONTROL: the same record with the byte REMOVED still reads its CURRENT report (the sentinel, exit 5)"
has "no report written" "t1 CONTROL: and reports the sentinel's own cause, so the guard does not red on correct input"
has "c.CURRENTX1.md" "t1 CONTROL: naming the CURRENT report, never the stale legacy one"
# CONTROL 2: a LEGITIMATE pre-nonce record — no `report-nonce:` line at all — must still read the
# legacy bare name. That branch is the one the byte impersonated, and a guard that broke it would
# red on correct input.
printf 'kind: c\nissue: 900\nagent: spec-auditor\nspawned-at: 2026-09-01T00:00:00Z\nspawned-epoch: 1756684800\ndeadline-secs: 3600\nreopen-count: 0\nhead-sha: unresolved\n' >"$T1D/c.stage"
rs "$T1R" verdict c --issue 900
rc_is 0 "t1 CONTROL: a genuine PRE-NONCE record (no such field at all) still reads the LEGACY report — the branch the byte impersonated is intact"
has "RESULT: PASS" "t1 CONTROL: reporting that legacy report's real verdict"

# (b) STRUCTURAL: the read is routed, and the permissive set is affirmative at BOTH callers.
if LC_ALL=C grep -q 'capture_map_nul "\$file" && printf' "$RS"; then
  ok "t1/structural: count_field_lines reads through capture_map_nul, with the two-signal completeness assertion"
else
  bad "t1/structural: count_field_lines does not read through the capture boundary"
fi
if [ "$(LC_ALL=C grep -c 'grep -c -i "\^\[\[:space:\]\]\*\${key}:" "\$file"' "$RS" || true)" -eq 0 ]; then
  ok "t1/structural: no reader greps the record FILE directly any more"
else
  bad "t1/structural: a direct grep of the record file remains, and a faithful reader is not a faithful answer"
fi
if LC_ALL=C grep -q '\*"\$CAPTURE_NUL_BYTE"\*) return 2' "$RS"; then
  ok "t1/structural: an unrepresentable record is its OWN status (2), not folded into the read-failed 1"
else
  bad "t1/structural: the unrepresentable case has no distinct status, so its refusal must borrow another cause's rationale"
fi
# RETARGETED IN ROUND 17 (W1). These pinned the fail-closed status idiom at the TWO callers that
# read the record file per question (`cmd_open` and `load_stage`). W1 collapsed both into ONE
# capture inside `observe_record`, so there is one call site to pin instead of two — which is the
# stronger property, and the reason the retarget is not a weakening: a status captured by
# `if ! …` (which can only see zero-vs-nonzero) would still lose the unrepresentable case, and now
# it would lose it for every caller at once, so the pin matters more than before.
if [ "$(LC_ALL=C grep -c 'text="\$(stage_record_text "\$sfile")" || rc=\$?' "$RS" || true)" -eq 1 ]; then
  ok "t1/structural: the ONE record capture takes the STATUS (|| rc=\$?), never 'if ! …' which reads 0"
else
  bad "t1/structural: the one record capture does not take stage_record_text's status the fail-closed way"
fi
if [ "$(LC_ALL=C grep -c 'nlines="\$(count_field_lines_from "\$STAGE_RECORD_TEXT" report-nonce)" || cfl_rc=\$?' "$RS" || true)" -eq 1 ]; then
  ok "t1/structural: and the line count over that capture takes its status the same way"
else
  bad "t1/structural: the line count does not take count_field_lines_from's status the fail-closed way"
fi
if [ "$(LC_ALL=C grep -c 'again="\$(stage_record_text "\$sfile")" || arc=\$?' "$RS" || true)" -eq 1 ]; then
  ok "t1/structural: and so does the RE-VERIFICATION that makes the record and the report one observation"
else
  bad "t1/structural: the observation's re-verification does not take its read status the fail-closed way"
fi
if [ "$(LC_ALL=C grep -c 'if ! nnonce' "$RS" || true)" -eq 0 ]; then
  ok "t1/structural: no caller branches on a bare 'if ! …' any more, which could only see zero-vs-nonzero"
else
  bad "t1/structural: a caller still branches on 'if ! …', so it cannot tell status 2 from status 1"
fi

# (c) THE STRUCTURAL READ-BOUNDARY GUARD (round 14, T1). Round 13's asserts check that the mapping
#     appears exactly ONCE — a property of the BOUNDARY, not of its CALLERS — which is why neither
#     round-14 site was visible to them. `read-boundary-scan.sh` is the caller-side mirror of
#     section 18's emit-boundary scanner. Its positive control is the requirement, and not a
#     formality: written without an assignment-prefix stripper the scanner reported CLEAN on the
#     pre-fix script AND on a planted `cat "$file"`, because every text call here is spelled
#     `LC_ALL=C grep …` and the text before the command word therefore ends in `C`.
RBS="$SCRIPT_DIR/lib/read-boundary-scan.sh"
if [ ! -f "$RBS" ]; then
  # NINE, matching the nine assertions the else-branch emits, so the EXACT floor holds either way.
  bad "read-guard: $RBS is missing — the structural guard did not run (1/9)"
  bad "read-guard: the same absence (2/9)"
  bad "read-guard: the same absence (3/9)"
  bad "read-guard: the same absence (4/9)"
  bad "read-guard: the same absence (5/9)"
  bad "read-guard: the same absence (6/9)"
  bad "read-guard: the same absence (7/9)"
  bad "read-guard: the same absence (8/9)"
  bad "read-guard: the same absence (9/9)"
else
  RBS_OUT="$(bash "$RBS" "$RS" 2>&1)"; RBS_RC=$?
  if [ "$RBS_RC" -eq 0 ]; then
    ok "read-guard: the SHIPPED review-stage.sh is CLEAN — every read of file content is routed or declared with its reason"
  else
    bad "read-guard: the shipped review-stage.sh has a read-boundary BYPASS: $RBS_OUT"
  fi
  case "$RBS_OUT" in
    *"NOT COVERED"*) ok "read-guard: the scan DECLARES what it does not cover, on every run" ;;
    *) bad "read-guard: the scan did not declare its scope (got: $RBS_OUT)" ;;
  esac
  case "$RBS_OUT" in
    *"recogniser hit(s)"*) ok "read-guard: and it reports its COUNTS — hits, declared reads and boundary calls, not an adjective" ;;
    *) bad "read-guard: the scan did not report its subject counts (got: $RBS_OUT)" ;;
  esac
  # CONTROL (a): THE EXACT PRE-FIX SHAPE, planted in a throwaway copy — a `grep -c` of the record
  #              file inside a `$( … )` behind an `LC_ALL=C` assignment prefix. All three of those
  #              details were what made the real defect invisible to the first draft of the guard.
  RBS_D="$T/rbs-grep"; mkdir -p "$RBS_D"
  LC_ALL=C sed -e '/^count_field_lines_from() {/a\  PLANTED_OUT="$(LC_ALL=C grep -c -i "^x:" "$PLANTED_RECORD_READ")"' \
    "$RS" >"$RBS_D/review-stage.sh" 2>/dev/null || true
  RBS_LINE="$(LC_ALL=C grep -n 'PLANTED_RECORD_READ' "$RBS_D/review-stage.sh" 2>/dev/null | LC_ALL=C head -1 || true)"
  if [ -n "$RBS_LINE" ]; then
    ok "read-guard/control: the pre-fix-shaped plant landed in the scratch copy (asserted, not assumed)"
  else
    bad "read-guard/control: the plant did NOT land, so the control below proves nothing"
  fi
  # AND IT MUST REALLY BE BEHIND AN ASSIGNMENT PREFIX INSIDE A SUBSTITUTION, or this control is a
  # weaker case than the defect it stands for. Measured from the planted text itself.
  case "${RBS_LINE#*:}" in
    *'$(LC_ALL=C grep'*) ok "read-guard/control: the planted read sits inside a \$( … ) behind an LC_ALL=C prefix — the exact shape that defeated the first draft of this guard" ;;
    *) bad "read-guard/control: the planted read is not in the pre-fix shape, so it tests a weaker case (line: $RBS_LINE)" ;;
  esac
  RBS_POUT="$(bash "$RBS" "$RBS_D/review-stage.sh" 2>&1)"; RBS_PRC=$?
  if [ "$RBS_PRC" -ne 0 ]; then
    ok "read-guard/control: the guard REDS on the planted raw read"
  else
    bad "read-guard/control: the guard reported CLEAN on the planted raw read — it proves nothing (got: $RBS_POUT)"
  fi
  case "$RBS_POUT" in
    *'the reading command `grep` starts a pipeline'*) ok "read-guard/control: and it NAMES the command and the recogniser that fired, so the red is attributable" ;;
    *) bad "read-guard/control: the guard red without naming the reading command (got: $RBS_POUT)" ;;
  esac
  # CONTROL (b): A DIFFERENT READING COMMAND, to prove the recogniser is a LIST and not one pattern.
  RBS_C="$T/rbs-cat"; mkdir -p "$RBS_C"
  LC_ALL=C sed -e '/^count_field_lines_from() {/a\  PLANTED_OUT="$(LC_ALL=C cat "$PLANTED_CAT_READ")"' \
    "$RS" >"$RBS_C/review-stage.sh" 2>/dev/null || true
  RBS_COUT="$(bash "$RBS" "$RBS_C/review-stage.sh" 2>&1)"; RBS_CRC=$?
  if [ "$RBS_CRC" -ne 0 ] && [ "${RBS_COUT#*"\`cat\` starts a pipeline"}" != "$RBS_COUT" ]; then
    ok "read-guard/control: a planted 'cat' read reds too and is named — the reader set is a declared LIST, not one command"
  else
    bad "read-guard/control: a planted 'cat' read was not caught or not named (rc=$RBS_CRC; got: $RBS_COUT)"
  fi
  # CONTROL (c): A STALE ALLOWLIST ENTRY is its own failure. An entry matching nothing excuses
  #              nothing — and it is the signal that the read it described has CHANGED, which is why
  #              entries are matched on SOURCE TEXT and never by line number.
  RBS_S="$T/rbs-stale"; mkdir -p "$RBS_S"
  LC_ALL=C sed -e "s|sed -n '2,/\^# ---END-HELP---\$/p' \"\\\$0\"|sed -n '3,/^# ---END-HELP---\$/p' \"\\\$0\"|" \
    "$RS" >"$RBS_S/review-stage.sh" 2>/dev/null || true
  RBS_SOUT="$(bash "$RBS" "$RBS_S/review-stage.sh" 2>&1)"
  case "$RBS_SOUT" in
    *"STALE allowlist entry"*) ok "read-guard/stale: an allowlist entry whose source text has CHANGED is reported STALE by name, not silently kept as a standing excusal" ;;
    *) bad "read-guard/stale: the guard did not report a stale entry when the declared --help read was reworded (got: $RBS_SOUT)" ;;
  esac
fi
# --- 29. A LEGAL '=' IN THE PATH MUST NOT MAKE `report=` PUBLISH A FILE THAT DOES NOT EXIST
#         (round 16, V2) ----------------------------------------------------------------------
# THE FINDING (roborev job 393, V2, review-stage.sh:2455). `report=` went through `field_value`,
# which maps the ONE reserved character of these `key=value` lines, '=', to '~' so a value cannot
# forge a pair. A repository root may LEGALLY contain '=' — so on such a checkout the verdict line
# advertised a path that DOES NOT EXIST, while the grammar promises the absolute report-of-record
# path and `open` (which prints a raw path line of its own) had just created the real one. Measured
# on the shipped script in a checkout named `…/eq=repo`:
#
#   open  (raw line):  …/eq=repo/.review-stage/issue-3751/c.0sqezS2DJW.md   <- exists
#   verdict report=:   …/eq~repo/.review-stage/issue-3751/c.0sqezS2DJW.md   <- does not exist
#
# Round 10's nonce check and any consumer that OPENS that path are reading a corrupted value, and
# `verdict` — unlike `open` — offers NO separate raw channel to fall back to.
#
# THE FIX IS AN EXEMPTION COUPLED TO THE PROPERTY THAT JUSTIFIES IT. Since round 11 (Q3) `report=`
# is emitted LAST and read as the REMAINDER of the line, so an '=' inside it cannot create an
# ambiguous field — the anti-forgery reason for the map does not apply to this ONE field. Control
# characters are still neutralised (rounds 5/7/13/14). The coupling is asserted STRUCTURALLY below
# and in section 44l(d) of the premerge suite, so appending a field after `report=` reds a suite
# rather than silently re-corrupting the value or re-enabling forgery.
#
# THE EXEMPTION IS CONFINED TO ONE FIELD ON ONE LINE, and the control that matters proves the
# confinement rather than the fix: a forged `key=value` smuggled through a DIFFERENT field is what
# the remainder rule depends on being neutralised, since an unmapped `agent: a report=/forged`
# would put a REAL `report=` pair AHEAD of the measured one and the remainder parse takes the first.
V2_ROOT="$T/v2 eq=path"
V2_REPO="$V2_ROOT/lane"
V2_OK=0
if mkdir -p "$V2_REPO" && git -C "$V2_REPO" init -q >/dev/null 2>&1; then
  printf '.review-stage/\n' >"$V2_REPO/.gitignore"
  case "$V2_REPO" in
    *=*) V2_OK=1; ok "v2 fixture: a checkout whose PATH contains a legal '=' was built" ;;
    *) bad "v2 fixture: the fixture path carries no '=', so every case below would be vacuous" ;;
  esac
else
  bad "v2 fixture: could not build it — every case below would be vacuous"
fi

V2_RAW=""
if [ "$V2_OK" -eq 1 ]; then
  rs "$V2_REPO" open c --issue 3751 --agent spec-auditor
  rc_is 0 "v2: the stage opened in the '='-bearing checkout"
  # `open`'s RAW path line is deliberately NOT routed through the boundary, so it is the ground
  # truth this section compares against — and it is the reason `open` was scoped OUT of the finding.
  V2_RAW="$(printed_report_path)"
  if [ -n "$V2_RAW" ] && printf 'result: PASS\n\n## Findings\n\nnone.\n' >"$V2_RAW" 2>/dev/null &&
    [ -f "$V2_RAW" ]; then
    ok "v2: open's RAW path line names a file that EXISTS (the premise: there IS a real report to publish)"
  else
    bad "v2: open's raw path is unusable (got: $V2_RAW) — the assertions below would be vacuous"
    V2_OK=0
  fi
  case "$V2_RAW" in
    *=*) ok "v2: and that real path CONTAINS the '=' (so the corruption below is about this fixture)" ;;
    *) bad "v2: the real path carries no '=' — the fixture does not exercise the defect (got: $V2_RAW)" ;;
  esac
fi

if [ "$V2_OK" -eq 1 ]; then
  # (a) THE FINDING: the published value must BE the real path, and must NAME AN EXISTING FILE.
  #     Both are asserted, because they are different claims: a value could be textually equal to
  #     something and still not exist, and a value could exist and not be the report of record.
  rs "$V2_REPO" verdict c --issue 3751
  rc_is 0 "v2/publish: the verdict reports PASS"
  V2_PUB="${OUT#*report=}"
  if [ "$V2_PUB" = "$V2_RAW" ]; then
    ok "v2/publish: the published report= is EXACTLY the path open printed"
  else
    bad "v2/publish: the published report= is not the real path (published: $V2_PUB / real: $V2_RAW)"
  fi
  if [ -f "$V2_PUB" ]; then
    ok "v2/publish: and that published path names a file that EXISTS"
  else
    bad "v2/publish: the published path does NOT exist — the verdict advertises a file nobody can open ($V2_PUB)"
  fi
  # THE VALUE IS NOT MERELY UNMAPPED, IT IS WHOLE: the '=' survives rather than being dropped, which
  # a scrubber that DELETED the character would also satisfy for the equality above only by accident.
  case "$V2_PUB" in
    *"eq=path"*) ok "v2/publish: the '=' is present in the published value, not dropped or substituted" ;;
    *) bad "v2/publish: the '=' did not survive into the published value (got: $V2_PUB)" ;;
  esac

  # (b) THE CONFINEMENT CONTROL: a forged `report=` smuggled through ANOTHER field is still
  #     neutralised. This is the property the remainder rule RESTS on — unmapped, the plant below
  #     puts a REAL `report=` pair ahead of the measured one and the remainder parse takes the
  #     FIRST. Without this case, (a) is satisfiable by dropping the map from every field.
  V2_SF="$V2_REPO/.review-stage/issue-3751/c.stage"
  if [ -f "$V2_SF" ] &&
    LC_ALL=C sed -e 's|^agent:.*|agent: spec-auditor report=/forged/elsewhere.md|' \
      "$V2_SF" >"$V2_SF.new" 2>/dev/null && mv "$V2_SF.new" "$V2_SF"; then
    ok "v2/forge: the record's agent field was re-written to smuggle a report= pair"
  else
    bad "v2/forge: the plant could not be written, so the control below proves nothing"
  fi
  rs "$V2_REPO" verdict c --issue 3751
  rc_is 0 "v2/forge: the verdict still reports PASS — a display boundary decides nothing"
  # COUNTED, NOT MATCHED: the line legitimately CONTAINS `report=`, so a substring test passes on
  # the broken script too. Exactly one pair is the property.
  V2_N="$(printf '%s' "$OUT" | LC_ALL=C tr ' ' '\n' | LC_ALL=C grep -c '^report=' || true)"
  if [ "$V2_N" = "1" ]; then
    ok "v2/forge: the verdict line carries EXACTLY ONE 'report=' pair — a smuggled one cannot forge a second"
  else
    bad "v2/forge: the line carries $V2_N 'report=' pairs — the exemption re-enabled forgery (got: $OUT)"
  fi
  has "agent=spec-auditor report~/forged/elsewhere.md" \
    "v2/forge: and the smuggled '=' is still rendered '~' in the agent field — neutralised, not dropped"
  V2_PUB2="${OUT#*report=}"
  if [ "$V2_PUB2" = "$V2_RAW" ]; then
    ok "v2/forge: and the remainder still resolves to the REAL report path, not the forged one"
  else
    bad "v2/forge: the forged pair displaced the real path (got: $V2_PUB2)"
  fi
fi

# (c) THE ORDINARY CHECKOUT STILL ROUND-TRIPS — a guard that reds on correct input is the guard
#     agents learn to waive, and this is the case every other section's repository is.
V2_PLAIN="$(newrepo)"
if [ -n "$V2_PLAIN" ]; then
  rs "$V2_PLAIN" open c --issue 982 --agent spec-auditor
  rc_is 0 "v2/plain CONTROL: an ordinary stage opened"
  V2_PRAW="$(printed_report_path)"
  printf 'result: PASS\n' >"$V2_PRAW" 2>/dev/null || true
  rs "$V2_PLAIN" verdict c --issue 982
  V2_PPUB="${OUT#*report=}"
  if [ -n "$V2_PRAW" ] && [ "$V2_PPUB" = "$V2_PRAW" ]; then
    ok "v2/plain CONTROL: an '='-free path is published unchanged too"
  else
    bad "v2/plain CONTROL: an ordinary path was altered (published: $V2_PPUB / real: $V2_PRAW)"
  fi
else
  bad "v2/plain CONTROL: could not build an ordinary repository"
fi

# (d) STRUCTURAL — THE EXEMPTION IS COUPLED TO ITS JUSTIFICATION AND CONFINED TO ONE SITE.
#     Behavioural cases cover the shapes someone thought of; these pin the arrangement, which is
#     what a later change would break silently.
V2_SRC="$RS"
if [ "$(LC_ALL=C grep -c 'remainder_value()' "$V2_SRC" || true)" -eq 1 ]; then
  ok "v2/structural: there is exactly ONE remainder-exempt boundary function"
else
  bad "v2/structural: the remainder-exempt boundary is not defined exactly once"
fi
# CALLED FROM EXACTLY ONE PLACE. The exemption's justification — LAST on the line and read as the
# REMAINDER — is a property of the VERDICT line alone; the `status`, `OPEN-OK`, `already-open`,
# `AUTHOR-REFUSED`, `report-changed-mid-write` and `RECORD-OK` lines keep `field_value`, because no
# consumer reads any of them as a remainder and a permission derived from "no consumer exists
# today" is a permission derived from the absence of a bad signal.
V2_CALLS="$(LC_ALL=C grep -c 'remainder_value "' "$V2_SRC" || true)"
if [ "$V2_CALLS" -eq 1 ]; then
  ok "v2/structural: and it is CALLED from exactly one site, so the exemption cannot spread unnoticed"
else
  bad "v2/structural: the remainder-exempt boundary has $V2_CALLS call sites — the exemption is no longer confined"
fi
# THE ONE CALL SITE IS THE VERDICT LINE, AND `report=` IS STILL LAST ON IT.
V2_EMIT="$(LC_ALL=C grep -h 'RESULT: \$rendered' "$V2_SRC" 2>/dev/null || true)"
case "$V2_EMIT" in
  *'report=$(remainder_value "${STAGE_REPORT:-unresolved}")"')
    ok "v2/structural: the verdict line ends with report= through the remainder-exempt boundary" ;;
  *) bad "v2/structural: the verdict line's report= is not the last field through that boundary (got: $V2_EMIT)" ;;
esac
# AND THE TWO BOUNDARIES DIFFER IN EXACTLY ONE RESPECT: the '=' map. Asserted BEHAVIOURALLY against
# the shipped functions rather than by reading their source, because "does this map '='" is a
# question about what they DO. A control character must still be neutralised by BOTH, or the
# exemption would have traded a corrupted path for an injectable one (rounds 5/7/13/14).
V2_FN_OK=0
if V2_FV="$(LC_ALL=C awk '/^one_line\(\) \{/,/^\}/' "$V2_SRC")" &&
  V2_F2="$(LC_ALL=C awk '/^field_value\(\) \{/,/^\}/' "$V2_SRC")" &&
  V2_F3="$(LC_ALL=C awk '/^remainder_value\(\) \{/,/^\}/' "$V2_SRC")" &&
  [ -n "$V2_FV" ] && [ -n "$V2_F2" ] && [ -n "$V2_F3" ]; then
  V2_FN_OK=1
  ok "v2/structural: all three boundary functions were extracted (the differential below is not vacuous)"
else
  bad "v2/structural: a boundary function could not be extracted, so the differential is UNMEASURED"
fi
if [ "$V2_FN_OK" -eq 1 ]; then
  V2_OUT_F="$(printf '%s\n%s\n%s\n' "$V2_FV" "$V2_F2" \
    'field_value "a=b" ' | bash 2>/dev/null || true)"
  V2_OUT_R="$(printf '%s\n%s\n%s\n' "$V2_FV" "$V2_F3" \
    'remainder_value "a=b" ' | bash 2>/dev/null || true)"
  if [ "$V2_OUT_F" = "a~b" ]; then
    ok "v2/structural: field_value STILL maps '=' to '~' (the anti-forgery rule is untouched)"
  else
    bad "v2/structural: field_value no longer maps '=' (got: $V2_OUT_F)"
  fi
  if [ "$V2_OUT_R" = "a=b" ]; then
    ok "v2/structural: remainder_value does NOT map '=', which is the whole exemption"
  else
    bad "v2/structural: remainder_value still alters '=' (got: $V2_OUT_R)"
  fi
  # BOTH must still neutralise a control character. An ESC is used because round 5's J3 is the
  # finding that made the C0 range, not just the three whitespace controls, part of the contract.
  V2_ESC_R="$(printf '%s\n%s\n%s\n' "$V2_FV" "$V2_F3" \
    'remainder_value "$(printf "a\033[31mb")" ' | bash 2>/dev/null || true)"
  if [ "$V2_ESC_R" = "a?[31mb" ]; then
    ok "v2/structural: remainder_value STILL renders a C0 control byte visibly (the exemption is the '=' map ALONE)"
  else
    bad "v2/structural: remainder_value does not neutralise a control byte (got: $V2_ESC_R)"
  fi
  V2_NL_R="$(printf '%s\n%s\n%s\n' "$V2_FV" "$V2_F3" \
    'remainder_value "$(printf "a\nb")" ' | bash 2>/dev/null || true)"
  if [ "$V2_NL_R" = "a b" ]; then
    ok "v2/structural: and it still flattens a NEWLINE, so one record cannot become two"
  else
    bad "v2/structural: remainder_value lets a newline through — a second line is a forged record (got: $V2_NL_R)"
  fi
fi

# --- 30. A CHECKOUT PATH THIS GRAMMAR CANNOT CARRY IS REFUSED AT THE BOUNDARY (round 17, W2) ------
# THE FINDING (roborev job 396, W2). Every line this tool emits is a ONE-LINE record, and the
# `report=` field on it carries an ABSOLUTE path whose only variable component is the REPOSITORY
# ROOT. A root containing a NEWLINE is therefore not merely awkward — the two commands lie
# DIFFERENTLY about it:
#
#   * `open` prints the raw report path on its own line, so the value SPLIT across two physical
#     lines: the second carried NO `REVIEW-STAGE: ` prefix, which is the anchor every consumer of
#     this grammar reads, and the paste-ready spawn clause handed the agent two broken fragments.
#   * `verdict` flattens it through `remainder_value`, publishing `…/lane two/…` — a DIFFERENT,
#     NONEXISTENT path — on the one line whose whole promise is the absolute report-of-record path.
#     Measured on the shipped script in a checkout named `lane<LF>two`:
#       REVIEW-STAGE: c RESULT: NOT-RUN (…) … report=/…/lane two/.review-stage/issue-700/c.<n>.md
#     while the real file was at `/…/lane` + LF + `two/…`.
#
# ROUND 11 DECLARED THIS UNREPRESENTABLE AND LEFT IT ("a path containing a NEWLINE is not
# representable on a one-line grammar and never arrives"). THAT DECLARATION IS WITHDRAWN: the
# premise was false — such a path DOES arrive, because git resolves the root of whatever checkout
# the tool is run in — and silently publishing a wrong path is not an acceptable resting state for
# the value this grammar promises. So the refusal is AT THE BOUNDARY, at the ONE place the root is
# resolved, which is why every subcommand inherits it.
#
# AND IT IS THE MEASURED PROPERTY, NOT A CHARACTER LIST. `one_line` is what renders these values,
# so the question asked is *does this root survive it UNCHANGED* — a rendering that differs is a
# published path that does not exist, whatever byte caused it. A curated list of bad characters is
# a list to keep complete; this one cannot drift from the renderer, because it IS the renderer. The
# LF/CR case keeps its OWN detail line, because its harm is different in kind (the grammar itself
# breaks: a value that spans lines cannot be a field of a one-line record at any rendering).
W2_D="$T/w2"; mkdir -p "$W2_D"

# w2_repo <outvar> <dirname> — a git checkout at a LITERAL directory name, ASSIGNED to <outvar>.
# Returns non-zero (leaving <outvar> empty) when this filesystem cannot hold the name. NOTHING is
# asserted from a fixture that did not build: a case that silently ran in an ordinary directory
# would pass for the wrong reason (test_premerge_assert.sh §44's lesson).
#
# THE PATH IS ASSIGNED, NEVER PRINTED — A TEST HELPER THAT TRANSPORTS A PATH THROUGH A COMMAND
# SUBSTITUTION CANNOT CONSTRUCT ITS OWN SUBJECT (#3751 round 18, X1).
#
# This helper used to `printf '%s\n' "$d"` and every call site was `W2_x="$(w2_repo …)"` — so it
# had the EXACT blind spot round 17's subject has: `$( )` strips every trailing newline, so a
# fixture named with a TRAILING LF arrived at the case as its SIBLING path. Round 17's LF case
# passed only because its name is `lane<LF>two`, where the newline is EMBEDDED and survives; the
# trailing-LF shape — which is the one that defeats the shipped resolver, because a truncated path
# carries no newline for the representability refusal to see — could not be presented AT ALL. The
# harness therefore reported a refusal it had never actually tested against its worst input, which
# is this repository's harness-that-never-reached-the-code class inside the guard for a lossy
# capture. `printf -v` is byte-faithful and there is no substitution left to strip anything.
w2_repo() {
  local out="$1" d="$W2_D/$2"
  printf -v "$out" '%s' '' 2>/dev/null || return 1
  mkdir -p "$d" 2>/dev/null || return 1
  git -C "$d" init -q >/dev/null 2>&1 || return 1
  printf '.review-stage/\n' >"$d/.gitignore" 2>/dev/null || return 1
  printf -v "$out" '%s' "$d" 2>/dev/null || return 1
  return 0
}

W2_LF=""
W2_LF_NAME="lane
two"
if w2_repo W2_LF "$W2_LF_NAME" && [ -n "$W2_LF" ] && [ -d "$W2_LF" ]; then
  case "$W2_LF" in
    *"
"*) ok "w2 fixture: a git checkout whose PATH contains a literal LF was built (asserted, not assumed)" ;;
    *) bad "w2 fixture: the fixture path carries NO newline, so every case below would be vacuous ($W2_LF)"; W2_LF="" ;;
  esac
else
  bad "w2 fixture: a newline-bearing checkout could not be built on this filesystem — the cases below are UNMEASURED"
  W2_LF=""
fi

if [ -n "$W2_LF" ]; then
  # (a) `open` REFUSES, and it refuses BEFORE anything is written.
  rs "$W2_LF" open c --issue 700 --agent spec-auditor
  rc_is 64 "w2/open: a newline-bearing checkout is REFUSED (usage class, exactly as an unresolvable worktree is)"
  has "cannot be represented" "w2/open: and the refusal says the path cannot be REPRESENTED on this one-line grammar"
  has "NEWLINE" "w2/open: naming the newline specifically, because its harm is different in kind (the value spans lines)"
  if [ -d "$W2_LF/.review-stage" ]; then
    bad "w2/open: the refused open created the stage directory anyway"
  else
    ok "w2/open: NOTHING was written — no stage directory, so no report and no record"
  fi
  # (b) `verdict` REFUSES, and — the property that matters — it publishes NO path at all. A wrong
  #     path on this line is worse than no line, because the line is what a consumer binds to.
  rs "$W2_LF" verdict c --issue 700
  rc_is 64 "w2/verdict: the same refusal at the same boundary, so the two commands cannot disagree"
  hasnt "report=" "w2/verdict: and NO report= field is published — a nonexistent path is never advertised"
  hasnt "RESULT:" "w2/verdict: nor any RESULT: token, so nothing can read a verdict off a checkout we cannot name"
  # (c) `status` and (d) `record-author-performed` inherit it, because the check is at the ONE
  #     resolution site rather than in `open`.
  rs "$W2_LF" status c --issue 700
  rc_is 64 "w2/status: status inherits the refusal (the check is at the root resolution, not per subcommand)"
  rs "$W2_LF" record-author-performed c --issue 700 --reason 'no peer agent available on this box' \
    --evidence 'docs/round-artifacts/issue-3751-hand-c.md' --performed-by author
  rc_is 64 "w2/author: and so does record-author-performed, which would otherwise WRITE under that path"
fi

# (a2) A *TRAILING* NEWLINE IS THE SHAPE THAT DEFEATED THE RESOLVER, AND IT IS THE PEER-ARTIFACT
#      CLASS (#3751 round 18, X1; roborev job 397).
#
# THE FINDING. `require_repo_root` captured the root with `root="$(git rev-parse
# --show-toplevel)"`, and a command substitution strips EVERY trailing newline. So a checkout whose
# DIRECTORY NAME ends in an LF resolved to a DIFFERENT, EXISTING SIBLING path — and the captured
# value then carries NO newline, so the representability refusal above never fires. The tool
# proceeds, silently, against the sibling's `.review-stage/`: measured on the shipped script,
# `verdict` reported `REVIEW-STAGE: c RESULT: PASS … report=…/lanetrail/.review-stage/…` at exit 0
# off a report THIS LANE NEVER OPENED. That is #3616's peer-artifact class reached through a lossy
# capture rather than through a recency scan, and it is why the case below asserts the PEER's
# verdict is not reported, not merely that something refused.
#
# WHY ROUND 17's OWN LF CASE COULD NOT SEE IT. Its fixture is named `lane<LF>two`, where the
# newline is EMBEDDED and therefore survives `$( )`; and `w2_repo` itself returned the fixture path
# through a command substitution, so the trailing-LF shape could not be presented AT ALL. The
# helper now ASSIGNS (see `w2_repo`), which is what makes this case constructible.
#
# ROUND 13 (S2) ENUMERATED TRAILING-NEWLINE STRIPPING AND DECLARED IT HARMLESS — correctly, about
# REPORT CONTENT, where every grammar is per-line and column-zero anchored. It is FALSE about a
# PATH, whose stripped bytes are part of its identity. The durable rule this case pins: a
# lossy-capture conclusion must be RE-DERIVED PER CONSUMER, never carried from the consumer it was
# reasoned about.
W2_TR=""        # the real lane: a checkout whose directory name ENDS in an LF
W2_TR_SIB=""    # the sibling the stripped capture named
W2_TR_PEER=""   # the peer report the stripped capture would have certified
W2_TR_NAME="lanetrail
"
if w2_repo W2_TR "$W2_TR_NAME" && [ -n "$W2_TR" ] && [ -d "$W2_TR" ] &&
  case "$W2_TR" in *"
") true ;; *) false ;; esac; then
  ok "w2 fixture: a git checkout whose path ENDS in an LF was built (asserted on the path, not assumed)"
else
  bad "w2 fixture: a TRAILING-LF checkout could not be built — every case below is UNMEASURED"
  W2_TR=""
fi
# THE SIBLING IS THE PATH THE STRIPPED CAPTURE NAMES, and it must be a REAL, WORKING lane holding a
# CLEAN verdict — otherwise a refusal below could come from the sibling being broken rather than
# from the root being resolved faithfully.
#
# `$'\n'` AND NOT `"$(printf '\n')"`: the first draft of this line used the substitution and it
# expanded to the EMPTY STRING, because a command substitution strips every trailing newline — so
# the suffix removal removed NOTHING and the equality failed on a correctly-built fixture. The
# subject of this case, reproduced inside the case, in one line. `test_premerge_assert.sh` records
# the same trap at its own §44.
if [ -n "$W2_TR" ] && w2_repo W2_TR_SIB "lanetrail" && [ -n "$W2_TR_SIB" ] &&
  [ "$W2_TR_SIB" = "${W2_TR%$'\n'}" ]; then
  ok "w2 fixture: the sibling at the STRIPPED path exists and is exactly the trailing-LF path minus its last byte"
else
  bad "w2 fixture: the sibling path could not be built or is not the stripped form of the real one — the peer-read case proves nothing"
  W2_TR=""
fi
if [ -n "$W2_TR" ]; then
  rs "$W2_TR_SIB" open c --issue 704 --agent spec-auditor
  W2_TR_PEER="$(REPORT_OF "$W2_TR_SIB" 704 c)"
  printf 'result: PASS\n\n## Findings\n\nnone.\n' >"$W2_TR_PEER" 2>/dev/null || true
  rs "$W2_TR_SIB" verdict c --issue 704
  case "$OUT" in
    *"RESULT: PASS"*) ok "w2 fixture: the sibling's stage is VALID BAIT — read from the sibling ITSELF it reports RESULT: PASS" ;;
    *) bad "w2 fixture: the sibling's stage does not report PASS, so a refusal below is not evidence about the peer read (got: $OUT)"; W2_TR="" ;;
  esac
else
  bad "w2 fixture: the peer bait was not attempted, because the fixture above did not build"
fi
if [ -n "$W2_TR" ]; then
  W2_TR_PEER_BEFORE="$(LC_ALL=C cat "$W2_TR_PEER" 2>/dev/null || printf '<unreadable>')"
  # (i) `open` REFUSES — a DIFFERENT issue number, so a write that landed in EITHER root is visible
  #     as a directory that did not exist before.
  rs "$W2_TR" open c --issue 705 --agent spec-auditor
  rc_is 64 "w2/trailing-lf/open: a checkout whose path ENDS in an LF is REFUSED, not resolved to its sibling"
  has "NEWLINE" "w2/trailing-lf/open: under the newline cause, which is now REACHABLE — before the fix the captured value held no newline at all"
  if [ -d "$W2_TR/.review-stage/issue-705" ]; then
    bad "w2/trailing-lf/open: the refused open wrote under the real root anyway"
  else
    ok "w2/trailing-lf/open: nothing was written under the REAL root"
  fi
  if [ -d "$W2_TR_SIB/.review-stage/issue-705" ]; then
    bad "w2/trailing-lf/open: the refused open wrote into the SIBLING lane — the peer-artifact defect is live on the write side"
  else
    ok "w2/trailing-lf/open: and nothing was written into the SIBLING lane"
  fi
  # (ii) `verdict` REFUSES, and — the property that matters — it does not report the PEER's clean
  #      verdict. This is the exact measured false PASS.
  rs "$W2_TR" verdict c --issue 704
  rc_is 64 "w2/trailing-lf/verdict: the same refusal at the same boundary (the check is at the ONE root resolution)"
  hasnt "report=" "w2/trailing-lf/verdict: NO report= field is published — the sibling's path is never advertised as this lane's report of record"
  hasnt "RESULT:" "w2/trailing-lf/verdict: and NO RESULT: token at all, so the PEER's PASS is not reported as this lane's verdict"
  W2_TR_PEER_AFTER="$(LC_ALL=C cat "$W2_TR_PEER" 2>/dev/null || printf '<unreadable>')"
  if [ "$W2_TR_PEER_BEFORE" = "$W2_TR_PEER_AFTER" ]; then
    ok "w2/trailing-lf: the peer lane's report is byte-unchanged — the refusal neither read it as ours nor wrote over it"
  else
    bad "w2/trailing-lf: the peer lane's report CHANGED, so this lane wrote into another lane's stage"
  fi
else
  bad "w2/trailing-lf: UNMEASURED (1/8) — the fixture did not build"
  bad "w2/trailing-lf: UNMEASURED (2/8)"
  bad "w2/trailing-lf: UNMEASURED (3/8)"
  bad "w2/trailing-lf: UNMEASURED (4/8)"
  bad "w2/trailing-lf: UNMEASURED (5/8)"
  bad "w2/trailing-lf: UNMEASURED (6/8)"
  bad "w2/trailing-lf: UNMEASURED (7/8)"
  bad "w2/trailing-lf: UNMEASURED (8/8)"
fi

# (e) A CARRIAGE RETURN IS THE SAME CLASS AND IS REFUSED THE SAME WAY. `one_line` maps CR to a
#     space exactly as it maps LF, so a CR-bearing root publishes a nonexistent path too — and a
#     guard keyed on LF alone would be the character list this case exists to rule out.
W2_CR=""
W2_CR_NAME="$(printf 'lane\rtwo')"
if w2_repo W2_CR "$W2_CR_NAME" && [ -n "$W2_CR" ] && [ -d "$W2_CR" ] &&
  case "$W2_CR" in *"$(printf '\r')"*) true ;; *) false ;; esac; then
  ok "w2 fixture: a CR-bearing checkout was built"
  rs "$W2_CR" open c --issue 701 --agent spec-auditor
  rc_is 64 "w2/cr: a CR-bearing checkout is REFUSED as well"
  has "NEWLINE" "w2/cr: under the same named cause (CR and LF are one class here: both break the line)"
else
  ok "w2 fixture: SKIPPED the CR variant — this filesystem or shell could not hold the name; nothing is asserted about a case that did not run"
  ok "w2 fixture: (the same, second half — the case emits a fixed number of assertions either way)"
  ok "w2 fixture: (the same, third half)"
fi

# (f) A TAB IS NOT A NEWLINE AND IS STILL UNPUBLISHABLE. `one_line` maps it to a space, so the
#     published path does not exist — the SAME harm, a DIFFERENT cause, and it is why the check is
#     the renderer's own answer rather than a two-character test.
W2_TAB=""
W2_TAB_NAME="$(printf 'lane\ttwo')"
if w2_repo W2_TAB "$W2_TAB_NAME" && [ -n "$W2_TAB" ] && [ -d "$W2_TAB" ] &&
  case "$W2_TAB" in *"$(printf '\t')"*) true ;; *) false ;; esac; then
  ok "w2 fixture: a TAB-bearing checkout was built"
  rs "$W2_TAB" open c --issue 702 --agent spec-auditor
  rc_is 64 "w2/tab: a TAB-bearing checkout is REFUSED — the published path would not exist"
  has "cannot be represented" "w2/tab: under the representability cause"
  hasnt "NEWLINE" "w2/tab: and NOT under the newline one, because that would be a false rationale about this path"
else
  ok "w2 fixture: SKIPPED the TAB variant — this filesystem could not hold the name"
  ok "w2 fixture: (the same, second half)"
  ok "w2 fixture: (the same, third half)"
  ok "w2 fixture: (the same, fourth half)"
fi

# (g) CONTROL — A SINGLE SPACE STILL WORKS, END TO END. Round 11's Q3 exists because a path may
#     LEGALLY contain a space and `premerge-assert.sh` reads `report=` as the line remainder for
#     exactly that reason. A refusal that caught this would red on correct input and be the guard
#     agents learn to waive, so the control asserts the FULL path is published AND that it EXISTS.
W2_SP=""
if w2_repo W2_SP "work tree" && [ -n "$W2_SP" ]; then
  ok "w2 CONTROL: a SPACE-bearing checkout was built"
  rs "$W2_SP" open c --issue 703 --agent spec-auditor
  rc_is 0 "w2 CONTROL: a space-bearing checkout is NOT refused — a space survives one_line unchanged"
  W2_SP_REP="$(REPORT_OF "$W2_SP" 703 c)"
  rs "$W2_SP" verdict c --issue 703
  has "report=$W2_SP_REP" "w2 CONTROL: and verdict publishes the WHOLE space-bearing path, spaces included"
  if [ -f "$W2_SP_REP" ]; then
    ok "w2 CONTROL: which names a file that EXISTS (the published value is the real report of record)"
  else
    bad "w2 CONTROL: the published path does not exist: $W2_SP_REP"
  fi
else
  bad "w2 CONTROL: a space-bearing checkout could not be built — the false-refusal control is UNMEASURED"
  bad "w2 CONTROL: (the same absence, 2/4)"
  bad "w2 CONTROL: (the same absence, 3/4)"
  bad "w2 CONTROL: (the same absence, 4/4)"
fi

# (h) STRUCTURAL — THE CHECK IS AT THE ONE RESOLUTION SITE, which is what makes "every entry
#     inherits it" a property of the code rather than of this test's enumeration. Two pins: the
#     root is resolved exactly once in the script, and the refusal sits in that same function
#     BEFORE the global is set (a check after the assignment would let a subcommand build a path
#     from a root it had already accepted).
# COUNTED OVER CODE, NOT OVER PROSE (#3751 round 18, X1). A whole-file `grep -c` counted the
# idiom wherever it appeared, INCLUDING the comment in which round 18 quotes the lossy capture it
# replaced — so writing down what was fixed reported a second resolution site. That is the
# stale-prose failure inverted: a structural assert must be about the CODE, and a doctrine comment
# has to be able to NAME the idiom it retired. Full-line comments are blanked first (every
# occurrence in this file's subject is a full-line `  # …`), so a real second resolution — which
# is necessarily code — still reds.
W2_RESOLVE="$(LC_ALL=C sed -e 's/^[[:space:]]*#.*$//' "$RS" | LC_ALL=C grep -c 'rev-parse --show-toplevel' || true)"
if [ "$W2_RESOLVE" -eq 1 ]; then
  ok "w2/structural: the repository root is resolved in exactly ONE place, so one check covers every subcommand"
else
  bad "w2/structural: the root is resolved at $W2_RESOLVE sites — a second resolution would bypass the check"
fi
W2_RRR="$(LC_ALL=C sed -n '/^require_repo_root() {$/,/^}$/p' "$RS")"
if [ -n "$W2_RRR" ]; then
  ok "w2/structural: require_repo_root was extracted (the pins below are not vacuous)"
else
  bad "w2/structural: require_repo_root could not be extracted — the pins below are UNMEASURED"
fi
W2_REJ_LN="$(printf '%s\n' "$W2_RRR" | LC_ALL=C grep -n 'cannot be represented' | LC_ALL=C head -1 | cut -d: -f1)"
W2_SET_LN="$(printf '%s\n' "$W2_RRR" | LC_ALL=C grep -n 'REPO_ROOT="\$root"' | LC_ALL=C head -1 | cut -d: -f1)"
if [ -n "$W2_REJ_LN" ] && [ -n "$W2_SET_LN" ] && [ "$W2_REJ_LN" -lt "$W2_SET_LN" ]; then
  ok "w2/structural: the refusal is raised BEFORE the root is published to the rest of the script (lines $W2_REJ_LN < $W2_SET_LN)"
else
  bad "w2/structural: the representability refusal is not before REPO_ROOT is set (reject=$W2_REJ_LN set=$W2_SET_LN)"
fi
# AND IT IS THE RENDERER'S OWN ANSWER, NOT A CHARACTER LIST. A test keyed on a hand-written class
# would drift from `one_line` the day `one_line` changes; the check compares the rendering to the
# raw value, so it cannot.
case "$W2_RRR" in
  *'one_line "$root"'*)
    ok "w2/structural: the representability test asks the RENDERER (one_line) itself, so it cannot drift from it" ;;
  *) bad "w2/structural: the check does not compare against the renderer's own output, so it is a character list that can drift" ;;
esac
# AND THE RENDERER IT ASKS IS THE ONE THE PUBLISHED FIELD GOES THROUGH. The probe calls `one_line`
# rather than `remainder_value` so that section 29's '='-exemption confinement pin keeps counting
# EMIT sites only (a probe is not an emit) — which is sound exactly while the two agree, so that is
# asserted BEHAVIOURALLY over the shipped functions rather than left to the comment.
W2_AGREE_OK=0
if W2_OL="$(LC_ALL=C awk '/^one_line\(\) \{/,/^\}/' "$RS")" &&
  W2_RV="$(LC_ALL=C awk '/^remainder_value\(\) \{/,/^\}/' "$RS")" &&
  [ -n "$W2_OL" ] && [ -n "$W2_RV" ]; then
  W2_AGREE_OK=1
  ok "w2/structural: both renderers were extracted (the agreement check below is not vacuous)"
else
  bad "w2/structural: a renderer could not be extracted — the agreement between probe and emit is UNMEASURED"
fi
if [ "$W2_AGREE_OK" -eq 1 ]; then
  W2_SAMPLE='a=b	c  d
e'
  W2_A="$(printf '%s\n%s\n' "$W2_OL" 'one_line "$1"' | bash -s "$W2_SAMPLE" 2>/dev/null || true)"
  W2_B="$(printf '%s\n%s\n%s\n' "$W2_OL" "$W2_RV" 'remainder_value "$1"' | bash -s "$W2_SAMPLE" 2>/dev/null || true)"
  if [ -n "$W2_A" ] && [ "$W2_A" = "$W2_B" ]; then
    ok "w2/structural: the probe's renderer and the published field's renderer AGREE on a sample carrying a tab, a whitespace run, an '=' and a newline"
  else
    bad "w2/structural: probe and emit renderers DISAGREE (one_line='$W2_A' remainder_value='$W2_B') — the probe would accept a root the verdict line then corrupts"
  fi
fi
# AND THERE IS NO OPT-OUT. A checkout is always renamable, so an escape hatch could only buy a
# published path that does not exist — the same reasoning as the missing-schemas check's absence of
# one.
if [ "$(LC_ALL=C grep -c -E 'REVIEW_STAGE_ALLOW_[A-Z_]*PATH|ALLOW_UNREPRESENTABLE' "$RS" || true)" -eq 0 ]; then
  ok "w2/structural: no environment variable opts out of it — a renamable checkout needs no escape hatch"
else
  bad "w2/structural: an opt-out env var exists, which could only buy a published path that does not exist"
fi
# THE WITHDRAWN ROUND-11 DECLARATION MAY NOT SURVIVE ANYWHERE. It said such a path is not
# representable and NEVER ARRIVES; the second half was false, and a stale declaration is what stops
# the next person looking. Needles SPLIT so this guard cannot match its own source line.
W2_N1="a path containing a NEWLINE is not repr""esentable"
W2_N2="there is no newline to split a record line on"
W2_N3="no newline to split"
w2_carries_withdrawn() { LC_ALL=C grep -qiF -e "$W2_N1" -e "$W2_N2" -e "$W2_N3" "$1"; }
W2_SWEPT=0; W2_STALE=0; W2_STALE_WHERE=""
for W2_F in "$RS" "$SCRIPT_DIR/../flow/premerge-assert.sh" \
  "$SCRIPT_DIR/../../CLAUDE.md" "$SCRIPT_DIR/../../docs/development/review-stage-reporting.md"; do
  [ -f "$W2_F" ] || continue
  W2_SWEPT=$((W2_SWEPT + 1))
  if w2_carries_withdrawn "$W2_F"; then
    W2_STALE=$((W2_STALE + 1)); W2_STALE_WHERE="$W2_STALE_WHERE $W2_F"
  fi
done
if [ "$W2_STALE" -eq 0 ] && [ "$W2_SWEPT" -eq 4 ]; then
  ok "w2/structural: round 11's WITHDRAWN declaration (a newline-bearing path is unrepresentable and never arrives) survives nowhere ($W2_SWEPT site(s) swept)"
else
  bad "w2/structural: $W2_STALE of $W2_SWEPT swept site(s) still carry it (want 0 of 4):$W2_STALE_WHERE"
fi
# A POSITIVE CONTROL: a sweep that matches nothing is indistinguishable from a sweep that cannot
# match. This repository has the incident where a scan built to close one blind spot shipped with
# its own and reported CLEAN on four real sites.
W2_PLANT="$T/w2-withdrawn-plant.md"
printf 'prose, then the withdrawn claim: %s, stated as a residual\n' "$W2_N1" >"$W2_PLANT" 2>/dev/null || true
if [ -f "$W2_PLANT" ] && w2_carries_withdrawn "$W2_PLANT"; then
  ok "w2/structural CONTROL: the sweep DOES find the withdrawn declaration when it is present"
else
  bad "w2/structural CONTROL: the sweep did not find a PLANTED copy — the clean result above proves nothing"
fi

# --- 31. THE RECORD AND THE REPORT MUST BE *ONE* OBSERVATION (round 17, W1) -----------------------
# THE FINDING (roborev job 396, W1). `record-author-performed` read the REPORT using the generation
# loaded EARLIER and then read the STAGE RECORD independently. An `open --force` publishing
# generation B between those two reads left the final re-verifications comparing an unchanged
# report **A** against an unchanged record **B** — each individually consistent — so the recording
# published `AUTHOR-PERFORMED` over **B** without ever inspecting B's verdict, without requiring
# `--force`, and with a trace claiming **A** was superseded when **B** was. A blocking `FINDINGS`
# in B stopped being the stage's verdict silently, and the audit trail said the wrong thing about
# which generation that was. Falsifying the audit trail is the worst failure this tool can have: it
# is the harm #3751 exists to prevent, committed by the mechanism itself.
#
# THIS IS THE THIRD INSTANCE OF ONE SHAPE IN THIS ISSUE, and that is why the fix is a MECHANISM and
# not a third patch: round 9's N2 (`premerge-assert` validated `head-sha` from one read and consumed
# a second read for the nonce), round 12's R2 (`classify_report` read its fields independently, so a
# verdict could be assembled from versions that never coexisted), and now the record and the report
# read from DIFFERENT generations. Each was fixed at its own site. The consolidation is ONE
# primitive — `observe_stage` — that captures the record's bytes, derives the generation THOSE bytes
# name, reads THAT generation's report, and RE-VERIFIES the record has not moved between the two;
# anything inconsistent is a NAMED refusal, never a silent second opinion. Every decision path
# (`verdict`, `status`, `record-author-performed`) reasons from one such observation.
W1_D="$T/w1"; mkdir -p "$W1_D"
W1_REASON='no peer agent available on this box; hand C against the spec deltas'
W1_EV='docs/round-artifacts/issue-3751-hand-c.md'

# THE INTERLEAVE: a concurrent `open --force` that publishes a NEW generation, into whose report a
# reviewer then lands a BLOCKING verdict. Driven by running the SHIPPED script (baked in at build
# time — the scratch copy under test must not be the one that opens the stage, or the case would be
# measuring the mutant) and then writing into the generation the record now names. SIMULATED, NOT
# RACED: one injected line at a fixed point, nothing concurrent, no timing dependence.
W1_INJECTION='  { bash '"'$RS'"' open c --issue "$issue" --agent spec-auditor --force >/dev/null 2>&1; W1_B="$(LC_ALL=C sed -n '"'"'s/^report-nonce:[[:space:]]*//p'"'"' "$sfile" | LC_ALL=C head -1)"; printf '"'"'result: FINDINGS\n\n### [BLOCKER] N1_LATE_REVIEWER landed this in generation B\n'"'"' >"$(dirname "$sfile")/$kind.$W1_B.md"; } || true'

# The anchor is the LAST `dir="$(dirname "$sfile")"` — the one inside `record-author-performed`
# (cmd_open holds the first) — so the plant lands after the already-recorded check, AFTER `sfile` is
# assigned (the injected line reads it, and an unset one would make the plant a no-op that looked
# like a case), and BEFORE this call reads the record it is going to republish. It is deliberately a
# line that exists in BOTH the pre- and post-fix shapes, so the same case measures both.
if n1_build_last "$W1_D/interleave.sh" 'dir="$(dirname "$sfile")"' "$W1_INJECTION"; then
  ok "w1/interleave: the plant landed at the last dirname assignment (asserted, not assumed)"
else
  bad "w1/interleave: the plant did NOT land, so this case proves nothing"
fi
R31="$(newrepo)"
rs "$R31" open c --issue 710 --agent spec-auditor
W1_A_NONCE="$(RECORD_NONCE "$R31" 710 c)"
if [ -n "$W1_A_NONCE" ]; then
  ok "w1/interleave PREMISE: generation A was opened and the record names it ($W1_A_NONCE)"
else
  bad "w1/interleave PREMISE: no generation A nonce — the case below cannot distinguish the generations"
fi
OUT="$(cd "$R31" && bash "$W1_D/interleave.sh" record-author-performed c --issue 710 \
  --reason "$W1_REASON" --evidence "$W1_EV" --performed-by author 2>&1)"; RC=$?
# GENERATION B IS FOUND ON DISK, NOT PREDICTED: its nonce is generated by the interleaved
# `open --force` and is unknowable to this test, so it is located by the reviewer's own marker.
# Reading the RECORD for it would be wrong — pre-fix the record ends up naming the SUBSTITUTE's
# generation, which is the defect, not the premise.
W1_BFILE="$(LC_ALL=C grep -rl 'N1_LATE_REVIEWER' "$R31/.review-stage/issue-710" 2>/dev/null | LC_ALL=C head -1)"
W1_B_NONCE=""
if [ -n "$W1_BFILE" ]; then
  W1_B_NONCE="$(basename "$W1_BFILE")"; W1_B_NONCE="${W1_B_NONCE#c.}"; W1_B_NONCE="${W1_B_NONCE%.md}"
fi
# THE INTERLEAVE PREMISE, MEASURED ON DISK: generation B exists, is DIFFERENT from A, and really
# holds the blocking verdict. Without this the case could pass because nothing happened at all.
if [ -n "$W1_B_NONCE" ] && [ "$W1_B_NONCE" != "$W1_A_NONCE" ]; then
  ok "w1/interleave PREMISE: the plant published a DIFFERENT generation B ($W1_B_NONCE != $W1_A_NONCE)"
else
  bad "w1/interleave PREMISE: no second generation was published (A=$W1_A_NONCE B=${W1_B_NONCE:-<none>}) — the interleave did not happen"
fi
if [ -n "$W1_BFILE" ] && LC_ALL=C grep -q '^result: FINDINGS' "$W1_BFILE" 2>/dev/null; then
  ok "w1/interleave PREMISE: and a BLOCKING verdict really is recorded in generation B"
else
  bad "w1/interleave PREMISE: generation B holds no blocking verdict, so nothing dangerous was on offer"
fi
rc_is 2 "w1/interleave: the recording is REFUSED — the generation it would supersede is not the one it inspected"
has "changed" "w1/interleave: and the refusal says something CHANGED between the reads (never a silent second opinion)"
hasnt "supersedes-report-nonce=$W1_A_NONCE" "w1/interleave: NOTHING records that generation A was superseded — A is not what this call would have taken over from"
hasnt "RECORD-OK" "w1/interleave: no successful recording is reported"
# THE RECORD STILL NAMES B, so the generation whose verdict was never inspected is still current.
W1_NOW_NONCE="$(RECORD_NONCE "$R31" 710 c)"
if [ -n "$W1_B_NONCE" ] && [ "$W1_NOW_NONCE" = "$W1_B_NONCE" ]; then
  ok "w1/interleave: the stage record still names generation B — nothing was published over it"
else
  bad "w1/interleave: the record now names '$W1_NOW_NONCE', not generation B ('$W1_B_NONCE') — a substitute was published over a verdict nobody inspected"
fi
# THE PROPERTY THAT MATTERS: the blocking verdict in generation B is STILL THIS STAGE'S VERDICT.
rs "$R31" verdict c --issue 710
rc_is 4 "w1/interleave: the stage still reports FINDINGS — the blocking verdict in generation B did not become history"
has "RESULT: FINDINGS" "w1/interleave: naming the blocking token"
hasnt "AUTHOR-PERFORMED" "w1/interleave: and the merge-proceeding token was NOT published over a verdict nobody inspected"

# CONTROL — AN UNDISTURBED `--force` SUPERSESSION STILL WORKS AND NAMES THE RIGHT GENERATION. The
# refusal above is satisfiable by a tool that refuses every supersession, and the trace is
# satisfiable by one that records nothing, so both halves are asserted here on the path B2
# deliberately leaves open.
R31C="$(newrepo)"
rs "$R31C" open c --issue 711 --agent spec-auditor
W1_C_NONCE="$(RECORD_NONCE "$R31C" 711 c)"
printf 'result: FINDINGS\n\n### [BLOCKER] a real gap the author is superseding by hand\n' \
  >"$(REPORT_OF "$R31C" 711 c)"
rs "$R31C" record-author-performed c --issue 711 --reason "$W1_REASON" --evidence "$W1_EV" \
  --performed-by author --force
rc_is 0 "w1 CONTROL: an undisturbed forced supersession still records"
has "replaced-verdict=FINDINGS" "w1 CONTROL: naming the token it replaced"
has "supersedes-report-nonce=$W1_C_NONCE" "w1 CONTROL: and the generation it took over from is EXACTLY the one it inspected"
W1_C_NEW="$(REPORT_OF "$R31C" 711 c)"
OUT="$(cat "$W1_C_NEW" 2>/dev/null || printf '<absent>\n')"; RC=0
has "supersedes-report-nonce: $W1_C_NONCE" "w1 CONTROL: the published report carries the same generation in its own trace"
has "replaced-verdict: FINDINGS" "w1 CONTROL: and the replaced token, so an operator can follow both"

# CONTROL — THE ORDINARY PATH IS UNTOUCHED: a sentinel-only report is still freely replaceable,
# because a guard that reds on correct input is the guard agents learn to waive.
R31S="$(newrepo)"
rs "$R31S" open c --issue 712 --agent spec-auditor
W1_S_NONCE="$(RECORD_NONCE "$R31S" 712 c)"
rs "$R31S" record-author-performed c --issue 712 --reason "$W1_REASON" --evidence "$W1_EV" \
  --performed-by author
rc_is 0 "w1 CONTROL: the normal recording over a sentinel still succeeds"
has "supersedes-report-nonce=$W1_S_NONCE" "w1 CONTROL: naming the sentinel's own generation"
hasnt "replaced-verdict" "w1 CONTROL: and claiming NO replacement, because nothing was recorded there"

# THE INTERLEAVE *INSIDE* THE OBSERVATION. The case above lands between the observation and the
# publication; this one lands between the two halves of the observation itself — after the record's
# bytes are captured and before the report of the generation they name is read — which is the span
# the primitive's own re-verification exists for. The anchor is the report read INSIDE the
# primitive, so on a script that has no such single site the plant cannot land and this case fails
# CLOSED (4 bads) rather than passing vacuously.
if n1_build "$W1_D/inside.sh" 'STAGE_REPORT_OBS="$(report_bytes "$STAGE_REPORT")"' "$W1_INJECTION"; then
  ok "w1/inside: the plant landed between the record capture and the report read"
  R31I="$(newrepo)"
  rs "$R31I" open c --issue 713 --agent spec-auditor
  OUT="$(cd "$R31I" && bash "$W1_D/inside.sh" record-author-performed c --issue 713 \
    --reason "$W1_REASON" --evidence "$W1_EV" --performed-by author 2>&1)"; RC=$?
  rc_is 2 "w1/inside: a record that MOVED between the two halves of one observation is a NAMED refusal"
  has "changed" "w1/inside: the refusal says the record changed while it was being observed"
  rs "$R31I" verdict c --issue 713
  rc_is 4 "w1/inside: and the blocking verdict that arrived in the new generation is still the stage's verdict"
else
  bad "w1/inside: the plant did NOT land — there is no single record-then-report observation to interleave"
  bad "w1/inside: (the same absence, 2/4)"
  bad "w1/inside: (the same absence, 3/4)"
  bad "w1/inside: (the same absence, 4/4)"
fi

# AND A RECORD THAT MOVED IS NOT REPORTED AS UNREADABLE. `status`/`verdict` must name the CHANGE,
# because the operator action differs: read it again, versus repair the record or chmod it. A false
# rationale is worse than none (round 2, B7; round 4, H4).
if n1_build "$W1_D/read-moved.sh" 'STAGE_REPORT_OBS="$(report_bytes "$STAGE_REPORT")"' "$W1_INJECTION"; then
  ok "w1/read: the plant landed for the READ-side case too"
  R31R="$(newrepo)"
  rs "$R31R" open c --issue 714 --agent spec-auditor
  OUT="$(cd "$R31R" && bash "$W1_D/read-moved.sh" verdict c --issue 714 2>&1)"; RC=$?
  rc_is 5 "w1/read: verdict over a record that moved mid-observation is a NON-VERDICT (NOT-RUN)"
  has "stage record changed mid-read" "w1/read: naming the CHANGE, not an unreadable record — the operator action is to read it again"
  OUT="$(cd "$R31R" && bash "$W1_D/read-moved.sh" status c --issue 714 2>&1)"; RC=$?
  has "state=stage-record-changed" "w1/read: and status maps it to its OWN state, not onto stage-record-unreadable"
else
  bad "w1/read: the plant did NOT land for the read-side case"
  bad "w1/read: (the same absence, 2/5)"
  bad "w1/read: (the same absence, 3/5)"
  bad "w1/read: (the same absence, 4/5)"
  bad "w1/read: (the same absence, 5/5)"
fi

# (c) THE STRUCTURAL GUARD OVER THE OBSERVATION BOUNDARY, WITH ITS POSITIVE CONTROLS. The
#     behavioural cases above prove the interleave is refused TODAY; they cannot see a NEW second
#     read added tomorrow in a place none of them drives. That is the same argument round 14 made
#     for `read-boundary-scan.sh` — three rounds of "a boundary exists and one path bypasses it" —
#     so the answer is a mechanism, `scripts/tests/lib/observation-boundary-scan.sh`, which
#     attributes every STAGE-FILE READER call to the function it appears in and requires the owner
#     to be the primitive (or a statement declared in the scanner WITH ITS REASON).
OBS="$SCRIPT_DIR/lib/observation-boundary-scan.sh"
if [ ! -f "$OBS" ]; then
  bad "obs-guard: $OBS is missing — the structural guard did not run (1/9)"
  bad "obs-guard: the same absence (2/9)"
  bad "obs-guard: the same absence (3/9)"
  bad "obs-guard: the same absence (4/9)"
  bad "obs-guard: the same absence (5/9)"
  bad "obs-guard: the same absence (6/9)"
  bad "obs-guard: the same absence (7/9)"
  bad "obs-guard: the same absence (8/9)"
  bad "obs-guard: the same absence (9/9)"
else
  OBS_OUT="$(bash "$OBS" "$RS" 2>&1)"; OBS_RC=$?
  if [ "$OBS_RC" -eq 0 ]; then
    ok "obs-guard: the SHIPPED review-stage.sh is CLEAN — no decision path reads a stage file for itself"
  else
    bad "obs-guard: the shipped review-stage.sh has an observation-boundary violation: $OBS_OUT"
  fi
  case "$OBS_OUT" in
    *"NOT COVERED"*) ok "obs-guard: the scan DECLARES what it does not cover, on every run" ;;
    *) bad "obs-guard: the scan did not declare its scope (got: $OBS_OUT)" ;;
  esac
  case "$OBS_OUT" in
    *"reader call(s) attributed"*) ok "obs-guard: and it reports its COUNTS — attributed calls and declared reads, not an adjective" ;;
    *) bad "obs-guard: the scan did not report its subject counts (got: $OBS_OUT)" ;;
  esac
  # CONTROL (a): AN INDEPENDENT SECOND READ OF THE RECORD in the decision path that had the defect.
  #              This is the pre-fix shape itself: `record-author-performed` reading the record for
  #              its rewrite after the report had been read from the earlier generation.
  OBS_D="$T/obs-plant"; mkdir -p "$OBS_D"
  LC_ALL=C sed -e '/^cmd_record_author_performed() {/a\  PLANTED_SECOND="$(stage_record_text "$sfile")"' \
    "$RS" >"$OBS_D/review-stage.sh" 2>/dev/null || true
  if LC_ALL=C grep -q 'PLANTED_SECOND' "$OBS_D/review-stage.sh" 2>/dev/null; then
    ok "obs-guard/control: the second-read plant landed in the scratch copy (asserted, not assumed)"
  else
    bad "obs-guard/control: the plant did NOT land, so the control below proves nothing"
  fi
  OBS_POUT="$(bash "$OBS" "$OBS_D/review-stage.sh" 2>&1)"; OBS_PRC=$?
  if [ "$OBS_PRC" -ne 0 ]; then
    ok "obs-guard/control: the guard REDS on an independent second read in a decision path"
  else
    bad "obs-guard/control: the guard reported CLEAN on a planted second read — it proves nothing (got: $OBS_POUT)"
  fi
  # A BARE RED IS NOT EVIDENCE EITHER: an unrelated breakage produces the same exit code, so the
  # guard must NAME the reader, the function and the line.
  case "$OBS_POUT" in
    *'`stage_record_text` reads a stage file inside `cmd_record_author_performed`'*)
      ok "obs-guard/control: and it NAMES the reader and the function, so the red is attributable" ;;
    *) bad "obs-guard/control: the guard red without naming the reader and its function (got: $OBS_POUT)" ;;
  esac
  OBS_PLINE="$(LC_ALL=C grep -n 'PLANTED_SECOND' "$OBS_D/review-stage.sh" | LC_ALL=C head -1 | cut -d: -f1)"
  case "$OBS_POUT" in
    *"review-stage.sh:$OBS_PLINE "*)
      ok "obs-guard/control: and it names the planted LINE, not just the file" ;;
    *) bad "obs-guard/control: the guard did not name the planted line $OBS_PLINE (got: $OBS_POUT)" ;;
  esac
  # CONTROL (b): A DIFFERENT READER, in a DIFFERENT decision path — so the reader set is a declared
  #              LIST and the attribution is not one hard-coded pair.
  OBS_D2="$T/obs-plant2"; mkdir -p "$OBS_D2"
  LC_ALL=C sed -e '/^cmd_verdict() {/a\  PLANTED_RB="$(report_bytes "$STAGE_REPORT")"' \
    "$RS" >"$OBS_D2/review-stage.sh" 2>/dev/null || true
  OBS_P2="$(bash "$OBS" "$OBS_D2/review-stage.sh" 2>&1)"; OBS_P2RC=$?
  if [ "$OBS_P2RC" -ne 0 ] && [ "${OBS_P2#*'`report_bytes` reads a stage file inside `cmd_verdict`'}" != "$OBS_P2" ]; then
    ok "obs-guard/control: a planted report read in `verdict` reds too and is named — the reader set is a LIST"
  else
    bad "obs-guard/control: a planted report_bytes read in cmd_verdict was not caught or not named (rc=$OBS_P2RC; got: $OBS_P2)"
  fi
  # CONTROL (c): A DECISION PATH THAT DOES NOT OBSERVE AT ALL. Without this, the guard would be
  #              satisfied by a script whose paths read nothing and decide from globals a previous
  #              call left behind — which is not "one observation", it is none.
  OBS_D3="$T/obs-plant3"; mkdir -p "$OBS_D3"
  LC_ALL=C sed -e '/^  observe_stage "\$KI_ISSUE" "\$KI_KIND"$/d' "$RS" >"$OBS_D3/review-stage.sh" 2>/dev/null || true
  OBS_P3="$(bash "$OBS" "$OBS_D3/review-stage.sh" 2>&1)"; OBS_P3RC=$?
  if [ "$OBS_P3RC" -ne 0 ] && [ "${OBS_P3#*"calls \`observe_stage\` 0 time(s)"}" != "$OBS_P3" ]; then
    ok "obs-guard/control: a decision path that takes NO observation is a named FAIL, not a clean run"
  else
    bad "obs-guard/control: a decision path with no observation was not caught (rc=$OBS_P3RC; got: $OBS_P3)"
  fi
  # CONTROL (d): TWO OBSERVATIONS IN ONE PATH — the defect itself, wearing the primitive's name.
  OBS_D4="$T/obs-plant4"; mkdir -p "$OBS_D4"
  LC_ALL=C sed -e '/^cmd_status() {/a\  observe_stage "$KI_ISSUE" "$KI_KIND"' "$RS" >"$OBS_D4/review-stage.sh" 2>/dev/null || true
  OBS_P4="$(bash "$OBS" "$OBS_D4/review-stage.sh" 2>&1)"; OBS_P4RC=$?
  if [ "$OBS_P4RC" -ne 0 ] && [ "${OBS_P4#*"calls \`observe_stage\` 2 time(s)"}" != "$OBS_P4" ]; then
    ok "obs-guard/control: TWO observations in one decision path is a named FAIL — the primitive's name is not a licence"
  else
    bad "obs-guard/control: two observations in one path were not caught (rc=$OBS_P4RC; got: $OBS_P4)"
  fi
  # CONTROL (e): A STALE ALLOWLIST ENTRY is its own failure. An entry matching nothing excuses
  #              nothing — and it is the signal that the read it described has CHANGED, which is why
  #              entries are matched on SOURCE TEXT and never by line number.
  OBS_D5="$T/obs-stale"; mkdir -p "$OBS_D5"
  LC_ALL=C sed -e 's|now_obs="\$(report_bytes "\$STAGE_REPORT")"|now_obs="$(report_bytes "${STAGE_REPORT}")"|' \
    "$RS" >"$OBS_D5/review-stage.sh" 2>/dev/null || true
  OBS_P5="$(bash "$OBS" "$OBS_D5/review-stage.sh" 2>&1)"
  case "$OBS_P5" in
    *"STALE allowlist entry"*) ok "obs-guard/stale: an allowlist entry whose source text has CHANGED is reported STALE by name, never kept as a standing excusal" ;;
    *) bad "obs-guard/stale: the guard did not report a stale entry when the declared re-verification was reworded (got: $OBS_P5)" ;;
  esac
  # CONTROL (f): AN UNKNOWN SUBJECT IS REFUSED, NOT REPORTED CLEAN. A scanner with no declared
  #              primitive for a script knows nothing about it, and `premerge-assert.sh` is the
  #              subject someone will reach for first — it is DECLARED as not covered, with its
  #              reason, rather than silently passing.
  OBS_P6="$(bash "$OBS" "$SCRIPT_DIR/../flow/premerge-assert.sh" 2>&1)"; OBS_P6RC=$?
  if [ "$OBS_P6RC" -eq 2 ] && [ "${OBS_P6#*"no observation primitive is declared"}" != "$OBS_P6" ]; then
    ok "obs-guard/subject: an undeclared subject is REFUSED by name (exit 2), never reported clean"
  else
    bad "obs-guard/subject: an undeclared subject was not refused (rc=$OBS_P6RC; got: $OBS_P6)"
  fi
fi

# (d) STRUCTURAL — THE OBSERVATION IS ONE READ OF EACH FILE, AND THE RE-VERIFICATION IS INSIDE IT.
#     The scanner above says WHO may read; these say the primitive is shaped the way its contract
#     claims, which the scanner deliberately does not check.
W1_OBSR="$(LC_ALL=C sed -n '/^observe_record() {/,/^}$/p' "$RS" 2>/dev/null || true)"
W1_OBSS="$(LC_ALL=C sed -n '/^observe_stage() {/,/^}$/p' "$RS" 2>/dev/null || true)"
if [ -n "$W1_OBSR" ] && [ -n "$W1_OBSS" ]; then
  ok "w1/structural: both halves of the primitive were extracted (the pins below are not vacuous)"
else
  bad "w1/structural: a half of the primitive could not be extracted — the pins below are UNMEASURED"
fi
# COUNTED OVER CODE, NOT THE PROSE BESIDE IT. The primitive's comments legitimately NAME its
# reader (they state what its three statuses mean), so a whole-body count reds on a correct script
# — a guard that reds on correct input is the guard agents learn to waive, and this is the second
# place in this round where the fix was to strip comments before judging.
W1_OBSR_CODE="$(printf '%s\n' "$W1_OBSR" | LC_ALL=C grep -v '^[[:space:]]*#' || true)"
W1_NRD="$(printf '%s\n' "$W1_OBSR_CODE" | LC_ALL=C grep -c 'stage_record_text' || true)"
if [ "$W1_NRD" = "1" ]; then
  ok "w1/structural: the record half reads the record file EXACTLY ONCE — every field is parsed from that capture"
else
  bad "w1/structural: the record half reads the record file $W1_NRD time(s); more than one is the class W1 names"
fi
if [ "$(printf '%s\n' "$W1_OBSR" | LC_ALL=C grep -c 'read_field_from "\$STAGE_RECORD_TEXT"' || true)" -ge 4 ]; then
  ok "w1/structural: and its display fields come from that capture (read_field_from over the text), not from the file"
else
  bad "w1/structural: the record half still reads fields from somewhere other than its own capture"
fi
# THE RE-VERIFICATION IS AFTER THE REPORT READ, which is the whole property: before it, it would
# compare the record with itself and see nothing.
W1_RB_LN="$(printf '%s\n' "$W1_OBSS" | LC_ALL=C grep -n 'report_bytes "\$STAGE_REPORT"' | LC_ALL=C head -1 | cut -d: -f1)"
W1_RV_LN="$(printf '%s\n' "$W1_OBSS" | LC_ALL=C grep -n 'again="\$(stage_record_text' | LC_ALL=C head -1 | cut -d: -f1)"
if [ -n "$W1_RB_LN" ] && [ -n "$W1_RV_LN" ] && [ "$W1_RV_LN" -gt "$W1_RB_LN" ]; then
  ok "w1/structural: the re-verification is taken AFTER the report read (lines $W1_RB_LN < $W1_RV_LN) — before it, it would compare the record with itself"
else
  bad "w1/structural: the re-verification is not after the report read (report=$W1_RB_LN reverify=$W1_RV_LN)"
fi
# AND A MOVED RECORD DISCARDS THE REPORT OBSERVATION. Keeping it would publish bytes belonging to a
# generation the observation can no longer claim is current — the second opinion in person.
case "$W1_OBSS" in
  *'STAGE_REPORT_OBS=""; STAGE_REPORT_STATE=""'*)
    ok "w1/structural: a moved record DISCARDS the report observation rather than publishing it beside a defect" ;;
  *) bad "w1/structural: the report observation survives a moved record, so a consumer could still read it" ;;
esac
# AND THE DEFECT KIND IS A CLOSED TOKEN, NOT A MATCH ON THE DETAIL SENTENCE. A consumer keyed on
# the prose would be reading a DIAGNOSTIC as a CONTROL (#3312), and it fired immediately while this
# was being written: two legitimate detail sentences both contain the words `report-nonce`, so a
# text match sent a read-level failure to the refusal that says "this record names two".
if [ "$(LC_ALL=C grep -c 'STAGE_RECORD_DEFECT_KIND=' "$RS" || true)" -ge 6 ] &&
  LC_ALL=C grep -q 'case "\$STAGE_RECORD_DEFECT_KIND" in' "$RS"; then
  ok "w1/structural: the record defect is published as a CLOSED KIND beside its sentence, and consumers branch on the kind"
else
  bad "w1/structural: the defect kind is missing or nobody branches on it, so a consumer must match the detail prose"
fi


# --- 32. THE *READ* PATH REFUSES A SYMLINKED ARTIFACT (round 19, Y1) ------------------------------
# THE FINDING: round 1's F5 walk refuses a symlink where this tool WRITES, and NOTHING refused one
# where it READS. `report_bytes` decided the verdict through `[ -f ]` plus an input redirection,
# and BOTH dereference — so replacing this generation's report with a link to any regular file
# holding `result: PASS` made `verdict` (and `premerge-assert.sh`'s AUTO C validation with it)
# accept a verdict from an artifact that is not the report of record. Measured on the shipped
# script before the fix: `RESULT: PASS`, exit 0, off a link into /tmp.
#
# THE SAME REASONING IS APPLIED TO EVERY ARTIFACT THE READ PATH OPENS — the REPORT and the STAGE
# RECORD — because the record is what names WHICH generation is authoritative and carries the
# `head-sha:` the stage was opened at, so a link there redirects the decision at its root.
#
# THE DANGLING CASE IS THE ONE THAT PROVES `-f` CANNOT ANSWER IT: for a dangling link `[ -f ]` is
# FALSE, which is `no-such-file`, which is the PERMISSIVE `absent` state that the clobber guard
# treats as "there is no recorded verdict to destroy".
#
# DECLARED RESIDUAL, asserted as DECLARED rather than as closed: a leaf `-L` test before the open
# leaves a TOCTOU window bash cannot close (no `openat`, no `O_NOFOLLOW`) and that window is
# #3929's family. What is closed COMPLETELY is the non-racing case — a link planted at any earlier
# time and simply followed, with no check at all.
R32="$(newrepo)"
rs "$R32" open c --issue 1901 --agent spec-auditor
rc_is 0 "y1: the stage opened"
R32_REP="$(REPORT_OF "$R32" 1901 c)"

# (a) POSITIVE CONTROL FIRST — an ordinary regular-file report yields its verdict, so the refusals
#     below are not a mechanism that answers NOT-RUN for everything.
printf 'result: PASS\n\nreviewed.\n' >"$R32_REP"
rs "$R32" verdict c --issue 1901
rc_is 0 "y1/control: a REGULAR-FILE report still yields its verdict (exit 0)"
has "RESULT: PASS" "y1/control: and the token is PASS"

# (b) A SYMLINK TO A REGULAR FILE RECORDING `result: PASS` — the finding's own scenario.
R32_BAIT="$T/y1-bait-$$.md"
printf 'result: PASS\n\nNOT the report of record.\n' >"$R32_BAIT"
rm -f "$R32_REP"
if ln -s "$R32_BAIT" "$R32_REP" 2>/dev/null && [ -L "$R32_REP" ]; then
  ok "y1: PREMISE — a symlink was planted at this generation's report name"
else
  bad "y1: PREMISE — could not plant a symlink at the report name; the assertions below would be vacuous"
fi
rs "$R32" verdict c --issue 1901
rc_is 5 "y1: a SYMLINKED report is a NON-VERDICT (exit 5)"
has "RESULT: NOT-RUN (report is a symlink)" "y1: and it carries its OWN named cause"
hasnt "RESULT: PASS" "y1: the bait file's PASS is NOT reported — the link was refused, not followed"
hasnt "report unreadable" "y1: NOT reported as unreadable — the operator action is to remove the link, not a chmod"
hasnt "report absent" "y1: and NOT as absent — the file is right there"
rs "$R32" status c --issue 1901
has "state=report-symlink" "y1/status: its own state word, one per operator action"
has "STATUS-NOTE" "y1/status: with a note naming what to do"

# (c) A DANGLING link. `[ -f ]` answers FALSE here, i.e. `no-such-file` -> the PERMISSIVE `absent`
#     state, so this case is what proves the test must be `-L` and must come FIRST.
rm -f "$R32_REP"
if ln -s "$T/y1-does-not-exist-$$" "$R32_REP" 2>/dev/null && [ -L "$R32_REP" ] && [ ! -f "$R32_REP" ]; then
  ok "y1: PREMISE — a DANGLING link is planted, and \`-f\` really answers false for it"
else
  bad "y1: PREMISE — could not plant a dangling link that \`-f\` calls absent; the next assertion is vacuous"
fi
rs "$R32" verdict c --issue 1901
has "RESULT: NOT-RUN (report is a symlink)" "y1: a DANGLING link is refused as a symlink, NOT excused as 'report absent' (the permissive state)"

# (d) CONTROL: remove the link, write a regular file — the stage reads clean again, so the refusal
#     is about the link and not about this stage.
rm -f "$R32_REP"
printf 'result: FINDINGS\n\none blocking gap.\n' >"$R32_REP"
rs "$R32" verdict c --issue 1901
rc_is 4 "y1/control: with the link gone a regular-file FINDINGS report reads normally (exit 4)"

# (e) THE OTHER ARTIFACT THE READ PATH OPENS: the STAGE RECORD.
R32B="$(newrepo)"
rs "$R32B" open c --issue 1902 --agent spec-auditor
rc_is 0 "y1/record: the stage opened"
printf 'result: PASS\n\nreviewed.\n' >"$(REPORT_OF "$R32B" 1902 c)"
R32B_SFILE="$R32B/.review-stage/issue-1902/c.stage"
R32B_FOREIGN="$T/y1-foreign-$$.stage"
cp "$R32B_SFILE" "$R32B_FOREIGN"
rm -f "$R32B_SFILE"
if ln -s "$R32B_FOREIGN" "$R32B_SFILE" 2>/dev/null && [ -L "$R32B_SFILE" ]; then
  ok "y1/record: PREMISE — the stage record is replaced by a link to a FOREIGN record naming the same generation"
else
  bad "y1/record: PREMISE — could not plant the link; the assertions below would be vacuous"
fi
rs "$R32B" verdict c --issue 1902
rc_is 5 "y1/record: a SYMLINKED stage record is a NON-VERDICT (exit 5)"
has "RESULT: NOT-RUN (stage record is a symlink" "y1/record: and it carries its OWN named cause"
hasnt "RESULT: PASS" "y1/record: the report the foreign record named is NOT reported"
hasnt "stage record unreadable" "y1/record: NOT folded onto stage-record-unreadable — the file reads perfectly, which is the hazard"
has "report=unresolved" "y1/record: and no report path is published, because none was identified"
rs "$R32B" status c --issue 1902
has "state=stage-record-symlink" "y1/record/status: its own state word"

# (f) AND THE WRITE COMMAND REFUSES ON IT BY NAME, rather than through the record-unreadable
#     rationale, because the operator action differs.
rs "$R32B" record-author-performed c --issue 1902 \
  --reason "no independent auditor was available on this lane" \
  --evidence "docs/round-artifacts/y1-note.md" --performed-by author
rc_is 2 "y1/record: record-author-performed REFUSES over a symlinked record"
has "reason=stage-record-is-a-symlink" "y1/record: with its own reason token"
if [ "$(LC_ALL=C ls -1 "$R32B/.review-stage/issue-1902" | LC_ALL=C grep -c '\.md$' || true)" = "1" ]; then
  ok "y1/record: and NOTHING was written — the stage directory still holds exactly one report"
else
  bad "y1/record: the refusal wrote a report anyway ($(LC_ALL=C ls -1 "$R32B/.review-stage/issue-1902" | LC_ALL=C tr '\n' ' '))"
fi

# (g) STRUCTURAL — THE CENSUS OF READ TARGETS. A behavioural case can only cover the readers
#     someone thought of; the property is that EVERY function which opens a FILE through the one
#     capture boundary carries the leaf test. Derived from the shipped script, so a THIRD reader
#     added later joins the census instead of needing this list edited.
# COUNTED OVER *CODE*, NEVER OVER PROSE — round 18's X1 lesson inside this round's own guard, and
# it fired while this was being written: the comment that EXPLAINS the check quotes `[ -L "$p" ]`
# verbatim, so a whole-body match reported `report_bytes` as carrying the test with the code hunk
# REMOVED. A guard that a comment can satisfy measures the documentation, not the mechanism. The
# `#3929` declaration assert below deliberately reads the COMMENTS, which is why it is separate.
Y1_CENSUS="$(LC_ALL=C awk '
  /^[A-Za-z_][A-Za-z0-9_]*\(\) \{/ { fn = $1; sub(/\(\).*/, "", fn); body = ""; inf = 1; next }
  inf && /^\}/ {
    if (body ~ /capture_map_nul "\$/) {
      printf "%s %s\n", fn, (body ~ /\[ -L "\$/ ? "HAS-L" : "NO-L")
    }
    inf = 0; next
  }
  inf && /^[ \t]*#/ { next }
  inf { body = body "\n" $0 }
' "$RS")"
Y1_NREAD="$(printf '%s\n' "$Y1_CENSUS" | LC_ALL=C grep -c . || true)"
Y1_NOL="$(printf '%s\n' "$Y1_CENSUS" | LC_ALL=C grep -c 'NO-L' || true)"
if [ "${Y1_NREAD:-0}" -eq 2 ]; then
  ok "y1/structural: the census finds EXACTLY 2 functions that open a file through the capture boundary (report_bytes, stage_record_text) — a third would show up here"
else
  bad "y1/structural: the census finds ${Y1_NREAD:-0} file-reading function(s), not 2 — either the extraction broke or a new reader appeared: $(printf '%s' "$Y1_CENSUS" | LC_ALL=C tr '\n' ' ')"
fi
if [ "${Y1_NOL:-0}" -eq 0 ]; then
  ok "y1/structural: and 0 of them lack the leaf \`[ -L ]\` test, so no read target follows a link unchecked"
else
  bad "y1/structural: ${Y1_NOL:-0} file-reading function(s) lack the leaf \`[ -L ]\` test: $(printf '%s' "$Y1_CENSUS" | LC_ALL=C grep 'NO-L' | LC_ALL=C tr '\n' ' ')"
fi
# THE ORDER IS THE PROPERTY: a `-L` test placed after a dereferencing predicate is not a check.
Y1_RB="$(LC_ALL=C awk '/^report_bytes\(\) \{/ { inf = 1 } inf { print } inf && /^\}/ { exit }' "$RS")"
# THE SAME COMMENT-STRIPPED VIEW for the ORDER asserts. Read over the whole body, the first match
# for either pattern is the COMMENT that documents it, so the comparison would order two comments.
Y1_RB_CODE="$(printf '%s\n' "$Y1_RB" | LC_ALL=C grep -v '^[[:space:]]*#')"
Y1_L_LN="$(printf '%s\n' "$Y1_RB_CODE" | LC_ALL=C grep -n '\[ -L "\$p" \]' | LC_ALL=C head -1 | cut -d: -f1)"
Y1_F_LN="$(printf '%s\n' "$Y1_RB_CODE" | LC_ALL=C grep -n '\[ ! -f "\$p" \]' | LC_ALL=C head -1 | cut -d: -f1)"
if [ -n "$Y1_L_LN" ] && [ -n "$Y1_F_LN" ] && [ "$Y1_L_LN" -lt "$Y1_F_LN" ]; then
  ok "y1/structural: report_bytes asks \`-L\` BEFORE the dereferencing \`-f\` (lines $Y1_L_LN < $Y1_F_LN)"
else
  bad "y1/structural: report_bytes does not ask -L before -f (L=$Y1_L_LN f=$Y1_F_LN) — a dangling link would take the permissive absent branch"
fi
Y1_SRT="$(LC_ALL=C awk '/^stage_record_text\(\) \{/ { inf = 1 } inf { print } inf && /^\}/ { exit }' "$RS")"
Y1_SRT_CODE="$(printf '%s\n' "$Y1_SRT" | LC_ALL=C grep -v '^[[:space:]]*#')"
Y1_SL_LN="$(printf '%s\n' "$Y1_SRT_CODE" | LC_ALL=C grep -n '\[ -L "\$file" \]' | LC_ALL=C head -1 | cut -d: -f1)"
Y1_SC_LN="$(printf '%s\n' "$Y1_SRT_CODE" | LC_ALL=C grep -n 'capture_map_nul "\$file"' | LC_ALL=C head -1 | cut -d: -f1)"
if [ -n "$Y1_SL_LN" ] && [ -n "$Y1_SC_LN" ] && [ "$Y1_SL_LN" -lt "$Y1_SC_LN" ]; then
  ok "y1/structural: stage_record_text asks \`-L\` BEFORE the redirection that dereferences (lines $Y1_SL_LN < $Y1_SC_LN)"
else
  bad "y1/structural: stage_record_text does not ask -L before its read (L=$Y1_SL_LN read=$Y1_SC_LN)"
fi
# AND THE RESIDUAL IS DECLARED AT BOTH SITES, not left for a reader to infer that the race is
# closed. #3929 is the accepted boundary; a fix that silently claimed more than it delivers is the
# false-assurance shape this whole issue is about.
Y1_DECL=0
case "$Y1_RB" in *'#3929'*) Y1_DECL=$((Y1_DECL + 1)) ;; esac
case "$Y1_SRT" in *'#3929'*) Y1_DECL=$((Y1_DECL + 1)) ;; esac
if [ "$Y1_DECL" -eq 2 ]; then
  ok "y1/structural: BOTH read sites DECLARE the TOCTOU residual (#3929) beside the check, so the fix does not read as closing the race"
else
  bad "y1/structural: only $Y1_DECL of 2 read sites declare the #3929 residual — a leaf test before an open cannot close that window and must not imply it does"
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
#
# ROUND 9's N3 MOVES IT TO 605. Section 18 gains 4: the COMPOUND-STATEMENT positive control. The
# guard's scope was anchored at the start of a line, so every compound statement was invisible to
# it and it reported this script CLEAN with two real bypasses in it (`$extra` behind a
# `[ -z … ] ||`, `$token` in a one-line `case` arm). The control REPRODUCES one of them in a
# throwaway copy and requires the guard to red AND to NAME the planted symbol — plus an assertion
# that the planted statement really does NOT begin its line, without which the case would be a
# duplicate of the line-start control beside it. The `[ ! -f "$EBS" ]` fallback arm was widened to
# ten bads to match, so the EXACT floor still holds either way.
#
# ROUND 9's N4 MOVES IT TO 628. Section 22 adds 23: an audit counter at its ceiling must not
# restart. `reopen-count`'s `$(( prior + 1 ))` walked off round 8's ten-digit bound, so the NEXT
# re-open read an eleven-digit value as incomparable and silently RESTARTED THE COUNT AT 1
# (measured: the record held `10000000000`, then `1`). The disposition is SATURATION with the hold
# NAMED — refusal would contradict round 8's own ruling for this field and would block a spawn over
# a cosmetic number — so the cases pin the boundary from both sides (AT the ceiling it is held and
# noted; ONE BELOW it still increments and claims no hold), that `status` reports the counter at all
# and agrees with `open` about it, that the at-least `+` appears ONLY at the ceiling, that an
# incomparable counter is displayed VERBATIM and unmarked, and STRUCTURALLY that the digit width is
# DERIVED from the ceiling value so the acceptance and saturation boundaries cannot drift apart.
# Every assertion is unconditional.
#
# ROUND 12's R1 MOVES IT TO 663. Section 23 adds 35: the report nonce must be RESERVED, not merely
# GENERATED. Round 6 replaced the scanned generation with a random nonce and deleted the existence
# belt with the scan; `mktemp -u` creates nothing, so a nonce repeating a report already on disk let
# `open` write over that report and REPUBLISH its path, handing the superseded agent that still
# holds it the ability to write the CURRENT verdict. The collision is FORCED, not waited for — the
# generator is driven from a feed file in a SCRATCH COPY of the shipped script (the ARTIFACT is
# substituted; no settable seam), so both branches are deterministic and cannot flake: every
# attempt colliding must REFUSE BY NAME with the historical report byte-intact and no record
# published, and ONE collision must retry to a FRESH nonce rather than red on recoverable input.
# Plus three CONTROLS (the same scratch machinery with an empty feed still opens AND re-opens to a
# different path; a superseded agent's held path is still dead through the SHIPPED script), six
# STRUCTURAL pins (the claim is an `O_EXCL` create, it makes no existence TEST — which is what
# distinguishes it from round 6's deleted scan — the retry is bounded by ONE top-level literal, the
# claim precedes the write by line number, and the source states the distinction) and five
# LIFETIME assertions (an owned resource inherits its owner's lifetime bugs, #3544 round 17): the
# name is registered inside the reservation, reaped by the SAME single `trap`, de-registered on
# fulfilment, and a reserve-then-refuse open leaves the stage directory EMPTY. Every assertion is
# unconditional — each `if`/`case` calls exactly one of `ok`/`bad` — so the EXACT floor holds.
#
# ROUND 12's R2 MOVES IT TO 690. Section 24 adds 27: a verdict must describe a state that existed.
# `classify_report` read its subject EIGHT times (existence, a readability probe, the body for
# emptiness, the `result:` census, the disclosure, then `performed-by`, `reason` and `evidence` each
# through their own `read_field`), so a report replaced between two of those reads let it assemble
# `AUTHOR-PERFORMED` out of fields drawn from DIFFERENT, INDIVIDUALLY INVALID versions — working no
# single snapshot ever held. The interleave is SIMULATED, not raced: one line injected into a
# SCRATCH COPY swaps the file at a NAMED field read, and the anchor is deliberately the ONE line
# present in BOTH the pre-fix and post-fix scripts (the field-grammar entry `key="$2"`, which was
# `read_field` re-reading the FILE and is now `read_field_from` reading the caller's SNAPSHOT), so
# the same plant lands either way. Three PREMISE assertions measure that each version alone does
# NOT reach the token (a refusal from a broken fixture would prove nothing), two interleave cases
# cover working assembled across versions and a token validated by another document's working,
# three CONTROLS (the scratch machinery with no swap, and the SHIPPED script, still reach
# AUTHOR-PERFORMED), six CAUSE assertions re-pin every consolidated read (absent / empty /
# no record / two records / a real PASS), and seven STRUCTURAL pins: the report path is named
# EXACTLY ONCE in the classifier, that use is the shared `report_bytes` helper, no field is re-read
# from the file, `read_field` delegates to the ONE field grammar, and the downstream consumer
# (`record-author-performed`) shares the snapshot at both its call sites. Every assertion is
# unconditional.
#
# ROUND 13's S1 MOVES IT TO 713. Section 25 adds 23: an unreadable prior verdict is UNKNOWN, not
# REPLACEABLE. Round 12's R2 gave the classifier an UNREADABLE observation state, and
# `record-author-performed`'s clobber guard branched on the TOKEN — where that state arrives as
# `NOT-RUN`, the REPLACEABLE side — so a report whose recorded verdict was unknown, possibly a
# blocking `FINDINGS`, was overwritten by the merge-proceeding `AUTHOR-PERFORMED` with no `--force`
# and no `replaced-verdict:` trace. Two PREMISE assertions read the blocking verdict before it is
# made unreadable (a refusal from a broken fixture proves nothing), ELEVEN host-branched ones cover
# the refusal by name, `--force` NOT covering it and the report surviving both attempts — with the
# no-subject branch (a host that reads a mode-000 file) executing the same ELEVEN, asserting that
# the recorded-verdict guard covers the file there, so the EXACT floor is host-independent — six
# CONTROLS keep a verified-ABSENT report freely replaceable, and four STRUCTURAL pins require the
# single state reader `report_state`, both callers going through it, and the guard's permissive set
# being an AFFIRMATIVE `absent | present` match rather than a `!= unreadable` test.
#
# ROUND 13's S2 MOVES IT TO 747. Section 26 adds 34: a capture that normalises its input cannot be
# the thing that validates it. Bash DISCARDS NUL bytes in a command substitution, so the capture did
# not merely lose information, it MANUFACTURED grammar — a report whose bytes are `res\0ult: PASS`
# holds no column-zero `result:` line and `verdict` reported `RESULT: PASS` at exit 0 — and the same
# idiom in `read_field` forged a VALID nonce out of `STALE\0PASS1`, redirecting the reader to a STALE
# report's `PASS`. Both routes are asserted with the premise MEASURED ON THE FILE (grep says the
# record is not there), a control proving the same bytes without the NUL are a genuine PASS, S1's
# write-side refusal on a subject EVERY host has (unlike a mode-000 file), the `open --force`
# recovery, seven STRUCTURAL pins (one mapping implementation, ONE literal with the byte DERIVED
# from it, both captures routed, zero raw file captures left, and the two-signal completeness
# assertion), and a BEHAVIOURAL cross-locale invariance case — a source scan for unpinned text tools
# was written first and discarded for firing on four indented comments, a heredoc opener and the
# `--help` renderer. Its wall-clock `elapsed=` field is neutralised before comparison, which is not
# cosmetic: unneutralised it flaked 1 run in 5.
#
# ROUND 14's T2 MOVES IT TO 775. Section 27 adds 28: the output primitive must be a LITERAL
# PRINTER. `emit`, `note` and `die_usage` used `echo`, which under the bash option `xpg_echo` — set
# by an INHERITED environment (`BASHOPTS`/`SHELLOPTS`, a `BASH_ENV` file), never by this script —
# performs BACKSLASH ESCAPE PROCESSING on its argument and so makes that argument a FORMAT. Measured
# on the shipped script from a LEGAL directory name alone: the one-line verdict became TWO lines
# whose second was a column-zero `REVIEW-STAGE: … RESULT: PASS`, with REAL `key=` pairs on it from
# octal `\075`, defeating section 11c's `=`→`~` neutralisation outright. The subject is the SHIPPED
# SOURCE, extracted by text and evaluated with `xpg_echo` forced ON, so reverting one definition
# REDS the section (measured: 18 failures) — and a RED CONTROL reconstructs the pre-fix `echo`
# spelling inline, so the differential is proved able to see the defect. Host-branched on a MEASURED
# premise with both arms the same length, ten assertions over the extended
# `emit-boundary-scan.sh` (which now refuses `echo` outright and requires every `printf` FORMAT to
# be a script-authored literal) with a COMPOUND positive control and a data-derived-format control,
# and three source pins as a belt.
#
# ROUND 14's T1 MOVES IT TO 813. Section 28 adds 38: a FAITHFUL READER IS NOT A FAITHFUL ANSWER.
# Round 13's S2 gave this script one faithful-read boundary and left `count_field_lines` reading the
# stage record with `grep -c` on the FILE — and `grep` is faithful; the ANSWER is not. A record whose
# key is spelt `report-<NUL>nonce:` holds NO `report-nonce:` line, so the count was a TRUTHFUL `0`,
# which is exactly the value meaning "a pre-nonce record whose single report is the LEGACY bare
# name" — so a stale `c.md` recording `result: PASS` was reported as this stage's verdict at exit 0
# while the CURRENT report held the sentinel (measured; 19 failures with the hunk reverted, 0 after).
# THREE PREMISE assertions measure the fixture on the FILES (the record really lacks the line, really
# holds the byte, and the stale bait really records a PASS), ten cover the verdict/status surfaces and
# the byte being NAMED with its own next action, five the WRITE side refusing under its own reason
# token `stage-record-unrepresentable` with the artifact intact, five CONTROLS keep both legitimate
# neighbours working (the same record without the byte reads its CURRENT report; a genuine pre-nonce
# record still reads the LEGACY one — the branch the byte impersonated), six STRUCTURAL pins (the read
# is routed with the two-signal completeness assertion, no direct grep of the record file remains, the
# unrepresentable case is its OWN status 2, and BOTH callers capture the status with `|| cfl_rc=$?`
# rather than an `if ! …` that can only see zero-vs-nonzero), and nine over the new caller-side guard
# `scripts/tests/lib/read-boundary-scan.sh` — round 13's asserts check the mapping appears exactly
# ONCE, a property of the BOUNDARY and not of its CALLERS, which is why neither round-14 site was
# visible to them. Its controls plant the EXACT pre-fix shape (a `grep -c` inside a `$( … )` behind an
# `LC_ALL=C` prefix, asserted to really be that shape), a DIFFERENT reading command, and a REWORDED
# declared read, requiring the guard to red AND to name the command / the STALE entry. That is not a
# formality: written without an assignment-prefix stripper the scanner reported CLEAN on the real
# defect and on a planted `cat "$file"`, because every text call here is spelled `LC_ALL=C grep …`.
#
# ROUND 15 ADDED 20 HOST-INDEPENDENT ASSERTIONS (813 -> 833), all in section 21 and section 9b, for
# U1 — the overwrite made UNEXPRESSIBLE rather than narrowed. Section 21 gains case (g), which
# drives the interleaving at the LAST instant before publication (inside the span round 9 declared
# irreducible, AFTER the re-observation — the case round 9 said could not be written, writable now
# because the assertion is that the late verdict SURVIVES rather than that a clobber happens), its
# RED-CONTROL reconstructing the pre-U1 write destination and requiring the late verdict to be GONE
# FROM DISK there, four structural pins (the re-observation sits between the substitute's commit and
# the publication; NO write in the script targets `$STAGE_REPORT`; BOTH report writers claim their
# path through `reserve_report_path`; the remaining window is declared with its ACTUAL consequence),
# and a doctrine sweep — with its own positive control — that round 9's WITHDRAWN residual
# declaration survives at none of the five sites that carried it. Section 9b gains the property
# itself: the superseded generation still records the FINDINGS, prose included. Every one needs only
# bash, git and coreutils; none branches on the host.
#
# ROUND 16 ADDED 23 HOST-INDEPENDENT ASSERTIONS (833 -> 856), all in the new section 29, for V2 —
# a legal `=` in the repository path made `report=` publish a path that DOES NOT EXIST. `report=`
# went through `field_value`, whose `=`->`~` map exists so a value cannot forge a `key=value` pair;
# a repository root may legally contain `=`, so on such a checkout the verdict line advertised
# `…/eq~path/…/c.<nonce>.md` while `open`'s own raw line had just created `…/eq=path/…` — and
# `verdict`, unlike `open`, offers NO separate raw channel. Measured on the shipped script. The
# fixture is a checkout whose PATH contains `=` (asserted to contain one, or every case is
# vacuous), the premise is `open`'s RAW path existing on disk, and the finding is asserted as TWO
# claims — the published value IS the real path AND names a file that EXISTS — because a value can
# be equal to something without existing and can exist without being the report of record; plus
# that the `=` SURVIVED rather than being dropped, which a deleting scrubber would satisfy by
# accident. The case that matters most is the CONFINEMENT control (b): a `report=` pair smuggled
# through the `agent=` field must still be neutralised, since unmapped it puts a REAL `report=`
# ahead of the measured one and the remainder reader takes the FIRST — so it is COUNTED (exactly
# one pair), the `~` rendering asserted, and the remainder required to still resolve to the real
# path. Without it, (a) is satisfiable by dropping the map from every field. An ordinary `=`-free
# checkout is the other control. Six STRUCTURAL pins: ONE definition and ONE call site for
# `remainder_value` (so the exemption cannot spread unnoticed — the six other `report=` emitters
# keep `field_value`, and one of them, `report-changed-mid-write`, has `now-verdict=` AFTER
# `report=`, where the exemption would be unsound), the verdict line ending with it, and a
# BEHAVIOURAL differential over the extracted functions: `field_value` still maps `=`,
# `remainder_value` does not, and `remainder_value` still renders a C0 byte visibly and still
# flattens a newline — the exemption is the `=` map ALONE. Every one needs only bash, git and
# coreutils; none branches on the host.
#
# ROUND 17 ADDED 30 HOST-INDEPENDENT ASSERTIONS (856 -> 886), all in the new section 30, for W2 —
# a checkout path this one-line grammar cannot carry made the two commands lie DIFFERENTLY about
# the same file: `open` printed the RAW path, so a newline-bearing root SPLIT it across physical
# lines (the second carrying no `REVIEW-STAGE: ` anchor), while `verdict` FLATTENED it and
# published `…/lane two/…`, a path no `open(2)` can resolve, on the line whose whole promise is the
# absolute report-of-record path (measured on the shipped script; 16 failures before the fix, 0
# after). Round 11 declared such a path unrepresentable and "never arriving"; the second half was
# FALSE — git resolves the root of whatever checkout the tool runs in — and that declaration is
# WITHDRAWN, swept with a positive control over four sites. The fixture PREMISE asserts the
# newline really is in the built path (a case that silently ran in an ordinary directory would pass
# for the wrong reason), the refusal is asserted on ALL FOUR entries because the check sits at the
# ONE root resolution, `verdict` is required to publish NO `report=` and NO `RESULT:` at all (a
# wrong path on that line is worse than no line), and the CR and TAB variants pin that the rule is
# the RENDERER's own answer rather than a two-character list — with the tab case requiring the
# NEWLINE rationale to be ABSENT, since a true statement that hides the sharper one is still the
# wrong one. The controls are what stop it redding correct input: a SPACE-bearing checkout (round
# 11's own subject) still publishes its whole path AND that path must EXIST. Six structural pins:
# one resolution site, the refusal BEFORE the global is published, the probe asking `one_line`
# itself, a BEHAVIOURAL agreement between the probe's renderer and the published field's, and no
# opt-out env var. Every one needs only bash, git and coreutils; none branches on the host, and the
# CR/TAB fixtures emit a fixed number of assertions whether or not the filesystem can hold them.
#
# ROUND 17's W1 MOVES IT TO 937. Section 31 adds 51: THE RECORD AND THE REPORT MUST BE *ONE*
# OBSERVATION. `record-author-performed` read the REPORT using the generation loaded earlier and
# then read the STAGE RECORD independently, so an `open --force` publishing generation B between
# those reads left BOTH final re-verifications satisfied — an unchanged report A, an unchanged
# record B — and the recording published `AUTHOR-PERFORMED` over B without ever inspecting B's
# verdict, without `--force`, and with a trace claiming A was superseded. Measured on the shipped
# script (17 failures before the fix, 0 after): `RECORD-OK … supersedes-report-nonce=<A>` at exit 0
# while B held `result: FINDINGS`, and `verdict` then reported AUTHOR-PERFORMED. THREE PREMISE
# assertions measure the fixture on disk — generation A named by the record, a DIFFERENT generation
# B published by the plant (located by the reviewer's own marker, because its nonce is unknowable to
# the test and reading the RECORD for it would read the defect instead of the premise), and a real
# blocking verdict in B — then the refusal, the ABSENCE of a trace naming A, the record still naming
# B, and `verdict` still reporting FINDINGS. Two further interleaves land INSIDE the observation
# (between the record capture and the report read), where the primitive's own re-verification is what
# refuses, and the read side asserts the cause is `stage record changed mid-read` with its OWN
# `state=`, never `stage-record-unreadable` — a perfectly readable record reported as unreadable is a
# false rationale, and the operator action differs (read it again). THREE CONTROLS keep the
# legitimate paths open: an undisturbed `--force` supersession still records AND names EXACTLY the
# generation it inspected (in the RECORD-OK line and in the published report's own trace), and an
# ordinary recording over a sentinel still succeeds while claiming no replacement. TWELVE assertions
# cover the new `scripts/tests/lib/observation-boundary-scan.sh`, whose six controls plant an
# independent second read in the decision path that HAD the defect (requiring the guard to red AND
# to name the reader, the function and the LINE), a different reader in a different path, a decision
# path that observes zero times, one that observes twice, a reworded declared re-verification
# (STALE), and an undeclared subject (REFUSED, never reported clean). Six structural pins over the
# primitive itself, which the scanner deliberately does not check: one record read, display fields
# from that capture, the re-verification AFTER the report read (before it, it would compare the
# record with itself), a moved record DISCARDING the report observation, and the defect published as
# a CLOSED KIND beside its sentence rather than matched as prose. Every one needs only bash, git and
# coreutils; none branches on the host.
#
# ROUND 18's X1 MOVES IT TO 948. Section 30 adds 11: a CAPTURED PATH IS NOT THE PATH.
# `require_repo_root` captured the root with `root="$(git rev-parse --show-toplevel)"`, and a
# command substitution strips EVERY trailing newline — so a checkout whose DIRECTORY NAME ends in
# an LF resolved to a DIFFERENT, EXISTING SIBLING, and the captured value then held no newline for
# round 17's representability refusal to see. Measured on the shipped script: `verdict` reported
# `RESULT: PASS … report=…/lanetrail/.review-stage/issue-704/c.<nonce>.md` at exit 0 off a report
# THIS LANE NEVER OPENED, and the refused `open` created a directory INSIDE the peer lane — the
# #3616 peer-artifact class reached through a lossy capture rather than a recency scan (6 failures
# with the hunk reverted, 0 after). Round 17's own LF case could not see it twice over: its
# fixture is `lane<LF>two`, where the newline is EMBEDDED and survives `$( )`, and `w2_repo`
# ITSELF returned the fixture path through a command substitution, so the trailing-LF shape could
# not be presented at all — the harness-that-never-reached-the-code class inside the guard for a
# lossy capture. The helper now ASSIGNS through `printf -v`, which is what makes the case
# constructible, and the four existing call sites were converted with it (RE-VERIFIED against the
# PRE-round-17 script, where 16 of round 17's own assertions red, so they measure the refusal and
# not the helper). FOUR PREMISE assertions measure the fixture on the path — the trailing LF is
# really the last byte, the sibling really is that path minus that byte, and the sibling's stage is
# VALID BAIT because read from the sibling ITSELF it reports `RESULT: PASS` — then the refusal on
# `open` (a DIFFERENT issue number, so a write landing in either root is a directory that did not
# exist), on `verdict` with NO `report=` and NO `RESULT:` at all, and the peer's report BYTE-
# UNCHANGED afterwards. Round 13 (S2) enumerated trailing-newline stripping and declared it
# harmless: correct about REPORT CONTENT, where every grammar is per-line and column-zero
# anchored, and false about a PATH, whose stripped bytes are part of its identity — so the durable
# rule is that a lossy-capture conclusion must be RE-DERIVED PER CONSUMER, never carried. The
# structural pin that counts resolution sites is also corrected to count over CODE rather than
# prose: a whole-file `grep -c` counted the idiom in the comment that RECORDS the retired capture,
# so writing down the fix reported a second resolution site. Every one needs only bash, git and
# coreutils; none branches on the host.
#
# ROUND 19's Y1 MOVES IT TO 980. Section 32 adds 32: the READ path followed a symlink. Round 1's
# F5 walk refuses one where this tool WRITES and NOTHING refused one where it READS, so replacing
# a generation's report with a link to any regular file holding `result: PASS` made `verdict` — and
# `premerge-assert.sh`'s AUTO C validation with it — accept a verdict from an artifact that is not
# the report of record (measured on the shipped script: `RESULT: PASS`, exit 0, off a link into the
# scratch dir; 15 of this section's assertions RED with the two code hunks removed, 0 after). Both
# artifacts the read path opens are covered — the report AND the stage record, the latter because
# it names WHICH generation is authoritative and carries the `head-sha:` the stage was opened at.
# The DANGLING case is the one that proves `-f` cannot answer the question: for a dangling link it
# is FALSE, i.e. `no-such-file`, i.e. the PERMISSIVE `absent` state the clobber guard reads as "no
# recorded verdict to destroy". The structural half is a DERIVED CENSUS of read targets (every
# function opening a file through `capture_map_nul` must carry the leaf test), so a third reader
# added later joins the census instead of needing a curated list — and it counts over CODE, not
# prose, which is round 18's X1 lesson INSIDE this round's own guard: the comment explaining the
# check quotes `[ -L "$p" ]` verbatim, so a whole-body match reported the test as present with the
# code hunk removed. The residual (a leaf `-L` before an open leaves a TOCTOU window bash cannot
# close — #3929's family) is asserted as DECLARED at both sites rather than claimed closed. Every
# assertion needs only bash, git, `ln -s` and coreutils; none branches on the host, and the two
# PREMISE assertions that could fail on a filesystem without symlinks call `bad` (a red run, never
# a displaced count).
ASSERT_FLOOR=980
EXECUTED=$((PASS + FAIL))
if [ "$EXECUTED" -lt "$ASSERT_FLOOR" ]; then
  bad "CASE FLOOR: only $EXECUTED assertions executed, below the committed floor of $ASSERT_FLOOR — a section died silently, and 'failed: 0' over a shrunken suite is not a pass"
fi

printf '\n=== review-stage: %d passed, %d failed (executed %d, floor %d) ===\n' \
  "$PASS" "$FAIL" "$((PASS + FAIL))" "$ASSERT_FLOOR"
[ "$FAIL" -eq 0 ]
