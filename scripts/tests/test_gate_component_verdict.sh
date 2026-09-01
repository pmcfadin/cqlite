#!/usr/bin/env bash
# test_gate_component_verdict.sh — non-vacuity proof for the #3750 split of COMPLETION
# from VERDICT: scripts/gate-component-verdict.sh (the verdict reader) and the two
# DOCUMENTED text-completion grammars it sits beside.
#
# THE DEFECT THIS CLOSES
# ----------------------
# `--only <component>` demotes a successful run to `RESULT: PARTIAL` (agent-gate.sh, on
# purpose: a component probe must never be pastable as the gate of record). CLAUDE.md's
# mandated #3041 completion probe matched only `PASS|FAIL`, so an `--only` run that
# SUCCEEDED was never detected as terminal — the probe terminated on failure and spun
# forever on success. A lane spun 8+ minutes past a terminal PASS and then re-ran an
# 18-minute component that had already passed.
#
# The lead's ruling on the FIRST fix for that is what this suite is really pinning:
# widening the completion grammar to accept `PARTIAL` fixes the hang and introduces a
# worse bug if anything then reads success out of it. `PARTIAL` says THE RUN ENDED, not
# MY COMPONENT PASSED. So:
#
#   * COMPLETION and VERDICT are two assertions, and no probe may derive the second
#     from the first (asserted here, in both directions);
#   * the VERDICT is read from the component's OWN line, so a completed run whose
#     component SKIPped or is ABSENT is NOT a pass — a SKIP means the check never ran,
#     which is the vacuous pass itself;
#   * the gate-of-record grammar must keep REFUSING `PARTIAL`, and the two grammars must
#     be textually distinguishable.
#
# WHAT MAKES THIS SUITE NON-VACUOUS. Every verdict has its own green AND its own red, and
# the three dangerous confusions get dedicated cases: a SKIPped component (the original
# vacuous pass), a status token that is a PREFIX of PASS (`PASSENGER` — the mistake this
# repo has now made twice), and a PASS component line inside a NON-TERMINAL block (which
# would make the verdict readable before the run ended).
#
# Hermetic: temp dirs only. No cargo, no datasets, no network, no gh, no nested gate.
set -uo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
VERDICT="$REPO_ROOT/scripts/gate-component-verdict.sh"
READER="$REPO_ROOT/scripts/gate-liveness.sh"

TMP=$(mktemp -d "${TMPDIR:-/tmp}/gate-component-verdict-test.XXXXXX")
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

pass=0; fail=0
# Incremented in the TOP-LEVEL shell only. Never wrap a case in `( … )` — a subshell's
# increments are discarded and the suite reports failed:0 while printing FAILs (a real
# incident in this repo's tooling tests).
ok()  { pass=$((pass+1)); printf 'ok   %s\n' "$1"; }
bad() { fail=$((fail+1)); printf 'FAIL %s\n' "$1"; [ $# -ge 2 ] && printf '     %s\n' "$2"; }

# ---------------------------------------------------------------------------
# Fixtures. Written directly with the content they mean — no in-place `sed`, which
# needs a suffix argument on BSD and rejects the GNU form (this suite runs in the full
# gate's `tooling-tests`, and macOS is a first-class gate host here).
#
# The component line's shape is agent-gate.sh's `_fm_summary_line`:
#   printf '%-18s %s (%s)  %s' "<name>:" "<STATUS>" "<secs>s" "<annotation>"
# reproduced here rather than described, so a fixture cannot drift into a shape the
# gate never emits.
# ---------------------------------------------------------------------------
comp_line() { printf '%-18s %s (%s)  %s\n' "$1:" "$2" "$3" "${4:-[no-cargo]}"; }

# mk_summary <path> <run-id> <RESULT-value> [component-line...] — a FULL-marker block.
# `--only` uses the FULL markers (only `--lite`/`--delta` swap them), so this one helper
# builds both the `--only` and the full-gate fixtures; the RESULT value is what differs.
mk_summary() {
  local path="$1" rid="$2" result="$3"; shift 3
  { echo "==== AGENT-GATE SUMMARY ===="
    echo "run-id: $rid"
    echo "commit: abc1234 branch: issue-3750 dirty: no"
    echo "tree-integrity: PASS"
    local l; for l in "$@"; do printf '%s\n' "$l"; done
    [ -n "$result" ] && echo "RESULT: $result"
    echo "==== END AGENT-GATE SUMMARY ===="
  } > "$path"
}
# mk_only_summary: the same, plus the LOWERCASE `mode: PARTIAL (--only …)` line the gate
# really emits for --only (verified against agent-gate.sh's emit path).
mk_only_summary() {
  local path="$1" rid="$2" result="$3" only="$4"; shift 4
  { echo "==== AGENT-GATE SUMMARY ===="
    echo "run-id: $rid"
    echo "commit: abc1234 branch: issue-3750 dirty: no"
    echo "tree-integrity: PASS"
    echo "mode: PARTIAL (--only $only) - does NOT count as the gate"
    local l; for l in "$@"; do printf '%s\n' "$l"; done
    [ -n "$result" ] && echo "RESULT: $result"
    echo "==== END AGENT-GATE SUMMARY ===="
  } > "$path"
}

# ---------------------------------------------------------------------------
# expect <label> <want-verdict> <want-rc> <needle> -- <verdict-script args...>
#
# THREE INVARIANTS ARE ASSERTED FOR EVERY CASE, not case by case, so a case added later
# inherits them without anyone remembering to ask:
#
#  (1) a verdict may never carry an EMPTY cause;
#  (2) the output must NEVER contain a `RESULT: <TOKEN>` form. This tool's output gets
#      pasted and grepped, and a line reading `run RESULT: PASS` would MATCH the
#      documented gate-of-record completion probe — the artifact becoming the credential
#      (CLAUDE.md #3312). Run context is rendered `run-result=<TOKEN>` for that reason;
#  (3) the output must never contain an `==== AGENT-GATE` marker, for the same reason.
# ---------------------------------------------------------------------------
expect() {
  local label="$1" want="$2" wantrc="$3" needle="$4"; shift 5
  local out rc
  out=$(bash "$VERDICT" "$@" 2>&1); rc=$?
  if printf '%s' "$out" | grep -qE '^gate-verdict: [A-Z-]+ [^(]*\([[:space:]]*\)[[:space:]]*$'; then
    bad "$label" "verdict carried an EMPTY cause: $(printf '%s' "$out" | head -1)"; return
  fi
  if printf '%s' "$out" | grep -qE 'RESULT:[[:space:]]*[A-Z]'; then
    bad "$label" "output carries a RESULT: token (pastable as a gate verdict): $(printf '%s' "$out" | head -3)"; return
  fi
  if printf '%s' "$out" | grep -qF '==== AGENT-GATE'; then
    bad "$label" "output carries an AGENT-GATE block marker: $(printf '%s' "$out" | head -3)"; return
  fi
  if ! printf '%s' "$out" | grep -q "^gate-verdict: $want\b"; then
    bad "$label" "expected verdict $want, got: $(printf '%s' "$out" | head -1)"; return
  fi
  if [ "$rc" != "$wantrc" ]; then
    bad "$label" "expected exit $wantrc, got $rc (output: $(printf '%s' "$out" | head -1))"; return
  fi
  if [ -n "$needle" ] && ! printf '%s' "$out" | grep -q "$needle"; then
    bad "$label" "expected cause to mention '$needle', got: $(printf '%s' "$out" | head -2)"; return
  fi
  ok "$label"
}

echo "=== section 1: AC5 — a COMPLETED --only whose component SKIPped is NOT a pass ==="
# THE CASE THE LEAD ASKED FOR BY NAME. This fixture is exactly what the gate emits for
# `--only python-bindings` on a box with no python3: the component SKIPs, nothing FAILs,
# so OVERALL stays PASS, is demoted to PARTIAL, and the run exits 3. Read the terminal
# token and you have a "successful" run. Read the component's own line and the check
# never ran.
S1="$TMP/only-skip.txt"
mk_only_summary "$S1" run-1 PARTIAL python-bindings "$(comp_line python-bindings SKIP 0s '[indirect:maturin]')"
expect "1.1 --only + component SKIP => NOT-PASS (the vacuous pass itself)" \
  NOT-PASS 1 "SKIP" -- "$S1" --mode only --component python-bindings --run-id run-1

S2="$TMP/only-pass.txt"
mk_only_summary "$S2" run-1 PARTIAL tooling-tests "$(comp_line tooling-tests PASS 1112s '[unobservable:nested]')"
expect "1.2 control: --only + component PASS => PASS (the reader is not refuse-everything)" \
  PASS 0 "tooling-tests" -- "$S2" --mode only --component tooling-tests --run-id run-1

S3="$TMP/only-fail.txt"
mk_only_summary "$S3" run-1 FAIL fmt "$(comp_line fmt FAIL 3s)"
expect "1.3 --only + component FAIL => NOT-PASS" \
  NOT-PASS 1 "FAIL" -- "$S3" --mode only --component fmt --run-id run-1

S4="$TMP/only-absent.txt"
mk_only_summary "$S4" run-1 PARTIAL file-size "$(comp_line file-size PASS 0s)"
expect "1.4 component ABSENT from a COMPLETE block => NOT-PASS, never permissive" \
  NOT-PASS 1 "absent" -- "$S4" --mode only --component tooling-tests --run-id run-1

echo "=== section 2: the status token is matched EXACTLY, never by prefix ==="
# `PASS*` accepts `PASSthisNeverRan` — this repo has made that mistake twice (the roborev
# wrapper's verdict scan, then gate-liveness.sh's RESULT token). A closed grammar that
# admits a prefix checks a SPELLING rather than a STATE.
S5="$TMP/prefix.txt"
mk_only_summary "$S5" run-1 PARTIAL core-tests "$(comp_line core-tests PASSENGER 9s)"
expect "2.1 a status token that merely STARTS WITH pass is not a PASS" \
  COULD-NOT-MEASURE 4 "unrecognised" -- "$S5" --mode only --component core-tests --run-id run-1

S6="$TMP/prefix2.txt"
mk_only_summary "$S6" run-1 PARTIAL fmt "$(comp_line fmt SKIPPED 0s)"
expect "2.2 an unrecognised status is COULD-NOT-MEASURE (closed grammar), never a pass" \
  COULD-NOT-MEASURE 4 "unrecognised" -- "$S6" --mode only --component fmt --run-id run-1

# The COMPONENT NAME is matched exactly too. `fmt` must not bind to `fmt-extra`, and a
# sibling whose name merely starts with the requested one must not answer for it.
S7="$TMP/name-prefix.txt"
mk_only_summary "$S7" run-1 PARTIAL fmt "$(comp_line fmt SKIP 0s)" "$(comp_line fmt-extra PASS 1s)"
expect "2.3 the component NAME binds exactly — a PASSing sibling does not answer for it" \
  NOT-PASS 1 "SKIP" -- "$S7" --mode only --component fmt --run-id run-1

S8="$TMP/name-prefix2.txt"
mk_only_summary "$S8" run-1 PARTIAL fmt-extra "$(comp_line fmt-extra PASS 1s)"
expect "2.4 requesting the SHORTER name when only the longer one is present => absent" \
  NOT-PASS 1 "absent" -- "$S8" --mode only --component fmt --run-id run-1

# A META line is not a component line. `tree-integrity: PASS` carries no `(<N>s)` field,
# so the structural grammar (derived from _fm_summary_line) refuses to read it as one —
# otherwise asking for a mistyped or non-component name would return a confident PASS.
expect "2.5 a META line (tree-integrity: PASS) is NOT readable as a component verdict" \
  NOT-PASS 1 "absent" -- "$S2" --mode only --component tree-integrity --run-id run-1

echo "=== section 3: COMPLETION is a precondition, and the verdict is never DERIVED from it ==="
# Direction 1: a PASS component line inside a NON-TERMINAL block is not a verdict. The
# startup sentinel is written before any component runs, and a reader that answered from
# component lines alone could report a verdict for a run still in flight.
S9="$TMP/incomplete.txt"
mk_summary "$S9" run-1 "INCOMPLETE (gate did not finish)" "$(comp_line tooling-tests PASS 1112s)"
expect "3.1 a PASS component line in an INCOMPLETE block => COULD-NOT-MEASURE" \
  COULD-NOT-MEASURE 4 "not complete" -- "$S9" --mode only --component tooling-tests --run-id run-1

# Direction 2 — the one the lead's correction is about: the run's terminal token may not
# stand in for the component's verdict, in EITHER direction.
SA="$TMP/full-pass-absent.txt"
mk_summary "$SA" run-1 PASS "$(comp_line file-size PASS 0s)"
expect "3.2 a run RESULT of PASS does NOT make an absent component a pass" \
  NOT-PASS 1 "absent" -- "$SA" --mode only --component tooling-tests --run-id run-1

SB="$TMP/full-fail-comp-pass.txt"
mk_summary "$SB" run-1 "FAIL (1 component)" "$(comp_line fmt FAIL 3s)" "$(comp_line tooling-tests PASS 1112s)"
expect "3.3 a run RESULT of FAIL does NOT make a PASSing component not-pass" \
  PASS 0 "tooling-tests" -- "$SB" --mode only --component tooling-tests --run-id run-1

echo "=== section 4: every unmeasurable input is COULD-NOT-MEASURE with a NAMED cause ==="
expect "4.1 a summary file that does not exist" \
  COULD-NOT-MEASURE 4 "" -- "$TMP/nope.txt" --mode only --component fmt

SC="$TMP/truncated.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  comp_line tooling-tests PASS 1112s
  echo "RESULT: PARTIAL"
} > "$SC"   # NO end marker: a permanently truncated artifact
expect "4.2 a truncated block (no end marker) is never a verdict" \
  COULD-NOT-MEASURE 4 "" -- "$SC" --mode only --component tooling-tests --run-id run-1

SD="$TMP/foreign.txt"
mk_only_summary "$SD" run-PEER PARTIAL tooling-tests "$(comp_line tooling-tests PASS 1112s)"
expect "4.3 #2874: a block bearing a FOREIGN run-id answers about a peer, not us" \
  COULD-NOT-MEASURE 4 "" -- "$SD" --mode only --component tooling-tests --run-id run-1

SE="$TMP/notasummary.txt"
printf 'not a gate summary at all\n' > "$SE"
expect "4.4 a file that is not a gate summary" \
  COULD-NOT-MEASURE 4 "" -- "$SE" --mode only --component tooling-tests --run-id run-1

SF="$TMP/dup.txt"
mk_only_summary "$SF" run-1 PARTIAL tooling-tests \
  "$(comp_line tooling-tests PASS 1112s)" "$(comp_line tooling-tests SKIP 0s)"
expect "4.5 two lines for one component is AMBIGUOUS, never resolved in favour of PASS" \
  COULD-NOT-MEASURE 4 "" -- "$SF" --mode only --component tooling-tests --run-id run-1

echo "=== section 5: the accepted-verdict set is a PARAMETER OF THE RUN MODE ==="
# The issue's own words: a shared polling helper must take the accepted-verdict set as a
# parameter of the run MODE, never hard-code one grammar for both. So `--mode` is
# REQUIRED, and the modes this tool does not serve are NAMED REFUSALS pointing at their
# authority — not a second implementation of a grammar that already has an owner.
expect "5.1 --mode is REQUIRED (the accepted set is never implicit)" \
  USAGE 64 "--mode" -- "$S2" --component tooling-tests
expect "5.2 --mode record is REFUSED and names premerge-assert.sh as the authority" \
  USAGE 64 "premerge-assert" -- "$S2" --mode record --component tooling-tests
expect "5.3 --mode lite is REFUSED (a lite PASS is a different claim entirely)" \
  USAGE 64 "lite" -- "$S2" --mode lite --component tooling-tests
expect "5.4 an unknown mode is a refusal, never a default" \
  USAGE 64 "" -- "$S2" --mode wibble --component tooling-tests
expect "5.5 --mode only REQUIRES --component" \
  USAGE 64 "component" -- "$S2" --mode only
expect "5.6 a component name outside the closed grammar is refused, not injected" \
  USAGE 64 "" -- "$S2" --mode only --component 'foo.*bar|baz'

echo "=== section 6: the two DOCUMENTED text-completion grammars, run against real fixtures ==="
# These are the strings CLAUDE.md publishes. The whole defect was that the documented
# string did not behave as documented, so they are asserted BEHAVIOURALLY here rather
# than being trusted.
#
#   RECORD grammar (full / --lite / --delta): terminal ⇔ PASS or FAIL.
#   ONLY   grammar (--only):                  terminal ⇔ PASS, FAIL or PARTIAL.
#
# Both are ANCHORED and token-terminated. An unanchored `RESULT: (PASS|FAIL)` matches
# `RESULT: PASSENGER`, and an unanchored `…|PARTIAL)` matches `RESULT: PARTIALLY` — the
# prefix defect one layer out, in the very string being published.
RECORD_RE='^RESULT: (PASS|FAIL)([[:space:]]|$)'
ONLY_RE='^RESULT: (PASS|FAIL|PARTIAL)([[:space:]]|$)'

g() { grep -qE "$1" "$2"; }   # returns grep's status; called in `if`, never through a pipe

if ! g "$ONLY_RE" "$S2"; then
  bad "6.1 the --only grammar TERMINATES on a successful --only run (the #3750 hang)"
else ok "6.1 the --only grammar TERMINATES on a successful --only run (the #3750 hang)"; fi

if g "$RECORD_RE" "$S2"; then
  bad "6.2 the RECORD grammar must keep REFUSING PARTIAL (AC4)" \
      "a PARTIAL run matched the gate-of-record grammar"
else ok "6.2 the RECORD grammar must keep REFUSING PARTIAL (AC4)"; fi

if g "$ONLY_RE" "$S9"; then
  bad "6.3 widening to PARTIAL must not readmit the #3041 INCOMPLETE sentinel" \
      "INCOMPLETE matched the --only grammar"
else ok "6.3 widening to PARTIAL must not readmit the #3041 INCOMPLETE sentinel"; fi

SG="$TMP/partially.txt"
mk_summary "$SG" run-1 "PARTIALLY DONE" "$(comp_line fmt PASS 1s)"
if g "$ONLY_RE" "$SG"; then
  bad "6.4 the --only grammar is token-terminated (PARTIALLY is not PARTIAL)" \
      "PARTIALLY matched — the published string is a prefix match"
else ok "6.4 the --only grammar is token-terminated (PARTIALLY is not PARTIAL)"; fi

SH="$TMP/passenger.txt"
mk_summary "$SH" run-1 "PASSENGER" "$(comp_line fmt PASS 1s)"
if g "$RECORD_RE" "$SH"; then
  bad "6.5 the RECORD grammar is token-terminated (PASSENGER is not PASS)" \
      "PASSENGER matched — the published string is a prefix match"
else ok "6.5 the RECORD grammar is token-terminated (PASSENGER is not PASS)"; fi

# CONTROL: the record grammar still accepts the runs it is FOR, or 6.2/6.5 would pass
# against a grammar that matches nothing at all.
if g "$RECORD_RE" "$SA"; then ok "6.6 control: the RECORD grammar still accepts a real full-gate PASS"
else bad "6.6 control: the RECORD grammar still accepts a real full-gate PASS"; fi
if g "$RECORD_RE" "$SB"; then ok "6.7 control: the RECORD grammar still accepts a real full-gate FAIL"
else bad "6.7 control: the RECORD grammar still accepts a real full-gate FAIL"; fi

echo "=== section 7: COMPLETION is the shared reader's question, and it says COMPLETE on PARTIAL ==="
# One implementation, one grammar (roborev job 172): the verdict script ASKS
# gate-liveness.sh whether the run ended rather than re-greping the terminal set. This
# case pins the delegated half — that the reader treats a --only PARTIAL as terminal —
# so if that ever changes, this suite reds instead of the verdict script silently
# becoming unable to answer about any --only run.
_gl=$(bash "$READER" "$S2" --run-id run-1 --no-wait 2>&1); _glrc=$?
if [ "$_glrc" -eq 0 ] && printf '%s' "$_gl" | grep -q '^gate-liveness: COMPLETE '; then
  ok "7.1 gate-liveness.sh reports COMPLETE (exit 0) for a --only PARTIAL block"
else
  bad "7.1 gate-liveness.sh reports COMPLETE (exit 0) for a --only PARTIAL block" \
      "rc=$_glrc out=$(printf '%s' "$_gl" | head -1)"
fi
_gl=$(bash "$READER" "$S9" --run-id run-1 --no-wait 2>&1); _glrc=$?
if [ "$_glrc" -ne 0 ]; then
  ok "7.2 control: gate-liveness.sh does NOT report COMPLETE for an INCOMPLETE block"
else
  bad "7.2 control: gate-liveness.sh does NOT report COMPLETE for an INCOMPLETE block" \
      "out=$(printf '%s' "$_gl" | head -1)"
fi

# ---------------------------------------------------------------------------
# CASE FLOOR (#3544). A span-replacing edit once silently deleted four cases from a
# sibling suite and it reported `failed: 0` at 102 instead of 105 for a whole round — a
# green tally over a shrunken suite. Assert the count, not just the failures.
# ---------------------------------------------------------------------------
FLOOR=27
total=$((pass + fail))
if [ "$total" -lt "$FLOOR" ]; then
  bad "case floor: ran $total cases, expected at least $FLOOR (cases deleted?)"
fi

echo
echo "==== test_gate_component_verdict.sh: passed=$pass failed=$fail ===="
[ "$fail" -eq 0 ] || exit 1
exit 0
