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

# mk_block <path> <opener-suffix> <run-id> <RESULT> [line...] — a block with an
# ARBITRARY opener/closer variant (LITE / DELTA), for the mode-enforcement cases.
mk_block() {
  local path="$1" suf="$2" rid="$3" result="$4"; shift 4
  { echo "==== AGENT-GATE${suf} SUMMARY ===="
    echo "run-id: $rid"
    echo "commit: abc1234 branch: issue-3750 dirty: no"
    echo "tree-integrity: PASS"
    local l; for l in "$@"; do printf '%s\n' "$l"; done
    [ -n "$result" ] && echo "RESULT: $result"
    echo "==== END AGENT-GATE${suf} SUMMARY ===="
  } > "$path"
}
# mk_beat <path> <run-id> <age-secs> [interval] — copied field-for-field from
# scripts/tests/test_gate_liveness.sh's fixture, so the shared reader sees the shape it
# really parses.
mk_beat() {
  local iv="${4:-20}"
  { echo "==== AGENT-GATE HEARTBEAT ===="
    echo "run-id: $2"
    echo "gate-pid: 4242"
    echo "parent-check: starttime"
    echo "host: $(uname -n 2>/dev/null || echo unknown)"
    echo "interval: $iv"
    echo "beat-seq: 7"
    echo "beat-epoch: $(( $(date +%s) - $3 ))"
    echo "==== END AGENT-GATE HEARTBEAT ===="
  } > "$1"
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
  # The mktemp SUFFIX, not the bare name: the script's own file name is
  # `gate-component-verdict.sh`, which --help legitimately prints.
  if printf '%s' "$out" | grep -qE 'gate-component-verdict\.[A-Za-z0-9]{6}'; then
    bad "$label" "output names the PRIVATE SNAPSHOT path, which will not exist when anyone reads it: $(printf '%s' "$out" | head -2)"; return
  fi
  if printf '%s' "$out" | grep -qF '==== AGENT-GATE'; then
    bad "$label" "output carries an AGENT-GATE block marker: $(printf '%s' "$out" | head -3)"; return
  fi
  # (4) EVERY non-empty output line, stdout AND stderr, must carry the anchor, or the
  # output is not reliably attributable and a fragment of it could be pasted as
  # something else. Same property base-staleness.sh's `BASE-STALENESS: ` prefix has.
  local unanchored
  unanchored=$(printf '%s\n' "$out" | grep -vE '^gate-verdict: ' | grep -v '^$' || true)
  if [ -n "$unanchored" ]; then
    bad "$label" "output line(s) missing the gate-verdict: anchor: $(printf '%s' "$unanchored" | head -2)"; return
  fi
  if ! printf '%s' "$out" | grep -q "^gate-verdict: $want "; then
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
  COULD-NOT-MEASURE 4 "run-not-complete" -- "$S9" --mode only --component tooling-tests --run-id run-1

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
  USAGE 64 "mode is required" -- "$S2" --component tooling-tests
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

# --help must print the header COMMENT BLOCK and stop there. It used to be a fixed line
# range, which bleeds into the code the moment the header changes length — and a --help
# that prints `set -uo pipefail` is a reader being shown the wrong thing.
_h=$(bash "$VERDICT" --help 2>&1); _hrc=$?
_hlast=$(printf '%s\n' "$_h" | grep -v '^$' | tail -1)
if [ "$_hrc" -eq 0 ] && [ -n "$_h" ] && printf '%s' "$_hlast" | grep -q '^#'; then
  ok "5.7 --help exits 0 and stops at the header boundary (never bleeds into the code)"
else
  bad "5.7 --help exits 0 and stops at the header boundary (never bleeds into the code)" \
      "rc=$_hrc last-line='$_hlast'"
fi

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

echo "=== section 8: every read is BOUNDED BY THE VALIDATED BLOCK (B1) ==="
# gate-liveness.sh:143-149 DECLARES its own residual: a well-formed blend is
# indistinguishable, and its structure check constrains only the COUNTS and ORDERING of
# opener/closer/RESULT/run-id — never that no lines sit OUTSIDE the span. So a stale tail
# left by a PREVIOUS write to the same path is inside the FILE and outside the BLOCK, and
# a whole-file grep reads it as this run's verdict. That is a false PASS in a tool whose
# entire subject is the vacuous pass.
S8="$TMP/outside-above.txt"
{ comp_line tooling-tests PASS 1112s '[stale tail of a previous write]'
  echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: PASS"
  echo "mode: PARTIAL (--only fmt) - does NOT count as the gate"
  comp_line fmt FAIL 3s
  echo "RESULT: FAIL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$S8"
expect "8.1 a stale component line ABOVE the opener is NOT this run's verdict" \
  NOT-PASS 1 "absent" -- "$S8" --mode only --component tooling-tests --run-id run-1

S8b="$TMP/outside-below.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: PASS"
  echo "mode: PARTIAL (--only fmt) - does NOT count as the gate"
  comp_line fmt FAIL 3s
  echo "RESULT: FAIL"
  echo "==== END AGENT-GATE SUMMARY ===="
  comp_line tooling-tests PASS 1112s '[stale tail]'
} > "$S8b"
expect "8.2 a stale component line BELOW the closer is NOT this run's verdict either" \
  NOT-PASS 1 "absent" -- "$S8b" --mode only --component tooling-tests --run-id run-1

# CONTROL: the SAME line INSIDE the block still reads PASS, or 8.1/8.2 would pass against
# a reader that had simply stopped finding component lines at all.
S8c="$TMP/inside.txt"
mk_only_summary "$S8c" run-1 PARTIAL tooling-tests "$(comp_line tooling-tests PASS 1112s)"
expect "8.3 control: the same line INSIDE the block still reads PASS" \
  PASS 0 "tooling-tests" -- "$S8c" --mode only --component tooling-tests --run-id run-1

# TWO openers: the extent is not unique, so no read can be bounded. Refuse.
S8d="$TMP/two-blocks.txt"
{ cat "$S8c"; cat "$S8c"; } > "$S8d"
expect "8.4 two blocks in one file => the extent is not unique, so refuse" \
  COULD-NOT-MEASURE 4 "" -- "$S8d" --mode only --component tooling-tests --run-id run-1

echo "=== section 9: --mode only is ENFORCED against the artifact, not merely validated (B2) ==="
# --mode was validated and then never read again, so the record/lite/delta refusals were
# defeated by declaring `--mode only` and pointing at the other artifact. Measured: a LITE
# block's `clippy: PASS` was returned as a component verdict — for --lite's PER-PACKAGE
# SCOPED clippy — which is exactly the misreading the --mode lite refusal text claims to
# prevent.
S9L="$TMP/lite.txt"
mk_block "$S9L" " LITE" run-1 PASS "MODE: lite (FAST ITERATION — NOT the gate of record)" \
  "$(comp_line clippy PASS 219s)"
expect "9.1 a LITE block under --mode only is REFUSED by name, never answered" \
  COULD-NOT-MEASURE 4 "lite" -- "$S9L" --mode only --component clippy --run-id run-1

S9D="$TMP/delta.txt"
mk_block "$S9D" " DELTA" run-1 PASS "MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION)" \
  "$(comp_line fmt PASS 8s)"
expect "9.2 a DELTA block under --mode only is REFUSED by name" \
  COULD-NOT-MEASURE 4 "delta" -- "$S9D" --mode only --component fmt --run-id run-1

# TRAP 1: `--only` takes a COMMA-SEPARATED LIST (agent-gate.sh's `--only` arg), so a
# membership test written as equality REDS a correct `--only fmt,clippy` run.
S9C="$TMP/only-list.txt"
mk_only_summary "$S9C" run-1 PARTIAL "fmt,clippy" \
  "$(comp_line fmt PASS 8s)" "$(comp_line clippy PASS 219s)"
expect "9.3 control: a COMMA-LIST --only run is answered, not red (trap 1)" \
  PASS 0 "clippy" -- "$S9C" --mode only --component clippy --run-id run-1

# A component line present for a component the run did NOT select is a contradiction
# between the block's own scope and its content — refuse rather than pick one.
S9X="$TMP/scope-contradiction.txt"
mk_only_summary "$S9X" run-1 PARTIAL fmt \
  "$(comp_line fmt PASS 8s)" "$(comp_line tooling-tests PASS 1112s)"
expect "9.4 a line for a component the --only scope EXCLUDES is a contradiction, not a pass" \
  COULD-NOT-MEASURE 4 "scope" -- "$S9X" --mode only --component tooling-tests --run-id run-1

# TRAP 2: the tree-integrity BOUNDARY emit writes NO `mode:` line at all, so requiring one
# unconditionally REDS a legitimate --only block. A full-marker block with no `mode:` line
# must still be answerable.
S9N="$TMP/no-mode-line.txt"
mk_summary "$S9N" run-1 PARTIAL "$(comp_line tooling-tests PASS 1112s)"
expect "9.5 control: a full-marker block with NO mode: line is still answerable (trap 2)" \
  PASS 0 "tooling-tests" -- "$S9N" --mode only --component tooling-tests --run-id run-1

echo "=== section 10: the INTEGRITY lines invalidate every component in the block (B3) ==="
# A mutated-mid-run run stamps `tree-integrity: FAIL (tree-mutated-midrun; …)` and a FAIL
# verdict while the component line still reads PASS. Emitting PASS from such a block
# contradicts #2926/#2874 and this diff's own gate-ops.md text. Unlike a SIBLING
# component's FAIL (case 3.3, which is correct to ignore), the integrity lines invalidate
# the WHOLE block.
SA1="$TMP/tree-mutated.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: FAIL (tree-mutated-midrun; head aaaa→bbbb; detected-after-component: tooling-tests)"
  echo "mode: PARTIAL (--only tooling-tests) - does NOT count as the gate"
  comp_line tooling-tests PASS 1112s
  echo "RESULT: FAIL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$SA1"
expect "10.1 tree-integrity FAIL + a PASSing component line => NOT-PASS" \
  NOT-PASS 1 "tree-integrity" -- "$SA1" --mode only --component tooling-tests --run-id run-1

# TRAP: a THIRD legitimate value exists. SKIP means the check never ran, which is
# unmeasurable — never FAIL.
for _v in "SKIP (no capture)" "PENDING" "PASSENGER"; do
  _f="$TMP/ti-$(printf '%s' "$_v" | tr -c 'A-Za-z' '-').txt"
  { echo "==== AGENT-GATE SUMMARY ===="
    echo "run-id: run-1"
    echo "tree-integrity: $_v"
    echo "mode: PARTIAL (--only tooling-tests) - does NOT count as the gate"
    comp_line tooling-tests PASS 1112s
    echo "RESULT: PARTIAL"
    echo "==== END AGENT-GATE SUMMARY ===="
  } > "$_f"
  expect "10.2[$_v] a tree-integrity token outside {PASS,FAIL} is COULD-NOT-MEASURE, never FAIL" \
    COULD-NOT-MEASURE 4 "tree-integrity" -- "$_f" --mode only --component tooling-tests --run-id run-1
done

SA3="$TMP/ti-absent.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "mode: PARTIAL (--only tooling-tests) - does NOT count as the gate"
  comp_line tooling-tests PASS 1112s
  echo "RESULT: PARTIAL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$SA3"
expect "10.3 an ABSENT tree-integrity line is COULD-NOT-MEASURE, never assumed benign" \
  COULD-NOT-MEASURE 4 "tree-integrity" -- "$SA3" --mode only --component tooling-tests --run-id run-1

SA4="$TMP/ti-twice.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: PASS"
  echo "tree-integrity: FAIL (tree-mutated-midrun)"
  comp_line tooling-tests PASS 1112s
  echo "RESULT: PARTIAL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$SA4"
expect "10.4 two tree-integrity lines is ambiguous, never resolved in favour of PASS" \
  COULD-NOT-MEASURE 4 "tree-integrity" -- "$SA4" --mode only --component tooling-tests --run-id run-1

SA5="$TMP/summary-integrity.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: PASS"
  echo "summary-integrity: FAIL (foreign run-id observed; detected-after-component: fmt)"
  comp_line tooling-tests PASS 1112s
  echo "RESULT: FAIL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$SA5"
expect "10.5 a summary-integrity line (only ever FAIL) invalidates the block too" \
  NOT-PASS 1 "summary-integrity" -- "$SA5" --mode only --component tooling-tests --run-id run-1

# CONTROL: the check is TOKEN-terminated, not whole-value equality — the gate emits
# `tree-integrity: PASS (selftest)` and `PASS (lockfile-settled: …)`.
SA6="$TMP/ti-detail.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: PASS (lockfile-settled: Cargo.lock)"
  comp_line tooling-tests PASS 1112s
  echo "RESULT: PARTIAL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$SA6"
expect "10.6 control: tree-integrity PASS with trailing detail is still a PASS" \
  PASS 0 "tooling-tests" -- "$SA6" --mode only --component tooling-tests --run-id run-1

echo "=== section 11: --help obeys the SAME four invariants as every verdict (B4) ==="
# CLAUDE.md #3312 instance 2, verbatim: the artifact that DESCRIBED the escape hatch became
# it. --help printed the header, which spelled the forbidden literals out — so the sentence
# explaining why the token must never be emitted WAS the emitted token, and an unanchored
# record probe over --help MATCHED. The bespoke check that used to sit here asserted none of
# the four invariants, which is how it shipped. So --help now goes through `expect_raw`,
# which applies exactly the invariant block every verdict case gets.
expect_raw_help() {
  local out rc bad_any=0
  out=$(bash "$VERDICT" --help 2>&1); rc=$?
  [ "$rc" -eq 0 ] || { bad "11.1 --help exits 0" "rc=$rc"; bad_any=1; }
  local unanchored
  unanchored=$(printf '%s\n' "$out" | grep -vE '^gate-verdict: ' | grep -v '^$' || true)
  if [ -n "$unanchored" ]; then
    bad "11.2 every --help line carries the gate-verdict: anchor" "$(printf '%s' "$unanchored" | head -2)"; bad_any=1
  else ok "11.2 every --help line carries the gate-verdict: anchor"; fi
  if printf '%s' "$out" | grep -qE 'RESULT:[[:space:]]*[A-Z]'; then
    bad "11.3 --help carries no bare RESULT token" "$(printf '%s' "$out" | grep -E 'RESULT:[[:space:]]*[A-Z]' | head -2)"; bad_any=1
  else ok "11.3 --help carries no bare RESULT token"; fi
  if printf '%s' "$out" | grep -qF '==== AGENT-GATE'; then
    bad "11.4 --help carries no AGENT-GATE block marker" "$(printf '%s' "$out" | grep -F '==== AGENT-GATE' | head -2)"; bad_any=1
  else ok "11.4 --help carries no AGENT-GATE block marker"; fi
  # The whole point, stated as the property rather than as a spelling: the UNANCHORED
  # record probe — the one every stale poll site still carries — must not match --help.
  if printf '%s\n' "$out" | grep -qE 'RESULT: (PASS|FAIL)'; then
    bad "11.5 the unanchored record probe does NOT match --help output"; bad_any=1
  else ok "11.5 the unanchored record probe does NOT match --help output"; fi
  [ "$bad_any" -eq 0 ] && ok "11.1 --help exits 0"
  # AND it must still TEACH both grammars, or the fix would be "delete the content".
  if printf '%s' "$out" | grep -qF 'PARTIAL' && printf '%s' "$out" | grep -qF '[[:space:]]'; then
    ok "11.6 --help still teaches both anchored, token-terminated grammars"
  else
    bad "11.6 --help still teaches both anchored, token-terminated grammars"
  fi
}
expect_raw_help

echo "=== section 12: a missing OPTION VALUE is an anchored USAGE refusal, not a bash error ==="
# `${2:?…}` exits 1 with an UNANCHORED bash diagnostic where this script documents an
# anchored USAGE refusal at 64. The suite had no case per option, which is why it shipped.
for _o in --mode --component --run-id --heartbeat; do
  expect "12[$_o] a missing value is an anchored USAGE refusal at 64" \
    USAGE 64 "$(printf '%s' "$_o" | tr -d '-')" -- "$S8c" "$_o"
done
# The closed name grammar must be NEWLINE-SAFE: a line-based check validates $'fmt\nx',
# and COMP_RE then becomes a multi-pattern grep with a bare `^fmt`. It only failed to
# produce a PASS by accident, and safe-by-accident is not safe.
expect "12.5 a component name containing a NEWLINE is refused by the grammar itself" \
  USAGE 64 "" -- "$S8c" --mode only --component "$(printf 'fmt\nx')"

echo "=== section 13: RETRYABLE and PERMANENT do not share an exit code ==="
# The #3750 hang shape, reproduced one directory over inside its own fix: a caller polling
# the exit code cannot tell "keep waiting" from "never measurable", so it spins forever.
S13="$TMP/running.txt"
mk_summary "$S13" run-1 "INCOMPLETE (gate did not finish)"
mk_beat "$S13.heartbeat" run-1 5 20
expect "13.1 a LIVE run gets its own RETRYABLE verdict and exit code" \
  NOT-COMPLETE 5 "" -- "$S13" --mode only --component tooling-tests --run-id run-1
# And the permanent states keep exit 4, so the two are distinguishable by code alone.
expect "13.2 control: a permanently truncated block stays PERMANENT (4), not retryable" \
  COULD-NOT-MEASURE 4 "" -- "$SC" --mode only --component tooling-tests --run-id run-1

# ---------------------------------------------------------------------------
# CASE FLOOR (#3544). A span-replacing edit once silently deleted four cases from a
# sibling suite and it reported `failed: 0` at 102 instead of 105 for a whole round — a
# green tally over a shrunken suite. Assert the count, not just the failures.
# ---------------------------------------------------------------------------
FLOOR=28
total=$((pass + fail))
if [ "$total" -lt "$FLOOR" ]; then
  bad "case floor: ran $total cases, expected at least $FLOOR (cases deleted?)"
fi

echo
echo "==== test_gate_component_verdict.sh: passed=$pass failed=$fail ===="
[ "$fail" -eq 0 ] || exit 1
exit 0
