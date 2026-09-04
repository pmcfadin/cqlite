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
# Cases are counted PER SECTION as well as in total — see the floor block at the end for
# why a total alone is not enough. The section is the label's leading digits.
_count_section() {
  local sec=${1%%[^0-9]*}
  [ -n "$sec" ] || return 0
  eval "SEC_$sec=\$(( \${SEC_$sec:-0} + 1 ))"
}
ok()  { pass=$((pass+1)); _count_section "$1"; printf 'ok   %s\n' "$1"; }
bad() { fail=$((fail+1)); _count_section "$1"; printf 'FAIL %s\n' "$1"; [ $# -ge 2 ] && printf '     %s\n' "$2"; }

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
  # (5) A VERDICT MAY NOT OPINE ON LIVENESS. This tool is not a completion probe and has no
  # three-valued view of a running gate; `gate-liveness.sh` is the authority and the only one
  # of the two that may be polled. A retryability claim here was measured to tell a lane `do
  # not poll` about a LIVE gate whose beat was merely stale, whose obedient response is to
  # relaunch it — two gates on one summary path, the outcome gate-ops.md exists to prevent.
  # Asserted for EVERY case, so no branch can grow one back. Scoped to verdicts on purpose:
  # --help legitimately EXPLAINS the boundary (section 11).
  if printf '%s' "$out" | grep -qiE 'retryab|poll on exit|do not poll'; then
    bad "$label" "verdict asserts a retryability claim it cannot support: $(printf '%s' "$out" | head -2)"; return
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

# A META line is not a component line, and since F6-2 that is refused ONE LAYER EARLIER and
# more strongly: `tree-integrity` is absent from the gate's component manifest, so the request
# is refused BY NAME before any block is scanned. The structural grammar (no `(<N>s)`
# duration field on a meta line) remains as the second layer behind it — 17.1/17.6 pin that —
# so a component-shaped meta line still cannot answer even for a name that IS in the manifest.
expect "2.5 a META name (tree-integrity) is refused by name, before any block is scanned" \
  USAGE 64 "manifest" -- "$S2" --mode only --component tree-integrity --run-id run-1

echo "=== section 3: COMPLETION is a precondition, and the verdict is never DERIVED from it ==="
# Direction 1: a PASS component line inside a NON-TERMINAL block is not a verdict. The
# startup sentinel is written before any component runs, and a reader that answered from
# component lines alone could report a verdict for a run still in flight.
S9="$TMP/incomplete.txt"
mk_summary "$S9" run-1 "INCOMPLETE (gate did not finish)" "$(comp_line tooling-tests PASS 1112s)"
expect "3.1 a PASS component line in an INCOMPLETE block (no beat) => COULD-NOT-MEASURE" \
  COULD-NOT-MEASURE 4 "run-not-terminal" -- "$S9" --mode only --component tooling-tests --run-id run-1

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
  COULD-NOT-MEASURE 4 "summary-absent" -- "$TMP/nope.txt" --mode only --component fmt

SC="$TMP/truncated.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  comp_line tooling-tests PASS 1112s
  echo "RESULT: PARTIAL"
} > "$SC"   # NO end marker: a permanently truncated artifact
expect "4.2 a truncated block (no end marker) is never a verdict" \
  COULD-NOT-MEASURE 4 "gate-liveness" -- "$SC" --mode only --component tooling-tests --run-id run-1

SD="$TMP/foreign.txt"
mk_only_summary "$SD" run-PEER PARTIAL tooling-tests "$(comp_line tooling-tests PASS 1112s)"
expect "4.3 #2874: a block bearing a FOREIGN run-id answers about a peer, not us" \
  COULD-NOT-MEASURE 4 "run-id" -- "$SD" --mode only --component tooling-tests --run-id run-1

SE="$TMP/notasummary.txt"
printf 'not a gate summary at all\n' > "$SE"
expect "4.4 a file that is not a gate summary" \
  COULD-NOT-MEASURE 4 "gate-liveness" -- "$SE" --mode only --component tooling-tests --run-id run-1

SF="$TMP/dup.txt"
mk_only_summary "$SF" run-1 PARTIAL tooling-tests \
  "$(comp_line tooling-tests PASS 1112s)" "$(comp_line tooling-tests SKIP 0s)"
expect "4.5 two lines for one component is AMBIGUOUS, never resolved in favour of PASS" \
  COULD-NOT-MEASURE 4 "ambiguous-component-line" -- "$SF" --mode only --component tooling-tests --run-id run-1

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
  USAGE 64 "unknown --mode" -- "$S2" --mode wibble --component tooling-tests
expect "5.5 --mode only REQUIRES --component" \
  USAGE 64 "component" -- "$S2" --mode only
expect "5.6 a component name outside the closed grammar is refused, not injected" \
  USAGE 64 "closed grammar" -- "$S2" --mode only --component 'foo.*bar|baz'

# --help must print the header COMMENT BLOCK and stop there. It used to be a fixed line
# range, which bleeds into the code the moment the header changes length — and a --help
# that prints `set -uo pipefail` is a reader being shown the wrong thing.
_h=$(bash "$VERDICT" --help 2>&1); _hrc=$?
_hlast=$(printf '%s\n' "$_h" | grep -v '^$' | tail -1)
if [ "$_hrc" -eq 0 ] && [ -n "$_h" ] && printf '%s' "$_hlast" | grep -q '^gate-verdict: #'; then
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
#   RECORD grammar (full / --lite):           terminal ⇔ PASS or FAIL.
#   ONLY   grammar (--only <component>):      terminal ⇔ PASS, FAIL or PARTIAL.
#   DELTA  grammar (--delta <anchor>):        + ERROR or REFUSED (see §15).
#
# Both are ANCHORED and token-terminated. An unanchored `RESULT: (PASS|FAIL)` matches
# `RESULT: PASSENGER`, and an unanchored `…|PARTIAL)` matches `RESULT: PARTIALLY` — the
# prefix defect one layer out, in the very string being published.
RECORD_RE='^RESULT: (PASS|FAIL)([[:space:]]|$)'
ONLY_RE='^RESULT: (PASS|FAIL|PARTIAL)([[:space:]]|$)'
# THE THIRD MODE. `--delta` can terminate with ERROR (4 emit sites) or REFUSED (3, via
# `emit_summary "$(_tree_result REFUSED)"`), neither of which the RECORD grammar matches —
# so a --delta poller using the record grammar HANGS FOREVER on a terminal outcome, which
# is #3750's own defect class in a different mode. Set token-for-token with
# gate-liveness.sh's enumerated terminal set, so there is ONE source of truth for "what is
# terminal"; case 15.10 DERIVES that equality from the reader rather than trusting this line.
DELTA_RE='^RESULT: (PASS|FAIL|PARTIAL|ERROR|REFUSED)([[:space:]]|$)'

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

# THE PUBLISHED EXIT MAPPING, DERIVED FROM THE GATE'S SOURCE (the 17.5 / 19.3 idiom).
# Every doc site this change touches publishes "--only exits 3" as the PRIMARY completion
# signal, and NOTHING in the gate's own component set derives that from source — the only
# assertion of it lives in an opt-in suite which swallows the status in an `if …; then :; fi`.
# The claim is TRUE at HEAD, so this is not a defect; but a published exit code that nothing
# checks can ROT SILENTLY, and a lane that then polls exit 3 waits forever on a run that has
# already finished. That is #3750's own failure mode, one layer down from the text grammars.
#
# Asserted as a MAPPING, not a grep for `exit 3`: PASS->0, PARTIAL->3, everything else->1, in
# ONE terminal `case "$OVERALL"`, and `exit 3` reachable from NO other top-level arm — because
# what the docs promise is that 3 means "completed PARTIAL" and nothing else.
_gate="$REPO_ROOT/scripts/agent-gate.sh"
_map=$(sed -n '/^case "\$OVERALL" in$/,/^esac$/p' "$_gate" | tr -s '[:space:]' ' ')
_map_n=$(grep -c '^case "\$OVERALL" in$' "$_gate")
_other3=$(grep -cE '^[[:space:]]*(\*\)|[A-Z]+\))[[:space:]]*exit 3' "$_gate")
if [ "$_map_n" = 1 ] \
   && printf '%s' "$_map" | grep -qF 'PASS) exit 0 ;; PARTIAL) exit 3 ;; *) exit 1 ;;' \
   && [ "$_other3" = 1 ]; then
  ok "6.8 the gate's terminal exit mapping is PASS->0 / PARTIAL->3 / else->1, and 3 is PARTIAL's alone"
else
  bad "6.8 the gate's terminal exit mapping is PASS->0 / PARTIAL->3 / else->1, and 3 is PARTIAL's alone" \
      "terminal-case-blocks=$_map_n exit-3-arms=$_other3 map='$(printf '%s' "$_map" | cut -c1-90)'"
fi

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
if [ "$_glrc" -eq 4 ] && printf '%s' "$_gl" | grep -q '^gate-liveness: UNKNOWN '; then
  ok "7.2 control: gate-liveness.sh reports UNKNOWN (4), not COMPLETE, for an INCOMPLETE block"
else
  bad "7.2 control: gate-liveness.sh reports UNKNOWN (4), not COMPLETE, for an INCOMPLETE block" \
      "rc=$_glrc out=$(printf '%s' "$_gl" | head -1)"
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
# HONESTY, and the needle is what buys it. This shape is refused UPSTREAM by the shared
# reader's own framing check (gate-liveness.sh's `summary-not-a-single-block`), so it never
# reaches the extent branches at all — with an EMPTY needle it passed identically with the
# whole B1 extent block deleted, i.e. it proved nothing about the code it was filed under.
# The needle names the layer that ANSWERED. B1 itself stays pinned by 8.1/8.2, which
# genuinely need the slice; the extent branches are DEFENCE IN DEPTH behind a reader that
# refuses this shape first, and saying so is worth more than a green that misattributes.
expect "8.4 two blocks in one file is refused UPSTREAM by the reader's framing check (the extent branch below it is defence-in-depth)" \
  COULD-NOT-MEASURE 4 "single-block" -- "$S8d" --mode only --component tooling-tests --run-id run-1

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

# TRAP 2, AND ITS JUSTIFICATION IS NOW VOID — see section 18. The rule used to be "an absent
# `mode:` line is not required, because the tree-integrity BOUNDARY emit writes none".
# MEASURED: that boundary emit's run token is FAIL, never PARTIAL, and its block always carries
# `tree-integrity: FAIL`, which the B3 precondition rejects upstream. So a PARTIAL token with no
# `mode:` line is a shape NO emitter produces, and accepting it was dead permissiveness. The
# trap-2 property SURVIVES, narrowed to what is real: a FAIL-token block with no `mode:` line is
# still answerable (18.4).
S9N="$TMP/no-mode-line.txt"
mk_summary "$S9N" run-1 PARTIAL "$(comp_line tooling-tests PASS 1112s)"
expect "9.5 a PARTIAL token with NO mode: line is a shape no emitter produces => refuse" \
  COULD-NOT-MEASURE 4 "mode-scope-missing" -- "$S9N" --mode only --component tooling-tests --run-id run-1

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
# The scope line is present because F4 requires it of any PARTIAL block — and no emitter
# produces a PARTIAL token without one, so a fixture lacking it was never realistic. This
# case's subject is the tree-integrity TOKEN, not the scope line.
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: PASS (lockfile-settled: Cargo.lock)"
  echo "mode: PARTIAL (--only tooling-tests) - does NOT count as the gate"
  comp_line tooling-tests PASS 1112s
  echo "RESULT: PARTIAL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$SA6"
expect "10.6 control: tree-integrity PASS with trailing detail is still a PASS" \
  PASS 0 "tooling-tests" -- "$SA6" --mode only --component tooling-tests --run-id run-1

# AN UNCERTAIN TREE-INTEGRITY MUST NOT MASK A CERTAIN SUMMARY-INTEGRITY FAILURE (#3951).
# The tree-integrity checks return EARLY, so a block carrying an UNMEASURED tree verdict
# together with `summary-integrity: FAIL` was answered COULD-NOT-MEASURE (4) where the
# header documents NOT-PASS (1). `summary-integrity: FAIL` is an INDEPENDENT, affirmatively
# established declaration that the whole block is non-certifying (#2874 — a foreign run-id
# clobbered the path mid-run), so the more specific, affirmatively-established signal wins:
# never let a "cannot tell" mask a "definitely not".
#
# SEVERITY, STATED PLAINLY: this can produce no false PASS — 4 and 1 are both refusals. What
# was wrong is the PRECISION of the refusal, and that a header claim and the code disagreed,
# which is the stale-prose class.
_mk_si() {  # <path> <tree-integrity value> [extra summary-integrity line...]
  local path="$1" ti="$2"; shift 2
  { echo "==== AGENT-GATE SUMMARY ===="
    echo "run-id: run-1"
    [ -n "$ti" ] && echo "tree-integrity: $ti"
    local l; for l in "$@"; do printf '%s\n' "$l"; done
    echo "mode: PARTIAL (--only tooling-tests) - does NOT count as the gate"
    comp_line tooling-tests PASS 1112s
    echo "RESULT: PARTIAL"
    echo "==== END AGENT-GATE SUMMARY ===="
  } > "$path"
}
_SI_FAIL="summary-integrity: FAIL (foreign run-id observed; detected-after-component: fmt)"

_mk_si "$TMP/si-over-skip.txt" "SKIP (no capture)" "$_SI_FAIL"
expect "10.7 tree-integrity SKIP + summary-integrity FAIL => NOT-PASS, naming the CERTAIN signal" \
  NOT-PASS 1 "summary-integrity" -- "$TMP/si-over-skip.txt" --mode only --component tooling-tests --run-id run-1

_mk_si "$TMP/si-over-pending.txt" "PENDING" "$_SI_FAIL"
expect "10.8 tree-integrity PENDING + summary-integrity FAIL => NOT-PASS" \
  NOT-PASS 1 "summary-integrity" -- "$TMP/si-over-pending.txt" --mode only --component tooling-tests --run-id run-1

_mk_si "$TMP/si-over-absent.txt" "" "$_SI_FAIL"
expect "10.9 an ABSENT tree-integrity line + summary-integrity FAIL => NOT-PASS" \
  NOT-PASS 1 "summary-integrity" -- "$TMP/si-over-absent.txt" --mode only --component tooling-tests --run-id run-1

_mk_si "$TMP/si-over-ambig-ti.txt" "PASS" "tree-integrity: SKIP (no capture)" "$_SI_FAIL"
expect "10.10 an AMBIGUOUS tree-integrity + summary-integrity FAIL => NOT-PASS" \
  NOT-PASS 1 "summary-integrity" -- "$TMP/si-over-ambig-ti.txt" --mode only --component tooling-tests --run-id run-1

# AMBIGUITY IS NEVER A GUESS, in EITHER direction. A duplicated `summary-integrity:` line is
# not an affirmative FAIL, so it may not preempt an uncertain tree verdict either — the
# refusal stands, unpromoted.
_mk_si "$TMP/si-dup-with-skip.txt" "SKIP (no capture)" "$_SI_FAIL" "$_SI_FAIL"
expect "10.11 a DUPLICATED summary-integrity line cannot promote anything — ambiguity stays a refusal" \
  COULD-NOT-MEASURE 4 "" -- "$TMP/si-dup-with-skip.txt" --mode only --component tooling-tests --run-id run-1

_mk_si "$TMP/si-dup-with-pass.txt" "PASS" "$_SI_FAIL" "$_SI_FAIL"
expect "10.12 a DUPLICATED summary-integrity line is ambiguous even under a PASSing tree verdict" \
  COULD-NOT-MEASURE 4 "summary-integrity-ambiguous" -- "$TMP/si-dup-with-pass.txt" --mode only --component tooling-tests --run-id run-1

# CONTROLS — the mappings the C audit verified behaviourally against the shipped script must
# be UNDISTURBED by this change, so both are re-asserted right beside it.
_mk_si "$TMP/si-none-skip.txt" "SKIP (no capture)"
expect "10.13 control: tree-integrity SKIP ALONE is still COULD-NOT-MEASURE (mapping undisturbed)" \
  COULD-NOT-MEASURE 4 "the tree check never ran" -- "$TMP/si-none-skip.txt" --mode only --component tooling-tests --run-id run-1

_mk_si "$TMP/si-with-ti-fail.txt" "FAIL (tree-mutated-midrun; detected-after-component: fmt)" "$_SI_FAIL"
expect "10.14 control: tree-integrity FAIL keeps naming the TREE cause (its verdict is already certain)" \
  NOT-PASS 1 "tree mutation" -- "$TMP/si-with-ti-fail.txt" --mode only --component tooling-tests --run-id run-1

echo "=== section 11: --help obeys the SAME four invariants as every verdict (B4) ==="
# CLAUDE.md #3312 instance 2, verbatim: the artifact that DESCRIBED the escape hatch became
# it. --help printed the header, which spelled the forbidden literals out — so the sentence
# explaining why the token must never be emitted WAS the emitted token, and an unanchored
# record probe over --help MATCHED. The bespoke check that used to sit here asserted none of
# the four invariants, which is how it shipped. So --help now goes through `expect_raw`,
# which applies exactly the invariant block every verdict case gets.
expect_raw_help() {
  local out rc
  out=$(bash "$VERDICT" --help 2>&1); rc=$?
  # EMITTED UNCONDITIONALLY, here and nowhere else. The earlier form emitted 11.1 only in
  # the all-green tail, so any other 11.x failure left section 11 at 5 cases and the section
  # floor added a SPURIOUS failure to an already-failing run.
  if [ "$rc" -eq 0 ]; then ok "11.1 --help exits 0"; else bad "11.1 --help exits 0" "rc=$rc"; fi
  local unanchored
  unanchored=$(printf '%s\n' "$out" | grep -vE '^gate-verdict: ' | grep -v '^$' || true)
  if [ -n "$unanchored" ]; then
    bad "11.2 every --help line carries the gate-verdict: anchor" "$(printf '%s' "$unanchored" | head -2)"
  else ok "11.2 every --help line carries the gate-verdict: anchor"; fi
  if printf '%s' "$out" | grep -qE 'RESULT:[[:space:]]*[A-Z]'; then
    bad "11.3 --help carries no bare RESULT token" "$(printf '%s' "$out" | grep -E 'RESULT:[[:space:]]*[A-Z]' | head -2)"
  else ok "11.3 --help carries no bare RESULT token"; fi
  if printf '%s' "$out" | grep -qF '==== AGENT-GATE'; then
    bad "11.4 --help carries no AGENT-GATE block marker" "$(printf '%s' "$out" | grep -F '==== AGENT-GATE' | head -2)"
  else ok "11.4 --help carries no AGENT-GATE block marker"; fi
  # The whole point, stated as the property rather than as a spelling: the UNANCHORED
  # record probe — the one every stale poll site still carries — must not match --help.
  if printf '%s\n' "$out" | grep -qE 'RESULT: (PASS|FAIL)'; then
    bad "11.5 the unanchored record probe does NOT match --help output"
  else ok "11.5 the unanchored record probe does NOT match --help output"; fi
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
    USAGE 64 "${_o#--}" -- "$S8c" "$_o"
done
# The closed name grammar must be NEWLINE-SAFE: a line-based check validates $'fmt\nx',
# and COMP_RE then becomes a multi-pattern grep with a bare `^fmt`. It only failed to
# produce a PASS by accident, and safe-by-accident is not safe.
expect "12.5 a component name containing a NEWLINE is refused by the grammar itself" \
  USAGE 64 "closed grammar" -- "$S8c" --mode only --component "$(printf 'fmt\nx')"

echo "=== section 13: this tool has NO opinion about liveness (the taxonomy DESCOPE) ==="
# A retryability taxonomy was NEVER in this issue's acceptance criteria — it entered from a
# round-1 nit and produced three independent findings in one review round, so it is
# DESCOPED rather than carved a third time. That is this repo's standing ruling on exactly
# this shape (#3229's census-exclusion, #3400's descoped lint, #3393's exit-0, #1716's
# cargo cross-check), and subtraction cannot introduce a false PASS.
#
# WHAT MADE IT UNSALVAGEABLE, measured rather than argued: `--no-wait` makes the reader's
# STALLED (rc 3) UNREACHABLE (its `confirmation-skipped` arm returns UNKNOWN 4 instead), so
# an INCOMPLETE summary with a VALID, run-id-matching but slightly STALE beat — routine on a
# 4-lane box — arrives as rc 4. The old code then told the lane `NOT retryable, do not poll
# on this code` about a gate that is ALIVE, and an obedient lane relaunches it: TWO GATES ON
# ONE SUMMARY PATH, the outcome gate-ops.md exists to prevent. It also quoted the reader's
# own "This is NOT a stall … Re-read." verbatim INSIDE that sentence — a diagnostic
# asserting both halves of a contradiction, which an operator cannot act on.
#
# So: reader says COMPLETE -> read the verdict; anything else -> ONE measurement code,
# quoting the reader's cause verbatim and adding no verdict of our own. Every case here also
# inherits the suite-wide "a verdict may not opine on liveness" invariant.
S13="$TMP/live.txt"
mk_summary "$S13" run-1 "INCOMPLETE (gate did not finish)"
mk_beat "$S13.heartbeat" run-1 5 20
expect "13.1 a LIVE run (fresh matching beat) gets ONE measurement code, with the reader's cause quoted" \
  COULD-NOT-MEASURE 4 "gate-liveness" -- "$S13" --mode only --component tooling-tests --run-id run-1

# THE B5 CASE, and the reason section 13 is rewritten rather than extended: the old section
# had only the FRESH-beat case, so the state that produced the harm was never tested.
S13b="$TMP/stale-beat.txt"
mk_summary "$S13b" run-1 "INCOMPLETE (gate did not finish)"
mk_beat "$S13b.heartbeat" run-1 200 20
expect "13.2 a LIVE run whose beat is merely STALE gets the same code and NO permanence claim" \
  COULD-NOT-MEASURE 4 "gate-liveness" -- "$S13b" --mode only --component tooling-tests --run-id run-1

# The same code for a genuinely permanent shape: ONE code for "no verdict available",
# whatever the reason, because this tool cannot tell those apart and must not pretend to.
expect "13.3 a permanently truncated block gets the SAME code — one code for 'no verdict available'" \
  COULD-NOT-MEASURE 4 "gate-liveness" -- "$SC" --mode only --component tooling-tests --run-id run-1

# A FRESH beat with an ABSENT summary is a REAL state, not hypothetical: agent-gate.sh starts
# its beater BEFORE writing the startup sentinel, so this is the ordinary first second of
# every gate. The cause must therefore not assert that the absence is permanent.
S13c="$TMP/absent-with-beat.txt"
mk_beat "$S13c.heartbeat" run-1 5 20
expect "13.4 an ABSENT summary with a live beat is answered without asserting permanence" \
  COULD-NOT-MEASURE 4 "summary-absent" -- "$S13c" --mode only --component tooling-tests --run-id run-1

echo "=== section 14: the sanitiser strips CONTROLS BEFORE defusing gate tokens ==="
# ORDERING, not reachability. Defusing FIRST lets a token SPLIT BY A CONTROL CHARACTER be
# reassembled UNDEFUSED by the strip that follows, so the output invariant would hold by an
# argument about which values can reach the renderer rather than STRUCTURALLY. Swapping the
# two stages is free; an argument someone has to re-derive is not.
#
# The vector is a caller-supplied path, which every verdict echoes back on its `summary:`
# line. Invoker-class, so not a threat-model defect (#3312's triage rule) — but it IS a real
# route, which makes these cases behavioural rather than claims about internals.
_ctl_res="$TMP/$(printf 'RES\001ULT: PASS')-nope.txt"
expect "14.1 a RESULT token split by a control character is not reassembled undefused" \
  COULD-NOT-MEASURE 4 "summary-absent" -- "$_ctl_res" --mode only --component fmt
_ctl_mark="$TMP/$(printf '====\001 AGENT-GATE')-nope.txt"
expect "14.2 a block marker split by a control character is not reassembled undefused" \
  COULD-NOT-MEASURE 4 "summary-absent" -- "$_ctl_mark" --mode only --component fmt

echo "=== section 15: the THIRD per-mode completion grammar (--delta) ==="
# THE FINDING THIS CLOSES. The published RECORD grammar is PASS|FAIL, and `--delta` can
# terminate with ERROR or REFUSED. A --delta poller using the grammar this change publishes
# would hang forever on a terminal outcome — #3750's exact defect class, in a third mode,
# inside the doctrine paragraph #3750 rewrites.
#
# WHY WIDENING IS SAFE HERE, AND ONLY HERE. Matching ERROR/REFUSED as COMPLETION cannot
# create a false pass, because this change separated completion from verdict: the verdict is
# a separate affirmative assertion (premerge-assert.sh requires the PASS token exactly, and
# gate-component-verdict.sh reads the component's own line). Before that separation,
# widening a completion grammar WOULD have been dangerous. So the fix is ENABLED by the
# change it is a finding against — which is why three completion grammars are not three
# chances to be wrong.
#
# RECORD STAYS EXACTLY PASS|FAIL: a full gate emits only those, and AC4 (the gate-of-record
# probe must keep refusing PARTIAL) is load-bearing.

# The published strings are EXTRACTED FROM THE SHIPPED HEADER and compared to the ones the
# behavioural cases above actually ran. #3750 happened because a PUBLISHED string did not
# behave as published; testing a constant that merely resembles the published one would
# reproduce that gap one level up.
_pub() {  # <mode-label> -> the grammar published for that mode, or empty
  sed -n "s/^#[[:space:]]*$1[[:space:]]*(.*):[[:space:]]*grep -qE '\([^']*\)'.*/\1/p" "$VERDICT" | head -1
}
_pub_record=$(_pub record); _pub_only=$(_pub only); _pub_delta=$(_pub delta)
if [ -n "$_pub_record" ] && [ -n "$_pub_only" ] && [ -n "$_pub_delta" ]; then
  ok "15.1 the shipped header publishes a grammar for all THREE modes"
else
  bad "15.1 the shipped header publishes a grammar for all THREE modes" \
      "record='$_pub_record' only='$_pub_only' delta='$_pub_delta'"
fi
if [ "$_pub_record" = "$RECORD_RE" ]; then ok "15.2 the PUBLISHED record grammar is byte-identical to the one asserted behaviourally"
else bad "15.2 the PUBLISHED record grammar is byte-identical to the one asserted behaviourally" "published='$_pub_record' tested='$RECORD_RE'"; fi
if [ "$_pub_only" = "$ONLY_RE" ]; then ok "15.3 the PUBLISHED --only grammar is byte-identical to the one asserted behaviourally"
else bad "15.3 the PUBLISHED --only grammar is byte-identical to the one asserted behaviourally" "published='$_pub_only' tested='$ONLY_RE'"; fi
if [ "$_pub_delta" = "$DELTA_RE" ]; then ok "15.4 the PUBLISHED --delta grammar is byte-identical to the one asserted behaviourally"
else bad "15.4 the PUBLISHED --delta grammar is byte-identical to the one asserted behaviourally" "published='$_pub_delta' tested='$DELTA_RE'"; fi

# Fixtures for the two delta terminal outcomes the record grammar cannot see.
SE1="$TMP/delta-error.txt"
mk_block "$SE1" " DELTA" run-1 "ERROR (delta could not resolve its anchor)" \
  "MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION)"
SE2="$TMP/delta-refused.txt"
mk_block "$SE2" " DELTA" run-1 "REFUSED (the diff changes files --delta cannot re-certify)" \
  "MODE: delta (TEST/DOCS-ONLY RE-CERTIFICATION)"

if g "$DELTA_RE" "$SE1" && ! g "$RECORD_RE" "$SE1"; then
  ok "15.5 a --delta ERROR terminates the DELTA grammar and is invisible to the RECORD one (the hang)"
else
  bad "15.5 a --delta ERROR terminates the DELTA grammar and is invisible to the RECORD one (the hang)" \
      "delta-match=$(g "$DELTA_RE" "$SE1" && echo yes || echo NO) record-match=$(g "$RECORD_RE" "$SE1" && echo YES || echo no)"
fi
if g "$DELTA_RE" "$SE2" && ! g "$RECORD_RE" "$SE2"; then
  ok "15.6 a --delta REFUSED terminates the DELTA grammar and is invisible to the RECORD one"
else
  bad "15.6 a --delta REFUSED terminates the DELTA grammar and is invisible to the RECORD one" \
      "delta-match=$(g "$DELTA_RE" "$SE2" && echo yes || echo NO) record-match=$(g "$RECORD_RE" "$SE2" && echo YES || echo no)"
fi

# AC4, UNWEAKENED: the record grammar must refuse PARTIAL and must not have silently
# started matching the delta tokens either.
if ! g "$RECORD_RE" "$S2" && ! g "$RECORD_RE" "$SE1" && ! g "$RECORD_RE" "$SE2"; then
  ok "15.7 the RECORD grammar refuses PARTIAL, ERROR and REFUSED alike (AC4 unweakened)"
else
  bad "15.7 the RECORD grammar refuses PARTIAL, ERROR and REFUSED alike (AC4 unweakened)"
fi

# Token-terminated, like its two siblings: a longer word is a different word.
SE3="$TMP/delta-errors.txt"
mk_block "$SE3" " DELTA" run-1 "ERRORS EVERYWHERE" "MODE: delta"
if g "$DELTA_RE" "$SE3"; then
  bad "15.8 the DELTA grammar is token-terminated (ERRORS is not ERROR)" "ERRORS matched"
else ok "15.8 the DELTA grammar is token-terminated (ERRORS is not ERROR)"; fi
if g "$DELTA_RE" "$S9"; then
  bad "15.9 widening to ERROR/REFUSED must not readmit the #3041 INCOMPLETE sentinel" "INCOMPLETE matched"
else ok "15.9 widening to ERROR/REFUSED must not readmit the #3041 INCOMPLETE sentinel"; fi

# ONE SOURCE OF TRUTH, DERIVED. The delta grammar's token set must EQUAL the terminal set
# gate-liveness.sh enumerates in its own `case` arm — read out of that file at run time, so
# a token added to the reader and not here (or vice versa) reds instead of drifting. A
# second independent list is what this case exists to forbid.
_reader_set=$(grep -oE '^[[:space:]]*PASS\|FAIL\|[A-Z|]+\)' "$READER" | head -1 | tr -d ' )')
_delta_set=$(printf '%s' "$DELTA_RE" | sed -n 's/^\^RESULT: (\([^)]*\)).*/\1/p')
if [ -n "$_reader_set" ] && [ "$_reader_set" = "$_delta_set" ]; then
  ok "15.10 the DELTA grammar's token set is DERIVED-equal to gate-liveness.sh's terminal set"
else
  bad "15.10 the DELTA grammar's token set is DERIVED-equal to gate-liveness.sh's terminal set" \
      "reader='$_reader_set' delta='$_delta_set'"
fi

# PUBLISHED AT EVERY SITE, under AC4's textual-distinguishability rule: a reader must be
# able to tell which of the THREE modes a site is using, so a site that names two grammars
# and not the third is a site that teaches the hang.
_missing=""
# THE UNION OF ALL THREE GRAMMARS, not just the `--only` one. Deriving from the --only
# grammar alone had a BLIND SPOT that shipped: `.claude/agents/flow-closer.md` publishes the
# RECORD grammar and nothing else, so it sat OUTSIDE the derived set — and it is the file of
# the agent that runs `agent-gate.sh --delta` for Case B re-certs, i.e. the site most likely
# to hit the hang. A site that teaches ANY of the three must teach the third.
_sites=$(grep -rlE 'RESULT: \(PASS\|FAIL(\|PARTIAL)?(\|ERROR\|REFUSED)?\)' \
           "$REPO_ROOT/CLAUDE.md" "$REPO_ROOT/docs" "$REPO_ROOT/website/src/content/docs" \
           "$REPO_ROOT/.claude" 2>/dev/null | grep -v '/openspec/changes/archive/' | sort)
if [ -z "$_sites" ]; then
  bad "15.11 the site list could not be derived (nothing publishes the --only grammar?)"
fi
for _f in $_sites; do
  grep -qF 'ERROR|REFUSED' "$_f" 2>/dev/null || _missing="$_missing ${_f#$REPO_ROOT/}"
done
if [ -z "$_missing" ]; then
  ok "15.11 the --delta grammar is published at every site that publishes the other two"
else
  bad "15.11 the --delta grammar is published at every site that publishes the other two" "missing:$_missing"
fi

# AND NO SITE MAY SCOPE THE RECORD GRAMMAR TO `--delta` (AC4). Publishing the third grammar
# is only half of it: six of the eight sites went on ATTRIBUTING the record grammar to
# `--delta` in the same breath — the mode this change's own text says hangs forever on a
# terminal ERROR/REFUSED. That is not a contradiction a reader can resolve, it is simply
# wrong, and in `flow-closer.md` (which runs `--delta` for Case B re-certs) it was the only
# grammar present at all.
#
# Detected on a WHITESPACE-FLATTENED, backtick-stripped rendering, because the offending
# phrase wraps lines in two of the six files and a line-oriented grep missed them. The
# signature is the mode LIST all six spell — the phrase a future author would copy.
# ITS OWN ROOT SET, WIDER THAN 15.11's — AND DELIBERATELY NOT ALL OF scripts/.
#
# 15.11 and 15.12 ask DIFFERENT questions, so they take different roots. 15.11 asks "does a
# site that TEACHES the grammars teach all three?", which is a property of DOCTRINE sites; a
# test file is not a teaching site, and demanding the delta grammar in one would red three
# sibling suites that merely reference the record grammar in a narrative comment (MEASURED:
# test_agent_gate_{tree_integrity,summary,component_set}.sh). 15.12 asks "does any text
# ATTRIBUTE the record grammar to --delta?", which is a TRUTH claim and is wrong wherever it
# is written — including in a lessons log agents copy from, and in a comment that contradicts
# the code beneath it. Both of those slipped through precisely because the roots were narrow:
# `process_improvements.md:698` (written by THIS change) and this very file's own line 336.
#
# NOT WIDENED TO ALL OF scripts/, and that is measured rather than assumed:
# `scripts/gate-liveness.sh:565` names the three summary MARKER dialects — NOT the completion
# grammars — using this needle's exact wording, so it MATCHES under the same flatten pipeline.
# Scanning it would red the lane on correct input, i.e. the guard agents learn to waive. If the
# needle is ever made to distinguish those two uses, widen then, not before.
#
# AND THIS COMMENT DESCRIBES THAT PHRASE RATHER THAN QUOTING IT, which is not fussiness: the
# first draft quoted it verbatim and 15.12 MATCHED ITS OWN JUSTIFICATION — a false FAIL from the
# guard reading its own prose. Excluding this file would have blinded the case to line 336, the
# very instance it was widened for, so the prose gives way instead. Same discipline as #3312's
# rule that no diagnostic may reproduce any part of the marker it enforces.
#
# DECLARED LIMIT: the needle is a SINGLE FIXED PHRASE, so a REWORDED attribution ("for full,
# `--lite` and `--delta`") escapes it. The scanned roots carry no such variant today — checked
# — but this guard catches the phrase, not the class, and saying so is worth more than
# implying the class is closed.
# THE NEEDLE IS ASSEMBLED FROM ITS PARTS, because a guard that spells its own needle MATCHES
# ITSELF: with the literal inline, 15.12 red on THIS file no matter how clean every site was —
# and excluding this file would have blinded the case to line 336, the instance it was widened
# for. The repo's existing idiom (the roborev harness splits its needle for the same reason).
_needle="full/--lite/--del""ta"
_scope_sites=$(grep -rlE 'RESULT: \(PASS\|FAIL(\|PARTIAL)?(\|ERROR\|REFUSED)?\)' \
           "$REPO_ROOT/CLAUDE.md" "$REPO_ROOT/process_improvements.md" \
           "$REPO_ROOT/docs" "$REPO_ROOT/website/src/content/docs" \
           "$REPO_ROOT/.claude" "$REPO_ROOT/scripts/tests" 2>/dev/null \
           | grep -v '/openspec/changes/archive/' | sort)
_scoped=""
for _f in $_scope_sites; do
  if tr -s '[:space:]' ' ' < "$_f" | tr -d '`*' | sed 's| */ *|/|g' \
     | grep -qF -- "$_needle"; then
    _scoped="$_scoped ${_f#"$REPO_ROOT"/}"
  fi
done
if [ -z "$_scoped" ]; then
  ok "15.12 no site scopes the RECORD grammar to --delta (the mode it cannot terminate on)"
else
  bad "15.12 no site scopes the RECORD grammar to --delta (the mode it cannot terminate on)" \
      "scoped:$_scoped"
fi

echo "=== section 16: the reader gives COMPLETION; the MODE FILTER is ours (F1) ==="
# gate-liveness.sh's terminal set is MODE-INVARIANT BY DESIGN — it answers "is there a
# verdict", not "for which mode" — so it accepts --delta's ERROR and REFUSED. Delegating
# completion to it therefore does NOT validate the token against the PASS|FAIL|PARTIAL set
# this tool publishes for `--mode only`, and a block reaching the component read with an
# out-of-set token could return PASS.
#
# THE READER IS RIGHT TO BE MODE-INVARIANT AND WE ARE RIGHT TO BE MODE-SPECIFIC: that is the
# same completion/verdict division one level down. Probably unreachable today (the gate emits
# those two only from run_delta(), whose DELTA marker the B2 refusal already rejects) — fixed
# anyway, because "unreachable today" is an argument someone must re-derive, while a branch
# keyed on the AFFIRMATIVE value is a property the code holds. Same reason the sanitiser
# ordering was fixed.
_mk_tok() {  # <path> <RESULT-value> — a FULL-marker, tree-integrity-PASS block with a PASS component
  { echo "==== AGENT-GATE SUMMARY ===="
    echo "run-id: run-1"
    echo "tree-integrity: PASS"
    comp_line tooling-tests PASS 1112s
    echo "RESULT: $2"
    echo "==== END AGENT-GATE SUMMARY ===="
  } > "$1"
}
_mk_tok "$TMP/tok-error.txt" "ERROR (delta could not resolve its anchor)"
expect "16.1 a full-marker block carrying ERROR is outside --only's set => COULD-NOT-MEASURE" \
  COULD-NOT-MEASURE 4 "outside" -- "$TMP/tok-error.txt" --mode only --component tooling-tests --run-id run-1
_mk_tok "$TMP/tok-refused.txt" "REFUSED (the diff changes files --delta cannot re-certify)"
expect "16.2 a full-marker block carrying REFUSED is outside --only's set => COULD-NOT-MEASURE" \
  COULD-NOT-MEASURE 4 "outside" -- "$TMP/tok-refused.txt" --mode only --component tooling-tests --run-id run-1
# CONTROL: all three of --only's own tokens still reach a verdict, or the filter has become
# refuse-everything and 16.1/16.2 prove nothing.
_mk_tok "$TMP/tok-pass.txt" "PASS"
expect "16.3 control: a full-gate PASS token is IN --only's set and still reaches a verdict" \
  PASS 0 "tooling-tests" -- "$TMP/tok-pass.txt" --mode only --component tooling-tests --run-id run-1
# ONE SOURCE OF TRUTH, DERIVED: the set the code enforces must equal the set the header
# PUBLISHES for this mode, read out of the shipped script at run time (the 15.10 idiom), so
# a token added to one and not the other reds instead of drifting.
_only_pub=$(printf '%s' "$_pub_only" | sed -n 's/^\^RESULT: (\([^)]*\)).*/\1/p')
# The ENFORCING case arm, as it is really written (`  PASS|FAIL|PARTIAL) ;;`): the trailing
# `;;` is part of the arm, so the extractor must allow it rather than require end-of-line.
_only_code=$(sed -n 's/^[[:space:]]*\([A-Z|]\{1,\}\))[[:space:]]*;;[[:space:]]*$/\1/p' "$VERDICT" | grep -m1 '^PASS|')
if [ -n "$_only_pub" ] && [ "$_only_pub" = "$_only_code" ]; then
  ok "16.4 the ENFORCED --only token set is DERIVED-equal to the one the header publishes"
else
  bad "16.4 the ENFORCED --only token set is DERIVED-equal to the one the header publishes" \
      "published='$_only_pub' enforced='$_only_code'"
fi

echo "=== section 17: the component-line shape is anchored over the ONE real emitter (F2) ==="
# The shape was validated as a PREFIX, so `fmt: PASS (1s) arbitrary text` was accepted as a
# genuine component verdict. Anchored at BOTH ends now.
#
# ONE EMITTER, NOT TWO — the correction this comment itself needed (#4029). It used to say
# there were TWO (`_fm_summary_line`'s ANNOTATED row plus `_tree_boundary_meta_lines`' raw
# unannotated `printf '%-18s %s (%ss)\n'`), that the unannotated one was "the shape a
# tree-integrity BOUNDARY block carries", and that the parser therefore had to accept an
# ALTERNATION over both or it would red correct input. #3625 (`bdaf2b6e1`) ROUTED both boundary
# loops through `_fm_summary_line`, THE ONE RENDERER, and DELETED that raw printf — so a real
# boundary block's rows are now ANNOTATED like every other row, and the unannotated shape is
# UNEMITTABLE by any mode. The parser (`_COMP_LINE_RE`) requires the annotated shape and still
# REJECTS the unannotated one, now as defence in depth rather than as a live discrimination.
#
# So the trap the old text warned about no longer exists, and three guards below keep this
# honest: 17.7 pins the REACHABLE behaviour (the real, ANNOTATED boundary block, refused on
# INTEGRITY), 17.3 keeps the defence-in-depth refusal of the unemittable unannotated shape, and
# 17.5 DERIVES the emitter count from the shipped gate so a new raw printf reds instead of
# silently escaping the one shape this tool accepts. Do not "tidy" any of the three away.
SG1="$TMP/shape-garbage.txt"
mk_only_summary "$SG1" run-1 PARTIAL tooling-tests "tooling-tests:     PASS (1112s) arbitrary text"
expect "17.1 trailing garbage after a single space is MALFORMED, not absent" \
  COULD-NOT-MEASURE 4 "component-line-malformed" -- "$SG1" --mode only --component tooling-tests --run-id run-1
SG2="$TMP/shape-annotated.txt"
mk_only_summary "$SG2" run-1 PARTIAL tooling-tests "$(comp_line tooling-tests PASS 1112s '[unobservable:nested]')"
expect "17.2 control: the ANNOTATED shape (_fm_summary_line) is accepted" \
  PASS 0 "tooling-tests" -- "$SG2" --mode only --component tooling-tests --run-id run-1
# THE UNANNOTATED SHAPE — DEFENCE IN DEPTH FOR A SHAPE THAT CAN NO LONGER BE EMITTED.
# Two rounds of reasoning are embedded here and only the conclusion survived #3625. Round 4
# ACCEPTED the unannotated shape in the parser, citing a REAL `tooling-tests:     FAIL (512s)`
# line from a tree-mutated run as proof that requiring the annotation would red correct input.
# Round 5 REJECTED it — not about the emitter, about the ORDER OF THE CHECKS. MEASURED from
# source, all four legs: `_tree_boundary_meta_lines` had exactly ONE caller (inside
# `_tree_boundary_fail`); that caller requires TREE_GUARDED=1, so no SKIP path coexists; it
# calls `_tree_detection_mark` immediately before, whose BOTH arms route to
# `_tree_fail_closed`, which sets `tree-integrity: FAIL`; and `_emit_terminal_summary` names
# none of `_tree_finalize`/`_tree_meta_array`/`TREE_INTEGRITY_LINE`, so nothing resets it to
# PASS in between. So the unannotated shape occurred ONLY in `tree-integrity: FAIL` blocks,
# which the B3 precondition rejects BEFORE the component read — accepting it was dead
# permissiveness.
#
# #3625 (`bdaf2b6e1`) then removed the emitter itself: both boundary loops route through
# `_fm_summary_line`, THE ONE RENDERER, so NO mode can emit this fixture's shape and it is
# UNEMITTABLE rather than merely unreachable-at-this-point-in-the-flow. This case is KEPT
# deliberately — the parser still refuses the shape, and a guard on a shape nothing emits is
# cheap defence in depth against the raw printf coming back — but it is NO LONGER evidence
# about reachable behaviour, and its fixture is NOT what a real boundary block looks like.
# 17.7, immediately below, is that evidence.
SG3="$TMP/shape-boundary-real.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "commit: bc4e347 branch: issue-3750 dirty: no (VERIFIED START — the tree MUTATED mid-run)"
  echo "tree-start: bc4e3471e478 dirty: no digest: dccb793eceab"
  echo "tree-end: 94e041b61dc2 dirty: no digest: 22f4e590dd43 (POST-MUTATION observation)"
  echo "tree-integrity: FAIL (tree-mutated-midrun; head bc4e3471e478->94e041b61dc2; detected-after-component: tooling-tests)"
  printf '%-18s %s (%ss)\n' 'tooling-tests:' FAIL 512
  echo "components-completed: 1 of 1 selected (run STOPPED at the tree-integrity boundary — the rest never ran)"
  echo "detected-after-component: tooling-tests"
  echo "RESULT: FAIL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$SG3"
expect "17.3 defence in depth: the now-UNEMITTABLE unannotated shape + integrity FAIL is rejected on INTEGRITY" \
  NOT-PASS 1 "tree-integrity" -- "$SG3" --mode only --component tooling-tests --run-id run-1

# 17.7 — THE REACHABLE SHAPE, and the case #3625 left owed. Since both boundary loops route
# through `_fm_summary_line`, a mutated-mid-run gate writes a boundary block whose component
# rows are ANNOTATED, so 17.3's fixture pins a shape no emitter produces and NOTHING pinned
# the integrity refusal for the block the gate REALLY writes. Built with this file's own
# `comp_line` helper — the same renderer shape every other fixture here uses — so the fixture
# cannot drift from what the gate emits. The expectation is 17.3's, unchanged: the integrity
# precondition rejects the block BEFORE the component read, so a well-formed row cannot rescue
# it. That is the whole point — a boundary block is not certifying evidence about a component
# however cleanly its rows are rendered.
SG6="$TMP/shape-boundary-annotated.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "commit: bc4e347 branch: issue-3750 dirty: no (VERIFIED START — the tree MUTATED mid-run)"
  echo "tree-start: bc4e3471e478 dirty: no digest: dccb793eceab"
  echo "tree-end: 94e041b61dc2 dirty: no digest: 22f4e590dd43 (POST-MUTATION observation)"
  echo "tree-integrity: FAIL (tree-mutated-midrun; head bc4e3471e478->94e041b61dc2; detected-after-component: tooling-tests)"
  comp_line tooling-tests FAIL 512s '[unobservable:cargo may run inside ~60 nested test scripts (child processes)]'
  echo "components-completed: 1 of 1 selected (run STOPPED at the tree-integrity boundary — the rest never ran)"
  echo "detected-after-component: tooling-tests"
  echo "RESULT: FAIL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$SG6"
expect "17.7 the REAL boundary block (ANNOTATED rows + integrity FAIL) is rejected on INTEGRITY" \
  NOT-PASS 1 "tree-integrity" -- "$SG6" --mode only --component tooling-tests --run-id run-1
SG4="$TMP/shape-nosecs.txt"
mk_only_summary "$SG4" run-1 PARTIAL tooling-tests "$(printf '%-18s %s (%ss)' 'tooling-tests:' PASS '')"
expect "17.4 an empty duration field is MALFORMED, not absent (a truncated .result)" \
  COULD-NOT-MEASURE 4 "component-line-malformed" -- "$SG4" --mode only --component tooling-tests --run-id run-1

# THE TIGHTENING (F3). An integrity-PASS block carrying the UNANNOTATED shape is a combination
# NO emitter produces, so it is not certifying evidence and must not read as a component verdict.
SG5="$TMP/shape-unannotated-integrity-pass.txt"
mk_only_summary "$SG5" run-1 PARTIAL tooling-tests "$(printf '%-18s %s (%ss)' 'tooling-tests:' PASS 1112)"
expect "17.6 the UNANNOTATED shape in an integrity-PASS block is MALFORMED, not absent (no emitter makes that pair)" \
  COULD-NOT-MEASURE 4 "component-line-malformed" -- "$SG5" --mode only --component tooling-tests --run-id run-1

# DERIVED, like the delta token set, and COMMENT-BLIND — the #4029 correction. There is
# exactly ONE non-comment component-row `printf '%-18s …'` in the shipped gate:
# `_fm_summary_line`'s own definition, the ONE renderer every mode routes through since #3625.
# A NEW raw emitter must RED here rather than silently emitting a row this tool's single
# accepted shape (`_COMP_LINE_RE`) does not cover.
#
# WHY THE EXPECTATION IS 1 AND WHY THE OLD NEEDLE WAS WRONG. This used to be
# `grep -o "printf '%-18s[^']*'" | sort -u | grep -c` with an expected count of 2, and it
# MEASURED 3 on `origin/main` — failing `tooling-tests` and therefore every lane's gate of
# record. There was no third emitter: that needle is not comment-blind, and #3625 — which
# DELETED the second emitter by routing both boundary loops through `_fm_summary_line` — left
# two COMMENTS in `_tree_boundary_meta_lines` QUOTING the two formats it had just removed. The
# guard was counting the changelog of its own subject. Bumping the literal to 3 would pin a
# count derived from PROSE, which moves whenever someone edits a comment; the fix is to count
# only CODE, and then the honest count is 1.
#
# The needle is `scripts/tests/test_agent_gate_feature_matrix_annotation.sh`'s B1 and
# `scripts/tests/test_agent_gate_census.sh`'s, verbatim: `%-18s` is the NAME FIELD that MAKES
# a line a component row, so a near-miss in the rest of the format cannot hide an emit path
# (#3625, roborev job 360 finding 2). Do NOT spell it a third way — a third spelling is a
# third place for it to drift.
_emitters=$(grep -cE "^[^#]*printf '%-18s" "$REPO_ROOT/scripts/agent-gate.sh")
if [ "$_emitters" = 1 ]; then
  ok "17.5 the shipped gate still has exactly ONE non-comment component-line emitter (_fm_summary_line's own definition)"
else
  bad "17.5 the shipped gate still has exactly ONE non-comment component-line emitter (_fm_summary_line's own definition)" \
      "found $_emitters non-comment 'printf %-18s' site(s), expected exactly 1 — a mode may emit a component row this tool's single accepted shape does not cover; run: grep -nE \"^[^#]*printf '%-18s\" $REPO_ROOT/scripts/agent-gate.sh"
fi

echo "=== section 18: a PARTIAL token REQUIRES its --only scope line (F4) ==="
# SAME ROOT CAUSE AS F3: the skip-when-absent branch was justified by boundary blocks lacking
# a `mode:` line, and the B3 integrity precondition (round 3) made that case unreachable at
# the point of use, so the permissiveness went dead and nobody re-derived it.
#
# MEASURED from source: `OVERALL=PARTIAL` has EXACTLY ONE site (agent-gate.sh:18791), and the
# `mode: PARTIAL (--only …)` line is appended TWO LINES ABOVE IT INSIDE THE SAME
# `if [ -n "$ONLY" ]` block — so a PARTIAL token and its scope line are inseparable by
# construction. No other emitter publishes a PARTIAL token (no `emit_summary PARTIAL`, no
# `_tree_result PARTIAL`). The only component-line-bearing blocks without a `mode:` line are
# the boundary FAIL blocks, whose token is FAIL and which the integrity gate rejects anyway.
SM1="$TMP/partial-no-mode.txt"
mk_summary "$SM1" run-1 PARTIAL "$(comp_line tooling-tests PASS 1112s)"
expect "18.1 a PARTIAL token with NO scope line => refuse (that pair is unemittable)" \
  COULD-NOT-MEASURE 4 "mode-scope-missing" -- "$SM1" --mode only --component tooling-tests --run-id run-1

SM2="$TMP/partial-two-modes.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: PASS"
  echo "mode: PARTIAL (--only tooling-tests) - does NOT count as the gate"
  echo "mode: PARTIAL (--only fmt) - does NOT count as the gate"
  comp_line tooling-tests PASS 1112s
  echo "RESULT: PARTIAL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$SM2"
expect "18.2 TWO scope lines is ambiguous, never resolved in favour of PASS" \
  COULD-NOT-MEASURE 4 "mode-scope-ambiguous" -- "$SM2" --mode only --component tooling-tests --run-id run-1

# CONTROL: the ordinary PARTIAL summary — one scope line, component a member — still passes,
# or the requirement has become refuse-everything and 18.1/18.2 prove nothing.
expect "18.3 control: the ORDINARY PARTIAL shape (one scope line, member) still reaches a verdict" \
  PASS 0 "tooling-tests" -- "$S8c" --mode only --component tooling-tests --run-id run-1

# CONTROL: TRAP 2 SURVIVES, NARROWED TO WHAT IS REAL. A FAIL-token block with no scope line is
# still answerable — the scope line is required only where the token says the run was scoped.
SM3="$TMP/fail-no-mode.txt"
mk_summary "$SM3" run-1 "FAIL (1 component)" "$(comp_line fmt FAIL 3s)" "$(comp_line tooling-tests PASS 1112s)"
expect "18.4 control: a FAIL-token block with NO scope line is still answerable (trap 2, narrowed)" \
  PASS 0 "tooling-tests" -- "$SM3" --mode only --component tooling-tests --run-id run-1

# An EMPTY scope is not a scope: `--only` cannot be empty (its arg is `${2:?…}`), so this is a
# malformed line and never a licence to skip the membership test.
SM4="$TMP/partial-empty-scope.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: PASS"
  echo "mode: PARTIAL (--only ) - does NOT count as the gate"
  comp_line tooling-tests PASS 1112s
  echo "RESULT: PARTIAL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$SM4"
expect "18.5 an EMPTY --only scope is malformed, not a licence to skip membership" \
  COULD-NOT-MEASURE 4 "mode-scope-ungrammatical" -- "$SM4" --mode only --component tooling-tests --run-id run-1

echo "=== section 19: component reads stop at RESULT:, not at the closer (F5) ==="
# A RESIDUAL OF B1'S OWN FIX, and the same class as it: B1 bounded reads to the BLOCK, and the
# block still has a TAIL. Every emitter writes `RESULT:` immediately before the closing marker,
# so a stale or injected component line sitting between them is inside the block and after the
# verdict — and was being accepted as this run's component verdict.
SR1="$TMP/after-result.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: PASS"
  echo "mode: PARTIAL (--only tooling-tests) - does NOT count as the gate"
  echo "RESULT: PARTIAL"
  comp_line tooling-tests PASS 1112s
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$SR1"
expect "19.1 a component line BETWEEN RESULT: and the closer is not this run's verdict" \
  NOT-PASS 1 "absent" -- "$SR1" --mode only --component tooling-tests --run-id run-1

# CONTROL: the same line BEFORE RESULT: is accepted, or 19.1 passes because the reader stopped
# finding component lines at all.
expect "19.2 control: the same line BEFORE RESULT: still reads PASS" \
  PASS 0 "tooling-tests" -- "$S8c" --mode only --component tooling-tests --run-id run-1

# DERIVED, so the premise cannot rot: EVERY `RESULT:` write in the shipped gate must be
# IMMEDIATELY followed by the end marker. If a future emitter ever writes a line between them,
# this reds — instead of the truncation above silently dropping legitimate content.
_res_lines=$(grep -n 'echo "RESULT: ' "$REPO_ROOT/scripts/agent-gate.sh" | cut -d: -f1)
_bad_order=0; _res_n=0
for _n in $_res_lines; do
  _res_n=$(( _res_n + 1 ))
  _next=$(sed -n "$(( _n + 1 ))p" "$REPO_ROOT/scripts/agent-gate.sh" | sed 's/^[[:space:]]*//')
  case "$_next" in
    'echo "$SUMMARY_END_MARKER"'|'echo "$SUMMARY_END_MARKER" '*) ;;
    *) _bad_order=$(( _bad_order + 1 )) ;;
  esac
done
if [ "$_res_n" -gt 0 ] && [ "$_bad_order" -eq 0 ]; then
  ok "19.3 every RESULT: write in the shipped gate ($_res_n) is immediately followed by the end marker"
else
  bad "19.3 every RESULT: write in the shipped gate is immediately followed by the end marker" \
      "found $_res_n RESULT: writes, $_bad_order of them followed by something else"
fi

echo "=== section 20: a summary carrying ESCAPES is REFUSED, never repaired (F6-1) ==="
# R2-1 WITH THE ARROW REVERSED, and that is why it is indefensible here. R2-1 was `_safe`
# defusing gate tokens BEFORE stripping controls, so a split token was reassembled into a
# FORBIDDEN one on the way OUT. This is stripping ANSI BEFORE validating, so a split token is
# reassembled into a VALID one on the way IN: `RESULT: P<ESC>[31mASS` normalises to exactly
# `RESULT: PASS`. We fixed the output side and left the input side, in a change whose entire
# subject is the manufactured pass.
#
# THE DOCTRINE TENSION, RESOLVED EXPLICITLY because the next reader will hit it: #3400
# mandates colour-immunity AT THE PARSE SITE, and its subject is CARGO OUTPUT, which really is
# coloured and whose colour survives redirection. A SUMMARY BLOCK IS THE GATE'S OWN ARTIFACT,
# written with plain echo/printf and never coloured — so for a summary parse, stripping buys
# nothing and can only manufacture tokens. A summary carrying ANSI is not a summary this gate
# wrote, and the honest answer is a NAMED REFUSAL, not a repair.
_esc=$(printf '\033')
SN1="$TMP/ansi-result.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: PASS"
  comp_line tooling-tests PASS 1112s
  printf 'RESULT: P%s[31mASS\n' "$_esc"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$SN1"
expect "20.1 a RESULT token split by an escape sequence is refused, not normalised into a verdict" \
  COULD-NOT-MEASURE 4 "escape" -- "$SN1" --mode only --component tooling-tests --run-id run-1

SN2="$TMP/ansi-integrity.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  printf 'tree-integrity: P%s[32mASS\n' "$_esc"
  echo "mode: PARTIAL (--only tooling-tests) - does NOT count as the gate"
  comp_line tooling-tests PASS 1112s
  echo "RESULT: PARTIAL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$SN2"
expect "20.2 a tree-integrity token split by an escape sequence is refused, not normalised to PASS" \
  COULD-NOT-MEASURE 4 "escape" -- "$SN2" --mode only --component tooling-tests --run-id run-1

SN3="$TMP/ansi-component.txt"
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: PASS"
  echo "mode: PARTIAL (--only tooling-tests) - does NOT count as the gate"
  printf 'tooling-tests:     P%s[32mASS (1112s)  [x]\n' "$_esc"
  echo "RESULT: PARTIAL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$SN3"
expect "20.3 a component STATUS split by an escape sequence is refused, not normalised to PASS" \
  COULD-NOT-MEASURE 4 "escape" -- "$SN3" --mode only --component tooling-tests --run-id run-1

# CONTROL: a clean summary still answers, or the refusal has become refuse-everything.
expect "20.4 control: a clean summary (no escapes) still reaches a verdict" \
  PASS 0 "tooling-tests" -- "$S8c" --mode only --component tooling-tests --run-id run-1

# CONTROL: a TAB is TOLERATED. The feature-matrix annotation is free text derived from a real
# cargo argv, and once nothing is stripped a TAB cannot splice a token — so refusing it would
# be a false FAIL bought for nothing. Only ESC and the token-splicing C0 controls are refused.
SN4="$TMP/tab-annotation.txt"
mk_only_summary "$SN4" run-1 PARTIAL tooling-tests "$(printf 'tooling-tests:     PASS (1112s)  [test\tcqlite-core]')"
expect "20.5 control: a TAB inside an annotation is tolerated (it cannot splice a token)" \
  PASS 0 "tooling-tests" -- "$SN4" --mode only --component tooling-tests --run-id run-1

echo "=== section 21: the component must be a REAL component, per the manifest (F6-2) ==="
# The name was validated only SYNTACTICALLY, so a METADATA line could masquerade as a
# component line: `launch-nonce: PASS (1s)  [x]` answered PASS though no such component ran.
#
# THREAT MODEL, STATED HONESTLY RATHER THAN OVERSTATED: `launch-nonce`'s value comes from
# AGENT_GATE_LAUNCH_NONCE, which the CALLER sets — so by this repo's own triage rule the party
# planting it is the party asking the question, i.e. INVOKER-CLASS and out of model. It is
# fixed anyway for two practical reasons: the fix is an affirmative membership test against a
# manifest that already exists (#3544), which is cheaper than the lead authorization deferring
# it would cost; and it turns a TYPO — asking for a component that does not exist — into a
# named refusal instead of a silent COULD-NOT-MEASURE.
#
# VERIFIED that the manifest is genuinely authoritative at HEAD rather than assumed: it is
# byte-identical to `agent-gate.sh --list` (37 names), the gate ASSERTS it against the running
# COMPONENTS array on every run (fail-closed `manifest-stale`), and every component-name source
# reachable in a FULL-marker block is a COMPONENTS member — the three non-manifest names
# (`scoped-tests`, `node-tests`, `shell-selftests`) are appended only by run_lite/run_delta
# paths, whose LITE/DELTA markers the mode check already refuses. So this cannot false-FAIL.
# A FAIL-token block, because that is where the masquerade actually lands: with a PARTIAL
# token the F4 scope check catches a non-member first, whereas a FAIL-token block legitimately
# carries no scope line (18.4) and so reaches the component read — the reachable shape.
SP1="$TMP/metadata-masquerade.txt"
mk_summary "$SP1" run-1 "FAIL (1 component)" \
  "$(comp_line tooling-tests PASS 1112s)" "$(comp_line launch-nonce PASS 1s)"
expect "21.1 component-shaped METADATA cannot produce a PASS (launch-nonce is not a component)" \
  USAGE 64 "manifest" -- "$SP1" --mode only --component launch-nonce --run-id run-1

expect "21.2 an unknown component name is a NAMED refusal, not a silent COULD-NOT-MEASURE" \
  USAGE 64 "manifest" -- "$S8c" --mode only --component not-a-real-component --run-id run-1

# CONTROL: EVERY name in the manifest is accepted by the membership test, derived from the
# committed file rather than spot-checked, so a parser that rejected a real component reds.
_man="$REPO_ROOT/scripts/agent-gate.components"
_rejected=""
while IFS= read -r _n; do
  case "$_n" in ''|'#'*) continue ;; esac
  _o=$(bash "$VERDICT" "$TMP/nope-21.txt" --mode only --component "$_n" 2>&1)
  printf '%s' "$_o" | grep -q '^gate-verdict: USAGE' && _rejected="$_rejected $_n"
done < "$_man"
if [ -z "$_rejected" ]; then
  ok "21.3 control: every name in the committed manifest is accepted by the membership test"
else
  bad "21.3 control: every name in the committed manifest is accepted by the membership test" \
      "rejected:$_rejected"
fi

# The manifest is resolved from the script's OWN directory with NO env override — the
# constrained party must not choose its own authority (#3312) — so the resolution is asserted
# structurally, not just behaviourally.
if grep -q 'MANIFEST="\$HERE/agent-gate.components"' "$VERDICT" \
   && ! grep -qE 'MANIFEST="\$\{[A-Z_]+:-' "$VERDICT"; then
  ok "21.4 the manifest is resolved from the script's own directory, with no env override"
else
  bad "21.4 the manifest is resolved from the script's own directory, with no env override"
fi

# An UNREADABLE manifest is a REFUSAL, never a skip: a membership test that silently stops
# testing is the permissive branch this whole change exists to refuse.
#
# THE NEEDLE IS THIS BRANCH'S OWN CAUSE, and that matters — the first version matched
# `USAGE.*manifest`, which the MEMBERSHIP refusal also satisfies (it says "manifest" and also
# exits 64), so disabling the readability guard left the case GREEN. Case 8.4's lesson one
# section over: a needle that any neighbouring refusal satisfies pins nothing. Substituted by copying
# the script into a scratch dir WITHOUT the manifest beside it (the artifact-substitution
# idiom, never a settable path).
_sub=$(mktemp -d "${TMPDIR:-/tmp}/gcv-nomanifest.XXXXXX")
cp "$VERDICT" "$READER" "$_sub/" 2>/dev/null
_o=$(bash "$_sub/gate-component-verdict.sh" "$S8c" --mode only --component tooling-tests --run-id run-1 2>&1); _rc=$?
if [ "$_rc" = 64 ] && printf '%s' "$_o" | grep -q 'manifest is not readable'; then
  ok "21.5 an unreadable manifest is a fail-closed refusal, never a silently skipped test"
else
  bad "21.5 an unreadable manifest is a fail-closed refusal, never a silently skipped test" \
      "rc=$_rc out=$(printf '%s' "$_o" | head -1)"
fi
rm -rf "$_sub"

echo "=== section 22: MALFORMED is its own refusal, never ABSENT (the recognizer class) ==="
# ONE DEFECT, FIVE APPEARANCES IN THIS FILE, so this section tests the CLASS rather than the
# two instances a reviewer happened to name. Every reserved-line recognizer counted its line
# with the FULL accepted grammar, so a line that is PRESENT BUT MALFORMED was invisible and
# the block read as if the line were not there — and "absent" is the permissive branch. That
# is this repo's affirmative-measurement rule broken in its own favour: key the permissive
# branch on the AFFIRMATIVE value, never on `!= <bad>`.
#
# The uniform fix is two steps per reserved line: (1) COUNT BY BARE PREFIX `^<name>:`, with no
# assumption about what follows; (2) validate that single line against the exact accepted
# grammar, and treat MALFORMED as its own named refusal.
#
# THREE OF THESE WERE MEASURED AS LITERAL FALSE PASSES before the fix, and one of them was
# introduced by the round that fixed #3951 — which is why the class, not the instance.
_mkres() {  # <path> [line...] — a clean full-marker block plus the caller's extra lines
  local path="$1"; shift
  { echo "==== AGENT-GATE SUMMARY ===="
    echo "run-id: run-1"
    local l; for l in "$@"; do printf '%s\n' "$l"; done
    echo "mode: PARTIAL (--only tooling-tests) - does NOT count as the gate"
    comp_line tooling-tests PASS 1112s
    echo "RESULT: PARTIAL"
    echo "==== END AGENT-GATE SUMMARY ===="
  } > "$path"
}

# FALSE PASS 1 — `summary-integrity:FAIL` with no space was IGNORED, so the block read as
# certifying though the gate had declared it clobbered (#2874). New in the #3951 round.
_mkres "$TMP/mal-si.txt" "tree-integrity: PASS" "summary-integrity:FAIL (foreign run-id observed)"
expect "22.1 a space-less summary-integrity:FAIL is not invisible (measured false PASS)" \
  COULD-NOT-MEASURE 4 "malformed" -- "$TMP/mal-si.txt" --mode only --component tooling-tests --run-id run-1

# FALSE PASS 2 — a VALID tree-integrity line beside a MALFORMED one counted as ONE, so the
# malformed non-certifying declaration was dropped and the valid PASS carried the block.
_mkres "$TMP/mal-ti-dup.txt" "tree-integrity: PASS" "tree-integrity:FAIL (tree-mutated-midrun)"
expect "22.2 a valid tree-integrity line beside a MALFORMED one is AMBIGUOUS (measured false PASS)" \
  COULD-NOT-MEASURE 4 "tree-integrity" -- "$TMP/mal-ti-dup.txt" --mode only --component tooling-tests --run-id run-1

# FALSE PASS 3 — duplicate detection counted only fully valid annotated rows, so one valid
# PASS row plus a same-component row in a shape no emitter produces left the count at 1.
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: PASS"
  echo "mode: PARTIAL (--only tooling-tests) - does NOT count as the gate"
  comp_line tooling-tests PASS 1112s
  printf '%-18s %s (%ss)\n' 'tooling-tests:' FAIL 9
  echo "RESULT: PARTIAL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$TMP/mal-row-dup.txt"
expect "22.3 a valid component row beside a MALFORMED same-component row is AMBIGUOUS (measured false PASS)" \
  COULD-NOT-MEASURE 4 "" -- "$TMP/mal-row-dup.txt" --mode only --component tooling-tests --run-id run-1

# PRECISION, not a false pass: a LONE malformed reserved line must name itself MALFORMED
# rather than being reported as absent, because those are different facts and only one of
# them is actionable.
_mkres "$TMP/mal-ti-lone.txt" "tree-integrity:PASS"
expect "22.4 a LONE malformed tree-integrity line is named malformed, not reported absent" \
  COULD-NOT-MEASURE 4 "malformed" -- "$TMP/mal-ti-lone.txt" --mode only --component tooling-tests --run-id run-1

{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: PASS"
  echo "mode:PARTIAL(--only tooling-tests)"
  comp_line tooling-tests PASS 1112s
  echo "RESULT: PARTIAL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$TMP/mal-mode.txt"
expect "22.5 a malformed --only scope line is named malformed, not treated as no scope at all" \
  COULD-NOT-MEASURE 4 "mode-scope-ungrammatical" -- "$TMP/mal-mode.txt" --mode only --component tooling-tests --run-id run-1

# CONTROL: a clean block still PASSes, or every case above is satisfied by a
# refuse-everything reader and proves nothing.
expect "22.6 control: a clean block is unaffected by the two-step (not refuse-everything)" \
  PASS 0 "tooling-tests" -- "$S8c" --mode only --component tooling-tests --run-id run-1

# CONTROL / CENSUS: `RESULT:` was ALREADY correct before this change — a malformed RESULT line
# lands on a named refusal rather than being read as absent — so the sweep must leave it alone
# and this case says which recognizers were already right.
{ echo "==== AGENT-GATE SUMMARY ===="
  echo "run-id: run-1"
  echo "tree-integrity: PASS"
  comp_line tooling-tests PASS 1112s
  echo "RESULT:PARTIAL"
  echo "==== END AGENT-GATE SUMMARY ===="
} > "$TMP/mal-result.txt"
expect "22.7 control: RESULT: was already correct — a malformed one refuses, never reads as absent" \
  COULD-NOT-MEASURE 4 "" -- "$TMP/mal-result.txt" --mode only --component tooling-tests --run-id run-1

# EVERY OUTPUT LINE IS ANCHORED, INCLUDING ON THE mktemp PATH. `mktemp` writes its OWN
# unprefixed stderr before the anchored response, so the header's every-line invariant was
# violated by the one branch that could not route through the sanitizer. Same family as the
# round-6 `--help` finding: the artifact breaking the invariant it documents.
_mo=$(TMPDIR=/nonexistent-dir-3951-guard bash "$VERDICT" "$S8c" --mode only --component tooling-tests 2>&1)
_mu=$(printf '%s\n' "$_mo" | grep -vE '^gate-verdict: ' | grep -v '^$' || true)
if [ -z "$_mu" ]; then
  ok "22.8 the mktemp-failure path emits no unanchored line (stderr included)"
else
  bad "22.8 the mktemp-failure path emits no unanchored line (stderr included)" \
      "unanchored: $(printf '%s' "$_mu" | head -2)"
fi

# STRUCTURAL: the two-step must be the ONLY way a reserved line is counted, enumerated in ONE
# place, so a future reserved line cannot join without it. Behavioural cases only cover the
# lines someone already thought of.
_bad_recognizers=$(grep -cE "_count_re '\^[a-z-]+:\[\[:space:\]\]" "$VERDICT" || true)
if grep -q '^_count_prefix()' "$VERDICT" \
   && grep -q '^# _RESERVED_LINES' "$VERDICT" \
   && [ "${_bad_recognizers:-0}" = 0 ]; then
  ok "22.9 reserved lines are enumerated in ONE place and counted by BARE PREFIX only"
else
  bad "22.9 reserved lines are enumerated in ONE place and counted by BARE PREFIX only" \
      "full-grammar counters still present: ${_bad_recognizers:-?}"
fi

# ---------------------------------------------------------------------------
# CASE FLOORS (#3544). A span-replacing edit once silently deleted four cases from a
# sibling suite and it reported `failed: 0` at 102 instead of 105 for a whole round — a
# green tally over a shrunken suite.
#
# A TOTAL FLOOR ALONE IS NOT ENOUGH, and this suite's own first version proved it: at 33
# cases with FLOOR=28 there was room to delete ALL FIVE of section 4 — the
# affirmative-measurement core, where every unmeasurable input must land on a named
# non-pass — without redding. So each section carries its own floor, and the total is
# EXACT rather than slack: a deliberate addition updates one number, while a deletion
# anywhere reds.
# ---------------------------------------------------------------------------
# DERIVED, then committed — the counts below were read off a green run rather than
# predicted (`grep -cE '^(ok|FAIL) '` per leading section number), because a floor written
# from memory is a floor that silently drifts low. Round 2 moved section 13 from 2 to 4
# (the taxonomy descope replaced one code-taxonomy case with four liveness-silence ones)
# and added section 14, so the total rose 63 -> 67; round 3 added section 15 (the third
# per-mode completion grammar), 67 -> 78; round 4 added sections 16 (the mode filter over
# the reader's token) and 17 (the anchored line shape), 78 -> 87; round 5 grew 17 by one and
# added 18 (a PARTIAL token requires its scope line) and 19 (meta reads stop at RESULT:),
# 87 -> 96; round 6 added 20 (escapes are refused, not normalised) and 21 (the name must be a
# real component), 96 -> 106; the C (spec-auditor) round added 6.8 (the gate's exit mapping,
# derived from source) and 15.12 (no site may scope the RECORD grammar to --delta),
# 106 -> 108; #3951 grew section 10 by eight (an uncertain tree-integrity must not mask a
# certain summary-integrity FAIL, plus four controls for the mappings that must NOT move),
# 108 -> 116; job 401 added section 22 (the reserved-line recognizer class: malformed is its
# own refusal, never absent), 116 -> 125; #4029 grew section 17 by one (17.7: the REAL,
# post-#3625 ANNOTATED boundary block refused on INTEGRITY — the reachable shape 17.3's
# fixture stopped being), 125 -> 126. Raised
# DELIBERATELY each time: the total is a
# `-lt` floor, so leaving it low would let the added cases be deleted while the suite still
# reported green — this repo's own case-floor lesson.
SECTION_FLOORS="1:4 2:5 3:3 4:5 5:7 6:8 7:2 8:4 9:5 10:16 11:6 12:5 13:4 14:2 15:12 16:4 17:7 18:5 19:3 20:5 21:5 22:9"
FLOOR=126
for _sf in $SECTION_FLOORS; do
  _sec=${_sf%%:*}; _min=${_sf##*:}
  eval "_got=\${SEC_$_sec:-0}"
  if [ "$_got" -lt "$_min" ]; then
    bad "section floor: section $_sec ran $_got cases, expected at least $_min (cases deleted?)"
  fi
done
total=$((pass + fail))
if [ "$total" -lt "$FLOOR" ]; then
  bad "case floor: ran $total cases, expected at least $FLOOR (cases deleted?)"
fi

echo
echo "==== test_gate_component_verdict.sh: passed=$pass failed=$fail ===="
[ "$fail" -eq 0 ] || exit 1
exit 0
