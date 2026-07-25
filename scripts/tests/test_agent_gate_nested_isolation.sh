#!/usr/bin/env bash
# Regression test for issue #2874: the gate of record must be IMMUNE to nested /
# concurrent gate activity — the residual clobber surface left after #2751 closed the
# AGENT_GATE_SUMMARY_FILE env-inheritance vector.
#
# It proves four properties of scripts/agent-gate.sh:
#   1. NESTED-CLOBBER IMMUNITY: a nested invocation (started with an ENCLOSING gate's
#      AGENT_GATE_PARENT_RUN_ID marker + no explicit summary path) defaults its summary
#      to its OWN private log dir, NEVER the enclosing checkout's shared default
#      (.agent-gate-summary.txt) — so it cannot alter the parent gate's summary.
#   2. EXPLICIT-WINS: a nested caller that DOES pin AGENT_GATE_SUMMARY_FILE still gets
#      exactly that path (existing self-tests keep asserting on summary content).
#   3. MID-RUN INTEGRITY GUARD: a summary externally overwritten with a FOREIGN run-id
#      is caught at the component boundary with a NAMED `summary-integrity: FAIL` line
#      and a non-zero exit — never a bare INCOMPLETE death.
#   4. SAME-CHECKOUT CONCURRENCY: two gate self-test lanes run concurrently in one
#      checkout both pass (per-run mktemp namespaces proven).
#
# Fast + hermetic: drives the no-cargo `--emit-summary-selftest` path and the hidden
# AGENT_GATE_INTEGRITY_SELFTEST hook against an ISOLATED fake checkout (a copy of the
# gate script whose REPO_ROOT resolves into a temp dir), so it never touches the real
# repo's summary artifacts. No datasets/Docker/network.
#
# Run standalone:   bash scripts/tests/test_agent_gate_nested_isolation.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"
# Scrub any inherited summary path so a standalone run can never clobber a caller's
# file, and DISABLE the machine slot cap so the nested gates below never block on it
# (existing pattern — see test_agent_gate_summary.sh).
unset AGENT_GATE_SUMMARY_FILE
export CQLITE_GATE_DISABLE_CAP=1

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-nested.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

# Isolated fake checkout: copy ONLY the gate script into <fakeroot>/scripts/ so that
# `cd "$(dirname "$0")/.."` inside the gate resolves REPO_ROOT to $fakeroot and the
# checkout-default summary path becomes $fakeroot/.agent-gate-summary.txt — never the
# real repo's. The --emit-summary-selftest path needs no other repo file.
fakeroot="$tmp/fakeroot"
mkdir -p "$fakeroot/scripts"
cp "$GATE" "$fakeroot/scripts/agent-gate.sh"
FAKE_GATE="$fakeroot/scripts/agent-gate.sh"
DEFAULT_SUMMARY="$fakeroot/.agent-gate-summary.txt"

hash_of() { shasum "$1" 2>/dev/null | awk '{print $1}'; }
# summary-file: line value emitted in a --emit-summary-selftest block on stdout.
summary_path_of() { sed -n 's/^summary-file:[[:space:]]*//p' "$1" | head -1; }

# --- Property 1: nested-clobber immunity ---------------------------------------
# 1a. A NON-nested run (no parent marker, no explicit summary) writes the checkout
#     default. This establishes the "parent summary S" the nested run must not touch.
env -u AGENT_GATE_PARENT_RUN_ID -u AGENT_GATE_SUMMARY_FILE \
  bash "$FAKE_GATE" --emit-summary-selftest >/dev/null 2>&1
if [ -s "$DEFAULT_SUMMARY" ]; then
  ok "non-nested run writes the checkout default summary (parent S established)"
else
  bad "non-nested run did not write the checkout default summary — cannot run the immunity check"
fi
parent_before=$(hash_of "$DEFAULT_SUMMARY")

# 1b. A NESTED run (parent marker present, NO explicit summary) in the SAME checkout
#     must write its OWN private log dir and leave the parent default byte-identical.
nested_out="$tmp/nested.out"
env -u AGENT_GATE_SUMMARY_FILE AGENT_GATE_PARENT_RUN_ID="/tmp/agent-gate.PARENT-FAKE" \
  bash "$FAKE_GATE" --emit-summary-selftest >"$nested_out" 2>&1
parent_after=$(hash_of "$DEFAULT_SUMMARY")

if [ -n "$parent_before" ] && [ "$parent_before" = "$parent_after" ]; then
  ok "nested run left the parent checkout-default summary BYTE-IDENTICAL"
else
  bad "nested run ALTERED the parent checkout-default summary ($parent_before -> $parent_after) — clobber not prevented"
  echo "------- nested stdout -------"; cat "$nested_out"; echo "-----------------------------"
fi

nested_summary_path=$(summary_path_of "$nested_out")
case "$nested_summary_path" in
  "$DEFAULT_SUMMARY")
    bad "nested run wrote the checkout default ($nested_summary_path) instead of its own log dir" ;;
  */agent-gate.*/summary-primary.txt)
    ok "nested run wrote its OWN private log dir ($nested_summary_path)" ;;
  *)
    bad "nested run summary-file was unexpected: '$nested_summary_path'" ;;
esac

if grep -q "nested-under: /tmp/agent-gate.PARENT-FAKE" "$nested_out"; then
  ok "nested run stamps 'nested-under: <parent-run-id>' for traceability"
else
  bad "nested run did not stamp the 'nested-under:' traceability line"
fi

# --- Property 2: explicit summary path still wins even when nested --------------
# The explicit path is honored (no private redirect), AND — per review finding 6 —
# `nested-under:` is stamped whenever the run was spawned by an enclosing gate,
# INDEPENDENT of the redirect decision. So the real property to assert is the PATH
# (explicit wins), not the absence of the nested-under stamp.
pinned="$tmp/pinned.txt"
env AGENT_GATE_PARENT_RUN_ID="/tmp/agent-gate.PARENT-FAKE" AGENT_GATE_SUMMARY_FILE="$pinned" \
  bash "$FAKE_GATE" --emit-summary-selftest >/dev/null 2>&1
if [ -s "$pinned" ] && grep -q 'RESULT: PASS' "$pinned"; then
  ok "nested caller's explicit AGENT_GATE_SUMMARY_FILE is still honored"
else
  bad "nested caller's explicit AGENT_GATE_SUMMARY_FILE was NOT honored"
fi
# The explicit path must be the ONE written — the run must NOT redirect to a private
# $LOG_DIR path (that is the property; traceability via nested-under is orthogonal).
pinned_path=$(summary_path_of "$pinned")
if [ "$pinned_path" = "$pinned" ]; then
  ok "explicit-path nested run wrote exactly the pinned path (no private redirect)"
else
  bad "explicit-path nested run wrote '$pinned_path', not the pinned '$pinned'"
fi
# And it IS still traceably marked nested (decoupled from the redirect, finding 6).
if grep -q "nested-under: /tmp/agent-gate.PARENT-FAKE" "$pinned"; then
  ok "explicit-path nested run is still stamped nested-under (traceability decoupled from redirect)"
else
  bad "explicit-path nested run lost its nested-under traceability stamp"
fi

# --- Property 3: mid-run summary-integrity guard names the failure -------------
integ="$tmp/integ.txt"
integ_err="$tmp/integ.err"
if env AGENT_GATE_SUMMARY_FILE="$integ" AGENT_GATE_INTEGRITY_SELFTEST=1 \
     bash "$FAKE_GATE" >/dev/null 2>"$integ_err"; then
  bad "integrity guard did NOT exit non-zero on a foreign run-id (silent pass)"
  echo "------- summary -------"; cat "$integ" 2>/dev/null; echo "-----------------------"
else
  ok "integrity guard exits non-zero on a mid-run foreign run-id"
fi
if grep -q 'summary-integrity: FAIL (foreign run-id detected mid-run;' "$integ"; then
  ok "integrity guard writes a NAMED 'summary-integrity: FAIL' line"
else
  bad "integrity guard did not write the named 'summary-integrity: FAIL' line"
  echo "------- summary -------"; cat "$integ" 2>/dev/null; echo "-----------------------"
fi
if grep -q 'RESULT: FAIL' "$integ"; then
  ok "integrity guard summary is RESULT: FAIL (never a bare INCOMPLETE)"
else
  bad "integrity guard summary was not RESULT: FAIL"
fi

# --- Property 3b: SIDE-lane (backgrounded subshell) clobber path ----------------
# record_result runs both on the MAIN foreground lane AND inside backgrounded SIDE-lane
# subshells. In a subshell the guard must NOT emit+exit (that would only kill the
# subshell — the clobber silently lost — and write a false mid-run terminal block a
# poller misreads); it records a marker + returns 1. The post-drain conversion then
# turns the marker into a terminal summary-integrity FAIL. Both halves are driven
# deterministically via the AGENT_GATE_INTEGRITY_SELFTEST=side / =marker hooks.
side_out="$tmp/side.out"
env AGENT_GATE_SUMMARY_FILE="$tmp/side-integ.txt" AGENT_GATE_INTEGRITY_SELFTEST=side \
  bash "$FAKE_GATE" >"$side_out" 2>/dev/null
if grep -q 'side-integrity-selftest: rc=1 marker=yes' "$side_out" \
   && grep -q 'side-integrity-selftest: summary-untouched=yes' "$side_out"; then
  ok "SIDE-lane clobber records a marker + returns 1 WITHOUT emitting a mid-run terminal block"
else
  bad "SIDE-lane clobber path wrong"
  echo "------- side-selftest out -------"; cat "$side_out"; echo "---------------------------------"
fi

marker_sum="$tmp/marker-integ.txt"
env AGENT_GATE_SUMMARY_FILE="$marker_sum" AGENT_GATE_INTEGRITY_SELFTEST=marker \
  bash "$FAKE_GATE" >/dev/null 2>&1
if grep -q 'summary-integrity: FAIL' "$marker_sum" && grep -q 'RESULT: FAIL' "$marker_sum"; then
  ok "post-drain marker conversion -> terminal summary carries summary-integrity FAIL + RESULT FAIL"
else
  bad "post-drain marker conversion did not produce a terminal integrity FAIL"
  echo "------- marker summary -------"; cat "$marker_sum" 2>/dev/null; echo "-----------------------------"
fi

# --- Property 3c (WIRING): the guard is actually called from record_result ----------
# All the hook-driven properties above call _assert_summary_integrity /
# _apply_integrity_marker DIRECTLY; deleting either real call site would leave them
# green while the mechanism is inert on a real gate (the "green tests on the wrong
# surface" class this change exists to close). Pin both call sites — structurally AND
# end-to-end (review finding 1).
#
# Structural: record_result's body must call _assert_summary_integrity "$1", and a bare
# _apply_integrity_marker call must appear AFTER `launch_components` and BEFORE the
# terminal `emit_summary "$OVERALL"`.
rr_body=$(awk '/^record_result\(\) \{/{f=1} f{print} f&&/^\}/{exit}' "$GATE")
if printf '%s\n' "$rr_body" | grep -q '_assert_summary_integrity "\$1"'; then
  ok "WIRING: record_result() body calls _assert_summary_integrity \"\$1\""
else
  bad "WIRING: record_result() does NOT call the integrity guard — mechanism inert on a real gate"
fi
lc_ln=$(grep -n '^launch_components$' "$GATE" | head -1 | cut -d: -f1)
apply_ln=$(grep -nE '^[[:space:]]*_apply_integrity_marker[[:space:]]*$' "$GATE" | tail -1 | cut -d: -f1)
# The TERMINAL full-gate emit (with "${SUMMARY_META[@]}"), which is the LAST such call
# in the file — the lite/delta paths emit the same shape earlier and exit before here.
emit_ln=$(grep -nE '^[[:space:]]*emit_summary "\$OVERALL" "\$\{SUMMARY_META' "$GATE" | tail -1 | cut -d: -f1)
if [ -n "$lc_ln" ] && [ -n "$apply_ln" ] && [ -n "$emit_ln" ] \
   && [ "$apply_ln" -gt "$lc_ln" ] && [ "$apply_ln" -lt "$emit_ln" ]; then
  ok "WIRING: _apply_integrity_marker runs after launch_components and before terminal emit_summary (lines $lc_ln<$apply_ln<$emit_ln)"
else
  bad "WIRING: _apply_integrity_marker call not in the post-drain/pre-emit region (launch=$lc_ln apply=$apply_ln emit=$emit_ln)"
fi

# End-to-end: a REAL `--only file-size` run whose summary carries a FOREIGN run-id must
# die non-zero with the named line — proving record_result invokes the guard in a real
# gate, not just via the hooks. The summary is seeded then made READ-ONLY so the startup
# sentinel cannot overwrite it (the foreign block survives to file-size's record_result).
e2e_sum="$tmp/e2e-foreign.txt"
{ echo '==== AGENT-GATE SUMMARY ===='; echo 'run-id: /tmp/agent-gate.FOREIGN-E2E'
  echo 'RESULT: INCOMPLETE'; echo '==== END AGENT-GATE SUMMARY ===='; } > "$e2e_sum"
chmod 0444 "$e2e_sum"
e2e_out="$tmp/e2e.out"
if env AGENT_GATE_SUMMARY_FILE="$e2e_sum" bash "$FAKE_GATE" --only file-size >"$e2e_out" 2>&1; then
  bad "WIRING e2e: real --only file-size did NOT fail on a foreign summary (guard not reached from record_result)"
else
  ok "WIRING e2e: real --only file-size dies non-zero on a foreign summary (guard wired into record_result)"
fi
chmod 0644 "$e2e_sum" 2>/dev/null || true
if grep -q 'summary-integrity: FAIL' "$e2e_out"; then
  ok "WIRING e2e: the death carries the named 'summary-integrity: FAIL' line"
else
  bad "WIRING e2e: no named 'summary-integrity: FAIL' line in the real-gate output"
  echo "------- e2e out tail -------"; tail -20 "$e2e_out"; echo "---------------------------"
fi

# --- Property 4: same-checkout concurrency on the historically-racy self-test ----
# Two concurrent test_agent_gate_parity_report.sh lanes in ONE checkout must both pass.
# This is the file whose FIXED mutated-manifest fixture used to race across lanes (one
# lane's EXIT trap rm'ing the other's live fixture — the residual #2874 kill surface);
# with the per-run mktemp fixture each lane is isolated. To keep the probe CHEAP and
# targeted (review finding 3), both lanes run with PARITY_REPORT_MANIFEST pointed at a
# NONEXISTENT path so each takes the fast no-cargo SKIP path — which still creates and
# trap-rm's its per-run mktemp fixture (the operation that used to race), WITHOUT the
# two cargo-backed nested `--only parity-report` gates a full run would spawn. Cost:
# ~two fast SKIP runs (no cargo compile); no python3 dependency; the lanes overlap.
real_repo=$(cd "$SCRIPT_DIR/../.." && pwd)
# Capture the PRE-EXISTING per-run fixture set (a peer lane may legitimately hold one)
# so the leak check asserts our two lanes ADD none — never couples to peer state
# (review finding 8).
fixtures_before=$(ls -1 "$real_repo"/test-data/.tmp-parity-manifest-mutated* 2>/dev/null | sort)
cflagA="$tmp/concA.rc"; cflagB="$tmp/concB.rc"
( PARITY_REPORT_MANIFEST="$tmp/no-such-manifest.yml" \
    bash "$SCRIPT_DIR/test_agent_gate_parity_report.sh" >"$tmp/concA.log" 2>&1; echo $? >"$cflagA" ) &
( PARITY_REPORT_MANIFEST="$tmp/no-such-manifest.yml" \
    bash "$SCRIPT_DIR/test_agent_gate_parity_report.sh" >"$tmp/concB.log" 2>&1; echo $? >"$cflagB" ) &
wait
rcA=$(cat "$cflagA" 2>/dev/null); rcB=$(cat "$cflagB" 2>/dev/null)
if [ "$rcA" = 0 ] && [ "$rcB" = 0 ]; then
  ok "two concurrent parity-report self-test lanes (fast SKIP path) in one checkout both passed"
else
  bad "concurrent parity-report lanes collided (rcA=$rcA rcB=$rcB)"
  echo "------- lane A tail -------"; tail -15 "$tmp/concA.log"; echo "---------------------------"
  echo "------- lane B tail -------"; tail -15 "$tmp/concB.log"; echo "---------------------------"
fi
# Our two lanes must ADD no per-run mutated-manifest fixture (set unchanged vs before).
fixtures_after=$(ls -1 "$real_repo"/test-data/.tmp-parity-manifest-mutated* 2>/dev/null | sort)
if [ "$fixtures_before" = "$fixtures_after" ]; then
  ok "the concurrent lanes left the test-data/ mutated-manifest fixture set unchanged (each trap-rm'd its own)"
else
  bad "the concurrent lanes changed the test-data/ fixture set (before='$fixtures_before' after='$fixtures_after')"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
