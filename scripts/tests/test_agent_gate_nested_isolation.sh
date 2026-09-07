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
# #3755: this test's subject is not disk. Pin the admission bar to 0 so a box that
# happens to be low on space cannot red a component whose subject is something else.
export CQLITE_GATE_MIN_FREE_GB=0

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-nested.XXXXXX" 2>/dev/null) || tmp=""
if [ -z "$tmp" ] || [ ! -d "$tmp" ]; then
  printf 'FAIL - could not create a scratch dir under %s — refusing to run\n' "${TMPDIR:-/tmp}"
  exit 1
fi
trap 'rm -rf "$tmp"' EXIT INT TERM

# #3637: every gate this file spawns creates a per-run LOG_DIR under ITS $TMPDIR, and
# the runs here are nested ones whose #2874 private summary lives INSIDE that
# directory — so the gate RETAINS them by design (removing one would delete the
# verdict block the parent asserts on). Retained under the AMBIENT shared temp they
# are a leak this harness owns; retained under the harness's own scratch root the
# trap above reclaims them. The scratch root is validated FIRST because this export
# is one more derivation from it (an empty $tmp would silently restore the ambient
# /tmp and the leak with it).
export TMPDIR="$tmp/tmpdir"
mkdir -p "$TMPDIR" || { printf 'FAIL - could not create the scoped TMPDIR %s\n' "$TMPDIR"; exit 1; }

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
# LIVE-PEER case (SENTINEL_WROTE=1: the writable throwaway path took our startup sentinel, then a
# foreign run-id appears — a live peer owns it). The guard MUST name the failure on stderr + exit
# non-zero WITHOUT rewriting the contended summary path; rewriting it would clobber the live peer
# (HIGH counter-clobber review finding).
integ="$tmp/integ.txt"
integ_err="$tmp/integ.err"
if env AGENT_GATE_SUMMARY_FILE="$integ" AGENT_GATE_INTEGRITY_SELFTEST=1 \
     bash "$FAKE_GATE" >/dev/null 2>"$integ_err"; then
  bad "integrity guard did NOT exit non-zero on a foreign run-id (silent pass)"
  echo "------- stderr -------"; cat "$integ_err" 2>/dev/null; echo "-----------------------"
else
  ok "integrity guard exits non-zero on a mid-run foreign run-id"
fi
if grep -q 'summary-integrity: FAIL (foreign run-id detected mid-run;' "$integ_err"; then
  ok "integrity guard names the failure on stderr ('summary-integrity: FAIL')"
else
  bad "integrity guard did not name the failure on stderr"
  echo "------- stderr -------"; cat "$integ_err" 2>/dev/null; echo "-----------------------"
fi
if grep -q 'RESULT: FAIL' "$integ_err"; then
  ok "integrity guard names RESULT: FAIL on stderr (never a bare INCOMPLETE)"
else
  bad "integrity guard did not name RESULT: FAIL on stderr"
  echo "------- stderr -------"; cat "$integ_err" 2>/dev/null; echo "-----------------------"
fi
# The CONTENDED path must be LEFT INTACT — the live peer's (seeded foreign) block survives and our
# FAIL block never overwrote it. This is the HIGH counter-clobber fix's core assertion.
if grep -q 'run-id: /tmp/agent-gate.FOREIGN' "$integ" && ! grep -q 'RESULT: FAIL' "$integ"; then
  ok "contended summary path left intact (live peer NOT clobbered) — HIGH counter-clobber fix"
else
  bad "contended summary path was rewritten (live peer clobbered)"
  echo "------- contended path -------"; cat "$integ" 2>/dev/null; echo "-----------------------"
fi
# The named FAIL block must actually LAND at caller-reachable NON-clobbering paths (review job-2106
# LOW: the private/sibling write is `|| true`, so a silent dark path would leave the suite green).
# Assert BOTH the co-located sibling ($integ.integrity-fail.<run-id>) AND the private log named in
# the stderr line carry the full named block.
sib=$(printf '%s\n' "$integ".integrity-fail.* | head -1)
priv=$(sed -n 's/.*verdict in \([^ ]*\) and .*/\1/p' "$integ_err" | head -1)
sib_ok=0
[ -f "$sib" ] \
  && grep -q 'summary-integrity: FAIL (foreign run-id detected mid-run;' "$sib" \
  && grep -q 'detected-after-component:' "$sib" \
  && grep -q 'RESULT: FAIL' "$sib" && sib_ok=1
priv_ok=0
[ -n "$priv" ] && [ -f "$priv" ] \
  && grep -q 'summary-integrity: FAIL (foreign run-id detected mid-run;' "$priv" \
  && grep -q 'RESULT: FAIL' "$priv" && priv_ok=1
if [ "$sib_ok" = 1 ] && [ "$priv_ok" = 1 ]; then
  ok "verdict published to non-clobbering sibling + private log (both carry the named FAIL block)"
else
  bad "verdict not fully published to sibling ($sib_ok) / private log ($priv_ok)"
  echo "------- sibling ($sib) -------"; cat "$sib" 2>/dev/null
  echo "------- private ($priv) -------"; cat "$priv" 2>/dev/null; echo "-----------------------"
fi

# --- Property 3a: invalid selftest selector fails closed BEFORE any gate work ----
# A typo like `Side` (with an explicit summary file) must NOT fall through and run a REAL gate;
# it must exit 2 with a named error before the startup sentinel (review finding: MEDIUM).
badsel_out="$tmp/badsel.out"
if env AGENT_GATE_SUMMARY_FILE="$tmp/badsel.txt" AGENT_GATE_INTEGRITY_SELFTEST=Side \
     bash "$FAKE_GATE" --only file-size >"$badsel_out" 2>&1; then
  bad "invalid AGENT_GATE_INTEGRITY_SELFTEST=Side did NOT fail closed (ran a gate)"
else
  badsel_rc=$?
  if [ "$badsel_rc" = 2 ] && grep -q 'invalid AGENT_GATE_INTEGRITY_SELFTEST' "$badsel_out"; then
    ok "invalid selftest selector exits 2 with a named error before any gate work"
  else
    bad "invalid selftest selector wrong failure (rc=$badsel_rc)"
    echo "------- badsel out -------"; cat "$badsel_out" 2>/dev/null; echo "-------------------------"
  fi
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

# marker mode exercises the SIDE-lane post-drain conversion in the LIVE-PEER case: the terminal
# path must publish FAIL to the private log + sibling WITHOUT clobbering the contended peer block
# (ratified job-2106 contract — this is the SIDE-lane analogue of Property 3's MAIN-lane no-clobber).
marker_sum="$tmp/marker-integ.txt"
marker_out="$tmp/marker.out"
env AGENT_GATE_SUMMARY_FILE="$marker_sum" AGENT_GATE_INTEGRITY_SELFTEST=marker \
  bash "$FAKE_GATE" >"$marker_out" 2>/dev/null
if grep -q 'marker-integrity-selftest: contended-untouched=yes sibling=yes' "$marker_out"; then
  ok "post-drain marker conversion (live peer): contended path intact + sibling written"
else
  bad "post-drain marker conversion clobbered the peer or skipped the sibling"
  echo "------- marker out -------"; cat "$marker_out" 2>/dev/null; echo "-------------------------"
fi
if grep -q 'run-id: /tmp/agent-gate.FOREIGN-' "$marker_sum" && ! grep -q 'RESULT: FAIL' "$marker_sum"; then
  ok "marker mode: contended path still the foreign peer block (SIDE-lane live peer NOT clobbered)"
else
  bad "marker mode: contended path was rewritten (peer clobbered)"
  echo "------- marker summary -------"; cat "$marker_sum" 2>/dev/null; echo "-----------------------------"
fi
msib=$(printf '%s\n' "$marker_sum".integrity-fail.* | head -1)
if [ -f "$msib" ] && grep -q 'summary-integrity: FAIL' "$msib" && grep -q 'RESULT: FAIL' "$msib"; then
  ok "marker mode: sibling carries summary-integrity FAIL + RESULT FAIL (verdict never lost)"
else
  bad "marker mode: sibling missing the terminal FAIL verdict"
  echo "------- marker sibling ($msib) -------"; cat "$msib" 2>/dev/null; echo "-----------------------------"
fi
# #2926 H3: these self-test hooks reach the REAL terminal emit with a hand-built meta. Every
# other emit path in #2926 is THREADED through the tree renderers, and an emit path that is
# not is precisely how "nine emit sites, six of them undiscovered" happened — an emitted
# block with no tree lines is untraceable, whatever the block's other content. Assert the
# lines are present on both hook-driven blocks (their VALUES depend on whether the fixture
# is a git worktree; their PRESENCE does not).
assert_tree_lines() { # assert_tree_lines <label> <file>
  local label="$1" f="$2" miss=()
  [ -f "$f" ] || { bad "$label: block file missing ($f)"; return; }
  grep -q '^tree-start: '     "$f" || miss+=("tree-start")
  grep -q '^tree-end: '       "$f" || miss+=("tree-end")
  grep -q '^tree-integrity: ' "$f" || miss+=("tree-integrity")
  # …exactly ONCE. This publish path re-emits the tree lines from the live globals and
  # drops any the caller also supplied, so threading a caller's meta must not double them.
  [ "$(grep -c '^tree-start: ' "$f")" = 1 ] || miss+=("tree-start-not-exactly-once")
  [ "$(grep -c '^tree-end: ' "$f")" = 1 ]   || miss+=("tree-end-not-exactly-once")
  if [ "${#miss[@]}" -eq 0 ]; then
    ok "$label: the emitted block carries tree-start/tree-end/tree-integrity (#2926 H3: no untraceable emit path)"
  else
    bad "$label: emitted block is missing ${miss[*]} — this emit path is not threaded through the tree renderers"
    echo "------- block ($f) -------"; cat "$f" 2>/dev/null; echo "--------------------------"
  fi
}
assert_tree_lines "marker mode" "$msib"
# --- Property 3d (job-2107 MED#1): marker-LESS terminal detection ---------------
# A peer that writes the contended path AFTER the last component boundary leaves no marker; the
# terminal emit must STILL detect it on the observable condition alone, not clobber, and force FAIL.
tnm_out="$tmp/tnm.out"
env AGENT_GATE_SUMMARY_FILE="$tmp/tnm-integ.txt" AGENT_GATE_INTEGRITY_SELFTEST=terminal-nomarker \
  bash "$FAKE_GATE" >"$tnm_out" 2>/dev/null
if grep -q 'terminal-nomarker-selftest: contended-untouched=yes sibling=yes overall=FAIL rc=1' "$tnm_out"; then
  ok "terminal detection (no marker): foreign peer at terminal → no clobber + sibling + forced FAIL (MED#1)"
else
  bad "terminal no-marker detection wrong (marker-less clobber window reopened)"
  echo "------- tnm out -------"; cat "$tnm_out" 2>/dev/null; echo "----------------------"
fi
# The published sibling on the marker-less path must ALSO carry the SUMMARY_META (job-2107 MED#2:
# a live-peer FAIL block keeps the commit/branch meta, not just reason/component).
tnm_sib=$(printf '%s\n' "$tmp/tnm-integ.txt".integrity-fail.* | head -1)
if [ -f "$tnm_sib" ] && grep -q 'commit: selftest branch: selftest' "$tnm_sib"; then
  ok "live-peer FAIL block preserves SUMMARY_META (component/commit context not dropped) — MED#2"
else
  bad "live-peer FAIL block dropped SUMMARY_META"
  echo "------- tnm sibling ($tnm_sib) -------"; cat "$tnm_sib" 2>/dev/null; echo "----------------------"
fi
assert_tree_lines "terminal-nomarker mode" "$tnm_sib"
# …and the WIRING that keeps it that way: both hooks must render through _tree_meta_array,
# never a second hand-built dialect of the tree lines.
for hook_fn in 'marker' 'terminal-nomarker'; do
  # The hook name reaches awk through ENVIRON, never `awk -v` (whose escape processing can
  # rewrite a value) — the convention the tree suites use throughout.
  if TEST_AWK_H="  $hook_fn)" \
       awk 'index($0, ENVIRON["TEST_AWK_H"]) == 1 { f = 1 } f { print } f && /exit 0 ;;/ { exit }' "$GATE" \
       | grep -q '^    _tree_meta_array$'; then
    ok "WIRING: the '$hook_fn' self-test hook threads its block through _tree_meta_array (#2926 H3)"
  else
    bad "WIRING: the '$hook_fn' self-test hook emits without the tree renderers — an untraceable emit path"
  fi
done

# --- Property 3c (WIRING): the guard is actually called from record_result ----------
# All the hook-driven properties above call _assert_summary_integrity /
# _apply_integrity_marker DIRECTLY; deleting either real call site would leave them
# green while the mechanism is inert on a real gate (the "green tests on the wrong
# surface" class this change exists to close). Pin both call sites — structurally AND
# end-to-end (review finding 1).
#
# Structural: record_result's body must call _assert_summary_integrity "$1", and a bare
# _apply_integrity_marker call must appear AFTER `launch_components` and BEFORE the full-gate
# terminal `_emit_terminal_summary "$OVERALL"` (the shared MAIN/SIDE no-clobber emit).
rr_body=$(awk '/^record_result\(\) \{/{f=1} f{print} f&&/^\}/{exit}' "$GATE")
if printf '%s\n' "$rr_body" | grep -q '_assert_summary_integrity "\$1"'; then
  ok "WIRING: record_result() body calls _assert_summary_integrity \"\$1\""
else
  bad "WIRING: record_result() does NOT call the integrity guard — mechanism inert on a real gate"
fi
lc_ln=$(grep -n '^launch_components$' "$GATE" | head -1 | cut -d: -f1)
apply_ln=$(grep -nE '^[[:space:]]*_apply_integrity_marker[[:space:]]*$' "$GATE" | tail -1 | cut -d: -f1)
# The TERMINAL full-gate emit is now `_emit_terminal_summary "$OVERALL" "${SUMMARY_META[@]}"` (the
# shared no-clobber contract). The lite/delta paths still emit_summary the same shape and exit
# earlier, so we target the full-gate terminal by its distinct function name.
emit_ln=$(grep -nE '^[[:space:]]*_emit_terminal_summary "\$OVERALL" "\$\{SUMMARY_META' "$GATE" | tail -1 | cut -d: -f1)
if [ -n "$lc_ln" ] && [ -n "$apply_ln" ] && [ -n "$emit_ln" ] \
   && [ "$apply_ln" -gt "$lc_ln" ] && [ "$apply_ln" -lt "$emit_ln" ]; then
  ok "WIRING: _apply_integrity_marker runs after launch_components and before terminal _emit_terminal_summary (lines $lc_ln<$apply_ln<$emit_ln)"
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
# Assert the SPECIFIC route this e2e actually exercises (review finding): the summary is chmod-0444,
# so our startup sentinel could not land (SENTINEL_WROTE=0) and the guard fires the *unwritable /
# stale prior-run* branch, detected after the `file-size` component. Pinning the exact reason +
# component keeps this from passing via any other 'summary-integrity: FAIL' path.
if grep -q 'summary-integrity: FAIL (summary-file unwritable / stale prior-run block' "$e2e_out" \
   && grep -q 'detected-after-component: file-size' "$e2e_out"; then
  ok "WIRING e2e: death carries the SPECIFIC named line (unwritable route, detected-after-component: file-size)"
else
  bad "WIRING e2e: missing the SPECIFIC 'summary-integrity: FAIL (summary-file unwritable...)' + 'detected-after-component: file-size' lines"
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
