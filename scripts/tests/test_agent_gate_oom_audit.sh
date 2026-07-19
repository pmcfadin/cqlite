#!/usr/bin/env bash
# Regression test for issue #2012: the agent-gate `oom-audit` component must
#   FAIL on an unallowlisted STREAM_RETURNS_VEC violation on a scoped path,
#   PASS on a clean scoped tree, and
#   SKIP (loudly, never silently PASS) when the xtask tool cannot build.
#
# It drives the real gate via `agent-gate.sh --only oom-audit` and asserts the
# three outcomes from the SUMMARY block, mirroring test_agent_gate_parity_report.sh.
# The FAIL/PASS cases point the audit at a synthetic source tree via
# CQLITE_OOM_AUDIT_ROOT (a planted violation / a bounded body under a scoped
# path) so the real source is never touched. The SKIP case points
# OOM_AUDIT_XTASK_DIR at an absent dir.
#
# Docker/dataset-free. The FAIL/PASS cases need cargo (they build + run xtask);
# if cargo is unavailable they are reported as INFO, mirroring the component's
# own SKIP-awareness. The SKIP case needs no cargo.
#
# Run standalone:   bash scripts/tests/test_agent_gate_oom_audit.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"

# #2751 defense-in-depth: this self-test drives nested `agent-gate.sh --only
# oom-audit` runs. Each case below pins its own AGENT_GATE_SUMMARY_FILE, but scrub
# any inherited value up front so a standalone run can never clobber the caller's
# summary file (the tooling-tests component scrubs it too).
unset AGENT_GATE_SUMMARY_FILE

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }
inf() { printf 'info - %s\n' "$1"; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-oom.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

# oom_status <summary-file>: the component's status token from the SUMMARY block.
oom_status() {
  sed -n 's/^oom-audit:[[:space:]]*\([A-Z]*\).*/\1/p' "$1" | head -1
}

# --- Build the synthetic trees -------------------------------------------------
# A scoped path per the committed scope roots: cqlite-core/src/query/**.
viol_root="$tmp/violation"
clean_root="$tmp/clean"
mkdir -p "$viol_root/cqlite-core/src/query" "$clean_root/cqlite-core/src/query"

cat >"$viol_root/cqlite-core/src/query/planted.rs" <<'RS'
// Planted STREAM_RETURNS_VEC violation on a scoped path (self-test only).
fn scan_all_rows(reader: &Reader) -> Vec<DataRow> {
    let rows = reader.rows().collect::<Vec<DataRow>>();
    rows
}
RS

cat >"$clean_root/cqlite-core/src/query/planted.rs" <<'RS'
// Bounded equivalent: a ResultBudget / .take(limit) suppresses the rule.
fn scan_all_rows(reader: &Reader, limit: usize) -> Vec<DataRow> {
    let rows = reader.rows().take(limit).collect::<Vec<DataRow>>();
    rows
}
RS

# --- Outcome 1: SKIP when the xtask crate is absent (no cargo needed). ----------
sum="$tmp/skip.txt"; log="$tmp/skip.log"
AGENT_GATE_SUMMARY_FILE="$sum" OOM_AUDIT_XTASK_DIR="$tmp/no-xtask" \
  bash "$GATE" --only oom-audit >"$log" 2>&1
if [ "$(oom_status "$sum")" = "SKIP" ]; then
  ok "xtask-crate-absent -> oom-audit SKIP"
else
  bad "xtask-crate-absent: expected SKIP, got '$(oom_status "$sum")'"
  echo "------- summary -------"; cat "$sum"; echo "-----------------------"
fi

# The FAIL/PASS cases build + run xtask.
if ! command -v cargo >/dev/null 2>&1; then
  inf "cargo unavailable; skipping FAIL/PASS cases (component is SKIP-aware here)"
  echo "----"; echo "passed: $PASS  failed: $FAIL"; [ "$FAIL" -eq 0 ]; exit $?
fi

# --- Outcome 2: FAIL on an unallowlisted violation on a scoped path. ------------
sum="$tmp/fail.txt"; log="$tmp/fail.log"
AGENT_GATE_SUMMARY_FILE="$sum" CQLITE_OOM_AUDIT_ROOT="$viol_root" \
  bash "$GATE" --only oom-audit >"$log" 2>&1
if [ "$(oom_status "$sum")" = "FAIL" ]; then
  ok "planted-violation -> oom-audit FAIL"
else
  bad "planted-violation: expected FAIL, got '$(oom_status "$sum")'"
  echo "------- gate output -------"; tail -30 "$log"; echo "---------------------------"
fi
# Strengthen: the FAIL must be the genuine rule firing, not an unrelated error.
if grep -q 'STREAM_RETURNS_VEC' "$log"; then
  ok "FAIL output names the STREAM_RETURNS_VEC rule"
else
  bad "FAIL case did not emit STREAM_RETURNS_VEC (rule not exercised)"
  echo "------- gate output -------"; tail -30 "$log"; echo "---------------------------"
fi

# --- Outcome 3: PASS on a clean (bounded) scoped tree. -------------------------
sum="$tmp/pass.txt"; log="$tmp/pass.log"
AGENT_GATE_SUMMARY_FILE="$sum" CQLITE_OOM_AUDIT_ROOT="$clean_root" \
  bash "$GATE" --only oom-audit >"$log" 2>&1
if [ "$(oom_status "$sum")" = "PASS" ]; then
  ok "bounded-tree -> oom-audit PASS"
else
  bad "bounded-tree: expected PASS, got '$(oom_status "$sum")'"
  echo "------- gate output -------"; tail -30 "$log"; echo "---------------------------"
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
