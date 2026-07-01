#!/usr/bin/env bash
# Regression test for issue #1338: the agent-gate `parity-report` component must
# catch a stale committed parity report (docs/reports/cassandra-test-parity.md)
# before push, and must SKIP — never silently PASS — when the cassandra-parity
# tool or the manifest is unavailable.
#
# It drives the real gate via `agent-gate.sh --only parity-report` and asserts the
# three outcomes from the SUMMARY block:
#   SKIP - manifest (or tool crate) absent          -> `parity-report: SKIP`
#   PASS - committed report matches a fresh render   -> `parity-report: PASS`
#   FAIL - manifest changed without regenerating     -> `parity-report: FAIL`,
#          and the gate output names docs/reports/cassandra-test-parity.md
#
# Docker/dataset-free. The SKIP case needs no cargo; the PASS/FAIL cases render the
# manifest with `cassandra-parity report --check` (one small crate, reused from the
# gate's own build cache). If cargo is unavailable they are reported as INFO/SKIP
# rather than failing, mirroring the component's own SKIP-awareness.
#
# Run standalone:   bash scripts/tests/test_agent_gate_parity_report.sh
# Or via the gate:  scripts/agent-gate.sh runs it inside the `tooling-tests` component.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
GATE="$SCRIPT_DIR/../agent-gate.sh"
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
MANIFEST="$REPO_ROOT/test-data/cassandra-parity-manifest.yml"
REPORT_REL="docs/reports/cassandra-test-parity.md"

PASS=0
FAIL=0
ok()  { printf 'ok   - %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf 'FAIL - %s\n' "$1"; FAIL=$((FAIL + 1)); }
inf() { printf 'info - %s\n' "$1"; }

tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-gate-parity.XXXXXX")
trap 'rm -rf "$tmp"' EXIT

# parity_status <summary-file>: print the component's status token (PASS/FAIL/SKIP)
# from the SUMMARY block, or empty if absent.
parity_status() {
  sed -n 's/^parity-report:[[:space:]]*\([A-Z]*\).*/\1/p' "$1" | head -1
}

# --- Outcome 1: SKIP when the manifest is absent (no cargo needed). -----------
sum="$tmp/skip.txt"; log="$tmp/skip.log"
AGENT_GATE_SUMMARY_FILE="$sum" PARITY_REPORT_MANIFEST="$tmp/does-not-exist.yml" \
  bash "$GATE" --only parity-report >"$log" 2>&1
if [ "$(parity_status "$sum")" = "SKIP" ]; then
  ok "manifest-absent -> parity-report SKIP"
else
  bad "manifest-absent: expected SKIP, got '$(parity_status "$sum")'"
  echo "------- summary -------"; cat "$sum"; echo "-----------------------"
fi

# Also SKIP when the tool crate dir is absent.
sum="$tmp/skip-tool.txt"
AGENT_GATE_SUMMARY_FILE="$sum" PARITY_REPORT_TOOL_DIR="$tmp/no-tool" \
  bash "$GATE" --only parity-report >/dev/null 2>&1
if [ "$(parity_status "$sum")" = "SKIP" ]; then
  ok "tool-crate-absent -> parity-report SKIP"
else
  bad "tool-crate-absent: expected SKIP, got '$(parity_status "$sum")'"
fi

# The PASS/FAIL cases render the manifest, which needs cargo + the manifest.
if ! command -v cargo >/dev/null 2>&1 || [ ! -f "$MANIFEST" ]; then
  inf "cargo or manifest unavailable; skipping PASS/FAIL render cases (component is SKIP-aware here)"
  echo "----"; echo "passed: $PASS  failed: $FAIL"; [ "$FAIL" -eq 0 ]; exit $?
fi

# --- Outcome 2: PASS when the committed report matches a fresh render. ---------
# Uses the real manifest + committed report (in sync on this branch).
sum="$tmp/pass.txt"; log="$tmp/pass.log"
AGENT_GATE_SUMMARY_FILE="$sum" \
  bash "$GATE" --only parity-report >"$log" 2>&1
if [ "$(parity_status "$sum")" = "PASS" ]; then
  ok "report-in-sync -> parity-report PASS"
else
  bad "report-in-sync: expected PASS, got '$(parity_status "$sum")'"
  echo "------- gate output -------"; tail -30 "$log"; echo "---------------------------"
fi

# --- Outcome 3: FAIL when the manifest changed without regenerating the report.
# Render a MUTATED manifest copy against the (unchanged) committed report: the
# fresh render no longer matches the committed report -> --check fails. We mutate a
# rendered field (cassandra_source.ref appears in the report header), keeping the
# YAML valid. --output stays the canonical committed report, which --check only
# READS, so the working tree is never modified.
mut="$tmp/manifest-mutated.yml"
# Match the cassandra_source.ref by KEY, not by its current pinned value, so a
# routine `ref:` bump does not silently turn this into a no-op (which would make
# the FAIL case unexercised and hard-FAIL the whole gate for an unrelated reason).
# roborev finding (#1338): value-coupled `sed` on `cassandra-5.0.2` was brittle.
sed -E 's/^(  ref: ).*/\1cassandra-5.0.99-staleness-probe/' "$MANIFEST" >"$mut"
if cmp -s "$MANIFEST" "$mut"; then
  bad "could not mutate manifest copy (cassandra_source.ref not found); FAIL case not exercised"
else
  sum="$tmp/fail.txt"; log="$tmp/fail.log"
  AGENT_GATE_SUMMARY_FILE="$sum" PARITY_REPORT_MANIFEST="$mut" \
    bash "$GATE" --only parity-report >"$log" 2>&1
  if [ "$(parity_status "$sum")" = "FAIL" ]; then
    ok "manifest-changed-without-regen -> parity-report FAIL"
  else
    bad "manifest-changed-without-regen: expected FAIL, got '$(parity_status "$sum")'"
    echo "------- gate output -------"; tail -30 "$log"; echo "---------------------------"
  fi
  if grep -q "$REPORT_REL" "$log"; then
    ok "FAIL output names $REPORT_REL"
  else
    bad "FAIL output does not name $REPORT_REL"
  fi
  # The committed report must be untouched by a --check run.
  if git -C "$REPO_ROOT" diff --quiet -- "$REPORT_REL" 2>/dev/null; then
    ok "committed report unchanged by the FAIL-case --check run"
  else
    bad "committed report was modified by a --check run (must be read-only)"
  fi
fi

echo "----"
echo "passed: $PASS  failed: $FAIL"
[ "$FAIL" -eq 0 ]
