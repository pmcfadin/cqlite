#!/usr/bin/env bash
# check-parity-partial-open-issues.sh — the NETWORK half of the #1401 orphaned-debt
# guard (the OFFLINE half is `cassandra-parity lint`, which fail-closes when a
# `partial` scenario lacks scope.target_issue or its next_step does not cite it).
#
# Every `evidence.type: partial` scenario in the parity manifest is a promise of
# unfinished work parked on a tracking issue. This script verifies each such
# target_issue is still OPEN on GitHub — a debt parked on a CLOSED issue is
# orphaned and must be re-parked (that is exactly the failure #1401 remediated).
#
# SKIP-aware, modelled on the SKIP-aware agent-gate components: because it needs
# the network + an authenticated `gh`, it records SKIP (loudly, never a silent
# PASS) when `gh` is unavailable or unauthenticated, so it is safe to wire into a
# nightly lane without flaking a PR gate. Deterministic and fail-closed: any
# reachable CLOSED target issue exits non-zero and names the offender.
#
# Usage:
#   scripts/tests/check-parity-partial-open-issues.sh [MANIFEST]
# Env:
#   PARITY_MANIFEST  override manifest path (default: test-data/cassandra-parity-manifest.yml)
set -euo pipefail

MANIFEST="${1:-${PARITY_MANIFEST:-test-data/cassandra-parity-manifest.yml}}"

if [ ! -f "$MANIFEST" ]; then
  echo "SKIP: manifest not found at $MANIFEST (not a full checkout)"
  exit 0
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "SKIP: python3 unavailable (needed to extract partial target issues)"
  exit 0
fi
if ! command -v gh >/dev/null 2>&1; then
  echo "SKIP: gh CLI unavailable (open-state check needs the network + gh)"
  exit 0
fi
if ! gh auth status >/dev/null 2>&1; then
  echo "SKIP: gh is not authenticated (open-state check needs the network + gh)"
  exit 0
fi

# Distinct target_issue numbers cited by evidence.type: partial scenarios. The
# offline linter guarantees each partial has a target_issue, so the set below is
# the complete list of open-tracker claims to verify. Dependency-free block-parse
# (no PyYAML on runners): a scenario opens at `  - id:`; evidence.type and
# scope.target_issue are located by their fixed indent within the block.
# Capture to a file with an explicit exit-code check so a parser failure hard-fails
# rather than silently yielding an empty set (this epic fights silent skips).
issues_file="$(mktemp)"
trap 'rm -f "$issues_file"' EXIT
if ! python3 - "$MANIFEST" >"$issues_file" <<'PY'
import re, sys
lines = open(sys.argv[1]).read().split('\n')
starts = [i for i, l in enumerate(lines) if re.match(r'^  - id:', l)]
end = next((i for i, l in enumerate(lines) if re.match(r'^claims:', l)), len(lines))
def getval(a, b, indent, key):
    pat = re.compile(r'^' + (' ' * indent) + re.escape(key) + r':\s*(.*)$')
    for i in range(a, b):
        m = pat.match(lines[i])
        if m:
            return m.group(1).strip()
    return None
issues = set()
for k, si in enumerate(starts):
    ei = starts[k + 1] if k + 1 < len(starts) else end
    if getval(si, ei, 6, 'type') != 'partial':
        continue
    ti = getval(si, ei, 6, 'target_issue')
    if ti and ti.isdigit():
        issues.add(int(ti))
for n in sorted(issues):
    print(n)
PY
then
  echo "FAIL: could not parse partial target issues from $MANIFEST"
  exit 1
fi
mapfile -t ISSUES <"$issues_file"

if [ "${#ISSUES[@]}" -eq 0 ]; then
  echo "OK: no evidence.type: partial scenarios with a target_issue to verify"
  exit 0
fi

echo "Checking open-state of ${#ISSUES[@]} partial target issue(s): ${ISSUES[*]}"
closed=()
for n in "${ISSUES[@]}"; do
  state="$(gh issue view "$n" --json state --jq .state 2>/dev/null || echo UNKNOWN)"
  if [ "$state" = "OPEN" ]; then
    echo "  #$n OPEN"
  elif [ "$state" = "UNKNOWN" ]; then
    # A transient API/network error must not silently pass; treat as SKIP-worthy
    # only if it affects ALL issues, else fail loudly on the specific number.
    echo "  #$n UNKNOWN (could not read state)"
    closed+=("$n(unknown)")
  else
    echo "  #$n $state  <-- must be re-parked onto an OPEN tracker"
    closed+=("$n($state)")
  fi
done

if [ "${#closed[@]}" -ne 0 ]; then
  echo "FAIL: partial scenarios are parked on non-open issue(s): ${closed[*]}"
  echo "Re-park them (update scope.target_issue + next_step) onto an OPEN tracker."
  exit 1
fi
echo "OK: every partial target issue is OPEN"
