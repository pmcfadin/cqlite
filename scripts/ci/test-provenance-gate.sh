#!/bin/bash
# Tests for the dataset-provenance gate (Issue #545).
#
# Verifies that:
#   1. A branch name containing "fixture" or "mock" in a GitHub CI env var
#      (GITHUB_HEAD_REF, GITHUB_REF, GITHUB_BASE_REF) does NOT trip the gate.
#   2. A real synthetic dataset path in a dataset-relevant env var (*_ROOT)
#      DOES trip the gate.
#   3. A clean, real-dataset invocation passes the gate unchanged.
#
# Run from any directory:
#   bash scripts/ci/test-provenance-gate.sh
#
# No external dependencies (no Docker, no Cargo) — pure shell.

set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
GATE="$SCRIPT_DIR/ensure_real_dataset.sh"

PASS=0
FAIL=0

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ok() {
    echo -e "${GREEN}✓ PASS${NC}: $1"
    PASS=$(( PASS + 1 ))
}

fail_test() {
    echo -e "${RED}✗ FAIL${NC}: $1"
    FAIL=$(( FAIL + 1 ))
}

# Helper: run gate with given env overrides; return exit code without failing
# the test script.  Uses a subshell so exported vars don't leak.
run_gate() {
    local input="$1"; shift   # positional arg passed to the gate
    # remaining args are NAME=VALUE pairs to export into the subshell env
    (
        for pair in "$@"; do
            export "${pair?}"
        done
        bash "$GATE" "$input" 2>/dev/null
    )
}

echo ""
echo "════════════════════════════════════════════════════════"
echo "  Provenance Gate Tests — Issue #545"
echo "════════════════════════════════════════════════════════"
echo ""

# ── Test 1 ──────────────────────────────────────────────────────────────────
# GITHUB_HEAD_REF containing "fixture" should NOT trip the gate.
echo -e "${YELLOW}Test 1${NC}: GITHUB_HEAD_REF with 'fixture' in branch name (should PASS gate)"
if run_gate "test-data/datasets" \
        "GITHUB_HEAD_REF=perf/issue-537-bench-fixtures" > /dev/null 2>&1; then
    ok "GITHUB_HEAD_REF=perf/issue-537-bench-fixtures did not trip gate"
else
    fail_test "GITHUB_HEAD_REF=perf/issue-537-bench-fixtures wrongly tripped gate (false-positive regression)"
fi

# ── Test 2 ──────────────────────────────────────────────────────────────────
# GITHUB_HEAD_REF containing "mock" should NOT trip the gate.
echo -e "${YELLOW}Test 2${NC}: GITHUB_HEAD_REF with 'mock' in branch name (should PASS gate)"
if run_gate "test-data/datasets" \
        "GITHUB_HEAD_REF=feat/replace-mock-helper" > /dev/null 2>&1; then
    ok "GITHUB_HEAD_REF=feat/replace-mock-helper did not trip gate"
else
    fail_test "GITHUB_HEAD_REF=feat/replace-mock-helper wrongly tripped gate (false-positive regression)"
fi

# ── Test 3 ──────────────────────────────────────────────────────────────────
# GITHUB_REF containing "fixture" should NOT trip the gate.
echo -e "${YELLOW}Test 3${NC}: GITHUB_REF with 'fixture' in ref name (should PASS gate)"
if run_gate "test-data/datasets" \
        "GITHUB_REF=refs/heads/perf/bench-fixture-loader" > /dev/null 2>&1; then
    ok "GITHUB_REF=refs/heads/perf/bench-fixture-loader did not trip gate"
else
    fail_test "GITHUB_REF=refs/heads/perf/bench-fixture-loader wrongly tripped gate (false-positive regression)"
fi

# ── Test 4 ──────────────────────────────────────────────────────────────────
# GITHUB_BASE_REF containing "mock" should NOT trip the gate.
echo -e "${YELLOW}Test 4${NC}: GITHUB_BASE_REF with 'mock' in value (should PASS gate)"
if run_gate "test-data/datasets" \
        "GITHUB_BASE_REF=fix/remove-mock-layer" > /dev/null 2>&1; then
    ok "GITHUB_BASE_REF=fix/remove-mock-layer did not trip gate"
else
    fail_test "GITHUB_BASE_REF=fix/remove-mock-layer wrongly tripped gate (false-positive regression)"
fi

# ── Test 5 ──────────────────────────────────────────────────────────────────
# A synthetic dataset path passed directly as a CLI argument DOES trip the gate.
echo -e "${YELLOW}Test 5${NC}: Synthetic path in CLI argument (should FAIL gate = gate correctly rejects it)"
if run_gate "test-data/generated/synthetic-tables" > /dev/null 2>&1; then
    fail_test "Synthetic path in CLI arg was not caught — gate broken"
else
    ok "Synthetic path in CLI arg correctly tripped gate"
fi

# ── Test 6 ──────────────────────────────────────────────────────────────────
# A synthetic path in a *_ROOT env var (dataset-relevant) DOES trip the gate.
echo -e "${YELLOW}Test 6${NC}: Synthetic path in CQLITE_DATASETS_ROOT (should FAIL gate)"
if run_gate "test-data/datasets" \
        "CQLITE_DATASETS_ROOT=/tmp/mock-sstables" > /dev/null 2>&1; then
    fail_test "Synthetic path in CQLITE_DATASETS_ROOT was not caught — gate broken"
else
    ok "Synthetic path in CQLITE_DATASETS_ROOT correctly tripped gate"
fi

# ── Test 7 ──────────────────────────────────────────────────────────────────
# A synthetic path in a DATASET_* env var DOES trip the gate.
echo -e "${YELLOW}Test 7${NC}: Synthetic path in DATASET_PATH (should FAIL gate)"
if run_gate "test-data/datasets" \
        "DATASET_PATH=/ci/fixture-data" > /dev/null 2>&1; then
    fail_test "Synthetic path in DATASET_PATH was not caught — gate broken"
else
    ok "Synthetic path in DATASET_PATH correctly tripped gate"
fi

# ── Test 8 ──────────────────────────────────────────────────────────────────
# A clean invocation with a real dataset path passes.
echo -e "${YELLOW}Test 8${NC}: Clean real-dataset invocation (should PASS gate)"
if run_gate "test-data/datasets/sstables/test_basic" > /dev/null 2>&1; then
    ok "Clean real-dataset invocation passed"
else
    fail_test "Clean real-dataset invocation unexpectedly failed"
fi

# ── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════"
echo -e "  Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}"
echo "════════════════════════════════════════════════════════"
echo ""

if [[ $FAIL -gt 0 ]]; then
    exit 1
fi
