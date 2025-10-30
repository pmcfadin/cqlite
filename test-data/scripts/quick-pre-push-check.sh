#!/usr/bin/env bash
# Quick Pre-Push Validation for Issue #140
# Runs essential checks before pushing to CI (~2 minutes)
#
# Usage:
#   ./quick-pre-push-check.sh

set -euo pipefail

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Logging functions
log_header() {
    echo ""
    echo -e "${BOLD}${BLUE}========================================${NC}"
    echo -e "${BOLD}${BLUE}$*${NC}"
    echo -e "${BOLD}${BLUE}========================================${NC}"
    echo ""
}

log_step() {
    echo -e "${BLUE}▶${NC} $*"
}

log_success() {
    echo -e "${GREEN}✓${NC} $*"
}

log_error() {
    echo -e "${RED}✗${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}⚠${NC} $*"
}

# Track failures
FAILED_CHECKS=0

# Check function
check() {
    local name="$1"
    shift
    log_step "${name}..."
    if "$@" > /tmp/quick-check-$$.log 2>&1; then
        log_success "${name}"
    else
        log_error "${name} FAILED"
        echo "Error output:"
        cat /tmp/quick-check-$$.log
        rm -f /tmp/quick-check-$$.log
        FAILED_CHECKS=$((FAILED_CHECKS + 1))
        return 1
    fi
    rm -f /tmp/quick-check-$$.log
}

log_header "Quick Pre-Push Check for Issue #140"

cd "${WORKSPACE_ROOT}"

# Set up environment
export CQLITE_DATASETS_ROOT="${WORKSPACE_ROOT}/test-data/datasets"
export CQLITE_SCHEMA="${WORKSPACE_ROOT}/test-data/schemas/basic-types.cql"
export CQLITE_DATASET="test_basic"

log_step "Checking test data..."
if [[ ! -d "${CQLITE_DATASETS_ROOT}/sstables/${CQLITE_DATASET}" ]]; then
    log_error "Test dataset not found: ${CQLITE_DATASETS_ROOT}/sstables/${CQLITE_DATASET}"
    exit 1
fi
log_success "Test data found"

# 1. Build
log_header "Step 1: Building CLI"
check "Building cqlite CLI" \
    cargo build --package cqlite-cli --bin cqlite --quiet

# 2. CI Smoke Tests
log_header "Step 2: CI Smoke Tests"
check "Running smoke test suite" \
    bash "${SCRIPT_DIR}/ci-one-shot-smoke.sh"

# 3. Unit Tests
log_header "Step 3: Unit Tests"
check "Running core unit tests" \
    env CQLITE_DATASETS_ROOT="${CQLITE_DATASETS_ROOT}" \
        cargo test --package cqlite-core --quiet

# 4. Clippy
log_header "Step 4: Code Quality - Clippy"
check "Running clippy checks" \
    cargo clippy --package cqlite-core --quiet

# 5. Formatting
log_header "Step 5: Code Quality - Formatting"
check "Checking code formatting" \
    cargo fmt --check

# 6. Quick determinism check (3 runs)
log_header "Step 6: Quick Determinism Check"
log_step "Running query 3 times to verify deterministic output..."

TMP_DIR=$(mktemp -d)
for i in {1..3}; do
    ./target/debug/cqlite \
        --schema "${CQLITE_SCHEMA}" \
        --dataset "${CQLITE_DATASET}" \
        --execute "SELECT * FROM test_basic.simple_table LIMIT 3" \
        --format json \
        > "${TMP_DIR}/run_${i}.json" 2>/dev/null
done

if diff -q "${TMP_DIR}/run_1.json" "${TMP_DIR}/run_2.json" > /dev/null && \
   diff -q "${TMP_DIR}/run_1.json" "${TMP_DIR}/run_3.json" > /dev/null; then
    log_success "Output is deterministic (3 runs identical)"
else
    log_error "Output is NOT deterministic"
    echo "Differences found:"
    diff "${TMP_DIR}/run_1.json" "${TMP_DIR}/run_2.json" || true
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
fi
rm -rf "${TMP_DIR}"

# Summary
echo ""
log_header "SUMMARY"

if [[ ${FAILED_CHECKS} -eq 0 ]]; then
    echo -e "${GREEN}${BOLD}✓ ALL CHECKS PASSED${NC}"
    echo ""
    echo "Your changes are ready to push to CI!"
    echo ""
    echo "Next steps:"
    echo "  1. Review your changes: git diff"
    echo "  2. Stage changes: git add -u"
    echo "  3. Commit: git commit -m 'fix(issue-140): your message'"
    echo "  4. Push: git push origin main"
    echo ""
    echo "For comprehensive testing, run:"
    echo "  ./test-data/scripts/validate-issue-140-fix.sh"
    echo ""
    exit 0
else
    echo -e "${RED}${BOLD}✗ ${FAILED_CHECKS} CHECK(S) FAILED${NC}"
    echo ""
    echo "Please fix the issues above before pushing to CI."
    echo ""
    echo "For detailed diagnostics, run:"
    echo "  ./test-data/scripts/validate-issue-140-fix.sh"
    echo ""
    exit 1
fi
