#!/bin/bash
# test-all-ci-locally.sh
# Validates ALL active CI workflows locally before pushing
# This ensures complete CI parity across all workflows, not just M1

set -e

# Color output for better visibility
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

FAILED_WORKFLOWS=()
TOTAL_WORKFLOWS=0

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  CQLite - Complete CI Parity Validation (All Workflows)     ║${NC}"
echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
echo ""

# Set CI environment
export RUSTFLAGS="-D warnings"
export CQLITE_DATASETS_ROOT="$PWD/test-data/datasets"

echo -e "${YELLOW}Environment:${NC}"
echo "  RUSTFLAGS=$RUSTFLAGS"
echo "  CQLITE_DATASETS_ROOT=$CQLITE_DATASETS_ROOT"
echo ""

# Function to run a workflow validation
run_workflow() {
    local workflow_name="$1"
    local workflow_commands="$2"

    TOTAL_WORKFLOWS=$((TOTAL_WORKFLOWS + 1))

    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}Workflow $TOTAL_WORKFLOWS: $workflow_name${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"

    if eval "$workflow_commands"; then
        echo -e "${GREEN}✅ $workflow_name - PASSED${NC}"
        echo ""
        return 0
    else
        echo -e "${RED}❌ $workflow_name - FAILED${NC}"
        echo ""
        FAILED_WORKFLOWS+=("$workflow_name")
        return 1
    fi
}

# Workflow 1: M1 Minimal CI Pipeline
run_workflow "M1 Minimal CI Pipeline" '
echo "Step 1: Format check"
cargo fmt --all -- --check || { echo "❌ Format check failed. Run: cargo fmt --all"; exit 1; }

echo "Step 2: Clippy on cqlite-core"
cargo clippy --package cqlite-core --all-features

echo "Step 3: Clippy on cqlite-cli"
cargo clippy --package cqlite-cli --all-features

echo "Step 4: Core library tests"
cargo test --package cqlite-core --lib --no-fail-fast

echo "Step 5: M1 integration tests"
cargo test --package cqlite-core \
  --test P0_4_modern_format_rejection_tests \
  --test cassandra_compatibility \
  --test parser_abstraction_tests \
  --test parsing_improvements_test \
  --no-fail-fast

echo "Step 6: Documentation tests"
cargo test --package cqlite-core --doc --no-fail-fast

echo "Step 7: Build verification"
cargo build --package cqlite-core --all-features
'

# Workflow 2: Main CI (test job)
run_workflow "Main CI (test job)" '
echo "Running main CI test job"
cargo test --package cqlite-core --all-features -- --skip test_legacy_format_allows_blob_fallback_with_feature
'

# Workflow 3: SSTableDump Parity Gate
run_workflow "SSTableDump Parity Gate" '
echo "Step 1: Build release"
cargo build --release

echo "Step 2: Build SSTableDump parity tests"
cargo test --no-run --release --package cqlite-core \
  --test sstabledump_parity_statistics \
  --test sstabledump_parity_index \
  --test sstabledump_parity_summary

echo "Step 3: Run Statistics.db parity tests"
cargo test --release --package cqlite-core \
  --test sstabledump_parity_statistics \
  -- --nocapture

echo "Step 4: Run Summary.db parity tests"
cargo test --release --package cqlite-core \
  --test sstabledump_parity_summary \
  -- --nocapture

echo "✅ SSTableDump parity tests passed"
echo "Note: Index.db tests skipped (Issue #89 - refs-only datasets)"
'

# Summary
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}                    Validation Summary                      ${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════${NC}"

if [ ${#FAILED_WORKFLOWS[@]} -eq 0 ]; then
    echo -e "${GREEN}✅ SUCCESS: All $TOTAL_WORKFLOWS CI workflows validated locally${NC}"
    echo ""
    echo -e "${GREEN}You can safely push. CI should pass.${NC}"
    exit 0
else
    echo -e "${RED}❌ FAILURE: ${#FAILED_WORKFLOWS[@]} of $TOTAL_WORKFLOWS workflows failed${NC}"
    echo ""
    echo "Failed workflows:"
    for workflow in "${FAILED_WORKFLOWS[@]}"; do
        echo -e "  ${RED}✗${NC} $workflow"
    done
    echo ""
    echo -e "${RED}Fix the failures above before pushing.${NC}"
    exit 1
fi
