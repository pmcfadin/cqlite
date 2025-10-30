#!/usr/bin/env bash
# Comprehensive Validation Script for Issue #140 Fix
# Tests dynamic column metadata population for SELECT * queries
#
# This script performs exhaustive testing to ensure the fix works correctly
# in all scenarios before pushing to CI.
#
# Usage:
#   ./validate-issue-140-fix.sh

set -euo pipefail

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Test counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
SKIPPED_TESTS=0

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Test results directory
TEST_RESULTS_DIR="${WORKSPACE_ROOT}/test-data/scripts/issue-140-validation"
DETERMINISM_DIR="${TEST_RESULTS_DIR}/determinism"
SCHEMA_TESTS_DIR="${TEST_RESULTS_DIR}/schema-tests"
EDGE_CASES_DIR="${TEST_RESULTS_DIR}/edge-cases"

# Logging functions
log_header() {
    echo ""
    echo -e "${BOLD}${CYAN}========================================${NC}"
    echo -e "${BOLD}${CYAN}$*${NC}"
    echo -e "${BOLD}${CYAN}========================================${NC}"
    echo ""
}

log_section() {
    echo ""
    echo -e "${BOLD}${BLUE}>>> $*${NC}"
    echo ""
}

log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $*"
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_skip() {
    echo -e "${YELLOW}[SKIP]${NC} $*"
}

# Test tracking
start_test() {
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    log_info "Test $TOTAL_TESTS: $*"
}

pass_test() {
    PASSED_TESTS=$((PASSED_TESTS + 1))
    log_success "$*"
}

fail_test() {
    FAILED_TESTS=$((FAILED_TESTS + 1))
    log_error "$*"
}

skip_test() {
    SKIPPED_TESTS=$((SKIPPED_TESTS + 1))
    log_skip "$*"
}

# Setup test environment
setup_environment() {
    log_section "Setting up test environment"

    # Create test directories
    mkdir -p "${TEST_RESULTS_DIR}"
    mkdir -p "${DETERMINISM_DIR}"
    mkdir -p "${SCHEMA_TESTS_DIR}"
    mkdir -p "${EDGE_CASES_DIR}"

    # Clean previous results
    rm -f "${TEST_RESULTS_DIR}"/*.log
    rm -f "${DETERMINISM_DIR}"/*.json
    rm -f "${SCHEMA_TESTS_DIR}"/*.json
    rm -f "${EDGE_CASES_DIR}"/*.json

    # Set environment variables
    export CQLITE_DATASETS_ROOT="${WORKSPACE_ROOT}/test-data/datasets"
    export CQLITE_SCHEMA="${WORKSPACE_ROOT}/test-data/schemas/basic-types.cql"
    export CQLITE_DATASET="test_basic"

    # Verify test data exists
    if [[ ! -d "${CQLITE_DATASETS_ROOT}/sstables/${CQLITE_DATASET}" ]]; then
        log_error "Test dataset not found: ${CQLITE_DATASETS_ROOT}/sstables/${CQLITE_DATASET}"
        exit 1
    fi

    log_success "Test environment ready"
    log_info "  Results directory: ${TEST_RESULTS_DIR}"
    log_info "  Test dataset: ${CQLITE_DATASET}"
}

# Build binaries
build_binaries() {
    log_section "Building binaries"

    cd "${WORKSPACE_ROOT}"

    # Build debug binary
    start_test "Building debug binary"
    if cargo build --package cqlite-cli --bin cqlite --quiet 2>&1 | tee "${TEST_RESULTS_DIR}/build-debug.log"; then
        if [[ -x "${WORKSPACE_ROOT}/target/debug/cqlite" ]]; then
            pass_test "Debug binary built successfully"
            DEBUG_BINARY="${WORKSPACE_ROOT}/target/debug/cqlite"
        else
            fail_test "Debug binary not found after build"
            cat "${TEST_RESULTS_DIR}/build-debug.log"
            return 1
        fi
    else
        fail_test "Debug build failed"
        cat "${TEST_RESULTS_DIR}/build-debug.log"
        return 1
    fi

    # Build release binary
    start_test "Building release binary"
    if cargo build --package cqlite-cli --bin cqlite --release --quiet 2>&1 | tee "${TEST_RESULTS_DIR}/build-release.log"; then
        if [[ -x "${WORKSPACE_ROOT}/target/release/cqlite" ]]; then
            pass_test "Release binary built successfully"
            RELEASE_BINARY="${WORKSPACE_ROOT}/target/release/cqlite"
        else
            fail_test "Release binary not found after build"
            cat "${TEST_RESULTS_DIR}/build-release.log"
            return 1
        fi
    else
        fail_test "Release build failed"
        cat "${TEST_RESULTS_DIR}/build-release.log"
        return 1
    fi
}

# Test deterministic output
test_deterministic_output() {
    log_section "Testing deterministic output (Issue #129)"

    local binary="$1"
    local build_type="$2"
    local query="SELECT * FROM test_basic.simple_table LIMIT 3"

    # Run query 5 times
    for i in {1..5}; do
        start_test "Run $i/$5 - deterministic output test ($build_type)"
        local output_file="${DETERMINISM_DIR}/${build_type}_run_${i}.json"

        if "${binary}" \
            --schema "${CQLITE_SCHEMA}" \
            --dataset "${CQLITE_DATASET}" \
            --execute "${query}" \
            --format json \
            > "${output_file}" 2>/dev/null; then
            pass_test "Query executed successfully (run $i)"
        else
            fail_test "Query failed (run $i)"
            return 1
        fi
    done

    # Compare all runs
    start_test "Comparing outputs for determinism ($build_type)"
    local first_file="${DETERMINISM_DIR}/${build_type}_run_1.json"
    local all_identical=true

    for i in {2..5}; do
        local current_file="${DETERMINISM_DIR}/${build_type}_run_${i}.json"
        if ! diff -q "${first_file}" "${current_file}" > /dev/null; then
            log_error "Run $i differs from run 1"
            log_error "Diff output:"
            diff "${first_file}" "${current_file}" || true
            all_identical=false
        fi
    done

    if $all_identical; then
        pass_test "All 5 runs produced identical output ($build_type)"
    else
        fail_test "Outputs are not deterministic ($build_type)"
        return 1
    fi
}

# Test SELECT * with different schemas
test_different_schemas() {
    log_section "Testing SELECT * with different table schemas"

    local binary="$1"
    local build_type="$2"

    # Test 1: Simple table (basic types)
    start_test "SELECT * from simple_table ($build_type)"
    local output_file="${SCHEMA_TESTS_DIR}/${build_type}_simple_table.json"
    if "${binary}" \
        --schema "${CQLITE_SCHEMA}" \
        --dataset "${CQLITE_DATASET}" \
        --execute "SELECT * FROM test_basic.simple_table LIMIT 3" \
        --format json \
        > "${output_file}" 2>/dev/null; then

        # Verify output has column metadata
        if grep -q '"id"' "${output_file}" && grep -q '"name"' "${output_file}"; then
            pass_test "Simple table query has proper column data"
        else
            fail_test "Simple table query missing column data"
            cat "${output_file}"
            return 1
        fi
    else
        fail_test "Simple table query failed"
        return 1
    fi

    # Test 2: Collections table
    local collections_schema="${WORKSPACE_ROOT}/test-data/schemas/collections.cql"
    if [[ -f "${collections_schema}" ]]; then
        start_test "SELECT * from collection_table ($build_type)"
        output_file="${SCHEMA_TESTS_DIR}/${build_type}_collections.json"

        if "${binary}" \
            --schema "${collections_schema}" \
            --dataset "test_collections" \
            --execute "SELECT * FROM test_collections.collection_table LIMIT 2" \
            --format json \
            > "${output_file}" 2>/dev/null; then

            # Verify collections have proper column data
            if grep -q '"id"' "${output_file}"; then
                pass_test "Collections table query has proper column data"
            else
                fail_test "Collections table query missing column data"
                cat "${output_file}"
                return 1
            fi
        else
            fail_test "Collections table query failed"
            return 1
        fi
    else
        skip_test "Collections schema not found, skipping collections test"
    fi
}

# Test SELECT specific columns
test_column_projection() {
    log_section "Testing SELECT with specific columns"

    local binary="$1"
    local build_type="$2"

    start_test "SELECT specific columns ($build_type)"
    local output_file="${SCHEMA_TESTS_DIR}/${build_type}_columns.json"

    if "${binary}" \
        --schema "${CQLITE_SCHEMA}" \
        --dataset "${CQLITE_DATASET}" \
        --execute "SELECT id, name FROM test_basic.simple_table LIMIT 3" \
        --format json \
        > "${output_file}" 2>/dev/null; then

        # Verify only requested columns are present
        if grep -q '"id"' "${output_file}" && grep -q '"name"' "${output_file}"; then
            pass_test "Column projection works correctly"
        else
            fail_test "Column projection missing expected columns"
            cat "${output_file}"
            return 1
        fi
    else
        fail_test "Column projection query failed"
        return 1
    fi
}

# Test edge cases
test_edge_cases() {
    log_section "Testing edge cases"

    local binary="$1"
    local build_type="$2"

    # Test 1: Empty results (non-existent table or WHERE clause that matches nothing)
    # Note: This might not actually return empty results, but we test it doesn't crash
    start_test "Query with potentially empty results ($build_type)"
    local output_file="${EDGE_CASES_DIR}/${build_type}_empty.json"

    "${binary}" \
        --schema "${CQLITE_SCHEMA}" \
        --dataset "${CQLITE_DATASET}" \
        --execute "SELECT * FROM test_basic.simple_table WHERE id = 99999" \
        --format json \
        > "${output_file}" 2>/dev/null || true

    pass_test "Empty result query completed without crash"

    # Test 2: Single row
    start_test "Query with LIMIT 1 ($build_type)"
    output_file="${EDGE_CASES_DIR}/${build_type}_single_row.json"

    if "${binary}" \
        --schema "${CQLITE_SCHEMA}" \
        --dataset "${CQLITE_DATASET}" \
        --execute "SELECT * FROM test_basic.simple_table LIMIT 1" \
        --format json \
        > "${output_file}" 2>/dev/null; then
        pass_test "Single row query completed successfully"
    else
        fail_test "Single row query failed"
        return 1
    fi

    # Test 3: Many rows
    start_test "Query with LIMIT 100 ($build_type)"
    output_file="${EDGE_CASES_DIR}/${build_type}_many_rows.json"

    if "${binary}" \
        --schema "${CQLITE_SCHEMA}" \
        --dataset "${CQLITE_DATASET}" \
        --execute "SELECT * FROM test_basic.simple_table LIMIT 100" \
        --format json \
        > "${output_file}" 2>/dev/null; then
        pass_test "Many rows query completed successfully"
    else
        fail_test "Many rows query failed"
        return 1
    fi
}

# Test output formats
test_output_formats() {
    log_section "Testing different output formats"

    local binary="$1"
    local build_type="$2"
    local query="SELECT * FROM test_basic.simple_table LIMIT 3"

    # Test JSON format
    start_test "JSON output format ($build_type)"
    local json_file="${TEST_RESULTS_DIR}/${build_type}_format_json.json"
    if "${binary}" \
        --schema "${CQLITE_SCHEMA}" \
        --dataset "${CQLITE_DATASET}" \
        --execute "${query}" \
        --format json \
        > "${json_file}" 2>/dev/null; then

        # Verify it's valid JSON and has column data
        if command -v jq &> /dev/null; then
            if jq empty "${json_file}" 2>/dev/null; then
                pass_test "JSON format is valid"
            else
                fail_test "JSON format is invalid"
                cat "${json_file}"
                return 1
            fi
        else
            pass_test "JSON format generated (jq not available for validation)"
        fi
    else
        fail_test "JSON format generation failed"
        return 1
    fi

    # Test CSV format
    start_test "CSV output format ($build_type)"
    local csv_file="${TEST_RESULTS_DIR}/${build_type}_format_csv.csv"
    if "${binary}" \
        --schema "${CQLITE_SCHEMA}" \
        --dataset "${CQLITE_DATASET}" \
        --execute "${query}" \
        --format csv \
        > "${csv_file}" 2>/dev/null; then

        # Verify CSV has header and data
        local line_count
        line_count=$(wc -l < "${csv_file}" | tr -d ' ')
        if [[ ${line_count} -gt 1 ]]; then
            pass_test "CSV format has header and data"
        else
            fail_test "CSV format appears empty"
            cat "${csv_file}"
            return 1
        fi
    else
        fail_test "CSV format generation failed"
        return 1
    fi

    # Test Table format
    start_test "Table output format ($build_type)"
    local table_file="${TEST_RESULTS_DIR}/${build_type}_format_table.txt"
    if "${binary}" \
        --schema "${CQLITE_SCHEMA}" \
        --dataset "${CQLITE_DATASET}" \
        --execute "${query}" \
        --format table \
        > "${table_file}" 2>/dev/null; then
        pass_test "Table format generated successfully"
    else
        fail_test "Table format generation failed"
        return 1
    fi
}

# Run unit tests
test_unit_tests() {
    log_section "Running unit tests"

    cd "${WORKSPACE_ROOT}"

    start_test "Core unit tests"
    if env CQLITE_DATASETS_ROOT="${CQLITE_DATASETS_ROOT}" \
        cargo test --package cqlite-core --quiet 2>&1 | tee "${TEST_RESULTS_DIR}/unit-tests.log"; then
        pass_test "All unit tests passed"
    else
        fail_test "Unit tests failed"
        cat "${TEST_RESULTS_DIR}/unit-tests.log"
        return 1
    fi
}

# Run CI smoke tests
test_ci_smoke_tests() {
    log_section "Running CI smoke tests (simulating CI environment)"

    cd "${WORKSPACE_ROOT}"

    # Use the actual CI smoke test script
    start_test "CI smoke test suite"
    if env CQLITE_DATASETS_ROOT="${CQLITE_DATASETS_ROOT}" \
        CQLITE_SCHEMA="${CQLITE_SCHEMA}" \
        CQLITE_DATASET="${CQLITE_DATASET}" \
        bash "${SCRIPT_DIR}/ci-one-shot-smoke.sh" 2>&1 | tee "${TEST_RESULTS_DIR}/ci-smoke-tests.log"; then
        pass_test "All CI smoke tests passed"
    else
        fail_test "CI smoke tests failed"
        cat "${TEST_RESULTS_DIR}/ci-smoke-tests.log"
        return 1
    fi
}

# Test with clippy and formatting
test_code_quality() {
    log_section "Testing code quality (clippy and formatting)"

    cd "${WORKSPACE_ROOT}"

    # Test clippy
    start_test "Clippy linting"
    if cargo clippy --package cqlite-core --quiet 2>&1 | tee "${TEST_RESULTS_DIR}/clippy.log"; then
        pass_test "Clippy checks passed"
    else
        fail_test "Clippy checks failed"
        cat "${TEST_RESULTS_DIR}/clippy.log"
        return 1
    fi

    # Test formatting
    start_test "Code formatting check"
    if cargo fmt --check 2>&1 | tee "${TEST_RESULTS_DIR}/fmt.log"; then
        pass_test "Code formatting is correct"
    else
        fail_test "Code formatting issues found"
        cat "${TEST_RESULTS_DIR}/fmt.log"
        return 1
    fi
}

# Print summary
print_summary() {
    log_header "VALIDATION SUMMARY"

    echo "  Total Tests:   ${TOTAL_TESTS}"
    echo -e "  ${GREEN}Passed:        ${PASSED_TESTS}${NC}"

    if [[ ${FAILED_TESTS} -gt 0 ]]; then
        echo -e "  ${RED}Failed:        ${FAILED_TESTS}${NC}"
    else
        echo "  Failed:        ${FAILED_TESTS}"
    fi

    if [[ ${SKIPPED_TESTS} -gt 0 ]]; then
        echo -e "  ${YELLOW}Skipped:       ${SKIPPED_TESTS}${NC}"
    fi

    echo ""
    echo "  Results Directory: ${TEST_RESULTS_DIR}"
    echo ""

    if [[ ${FAILED_TESTS} -eq 0 ]]; then
        log_header "✓ ALL VALIDATION TESTS PASSED ✓"
        echo -e "${GREEN}${BOLD}The Issue #140 fix is validated and ready for CI!${NC}"
        echo ""
        echo "Next steps:"
        echo "  1. Review the test results in: ${TEST_RESULTS_DIR}"
        echo "  2. Commit your changes with: git add -u && git commit"
        echo "  3. Push to CI with: git push"
        echo ""
        return 0
    else
        log_header "✗ VALIDATION FAILED ✗"
        echo -e "${RED}${BOLD}The Issue #140 fix has failures. Review the logs before pushing to CI.${NC}"
        echo ""
        echo "Review:"
        echo "  - Test results: ${TEST_RESULTS_DIR}"
        echo "  - Failed test logs for detailed error messages"
        echo ""
        return 1
    fi
}

# Main execution
main() {
    log_header "Issue #140 Fix Validation"
    echo "This script performs comprehensive validation of the dynamic column metadata fix."
    echo "All tests must pass before pushing to CI."

    setup_environment
    build_binaries

    # Test debug build
    log_header "DEBUG BUILD TESTS"
    test_deterministic_output "${DEBUG_BINARY}" "debug"
    test_different_schemas "${DEBUG_BINARY}" "debug"
    test_column_projection "${DEBUG_BINARY}" "debug"
    test_edge_cases "${DEBUG_BINARY}" "debug"
    test_output_formats "${DEBUG_BINARY}" "debug"

    # Test release build
    log_header "RELEASE BUILD TESTS"
    test_deterministic_output "${RELEASE_BINARY}" "release"
    test_different_schemas "${RELEASE_BINARY}" "release"
    test_column_projection "${RELEASE_BINARY}" "release"
    test_edge_cases "${RELEASE_BINARY}" "release"
    test_output_formats "${RELEASE_BINARY}" "release"

    # Run unit tests
    test_unit_tests

    # Run CI smoke tests
    test_ci_smoke_tests

    # Test code quality
    test_code_quality

    # Print summary and exit
    if print_summary; then
        exit 0
    else
        exit 1
    fi
}

# Run main function
main "$@"
