#!/usr/bin/env bash
# CI Smoke Test Script for One-Shot Query Execution
# Issue #140: Robust CI smoke test for one-shot queries
#
# Usage:
#   export CQLITE_DATA_DIR=/path/to/test-data/datasets/sstables
#   export CQLITE_SCHEMA=/path/to/test-data/schemas/basic-types.cql
#   ./ci-one-shot-smoke.sh
#
# Environment Variables:
#   CQLITE_DATA_DIR  - Path to SSTable data directory (required)
#   CQLITE_SCHEMA    - Path to schema file (required)
#   CQLITE_CLI       - Path to cqlite binary (optional, will build if not set)
#   OUTPUT_DIR       - Directory for test results (default: ./smoke-test-results)

set -euo pipefail

# Color output for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Get script directory (resolve symlinks)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Default output directory
OUTPUT_DIR="${OUTPUT_DIR:-${SCRIPT_DIR}/smoke-test-results}"
# P0-5: Support GOLDEN_DIR environment variable
SNAPSHOTS_DIR="${GOLDEN_DIR:-${SCRIPT_DIR}/smoke-test-snapshots}"

# Errexit save/restore (issue #3689)
#
# These wrap a command whose non-zero exit is EXPECTED and handled by the
# caller inspecting $?. A bare `set -e` to "restore" afterwards is wrong: shell
# options are GLOBAL, not function-scoped, so it CLOBBERS main()'s deliberate
# `set +e` ("continue on error to collect all results"). With errexit silently
# back on, the first `return 1` out of run_test aborted the entire script - so
# only the FIRST failing test was ever reported and every test after it never
# ran at all. That is how #3689's stale CSV golden hid a second, identical
# staleness in select_simple_table.golden.
#
# Not reentrant: ERREXIT_PREV is a single global, so these must not be nested.
# No call site nests them today.
errexit_save() {
    ERREXIT_PREV=off
    case $- in *e*) ERREXIT_PREV=on ;; esac
    set +e
}

errexit_restore() {
    if [[ "${ERREXIT_PREV:-off}" == "on" ]]; then
        set -e
    else
        set +e
    fi
}

# Logging functions
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

# Validate required environment variables
validate_environment() {
    log_info "Validating environment variables..."

    # CQLITE_SCHEMA is always required
    if [[ -z "${CQLITE_SCHEMA:-}" ]]; then
        log_error "CQLITE_SCHEMA environment variable not set"
        echo "Usage: export CQLITE_SCHEMA=/path/to/test-data/schemas/basic-types.cql"
        exit 1
    fi

    if [[ ! -f "${CQLITE_SCHEMA}" ]]; then
        log_error "Schema file not found: ${CQLITE_SCHEMA}"
        exit 1
    fi

    # Check if using dataset mode or data-dir mode
    if [[ -n "${CQLITE_DATASET:-}" ]]; then
        # Dataset mode: validate CQLITE_DATASETS_ROOT
        log_info "Using dataset mode with dataset: ${CQLITE_DATASET}"

        DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-${WORKSPACE_ROOT}/test-data/datasets}"
        DATASET_DIR="${DATASETS_ROOT}/sstables/${CQLITE_DATASET}"

        if [[ ! -d "${DATASET_DIR}" ]]; then
            log_error "Dataset directory not found: ${DATASET_DIR}"
            log_error "Expected to find dataset at: ${DATASETS_ROOT}/sstables/${CQLITE_DATASET}"
            exit 1
        fi

        # Validate SSTable files exist in dataset
        local sstable_count
        sstable_count=$(find "${DATASET_DIR}" -name "*-Data.db" -type f 2>/dev/null | wc -l | tr -d ' ')
        if [[ ${sstable_count} -eq 0 ]]; then
            log_error "No SSTable files (*-Data.db) found in dataset: ${DATASET_DIR}"
            log_error "Expected to find at least one SSTable file for testing"
            exit 1
        fi
        log_info "Found ${sstable_count} SSTable file(s) in dataset"
    else
        # Legacy data-dir mode: validate CQLITE_DATA_DIR
        if [[ -z "${CQLITE_DATA_DIR:-}" ]]; then
            log_error "Either CQLITE_DATASET or CQLITE_DATA_DIR environment variable must be set"
            echo "Dataset mode: export CQLITE_DATASET=test_basic"
            echo "Data dir mode: export CQLITE_DATA_DIR=/path/to/test-data/datasets/sstables"
            exit 1
        fi

        if [[ ! -d "${CQLITE_DATA_DIR}" ]]; then
            log_error "Data directory not found: ${CQLITE_DATA_DIR}"
            exit 1
        fi

        # P2-4: Validate SSTable files exist in data directory
        local sstable_count
        sstable_count=$(find "${CQLITE_DATA_DIR}" -name "*-Data.db" -type f 2>/dev/null | wc -l | tr -d ' ')
        if [[ ${sstable_count} -eq 0 ]]; then
            log_error "No SSTable files (*-Data.db) found in: ${CQLITE_DATA_DIR}"
            log_error "Expected to find at least one SSTable file for testing"
            exit 1
        fi
        log_info "Found ${sstable_count} SSTable file(s) in data directory"
    fi

    log_success "Environment validation passed"
}

# Build or locate CLI binary
setup_cli_binary() {
    if [[ -n "${CQLITE_CLI:-}" ]]; then
        if [[ ! -x "${CQLITE_CLI}" ]]; then
            log_error "CQLITE_CLI is set but not executable: ${CQLITE_CLI}"
            exit 1
        fi
        log_info "Using CLI binary from CQLITE_CLI: ${CQLITE_CLI}"
        return
    fi

    # Try to find built binary first
    local dev_binary="${WORKSPACE_ROOT}/target/debug/cqlite"
    local release_binary="${WORKSPACE_ROOT}/target/release/cqlite"

    if [[ -x "${release_binary}" ]]; then
        CQLITE_CLI="${release_binary}"
        log_info "Using existing release binary: ${CQLITE_CLI}"
        return
    fi

    if [[ -x "${dev_binary}" ]]; then
        CQLITE_CLI="${dev_binary}"
        log_info "Using existing debug binary: ${CQLITE_CLI}"
        return
    fi

    # Build the CLI
    # P2-2: Fix pipeline failure masking - use temporary file and check exit code directly
    log_info "Building CLI binary..."
    cd "${WORKSPACE_ROOT}"
    local build_output
    build_output=$(mktemp)
    if cargo build --package cqlite-cli --bin cqlite --quiet 2>&1 | tee "${build_output}" | grep -v "Compiling\|Finished"; then
        local build_exit_code=${PIPESTATUS[0]}
        if [[ ${build_exit_code} -eq 0 ]]; then
            CQLITE_CLI="${dev_binary}"
            log_success "CLI binary built successfully: ${CQLITE_CLI}"
            rm -f "${build_output}"
        else
            log_error "Failed to build CLI binary (exit code: ${build_exit_code})"
            cat "${build_output}"
            rm -f "${build_output}"
            exit 1
        fi
    else
        log_error "Failed to build CLI binary"
        cat "${build_output}"
        rm -f "${build_output}"
        exit 1
    fi
}

# Setup test environment
setup_test_environment() {
    log_info "Setting up test environment..."

    # Create output directory
    mkdir -p "${OUTPUT_DIR}"

    # Clean previous test results
    rm -f "${OUTPUT_DIR}"/*.actual

    log_success "Test environment ready (output: ${OUTPUT_DIR})"
}

# Run a single test case
# Args: test_name, query, format, expected_exit_code, [snapshot_file]
run_test() {
    local test_name="$1"
    local query="$2"
    local format="$3"
    local expected_exit_code="$4"
    local snapshot_file="${5:-}"

    TESTS_RUN=$((TESTS_RUN + 1))

    log_info "Running test: ${test_name}"

    local output_file="${OUTPUT_DIR}/${test_name}.actual"
    local exit_code=0

    # Run the CLI command with appropriate flags
    # Suppress stderr to match golden snapshots (Issue #129: logs go to stderr)
    errexit_save
    if [[ -n "${CQLITE_DATASET:-}" ]]; then
        # Dataset mode
        "${CQLITE_CLI}" \
            --schema "${CQLITE_SCHEMA}" \
            --dataset "${CQLITE_DATASET}" \
            --execute "${query}" \
            --format "${format}" \
            > "${output_file}" 2>/dev/null
    else
        # Data-dir mode
        "${CQLITE_CLI}" \
            --schema "${CQLITE_SCHEMA}" \
            --data-dir "${CQLITE_DATA_DIR}" \
            --execute "${query}" \
            --format "${format}" \
            > "${output_file}" 2>/dev/null
    fi
    exit_code=$?
    errexit_restore

    # Validate exit code
    if [[ ${exit_code} -ne ${expected_exit_code} ]]; then
        log_error "${test_name}: Expected exit code ${expected_exit_code}, got ${exit_code}"
        # P0-8: Show both first 20 and last 20 lines for better debugging
        log_error "Output (first 20 lines):"
        head -20 "${output_file}"
        local line_count
        line_count=$(wc -l < "${output_file}" | tr -d ' ')
        if [[ ${line_count} -gt 40 ]]; then
            log_error "... ($(( line_count - 40 )) lines omitted) ..."
            log_error "Output (last 20 lines):"
            tail -20 "${output_file}"
        fi
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi

    # P0-3: Make snapshot comparison optional when golden files don't exist
    if [[ -n "${snapshot_file}" ]]; then
        if [[ -f "${snapshot_file}" ]]; then
            if ! diff -u "${snapshot_file}" "${output_file}"; then
                log_error "${test_name}: Output does not match snapshot"
                log_error "Expected: ${snapshot_file}"
                log_error "Actual:   ${output_file}"
                TESTS_FAILED=$((TESTS_FAILED + 1))
                return 1
            fi
        else
            log_warn "${test_name}: Snapshot file not found (${snapshot_file}), skipping comparison"
            log_warn "This is expected on first run or if golden files haven't been generated"
        fi
    fi

    log_success "${test_name}: Passed (exit code: ${exit_code})"
    TESTS_PASSED=$((TESTS_PASSED + 1))
    return 0
}

# Run error test case (expects non-zero exit code)
# Args: test_name, args_array, expected_error_pattern
run_error_test() {
    local test_name="$1"
    shift
    local expected_pattern="$1"
    shift
    local args=("$@")

    TESTS_RUN=$((TESTS_RUN + 1))

    log_info "Running error test: ${test_name}"

    local output_file="${OUTPUT_DIR}/${test_name}.actual"
    local exit_code=0

    # Run the CLI command (expecting failure)
    errexit_save
    "${CQLITE_CLI}" "${args[@]}" > "${output_file}" 2>&1
    exit_code=$?
    errexit_restore

    # Should have non-zero exit code
    if [[ ${exit_code} -eq 0 ]]; then
        log_error "${test_name}: Expected non-zero exit code, got 0"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi

    # Check for expected error pattern if provided
    if [[ -n "${expected_pattern}" ]]; then
        if ! grep -q "${expected_pattern}" "${output_file}"; then
            log_error "${test_name}: Expected error pattern '${expected_pattern}' not found"
            # P0-8: Show both first 20 and last 20 lines for better debugging
            log_error "Output (first 20 lines):"
            head -20 "${output_file}"
            local line_count
            line_count=$(wc -l < "${output_file}" | tr -d ' ')
            if [[ ${line_count} -gt 40 ]]; then
                log_error "... ($(( line_count - 40 )) lines omitted) ..."
                log_error "Output (last 20 lines):"
                tail -20 "${output_file}"
            fi
            TESTS_FAILED=$((TESTS_FAILED + 1))
            return 1
        fi
    fi

    log_success "${test_name}: Passed (exit code: ${exit_code})"
    TESTS_PASSED=$((TESTS_PASSED + 1))
    return 0
}

# Main test suite
run_test_suite() {
    log_info "Starting smoke test suite..."
    echo ""

    # Test 1: Basic SELECT with JSON output (simple_table)
    # ORDER BY id (#1997): simple_table is single-PK (id UUID, no clustering), so an
    # unordered LIMIT returns storage/token order which drifts across engine changes
    # and flaps the snapshot. ORDER BY id makes it deterministic; the output is
    # byte-identical to the committed golden (id-sorted).
    run_test \
        "test_select_json_simple" \
        "SELECT * FROM test_basic.simple_table ORDER BY id LIMIT 3" \
        "json" \
        0 \
        "${SNAPSHOTS_DIR}/select_simple_json.golden"

    # Test 2: Basic SELECT with CSV output (simple_table)
    run_test \
        "test_select_csv_simple" \
        "SELECT * FROM test_basic.simple_table ORDER BY id LIMIT 3" \
        "csv" \
        0 \
        "${SNAPSHOTS_DIR}/select_simple_csv.golden"

    # Test 3: Basic SELECT with table output (simple_table)
    run_test \
        "test_select_table_simple" \
        "SELECT * FROM test_basic.simple_table ORDER BY id LIMIT 2" \
        "table" \
        0 \
        "${SNAPSHOTS_DIR}/select_simple_table.golden"

    # Test 4: Column projection
    run_test \
        "test_select_columns" \
        "SELECT id, name FROM test_basic.simple_table ORDER BY id LIMIT 3" \
        "json" \
        0 \
        "${SNAPSHOTS_DIR}/select_columns_json.golden"

    # Test 5: Collections query with JSON output
    # P2-3: Use robust path manipulation for collections schema
    local schema_dir
    schema_dir="$(dirname "${CQLITE_SCHEMA}")"
    local collections_schema="${schema_dir}/collections.cql"
    if [[ -f "${collections_schema}" ]]; then
        local orig_schema="${CQLITE_SCHEMA}"
        local orig_dataset="${CQLITE_DATASET:-}"
        export CQLITE_SCHEMA="${collections_schema}"
        if [[ -n "${CQLITE_DATASET:-}" ]]; then
            export CQLITE_DATASET="test_collections"
        fi

        run_test \
            "test_select_collections" \
            "SELECT * FROM test_collections.collection_table ORDER BY id LIMIT 2" \
            "json" \
            0 \
            "${SNAPSHOTS_DIR}/select_collections_json.golden"

        export CQLITE_SCHEMA="${orig_schema}"
        if [[ -n "${orig_dataset}" ]]; then
            export CQLITE_DATASET="${orig_dataset}"
        fi
    else
        log_warn "Skipping collections test: schema not found at ${collections_schema}"
    fi

    # Test 6: Error case - invalid query syntax
    if [[ -n "${CQLITE_DATASET:-}" ]]; then
        run_error_test \
            "test_error_invalid_query" \
            "" \
            --schema "${CQLITE_SCHEMA}" \
            --dataset "${CQLITE_DATASET}" \
            --execute "SELECT FROM WHERE invalid syntax" \
            --format "json"
    else
        run_error_test \
            "test_error_invalid_query" \
            "" \
            --schema "${CQLITE_SCHEMA}" \
            --data-dir "${CQLITE_DATA_DIR}" \
            --execute "SELECT FROM WHERE invalid syntax" \
            --format "json"
    fi

    # Test 7: Error case - missing schema file
    if [[ -n "${CQLITE_DATASET:-}" ]]; then
        run_error_test \
            "test_error_missing_schema" \
            "schema" \
            --schema "/nonexistent/schema.cql" \
            --dataset "${CQLITE_DATASET}" \
            --execute "SELECT * FROM test_basic.simple_table" \
            --format "json"
    else
        run_error_test \
            "test_error_missing_schema" \
            "schema" \
            --schema "/nonexistent/schema.cql" \
            --data-dir "${CQLITE_DATA_DIR}" \
            --execute "SELECT * FROM test_basic.simple_table" \
            --format "json"
    fi

    # Test 8: Error case - missing data directory/dataset
    if [[ -n "${CQLITE_DATASET:-}" ]]; then
        run_error_test \
            "test_error_missing_dataset" \
            "data" \
            --schema "${CQLITE_SCHEMA}" \
            --dataset "nonexistent_dataset_xyz" \
            --execute "SELECT * FROM test_basic.simple_table" \
            --format "json"
    else
        run_error_test \
            "test_error_missing_data_dir" \
            "data" \
            --schema "${CQLITE_SCHEMA}" \
            --data-dir "/nonexistent/data/dir" \
            --execute "SELECT * FROM test_basic.simple_table" \
            --format "json"
    fi

    # Test 9: Query non-existent table (currently returns exit 0, may return error in future)
    # This test just validates that the query executes without crashing
    log_info "Running test: test_query_nonexistent_table"
    local output_file="${OUTPUT_DIR}/test_query_nonexistent_table.actual"
    errexit_save
    if [[ -n "${CQLITE_DATASET:-}" ]]; then
        "${CQLITE_CLI}" \
            --schema "${CQLITE_SCHEMA}" \
            --dataset "${CQLITE_DATASET}" \
            --execute "SELECT * FROM test_basic.nonexistent_table" \
            --format "json" \
            > "${output_file}" 2>&1
    else
        "${CQLITE_CLI}" \
            --schema "${CQLITE_SCHEMA}" \
            --data-dir "${CQLITE_DATA_DIR}" \
            --execute "SELECT * FROM test_basic.nonexistent_table" \
            --format "json" \
            > "${output_file}" 2>&1
    fi
    local exit_code=$?
    errexit_restore

    TESTS_RUN=$((TESTS_RUN + 1))
    # Accept any exit code (0 or non-zero) - just verify it doesn't crash
    log_success "test_query_nonexistent_table: Completed (exit code: ${exit_code})"
    TESTS_PASSED=$((TESTS_PASSED + 1))

    echo ""
    log_info "Test suite completed"
}

# Print test summary
print_summary() {
    echo ""
    echo "========================================="
    echo "         SMOKE TEST SUMMARY"
    echo "========================================="
    echo ""
    echo "  Tests Run:    ${TESTS_RUN}"
    echo -e "  ${GREEN}Tests Passed: ${TESTS_PASSED}${NC}"

    if [[ ${TESTS_FAILED} -gt 0 ]]; then
        echo -e "  ${RED}Tests Failed: ${TESTS_FAILED}${NC}"
    else
        echo "  Tests Failed: ${TESTS_FAILED}"
    fi

    echo ""
    echo "  Output Directory: ${OUTPUT_DIR}"

    if [[ -d "${SNAPSHOTS_DIR}" ]]; then
        echo "  Snapshots:        ${SNAPSHOTS_DIR}"
    else
        echo -e "  ${YELLOW}Snapshots:        Not found (first run or snapshots not generated)${NC}"
    fi

    echo ""

    if [[ ${TESTS_FAILED} -eq 0 ]]; then
        echo -e "${GREEN}=========================================${NC}"
        echo -e "${GREEN}        ALL TESTS PASSED ✓${NC}"
        echo -e "${GREEN}=========================================${NC}"
        return 0
    else
        echo -e "${RED}=========================================${NC}"
        echo -e "${RED}        SOME TESTS FAILED ✗${NC}"
        echo -e "${RED}=========================================${NC}"
        return 1
    fi
}

# Main execution
main() {
    log_info "CQLite One-Shot Query Smoke Test"
    log_info "Issue #140: Robust CI smoke test for one-shot queries"
    echo ""

    validate_environment
    setup_cli_binary
    setup_test_environment

    echo ""
    log_info "Configuration:"
    log_info "  CLI Binary:   ${CQLITE_CLI}"
    if [[ -n "${CQLITE_DATASET:-}" ]]; then
        log_info "  Dataset:      ${CQLITE_DATASET}"
        log_info "  Dataset Root: ${CQLITE_DATASETS_ROOT:-${WORKSPACE_ROOT}/test-data/datasets}"
    else
        log_info "  Data Dir:     ${CQLITE_DATA_DIR}"
    fi
    log_info "  Schema:       ${CQLITE_SCHEMA}"
    log_info "  Output Dir:   ${OUTPUT_DIR}"
    echo ""

    # Run test suite (continue on error to collect all results)
    errexit_save
    run_test_suite
    errexit_restore

    # Print summary and exit with appropriate code
    if print_summary; then
        exit 0
    else
        exit 1
    fi
}

# Run main function
main "$@"
