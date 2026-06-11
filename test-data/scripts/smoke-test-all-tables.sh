#!/usr/bin/env bash
# Comprehensive Smoke Test Script for All Test Tables
# Issue #200: Validate that all 33 nb test tables can be loaded successfully
# Issue #654: Also discover oa/da keyspaces (reported as SKIP-PENDING until VG3/VG4)
#
# This script discovers all test tables across all keyspaces:
#   - nb (enforced): test_basic, test_collections, test_timeseries, test_wide_rows
#   - oa (skip-pending, VG4): test_oa
#   - da/bti (skip-pending, future BTI epic): test_da
#
# Tables in SKIP_PENDING_KEYSPACES are discovered and listed, but not run
# through the read-sstable command. They appear explicitly in the summary
# as "SKIP-PENDING" so CI can see them (not silent, not failing).
#
# Test command used for each nb table:
#   cargo run --bin cqlite -- read-sstable <table_dir> --format json
#
# Usage:
#   ./smoke-test-all-tables.sh
#
# Environment Variables:
#   CQLITE_DATASETS_ROOT - Path to datasets directory (default: $PWD/test-data/datasets)
#   CQLITE_CLI           - Path to cqlite binary (optional, will build if not set)
#   OUTPUT_DIR           - Directory for test results (default: ./smoke-test-all-tables-results)

set -euo pipefail
# Production-grade error handling: -e (exit on error), -u (error on unset), -o pipefail (pipeline fails if any command fails)

# Color output for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test tracking arrays
declare -a PASSED_TABLES=()
declare -a FAILED_TABLES=()
declare -a FAILED_DETAILS=()
declare -a SKIPPED_PENDING_TABLES=()

# Get script directory (resolve symlinks)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Default configuration
DATASETS_ROOT="${CQLITE_DATASETS_ROOT:-${WORKSPACE_ROOT}/test-data/datasets}"
SSTABLES_DIR="${DATASETS_ROOT}/sstables"
OUTPUT_DIR="${OUTPUT_DIR:-${SCRIPT_DIR}/smoke-test-all-tables-results}"

# Enforced keyspaces (must all pass, failures exit non-zero)
KEYSPACES=("test_basic" "test_collections" "test_timeseries" "test_wide_rows")

# Skip-pending keyspaces (Issue #654):
#   - test_oa: oa format (BIG) - skip until VG4 (oa parser lands)
#   - test_da: da format (BTI) - skip until future BTI read epic
# These keyspaces are discovered and listed explicitly as SKIP-PENDING,
# but are not run through read-sstable (would produce parse errors).
SKIP_PENDING_KEYSPACES=("test_oa" "test_da")
# Reason per keyspace (parallel arrays, bash 3.x compatible)
SKIP_PENDING_KEYSPACE_NAMES=("test_oa" "test_da")
SKIP_PENDING_KEYSPACE_REASONS=("oa-format parsing not yet implemented (lands in VG4)" "da/BTI-format parsing not yet implemented (future BTI epic)")

# Get skip reason for a keyspace (bash 3.x compatible, no associative arrays)
get_skip_reason() {
    local ks="$1"
    local i
    for i in "${!SKIP_PENDING_KEYSPACE_NAMES[@]}"; do
        if [[ "${SKIP_PENDING_KEYSPACE_NAMES[$i]}" == "$ks" ]]; then
            echo "${SKIP_PENDING_KEYSPACE_REASONS[$i]}"
            return
        fi
    done
    echo "pending"
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

# Detect timeout command (GNU timeout or macOS gtimeout)
detect_timeout_command() {
    if command -v timeout >/dev/null 2>&1; then
        TIMEOUT_CMD="timeout 30s"
    elif command -v gtimeout >/dev/null 2>&1; then
        TIMEOUT_CMD="gtimeout 30s"  # From homebrew coreutils on macOS
    else
        TIMEOUT_CMD=""
        log_warn "timeout command not found - tests may hang indefinitely"
        log_warn "On macOS, install with: brew install coreutils"
    fi
}

# Validate environment
validate_environment() {
    log_info "Validating environment..."

    if [[ ! -d "${SSTABLES_DIR}" ]]; then
        log_error "SSTables directory not found: ${SSTABLES_DIR}"
        log_error "Set CQLITE_DATASETS_ROOT to the correct path or run from workspace root"
        exit 1
    fi

    # Verify all enforced keyspaces exist
    local missing_keyspaces=()
    for keyspace in "${KEYSPACES[@]}"; do
        if [[ ! -d "${SSTABLES_DIR}/${keyspace}" ]]; then
            missing_keyspaces+=("${keyspace}")
        fi
    done

    if [[ ${#missing_keyspaces[@]} -gt 0 ]]; then
        log_error "Missing keyspaces: ${missing_keyspaces[*]}"
        log_error "Expected keyspaces: ${KEYSPACES[*]}"
        exit 1
    fi

    # Warn (but do not fail) if skip-pending keyspaces are absent
    for keyspace in "${SKIP_PENDING_KEYSPACES[@]}"; do
        if [[ ! -d "${SSTABLES_DIR}/${keyspace}" ]]; then
            log_warn "Skip-pending keyspace not present (OK): ${keyspace}"
        fi
    done

    log_success "Environment validation passed"
    log_info "  SSTables directory: ${SSTABLES_DIR}"
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
    log_info "Building CLI binary..."
    cd "${WORKSPACE_ROOT}"
    local build_output
    build_output=$(mktemp)

    # Build CLI binary and capture output
    if cargo build --package cqlite-cli --bin cqlite --quiet 2>&1 | tee "${build_output}"; then
        CQLITE_CLI="${dev_binary}"
        log_success "CLI binary built successfully: ${CQLITE_CLI}"
        rm -f "${build_output}"
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

    # Validate OUTPUT_DIR is safe (defense in depth)
    if [[ -z "${OUTPUT_DIR}" || "${OUTPUT_DIR}" == "/" || "${OUTPUT_DIR}" == "${HOME}" ]]; then
        log_error "Invalid or unsafe OUTPUT_DIR: ${OUTPUT_DIR}"
        exit 1
    fi

    # Create output directory
    mkdir -p "${OUTPUT_DIR}"

    # Clean previous test results (safely - directory validated above)
    if [[ -d "${OUTPUT_DIR}" ]]; then
        rm -f "${OUTPUT_DIR}"/*.json 2>/dev/null || true
    fi

    log_success "Test environment ready (output: ${OUTPUT_DIR})"
}

# Extract table name from directory (remove UUID suffix)
# Args: table_dir_name
extract_table_name() {
    local dir_name="$1"
    # Remove UUID suffix pattern: -XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    echo "${dir_name}" | sed 's/-[0-9a-f]\{32\}$//'
}

# Discover all table directories in all test keyspaces
# Returns array of "keyspace/table_dir" paths
discover_tables() {
    local tables=()

    for keyspace in "${KEYSPACES[@]}"; do
        local keyspace_dir="${SSTABLES_DIR}/${keyspace}"

        if [[ ! -d "${keyspace_dir}" ]]; then
            log_warn "Keyspace directory not found: ${keyspace_dir}"
            continue
        fi

        # Find all table directories (directories containing Data.db files)
        while IFS= read -r table_dir; do
            local table_dir_name
            table_dir_name=$(basename "${table_dir}")
            tables+=("${keyspace}/${table_dir_name}")
        done < <(find "${keyspace_dir}" -maxdepth 1 -type d -name "*-*" | sort)
    done

    printf '%s\n' "${tables[@]}"
}

# Test a single table
# Args: keyspace/table_dir
test_table() {
    local table_path="$1"
    local keyspace
    keyspace=$(dirname "${table_path}")
    local table_dir_name
    table_dir_name=$(basename "${table_path}")
    local table_name
    table_name=$(extract_table_name "${table_dir_name}")

    local full_table_path="${SSTABLES_DIR}/${table_path}"
    local qualified_name="${keyspace}.${table_name}"

    # Find Data.db file
    # Exclude macOS AppleDouble resource fork sidecar files (._*-Data.db) which are
    # 4 KB metadata files that look like SSTables to a naive *-Data.db glob (Issue #481).
    local data_db_file
    data_db_file=$(find "${full_table_path}" -name "*-Data.db" -type f -not -name "._*" | head -1)

    if [[ -z "${data_db_file}" ]]; then
        log_error "${qualified_name} ... FAIL (no Data.db file found)"
        FAILED_TABLES+=("${qualified_name}")
        FAILED_DETAILS+=("${qualified_name}: No Data.db file found in ${full_table_path}")
        return 1
    fi

    # Find corresponding JSONL file
    local jsonl_file
    jsonl_file=$(find "${full_table_path}" -name "*.jsonl" -type f | head -1)

    local output_file="${OUTPUT_DIR}/${keyspace}_${table_name}.json"
    local exit_code=0

    # Run read-sstable command with Data.db file directly
    # Use timeout if available to prevent hangs, suppress stderr (Issue #129: logs go to stderr)
    set +e
    if [[ -n "${TIMEOUT_CMD}" ]]; then
        ${TIMEOUT_CMD} "${CQLITE_CLI}" read-sstable "${data_db_file}" --format json > "${output_file}" 2>/dev/null
        exit_code=$?
    else
        # No timeout available - run without it
        "${CQLITE_CLI}" read-sstable "${data_db_file}" --format json > "${output_file}" 2>/dev/null
        exit_code=$?
    fi
    set -e

    # Check for timeout (exit code 124 for GNU timeout, 143 for some implementations)
    if [[ -n "${TIMEOUT_CMD}" && ( ${exit_code} -eq 124 || ${exit_code} -eq 143 ) ]]; then
        log_error "${qualified_name} ... FAIL (timeout after 30s)"
        FAILED_TABLES+=("${qualified_name}")
        FAILED_DETAILS+=("${qualified_name}: Command timed out after 30 seconds")
        return 1
    fi

    # Test 1: Check exit code
    if [[ ${exit_code} -ne 0 ]]; then
        log_error "${qualified_name} ... FAIL (exit code: ${exit_code})"
        FAILED_TABLES+=("${qualified_name}")
        # Store simple failure message (detailed output available in ${output_file})
        FAILED_DETAILS+=("${qualified_name}: Exit code ${exit_code}, see ${output_file}")
        return 1  # Early return on failure
    fi

    # Test 2: Validate output contains JSON (at least one '{')
    set +e
    grep -q '{' "${output_file}"
    local grep_result=$?
    set -e

    if [[ ${grep_result} -ne 0 ]]; then
        log_error "${qualified_name} ... FAIL (no JSON output)"
        FAILED_TABLES+=("${qualified_name}")
        FAILED_DETAILS+=("${qualified_name}: Output does not contain valid JSON objects")
        return 1
    fi

    # Test 3: Validate we got some data
    # Note: Row count comparison is skipped because JSONL format (sstabledump)
    # represents partitions (one line per partition with nested rows), while
    # read-sstable JSON output represents individual entries. The formats are
    # incompatible for direct line count comparison.
    local entry_count
    set +e
    entry_count=$(grep -c '^  {' "${output_file}")
    local grep_exit=$?
    set -e
    # grep -c returns 1 if no matches, which is fine
    if [[ ${grep_exit} -gt 1 ]]; then
        entry_count=0
    fi

    if [[ ${entry_count} -eq 0 ]]; then
        log_error "${qualified_name} ... FAIL (no entries found in output)"
        FAILED_TABLES+=("${qualified_name}")
        FAILED_DETAILS+=("${qualified_name}: No entries found in JSON output")
        return 1
    fi

    # Success - table loaded and produced entries
    if [[ -n "${jsonl_file}" && -f "${jsonl_file}" ]]; then
        local partition_count
        set +e
        partition_count=$(wc -l < "${jsonl_file}" | tr -d ' ')
        set -e
        log_success "${qualified_name} ... PASS (${entry_count} entries, ${partition_count} partitions in reference)"
    else
        log_warn "${qualified_name} ... PASS (${entry_count} entries, no JSONL reference)"
    fi

    PASSED_TABLES+=("${qualified_name}")
    return 0
}

# Discover and register skip-pending tables (oa, da)
# These are listed in the summary but not run through read-sstable.
register_skip_pending_tables() {
    for keyspace in "${SKIP_PENDING_KEYSPACES[@]}"; do
        local keyspace_dir="${SSTABLES_DIR}/${keyspace}"
        if [[ ! -d "${keyspace_dir}" ]]; then
            continue
        fi
        while IFS= read -r table_dir; do
            local table_dir_name
            table_dir_name=$(basename "${table_dir}")
            local table_name
            table_name=$(extract_table_name "${table_dir_name}")
            local qualified_name="${keyspace}.${table_name}"
            local reason
            reason=$(get_skip_reason "${keyspace}")
            log_warn "${qualified_name} ... SKIP-PENDING (${reason})"
            SKIPPED_PENDING_TABLES+=("${qualified_name} [${reason}]")
        done < <(find "${keyspace_dir}" -maxdepth 1 -type d -name "*-*" | sort)
    done
}

# Run all table tests
run_all_tests() {
    log_info "Discovering test tables..."

    local tables=()
    while IFS= read -r table_path; do
        tables+=("${table_path}")
    done < <(discover_tables)

    local total_tables=${#tables[@]}

    if [[ ${total_tables} -eq 0 ]]; then
        log_error "No test tables discovered in ${SSTABLES_DIR}"
        exit 1
    fi

    log_info "Found ${total_tables} tables across ${#KEYSPACES[@]} keyspaces"
    echo ""

    log_info "Starting table loading tests..."
    echo ""

    # Test each table (continue on failure to test all tables)
    # Temporarily disable errexit for the entire loop to allow failures
    set +e
    for table_path in "${tables[@]}"; do
        test_table "${table_path}" || true  # Continue even if test fails
    done
    set -e

    echo ""
    log_info "Checking skip-pending keyspaces (oa/da - not enforced yet)..."
    echo ""
    register_skip_pending_tables

    echo ""
    log_info "All table tests completed"
}

# Print comprehensive test summary
print_summary() {
    local total_tables=$((${#PASSED_TABLES[@]} + ${#FAILED_TABLES[@]}))

    echo ""
    echo "========================================="
    echo "    SMOKE TEST SUMMARY - ALL TABLES"
    echo "========================================="
    echo ""
    echo "  Total Tables Tested: ${total_tables}/33 (nb enforced)"
    echo -e "  ${GREEN}Passed:              ${#PASSED_TABLES[@]}${NC}"

    if [[ ${#FAILED_TABLES[@]} -gt 0 ]]; then
        echo -e "  ${RED}Failed:              ${#FAILED_TABLES[@]}${NC}"
    else
        echo "  Failed:              ${#FAILED_TABLES[@]}"
    fi

    if [[ ${#SKIPPED_PENDING_TABLES[@]} -gt 0 ]]; then
        echo -e "  ${YELLOW}Skip-pending:        ${#SKIPPED_PENDING_TABLES[@]} (oa/da - not enforced until VG4/BTI epic)${NC}"
    fi

    echo ""
    echo "  Output Directory:    ${OUTPUT_DIR}"
    echo ""

    # List failed tables with details if any
    if [[ ${#FAILED_TABLES[@]} -gt 0 ]]; then
        echo -e "${RED}Failed Tables:${NC}"
        echo ""
        for detail in "${FAILED_DETAILS[@]}"; do
            echo -e "${RED}  • ${detail}${NC}"
        done
        echo ""
    fi

    # List skip-pending tables
    if [[ ${#SKIPPED_PENDING_TABLES[@]} -gt 0 ]]; then
        echo -e "${YELLOW}Skip-Pending Tables (fixtures present, parser not yet wired):${NC}"
        echo ""
        for entry in "${SKIPPED_PENDING_TABLES[@]}"; do
            echo -e "${YELLOW}  • ${entry}${NC}"
        done
        echo ""
    fi

    if [[ ${#FAILED_TABLES[@]} -eq 0 ]]; then
        echo -e "${GREEN}=========================================${NC}"
        echo -e "${GREEN}  All nb tables passed smoke test${NC}"
        echo -e "${GREEN}  oa/da tables present but skip-pending${NC}"
        echo -e "${GREEN}=========================================${NC}"
        return 0
    else
        echo -e "${RED}=========================================${NC}"
        echo -e "${RED}  Some nb tables failed${NC}"
        echo -e "${RED}=========================================${NC}"
        return 1
    fi
}

# Main execution
main() {
    log_info "CQLite Comprehensive Table Loading Smoke Test"
    log_info "Issue #200: Validate all 33 nb test tables load successfully"
    log_info "Issue #654: oa/da tables discovered and listed as SKIP-PENDING"
    echo ""

    detect_timeout_command
    validate_environment
    setup_cli_binary
    setup_test_environment

    echo ""
    log_info "Configuration:"
    log_info "  CLI Binary:         ${CQLITE_CLI}"
    log_info "  Datasets Root:      ${DATASETS_ROOT}"
    log_info "  SSTables Directory: ${SSTABLES_DIR}"
    log_info "  Output Directory:   ${OUTPUT_DIR}"
    log_info "  Enforced Keyspaces: ${KEYSPACES[*]}"
    log_info "  Skip-Pending:       ${SKIP_PENDING_KEYSPACES[*]}"
    echo ""

    # Run all tests (continue on error to collect all results)
    set +e
    run_all_tests
    set -e

    # Print summary and exit with appropriate code
    if print_summary; then
        exit 0
    else
        exit 1
    fi
}

# Run main function
main "$@"
