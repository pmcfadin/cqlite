#!/bin/bash
#
# Comprehensive CQLite CLI Test Runner
# Tests CLI functionality with real Cassandra SSTable files
#

set -euo pipefail

# Configuration
CLI_BINARY="target/release/cqlite"
TEST_DATA_DIR="test-env/cassandra5/data/cassandra5-sstables"
SCHEMA_DIR="cqlite-cli/tests/test_data/schemas"
LOG_FILE="cli_test_results.log"
VERBOSE=${VERBOSE:-false}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

success() {
    echo -e "${GREEN}✅ $1${NC}" | tee -a "$LOG_FILE"
}

warning() {
    echo -e "${YELLOW}⚠️  $1${NC}" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}❌ $1${NC}" | tee -a "$LOG_FILE"
}

# Test result tracking
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_SKIPPED=0

# Function to run a test command with timeout and error handling
run_test() {
    local test_name="$1"
    local command="$2"
    local expected_success="${3:-true}"
    local timeout="${4:-30}"
    
    TESTS_RUN=$((TESTS_RUN + 1))
    
    log "Running test: $test_name"
    if [[ "$VERBOSE" == "true" ]]; then
        log "Command: $command"
    fi
    
    # Run command with timeout
    if timeout "$timeout" bash -c "$command" >> "$LOG_FILE" 2>&1; then
        if [[ "$expected_success" == "true" ]]; then
            success "$test_name - PASSED"
            TESTS_PASSED=$((TESTS_PASSED + 1))
            return 0
        else
            error "$test_name - FAILED (expected failure but succeeded)"
            TESTS_FAILED=$((TESTS_FAILED + 1))
            return 1
        fi
    else
        local exit_code=$?
        if [[ "$expected_success" == "false" ]]; then
            success "$test_name - PASSED (expected failure)"
            TESTS_PASSED=$((TESTS_PASSED + 1))
            return 0
        else
            error "$test_name - FAILED (exit code: $exit_code)"
            TESTS_FAILED=$((TESTS_FAILED + 1))
            return 1
        fi
    fi
}

# Function to skip a test
skip_test() {
    local test_name="$1"
    local reason="$2"
    
    TESTS_RUN=$((TESTS_RUN + 1))
    TESTS_SKIPPED=$((TESTS_SKIPPED + 1))
    warning "$test_name - SKIPPED ($reason)"
}

# Setup and validation
setup_tests() {
    log "Setting up CQLite CLI comprehensive test suite"
    
    # Clear previous log
    > "$LOG_FILE"
    
    # Check if CLI binary exists
    if [[ ! -f "$CLI_BINARY" ]]; then
        error "CLI binary not found at $CLI_BINARY"
        log "Building CLI binary..."
        cargo build --release || {
            error "Failed to build CLI binary"
            exit 1
        }
    fi
    
    success "CLI binary found: $CLI_BINARY"
    
    # Get CLI version
    local version
    version=$($CLI_BINARY --version 2>/dev/null || echo "unknown")
    log "CLI version: $version"
    
    # Check test data availability
    if [[ -d "$TEST_DATA_DIR" ]]; then
        local sstable_count
        sstable_count=$(find "$TEST_DATA_DIR" -type d -name "*-*" | wc -l)
        success "Test data directory found with $sstable_count SSTable directories"
    else
        warning "Test data directory not found: $TEST_DATA_DIR"
        warning "Some tests will be skipped. To generate test data:"
        warning "  cd test-env/cassandra5 && ./manage.sh start && ./scripts/extract-sstables.sh"
    fi
    
    # Verify schema files exist
    if [[ -d "$SCHEMA_DIR" ]]; then
        local schema_count
        schema_count=$(find "$SCHEMA_DIR" -name "*.json" -o -name "*.cql" | wc -l)
        success "Schema directory found with $schema_count schema files"
    else
        error "Schema directory not found: $SCHEMA_DIR"
        exit 1
    fi
}

# Test basic CLI functionality
test_basic_functionality() {
    log "Testing basic CLI functionality"
    
    run_test "CLI help command" "$CLI_BINARY --help"
    run_test "CLI version command" "$CLI_BINARY --version"
    run_test "Invalid argument handling" "$CLI_BINARY --invalid-argument" false
}

# Test SSTable info command
test_sstable_info() {
    log "Testing SSTable info command"
    
    if [[ ! -d "$TEST_DATA_DIR" ]]; then
        skip_test "SSTable info tests" "test data not available"
        return
    fi
    
    # Find test directories
    local test_dirs
    mapfile -t test_dirs < <(find "$TEST_DATA_DIR" -maxdepth 1 -type d -name "*-*" | head -3)
    
    if [[ ${#test_dirs[@]} -eq 0 ]]; then
        skip_test "SSTable info tests" "no SSTable directories found"
        return
    fi
    
    for dir in "${test_dirs[@]}"; do
        local basename
        basename=$(basename "$dir")
        
        run_test "Info command for $basename" "$CLI_BINARY info '$dir'"
        run_test "Detailed info for $basename" "$CLI_BINARY info '$dir' --detailed"
        run_test "Info with auto-detect for $basename" "$CLI_BINARY info '$dir' --auto-detect"
    done
}

# Test SSTable read command
test_sstable_read() {
    log "Testing SSTable read command"
    
    if [[ ! -d "$TEST_DATA_DIR" ]]; then
        skip_test "SSTable read tests" "test data not available"
        return
    fi
    
    # Find matching SSTable directories and schemas
    local users_dir
    users_dir=$(find "$TEST_DATA_DIR" -maxdepth 1 -type d -name "*users*" | head -1)
    
    local all_types_dir
    all_types_dir=$(find "$TEST_DATA_DIR" -maxdepth 1 -type d -name "*all_types*" | head -1)
    
    # Test with users table if available
    if [[ -n "$users_dir" && -f "$SCHEMA_DIR/users.json" ]]; then
        run_test "Read users table (JSON format)" "$CLI_BINARY read '$users_dir' --schema '$SCHEMA_DIR/users.json' --limit 5"
        run_test "Read users table (CSV format)" "$CLI_BINARY read '$users_dir' --schema '$SCHEMA_DIR/users.json' --format csv --limit 3"
        run_test "Read users table (JSON output)" "$CLI_BINARY read '$users_dir' --schema '$SCHEMA_DIR/users.json' --format json --limit 2"
    else
        skip_test "Users table read tests" "users SSTable or schema not available"
    fi
    
    # Test with all_types table if available  
    if [[ -n "$all_types_dir" && -f "$SCHEMA_DIR/all_types.json" ]]; then
        run_test "Read all_types table" "$CLI_BINARY read '$all_types_dir' --schema '$SCHEMA_DIR/all_types.json' --limit 3" true 60
    else
        skip_test "All types table read tests" "all_types SSTable or schema not available"
    fi
}

# Test SELECT query functionality
test_select_queries() {
    log "Testing SELECT query functionality"
    
    if [[ ! -d "$TEST_DATA_DIR" ]]; then
        skip_test "SELECT query tests" "test data not available"
        return
    fi
    
    local users_dir
    users_dir=$(find "$TEST_DATA_DIR" -maxdepth 1 -type d -name "*users*" | head -1)
    
    if [[ -n "$users_dir" && -f "$SCHEMA_DIR/users.json" ]]; then
        run_test "SELECT * query" "$CLI_BINARY select '$users_dir' --schema '$SCHEMA_DIR/users.json' 'SELECT * FROM users LIMIT 5'"
        run_test "SELECT specific columns" "$CLI_BINARY select '$users_dir' --schema '$SCHEMA_DIR/users.json' 'SELECT user_id, email FROM users LIMIT 3'"
        run_test "SELECT with JSON output" "$CLI_BINARY select '$users_dir' --schema '$SCHEMA_DIR/users.json' 'SELECT * FROM users LIMIT 2' --format json"
        run_test "SELECT COUNT query" "$CLI_BINARY select '$users_dir' --schema '$SCHEMA_DIR/users.json' 'SELECT COUNT(*) FROM users'" true 45
    else
        skip_test "SELECT query tests" "users SSTable or schema not available"
    fi
}

# Test schema format detection
test_schema_formats() {
    log "Testing schema format detection"
    
    if [[ ! -d "$TEST_DATA_DIR" ]]; then
        skip_test "Schema format tests" "test data not available"
        return
    fi
    
    local test_dir
    test_dir=$(find "$TEST_DATA_DIR" -maxdepth 1 -type d -name "*-*" | head -1)
    
    if [[ -n "$test_dir" ]]; then
        # Test JSON schema format
        if [[ -f "$SCHEMA_DIR/users.json" ]]; then
            run_test "JSON schema format detection" "$CLI_BINARY read '$test_dir' --schema '$SCHEMA_DIR/users.json' --limit 1"
        fi
        
        # Test CQL schema format  
        if [[ -f "$SCHEMA_DIR/products.cql" ]]; then
            run_test "CQL schema format detection" "$CLI_BINARY read '$test_dir' --schema '$SCHEMA_DIR/products.cql' --limit 1" false
        fi
    else
        skip_test "Schema format tests" "no SSTable directories found"
    fi
}

# Test version compatibility
test_version_compatibility() {
    log "Testing version compatibility"
    
    if [[ ! -d "$TEST_DATA_DIR" ]]; then
        skip_test "Version compatibility tests" "test data not available"
        return
    fi
    
    local test_dir
    test_dir=$(find "$TEST_DATA_DIR" -maxdepth 1 -type d -name "*-*" | head -1)
    
    if [[ -n "$test_dir" ]]; then
        run_test "Auto-detect version" "$CLI_BINARY info '$test_dir' --auto-detect"
        run_test "Cassandra 5.0 compatibility" "$CLI_BINARY info '$test_dir' --cassandra-version 5.0"
        run_test "Cassandra 4.0 compatibility" "$CLI_BINARY info '$test_dir' --cassandra-version 4.0"
        run_test "Invalid version handling" "$CLI_BINARY info '$test_dir' --cassandra-version 99.99" false
    else
        skip_test "Version compatibility tests" "no SSTable directories found"
    fi
}

# Test error handling
test_error_handling() {
    log "Testing error handling"
    
    # Test with non-existent files
    run_test "Non-existent file handling" "$CLI_BINARY info /non/existent/path" false
    run_test "Non-existent schema handling" "$CLI_BINARY read /tmp --schema /non/existent/schema.json" false
    
    # Test with invalid schema files
    if [[ -f "cqlite-cli/tests/test_data/fixtures/invalid_schemas/malformed.json" ]]; then
        run_test "Invalid JSON schema handling" "$CLI_BINARY read /tmp --schema cqlite-cli/tests/test_data/fixtures/invalid_schemas/malformed.json" false
    fi
    
    if [[ -f "cqlite-cli/tests/test_data/fixtures/invalid_schemas/empty.json" ]]; then
        run_test "Empty schema handling" "$CLI_BINARY read /tmp --schema cqlite-cli/tests/test_data/fixtures/invalid_schemas/empty.json" false
    fi
    
    # Test with invalid queries
    if [[ -d "$TEST_DATA_DIR" && -f "$SCHEMA_DIR/users.json" ]]; then
        local test_dir
        test_dir=$(find "$TEST_DATA_DIR" -maxdepth 1 -type d -name "*users*" | head -1)
        
        if [[ -n "$test_dir" ]]; then
            run_test "Invalid SQL syntax" "$CLI_BINARY select '$test_dir' --schema '$SCHEMA_DIR/users.json' 'INVALID SQL SYNTAX'" false
            run_test "Missing FROM clause" "$CLI_BINARY select '$test_dir' --schema '$SCHEMA_DIR/users.json' 'SELECT *'" false
        fi
    fi
}

# Test performance with larger datasets
test_performance() {
    log "Testing performance with larger datasets"
    
    if [[ ! -d "$TEST_DATA_DIR" ]]; then
        skip_test "Performance tests" "test data not available"
        return
    fi
    
    # Find largest available directory
    local largest_dir
    largest_dir=$(find "$TEST_DATA_DIR" -maxdepth 1 -type d -name "*-*" -exec du -s {} \; 2>/dev/null | sort -nr | head -1 | cut -f2)
    
    if [[ -n "$largest_dir" ]]; then
        local basename
        basename=$(basename "$largest_dir")
        
        log "Running performance tests with largest dataset: $basename"
        
        run_test "Performance: Info command" "$CLI_BINARY info '$largest_dir'" true 120
        run_test "Performance: Detailed info" "$CLI_BINARY info '$largest_dir' --detailed" true 180
        
        # Test with schema if available
        if [[ -f "$SCHEMA_DIR/users.json" ]]; then
            run_test "Performance: Read with limit" "$CLI_BINARY read '$largest_dir' --schema '$SCHEMA_DIR/users.json' --limit 100" true 300
        fi
    else
        skip_test "Performance tests" "no suitable SSTable directories found"
    fi
}

# Generate test report
generate_report() {
    log "Generating comprehensive test report"
    
    echo "
========================================
CQLite CLI Comprehensive Test Results
========================================
Date: $(date)
CLI Version: $($CLI_BINARY --version 2>/dev/null || echo 'unknown')
Test Data: $TEST_DATA_DIR
Log File: $LOG_FILE

Summary:
--------
Tests Run:     $TESTS_RUN
Tests Passed:  $TESTS_PASSED
Tests Failed:  $TESTS_FAILED
Tests Skipped: $TESTS_SKIPPED

Success Rate: $(( TESTS_RUN > 0 ? (TESTS_PASSED * 100) / TESTS_RUN : 0 ))%
" | tee -a "$LOG_FILE"

    if [[ $TESTS_FAILED -gt 0 ]]; then
        echo "
❌ FAILED TESTS:
See $LOG_FILE for detailed error messages
" | tee -a "$LOG_FILE"
        return 1
    elif [[ $TESTS_SKIPPED -gt 0 ]]; then
        echo "
⚠️  Some tests were skipped due to missing test data
To run all tests, generate SSTable test data:
  cd test-env/cassandra5 && ./manage.sh start && ./scripts/extract-sstables.sh
" | tee -a "$LOG_FILE"
    else
        echo "
🎉 ALL TESTS PASSED!
CLI is functioning correctly with SSTable files.
" | tee -a "$LOG_FILE"
    fi
    
    echo "
Full test log available at: $LOG_FILE
"
}

# Main test execution
main() {
    echo "🧪 CQLite CLI Comprehensive Test Suite"
    echo "======================================"
    
    setup_tests
    
    test_basic_functionality
    test_sstable_info
    test_sstable_read
    test_select_queries
    test_schema_formats
    test_version_compatibility
    test_error_handling
    test_performance
    
    generate_report
}

# Handle command line arguments
case "${1:-}" in
    --help|-h)
        echo "CQLite CLI Comprehensive Test Runner"
        echo ""
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --help, -h     Show this help message"
        echo "  --verbose, -v  Enable verbose output"
        echo "  --setup        Only run setup validation"
        echo ""
        echo "Environment Variables:"
        echo "  VERBOSE=true   Enable verbose output"
        echo ""
        echo "Test Categories:"
        echo "  - Basic CLI functionality"
        echo "  - SSTable info command"
        echo "  - SSTable read command" 
        echo "  - SELECT query functionality"
        echo "  - Schema format detection"
        echo "  - Version compatibility"
        echo "  - Error handling"
        echo "  - Performance testing"
        exit 0
        ;;
    --verbose|-v)
        VERBOSE=true
        main
        ;;
    --setup)
        setup_tests
        exit 0
        ;;
    *)
        main
        ;;
esac