#!/bin/bash
#
# Comprehensive Integration Test Runner Script
# 
# This script runs the complete CQLite integration test suite against real
# Cassandra 5.0 SSTable data to validate compatibility and performance.
#

set -e  # Exit on any error

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DATA_PATH="${SCRIPT_DIR}/test-env/cassandra5/sstables"
LOG_DIR="${SCRIPT_DIR}/test-logs"
RESULTS_DIR="${SCRIPT_DIR}/test-results"

# Test configuration
TEST_MODE="full"
TIMEOUT_SECONDS=300
FAIL_FAST=false
VERBOSE=false
GENERATE_TEST_DATA=false
CLEAN_BEFORE_RUN=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    cat << EOF
CQLite Comprehensive Integration Test Runner

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -m, --mode MODE           Test mode: full, quick, real-only, performance-only, collections-only
                             [default: full]
    -t, --timeout SECONDS    Timeout in seconds [default: 300]
    -f, --fail-fast          Stop on first failure
    -v, --verbose            Verbose output
    -g, --generate-data      Generate test data before running tests
    -c, --clean              Clean previous test results before running
    -d, --data-path PATH     Custom path to test SSTable data
    -h, --help               Show this help message

TEST MODES:
    full                     Complete test suite (all features)
    quick                    Essential tests only (faster feedback)
    real-only                Real SSTable reading tests only
    performance-only         Performance and benchmarking tests
    collections-only         Collection type and UDT tests

EXAMPLES:
    # Run full test suite
    $0

    # Quick tests for CI feedback
    $0 --mode quick --fail-fast

    # Generate test data and run comprehensive tests
    $0 --generate-data --verbose

    # Performance testing only with extended timeout
    $0 --mode performance-only --timeout 600

EXIT CODES:
    0    All tests passed
    1    Some tests failed or compatibility below threshold
    2    Critical issues found
    3    Test runner error or timeout
    4    Test data generation failed
    5    Environment setup error

EOF
}

# Function to parse command line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -m|--mode)
                TEST_MODE="$2"
                shift 2
                ;;
            -t|--timeout)
                TIMEOUT_SECONDS="$2"
                shift 2
                ;;
            -f|--fail-fast)
                FAIL_FAST=true
                shift
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -g|--generate-data)
                GENERATE_TEST_DATA=true
                shift
                ;;
            -c|--clean)
                CLEAN_BEFORE_RUN=true
                shift
                ;;
            -d|--data-path)
                TEST_DATA_PATH="$2"
                shift 2
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
}

# Function to check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."

    # Check if Rust is installed
    if ! command -v cargo &> /dev/null; then
        print_error "Cargo (Rust) is not installed. Please install Rust first."
        exit 5
    fi

    # Check if Docker is available (for test data generation)
    if [ "$GENERATE_TEST_DATA" = true ] && ! command -v docker &> /dev/null; then
        print_error "Docker is required for test data generation but is not installed."
        exit 5
    fi

    # Create necessary directories
    mkdir -p "$LOG_DIR" "$RESULTS_DIR"

    print_success "Prerequisites check passed"
}

# Function to generate test data
generate_test_data() {
    if [ "$GENERATE_TEST_DATA" = true ]; then
        print_status "Generating test data..."
        
        local cassandra_env_dir="${SCRIPT_DIR}/test-env/cassandra5"
        
        if [ ! -d "$cassandra_env_dir" ]; then
            print_error "Cassandra test environment not found at $cassandra_env_dir"
            exit 4
        fi

        cd "$cassandra_env_dir"
        
        # Make sure manage.sh is executable
        chmod +x manage.sh

        # Generate fresh test data
        print_status "Running Cassandra test data generation..."
        if ! ./manage.sh all; then
            print_error "Failed to generate test data"
            exit 4
        fi

        # Extract SSTable files
        print_status "Extracting SSTable files..."
        if ! ./manage.sh extract-sstables; then
            print_error "Failed to extract SSTable files"
            exit 4
        fi

        cd "$SCRIPT_DIR"
        print_success "Test data generation completed"
    fi
}

# Function to validate test environment
validate_test_environment() {
    print_status "Validating test environment..."

    # Check if test data path exists
    if [ ! -d "$TEST_DATA_PATH" ]; then
        print_error "Test data path does not exist: $TEST_DATA_PATH"
        print_error "Run with --generate-data to create test data, or check the path."
        exit 5
    fi

    # Check if we have any SSTable directories
    local sstable_count=$(find "$TEST_DATA_PATH" -maxdepth 1 -type d | wc -l)
    if [ "$sstable_count" -lt 2 ]; then  # At least one table directory (plus the base directory)
        print_warning "Very few SSTable directories found in $TEST_DATA_PATH"
        print_warning "Consider regenerating test data with --generate-data"
    fi

    # Check for expected table types
    local expected_tables=("all_types" "collections_table" "time_series" "users")
    local found_tables=0
    
    for table in "${expected_tables[@]}"; do
        if find "$TEST_DATA_PATH" -maxdepth 1 -type d -name "${table}*" | grep -q .; then
            ((found_tables++))
        fi
    done

    if [ "$found_tables" -lt 2 ]; then
        print_warning "Expected table types not found. Test coverage may be limited."
        print_warning "Expected tables: ${expected_tables[*]}"
    fi

    print_success "Test environment validation completed ($found_tables/${#expected_tables[@]} expected tables found)"
}

# Function to clean previous results
clean_previous_results() {
    if [ "$CLEAN_BEFORE_RUN" = true ]; then
        print_status "Cleaning previous test results..."
        rm -rf "$LOG_DIR"/* "$RESULTS_DIR"/*
        rm -f integration_test_results.json integration_test_status.txt
        print_success "Previous results cleaned"
    fi
}

# Function to build the test runner
build_test_runner() {
    print_status "Building integration test runner..."
    
    local build_start=$(date +%s)
    
    if [ "$VERBOSE" = true ]; then
        cargo build --release --bin comprehensive_integration_test_runner
    else
        cargo build --release --bin comprehensive_integration_test_runner > "$LOG_DIR/build.log" 2>&1
    fi

    local build_result=$?
    local build_end=$(date +%s)
    local build_time=$((build_end - build_start))

    if [ $build_result -eq 0 ]; then
        print_success "Build completed in ${build_time}s"
    else
        print_error "Build failed. Check $LOG_DIR/build.log for details."
        exit 5
    fi
}

# Function to run the integration tests
run_integration_tests() {
    print_status "Running integration tests (mode: $TEST_MODE, timeout: ${TIMEOUT_SECONDS}s)..."

    local test_start=$(date +%s)
    local test_cmd="./target/release/comprehensive_integration_test_runner"
    
    # Build command line arguments
    test_cmd="$test_cmd --mode $TEST_MODE"
    test_cmd="$test_cmd --test-data $TEST_DATA_PATH"
    test_cmd="$test_cmd --timeout $TIMEOUT_SECONDS"
    
    if [ "$FAIL_FAST" = true ]; then
        test_cmd="$test_cmd --fail-fast"
    fi
    
    if [ "$VERBOSE" = false ]; then
        test_cmd="$test_cmd --brief"
    fi

    # Create log file with timestamp
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local test_log="$LOG_DIR/integration_test_${timestamp}.log"

    print_status "Test command: $test_cmd"
    print_status "Test log: $test_log"

    # Run the tests
    if [ "$VERBOSE" = true ]; then
        $test_cmd 2>&1 | tee "$test_log"
        local test_result=${PIPESTATUS[0]}
    else
        $test_cmd > "$test_log" 2>&1
        local test_result=$?
    fi

    local test_end=$(date +%s)
    local test_time=$((test_end - test_start))

    # Process results
    if [ -f "integration_test_status.txt" ]; then
        local test_status=$(cat integration_test_status.txt)
        print_status "Test status: $test_status"
    fi

    case $test_result in
        0)
            print_success "All tests passed! (${test_time}s)"
            ;;
        1)
            print_warning "Some tests failed or compatibility below threshold (${test_time}s)"
            ;;
        2)
            print_error "Critical issues found (${test_time}s)"
            ;;
        3)
            print_error "Test runner error or timeout (${test_time}s)"
            ;;
        *)
            print_error "Unknown test result code: $test_result (${test_time}s)"
            ;;
    esac

    # Copy results to results directory
    if [ -f "integration_test_results.json" ]; then
        cp "integration_test_results.json" "$RESULTS_DIR/"
        print_status "Results saved to $RESULTS_DIR/integration_test_results.json"
    fi

    if [ -f "integration_test_status.txt" ]; then
        cp "integration_test_status.txt" "$RESULTS_DIR/"
    fi

    return $test_result
}

# Function to generate summary report
generate_summary_report() {
    local test_result=$1
    local report_file="$RESULTS_DIR/test_summary.md"

    print_status "Generating summary report..."

    cat > "$report_file" << EOF
# CQLite Integration Test Summary

**Date:** $(date)  
**Mode:** $TEST_MODE  
**Timeout:** ${TIMEOUT_SECONDS}s  
**Test Data:** $TEST_DATA_PATH  

## Results

EOF

    case $test_result in
        0)
            echo "**Status:** ✅ PASSED" >> "$report_file"
            echo "" >> "$report_file"
            echo "All integration tests passed successfully. CQLite is compatible with Cassandra 5+ SSTable format." >> "$report_file"
            ;;
        1)
            echo "**Status:** ⚠️ PARTIAL" >> "$report_file"
            echo "" >> "$report_file"
            echo "Some tests failed or compatibility is below the required threshold. Review the detailed results." >> "$report_file"
            ;;
        2)
            echo "**Status:** 🚨 CRITICAL" >> "$report_file"
            echo "" >> "$report_file"
            echo "Critical compatibility issues found. Address these before production use." >> "$report_file"
            ;;
        *)
            echo "**Status:** ❌ ERROR" >> "$report_file"
            echo "" >> "$report_file"
            echo "Test execution failed or encountered errors." >> "$report_file"
            ;;
    esac

    if [ -f "integration_test_results.json" ]; then
        echo "" >> "$report_file"
        echo "## Detailed Results" >> "$report_file"
        echo "" >> "$report_file"
        echo "See \`integration_test_results.json\` for detailed test results and metrics." >> "$report_file"
    fi

    echo "" >> "$report_file"
    echo "## Log Files" >> "$report_file"
    echo "" >> "$report_file"
    echo "- Build log: \`$LOG_DIR/build.log\`" >> "$report_file"
    echo "- Test logs: \`$LOG_DIR/integration_test_*.log\`" >> "$report_file"

    print_success "Summary report generated: $report_file"
}

# Function to cleanup on exit
cleanup() {
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        print_error "Script exited with error code $exit_code"
        if [ -f "$LOG_DIR/build.log" ]; then
            print_error "Recent build log:"
            tail -20 "$LOG_DIR/build.log"
        fi
    fi
}

# Main execution function
main() {
    echo "🧪 CQLite Comprehensive Integration Test Runner"
    echo "================================================"

    # Set up error handling
    trap cleanup EXIT

    # Parse command line arguments
    parse_arguments "$@"

    # Check prerequisites
    check_prerequisites

    # Clean previous results if requested
    clean_previous_results

    # Generate test data if requested
    generate_test_data

    # Validate test environment
    validate_test_environment

    # Build the test runner
    build_test_runner

    # Run the integration tests
    run_integration_tests
    local test_exit_code=$?

    # Generate summary report
    generate_summary_report $test_exit_code

    # Print final status
    echo ""
    echo "================================================"
    case $test_exit_code in
        0)
            print_success "🎉 Integration tests completed successfully!"
            print_success "CQLite is ready for production use with Cassandra 5+"
            ;;
        1)
            print_warning "⚠️ Integration tests completed with issues"
            print_warning "Review failed tests and address compatibility gaps"
            ;;
        2)
            print_error "🚨 Critical compatibility issues found"
            print_error "Address critical issues before production deployment"
            ;;
        *)
            print_error "❌ Integration tests failed to complete"
            print_error "Check logs for detailed error information"
            ;;
    esac

    echo ""
    print_status "Results directory: $RESULTS_DIR"
    print_status "Logs directory: $LOG_DIR"

    exit $test_exit_code
}

# Run main function with all arguments
main "$@"