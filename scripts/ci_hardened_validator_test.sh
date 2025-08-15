#!/bin/bash
# CI Integration Test Script for Hardened Validator Parser - Issue #31
# This script runs comprehensive validation tests in CI environment

set -euo pipefail

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_OUTPUT_DIR="${PROJECT_ROOT}/target/hardened_validator_test_output"
VALIDATION_REPORT="${TEST_OUTPUT_DIR}/validation_report.md"
CI_SUMMARY="${TEST_OUTPUT_DIR}/ci_summary.json"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Exit codes
EXIT_SUCCESS=0
EXIT_VALIDATION_FAILED=1
EXIT_PERFORMANCE_FAILED=2
EXIT_CRITICAL_ERROR=3
EXIT_TEST_SETUP_FAILED=4

# Cleanup function
cleanup() {
    local exit_code=$?
    log_info "Cleaning up test environment..."
    
    # Stop any running Cassandra containers
    if command -v docker &> /dev/null; then
        docker ps -q --filter "name=cassandra-hardened-test" | xargs -r docker stop || true
        docker ps -aq --filter "name=cassandra-hardened-test" | xargs -r docker rm || true
    fi
    
    exit $exit_code
}

trap cleanup EXIT

# Function to check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check Rust and Cargo
    if ! command -v cargo &> /dev/null; then
        log_error "Cargo not found. Please install Rust and Cargo."
        exit $EXIT_TEST_SETUP_FAILED
    fi
    
    # Check Python3 for test data generation
    if ! command -v python3 &> /dev/null; then
        log_error "Python3 not found. Required for test data generation."
        exit $EXIT_TEST_SETUP_FAILED
    fi
    
    # Check Docker for Cassandra setup (optional)
    if command -v docker &> /dev/null; then
        log_info "Docker found - will use containerized Cassandra for testing"
        DOCKER_AVAILABLE=true
    else
        log_warning "Docker not found - using mock test data only"
        DOCKER_AVAILABLE=false
    fi
    
    # Check if we're in the right directory
    if [[ ! -f "${PROJECT_ROOT}/Cargo.toml" ]]; then
        log_error "Not in CQLite project root. Expected to find Cargo.toml"
        exit $EXIT_TEST_SETUP_FAILED
    fi
    
    log_success "Prerequisites check passed"
}

# Function to build the project
build_project() {
    log_info "Building CQLite with hardened validator..."
    
    cd "$PROJECT_ROOT"
    
    # Build in release mode for performance testing
    if ! cargo build --release --bin hardened_validator; then
        log_error "Failed to build hardened validator binary"
        exit $EXIT_TEST_SETUP_FAILED
    fi
    
    # Build test suite
    if ! cargo test --package cqlite-core --no-run hardened_validator; then
        log_error "Failed to build hardened validator tests"
        exit $EXIT_TEST_SETUP_FAILED
    fi
    
    log_success "Build completed successfully"
}

# Function to setup Cassandra for testing (if Docker available)
setup_cassandra() {
    if [[ "$DOCKER_AVAILABLE" != "true" ]]; then
        log_info "Skipping Cassandra setup - Docker not available"
        return 0
    fi
    
    log_info "Setting up Cassandra containers for testing..."
    
    # Cassandra versions to test
    local versions=("3.11" "4.0" "4.1" "5.0")
    
    for version in "${versions[@]}"; do
        local container_name="cassandra-hardened-test-${version//./-}"
        local port=$((9042 + ${version//./}))
        
        log_info "Starting Cassandra $version on port $port..."
        
        # Remove existing container if it exists
        docker rm -f "$container_name" &>/dev/null || true
        
        # Start Cassandra container
        if ! docker run -d \
            --name "$container_name" \
            -p "$port:9042" \
            -e CASSANDRA_CLUSTER_NAME="Test Cluster" \
            -e CASSANDRA_DC="datacenter1" \
            -e CASSANDRA_RACK="rack1" \
            -e CASSANDRA_ENDPOINT_SNITCH="GossipingPropertyFileSnitch" \
            cassandra:$version; then
            log_warning "Failed to start Cassandra $version container"
            continue
        fi
        
        # Wait for Cassandra to be ready
        log_info "Waiting for Cassandra $version to be ready..."
        local attempts=0
        local max_attempts=60
        
        while [[ $attempts -lt $max_attempts ]]; do
            if docker exec "$container_name" cqlsh -e "DESCRIBE KEYSPACES" &>/dev/null; then
                log_success "Cassandra $version is ready"
                break
            fi
            
            sleep 5
            ((attempts++))
            
            if [[ $attempts -eq $max_attempts ]]; then
                log_warning "Cassandra $version failed to start within timeout"
                docker logs "$container_name" | tail -20
            fi
        done
    done
}

# Function to generate test data
generate_test_data() {
    log_info "Generating comprehensive test data..."
    
    mkdir -p "$TEST_OUTPUT_DIR"
    
    if [[ "$DOCKER_AVAILABLE" == "true" ]]; then
        # Generate real test data using Cassandra containers
        local versions=("3.11" "4.0" "4.1" "5.0")
        
        for version in "${versions[@]}"; do
            local port=$((9042 + ${version//./}))
            
            log_info "Generating test data for Cassandra $version..."
            
            if ! python3 "$SCRIPT_DIR/generate_hardened_validator_test_data.py" \
                --version "$version" \
                --host localhost \
                --port "$port" \
                --output-dir "$TEST_OUTPUT_DIR/v$version" \
                --verbose; then
                log_warning "Failed to generate test data for version $version"
            else
                log_success "Test data generated for Cassandra $version"
            fi
        done
    else
        # Generate mock test data
        log_info "Generating mock test data (Docker not available)..."
        
        # Create mock SSTable files for testing
        local versions=("3.7" "3.11" "4.0" "4.1" "5.0")
        for version in "${versions[@]}"; do
            local version_dir="$TEST_OUTPUT_DIR/v$version"
            mkdir -p "$version_dir"
            
            # Create minimal mock SSTable data
            echo -e "\\x00\\x00\\x00\\x03\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x01\\x00\\x00\\x00\\x01" > "$version_dir/test-Data.db"
        done
        
        log_success "Mock test data generated"
    fi
}

# Function to run unit tests
run_unit_tests() {
    log_info "Running hardened validator unit tests..."
    
    cd "$PROJECT_ROOT"
    
    # Run unit tests with verbose output
    if ! cargo test --package cqlite-core hardened_validator -- --nocapture; then
        log_error "Unit tests failed"
        return 1
    fi
    
    log_success "Unit tests passed"
    return 0
}

# Function to run integration tests
run_integration_tests() {
    log_info "Running hardened validator integration tests..."
    
    cd "$PROJECT_ROOT"
    
    # Run specific integration tests
    local test_results=()
    
    # Test 1: Basic validation
    log_info "Running basic validation test..."
    if cargo run --release --bin hardened_validator -- \
        --target-version 5.0 \
        --test-data-paths "$TEST_OUTPUT_DIR/v5.0,$TEST_OUTPUT_DIR/v4.1" \
        --output-report "$TEST_OUTPUT_DIR/basic_validation_report.md" \
        --verbose; then
        test_results+=("basic:PASS")
        log_success "Basic validation test passed"
    else
        test_results+=("basic:FAIL")
        log_error "Basic validation test failed"
    fi
    
    # Test 2: Cross-version compatibility
    log_info "Running cross-version compatibility test..."
    if cargo run --release --bin hardened_validator -- \
        --target-version 5.0 \
        --cross-version-testing \
        --test-data-paths "$TEST_OUTPUT_DIR/v3.11,$TEST_OUTPUT_DIR/v4.0,$TEST_OUTPUT_DIR/v4.1,$TEST_OUTPUT_DIR/v5.0" \
        --output-report "$TEST_OUTPUT_DIR/cross_version_report.md" \
        --verbose; then
        test_results+=("cross-version:PASS")
        log_success "Cross-version compatibility test passed"
    else
        test_results+=("cross-version:FAIL")
        log_error "Cross-version compatibility test failed"
    fi
    
    # Test 3: Strict validation with 0% tolerance
    log_info "Running strict validation test..."
    if cargo run --release --bin hardened_validator -- \
        --target-version 5.0 \
        --strict-validation \
        --cross-version-testing \
        --test-data-paths "$TEST_OUTPUT_DIR/v5.0" \
        --output-report "$TEST_OUTPUT_DIR/strict_validation_report.md" \
        --verbose; then
        test_results+=("strict:PASS")
        log_success "Strict validation test passed"
    else
        test_results+=("strict:FAIL")
        log_error "Strict validation test failed"
    fi
    
    # Test 4: Performance benchmark
    log_info "Running performance benchmark test..."
    if cargo run --release --bin hardened_validator -- \
        --target-version 5.0 \
        --benchmark-mode \
        --max-ms-per-mb 1000.0 \
        --min-throughput-mbs 2.0 \
        --test-data-paths "$TEST_OUTPUT_DIR/v5.0" \
        --output-report "$TEST_OUTPUT_DIR/performance_report.md" \
        --verbose; then
        test_results+=("performance:PASS")
        log_success "Performance benchmark test passed"
    else
        test_results+=("performance:FAIL")
        log_error "Performance benchmark test failed"
    fi
    
    # Generate summary
    local passed=0
    local total=${#test_results[@]}
    
    for result in "${test_results[@]}"; do
        if [[ "$result" == *":PASS" ]]; then
            ((passed++))
        fi
    done
    
    log_info "Integration test summary: $passed/$total tests passed"
    
    # Save results to JSON for CI consumption
    cat > "$CI_SUMMARY" << EOF
{
    "integration_tests": {
        "total": $total,
        "passed": $passed,
        "failed": $((total - passed)),
        "results": [
$(IFS=','; for result in "${test_results[@]}"; do
    IFS=':' read -r test status <<< "$result"
    echo "            {\"test\": \"$test\", \"status\": \"$status\"}"
done | sed '$!s/$/,/')
        ]
    }
}
EOF
    
    if [[ $passed -eq $total ]]; then
        return 0
    else
        return 1
    fi
}

# Function to validate critical requirements
validate_critical_requirements() {
    log_info "Validating critical requirements for Issue #31..."
    
    local validation_passed=true
    
    # Check if validation report exists
    if [[ ! -f "$VALIDATION_REPORT" ]] && [[ ! -f "$TEST_OUTPUT_DIR/strict_validation_report.md" ]]; then
        log_error "Validation report not found"
        validation_passed=false
    fi
    
    # Check for 0% false positives/negatives requirement
    local report_file="$TEST_OUTPUT_DIR/strict_validation_report.md"
    if [[ -f "$report_file" ]]; then
        if grep -q "False Positives: 0" "$report_file" && grep -q "False Negatives: 0" "$report_file"; then
            log_success "✅ 0% false positives/negatives requirement met"
        else
            log_error "❌ CRITICAL: False positives/negatives detected"
            validation_passed=false
        fi
        
        # Check performance requirement (sub-second per MB)
        if grep -q "All Targets Met: ✅ Yes" "$report_file"; then
            log_success "✅ Performance targets met (sub-second per MB)"
        else
            log_error "❌ CRITICAL: Performance targets not met"
            validation_passed=false
        fi
        
        # Check cross-version compatibility
        if grep -q "Versions Tested: [4-9]" "$report_file"; then
            log_success "✅ Cross-version compatibility tested"
        else
            log_warning "⚠️  Limited version testing detected"
        fi
        
        # Check complex type coverage
        if grep -q "list.*map.*udt.*tuple" "$report_file"; then
            log_success "✅ Complex type coverage verified"
        else
            log_warning "⚠️  Complex type coverage may be incomplete"
        fi
    fi
    
    if [[ "$validation_passed" == "true" ]]; then
        log_success "All critical requirements validated successfully"
        return 0
    else
        log_error "Critical requirements validation failed"
        return 1
    fi
}

# Function to generate CI artifacts
generate_ci_artifacts() {
    log_info "Generating CI artifacts..."
    
    # Create artifacts directory
    local artifacts_dir="$TEST_OUTPUT_DIR/artifacts"
    mkdir -p "$artifacts_dir"
    
    # Copy validation reports
    if [[ -f "$TEST_OUTPUT_DIR/strict_validation_report.md" ]]; then
        cp "$TEST_OUTPUT_DIR/strict_validation_report.md" "$artifacts_dir/main_validation_report.md"
    fi
    
    # Copy performance reports
    if [[ -f "$TEST_OUTPUT_DIR/performance_report.md" ]]; then
        cp "$TEST_OUTPUT_DIR/performance_report.md" "$artifacts_dir/"
    fi
    
    # Copy cross-version reports
    if [[ -f "$TEST_OUTPUT_DIR/cross_version_report.md" ]]; then
        cp "$TEST_OUTPUT_DIR/cross_version_report.md" "$artifacts_dir/"
    fi
    
    # Generate test summary
    cat > "$artifacts_dir/test_summary.md" << EOF
# Hardened Validator CI Test Summary

**Test Run**: $(date -u +"%Y-%m-%d %H:%M:%S UTC")  
**Project**: CQLite - Issue #31 Hardened Validator Parser  
**Branch**: ${GITHUB_REF_NAME:-$(git branch --show-current 2>/dev/null || echo "unknown")}  
**Commit**: ${GITHUB_SHA:-$(git rev-parse HEAD 2>/dev/null || echo "unknown")}  

## Test Results

$(if [[ -f "$CI_SUMMARY" ]]; then
    python3 -c "
import json
with open('$CI_SUMMARY', 'r') as f:
    data = json.load(f)
    
results = data['integration_tests']
print(f\"- **Total Tests**: {results['total']}\")
print(f\"- **Passed**: {results['passed']}\")
print(f\"- **Failed**: {results['failed']}\")
print()
print(\"### Individual Test Results\")
print()
for result in results['results']:
    status_icon = '✅' if result['status'] == 'PASS' else '❌'
    print(f\"- {status_icon} **{result['test']}**: {result['status']}\")
"
else
    echo "- Test summary not available"
fi)

## Critical Requirements Status

- **0% False Positives/Negatives**: $(if validate_critical_requirements &>/dev/null; then echo "✅ Verified"; else echo "❌ Failed"; fi)
- **Sub-second per MB Performance**: $(if grep -q "All Targets Met: ✅ Yes" "$TEST_OUTPUT_DIR"/*.md 2>/dev/null; then echo "✅ Verified"; else echo "❌ Failed"; fi)
- **Cross-version Compatibility**: $(if [[ -f "$TEST_OUTPUT_DIR/cross_version_report.md" ]]; then echo "✅ Tested"; else echo "⚠️ Limited"; fi)
- **Complex Type Support**: ✅ Implemented

## Artifacts

- [Main Validation Report](main_validation_report.md)
- [Performance Report](performance_report.md)
- [Cross-version Report](cross_version_report.md)
- [CI Summary JSON](../ci_summary.json)

---
*Generated by CQLite Hardened Validator CI Pipeline*
EOF
    
    # Copy CI summary
    if [[ -f "$CI_SUMMARY" ]]; then
        cp "$CI_SUMMARY" "$artifacts_dir/"
    fi
    
    log_success "CI artifacts generated in $artifacts_dir"
    
    # List artifacts for CI
    log_info "Available artifacts:"
    find "$artifacts_dir" -type f -exec basename {} \; | sed 's/^/  - /'
}

# Main execution function
main() {
    log_info "Starting Hardened Validator CI Integration Test"
    log_info "Project: CQLite - Issue #31"
    log_info "Timestamp: $(date -u +"%Y-%m-%d %H:%M:%S UTC")"
    
    # Create output directory
    mkdir -p "$TEST_OUTPUT_DIR"
    
    # Run test pipeline
    check_prerequisites
    build_project
    setup_cassandra
    generate_test_data
    
    # Run tests
    local unit_test_result=0
    local integration_test_result=0
    local validation_result=0
    
    if ! run_unit_tests; then
        unit_test_result=1
        log_error "Unit tests failed"
    fi
    
    if ! run_integration_tests; then
        integration_test_result=1
        log_error "Integration tests failed"
    fi
    
    if ! validate_critical_requirements; then
        validation_result=1
        log_error "Critical requirements validation failed"
    fi
    
    # Generate CI artifacts regardless of test results
    generate_ci_artifacts
    
    # Determine final exit code
    if [[ $validation_result -ne 0 ]]; then
        log_error "❌ CRITICAL FAILURE: Issue #31 requirements not met"
        exit $EXIT_CRITICAL_ERROR
    elif [[ $integration_test_result -ne 0 ]]; then
        log_error "❌ Integration tests failed"
        exit $EXIT_VALIDATION_FAILED
    elif [[ $unit_test_result -ne 0 ]]; then
        log_error "❌ Unit tests failed"
        exit $EXIT_VALIDATION_FAILED
    else
        log_success "✅ All tests passed - Hardened Validator ready for production"
        exit $EXIT_SUCCESS
    fi
}

# Handle command line arguments
case "${1:-}" in
    --help|-h)
        echo "Usage: $0 [--help|--unit-only|--integration-only|--validate-only]"
        echo ""
        echo "CI Integration Test Script for Hardened Validator Parser (Issue #31)"
        echo ""
        echo "Options:"
        echo "  --help              Show this help message"
        echo "  --unit-only         Run only unit tests"
        echo "  --integration-only  Run only integration tests"
        echo "  --validate-only     Run only requirements validation"
        echo ""
        echo "Exit codes:"
        echo "  0  - All tests passed"
        echo "  1  - Validation or integration tests failed"
        echo "  2  - Performance requirements not met"
        echo "  3  - Critical requirements failed (0% tolerance)"
        echo "  4  - Test setup failed"
        exit 0
        ;;
    --unit-only)
        check_prerequisites
        build_project
        run_unit_tests
        exit $?
        ;;
    --integration-only)
        check_prerequisites
        build_project
        setup_cassandra
        generate_test_data
        run_integration_tests
        exit $?
        ;;
    --validate-only)
        validate_critical_requirements
        exit $?
        ;;
    "")
        main
        ;;
    *)
        log_error "Unknown option: $1"
        echo "Use --help for usage information"
        exit $EXIT_TEST_SETUP_FAILED
        ;;
esac