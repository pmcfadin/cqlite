#!/bin/bash

# CQLite Docker Infrastructure Test Runner - Issue #30
# Comprehensive end-to-end testing of Docker validation infrastructure
# This script validates that all components work together

set -euo pipefail

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
WHITE='\033[1;37m'
NC='\033[0m'

# Configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
RESULTS_DIR="$PROJECT_ROOT/infrastructure-test-results-$(date +%Y%m%d-%H%M%S)"

# Test configuration
RUN_QUICK_TEST="${RUN_QUICK_TEST:-true}"
RUN_FULL_TEST="${RUN_FULL_TEST:-false}"
RUN_CI_TEST="${RUN_CI_TEST:-false}"
CLEANUP_AFTER="${CLEANUP_AFTER:-true}"

echo -e "${WHITE}═══════════════════════════════════════════════════════════════════════════════${NC}"
echo -e "${WHITE}    CQLite Docker Infrastructure Test Suite - Issue #30                        ${NC}"
echo -e "${WHITE}    End-to-end validation of Docker validation infrastructure                   ${NC}"
echo -e "${WHITE}═══════════════════════════════════════════════════════════════════════════════${NC}"
echo ""

# Logging function
test_log() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%H:%M:%S')
    
    case "$level" in
        "INFO")  echo -e "${BLUE}[$timestamp] [INFO]${NC}  $message" ;;
        "WARN")  echo -e "${YELLOW}[$timestamp] [WARN]${NC}  $message" ;;
        "ERROR") echo -e "${RED}[$timestamp] [ERROR]${NC} $message" ;;
        "SUCCESS") echo -e "${GREEN}[$timestamp] [SUCCESS]${NC} $message" ;;
        "TEST") echo -e "${PURPLE}[$timestamp] [TEST]${NC} $message" ;;
    esac
}

# Function to check prerequisites
check_test_prerequisites() {
    test_log "INFO" "Checking test prerequisites..."
    
    local missing_tools=()
    
    # Check required tools
    local tools=("docker" "docker-compose" "cargo" "bc")
    for tool in "${tools[@]}"; do
        if ! command -v "$tool" &>/dev/null; then
            missing_tools+=("$tool")
        fi
    done
    
    if [ ${#missing_tools[@]} -ne 0 ]; then
        test_log "ERROR" "Missing required tools: ${missing_tools[*]}"
        return 1
    fi
    
    # Check Docker daemon
    if ! docker info >/dev/null 2>&1; then
        test_log "ERROR" "Docker daemon is not running"
        return 1
    fi
    
    # Check if scripts exist and are executable
    local scripts=(
        "$SCRIPT_DIR/quick-docker-validation.sh"
        "$SCRIPT_DIR/docker-validator-orchestrator.sh"
        "$SCRIPT_DIR/ci-docker-validation.sh"
    )
    
    for script in "${scripts[@]}"; do
        if [ ! -f "$script" ]; then
            test_log "ERROR" "Script not found: $script"
            return 1
        fi
        
        if [ ! -x "$script" ]; then
            test_log "ERROR" "Script not executable: $script"
            return 1
        fi
    done
    
    test_log "SUCCESS" "All prerequisites satisfied"
    return 0
}

# Function to test quick validation
test_quick_validation() {
    test_log "TEST" "Testing quick Docker validation..."
    
    local test_start=$(date +%s)
    local quick_results="$RESULTS_DIR/quick-validation"
    mkdir -p "$quick_results"
    
    # Run quick validation
    if "$SCRIPT_DIR/quick-docker-validation.sh" > "$quick_results/output.log" 2>&1; then
        local test_end=$(date +%s)
        local duration=$((test_end - test_start))
        
        test_log "SUCCESS" "Quick validation passed in ${duration}s"
        echo "PASSED" > "$quick_results/status.txt"
        echo "$duration" > "$quick_results/duration.txt"
        return 0
    else
        local test_end=$(date +%s)
        local duration=$((test_end - test_start))
        
        test_log "ERROR" "Quick validation failed in ${duration}s"
        echo "FAILED" > "$quick_results/status.txt"
        echo "$duration" > "$quick_results/duration.txt"
        
        test_log "INFO" "Quick validation failure details:"
        tail -10 "$quick_results/output.log" | while read line; do
            test_log "ERROR" "  $line"
        done
        
        return 1
    fi
}

# Function to test full orchestrator
test_full_orchestrator() {
    test_log "TEST" "Testing full Docker validator orchestrator..."
    
    local test_start=$(date +%s)
    local full_results="$RESULTS_DIR/full-orchestrator"
    mkdir -p "$full_results"
    
    # Set environment variables for controlled test
    export ZERO_TOLERANCE="true"
    export VERBOSE="true"
    export ARCHIVE_RESULTS="true"
    
    # Run full orchestrator
    if timeout 1800 "$SCRIPT_DIR/docker-validator-orchestrator.sh" > "$full_results/output.log" 2>&1; then
        local test_end=$(date +%s)
        local duration=$((test_end - test_start))
        
        test_log "SUCCESS" "Full orchestrator passed in ${duration}s"
        echo "PASSED" > "$full_results/status.txt"
        echo "$duration" > "$full_results/duration.txt"
        
        # Copy orchestrator results if available
        local latest_results=$(find "$PROJECT_ROOT/validation-artifacts" -name "run-*" -type d | sort | tail -1 2>/dev/null || true)
        if [ -n "$latest_results" ] && [ -d "$latest_results" ]; then
            cp -r "$latest_results" "$full_results/orchestrator-artifacts/" 2>/dev/null || true
        fi
        
        return 0
    else
        local test_end=$(date +%s)
        local duration=$((test_end - test_start))
        
        test_log "ERROR" "Full orchestrator failed in ${duration}s"
        echo "FAILED" > "$full_results/status.txt"
        echo "$duration" > "$full_results/duration.txt"
        
        test_log "INFO" "Full orchestrator failure details:"
        tail -20 "$full_results/output.log" | while read line; do
            test_log "ERROR" "  $line"
        done
        
        return 1
    fi
}

# Function to test CI validation
test_ci_validation() {
    test_log "TEST" "Testing CI Docker validation..."
    
    local test_start=$(date +%s)
    local ci_results="$RESULTS_DIR/ci-validation"
    mkdir -p "$ci_results"
    
    # Set CI environment variables
    export CI_MODE="true"
    export GITHUB_ACTIONS="false"
    export STRICT_MODE="true"
    export FAIL_FAST="false"
    export CI_RESULTS_DIR="$ci_results/ci-artifacts"
    
    # Run CI validation
    if timeout 2400 "$SCRIPT_DIR/ci-docker-validation.sh" > "$ci_results/output.log" 2>&1; then
        local test_end=$(date +%s)
        local duration=$((test_end - test_start))
        
        test_log "SUCCESS" "CI validation passed in ${duration}s"
        echo "PASSED" > "$ci_results/status.txt"
        echo "$duration" > "$ci_results/duration.txt"
        return 0
    else
        local test_end=$(date +%s)
        local duration=$((test_end - test_start))
        
        test_log "ERROR" "CI validation failed in ${duration}s"
        echo "FAILED" > "$ci_results/status.txt"
        echo "$duration" > "$ci_results/duration.txt"
        
        test_log "INFO" "CI validation failure details:"
        tail -20 "$ci_results/output.log" | while read line; do
            test_log "ERROR" "  $line"
        done
        
        return 1
    fi
}

# Function to test Docker Compose configurations
test_docker_compose_configs() {
    test_log "TEST" "Testing Docker Compose configurations..."
    
    local compose_results="$RESULTS_DIR/compose-configs"
    mkdir -p "$compose_results"
    
    local docker_dir="$PROJECT_ROOT/test-data/docker"
    local configs=("docker-compose.yml" "docker-compose-cassandra5.yml" "docker-compose-multi-version.yml")
    
    for config in "${configs[@]}"; do
        local config_file="$docker_dir/$config"
        
        if [ ! -f "$config_file" ]; then
            test_log "ERROR" "Config file not found: $config_file"
            echo "MISSING" > "$compose_results/${config%.yml}_status.txt"
            continue
        fi
        
        test_log "INFO" "Validating $config..."
        
        # Validate Docker Compose syntax
        if docker-compose -f "$config_file" config > "$compose_results/${config%.yml}_validated.yml" 2> "$compose_results/${config%.yml}_errors.log"; then
            test_log "SUCCESS" "$config validation passed"
            echo "VALID" > "$compose_results/${config%.yml}_status.txt"
        else
            test_log "ERROR" "$config validation failed"
            echo "INVALID" > "$compose_results/${config%.yml}_status.txt"
            
            test_log "INFO" "Validation errors for $config:"
            cat "$compose_results/${config%.yml}_errors.log" | while read line; do
                test_log "ERROR" "  $line"
            done
        fi
    done
}

# Function to test validator build
test_validator_build() {
    test_log "TEST" "Testing validator build process..."
    
    local build_results="$RESULTS_DIR/validator-build"
    mkdir -p "$build_results"
    
    local validator_dir="$PROJECT_ROOT/tools/sstabledump-validator"
    
    if [ ! -d "$validator_dir" ]; then
        test_log "ERROR" "Validator directory not found: $validator_dir"
        echo "MISSING" > "$build_results/status.txt"
        return 1
    fi
    
    cd "$validator_dir"
    
    # Clean previous build
    cargo clean > "$build_results/clean.log" 2>&1 || true
    
    # Test build
    local build_start=$(date +%s)
    if cargo build --release > "$build_results/build.log" 2>&1; then
        local build_end=$(date +%s)
        local duration=$((build_end - build_start))
        
        test_log "SUCCESS" "Validator build succeeded in ${duration}s"
        echo "PASSED" > "$build_results/status.txt"
        echo "$duration" > "$build_results/duration.txt"
        
        # Test if binary works
        if ./target/release/sstabledump-validator --help > "$build_results/help.txt" 2>&1; then
            test_log "SUCCESS" "Validator binary is functional"
            echo "FUNCTIONAL" > "$build_results/binary_status.txt"
        else
            test_log "ERROR" "Validator binary is not functional"
            echo "BROKEN" > "$build_results/binary_status.txt"
        fi
        
        cd "$PROJECT_ROOT"
        return 0
    else
        local build_end=$(date +%s)
        local duration=$((build_end - build_start))
        
        test_log "ERROR" "Validator build failed in ${duration}s"
        echo "FAILED" > "$build_results/status.txt"
        echo "$duration" > "$build_results/duration.txt"
        
        test_log "INFO" "Build failure details:"
        tail -20 "$build_results/build.log" | while read line; do
            test_log "ERROR" "  $line"
        done
        
        cd "$PROJECT_ROOT"
        return 1
    fi
}

# Function to cleanup test environment
cleanup_test_environment() {
    if [ "$CLEANUP_AFTER" = "true" ]; then
        test_log "INFO" "Cleaning up test environment..."
        
        # Stop all Docker containers
        local docker_dir="$PROJECT_ROOT/test-data/docker"
        cd "$docker_dir"
        
        for compose_file in docker-compose*.yml; do
            if [ -f "$compose_file" ]; then
                docker-compose -f "$compose_file" down --remove-orphans --volumes >/dev/null 2>&1 || true
            fi
        done
        
        # Clean up Docker resources
        docker system prune -f >/dev/null 2>&1 || true
        
        test_log "SUCCESS" "Test environment cleanup complete"
    else
        test_log "INFO" "Skipping cleanup (CLEANUP_AFTER=false)"
    fi
}

# Function to generate comprehensive test report
generate_test_report() {
    test_log "INFO" "Generating comprehensive test report..."
    
    local report_file="$RESULTS_DIR/infrastructure_test_report.md"
    local passed_tests=0
    local failed_tests=0
    local total_tests=0
    
    # Count test results
    for status_file in $(find "$RESULTS_DIR" -name "status.txt"); do
        local status=$(cat "$status_file" 2>/dev/null || echo "UNKNOWN")
        total_tests=$((total_tests + 1))
        
        if [ "$status" = "PASSED" ] || [ "$status" = "VALID" ] || [ "$status" = "FUNCTIONAL" ]; then
            passed_tests=$((passed_tests + 1))
        else
            failed_tests=$((failed_tests + 1))
        fi
    done
    
    local success_rate=$(echo "scale=2; $passed_tests * 100 / $total_tests" | bc -l 2>/dev/null || echo "0.00")
    
    cat > "$report_file" << EOF
# CQLite Docker Infrastructure Test Report

**Issue #30**: Docker infrastructure validation testing  
**Status**: $([ $failed_tests -eq 0 ] && echo "✅ PASSED" || echo "❌ FAILED")  
**Generated**: $(date -Iseconds)

## Executive Summary

- **Total Tests**: $total_tests
- **Passed**: $passed_tests
- **Failed**: $failed_tests
- **Success Rate**: ${success_rate}%

## Test Results

### Quick Validation Test
$([ -f "$RESULTS_DIR/quick-validation/status.txt" ] && echo "- Status: $(cat "$RESULTS_DIR/quick-validation/status.txt")" || echo "- Status: NOT RUN")
$([ -f "$RESULTS_DIR/quick-validation/duration.txt" ] && echo "- Duration: $(cat "$RESULTS_DIR/quick-validation/duration.txt")s" || echo "- Duration: N/A")

### Full Orchestrator Test
$([ -f "$RESULTS_DIR/full-orchestrator/status.txt" ] && echo "- Status: $(cat "$RESULTS_DIR/full-orchestrator/status.txt")" || echo "- Status: NOT RUN")
$([ -f "$RESULTS_DIR/full-orchestrator/duration.txt" ] && echo "- Duration: $(cat "$RESULTS_DIR/full-orchestrator/duration.txt")s" || echo "- Duration: N/A")

### CI Validation Test
$([ -f "$RESULTS_DIR/ci-validation/status.txt" ] && echo "- Status: $(cat "$RESULTS_DIR/ci-validation/status.txt")" || echo "- Status: NOT RUN")
$([ -f "$RESULTS_DIR/ci-validation/duration.txt" ] && echo "- Duration: $(cat "$RESULTS_DIR/ci-validation/duration.txt")s" || echo "- Duration: N/A")

### Validator Build Test
$([ -f "$RESULTS_DIR/validator-build/status.txt" ] && echo "- Status: $(cat "$RESULTS_DIR/validator-build/status.txt")" || echo "- Status: NOT RUN")
$([ -f "$RESULTS_DIR/validator-build/duration.txt" ] && echo "- Duration: $(cat "$RESULTS_DIR/validator-build/duration.txt")s" || echo "- Duration: N/A")

### Docker Compose Configuration Tests
$([ -f "$RESULTS_DIR/compose-configs/docker-compose_status.txt" ] && echo "- docker-compose.yml: $(cat "$RESULTS_DIR/compose-configs/docker-compose_status.txt")" || echo "- docker-compose.yml: NOT TESTED")
$([ -f "$RESULTS_DIR/compose-configs/docker-compose-cassandra5_status.txt" ] && echo "- docker-compose-cassandra5.yml: $(cat "$RESULTS_DIR/compose-configs/docker-compose-cassandra5_status.txt")" || echo "- docker-compose-cassandra5.yml: NOT TESTED")
$([ -f "$RESULTS_DIR/compose-configs/docker-compose-multi-version_status.txt" ] && echo "- docker-compose-multi-version.yml: $(cat "$RESULTS_DIR/compose-configs/docker-compose-multi-version_status.txt")" || echo "- docker-compose-multi-version.yml: NOT TESTED")

## Environment Information

- **Docker Version**: $(docker --version)
- **Docker Compose Version**: $(docker-compose --version)
- **Rust Version**: $(rustc --version 2>/dev/null || echo "Not available")
- **System**: $(uname -s) $(uname -r)
- **Test Time**: $(date -Iseconds)

## Artifacts

- **Test Results**: \`$RESULTS_DIR\`
- **Quick Validation**: \`$RESULTS_DIR/quick-validation/\`
- **Full Orchestrator**: \`$RESULTS_DIR/full-orchestrator/\`
- **CI Validation**: \`$RESULTS_DIR/ci-validation/\`
- **Validator Build**: \`$RESULTS_DIR/validator-build/\`
- **Compose Configs**: \`$RESULTS_DIR/compose-configs/\`

## Recommendations

$(if [ $failed_tests -eq 0 ]; then
    echo "✅ **All tests passed**: Docker infrastructure is ready for production use"
    echo "- Ready for M1 release"
    echo "- CI integration can proceed"
    echo "- Zero-tolerance validation is operational"
else
    echo "❌ **Test failures detected**: Docker infrastructure needs attention"
    echo "- Review failed test logs"
    echo "- Fix identified issues before M1 release"
    echo "- Re-run tests after fixes"
fi)

---

**Generated by**: CQLite Docker Infrastructure Test Suite  
**Issue**: #30 - Validator on Docker infrastructure against real SSTables  
**Milestone**: M1 P0 Blocker  
EOF
    
    test_log "SUCCESS" "Test report generated: $report_file"
    
    # Create simple status file for CI
    echo "$([ $failed_tests -eq 0 ] && echo "PASSED" || echo "FAILED")" > "$RESULTS_DIR/overall_status.txt"
}

# Function to display final summary
display_final_summary() {
    local overall_status=$(cat "$RESULTS_DIR/overall_status.txt" 2>/dev/null || echo "UNKNOWN")
    
    echo ""
    echo -e "${WHITE}═══════════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${WHITE}                        INFRASTRUCTURE TEST COMPLETE                          ${NC}"
    echo -e "${WHITE}═══════════════════════════════════════════════════════════════════════════════${NC}"
    
    if [ "$overall_status" = "PASSED" ]; then
        echo -e "${GREEN}🎉 ALL INFRASTRUCTURE TESTS PASSED${NC}"
        echo -e "${GREEN}✅ Docker validation infrastructure is operational${NC}"
        echo -e "${GREEN}✅ Ready for Issue #38 CI integration${NC}"
        echo -e "${GREEN}✅ M1 P0 blocker resolved${NC}"
    else
        echo -e "${RED}❌ INFRASTRUCTURE TEST FAILURES DETECTED${NC}"
        echo -e "${RED}⚠️  Docker infrastructure needs fixes${NC}"
        echo -e "${RED}⚠️  M1 release blocked${NC}"
    fi
    
    echo ""
    echo -e "${BLUE}📋 Test Report: $RESULTS_DIR/infrastructure_test_report.md${NC}"
    echo -e "${BLUE}📁 Test Artifacts: $RESULTS_DIR${NC}"
    echo ""
}

# Main execution function
main() {
    # Create results directory
    mkdir -p "$RESULTS_DIR"
    
    test_log "INFO" "Starting Docker infrastructure test suite"
    test_log "INFO" "Results directory: $RESULTS_DIR"
    
    # Set up error handling
    set -E
    trap 'test_log "ERROR" "Test suite failed at line $LINENO"' ERR
    trap cleanup_test_environment EXIT
    
    # Run prerequisite checks
    if ! check_test_prerequisites; then
        test_log "ERROR" "Prerequisites check failed"
        exit 1
    fi
    
    # Run Docker Compose configuration tests
    test_docker_compose_configs
    
    # Run validator build test
    test_validator_build
    
    # Run validation tests based on configuration
    local test_failed=false
    
    if [ "$RUN_QUICK_TEST" = "true" ]; then
        if ! test_quick_validation; then
            test_failed=true
        fi
    fi
    
    if [ "$RUN_FULL_TEST" = "true" ]; then
        if ! test_full_orchestrator; then
            test_failed=true
        fi
    fi
    
    if [ "$RUN_CI_TEST" = "true" ]; then
        if ! test_ci_validation; then
            test_failed=true
        fi
    fi
    
    # Generate comprehensive report
    generate_test_report
    display_final_summary
    
    # Exit with appropriate code
    if [ "$test_failed" = "true" ]; then
        test_log "ERROR" "Some infrastructure tests failed"
        exit 1
    else
        test_log "SUCCESS" "All infrastructure tests passed!"
        exit 0
    fi
}

# Handle command line arguments
case "${1:-}" in
    --help|-h)
        echo "CQLite Docker Infrastructure Test Suite"
        echo ""
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --quick-only          Run only quick validation test"
        echo "  --full-only           Run only full orchestrator test"
        echo "  --ci-only             Run only CI validation test"
        echo "  --all                 Run all tests (default: quick only)"
        echo "  --no-cleanup          Don't cleanup after tests"
        echo "  --help                Show this help"
        echo ""
        echo "Environment Variables:"
        echo "  RUN_QUICK_TEST        Run quick test (default: true)"
        echo "  RUN_FULL_TEST         Run full test (default: false)"
        echo "  RUN_CI_TEST           Run CI test (default: false)"
        echo "  CLEANUP_AFTER         Cleanup after tests (default: true)"
        echo ""
        exit 0
        ;;
    --quick-only)
        RUN_QUICK_TEST="true"
        RUN_FULL_TEST="false"
        RUN_CI_TEST="false"
        main
        ;;
    --full-only)
        RUN_QUICK_TEST="false"
        RUN_FULL_TEST="true"
        RUN_CI_TEST="false"
        main
        ;;
    --ci-only)
        RUN_QUICK_TEST="false"
        RUN_FULL_TEST="false"
        RUN_CI_TEST="true"
        main
        ;;
    --all)
        RUN_QUICK_TEST="true"
        RUN_FULL_TEST="true"
        RUN_CI_TEST="true"
        main
        ;;
    --no-cleanup)
        CLEANUP_AFTER="false"
        main
        ;;
    *)
        # Default: run quick test only
        main
        ;;
esac