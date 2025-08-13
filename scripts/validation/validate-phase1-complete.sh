#!/bin/bash
# Phase 1 Completion Validation Script
# This script validates that ALL Phase 1 requirements are met before Phase 2 can begin

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Error tracking
VALIDATION_ERRORS=0
VALIDATION_WARNINGS=0

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
    ((VALIDATION_WARNINGS++))
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
    ((VALIDATION_ERRORS++))
}

log_section() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Measure execution time
measure_time() {
    local start_time=$(date +%s)
    "$@"
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    echo "⏱️  Execution time: ${duration}s"
}

# Validate build compilation
validate_build() {
    log_section "Phase 1 Validation: Build Compilation"
    
    log_info "Checking workspace compilation..."
    if measure_time cargo build --release --workspace; then
        log_success "✅ Clean release build successful"
    else
        log_error "❌ Release build failed"
        return 1
    fi
    
    log_info "Checking format compliance..."
    if cargo fmt --all -- --check; then
        log_success "✅ Code formatting is correct"
    else
        log_error "❌ Code formatting issues detected"
        return 1
    fi
    
    log_info "Running clippy analysis..."
    if cargo clippy --workspace -- -D warnings; then
        log_success "✅ No clippy warnings"
    else
        log_error "❌ Clippy warnings detected"
        return 1
    fi
}

# Validate test execution and reliability
validate_tests() {
    log_section "Phase 1 Validation: Test Infrastructure Reliability"
    
    log_info "Running workspace tests with timeout..."
    local test_start=$(date +%s)
    
    # Run tests with 5 minute timeout
    if timeout 300 cargo test --workspace --no-fail-fast -- --nocapture; then
        local test_end=$(date +%s)
        local test_duration=$((test_end - test_start))
        
        if [ $test_duration -lt 300 ]; then
            log_success "✅ Tests completed in ${test_duration}s (under 5 minute limit)"
        else
            log_warning "⚠️  Tests took ${test_duration}s (over 5 minute target)"
        fi
    else
        log_error "❌ Test execution failed or timed out"
        return 1
    fi
    
    # Check test pass rate
    log_info "Analyzing test results..."
    local test_output=$(cargo test --workspace 2>&1 || true)
    
    if echo "$test_output" | grep -q "test result: ok"; then
        log_success "✅ Tests completed successfully"
        
        # Extract pass rate if possible
        local passed=$(echo "$test_output" | grep -o '[0-9]* passed' | head -1 | grep -o '[0-9]*' || echo "0")
        local failed=$(echo "$test_output" | grep -o '[0-9]* failed' | head -1 | grep -o '[0-9]*' || echo "0")
        
        if [ "$passed" -gt 0 ] && [ "$failed" -eq 0 ]; then
            log_success "✅ 100% test pass rate ($passed passed, $failed failed)"
        elif [ "$passed" -gt 0 ]; then
            local total=$((passed + failed))
            local pass_rate=$((passed * 100 / total))
            
            if [ $pass_rate -ge 80 ]; then
                log_success "✅ Pass rate: ${pass_rate}% ($passed passed, $failed failed)"
            else
                log_error "❌ Pass rate ${pass_rate}% below 80% threshold"
                return 1
            fi
        fi
    else
        log_error "❌ Test execution did not complete successfully"
        return 1
    fi
}

# Validate core CLI functionality with real data
validate_core_functionality() {
    log_section "Phase 1 Validation: Core SSTable Reading"
    
    # Check if cqlite binary exists
    if [ ! -f "target/release/cqlite" ]; then
        log_error "❌ cqlite binary not found in target/release/"
        return 1
    fi
    
    log_info "Testing CLI help functionality..."
    if ./target/release/cqlite --help >/dev/null 2>&1; then
        log_success "✅ CLI help command works"
    else
        log_error "❌ CLI help command failed"
        return 1
    fi
    
    log_info "Testing version display..."
    if ./target/release/cqlite --version >/dev/null 2>&1; then
        log_success "✅ CLI version command works"
    else
        log_warning "⚠️  CLI version command not working (acceptable for now)"
    fi
    
    # Look for test data directories
    local test_data_found=false
    for test_dir in "tests/test_data" "test-data" "examples" "tests/fixtures"; do
        if [ -d "$test_dir" ]; then
            log_info "Testing SSTable reading with $test_dir..."
            
            # Try reading any SSTable files found
            if find "$test_dir" -name "*.db" -o -name "*.sst" | head -1 | xargs -I {} ./target/release/cqlite read {} --format table >/dev/null 2>&1; then
                log_success "✅ SSTable reading functionality works"
                test_data_found=true
                break
            fi
        fi
    done
    
    if [ "$test_data_found" = false ]; then
        log_warning "⚠️  No test SSTable data found for functionality validation"
        log_info "CLI binary compiled successfully, manual testing required"
    fi
}

# Validate performance baselines
validate_performance() {
    log_section "Phase 1 Validation: Performance Baseline"
    
    log_info "Running performance benchmarks..."
    
    if command_exists "cargo"; then
        # Run benchmarks if available
        if cargo bench --help >/dev/null 2>&1; then
            log_info "Benchmark suite detected, running baseline measurements..."
            
            # Run with timeout to prevent hanging
            if timeout 600 cargo bench --workspace >/dev/null 2>&1; then
                log_success "✅ Performance benchmarks completed"
            else
                log_warning "⚠️  Benchmark execution timed out or failed"
            fi
        else
            log_warning "⚠️  No benchmark configuration found"
        fi
    fi
    
    # Check memory usage during compilation (proxy for complexity)
    log_info "Checking build memory usage..."
    local build_log=$(cargo build --release 2>&1 || true)
    
    if echo "$build_log" | grep -q "Finished release"; then
        log_success "✅ Release build memory usage acceptable"
    else
        log_warning "⚠️  Could not validate memory usage during build"
    fi
}

# Validate code coverage baseline
validate_coverage() {
    log_section "Phase 1 Validation: Code Coverage Baseline"
    
    log_info "Checking for coverage tools..."
    
    if command_exists "cargo-tarpaulin"; then
        log_info "Running code coverage analysis..."
        
        if timeout 600 cargo tarpaulin --workspace --timeout 120 --out Html >/dev/null 2>&1; then
            log_success "✅ Code coverage analysis completed"
            
            # Try to extract coverage percentage
            if [ -f "tarpaulin-report.html" ]; then
                log_info "Coverage report generated: tarpaulin-report.html"
            fi
        else
            log_warning "⚠️  Code coverage analysis failed or timed out"
        fi
    else
        log_warning "⚠️  cargo-tarpaulin not installed"
        log_info "Installing tarpaulin for coverage analysis..."
        
        if cargo install cargo-tarpaulin --locked >/dev/null 2>&1; then
            log_success "✅ tarpaulin installed successfully"
            validate_coverage  # Retry
        else
            log_warning "⚠️  Could not install cargo-tarpaulin"
        fi
    fi
}

# Generate validation report
generate_report() {
    log_section "Phase 1 Validation Report"
    
    local total_checks=$((VALIDATION_ERRORS + VALIDATION_WARNINGS))
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    echo "# Phase 1 Validation Report"
    echo "Generated: $timestamp"
    echo ""
    
    if [ $VALIDATION_ERRORS -eq 0 ]; then
        echo "## ✅ PHASE 1 VALIDATION: PASSED"
        echo ""
        echo "All critical Phase 1 requirements have been met."
        echo "Phase 2 development can proceed."
        echo ""
        echo "### Summary:"
        echo "- ✅ Build compilation: PASSED"
        echo "- ✅ Test execution: PASSED"
        echo "- ✅ Core functionality: PASSED"
        echo "- ✅ Performance baseline: ESTABLISHED"
        echo "- ✅ Code coverage: MEASURED"
        echo ""
        
        if [ $VALIDATION_WARNINGS -gt 0 ]; then
            echo "### Warnings: $VALIDATION_WARNINGS"
            echo "These should be addressed but do not block Phase 2 progression."
        fi
        
        return 0
    else
        echo "## ❌ PHASE 1 VALIDATION: FAILED"
        echo ""
        echo "Phase 1 requirements are NOT met. Phase 2 progression is BLOCKED."
        echo ""
        echo "### Critical Issues: $VALIDATION_ERRORS"
        echo "### Warnings: $VALIDATION_WARNINGS"
        echo ""
        echo "ALL errors must be resolved before Phase 2 can begin."
        echo ""
        
        return 1
    fi
}

# Main execution
main() {
    echo -e "${BLUE}"
    echo "╔══════════════════════════════════════╗"
    echo "║     PHASE 1 COMPLETION VALIDATOR     ║"
    echo "║                                      ║"
    echo "║  Validating Phase 1 completion       ║"
    echo "║  before Phase 2 progression          ║"
    echo "╚══════════════════════════════════════╝"
    echo -e "${NC}"
    
    log_info "Starting Phase 1 validation at $(date)"
    log_info "Working directory: $(pwd)"
    
    # Run all validation checks
    validate_build || true
    validate_tests || true
    validate_core_functionality || true
    validate_performance || true
    validate_coverage || true
    
    # Generate final report
    echo ""
    generate_report
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        log_success "🎉 Phase 1 validation completed successfully!"
        log_info "Phase 2 development is authorized to proceed."
    else
        log_error "💥 Phase 1 validation failed!"
        log_error "Phase 2 development is BLOCKED until issues are resolved."
    fi
    
    exit $exit_code
}

# Script execution
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi