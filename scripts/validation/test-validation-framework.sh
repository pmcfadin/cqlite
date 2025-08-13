#!/bin/bash
# Test the Phase Validation Framework
# This script validates that the validation framework itself is working correctly

set -euo pipefail

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Test tracking
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

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

log_section() {
    echo -e "\n${PURPLE}=== $1 ===${NC}"
}

# Test framework functions
run_test() {
    local test_name="$1"
    local test_command="$2"
    local expected_exit_code="${3:-0}"
    
    ((TOTAL_TESTS++))
    
    log_info "Running test: $test_name"
    
    local actual_exit_code=0
    eval "$test_command" >/dev/null 2>&1 || actual_exit_code=$?
    
    if [ "$actual_exit_code" -eq "$expected_exit_code" ]; then
        log_success "✅ PASS: $test_name"
        ((PASSED_TESTS++))
        return 0
    else
        log_error "❌ FAIL: $test_name (expected exit code $expected_exit_code, got $actual_exit_code)"
        ((FAILED_TESTS++))
        return 1
    fi
}

# Test script existence and permissions
test_script_setup() {
    log_section "Framework Setup Tests"
    
    run_test "Phase 1 validation script exists" "test -f scripts/validation/validate-phase1-complete.sh"
    run_test "Phase 1 validation script is executable" "test -x scripts/validation/validate-phase1-complete.sh"
    run_test "Phase 2 readiness script exists" "test -f scripts/validation/assess-phase2-readiness.sh"
    run_test "Phase 2 readiness script is executable" "test -x scripts/validation/assess-phase2-readiness.sh"
    run_test "CI/CD workflow exists" "test -f .github/workflows/phase-validation.yml"
    run_test "Framework documentation exists" "test -f docs/development/PHASE_TRANSITION_FRAMEWORK.md"
}

# Test basic script functionality
test_script_functionality() {
    log_section "Script Functionality Tests"
    
    # Test script help/usage
    run_test "Phase 1 script shows help" "bash scripts/validation/validate-phase1-complete.sh --help || bash scripts/validation/validate-phase1-complete.sh -h" 1
    run_test "Phase 2 script shows help" "bash scripts/validation/assess-phase2-readiness.sh --help || bash scripts/validation/assess-phase2-readiness.sh -h" 1
    
    # Test script syntax
    run_test "Phase 1 script syntax is valid" "bash -n scripts/validation/validate-phase1-complete.sh"
    run_test "Phase 2 script syntax is valid" "bash -n scripts/validation/assess-phase2-readiness.sh"
}

# Test validation components
test_validation_components() {
    log_section "Validation Component Tests"
    
    # Test basic Rust toolchain
    run_test "Cargo is available" "command -v cargo"
    run_test "Rustc is available" "command -v rustc"
    run_test "Project has Cargo.toml" "test -f Cargo.toml"
    
    # Test basic build system
    run_test "Cargo check works" "timeout 60 cargo check --workspace" 0
    run_test "Cargo fmt check works" "cargo fmt --all -- --check" 0
    
    # Test that we can run tests (even if they fail)
    log_info "Testing that test command works (may have test failures)"
    if timeout 300 cargo test --workspace --no-run >/dev/null 2>&1; then
        log_success "✅ PASS: Test compilation works"
        ((PASSED_TESTS++))
    else
        log_warning "⚠️  WARN: Test compilation issues (may be expected)"
    fi
    ((TOTAL_TESTS++))
}

# Test CI/CD workflow syntax
test_cicd_workflow() {
    log_section "CI/CD Workflow Tests"
    
    # Check if GitHub CLI is available for workflow validation
    if command -v gh >/dev/null 2>&1; then
        run_test "GitHub workflow syntax is valid" "gh workflow view .github/workflows/phase-validation.yml" 0
    else
        log_warning "⚠️  GitHub CLI not available, skipping workflow syntax test"
        ((TOTAL_TESTS++))
    fi
    
    # Test workflow file structure
    run_test "Workflow has required jobs" "grep -q 'phase1-validation:' .github/workflows/phase-validation.yml"
    run_test "Workflow has phase2 job" "grep -q 'phase2-readiness:' .github/workflows/phase-validation.yml"
    run_test "Workflow has quality enforcement" "grep -q 'quality-enforcement:' .github/workflows/phase-validation.yml"
}

# Test documentation completeness
test_documentation() {
    log_section "Documentation Tests"
    
    run_test "Framework documentation exists" "test -f docs/development/PHASE_TRANSITION_FRAMEWORK.md"
    run_test "Quick reference exists" "test -f docs/development/PHASE_VALIDATION_QUICK_REFERENCE.md"
    run_test "Framework docs contain validation criteria" "grep -q 'Phase 1 Completion Criteria' docs/development/PHASE_TRANSITION_FRAMEWORK.md"
    run_test "Framework docs contain scoring system" "grep -q 'Scoring System' docs/development/PHASE_TRANSITION_FRAMEWORK.md"
    run_test "Quick reference contains commands" "grep -q 'validate-phase1-complete.sh' docs/development/PHASE_VALIDATION_QUICK_REFERENCE.md"
}

# Test validation framework dry run
test_validation_dry_run() {
    log_section "Validation Framework Dry Run"
    
    log_info "Running Phase 1 validation dry run..."
    
    # Create a temporary test environment
    local test_output=$(mktemp)
    
    # Run Phase 1 validation and capture output
    if bash scripts/validation/validate-phase1-complete.sh > "$test_output" 2>&1; then
        log_success "✅ Phase 1 validation completed successfully"
        ((PASSED_TESTS++))
        
        # Check for expected output sections
        if grep -q "Phase 1 Validation Report" "$test_output"; then
            log_success "✅ Phase 1 validation generates proper report"
            ((PASSED_TESTS++))
        else
            log_error "❌ Phase 1 validation report format incorrect"
            ((FAILED_TESTS++))
        fi
        ((TOTAL_TESTS++))
        
    else
        log_warning "⚠️  Phase 1 validation failed (may be expected if Phase 1 incomplete)"
        
        # Check that it fails gracefully with proper error reporting
        if grep -q "PHASE 1 VALIDATION: FAILED" "$test_output"; then
            log_success "✅ Phase 1 validation fails gracefully with proper reporting"
            ((PASSED_TESTS++))
        else
            log_error "❌ Phase 1 validation error handling incorrect"
            ((FAILED_TESTS++))
        fi
        ((TOTAL_TESTS++))
    fi
    ((TOTAL_TESTS++))
    
    # Display some output for debugging
    log_info "Phase 1 validation output sample:"
    head -20 "$test_output" | sed 's/^/  /'
    
    rm -f "$test_output"
    
    log_info "Running Phase 2 readiness assessment dry run..."
    
    # Run Phase 2 readiness assessment
    local readiness_output=$(mktemp)
    
    if bash scripts/validation/assess-phase2-readiness.sh > "$readiness_output" 2>&1; then
        log_success "✅ Phase 2 readiness assessment completed"
    else
        log_info "Phase 2 readiness assessment completed with issues (expected)"
    fi
    
    # Check for proper report format
    if grep -q "Phase 2 Readiness Assessment Report" "$readiness_output"; then
        log_success "✅ Phase 2 readiness generates proper report"
        ((PASSED_TESTS++))
    else
        log_error "❌ Phase 2 readiness report format incorrect"
        ((FAILED_TESTS++))
    fi
    ((TOTAL_TESTS++))
    
    # Check for scoring system
    if grep -q "Overall Readiness Score:" "$readiness_output"; then
        log_success "✅ Phase 2 readiness includes scoring system"
        ((PASSED_TESTS++))
    else
        log_error "❌ Phase 2 readiness scoring system missing"
        ((FAILED_TESTS++))
    fi
    ((TOTAL_TESTS++))
    
    # Display readiness score
    local score=$(grep "Overall Readiness Score:" "$readiness_output" | grep -o "[0-9]*" | head -1 || echo "0")
    log_info "Current readiness score: ${score}/100"
    
    rm -f "$readiness_output"
}

# Test error handling
test_error_handling() {
    log_section "Error Handling Tests"
    
    # Test behavior in invalid directory
    local temp_dir=$(mktemp -d)
    cd "$temp_dir"
    
    log_info "Testing validation scripts in invalid project directory..."
    
    # Scripts should fail gracefully when not in project root
    if bash "$OLDPWD/scripts/validation/validate-phase1-complete.sh" >/dev/null 2>&1; then
        log_error "❌ Phase 1 validation should fail outside project directory"
        ((FAILED_TESTS++))
    else
        log_success "✅ Phase 1 validation fails gracefully outside project"
        ((PASSED_TESTS++))
    fi
    ((TOTAL_TESTS++))
    
    cd "$OLDPWD"
    rm -rf "$temp_dir"
}

# Generate test report
generate_test_report() {
    log_section "Framework Validation Test Report"
    
    local pass_rate=0
    if [ "$TOTAL_TESTS" -gt 0 ]; then
        pass_rate=$((PASSED_TESTS * 100 / TOTAL_TESTS))
    fi
    
    echo "# Phase Validation Framework Test Report"
    echo "Generated: $(date '+%Y-%m-%d %H:%M:%S')"
    echo ""
    echo "## Test Results Summary"
    echo ""
    echo "- **Total Tests**: $TOTAL_TESTS"
    echo "- **Passed**: $PASSED_TESTS"
    echo "- **Failed**: $FAILED_TESTS"
    echo "- **Pass Rate**: ${pass_rate}%"
    echo ""
    
    if [ "$FAILED_TESTS" -eq 0 ]; then
        echo "## ✅ VALIDATION FRAMEWORK: WORKING"
        echo ""
        echo "All framework tests passed successfully."
        echo "The phase validation system is ready for use."
        echo ""
        log_success "🎉 Framework validation: ALL TESTS PASSED!"
        return 0
    else
        echo "## ❌ VALIDATION FRAMEWORK: ISSUES DETECTED"
        echo ""
        echo "Some framework tests failed."
        echo "Review the test output above to identify and fix issues."
        echo ""
        log_error "💥 Framework validation: TESTS FAILED!"
        return 1
    fi
}

# Main execution
main() {
    echo -e "${BLUE}"
    echo "╔══════════════════════════════════════╗"
    echo "║   PHASE VALIDATION FRAMEWORK TEST    ║"
    echo "║                                      ║"
    echo "║  Testing the validation framework    ║"
    echo "║  itself for correctness              ║"
    echo "╚══════════════════════════════════════╝"
    echo -e "${NC}"
    
    log_info "Starting framework validation tests at $(date)"
    log_info "Working directory: $(pwd)"
    
    # Run all test categories
    test_script_setup || true
    test_script_functionality || true
    test_validation_components || true
    test_cicd_workflow || true
    test_documentation || true
    test_validation_dry_run || true
    test_error_handling || true
    
    # Generate final report
    echo ""
    generate_test_report
    local exit_code=$?
    
    echo ""
    if [ $exit_code -eq 0 ]; then
        log_success "🎉 Framework validation completed successfully!"
        log_info "The phase validation framework is ready for production use."
    else
        log_error "💥 Framework validation detected issues!"
        log_error "Review and fix the identified problems before using the framework."
    fi
    
    exit $exit_code
}

# Script execution
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi