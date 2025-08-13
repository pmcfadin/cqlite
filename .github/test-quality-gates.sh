#!/bin/bash

# Quality Gates Testing Script
# This script tests the quality gate enforcement by introducing intentional failures
# and verifying that they are properly blocked by the CI/CD pipeline.

set -e

echo "🧪 CQLite Quality Gates Testing Script"
echo "====================================="
echo "This script will test quality gate enforcement by creating intentional failures."
echo "⚠️  This will create temporary test branches and may trigger CI builds."
echo ""

# Configuration
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEST_BRANCH_PREFIX="test-quality-gates"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
TEST_BRANCH="${TEST_BRANCH_PREFIX}-${TIMESTAMP}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if we're in a git repository
    if ! git rev-parse --git-dir > /dev/null 2>&1; then
        log_error "Not in a git repository"
        exit 1
    fi
    
    # Check if we're in the correct repository
    if [[ ! -f "Cargo.toml" ]] || ! grep -q "cqlite" Cargo.toml; then
        log_error "Not in the CQLite repository root"
        exit 1
    fi
    
    # Check required tools
    local missing_tools=()
    
    if ! command -v cargo &> /dev/null; then
        missing_tools+=("cargo")
    fi
    
    if ! command -v git &> /dev/null; then
        missing_tools+=("git")
    fi
    
    if ! command -v gh &> /dev/null; then
        log_warning "gh CLI not found - some features may be limited"
    fi
    
    if [[ ${#missing_tools[@]} -ne 0 ]]; then
        log_error "Missing required tools: ${missing_tools[*]}"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Backup current state
backup_current_state() {
    log_info "Backing up current state..."
    
    # Ensure we're on a clean state
    if [[ -n $(git status --porcelain) ]]; then
        log_warning "Working directory has uncommitted changes"
        git status --short
        
        read -p "Continue anyway? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            log_info "Aborting test"
            exit 0
        fi
    fi
    
    # Store current branch
    ORIGINAL_BRANCH=$(git branch --show-current)
    log_info "Current branch: $ORIGINAL_BRANCH"
    
    log_success "State backup complete"
}

# Create test branch
create_test_branch() {
    log_info "Creating test branch: $TEST_BRANCH"
    
    git checkout -b "$TEST_BRANCH"
    log_success "Test branch created"
}

# Test 1: Compilation failure
test_compilation_failure() {
    log_info "🔨 Testing compilation failure enforcement..."
    
    # Create a file with compilation errors
    cat > "${REPO_ROOT}/test_compilation_failure.rs" << 'EOF'
// This file contains intentional compilation errors to test quality gates

use std::collections::HashMap;

fn main() {
    // Error 1: Unused variable (warning that should be treated as error)
    let unused_variable = "test";
    
    // Error 2: Type mismatch
    let numbers: Vec<i32> = vec![1, 2, 3];
    let sum: String = numbers.iter().sum(); // This should fail
    
    // Error 3: Undefined function
    undefined_function();
    
    println!("This code should not compile");
}

// Error 4: Unused function (warning)
fn unused_function() {
    println!("This function is never called");
}

// Error 5: Missing return type
fn function_with_missing_return() {
    return 42;
}
EOF
    
    # Add to Cargo.toml as example
    if ! grep -q "test_compilation_failure" Cargo.toml; then
        cat >> Cargo.toml << 'EOF'

[[example]]
name = "test_compilation_failure"
path = "test_compilation_failure.rs"
EOF
    fi
    
    log_info "Compilation failure test file created"
    
    # Test local compilation (should fail)
    if cargo check --examples 2>/dev/null; then
        log_error "Expected compilation to fail, but it succeeded"
        return 1
    else
        log_success "Compilation correctly failed as expected"
    fi
    
    return 0
}

# Test 2: Test failure
test_test_failure() {
    log_info "🧪 Testing test failure enforcement..."
    
    # Create a test file with failing tests
    mkdir -p "${REPO_ROOT}/tests/quality_gate_tests"
    cat > "${REPO_ROOT}/tests/quality_gate_tests/test_failure_tests.rs" << 'EOF'
//! Intentional test failures to verify quality gate enforcement

#[cfg(test)]
mod tests {
    #[test]
    fn test_that_should_fail() {
        // This test is designed to fail
        assert_eq!(1 + 1, 3, "This test should fail to verify quality gates");
    }
    
    #[test]
    fn test_panic() {
        panic!("This test panics intentionally");
    }
    
    #[test]
    #[should_panic]
    fn test_should_panic_but_doesnt() {
        // This test expects panic but doesn't panic - should fail
        println!("This should panic but doesn't");
    }
    
    #[test]
    fn test_with_assertion_failure() {
        let expected = vec![1, 2, 3];
        let actual = vec![1, 2, 4];
        assert_eq!(expected, actual, "Vectors should be equal but aren't");
    }
}
EOF
    
    log_info "Test failure test file created"
    
    # Test local test execution (should fail)
    if cargo test test_failure_tests 2>/dev/null; then
        log_error "Expected tests to fail, but they succeeded"
        return 1
    else
        log_success "Tests correctly failed as expected"
    fi
    
    return 0
}

# Test 3: Code formatting violations
test_formatting_violations() {
    log_info "🎯 Testing code formatting violation enforcement..."
    
    # Create a file with formatting issues
    cat > "${REPO_ROOT}/badly_formatted.rs" << 'EOF'
// This file has intentional formatting violations

use std::collections::HashMap;
use std::io::{self,Write};

fn main(){
let mut map=HashMap::new();
map.insert("key","value");

for(key,value) in &map{
println!("{}:{}",key,value);
}

if true{
println!("Badly formatted");
}else{
println!("Also badly formatted");
}

let x=1+2+3+4+5;
let y=x*2;

match x{
1=>println!("one"),
2=>println!("two"),
_=>println!("other"),
}
}

struct BadlyFormattedStruct{pub field1:String,pub field2:i32}

impl BadlyFormattedStruct{
fn new()->Self{
Self{field1:"test".to_string(),field2:42}
}
}
EOF
    
    log_info "Formatting violation test file created"
    
    # Test local formatting check (should fail)
    if cargo fmt --all -- --check 2>/dev/null; then
        log_error "Expected formatting check to fail, but it succeeded"
        return 1
    else
        log_success "Formatting check correctly failed as expected"
    fi
    
    return 0
}

# Test 4: Clippy lint violations
test_clippy_violations() {
    log_info "🎯 Testing clippy lint violation enforcement..."
    
    # Create a file with clippy violations
    cat > "${REPO_ROOT}/clippy_violations.rs" << 'EOF'
// This file contains intentional clippy violations

#![allow(dead_code)] // Allow this to focus on other lints

use std::collections::HashMap;

// Clippy violation: needless_return
fn needless_return_function() -> i32 {
    return 42;
}

// Clippy violation: single_char_pattern  
fn single_char_pattern_violation(s: &str) -> Vec<&str> {
    s.split("x").collect()
}

// Clippy violation: redundant_clone
fn redundant_clone_violation() {
    let s = String::from("test");
    let _s2 = s.clone().clone();
}

// Clippy violation: useless_vec
fn useless_vec_violation() {
    for _ in vec![1, 2, 3].iter() {
        println!("test");
    }
}

// Clippy violation: manual_memcpy
fn manual_memcpy_violation(src: &[u8], dst: &mut [u8]) {
    for i in 0..src.len() {
        dst[i] = src[i];
    }
}

// Clippy violation: collapsible_if
fn collapsible_if_violation(x: i32, y: i32) {
    if x > 0 {
        if y > 0 {
            println!("Both positive");
        }
    }
}

// Clippy violation: unused_unit
fn unused_unit_violation() -> () {
    ()
}

// Clippy violation: len_zero
fn len_zero_violation(v: &Vec<i32>) -> bool {
    v.len() == 0
}

// Clippy violation: match_bool
fn match_bool_violation(b: bool) -> &'static str {
    match b {
        true => "yes",
        false => "no",
    }
}

// Clippy violation: unnecessary_mut_passed
fn unnecessary_mut_passed_violation() {
    let mut v = vec![1, 2, 3];
    let _len = v.len();
}
EOF
    
    log_info "Clippy violation test file created"
    
    # Test local clippy check (should fail)
    if cargo clippy --all-targets -- -D warnings 2>/dev/null; then
        log_error "Expected clippy to fail, but it succeeded"
        return 1
    else
        log_success "Clippy correctly failed as expected"
    fi
    
    return 0
}

# Test 5: Security audit failure (simulate with vulnerable dependency)
test_security_audit_failure() {
    log_info "🛡️ Testing security audit enforcement..."
    
    # Backup current Cargo.toml
    cp Cargo.toml Cargo.toml.backup
    
    # Add a known vulnerable dependency (example - adjust based on current advisories)
    cat >> Cargo.toml << 'EOF'

# Intentionally vulnerable dependency for testing (remove after test)
[dev-dependencies.vulnerable-test-dep]
package = "time"
version = "0.1.40"  # Known vulnerable version
EOF
    
    log_info "Added potentially vulnerable dependency for testing"
    
    # Test local security audit
    if cargo audit 2>/dev/null; then
        log_warning "Security audit passed - no current vulnerabilities in test dependency"
        log_info "This is expected if the vulnerability has been fixed"
    else
        log_success "Security audit correctly identified issues"
    fi
    
    # Restore original Cargo.toml
    mv Cargo.toml.backup Cargo.toml
    
    return 0
}

# Commit and push test failures
commit_and_push_failures() {
    log_info "Committing and pushing test failures..."
    
    git add .
    git commit -m "Test quality gate enforcement with intentional failures

This commit contains intentional failures to test quality gate enforcement:
- Compilation errors and warnings
- Failing tests  
- Code formatting violations
- Clippy lint violations

These should be blocked by the quality gate enforcement system.

DO NOT MERGE - This is a test commit"
    
    if git push origin "$TEST_BRANCH" 2>/dev/null; then
        log_success "Test branch pushed successfully"
        
        # If gh CLI is available, create a draft PR
        if command -v gh &> /dev/null; then
            log_info "Creating draft PR for testing..."
            
            if gh pr create --title "🧪 Quality Gate Test - DO NOT MERGE" \
                          --body "This is a test PR to verify quality gate enforcement. It contains intentional failures and should be blocked from merging." \
                          --draft \
                          --base "$ORIGINAL_BRANCH" \
                          --head "$TEST_BRANCH" 2>/dev/null; then
                log_success "Draft PR created for testing"
                log_info "You can view the PR and quality gate results in GitHub"
            else
                log_warning "Could not create PR automatically"
            fi
        fi
        
        return 0
    else
        log_error "Failed to push test branch"
        return 1
    fi
}

# Monitor quality gates
monitor_quality_gates() {
    log_info "Monitoring quality gate results..."
    
    if ! command -v gh &> /dev/null; then
        log_warning "gh CLI not available - cannot monitor quality gates automatically"
        log_info "Please check GitHub Actions manually for quality gate results"
        return 0
    fi
    
    log_info "Waiting for quality gates to run..."
    sleep 30
    
    # Check workflow runs
    log_info "Recent workflow runs for test branch:"
    gh run list --branch "$TEST_BRANCH" --limit 5 || true
    
    log_info "Check the GitHub Actions tab to see quality gate enforcement in action"
}

# Cleanup
cleanup() {
    log_info "Cleaning up test files and branches..."
    
    # Switch back to original branch
    git checkout "$ORIGINAL_BRANCH" 2>/dev/null || true
    
    # Remove test files if they exist
    rm -f "${REPO_ROOT}/test_compilation_failure.rs"
    rm -f "${REPO_ROOT}/badly_formatted.rs" 
    rm -f "${REPO_ROOT}/clippy_violations.rs"
    rm -rf "${REPO_ROOT}/tests/quality_gate_tests"
    
    # Restore Cargo.toml if backup exists
    if [[ -f Cargo.toml.backup ]]; then
        mv Cargo.toml.backup Cargo.toml
    fi
    
    # Ask about branch cleanup
    if git branch | grep -q "$TEST_BRANCH"; then
        log_warning "Test branch '$TEST_BRANCH' still exists"
        read -p "Delete test branch? (y/N) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            git branch -D "$TEST_BRANCH" 2>/dev/null || true
            
            # Delete remote branch if it exists
            if git ls-remote --heads origin "$TEST_BRANCH" | grep -q "$TEST_BRANCH"; then
                read -p "Delete remote test branch? (y/N) " -n 1 -r
                echo
                if [[ $REPLY =~ ^[Yy]$ ]]; then
                    git push origin --delete "$TEST_BRANCH" 2>/dev/null || true
                fi
            fi
        fi
    fi
    
    log_success "Cleanup completed"
}

# Main execution
main() {
    echo "Starting quality gates testing..."
    echo ""
    
    # Setup
    check_prerequisites
    backup_current_state
    create_test_branch
    
    # Run tests
    local test_results=()
    
    echo ""
    log_info "Running quality gate enforcement tests..."
    echo ""
    
    if test_compilation_failure; then
        test_results+=("✅ Compilation failure test passed")
    else
        test_results+=("❌ Compilation failure test failed")
    fi
    
    if test_test_failure; then
        test_results+=("✅ Test failure test passed") 
    else
        test_results+=("❌ Test failure test failed")
    fi
    
    if test_formatting_violations; then
        test_results+=("✅ Formatting violation test passed")
    else
        test_results+=("❌ Formatting violation test failed")
    fi
    
    if test_clippy_violations; then
        test_results+=("✅ Clippy violation test passed")
    else
        test_results+=("❌ Clippy violation test failed")
    fi
    
    if test_security_audit_failure; then
        test_results+=("✅ Security audit test passed")
    else
        test_results+=("❌ Security audit test failed")
    fi
    
    # Commit and push for CI testing
    if commit_and_push_failures; then
        test_results+=("✅ Test branch pushed successfully")
        monitor_quality_gates
    else
        test_results+=("❌ Failed to push test branch")
    fi
    
    # Results summary
    echo ""
    log_info "Quality Gate Testing Results:"
    echo "=============================="
    for result in "${test_results[@]}"; do
        echo -e "$result"
    done
    
    echo ""
    log_info "Expected Behavior:"
    echo "- All local quality checks should FAIL (this confirms they work)"
    echo "- GitHub Actions should show quality gate enforcement BLOCKING the PR"
    echo "- The test PR should NOT be mergeable"
    echo ""
    
    # Cleanup prompt
    read -p "Clean up test files and branches now? (Y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        cleanup
    else
        log_warning "Test files and branch left for manual inspection"
        log_info "Test branch: $TEST_BRANCH"
        log_info "Remember to clean up manually when done"
    fi
    
    log_success "Quality gates testing completed!"
}

# Trap cleanup on exit
trap cleanup EXIT

# Execute main function
main "$@"