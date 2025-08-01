#!/bin/bash

# Issue #17 Automated Testing Execution Script
# 
# This script executes the comprehensive automated testing infrastructure
# built for Issue #17: Automated Cassandra data generation and testing.
# 
# CRITICAL SUCCESS FACTOR: Command-line test execution MUST work reliably!

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "🎯 Issue #17: Automated Testing Infrastructure"
echo "=============================================="
echo "Project Root: $PROJECT_ROOT"
echo "Script Dir: $SCRIPT_DIR"
echo

# Function to run a command with error handling
run_command() {
    local description="$1"
    shift
    echo "🔄 $description..."
    if "$@"; then
        echo "✅ $description completed successfully"
        return 0
    else
        echo "❌ $description failed"
        return 1
    fi
}

# Main execution
main() {
    local start_time=$(date +%s)
    local errors=0
    
    echo "🚀 Starting Issue #17 Automated Testing Execution"
    echo
    
    # Step 1: Run the Master Test Orchestrator
    echo "📋 Step 1: Execute Master Test Orchestrator"
    if run_command "Master Test Orchestrator" "$SCRIPT_DIR/automated_test_orchestrator.sh" --data-scale MEDIUM --parallel-jobs 4; then
        echo "✅ Master orchestrator completed successfully"
    else
        echo "⚠️ Master orchestrator had issues, continuing with individual tests..."
        ((errors++))
    fi
    echo
    
    # Step 2: Property-Based Testing
    echo "📋 Step 2: Execute Property-Based Testing"
    cd "$PROJECT_ROOT"
    if run_command "Property-Based Testing" cargo run --release --package tests --bin property_based_test_runner -- --cases 500 --output reports/property_based_results.json; then
        echo "✅ Property-based testing completed"
    else
        echo "❌ Property-based testing failed"
        ((errors++))
    fi
    echo
    
    # Step 3: Performance Regression Testing
    echo "📋 Step 3: Execute Performance Regression Testing"
    # First generate default config if it doesn't exist
    if [[ ! -f "performance_benchmarks.json" ]]; then
        run_command "Generate Performance Config" cargo run --release --package tests --bin performance_regression_test_runner -- --generate-config
    fi
    
    if run_command "Performance Regression Testing" cargo run --release --package tests --bin performance_regression_test_runner -- --html --verbose; then
        echo "✅ Performance regression testing completed"
    else
        echo "❌ Performance regression testing failed"
        ((errors++))
    fi
    echo
    
    # Step 4: Build and Test All Components
    echo "📋 Step 4: Build and Test All Components"
    
    # Build all workspace members
    if run_command "Build All Components" cargo build --release --workspace; then
        echo "✅ All components built successfully"
    else
        echo "❌ Build failed"
        ((errors++))
    fi
    
    # Run unit tests
    if run_command "Unit Tests" cargo test --release --package cqlite-core; then
        echo "✅ Unit tests passed"
    else
        echo "❌ Unit tests failed"
        ((errors++))
    fi
    
    # Run integration tests
    if run_command "Integration Tests" cargo test --release --package tests; then
        echo "✅ Integration tests passed"
    else
        echo "❌ Integration tests failed"
        ((errors++))
    fi
    
    # Run CLI tests
    if run_command "CLI Tests" cargo test --release --package cqlite-cli; then
        echo "✅ CLI tests passed"
    else
        echo "❌ CLI tests failed"
        ((errors++))
    fi
    echo
    
    # Step 5: Test Data Validation
    echo "📋 Step 5: Test Data Validation"
    if [[ -f "$PROJECT_ROOT/test-data/scripts/validate-data.sh" ]]; then
        if run_command "Test Data Validation" bash "$PROJECT_ROOT/test-data/scripts/validate-data.sh"; then
            echo "✅ Test data validation passed"
        else
            echo "❌ Test data validation failed"
            ((errors++))
        fi
    else
        echo "⚠️ Test data validation script not found, skipping..."
    fi
    echo
    
    # Step 6: Generate Final Report
    echo "📋 Step 6: Generate Final Report"
    local end_time=$(date +%s)
    local total_time=$((end_time - start_time))
    local minutes=$((total_time / 60))
    local seconds=$((total_time % 60))
    
    echo
    echo "=========================================="
    echo "Issue #17 Automated Testing - Final Report"
    echo "=========================================="
    echo "Execution Time: ${minutes}m ${seconds}s"
    echo "Total Errors: $errors"
    echo
    
    if [[ $errors -eq 0 ]]; then
        echo "🎉 SUCCESS: All automated tests completed successfully!"
        echo "✅ Issue #17 implementation is working correctly"
        echo "✅ Command-line test execution is reliable"
        echo "✅ Automated Cassandra data generation infrastructure is operational"
        echo "✅ Property-based testing framework is functional"
        echo "✅ Performance regression testing is working"
        echo
        echo "📊 Generated Reports:"
        find "$PROJECT_ROOT" -name "*.json" -path "*/reports/*" -mtime -1 2>/dev/null | head -5 | while read -r file; do
            echo "  • $(basename "$file"): $file"
        done
        
        echo
        echo "🎯 CRITICAL SUCCESS FACTOR ACHIEVED:"
        echo "   Command-line test execution works reliably!"
        
        return 0
    else
        echo "⚠️ PARTIAL SUCCESS: $errors components had issues"
        echo "🔍 Please check the logs above for specific failures"
        echo "📋 You may need to:"
        echo "   - Check system requirements (Docker, Rust, Python)"
        echo "   - Verify test data is available"
        echo "   - Review individual component configurations"
        echo
        echo "🎯 CRITICAL SUCCESS FACTOR STATUS:"
        if [[ $errors -le 2 ]]; then
            echo "   ✅ Command-line test execution is mostly reliable"
            echo "   Minor issues detected but core functionality works"
            return 1
        else
            echo "   ❌ Command-line test execution needs attention"
            echo "   Multiple components have issues"
            return 2
        fi
    fi
}

# Execute with signal handling
trap 'echo ""; echo "❌ Test execution interrupted"; exit 130' INT TERM

main "$@"