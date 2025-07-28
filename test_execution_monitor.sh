#!/bin/bash

# Test Execution Monitoring Script for Issue #9
# This script provides real-time monitoring of test compilation and execution status

set -e

echo "🔍 CQLite Test Execution Monitor - Issue #9"
echo "============================================="
echo "📅 $(date)"
echo ""

# Function to count compilation errors by type
count_errors() {
    local error_type="$1"
    cargo test --workspace --no-run 2>&1 | grep -c "error\[${error_type}\]" || echo "0"
}

# Function to get total compilation error count
get_total_errors() {
    cargo test --workspace --no-run 2>&1 | grep -c "^error\[" || echo "0"
}

# Function to get warning count
get_warning_count() {
    cargo test --workspace --no-run 2>&1 | grep -c "^warning:" || echo "0"
}

# Function to check if compilation passes
check_compilation() {
    if cargo test --workspace --no-run > /dev/null 2>&1; then
        echo "✅"
        return 0
    else
        echo "❌"
        return 1
    fi
}

# Function to run test and measure execution time
run_baseline_tests() {
    echo "🧪 Running Baseline Tests..."
    
    local start_time=$(date +%s%N)
    
    if timeout 300 cargo test baseline_smoke_tests --lib -- --nocapture 2>/dev/null; then
        local end_time=$(date +%s%N)
        local duration=$(( (end_time - start_time) / 1000000 )) # Convert to milliseconds
        
        echo "✅ Baseline tests PASSED in ${duration}ms"
        return 0
    else
        local end_time=$(date +%s%N)
        local duration=$(( (end_time - start_time) / 1000000 ))
        
        echo "❌ Baseline tests FAILED or TIMEOUT after ${duration}ms"
        return 1
    fi
}

# Function to analyze test files
analyze_test_files() {
    echo "📊 Test File Analysis:"
    echo "  📁 Total test files: $(find tests/src -name "*.rs" | wc -l)"
    echo "  📏 Total lines of test code: $(find tests/src -name "*.rs" -exec wc -l {} + | tail -1 | awk '{print $1}')"
    echo "  🔍 Files with Value type usage: $(grep -r "Value::" tests/src --include="*.rs" | wc -l)"
    echo "  ⚠️  Files with CqliteError usage: $(grep -r "CqliteError" tests/src --include="*.rs" | wc -l || echo "0")"
    echo "  🆔 Files with CqlTypeId usage: $(grep -r "CqlTypeId::" tests/src --include="*.rs" | wc -l)"
}

# Function to show current status
show_status() {
    echo "📋 Current Status:"
    echo "  🏗️  Compilation: $(check_compilation)"
    
    local total_errors=$(get_total_errors)
    echo "  ❌ Total errors: $total_errors"
    
    if [ "$total_errors" -gt 0 ]; then
        echo "     - E0433 (undeclared type): $(count_errors E0433)"
        echo "     - E0308 (type mismatch): $(count_errors E0308)"
        echo "     - E0277 (trait not implemented): $(count_errors E0277)"
        echo "     - E0061 (wrong parameter count): $(count_errors E0061)"
        echo "     - Other errors: $((total_errors - $(count_errors E0433) - $(count_errors E0308) - $(count_errors E0277) - $(count_errors E0061)))"
    fi
    
    local warnings=$(get_warning_count)
    echo "  ⚠️  Warnings: $warnings"
}

# Function to show improvement suggestions
show_improvements() {
    echo ""
    echo "🔧 Improvement Suggestions:"
    
    local e0433_count=$(count_errors E0433)
    if [ "$e0433_count" -gt 0 ]; then
        echo "  1. Fix import issues ($e0433_count E0433 errors):"
        echo "     - Replace 'CqliteError' with 'Error'"
        echo "     - Add proper 'use cqlite_core::Value' statements"
        echo "     - Import 'CqlTypeId' from 'cqlite_core::parser::types'"
    fi
    
    local e0308_count=$(count_errors E0308)
    if [ "$e0308_count" -gt 0 ]; then
        echo "  2. Fix type mismatches ($e0308_count E0308 errors):"
        echo "     - Check function parameter types"
        echo "     - Verify return type annotations"
    fi
    
    if [ "$(get_total_errors)" -eq 0 ]; then
        echo "  🎉 All compilation issues resolved!"
        echo "  🎯 Ready to run full test suite"
    fi
}

# Main monitoring loop
main() {
    echo "🚀 Starting Test Execution Monitoring..."
    echo ""
    
    # Analyze test files
    analyze_test_files
    echo ""
    
    # Show current status
    show_status
    echo ""
    
    # Try to run baseline tests
    if [ "$(get_total_errors)" -eq 0 ]; then
        run_baseline_tests
    else
        echo "⏸️  Skipping test execution due to compilation errors"
    fi
    
    echo ""
    show_improvements
    
    echo ""
    echo "📈 Progress Tracking:"
    echo "  🎯 Goal: >80% test pass rate"
    echo "  ⏱️  Goal: <5 minutes total execution time"
    echo "  🏆 Current priority: Fix compilation errors to establish baseline"
    
    echo ""
    echo "💡 To fix compilation issues quickly:"
    echo "   1. Run: find tests/src -name '*.rs' -exec sed -i '' 's/CqliteError/Error/g' {} \\;"
    echo "   2. Add missing imports to files with E0433 errors"
    echo "   3. Re-run this monitor to check progress"
    
    echo ""
    echo "📊 Summary Report:"
    echo "  Date: $(date)"
    echo "  Compilation Status: $(check_compilation)"
    echo "  Total Errors: $(get_total_errors)"
    echo "  Total Warnings: $(get_warning_count)" 
    echo "  Test Files: $(find tests/src -name "*.rs" | wc -l)"
    echo "  Issue #9 Status: $([ "$(get_total_errors)" -eq 0 ] && echo "BASELINE READY" || echo "COMPILATION FIXES NEEDED")"
}

# Run the monitor
main