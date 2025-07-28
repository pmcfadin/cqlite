#!/bin/bash

# Issue #9: Test Execution Baseline Monitoring Script
# QA Lead: Automated test execution and coverage measurement

set -e

echo "🎯 Issue #9: Test Execution Baseline Monitor"
echo "============================================="

# Check if Issue #8 is resolved (compilation clean)
echo "🔍 Checking compilation status (Issue #8 dependency)..."
if ! cargo check --tests >/dev/null 2>&1; then
    echo "❌ BLOCKED: Issue #8 compilation errors still exist"
    echo "   Cannot proceed with test execution until compilation is clean"
    exit 1
fi
echo "✅ Compilation clean - Issue #8 resolved!"

# Phase 1: Smoke Tests (30 seconds)
echo ""
echo "🚀 Phase 1: Critical Smoke Tests"
echo "================================="
start_time=$(date +%s)

echo "Running minimal smoke tests..."
if cargo test minimal_smoke_tests --lib --quiet; then
    echo "✅ Minimal smoke tests: PASSED"
    smoke_tests_passed=1
else
    echo "❌ Minimal smoke tests: FAILED"
    smoke_tests_passed=0
fi

echo "Running crate loading tests..."
if cargo test integration_test_crate_loads --lib --quiet; then
    echo "✅ Crate loading tests: PASSED" 
    crate_tests_passed=1
else
    echo "❌ Crate loading tests: FAILED"
    crate_tests_passed=0
fi

smoke_time=$(($(date +%s) - start_time))
echo "Phase 1 completed in ${smoke_time}s"

# Phase 2: Integration Tests (2 minutes)
echo ""
echo "🔧 Phase 2: Integration Tests" 
echo "=============================="
integration_start=$(date +%s)

echo "Running SSTable format tests..."
if timeout 60 cargo test sstable_format_tests --quiet; then
    echo "✅ SSTable format tests: PASSED"
    sstable_tests_passed=1
else
    echo "❌ SSTable format tests: FAILED/TIMEOUT"
    sstable_tests_passed=0
fi

echo "Running parser validation tests..."
if timeout 60 cargo test parser_validation --quiet; then
    echo "✅ Parser validation tests: PASSED"
    parser_tests_passed=1  
else
    echo "❌ Parser validation tests: FAILED/TIMEOUT"
    parser_tests_passed=0
fi

integration_time=$(($(date +%s) - integration_start))
echo "Phase 2 completed in ${integration_time}s"

# Phase 3: Performance Tests (2 minutes)
echo ""
echo "⚡ Phase 3: Performance Tests"
echo "============================="
perf_start=$(date +%s)

echo "Running performance benchmarks..."
if timeout 120 cargo test performance_benchmarks --release --quiet; then
    echo "✅ Performance benchmarks: PASSED"
    perf_tests_passed=1
else
    echo "❌ Performance benchmarks: FAILED/TIMEOUT"
    perf_tests_passed=0
fi

perf_time=$(($(date +%s) - perf_start))
echo "Phase 3 completed in ${perf_time}s"

# Phase 4: Edge Cases (1 minute)
echo ""
echo "🎲 Phase 4: Edge Case Tests"
echo "==========================="
edge_start=$(date +%s)

echo "Running edge case tests..."
if timeout 60 cargo test edge_case --quiet; then
    echo "✅ Edge case tests: PASSED"
    edge_tests_passed=1
else
    echo "❌ Edge case tests: FAILED/TIMEOUT"
    edge_tests_passed=0
fi

edge_time=$(($(date +%s) - edge_start))
echo "Phase 4 completed in ${edge_time}s"

# Calculate Results
total_time=$((smoke_time + integration_time + perf_time + edge_time))
total_phases=4
passed_phases=$((smoke_tests_passed + sstable_tests_passed + parser_tests_passed + perf_tests_passed + edge_tests_passed))
execution_rate=$((passed_phases * 100 / 5))  # 5 main test categories

echo ""
echo "📊 BASELINE EXECUTION RESULTS"
echo "============================="
echo "Total execution time: ${total_time}s (target: <300s)"
echo "Test execution rate: ${execution_rate}% (target: >80%)"
echo ""
echo "Phase Results:"
echo "  ✅ Smoke tests: $([[ $smoke_tests_passed -eq 1 ]] && echo "PASSED" || echo "FAILED")"
echo "  ✅ SSTable tests: $([[ $sstable_tests_passed -eq 1 ]] && echo "PASSED" || echo "FAILED")"  
echo "  ✅ Parser tests: $([[ $parser_tests_passed -eq 1 ]] && echo "PASSED" || echo "FAILED")"
echo "  ✅ Performance tests: $([[ $perf_tests_passed -eq 1 ]] && echo "PASSED" || echo "FAILED")"
echo "  ✅ Edge case tests: $([[ $edge_tests_passed -eq 1 ]] && echo "PASSED" || echo "FAILED")"

# Quality Gate Validation
echo ""
echo "🎯 QUALITY GATE VALIDATION"
echo "=========================="

if [[ $execution_rate -ge 80 ]]; then
    echo "✅ Execution rate: ${execution_rate}% (>80% required) - PASSED"
    execution_gate=1
else
    echo "❌ Execution rate: ${execution_rate}% (>80% required) - FAILED"
    execution_gate=0
fi

if [[ $total_time -le 300 ]]; then
    echo "✅ Performance: ${total_time}s (<300s required) - PASSED"
    performance_gate=1
else
    echo "❌ Performance: ${total_time}s (<300s required) - FAILED"
    performance_gate=0
fi

# Final Status
if [[ $execution_gate -eq 1 && $performance_gate -eq 1 ]]; then
    echo ""
    echo "🎉 SUCCESS: Issue #9 Quality Gates PASSED"
    echo "   - Test execution rate: ${execution_rate}% (>80%)"
    echo "   - Performance: ${total_time}s (<5min)"
    echo "   - Baseline established successfully"
    exit 0
else
    echo ""
    echo "❌ FAILURE: Issue #9 Quality Gates FAILED"
    echo "   - Additional investigation required"
    echo "   - Test reliability issues identified"
    exit 1
fi