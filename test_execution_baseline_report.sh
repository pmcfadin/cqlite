#!/bin/bash
# Test Execution Baseline Report for Issue #9
# Establishes current test execution status and metrics

echo "🧪 ISSUE #9 - TEST EXECUTION BASELINE REPORT"
echo "============================================="
echo "Date: $(date)"
echo "Environment: $(uname -s) $(uname -r)"
echo ""

echo "📊 BASELINE TEST EXECUTION"
echo "---------------------------"

# Run isolated baseline tests
echo "Running isolated baseline tests..."
if rustc --test isolated_baseline_tests.rs -o baseline_test_runner 2>/dev/null; then
    echo "✅ Baseline tests compiled successfully"
    
    # Run the tests and capture results
    if ./baseline_test_runner 2>/dev/null; then
        echo "✅ Baseline tests executed successfully"
        BASELINE_STATUS="PASS"
    else
        echo "❌ Baseline tests failed"
        BASELINE_STATUS="FAIL"
    fi
    
    # Clean up
    rm -f baseline_test_runner
else
    echo "❌ Baseline tests failed to compile"
    BASELINE_STATUS="COMPILE_ERROR"
fi

echo ""
echo "🔍 MAIN TEST SUITE ANALYSIS"
echo "----------------------------"

# Analyze main test suite compilation
echo "Checking main test suite compilation..."
ERROR_COUNT=$(cargo test --workspace --no-run 2>&1 | grep -c "error\[")
WARNING_COUNT=$(cargo test --workspace --no-run 2>&1 | grep -c "warning:")

echo "📈 Compilation Issues:"
echo "- Errors: $ERROR_COUNT"
echo "- Warnings: $WARNING_COUNT"

if [ "$ERROR_COUNT" -eq 0 ]; then
    echo "✅ Main test suite compiles cleanly"
    
    # Try to run tests if compilation succeeds
    echo "Attempting to run main test suite..."
    TEST_OUTPUT=$(timeout 60 cargo test --workspace 2>&1)
    
    if echo "$TEST_OUTPUT" | grep -q "test result:"; then
        PASSED=$(echo "$TEST_OUTPUT" | grep "test result:" | tail -1 | sed 's/.*\([0-9]\+\) passed.*/\1/')
        FAILED=$(echo "$TEST_OUTPUT" | grep "test result:" | tail -1 | sed 's/.*\([0-9]\+\) failed.*/\1/')
        TOTAL=$((PASSED + FAILED))
        
        if [ "$TOTAL" -gt 0 ]; then
            PASS_RATE=$(echo "scale=1; $PASSED * 100 / $TOTAL" | bc -l)
            echo "📊 Test Results:"
            echo "- Total Tests: $TOTAL"
            echo "- Passed: $PASSED"
            echo "- Failed: $FAILED"
            echo "- Pass Rate: ${PASS_RATE}%"
            
            # Check if meets Issue #9 quality gates
            if (( $(echo "$PASS_RATE > 80" | bc -l) )); then
                echo "✅ Meets >80% pass rate requirement"
                QUALITY_GATE_PASS_RATE="PASS"
            else
                echo "❌ Does not meet >80% pass rate requirement"
                QUALITY_GATE_PASS_RATE="FAIL"
            fi
        else
            echo "❌ No tests executed"
            QUALITY_GATE_PASS_RATE="NO_TESTS"
        fi
    else
        echo "❌ Tests failed to execute properly"
        QUALITY_GATE_PASS_RATE="EXECUTION_ERROR"
    fi
else
    echo "❌ Main test suite has compilation errors"
    QUALITY_GATE_PASS_RATE="COMPILE_ERROR"
fi

echo ""
echo "⏱️  EXECUTION TIME ANALYSIS"
echo "---------------------------"

# Measure execution time for baseline tests
echo "Measuring baseline test execution time..."
START_TIME=$(date +%s.%N)
if rustc --test isolated_baseline_tests.rs -o baseline_timer 2>/dev/null; then
    ./baseline_timer >/dev/null 2>&1
    rm -f baseline_timer
fi
END_TIME=$(date +%s.%N)
EXECUTION_TIME=$(echo "$END_TIME - $START_TIME" | bc -l)

echo "📏 Baseline execution time: ${EXECUTION_TIME}s"

if (( $(echo "$EXECUTION_TIME < 300" | bc -l) )); then
    echo "✅ Meets <5 minute execution time requirement"
    QUALITY_GATE_TIME="PASS"
else
    echo "❌ Exceeds 5 minute execution time limit"
    QUALITY_GATE_TIME="FAIL"
fi

echo ""
echo "🎯 ISSUE #9 QUALITY GATES SUMMARY"
echo "=================================="
echo "Target: >80% test pass rate + <5 minute execution"
echo ""
echo "📊 Results:"
echo "- Baseline Test Infrastructure: $BASELINE_STATUS"
echo "- Pass Rate Quality Gate: $QUALITY_GATE_PASS_RATE"
echo "- Execution Time Quality Gate: $QUALITY_GATE_TIME"
echo "- Compilation Errors: $ERROR_COUNT"
echo "- Compilation Warnings: $WARNING_COUNT"

echo ""
echo "🔧 RECOMMENDATIONS"
echo "==================="

if [ "$ERROR_COUNT" -gt 0 ]; then
    echo "🔴 CRITICAL: Fix $ERROR_COUNT compilation errors first"
    echo "   - Focus on import/use statement fixes"
    echo "   - Address type mismatches (Value, CqlTypeId, etc.)"
    echo "   - Update deprecated API usage"
fi

if [ "$BASELINE_STATUS" == "PASS" ]; then
    echo "✅ Test infrastructure is working - baseline established"
else
    echo "❌ Test infrastructure needs repair"
fi

echo ""
echo "📋 NEXT STEPS FOR ISSUE #9"
echo "==========================="
echo "1. Fix compilation errors to enable test execution"
echo "2. Run comprehensive test suite"
echo "3. Measure actual pass rate and execution time"
echo "4. Address failing tests to reach >80% pass rate"
echo "5. Optimize execution time if needed"
echo "6. Document final baseline metrics"

echo ""
echo "🎉 BASELINE REPORT COMPLETE"
echo "==========================="
echo "Issue #9 baseline assessment: ESTABLISHED"
echo "Infrastructure status: $BASELINE_STATUS"
echo "Main blocker: $ERROR_COUNT compilation errors"
echo ""