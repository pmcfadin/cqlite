#!/bin/bash

# Phase 1 Continuous Validation Monitor
# Monitors quality gates and agent progress throughout operation

export CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets

# Baseline metrics
BASELINE_TESTS_PASSED=615
BASELINE_TESTS_IGNORED=13
BASELINE_WARNINGS=105
BASELINE_RUNTIME=0.15

echo "=== Phase 1 Continuous Validation Monitor ==="
echo "Baseline: $BASELINE_TESTS_PASSED passed; 0 failed; $BASELINE_TESTS_IGNORED ignored"
echo "Baseline warnings: $BASELINE_WARNINGS"
echo "Baseline runtime: $BASELINE_RUNTIME seconds"

# Function to check test baseline
check_test_baseline() {
    echo "--- Testing Baseline Check ---"
    local start_time=$(date +%s.%N)
    local result=$(cargo test --package cqlite-core --lib --quiet 2>&1)
    local end_time=$(date +%s.%N)
    local runtime=$(echo "$end_time - $start_time" | bc)

    echo "$result"

    # Extract metrics
    local passed=$(echo "$result" | grep -o '[0-9]\+ passed' | cut -d' ' -f1)
    local failed=$(echo "$result" | grep -o '[0-9]\+ failed' | cut -d' ' -f1)
    local ignored=$(echo "$result" | grep -o '[0-9]\+ ignored' | cut -d' ' -f1)

    # Default to 0 if not found
    failed=${failed:-0}

    echo "Runtime: ${runtime}s"

    # Quality gate checks
    if [ "$failed" -gt 0 ]; then
        echo "🚨 QUALITY GATE FAILURE: $failed test failures detected!"
        return 1
    fi

    if [ "$passed" -lt "$BASELINE_TESTS_PASSED" ]; then
        echo "🚨 QUALITY GATE FAILURE: Tests passed ($passed) below baseline ($BASELINE_TESTS_PASSED)!"
        return 1
    fi

    if (( $(echo "$runtime > 15" | bc -l) )); then
        echo "🚨 QUALITY GATE FAILURE: Runtime ${runtime}s exceeds 15s threshold!"
        return 1
    fi

    echo "✅ Test baseline check passed"
    return 0
}

# Function to check clippy warnings
check_clippy_baseline() {
    echo "--- Clippy Baseline Check ---"
    local warnings=$(cargo clippy --package cqlite-core --all-targets --message-format=short 2>&1 | grep -E "(warning|error)" | wc -l)

    echo "Current warnings: $warnings"

    if [ "$warnings" -gt "$BASELINE_WARNINGS" ]; then
        echo "🚨 QUALITY GATE FAILURE: Warnings ($warnings) exceed baseline ($BASELINE_WARNINGS)!"
        return 1
    fi

    echo "✅ Clippy baseline check passed"
    return 0
}

# Function to check git status for changes
check_git_changes() {
    echo "--- Git Changes Check ---"
    local changes=$(git status --porcelain | wc -l)
    echo "Modified files: $changes"

    if [ "$changes" -gt 0 ]; then
        echo "📝 Changes detected - monitoring agent progress"
        git status --porcelain
    else
        echo "📋 No new changes"
    fi
}

# Main monitoring loop
monitor_continuous() {
    echo "Starting continuous monitoring..."

    while true; do
        echo ""
        echo "=== Validation Check $(date) ==="

        check_git_changes

        if ! check_test_baseline; then
            echo "💥 CRITICAL FAILURE - STOPPING ALL OPERATIONS"
            exit 1
        fi

        if ! check_clippy_baseline; then
            echo "⚠️  WARNING - New clippy issues detected"
        fi

        echo "✅ All quality gates passed"

        # Check every 30 seconds
        sleep 30
    done
}

# Run based on argument
case "${1:-monitor}" in
    "test")
        check_test_baseline
        ;;
    "clippy")
        check_clippy_baseline
        ;;
    "git")
        check_git_changes
        ;;
    "monitor")
        monitor_continuous
        ;;
    *)
        echo "Usage: $0 [test|clippy|git|monitor]"
        exit 1
        ;;
esac