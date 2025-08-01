#!/bin/bash
# Issue #17 Test Execution Script
# 
# This script runs the comprehensive SSTable reading validation tests
# for Issue #17 with proper environment setup and result reporting.

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEST_DATA_PATH="$PROJECT_ROOT/test-env/cassandra5/sstables"
LOG_FILE="$SCRIPT_DIR/issue_17_test_results.log"
TIMESTAMP=$(date '+%Y-%m-%d_%H-%M-%S')

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo_color() {
    echo -e "${1}${2}${NC}"
}

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Check prerequisites
check_prerequisites() {
    echo_color $BLUE "🔍 Checking Prerequisites..."
    
    # Check if Rust is available
    if ! command -v cargo &> /dev/null; then
        echo_color $RED "❌ Cargo/Rust not found. Please install Rust."
        exit 1
    fi
    
    # Check if test data exists
    if [ ! -d "$TEST_DATA_PATH" ]; then
        echo_color $YELLOW "⚠️  Test data not found at $TEST_DATA_PATH"
        echo_color $YELLOW "   Consider running: cd test-env/cassandra5 && ./manage.sh all"
        echo_color $BLUE "   Continuing with synthetic tests only..."
    else
        echo_color $GREEN "✅ Test data found at $TEST_DATA_PATH"
        
        # Count available test files
        SSTABLE_COUNT=$(find "$TEST_DATA_PATH" -name "*.db" | wc -l)
        echo_color $BLUE "   Found $SSTABLE_COUNT SSTable files for testing"
    fi
    
    # Check if binary can be built
    echo_color $BLUE "🔧 Building test runner..."
    cd "$PROJECT_ROOT"
    if ! cargo build --bin issue_17_test_runner --release; then
        echo_color $RED "❌ Failed to build test runner binary"
        exit 1
    fi
    
    echo_color $GREEN "✅ Prerequisites check completed"
}

# Run the comprehensive test suite
run_comprehensive_tests() {
    echo_color $BLUE "\n🚀 Starting Issue #17 Comprehensive Validation"
    echo_color $BLUE "=============================================="
    
    log "Starting Issue #17 comprehensive SSTable reading validation"
    
    # Set environment variables
    export RUST_BACKTRACE=1
    export RUST_LOG=info
    
    # Run the test binary with timeout
    local exit_code=0
    local timeout_duration=600  # 10 minutes
    
    echo_color $BLUE "⏱️  Running tests with ${timeout_duration}s timeout..."
    
    if timeout ${timeout_duration}s cargo run --bin issue_17_test_runner --release 2>&1 | tee -a "$LOG_FILE"; then
        exit_code=${PIPESTATUS[0]}
    else
        exit_code=$?
    fi
    
    # Interpret results
    case $exit_code in
        0)
            echo_color $GREEN "\n🎉 SUCCESS: All tests passed!"
            echo_color $GREEN "✅ Issue #17 requirements fully satisfied"
            echo_color $GREEN "🚀 Ready for M1 milestone completion"
            ;;
        1)
            echo_color $YELLOW "\n🟡 MOSTLY SUCCESSFUL: Minor issues found"
            echo_color $YELLOW "⚠️  Review test output and address remaining issues"
            ;;
        2)
            echo_color $YELLOW "\n🟠 PARTIAL SUCCESS: Significant issues found"
            echo_color $YELLOW "🔧 Additional work needed before M1 milestone"
            ;;
        3)
            echo_color $RED "\n🔴 FAILURE: Major compatibility issues"
            echo_color $RED "🚨 Critical work required for Issue #17"
            ;;
        4)
            echo_color $RED "\n💥 TEST EXECUTION FAILED"
            echo_color $RED "🔧 Check configuration and dependencies"
            ;;
        124)
            echo_color $RED "\n⏰ TEST TIMEOUT EXCEEDED"
            echo_color $RED "🔧 Tests took longer than ${timeout_duration} seconds"
            ;;
        *)
            echo_color $RED "\n❓ UNKNOWN ERROR (exit code: $exit_code)"
            echo_color $RED "🔧 Check logs for details"
            ;;
    esac
    
    log "Test execution completed with exit code: $exit_code"
    return $exit_code
}

# Generate test report
generate_report() {
    local exit_code=$1
    local report_file="$SCRIPT_DIR/issue_17_test_report_$TIMESTAMP.md"
    
    echo_color $BLUE "\n📊 Generating Test Report..."
    
    cat > "$report_file" << EOF
# Issue #17: SSTable Reading Validation Report

**Date:** $(date '+%Y-%m-%d %H:%M:%S')  
**Test Runner:** issue_17_test_runner  
**Exit Code:** $exit_code  
**Log File:** $LOG_FILE  

## Summary

EOF
    
    case $exit_code in
        0)
            cat >> "$report_file" << EOF
✅ **PASSED** - All acceptance criteria met for Issue #17

The comprehensive SSTable reading functionality has been validated across:
- Multiple Cassandra versions (3.x, 4.x, 5.x)
- Various compression formats (Snappy, LZ4, Deflate)
- All required data types and collections
- Error handling scenarios
- Performance requirements

**Recommendation:** Issue #17 can be closed. Ready for M1 milestone.

EOF
            ;;
        1|2)
            cat >> "$report_file" << EOF
⚠️ **PARTIAL** - Some issues found but core functionality works

The SSTable reading functionality is mostly working but has some limitations.
Review the detailed test output to identify specific areas needing attention.

**Recommendation:** Address identified issues before closing Issue #17.

EOF
            ;;
        *)
            cat >> "$report_file" << EOF
❌ **FAILED** - Significant issues prevent Issue #17 completion

Major problems were found in the SSTable reading functionality that prevent
meeting the acceptance criteria. Substantial work is needed.

**Recommendation:** Review failed tests and implementation before M1 milestone.

EOF
            ;;
    esac
    
    cat >> "$report_file" << EOF
## Test Configuration

- **Test Data Path:** $TEST_DATA_PATH
- **Available SSTable Files:** $(find "$TEST_DATA_PATH" -name "*.db" 2>/dev/null | wc -l || echo "0")
- **Test Environment:** $(uname -s) $(uname -m)
- **Rust Version:** $(rustc --version)

## Links

- **GitHub Issue:** https://github.com/pmcfadin/cqlite/issues/17
- **M1 Milestone:** https://github.com/pmcfadin/cqlite/milestone/1
- **Test Log:** $LOG_FILE

## Detailed Output

See attached log file for complete test execution details.
EOF
    
    echo_color $GREEN "📄 Report generated: $report_file"
}

# Main execution
main() {
    echo_color $BLUE "Issue #17: SSTable Reading Validation Test Runner"
    echo_color $BLUE "================================================"
    echo_color $BLUE "Timestamp: $(date)"
    echo_color $BLUE "Project: $(basename "$PROJECT_ROOT")"
    echo ""
    
    # Initialize log file
    echo "Issue #17 Test Execution - $(date)" > "$LOG_FILE"
    
    check_prerequisites
    
    local exit_code=0
    if run_comprehensive_tests; then
        exit_code=0
    else
        exit_code=$?
    fi
    
    generate_report $exit_code
    
    echo_color $BLUE "\n📋 Test execution summary:"
    echo_color $BLUE "   Log file: $LOG_FILE"
    echo_color $BLUE "   Report: issue_17_test_report_$TIMESTAMP.md"
    
    exit $exit_code
}

# Execute main function
main "$@"