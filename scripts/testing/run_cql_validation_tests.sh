#!/bin/bash

# CQL Schema Validation Test Runner Script
# 
# This script runs comprehensive CQL parser validation tests including:
# - Basic CQL parsing validation
# - Integration tests with real schemas
# - Performance benchmarks
# - Error handling validation
# - Type conversion testing

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
OUTPUT_DIR="target/cql_validation_reports"
VERBOSE=false
RUN_BENCHMARKS=true
RUN_INTEGRATION=true
RUN_VALIDATION=true
GENERATE_HTML=false
TIMEOUT=300

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --validation-only)
            RUN_VALIDATION=true
            RUN_INTEGRATION=false
            RUN_BENCHMARKS=false
            shift
            ;;
        --integration-only)
            RUN_VALIDATION=false
            RUN_INTEGRATION=true
            RUN_BENCHMARKS=false
            shift
            ;;
        --benchmarks-only)
            RUN_VALIDATION=false
            RUN_INTEGRATION=false
            RUN_BENCHMARKS=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --html)
            GENERATE_HTML=true
            shift
            ;;
        --output|-o)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --timeout|-t)
            TIMEOUT="$2"
            shift 2
            ;;
        --help|-h)
            echo "CQL Schema Validation Test Runner"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --validation-only     Run only validation suite tests"
            echo "  --integration-only    Run only integration tests"
            echo "  --benchmarks-only     Run only performance benchmarks"
            echo "  --verbose, -v         Enable verbose output"
            echo "  --html                Generate HTML reports"
            echo "  --output, -o DIR      Output directory for reports (default: target/cql_validation_reports)"
            echo "  --timeout, -t SEC     Test timeout in seconds (default: 300)"
            echo "  --help, -h            Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                    # Run all tests"
            echo "  $0 --validation-only  # Run only validation tests"
            echo "  $0 --verbose --html   # Run all tests with verbose output and HTML reports"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Print configuration
echo -e "${BLUE}🧪 CQL Schema Validation Test Runner${NC}"
echo "======================================"
echo "Configuration:"
echo "  Validation Tests: $([ "$RUN_VALIDATION" = true ] && echo -e "${GREEN}enabled${NC}" || echo -e "${YELLOW}disabled${NC}")"
echo "  Integration Tests: $([ "$RUN_INTEGRATION" = true ] && echo -e "${GREEN}enabled${NC}" || echo -e "${YELLOW}disabled${NC}")"
echo "  Performance Benchmarks: $([ "$RUN_BENCHMARKS" = true ] && echo -e "${GREEN}enabled${NC}" || echo -e "${YELLOW}disabled${NC}")"
echo "  Verbose Output: $([ "$VERBOSE" = true ] && echo -e "${GREEN}enabled${NC}" || echo -e "${YELLOW}disabled${NC}")"
echo "  HTML Reports: $([ "$GENERATE_HTML" = true ] && echo -e "${GREEN}enabled${NC}" || echo -e "${YELLOW}disabled${NC}")"
echo "  Output Directory: $OUTPUT_DIR"
echo "  Timeout: ${TIMEOUT}s"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Build the project first
echo -e "${BLUE}🔨 Building project...${NC}"
if ! cargo build --release; then
    echo -e "${RED}❌ Build failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Build completed${NC}"
echo ""

# Function to run test with timeout
run_with_timeout() {
    local cmd="$1"
    local description="$2"
    
    echo -e "${BLUE}$description${NC}"
    echo "Command: $cmd"
    echo ""
    
    if timeout "${TIMEOUT}s" bash -c "$cmd"; then
        echo -e "${GREEN}✅ $description completed successfully${NC}"
        return 0
    else
        local exit_code=$?
        if [ $exit_code -eq 124 ]; then
            echo -e "${RED}❌ $description timed out after ${TIMEOUT}s${NC}"
        else
            echo -e "${RED}❌ $description failed with exit code $exit_code${NC}"
        fi
        return $exit_code
    fi
}

# Build command line arguments for the test runner
build_test_args() {
    local args=""
    
    if [ "$RUN_VALIDATION" = true ] && [ "$RUN_INTEGRATION" = false ] && [ "$RUN_BENCHMARKS" = false ]; then
        args="$args --validation"
    elif [ "$RUN_INTEGRATION" = true ] && [ "$RUN_VALIDATION" = false ] && [ "$RUN_BENCHMARKS" = false ]; then
        args="$args --integration"
    elif [ "$RUN_BENCHMARKS" = true ] && [ "$RUN_VALIDATION" = false ] && [ "$RUN_INTEGRATION" = false ]; then
        args="$args --benchmarks"
    fi
    
    if [ "$VERBOSE" = true ]; then
        args="$args --verbose"
    fi
    
    if [ "$GENERATE_HTML" = true ]; then
        args="$args --html"
    fi
    
    args="$args --output $OUTPUT_DIR"
    args="$args --timeout $TIMEOUT"
    
    echo "$args"
}

# Track overall success
OVERALL_SUCCESS=true

# Run the comprehensive test runner
TEST_ARGS=$(build_test_args)
if ! run_with_timeout "cargo run --release --bin cql_validation_test_runner -- $TEST_ARGS" "🚀 Running CQL Validation Test Suite"; then
    OVERALL_SUCCESS=false
fi

echo ""
echo "======================================"

# Show results summary
if [ "$OVERALL_SUCCESS" = true ]; then
    echo -e "${GREEN}🎉 All CQL validation tests completed successfully!${NC}"
    echo ""
    echo "📊 Reports generated in: $OUTPUT_DIR"
    
    if [ -f "$OUTPUT_DIR/consolidated_report.json" ]; then
        echo "  📄 Consolidated report: $OUTPUT_DIR/consolidated_report.json"
    fi
    
    if [ "$RUN_VALIDATION" = true ] && [ -f "$OUTPUT_DIR/validation_report.json" ]; then
        echo "  📄 Validation report: $OUTPUT_DIR/validation_report.json"
    fi
    
    if [ "$RUN_INTEGRATION" = true ] && [ -f "$OUTPUT_DIR/integration_report.json" ]; then
        echo "  📄 Integration report: $OUTPUT_DIR/integration_report.json"
    fi
    
    if [ "$RUN_BENCHMARKS" = true ] && [ -f "$OUTPUT_DIR/benchmark_report.json" ]; then
        echo "  📄 Benchmark report: $OUTPUT_DIR/benchmark_report.json"
    fi
    
    if [ "$GENERATE_HTML" = true ] && [ -f "$OUTPUT_DIR/test_report.html" ]; then
        echo "  🌐 HTML report: $OUTPUT_DIR/test_report.html"
        echo ""
        echo "To view the HTML report:"
        echo "  open $OUTPUT_DIR/test_report.html"
    fi
    
    echo ""
    echo -e "${GREEN}✅ CQL schema validation is working correctly!${NC}"
    exit 0
else
    echo -e "${RED}❌ Some CQL validation tests failed${NC}"
    echo ""
    echo "📊 Check reports in: $OUTPUT_DIR"
    echo ""
    echo -e "${RED}⚠️  CQL schema validation needs attention${NC}"
    exit 1
fi