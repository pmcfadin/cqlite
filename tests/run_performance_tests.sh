#!/bin/bash

# CQLite Phase 2 Performance Testing Script
# Runs comprehensive performance benchmarks and generates reports

set -e

echo "🚀 CQLite Phase 2 Performance Testing Suite"
echo "==========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Create results directory
RESULTS_DIR="performance_results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

echo -e "${BLUE}Results will be saved to: $RESULTS_DIR${NC}"

# Performance targets
WRITE_TARGET=50000
READ_TARGET=100000
MEMORY_TARGET=128

echo -e "\n${YELLOW}Performance Targets:${NC}"
echo "- Write operations: ${WRITE_TARGET} ops/sec"
echo "- Read operations: ${READ_TARGET} ops/sec"
echo "- Memory usage: <${MEMORY_TARGET}MB"

# Function to run benchmark and capture output
run_benchmark() {
    local bench_name=$1
    local description=$2
    
    echo -e "\n${BLUE}Running $description...${NC}"
    
    # Run benchmark and capture output
    if cargo bench --bench "$bench_name" > "$RESULTS_DIR/${bench_name}_output.txt" 2>&1; then
        echo -e "${GREEN}✅ $description completed successfully${NC}"
        
        # Extract key metrics (this would need to be customized based on actual output)
        if grep -q "ops/sec" "$RESULTS_DIR/${bench_name}_output.txt"; then
            echo -e "${GREEN}📊 Performance metrics found${NC}"
        fi
    else
        echo -e "${RED}❌ $description failed${NC}"
        echo "Error output:"
        tail -n 10 "$RESULTS_DIR/${bench_name}_output.txt"
    fi
}

# Function to run integration tests
run_integration_tests() {
    echo -e "\n${BLUE}Running Integration Tests...${NC}"
    
    if cargo test --test performance_integration > "$RESULTS_DIR/integration_output.txt" 2>&1; then
        echo -e "${GREEN}✅ Integration tests completed successfully${NC}"
        
        # Check for performance assertions
        if grep -q "assertion failed" "$RESULTS_DIR/integration_output.txt"; then
            echo -e "${RED}⚠️  Some performance targets not met${NC}"
        else
            echo -e "${GREEN}🎯 All performance targets met${NC}"
        fi
    else
        echo -e "${RED}❌ Integration tests failed${NC}"
        echo "Error output:"
        tail -n 10 "$RESULTS_DIR/integration_output.txt"
    fi
}

# Function to generate performance report
generate_report() {
    echo -e "\n${BLUE}Generating Performance Report...${NC}"
    
    REPORT_FILE="$RESULTS_DIR/performance_report.md"
    
    cat > "$REPORT_FILE" << EOF
# CQLite Phase 2 Performance Report

**Generated:** $(date)
**Test Environment:** $(uname -a)
**Rust Version:** $(rustc --version)

## Performance Targets

| Metric | Target | Status |
|--------|--------|--------|
| Write Operations | ${WRITE_TARGET} ops/sec | TBD |
| Read Operations | ${READ_TARGET} ops/sec | TBD |
| Memory Usage | <${MEMORY_TARGET}MB | TBD |

## Benchmark Results

### Performance Suite
EOF

    # Parse benchmark results (simplified - would need actual parsing logic)
    if [ -f "$RESULTS_DIR/performance_suite_output.txt" ]; then
        echo "See \`performance_suite_output.txt\` for detailed results." >> "$REPORT_FILE"
    fi
    
    cat >> "$REPORT_FILE" << EOF

### Load Testing
EOF

    if [ -f "$RESULTS_DIR/load_testing_output.txt" ]; then
        echo "See \`load_testing_output.txt\` for detailed results." >> "$REPORT_FILE"
    fi
    
    cat >> "$REPORT_FILE" << EOF

### Compatibility Testing
EOF

    if [ -f "$RESULTS_DIR/compatibility_testing_output.txt" ]; then
        echo "See \`compatibility_testing_output.txt\` for detailed results." >> "$REPORT_FILE"
    fi
    
    cat >> "$REPORT_FILE" << EOF

## Integration Test Results

EOF

    if [ -f "$RESULTS_DIR/integration_output.txt" ]; then
        if grep -q "test result: ok" "$RESULTS_DIR/integration_output.txt"; then
            echo "✅ All integration tests passed" >> "$REPORT_FILE"
        else
            echo "❌ Some integration tests failed" >> "$REPORT_FILE"
        fi
    fi
    
    echo -e "${GREEN}📄 Performance report generated: $REPORT_FILE${NC}"
}

# Function to check system requirements
check_requirements() {
    echo -e "\n${BLUE}Checking System Requirements...${NC}"
    
    # Check available memory
    if command -v free > /dev/null; then
        AVAILABLE_MEM=$(free -m | awk 'NR==2{printf "%s", $7}')
        if [ "$AVAILABLE_MEM" -lt 1024 ]; then
            echo -e "${YELLOW}⚠️  Low available memory: ${AVAILABLE_MEM}MB${NC}"
        else
            echo -e "${GREEN}✅ Available memory: ${AVAILABLE_MEM}MB${NC}"
        fi
    fi
    
    # Check CPU cores
    if command -v nproc > /dev/null; then
        CPU_CORES=$(nproc)
        echo -e "${GREEN}✅ CPU cores: ${CPU_CORES}${NC}"
    fi
    
    # Check disk space
    DISK_SPACE=$(df -h . | awk 'NR==2{print $4}')
    echo -e "${GREEN}✅ Available disk space: ${DISK_SPACE}${NC}"
}

# Main execution
main() {
    echo -e "\n${BLUE}Starting Performance Test Suite...${NC}"
    
    # Check requirements
    check_requirements
    
    # Build in release mode for accurate performance testing
    echo -e "\n${BLUE}Building in release mode...${NC}"
    if cargo build --release > "$RESULTS_DIR/build_output.txt" 2>&1; then
        echo -e "${GREEN}✅ Build completed successfully${NC}"
    else
        echo -e "${RED}❌ Build failed${NC}"
        cat "$RESULTS_DIR/build_output.txt"
        exit 1
    fi
    
    # Run benchmarks
    run_benchmark "performance_suite" "Performance Suite (Write/Read/Memory/SSTable)"
    run_benchmark "load_testing" "Load Testing (Concurrent/Sustained/Stress)"
    run_benchmark "compatibility_testing" "Compatibility Testing (Cassandra Format)"
    
    # Run integration tests
    run_integration_tests
    
    # Generate report
    generate_report
    
    echo -e "\n${GREEN}🎉 Performance testing completed!${NC}"
    echo -e "${BLUE}Results saved to: $RESULTS_DIR${NC}"
    
    # Show summary
    echo -e "\n${YELLOW}Summary:${NC}"
    echo "- Performance Suite: $([ -f "$RESULTS_DIR/performance_suite_output.txt" ] && echo "✅ Completed" || echo "❌ Failed")"
    echo "- Load Testing: $([ -f "$RESULTS_DIR/load_testing_output.txt" ] && echo "✅ Completed" || echo "❌ Failed")"
    echo "- Compatibility Testing: $([ -f "$RESULTS_DIR/compatibility_testing_output.txt" ] && echo "✅ Completed" || echo "❌ Failed")"
    echo "- Integration Tests: $([ -f "$RESULTS_DIR/integration_output.txt" ] && echo "✅ Completed" || echo "❌ Failed")"
}

# Handle script arguments
case "$1" in
    --quick)
        echo "Running quick performance tests..."
        run_benchmark "performance_suite" "Performance Suite (Quick)"
        ;;
    --load-only)
        echo "Running load tests only..."
        run_benchmark "load_testing" "Load Testing"
        ;;
    --compatibility-only)
        echo "Running compatibility tests only..."
        run_benchmark "compatibility_testing" "Compatibility Testing"
        ;;
    --integration-only)
        echo "Running integration tests only..."
        run_integration_tests
        ;;
    --help)
        echo "Usage: $0 [--quick|--load-only|--compatibility-only|--integration-only|--help]"
        echo ""
        echo "Options:"
        echo "  --quick                Run quick performance tests only"
        echo "  --load-only           Run load tests only"
        echo "  --compatibility-only  Run compatibility tests only"
        echo "  --integration-only    Run integration tests only"
        echo "  --help                Show this help message"
        echo ""
        echo "Default: Run all tests and generate comprehensive report"
        ;;
    *)
        main
        ;;
esac