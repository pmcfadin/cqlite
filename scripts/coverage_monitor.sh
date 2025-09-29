#!/bin/bash
# Coverage Validator - Continuous monitoring script for cqlite Phase 2
# Target: 95% coverage for M1 milestone

set -euo pipefail

PROJECT_ROOT="/Users/patrick/local_projects/cqlite"
COVERAGE_DIR="$PROJECT_ROOT/coverage-reports"
MEMORY_FILE="$PROJECT_ROOT/.swarm/memory.db"
TIMESTAMP=$(date "+%Y%m%d_%H%M%S")

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
TARGET_COVERAGE=95.0
CURRENT_BASELINE=45.0
IGNORED_TESTS_TARGET=0
IGNORED_TESTS_CURRENT=43

echo -e "${BLUE}🎯 COVERAGE VALIDATOR - cqlite Phase 2 M1 Monitoring${NC}"
echo "=================================="
echo "Target Coverage: ${TARGET_COVERAGE}%"
echo "Current Baseline: ${CURRENT_BASELINE}%"
echo "Ignored Tests to Remove: ${IGNORED_TESTS_CURRENT} → ${IGNORED_TESTS_TARGET}"
echo ""

# Function to run coverage analysis
run_coverage_analysis() {
    echo -e "${BLUE}📊 Running comprehensive coverage analysis...${NC}"

    cd "$PROJECT_ROOT"

    # Generate coverage with detailed output
    echo "Running cargo tarpaulin..."
    cargo tarpaulin \
        --config tarpaulin.toml \
        --out Html \
        --output-dir "$COVERAGE_DIR" \
        --verbose \
        --timeout 300 \
        --exclude-files "*/tests/*" "*/target/*" "*/examples/*" \
        --workspace \
        --all-features > "$COVERAGE_DIR/coverage_${TIMESTAMP}.log" 2>&1

    # Extract coverage percentage from output
    local coverage_percent=$(grep -o "Coverage: [0-9.]*%" "$COVERAGE_DIR/coverage_${TIMESTAMP}.log" | tail -1 | grep -o "[0-9.]*" || echo "0.0")

    echo "Current Coverage: ${coverage_percent}%"

    # Check if we're meeting targets
    if (( $(echo "$coverage_percent >= $TARGET_COVERAGE" | bc -l) )); then
        echo -e "${GREEN}✅ COVERAGE TARGET MET: ${coverage_percent}% >= ${TARGET_COVERAGE}%${NC}"
        npx claude-flow@alpha hooks notify --message "🎉 M1 Coverage Target Achieved: ${coverage_percent}%"
    elif (( $(echo "$coverage_percent > $CURRENT_BASELINE" | bc -l) )); then
        echo -e "${YELLOW}⚡ PROGRESS: ${coverage_percent}% (improved from ${CURRENT_BASELINE}%)${NC}"
        npx claude-flow@alpha hooks notify --message "📈 Coverage Progress: ${coverage_percent}%"
    else
        echo -e "${RED}⚠️  COVERAGE BELOW BASELINE: ${coverage_percent}% < ${CURRENT_BASELINE}%${NC}"
        npx claude-flow@alpha hooks notify --message "🚨 Coverage Regression: ${coverage_percent}%"
    fi

    echo "$coverage_percent" > "$COVERAGE_DIR/latest_coverage.txt"
    return 0
}

# Function to analyze ignored tests
analyze_ignored_tests() {
    echo -e "${BLUE}📋 Analyzing ignored tests...${NC}"

    cd "$PROJECT_ROOT"

    # Count ignored tests
    local ignored_count=$(find . -name "*.rs" -exec grep -l "#\[ignore\]" {} \; | wc -l)
    local ignored_tests=$(grep -r "#\[ignore\]" --include="*.rs" . | wc -l)

    echo "Files with ignored tests: $ignored_count"
    echo "Total ignored test cases: $ignored_tests"

    # Generate ignored tests report
    echo "# Ignored Tests Analysis - $(date)" > "$COVERAGE_DIR/ignored_tests_${TIMESTAMP}.md"
    echo "## Summary" >> "$COVERAGE_DIR/ignored_tests_${TIMESTAMP}.md"
    echo "- Total ignored test cases: $ignored_tests" >> "$COVERAGE_DIR/ignored_tests_${TIMESTAMP}.md"
    echo "- Files affected: $ignored_count" >> "$COVERAGE_DIR/ignored_tests_${TIMESTAMP}.md"
    echo "- Target: $IGNORED_TESTS_TARGET ignored tests" >> "$COVERAGE_DIR/ignored_tests_${TIMESTAMP}.md"
    echo "" >> "$COVERAGE_DIR/ignored_tests_${TIMESTAMP}.md"
    echo "## Ignored Test Locations" >> "$COVERAGE_DIR/ignored_tests_${TIMESTAMP}.md"

    find . -name "*.rs" -exec grep -Hn "#\[ignore\]" {} \; | \
        while IFS=: read -r file line content; do
            echo "- \`$file:$line\` - $content" >> "$COVERAGE_DIR/ignored_tests_${TIMESTAMP}.md"
        done

    if [ "$ignored_tests" -le "$IGNORED_TESTS_TARGET" ]; then
        echo -e "${GREEN}✅ IGNORED TESTS TARGET MET: ${ignored_tests} <= ${IGNORED_TESTS_TARGET}${NC}"
    else
        echo -e "${YELLOW}⚡ IGNORED TESTS REMAINING: ${ignored_tests} (target: ${IGNORED_TESTS_TARGET})${NC}"
    fi

    echo "$ignored_tests" > "$COVERAGE_DIR/latest_ignored_count.txt"
}

# Function to generate coverage gap analysis
generate_coverage_gaps() {
    echo -e "${BLUE}🔍 Generating coverage gap analysis...${NC}"

    cd "$PROJECT_ROOT"

    # Run coverage with JSON output for detailed analysis
    cargo tarpaulin \
        --config tarpaulin.toml \
        --out Json \
        --output-dir "$COVERAGE_DIR" \
        --timeout 300 \
        --workspace \
        --all-features > /dev/null 2>&1 || true

    # Generate gap analysis report
    echo "# Coverage Gap Analysis - $(date)" > "$COVERAGE_DIR/coverage_gaps_${TIMESTAMP}.md"
    echo "## Critical Gaps (Modules below 50% coverage)" >> "$COVERAGE_DIR/coverage_gaps_${TIMESTAMP}.md"
    echo "" >> "$COVERAGE_DIR/coverage_gaps_${TIMESTAMP}.md"

    # Extract module coverage from tarpaulin output
    if [ -f "$COVERAGE_DIR/tarpaulin-report.json" ]; then
        echo "Found JSON coverage report, analyzing modules..."
        # This would need jq for proper JSON parsing
        # For now, we'll use the HTML report
    fi

    echo "## Recommendations" >> "$COVERAGE_DIR/coverage_gaps_${TIMESTAMP}.md"
    echo "1. Focus on modules with <50% coverage" >> "$COVERAGE_DIR/coverage_gaps_${TIMESTAMP}.md"
    echo "2. Convert ignored tests to active tests" >> "$COVERAGE_DIR/coverage_gaps_${TIMESTAMP}.md"
    echo "3. Add integration tests for end-to-end scenarios" >> "$COVERAGE_DIR/coverage_gaps_${TIMESTAMP}.md"
    echo "4. Increase unit test coverage for error paths" >> "$COVERAGE_DIR/coverage_gaps_${TIMESTAMP}.md"
}

# Function to create M1 milestone dashboard
create_m1_dashboard() {
    echo -e "${BLUE}📊 Creating M1 milestone dashboard...${NC}"

    local current_coverage=$(cat "$COVERAGE_DIR/latest_coverage.txt" 2>/dev/null || echo "0.0")
    local current_ignored=$(cat "$COVERAGE_DIR/latest_ignored_count.txt" 2>/dev/null || echo "43")
    local coverage_progress=$(echo "scale=1; ($current_coverage - $CURRENT_BASELINE) / ($TARGET_COVERAGE - $CURRENT_BASELINE) * 100" | bc -l)
    local ignored_progress=$(echo "scale=1; ($IGNORED_TESTS_CURRENT - $current_ignored) / $IGNORED_TESTS_CURRENT * 100" | bc -l)

    cat > "$COVERAGE_DIR/m1_dashboard_${TIMESTAMP}.md" << EOF
# cqlite Phase 2 - M1 Milestone Dashboard
*Generated: $(date)*

## 🎯 M1 Targets
- **Coverage Target**: ${TARGET_COVERAGE}%
- **Ignored Tests Target**: ${IGNORED_TESTS_TARGET}

## 📊 Current Status

### Coverage Progress
- **Current**: ${current_coverage}%
- **Baseline**: ${CURRENT_BASELINE}%
- **Target**: ${TARGET_COVERAGE}%
- **Progress**: ${coverage_progress}% towards target

### Ignored Tests Progress
- **Current**: ${current_ignored} ignored tests
- **Starting**: ${IGNORED_TESTS_CURRENT} ignored tests
- **Target**: ${IGNORED_TESTS_TARGET} ignored tests
- **Progress**: ${ignored_progress}% reduction completed

## 🚦 Quality Gates

### Coverage Gate
$(if (( $(echo "$current_coverage >= $TARGET_COVERAGE" | bc -l) )); then
    echo "✅ **PASSED** - Coverage target met"
else
    echo "❌ **FAILED** - Coverage below target (${current_coverage}% < ${TARGET_COVERAGE}%)"
fi)

### Ignored Tests Gate
$(if [ "$current_ignored" -le "$IGNORED_TESTS_TARGET" ]; then
    echo "✅ **PASSED** - All ignored tests resolved"
else
    echo "❌ **FAILED** - ${current_ignored} ignored tests remaining"
fi)

## 📈 Progress Tracking

| Metric | Current | Target | Status |
|--------|---------|--------|--------|
| Coverage | ${current_coverage}% | ${TARGET_COVERAGE}% | $(if (( $(echo "$current_coverage >= $TARGET_COVERAGE" | bc -l) )); then echo "✅"; else echo "⚠️"; fi) |
| Ignored Tests | ${current_ignored} | ${IGNORED_TESTS_TARGET} | $(if [ "$current_ignored" -le "$IGNORED_TESTS_TARGET" ]; then echo "✅"; else echo "⚠️"; fi) |

## 🎯 Next Actions
$(if (( $(echo "$current_coverage < $TARGET_COVERAGE" | bc -l) )); then
    echo "- [ ] Increase coverage by $((TARGET_COVERAGE - current_coverage))%"
fi)
$(if [ "$current_ignored" -gt "$IGNORED_TESTS_TARGET" ]; then
    echo "- [ ] Resolve ${current_ignored} remaining ignored tests"
fi)
- [ ] Focus on high-impact coverage gaps
- [ ] Maintain code quality while increasing coverage

---
*Coverage Validator - Orchestrating cqlite Phase 2 success*
EOF

    echo -e "${GREEN}📊 M1 Dashboard created: coverage-reports/m1_dashboard_${TIMESTAMP}.md${NC}"
}

# Function to store metrics in memory
store_metrics_in_memory() {
    echo -e "${BLUE}💾 Storing metrics in swarm memory...${NC}"

    local current_coverage=$(cat "$COVERAGE_DIR/latest_coverage.txt" 2>/dev/null || echo "0.0")
    local current_ignored=$(cat "$COVERAGE_DIR/latest_ignored_count.txt" 2>/dev/null || echo "43")

    # Store metrics using Claude Flow memory hooks
    npx claude-flow@alpha hooks post-edit \
        --file "coverage_metrics" \
        --memory-key "coverage_validator/metrics/latest" \
        --content "{\"coverage\": $current_coverage, \"ignored_tests\": $current_ignored, \"timestamp\": \"$TIMESTAMP\", \"target_coverage\": $TARGET_COVERAGE, \"target_ignored\": $IGNORED_TESTS_TARGET}"

    npx claude-flow@alpha hooks post-edit \
        --file "m1_progress" \
        --memory-key "coverage_validator/m1/progress" \
        --content "{\"coverage_progress\": $(echo "scale=1; ($current_coverage - $CURRENT_BASELINE) / ($TARGET_COVERAGE - $CURRENT_BASELINE) * 100" | bc -l), \"ignored_reduction\": $(echo "scale=1; ($IGNORED_TESTS_CURRENT - $current_ignored) / $IGNORED_TESTS_CURRENT * 100" | bc -l)}"
}

# Main execution
main() {
    echo -e "${BLUE}🚀 Starting Coverage Validator monitoring cycle...${NC}"

    # Create coverage reports directory
    mkdir -p "$COVERAGE_DIR"

    # Run all monitoring tasks
    run_coverage_analysis
    analyze_ignored_tests
    generate_coverage_gaps
    create_m1_dashboard
    store_metrics_in_memory

    echo ""
    echo -e "${GREEN}✅ Coverage monitoring cycle completed${NC}"
    echo "Reports generated in: $COVERAGE_DIR"
    echo "Latest dashboard: coverage-reports/m1_dashboard_${TIMESTAMP}.md"

    # Final status summary
    local current_coverage=$(cat "$COVERAGE_DIR/latest_coverage.txt")
    local current_ignored=$(cat "$COVERAGE_DIR/latest_ignored_count.txt")

    echo ""
    echo "=== COVERAGE VALIDATOR SUMMARY ==="
    echo "Coverage: ${current_coverage}% (target: ${TARGET_COVERAGE}%)"
    echo "Ignored Tests: ${current_ignored} (target: ${IGNORED_TESTS_TARGET})"

    if (( $(echo "$current_coverage >= $TARGET_COVERAGE" | bc -l) )) && [ "$current_ignored" -le "$IGNORED_TESTS_TARGET" ]; then
        echo -e "${GREEN}🎉 M1 MILESTONE ACHIEVED!${NC}"
        npx claude-flow@alpha hooks notify --message "🏆 M1 Milestone Complete - All targets met!"
    else
        echo -e "${YELLOW}⚡ M1 IN PROGRESS - Continue monitoring${NC}"
    fi
}

# Handle script arguments
case "${1:-monitor}" in
    "monitor")
        main
        ;;
    "coverage")
        run_coverage_analysis
        ;;
    "ignored")
        analyze_ignored_tests
        ;;
    "gaps")
        generate_coverage_gaps
        ;;
    "dashboard")
        create_m1_dashboard
        ;;
    "continuous")
        echo "Starting continuous monitoring (every 30 minutes)..."
        while true; do
            main
            sleep 1800  # 30 minutes
        done
        ;;
    *)
        echo "Usage: $0 [monitor|coverage|ignored|gaps|dashboard|continuous]"
        echo "  monitor (default): Run full monitoring cycle"
        echo "  coverage: Run coverage analysis only"
        echo "  ignored: Analyze ignored tests only"
        echo "  gaps: Generate coverage gap analysis"
        echo "  dashboard: Create M1 dashboard"
        echo "  continuous: Run monitoring every 30 minutes"
        exit 1
        ;;
esac