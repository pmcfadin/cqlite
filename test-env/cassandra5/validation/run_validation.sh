#!/bin/bash

# SSTable Validation Runner Script
# Compiles and runs all validation programs for different SSTable types

set -e

echo "🧪 SSTable Validation Suite Runner"
echo "=================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if we're in the right directory
if [[ ! -f "Cargo.toml" ]]; then
    echo -e "${RED}❌ Error: Cargo.toml not found. Please run this script from the validation directory.${NC}"
    exit 1
fi

# Check if SSTable files exist
SSTABLE_DIR="../sstables"
if [[ ! -d "$SSTABLE_DIR" ]]; then
    echo -e "${RED}❌ Error: SSTable directory not found at $SSTABLE_DIR${NC}"
    echo "Please ensure SSTable files are generated first."
    exit 1
fi

# Build all validation programs
echo -e "${BLUE}🔨 Building validation programs...${NC}"
if cargo build --release; then
    echo -e "${GREEN}✅ Build successful${NC}"
else
    echo -e "${RED}❌ Build failed${NC}"
    exit 1
fi

# Create results directory
mkdir -p validation_results
cd validation_results

echo -e "${BLUE}📁 Results will be saved to: $(pwd)${NC}"

# Run individual validation programs
echo -e "\n${BLUE}🚀 Running individual validations...${NC}"

PROGRAMS=(
    "validate_all_types:All Types (Primitive CQL types)"
    "validate_collections:Collections (List, Set, Map)"
    "validate_users:Users (User Defined Types)"
    "validate_time_series:Time Series (Clustering columns)"
    "validate_large_table:Large Table (Performance testing)"
)

SUCCESS_COUNT=0
TOTAL_COUNT=${#PROGRAMS[@]}

for program_info in "${PROGRAMS[@]}"; do
    IFS=':' read -r program description <<< "$program_info"
    
    echo -e "\n${YELLOW}📋 Running: $description${NC}"
    echo "   Program: $program"
    
    if ../target/release/$program 2>&1 | tee "${program}_output.log"; then
        echo -e "${GREEN}✅ $program completed successfully${NC}"
        ((SUCCESS_COUNT++))
    else
        echo -e "${RED}❌ $program failed${NC}"
    fi
done

# Run comprehensive validation
echo -e "\n${BLUE}🎯 Running comprehensive validation...${NC}"
if ../target/release/validate_all 2>&1 | tee "comprehensive_validation_output.log"; then
    echo -e "${GREEN}✅ Comprehensive validation completed${NC}"
else
    echo -e "${RED}❌ Comprehensive validation failed${NC}"
fi

# Generate summary
echo -e "\n${BLUE}📊 Validation Summary${NC}"
echo "===================="
echo "Individual validations: $SUCCESS_COUNT/$TOTAL_COUNT successful"

if [[ $SUCCESS_COUNT -eq $TOTAL_COUNT ]]; then
    echo -e "${GREEN}🎉 All individual validations passed!${NC}"
else
    echo -e "${YELLOW}⚠️  Some validations failed. Check logs for details.${NC}"
fi

# Show generated reports
echo -e "\n${BLUE}📄 Generated Reports:${NC}"
for report in *.json; do
    if [[ -f "$report" ]]; then
        echo "   📄 $report"
        # Show a snippet of the report
        if command -v jq &> /dev/null; then
            echo "      └─ $(jq -r '.test_name // .validation_suite // "Report"' "$report") - $(jq -r '.timestamp // "No timestamp"' "$report")"
        fi
    fi
done

# Show log files
echo -e "\n${BLUE}📋 Log Files:${NC}"
for log in *.log; do
    if [[ -f "$log" ]]; then
        echo "   📋 $log"
    fi
done

echo -e "\n${BLUE}💡 Next Steps:${NC}"
echo "   1. Review JSON reports for detailed results"
echo "   2. Check log files for any error details"
echo "   3. Compare expected vs actual data formats"
echo "   4. Update cqlite parser if discrepancies found"

# Check if comprehensive report exists and show key metrics
if [[ -f "comprehensive_validation_report.json" ]]; then
    echo -e "\n${BLUE}🎯 Key Metrics from Comprehensive Report:${NC}"
    if command -v jq &> /dev/null; then
        echo "   • Overall success rate: $(jq -r '.overall_success_rate // "N/A"' comprehensive_validation_report.json)%"
        echo "   • Total test cases: $(jq -r '.summary.total_test_cases // "N/A"' comprehensive_validation_report.json)"
        echo "   • Total tests: $(jq -r '.summary.total_tests // "N/A"' comprehensive_validation_report.json)"
        echo "   • Tests passed: $(jq -r '.summary.total_passed // "N/A"' comprehensive_validation_report.json)"
        echo "   • Tests failed: $(jq -r '.summary.total_failed // "N/A"' comprehensive_validation_report.json)"
    else
        echo "   📄 Install 'jq' to see detailed metrics"
    fi
fi

echo -e "\n${GREEN}✅ Validation suite completed!${NC}"