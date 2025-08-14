#!/bin/bash

# SSTableDump Validator Docker Integration Script
# Issue #30: Test sstabledump validator against real Cassandra
# 
# This script wires the existing sstabledump validator into Docker infrastructure
# and runs it against real SSTables across versions.

set -e

# Color output for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
VALIDATOR_DIR="$PROJECT_ROOT/tools/sstabledump-validator"
TEST_DATA_DIR="$PROJECT_ROOT/tests/data/sstables"
DOCKER_DIR="$PROJECT_ROOT/test-data/docker"
RESULTS_DIR="$PROJECT_ROOT/validation-results-$(date +%Y%m%d-%H%M%S)"

# Configuration
CASSANDRA_VERSION="${CASSANDRA_VERSION:-5.0}"
ZERO_TOLERANCE="${ZERO_TOLERANCE:-true}"
VERBOSE="${VERBOSE:-false}"

echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}    SSTableDump Validator - Docker Integration Test     ${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
echo ""

# Function to check prerequisites
check_prerequisites() {
    echo -e "${YELLOW}Checking prerequisites...${NC}"
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}Error: Docker is not installed${NC}"
        exit 1
    fi
    
    # Check docker-compose
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${RED}Error: docker-compose is not installed${NC}"
        exit 1
    fi
    
    # Check if validator is built
    if [ ! -f "$VALIDATOR_DIR/target/release/sstabledump-validator" ]; then
        echo -e "${YELLOW}Building sstabledump-validator...${NC}"
        cd "$VALIDATOR_DIR"
        cargo build --release
    fi
    
    echo -e "${GREEN}✓ Prerequisites satisfied${NC}"
}

# Function to start Docker infrastructure
start_docker_infrastructure() {
    echo -e "${YELLOW}Starting Docker Cassandra 5.0 infrastructure...${NC}"
    
    cd "$DOCKER_DIR"
    
    # Stop any existing containers
    docker-compose -f docker-compose-cassandra5.yml down 2>/dev/null || true
    
    # Start Cassandra 5.0 cluster
    docker-compose -f docker-compose-cassandra5.yml up -d cassandra-5-0
    
    # Wait for Cassandra to be ready
    echo -e "${YELLOW}Waiting for Cassandra to be ready...${NC}"
    local max_attempts=30
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if docker exec cqlite-cassandra-5-0 cqlsh -e "SELECT cluster_name FROM system.local;" &>/dev/null; then
            echo -e "${GREEN}✓ Cassandra is ready${NC}"
            break
        fi
        echo -n "."
        sleep 10
        attempt=$((attempt + 1))
    done
    
    if [ $attempt -eq $max_attempts ]; then
        echo -e "${RED}Error: Cassandra failed to start${NC}"
        exit 1
    fi
}

# Function to generate test data if needed
generate_test_data() {
    echo -e "${YELLOW}Checking for existing test data...${NC}"
    
    # Check if we have existing SSTable collections
    local sstable_count=$(find "$TEST_DATA_DIR" -name "*.db" 2>/dev/null | wc -l)
    
    if [ "$sstable_count" -eq 0 ]; then
        echo -e "${YELLOW}No existing SSTables found. Generating test data...${NC}"
        
        # Run the data generation script
        if [ -f "$SCRIPT_DIR/generate-all-test-data.sh" ]; then
            bash "$SCRIPT_DIR/generate-all-test-data.sh"
        else
            # Use validator's generate command
            "$VALIDATOR_DIR/target/release/sstabledump-validator" generate \
                --count 100 \
                --edge-cases
        fi
    else
        echo -e "${GREEN}✓ Found $sstable_count existing SSTable files${NC}"
    fi
}

# Function to identify SSTable collections
identify_sstable_collections() {
    echo -e "${YELLOW}Identifying SSTable collections...${NC}"
    
    # The 8 collections as identified
    COLLECTIONS=(
        "all_types-285fca806e5411f0a72add2bbbd2f55e"
        "collections_table-286e22606e5411f0a72add2bbbd2f55e"
        "counters-28b7fca06e5411f0a72add2bbbd2f55e"
        "large_table-28aed4e06e5411f0a72add2bbbd2f55e"
        "multi_clustering-28a44d906e5411f0a72add2bbbd2f55e"
        "static_test-28c25ce06e5411f0a72add2bbbd2f55e"
        "time_series-2894bd306e5411f0a72add2bbbd2f55e"
        "users-28883a106e5411f0a72add2bbbd2f55e"
    )
    
    echo -e "${GREEN}✓ Found ${#COLLECTIONS[@]} SSTable collections${NC}"
    for collection in "${COLLECTIONS[@]}"; do
        echo "  - $collection"
    done
}

# Function to run validator on a single collection
run_validator_on_collection() {
    local collection_name="$1"
    local collection_path="$TEST_DATA_DIR/$collection_name"
    
    echo -e "${BLUE}Testing collection: $collection_name${NC}"
    
    # Create results directory for this collection
    local collection_results="$RESULTS_DIR/$collection_name"
    mkdir -p "$collection_results"
    
    # Find Data.db file in the collection
    local data_file=$(find "$collection_path" -name "*-Data.db" | head -1)
    
    if [ -z "$data_file" ]; then
        echo -e "${YELLOW}Warning: No Data.db file found in $collection_name${NC}"
        return 1
    fi
    
    # Run the validator
    local cmd="$VALIDATOR_DIR/target/release/sstabledump-validator validate \"$data_file\""
    
    if [ "$ZERO_TOLERANCE" = "true" ]; then
        cmd="$cmd --fail-on-diff"
    fi
    
    if [ "$VERBOSE" = "true" ]; then
        cmd="$cmd --detailed"
    fi
    
    echo "Running: $cmd"
    
    # Execute and capture output
    if eval "$cmd" > "$collection_results/validation.log" 2>&1; then
        echo -e "${GREEN}✓ Validation passed for $collection_name${NC}"
        echo "PASSED" > "$collection_results/status.txt"
        return 0
    else
        echo -e "${RED}✗ Validation failed for $collection_name${NC}"
        echo "FAILED" > "$collection_results/status.txt"
        return 1
    fi
}

# Function to run validator on all collections
run_validator_on_all_collections() {
    echo -e "${YELLOW}Running validator in zero-tolerance mode...${NC}"
    echo ""
    
    mkdir -p "$RESULTS_DIR"
    
    local total_collections=${#COLLECTIONS[@]}
    local passed_count=0
    local failed_count=0
    
    for collection in "${COLLECTIONS[@]}"; do
        if run_validator_on_collection "$collection"; then
            passed_count=$((passed_count + 1))
        else
            failed_count=$((failed_count + 1))
        fi
        echo ""
    done
    
    # Generate summary report
    generate_summary_report "$total_collections" "$passed_count" "$failed_count"
    
    # Return failure if any collection failed in zero-tolerance mode
    if [ "$ZERO_TOLERANCE" = "true" ] && [ "$failed_count" -gt 0 ]; then
        return 1
    fi
    
    return 0
}

# Function to generate summary report
generate_summary_report() {
    local total="$1"
    local passed="$2"
    local failed="$3"
    
    local report_file="$RESULTS_DIR/summary.md"
    
    cat > "$report_file" << EOF
# SSTableDump Validation Report

**Issue #30**: Test sstabledump validator against real Cassandra

## Summary

- **Date**: $(date)
- **Cassandra Version**: $CASSANDRA_VERSION
- **Zero Tolerance Mode**: $ZERO_TOLERANCE
- **Total Collections**: $total
- **Passed**: $passed
- **Failed**: $failed
- **Success Rate**: $(echo "scale=2; $passed * 100 / $total" | bc)%

## Collections Tested

EOF
    
    for collection in "${COLLECTIONS[@]}"; do
        local status=$(cat "$RESULTS_DIR/$collection/status.txt" 2>/dev/null || echo "UNKNOWN")
        if [ "$status" = "PASSED" ]; then
            echo "- ✅ $collection" >> "$report_file"
        elif [ "$status" = "FAILED" ]; then
            echo "- ❌ $collection" >> "$report_file"
        else
            echo "- ⚠️ $collection (status unknown)" >> "$report_file"
        fi
    done
    
    cat >> "$report_file" << EOF

## Commands Used

\`\`\`bash
# Start Docker infrastructure
docker-compose -f $DOCKER_DIR/docker-compose-cassandra5.yml up -d cassandra-5-0

# Run validator
$VALIDATOR_DIR/target/release/sstabledump-validator validate <sstable> \\
    $([ "$ZERO_TOLERANCE" = "true" ] && echo "--fail-on-diff") \\
    $([ "$VERBOSE" = "true" ] && echo "--detailed")
\`\`\`

## Logs

Detailed logs are available in: \`$RESULTS_DIR\`

## Environment

- **Docker Version**: $(docker --version)
- **Docker Compose Version**: $(docker-compose --version)
- **Rust Version**: $(rustc --version)
- **CQLite Commit**: $(cd "$PROJECT_ROOT" && git rev-parse --short HEAD)

---

Generated for GitHub Issue #30
EOF
    
    echo ""
    echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}                    VALIDATION SUMMARY                   ${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
    echo -e "Total Collections: $total"
    echo -e "Passed: ${GREEN}$passed${NC}"
    echo -e "Failed: ${RED}$failed${NC}"
    echo -e "Success Rate: $(echo "scale=2; $passed * 100 / $total" | bc)%"
    echo ""
    echo -e "Full report: ${BLUE}$report_file${NC}"
    echo -e "Detailed logs: ${BLUE}$RESULTS_DIR${NC}"
}

# Function to cleanup
cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    
    # Optional: stop Docker containers
    # cd "$DOCKER_DIR"
    # docker-compose -f docker-compose-cassandra5.yml down
    
    echo -e "${GREEN}✓ Cleanup complete${NC}"
}

# Main execution
main() {
    # Trap for cleanup on exit
    trap cleanup EXIT
    
    # Run the validation steps
    check_prerequisites
    start_docker_infrastructure
    generate_test_data
    identify_sstable_collections
    
    # Run the validator
    if run_validator_on_all_collections; then
        echo -e "${GREEN}✅ All validations passed successfully!${NC}"
        exit 0
    else
        echo -e "${RED}❌ Some validations failed${NC}"
        exit 1
    fi
}

# Run main function
main "$@"