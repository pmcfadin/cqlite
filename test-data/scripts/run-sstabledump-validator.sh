#!/bin/bash

# SSTableDump Validator Docker Integration Script
# Issue #30: Test sstabledump validator against real Cassandra (BIG format only)
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

# Configuration with parametrization support for future datasets (#36)
CASSANDRA_VERSION="${CASSANDRA_VERSION:-5.0}"
ZERO_TOLERANCE="${ZERO_TOLERANCE:-true}"
VERBOSE="${VERBOSE:-false}"

# Parametrization for future datasets (used later by #36)
DATASET_DIRS="${DATASET_DIRS:-$TEST_DATA_DIR}"
DATASET_LIST="${DATASET_LIST:-all_types-285fca806e5411f0a72add2bbbd2f55e,collections_table-286e22606e5411f0a72add2bbbd2f55e,counters-28b7fca06e5411f0a72add2bbbd2f55e,large_table-28aed4e06e5411f0a72add2bbbd2f55e,multi_clustering-28a44d906e5411f0a72add2bbbd2f55e,static_test-28c25ce06e5411f0a72add2bbbd2f55e,time_series-2894bd306e5411f0a72add2bbbd2f55e,users-28883a106e5411f0a72add2bbbd2f55e}"

echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}    SSTableDump Validator - Docker Integration (BIG)     ${NC}"
echo -e "${BLUE}    Issue #30: BIG format only (BTI reserved for #36)    ${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
echo ""

# Function to check prerequisites with deterministic failure policy
check_prerequisites() {
    echo -e "${YELLOW}Checking prerequisites...${NC}"
    local failed=false
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}Error: Docker is not installed${NC}"
        failed=true
    fi
    
    # Check docker-compose
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${RED}Error: docker-compose is not installed${NC}"
        failed=true
    fi
    
    # Check if validator directory exists
    if [ ! -d "$VALIDATOR_DIR" ]; then
        echo -e "${RED}Error: Validator directory not found at $VALIDATOR_DIR${NC}"
        failed=true
    fi
    
    # Check if validator is built
    if [ ! -f "$VALIDATOR_DIR/target/release/sstabledump-validator" ]; then
        echo -e "${YELLOW}Building sstabledump-validator...${NC}"
        cd "$VALIDATOR_DIR"
        if ! cargo build --release; then
            echo -e "${RED}Error: Failed to build validator${NC}"
            failed=true
        fi
    fi
    
    # Fail fast if any prerequisite is missing
    if [ "$failed" = true ]; then
        echo -e "${RED}Prerequisites check failed. Exiting to avoid false greens.${NC}"
        exit 1
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
    if ! docker-compose -f docker-compose-cassandra5.yml up -d cassandra-5-0; then
        echo -e "${RED}Error: Failed to start Cassandra container${NC}"
        exit 1
    fi
    
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
        echo -e "${RED}Error: Cassandra failed to start within timeout${NC}"
        exit 1
    fi
}

# Function to create BIG smoke test data in container
create_big_smoke_test() {
    echo -e "${YELLOW}Creating BIG smoke test data in container...${NC}"
    
    local smoke_dir="$PROJECT_ROOT/test-data/cassandra5/big/smoke"
    mkdir -p "$smoke_dir"
    
    # Create a simple BIG table in the container
    docker exec cqlite-cassandra-5-0 cqlsh -e "
        CREATE KEYSPACE IF NOT EXISTS smoke_test 
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
        
        USE smoke_test;
        
        CREATE TABLE IF NOT EXISTS big_smoke_test (
            id UUID PRIMARY KEY,
            name TEXT,
            value INT,
            created_at TIMESTAMP
        );
        
        INSERT INTO big_smoke_test (id, name, value, created_at) 
        VALUES (uuid(), 'test_row_1', 42, toTimestamp(now()));
        
        INSERT INTO big_smoke_test (id, name, value, created_at) 
        VALUES (uuid(), 'test_row_2', 84, toTimestamp(now()));
        
        INSERT INTO big_smoke_test (id, name, value, created_at) 
        VALUES (uuid(), 'test_row_3', 126, toTimestamp(now()));
    " || {
        echo -e "${RED}Error: Failed to create smoke test data${NC}"
        exit 1
    }
    
    # Force flush to ensure data is written to SSTables
    docker exec cqlite-cassandra-5-0 nodetool flush smoke_test big_smoke_test || {
        echo -e "${RED}Error: Failed to flush smoke test data${NC}"
        exit 1
    }
    
    # Find and copy the Data.db file
    local data_file=$(docker exec cqlite-cassandra-5-0 find /var/lib/cassandra/data/smoke_test/big_smoke_test* -name "*-Data.db" | head -1)
    
    if [ -z "$data_file" ]; then
        echo -e "${RED}Error: No Data.db file found for smoke test${NC}"
        exit 1
    fi
    
    # Copy the SSTable file to our test directory
    docker cp "cqlite-cassandra-5-0:$data_file" "$smoke_dir/smoke-test-Data.db"
    
    echo -e "${GREEN}✓ BIG smoke test data created at $smoke_dir${NC}"
    
    # Add smoke test to our dataset list
    DATASET_LIST="$DATASET_LIST,smoke_test"
    DATASET_DIRS="$DATASET_DIRS,$smoke_dir"
}

# Function to identify and validate SSTable collections
identify_sstable_collections() {
    echo -e "${YELLOW}Identifying and validating SSTable collections...${NC}"
    
    # Convert comma-separated list to array
    IFS=',' read -ra COLLECTIONS <<< "$DATASET_LIST"
    IFS=',' read -ra DIRS <<< "$DATASET_DIRS"
    
    local failed_collections=()
    
    for i in "${!COLLECTIONS[@]}"; do
        local collection="${COLLECTIONS[i]}"
        local search_dir="${DIRS[i]:-$TEST_DATA_DIR}"
        
        # Handle smoke test special case
        if [ "$collection" = "smoke_test" ]; then
            local collection_path="$search_dir"
        else
            local collection_path="$search_dir/$collection"
        fi
        
        if [ ! -d "$collection_path" ]; then
            echo -e "${RED}✗ Collection directory missing: $collection_path${NC}"
            failed_collections+=("$collection")
            continue
        fi
        
        # Find Data.db file - fail fast if missing
        local data_file=$(find "$collection_path" -name "*-Data.db" | head -1)
        if [ -z "$data_file" ]; then
            echo -e "${RED}✗ No Data.db file found in $collection_path${NC}"
            failed_collections+=("$collection")
            continue
        fi
        
        echo -e "${GREEN}✓ $collection${NC}: $(basename "$data_file")"
    done
    
    # Fail fast if any collection is missing Data.db files
    if [ ${#failed_collections[@]} -ne 0 ]; then
        echo -e "${RED}Error: Missing Data.db files for collections: ${failed_collections[*]}${NC}"
        echo -e "${RED}Failing fast to avoid false greens.${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ Found ${#COLLECTIONS[@]} valid SSTable collections${NC}"
}

# Function to generate JUnit XML for a collection result
generate_junit_xml() {
    local collection_name="$1"
    local status="$2"
    local collection_results="$3"
    local duration="$4"
    
    local junit_file="$collection_results/junit.xml"
    local test_case_name="sstabledump_validation_${collection_name}"
    
    cat > "$junit_file" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="SSTableDump Validation" tests="1" failures="$([ "$status" = "FAILED" ] && echo "1" || echo "0")" errors="0" time="$duration">
  <testcase name="$test_case_name" classname="ValidationHarness" time="$duration">
EOF
    
    if [ "$status" = "FAILED" ]; then
        cat >> "$junit_file" << EOF
    <failure message="Validation failed for $collection_name">
      <![CDATA[$(cat "$collection_results/validation.log" 2>/dev/null | tail -20)]]>
    </failure>
EOF
    fi
    
    cat >> "$junit_file" << EOF
  </testcase>
</testsuite>
EOF
    
    echo -e "${BLUE}Generated JUnit XML: $junit_file${NC}"
}

# Function to run validator on a single collection
run_validator_on_collection() {
    local collection_name="$1"
    local search_dir="$2"
    
    echo -e "${BLUE}Testing collection: $collection_name${NC}"
    
    # Create results directory for this collection
    local collection_results="$RESULTS_DIR/$collection_name"
    mkdir -p "$collection_results"
    
    local start_time=$(date +%s.%N)
    
    # Handle smoke test special case
    if [ "$collection_name" = "smoke_test" ]; then
        local collection_path="$search_dir"
    else
        local collection_path="$search_dir/$collection_name"
    fi
    
    # Find Data.db file
    local data_file=$(find "$collection_path" -name "*-Data.db" | head -1)
    
    if [ -z "$data_file" ]; then
        echo -e "${RED}Error: No Data.db file found in $collection_path${NC}"
        local end_time=$(date +%s.%N)
        local duration=$(awk "BEGIN {print $end_time - $start_time}")
        echo "FAILED" > "$collection_results/status.txt"
        echo "No Data.db file found" > "$collection_results/validation.log"
        generate_junit_xml "$collection_name" "FAILED" "$collection_results" "$duration"
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
        local end_time=$(date +%s.%N)
        local duration=$(awk "BEGIN {print $end_time - $start_time}")
        echo -e "${GREEN}✓ Validation passed for $collection_name${NC}"
        echo "PASSED" > "$collection_results/status.txt"
        generate_junit_xml "$collection_name" "PASSED" "$collection_results" "$duration"
        return 0
    else
        local end_time=$(date +%s.%N)
        local duration=$(awk "BEGIN {print $end_time - $start_time}")
        echo -e "${RED}✗ Validation failed for $collection_name${NC}"
        echo "FAILED" > "$collection_results/status.txt"
        generate_junit_xml "$collection_name" "FAILED" "$collection_results" "$duration"
        return 1
    fi
}

# Function to run validator on all collections
run_validator_on_all_collections() {
    echo -e "${YELLOW}Running validator in zero-tolerance mode...${NC}"
    echo ""
    
    mkdir -p "$RESULTS_DIR"
    
    # Convert comma-separated lists to arrays
    IFS=',' read -ra COLLECTIONS <<< "$DATASET_LIST"
    IFS=',' read -ra DIRS <<< "$DATASET_DIRS"
    
    local total_collections=${#COLLECTIONS[@]}
    local passed_count=0
    local failed_count=0
    
    for i in "${!COLLECTIONS[@]}"; do
        local collection="${COLLECTIONS[i]}"
        local search_dir="${DIRS[i]:-$TEST_DATA_DIR}"
        
        if run_validator_on_collection "$collection" "$search_dir"; then
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

# Function to generate summary report (using awk instead of bc for portability)
generate_summary_report() {
    local total="$1"
    local passed="$2"
    local failed="$3"
    
    local report_file="$RESULTS_DIR/summary.md"
    
    # Calculate success rate using awk
    local success_rate=$(awk "BEGIN {print ($passed * 100 / $total)}")
    
    cat > "$report_file" << EOF
# SSTableDump Validation Report - Issue #30

**BIG Format Validation Only** (BTI reserved for Issue #36)

## Summary

- **Date**: $(date)
- **Cassandra Version**: $CASSANDRA_VERSION
- **Zero Tolerance Mode**: $ZERO_TOLERANCE
- **Total Collections**: $total
- **Passed**: $passed
- **Failed**: $failed
- **Success Rate**: ${success_rate}%

## Collections Tested

EOF
    
    # Convert comma-separated list to array for reporting
    IFS=',' read -ra COLLECTIONS <<< "$DATASET_LIST"
    
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

## Artifacts

- **JUnit XML**: One per collection in \`$RESULTS_DIR/<collection>/junit.xml\`
- **Detailed Logs**: \`$RESULTS_DIR/<collection>/validation.log\`
- **Status Files**: \`$RESULTS_DIR/<collection>/status.txt\`

## Environment

- **Docker Version**: $(docker --version)
- **Docker Compose Version**: $(docker-compose --version)
- **Rust Version**: $(rustc --version)
- **CQLite Commit**: $(cd "$PROJECT_ROOT" && git rev-parse --short HEAD)

---

Generated for GitHub Issue #30 (BIG format only)
BTI validation will be handled in Issue #36
EOF
    
    echo ""
    echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}                    VALIDATION SUMMARY                   ${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
    echo -e "Total Collections: $total"
    echo -e "Passed: ${GREEN}$passed${NC}"
    echo -e "Failed: ${RED}$failed${NC}"
    echo -e "Success Rate: ${success_rate}%"
    echo ""
    echo -e "Full report: ${BLUE}$report_file${NC}"
    echo -e "JUnit artifacts: ${BLUE}$RESULTS_DIR/*/junit.xml${NC}"
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
    create_big_smoke_test
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