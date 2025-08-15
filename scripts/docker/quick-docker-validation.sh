#!/bin/bash

# CQLite Quick Docker Validation - Issue #30
# Lightweight validation script for rapid testing and development
# Companion to docker-validator-orchestrator.sh

set -euo pipefail

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Script configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
DOCKER_DIR="$PROJECT_ROOT/test-data/docker"
VALIDATOR_DIR="$PROJECT_ROOT/tools/sstabledump-validator"

# Quick validation settings
QUICK_MODE="${QUICK_MODE:-true}"
SINGLE_TABLE="${SINGLE_TABLE:-basic_types}"
TIMEOUT="${TIMEOUT:-300}"

echo -e "${BLUE}🚀 CQLite Quick Docker Validation${NC}"
echo -e "${BLUE}   Fast validation for development/CI${NC}"
echo ""

# Function for logging
quick_log() {
    echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"
}

# Function to check if containers are running
check_containers() {
    quick_log "Checking container status..."
    
    if ! docker ps --filter "name=cqlite-cassandra-5-0" --format "{{.Names}}" | grep -q "cqlite-cassandra-5-0"; then
        quick_log "${YELLOW}Starting Cassandra 5.0 container...${NC}"
        cd "$DOCKER_DIR"
        docker-compose -f docker-compose-cassandra5.yml up -d cassandra-5-0
        
        # Quick health check
        local attempts=0
        while [ $attempts -lt 30 ]; do
            if docker exec cqlite-cassandra-5-0 cqlsh -e "SELECT cluster_name FROM system.local;" &>/dev/null; then
                break
            fi
            sleep 5
            attempts=$((attempts + 1))
        done
        
        if [ $attempts -eq 30 ]; then
            echo -e "${RED}❌ Cassandra failed to start${NC}"
            exit 1
        fi
    fi
    
    quick_log "${GREEN}✓ Cassandra 5.0 container is ready${NC}"
}

# Function to create minimal test data
create_minimal_test_data() {
    quick_log "Creating minimal test data..."
    
    docker exec cqlite-cassandra-5-0 cqlsh -e "
        CREATE KEYSPACE IF NOT EXISTS quick_test 
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
        
        USE quick_test;
        
        CREATE TABLE IF NOT EXISTS $SINGLE_TABLE (
            id UUID PRIMARY KEY,
            name TEXT,
            value INT,
            created_at TIMESTAMP
        );
        
        INSERT INTO $SINGLE_TABLE (id, name, value, created_at) 
        VALUES (uuid(), 'quick_test_1', 42, toTimestamp(now()));
        
        INSERT INTO $SINGLE_TABLE (id, name, value, created_at) 
        VALUES (uuid(), 'quick_test_2', 84, toTimestamp(now()));
    " || {
        echo -e "${RED}❌ Failed to create test data${NC}"
        exit 1
    }
    
    # Flush to SSTable
    docker exec cqlite-cassandra-5-0 nodetool flush quick_test $SINGLE_TABLE
    
    quick_log "${GREEN}✓ Test data created and flushed${NC}"
}

# Function to extract and validate single SSTable
extract_and_validate() {
    quick_log "Extracting SSTable for validation..."
    
    local temp_dir=$(mktemp -d)
    local table_dir=$(docker exec cqlite-cassandra-5-0 find /var/lib/cassandra/data/quick_test -name "${SINGLE_TABLE}-*" -type d | head -1)
    
    if [ -z "$table_dir" ]; then
        echo -e "${RED}❌ No SSTable directory found${NC}"
        exit 1
    fi
    
    local sstable_file=$(docker exec cqlite-cassandra-5-0 find "$table_dir" -name "*-Data.db" | head -1)
    
    if [ -z "$sstable_file" ]; then
        echo -e "${RED}❌ No Data.db file found${NC}"
        exit 1
    fi
    
    local local_file="$temp_dir/test-Data.db"
    docker cp "cqlite-cassandra-5-0:$sstable_file" "$local_file"
    
    quick_log "${GREEN}✓ SSTable extracted to $local_file${NC}"
    
    # Run validation
    quick_log "Running zero-tolerance validation..."
    
    cd "$VALIDATOR_DIR"
    if ! cargo build --release --quiet; then
        echo -e "${RED}❌ Failed to build validator${NC}"
        exit 1
    fi
    
    local start_time=$(date +%s)
    if timeout $TIMEOUT ./target/release/sstabledump-validator validate "$local_file" --fail-on-diff --detailed; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        echo -e "${GREEN}✅ Validation PASSED in ${duration}s${NC}"
        echo -e "${GREEN}✅ Zero-tolerance validation successful${NC}"
        rm -rf "$temp_dir"
        return 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        echo -e "${RED}❌ Validation FAILED in ${duration}s${NC}"
        echo -e "${RED}❌ Zero-tolerance validation failed${NC}"
        rm -rf "$temp_dir"
        return 1
    fi
}

# Function to run performance test
run_performance_test() {
    quick_log "Running performance test..."
    
    local start_time=$(date +%s.%N)
    
    # Create larger dataset
    docker exec cqlite-cassandra-5-0 cqlsh -e "
        USE quick_test;
        BEGIN BATCH
        $(for i in {1..100}; do echo "INSERT INTO $SINGLE_TABLE (id, name, value, created_at) VALUES (uuid(), 'perf_test_$i', $((i * 10)), toTimestamp(now()));"; done)
        APPLY BATCH;
    " &>/dev/null
    
    docker exec cqlite-cassandra-5-0 nodetool flush quick_test $SINGLE_TABLE &>/dev/null
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l 2>/dev/null || echo "0")
    
    quick_log "${GREEN}✓ Performance test completed in ${duration}s${NC}"
}

# Main execution
main() {
    local start_time=$(date +%s)
    
    quick_log "Starting quick validation run..."
    
    # Check prerequisites
    if ! command -v docker &>/dev/null; then
        echo -e "${RED}❌ Docker not found${NC}"
        exit 1
    fi
    
    if [ ! -d "$VALIDATOR_DIR" ]; then
        echo -e "${RED}❌ Validator directory not found${NC}"
        exit 1
    fi
    
    # Run validation steps
    check_containers
    create_minimal_test_data
    
    if [ "${1:-}" = "--perf" ]; then
        run_performance_test
    fi
    
    extract_and_validate
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    echo ""
    echo -e "${GREEN}🎉 Quick validation completed successfully!${NC}"
    echo -e "${GREEN}⏱️  Total time: ${total_duration}s${NC}"
    echo -e "${BLUE}📋 Ready for full validation with docker-validator-orchestrator.sh${NC}"
    echo ""
}

# Help function
show_help() {
    echo "CQLite Quick Docker Validation"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --perf        Run performance test with larger dataset"
    echo "  --table NAME  Use specific table name (default: basic_types)"
    echo "  --timeout SEC Set timeout in seconds (default: 300)"
    echo "  --help        Show this help"
    echo ""
    echo "Environment Variables:"
    echo "  SINGLE_TABLE  Table name to test (default: basic_types)"
    echo "  TIMEOUT       Timeout in seconds (default: 300)"
    echo ""
    echo "Examples:"
    echo "  $0                    # Quick validation"
    echo "  $0 --perf            # With performance test"
    echo "  $0 --table users     # Test specific table"
    echo "  TIMEOUT=600 $0       # Extended timeout"
}

# Parse arguments
case "${1:-}" in
    --help|-h)
        show_help
        exit 0
        ;;
    --table)
        SINGLE_TABLE="$2"
        shift 2
        main "$@"
        ;;
    --timeout)
        TIMEOUT="$2"
        shift 2
        main "$@"
        ;;
    *)
        main "$@"
        ;;
esac