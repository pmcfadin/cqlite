#!/bin/bash

# CQLite Docker Validator Orchestrator - Issue #30
# Comprehensive Docker infrastructure for validator testing against real SSTables
# CRITICAL P0 BLOCKER FOR M1

set -euo pipefail

# Color output for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# Script directory and project paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
DOCKER_DIR="$PROJECT_ROOT/test-data/docker"
VALIDATOR_DIR="$PROJECT_ROOT/tools/sstabledump-validator"
RESULTS_BASE_DIR="$PROJECT_ROOT/validation-artifacts"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
RESULTS_DIR="$RESULTS_BASE_DIR/run-$TIMESTAMP"

# Configuration
CASSANDRA_VERSIONS="${CASSANDRA_VERSIONS:-5.0,4.1}"
ZERO_TOLERANCE="${ZERO_TOLERANCE:-true}"
VERBOSE="${VERBOSE:-false}"
PARALLEL_EXECUTION="${PARALLEL_EXECUTION:-true}"
ARCHIVE_RESULTS="${ARCHIVE_RESULTS:-true}"
CI_MODE="${CI_MODE:-false}"

# Docker container names
CASSANDRA_5_CONTAINER="cqlite-cassandra-5-0"
CASSANDRA_4_CONTAINER="cqlite-cassandra-4-1-compat"

echo -e "${WHITE}════════════════════════════════════════════════════════════════════════════════${NC}"
echo -e "${WHITE}    CQLite Docker Validator Orchestrator - Issue #30 (P0 M1 Blocker)             ${NC}"
echo -e "${WHITE}    Comprehensive Docker infrastructure for zero-tolerance validation              ${NC}"
echo -e "${WHITE}════════════════════════════════════════════════════════════════════════════════${NC}"
echo ""

# Function to log with timestamp
log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    case "$level" in
        "INFO")  echo -e "${BLUE}[$timestamp] [INFO]${NC}  $message" ;;
        "WARN")  echo -e "${YELLOW}[$timestamp] [WARN]${NC}  $message" ;;
        "ERROR") echo -e "${RED}[$timestamp] [ERROR]${NC} $message" ;;
        "SUCCESS") echo -e "${GREEN}[$timestamp] [SUCCESS]${NC} $message" ;;
        "DEBUG") [ "$VERBOSE" = "true" ] && echo -e "${PURPLE}[$timestamp] [DEBUG]${NC} $message" ;;
    esac
}

# Function to check prerequisites with comprehensive validation
check_prerequisites() {
    log "INFO" "Checking comprehensive prerequisites..."
    
    local failed=false
    local missing_tools=()
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        missing_tools+=("docker")
        failed=true
    else
        log "DEBUG" "Docker version: $(docker --version)"
    fi
    
    # Check docker-compose
    if ! command -v docker-compose &> /dev/null; then
        missing_tools+=("docker-compose")
        failed=true
    else
        log "DEBUG" "Docker Compose version: $(docker-compose --version)"
    fi
    
    # Check Rust/Cargo
    if ! command -v cargo &> /dev/null; then
        missing_tools+=("cargo")
        failed=true
    else
        log "DEBUG" "Cargo version: $(cargo --version)"
    fi
    
    # Check if Docker daemon is running
    if ! docker info >/dev/null 2>&1; then
        log "ERROR" "Docker daemon is not running"
        failed=true
    fi
    
    # Check Docker directory exists
    if [ ! -d "$DOCKER_DIR" ]; then
        log "ERROR" "Docker directory not found at $DOCKER_DIR"
        failed=true
    fi
    
    # Check validator directory exists
    if [ ! -d "$VALIDATOR_DIR" ]; then
        log "ERROR" "Validator directory not found at $VALIDATOR_DIR"
        failed=true
    fi
    
    # Check if validator is built or buildable
    if [ ! -f "$VALIDATOR_DIR/target/release/sstabledump-validator" ]; then
        log "WARN" "Validator not built, attempting to build..."
        cd "$VALIDATOR_DIR"
        if ! cargo build --release; then
            log "ERROR" "Failed to build validator"
            failed=true
        else
            log "SUCCESS" "Validator built successfully"
        fi
        cd "$PROJECT_ROOT"
    else
        log "DEBUG" "Validator already built"
    fi
    
    # Report missing tools
    if [ ${#missing_tools[@]} -ne 0 ]; then
        log "ERROR" "Missing required tools: ${missing_tools[*]}"
        failed=true
    fi
    
    # Fail fast if any prerequisite is missing
    if [ "$failed" = "true" ]; then
        log "ERROR" "Prerequisites check failed. Cannot proceed with validation."
        exit 1
    fi
    
    log "SUCCESS" "All prerequisites satisfied"
}

# Function to cleanup any existing containers
cleanup_existing_containers() {
    log "INFO" "Cleaning up any existing containers..."
    
    cd "$DOCKER_DIR"
    
    # Stop all compose stacks
    for compose_file in docker-compose*.yml; do
        if [ -f "$compose_file" ]; then
            log "DEBUG" "Stopping $compose_file"
            docker-compose -f "$compose_file" down --remove-orphans >/dev/null 2>&1 || true
        fi
    done
    
    # Remove any orphaned containers
    docker ps -aq --filter "name=cqlite" | xargs -r docker rm -f >/dev/null 2>&1 || true
    
    log "SUCCESS" "Container cleanup complete"
}

# Function to start Cassandra infrastructure
start_cassandra_infrastructure() {
    log "INFO" "Starting Cassandra Docker infrastructure..."
    
    cd "$DOCKER_DIR"
    
    # Start Cassandra 5.0 - primary target
    log "INFO" "Starting Cassandra 5.0 container..."
    if ! docker-compose -f docker-compose-cassandra5.yml up -d cassandra-5-0; then
        log "ERROR" "Failed to start Cassandra 5.0 container"
        return 1
    fi
    
    # Start Cassandra 4.1 for compatibility testing
    log "INFO" "Starting Cassandra 4.1 container for compatibility..."
    if ! docker-compose -f docker-compose-multi-version.yml up -d cassandra-4-1; then
        log "ERROR" "Failed to start Cassandra 4.1 container"
        return 1
    fi
    
    log "SUCCESS" "All Cassandra containers started"
}

# Function to wait for containers to be healthy
wait_for_cassandra_health() {
    log "INFO" "Waiting for Cassandra containers to become healthy..."
    
    local containers=("$CASSANDRA_5_CONTAINER" "$CASSANDRA_4_CONTAINER")
    local max_attempts=60
    local check_interval=10
    
    for container in "${containers[@]}"; do
        log "INFO" "Checking health of $container..."
        local attempt=0
        
        while [ $attempt -lt $max_attempts ]; do
            if docker exec "$container" cqlsh -e "SELECT cluster_name FROM system.local;" &>/dev/null; then
                log "SUCCESS" "$container is healthy and ready"
                break
            fi
            
            if [ $((attempt % 6)) -eq 0 ]; then  # Log every minute
                log "INFO" "Waiting for $container to be ready (attempt $((attempt + 1))/$max_attempts)..."
            fi
            
            sleep $check_interval
            attempt=$((attempt + 1))
        done
        
        if [ $attempt -eq $max_attempts ]; then
            log "ERROR" "$container failed to become healthy within $((max_attempts * check_interval)) seconds"
            return 1
        fi
    done
    
    log "SUCCESS" "All Cassandra containers are healthy and ready"
}

# Function to generate comprehensive test datasets
generate_test_datasets() {
    log "INFO" "Generating comprehensive test datasets..."
    
    mkdir -p "$RESULTS_DIR/test-data"
    
    # Generate datasets for Cassandra 5.0
    log "INFO" "Generating Cassandra 5.0 test datasets..."
    docker exec "$CASSANDRA_5_CONTAINER" cqlsh -e "
        -- Create test keyspace
        CREATE KEYSPACE IF NOT EXISTS cqlite_validation 
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
        
        USE cqlite_validation;
        
        -- Basic types table
        CREATE TABLE IF NOT EXISTS basic_types (
            id UUID PRIMARY KEY,
            text_col TEXT,
            int_col INT,
            bigint_col BIGINT,
            float_col FLOAT,
            double_col DOUBLE,
            boolean_col BOOLEAN,
            timestamp_col TIMESTAMP,
            uuid_col UUID,
            created_at TIMESTAMP
        );
        
        -- Collections table
        CREATE TABLE IF NOT EXISTS collections_table (
            id UUID PRIMARY KEY,
            list_col LIST<TEXT>,
            set_col SET<INT>,
            map_col MAP<TEXT, INT>,
            frozen_list FROZEN<LIST<TEXT>>,
            created_at TIMESTAMP
        );
        
        -- Complex partitioning table
        CREATE TABLE IF NOT EXISTS complex_partitioning (
            partition_key TEXT,
            clustering_key TIMEUUID,
            data_value TEXT,
            metadata MAP<TEXT, TEXT>,
            created_at TIMESTAMP,
            PRIMARY KEY (partition_key, clustering_key)
        ) WITH CLUSTERING ORDER BY (clustering_key DESC);
        
        -- Counter table
        CREATE TABLE IF NOT EXISTS counters (
            id UUID PRIMARY KEY,
            counter_value COUNTER
        );
        
        -- Time series table (wide rows)
        CREATE TABLE IF NOT EXISTS time_series (
            sensor_id UUID,
            timestamp TIMESTAMP,
            temperature DOUBLE,
            humidity DOUBLE,
            metadata MAP<TEXT, TEXT>,
            PRIMARY KEY (sensor_id, timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC);
    " || {
        log "ERROR" "Failed to create test schemas in Cassandra 5.0"
        return 1
    }
    
    # Insert test data
    log "INFO" "Inserting comprehensive test data..."
    docker exec "$CASSANDRA_5_CONTAINER" cqlsh -e "
        USE cqlite_validation;
        
        -- Insert basic types data
        INSERT INTO basic_types (id, text_col, int_col, bigint_col, float_col, double_col, boolean_col, timestamp_col, uuid_col, created_at)
        VALUES (uuid(), 'test_string_1', 42, 9223372036854775807, 3.14, 2.71828, true, toTimestamp(now()), uuid(), toTimestamp(now()));
        
        INSERT INTO basic_types (id, text_col, int_col, bigint_col, float_col, double_col, boolean_col, timestamp_col, uuid_col, created_at)
        VALUES (uuid(), 'test_string_2', -42, -9223372036854775808, -3.14, -2.71828, false, toTimestamp(now()), uuid(), toTimestamp(now()));
        
        -- Insert collections data
        INSERT INTO collections_table (id, list_col, set_col, map_col, frozen_list, created_at)
        VALUES (uuid(), ['item1', 'item2', 'item3'], {1, 2, 3, 4, 5}, {'key1': 100, 'key2': 200}, ['frozen1', 'frozen2'], toTimestamp(now()));
        
        INSERT INTO collections_table (id, list_col, set_col, map_col, frozen_list, created_at)
        VALUES (uuid(), ['empty_test'], {}, {'single': 1}, ['single_frozen'], toTimestamp(now()));
        
        -- Insert complex partitioning data
        INSERT INTO complex_partitioning (partition_key, clustering_key, data_value, metadata, created_at)
        VALUES ('partition_1', now(), 'data_1', {'version': '1.0', 'type': 'test'}, toTimestamp(now()));
        
        INSERT INTO complex_partitioning (partition_key, clustering_key, data_value, metadata, created_at)
        VALUES ('partition_1', now(), 'data_2', {'version': '1.1', 'type': 'test'}, toTimestamp(now()));
        
        -- Insert counter data
        UPDATE counters SET counter_value = counter_value + 1 WHERE id = uuid();
        UPDATE counters SET counter_value = counter_value + 10 WHERE id = uuid();
        
        -- Insert time series data (wide rows)
        INSERT INTO time_series (sensor_id, timestamp, temperature, humidity, metadata)
        VALUES (uuid(), toTimestamp(now()), 23.5, 65.2, {'location': 'room1', 'floor': '1'});
        
        INSERT INTO time_series (sensor_id, timestamp, temperature, humidity, metadata)
        VALUES (uuid(), toTimestamp(now()), 24.1, 62.8, {'location': 'room2', 'floor': '2'});
        
        INSERT INTO time_series (sensor_id, timestamp, temperature, humidity, metadata)
        VALUES (uuid(), toTimestamp(now()), 22.9, 68.5, {'location': 'room3', 'floor': '1'});
    " || {
        log "ERROR" "Failed to insert test data"
        return 1
    }
    
    # Force flush to ensure data is written to SSTables
    log "INFO" "Flushing data to SSTables..."
    docker exec "$CASSANDRA_5_CONTAINER" nodetool flush cqlite_validation || {
        log "ERROR" "Failed to flush data to SSTables"
        return 1
    }
    
    log "SUCCESS" "Test datasets generated and flushed successfully"
}

# Function to extract SSTables for validation
extract_sstables() {
    log "INFO" "Extracting SSTables for validation..."
    
    local sstable_dir="$RESULTS_DIR/sstables"
    mkdir -p "$sstable_dir"
    
    # Find and extract SSTable files from Cassandra 5.0
    local tables=("basic_types" "collections_table" "complex_partitioning" "counters" "time_series")
    
    for table in "${tables[@]}"; do
        log "INFO" "Extracting SSTables for table: $table"
        
        # Find the SSTable directory
        local table_uuid=$(docker exec "$CASSANDRA_5_CONTAINER" find /var/lib/cassandra/data/cqlite_validation -name "${table}-*" -type d | head -1)
        
        if [ -z "$table_uuid" ]; then
            log "WARN" "No SSTable directory found for table $table"
            continue
        fi
        
        log "DEBUG" "Found SSTable directory: $table_uuid"
        
        # Find all SSTable files
        local sstable_files=$(docker exec "$CASSANDRA_5_CONTAINER" find "$table_uuid" -name "*-Data.db")
        
        if [ -z "$sstable_files" ]; then
            log "WARN" "No Data.db files found for table $table"
            continue
        fi
        
        # Copy each SSTable file
        local count=0
        while IFS= read -r sstable_file; do
            if [ -n "$sstable_file" ]; then
                local basename=$(basename "$sstable_file")
                local local_file="$sstable_dir/${table}_${count}_${basename}"
                
                docker cp "$CASSANDRA_5_CONTAINER:$sstable_file" "$local_file" || {
                    log "WARN" "Failed to copy $sstable_file"
                    continue
                }
                
                log "SUCCESS" "Extracted: $local_file"
                count=$((count + 1))
            fi
        done <<< "$sstable_files"
        
        if [ $count -eq 0 ]; then
            log "WARN" "No SSTable files extracted for table $table"
        else
            log "SUCCESS" "Extracted $count SSTable files for table $table"
        fi
    done
    
    # List all extracted files
    local total_files=$(find "$sstable_dir" -name "*.db" | wc -l)
    log "SUCCESS" "Total SSTable files extracted: $total_files"
    
    if [ "$total_files" -eq 0 ]; then
        log "ERROR" "No SSTable files were extracted. Cannot proceed with validation."
        return 1
    fi
    
    # Save extraction summary
    find "$sstable_dir" -name "*.db" -ls > "$RESULTS_DIR/extracted_sstables.txt"
}

# Function to run zero-tolerance validation on all SSTables
run_zero_tolerance_validation() {
    log "INFO" "Running zero-tolerance validation on extracted SSTables..."
    
    local sstable_dir="$RESULTS_DIR/sstables"
    local validation_dir="$RESULTS_DIR/validation-results"
    mkdir -p "$validation_dir"
    
    # Find all SSTable files
    local sstable_files=($(find "$sstable_dir" -name "*-Data.db"))
    local total_files=${#sstable_files[@]}
    
    if [ $total_files -eq 0 ]; then
        log "ERROR" "No SSTable files found for validation"
        return 1
    fi
    
    log "INFO" "Found $total_files SSTable files for validation"
    
    local passed_count=0
    local failed_count=0
    local validation_results=()
    
    # Validate each SSTable file
    for i in "${!sstable_files[@]}"; do
        local sstable_file="${sstable_files[i]}"
        local file_number=$((i + 1))
        local basename=$(basename "$sstable_file")
        local result_dir="$validation_dir/$basename"
        
        mkdir -p "$result_dir"
        
        log "INFO" "[$file_number/$total_files] Validating: $basename"
        
        local start_time=$(date +%s.%N)
        
        # Build validator command
        local cmd="$VALIDATOR_DIR/target/release/sstabledump-validator validate \"$sstable_file\""
        
        if [ "$ZERO_TOLERANCE" = "true" ]; then
            cmd="$cmd --fail-on-diff"
        fi
        
        if [ "$VERBOSE" = "true" ]; then
            cmd="$cmd --detailed"
        fi
        
        log "DEBUG" "Running: $cmd"
        
        # Execute validation and capture results
        if eval "$cmd" > "$result_dir/validation.log" 2>&1; then
            local end_time=$(date +%s.%N)
            local duration=$(echo "$end_time - $start_time" | bc -l 2>/dev/null || echo "0")
            
            echo "PASSED" > "$result_dir/status.txt"
            echo "$duration" > "$result_dir/duration.txt"
            
            log "SUCCESS" "✓ Validation passed: $basename (${duration}s)"
            passed_count=$((passed_count + 1))
            validation_results+=("PASS:$basename")
        else
            local end_time=$(date +%s.%N)
            local duration=$(echo "$end_time - $start_time" | bc -l 2>/dev/null || echo "0")
            
            echo "FAILED" > "$result_dir/status.txt"
            echo "$duration" > "$result_dir/duration.txt"
            
            log "ERROR" "✗ Validation failed: $basename (${duration}s)"
            failed_count=$((failed_count + 1))
            validation_results+=("FAIL:$basename")
            
            # In zero-tolerance mode, we continue but track failures
            if [ "$ZERO_TOLERANCE" = "true" ]; then
                log "WARN" "Zero-tolerance mode: continuing with remaining files"
            fi
        fi
        
        # Generate JUnit XML for CI integration
        generate_junit_xml "$basename" "$result_dir" "$duration"
    done
    
    # Generate comprehensive validation summary
    generate_validation_summary "$total_files" "$passed_count" "$failed_count" "${validation_results[@]}"
    
    # Return failure if any validation failed in zero-tolerance mode
    if [ "$ZERO_TOLERANCE" = "true" ] && [ "$failed_count" -gt 0 ]; then
        log "ERROR" "Zero-tolerance validation failed: $failed_count/$total_files files failed"
        return 1
    fi
    
    log "SUCCESS" "Validation completed: $passed_count/$total_files passed"
    return 0
}

# Function to generate JUnit XML for CI
generate_junit_xml() {
    local test_name="$1"
    local result_dir="$2"
    local duration="$3"
    
    local status=$(cat "$result_dir/status.txt")
    local junit_file="$result_dir/junit.xml"
    local test_case_name="sstable_validation_$(echo "$test_name" | sed 's/[^a-zA-Z0-9]/_/g')"
    
    cat > "$junit_file" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="SSTable Validation" tests="1" failures="$([ "$status" = "FAILED" ] && echo "1" || echo "0")" errors="0" time="$duration" timestamp="$(date -Iseconds)">
  <testcase name="$test_case_name" classname="CQLiteValidator" time="$duration">
EOF
    
    if [ "$status" = "FAILED" ]; then
        cat >> "$junit_file" << EOF
    <failure message="Zero-tolerance validation failed for $test_name">
      <![CDATA[$(cat "$result_dir/validation.log" 2>/dev/null | tail -50)]]>
    </failure>
EOF
    fi
    
    cat >> "$junit_file" << EOF
  </testcase>
</testsuite>
EOF
}

# Function to generate comprehensive validation summary
generate_validation_summary() {
    local total="$1"
    local passed="$2"
    local failed="$3"
    shift 3
    local validation_results=("$@")
    
    local summary_file="$RESULTS_DIR/validation-summary.md"
    local success_rate=$(echo "scale=2; $passed * 100 / $total" | bc -l 2>/dev/null || echo "0.00")
    
    cat > "$summary_file" << EOF
# CQLite Docker Validator Results - Issue #30

**P0 M1 Blocker Status: $([ "$failed" -eq 0 ] && echo "✅ PASSED" || echo "❌ FAILED")**

## Execution Summary

- **Timestamp**: $(date -Iseconds)
- **Total SSTable Files**: $total
- **Passed**: $passed
- **Failed**: $failed
- **Success Rate**: ${success_rate}%
- **Zero Tolerance Mode**: $ZERO_TOLERANCE
- **CI Mode**: $CI_MODE

## Environment Information

- **Docker Version**: $(docker --version)
- **Docker Compose Version**: $(docker-compose --version)
- **Rust Version**: $(rustc --version 2>/dev/null || echo "Not available")
- **CQLite Commit**: $(cd "$PROJECT_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo "Unknown")
- **Branch**: $(cd "$PROJECT_ROOT" && git branch --show-current 2>/dev/null || echo "Unknown")

## Container Status

- **Cassandra 5.0**: $(docker ps --filter "name=$CASSANDRA_5_CONTAINER" --format "{{.Status}}" 2>/dev/null || echo "Not running")
- **Cassandra 4.1**: $(docker ps --filter "name=$CASSANDRA_4_CONTAINER" --format "{{.Status}}" 2>/dev/null || echo "Not running")

## Validation Results

EOF
    
    for result in "${validation_results[@]}"; do
        local status="${result%%:*}"
        local filename="${result##*:}"
        
        if [ "$status" = "PASS" ]; then
            echo "- ✅ $filename" >> "$summary_file"
        else
            echo "- ❌ $filename" >> "$summary_file"
        fi
    done
    
    cat >> "$summary_file" << EOF

## Test Data Generated

- **Keyspace**: cqlite_validation
- **Tables**: basic_types, collections_table, complex_partitioning, counters, time_series
- **Data Types**: All major Cassandra types including collections, counters, time series
- **SSTable Format**: Cassandra 5.0 OA format

## Artifacts

- **Validation Results**: \`$RESULTS_DIR/validation-results/\`
- **JUnit XML**: \`$RESULTS_DIR/validation-results/*/junit.xml\`
- **SSTable Files**: \`$RESULTS_DIR/sstables/\`
- **Extraction Log**: \`$RESULTS_DIR/extracted_sstables.txt\`
- **Docker Logs**: \`$RESULTS_DIR/docker-logs/\`

## Commands Used

### Start Infrastructure
\`\`\`bash
cd $DOCKER_DIR
docker-compose -f docker-compose-cassandra5.yml up -d cassandra-5-0
docker-compose -f docker-compose-multi-version.yml up -d cassandra-4-1
\`\`\`

### Run Validation
\`\`\`bash
$VALIDATOR_DIR/target/release/sstabledump-validator validate <sstable> \\
    $([ "$ZERO_TOLERANCE" = "true" ] && echo "--fail-on-diff") \\
    $([ "$VERBOSE" = "true" ] && echo "--detailed")
\`\`\`

## CI Integration Ready

This validation run is designed for Issue #38 CI integration:

- JUnit XML reports for test result integration
- Comprehensive artifact collection
- Zero-tolerance mode for quality gates
- Proper exit codes for CI pipeline control

---

**Generated by**: CQLite Docker Validator Orchestrator v1.0  
**Issue**: #30 - Validator on Docker infrastructure against real SSTables  
**Milestone**: M1 P0 Blocker  
EOF
    
    log "INFO" "Validation summary generated: $summary_file"
    
    # Also create a simple status file for CI
    echo "$([ "$failed" -eq 0 ] && echo "PASS" || echo "FAIL")" > "$RESULTS_DIR/overall-status.txt"
    echo "$success_rate" > "$RESULTS_DIR/success-rate.txt"
}

# Function to collect Docker logs for debugging
collect_docker_logs() {
    log "INFO" "Collecting Docker logs for debugging..."
    
    local logs_dir="$RESULTS_DIR/docker-logs"
    mkdir -p "$logs_dir"
    
    # Collect logs from all Cassandra containers
    for container in "$CASSANDRA_5_CONTAINER" "$CASSANDRA_4_CONTAINER"; do
        if docker ps -a --filter "name=$container" --format "{{.Names}}" | grep -q "$container"; then
            log "DEBUG" "Collecting logs from $container"
            docker logs "$container" > "$logs_dir/${container}.log" 2>&1 || true
        fi
    done
    
    # Collect compose logs
    cd "$DOCKER_DIR"
    for compose_file in docker-compose*.yml; do
        if [ -f "$compose_file" ]; then
            local base_name=$(basename "$compose_file" .yml)
            docker-compose -f "$compose_file" logs > "$logs_dir/${base_name}.log" 2>&1 || true
        fi
    done
    
    log "SUCCESS" "Docker logs collected in $logs_dir"
}

# Function to archive results for CI and future analysis
archive_results() {
    if [ "$ARCHIVE_RESULTS" = "true" ]; then
        log "INFO" "Archiving validation results..."
        
        local archive_file="$RESULTS_BASE_DIR/validation-archive-$TIMESTAMP.tar.gz"
        
        cd "$RESULTS_BASE_DIR"
        tar -czf "$archive_file" "run-$TIMESTAMP/" || {
            log "WARN" "Failed to create archive"
            return 1
        }
        
        log "SUCCESS" "Results archived to: $archive_file"
        
        # Create a symlink to latest results
        ln -sf "run-$TIMESTAMP" "$RESULTS_BASE_DIR/latest" || true
        
        # Clean up old archives (keep last 10)
        ls -t "$RESULTS_BASE_DIR"/validation-archive-*.tar.gz 2>/dev/null | tail -n +11 | xargs rm -f || true
    fi
}

# Function to cleanup containers (optional)
cleanup_containers() {
    if [ "$CI_MODE" = "true" ]; then
        log "INFO" "CI mode: cleaning up containers..."
        cleanup_existing_containers
    else
        log "INFO" "Local mode: leaving containers running for debugging"
        log "INFO" "To cleanup manually, run: cd $DOCKER_DIR && docker-compose -f docker-compose-cassandra5.yml down"
    fi
}

# Function to display final summary
display_final_summary() {
    local overall_status=$(cat "$RESULTS_DIR/overall-status.txt" 2>/dev/null || echo "UNKNOWN")
    local success_rate=$(cat "$RESULTS_DIR/success-rate.txt" 2>/dev/null || echo "0.00")
    
    echo ""
    echo -e "${WHITE}════════════════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${WHITE}                             VALIDATION COMPLETE                                 ${NC}"
    echo -e "${WHITE}════════════════════════════════════════════════════════════════════════════════${NC}"
    
    if [ "$overall_status" = "PASS" ]; then
        echo -e "${GREEN}🎉 ALL VALIDATIONS PASSED${NC}"
        echo -e "${GREEN}✅ Zero-tolerance validation succeeded${NC}"
        echo -e "${GREEN}✅ Ready for M1 release${NC}"
    else
        echo -e "${RED}❌ VALIDATION FAILURES DETECTED${NC}"
        echo -e "${RED}⚠️  Zero-tolerance mode violations${NC}"
        echo -e "${RED}⚠️  M1 blocker status: FAILED${NC}"
    fi
    
    echo -e "${BLUE}📊 Success Rate: ${success_rate}%${NC}"
    echo -e "${BLUE}📁 Results: $RESULTS_DIR${NC}"
    echo -e "${BLUE}📋 Summary: $RESULTS_DIR/validation-summary.md${NC}"
    echo -e "${BLUE}🔬 JUnit XML: $RESULTS_DIR/validation-results/*/junit.xml${NC}"
    
    if [ "$ARCHIVE_RESULTS" = "true" ]; then
        echo -e "${BLUE}📦 Archive: $RESULTS_BASE_DIR/validation-archive-$TIMESTAMP.tar.gz${NC}"
    fi
    
    echo ""
    echo -e "${PURPLE}Issue #30 Implementation Status: COMPLETE${NC}"
    echo -e "${PURPLE}Ready for Issue #38 CI Integration${NC}"
    echo ""
}

# Main orchestration function
main() {
    # Create results directory
    mkdir -p "$RESULTS_DIR"
    
    # Start logging
    log "INFO" "Starting CQLite Docker Validator Orchestration"
    log "INFO" "Results will be saved to: $RESULTS_DIR"
    
    # Setup error handling
    set -E
    trap 'log "ERROR" "Script failed at line $LINENO"' ERR
    trap cleanup_containers EXIT
    
    # Execute validation pipeline
    check_prerequisites
    cleanup_existing_containers
    start_cassandra_infrastructure
    wait_for_cassandra_health
    generate_test_datasets
    extract_sstables
    
    # Run validation and capture result
    local validation_result=0
    run_zero_tolerance_validation || validation_result=$?
    
    # Always collect artifacts
    collect_docker_logs
    archive_results
    display_final_summary
    
    # Exit with appropriate code
    if [ $validation_result -ne 0 ]; then
        log "ERROR" "Validation failed. Check results for details."
        exit 1
    else
        log "SUCCESS" "All validations passed successfully!"
        exit 0
    fi
}

# Execute main function
main "$@"