#!/bin/bash

# CQLite CI Docker Validation - Issue #30/#38
# Production-ready CI/CD pipeline for zero-tolerance validation
# Designed for GitHub Actions and automated testing

set -euo pipefail

# CI-specific configuration
CI_MODE="${CI_MODE:-true}"
GITHUB_ACTIONS="${GITHUB_ACTIONS:-false}"
STRICT_MODE="${STRICT_MODE:-true}"
MAX_PARALLEL_JOBS="${MAX_PARALLEL_JOBS:-4}"
CI_TIMEOUT="${CI_TIMEOUT:-1800}"  # 30 minutes
FAIL_FAST="${FAIL_FAST:-true}"

# Color codes (disabled in CI by default)
if [ "$CI_MODE" = "true" ]; then
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    NC=''
else
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m'
fi

# Script paths
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/../.." && pwd )"
DOCKER_DIR="$PROJECT_ROOT/test-data/docker"
VALIDATOR_DIR="$PROJECT_ROOT/tools/sstabledump-validator"

# CI results directory
CI_RESULTS_DIR="${CI_RESULTS_DIR:-$PROJECT_ROOT/ci-validation-results}"
JUNIT_DIR="$CI_RESULTS_DIR/junit"
ARTIFACTS_DIR="$CI_RESULTS_DIR/artifacts"
LOGS_DIR="$CI_RESULTS_DIR/logs"

echo "=== CQLite CI Docker Validation ==="
echo "Issue #30: Docker infrastructure validation"
echo "Issue #38: CI/CD integration"
echo "Mode: $([ "$CI_MODE" = "true" ] && echo "CI/CD" || echo "Local")"
echo "Time: $(date -Iseconds)"
echo ""

# Logging function with structured output
ci_log() {
    local level="$1"
    local message="$2"
    local timestamp=$(date -Iseconds)
    
    if [ "$GITHUB_ACTIONS" = "true" ]; then
        case "$level" in
            "ERROR")
                echo "::error::$message"
                ;;
            "WARNING")
                echo "::warning::$message"
                ;;
            "NOTICE")
                echo "::notice::$message"
                ;;
            *)
                echo "[$timestamp] [$level] $message"
                ;;
        esac
    else
        echo "[$timestamp] [$level] $message"
    fi
}

# Function to set up CI environment
setup_ci_environment() {
    ci_log "INFO" "Setting up CI environment..."
    
    # Create necessary directories
    mkdir -p "$CI_RESULTS_DIR" "$JUNIT_DIR" "$ARTIFACTS_DIR" "$LOGS_DIR"
    
    # Set environment variables for the run
    export DOCKER_BUILDKIT=1
    export COMPOSE_DOCKER_CLI_BUILD=1
    export BUILDKIT_PROGRESS=plain
    
    # Configure Docker for CI
    if [ "$CI_MODE" = "true" ]; then
        # Optimize Docker settings for CI
        echo '{"experimental": true, "features": {"buildkit": true}}' | sudo tee /etc/docker/daemon.json >/dev/null || true
        sudo systemctl reload docker 2>/dev/null || true
    fi
    
    ci_log "INFO" "CI environment setup complete"
}

# Function to validate CI prerequisites with strict checks
validate_ci_prerequisites() {
    ci_log "INFO" "Validating CI prerequisites..."
    
    local validation_failed=false
    
    # Essential tools check
    local required_tools=("docker" "docker-compose" "cargo" "rustc")
    for tool in "${required_tools[@]}"; do
        if ! command -v "$tool" &>/dev/null; then
            ci_log "ERROR" "Required tool not found: $tool"
            validation_failed=true
        else
            ci_log "DEBUG" "$tool: $(command -v "$tool")"
        fi
    done
    
    # Docker daemon check
    if ! docker info >/dev/null 2>&1; then
        ci_log "ERROR" "Docker daemon is not accessible"
        validation_failed=true
    fi
    
    # Disk space check (require at least 10GB free)
    local available_space=$(df "$PROJECT_ROOT" | awk 'NR==2 {print $4}')
    local required_space=10485760  # 10GB in KB
    if [ "$available_space" -lt "$required_space" ]; then
        ci_log "ERROR" "Insufficient disk space. Required: 10GB, Available: $((available_space / 1024 / 1024))GB"
        validation_failed=true
    fi
    
    # Memory check (require at least 4GB available)
    if command -v free >/dev/null; then
        local available_memory=$(free -m | awk 'NR==2{print $7}')
        if [ "$available_memory" -lt 4096 ]; then
            ci_log "WARNING" "Low available memory: ${available_memory}MB (recommended: 4GB+)"
        fi
    fi
    
    # Validator build check
    if [ ! -f "$VALIDATOR_DIR/target/release/sstabledump-validator" ]; then
        ci_log "INFO" "Building validator for CI..."
        cd "$VALIDATOR_DIR"
        if ! cargo build --release --locked; then
            ci_log "ERROR" "Failed to build validator"
            validation_failed=true
        fi
        cd "$PROJECT_ROOT"
    fi
    
    if [ "$validation_failed" = "true" ]; then
        ci_log "ERROR" "CI prerequisites validation failed"
        exit 1
    fi
    
    ci_log "INFO" "All CI prerequisites validated successfully"
}

# Function to start Docker infrastructure with health monitoring
start_ci_docker_infrastructure() {
    ci_log "INFO" "Starting Docker infrastructure for CI..."
    
    cd "$DOCKER_DIR"
    
    # Clean any existing containers
    docker-compose -f docker-compose-cassandra5.yml down --remove-orphans >/dev/null 2>&1 || true
    
    # Start Cassandra 5.0 with health monitoring
    ci_log "INFO" "Starting Cassandra 5.0 container..."
    
    if ! timeout $CI_TIMEOUT docker-compose -f docker-compose-cassandra5.yml up -d cassandra-5-0; then
        ci_log "ERROR" "Failed to start Cassandra 5.0 container"
        collect_ci_logs "startup_failure"
        exit 1
    fi
    
    # Wait for health with detailed monitoring
    ci_log "INFO" "Waiting for Cassandra to become healthy..."
    local max_attempts=$((CI_TIMEOUT / 10))
    local attempt=0
    
    while [ $attempt -lt $max_attempts ]; do
        if docker exec cqlite-cassandra-5-0 cqlsh -e "SELECT cluster_name FROM system.local;" &>/dev/null; then
            ci_log "INFO" "Cassandra is healthy and ready"
            break
        fi
        
        # Log progress every 30 seconds
        if [ $((attempt % 3)) -eq 0 ]; then
            ci_log "INFO" "Waiting for Cassandra health check (attempt $((attempt + 1))/$max_attempts)..."
        fi
        
        sleep 10
        attempt=$((attempt + 1))
    done
    
    if [ $attempt -eq $max_attempts ]; then
        ci_log "ERROR" "Cassandra failed to become healthy within $CI_TIMEOUT seconds"
        collect_ci_logs "health_check_failure"
        exit 1
    fi
    
    ci_log "INFO" "Docker infrastructure started successfully"
}

# Function to generate comprehensive CI test data
generate_ci_test_data() {
    ci_log "INFO" "Generating comprehensive test data for CI validation..."
    
    # Create comprehensive test schema
    docker exec cqlite-cassandra-5-0 cqlsh -e "
        CREATE KEYSPACE IF NOT EXISTS ci_validation 
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
        
        USE ci_validation;
        
        -- Basic types comprehensive test
        CREATE TABLE IF NOT EXISTS basic_types_comprehensive (
            id UUID PRIMARY KEY,
            ascii_col ASCII,
            bigint_col BIGINT,
            blob_col BLOB,
            boolean_col BOOLEAN,
            counter_col COUNTER,
            date_col DATE,
            decimal_col DECIMAL,
            double_col DOUBLE,
            duration_col DURATION,
            float_col FLOAT,
            inet_col INET,
            int_col INT,
            smallint_col SMALLINT,
            text_col TEXT,
            time_col TIME,
            timestamp_col TIMESTAMP,
            timeuuid_col TIMEUUID,
            tinyint_col TINYINT,
            uuid_col UUID,
            varchar_col VARCHAR,
            varint_col VARINT
        );
        
        -- Collections comprehensive test
        CREATE TABLE IF NOT EXISTS collections_comprehensive (
            id UUID PRIMARY KEY,
            list_text LIST<TEXT>,
            list_int LIST<INT>,
            set_text SET<TEXT>,
            set_int SET<INT>,
            map_text_int MAP<TEXT, INT>,
            map_int_text MAP<INT, TEXT>,
            map_uuid_timestamp MAP<UUID, TIMESTAMP>,
            frozen_list FROZEN<LIST<TEXT>>,
            frozen_set FROZEN<SET<INT>>,
            frozen_map FROZEN<MAP<TEXT, INT>>
        );
        
        -- Complex partitioning and clustering
        CREATE TABLE IF NOT EXISTS complex_keys (
            partition_key_1 TEXT,
            partition_key_2 INT,
            clustering_key_1 TIMESTAMP,
            clustering_key_2 UUID,
            data_col TEXT,
            metadata MAP<TEXT, TEXT>,
            PRIMARY KEY ((partition_key_1, partition_key_2), clustering_key_1, clustering_key_2)
        ) WITH CLUSTERING ORDER BY (clustering_key_1 DESC, clustering_key_2 ASC);
        
        -- Edge cases table
        CREATE TABLE IF NOT EXISTS edge_cases (
            id UUID PRIMARY KEY,
            null_col TEXT,
            empty_string TEXT,
            very_long_text TEXT,
            special_chars TEXT,
            unicode_text TEXT,
            max_values BIGINT,
            min_values BIGINT,
            zero_values INT
        );
    " || {
        ci_log "ERROR" "Failed to create CI test schemas"
        exit 1
    }
    
    # Insert comprehensive test data
    ci_log "INFO" "Inserting comprehensive test data..."
    
    docker exec cqlite-cassandra-5-0 cqlsh -e "
        USE ci_validation;
        
        -- Basic types data with edge cases
        INSERT INTO basic_types_comprehensive (
            id, ascii_col, bigint_col, blob_col, boolean_col, 
            date_col, decimal_col, double_col, float_col, inet_col,
            int_col, smallint_col, text_col, time_col, timestamp_col,
            timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col
        ) VALUES (
            uuid(), 'ASCII text', 9223372036854775807, textAsBlob('binary data'), true,
            '2024-01-15', 123.456, 2.71828, 3.14159, '192.168.1.1',
            2147483647, 32767, 'Unicode: 🚀 测试 émojis', '14:30:22.123', toTimestamp(now()),
            now(), 127, uuid(), 'VARCHAR data', 999999999999999999999
        );
        
        INSERT INTO basic_types_comprehensive (
            id, ascii_col, bigint_col, blob_col, boolean_col, 
            date_col, decimal_col, double_col, float_col, inet_col,
            int_col, smallint_col, text_col, time_col, timestamp_col,
            timeuuid_col, tinyint_col, uuid_col, varchar_col, varint_col
        ) VALUES (
            uuid(), 'Edge case', -9223372036854775808, textAsBlob(''), false,
            '1970-01-01', -123.456, -2.71828, -3.14159, '::1',
            -2147483648, -32768, '', '00:00:00.000', '1970-01-01 00:00:00+0000',
            now(), -128, uuid(), '', -999999999999999999999
        );
        
        -- Collections data with various sizes
        INSERT INTO collections_comprehensive (
            id, list_text, list_int, set_text, set_int,
            map_text_int, map_int_text, map_uuid_timestamp,
            frozen_list, frozen_set, frozen_map
        ) VALUES (
            uuid(), 
            ['item1', 'item2', 'item3', 'very_long_item_with_special_chars_🚀'],
            [1, 2, 3, 2147483647, -2147483648, 0],
            {'set1', 'set2', 'set3', 'unicode_set_🎯'},
            {1, 2, 3, 100, 1000, 10000},
            {'key1': 100, 'key2': 200, 'unicode_key_🔑': 999},
            {1: 'one', 2: 'two', 999: 'large_number'},
            {uuid(): toTimestamp(now()), uuid(): '1970-01-01 00:00:00+0000'},
            ['frozen1', 'frozen2'],
            {10, 20, 30},
            {'frozen_key': 42}
        );
        
        -- Empty collections edge case
        INSERT INTO collections_comprehensive (
            id, list_text, list_int, set_text, set_int,
            map_text_int, map_int_text, map_uuid_timestamp,
            frozen_list, frozen_set, frozen_map
        ) VALUES (
            uuid(), [], [], {}, {}, {}, {}, {}, [], {}, {}
        );
        
        -- Complex partitioning data
        INSERT INTO complex_keys (
            partition_key_1, partition_key_2, clustering_key_1, clustering_key_2,
            data_col, metadata
        ) VALUES (
            'partition_1', 100, toTimestamp(now()), uuid(),
            'Complex key data 1', {'version': '1.0', 'type': 'test'}
        );
        
        INSERT INTO complex_keys (
            partition_key_1, partition_key_2, clustering_key_1, clustering_key_2,
            data_col, metadata
        ) VALUES (
            'partition_1', 100, toTimestamp(now()), uuid(),
            'Complex key data 2', {'version': '1.1', 'type': 'test'}
        );
        
        -- Edge cases data
        INSERT INTO edge_cases (
            id, null_col, empty_string, very_long_text, special_chars, unicode_text,
            max_values, min_values, zero_values
        ) VALUES (
            uuid(), null, '', 
            '$(printf 'A%.0s' {1..1000})',  -- 1000 character string
            '!@#$%^&*()[]{}|;:,.<>?/~\`\"''\\',
            '🚀🎯🔑💾🌟⚡🎉📊🔬🎨🎵🎮🎪🎭🎨🎯🎲🎰🎳🏆🏅🏆',
            9223372036854775807, -9223372036854775808, 0
        );
    " || {
        ci_log "ERROR" "Failed to insert CI test data"
        exit 1
    }
    
    # Force flush all data
    ci_log "INFO" "Flushing all test data to SSTables..."
    docker exec cqlite-cassandra-5-0 nodetool flush ci_validation || {
        ci_log "ERROR" "Failed to flush CI test data"
        exit 1
    }
    
    ci_log "INFO" "Comprehensive CI test data generated successfully"
}

# Function to extract SSTables for CI validation
extract_ci_sstables() {
    ci_log "INFO" "Extracting SSTables for CI validation..."
    
    local sstables_dir="$ARTIFACTS_DIR/sstables"
    mkdir -p "$sstables_dir"
    
    local tables=("basic_types_comprehensive" "collections_comprehensive" "complex_keys" "edge_cases")
    local extracted_count=0
    
    for table in "${tables[@]}"; do
        ci_log "INFO" "Extracting SSTables for table: $table"
        
        # Find table directory
        local table_dir=$(docker exec cqlite-cassandra-5-0 find /var/lib/cassandra/data/ci_validation -name "${table}-*" -type d | head -1)
        
        if [ -z "$table_dir" ]; then
            ci_log "WARNING" "No directory found for table $table"
            continue
        fi
        
        # Find all SSTable files
        local sstable_files=$(docker exec cqlite-cassandra-5-0 find "$table_dir" -name "*-Data.db")
        
        if [ -z "$sstable_files" ]; then
            ci_log "WARNING" "No Data.db files found for table $table"
            continue
        fi
        
        # Extract each file
        local file_count=0
        while IFS= read -r sstable_file; do
            if [ -n "$sstable_file" ]; then
                local basename=$(basename "$sstable_file")
                local local_file="$sstables_dir/${table}_${file_count}_${basename}"
                
                if docker cp "cqlite-cassandra-5-0:$sstable_file" "$local_file"; then
                    ci_log "INFO" "Extracted: $local_file"
                    extracted_count=$((extracted_count + 1))
                    file_count=$((file_count + 1))
                fi
            fi
        done <<< "$sstable_files"
    done
    
    if [ $extracted_count -eq 0 ]; then
        ci_log "ERROR" "No SSTable files were extracted"
        exit 1
    fi
    
    ci_log "INFO" "Successfully extracted $extracted_count SSTable files"
    
    # Create manifest
    find "$sstables_dir" -name "*.db" -ls > "$ARTIFACTS_DIR/sstable_manifest.txt"
}

# Function to run CI validation with parallel execution
run_ci_validation() {
    ci_log "INFO" "Starting CI zero-tolerance validation..."
    
    local sstables_dir="$ARTIFACTS_DIR/sstables"
    local results_dir="$ARTIFACTS_DIR/validation_results"
    mkdir -p "$results_dir"
    
    # Find all SSTable files
    local sstable_files=($(find "$sstables_dir" -name "*-Data.db"))
    local total_files=${#sstable_files[@]}
    
    if [ $total_files -eq 0 ]; then
        ci_log "ERROR" "No SSTable files found for validation"
        exit 1
    fi
    
    ci_log "INFO" "Running validation on $total_files SSTable files"
    
    # Prepare parallel validation
    local passed_count=0
    local failed_count=0
    local validation_pids=()
    local validation_results=()
    
    # Function to validate single SSTable (for parallel execution)
    validate_single_sstable() {
        local sstable_file="$1"
        local result_index="$2"
        local basename=$(basename "$sstable_file")
        local result_dir="$results_dir/$basename"
        
        mkdir -p "$result_dir"
        
        local start_time=$(date +%s.%N)
        
        # Run validation
        if timeout 600 "$VALIDATOR_DIR/target/release/sstabledump-validator" validate "$sstable_file" --fail-on-diff --detailed > "$result_dir/validation.log" 2>&1; then
            echo "PASSED" > "$result_dir/status.txt"
            echo "PASS:$basename" > "$result_dir/result.txt"
        else
            echo "FAILED" > "$result_dir/status.txt"
            echo "FAIL:$basename" > "$result_dir/result.txt"
        fi
        
        local end_time=$(date +%s.%N)
        local duration=$(echo "$end_time - $start_time" | bc -l 2>/dev/null || echo "0")
        echo "$duration" > "$result_dir/duration.txt"
        
        # Generate JUnit XML
        generate_ci_junit_xml "$basename" "$result_dir" "$duration"
    }
    
    # Export function for parallel execution
    export -f validate_single_sstable
    export VALIDATOR_DIR results_dir
    
    # Run validations in parallel (with job control)
    local batch_size=$MAX_PARALLEL_JOBS
    local current_batch=0
    
    for i in "${!sstable_files[@]}"; do
        local sstable_file="${sstable_files[i]}"
        
        # Start validation in background
        validate_single_sstable "$sstable_file" "$i" &
        validation_pids+=($!)
        current_batch=$((current_batch + 1))
        
        # Wait for batch completion or if we've reached the last file
        if [ $current_batch -eq $batch_size ] || [ $i -eq $((total_files - 1)) ]; then
            # Wait for current batch to complete
            for pid in "${validation_pids[@]}"; do
                wait $pid
            done
            
            ci_log "INFO" "Completed batch of $current_batch validations"
            validation_pids=()
            current_batch=0
        fi
    done
    
    # Collect results
    for sstable_file in "${sstable_files[@]}"; do
        local basename=$(basename "$sstable_file")
        local result_dir="$results_dir/$basename"
        local status=$(cat "$result_dir/status.txt" 2>/dev/null || echo "UNKNOWN")
        
        if [ "$status" = "PASSED" ]; then
            passed_count=$((passed_count + 1))
            validation_results+=("PASS:$basename")
        else
            failed_count=$((failed_count + 1))
            validation_results+=("FAIL:$basename")
            
            if [ "$FAIL_FAST" = "true" ]; then
                ci_log "ERROR" "FAIL_FAST enabled: Stopping on first failure ($basename)"
                break
            fi
        fi
    done
    
    # Generate comprehensive CI report
    generate_ci_validation_report "$total_files" "$passed_count" "$failed_count" "${validation_results[@]}"
    
    # Determine success/failure
    if [ $failed_count -gt 0 ]; then
        ci_log "ERROR" "CI validation failed: $failed_count/$total_files files failed"
        return 1
    else
        ci_log "INFO" "CI validation passed: $passed_count/$total_files files passed"
        return 0
    fi
}

# Function to generate JUnit XML for CI systems
generate_ci_junit_xml() {
    local test_name="$1"
    local result_dir="$2"
    local duration="$3"
    
    local status=$(cat "$result_dir/status.txt")
    local junit_file="$JUNIT_DIR/${test_name}.xml"
    local test_case_name="sstable_validation_$(echo "$test_name" | sed 's/[^a-zA-Z0-9]/_/g')"
    
    cat > "$junit_file" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="CQLite SSTable Validation" 
           tests="1" 
           failures="$([ "$status" = "FAILED" ] && echo "1" || echo "0")" 
           errors="0" 
           time="$duration" 
           timestamp="$(date -Iseconds)"
           hostname="$(hostname)">
  <properties>
    <property name="issue" value="#30"/>
    <property name="validator" value="sstabledump-validator"/>
    <property name="mode" value="zero-tolerance"/>
    <property name="cassandra_version" value="5.0"/>
  </properties>
  <testcase name="$test_case_name" 
            classname="CQLiteDockerValidator" 
            time="$duration">
EOF
    
    if [ "$status" = "FAILED" ]; then
        cat >> "$junit_file" << EOF
    <failure message="Zero-tolerance validation failed for $test_name" type="ValidationFailure">
      <![CDATA[
SSTable: $test_name
Status: FAILED
Duration: ${duration}s

Last 20 lines of validation log:
$(cat "$result_dir/validation.log" 2>/dev/null | tail -20)
      ]]>
    </failure>
EOF
    fi
    
    cat >> "$junit_file" << EOF
  </testcase>
</testsuite>
EOF
}

# Function to generate comprehensive CI validation report
generate_ci_validation_report() {
    local total="$1"
    local passed="$2"
    local failed="$3"
    shift 3
    local validation_results=("$@")
    
    local success_rate=$(echo "scale=2; $passed * 100 / $total" | bc -l 2>/dev/null || echo "0.00")
    local report_file="$CI_RESULTS_DIR/validation_report.md"
    local status_file="$CI_RESULTS_DIR/validation_status.txt"
    
    # Overall status
    if [ $failed -eq 0 ]; then
        echo "PASSED" > "$status_file"
    else
        echo "FAILED" > "$status_file"
    fi
    
    # Generate detailed report
    cat > "$report_file" << EOF
# CQLite CI Docker Validation Report

**Issue #30**: Docker infrastructure validation  
**Issue #38**: CI/CD integration  
**Status**: $(cat "$status_file")  
**Generated**: $(date -Iseconds)

## Executive Summary

- **Total SSTable Files**: $total
- **Passed**: $passed
- **Failed**: $failed
- **Success Rate**: ${success_rate}%
- **Zero Tolerance Mode**: ✅ Enabled
- **Parallel Execution**: ✅ Enabled ($MAX_PARALLEL_JOBS jobs)

## Environment Information

- **CI Mode**: $CI_MODE
- **GitHub Actions**: $GITHUB_ACTIONS
- **Docker Version**: $(docker --version)
- **Rust Version**: $(rustc --version 2>/dev/null || echo "Not available")
- **Commit**: $(cd "$PROJECT_ROOT" && git rev-parse --short HEAD 2>/dev/null || echo "Unknown")
- **Branch**: $(cd "$PROJECT_ROOT" && git branch --show-current 2>/dev/null || echo "Unknown")

## Test Coverage

| Table | Description | Status |
|-------|-------------|--------|
| basic_types_comprehensive | All Cassandra data types | $(echo "${validation_results[@]}" | grep -q "basic_types_comprehensive" && echo "✅" || echo "❌") |
| collections_comprehensive | Lists, Sets, Maps | $(echo "${validation_results[@]}" | grep -q "collections_comprehensive" && echo "✅" || echo "❌") |
| complex_keys | Multi-part keys | $(echo "${validation_results[@]}" | grep -q "complex_keys" && echo "✅" || echo "❌") |
| edge_cases | Boundary conditions | $(echo "${validation_results[@]}" | grep -q "edge_cases" && echo "✅" || echo "❌") |

## Validation Results

EOF
    
    for result in "${validation_results[@]}"; do
        local status="${result%%:*}"
        local filename="${result##*:}"
        
        if [ "$status" = "PASS" ]; then
            echo "- ✅ $filename" >> "$report_file"
        else
            echo "- ❌ $filename" >> "$report_file"
        fi
    done
    
    cat >> "$report_file" << EOF

## Artifacts

- **JUnit XML Reports**: \`$JUNIT_DIR/*.xml\`
- **SSTable Files**: \`$ARTIFACTS_DIR/sstables/\`
- **Validation Logs**: \`$ARTIFACTS_DIR/validation_results/\`
- **Docker Logs**: \`$LOGS_DIR/\`

## CI Integration

This validation is ready for:
- ✅ GitHub Actions integration
- ✅ Automated PR checks
- ✅ Quality gates
- ✅ Artifact collection
- ✅ Parallel execution

## Quality Gate Status

$(if [ $failed -eq 0 ]; then
    echo "🟢 **PASSED** - All validations successful"
    echo "- Ready for M1 release"
    echo "- Zero-tolerance requirements met"
    echo "- CI pipeline can proceed"
else
    echo "🔴 **FAILED** - Validation failures detected"
    echo "- M1 release blocked"
    echo "- Zero-tolerance violations found"
    echo "- CI pipeline should stop"
fi)

---

**Generated by**: CQLite CI Docker Validator  
**Pipeline ID**: ${GITHUB_RUN_ID:-local}  
**Job ID**: ${GITHUB_JOB:-local}  
EOF
    
    ci_log "INFO" "CI validation report generated: $report_file"
}

# Function to collect comprehensive CI logs
collect_ci_logs() {
    local context="${1:-normal}"
    
    ci_log "INFO" "Collecting CI logs (context: $context)..."
    
    mkdir -p "$LOGS_DIR"
    
    # Collect Docker logs
    if docker ps -a --filter "name=cqlite-cassandra-5-0" --format "{{.Names}}" | grep -q "cqlite-cassandra-5-0"; then
        docker logs cqlite-cassandra-5-0 > "$LOGS_DIR/cassandra-5-0.log" 2>&1 || true
    fi
    
    # Collect Docker Compose logs
    cd "$DOCKER_DIR"
    docker-compose -f docker-compose-cassandra5.yml logs > "$LOGS_DIR/docker-compose.log" 2>&1 || true
    
    # Collect system information
    cat > "$LOGS_DIR/system_info.txt" << EOF
Docker Info:
$(docker info 2>&1)

Docker Version:
$(docker --version)

Docker Compose Version:
$(docker-compose --version)

System Resources:
$(df -h 2>/dev/null || echo "df not available")

Memory Usage:
$(free -h 2>/dev/null || echo "free not available")

Container Status:
$(docker ps -a --filter "name=cqlite")
EOF
    
    ci_log "INFO" "CI logs collected in $LOGS_DIR"
}

# Function to cleanup CI environment
cleanup_ci_environment() {
    ci_log "INFO" "Cleaning up CI environment..."
    
    # Stop and remove containers
    cd "$DOCKER_DIR"
    docker-compose -f docker-compose-cassandra5.yml down --remove-orphans --volumes >/dev/null 2>&1 || true
    
    # Clean up Docker resources (in CI only)
    if [ "$CI_MODE" = "true" ]; then
        docker system prune -f >/dev/null 2>&1 || true
        docker volume prune -f >/dev/null 2>&1 || true
    fi
    
    ci_log "INFO" "CI environment cleanup complete"
}

# Main CI execution pipeline
main() {
    local start_time=$(date +%s)
    
    # Set up error handling
    set -E
    trap 'ci_log "ERROR" "CI pipeline failed at line $LINENO"' ERR
    trap cleanup_ci_environment EXIT
    
    ci_log "INFO" "Starting CQLite CI Docker Validation Pipeline"
    
    # Execute CI pipeline
    setup_ci_environment
    validate_ci_prerequisites
    start_ci_docker_infrastructure
    generate_ci_test_data
    extract_ci_sstables
    
    # Run validation and capture result
    local validation_result=0
    run_ci_validation || validation_result=$?
    
    # Always collect logs and artifacts
    collect_ci_logs "validation_complete"
    
    local end_time=$(date +%s)
    local total_duration=$((end_time - start_time))
    
    # Final status report
    local overall_status=$(cat "$CI_RESULTS_DIR/validation_status.txt" 2>/dev/null || echo "UNKNOWN")
    
    echo ""
    echo "=== CI PIPELINE COMPLETE ==="
    echo "Status: $overall_status"
    echo "Duration: ${total_duration}s"
    echo "Results: $CI_RESULTS_DIR"
    echo ""
    
    if [ "$GITHUB_ACTIONS" = "true" ]; then
        echo "::set-output name=status::$overall_status"
        echo "::set-output name=duration::$total_duration"
        echo "::set-output name=results_dir::$CI_RESULTS_DIR"
    fi
    
    # Exit with appropriate code
    exit $validation_result
}

# Handle script arguments
case "${1:-}" in
    --help|-h)
        echo "CQLite CI Docker Validation"
        echo ""
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --help                Show this help"
        echo "  --local               Run in local mode (non-CI)"
        echo "  --timeout SECONDS     Set CI timeout (default: 1800)"
        echo "  --jobs N              Set parallel jobs (default: 4)"
        echo ""
        echo "Environment Variables:"
        echo "  CI_MODE               Enable CI mode (default: true)"
        echo "  GITHUB_ACTIONS        Enable GitHub Actions mode"
        echo "  STRICT_MODE           Enable strict validation"
        echo "  FAIL_FAST             Stop on first failure"
        echo ""
        exit 0
        ;;
    --local)
        CI_MODE="false"
        GITHUB_ACTIONS="false"
        shift
        main "$@"
        ;;
    --timeout)
        CI_TIMEOUT="$2"
        shift 2
        main "$@"
        ;;
    --jobs)
        MAX_PARALLEL_JOBS="$2"
        shift 2
        main "$@"
        ;;
    *)
        main "$@"
        ;;
esac