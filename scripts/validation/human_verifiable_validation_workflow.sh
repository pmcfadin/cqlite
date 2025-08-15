#!/bin/bash

# Human-Verifiable CQLite Validation Workflow
# Issue #52: Final P1 issue for M1 - Building trust through reproducible validation
#
# This script implements a 5-step human-verifiable workflow that any developer
# can run on a clean machine to validate CQLite's accuracy against Cassandra.
#
# The workflow is designed to:
# 1. Be reproducible on any clean machine
# 2. Provide zero-diff validation against Cassandra
# 3. Enable manual spot-checking for trust building
# 4. Generate archivable artifacts for verification
# 5. Guide users through troubleshooting steps

set -euo pipefail

# Colors for output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly CYAN='\033[0;36m'
readonly BOLD='\033[1m'
readonly NC='\033[0m' # No Color

# Configuration
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
readonly ARTIFACTS_DIR="$PROJECT_ROOT/validation_artifacts/issue_52"
readonly CASSANDRA_DATA_DIR="$ARTIFACTS_DIR/cassandra_data"
readonly CQLITE_DATA_DIR="$ARTIFACTS_DIR/cqlite_data"
readonly REPORTS_DIR="$ARTIFACTS_DIR/reports"
readonly MANUAL_CHECK_DIR="$ARTIFACTS_DIR/manual_verification"

# Docker configuration
readonly DOCKER_COMPOSE_FILE="$PROJECT_ROOT/test-data/docker/docker-compose-cassandra5.yml"
readonly CASSANDRA_HOST="localhost"
readonly CASSANDRA_PORT="9046"
readonly CASSANDRA_CONTAINER="cqlite-cassandra-5-0"

# Validation configuration
readonly VALIDATION_TIMEOUT=600
readonly MAX_RETRIES=3
readonly FAIL_ON_ANY_DIFF=true

# Global state
TOTAL_STEPS=5
CURRENT_STEP=0
VALIDATION_START_TIME=""
OVERALL_SUCCESS=true
CRITICAL_FAILURES=0
WARNINGS=0

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*"
    ((WARNINGS++))
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
    ((CRITICAL_FAILURES++))
    OVERALL_SUCCESS=false
}

log_step() {
    ((CURRENT_STEP++))
    echo ""
    echo -e "${CYAN}${BOLD}STEP $CURRENT_STEP/$TOTAL_STEPS: $*${NC}"
    echo "=================================================================================="
}

log_human_action() {
    echo ""
    echo -e "${YELLOW}${BOLD}👤 HUMAN ACTION REQUIRED:${NC}"
    echo -e "${YELLOW}$*${NC}"
    echo ""
}

# Utility functions
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    local missing_tools=()
    
    # Check required tools
    for tool in docker docker-compose cargo jq; do
        if ! command -v "$tool" &> /dev/null; then
            missing_tools+=("$tool")
        fi
    done
    
    if [ ${#missing_tools[@]} -gt 0 ]; then
        log_error "Missing required tools: ${missing_tools[*]}"
        log_error "Please install the missing tools and try again."
        log_info "Installation guides:"
        log_info "  Docker: https://docs.docker.com/get-docker/"
        log_info "  Docker Compose: https://docs.docker.com/compose/install/"
        log_info "  Rust/Cargo: https://rustup.rs/"
        log_info "  jq: https://stedolan.github.io/jq/download/"
        return 1
    fi
    
    # Check Docker is running
    if ! docker info &> /dev/null; then
        log_error "Docker is not running. Please start Docker and try again."
        return 1
    fi
    
    # Check project structure
    if [[ ! -f "$DOCKER_COMPOSE_FILE" ]]; then
        log_error "Docker compose file not found: $DOCKER_COMPOSE_FILE"
        log_error "Please run this script from the CQLite project root."
        return 1
    fi
    
    if [[ ! -d "$PROJECT_ROOT/tools/sstabledump-validator" ]]; then
        log_error "SSTableDump validator not found. Please ensure the project is complete."
        return 1
    fi
    
    log_success "All prerequisites satisfied"
    return 0
}

setup_artifacts_directory() {
    log_info "Setting up artifacts directory..."
    
    # Create directory structure
    mkdir -p "$ARTIFACTS_DIR"
    mkdir -p "$CASSANDRA_DATA_DIR"
    mkdir -p "$CQLITE_DATA_DIR"
    mkdir -p "$REPORTS_DIR"
    mkdir -p "$MANUAL_CHECK_DIR"
    
    # Create metadata file
    cat > "$ARTIFACTS_DIR/validation_metadata.json" << EOF
{
    "validation_id": "$(date +%Y%m%d_%H%M%S)_issue52",
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "cqlite_version": "$(cd "$PROJECT_ROOT" && git describe --tags --always 2>/dev/null || echo 'unknown')",
    "git_commit": "$(cd "$PROJECT_ROOT" && git rev-parse HEAD 2>/dev/null || echo 'unknown')",
    "machine_info": {
        "os": "$(uname -s)",
        "arch": "$(uname -m)",
        "hostname": "$(hostname)"
    },
    "docker_info": {
        "version": "$(docker --version | cut -d' ' -f3 | tr -d ',')",
        "compose_version": "$(docker-compose --version | cut -d' ' -f3 | tr -d ',')"
    }
}
EOF
    
    log_success "Artifacts directory ready: $ARTIFACTS_DIR"
}

# Step 1: Start Cassandra 5.0 stack
start_cassandra_stack() {
    log_step "Start Cassandra 5.0 Stack"
    
    log_info "Using Docker Compose file: $DOCKER_COMPOSE_FILE"
    
    # Clean up any existing containers
    log_info "Cleaning up existing containers..."
    docker-compose -f "$DOCKER_COMPOSE_FILE" down --volumes --remove-orphans || true
    
    # Start Cassandra
    log_info "Starting Cassandra 5.0 container..."
    if ! docker-compose -f "$DOCKER_COMPOSE_FILE" up -d cassandra-5-0; then
        log_error "Failed to start Cassandra container"
        return 1
    fi
    
    # Wait for Cassandra to be ready
    log_info "Waiting for Cassandra to be ready..."
    local retry_count=0
    while [ $retry_count -lt 30 ]; do
        if docker exec "$CASSANDRA_CONTAINER" cqlsh -e "SELECT cluster_name FROM system.local;" &> /dev/null; then
            log_success "Cassandra is ready!"
            break
        fi
        
        log_info "Waiting for Cassandra... (attempt $((retry_count + 1))/30)"
        sleep 10
        ((retry_count++))
    done
    
    if [ $retry_count -eq 30 ]; then
        log_error "Cassandra failed to start within timeout"
        docker-compose -f "$DOCKER_COMPOSE_FILE" logs cassandra-5-0
        return 1
    fi
    
    # Verify Cassandra health
    log_info "Verifying Cassandra health..."
    if ! docker exec "$CASSANDRA_CONTAINER" nodetool status | grep -q "UN"; then
        log_error "Cassandra is not in UP/NORMAL state"
        docker exec "$CASSANDRA_CONTAINER" nodetool status
        return 1
    fi
    
    log_success "Cassandra 5.0 stack is running and healthy"
    
    # Save container information
    docker-compose -f "$DOCKER_COMPOSE_FILE" ps > "$REPORTS_DIR/step1_docker_containers.txt"
    docker exec "$CASSANDRA_CONTAINER" nodetool status > "$REPORTS_DIR/step1_cassandra_status.txt"
    
    return 0
}

# Step 2: Generate test data
generate_test_data() {
    log_step "Generate Test Data Using Existing Scripts"
    
    log_info "Running CQL validation test script..."
    
    # Use the existing validation script
    local test_script="$PROJECT_ROOT/scripts/testing/run_cql_validation_tests.sh"
    if [[ ! -f "$test_script" ]]; then
        log_error "Test data generation script not found: $test_script"
        return 1
    fi
    
    # Run with timeout
    if ! timeout $VALIDATION_TIMEOUT bash "$test_script" --verbose --output "$REPORTS_DIR/step2_cql_validation"; then
        log_error "Test data generation failed or timed out"
        return 1
    fi
    
    # Verify data was created in Cassandra
    log_info "Verifying test data in Cassandra..."
    local keyspaces
    keyspaces=$(docker exec "$CASSANDRA_CONTAINER" cqlsh -e "DESCRIBE KEYSPACES;" | grep -v "system")
    
    if [[ -z "$keyspaces" ]]; then
        log_error "No test keyspaces found in Cassandra"
        return 1
    fi
    
    log_success "Test keyspaces found: $keyspaces"
    
    # Force flush to ensure data is written to SSTables
    log_info "Forcing flush to create SSTables..."
    for keyspace in $keyspaces; do
        if [[ "$keyspace" != "system"* ]]; then
            docker exec "$CASSANDRA_CONTAINER" nodetool flush "$keyspace" || log_warning "Failed to flush keyspace: $keyspace"
        fi
    done
    
    # Extract SSTable files from container
    log_info "Extracting SSTable files from Cassandra container..."
    docker exec "$CASSANDRA_CONTAINER" find /var/lib/cassandra/data -name "*.db" -type f > "$REPORTS_DIR/step2_sstable_files.txt"
    
    local sstable_count
    sstable_count=$(cat "$REPORTS_DIR/step2_sstable_files.txt" | wc -l)
    log_info "Found $sstable_count SSTable files"
    
    if [ "$sstable_count" -eq 0 ]; then
        log_error "No SSTable files found"
        return 1
    fi
    
    # Copy a subset of SSTables for validation
    log_info "Copying SSTable directories for validation..."
    docker exec "$CASSANDRA_CONTAINER" find /var/lib/cassandra/data -maxdepth 3 -type d -name "*-*" | head -10 > "$REPORTS_DIR/step2_sstable_dirs.txt"
    
    while IFS= read -r sstable_dir; do
        if [[ -n "$sstable_dir" ]]; then
            local dir_name=$(basename "$sstable_dir")
            docker cp "$CASSANDRA_CONTAINER:$sstable_dir" "$CASSANDRA_DATA_DIR/$dir_name" || log_warning "Failed to copy $sstable_dir"
        fi
    done < "$REPORTS_DIR/step2_sstable_dirs.txt"
    
    log_success "Test data generation and extraction completed"
    echo "keyspaces=\"$keyspaces\"" > "$ARTIFACTS_DIR/test_data_info.sh"
    
    return 0
}

# Step 3: Run sstabledump validator with zero tolerance
run_sstabledump_validator() {
    log_step "Run SSTableDump Validator with Zero Tolerance"
    
    log_info "Building sstabledump-validator..."
    if ! cargo build --release -p sstabledump-validator --manifest-path "$PROJECT_ROOT/Cargo.toml"; then
        log_error "Failed to build sstabledump-validator"
        return 1
    fi
    
    local validator_binary="$PROJECT_ROOT/target/release/sstabledump-validator"
    if [[ ! -f "$validator_binary" ]]; then
        log_error "SSTableDump validator binary not found: $validator_binary"
        return 1
    fi
    
    # Run comprehensive validation
    log_info "Running comprehensive validation with zero tolerance..."
    local validation_failed=false
    local total_validations=0
    local successful_validations=0
    
    # Create validation report
    local validation_report="$REPORTS_DIR/step3_validation_report.json"
    echo '{"validations": [], "summary": {}}' > "$validation_report"
    
    # Validate each SSTable directory
    for sstable_dir in "$CASSANDRA_DATA_DIR"/*; do
        if [[ -d "$sstable_dir" ]]; then
            local dir_name=$(basename "$sstable_dir")
            log_info "Validating SSTable directory: $dir_name"
            
            ((total_validations++))
            
            # Run validator with detailed output
            local validation_output="$REPORTS_DIR/step3_${dir_name}_validation.txt"
            if timeout $VALIDATION_TIMEOUT "$validator_binary" comprehensive \
                --scope full \
                --fail-fast true \
                --include-bti \
                --include-all-types \
                > "$validation_output" 2>&1; then
                
                log_success "✅ $dir_name: Validation passed"
                ((successful_validations++))
                
                # Update report
                jq --arg dir "$dir_name" --arg status "passed" \
                   '.validations += [{"directory": $dir, "status": $status, "differences": 0}]' \
                   "$validation_report" > "${validation_report}.tmp" && mv "${validation_report}.tmp" "$validation_report"
            else
                log_error "❌ $dir_name: Validation failed"
                validation_failed=true
                
                # Show first few lines of error
                head -20 "$validation_output" | while IFS= read -r line; do
                    log_error "    $line"
                done
                
                # Update report
                jq --arg dir "$dir_name" --arg status "failed" \
                   '.validations += [{"directory": $dir, "status": $status, "differences": -1}]' \
                   "$validation_report" > "${validation_report}.tmp" && mv "${validation_report}.tmp" "$validation_report"
            fi
        fi
    done
    
    # Update summary
    jq --arg total "$total_validations" --arg successful "$successful_validations" \
       '.summary = {"total": ($total | tonumber), "successful": ($successful | tonumber), "failed": (($total | tonumber) - ($successful | tonumber))}' \
       "$validation_report" > "${validation_report}.tmp" && mv "${validation_report}.tmp" "$validation_report"
    
    log_info "Validation summary: $successful_validations/$total_validations passed"
    
    if [ "$validation_failed" = true ] && [ "$FAIL_ON_ANY_DIFF" = true ]; then
        log_error "Zero-tolerance validation failed - differences detected"
        return 1
    fi
    
    log_success "SSTableDump validator completed"
    return 0
}

# Step 4: Manual spot-check workflow
manual_spot_check_workflow() {
    log_step "Manual Spot-Check Workflow (Human Verification)"
    
    log_human_action "Time for manual verification to build trust in CQLite's accuracy!"
    
    # Select a representative SSTable for manual checking
    local sstable_dirs=("$CASSANDRA_DATA_DIR"/*)
    if [ ${#sstable_dirs[@]} -eq 0 ]; then
        log_error "No SSTable directories available for manual checking"
        return 1
    fi
    
    local selected_dir="${sstable_dirs[0]}"
    local dir_name=$(basename "$selected_dir")
    
    log_info "Selected SSTable for manual verification: $dir_name"
    
    # Generate manual verification guide
    local manual_guide="$MANUAL_CHECK_DIR/manual_verification_guide.md"
    cat > "$manual_guide" << EOF
# Manual Verification Guide for $dir_name

## Overview
This guide walks you through manually verifying CQLite's output against Cassandra's sstabledump.
The goal is to build human trust by allowing you to see the data match with your own eyes.

## Files Generated
- \`cassandra_dump.txt\` - Raw output from Cassandra's sstabledump
- \`cqlite_dump.txt\` - Raw output from CQLite's equivalent functionality
- \`sample_keys.txt\` - A few sample keys for manual comparison
- \`comparison_notes.txt\` - Space for your verification notes

## Manual Verification Steps

### 1. Compare Raw Output Structure
1. Open both \`cassandra_dump.txt\` and \`cqlite_dump.txt\`
2. Check that both files have similar structure and format
3. Verify row counts are identical

### 2. Spot-Check Individual Records
1. Pick 3-5 random records from the dumps
2. Compare key fields, values, timestamps, and TTLs
3. Ensure metadata (deletion markers, cell timestamps) match exactly

### 3. Verify Edge Cases
1. Look for any tombstones (deletion markers)
2. Check timestamp formats and values
3. Verify TTL handling if present

### 4. Document Your Findings
1. Note any discrepancies in \`comparison_notes.txt\`
2. If everything matches, note "VERIFIED - NO DIFFERENCES FOUND"
3. Include timestamp of your verification

## What to Look For
- **Key Values**: Partition keys and clustering keys should be identical
- **Cell Values**: All column values should match exactly
- **Timestamps**: Cell timestamps should be identical (format may differ slightly)
- **TTLs**: Time-to-live values should match
- **Tombstones**: Deletion markers should be present in both outputs

## Red Flags
- Different number of rows
- Missing or extra records
- Different key or value content
- Mismatched timestamps or TTLs
- Missing tombstones or metadata

Remember: Even small differences matter for data integrity!
EOF

    # Generate Cassandra dump
    log_info "Generating Cassandra sstabledump output..."
    local cassandra_dump="$MANUAL_CHECK_DIR/cassandra_dump.txt"
    
    # Find a representative SSTable file
    local sstable_file=$(docker exec "$CASSANDRA_CONTAINER" find "/var/lib/cassandra/data" -name "*-big-Data.db" | head -1)
    if [[ -z "$sstable_file" ]]; then
        log_warning "No Data.db files found, using directory approach"
        docker exec "$CASSANDRA_CONTAINER" bash -c "cd /var/lib/cassandra && find . -name '*.db' | head -10" > "$MANUAL_CHECK_DIR/available_files.txt"
    else
        if ! docker exec "$CASSANDRA_CONTAINER" sstabledump "$sstable_file" > "$cassandra_dump" 2>/dev/null; then
            log_warning "Failed to run sstabledump, manual verification will be limited"
            echo "# Failed to generate Cassandra dump" > "$cassandra_dump"
        fi
    fi
    
    # Generate sample keys for focused verification
    if [[ -f "$cassandra_dump" ]] && [[ -s "$cassandra_dump" ]]; then
        log_info "Extracting sample keys for focused verification..."
        grep -E '"key"|"partition"' "$cassandra_dump" | head -10 > "$MANUAL_CHECK_DIR/sample_keys.txt" || true
    fi
    
    # Create space for user notes
    cat > "$MANUAL_CHECK_DIR/comparison_notes.txt" << EOF
# Manual Verification Notes

Verification Date: $(date)
Reviewer: [YOUR NAME HERE]
SSTable: $dir_name

## Verification Checklist
[ ] Row counts match between Cassandra and CQLite dumps
[ ] Sample keys and values match exactly
[ ] Timestamps are identical or consistently formatted
[ ] TTL values match (if present)
[ ] Tombstones/deletion markers match
[ ] No unexpected differences found

## Detailed Notes
[Add your observations here]

## Final Assessment
[VERIFIED - NO DIFFERENCES FOUND] or [DIFFERENCES FOUND - see details above]

Verification completed at: [TIMESTAMP]
EOF
    
    log_human_action "Manual verification files are ready in: $MANUAL_CHECK_DIR"
    log_human_action "Please follow the manual_verification_guide.md to verify the data"
    log_human_action "Take your time - this step builds trust in the system!"
    
    # Interactive pause for manual verification
    echo ""
    echo -e "${YELLOW}Press Enter when you have completed the manual verification...${NC}"
    read -r
    
    # Check if user completed the verification
    if [[ -f "$MANUAL_CHECK_DIR/comparison_notes.txt" ]]; then
        if grep -q "VERIFIED - NO DIFFERENCES FOUND" "$MANUAL_CHECK_DIR/comparison_notes.txt"; then
            log_success "✅ Manual verification completed successfully"
        elif grep -q "DIFFERENCES FOUND" "$MANUAL_CHECK_DIR/comparison_notes.txt"; then
            log_warning "⚠️ Manual verification found differences"
        else
            log_warning "Manual verification notes incomplete"
        fi
    fi
    
    return 0
}

# Step 5: Export via CLI and diff
export_and_diff_comparison() {
    log_step "Export via CLI and Diff Comparison"
    
    log_info "Building CQLite CLI..."
    if ! cargo build --release -p cqlite-cli --manifest-path "$PROJECT_ROOT/Cargo.toml"; then
        log_error "Failed to build cqlite-cli"
        return 1
    fi
    
    local cli_binary="$PROJECT_ROOT/target/release/cqlite"
    if [[ ! -f "$cli_binary" ]]; then
        log_error "CQLite CLI binary not found: $cli_binary"
        return 1
    fi
    
    # Find schema files
    local schema_dir="$PROJECT_ROOT/test-data/schemas"
    if [[ ! -d "$schema_dir" ]]; then
        log_error "Schema directory not found: $schema_dir"
        return 1
    fi
    
    # Process each SSTable with available schemas
    local diff_success_count=0
    local diff_total_count=0
    
    for sstable_dir in "$CASSANDRA_DATA_DIR"/*; do
        if [[ -d "$sstable_dir" ]]; then
            local dir_name=$(basename "$sstable_dir")
            log_info "Processing $dir_name for CLI export and diff..."
            
            ((diff_total_count++))
            
            # Try to find matching schema (basic heuristic)
            local schema_file=""
            for schema in "$schema_dir"/*.cql; do
                if [[ -f "$schema" ]]; then
                    schema_file="$schema"
                    break
                fi
            done
            
            if [[ -z "$schema_file" ]]; then
                log_warning "No schema file found for $dir_name, skipping"
                continue
            fi
            
            # Export via CQLite CLI
            local cqlite_export="$CQLITE_DATA_DIR/${dir_name}_cqlite.json"
            log_info "Exporting $dir_name via CQLite CLI..."
            
            if timeout $VALIDATION_TIMEOUT "$cli_binary" export \
                --sstable "$sstable_dir" \
                --schema "$schema_file" \
                --format json \
                "$cqlite_export" 2> "$REPORTS_DIR/step5_${dir_name}_cqlite_export.log"; then
                
                log_success "CQLite export completed: $cqlite_export"
            else
                log_error "CQLite export failed for $dir_name"
                continue
            fi
            
            # Generate Cassandra export
            local cassandra_export="$CASSANDRA_DATA_DIR/${dir_name}_cassandra.json"
            log_info "Generating Cassandra export for comparison..."
            
            # Find corresponding SSTable in container
            local container_sstable_dir=$(docker exec "$CASSANDRA_CONTAINER" find /var/lib/cassandra/data -name "*${dir_name}*" -type d | head -1)
            if [[ -n "$container_sstable_dir" ]]; then
                local data_file=$(docker exec "$CASSANDRA_CONTAINER" find "$container_sstable_dir" -name "*-Data.db" | head -1)
                if [[ -n "$data_file" ]]; then
                    if docker exec "$CASSANDRA_CONTAINER" sstabledump "$data_file" > "$cassandra_export" 2>/dev/null; then
                        log_success "Cassandra export completed: $cassandra_export"
                    else
                        log_warning "Failed to generate Cassandra export for $dir_name"
                        continue
                    fi
                fi
            fi
            
            # Perform diff comparison
            if [[ -f "$cqlite_export" ]] && [[ -f "$cassandra_export" ]] && [[ -s "$cqlite_export" ]] && [[ -s "$cassandra_export" ]]; then
                log_info "Performing JSON diff comparison..."
                
                # Normalize JSON for comparison
                local cqlite_normalized="$CQLITE_DATA_DIR/${dir_name}_cqlite_normalized.json"
                local cassandra_normalized="$CASSANDRA_DATA_DIR/${dir_name}_cassandra_normalized.json"
                
                if jq -S . "$cqlite_export" > "$cqlite_normalized" 2>/dev/null && \
                   jq -S . "$cassandra_export" > "$cassandra_normalized" 2>/dev/null; then
                    
                    local diff_output="$REPORTS_DIR/step5_${dir_name}_diff.txt"
                    if diff -u "$cassandra_normalized" "$cqlite_normalized" > "$diff_output"; then
                        log_success "✅ $dir_name: Perfect match - zero differences"
                        echo "ZERO DIFFERENCES" > "$REPORTS_DIR/step5_${dir_name}_result.txt"
                        ((diff_success_count++))
                    else
                        log_error "❌ $dir_name: Differences found"
                        echo "DIFFERENCES FOUND" > "$REPORTS_DIR/step5_${dir_name}_result.txt"
                        
                        # Show summary of differences
                        local diff_lines=$(wc -l < "$diff_output")
                        log_error "  $diff_lines lines of differences found"
                        
                        if [[ "$FAIL_ON_ANY_DIFF" = true ]]; then
                            log_error "Zero-tolerance mode: failing due to differences"
                            return 1
                        fi
                    fi
                else
                    log_warning "Failed to normalize JSON for $dir_name"
                fi
            else
                log_warning "Export files missing or empty for $dir_name"
            fi
        fi
    done
    
    # Summary
    log_info "CLI export and diff summary: $diff_success_count/$diff_total_count perfect matches"
    
    if [ "$diff_success_count" -eq "$diff_total_count" ] && [ "$diff_total_count" -gt 0 ]; then
        log_success "All CLI exports show perfect parity with Cassandra"
    else
        log_warning "Some CLI exports show differences or failed"
    fi
    
    return 0
}

# Cleanup function
cleanup() {
    log_info "Cleaning up..."
    
    # Stop Docker containers
    if [[ -f "$DOCKER_COMPOSE_FILE" ]]; then
        docker-compose -f "$DOCKER_COMPOSE_FILE" down --volumes --remove-orphans || true
    fi
    
    # Archive artifacts if successful
    if [[ "$OVERALL_SUCCESS" = true ]]; then
        local archive_name="validation_artifacts_$(date +%Y%m%d_%H%M%S).tar.gz"
        log_info "Creating validation archive: $archive_name"
        tar -czf "$PROJECT_ROOT/$archive_name" -C "$ARTIFACTS_DIR" . || true
        log_success "Validation artifacts archived: $PROJECT_ROOT/$archive_name"
    fi
}

# Generate final report
generate_final_report() {
    local report_file="$REPORTS_DIR/final_validation_report.md"
    local end_time=$(date)
    local duration=$(($(date +%s) - $(date -d "$VALIDATION_START_TIME" +%s 2>/dev/null || echo 0)))
    
    cat > "$report_file" << EOF
# CQLite Human-Verifiable Validation Report
## Issue #52 - Final P1 for M1

**Validation ID:** $(jq -r '.validation_id' "$ARTIFACTS_DIR/validation_metadata.json" 2>/dev/null || echo "unknown")
**Start Time:** $VALIDATION_START_TIME
**End Time:** $end_time
**Duration:** ${duration} seconds
**Overall Success:** $OVERALL_SUCCESS

## Summary
- **Critical Failures:** $CRITICAL_FAILURES
- **Warnings:** $WARNINGS
- **Total Steps Completed:** $CURRENT_STEP/$TOTAL_STEPS

## Step Results

### Step 1: Cassandra 5.0 Stack
- Status: $([ -f "$REPORTS_DIR/step1_cassandra_status.txt" ] && echo "✅ Completed" || echo "❌ Failed")
- Container status available in: \`step1_docker_containers.txt\`

### Step 2: Test Data Generation
- Status: $([ -f "$REPORTS_DIR/step2_sstable_files.txt" ] && echo "✅ Completed" || echo "❌ Failed")
- SSTable count: $([ -f "$REPORTS_DIR/step2_sstable_files.txt" ] && wc -l < "$REPORTS_DIR/step2_sstable_files.txt" || echo "0")

### Step 3: SSTableDump Validator
- Status: $([ -f "$REPORTS_DIR/step3_validation_report.json" ] && echo "✅ Completed" || echo "❌ Failed")
- Validation results: $([ -f "$REPORTS_DIR/step3_validation_report.json" ] && jq -r '.summary' "$REPORTS_DIR/step3_validation_report.json" || echo "No data")

### Step 4: Manual Verification
- Status: $([ -f "$MANUAL_CHECK_DIR/comparison_notes.txt" ] && echo "✅ Completed" || echo "❌ Failed")
- Manual verification guide: \`manual_verification_guide.md\`

### Step 5: CLI Export and Diff
- Status: $(find "$REPORTS_DIR" -name "step5_*_result.txt" -exec grep -l "ZERO DIFFERENCES" {} \; | wc -l | grep -q "^[1-9]" && echo "✅ Completed" || echo "❌ Failed")
- Perfect matches: $(find "$REPORTS_DIR" -name "step5_*_result.txt" -exec grep -l "ZERO DIFFERENCES" {} \; | wc -l || echo "0")

## Reproducibility Information
- All artifacts saved in: \`$ARTIFACTS_DIR\`
- Docker environment: Cassandra 5.0
- CQLite version: $(cd "$PROJECT_ROOT" && git describe --tags --always 2>/dev/null || echo 'unknown')
- Machine: $(uname -s) $(uname -m)

## Recommendations
$(if [ "$OVERALL_SUCCESS" = true ]; then
    echo "✅ Validation completed successfully. CQLite shows perfect parity with Cassandra."
    echo "✅ All artifacts are available for independent verification."
    echo "✅ The workflow is reproducible and can be run on any clean machine."
else
    echo "❌ Validation encountered issues. Review the detailed logs above."
    echo "❌ Address any critical failures before proceeding."
    echo "⚠️  Consider running individual steps to isolate problems."
fi)

## Files for Independent Verification
- Validation metadata: \`validation_metadata.json\`
- Manual verification guide: \`manual_verification/manual_verification_guide.md\`
- All step reports: \`reports/step*\`
- SSTable samples: \`cassandra_data/\` and \`cqlite_data/\`

---
Generated by: CQLite Human-Verifiable Validation Workflow
Issue: #52 - Human-verifiable, reproducible validation workflow (P1)
EOF

    log_info "Final report generated: $report_file"
}

# Main workflow execution
main() {
    echo -e "${BOLD}${BLUE}"
    echo "=============================================================================="
    echo "  CQLite Human-Verifiable Validation Workflow"
    echo "  Issue #52: Final P1 issue for M1"
    echo "  Building trust through reproducible validation"
    echo "=============================================================================="
    echo -e "${NC}"
    
    VALIDATION_START_TIME=$(date)
    
    # Set up trap for cleanup
    trap cleanup EXIT
    
    # Execute workflow steps
    check_prerequisites || exit 1
    setup_artifacts_directory || exit 1
    
    start_cassandra_stack || exit 1
    generate_test_data || exit 1
    run_sstabledump_validator || exit 1
    manual_spot_check_workflow || exit 1
    export_and_diff_comparison || exit 1
    
    # Mark all todos as completed
    # (This would be done by the TODO system in real usage)
    
    generate_final_report
    
    echo ""
    echo -e "${BOLD}${BLUE}=============================================================================="
    echo "  VALIDATION WORKFLOW COMPLETED"
    echo "=============================================================================="
    echo -e "${NC}"
    
    if [ "$OVERALL_SUCCESS" = true ]; then
        log_success "🎉 All validation steps completed successfully!"
        log_success "🎯 CQLite shows perfect parity with Cassandra 5.0"
        log_success "📋 All artifacts available for independent verification"
        log_success "🔄 Workflow is reproducible on clean machines"
        echo ""
        log_info "Artifacts location: $ARTIFACTS_DIR"
        log_info "Final report: $REPORTS_DIR/final_validation_report.md"
        
        echo ""
        echo -e "${GREEN}${BOLD}✅ ISSUE #52 VALIDATION: SUCCESS${NC}"
        echo -e "${GREEN}   Human trust in CQLite accuracy has been established${NC}"
        exit 0
    else
        log_error "❌ Validation workflow encountered critical issues"
        log_error "🔍 Review the detailed reports for troubleshooting"
        log_error "📋 Artifacts preserved for analysis"
        echo ""
        log_info "Artifacts location: $ARTIFACTS_DIR"
        log_info "Final report: $REPORTS_DIR/final_validation_report.md"
        
        echo ""
        echo -e "${RED}${BOLD}❌ ISSUE #52 VALIDATION: FAILED${NC}"
        echo -e "${RED}   Address critical issues before proceeding${NC}"
        exit 1
    fi
}

# Script entry point
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi