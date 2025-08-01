#!/bin/bash

# CQLite Automated Test Orchestrator - Issue #17
# Master script for comprehensive Cassandra data generation and testing infrastructure
# 
# CRITICAL SUCCESS FACTOR: Command-line test execution MUST work reliably!

set -euo pipefail

# Constants and Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$PROJECT_ROOT/logs/test-orchestrator"
REPORT_DIR="$PROJECT_ROOT/reports/automated-testing"
TEST_DATA_DIR="$PROJECT_ROOT/test-data"
CASSANDRA_VERSIONS=("3.7" "3.11" "4.0" "4.1" "5.0")

# Create necessary directories
mkdir -p "$LOG_DIR" "$REPORT_DIR"

# Logging configuration
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
MAIN_LOG="$LOG_DIR/orchestrator_${TIMESTAMP}.log"
ERROR_LOG="$LOG_DIR/errors_${TIMESTAMP}.log"

# Logging functions
log_info() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $*" | tee -a "$MAIN_LOG"
}

log_error() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "$MAIN_LOG" "$ERROR_LOG"
}

log_success() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] SUCCESS: $*" | tee -a "$MAIN_LOG"
}

log_warning() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $*" | tee -a "$MAIN_LOG"
}

# Configuration management
DEFAULT_CONFIG="$PROJECT_ROOT/.test-orchestrator-config.toml"
create_default_config() {
    cat > "$DEFAULT_CONFIG" << 'EOF'
# CQLite Test Orchestrator Configuration

[general]
parallel_jobs = 4
timeout_minutes = 60
fail_fast = false
verbose = true

[cassandra]
versions = ["3.7", "3.11", "4.0", "4.1", "5.0"]
data_scale = "COMPREHENSIVE"  # SMALL, MEDIUM, COMPREHENSIVE, LARGE
enable_docker = true
docker_timeout_minutes = 30

[tests]
unit_tests = true
integration_tests = true
performance_tests = true
property_based_tests = true
regression_tests = true
stress_tests = false

[data_generation]
enable_automated_generation = true
generate_on_startup = false
validate_generated_data = true
cleanup_on_failure = true

[reporting]
generate_html_report = true
export_json_results = true
create_performance_graphs = true
include_logs = true

[ci_cd]
export_junit_xml = true
set_exit_codes = true
create_artifacts = true
EOF
    log_info "Created default configuration at $DEFAULT_CONFIG"
}

# Load configuration
load_config() {
    if [[ ! -f "$DEFAULT_CONFIG" ]]; then
        create_default_config
    fi
    
    # Source configuration variables (simplified TOML parsing)
    PARALLEL_JOBS=$(grep -E "^parallel_jobs" "$DEFAULT_CONFIG" | cut -d'=' -f2 | tr -d ' "' || echo "4")
    TIMEOUT_MINUTES=$(grep -E "^timeout_minutes" "$DEFAULT_CONFIG" | cut -d'=' -f2 | tr -d ' "' || echo "60")
    FAIL_FAST=$(grep -E "^fail_fast" "$DEFAULT_CONFIG" | cut -d'=' -f2 | tr -d ' "' || echo "false")
    VERBOSE=$(grep -E "^verbose" "$DEFAULT_CONFIG" | cut -d'=' -f2 | tr -d ' "' || echo "true")
    DATA_SCALE=$(grep -E "^data_scale" "$DEFAULT_CONFIG" | cut -d'=' -f2 | tr -d ' "' || echo "COMPREHENSIVE")
    
    log_info "Configuration loaded: parallel_jobs=$PARALLEL_JOBS, timeout=${TIMEOUT_MINUTES}m, data_scale=$DATA_SCALE"
}

# System health checks
check_system_requirements() {
    log_info "Performing system health checks..."
    
    local failures=0
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        ((failures++))
    elif ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        ((failures++))
    else
        log_success "Docker is available and running"
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        log_error "Docker Compose is not available"
        ((failures++))
    else
        log_success "Docker Compose is available"
    fi
    
    # Check Rust toolchain
    if ! command -v cargo &> /dev/null; then
        log_error "Cargo (Rust) is not installed"
        ((failures++))
    else
        RUST_VERSION=$(rustc --version)
        log_success "Rust toolchain available: $RUST_VERSION"
    fi
    
    # Check Python (for data generation scripts)
    if ! command -v python3 &> /dev/null; then
        log_warning "Python 3 not found - some data generation features may be limited"
    else
        PYTHON_VERSION=$(python3 --version)
        log_success "Python available: $PYTHON_VERSION"
    fi
    
    # Check available disk space (minimum 10GB)
    AVAILABLE_SPACE=$(df "$PROJECT_ROOT" | awk 'NR==2 {print $4}')
    if [[ $AVAILABLE_SPACE -lt 10485760 ]]; then  # 10GB in KB
        log_warning "Low disk space detected. Recommend at least 10GB for comprehensive testing"
    else
        log_success "Sufficient disk space available"
    fi
    
    # Check available memory (minimum 4GB)
    if command -v free &> /dev/null; then
        AVAILABLE_MEM=$(free -m | awk 'NR==2{print $7}')
        if [[ $AVAILABLE_MEM -lt 4096 ]]; then
            log_warning "Low memory detected. Recommend at least 4GB for comprehensive testing"
        else
            log_success "Sufficient memory available"
        fi
    fi
    
    if [[ $failures -gt 0 ]]; then
        log_error "System health check failed with $failures critical issues"
        return 1
    fi
    
    log_success "All system health checks passed"
    return 0
}

# Cassandra data generation automation
automated_cassandra_data_generation() {
    local test_data_dir="$PROJECT_ROOT/test-data"
    local generation_log="$LOG_DIR/data_generation_${TIMESTAMP}.log"
    
    log_info "Starting automated Cassandra data generation..."
    
    if [[ ! -d "$test_data_dir" ]]; then
        log_error "Test data directory not found: $test_data_dir"
        return 1
    fi
    
    cd "$test_data_dir"
    
    # Check if Docker setup exists
    if [[ ! -f "docker/docker-compose.yml" ]]; then
        log_error "Docker compose configuration not found"
        return 1
    fi
    
    # Set environment variables for data generation
    export TEST_DATA_SCALE="$DATA_SCALE"
    export GENERATION_TIMEOUT=$((TIMEOUT_MINUTES * 60))
    
    log_info "Generating test data with scale: $DATA_SCALE"
    
    # Start data generation with timeout
    if timeout "${TIMEOUT_MINUTES}m" bash -c "
        cd docker && 
        docker-compose down --volumes --remove-orphans 2>/dev/null || true &&
        docker-compose up --build --abort-on-container-exit
    " 2>&1 | tee "$generation_log"; then
        log_success "Cassandra data generation completed successfully"
        
        # Validate generated data
        if [[ -f "scripts/validate-data.sh" ]]; then
            log_info "Validating generated test data..."
            if bash scripts/validate-data.sh 2>&1 | tee -a "$generation_log"; then
                log_success "Data validation passed"
            else
                log_error "Data validation failed"
                return 1
            fi
        fi
    else
        log_error "Cassandra data generation failed or timed out"
        return 1
    fi
    
    cd "$PROJECT_ROOT"
    return 0
}

# Build all test binaries
build_test_infrastructure() {
    log_info "Building test infrastructure..."
    
    local build_log="$LOG_DIR/build_${TIMESTAMP}.log"
    
    # Build main cqlite-cli
    log_info "Building cqlite-cli..."
    if cargo build --release --package cqlite-cli 2>&1 | tee "$build_log"; then
        log_success "cqlite-cli build completed"
    else
        log_error "cqlite-cli build failed"
        return 1
    fi
    
    # Build testing framework
    log_info "Building testing framework..."
    if cargo build --release --package cqlite-testing-framework 2>&1 | tee -a "$build_log"; then
        log_success "Testing framework build completed"
    else
        log_error "Testing framework build failed"
        return 1
    fi
    
    # Build test suite
    log_info "Building test suite..."
    if cargo build --release --package tests 2>&1 | tee -a "$build_log"; then
        log_success "Test suite build completed"
    else
        log_error "Test suite build failed"
        return 1
    fi
    
    return 0
}

# Execute comprehensive test suite
execute_comprehensive_tests() {
    local test_results_dir="$REPORT_DIR/test-results-${TIMESTAMP}"
    mkdir -p "$test_results_dir"
    
    log_info "Executing comprehensive test suite..."
    
    local total_tests=0
    local passed_tests=0
    local failed_tests=0
    
    # Test categories to run
    local test_categories=(
        "unit:Unit Tests:cargo test --package cqlite-core --release"
        "integration:Integration Tests:cargo test --package tests --release"
        "cli:CLI Tests:cargo test --package cqlite-cli --release"
        "compatibility:Compatibility Tests:cargo run --package tests --release --bin compatibility_test_runner"
        "performance:Performance Tests:cargo run --package tests --release --bin performance_benchmark_runner"
    )
    
    for category_def in "${test_categories[@]}"; do
        IFS=':' read -r category_name category_display category_command <<< "$category_def"
        
        log_info "Running $category_display..."
        local category_log="$test_results_dir/${category_name}_tests.log"
        
        ((total_tests++))
        
        if timeout "${TIMEOUT_MINUTES}m" bash -c "$category_command" 2>&1 | tee "$category_log"; then
            log_success "$category_display completed successfully"
            ((passed_tests++))
        else
            log_error "$category_display failed"
            ((failed_tests++))
            
            if [[ "$FAIL_FAST" == "true" ]]; then
                log_error "Fail-fast enabled, stopping test execution"
                break
            fi
        fi
    done
    
    # Property-based testing with proptest
    log_info "Running property-based tests..."
    local proptest_log="$test_results_dir/proptest.log"
    ((total_tests++))
    
    if PROPTEST_CASES=1000 cargo test --package tests --release --features "proptest" 2>&1 | tee "$proptest_log"; then
        log_success "Property-based tests completed successfully"
        ((passed_tests++))
    else
        log_error "Property-based tests failed"
        ((failed_tests++))
    fi
    
    # Generate test summary
    generate_test_summary "$total_tests" "$passed_tests" "$failed_tests" "$test_results_dir"
    
    return $failed_tests
}

# Generate comprehensive test summary and reports
generate_test_summary() {
    local total=$1
    local passed=$2
    local failed=$3
    local results_dir=$4
    
    local success_rate=$(( (passed * 100) / total ))
    local summary_file="$results_dir/test_summary.json"
    local html_report="$results_dir/test_report.html"
    
    log_info "Generating test summary and reports..."
    
    # Create JSON summary
    cat > "$summary_file" << EOF
{
    "timestamp": "$(date -Iseconds)",
    "total_tests": $total,
    "passed_tests": $passed,
    "failed_tests": $failed,
    "success_rate": $success_rate,
    "configuration": {
        "data_scale": "$DATA_SCALE",
        "parallel_jobs": $PARALLEL_JOBS,
        "timeout_minutes": $TIMEOUT_MINUTES,
        "fail_fast": "$FAIL_FAST"
    },
    "environment": {
        "rust_version": "$(rustc --version)",
        "cargo_version": "$(cargo --version)",
        "docker_version": "$(docker --version 2>/dev/null || echo 'Not available')"
    },
    "logs": {
        "main_log": "$MAIN_LOG",
        "error_log": "$ERROR_LOG",
        "results_directory": "$results_dir"
    }
}
EOF
    
    # Create HTML report
    cat > "$html_report" << EOF
<!DOCTYPE html>
<html>
<head>
    <title>CQLite Test Orchestrator Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .header { background: #f5f5f5; padding: 20px; border-radius: 5px; }
        .success { color: #28a745; }
        .error { color: #dc3545; }
        .warning { color: #ffc107; }
        .metric { display: inline-block; margin: 10px 20px; }
        .log-section { margin: 20px 0; }
        .log-content { background: #f8f9fa; padding: 15px; border-radius: 5px; font-family: monospace; white-space: pre-wrap; }
    </style>
</head>
<body>
    <div class="header">
        <h1>CQLite Automated Test Report</h1>
        <p><strong>Generated:</strong> $(date)</p>
        <p><strong>Issue:</strong> #17 - Automated Testing Infrastructure</p>
    </div>
    
    <h2>Test Results Summary</h2>
    <div class="metric"><strong>Total Tests:</strong> $total</div>
    <div class="metric success"><strong>Passed:</strong> $passed</div>
    <div class="metric error"><strong>Failed:</strong> $failed</div>
    <div class="metric"><strong>Success Rate:</strong> $success_rate%</div>
    
    <h2>Configuration</h2>
    <ul>
        <li><strong>Data Scale:</strong> $DATA_SCALE</li>
        <li><strong>Parallel Jobs:</strong> $PARALLEL_JOBS</li>
        <li><strong>Timeout:</strong> ${TIMEOUT_MINUTES} minutes</li>
        <li><strong>Fail Fast:</strong> $FAIL_FAST</li>
    </ul>
    
    <h2>Test Categories</h2>
    <p>Detailed logs are available in the results directory: <code>$results_dir</code></p>
    
    <div class="log-section">
        <h3>Main Log (Last 50 lines)</h3>
        <div class="log-content">$(tail -50 "$MAIN_LOG" 2>/dev/null || echo "Log not available")</div>
    </div>
</body>
</html>
EOF
    
    log_success "Test summary generated: $summary_file"
    log_success "HTML report generated: $html_report"
    
    # Print summary to console
    echo
    echo "=========================================="
    echo "CQLite Test Orchestrator - Final Summary"
    echo "=========================================="
    echo "Total Tests: $total"
    echo "Passed: $passed"
    echo "Failed: $failed"
    echo "Success Rate: $success_rate%"
    echo "Results Directory: $results_dir"
    echo "=========================================="
    echo
}

# Cleanup function
cleanup_on_exit() {
    local exit_code=$?
    
    log_info "Cleaning up test orchestrator (exit code: $exit_code)..."
    
    # Stop any running Docker containers
    if command -v docker-compose &> /dev/null; then
        cd "$TEST_DATA_DIR/docker" 2>/dev/null && docker-compose down --volumes --remove-orphans 2>/dev/null || true
    fi
    
    # Archive logs if tests failed
    if [[ $exit_code -ne 0 ]]; then
        local archive_name="failed_test_logs_${TIMESTAMP}.tar.gz"
        tar -czf "$REPORT_DIR/$archive_name" -C "$LOG_DIR" . 2>/dev/null || true
        log_info "Archived failed test logs: $REPORT_DIR/$archive_name"
    fi
    
    log_info "Cleanup completed"
}

# Performance monitoring
monitor_performance() {
    local monitor_log="$LOG_DIR/performance_monitor_${TIMESTAMP}.log"
    
    log_info "Starting performance monitoring..."
    
    # Start background monitoring
    {
        while true; do
            echo "$(date): CPU: $(top -l 1 -n 0 | grep "CPU usage" || echo "N/A")" 
            echo "$(date): Memory: $(top -l 1 -n 0 | grep "PhysMem" || echo "N/A")"
            echo "$(date): Disk: $(df -h "$PROJECT_ROOT" | tail -1)"
            echo "---"
            sleep 30
        done
    } > "$monitor_log" 2>&1 &
    
    local monitor_pid=$!
    echo $monitor_pid > "$LOG_DIR/monitor.pid"
    
    log_info "Performance monitoring started (PID: $monitor_pid)"
}

stop_performance_monitoring() {
    if [[ -f "$LOG_DIR/monitor.pid" ]]; then
        local monitor_pid=$(cat "$LOG_DIR/monitor.pid")
        kill "$monitor_pid" 2>/dev/null || true
        rm -f "$LOG_DIR/monitor.pid"
        log_info "Performance monitoring stopped"
    fi
}

# Usage information
show_usage() {
    cat << 'EOF'
CQLite Automated Test Orchestrator - Issue #17

USAGE:
    automated_test_orchestrator.sh [OPTIONS]

OPTIONS:
    --help, -h              Show this help message
    --config FILE           Use custom configuration file
    --data-scale SCALE      Set data generation scale (SMALL|MEDIUM|COMPREHENSIVE|LARGE)
    --parallel-jobs N       Number of parallel test jobs
    --timeout MINUTES       Timeout for test execution
    --fail-fast             Stop on first test failure
    --skip-data-gen         Skip Cassandra data generation
    --skip-build            Skip building test infrastructure
    --tests-only            Run only tests (skip data generation and build)
    --generate-data-only    Only generate Cassandra test data
    --verbose               Enable verbose logging
    --quiet                 Reduce log output
    --clean                 Clean previous test results before running

EXAMPLES:
    # Run full test suite with default settings
    ./automated_test_orchestrator.sh

    # Quick test run with small dataset
    ./automated_test_orchestrator.sh --data-scale SMALL --parallel-jobs 2

    # Generate test data only
    ./automated_test_orchestrator.sh --generate-data-only

    # Run tests without rebuilding
    ./automated_test_orchestrator.sh --skip-build --tests-only

    # Custom configuration
    ./automated_test_orchestrator.sh --config my-test-config.toml --verbose

CRITICAL SUCCESS FACTOR:
    Command-line test execution MUST work reliably!
    This orchestrator ensures comprehensive, automated testing of CQLite's
    Cassandra compatibility across multiple versions and data types.

EOF
}

# Main execution function
main() {
    local start_time=$(date +%s)
    
    # Set up signal handlers
    trap cleanup_on_exit EXIT
    trap 'log_error "Script interrupted"; exit 130' INT TERM
    
    # Parse command line arguments
    local custom_config=""
    local skip_data_gen=false
    local skip_build=false
    local tests_only=false
    local generate_data_only=false
    local clean_previous=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --help|-h)
                show_usage
                exit 0
                ;;
            --config)
                custom_config="$2"
                shift 2
                ;;
            --data-scale)
                DATA_SCALE="$2"
                shift 2
                ;;
            --parallel-jobs)
                PARALLEL_JOBS="$2"
                shift 2
                ;;
            --timeout)
                TIMEOUT_MINUTES="$2"
                shift 2
                ;;
            --fail-fast)
                FAIL_FAST=true
                shift
                ;;
            --skip-data-gen)
                skip_data_gen=true
                shift
                ;;
            --skip-build)
                skip_build=true
                shift
                ;;
            --tests-only)
                tests_only=true
                skip_data_gen=true
                skip_build=true
                shift
                ;;
            --generate-data-only)
                generate_data_only=true
                shift
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            --quiet)
                VERBOSE=false
                shift
                ;;
            --clean)
                clean_previous=true
                shift
                ;;
            *)
                log_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Load configuration
    if [[ -n "$custom_config" ]]; then
        DEFAULT_CONFIG="$custom_config"
    fi
    load_config
    
    # Initialize logging
    log_info "CQLite Automated Test Orchestrator starting..."
    log_info "Project root: $PROJECT_ROOT"
    log_info "Log directory: $LOG_DIR"
    log_info "Report directory: $REPORT_DIR"
    
    # Clean previous results if requested
    if [[ "$clean_previous" == true ]]; then
        log_info "Cleaning previous test results..."
        rm -rf "$REPORT_DIR"/test-results-* 2>/dev/null || true
        rm -rf "$LOG_DIR"/*.log 2>/dev/null || true
    fi
    
    # Start performance monitoring
    monitor_performance
    
    # System health checks
    if ! check_system_requirements; then
        log_error "System health checks failed. Please resolve issues before continuing."
        exit 1
    fi
    
    local exit_code=0
    
    # Execute based on mode
    if [[ "$generate_data_only" == true ]]; then
        log_info "Mode: Generate Cassandra test data only"
        if ! automated_cassandra_data_generation; then
            exit_code=1
        fi
    else
        # Full test orchestration
        log_info "Mode: Full automated testing pipeline"
        
        # Step 1: Generate Cassandra test data
        if [[ "$skip_data_gen" == false ]]; then
            if ! automated_cassandra_data_generation; then
                log_error "Cassandra data generation failed"
                exit_code=1
                if [[ "$FAIL_FAST" == true ]]; then
                    exit $exit_code
                fi
            fi
        else
            log_info "Skipping Cassandra data generation"
        fi
        
        # Step 2: Build test infrastructure
        if [[ "$skip_build" == false ]]; then
            if ! build_test_infrastructure; then
                log_error "Test infrastructure build failed"
                exit_code=1
                if [[ "$FAIL_FAST" == true ]]; then
                    exit $exit_code
                fi
            fi
        else
            log_info "Skipping test infrastructure build"
        fi
        
        # Step 3: Execute comprehensive tests
        if ! execute_comprehensive_tests; then
            log_error "Test execution had failures"
            exit_code=1
        fi
    fi
    
    # Stop performance monitoring
    stop_performance_monitoring
    
    # Calculate total execution time
    local end_time=$(date +%s)
    local total_time=$((end_time - start_time))
    local minutes=$((total_time / 60))
    local seconds=$((total_time % 60))
    
    log_info "Total execution time: ${minutes}m ${seconds}s"
    
    if [[ $exit_code -eq 0 ]]; then
        log_success "CQLite Automated Test Orchestrator completed successfully!"
    else
        log_error "CQLite Automated Test Orchestrator completed with failures (exit code: $exit_code)"
    fi
    
    exit $exit_code
}

# Execute main function with all arguments
main "$@"