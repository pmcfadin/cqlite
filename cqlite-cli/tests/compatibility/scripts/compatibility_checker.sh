#!/bin/bash
# Automated Cassandra Compatibility Checker for CQLite
# Usage: ./compatibility_checker.sh --version 5.1 --test-suite comprehensive

set -euo pipefail

# Default values
VERSION=""
TEST_SUITE="comprehensive"
OUTPUT_DIR="./compatibility-results"
VERBOSE=false
DOCKER_COMPOSE_FILE="./docker/docker-compose.yml"
CLEANUP=true
PARALLEL=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Print usage information
usage() {
    cat << EOF
🐘 CQLite Cassandra Compatibility Checker

Usage: $0 [OPTIONS]

OPTIONS:
    --version VERSION        Cassandra version to test (e.g., 4.0, 5.1)
    --test-suite SUITE      Test suite level (basic|comprehensive|full)
    --output DIR            Output directory for results
    --verbose               Enable verbose output
    --no-cleanup            Don't cleanup Docker containers after testing
    --parallel              Run tests in parallel for all versions
    --matrix                Run compatibility matrix for all versions
    --monitor INTERVAL      Monitor mode (e.g., 1h, 24h, 7d)
    --help                  Show this help message

EXAMPLES:
    # Test specific version
    $0 --version 5.1 --test-suite comprehensive

    # Run full compatibility matrix
    $0 --matrix --test-suite basic

    # Monitor compatibility continuously
    $0 --monitor 24h --test-suite basic

    # Parallel testing with custom output
    $0 --parallel --output ./my-results

EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --version)
                VERSION="$2"
                shift 2
                ;;
            --test-suite)
                TEST_SUITE="$2"
                shift 2
                ;;
            --output)
                OUTPUT_DIR="$2"
                shift 2
                ;;
            --verbose)
                VERBOSE=true
                shift
                ;;
            --no-cleanup)
                CLEANUP=false
                shift
                ;;
            --parallel)
                PARALLEL=true
                shift
                ;;
            --matrix)
                MATRIX=true
                shift
                ;;
            --monitor)
                MONITOR_INTERVAL="$2"
                shift 2
                ;;
            --help)
                usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
}

# Check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        print_error "Docker is required but not installed"
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_error "Docker Compose is required but not installed"
        exit 1
    fi
    
    # Check Rust/Cargo
    if ! command -v cargo &> /dev/null; then
        print_error "Cargo is required but not installed"
        exit 1
    fi
    
    # Check cqlsh (for testing connectivity)
    if ! command -v cqlsh &> /dev/null; then
        print_warning "cqlsh not found, will use Docker exec for CQL commands"
    fi
    
    print_success "Prerequisites check passed"
}

# Build the compatibility test suite
build_test_suite() {
    print_status "Building compatibility test suite..."
    
    cd tests/compatibility
    
    if [[ "$VERBOSE" == "true" ]]; then
        cargo build --release --bins
    else
        cargo build --release --bins > /dev/null 2>&1
    fi
    
    if [[ ! -f "target/release/compatibility-checker" ]]; then
        print_error "Failed to build compatibility-checker binary"
        exit 1
    fi
    
    print_success "Test suite built successfully"
    cd - > /dev/null
}

# Start Cassandra container for specific version
start_cassandra() {
    local version=$1
    local port=$(get_port_for_version "$version")
    
    print_status "Starting Cassandra $version on port $port..."
    
    # Use docker-compose if available, otherwise use docker run
    if [[ -f "$DOCKER_COMPOSE_FILE" ]]; then
        local service_name="cassandra-$(echo "$version" | tr '.' '-')"
        docker-compose -f "$DOCKER_COMPOSE_FILE" up -d "$service_name"
    else
        docker run -d \
            --name "cassandra-$version" \
            -p "$port:9042" \
            -e CASSANDRA_START_RPC=true \
            -e CASSANDRA_RPC_ADDRESS=0.0.0.0 \
            -e CASSANDRA_LISTEN_ADDRESS=auto \
            -e CASSANDRA_BROADCAST_ADDRESS=127.0.0.1 \
            -e CASSANDRA_BROADCAST_RPC_ADDRESS=127.0.0.1 \
            "cassandra:$version"
    fi
    
    # Wait for Cassandra to be ready
    wait_for_cassandra "$version" "$port"
}

# Wait for Cassandra to be ready
wait_for_cassandra() {
    local version=$1
    local port=$2
    local max_attempts=30
    local attempt=1
    
    print_status "Waiting for Cassandra $version to be ready..."
    
    while [[ $attempt -le $max_attempts ]]; do
        if docker exec "cassandra-$version" cqlsh -e "DESCRIBE KEYSPACES;" > /dev/null 2>&1; then
            print_success "Cassandra $version is ready!"
            return 0
        fi
        
        if [[ $((attempt % 5)) -eq 0 ]]; then
            print_status "Still waiting... (attempt $attempt/$max_attempts)"
        fi
        
        sleep 10
        ((attempt++))
    done
    
    print_error "Cassandra $version failed to start within timeout"
    return 1
}

# Get port number for specific version
get_port_for_version() {
    local version=$1
    case $version in
        4.0) echo 9042 ;;
        4.1) echo 9043 ;;
        5.0) echo 9044 ;;
        5.1) echo 9045 ;;
        6.0) echo 9046 ;;
        *) echo $((9042 + $(echo "$version" | tr -d '.' | head -c 2))) ;;
    esac
}

# Run compatibility test for specific version
test_version() {
    local version=$1
    
    print_status "Testing Cassandra version $version with $TEST_SUITE test suite"
    
    # Create output directory
    mkdir -p "$OUTPUT_DIR"
    
    # Start Cassandra
    start_cassandra "$version"
    
    # Run compatibility tests
    cd tests/compatibility
    
    local cmd="./target/release/compatibility-checker test \
        --version $version \
        --suite $TEST_SUITE \
        --detailed \
        --output $OUTPUT_DIR"
    
    if [[ "$VERBOSE" == "true" ]]; then
        print_status "Running: $cmd"
        $cmd
    else
        $cmd > /dev/null 2>&1
    fi
    
    local exit_code=$?
    cd - > /dev/null
    
    # Cleanup if requested
    if [[ "$CLEANUP" == "true" ]]; then
        cleanup_cassandra "$version"
    fi
    
    if [[ $exit_code -eq 0 ]]; then
        print_success "Cassandra $version compatibility test completed"
    else
        print_error "Cassandra $version compatibility test failed"
    fi
    
    return $exit_code
}

# Run compatibility matrix for all versions
run_compatibility_matrix() {
    local versions=("4.0" "4.1" "5.0" "5.1")
    
    print_status "Running compatibility matrix for versions: ${versions[*]}"
    
    mkdir -p "$OUTPUT_DIR"
    
    if [[ "$PARALLEL" == "true" ]]; then
        print_status "Running tests in parallel..."
        
        local pids=()
        for version in "${versions[@]}"; do
            (test_version "$version") &
            pids+=($!)
        done
        
        # Wait for all tests to complete
        local failed=0
        for pid in "${pids[@]}"; do
            if ! wait "$pid"; then
                ((failed++))
            fi
        done
        
        if [[ $failed -eq 0 ]]; then
            print_success "All parallel tests completed successfully"
        else
            print_error "$failed tests failed"
        fi
    else
        local failed=0
        for version in "${versions[@]}"; do
            if ! test_version "$version"; then
                ((failed++))
            fi
        done
        
        if [[ $failed -eq 0 ]]; then
            print_success "All sequential tests completed successfully"
        else
            print_error "$failed tests failed"
        fi
    fi
    
    # Generate combined report
    generate_matrix_report "${versions[@]}"
}

# Generate matrix report
generate_matrix_report() {
    local versions=("$@")
    
    print_status "Generating compatibility matrix report..."
    
    cd tests/compatibility
    ./target/release/compatibility-checker matrix \
        --format markdown \
        --output "$OUTPUT_DIR"
    cd - > /dev/null
    
    # Create dashboard summary
    create_dashboard_summary "${versions[@]}"
    
    print_success "Matrix report generated in $OUTPUT_DIR"
}

# Create dashboard summary
create_dashboard_summary() {
    local versions=("$@")
    local dashboard_file="$OUTPUT_DIR/compatibility-dashboard.md"
    
    cat > "$dashboard_file" << EOF
# 🐘 CQLite Cassandra Compatibility Dashboard

**Generated:** $(date -u '+%Y-%m-%d %H:%M:%S UTC')
**Test Suite:** $TEST_SUITE

## 📊 Compatibility Matrix

| Version | Status | Score | Issues | Details |
|---------|--------|-------|--------|---------|
EOF

    for version in "${versions[@]}"; do
        local result_file="$OUTPUT_DIR/compatibility-$version.json"
        
        if [[ -f "$result_file" ]]; then
            local score=$(jq -r '.compatibility_score' "$result_file" 2>/dev/null || echo "0")
            local issues=$(jq -r '.issues | length' "$result_file" 2>/dev/null || echo "unknown")
            
            local status
            if (( $(echo "$score >= 95" | bc -l 2>/dev/null || echo "0") )); then
                status="✅ Compatible"
            elif (( $(echo "$score >= 80" | bc -l 2>/dev/null || echo "0") )); then
                status="🟡 Minor Issues"
            else
                status="❌ Issues Found"
            fi
            
            echo "| $version | $status | ${score}% | $issues | [Details](./compatibility-$version.json) |" >> "$dashboard_file"
        else
            echo "| $version | ❌ Test Failed | - | - | No results |" >> "$dashboard_file"
        fi
    done
    
    cat >> "$dashboard_file" << EOF

## 🔗 Reports

- [Full Compatibility Matrix](./compatibility-matrix.md)
- [Detailed Test Results](./detailed-results/)

## 📈 Trends

$(generate_trend_analysis)

---
*Generated by CQLite Compatibility Checker*
EOF
    
    print_success "Dashboard created: $dashboard_file"
}

# Generate trend analysis
generate_trend_analysis() {
    echo "Trend analysis would show compatibility scores over time"
    echo "and highlight any regressions or improvements."
}

# Monitor compatibility continuously
monitor_compatibility() {
    local interval=$1
    
    print_status "Starting continuous compatibility monitoring (interval: $interval)"
    
    # Parse interval to seconds
    local seconds
    case $interval in
        *h) seconds=$((${interval%h} * 3600)) ;;
        *d) seconds=$((${interval%d} * 86400)) ;;
        *m) seconds=$((${interval%m} * 60)) ;;
        *) seconds=$interval ;;
    esac
    
    while true; do
        print_status "Running compatibility check..."
        
        if run_compatibility_matrix; then
            print_success "Compatibility check completed successfully"
        else
            print_error "Compatibility check failed"
            # Could send notifications here
        fi
        
        print_status "Sleeping for $interval..."
        sleep "$seconds"
    done
}

# Cleanup Cassandra container
cleanup_cassandra() {
    local version=$1
    
    print_status "Cleaning up Cassandra $version..."
    
    # Try docker-compose first, then docker
    if [[ -f "$DOCKER_COMPOSE_FILE" ]]; then
        local service_name="cassandra-$(echo "$version" | tr '.' '-')"
        docker-compose -f "$DOCKER_COMPOSE_FILE" stop "$service_name" 2>/dev/null || true
        docker-compose -f "$DOCKER_COMPOSE_FILE" rm -f "$service_name" 2>/dev/null || true
    else
        docker stop "cassandra-$version" 2>/dev/null || true
        docker rm "cassandra-$version" 2>/dev/null || true
    fi
}

# Cleanup all containers
cleanup_all() {
    print_status "Cleaning up all Cassandra containers..."
    
    # Stop all cassandra containers
    docker ps -q --filter "name=cassandra-" | xargs -r docker stop
    docker ps -aq --filter "name=cassandra-" | xargs -r docker rm
    
    # Clean up compose services if available
    if [[ -f "$DOCKER_COMPOSE_FILE" ]]; then
        docker-compose -f "$DOCKER_COMPOSE_FILE" down 2>/dev/null || true
    fi
}

# Signal handler for cleanup
cleanup_on_exit() {
    print_status "Received signal, cleaning up..."
    if [[ "$CLEANUP" == "true" ]]; then
        cleanup_all
    fi
    exit 0
}

# Main function
main() {
    # Set up signal handlers
    trap cleanup_on_exit SIGINT SIGTERM
    
    # Parse arguments
    parse_args "$@"
    
    # Check prerequisites
    check_prerequisites
    
    # Build test suite
    build_test_suite
    
    # Execute based on options
    if [[ -n "${MONITOR_INTERVAL:-}" ]]; then
        monitor_compatibility "$MONITOR_INTERVAL"
    elif [[ "${MATRIX:-false}" == "true" ]]; then
        run_compatibility_matrix
    elif [[ -n "$VERSION" ]]; then
        test_version "$VERSION"
    else
        print_error "No action specified. Use --version, --matrix, or --monitor"
        usage
        exit 1
    fi
    
    # Final cleanup if enabled
    if [[ "$CLEANUP" == "true" ]]; then
        cleanup_all
    fi
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi