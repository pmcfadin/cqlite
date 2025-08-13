#!/bin/bash

# CQLite Test Data Quick Setup Test
# Validates that the Docker-based test data generation system is properly configured
# Issue #18: Docker-based test data generation

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_COMPOSE_FILE="$SCRIPT_DIR/../docker/docker-compose.yml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Test results tracking
TESTS_PASSED=0
TESTS_FAILED=0

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
    TESTS_PASSED=$((TESTS_PASSED + 1))
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $1"
    TESTS_FAILED=$((TESTS_FAILED + 1))
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Test Docker availability
test_docker() {
    log_info "Testing Docker availability..."
    
    if command -v docker >/dev/null 2>&1; then
        if docker info >/dev/null 2>&1; then
            log_success "Docker is available and running"
        else
            log_error "Docker is installed but not running"
            return 1
        fi
    else
        log_error "Docker is not installed"
        return 1
    fi
}

# Test Docker Compose availability
test_docker_compose() {
    log_info "Testing Docker Compose availability..."
    
    if command -v docker-compose >/dev/null 2>&1; then
        local version=$(docker-compose --version)
        log_success "Docker Compose is available: $version"
    else
        log_error "Docker Compose is not installed"
        return 1
    fi
}

# Test directory structure
test_directory_structure() {
    log_info "Testing directory structure..."
    
    local required_dirs=(
        "$SCRIPT_DIR/../docker"
        "$SCRIPT_DIR/../schemas"  
        "$SCRIPT_DIR/../generated"
        "$SCRIPT_DIR"
    )
    
    local all_dirs_exist=true
    
    for dir in "${required_dirs[@]}"; do
        if [ -d "$dir" ]; then
            log_success "Directory exists: $(basename "$dir")"
        else
            log_error "Directory missing: $(basename "$dir")"
            all_dirs_exist=false
        fi
    done
    
    if [ "$all_dirs_exist" = false ]; then
        return 1
    fi
}

# Test required files
test_required_files() {
    log_info "Testing required files..."
    
    local required_files=(
        "$DOCKER_COMPOSE_FILE"
        "$SCRIPT_DIR/generate-all-test-data.sh"
        "$SCRIPT_DIR/export-sstables.sh"
        "$SCRIPT_DIR/cleanup.sh"
        "$SCRIPT_DIR/validate-data.sh"
        "$SCRIPT_DIR/../schemas/basic-types.cql"
        "$SCRIPT_DIR/../schemas/collections.cql"
        "$SCRIPT_DIR/../schemas/time-series.cql"
        "$SCRIPT_DIR/../schemas/wide-rows.cql"
    )
    
    local all_files_exist=true
    
    for file in "${required_files[@]}"; do
        if [ -f "$file" ]; then
            log_success "File exists: $(basename "$file")"
        else
            log_error "File missing: $(basename "$file")"
            all_files_exist=false
        fi
    done
    
    if [ "$all_files_exist" = false ]; then
        return 1
    fi
    
    # Test execute permissions
    local executable_files=(
        "$SCRIPT_DIR/generate-all-test-data.sh"
        "$SCRIPT_DIR/export-sstables.sh"
        "$SCRIPT_DIR/cleanup.sh"
        "$SCRIPT_DIR/validate-data.sh"
    )
    
    for file in "${executable_files[@]}"; do
        if [ -x "$file" ]; then
            log_success "File is executable: $(basename "$file")"
        else
            log_error "File not executable: $(basename "$file")"
            all_files_exist=false
        fi
    done
    
    if [ "$all_files_exist" = false ]; then
        return 1
    fi
}

# Test Docker Compose configuration
test_docker_compose_config() {
    log_info "Testing Docker Compose configuration..."
    
    if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
        log_error "Docker Compose file not found"
        return 1
    fi
    
    cd "$(dirname "$DOCKER_COMPOSE_FILE")"
    
    if docker-compose config >/dev/null 2>&1; then
        log_success "Docker Compose configuration is valid"
    else
        log_error "Docker Compose configuration is invalid"
        return 1
    fi
    
    # Test that required services are defined
    local required_services=(
        "cassandra-3-7"
        "cassandra-3-11" 
        "cassandra-4-0"
        "cassandra-4-1"
        "test-data-generator"
        "sstable-exporter"
    )
    
    for service in "${required_services[@]}"; do
        if docker-compose config --services | grep -q "^${service}$"; then
            log_success "Service defined: $service"
        else
            log_error "Service missing: $service"
            return 1
        fi
    done
}

# Test Python dependencies
test_python_dependencies() {
    log_info "Testing Python dependencies..."
    
    if command -v python3 >/dev/null 2>&1; then
        log_success "Python 3 is available"
    else
        log_error "Python 3 is not installed"
        return 1
    fi
    
    if command -v pip >/dev/null 2>&1 || command -v pip3 >/dev/null 2>&1; then
        log_success "pip is available"
    else
        log_error "pip is not installed"
        return 1
    fi
    
    # Test if we can install required packages (without actually installing)
    local required_packages=("cassandra-driver" "faker" "pyyaml")
    
    for package in "${required_packages[@]}"; do
        if python3 -c "import $package" 2>/dev/null; then
            log_success "Python package available: $package"
        else
            log_warning "Python package not installed (will be installed automatically): $package"
        fi
    done
}

# Test available disk space
test_disk_space() {
    log_info "Testing available disk space..."
    
    local available_kb=$(df . | tail -1 | awk '{print $4}')
    local available_gb=$((available_kb / 1024 / 1024))
    
    if [ $available_gb -ge 8 ]; then
        log_success "Sufficient disk space available: ${available_gb}GB"
    elif [ $available_gb -ge 4 ]; then
        log_warning "Limited disk space available: ${available_gb}GB (recommended: 8GB+)"
    else
        log_error "Insufficient disk space: ${available_gb}GB (minimum: 4GB)"
        return 1
    fi
}

# Test available memory
test_memory() {
    log_info "Testing available memory..."
    
    if command -v free >/dev/null 2>&1; then
        local available_mb=$(free -m | awk 'NR==2{print $7}')
        local available_gb=$((available_mb / 1024))
        
        if [ $available_gb -ge 4 ]; then
            log_success "Sufficient memory available: ${available_gb}GB"
        elif [ $available_gb -ge 2 ]; then
            log_warning "Limited memory available: ${available_gb}GB (recommended: 4GB+)"
        else
            log_error "Insufficient memory: ${available_gb}GB (minimum: 2GB)"
            return 1
        fi
    else
        log_warning "Cannot check memory availability (free command not found)"
    fi
}

# Test CI/CD workflow file
test_cicd_workflow() {
    log_info "Testing CI/CD workflow configuration..."
    
    local workflow_file="$SCRIPT_DIR/../../.github/workflows/test-data-generation.yml"
    
    if [ -f "$workflow_file" ]; then
        log_success "GitHub Actions workflow file exists"
        
        # Basic YAML syntax check
        if command -v python3 >/dev/null 2>&1; then
            if python3 -c "import yaml; yaml.safe_load(open('$workflow_file'))" 2>/dev/null; then
                log_success "Workflow file has valid YAML syntax"
            else
                log_error "Workflow file has invalid YAML syntax"
                return 1
            fi
        else
            log_warning "Cannot validate YAML syntax (Python not available)"
        fi
    else
        log_warning "GitHub Actions workflow file not found (CI/CD integration may not be available)"
    fi
}

# Run quick connectivity test
test_connectivity() {
    log_info "Testing Docker Hub connectivity..."
    
    if docker pull hello-world >/dev/null 2>&1; then
        log_success "Docker Hub connectivity working"
        docker rmi hello-world >/dev/null 2>&1 || true
    else
        log_error "Cannot connect to Docker Hub"
        return 1
    fi
}

# Generate test report
generate_report() {
    local total_tests=$((TESTS_PASSED + TESTS_FAILED))
    
    echo ""
    echo "=========================================="
    echo "CQLite Test Data System - Quick Test Report"
    echo "=========================================="
    echo ""
    echo "Total Tests: $total_tests"
    echo "Passed: $TESTS_PASSED"
    echo "Failed: $TESTS_FAILED"
    echo ""
    
    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}🎉 ALL TESTS PASSED${NC}"
        echo ""
        echo "✅ Your system is ready for CQLite test data generation!"
        echo ""
        echo "Next steps:"
        echo "1. Run full test data generation: cd test-data/docker && docker-compose up"
        echo "2. Validate generated data: cd test-data/scripts && ./validate-data.sh"
        echo "3. Clean up when done: ./cleanup.sh --all"
        echo ""
        return 0
    else
        echo -e "${RED}❌ SOME TESTS FAILED${NC}"
        echo ""
        echo "⚠️ Please address the failed tests before proceeding."
        echo ""
        echo "Common fixes:"
        echo "- Install Docker and Docker Compose"
        echo "- Start Docker daemon: sudo systemctl start docker"
        echo "- Free up disk space (need 8GB+)"
        echo "- Install Python 3 and pip"
        echo ""
        return 1
    fi
}

# Main execution
main() {
    echo "CQLite Test Data Generation - Quick Setup Test"
    echo "Issue #18: Docker-based test data generation"
    echo ""
    
    # Run all tests
    test_docker || true
    test_docker_compose || true
    test_directory_structure || true
    test_required_files || true
    test_docker_compose_config || true
    test_python_dependencies || true
    test_disk_space || true
    test_memory || true
    test_cicd_workflow || true
    test_connectivity || true
    
    # Generate final report
    generate_report
}

# Execute main function
main "$@"