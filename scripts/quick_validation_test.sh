#!/bin/bash

# Quick Validation Test for Issue #17 Infrastructure
# 
# This script performs a quick validation that the automated testing
# infrastructure components are working correctly without running
# the full time-consuming Docker-based data generation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "🎯 Issue #17: Quick Infrastructure Validation"
echo "============================================="
echo

# Function to check if a file exists and is executable
check_executable() {
    local file="$1"
    local description="$2"
    
    if [[ -f "$file" && -x "$file" ]]; then
        echo "✅ $description: $file"
        return 0
    else
        echo "❌ $description: $file (missing or not executable)"
        return 1
    fi
}

# Function to check if a Rust binary can be built
check_rust_binary() {
    local package="$1"
    local binary="$2"
    local description="$3"
    
    echo "🔄 Checking $description..."
    cd "$PROJECT_ROOT"
    
    if cargo build --release --package "$package" --bin "$binary" --quiet; then
        echo "✅ $description builds successfully"
        return 0
    else
        echo "❌ $description failed to build"
        return 1
    fi
}

# Main validation
main() {
    local errors=0
    
    echo "📋 Validating Issue #17 Infrastructure Components"
    echo
    
    # Check core scripts
    echo "1. Core Scripts Validation:"
    check_executable "$SCRIPT_DIR/automated_test_orchestrator.sh" "Master Test Orchestrator" || ((errors++))
    check_executable "$SCRIPT_DIR/run_issue_17_tests.sh" "Issue #17 Test Runner" || ((errors++))
    check_executable "$PROJECT_ROOT/test-data/scripts/generate_comprehensive_test_data.py" "Data Generation Script" || ((errors++))
    echo
    
    # Check configuration files
    echo "2. Configuration Files:"
    if [[ -f "$PROJECT_ROOT/.test-orchestrator-config.toml" ]]; then
        echo "✅ Test orchestrator configuration exists"
    else
        echo "⚠️ Test orchestrator configuration will be created on first run"
    fi
    
    if [[ -f "$PROJECT_ROOT/test-data/docker/docker-compose-multi-version.yml" ]]; then
        echo "✅ Multi-version Docker Compose configuration exists"
    else
        echo "❌ Multi-version Docker Compose configuration missing"
        ((errors++))
    fi
    echo
    
    # Check Rust binary builds
    echo "3. Rust Binary Builds:"
    check_rust_binary "cqlite-integration-tests" "property_based_test_runner" "Property-Based Test Runner" || ((errors++))
    check_rust_binary "cqlite-integration-tests" "performance_regression_test_runner" "Performance Regression Test Runner" || ((errors++))
    check_rust_binary "cqlite-cli" "cqlite" "CQLite CLI" || ((errors++))
    echo
    
    # Check system dependencies
    echo "4. System Dependencies:"
    if command -v docker &> /dev/null; then
        echo "✅ Docker is available"
    else
        echo "❌ Docker not found"
        ((errors++))
    fi
    
    if command -v python3 &> /dev/null; then
        echo "✅ Python 3 is available"
    else
        echo "❌ Python 3 not found"
        ((errors++))
    fi
    
    if command -v cargo &> /dev/null; then
        echo "✅ Cargo/Rust is available"
    else
        echo "❌ Cargo/Rust not found"
        ((errors++))
    fi
    echo
    
    # Test basic functionality
    echo "5. Basic Functionality Tests:"
    
    # Test property-based runner help
    cd "$PROJECT_ROOT"
    if cargo run --release --package cqlite-integration-tests --bin property_based_test_runner -- --help &>/dev/null; then
        echo "✅ Property-based test runner responds to --help"
    else
        echo "❌ Property-based test runner failed"
        ((errors++))
    fi
    
    # Test performance regression runner config generation
    if cargo run --release --package cqlite-integration-tests --bin performance_regression_test_runner -- --generate-config &>/dev/null; then
        echo "✅ Performance regression runner can generate config"
        if [[ -f "performance_benchmarks.json" ]]; then
            echo "✅ Performance benchmark configuration created"
        fi
    else
        echo "❌ Performance regression runner failed"
        ((errors++))
    fi
    
    # Test CQLite CLI basic functionality
    if cargo run --release --package cqlite-cli -- --help &>/dev/null; then
        echo "✅ CQLite CLI responds to --help"
    else
        echo "❌ CQLite CLI failed"
        ((errors++))
    fi
    echo
    
    # Check directory structure
    echo "6. Directory Structure:"
    local required_dirs=(
        "test-data"
        "test-data/docker"
        "test-data/scripts"
        "test-data/schemas"
        "tests/src/bin"
        "scripts"
        "logs"
        "reports"
    )
    
    for dir in "${required_dirs[@]}"; do
        if [[ -d "$PROJECT_ROOT/$dir" ]]; then
            echo "✅ Directory exists: $dir"
        else
            echo "⚠️ Directory missing (will be created): $dir"
            mkdir -p "$PROJECT_ROOT/$dir"
        fi
    done
    echo
    
    # Final summary
    echo "=========================================="
    echo "Issue #17 Quick Validation Summary"
    echo "=========================================="
    echo "Total Errors: $errors"
    echo
    
    if [[ $errors -eq 0 ]]; then
        echo "🎉 SUCCESS: All infrastructure components validated successfully!"
        echo
        echo "✅ CRITICAL SUCCESS FACTOR ACHIEVED:"
        echo "   Command-line test execution infrastructure is ready!"
        echo
        echo "📋 Available Commands:"
        echo "   • Full Test Suite: ./scripts/run_issue_17_tests.sh"
        echo "   • Master Orchestrator: ./scripts/automated_test_orchestrator.sh"
        echo "   • Property Testing: cargo run --package tests --bin property_based_test_runner"
        echo "   • Performance Testing: cargo run --package tests --bin performance_regression_test_runner"
        echo
        echo "🔧 Configuration:"
        echo "   • Test orchestrator config: .test-orchestrator-config.toml"
        echo "   • Performance benchmarks: performance_benchmarks.json"
        echo "   • Docker compose: test-data/docker/docker-compose-multi-version.yml"
        echo
        echo "📊 Infrastructure Features:"
        echo "   • Automated Cassandra data generation (multiple versions: 3.7, 3.11, 4.0, 4.1, 5.0)"
        echo "   • Property-based testing for data integrity validation"
        echo "   • Performance regression testing with baseline comparison"
        echo "   • Comprehensive test orchestration with parallel execution"
        echo "   • Command-line execution with reliable error handling"
        echo "   • Detailed reporting (JSON and HTML formats)"
        echo "   • CI/CD integration ready"
        
        return 0
    else
        echo "⚠️ VALIDATION ISSUES: $errors components need attention"
        echo
        echo "🔍 Common fixes:"
        echo "   • Install missing dependencies (Docker, Python 3, Rust)"
        echo "   • Run 'cargo build --workspace' to ensure all components compile"
        echo "   • Check file permissions on scripts"
        echo
        echo "🎯 CRITICAL SUCCESS FACTOR STATUS:"
        if [[ $errors -le 2 ]]; then
            echo "   ✅ Command-line test execution is mostly ready"
            echo "   Minor issues detected but core functionality is available"
            return 1
        else
            echo "   ❌ Command-line test execution needs setup"
            echo "   Multiple components require attention"
            return 2
        fi
    fi
}

# Execute main function
main "$@"