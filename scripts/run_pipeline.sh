#!/bin/bash
# Test Data Pipeline Runner for CQLite
# This script provides a convenient interface for running the test data pipeline

set -euo pipefail

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Default configuration
CONFIG_FILE="$PROJECT_ROOT/config/pipeline_config.yml"
CQLITE_BINARY=""
FORCE_REGENERATE=false
PARALLEL_JOBS=4
VERBOSE=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Help function
show_help() {
    cat << EOF
CQLite Test Data Pipeline Runner

USAGE:
    $0 [COMMAND] [OPTIONS]

COMMANDS:
    generate        Generate test data from scratch
    validate        Validate existing test data
    benchmark       Run performance benchmarks
    regression      Run regression tests
    ci              Run CI/CD pipeline validation
    cleanup         Clean up old data versions
    status          Show pipeline status
    setup           Initial pipeline setup
    help            Show this help message

OPTIONS:
    -c, --config FILE       Configuration file (default: config/pipeline_config.yml)
    -b, --cqlite-binary     Path to CQLite binary
    -f, --force             Force regeneration even if data is current
    -j, --jobs N            Number of parallel jobs (default: 4)
    -v, --verbose           Enable verbose output
    -h, --help              Show this help message

EXAMPLES:
    # Generate all test data
    $0 generate

    # Validate with custom CQLite binary
    $0 validate --cqlite-binary ./target/release/cqlite

    # Run full CI pipeline
    $0 ci --force

    # Check pipeline status
    $0 status

    # Setup pipeline for first time
    $0 setup
EOF
}

# Parse command line arguments
parse_args() {
    COMMAND=""

    while [[ $# -gt 0 ]]; do
        case $1 in
            generate|validate|benchmark|regression|ci|cleanup|status|setup|help)
                COMMAND="$1"
                shift
                ;;
            -c|--config)
                CONFIG_FILE="$2"
                shift 2
                ;;
            -b|--cqlite-binary)
                CQLITE_BINARY="$2"
                shift 2
                ;;
            -f|--force)
                FORCE_REGENERATE=true
                shift
                ;;
            -j|--jobs)
                PARALLEL_JOBS="$2"
                shift 2
                ;;
            -v|--verbose)
                VERBOSE=true
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done

    if [[ -z "$COMMAND" ]]; then
        log_error "No command specified"
        show_help
        exit 1
    fi
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 is required but not installed"
        exit 1
    fi

    # Check Python packages
    local required_packages=("pyyaml" "requests")
    for package in "${required_packages[@]}"; do
        if ! python3 -c "import $package" 2>/dev/null; then
            log_warning "Python package '$package' not found, installing..."
            pip3 install "$package" || {
                log_error "Failed to install Python package: $package"
                exit 1
            }
        fi
    done

    # Check Rust/Cargo if CQLite needs to be built
    if [[ -z "$CQLITE_BINARY" ]]; then
        if [[ -f "$PROJECT_ROOT/target/release/cqlite" ]]; then
            CQLITE_BINARY="$PROJECT_ROOT/target/release/cqlite"
        elif [[ -f "$PROJECT_ROOT/target/debug/cqlite" ]]; then
            CQLITE_BINARY="$PROJECT_ROOT/target/debug/cqlite"
        elif command -v cargo &> /dev/null; then
            log_info "Building CQLite..."
            cd "$PROJECT_ROOT"
            cargo build --release
            CQLITE_BINARY="$PROJECT_ROOT/target/release/cqlite"
        else
            log_error "CQLite binary not found and Cargo not available for building"
            exit 1
        fi
    fi

    # Verify CQLite binary
    if [[ ! -f "$CQLITE_BINARY" ]]; then
        log_error "CQLite binary not found at: $CQLITE_BINARY"
        exit 1
    fi

    # Check configuration file
    if [[ ! -f "$CONFIG_FILE" ]]; then
        log_warning "Configuration file not found: $CONFIG_FILE"
        log_info "Creating default configuration..."
        mkdir -p "$(dirname "$CONFIG_FILE")"
        # This would create a default config file
    fi

    log_success "Prerequisites check passed"
}

# Setup pipeline
setup_pipeline() {
    log_info "Setting up test data pipeline..."

    # Create directory structure
    local dirs=(
        "$PROJECT_ROOT/test-data"
        "$PROJECT_ROOT/test-data/datasets"
        "$PROJECT_ROOT/test-data/versions"
        "$PROJECT_ROOT/test-data/benchmarks"
        "$PROJECT_ROOT/test-data/reports"
        "$PROJECT_ROOT/ci-reports"
        "$PROJECT_ROOT/ci-artifacts"
        "$PROJECT_ROOT/logs"
        "$PROJECT_ROOT/config"
    )

    for dir in "${dirs[@]}"; do
        mkdir -p "$dir"
        log_info "Created directory: $dir"
    done

    # Setup Git hooks if in a Git repository
    if [[ -d "$PROJECT_ROOT/.git" ]]; then
        log_info "Setting up Git pre-commit hooks..."
        python3 "$SCRIPT_DIR/ci_integration.py" setup-hooks --config "$CONFIG_FILE"
    fi

    # Generate CI configuration files
    log_info "Generating CI configuration files..."

    # GitHub Actions
    local github_workflow_dir="$PROJECT_ROOT/.github/workflows"
    mkdir -p "$github_workflow_dir"
    python3 "$SCRIPT_DIR/ci_integration.py" generate-config --ci-type github > "$github_workflow_dir/test-data-validation.yml"
    log_info "Created GitHub Actions workflow: $github_workflow_dir/test-data-validation.yml"

    # Jenkins pipeline
    python3 "$SCRIPT_DIR/ci_integration.py" generate-config --ci-type jenkins > "$PROJECT_ROOT/Jenkinsfile.test-data"
    log_info "Created Jenkins pipeline: $PROJECT_ROOT/Jenkinsfile.test-data"

    log_success "Pipeline setup completed"
}

# Generate test data
generate_data() {
    log_info "Generating test data..."

    local args=()

    if [[ "$FORCE_REGENERATE" == "true" ]]; then
        args+=(--force)
    fi

    if [[ -n "$CQLITE_BINARY" ]]; then
        # Update config with CQLite binary path
        export CQLITE_BINARY_PATH="$CQLITE_BINARY"
    fi

    # Run data generation
    cd "$PROJECT_ROOT"
    python3 "$SCRIPT_DIR/data_pipeline_manager.py" generate \
        --config "$CONFIG_FILE" \
        --parallel "$PARALLEL_JOBS" \
        "${args[@]}"

    log_success "Test data generation completed"
}

# Validate test data
validate_data() {
    log_info "Validating test data..."

    cd "$PROJECT_ROOT"

    # Run validation
    python3 "$SCRIPT_DIR/validate_sstables.py" \
        test-data/datasets \
        --cqlite-binary "$CQLITE_BINARY" \
        --output-report "ci-reports/validation_$(date +%Y%m%d_%H%M%S).json" \
        --recursive

    log_success "Test data validation completed"
}

# Run benchmarks
run_benchmarks() {
    log_info "Running performance benchmarks..."

    cd "$PROJECT_ROOT"
    python3 "$SCRIPT_DIR/data_pipeline_manager.py" benchmark \
        --config "$CONFIG_FILE"

    log_success "Performance benchmarks completed"
}

# Run regression tests
run_regression_tests() {
    log_info "Running regression tests..."

    cd "$PROJECT_ROOT"
    python3 "$SCRIPT_DIR/data_pipeline_manager.py" regression \
        --config "$CONFIG_FILE"

    local exit_code=$?
    if [[ $exit_code -eq 0 ]]; then
        log_success "Regression tests passed"
    else
        log_error "Regression tests failed"
        exit $exit_code
    fi
}

# Run CI pipeline
run_ci_pipeline() {
    log_info "Running CI/CD pipeline validation..."

    cd "$PROJECT_ROOT"

    # Determine PR number if available
    local pr_number=""
    if [[ -n "${GITHUB_REF:-}" ]]; then
        pr_number=$(echo "$GITHUB_REF" | grep -o '[0-9]\+' || echo "")
    fi

    local args=()
    if [[ -n "$pr_number" ]]; then
        args+=(--pr-number "$pr_number")
    fi

    if [[ -n "$CQLITE_BINARY" ]]; then
        args+=(--cqlite-binary "$CQLITE_BINARY")
    fi

    python3 "$SCRIPT_DIR/ci_integration.py" pr-validation \
        --config "$CONFIG_FILE" \
        "${args[@]}"

    local exit_code=$?
    if [[ $exit_code -eq 0 ]]; then
        log_success "CI pipeline validation passed"
    else
        log_error "CI pipeline validation failed"
        exit $exit_code
    fi
}

# Cleanup old data
cleanup_data() {
    log_info "Cleaning up old data versions..."

    cd "$PROJECT_ROOT"
    python3 "$SCRIPT_DIR/data_pipeline_manager.py" cleanup \
        --config "$CONFIG_FILE"

    log_success "Data cleanup completed"
}

# Show pipeline status
show_status() {
    log_info "Pipeline status:"

    cd "$PROJECT_ROOT"
    python3 "$SCRIPT_DIR/data_pipeline_manager.py" status \
        --config "$CONFIG_FILE"
}

# Set verbose mode
setup_verbose() {
    if [[ "$VERBOSE" == "true" ]]; then
        set -x
        export PYTHONPATH="$SCRIPT_DIR:$PYTHONPATH"
    fi
}

# Main execution
main() {
    parse_args "$@"
    setup_verbose

    log_info "Starting CQLite test data pipeline: $COMMAND"
    log_info "Configuration: $CONFIG_FILE"
    log_info "CQLite binary: ${CQLITE_BINARY:-auto-detect}"
    log_info "Parallel jobs: $PARALLEL_JOBS"

    case "$COMMAND" in
        setup)
            check_prerequisites
            setup_pipeline
            ;;
        generate)
            check_prerequisites
            generate_data
            ;;
        validate)
            check_prerequisites
            validate_data
            ;;
        benchmark)
            check_prerequisites
            run_benchmarks
            ;;
        regression)
            check_prerequisites
            run_regression_tests
            ;;
        ci)
            check_prerequisites
            run_ci_pipeline
            ;;
        cleanup)
            cleanup_data
            ;;
        status)
            show_status
            ;;
        help)
            show_help
            ;;
        *)
            log_error "Unknown command: $COMMAND"
            show_help
            exit 1
            ;;
    esac

    log_success "Pipeline command '$COMMAND' completed successfully"
}

# Execute main function with all arguments
main "$@"