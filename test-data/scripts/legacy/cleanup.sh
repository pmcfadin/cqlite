#!/bin/bash

# CQLite Test Data Cleanup Script
# Cleans up Docker containers, volumes, and generated data
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

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Display usage information
usage() {
    cat << EOF
CQLite Test Data Cleanup Script

Usage: $0 [OPTIONS]

OPTIONS:
    --containers-only    Stop and remove only containers (keep volumes)
    --volumes-only       Remove only volumes (containers must be stopped first)
    --generated-data     Remove only generated test data files
    --all               Remove everything (containers, volumes, generated data) [default]
    --reset             Full reset: cleanup everything and rebuild
    -f, --force         Skip confirmation prompts
    -h, --help          Show this help message

Examples:
    $0                          # Interactive cleanup of everything
    $0 --all --force           # Force cleanup of everything
    $0 --containers-only       # Only stop and remove containers
    $0 --generated-data --force # Remove generated data files only
    $0 --reset                 # Full reset and rebuild

EOF
}

# Confirmation prompt
confirm() {
    local message=$1
    local force=${2:-false}
    
    if [ "$force" = true ]; then
        return 0
    fi
    
    echo -e "${YELLOW}[CONFIRM]${NC} $message (y/N): "
    read -r response
    case "$response" in
        [yY][eE][sS]|[yY])
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# Stop and remove Docker containers
cleanup_containers() {
    local force=$1
    
    log_info "Checking for running CQLite test containers..."
    
    if ! command -v docker >/dev/null 2>&1; then
        log_error "Docker is not installed or not in PATH"
        return 1
    fi
    
    # Check if docker-compose file exists
    if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
        log_warning "Docker compose file not found at $DOCKER_COMPOSE_FILE"
        return 1
    fi
    
    cd "$(dirname "$DOCKER_COMPOSE_FILE")"
    
    # Get list of containers
    local containers=$(docker-compose ps -q 2>/dev/null || true)
    
    if [ -z "$containers" ]; then
        log_info "No CQLite test containers found"
        return 0
    fi
    
    if confirm "Stop and remove all CQLite test containers?" "$force"; then
        log_info "Stopping CQLite test containers..."
        docker-compose down
        
        log_info "Removing CQLite test containers..."
        docker-compose rm -f
        
        # Remove any orphaned containers
        local cqlite_containers=$(docker ps -aq --filter "name=cqlite-" 2>/dev/null || true)
        if [ -n "$cqlite_containers" ]; then
            log_info "Removing orphaned CQLite containers..."
            docker rm -f $cqlite_containers
        fi
        
        log_success "Containers cleanup completed"
    else
        log_info "Container cleanup cancelled"
    fi
}

# Remove Docker volumes
cleanup_volumes() {
    local force=$1
    
    log_info "Checking for CQLite test volumes..."
    
    if ! command -v docker >/dev/null 2>&1; then
        log_error "Docker is not installed or not in PATH"
        return 1
    fi
    
    # Get list of CQLite volumes
    local volumes=$(docker volume ls -q --filter "name=cassandra-" 2>/dev/null || true)
    
    if [ -z "$volumes" ]; then
        log_info "No CQLite test volumes found"
        return 0
    fi
    
    if confirm "Remove all CQLite test volumes? This will delete all Cassandra data." "$force"; then
        log_info "Removing CQLite test volumes..."
        
        for volume in $volumes; do
            log_info "  Removing volume: $volume"
            docker volume rm "$volume" 2>/dev/null || log_warning "Failed to remove volume: $volume"
        done
        
        # Also remove the compose-defined volumes
        if [ -f "$DOCKER_COMPOSE_FILE" ]; then
            cd "$(dirname "$DOCKER_COMPOSE_FILE")"
            docker-compose down -v 2>/dev/null || true
        fi
        
        log_success "Volumes cleanup completed"
    else
        log_info "Volume cleanup cancelled"
    fi
}

# Remove generated test data files
cleanup_generated_data() {
    local force=$1
    local generated_dir="$SCRIPT_DIR/../generated"
    
    log_info "Checking for generated test data..."
    
    if [ ! -d "$generated_dir" ]; then
        log_info "No generated data directory found"
        return 0
    fi
    
    local data_size=$(du -sh "$generated_dir" 2>/dev/null | cut -f1 || echo "unknown")
    
    if confirm "Remove all generated test data? ($data_size in $generated_dir)" "$force"; then
        log_info "Removing generated test data..."
        
        rm -rf "$generated_dir"/*
        
        # Recreate directory structure
        mkdir -p "$generated_dir"/{v3.7,v3.11,v4.0,v4.1}
        
        log_success "Generated data cleanup completed"
    else
        log_info "Generated data cleanup cancelled"
    fi
}

# Clean up temporary files and logs
cleanup_temp_files() {
    log_info "Cleaning up temporary files..."
    
    # Remove Python cache files
    find "$SCRIPT_DIR" -name "*.pyc" -delete 2>/dev/null || true
    find "$SCRIPT_DIR" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    
    # Remove temporary data generation files
    rm -f /tmp/data_generator_*.py 2>/dev/null || true
    rm -f /tmp/metadata_generator_*.py 2>/dev/null || true
    
    # Remove log files
    rm -f "$SCRIPT_DIR"/*.log 2>/dev/null || true
    
    log_success "Temporary files cleanup completed"
}

# Full reset: cleanup and rebuild
full_reset() {
    local force=$1
    
    if confirm "Perform full reset? This will remove everything and rebuild the environment." "$force"; then
        log_info "Starting full reset..."
        
        cleanup_containers "$force"
        cleanup_volumes "$force"
        cleanup_generated_data "$force"
        cleanup_temp_files
        
        # Rebuild the environment
        log_info "Rebuilding test environment..."
        cd "$(dirname "$DOCKER_COMPOSE_FILE")"
        
        if docker-compose pull; then
            log_success "Docker images updated successfully"
        else
            log_warning "Failed to pull latest Docker images"
        fi
        
        log_success "Full reset completed"
    else
        log_info "Full reset cancelled"
    fi
}

# Display current status
show_status() {
    log_info "Current CQLite test environment status:"
    
    echo ""
    echo "Docker Containers:"
    if command -v docker >/dev/null 2>&1; then
        docker ps -a --filter "name=cqlite-" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || echo "  No CQLite containers found"
    else
        echo "  Docker not available"
    fi
    
    echo ""
    echo "Docker Volumes:"
    if command -v docker >/dev/null 2>&1; then
        docker volume ls --filter "name=cassandra-" --format "table {{.Name}}\t{{.Driver}}" 2>/dev/null || echo "  No CQLite volumes found"
    else
        echo "  Docker not available"
    fi
    
    echo ""
    echo "Generated Data:"
    local generated_dir="$SCRIPT_DIR/../generated"
    if [ -d "$generated_dir" ]; then
        local total_size=$(du -sh "$generated_dir" 2>/dev/null | cut -f1 || echo "unknown")
        local file_count=$(find "$generated_dir" -type f 2>/dev/null | wc -l || echo "unknown")
        echo "  Directory: $generated_dir"
        echo "  Size: $total_size"
        echo "  Files: $file_count"
        
        # Show version-specific data
        for version_dir in "$generated_dir"/v*; do
            if [ -d "$version_dir" ]; then
                local version=$(basename "$version_dir")
                local version_size=$(du -sh "$version_dir" 2>/dev/null | cut -f1 || echo "unknown")
                echo "    $version: $version_size"
            fi
        done
    else
        echo "  No generated data directory found"
    fi
    
    echo ""
}

# Main execution
main() {
    local containers_only=false
    local volumes_only=false
    local generated_data_only=false
    local all_cleanup=false
    local reset=false
    local force=false
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --containers-only)
                containers_only=true
                shift
                ;;
            --volumes-only)
                volumes_only=true
                shift
                ;;
            --generated-data)
                generated_data_only=true
                shift
                ;;
            --all)
                all_cleanup=true
                shift
                ;;
            --reset)
                reset=true
                shift
                ;;
            -f|--force)
                force=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
    
    # Default to all cleanup if no specific option is selected
    if ! $containers_only && ! $volumes_only && ! $generated_data_only && ! $reset; then
        all_cleanup=true
    fi
    
    log_info "Starting CQLite test environment cleanup..."
    
    # Show current status
    show_status
    
    # Perform cleanup based on options
    if $reset; then
        full_reset "$force"
    elif $all_cleanup; then
        cleanup_containers "$force"
        cleanup_volumes "$force"  
        cleanup_generated_data "$force"
        cleanup_temp_files
    else
        if $containers_only; then
            cleanup_containers "$force"
        fi
        
        if $volumes_only; then
            cleanup_volumes "$force"
        fi
        
        if $generated_data_only; then
            cleanup_generated_data "$force"
        fi
        
        cleanup_temp_files
    fi
    
    echo ""
    log_success "Cleanup process completed!"
    
    # Show final status
    show_status
}

# Execute main function
main "$@"