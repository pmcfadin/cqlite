#!/bin/bash
# Setup script for CQLite Cassandra compatibility testing

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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

print_status "🚀 Setting up CQLite Cassandra Compatibility Testing Environment"

# Check if we're in the right directory
if [[ ! -f "tests/compatibility/Cargo.toml" ]]; then
    print_error "Please run this script from the CQLite project root directory"
    exit 1
fi

# Check prerequisites
print_status "Checking prerequisites..."

# Check Docker
if ! command -v docker &> /dev/null; then
    print_error "Docker is required but not installed"
    print_status "Please install Docker: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check Docker Compose
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    print_error "Docker Compose is required but not installed"
    print_status "Please install Docker Compose: https://docs.docker.com/compose/install/"
    exit 1
fi

# Check Rust
if ! command -v cargo &> /dev/null; then
    print_error "Rust/Cargo is required but not installed"
    print_status "Please install Rust: https://rustup.rs/"
    exit 1
fi

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    print_error "Docker daemon is not running"
    print_status "Please start Docker and try again"
    exit 1
fi

print_success "Prerequisites check passed"

# Create necessary directories
print_status "Creating directories..."
mkdir -p tests/compatibility/{data,reports,results}
mkdir -p tests/compatibility/data/{cassandra-4.0,cassandra-4.1,cassandra-5.0,cassandra-5.1,cassandra-6.0}
print_success "Directories created"

# Build the compatibility test suite
print_status "Building compatibility test suite..."
cd tests/compatibility

# Add required dependencies if not present
if ! grep -q "bc = " Cargo.toml; then
    print_status "Adding missing dependencies..."
    cargo add bc --optional
fi

# Build the project
cargo build --release --bins
if [[ $? -ne 0 ]]; then
    print_error "Failed to build compatibility test suite"
    exit 1
fi

cd - > /dev/null
print_success "Compatibility test suite built successfully"

# Make scripts executable
print_status "Making scripts executable..."
chmod +x tests/compatibility/scripts/*.sh
print_success "Scripts are now executable"

# Download Cassandra Docker images
print_status "Downloading Cassandra Docker images..."
CASSANDRA_VERSIONS=("4.0" "4.1" "5.0" "5.1")

for version in "${CASSANDRA_VERSIONS[@]}"; do
    print_status "Pulling cassandra:$version..."
    if docker pull "cassandra:$version"; then
        print_success "✅ cassandra:$version"
    else
        print_warning "⚠️ Failed to pull cassandra:$version (might not be available yet)"
    fi
done

# Test Docker setup
print_status "Testing Docker setup..."
if docker run --rm cassandra:4.0 echo "Docker test successful"; then
    print_success "Docker setup is working"
else
    print_error "Docker setup test failed"
    exit 1
fi

# Create configuration files
print_status "Creating configuration files..."

# Create .env file for configuration
cat > tests/compatibility/.env << 'EOF'
# CQLite Cassandra Compatibility Testing Configuration

# Default test suite level
DEFAULT_TEST_SUITE=comprehensive

# Default output directory
OUTPUT_DIR=./results

# Docker network name
DOCKER_NETWORK=cassandra-compat

# Enable verbose logging
VERBOSE=false

# Cassandra versions to test
CASSANDRA_VERSIONS=4.0,4.1,5.0,5.1

# Test data configuration
TEST_ROWS=10000
ENABLE_COMPLEX_TYPES=true

# Performance thresholds
MIN_COMPATIBILITY_SCORE=95
PERFORMANCE_THRESHOLD=0.8

# Monitoring configuration
MONITOR_INTERVAL=24h
WEBHOOK_URL=

# GitHub Actions integration
GITHUB_TOKEN=
EOF

# Create README for the compatibility testing framework
cat > tests/compatibility/README.md << 'EOF'
# 🐘 CQLite Cassandra Compatibility Testing Framework

This framework provides comprehensive compatibility testing for CQLite across different Cassandra versions.

## Quick Start

```bash
# Setup (run once)
./scripts/setup.sh

# Test specific version
./scripts/compatibility_checker.sh --version 5.1 --test-suite comprehensive

# Run full compatibility matrix
./scripts/compatibility_checker.sh --matrix

# Monitor continuously
./scripts/compatibility_checker.sh --monitor 24h
```

## Components

- **Version Manager**: Manages different Cassandra versions via Docker
- **Format Detective**: Detects changes in SSTable formats between versions
- **Data Generator**: Creates test data across all Cassandra versions
- **Compatibility Suite**: Runs comprehensive compatibility tests
- **CI Integration**: GitHub Actions for automated testing

## Test Levels

- **Basic**: Core functionality and basic data types
- **Comprehensive**: All data types, collections, UDTs
- **Full**: Performance testing, edge cases, stress testing

## Docker Services

Use `docker-compose up` to start all Cassandra versions:

- Cassandra 4.0: Port 9042
- Cassandra 4.1: Port 9043
- Cassandra 5.0: Port 9044
- Cassandra 5.1: Port 9045
- Cassandra 6.0: Port 9046 (future)

## Reports

All test results are saved in the `results/` directory:

- `compatibility-matrix.json`: Full compatibility matrix
- `compatibility-{version}.json`: Individual version results
- `compatibility-dashboard.md`: Human-readable summary

## Future-Proofing

This framework automatically:

1. ✅ Tests new Cassandra releases as Docker images become available
2. 🔍 Detects format changes in SSTable files
3. 📊 Tracks performance regressions
4. 🚨 Alerts on compatibility issues
5. 📈 Provides trend analysis over time

## Architecture

```
tests/compatibility/
├── src/
│   ├── version_manager.rs    # Docker-based version management
│   ├── format_detective.rs   # SSTable format analysis
│   ├── data_generator.rs     # Test data generation
│   └── suite.rs              # Main test suite
├── scripts/
│   ├── compatibility_checker.sh  # Main test runner
│   └── setup.sh                  # Environment setup
├── docker/
│   └── docker-compose.yml       # Multi-version Cassandra setup
└── results/                     # Test results and reports
```

This ensures CQLite stays compatible as Cassandra evolves! 🚀
EOF

print_success "Configuration files created"

# Test the compatibility checker
print_status "Testing compatibility checker..."
cd tests/compatibility

if ./target/release/compatibility-checker --help > /dev/null; then
    print_success "Compatibility checker is working"
else
    print_error "Compatibility checker test failed"
    exit 1
fi

cd - > /dev/null

# Create sample test run
print_status "Running sample compatibility test..."
if tests/compatibility/scripts/compatibility_checker.sh --version 4.0 --test-suite basic --no-cleanup; then
    print_success "Sample test completed successfully"
else
    print_warning "Sample test had issues (this may be expected if Cassandra isn't fully ready)"
fi

print_success "🎉 CQLite Cassandra Compatibility Testing Environment Setup Complete!"

cat << 'EOF'

📋 Next Steps:

1. 🧪 Run a quick test:
   ./tests/compatibility/scripts/compatibility_checker.sh --version 4.0 --test-suite basic

2. 🔄 Run full compatibility matrix:
   ./tests/compatibility/scripts/compatibility_checker.sh --matrix

3. 👁️ Setup continuous monitoring:
   ./tests/compatibility/scripts/compatibility_checker.sh --monitor 24h

4. 📊 View results:
   open tests/compatibility/results/compatibility-dashboard.md

5. 🚀 Integrate with CI:
   The GitHub Actions workflow is already configured in .github/workflows/

📚 Documentation: tests/compatibility/README.md
🔧 Configuration: tests/compatibility/.env
📈 Results: tests/compatibility/results/

EOF