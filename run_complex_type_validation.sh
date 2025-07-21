#!/bin/bash

# M3 Complex Type Validation Script
# Run comprehensive validation of complex types against real Cassandra data

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1"
    exit 1
}

# Parse command line arguments
MODE="all"
VERBOSE=false
ENABLE_STRESS=false
ITERATIONS=10000

while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--mode)
            MODE="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --enable-stress)
            ENABLE_STRESS=true
            shift
            ;;
        -i|--iterations)
            ITERATIONS="$2"
            shift 2
            ;;
        -h|--help)
            echo "M3 Complex Type Validation Script"
            echo ""
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -m, --mode MODE        Validation mode: all, validation, real-data, performance (default: all)"
            echo "  -v, --verbose          Enable verbose output"
            echo "  --enable-stress        Enable stress testing with large datasets"
            echo "  -i, --iterations N     Number of performance benchmark iterations (default: 10000)"
            echo "  -h, --help            Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                     # Run all validations"
            echo "  $0 -m validation       # Run only validation tests"
            echo "  $0 -m performance -i 50000  # Run performance tests with 50k iterations"
            echo "  $0 --enable-stress     # Include stress testing"
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            ;;
    esac
done

echo -e "${BLUE}🚀 M3 Complex Type Validation${NC}"
echo -e "${BLUE}=============================${NC}"
echo "📍 Project: CQLite M3"
echo "🎯 Mission: PROVE 100% Cassandra 5+ Complex Type Compatibility"
echo "⚡ Mode: $MODE"
echo "🔄 Iterations: $ITERATIONS"
echo "💪 Stress Testing: $([ "$ENABLE_STRESS" = true ] && echo "ENABLED" || echo "DISABLED")"
echo "🔍 Verbose: $([ "$VERBOSE" = true ] && echo "ENABLED" || echo "DISABLED")"
echo ""

# Check if test data exists
if [ ! -d "tests/cassandra-cluster/test-data" ] || [ -z "$(ls -A tests/cassandra-cluster/test-data 2>/dev/null)" ]; then
    warn "Test data not found. Generating complex type test data..."
    
    if [ -f "tests/cassandra-cluster/scripts/generate-complex-type-test-data.sh" ]; then
        log "Running test data generation script..."
        bash tests/cassandra-cluster/scripts/generate-complex-type-test-data.sh
    else
        warn "Test data generation script not found. Creating sample data directories..."
        mkdir -p tests/cassandra-cluster/test-data
        mkdir -p tests/schemas
        
        # Create a sample schema file
        cat > tests/schemas/sample_complex_types.json << 'EOF'
{
    "keyspace": "complex_types_test",
    "table": "sample_table",
    "partition_keys": [
        {"name": "id", "type": "uuid", "position": 0}
    ],
    "clustering_keys": [],
    "columns": [
        {"name": "id", "type": "uuid", "nullable": false},
        {"name": "list_col", "type": "list<text>", "nullable": true},
        {"name": "set_col", "type": "set<int>", "nullable": true},
        {"name": "map_col", "type": "map<text,int>", "nullable": true}
    ]
}
EOF
    fi
fi

# Build the project
log "🔨 Building project..."
if ! cargo build --release; then
    error "Failed to build project"
fi

# Create output directory
OUTPUT_DIR="target/validation-reports"
mkdir -p "$OUTPUT_DIR"

# Prepare command arguments
ARGS=(
    --mode "$MODE"
    --test-data-dir tests/cassandra-cluster/test-data
    --schema-dir tests/schemas
    --output-dir "$OUTPUT_DIR"
    --iterations "$ITERATIONS"
    --cassandra-version 5.0
)

if [ "$VERBOSE" = true ]; then
    ARGS+=(--verbose)
fi

if [ "$ENABLE_STRESS" = true ]; then
    ARGS+=(--enable-stress)
fi

# Run the validation suite
log "🧪 Running validation tests..."
echo "📂 Test Data: tests/cassandra-cluster/test-data"
echo "📋 Schemas: tests/schemas"
echo "📄 Reports: $OUTPUT_DIR"
echo ""

if cargo run --release --bin complex_type_validation_runner -- "${ARGS[@]}"; then
    log "✅ Validation completed successfully!"
    echo ""
    echo -e "${GREEN}📊 VALIDATION SUMMARY${NC}"
    echo -e "${GREEN}=====================${NC}"
    
    # Show report files if they exist
    if [ -d "$OUTPUT_DIR" ]; then
        echo "📄 Generated Reports:"
        find "$OUTPUT_DIR" -name "*.json" -o -name "*.md" | while read -r file; do
            echo "  📋 $(basename "$file")"
        done
        echo ""
        echo "🔍 View detailed reports: ls -la $OUTPUT_DIR/"
    fi
    
    echo -e "${GREEN}🎯 M3 Complex Types: VALIDATION PASSED${NC}"
    echo -e "${GREEN}🏆 Cassandra 5+ Compatibility: VERIFIED${NC}"
    
else
    error "❌ Validation failed! Check the output above and reports in $OUTPUT_DIR/"
fi

echo ""
log "🎉 M3 Complex Type Validation Complete!"