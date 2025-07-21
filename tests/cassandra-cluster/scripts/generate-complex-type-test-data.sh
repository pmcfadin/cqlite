#!/bin/bash

# Generate Complex Type Test Data for M3 Validation
# 
# This script creates comprehensive test data using real Cassandra to generate
# SSTable files with complex types for M3 compatibility validation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
TEST_DATA_DIR="$PROJECT_ROOT/tests/cassandra-cluster/test-data"
SCHEMA_DIR="$PROJECT_ROOT/tests/schemas"

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

# Check dependencies
check_dependencies() {
    log "Checking dependencies..."
    
    if ! command -v cqlsh &> /dev/null; then
        error "cqlsh is required but not found. Please install Cassandra tools."
    fi
    
    if ! command -v docker &> /dev/null; then
        error "Docker is required but not found."
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        error "Docker Compose is required but not found."
    fi
    
    log "✅ All dependencies found"
}

# Start Cassandra cluster
start_cassandra() {
    log "Starting Cassandra cluster for test data generation..."
    
    cd "$PROJECT_ROOT/tests/cassandra-cluster"
    
    # Stop any existing containers
    docker-compose down -v 2>/dev/null || true
    
    # Start fresh cluster
    docker-compose up -d cassandra
    
    # Wait for Cassandra to be ready
    log "Waiting for Cassandra to be ready..."
    local retries=60
    while [ $retries -gt 0 ]; do
        if docker-compose exec -T cassandra cqlsh -e "SELECT now() FROM system.local;" &>/dev/null; then
            log "✅ Cassandra is ready"
            return 0
        fi
        retries=$((retries - 1))
        sleep 5
        echo -n "."
    done
    
    error "Cassandra failed to start within timeout"
}

# Create keyspace and tables with complex types
create_test_schema() {
    log "Creating test schema with complex types..."
    
    cd "$PROJECT_ROOT/tests/cassandra-cluster"
    
    # Create the test schema
    docker-compose exec -T cassandra cqlsh << 'EOF'
-- Create keyspace for complex type testing
CREATE KEYSPACE IF NOT EXISTS complex_types_test 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE complex_types_test;

-- Drop existing tables
DROP TABLE IF EXISTS collection_types_test;
DROP TABLE IF EXISTS udt_test;
DROP TABLE IF EXISTS tuple_test;
DROP TABLE IF EXISTS nested_complex_test;
DROP TABLE IF EXISTS frozen_types_test;
DROP TABLE IF EXISTS edge_cases_test;

-- Create User Defined Types
CREATE TYPE IF NOT EXISTS address (
    street text,
    city text,
    zip_code text,
    country text
);

CREATE TYPE IF NOT EXISTS person (
    first_name text,
    last_name text,
    age int,
    address frozen<address>,
    phone_numbers set<text>,
    metadata map<text, text>
);

CREATE TYPE IF NOT EXISTS product (
    id uuid,
    name text,
    price decimal,
    categories set<text>,
    attributes map<text, text>,
    created_at timestamp
);

-- Table 1: Collection Types
CREATE TABLE collection_types_test (
    id uuid PRIMARY KEY,
    simple_list list<text>,
    int_list list<int>,
    uuid_list list<uuid>,
    simple_set set<text>,
    int_set set<int>,
    uuid_set set<uuid>,
    text_map map<text, text>,
    int_map map<text, int>,
    uuid_map map<text, uuid>,
    mixed_map map<int, text>,
    timestamp_map map<text, timestamp>,
    created_at timestamp
);

-- Table 2: UDT Types
CREATE TABLE udt_test (
    id uuid PRIMARY KEY,
    person_info person,
    address_info address,
    product_info product,
    person_list list<frozen<person>>,
    address_set set<frozen<address>>,
    product_map map<text, frozen<product>>,
    created_at timestamp
);

-- Table 3: Tuple Types  
CREATE TABLE tuple_test (
    id uuid PRIMARY KEY,
    simple_tuple tuple<text, int>,
    complex_tuple tuple<text, int, boolean, timestamp>,
    uuid_tuple tuple<uuid, text, int>,
    nested_tuple tuple<text, frozen<list<int>>, map<text, int>>,
    tuple_list list<frozen<tuple<text, int>>>,
    tuple_set set<frozen<tuple<text, int, boolean>>>,
    tuple_map map<text, frozen<tuple<text, int>>>,
    created_at timestamp
);

-- Table 4: Nested Complex Types
CREATE TABLE nested_complex_test (
    id uuid PRIMARY KEY,
    list_of_maps list<frozen<map<text, int>>>,
    map_of_lists map<text, frozen<list<text>>>,
    set_of_maps set<frozen<map<text, text>>>,
    map_of_sets map<text, frozen<set<int>>>,
    list_of_tuples list<frozen<tuple<text, int, boolean>>>,
    map_of_tuples map<text, frozen<tuple<text, list<int>>>>,
    list_of_udts list<frozen<person>>,
    map_of_udts map<text, frozen<address>>,
    ultra_nested map<text, frozen<list<frozen<map<text, frozen<set<int>>>>>>>,
    created_at timestamp
);

-- Table 5: Frozen Types
CREATE TABLE frozen_types_test (
    id uuid PRIMARY KEY,
    frozen_list frozen<list<text>>,
    frozen_set frozen<set<int>>,
    frozen_map frozen<map<text, int>>,
    frozen_tuple frozen<tuple<text, int, boolean>>,
    frozen_udt frozen<person>,
    frozen_nested frozen<map<text, list<int>>>,
    list_of_frozen list<frozen<set<text>>>,
    map_of_frozen map<text, frozen<list<int>>>,
    created_at timestamp
);

-- Table 6: Edge Cases and Stress Test
CREATE TABLE edge_cases_test (
    id uuid PRIMARY KEY,
    empty_list list<text>,
    empty_set set<int>,
    empty_map map<text, text>,
    null_list list<text>,
    null_set set<text>,
    null_map map<text, text>,
    large_list list<text>,
    large_set set<int>,
    large_map map<text, text>,
    max_nested map<text, frozen<list<frozen<map<text, frozen<set<frozen<tuple<text, int>>>>>>>>>,
    created_at timestamp
);

EOF
    
    if [ $? -eq 0 ]; then
        log "✅ Test schema created successfully"
    else
        error "Failed to create test schema"
    fi
}

# Generate comprehensive test data
generate_test_data() {
    log "Generating comprehensive test data..."
    
    cd "$PROJECT_ROOT/tests/cassandra-cluster"
    
    # Generate test data with CQL
    docker-compose exec -T cassandra cqlsh << 'EOF'
USE complex_types_test;

-- Insert Collection Types Test Data
INSERT INTO collection_types_test (id, simple_list, int_list, uuid_list, simple_set, int_set, uuid_set, text_map, int_map, uuid_map, mixed_map, timestamp_map, created_at)
VALUES (
    uuid(),
    ['hello', 'world', 'test', 'collection'],
    [1, 2, 3, 42, 100, 1000],
    [uuid(), uuid(), uuid()],
    {'alpha', 'beta', 'gamma', 'delta'},
    {1, 2, 3, 5, 8, 13, 21},
    {uuid(), uuid(), uuid()},
    {'key1': 'value1', 'key2': 'value2', 'type': 'collection_test'},
    {'count': 42, 'max': 1000, 'min': 1},
    {'primary': uuid(), 'secondary': uuid()},
    {1: 'first', 2: 'second', 3: 'third'},
    {'created': toTimestamp(now()), 'updated': toTimestamp(now())},
    toTimestamp(now())
);

-- Insert more collection test data with varying sizes
INSERT INTO collection_types_test (id, simple_list, int_list, simple_set, int_set, text_map, int_map, created_at)
VALUES (uuid(), ['a'], [1], {'single'}, {1}, {'single': 'value'}, {'count': 1}, toTimestamp(now()));

INSERT INTO collection_types_test (id, simple_list, int_list, simple_set, int_set, text_map, int_map, created_at)
VALUES (
    uuid(), 
    ['item1', 'item2', 'item3', 'item4', 'item5', 'item6', 'item7', 'item8', 'item9', 'item10'],
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
    {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'},
    {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
    {'k1': 'v1', 'k2': 'v2', 'k3': 'v3', 'k4': 'v4', 'k5': 'v5'},
    {'n1': 1, 'n2': 2, 'n3': 3, 'n4': 4, 'n5': 5},
    toTimestamp(now())
);

-- Insert UDT Test Data
INSERT INTO udt_test (id, person_info, address_info, product_info, created_at)
VALUES (
    uuid(),
    {
        first_name: 'John',
        last_name: 'Doe', 
        age: 30,
        address: {street: '123 Main St', city: 'Anytown', zip_code: '12345', country: 'USA'},
        phone_numbers: {'555-1234', '555-5678'},
        metadata: {'department': 'engineering', 'level': 'senior'}
    },
    {street: '456 Oak Ave', city: 'Somewhere', zip_code: '67890', country: 'USA'},
    {
        id: uuid(),
        name: 'Test Product',
        price: 99.99,
        categories: {'electronics', 'gadgets'},
        attributes: {'color': 'blue', 'size': 'medium'},
        created_at: toTimestamp(now())
    },
    toTimestamp(now())
);

-- Insert Tuple Test Data
INSERT INTO tuple_test (id, simple_tuple, complex_tuple, uuid_tuple, nested_tuple, tuple_list, tuple_set, tuple_map, created_at)
VALUES (
    uuid(),
    ('simple', 42),
    ('complex', 100, true, toTimestamp(now())),
    (uuid(), 'uuid_test', 123),
    ('nested', [1, 2, 3, 4, 5], {'a': 1, 'b': 2}),
    [('first', 1), ('second', 2), ('third', 3)],
    {('alpha', 1, true), ('beta', 2, false), ('gamma', 3, true)},
    {'tuple1': ('value1', 10), 'tuple2': ('value2', 20)},
    toTimestamp(now())
);

-- Insert Nested Complex Test Data
INSERT INTO nested_complex_test (id, list_of_maps, map_of_lists, set_of_maps, map_of_sets, list_of_tuples, map_of_tuples, ultra_nested, created_at)
VALUES (
    uuid(),
    [{'count': 1}, {'count': 2}, {'count': 3}],
    {'fruits': ['apple', 'banana'], 'colors': ['red', 'blue']},
    {{'type': 'test'}, {'type': 'production'}},
    {'primes': {2, 3, 5, 7}, 'evens': {2, 4, 6, 8}},
    [('first', 1, true), ('second', 2, false)],
    {'coord1': ('x', [1, 2, 3]), 'coord2': ('y', [4, 5, 6])},
    {'level1': [{'level2': {1, 2, 3}}, {'level2': {4, 5, 6}}]},
    toTimestamp(now())
);

-- Insert Frozen Types Test Data
INSERT INTO frozen_types_test (id, frozen_list, frozen_set, frozen_map, frozen_tuple, frozen_udt, frozen_nested, list_of_frozen, map_of_frozen, created_at)
VALUES (
    uuid(),
    ['frozen', 'list', 'test'],
    {1, 2, 3, 4, 5},
    {'frozen': 1, 'map': 2, 'test': 3},
    ('frozen', 42, true),
    {
        first_name: 'Frozen',
        last_name: 'User',
        age: 25,
        address: {street: '789 Frozen St', city: 'Coldtown', zip_code: '00000', country: 'Antarctica'},
        phone_numbers: {'000-0000'},
        metadata: {'type': 'frozen_test'}
    },
    {'nested': [1, 2, 3]},
    [{'a', 'b'}, {'c', 'd'}],
    {'list1': [1, 2], 'list2': [3, 4]},
    toTimestamp(now())
);

-- Insert Edge Cases Test Data
INSERT INTO edge_cases_test (id, empty_list, empty_set, empty_map, null_list, null_set, null_map, created_at)
VALUES (uuid(), [], {}, {}, null, null, null, toTimestamp(now()));

-- Insert large data for stress testing
INSERT INTO edge_cases_test (id, large_list, large_set, large_map, created_at)
VALUES (
    uuid(),
    ['item001', 'item002', 'item003', 'item004', 'item005', 'item006', 'item007', 'item008', 'item009', 'item010',
     'item011', 'item012', 'item013', 'item014', 'item015', 'item016', 'item017', 'item018', 'item019', 'item020',
     'item021', 'item022', 'item023', 'item024', 'item025', 'item026', 'item027', 'item028', 'item029', 'item030'],
    {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30},
    {'k001': 'v001', 'k002': 'v002', 'k003': 'v003', 'k004': 'v004', 'k005': 'v005',
     'k006': 'v006', 'k007': 'v007', 'k008': 'v008', 'k009': 'v009', 'k010': 'v010'},
    toTimestamp(now())
);

EOF

    if [ $? -eq 0 ]; then
        log "✅ Test data generated successfully"
    else
        error "Failed to generate test data"
    fi
}

# Export schema definitions
export_schemas() {
    log "Exporting schema definitions..."
    
    mkdir -p "$SCHEMA_DIR"
    cd "$PROJECT_ROOT/tests/cassandra-cluster"
    
    # Export table schemas as JSON
    docker-compose exec -T cassandra cqlsh -e "DESCRIBE KEYSPACE complex_types_test;" > "$SCHEMA_DIR/complex_types_test.cql"
    
    # Create JSON schema files for each table
    cat > "$SCHEMA_DIR/collection_types_test.json" << 'EOF'
{
    "keyspace": "complex_types_test",
    "table": "collection_types_test",
    "partition_keys": [
        {"name": "id", "type": "uuid", "position": 0}
    ],
    "clustering_keys": [],
    "columns": [
        {"name": "id", "type": "uuid", "nullable": false},
        {"name": "simple_list", "type": "list<text>", "nullable": true},
        {"name": "int_list", "type": "list<int>", "nullable": true},
        {"name": "uuid_list", "type": "list<uuid>", "nullable": true},
        {"name": "simple_set", "type": "set<text>", "nullable": true},
        {"name": "int_set", "type": "set<int>", "nullable": true},
        {"name": "uuid_set", "type": "set<uuid>", "nullable": true},
        {"name": "text_map", "type": "map<text,text>", "nullable": true},
        {"name": "int_map", "type": "map<text,int>", "nullable": true},
        {"name": "uuid_map", "type": "map<text,uuid>", "nullable": true},
        {"name": "mixed_map", "type": "map<int,text>", "nullable": true},
        {"name": "timestamp_map", "type": "map<text,timestamp>", "nullable": true},
        {"name": "created_at", "type": "timestamp", "nullable": true}
    ]
}
EOF

    cat > "$SCHEMA_DIR/udt_test.json" << 'EOF'
{
    "keyspace": "complex_types_test",
    "table": "udt_test",
    "partition_keys": [
        {"name": "id", "type": "uuid", "position": 0}
    ],
    "clustering_keys": [],
    "columns": [
        {"name": "id", "type": "uuid", "nullable": false},
        {"name": "person_info", "type": "person", "nullable": true},
        {"name": "address_info", "type": "address", "nullable": true},
        {"name": "product_info", "type": "product", "nullable": true},
        {"name": "person_list", "type": "list<frozen<person>>", "nullable": true},
        {"name": "address_set", "type": "set<frozen<address>>", "nullable": true},
        {"name": "product_map", "type": "map<text,frozen<product>>", "nullable": true},
        {"name": "created_at", "type": "timestamp", "nullable": true}
    ]
}
EOF

    cat > "$SCHEMA_DIR/tuple_test.json" << 'EOF'
{
    "keyspace": "complex_types_test",
    "table": "tuple_test",
    "partition_keys": [
        {"name": "id", "type": "uuid", "position": 0}
    ],
    "clustering_keys": [],
    "columns": [
        {"name": "id", "type": "uuid", "nullable": false},
        {"name": "simple_tuple", "type": "tuple<text,int>", "nullable": true},
        {"name": "complex_tuple", "type": "tuple<text,int,boolean,timestamp>", "nullable": true},
        {"name": "uuid_tuple", "type": "tuple<uuid,text,int>", "nullable": true},
        {"name": "nested_tuple", "type": "tuple<text,frozen<list<int>>,map<text,int>>", "nullable": true},
        {"name": "tuple_list", "type": "list<frozen<tuple<text,int>>>", "nullable": true},
        {"name": "tuple_set", "type": "set<frozen<tuple<text,int,boolean>>>", "nullable": true},
        {"name": "tuple_map", "type": "map<text,frozen<tuple<text,int>>>", "nullable": true},
        {"name": "created_at", "type": "timestamp", "nullable": true}
    ]
}
EOF

    log "✅ Schema definitions exported"
}

# Force SSTable flush and export
export_sstables() {
    log "Flushing and exporting SSTable files..."
    
    cd "$PROJECT_ROOT/tests/cassandra-cluster"
    
    # Force flush to create SSTable files
    docker-compose exec -T cassandra nodetool flush complex_types_test
    
    # Wait for flush to complete
    sleep 5
    
    # Create test data directory
    mkdir -p "$TEST_DATA_DIR"
    
    # Copy SSTable files from container
    local container_id=$(docker-compose ps -q cassandra)
    local data_path="/var/lib/cassandra/data/complex_types_test"
    
    # Copy all table directories
    for table in collection_types_test udt_test tuple_test nested_complex_test frozen_types_test edge_cases_test; do
        log "Copying SSTable files for table: $table"
        docker cp "$container_id:$data_path/$table-*/" "$TEST_DATA_DIR/" 2>/dev/null || true
    done
    
    # List exported files
    log "Exported SSTable files:"
    find "$TEST_DATA_DIR" -name "*.db" -exec basename {} \; | sort
    
    log "✅ SSTable files exported to: $TEST_DATA_DIR"
}

# Generate validation scripts
generate_validation_scripts() {
    log "Generating validation scripts..."
    
    cat > "$PROJECT_ROOT/run_complex_type_validation.sh" << 'EOF'
#!/bin/bash

# M3 Complex Type Validation Script
# Run comprehensive validation of complex types against real Cassandra data

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

echo "🚀 M3 Complex Type Validation"
echo "============================"

# Build the project
echo "🔨 Building project..."
cargo build --release

# Run the validation suite
echo "🧪 Running validation tests..."
cargo run --release --bin complex_type_validation_runner -- \
    --mode all \
    --test-data-dir tests/cassandra-cluster/test-data \
    --schema-dir tests/schemas \
    --output-dir target/validation-reports \
    --iterations 10000 \
    --cassandra-version 5.0 \
    --verbose

echo "✅ Validation complete! Check target/validation-reports/ for detailed results."
EOF

    chmod +x "$PROJECT_ROOT/run_complex_type_validation.sh"
    
    log "✅ Validation scripts generated"
}

# Cleanup function
cleanup() {
    log "Cleaning up..."
    cd "$PROJECT_ROOT/tests/cassandra-cluster"
    docker-compose down -v 2>/dev/null || true
}

# Main execution
main() {
    log "🚀 M3 Complex Type Test Data Generator"
    log "======================================"
    
    # Set up cleanup trap
    trap cleanup EXIT
    
    # Check dependencies
    check_dependencies
    
    # Create directories
    mkdir -p "$TEST_DATA_DIR"
    mkdir -p "$SCHEMA_DIR"
    
    # Start Cassandra and generate data
    start_cassandra
    create_test_schema
    generate_test_data
    export_schemas
    export_sstables
    generate_validation_scripts
    
    log "✅ Complex type test data generation complete!"
    log ""
    log "📂 Test Data: $TEST_DATA_DIR"
    log "📋 Schemas: $SCHEMA_DIR"
    log "🧪 Run validation: ./run_complex_type_validation.sh"
    log ""
    log "🎯 Ready for M3 complex type validation!"
}

# Run if executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi