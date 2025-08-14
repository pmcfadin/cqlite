#!/bin/bash
# Generate BTI datasets for Issue #36 validation
# This script bridges from Issue #30 infrastructure to create BTI-specific test data

set -e

echo "🚀 Generating BTI datasets for Issue #36 validation"

# Configuration
CASSANDRA_VERSION=${CASSANDRA_VERSION:-"5.0"}
DOCKER_COMPOSE_FILE="test-data/docker/docker-compose-cassandra5.yml"
BTI_OUTPUT_DIR="test-data/cassandra5/bti"
CONTAINER_NAME="cqlite-cassandra-5-0"

# Create output directories
mkdir -p "$BTI_OUTPUT_DIR"/{multi_component_keys,wide_partitions,complex_types,range_tombstones,nested_collections}

echo "📋 BTI Dataset Generation Plan:"
echo "  - Multi-component partition keys"
echo "  - Wide partitions (1000+ clustering keys)"  
echo "  - Complex types (nested collections, UDTs)"
echo "  - Range tombstones and metadata"
echo "  - Byte-comparable key testing"

# Start Cassandra 5.0 container if not running
echo "🐳 Starting Cassandra 5.0 container..."
if ! docker ps | grep -q "$CONTAINER_NAME"; then
    cd test-data/docker
    docker-compose -f docker-compose-cassandra5.yml up -d cassandra-5-0
    cd ../..
    
    # Wait for Cassandra to be ready
    echo "⏳ Waiting for Cassandra to be ready..."
    timeout 180 bash -c "until docker exec $CONTAINER_NAME cqlsh -e 'SELECT cluster_name FROM system.local;' > /dev/null 2>&1; do sleep 5; done"
fi

echo "✅ Cassandra 5.0 is ready"

# Function to create BTI dataset
create_bti_dataset() {
    local scenario=$1
    local cql_commands="$2"
    local description="$3"
    
    echo "📊 Creating BTI dataset: $scenario"
    echo "   Description: $description"
    
    # Create keyspace and table with BTI format
    docker exec "$CONTAINER_NAME" cqlsh -e "
        DROP KEYSPACE IF EXISTS bti_test_$scenario;
        CREATE KEYSPACE bti_test_$scenario WITH REPLICATION = {
            'class': 'SimpleStrategy', 
            'replication_factor': 1
        };
        USE bti_test_$scenario;
        $cql_commands
    "
    
    # Force flush to create SSTables
    docker exec "$CONTAINER_NAME" bash -c "
        echo 'Flushing $scenario data...'
        /opt/cassandra/bin/nodetool flush bti_test_$scenario
    "
    
    # Find and copy BTI SSTables
    local sstable_path=$(docker exec "$CONTAINER_NAME" find /var/lib/cassandra/data/bti_test_$scenario -name "*-Data.db" | head -1)
    if [ -n "$sstable_path" ]; then
        local sstable_dir=$(dirname "$sstable_path")
        local sstable_prefix=$(basename "$sstable_path" | sed 's/-Data.db//')
        
        echo "📦 Copying BTI SSTables for $scenario..."
        
        # Copy all related files and track what was copied
        local copied_files=0
        for suffix in Data.db Index.db CompressionInfo.db Statistics.db Summary.db TOC.txt Digest.crc32 Partitions.db Rows.db; do
            local src_file="$sstable_dir/$sstable_prefix-$suffix"
            if docker exec "$CONTAINER_NAME" test -f "$src_file"; then
                docker cp "$CONTAINER_NAME:$src_file" "$BTI_OUTPUT_DIR/$scenario/"
                echo "   ✓ Copied $suffix"
                copied_files=$((copied_files + 1))
            fi
        done
        
        # Assert that required BTI files exist
        echo "   🔍 Validating BTI dataset presence..."
        local required_bti_files=("Partitions.db" "Rows.db" "Data.db")
        local missing_files=()
        
        for required_file in "${required_bti_files[@]}"; do
            if [ ! -f "$BTI_OUTPUT_DIR/$scenario"/*-"$required_file" ]; then
                missing_files+=("$required_file")
            fi
        done
        
        if [ ${#missing_files[@]} -eq 0 ]; then
            echo "   ✅ BTI dataset '$scenario' created successfully with all required files"
        else
            echo "   ❌ BTI dataset '$scenario' is incomplete - missing required files: ${missing_files[*]}"
            echo "   💡 Note: BTI format requires Partitions.db and Rows.db files for Issue #36 validation"
            return 1
        fi
    else
        echo "   ❌ No SSTables found for scenario '$scenario'"
        return 1
    fi
}

# Dataset 1: Multi-component partition keys
create_bti_dataset "multi_component_keys" "
CREATE TABLE multi_partition_test (
    user_id UUID,
    year INT,
    region TEXT,
    event_id TIMEUUID,
    event_type TEXT,
    event_data TEXT,
    metadata MAP<TEXT, TEXT>,
    PRIMARY KEY ((user_id, year, region), event_id, event_type)
) WITH sstable_format='bti';

INSERT INTO multi_partition_test (user_id, year, region, event_id, event_type, event_data, metadata)
VALUES (550e8400-e29b-41d4-a716-446655440000, 2023, 'us-west', now(), 'login', 'user_login_data', {'source': 'web', 'device': 'desktop'});

INSERT INTO multi_partition_test (user_id, year, region, event_id, event_type, event_data, metadata)  
VALUES (550e8400-e29b-41d4-a716-446655440001, 2023, 'us-east', now(), 'logout', 'user_logout_data', {'source': 'mobile', 'device': 'phone'});

INSERT INTO multi_partition_test (user_id, year, region, event_id, event_type, event_data, metadata)
VALUES (550e8400-e29b-41d4-a716-446655440002, 2024, 'eu-central', now(), 'purchase', 'purchase_data', {'source': 'api', 'amount': '299.99'});
" "Multi-component partition keys with UUID, INT, TEXT"

# Dataset 2: Wide partitions 
create_bti_dataset "wide_partitions" "
CREATE TABLE wide_partition_test (
    partition_key TEXT,
    clustering_key INT,
    data_column TEXT,
    timestamp_col TIMESTAMP,
    PRIMARY KEY (partition_key, clustering_key)
) WITH sstable_format='bti';

BEGIN BATCH
$(for i in {1..1000}; do
    echo "INSERT INTO wide_partition_test (partition_key, clustering_key, data_column, timestamp_col) VALUES ('wide_partition_1', $i, 'data_$i', toTimestamp(now()));"
done)
APPLY BATCH;
" "Wide partition with 1000+ clustering keys"

# Dataset 3: Complex types (nested collections, UDTs)
create_bti_dataset "complex_types" "
CREATE TYPE user_address (
    street TEXT,
    city TEXT,
    zip TEXT,
    coordinates FROZEN<MAP<TEXT, DOUBLE>>
);

CREATE TYPE user_profile (
    name TEXT,
    age INT,
    addresses FROZEN<LIST<user_address>>,
    preferences MAP<TEXT, TEXT>,
    tags SET<TEXT>
);

CREATE TABLE complex_types_test (
    user_id UUID PRIMARY KEY,
    profile FROZEN<user_profile>,
    social_data MAP<TEXT, FROZEN<LIST<TEXT>>>,
    activity_log LIST<FROZEN<MAP<TEXT, TEXT>>>,
    metadata FROZEN<MAP<TEXT, FROZEN<SET<TEXT>>>>
) WITH sstable_format='bti';

INSERT INTO complex_types_test (user_id, profile, social_data, activity_log, metadata)
VALUES (
    550e8400-e29b-41d4-a716-446655440000,
    {
        name: 'John Doe',
        age: 30,
        addresses: [
            {street: '123 Main St', city: 'Seattle', zip: '98101', coordinates: {'lat': 47.6062, 'lon': -122.3321}},
            {street: '456 Oak Ave', city: 'Portland', zip: '97201', coordinates: {'lat': 45.5152, 'lon': -122.6784}}
        ],
        preferences: {'theme': 'dark', 'language': 'en'},
        tags: {'developer', 'seattle', 'coffee'}
    },
    {
        'facebook': ['friend1', 'friend2'],
        'twitter': ['follower1', 'follower2', 'follower3']
    },
    [
        {'action': 'login', 'timestamp': '2023-01-01T10:00:00Z'},
        {'action': 'view_page', 'timestamp': '2023-01-01T10:05:00Z'}
    ],
    {
        'categories': {'tech', 'social'},
        'permissions': {'read', 'write'}
    }
);
" "Complex nested collections and UDTs"

# Dataset 4: Range tombstones and metadata
create_bti_dataset "range_tombstones" "
CREATE TABLE tombstone_test (
    partition_key TEXT,
    clustering_key INT,
    data_column TEXT,
    ttl_column TEXT,
    PRIMARY KEY (partition_key, clustering_key)
) WITH sstable_format='bti';

-- Insert initial data
INSERT INTO tombstone_test (partition_key, clustering_key, data_column, ttl_column)
VALUES ('partition1', 1, 'data1', 'ttl_data1');

INSERT INTO tombstone_test (partition_key, clustering_key, data_column, ttl_column)
VALUES ('partition1', 2, 'data2', 'ttl_data2');

INSERT INTO tombstone_test (partition_key, clustering_key, data_column, ttl_column)
VALUES ('partition1', 3, 'data3', 'ttl_data3');

INSERT INTO tombstone_test (partition_key, clustering_key, data_column, ttl_column)
VALUES ('partition1', 4, 'data4', 'ttl_data4');

INSERT INTO tombstone_test (partition_key, clustering_key, data_column, ttl_column)
VALUES ('partition1', 5, 'data5', 'ttl_data5');

-- Create range tombstone by deleting range
DELETE FROM tombstone_test WHERE partition_key = 'partition1' AND clustering_key >= 2 AND clustering_key <= 4;

-- Insert data with TTL
INSERT INTO tombstone_test (partition_key, clustering_key, data_column, ttl_column)
VALUES ('partition2', 1, 'expiring_data', 'will_expire') USING TTL 3600;
" "Range tombstones and TTL metadata"

# Dataset 5: Nested collections for byte-comparable testing
create_bti_dataset "nested_collections" "
CREATE TABLE nested_collections_test (
    id UUID PRIMARY KEY,
    simple_list LIST<TEXT>,
    simple_set SET<INT>,
    simple_map MAP<TEXT, TEXT>,
    nested_list LIST<FROZEN<MAP<TEXT, INT>>>,
    nested_set SET<FROZEN<LIST<TEXT>>>,
    nested_map MAP<TEXT, FROZEN<SET<INT>>>,
    complex_nested MAP<TEXT, FROZEN<LIST<FROZEN<MAP<TEXT, FROZEN<SET<TEXT>>>>>>>
) WITH sstable_format='bti';

INSERT INTO nested_collections_test (
    id, simple_list, simple_set, simple_map,
    nested_list, nested_set, nested_map, complex_nested
) VALUES (
    550e8400-e29b-41d4-a716-446655440000,
    ['item1', 'item2', 'item3'],
    {1, 2, 3, 4, 5},
    {'key1': 'value1', 'key2': 'value2'},
    [{'count': 10, 'score': 95}, {'count': 5, 'score': 87}],
    {['tag1', 'tag2'], ['tag3', 'tag4', 'tag5']},
    {'group1': {100, 200, 300}, 'group2': {400, 500}},
    {
        'level1': [
            {'sublevel1': {'item1', 'item2'}},
            {'sublevel2': {'item3', 'item4', 'item5'}}
        ]
    }
);
" "Nested collections for byte-comparable key testing"

echo ""
echo "🎉 BTI dataset generation completed!"
echo ""
echo "📊 Generated datasets:"
echo "  ✓ Multi-component partition keys: $BTI_OUTPUT_DIR/multi_component_keys/"
echo "  ✓ Wide partitions: $BTI_OUTPUT_DIR/wide_partitions/"  
echo "  ✓ Complex types: $BTI_OUTPUT_DIR/complex_types/"
echo "  ✓ Range tombstones: $BTI_OUTPUT_DIR/range_tombstones/"
echo "  ✓ Nested collections: $BTI_OUTPUT_DIR/nested_collections/"
echo ""
echo "🔍 Next steps:"
echo "  1. Run BTI validation: ./scripts/run_bti_validation.sh"
echo "  2. Generate sstabledump reference: ./scripts/generate_bti_reference.sh"
echo "  3. Run zero-tolerance comparison: DATASET_DIRS='$BTI_OUTPUT_DIR/*' ./test-data/scripts/run-sstabledump-validator.sh"
echo ""

# Create dataset list for the validator
echo "multi_component_keys,wide_partitions,complex_types,range_tombstones,nested_collections" > "$BTI_OUTPUT_DIR/dataset_list.txt"

echo "✅ BTI datasets ready for Issue #36 validation"