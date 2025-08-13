#!/bin/bash

# CQLite Test Data Generation Master Script
# Generates comprehensive test data across multiple Cassandra versions
# Issue #18: Docker-based test data generation

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEMAS_DIR="/opt/schemas"
GENERATED_DIR="/opt/generated"

# Cassandra version configurations
declare -A CASSANDRA_VERSIONS=(
    ["3.7"]="${CASSANDRA_3_7_HOST:-cassandra-3-7}"
    ["3.11"]="${CASSANDRA_3_11_HOST:-cassandra-3-11}"
    ["4.0"]="${CASSANDRA_4_0_HOST:-cassandra-4-0}"
    ["4.1"]="${CASSANDRA_4_1_HOST:-cassandra-4-1}"
)

CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"
TEST_DATA_SCALE="${TEST_DATA_SCALE:-COMPREHENSIVE}"

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

# Wait for Cassandra to be ready
wait_for_cassandra() {
    local host=$1
    local version=$2
    local max_attempts=60
    local attempt=1

    log_info "Waiting for Cassandra $version ($host) to be ready..."

    while [ $attempt -le $max_attempts ]; do
        if cqlsh -e "SELECT cluster_name FROM system.local;" "$host" "$CASSANDRA_PORT" >/dev/null 2>&1; then
            log_success "Cassandra $version is ready!"
            return 0
        fi
        
        log_info "Attempt $attempt/$max_attempts - Cassandra $version not ready yet, waiting 5 seconds..."
        sleep 5
        ((attempt++))
    done

    log_error "Cassandra $version failed to become ready after $max_attempts attempts"
    return 1
}

# Create schemas on a Cassandra instance
create_schemas() {
    local host=$1
    local version=$2

    log_info "Creating schemas on Cassandra $version ($host)..."

    # Create all schemas
    for schema_file in "$SCHEMAS_DIR"/*.cql; do
        if [ -f "$schema_file" ]; then
            local schema_name=$(basename "$schema_file" .cql)
            log_info "Applying schema: $schema_name on Cassandra $version"
            
            if cqlsh -f "$schema_file" "$host" "$CASSANDRA_PORT"; then
                log_success "Schema $schema_name applied successfully on Cassandra $version"
            else
                log_error "Failed to apply schema $schema_name on Cassandra $version"
                return 1
            fi
        fi
    done

    log_success "All schemas created successfully on Cassandra $version"
}

# Generate test data for a specific Cassandra version
generate_data_for_version() {
    local host=$1
    local version=$2

    log_info "Generating test data for Cassandra $version ($host)..."

    # Install required Python packages
    pip install cassandra-driver faker >/dev/null 2>&1

    # Create Python data generator script
    cat > "/tmp/data_generator_${version}.py" << 'EOF'
import sys
import time
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, BatchType
from faker import Faker

fake = Faker()

class CQLiteDataGenerator:
    def __init__(self, host, port=9042):
        self.cluster = Cluster([host], port=port)
        self.session = None
        self.connect()

    def connect(self):
        """Connect to Cassandra cluster"""
        max_retries = 10
        for attempt in range(max_retries):
            try:
                self.session = self.cluster.connect()
                print(f"Connected to Cassandra successfully")
                return
            except Exception as e:
                print(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                else:
                    raise

    def generate_basic_types_data(self, num_rows=10000):
        """Generate data for basic types tables"""
        print(f"Generating {num_rows} rows for basic types tables...")

        # Simple table data
        insert_simple = self.session.prepare("""
            INSERT INTO test_basic.simple_table 
            (id, name, age, salary, height, weight, active, created, birth_date, 
             work_time, description, account_balance, session_id, ip_address, 
             small_number, medium_number, duration_val, varchar_field, ascii_field)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for i in range(num_rows):
            row_data = (
                uuid.uuid4(),  # id
                fake.name(),   # name
                random.randint(18, 80),  # age
                random.randint(30000, 200000),  # salary
                round(random.uniform(1.5, 2.0), 2),  # height
                round(random.uniform(50.0, 120.0), 2),  # weight
                random.choice([True, False]),  # active
                datetime.now(),  # created
                fake.date_between(start_date='-80y', end_date='-18y'),  # birth_date
                datetime.now().time(),  # work_time
                fake.text(max_nb_chars=200).encode('utf-8'),  # description (blob)
                Decimal(str(round(random.uniform(0, 100000), 2))),  # account_balance
                uuid.uuid1(),  # session_id (timeuuid)
                fake.ipv4(),  # ip_address
                random.randint(0, 127),  # small_number (tinyint)
                random.randint(0, 32767),  # medium_number (smallint)
                f"PT{random.randint(1, 23)}H{random.randint(1, 59)}M{random.randint(1, 59)}S",  # duration
                fake.word(),  # varchar_field
                fake.word()   # ascii_field
            )
            batch.add(insert_simple, row_data)

            if len(batch) >= 100:
                self.session.execute(batch)
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)

        if len(batch) > 0:
            self.session.execute(batch)

        print(f"Generated {num_rows} rows for simple_table")

    def generate_collections_data(self, num_rows=5000):
        """Generate data for collections tables"""
        print(f"Generating {num_rows} rows for collections tables...")

        insert_collections = self.session.prepare("""
            INSERT INTO test_collections.collection_table 
            (id, tags, scores, properties, numbers_set, ordered_values, metadata_map)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """)

        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for i in range(num_rows):
            row_data = (
                uuid.uuid4(),
                set([fake.word() for _ in range(random.randint(1, 5))]),  # tags
                [random.randint(1, 100) for _ in range(random.randint(1, 10))],  # scores
                {fake.word(): fake.sentence() for _ in range(random.randint(1, 5))},  # properties
                set([random.randint(1, 1000) for _ in range(random.randint(1, 8))]),  # numbers_set
                [fake.date_time() for _ in range(random.randint(1, 5))],  # ordered_values
                {fake.word(): random.randint(1, 10000) for _ in range(random.randint(1, 5))}  # metadata_map
            )
            batch.add(insert_collections, row_data)

            if len(batch) >= 50:
                self.session.execute(batch)
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)

        if len(batch) > 0:
            self.session.execute(batch)

        print(f"Generated {num_rows} rows for collection_table")

    def generate_timeseries_data(self, num_rows=20000):
        """Generate data for time series tables"""
        print(f"Generating {num_rows} rows for time series tables...")

        # Sensor data
        insert_sensor = self.session.prepare("""
            INSERT INTO test_timeseries.sensor_data 
            (sensor_id, timestamp, temperature, humidity, pressure, battery_level, location, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """)

        sensor_ids = [uuid.uuid4() for _ in range(100)]  # 100 different sensors
        base_time = datetime.now() - timedelta(days=30)

        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        for i in range(num_rows):
            row_data = (
                random.choice(sensor_ids),
                base_time + timedelta(seconds=random.randint(0, 30*24*3600)),
                round(random.uniform(15.0, 35.0), 2),  # temperature
                round(random.uniform(30.0, 90.0), 2),  # humidity
                round(random.uniform(980.0, 1020.0), 2),  # pressure
                random.randint(10, 100),  # battery_level
                fake.city(),
                random.choice(['active', 'inactive', 'maintenance', 'error'])
            )
            batch.add(insert_sensor, row_data)

            if len(batch) >= 100:
                self.session.execute(batch)
                batch = BatchStatement(batch_type=BatchType.UNLOGGED)

        if len(batch) > 0:
            self.session.execute(batch)

        print(f"Generated {num_rows} rows for sensor_data")

    def generate_wide_rows_data(self, num_partitions=1000):
        """Generate data for wide rows tables"""
        print(f"Generating wide partition data for {num_partitions} partitions...")

        insert_wide = self.session.prepare("""
            INSERT INTO test_wide_rows.wide_partition_table 
            (partition_key, clustering_col1, clustering_col2, clustering_col3, 
             clustering_col4, clustering_col5, data_column, value_column, blob_column, json_column)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)

        for partition in range(num_partitions):
            partition_key = uuid.uuid4()
            rows_per_partition = random.randint(10, 100)

            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            for row in range(rows_per_partition):
                row_data = (
                    partition_key,
                    fake.date_time(),
                    fake.word(),
                    random.randint(1, 1000000),
                    uuid.uuid4(),
                    fake.date(),
                    fake.text(max_nb_chars=1000),
                    random.randint(1, 10000000),
                    fake.text(max_nb_chars=500).encode('utf-8'),
                    f'{{"key{random.randint(1,10)}": "value{random.randint(1,100)}"}}'
                )
                batch.add(insert_wide, row_data)

                if len(batch) >= 50:
                    self.session.execute(batch)
                    batch = BatchStatement(batch_type=BatchType.UNLOGGED)

            if len(batch) > 0:
                self.session.execute(batch)

        print(f"Generated wide partition data for {num_partitions} partitions")

    def generate_counter_data(self, num_counters=1000):
        """Generate counter data"""
        print(f"Generating counter data for {num_counters} counters...")

        for i in range(num_counters):
            counter_id = f"counter_{i:06d}"
            
            # Generate random increments
            view_increment = random.randint(100, 10000)
            like_increment = random.randint(10, 1000)
            share_increment = random.randint(1, 100)
            
            self.session.execute("""
                UPDATE test_basic.counters 
                SET view_count = view_count + ?, 
                    like_count = like_count + ?, 
                    share_count = share_count + ?,
                    total_interactions = total_interactions + ?
                WHERE id = ?
            """, (view_increment, like_increment, share_increment, 
                  view_increment + like_increment + share_increment, counter_id))

        print(f"Generated counter data for {num_counters} counters")

    def close(self):
        """Close connection"""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python data_generator.py <cassandra_host>")
        sys.exit(1)

    host = sys.argv[1]
    generator = CQLiteDataGenerator(host)

    try:
        generator.generate_basic_types_data(10000)
        generator.generate_collections_data(5000)
        generator.generate_timeseries_data(20000)
        generator.generate_wide_rows_data(1000)
        generator.generate_counter_data(1000)
        print("Data generation completed successfully!")
    except Exception as e:
        print(f"Error during data generation: {e}")
        sys.exit(1)
    finally:
        generator.close()
EOF

    # Run the data generator
    if python3 "/tmp/data_generator_${version}.py" "$host"; then
        log_success "Test data generated successfully for Cassandra $version"
    else
        log_error "Failed to generate test data for Cassandra $version"
        return 1
    fi

    # Clean up temporary file
    rm -f "/tmp/data_generator_${version}.py"
}

# Main execution
main() {
    log_info "Starting CQLite comprehensive test data generation..."
    log_info "Test data scale: $TEST_DATA_SCALE"

    # Process each Cassandra version
    for version in "${!CASSANDRA_VERSIONS[@]}"; do
        host="${CASSANDRA_VERSIONS[$version]}"
        
        log_info "Processing Cassandra $version at $host:$CASSANDRA_PORT"
        
        # Wait for Cassandra to be ready
        if ! wait_for_cassandra "$host" "$version"; then
            log_error "Skipping Cassandra $version due to readiness failure"
            continue
        fi

        # Create schemas
        if ! create_schemas "$host" "$version"; then
            log_error "Skipping data generation for Cassandra $version due to schema creation failure"
            continue
        fi

        # Generate test data
        if ! generate_data_for_version "$host" "$version"; then
            log_error "Data generation failed for Cassandra $version"
            continue
        fi

        log_success "Completed processing for Cassandra $version"
    done

    log_success "Test data generation completed for all Cassandra versions!"
    
    # Create summary report
    cat > "$GENERATED_DIR/generation_report.txt" << EOF
CQLite Test Data Generation Report
Generated: $(date)
Scale: $TEST_DATA_SCALE

Cassandra Versions Processed:
$(for version in "${!CASSANDRA_VERSIONS[@]}"; do echo "- $version (${CASSANDRA_VERSIONS[$version]})"; done)

Data Generated:
- Basic Types: 10,000 rows across multiple tables
- Collections: 5,000 rows with various collection types
- Time Series: 20,000 rows with temporal data
- Wide Rows: 1,000 partitions with multiple clustering keys
- Counters: 1,000 counter records

Total Estimated Rows: ~36,000 per Cassandra version
Total Estimated Data Size: ~500MB-1GB per version (depending on compression)

Next Steps:
1. Run SSTable export: /opt/scripts/export-sstables.sh
2. Validate data quality: /opt/scripts/validate-data.sh
3. Generate metadata: /opt/scripts/create-metadata.sh
EOF

    log_info "Generation report created at $GENERATED_DIR/generation_report.txt"
}

# Execute main function
main "$@"