-- Basic Types Schema for CQLite Test Data Generation
-- Covers all fundamental CQL data types
-- Issue #18: Docker-based test data generation

-- Create keyspace with simple strategy
CREATE KEYSPACE IF NOT EXISTS test_basic WITH replication = {
  'class': 'SimpleStrategy', 
  'replication_factor': 1
} AND durable_writes = true;

USE test_basic;

-- Basic primitive types table
CREATE TABLE IF NOT EXISTS simple_table (
    id UUID PRIMARY KEY,
    name TEXT,
    age INT,
    salary BIGINT,
    height FLOAT,
    weight DOUBLE,
    active BOOLEAN,
    created TIMESTAMP,
    birth_date DATE,
    work_time TIME,
    description BLOB,
    account_balance DECIMAL,
    session_id TIMEUUID,
    ip_address INET,
    small_number TINYINT,
    medium_number SMALLINT,
    duration_val DURATION,
    varchar_field VARCHAR,
    ascii_field ASCII
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Table with composite primary key
CREATE TABLE IF NOT EXISTS composite_key_table (
    partition_key UUID,
    clustering_key1 TIMESTAMP,
    clustering_key2 TEXT,
    data TEXT,
    value INT,
    PRIMARY KEY (partition_key, clustering_key1, clustering_key2)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'LeveledCompactionStrategy'}
  AND clustering ORDER BY (clustering_key1 DESC, clustering_key2 ASC);

-- Table with multiple partition keys
CREATE TABLE IF NOT EXISTS multi_partition_table (
    tenant_id UUID,
    user_id UUID,
    category TEXT,
    item_id TIMEUUID,
    name TEXT,
    value BIGINT,
    metadata TEXT,
    PRIMARY KEY ((tenant_id, user_id), category, item_id)
) WITH compression = {'class': 'DeflateCompressor'}
  AND compaction = {'class': 'TimeWindowCompactionStrategy'};

-- Table for testing different compression algorithms
CREATE TABLE IF NOT EXISTS compression_test_table (
    id UUID PRIMARY KEY,
    large_text TEXT,
    repeated_data TEXT,
    random_data BLOB,
    compressed_json TEXT
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Uncompressed table for comparison
CREATE TABLE IF NOT EXISTS uncompressed_table (
    id UUID PRIMARY KEY,
    data TEXT,
    value INT,
    timestamp_val TIMESTAMP
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Table with TTL for time-based testing
CREATE TABLE IF NOT EXISTS ttl_test_table (
    id UUID PRIMARY KEY,
    temporary_data TEXT,
    expiring_value INT,
    session_info TEXT
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'}
  AND default_time_to_live = 86400; -- 1 day TTL

-- Static columns table
CREATE TABLE IF NOT EXISTS static_columns_table (
    partition_key UUID,
    clustering_key TIMESTAMP,
    static_data TEXT STATIC,
    row_data TEXT,
    row_value INT,
    PRIMARY KEY (partition_key, clustering_key)
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Counter table
CREATE TABLE IF NOT EXISTS counters (
    id TEXT PRIMARY KEY,
    view_count COUNTER,
    like_count COUNTER,
    share_count COUNTER,
    total_interactions COUNTER
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};