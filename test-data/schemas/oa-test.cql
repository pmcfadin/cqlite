-- OA Format Test Schema for CQLite Issue #654
-- Generated with Cassandra 5.0.2, storage_compatibility_mode: NONE (oa BIG format)
--
-- Covers bug classes from epic #622:
--   simple types, collections, tuple/UDT with >=128-byte field,
--   TTL rows, row/range/cell tombstones, static columns
--
-- Keyspace: test_oa
-- SSTable version: oa (BIG format)
-- Parser status: SKIP-PENDING until VG4

CREATE KEYSPACE IF NOT EXISTS test_oa WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_oa;

-- 1. Simple types (mirrors test_basic.simple_table, subset of columns)
CREATE TABLE IF NOT EXISTS simple_table (
    id UUID PRIMARY KEY,
    name TEXT,
    age INT,
    salary BIGINT,
    height FLOAT,
    weight DOUBLE,
    active BOOLEAN,
    created TIMESTAMP
) WITH compression = {'class': 'LZ4Compressor'};

-- 2. Collections (mirrors test_collections.collection_table, subset)
CREATE TABLE IF NOT EXISTS collection_table (
    id UUID PRIMARY KEY,
    tags SET<TEXT>,
    scores LIST<INT>,
    properties MAP<TEXT, TEXT>
) WITH compression = {'class': 'LZ4Compressor'};

-- 3. UDT with >=128-byte field (exercises oa large-field code path)
CREATE TYPE IF NOT EXISTS address_type (
    street TEXT,
    city TEXT,
    country TEXT,
    postal_code TEXT
);

CREATE TABLE IF NOT EXISTS udt_table (
    id UUID PRIMARY KEY,
    name TEXT,
    address frozen<address_type>,
    large_field TEXT
) WITH compression = {'class': 'LZ4Compressor'};

-- 4. TTL rows (mirrors test_basic.ttl_test_table)
CREATE TABLE IF NOT EXISTS ttl_table (
    id UUID PRIMARY KEY,
    data TEXT,
    expiring_value INT
) WITH compression = {'class': 'LZ4Compressor'}
  AND default_time_to_live = 86400;

-- 5. Static columns (mirrors test_basic.static_columns_table)
CREATE TABLE IF NOT EXISTS static_table (
    partition_key UUID,
    clustering_key INT,
    static_col TEXT STATIC,
    row_data TEXT,
    PRIMARY KEY (partition_key, clustering_key)
) WITH compression = {'class': 'LZ4Compressor'};

-- 6. Tombstones: range tombstone, row tombstone, cell tombstone
CREATE TABLE IF NOT EXISTS tombstone_table (
    id UUID,
    ts TIMESTAMP,
    value TEXT,
    extra TEXT,
    PRIMARY KEY (id, ts)
) WITH compression = {'class': 'LZ4Compressor'};
