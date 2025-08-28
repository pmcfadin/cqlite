-- Collections Schema for CQL Test Data Generation
-- Tests SET, LIST, MAP collections with various sizes and patterns
-- Issue #18: Docker-based test data generation

-- Create keyspace for collections testing
CREATE KEYSPACE IF NOT EXISTS test_collections WITH replication = {
  'class': 'SimpleStrategy', 
  'replication_factor': 1
} AND durable_writes = true;

USE test_collections;

-- Basic collections table
CREATE TABLE IF NOT EXISTS collection_table (
    id UUID PRIMARY KEY,
    tags SET<TEXT>,
    scores LIST<INT>,
    properties MAP<TEXT, TEXT>,
    numbers_set SET<INT>,
    ordered_values LIST<TIMESTAMP>,
    metadata_map MAP<TEXT, BIGINT>
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Nested collections (maps with collection values)
CREATE TABLE IF NOT EXISTS nested_collections_table (
    id UUID PRIMARY KEY,
    tags_by_category MAP<TEXT, FROZEN<SET<TEXT>>>,
    scores_by_game MAP<TEXT, FROZEN<LIST<INT>>>,
    user_preferences MAP<TEXT, FROZEN<MAP<TEXT, TEXT>>>,
    time_series_data MAP<DATE, FROZEN<LIST<TIMESTAMP>>>
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'LeveledCompactionStrategy'};

-- Large collections for performance testing
CREATE TABLE IF NOT EXISTS large_collections_table (
    partition_key UUID,
    clustering_key INT,
    huge_set SET<TEXT>,
    massive_list LIST<UUID>,
    giant_map MAP<TEXT, BLOB>,
    PRIMARY KEY (partition_key, clustering_key)
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Collections with UDTs (User Defined Types)
CREATE TYPE IF NOT EXISTS address_type (
    street TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    country TEXT
);

CREATE TYPE IF NOT EXISTS contact_info (
    email TEXT,
    phone TEXT,
    address FROZEN<address_type>
);

CREATE TABLE IF NOT EXISTS collections_with_udts (
    user_id UUID PRIMARY KEY,
    addresses LIST<FROZEN<address_type>>,
    contacts SET<FROZEN<contact_info>>,
    locations_visited MAP<DATE, FROZEN<address_type>>,
    emergency_contacts MAP<TEXT, FROZEN<contact_info>>
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'LeveledCompactionStrategy'};

-- Frozen collections for atomic updates
CREATE TABLE IF NOT EXISTS frozen_collections_table (
    id UUID PRIMARY KEY,
    frozen_tags FROZEN<SET<TEXT>>,
    frozen_scores FROZEN<LIST<INT>>,
    frozen_properties FROZEN<MAP<TEXT, TEXT>>,
    regular_tags SET<TEXT>
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Collections with different data types
CREATE TABLE IF NOT EXISTS typed_collections_table (
    id UUID PRIMARY KEY,
    uuid_set SET<UUID>,
    timestamp_list LIST<TIMESTAMP>,
    boolean_map MAP<TEXT, BOOLEAN>,
    decimal_set SET<DECIMAL>,
    blob_list LIST<BLOB>,
    inet_map MAP<TEXT, INET>
) WITH compression = {'class': 'DeflateCompressor'}
  AND compaction = {'class': 'TimeWindowCompactionStrategy'};

-- Empty and null collections testing
CREATE TABLE IF NOT EXISTS empty_collections_table (
    id UUID PRIMARY KEY,
    empty_set SET<TEXT>,
    null_list LIST<INT>,
    sparse_map MAP<TEXT, TEXT>,
    optional_tags SET<TEXT>
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Collections in clustering keys (limited support)
CREATE TABLE IF NOT EXISTS collection_clustering_table (
    partition_key UUID,
    clustering_key FROZEN<LIST<TEXT>>,
    data TEXT,
    value INT,
    PRIMARY KEY (partition_key, clustering_key)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};