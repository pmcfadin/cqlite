-- Comprehensive Test Schema for Hardened Validator Parser
-- Issue #31: Cross-Version Complex Type Validation
-- This schema covers all complex types across Cassandra versions 3.7-5.0

-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS hardened_validator_test
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE hardened_validator_test;

-- User-Defined Types for testing UDT parsing
CREATE TYPE IF NOT EXISTS address (
    street TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    country TEXT
);

CREATE TYPE IF NOT EXISTS phone_number (
    country_code TEXT,
    area_code TEXT,
    number TEXT,
    extension TEXT
);

CREATE TYPE IF NOT EXISTS person (
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    home_address FROZEN<address>,
    work_address FROZEN<address>,
    phone_numbers LIST<FROZEN<phone_number>>,
    emergency_contacts MAP<TEXT, FROZEN<phone_number>>
);

-- Nested UDT for deep nesting tests
CREATE TYPE IF NOT EXISTS company (
    name TEXT,
    headquarters FROZEN<address>,
    employees LIST<FROZEN<person>>,
    departments MAP<TEXT, LIST<FROZEN<person>>>
);

-- Complex collection test table
CREATE TABLE IF NOT EXISTS complex_collections (
    id UUID PRIMARY KEY,
    
    -- Basic collections
    simple_list LIST<TEXT>,
    simple_set SET<INT>,
    simple_map MAP<TEXT, INT>,
    
    -- Frozen collections
    frozen_list FROZEN<LIST<TEXT>>,
    frozen_set FROZEN<SET<INT>>,
    frozen_map FROZEN<MAP<TEXT, INT>>,
    
    -- Nested collections (Cassandra 3.11+)
    list_of_lists LIST<FROZEN<LIST<TEXT>>>,
    set_of_sets SET<FROZEN<SET<INT>>>,
    map_of_lists MAP<TEXT, FROZEN<LIST<INT>>>,
    map_of_sets MAP<TEXT, FROZEN<SET<TEXT>>>,
    map_of_maps MAP<TEXT, FROZEN<MAP<TEXT, INT>>>,
    
    -- Ultra-nested collections (stress test)
    nested_map_list MAP<TEXT, FROZEN<LIST<FROZEN<MAP<TEXT, INT>>>>>,
    nested_list_set LIST<FROZEN<SET<FROZEN<LIST<TEXT>>>>>,
    
    -- Collections with UDTs
    address_list LIST<FROZEN<address>>,
    person_set SET<FROZEN<person>>,
    company_map MAP<TEXT, FROZEN<company>>,
    
    -- Mixed type collections (Cassandra 5.0+)
    -- Note: These require special handling for version compatibility
    timestamp_created TIMESTAMP,
    last_updated TIMESTAMP
);

-- Tuple test table
CREATE TABLE IF NOT EXISTS tuple_tests (
    id UUID PRIMARY KEY,
    
    -- Basic tuples
    simple_tuple TUPLE<TEXT, INT, BOOLEAN>,
    nested_tuple TUPLE<TEXT, TUPLE<INT, FLOAT>, BOOLEAN>,
    
    -- Frozen tuples
    frozen_tuple FROZEN<TUPLE<TEXT, INT>>,
    
    -- Tuples with collections
    tuple_with_list TUPLE<TEXT, LIST<INT>>,
    tuple_with_map TUPLE<TEXT, MAP<TEXT, INT>>,
    
    -- Tuples with UDTs
    tuple_with_udt TUPLE<TEXT, FROZEN<person>>,
    
    -- Complex tuple combinations
    complex_tuple TUPLE<
        TEXT,
        FROZEN<LIST<INT>>,
        FROZEN<MAP<TEXT, FROZEN<address>>>,
        FROZEN<person>
    >,
    
    timestamp_created TIMESTAMP
);

-- UDT test table
CREATE TABLE IF NOT EXISTS udt_tests (
    id UUID PRIMARY KEY,
    
    -- Basic UDTs
    simple_address FROZEN<address>,
    simple_person FROZEN<person>,
    simple_company FROZEN<company>,
    
    -- UDTs in collections
    addresses LIST<FROZEN<address>>,
    people SET<FROZEN<person>>,
    companies MAP<TEXT, FROZEN<company>>,
    
    -- Nested UDT collections
    person_addresses MAP<FROZEN<person>, FROZEN<address>>,
    company_employees MAP<FROZEN<company>, LIST<FROZEN<person>>>,
    
    -- UDTs with nulls and partial data
    partial_person FROZEN<person>,
    nullable_address FROZEN<address>,
    
    timestamp_created TIMESTAMP
);

-- Time-series table with complex metadata
CREATE TABLE IF NOT EXISTS time_series_complex (
    sensor_id UUID,
    measurement_time TIMESTAMP,
    measurement_type TEXT,
    
    -- Complex measurement data
    scalar_value DOUBLE,
    vector_value LIST<DOUBLE>,
    matrix_value MAP<TEXT, LIST<DOUBLE>>,
    
    -- Metadata with complex types
    sensor_info FROZEN<MAP<TEXT, TEXT>>,
    calibration_data FROZEN<LIST<TUPLE<TIMESTAMP, DOUBLE>>>,
    location_history LIST<FROZEN<address>>,
    
    -- TTL and tombstone testing
    temporary_data TEXT,
    
    PRIMARY KEY (sensor_id, measurement_time, measurement_type)
) WITH CLUSTERING ORDER BY (measurement_time DESC, measurement_type ASC)
  AND default_time_to_live = 86400; -- 1 day TTL

-- Wide partition table for performance testing
CREATE TABLE IF NOT EXISTS wide_partition_complex (
    partition_key TEXT,
    clustering_key TIMEUUID,
    
    -- Complex data in wide partition
    nested_data MAP<TEXT, FROZEN<LIST<FROZEN<MAP<TEXT, INT>>>>>,
    udt_data FROZEN<company>,
    
    -- Large collections
    large_list LIST<TEXT>,
    large_map MAP<INT, TEXT>,
    
    -- Metadata
    row_metadata FROZEN<MAP<TEXT, TEXT>>,
    
    PRIMARY KEY (partition_key, clustering_key)
);

-- Static columns with complex types
CREATE TABLE IF NOT EXISTS static_complex (
    partition_key TEXT,
    clustering_key INT,
    
    -- Static complex columns
    partition_metadata FROZEN<MAP<TEXT, TEXT>> STATIC,
    partition_summary FROZEN<LIST<FROZEN<address>>> STATIC,
    partition_stats FROZEN<TUPLE<TIMESTAMP, BIGINT, DOUBLE>> STATIC,
    
    -- Regular complex columns
    row_data FROZEN<person>,
    row_collections MAP<TEXT, LIST<INT>>,
    
    PRIMARY KEY (partition_key, clustering_key)
);

-- Counter table with complex metadata
CREATE TABLE IF NOT EXISTS counters_complex (
    id UUID PRIMARY KEY,
    
    -- Counters
    hit_count COUNTER,
    miss_count COUNTER,
    error_count COUNTER,
    
    -- Complex metadata (separate table needed for counters)
    timestamp_created TIMESTAMP
);

-- Metadata table for counters (counters can't mix with other types)
CREATE TABLE IF NOT EXISTS counter_metadata (
    counter_id UUID PRIMARY KEY,
    
    -- Complex metadata for counters
    metadata FROZEN<MAP<TEXT, TEXT>>,
    tags LIST<TEXT>,
    configuration FROZEN<TUPLE<TEXT, INT, BOOLEAN>>,
    
    timestamp_created TIMESTAMP
);

-- Compression test tables
CREATE TABLE IF NOT EXISTS lz4_complex (
    id UUID PRIMARY KEY,
    large_nested_data MAP<TEXT, FROZEN<LIST<FROZEN<MAP<TEXT, FROZEN<person>>>>>>,
    compressible_text TEXT,
    timestamp_created TIMESTAMP
) WITH COMPRESSION = {'class': 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS snappy_complex (
    id UUID PRIMARY KEY,
    large_nested_data MAP<TEXT, FROZEN<LIST<FROZEN<MAP<TEXT, FROZEN<person>>>>>>,
    compressible_text TEXT,
    timestamp_created TIMESTAMP
) WITH COMPRESSION = {'class': 'SnappyCompressor'};

CREATE TABLE IF NOT EXISTS uncompressed_complex (
    id UUID PRIMARY KEY,
    large_nested_data MAP<TEXT, FROZEN<LIST<FROZEN<MAP<TEXT, FROZEN<person>>>>>>,
    compressible_text TEXT,
    timestamp_created TIMESTAMP
) WITH COMPRESSION = {'enabled': false};

-- Edge case table for validation testing
CREATE TABLE IF NOT EXISTS edge_cases (
    id UUID PRIMARY KEY,
    
    -- Empty collections
    empty_list LIST<TEXT>,
    empty_set SET<INT>,
    empty_map MAP<TEXT, INT>,
    
    -- Single element collections
    single_list LIST<TEXT>,
    single_set SET<INT>,
    single_map MAP<TEXT, INT>,
    
    -- Large collections (for memory limit testing)
    large_list LIST<TEXT>,
    large_set SET<UUID>,
    large_map MAP<INT, TEXT>,
    
    -- Maximum nesting
    max_nested MAP<TEXT, FROZEN<LIST<FROZEN<MAP<TEXT, FROZEN<LIST<INT>>>>>>>,
    
    -- Special characters and Unicode
    unicode_map MAP<TEXT, TEXT>,
    special_chars_list LIST<TEXT>,
    
    -- Null handling
    nullable_udt FROZEN<person>,
    partial_udt FROZEN<company>,
    
    timestamp_created TIMESTAMP
);

-- Version-specific features table
CREATE TABLE IF NOT EXISTS version_features (
    id UUID PRIMARY KEY,
    cassandra_version TEXT,
    
    -- Duration type (Cassandra 3.11+)
    duration_field DURATION,
    
    -- Enhanced timestamp precision (Cassandra 4.0+)
    high_precision_timestamp TIMESTAMP,
    
    -- JSON support (if available)
    json_data TEXT, -- Store as TEXT for compatibility
    
    -- Feature flags
    supports_duration BOOLEAN,
    supports_json BOOLEAN,
    supports_mixed_collections BOOLEAN,
    
    timestamp_created TIMESTAMP
);

-- Tombstone and deletion testing table
CREATE TABLE IF NOT EXISTS deletion_tests (
    id UUID PRIMARY KEY,
    
    -- Regular data
    regular_data TEXT,
    complex_data FROZEN<person>,
    collection_data LIST<FROZEN<address>>,
    
    -- Data with TTL for tombstone testing
    ttl_data TEXT,
    ttl_complex FROZEN<MAP<TEXT, INT>>,
    
    -- Row-level deletion markers
    deleted_flag BOOLEAN,
    deletion_timestamp TIMESTAMP,
    
    timestamp_created TIMESTAMP
);

-- Performance benchmark table
CREATE TABLE IF NOT EXISTS performance_benchmark (
    benchmark_id UUID,
    iteration INT,
    
    -- Data size categories
    small_complex FROZEN<address>,
    medium_complex FROZEN<person>,
    large_complex FROZEN<company>,
    
    -- Collection size categories
    small_collection LIST<TEXT>,
    medium_collection MAP<TEXT, FROZEN<address>>,
    large_collection MAP<TEXT, FROZEN<LIST<FROZEN<person>>>>,
    
    -- Nesting depth categories
    depth_1 FROZEN<address>,
    depth_2 FROZEN<person>,
    depth_3 FROZEN<company>,
    depth_4 MAP<TEXT, FROZEN<company>>,
    depth_5 LIST<FROZEN<MAP<TEXT, FROZEN<company>>>>,
    
    -- Performance metadata
    parse_time_us BIGINT,
    memory_usage_bytes BIGINT,
    
    PRIMARY KEY (benchmark_id, iteration)
);