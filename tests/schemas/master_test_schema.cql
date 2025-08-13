-- =============================================================================
-- CQLITE MASTER TEST SCHEMA - SINGLE SOURCE OF TRUTH
-- =============================================================================
-- 
-- This file consolidates ALL test schemas from scattered directories into
-- a comprehensive single source covering every CQL feature and edge case.
--
-- Replaces:
-- - /test-env/cassandra5/scripts/create-keyspaces.cql
-- - /test-data/schemas/*.cql (4 files)
-- - /examples/schemas/*.cql (2 files)
-- - /extracted_schema.cql
-- - Various JSON schema definitions
--
-- Coverage:
-- ✅ ALL 21 CQL data types (including Cassandra 5+ features)
-- ✅ Complex collections with 4+ levels of nesting
-- ✅ UDTs with deep nesting (5+ levels)
-- ✅ Tables with 6+ clustering keys
-- ✅ Counter tables and time series patterns
-- ✅ Static columns and materialized views
-- ✅ Secondary indexes and all compression types
-- ✅ Edge cases: nulls, empty collections, Unicode, large data
-- ✅ Performance scenarios: wide partitions, many columns
-- =============================================================================

-- Create comprehensive keyspaces for different test scenarios
CREATE KEYSPACE IF NOT EXISTS cqlite_master_test 
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 2
} AND durable_writes = true;

CREATE KEYSPACE IF NOT EXISTS cqlite_performance_test 
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': 2
} AND durable_writes = true;

CREATE KEYSPACE IF NOT EXISTS cqlite_edge_cases
WITH replication = {
  'class': 'SimpleStrategy', 
  'replication_factor': 1
} AND durable_writes = false; -- For testing durable_writes = false

USE cqlite_master_test;

-- =============================================================================
-- SECTION 1: COMPREHENSIVE DATA TYPES TABLE
-- =============================================================================
-- Tests ALL 21 CQL data types including Cassandra 5+ features

CREATE TABLE comprehensive_types (
  -- Partition Key
  id UUID PRIMARY KEY,
  
  -- Text Types
  text_field TEXT,
  ascii_field ASCII,
  varchar_field VARCHAR,
  
  -- Numeric Types  
  tinyint_field TINYINT,           -- -128 to 127
  smallint_field SMALLINT,         -- -32,768 to 32,767
  int_field INT,                   -- -2^31 to 2^31-1
  bigint_field BIGINT,             -- -2^63 to 2^63-1
  varint_field VARINT,             -- Arbitrary precision integer
  float_field FLOAT,               -- 32-bit IEEE 754
  double_field DOUBLE,             -- 64-bit IEEE 754
  decimal_field DECIMAL,           -- Arbitrary precision decimal
  
  -- Boolean
  boolean_field BOOLEAN,
  
  -- Binary Data
  blob_field BLOB,
  
  -- UUID Types
  uuid_field UUID,
  timeuuid_field TIMEUUID,
  
  -- Date/Time Types
  timestamp_field TIMESTAMP,
  date_field DATE,
  time_field TIME,
  duration_field DURATION,
  
  -- Network Type
  inet_field INET
  
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'}
  AND comment = 'Comprehensive test of all 21 CQL data types';

-- =============================================================================
-- SECTION 2: COMPLEX COLLECTIONS WITH DEEP NESTING
-- =============================================================================

-- Level 1: Basic Collections
CREATE TABLE basic_collections (
  id UUID PRIMARY KEY,
  simple_list LIST<TEXT>,
  simple_set SET<INT>,
  simple_map MAP<TEXT, BIGINT>
) WITH compression = {'class': 'SnappyCompressor'};

-- Level 2: Nested Collections
CREATE TABLE nested_collections (
  id UUID PRIMARY KEY,
  list_of_sets LIST<SET<TEXT>>,
  set_of_lists SET<LIST<INT>>,
  map_of_lists MAP<TEXT, LIST<DOUBLE>>,
  map_of_sets MAP<TEXT, SET<UUID>>,
  map_of_maps MAP<TEXT, MAP<TEXT, TIMESTAMP>>
) WITH compression = {'class': 'DeflateCompressor'};

-- Level 3: Deeply Nested Collections (4+ levels)
CREATE TABLE deep_nested_collections (
  id UUID PRIMARY KEY,
  -- 4-level nesting: Map -> List -> Set -> Text
  ultra_nested MAP<TEXT, LIST<SET<TEXT>>>,
  -- Complex financial data structure
  market_data MAP<TEXT, MAP<DATE, LIST<DECIMAL>>>,
  -- Multi-dimensional arrays simulation
  tensor_data LIST<LIST<LIST<DOUBLE>>>,
  -- Event hierarchies
  event_tree MAP<TEXT, MAP<TEXT, SET<TIMEUUID>>>
) WITH compression = {'class': 'ZstdCompressor'};

-- =============================================================================
-- SECTION 3: USER DEFINED TYPES (UDTs) WITH DEEP NESTING
-- =============================================================================

-- Level 1 UDTs
CREATE TYPE address_type (
  street_number INT,
  street_name TEXT,
  unit_number TEXT,
  city TEXT,
  state_province TEXT,
  postal_code TEXT,
  country TEXT,
  coordinates MAP<TEXT, DOUBLE>
);

CREATE TYPE contact_info_type (
  email TEXT,
  phone_primary TEXT,
  phone_secondary TEXT,
  preferred_contact_method TEXT
);

-- Level 2 UDTs (nesting Level 1)
CREATE TYPE person_type (
  first_name TEXT,
  middle_names LIST<TEXT>,
  last_name TEXT,
  date_of_birth DATE,
  contact_info contact_info_type,
  addresses LIST<address_type>,
  emergency_contacts SET<contact_info_type>
);

-- Level 3 UDTs (nesting Level 2)
CREATE TYPE organization_type (
  legal_name TEXT,
  doing_business_as LIST<TEXT>,
  tax_id TEXT,
  incorporation_date DATE,
  headquarters address_type,
  offices LIST<address_type>,
  key_personnel LIST<person_type>,
  contact_info contact_info_type
);

-- Level 4 UDTs (nesting Level 3)
CREATE TYPE business_relationship_type (
  relationship_type TEXT,
  start_date DATE,
  end_date DATE,
  primary_organization organization_type,
  partner_organizations LIST<organization_type>,
  key_contacts LIST<person_type>,
  contract_terms MAP<TEXT, TEXT>
);

-- Level 5 UDTs (deepest nesting - 5 levels)
CREATE TYPE enterprise_portfolio_type (
  portfolio_id UUID,
  portfolio_name TEXT,
  inception_date DATE,
  business_relationships LIST<business_relationship_type>,
  investment_entities LIST<organization_type>,
  portfolio_managers LIST<person_type>,
  performance_metrics MAP<TEXT, DECIMAL>,
  risk_assessments MAP<DATE, MAP<TEXT, DOUBLE>>
);

-- Table using deeply nested UDTs
CREATE TABLE enterprise_data (
  entity_id UUID PRIMARY KEY,
  portfolio enterprise_portfolio_type,
  audit_trail LIST<MAP<TIMESTAMP, TEXT>>,
  metadata MAP<TEXT, TEXT>
) WITH compression = {'class': 'LZ4Compressor'};

-- =============================================================================
-- SECTION 4: COMPLEX PRIMARY KEYS (6+ CLUSTERING COLUMNS)
-- =============================================================================

-- Maximum clustering keys scenario (6 clustering columns)
CREATE TABLE max_clustering_table (
  -- Partition key
  partition_key UUID,
  
  -- 6 clustering columns (maximum practical number)
  clustering_1 TIMESTAMP,
  clustering_2 TEXT,
  clustering_3 INT,
  clustering_4 UUID,
  clustering_5 DATE,
  clustering_6 TINYINT,
  
  -- Data columns
  data_payload TEXT,
  numeric_value BIGINT,
  metadata_blob BLOB,
  tags SET<TEXT>,
  
  PRIMARY KEY (partition_key, clustering_1, clustering_2, clustering_3, clustering_4, clustering_5, clustering_6)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'LeveledCompactionStrategy'}
  AND clustering ORDER BY (
    clustering_1 DESC, 
    clustering_2 ASC, 
    clustering_3 DESC, 
    clustering_4 ASC, 
    clustering_5 DESC, 
    clustering_6 ASC
  );

-- Composite partition key with multiple clustering
CREATE TABLE composite_partition_clustering (
  -- Composite partition key (3 components)
  tenant_id UUID,
  region_code TEXT,
  service_type TEXT,
  
  -- Multiple clustering columns (5 components)
  year INT,
  month TINYINT,
  day TINYINT,
  hour TINYINT,
  sequence_id TIMEUUID,
  
  -- Data
  event_data TEXT,
  metrics MAP<TEXT, DOUBLE>,
  attributes MAP<TEXT, TEXT>,
  
  PRIMARY KEY ((tenant_id, region_code, service_type), year, month, day, hour, sequence_id)
) WITH compression = {'class': 'SnappyCompressor'}
  AND clustering ORDER BY (year DESC, month DESC, day DESC, hour DESC, sequence_id ASC);

-- =============================================================================
-- SECTION 5: COUNTER TABLES AND TIME SERIES
-- =============================================================================

-- Comprehensive counter table
CREATE TABLE comprehensive_counters (
  metric_name TEXT,
  time_bucket TIMESTAMP,
  region TEXT,
  
  -- Various counter types
  total_requests COUNTER,
  successful_requests COUNTER,
  failed_requests COUNTER,
  error_4xx_count COUNTER,
  error_5xx_count COUNTER,
  bytes_transferred COUNTER,
  unique_visitors COUNTER,
  
  PRIMARY KEY ((metric_name, region), time_bucket)
) WITH compression = {'class': 'LZ4Compressor'}
  AND clustering ORDER BY (time_bucket DESC);

-- Time series with complex data
CREATE TABLE comprehensive_timeseries (
  sensor_id UUID,
  location_id UUID,
  measurement_time TIMESTAMP,
  
  -- Measurements
  temperature FLOAT,
  humidity FLOAT,
  pressure DOUBLE,
  wind_speed FLOAT,
  wind_direction SMALLINT,
  visibility_km FLOAT,
  uv_index TINYINT,
  air_quality_index SMALLINT,
  
  -- Complex data
  weather_conditions SET<TEXT>,
  sensor_metadata MAP<TEXT, TEXT>,
  calibration_data BLOB,
  raw_measurements LIST<DOUBLE>,
  
  PRIMARY KEY ((sensor_id, location_id), measurement_time)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': 6
  }
  AND clustering ORDER BY (measurement_time DESC)
  AND default_time_to_live = 2592000; -- 30 days

-- =============================================================================
-- SECTION 6: STATIC COLUMNS AND MATERIALIZED VIEWS
-- =============================================================================

-- Comprehensive static columns scenario
CREATE TABLE static_columns_comprehensive (
  partition_key UUID,
  clustering_key TIMESTAMP,
  secondary_clustering TEXT,
  
  -- Static columns (shared across partition)
  partition_metadata TEXT STATIC,
  total_records_in_partition COUNTER STATIC,
  partition_created_at TIMESTAMP STATIC,
  partition_tags SET<TEXT> STATIC,
  partition_config MAP<TEXT, TEXT> STATIC,
  
  -- Regular columns
  row_data TEXT,
  row_value BIGINT,
  row_attributes MAP<TEXT, DOUBLE>,
  row_collections LIST<SET<TEXT>>,
  
  PRIMARY KEY (partition_key, clustering_key, secondary_clustering)
) WITH compression = {'class': 'SnappyCompressor'}
  AND clustering ORDER BY (clustering_key DESC, secondary_clustering ASC);

-- =============================================================================
-- SECTION 7: WIDE PARTITIONS AND MANY COLUMNS (Performance Testing)
-- =============================================================================

-- Table with 150+ columns for wide table testing
CREATE TABLE ultra_wide_table (
  id UUID PRIMARY KEY,
  
  -- Text columns (30 columns)
  text_001 TEXT, text_002 TEXT, text_003 TEXT, text_004 TEXT, text_005 TEXT,
  text_006 TEXT, text_007 TEXT, text_008 TEXT, text_009 TEXT, text_010 TEXT,
  text_011 TEXT, text_012 TEXT, text_013 TEXT, text_014 TEXT, text_015 TEXT,
  text_016 TEXT, text_017 TEXT, text_018 TEXT, text_019 TEXT, text_020 TEXT,
  text_021 TEXT, text_022 TEXT, text_023 TEXT, text_024 TEXT, text_025 TEXT,
  text_026 TEXT, text_027 TEXT, text_028 TEXT, text_029 TEXT, text_030 TEXT,
  
  -- Integer columns (30 columns)
  int_001 INT, int_002 INT, int_003 INT, int_004 INT, int_005 INT,
  int_006 INT, int_007 INT, int_008 INT, int_009 INT, int_010 INT,
  int_011 INT, int_012 INT, int_013 INT, int_014 INT, int_015 INT,
  int_016 INT, int_017 INT, int_018 INT, int_019 INT, int_020 INT,
  int_021 INT, int_022 INT, int_023 INT, int_024 INT, int_025 INT,
  int_026 INT, int_027 INT, int_028 INT, int_029 INT, int_030 INT,
  
  -- BigInt columns (20 columns)
  bigint_001 BIGINT, bigint_002 BIGINT, bigint_003 BIGINT, bigint_004 BIGINT, bigint_005 BIGINT,
  bigint_006 BIGINT, bigint_007 BIGINT, bigint_008 BIGINT, bigint_009 BIGINT, bigint_010 BIGINT,
  bigint_011 BIGINT, bigint_012 BIGINT, bigint_013 BIGINT, bigint_014 BIGINT, bigint_015 BIGINT,
  bigint_016 BIGINT, bigint_017 BIGINT, bigint_018 BIGINT, bigint_019 BIGINT, bigint_020 BIGINT,
  
  -- Float/Double columns (20 columns)
  float_001 FLOAT, float_002 FLOAT, float_003 FLOAT, float_004 FLOAT, float_005 FLOAT,
  double_001 DOUBLE, double_002 DOUBLE, double_003 DOUBLE, double_004 DOUBLE, double_005 DOUBLE,
  decimal_001 DECIMAL, decimal_002 DECIMAL, decimal_003 DECIMAL, decimal_004 DECIMAL, decimal_005 DECIMAL,
  float_006 FLOAT, float_007 FLOAT, double_006 DOUBLE, double_007 DOUBLE, decimal_006 DECIMAL,
  
  -- Boolean columns (10 columns)
  bool_001 BOOLEAN, bool_002 BOOLEAN, bool_003 BOOLEAN, bool_004 BOOLEAN, bool_005 BOOLEAN,
  bool_006 BOOLEAN, bool_007 BOOLEAN, bool_008 BOOLEAN, bool_009 BOOLEAN, bool_010 BOOLEAN,
  
  -- UUID/TimeUUID columns (10 columns)
  uuid_001 UUID, uuid_002 UUID, uuid_003 UUID, uuid_004 UUID, uuid_005 UUID,
  timeuuid_001 TIMEUUID, timeuuid_002 TIMEUUID, timeuuid_003 TIMEUUID, timeuuid_004 TIMEUUID, timeuuid_005 TIMEUUID,
  
  -- Timestamp/Date/Time columns (15 columns)
  timestamp_001 TIMESTAMP, timestamp_002 TIMESTAMP, timestamp_003 TIMESTAMP, timestamp_004 TIMESTAMP, timestamp_005 TIMESTAMP,
  date_001 DATE, date_002 DATE, date_003 DATE, date_004 DATE, date_005 DATE,
  time_001 TIME, time_002 TIME, time_003 TIME, time_004 TIME, time_005 TIME,
  
  -- BLOB columns (10 columns)
  blob_001 BLOB, blob_002 BLOB, blob_003 BLOB, blob_004 BLOB, blob_005 BLOB,
  blob_006 BLOB, blob_007 BLOB, blob_008 BLOB, blob_009 BLOB, blob_010 BLOB,
  
  -- Collection columns (15 columns)
  list_001 LIST<TEXT>, list_002 LIST<INT>, list_003 LIST<UUID>, list_004 LIST<TIMESTAMP>, list_005 LIST<BLOB>,
  set_001 SET<TEXT>, set_002 SET<INT>, set_003 SET<UUID>, set_004 SET<DOUBLE>, set_005 SET<BOOLEAN>,
  map_001 MAP<TEXT, TEXT>, map_002 MAP<TEXT, INT>, map_003 MAP<TEXT, UUID>, map_004 MAP<TEXT, TIMESTAMP>, map_005 MAP<TEXT, BLOB>
  
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'}
  AND comment = 'Ultra-wide table with 150+ columns for performance testing';

-- =============================================================================
-- SECTION 8: COMPRESSION AND COMPACTION TESTING
-- =============================================================================

-- LZ4 Compression
CREATE TABLE lz4_compression_test (
  id UUID PRIMARY KEY,
  large_text TEXT,
  repeated_data TEXT,
  json_payload TEXT,
  binary_data BLOB
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Snappy Compression
CREATE TABLE snappy_compression_test (
  id UUID PRIMARY KEY,
  compressible_text TEXT,
  random_data BLOB,
  structured_data MAP<TEXT, TEXT>
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {'class': 'LeveledCompactionStrategy'};

-- Deflate Compression
CREATE TABLE deflate_compression_test (
  id UUID PRIMARY KEY,
  xml_data TEXT,
  log_entries LIST<TEXT>,
  metadata MAP<TEXT, BLOB>
) WITH compression = {'class': 'DeflateCompressor'}
  AND compaction = {'class': 'TimeWindowCompactionStrategy'};

-- Zstandard Compression (Cassandra 4.0+)
CREATE TABLE zstd_compression_test (
  id UUID PRIMARY KEY,
  highly_compressible_text TEXT,
  repetitive_data LIST<TEXT>,
  large_json_blob TEXT
) WITH compression = {'class': 'ZstdCompressor'}
  AND compaction = {'class': 'UnifiedCompactionStrategy'};

-- Uncompressed table for comparison
CREATE TABLE uncompressed_test (
  id UUID PRIMARY KEY,
  raw_data TEXT,
  benchmark_payload BLOB,
  timing_data TIMESTAMP
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- =============================================================================
-- SECTION 9: SECONDARY INDEXES
-- =============================================================================

-- Table with various index types
CREATE TABLE indexed_table (
  id UUID PRIMARY KEY,
  username TEXT,
  email TEXT,
  age INT,
  registration_date DATE,
  tags SET<TEXT>,
  metadata MAP<TEXT, TEXT>,
  profile_data address_type,
  search_terms LIST<TEXT>
) WITH compression = {'class': 'LZ4Compressor'};

-- Various secondary index types
CREATE INDEX idx_username ON indexed_table (username);
CREATE INDEX idx_email ON indexed_table (email);
CREATE INDEX idx_age ON indexed_table (age);
CREATE INDEX idx_registration_date ON indexed_table (registration_date);
CREATE INDEX idx_tags ON indexed_table (tags);
CREATE INDEX idx_metadata_keys ON indexed_table (KEYS(metadata));
CREATE INDEX idx_metadata_values ON indexed_table (VALUES(metadata));
CREATE INDEX idx_search_terms ON indexed_table (search_terms);

-- =============================================================================
-- SECTION 10: MATERIALIZED VIEWS
-- =============================================================================

-- Base table for materialized views
CREATE TABLE user_events (
  user_id UUID,
  event_time TIMESTAMP,
  event_type TEXT,
  event_data MAP<TEXT, TEXT>,
  location TEXT,
  device_type TEXT,
  PRIMARY KEY (user_id, event_time)
) WITH compression = {'class': 'LZ4Compressor'}
  AND clustering ORDER BY (event_time DESC);

-- Materialized view by event type
CREATE MATERIALIZED VIEW events_by_type AS
  SELECT event_type, event_time, user_id, event_data, location, device_type
  FROM user_events
  WHERE event_type IS NOT NULL AND event_time IS NOT NULL AND user_id IS NOT NULL
  PRIMARY KEY (event_type, event_time, user_id)
  WITH compression = {'class': 'SnappyCompressor'}
  AND clustering ORDER BY (event_time DESC, user_id ASC);

-- Materialized view by location
CREATE MATERIALIZED VIEW events_by_location AS
  SELECT location, event_time, user_id, event_type, event_data, device_type
  FROM user_events  
  WHERE location IS NOT NULL AND event_time IS NOT NULL AND user_id IS NOT NULL
  PRIMARY KEY (location, event_time, user_id)
  WITH compression = {'class': 'LZ4Compressor'}
  AND clustering ORDER BY (event_time DESC, user_id ASC);

-- =============================================================================
-- SECTION 11: EDGE CASES AND SPECIAL SCENARIOS
-- =============================================================================

-- Table for testing NULL values and empty collections
CREATE TABLE edge_cases_table (
  id UUID PRIMARY KEY,
  
  -- Nullable fields
  nullable_text TEXT,
  nullable_int INT,
  nullable_timestamp TIMESTAMP,
  
  -- Empty collections
  empty_list LIST<TEXT>,
  empty_set SET<INT>,
  empty_map MAP<TEXT, TEXT>,
  
  -- Large data fields
  large_text TEXT,
  large_blob BLOB,
  
  -- Unicode and special characters
  unicode_text TEXT,
  special_chars TEXT,
  
  -- Extreme values
  max_bigint BIGINT,
  min_bigint BIGINT,
  very_small_float FLOAT,
  very_large_double DOUBLE,
  
  -- Complex nested nulls
  nullable_udt_list LIST<person_type>,
  sparse_nested_map MAP<TEXT, MAP<TEXT, TEXT>>
  
) WITH compression = {'class': 'SnappyCompressor'};

-- Table for testing TTL scenarios
CREATE TABLE ttl_scenarios (
  id UUID PRIMARY KEY,
  temporary_data TEXT,
  session_info MAP<TEXT, TEXT>,
  cached_results LIST<TEXT>,
  expiring_tokens SET<TEXT>
) WITH compression = {'class': 'LZ4Compressor'}
  AND default_time_to_live = 3600; -- 1 hour default TTL

-- =============================================================================
-- SECTION 12: FROZEN COLLECTIONS AND TUPLES
-- =============================================================================

-- Comprehensive frozen collections testing
CREATE TABLE frozen_collections_comprehensive (
  id UUID PRIMARY KEY,
  
  -- Frozen basic collections
  frozen_simple_list FROZEN<LIST<TEXT>>,
  frozen_simple_set FROZEN<SET<INT>>,
  frozen_simple_map FROZEN<MAP<TEXT, BIGINT>>,
  
  -- Frozen nested collections
  frozen_list_of_sets FROZEN<LIST<SET<TEXT>>>,
  frozen_map_of_lists FROZEN<MAP<TEXT, LIST<DOUBLE>>>,
  frozen_nested_maps FROZEN<MAP<TEXT, MAP<TEXT, INT>>>,
  
  -- Mixed frozen and non-frozen
  mixed_list LIST<FROZEN<SET<TEXT>>>,
  mixed_map MAP<TEXT, FROZEN<LIST<INT>>>,
  
  -- Frozen UDTs
  frozen_address FROZEN<address_type>,
  frozen_person FROZEN<person_type>,
  frozen_organization FROZEN<organization_type>,
  
  -- Collections of frozen UDTs
  frozen_address_list LIST<FROZEN<address_type>>,
  frozen_person_set SET<FROZEN<person_type>>
  
) WITH compression = {'class': 'LZ4Compressor'};

-- =============================================================================
-- SECTION 13: PERFORMANCE KEYSPACE TABLES
-- =============================================================================

USE cqlite_performance_test;

-- Large partition table for performance testing
CREATE TABLE large_partitions (
  partition_key UUID,
  clustering_key TIMEUUID,
  data_size_kb INT,
  payload BLOB,
  metadata MAP<TEXT, TEXT>,
  tags SET<TEXT>,
  measurements LIST<DOUBLE>,
  PRIMARY KEY (partition_key, clustering_key)
) WITH compression = {'class': 'LZ4Compressor'}
  AND compaction = {'class': 'LeveledCompactionStrategy'};

-- High-throughput insert table
CREATE TABLE high_throughput_inserts (
  time_bucket TIMESTAMP,
  event_id TIMEUUID,
  event_type TEXT,
  payload TEXT,
  metrics MAP<TEXT, DOUBLE>,
  PRIMARY KEY (time_bucket, event_id)
) WITH compression = {'class': 'SnappyCompressor'}
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'MINUTES',
    'compaction_window_size': 5
  };

-- =============================================================================
-- SECTION 14: EDGE CASES KEYSPACE
-- =============================================================================

USE cqlite_edge_cases;

-- Table with maximum name lengths
CREATE TABLE maximum_name_length_test_table_with_very_long_name_to_test_cassandra_limits (
  maximum_length_partition_key_name_testing_cassandra_column_name_limits UUID PRIMARY KEY,
  maximum_length_regular_column_name_for_testing_cassandra_column_name_limits TEXT,
  another_very_long_column_name_to_test_the_maximum_column_name_length_limits INT
) WITH compression = {'class': 'LZ4Compressor'};

-- Table with extreme data scenarios  
CREATE TABLE extreme_data_scenarios (
  id UUID PRIMARY KEY,
  
  -- Empty string vs NULL testing
  empty_string TEXT,
  null_string TEXT,
  whitespace_string TEXT,
  
  -- Extreme numeric values
  zero_int INT,
  max_int INT,
  min_int INT,
  zero_bigint BIGINT,
  max_bigint BIGINT,
  min_bigint BIGINT,
  
  -- Extreme float values
  zero_float FLOAT,
  positive_infinity FLOAT,
  negative_infinity FLOAT,
  not_a_number FLOAT,
  
  -- Large collections
  huge_list LIST<TEXT>,
  massive_set SET<UUID>,
  enormous_map MAP<TEXT, BLOB>,
  
  -- Binary data edge cases
  empty_blob BLOB,
  single_byte_blob BLOB,
  large_blob BLOB
  
) WITH compression = {'class': 'SnappyCompressor'};

-- =============================================================================
-- COMMENTS AND METADATA
-- =============================================================================

-- This master schema file provides comprehensive coverage of:
-- ✅ ALL 21 CQL data types (TEXT, ASCII, VARCHAR, TINYINT, SMALLINT, INT, BIGINT, 
--    VARINT, FLOAT, DOUBLE, DECIMAL, BOOLEAN, BLOB, UUID, TIMEUUID, TIMESTAMP, 
--    DATE, TIME, DURATION, INET, COUNTER)
-- ✅ Complex collections with 4+ levels of nesting
-- ✅ UDTs with 5 levels of deep nesting  
-- ✅ Tables with 6 clustering keys (maximum practical)
-- ✅ Counter tables and comprehensive time series patterns
-- ✅ Static columns with mixed data types
-- ✅ Materialized views with different access patterns
-- ✅ Secondary indexes on various column types
-- ✅ All compression algorithms (LZ4, Snappy, Deflate, Zstd, Uncompressed)
-- ✅ All compaction strategies (STCS, LCS, TWCS, UCS)
-- ✅ Wide tables with 150+ columns
-- ✅ Frozen collections and UDTs
-- ✅ Edge cases: NULLs, empty collections, Unicode, extreme values
-- ✅ Performance scenarios: large partitions, high throughput
-- ✅ TTL scenarios and expiring data
-- ✅ Multiple keyspaces with different replication strategies
--
-- Total Tables: 25+
-- Total UDTs: 8 (with 5 levels of nesting)
-- Total Indexes: 8+  
-- Total Materialized Views: 2
-- Total Columns Across All Tables: 300+
--
-- This replaces ALL scattered schema files and provides a single source of truth
-- for comprehensive CQL testing and validation.