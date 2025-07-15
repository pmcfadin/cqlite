-- Create test keyspaces with different configurations
CREATE KEYSPACE test_keyspace 
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 2
};

CREATE KEYSPACE test_keyspace_nts 
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': 2
};

USE test_keyspace;

-- Table with all primitive data types
CREATE TABLE all_types (
  id UUID PRIMARY KEY,
  text_col TEXT,
  ascii_col ASCII,
  varchar_col VARCHAR,
  bigint_col BIGINT,
  blob_col BLOB,
  boolean_col BOOLEAN,
  counter_col COUNTER,
  date_col DATE,
  decimal_col DECIMAL,
  double_col DOUBLE,
  duration_col DURATION,
  float_col FLOAT,
  inet_col INET,
  int_col INT,
  smallint_col SMALLINT,
  time_col TIME,
  timestamp_col TIMESTAMP,
  timeuuid_col TIMEUUID,
  tinyint_col TINYINT,
  varint_col VARINT
) WITH compression = {'class': 'LZ4Compressor'};

-- Table with collections
CREATE TABLE collections_table (
  id UUID PRIMARY KEY,
  list_col LIST<TEXT>,
  set_col SET<INT>,
  map_col MAP<TEXT, INT>,
  frozen_list FROZEN<LIST<TEXT>>,
  frozen_set FROZEN<SET<INT>>,
  frozen_map FROZEN<MAP<TEXT, INT>>
) WITH compression = {'class': 'SnappyCompressor'};

-- Table with UDTs
CREATE TYPE address (
  street TEXT,
  city TEXT,
  state TEXT,
  zip_code TEXT
);

CREATE TYPE person (
  name TEXT,
  age INT,
  address FROZEN<address>
);

CREATE TABLE users (
  id UUID PRIMARY KEY,
  profile FROZEN<person>,
  addresses LIST<FROZEN<address>>,
  metadata MAP<TEXT, TEXT>
) WITH compression = {'class': 'ZstdCompressor'};

-- Table with clustering columns
CREATE TABLE time_series (
  partition_key TEXT,
  timestamp TIMESTAMP,
  value DOUBLE,
  tags MAP<TEXT, TEXT>,
  PRIMARY KEY (partition_key, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND compression = {'class': 'DeflateCompressor'};

-- Table with multiple clustering columns
CREATE TABLE multi_clustering (
  pk TEXT,
  ck1 TEXT,
  ck2 INT,
  ck3 TIMESTAMP,
  data TEXT,
  PRIMARY KEY (pk, ck1, ck2, ck3)
) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC, ck3 ASC);

-- Large table for testing pagination
CREATE TABLE large_table (
  id UUID PRIMARY KEY,
  data TEXT,
  metadata BLOB,
  created_at TIMESTAMP
);

-- Counter table
CREATE TABLE counters (
  id TEXT PRIMARY KEY,
  view_count COUNTER,
  like_count COUNTER,
  share_count COUNTER
);

-- Static columns table
CREATE TABLE static_test (
  partition_key TEXT,
  clustering_key TEXT,
  static_data TEXT STATIC,
  regular_data TEXT,
  PRIMARY KEY (partition_key, clustering_key)
);

-- Materialized view
CREATE MATERIALIZED VIEW users_by_city AS
  SELECT city, id, profile
  FROM users
  WHERE profile IS NOT NULL AND profile.address.city IS NOT NULL
  PRIMARY KEY (city, id);

-- Secondary index
CREATE INDEX ON time_series (value);
CREATE INDEX ON users (metadata);