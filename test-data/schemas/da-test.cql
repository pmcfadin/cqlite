-- DA Format (BTI) Test Schema for CQLite Issue #654
-- Generated with Cassandra 5.0.2, sstable.selected_format: bti (da BTI format)
--
-- Covers basic table types to validate BTI index file structure
-- (Partitions.db, Rows.db instead of Index.db, Summary.db)
--
-- Keyspace: test_da
-- SSTable version: da (BTI format)
-- Parser status: SKIP-PENDING until future BTI read epic

CREATE KEYSPACE IF NOT EXISTS test_da WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_da;

-- 1. Simple types
CREATE TABLE IF NOT EXISTS simple_table (
    id UUID PRIMARY KEY,
    name TEXT,
    age INT,
    salary BIGINT,
    active BOOLEAN,
    created TIMESTAMP
) WITH compression = {'class': 'LZ4Compressor'};

-- 2. Collections
CREATE TABLE IF NOT EXISTS collection_table (
    id UUID PRIMARY KEY,
    tags SET<TEXT>,
    scores LIST<INT>,
    properties MAP<TEXT, TEXT>
) WITH compression = {'class': 'LZ4Compressor'};

-- 3. Formerly-TTL rows (issue #1935, #1896 cluster A owner decision:
--    default_time_to_live removed — it time-bombed the fixture so every row read
--    as expired. Table name kept for corpus stability; regenerated WITHOUT TTL.)
CREATE TABLE IF NOT EXISTS ttl_table (
    id UUID PRIMARY KEY,
    data TEXT,
    expiring_value INT
) WITH compression = {'class': 'LZ4Compressor'};
