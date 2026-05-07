-- Single-table schema for Node.js write API tests (Issue #391).
-- Contains exactly one CREATE TABLE so the no-heuristics mandate (Issue #28)
-- is satisfied: the write engine has an unambiguous write target.

CREATE KEYSPACE IF NOT EXISTS test_basic WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_basic;

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
