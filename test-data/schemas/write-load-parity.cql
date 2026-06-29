-- write-load-parity.cql — Cassandra 5.0.2 reference schema for the three
-- write_load_path byte-parity scenarios (issue #1190).
--
-- These three tables are written BY CASSANDRA (cqlsh + nodetool flush) to
-- produce committed reference SSTables, AND re-produced byte-for-byte by
-- CQLite's SSTableWriter for the same logical input. The byte-comparison test
-- (cqlite-core/tests/issue_1190_write_load_byte_parity.rs) diffs the two.
--
-- DETERMINISM CONTRACT (so two independent writers can byte-match):
--   * Every mutation carries an explicit USING TIMESTAMP so the cell writetime
--     (and therefore the EncodingStats.minTimestamp delta baseline) is fixed and
--     identical on both writers. The single fixed writetime is:
--         T_WRITE = 1700000000000000  (2023-11-14T22:13:20Z), micros
--   * Tables are UNCOMPRESSED (no CompressionInfo.db) so Data.db is a direct
--     byte slice with no chunk framing differences.
--   * Partition keys are int / (int,int) so the 4-byte big-endian key bytes and
--     token ordering are identical and deterministic on both writers.
--   * No TTL and no DELETE in these three tables: TTL/DELETE localDeletionTime
--     is wall-clock-derived (nowInSeconds) and therefore CANNOT byte-match across
--     two independent writers. Tombstone/TTL byte SHAPE parity is asserted
--     against derived expected values by issue_764/issue_821; whole-artifact
--     byte parity for live shapes is what this fixture establishes.
--
-- Tables:
--   1. finished_data        — int PK, one text col. Finished Data.db artifact.
--   2. ttl_tombstone_shape  — int PK, int CK, text col + static text col.
--                             Live static-row + clustering-row shapes (the
--                             tombstone/TTL artifact byte layout WITHOUT the
--                             wall-clock LDT cells, so it is reproducible).
--   3. partition_boundary   — int PK, int CK, text col. Multi-partition +
--                             multi-clustering-row partition-boundary layout.

CREATE KEYSPACE IF NOT EXISTS test_writeparity WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_writeparity;

CREATE TABLE IF NOT EXISTS finished_data (
    id   INT PRIMARY KEY,
    name TEXT
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

CREATE TABLE IF NOT EXISTS static_clustering_shape (
    id    INT,
    ck    INT,
    sdata TEXT STATIC,
    rdata TEXT,
    PRIMARY KEY (id, ck)
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

CREATE TABLE IF NOT EXISTS partition_boundary (
    id INT,
    ck INT,
    v  TEXT,
    PRIMARY KEY (id, ck)
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
