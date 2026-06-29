-- compaction-parity.cql — Cassandra 5.0.2 reference schema for the live-cell
-- compaction byte-parity slice (issue #1017, epic #973).
--
-- These two tables are written BY CASSANDRA (cqlsh + nodetool flush) as TWO
-- overlapping SSTables each, then MAJOR-COMPACTED (`nodetool compact`) into a
-- single output SSTable. CQLite re-produces the same two inputs and runs its own
-- `compact_sstables` over them. The byte-comparison test
-- (cqlite-core/tests/issue_1017_live_cell_compaction_byte_parity.rs) diffs the
-- two COMPACTED outputs.
--
-- This is the smallest blocking byte-for-byte COMPACTION slice: live cells only,
-- last-write-wins overlap by timestamp, NO TTL, NO tombstones, NO range
-- tombstones, NO static rows, NO collections/UDTs, NO schema evolution.
--
-- DETERMINISM CONTRACT (so two independent compactors can byte-match):
--   * Every mutation carries an explicit USING TIMESTAMP so the cell writetime
--     (and therefore the EncodingStats.minTimestamp delta baseline) is fixed and
--     identical on both engines. The two fixed writetimes are:
--         T_A = 1000   (older generation), micros
--         T_B = 2000   (newer generation, wins overlaps), micros
--   * Tables are UNCOMPRESSED (no CompressionInfo.db) so Data.db is a direct
--     byte slice with no chunk framing differences.
--   * Partition keys are int / (int,int) so the 4-byte big-endian key bytes and
--     Murmur3 token ordering are identical and deterministic on both engines.
--   * No TTL and no DELETE: purge never fires for live cells, so the compaction
--     output is independent of gcBefore / nowInSeconds and reproducible.
--   * autocompaction is disabled and a single explicit major compaction is run,
--     so exactly two inputs merge into exactly one deterministic output.
--
-- Tables:
--   1. live_no_clustering — int PK, one text col. Partition-key-only LWW overlap.
--   2. live_clustering    — int PK, int CK, text col. Clustering LWW overlap with
--                           preserved partition + clustering order.

CREATE KEYSPACE IF NOT EXISTS test_compactionparity WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_compactionparity;

CREATE TABLE IF NOT EXISTS live_no_clustering (
    id INT PRIMARY KEY,
    v  TEXT
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

CREATE TABLE IF NOT EXISTS live_clustering (
    id INT,
    ck INT,
    v  TEXT,
    PRIMARY KEY (id, ck)
) WITH compression = {'enabled': 'false'}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
