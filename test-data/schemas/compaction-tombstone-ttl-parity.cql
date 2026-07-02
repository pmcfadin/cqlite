-- compaction-tombstone-ttl-parity.cql — Cassandra 5.0.2 reference schema for the
-- TOMBSTONE / TTL COMPACTION byte-parity slice (issue #1387, epic #973).
--
-- This is the tombstone/TTL analogue of compaction-parity.cql (issue #1017, live
-- cells only). Each table below is written BY CASSANDRA as TWO (or more)
-- overlapping SSTables, then MAJOR-COMPACTED (`nodetool compact`) into a single
-- output SSTable. CQLite re-produces the same inputs and runs its own
-- `compact_sstables` over them; the byte-comparison test
-- (cqlite-core/tests/issue_1387_tombstone_ttl_compaction_byte_parity.rs) diffs the
-- two COMPACTED outputs for Data.db / Index.db / Summary.db / Digest.crc32.
--
-- ============================================================================
-- DETERMINISM CONTRACT (so two independent compactors can byte-match)
--
--   * Every mutation carries an explicit USING TIMESTAMP so the cell writetime
--     (and therefore the EncodingStats.minTimestamp delta baseline) is fixed and
--     identical on both engines. Fixed writetimes (micros):
--         T_A = 1000   (older generation)
--         T_B = 2000   (newer generation, wins overlaps / shadows older cells)
--   * Every DELETE carries an explicit USING TIMESTAMP so the tombstone's
--     markedForDeleteAt is fixed. The tombstone's localDeletionTime is derived by
--     Cassandra from nowInSeconds at write time; to make the ON-DISK LDT
--     deterministic and reproducible, the generator PINS the wall clock inside the
--     container via a fixed nowInSeconds where the shape depends on it, and the
--     CQLite side stamps the SAME LDT explicitly via Mutation::with_local_deletion_time
--     / CellOperation::Delete { local_deletion_time }.
--   * PURGE determinism: a tombstone is gc-purgeable iff localDeletionTime < gcBefore
--     where gcBefore = nowInSeconds - gc_grace_seconds. Two purge regimes are pinned:
--       - gc_grace_seconds = 864000 (10 days, DEFAULT): tombstones with a recent LDT
--         are NOT purgeable, so they SURVIVE the compaction (shadow markers kept).
--       - gc_grace_seconds = 0: gcBefore = nowInSeconds, so ANY tombstone whose LDT
--         <= now is immediately purgeable. A major compaction with no other data to
--         shadow drops the purged tombstone entirely (deterministic, wall-clock
--         independent because LDT is always <= now).
--   * Tables are UNCOMPRESSED (no CompressionInfo.db) so Data.db is a direct byte
--     slice; partition keys are int / (int,int) so key bytes + Murmur3 token
--     ordering are identical on both engines.
--   * autocompaction is disabled and a single explicit major compaction is run.
-- ============================================================================
--
-- Tables (one per issue #1387 coverage area):
--   1. shadow_row_delete   — (a) newer-gen ROW tombstone shadows older-gen live row,
--                            gc_grace default so the shadow marker SURVIVES.
--   2. ttl_expired_live    — (b) older-gen TTL cell already expired + a live cell;
--                            expired cell collapses to a tombstone on compaction.
--   3. gc_purge_grace0     — (c) gc_grace_seconds=0: a row tombstone with no live
--                            data to shadow is PURGED away by the major compaction.
--   4. rt_cross_gen        — (d) a range tombstone in gen A + an overlapping range
--                            tombstone / rows in gen B: cross-generation RT open/close
--                            boundary synthesis in the merged output.

CREATE KEYSPACE IF NOT EXISTS test_compaction_tombstone_ttl WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_compaction_tombstone_ttl;

-- (a) ROW-tombstone shadowing: default gc_grace so the tombstone SURVIVES compaction.
CREATE TABLE IF NOT EXISTS shadow_row_delete (
    id INT,
    ck INT,
    v  TEXT,
    PRIMARY KEY (id, ck)
) WITH compression = {'enabled': 'false'}
  AND gc_grace_seconds = 864000
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- (b) TTL: an already-expired TTL cell coexists with a live cell across generations.
--     Default gc_grace so the expired cell's derived tombstone SURVIVES compaction.
CREATE TABLE IF NOT EXISTS ttl_expired_live (
    id INT,
    ck INT,
    v  TEXT,
    PRIMARY KEY (id, ck)
) WITH compression = {'enabled': 'false'}
  AND gc_grace_seconds = 864000
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- (c) gc-purgeable: gc_grace_seconds = 0 so a tombstone with nothing to shadow is
--     PURGED by the major compaction (deterministic gcBefore = nowInSeconds).
CREATE TABLE IF NOT EXISTS gc_purge_grace0 (
    id INT,
    ck INT,
    v  TEXT,
    PRIMARY KEY (id, ck)
) WITH compression = {'enabled': 'false'}
  AND gc_grace_seconds = 0
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};

-- (d) cross-generation range-tombstone open/close boundary synthesis. Default
--     gc_grace so the range markers SURVIVE compaction.
CREATE TABLE IF NOT EXISTS rt_cross_gen (
    id INT,
    ck INT,
    v  TEXT,
    PRIMARY KEY (id, ck)
) WITH compression = {'enabled': 'false'}
  AND gc_grace_seconds = 864000
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
