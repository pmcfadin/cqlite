-- tombstone-parity.cql — Tombstone / TTL parity SSTable fixtures for CQLite epic #972
--
-- Generated with Cassandra 5.0.2, storage_compatibility_mode: CASSANDRA_4 (nb BIG format)
--
-- Keyspace: test_tomb (isolated from test_deltas so the two corpora never collide)
-- SSTable version: nb (BIG format, storage_compatibility_mode: CASSANDRA_4)
--
-- All writes use explicit `USING TIMESTAMP <micros>` and explicit TTLs so that
-- writetime, ttl, and local_deletion_time are 100% deterministic and reproducible.
--
-- FIXED TIMESTAMP SCHEME (documented in generate-tombstone-parity.sh too)
-- -----------------------------------------------------------------------
--   BASE_EPOCH_SECONDS = 1609459200   (2021-01-01T00:00:00Z)
--   BASE_TS_MICROS     = 1609459200000000
--
--   Per-generation write timestamps (micros) — strictly increasing so newer
--   deletes always shadow older live data:
--     T_GEN1 = 1609459200000000  (2021-01-01T00:00:00Z)  base
--     T_GEN2 = 1609545600000000  (2021-01-02T00:00:00Z)  base + 1 day
--     T_GEN3 = 1609632000000000  (2021-01-03T00:00:00Z)  base + 2 days
--
--   DELETE timestamps reuse the same per-generation constants (a newer
--   generation's delete carries T_GEN2/T_GEN3, an older live row carries
--   T_GEN1) so resurrection / shadowing is provable purely from writetimes.
--
--   gc_before TTL boundary (table gc_before_boundary, issue #1011)
--   ---------------------------------------------------------------
--   Cells are written with writetime-seconds = BASE_EPOCH_SECONDS (1609459200).
--   localDeletionTime = writetime_seconds + TTL.  We pick the documented
--   boundary value:
--       GC_BOUNDARY_LDT = 1609545600   (2021-01-02T00:00:00Z = base + 86400s)
--   so:
--       TTL = 86400  -> localDeletionTime == 1609545600  (EXACTLY at boundary)
--       TTL = 86401  -> localDeletionTime == 1609545601  (one second PAST boundary)
--   These two TTLs are written under the SAME writetime so the only varying
--   field is localDeletionTime, landing it on / past the chosen boundary.
--
-- Tables (table -> issue/manifest-ID -> generations):
--   1. gc_before_boundary      -> #1011  (TTL localDeletionTime boundary)        1 gen
--   2. tombstone_histogram     -> #1011  (non-empty drop-time histogram, gc=0)   1 gen
--   3. skipped_partition_delete-> #1012  (skipped reads / tombstone-only part.)  2 gens
--   4. resurrection_gc0        -> #1014  (gc_grace=0, deletes shadow older)      2 gens
--   5. resurrection_gc_positive-> #1014  (gc_grace=864000, tombstones retained)  2 gens
--   6. dropped_regular_col     -> #1015  (per-cell dropped regular column)       2 gens
--   7. dropped_static_col      -> #1015  (dropped STATIC column)                 2 gens
--   8. static_with_tombstones  -> #1015  (static survives adjacent tombstones)   1 gen
--   9. wide_range_tombstone    -> #1013  (range tombstone at index-block edges)  1 gen

CREATE KEYSPACE IF NOT EXISTS test_tomb WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_tomb;

-- -----------------------------------------------------------------------
-- Table 1: gc_before_boundary  (issue #1011)
-- Shape: TTL'd cells whose localDeletionTime lands ON and just PAST a chosen,
--        documented boundary (GC_BOUNDARY_LDT = 1609545600).
-- Pattern: INSERT cells USING TIMESTAMP T_GEN1 AND TTL {86400|86401}.  All
--          cells share writetime-seconds = BASE_EPOCH_SECONDS so the only
--          difference is the TTL -> deterministic localDeletionTime.
-- gc_grace_seconds left at the table default (864000); this fixture is about
-- localDeletionTime math, not about purge.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gc_before_boundary (
    pk  INT,
    ck  INT,
    val TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};

-- -----------------------------------------------------------------------
-- Table 2: tombstone_histogram  (issue #1011)
-- Shape: A table that yields a NON-empty "Estimated tombstone drop times"
--        histogram in sstablemetadata (existing fixtures all show empty ones).
-- Pattern: Insert live rows, then apply row + cell tombstones with explicit
--          deletion timestamps.  gc_grace_seconds = 0 so each tombstone's
--          localDeletionTime becomes an immediately-droppable point that
--          Cassandra records in the drop-time histogram at flush.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tombstone_histogram (
    pk    INT,
    ck    INT,
    col_a TEXT,
    col_b TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'}
  AND gc_grace_seconds = 0;

-- -----------------------------------------------------------------------
-- Table 3: skipped_partition_delete  (issue #1012)  — MULTI-GEN (2 flushes)
-- Shape: Cross-generation partition tombstone + a tombstone-only partition.
-- Pattern:
--   gen-1: INSERT older live rows into partition P (pk=1) at clustering keys
--          within a known bound (ck 1..10), USING TIMESTAMP T_GEN1.  flush.
--   gen-2: DELETE FROM ... WHERE pk=1 (partition tombstone) USING TIMESTAMP
--          T_GEN2 (> T_GEN1) so the whole partition is shadowed.  flush.
--   Partition Q (pk=2) is a TOMBSTONE-ONLY partition: it never receives a
--          live row in any generation; gen-2 issues DELETE WHERE pk=2 only.
-- Result: nb-1-big (live P) + nb-2-big (partition tombstones for P and Q).
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS skipped_partition_delete (
    pk  INT,
    ck  INT,
    val TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};

-- -----------------------------------------------------------------------
-- Table 4: resurrection_gc0  (issue #1014)  — MULTI-GEN (2 flushes)
-- Shape: gc_grace_seconds = 0; newer deletes shadow older live rows across
--        generations (proves no resurrection even at gc=0).
-- Pattern:
--   gen-1: live rows (pk=1 ck 1..5) USING TIMESTAMP T_GEN1.  flush.
--   gen-2: row delete (ck=2), cell delete (col on ck=3), and partition delete
--          (pk=2, which had gen-1 rows) USING TIMESTAMP T_GEN2.  flush.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS resurrection_gc0 (
    pk    INT,
    ck    INT,
    val   TEXT,
    extra TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'}
  AND gc_grace_seconds = 0;

-- -----------------------------------------------------------------------
-- Table 5: resurrection_gc_positive  (issue #1014)  — MULTI-GEN (2 flushes)
-- Shape: Same write/delete shapes as resurrection_gc0 but gc_grace_seconds is
--        positive (864000), so tombstones are RETAINED across flushes.
-- Pattern: identical to resurrection_gc0 (gen-1 live, gen-2 deletes).
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS resurrection_gc_positive (
    pk    INT,
    ck    INT,
    val   TEXT,
    extra TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'}
  AND gc_grace_seconds = 864000;

-- -----------------------------------------------------------------------
-- Table 6: dropped_regular_col  (issue #1015)  — MULTI-GEN (2 flushes)
-- Shape: Per-cell dropped-column purge with distinct pre/post-drop timestamps.
-- Pattern:
--   Phase A (gen-1): INSERT pk/ck with BOTH keep_col and drop_col USING
--          TIMESTAMP T_GEN1 (EARLY).  flush.  Then ALTER TABLE ... DROP drop_col.
--   Phase B (gen-2): INSERT new rows into keep_col USING TIMESTAMP T_GEN2
--          (LATER).  flush.
-- The serialization header of gen-1 still references drop_col; gen-2 does not.
-- NOTE: drop_col is declared here so the initial Phase-A writes type-check;
--       the script ALTERs it away between flushes.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dropped_regular_col (
    pk       INT,
    ck       INT,
    keep_col TEXT,
    drop_col TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};

-- -----------------------------------------------------------------------
-- Table 7: dropped_static_col  (issue #1015)  — MULTI-GEN (2 flushes)
-- Shape: A dropped STATIC column.  The serialization header must still declare
--        the dropped static column for the gen-1 SSTable to remain readable.
-- Pattern:
--   gen-1: INSERT static value + regular rows USING TIMESTAMP T_GEN1.  flush.
--          Then ALTER TABLE ... DROP stat_col.
--   gen-2: INSERT a fresh regular row USING TIMESTAMP T_GEN2.  flush.
-- NOTE: stat_col is declared here so Phase-A writes type-check; the script
--       ALTERs it away between flushes.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dropped_static_col (
    pk       INT,
    ck       INT,
    stat_col TEXT STATIC,
    row_col  TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};

-- -----------------------------------------------------------------------
-- Table 8: static_with_tombstones  (issue #1015)
-- Shape: A live static cell that survives adjacent row / cell / range
--        tombstones in the SAME partition.  SINGLE flush.
-- Pattern (all USING TIMESTAMP, single partition pk=1):
--   - INSERT static value (stat_col) USING TIMESTAMP T_GEN1.
--   - INSERT several clustering rows (ck 1..6) USING TIMESTAMP T_GEN1.
--   - Row tombstone:   DELETE FROM ... USING TIMESTAMP T_GEN2 WHERE pk=1 AND ck=2.
--   - Cell tombstone:  DELETE row_col FROM ... USING TIMESTAMP T_GEN2 WHERE pk=1 AND ck=3.
--   - Range tombstone: DELETE FROM ... USING TIMESTAMP T_GEN2 WHERE pk=1 AND ck>=4 AND ck<=5.
--   The static cell is NEVER deleted -> proves static survives adjacent tombstones.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS static_with_tombstones (
    pk       INT,
    ck       INT,
    stat_col TEXT STATIC,
    row_col  TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};

-- -----------------------------------------------------------------------
-- Table 9: wide_range_tombstone  (issue #1013)  — nb BIG only
-- Shape: A SINGLE wide partition large enough to force MULTIPLE column-index
--        (IndexInfo) blocks in the BIG format, with range tombstones whose
--        markers land at block edges.
-- Pattern (single partition pk=1):
--   - Insert MANY clustering rows (ck 1..4000) each with a wide TEXT value so
--     the serialized partition exceeds column_index_size_in_kb (default 64KB),
--     forcing several IndexInfo blocks.
--   - Apply range tombstones (USING TIMESTAMP T_GEN2) chosen to fall at block
--     edges.  Exact block boundaries depend on serialized size; we
--     over-provision rows and DOCUMENT the intended edge for each delete.  The
--     lead/tests inspect the resulting Index.db to confirm placement.
-- NOTE: BTI (`da`) generation is NOT supported by this CASSANDRA_4-mode
--       container.  The BTI variant of this fixture is DEFERRED.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS wide_range_tombstone (
    pk  INT,
    ck  INT,
    val TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};
