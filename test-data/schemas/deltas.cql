-- deltas.cql — Delete-bearing SSTable fixtures for CQLite issue #701
-- Coordinates with issue #667 (tombstone readback coverage).
--
-- Generated with Cassandra 5.0.2, storage_compatibility_mode: CASSANDRA_4 (nb BIG format)
--
-- Covers all eight delete/shape cases the delta-scan envelope must represent:
--   1. Cell tombstone    (DELETE col FROM ...)                 → cell_tombstones
--   2. Row tombstone     (DELETE FROM ... WHERE pk AND ck)     → row_tombstones
--   3. Range tombstone   (DELETE FROM ... WHERE pk AND ck>=…)  → range_tombstones
--      • includes prefix bound on multi-column clustering
--      • includes mixed open/closed inclusivity
--   4. Partition tombstone (DELETE FROM ... WHERE pk)          → partition_tombstones
--   5. TTL'd cells (live, expiration metadata present)         → ttl_cells
--   6. Static column writes alongside regular rows             → static_with_rows
--   7. Collection operations:
--        append   (s = s + {…})
--        overwrite (s = {…})
--        element remove (s = s - {…})                          → collection_ops
--   8. Partial UPDATE (no row liveness) vs INSERT              → partial_updates
--
-- Keyspace: test_deltas
-- SSTable version: nb (BIG format, storage_compatibility_mode: CASSANDRA_4)

CREATE KEYSPACE IF NOT EXISTS test_deltas WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_deltas;

-- -----------------------------------------------------------------------
-- Table 1: cell_tombstones
-- Shape: Cell tombstone (DELETE col FROM … WHERE pk AND ck)
-- Pattern: Insert rows with col_a and col_b; then null-out col_b on selected
--          rows via "UPDATE … SET col_b = null …" to produce cell tombstones.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cell_tombstones (
    pk  INT,
    ck  INT,
    col_a TEXT,
    col_b TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};

-- -----------------------------------------------------------------------
-- Table 2: row_tombstones
-- Shape: Row tombstone (DELETE FROM … WHERE pk AND ck)
-- Pattern: Insert several clustering rows per partition; delete specific rows.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS row_tombstones (
    pk  INT,
    ck  INT,
    val TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};

-- -----------------------------------------------------------------------
-- Table 3: range_tombstones
-- Shape: Range tombstone with multi-column clustering key.
--   - Prefix bound (first CK component open, second ignored): pk=1
--   - Closed-open range:  pk=2 (ck1 IN [2..4), i.e. ck1>=2 AND ck1<4)
--   - Mixed inclusivity:  pk=3 (ck1>1 AND ck1<=3)
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS range_tombstones (
    pk   INT,
    ck1  INT,
    ck2  TEXT,
    val  TEXT,
    PRIMARY KEY (pk, ck1, ck2)
) WITH compression = {'class': 'LZ4Compressor'};

-- -----------------------------------------------------------------------
-- Table 4: partition_tombstones
-- Shape: Partition tombstone (DELETE FROM … WHERE pk)
-- Pattern: Insert rows into several partitions; delete whole partitions.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS partition_tombstones (
    pk  INT,
    ck  INT,
    val TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};

-- -----------------------------------------------------------------------
-- Table 5: ttl_cells
-- Shape: TTL'd cells — live, with expiration metadata in SSTable.
-- Pattern: INSERT … USING TTL 3600 so every cell has a local_deletion_time
--          and ttl in the SSTable cell metadata.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ttl_cells (
    pk   INT,
    ck   INT,
    val  TEXT,
    extra TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};

-- -----------------------------------------------------------------------
-- Table 6: static_with_rows
-- Shape: Static column writes alongside regular rows.
-- Pattern: Write static_col (partition-level) + per-row col; verify both
--          survive independently in the SSTable.
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS static_with_rows (
    pk          INT,
    ck          INT,
    static_col  TEXT STATIC,
    row_col     TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};

-- -----------------------------------------------------------------------
-- Table 7: collection_ops
-- Shape: Collection append vs overwrite vs element removal.
--   - Append:         UPDATE … SET tags = tags + {'new'}
--   - Overwrite:      UPDATE … SET tags = {'only'}
--   - Element remove: UPDATE … SET tags = tags - {'remove_me'}
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS collection_ops (
    pk   INT,
    ck   INT,
    tags SET<TEXT>,
    vals LIST<INT>,
    props MAP<TEXT,TEXT>,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};

-- -----------------------------------------------------------------------
-- Table 8: partial_updates
-- Shape: Partial UPDATE (no row liveness token) vs INSERT (has row liveness).
-- Pattern: Row ck=1 inserted via INSERT (has liveness token).
--          Row ck=2 created via UPDATE only (no liveness — column-level only).
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS partial_updates (
    pk   INT,
    ck   INT,
    col_a TEXT,
    col_b TEXT,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};
