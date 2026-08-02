-- BTI (`da`) COMPOUND-CLUSTERING fixture schema (issue #3032).
--
-- `test_da/multiclustering_table` is a Cassandra 5.0.2 BTI ("da") SSTable with 3
-- partitions (pk = 1, 2, 3) whose clustering key has TWO components of DIFFERING
-- types — `bucket text` then `seq int`. Unlike `test_da/wide_table` (a single
-- `int` clustering column, whose row-index separators are all
-- `40 80 00 00 <byte>` and whose trie therefore degenerates into a chain of
-- 2-byte single-transition nodes), the bucket names here have DISTINCT FIRST
-- BYTES and heterogeneous lengths, so the byte-comparable separators branch
-- immediately below the `Rows.db` row-index trie root.
--
-- Per-partition shapes are deliberately NON-uniform (3x60, 5x32, 8x16 rows =
-- 180 + 160 + 128 = 468 rows), so the three tries differ structurally from each
-- other.
--
-- Fixture provenance: test-data/scripts/gen-multiclustering-bti.sh
-- (cassandra:5.0.2, storage_compatibility_mode NONE, selected_format bti,
-- column_index_size 2KiB, LZ4).

CREATE KEYSPACE IF NOT EXISTS test_da WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_da;

CREATE TABLE IF NOT EXISTS multiclustering_table (
    pk int,
    bucket text,
    seq int,
    payload text,
    PRIMARY KEY (pk, bucket, seq)
) WITH compression = {'class': 'LZ4Compressor'};
