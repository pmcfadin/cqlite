-- BTI (`da`) multi-clustering-row fixture schema (Issue #953 regression).
--
-- `test_da/wide_table` is a Cassandra 5.0 BTI ("da") SSTable with 3 partitions
-- (pk = 1, 2, 3), each holding 300 clustering rows (ck = 0..299) of a ~2 KiB
-- `payload` text column. It is the BTI multi-row-per-partition fixture the
-- within-SSTable seek (Issue #953 / Epic #951) must return ALL rows for; the
-- original #953 decoder dropped every row after the first.
--
-- Fixture provenance: cqlite-core/tests/issue_832_bti_traversal.rs
-- (wide_table(pk int, ck int, payload text, PRIMARY KEY (pk, ck)), LZ4).

CREATE KEYSPACE IF NOT EXISTS test_da WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_da;

CREATE TABLE IF NOT EXISTS wide_table (
    pk int,
    ck int,
    payload text,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};
