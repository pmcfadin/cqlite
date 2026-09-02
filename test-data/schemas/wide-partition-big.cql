-- BIG (`nb`) COMPRESSED wide-partition fixture schema (Issue #3890 regression).
--
-- `test_big/wide_partition` is a real Cassandra 5.0 BIG ("nb") SSTable whose
-- LZ4-compressed `Data.db` spans 114 chunks over ~1.8 MB — the largest committed
-- multi-chunk compressed fixture in the corpus, and therefore the one whose
-- single-partition seek window overruns furthest into the successor partition
-- when the PARSE input is not bounded by the partition's authoritative extent
-- (issue #3890).
--
-- Its schema was previously available only as an INLINE string literal in
-- `cqlite-core/tests/issue_1578_scan_stream_stitch_table_id_regression.rs:49`,
-- which is why no committed-schema lane (the point-vs-full differential, the
-- query-semantics oracle) could reach this table. Committing it here makes the
-- table addressable by every lane that resolves schemas through
-- `test-data/schemas` — it does NOT change the fixture bytes.
--
-- Fixture provenance: cqlite-core/tests/issue_1578_scan_stream_stitch_table_id_regression.rs
-- (wide_partition(pk int, ck int, payload text, PRIMARY KEY (pk, ck)), LZ4).

CREATE KEYSPACE IF NOT EXISTS test_big WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = true;

USE test_big;

CREATE TABLE IF NOT EXISTS wide_partition (
    pk int,
    ck int,
    payload text,
    PRIMARY KEY (pk, ck)
) WITH compression = {'class': 'LZ4Compressor'};
