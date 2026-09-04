-- BIG (`nb`) COMPRESSED wide-partition fixture schema (Issue #3890 regression).
--
-- `test_big/wide_partition` is a real Cassandra 5.0 BIG ("nb") SSTable with a
-- multi-chunk LZ4 `Data.db`: 114 chunks over an UNCOMPRESSED data section of
-- 1,837,037 bytes, which is 27,823 bytes on disk compressed (chunk_length 16384).
-- That makes it the LARGEST committed multi-chunk compressed BIG ("nb") fixture in
-- the corpus, and second overall — `test_da/wide_table` is larger at 115 chunks /
-- 1,857,615 bytes, but it is BTI ("da"). A wide compressed partition is where an
-- unbounded PARSE input overruns furthest into the successor partition when the
-- single-partition seek is not bounded by the partition's authoritative extent
-- (issue #3890).
--
-- Every figure above is READ FROM `CompressionInfo.db`'s header (chunk_count,
-- data_length) and `stat`, not estimated. Re-derive the ranking with:
--
--   for f in $(git ls-files 'test-data/datasets/sstables/**-Data.db'); do
--     ci=$(ls "$(dirname "$f")"/*-CompressionInfo.db 2>/dev/null | head -1)
--     [ -n "$ci" ] && printf '%s\t%s\n' "$(python3 -c '
--   import struct,sys; b=open(sys.argv[1],"rb").read(); o=0
--   n=struct.unpack_from(">H",b,o)[0]; o+=2+n
--   k=struct.unpack_from(">i",b,o)[0]; o+=4
--   for _ in range(k):
--       l=struct.unpack_from(">H",b,o)[0]; o+=2+l
--       l=struct.unpack_from(">H",b,o)[0]; o+=2+l
--   o+=8  # chunk_length, max_compressed_length
--   dl=struct.unpack_from(">q",b,o)[0]; o+=8
--   print(struct.unpack_from(">i",b,o)[0], dl)' "$ci")" "$f"
--   done | sort -rn | head
--
-- NOTE the units, because getting them wrong looks like a 65x error: `chunk_length`
-- (16384) is the UNCOMPRESSED chunk size, so dividing the COMPRESSED file size by
-- it does not yield the chunk count. The count is a field in the header.
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
