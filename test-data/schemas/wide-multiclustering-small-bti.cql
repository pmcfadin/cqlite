-- SMALL Cassandra-WRITTEN BTI (`da`) golden — issue #3234 (owner decision 4B).
--
-- WHAT THIS IS: a COMMITTED, Cassandra-5.0.2-WRITTEN BTI SSTable with the same wide
-- multi-component clustering shape as the #3234 perf corpus —
-- `PRIMARY KEY (pk, bucket, seq)`, LZ4 chunk_length_in_kb=16 — sized so its
-- `sstabledump -l` golden is committable:
--
--   test-data/datasets/sstables/test_da/wide_multiclustering_small-6cf636608f6511f18f0f8ffe1ea52820/
--     da-1-bti-Data.db            973,239 B
--     da-1-bti-Data.db.jsonl    1,916,747 B  (1.83 MiB — the sstabledump -l golden)
--     da-1-bti-Rows.db              1,348 B  (non-empty: one 4,000-row partition
--                                             spans several row-index blocks)
--     + Partitions.db / Statistics.db / CompressionInfo.db / Filter.db /
--       Digest.crc32 / TOC.txt / schema.cql
--
--   6,000 rows over 5 partitions (rows/partition 200..4000, mean 1200)
--   Data.db sha256 487dec609dc100f37d2ed642ff02e2d6de6bd22d8f57cae351d8a48fb4eb67c4
--
-- IT IS A CORRECTNESS ORACLE (issue #3042). Every byte was written by Apache
-- Cassandra 5.0.2, so the JSONL golden is a legitimate parity oracle for BTI row/cell
-- decode, row-index (Rows.db) trie descent and clustering-slice behaviour on a
-- COMPOUND clustering key. This is exactly what a CQLite-written round-trip fixture
-- can never be: two symmetric defects that cancel are undetectable by construction —
-- the #3002 BTI `Rows.db` root-base bug lived behind such a test.
--
-- IT IS **NOT** A PROFILE TARGET, and is deliberately distinct from the #3234 perf
-- corpus (`perf_bti.wide_multiclustering`, 27 SSTables / 13.2 M rows / ~2 GiB,
-- gitignored, regenerated on demand). At 973 KB its Data.db is far below the 8 MiB
-- `POINT_MMAP_MADV_RANDOM_MIN_BYTES` threshold, so the point-read and scan mappings
-- are the SAME mapping here and any read-plane A/B measured on it is structurally
-- zero. Read-path measurement belongs on the perf corpus (and the BTI mmap/trie plane
-- itself is unmeasured even there — see `read_path_measurement_scope` in
-- test-data/perf-corpus-bti-manifest.json).
--
-- PROVENANCE / REGENERATION (a fresh short container run; ~2 min, and it does NOT
-- touch the 2 GiB perf corpus):
--   bash test-data/scripts/gen-perf-corpus-bti.sh --small-golden --out /data/corpus-3234-small-golden
-- with cassandra:5.0.2 under the two mandatory yaml settings
-- (storage_compatibility_mode: NONE + sstable.selected_format: bti — a miss on either
-- silently emits `nb`), seed 20260803, --payload-bytes 160,
-- --widths 4000:20,800:30,200:50. Recorded identity:
-- test-data/perf-corpus-bti-small-golden-manifest.json (mode: small_golden).
--
-- The DDL below is the shape the generator creates. The captured, fully-expanded
-- `DESCRIBE KEYSPACE` output ships beside the components as schema.cql.

CREATE KEYSPACE IF NOT EXISTS test_da WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
} AND durable_writes = false;

USE test_da;

CREATE TABLE IF NOT EXISTS wide_multiclustering_small (
    pk int,
    bucket text,
    seq int,
    payload text,
    PRIMARY KEY (pk, bucket, seq)
) WITH CLUSTERING ORDER BY (bucket ASC, seq ASC)
  AND compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 16}
  AND compaction = {'class': 'SizeTieredCompactionStrategy'};
