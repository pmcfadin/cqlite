-- SMALL Cassandra-WRITTEN BTI (`da`) golden — issue #3234 (owner decision 4B).
--
-- WHAT THIS IS: a COMMITTED, Cassandra-5.0.2-WRITTEN BTI SSTable with the same wide
-- multi-component clustering shape as the #3234 perf corpus —
-- `PRIMARY KEY (pk, bucket, seq)`, LZ4 chunk_length_in_kb=16 — sized so its
-- `sstabledump -l` golden is committable:
--
--   test-data/datasets/sstables/test_da/wide_multiclustering_small-47f6a3008f6911f1bc0f8df8badcc262/
--     da-1-bti-Data.db             97,780 B
--     da-1-bti-Data.db.jsonl      192,935 B  (188.4 KiB — the sstabledump -l golden)
--     da-1-bti-Rows.db                197 B  (non-empty: the 400-row partition is
--                                             ~74 KiB uncompressed, ~18x the image's
--                                             column_index_size default of 4KiB)
--     + Partitions.db / Statistics.db / CompressionInfo.db / Filter.db /
--       Digest.crc32 / TOC.txt / schema.cql
--
--   600 rows over 5 partitions (rows/partition 20..400, mean 120)
--   Data.db sha256 d59cd894005ebefe9a57d418a0b8a3d55ab0886ee484f61f38bef4a5bb6d2eaa
--
-- SIZED TO THE REPO'S CONVENTION, not to a row count. The closest committed
-- analogue is #3032's test_da/multiclustering_table -- the SAME
-- `PRIMARY KEY (pk, bucket, seq)` shape -- at 468 rows / 3 partitions and a
-- 121,020 B golden. A golden's worth as a Cassandra-written oracle does not scale
-- with row count, but its committed size does (~320 B/row), so this fixture is the
-- first cut's 6,000-row shape divided by exactly 10: the same width WEIGHTS, hence
-- the same partition-count and bucket-spread structure, one order of magnitude
-- smaller (2,898,284 B -> 297,374 B for the whole directory).
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
-- gitignored, regenerated on demand). At 97.8 KB its Data.db is far below the 8 MiB
-- `POINT_MMAP_MADV_RANDOM_MIN_BYTES` threshold, so the point-read and scan mappings
-- are the SAME mapping here and any read-plane A/B measured on it is structurally
-- zero. Read-path measurement belongs on the perf corpus (and the BTI mmap/trie plane
-- itself is unmeasured even there — the measurement scope lives in
-- docs/development/dev-cookbook.md, "BTI (`da`) perf corpus", and `bti_perf_scan`
-- prints `access_path` / `storage_route` beside every number it reports; the manifest
-- deliberately records NO throughput figure, because nothing in the corpus bytes
-- observes one).
--
-- PROVENANCE / REGENERATION (a fresh short container run; ~2 min, and it does NOT
-- touch the 2 GiB perf corpus):
--   bash test-data/scripts/gen-perf-corpus-bti.sh --small-golden --out /data/corpus-3234-small-golden
-- with cassandra:5.0.2 under the two mandatory yaml settings
-- (storage_compatibility_mode: NONE + sstable.selected_format: bti — a miss on either
-- silently emits `nb`), seed 20260803, --payload-bytes 160,
-- --widths 400:20,80:30,20:50. Recorded identity:
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
