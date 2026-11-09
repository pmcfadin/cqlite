# issue_4482 — Cassandra 5.0.8 oracle-OUTPUT samples (stage 1)

Backs **#4482 stage 1**: CQLite's readers, comparators and test oracles must not
depend on two things that changed between `cassandra:5.0.2` and `cassandra:5.0.8`
oracle output. NOTHING here is a regenerated fixture; these are two small text
samples used only by `cqlite-core/tests/issue_4482_oracle_output_independence.rs`
and a test in `cqlite-cli/tests/issue_4196_salvage_publication_barrier.rs`.

## Provenance

- Generator: `test-data/scripts/generate-issue-3722-udt-wide-fields.sh`, with its
  image tag changed to `cassandra:5.0.8` in a scratch copy (no tracked file
  changed), run on the native `cassandra` VM on 2026-11-09. Table
  `test_udt_wide_fields.udt_wide_fields`, generation `nb-1-big`.
- `cassandra-5.0.8/test_udt_wide_fields/nb-1-big-Data.db.jsonl` — `sstabledump -l`
  output from 5.0.8. The `Data.db`, `Index.db`, `Filter.db`, `Summary.db` and
  checksum files of that run were byte-identical to the committed 5.0.2 fixture
  `test-data/fixtures/issue_3722`; only the dump text and `Statistics.db` differ.
- `cassandra-5.0.8/test_udt_wide_fields/nb-1-big-TOC.txt` — the 5.0.8 `TOC.txt`.

## The two oracle-output changes

1. **`TOC.txt` is written in lexicographic order** (CASSANDRA-20494, in 5.0.6):
   `TOCComponent.updateTOC` merges into a `TreeSet`. 5.0.2 wrote
   `Data, Statistics, Digest, TOC, CompressionInfo, Filter, Index, Summary`.
2. **`sstabledump` `position` is the scan reader's file pointer at the moment the
   partition object is serialized**, not a partition start:
   `JsonTransformer.serializePartition` writes
   `currentScanner.getCurrentPosition()`, and
   `SSTableScanner.getCurrentPosition()` is `dfile.getFilePointer()`
   (`git show cassandra-5.0.8:src/java/org/apache/cassandra/io/sstable/format/SSTableScanner.java`).
   `JsonTransformer` is byte-identical between the two tags; 5.0.8 reads the data
   file through `openDataReaderForScan()` (5.0.2: `openDataReader()`), so the first
   partition now reports 18 (this sample) where 5.0.2 reported 0, and later
   partitions shift by the same header length. `Data.db` is unchanged.
   Consequence: `position` is a lower bound on the partition's data offset in 5.0.8
   and the exact start only in 5.0.2; it must never be compared for equality.

The sample's `local_delete_time` values are wall-clock (2026-11-09) and differ from
the committed 5.0.2 fixture for that reason alone.
