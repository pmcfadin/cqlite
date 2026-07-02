# Generating the zstd-dictionary negative SSTable fixture (issue #1399 / #1413)

This documents how to produce the **commissioned** dictionary-compressed SSTable
that `cqlite-core/tests/sstable_zstd_dictionary_reject_test.rs` gates on. It is a
*negative* fixture: CQLite must **fail closed** on it (typed rejection, never
rows, never a partial/garbled decode).

## Why this cannot be a plain `nodetool flush`

Apache Cassandra 5.x `org.apache.cassandra.io.compress.ZstdCompressor`
(CASSANDRA-14482) exposes only a `compression_level` option. It does **not**
train, store, reference, or apply a Zstd **dictionary**. There is no
`WITH compression = {'class': 'ZstdCompressor', 'dictionary': ...}` option, and
Cassandra records no separate dictionary compressor class in `CompressionInfo.db`
(the compressor simple-name stays `ZstdCompressor`). **No stock Apache Cassandra
release can flush a dictionary-compressed SSTable.** ScyllaDB's SSTable format is
out of CQLite's scope, so it is not an acceptable source either.

The fixture must therefore be produced by **post-processing** a genuine
Cassandra-written Zstd SSTable, not by a vanilla flush.

## Target layout (what the tests expect)

```
test-data/datasets/sstables/test_comp/zstd_dictionary_table/
  nb-1-big-Data.db              # chunks re-compressed with a trained dictionary
  nb-1-big-CompressionInfo.db   # ZstdCompressor, offsets updated for new chunks
  nb-1-big-Statistics.db
  nb-1-big-Index.db
  nb-1-big-Summary.db
  nb-1-big-Filter.db
  nb-1-big-Digest.crc32         # recomputed over the rewritten Data.db
  nb-1-big-TOC.txt
```

## Generation procedure (record full provenance for every step)

1. **Base SSTable.** Flush a real `ZstdCompressor` table with Apache Cassandra
   5.0.2 (record the exact image digest + `cassandra_git_sha`). Reuse the
   existing corpus generator:
   `bash test-data/scripts/regenerate-datasets.sh` (cassandra:5.0.2 → flush).
   Start from `test_comp/zstd_table-*` (plain Zstd) as the source of truth for
   the row data, Index/Summary/Statistics/Filter/TOC components.

2. **Train a dictionary.** Train a zstd dictionary over the *uncompressed* chunk
   payloads of the base Data.db (or a representative row corpus). Record the
   `zstd`/`zdict` library version and the exact training corpus. The trained
   dictionary embeds a Dictionary_ID in the frame header — this is the property
   that makes the frames un-decodable without the dictionary.

3. **Re-compress each chunk.** For every chunk in the base Data.db, decompress it
   (plain zstd), then re-compress it **with the trained dictionary**
   (`ZSTD_compress_usingDict`, dictID flag left enabled so the ID lands in the
   header). Re-append the 4-byte big-endian inline CRC32 of the *new* compressed
   bytes (`CompressedSequentialWriter.java:192`).

4. **Rewrite `CompressionInfo.db`.** Keep the compressor simple-name
   `ZstdCompressor`, keep `chunk_length`, recompute the chunk offset table for
   the new compressed sizes (`chunkOffset += compressedLength + 4`), and update
   `data_length`/`chunk_count` if the chunking changed (prefer keeping identical
   chunk boundaries so only the compressed bytes differ).

5. **Recompute `Digest.crc32`.** Recompute the whole-Data.db CRC32 to match the
   rewritten file (`DigestWriter`), so the digest is *valid* — the fixture must
   fail on the dictionary, not on a checksum mismatch (issue #1399 AC#3).

6. **Provenance + integrity.** Record: base Cassandra version + git sha, zstd/
   zdict version, the training corpus, and a sha-256 of every committed
   component. The `.db` binaries are gitignored — force-add the tiny reference
   files (`git add -f`) and verify the tests against a fresh detached worktree
   (`git worktree add --detach HEAD`), not the dirty tree.

## Validation once committed

```bash
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets CQLITE_REQUIRE_FIXTURES=1 \
  cargo test --package cqlite-core --test sstable_zstd_dictionary_reject_test
```

The two fixture-gated oracle tests
(`zstd_dictionary_sstable_rejected_via_reader_fixture`,
`zstd_dictionary_verify_reports_unsupported_not_checksum_fixture`) must run (not
skip) and prove the reader/verify surfaces fail closed. If they instead reveal
that CQLite does not yet return a *typed* dictionary rejection, that is the
production gap tracked by **#1414** — update those tests to the
characterization/`#[ignore = "blocked on #1414"]` form so the suite stays green.
