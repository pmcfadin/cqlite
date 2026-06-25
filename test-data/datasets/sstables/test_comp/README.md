# test_comp — Compression / chunk-format parity fixtures (epic #970, issue #996)

Cassandra **5.0.2** SSTable fixtures, one table per compression scenario the
CQLite compression-parity epic must read byte-for-byte the same as Apache
Cassandra. Generated with `storage_compatibility_mode: CASSANDRA_4`
(nb / BIG format, prefix `nb-1-big`).

- Schema: `test-data/schemas/compression-parity.cql`
- Generator: `test-data/scripts/generate-compression-parity.sh`
- Keyspace: `test_comp` (SimpleStrategy, RF=1)
- Fixed writetime: `USING TIMESTAMP 1609459200000000` (2021-01-01T00:00:00Z)
- Fixed PRNG seed for incompressible blobs: `0x00C0FFEE` (`random.Random(12648430)`)

## CRITICAL generation detail: `flush_compression: table`

In `CASSANDRA_4` storage-compatibility mode the cassandra.yaml default is
`flush_compression: fast`, which forces **every memtable flush** to write
SSTables with **LZ4Compressor regardless of the table's schema compressor**
(the schema compressor would only take effect on the next compaction). The
generator therefore patches `flush_compression: table` and restarts Cassandra
before any flush, so each table's `nb-1-big` SSTable honors its schema
compressor directly (verified: deflate/zstd flush as LZ4 with the default).

## Table -> scenario -> manifest key -> compression options -> chunk length

| Table | Scenario | Manifest key (`fixture_matrix.*`) | Compressor class | chunk_length_in_kb | Notes |
|-------|----------|-----------------------------------|------------------|--------------------|-------|
| `lz4_table` | LZ4 baseline | `lz4` | `LZ4Compressor` | 16 | 4-byte LE uncompressed-size prefix per chunk |
| `snappy_table` | Snappy | `snappy` | `SnappyCompressor` | 16 | raw Snappy, no prefix |
| `deflate_table` | Deflate | `deflate` | `DeflateCompressor` | 16 | raw zlib deflate, no prefix |
| `zstd_table` | Zstd plain (no dictionary) | `zstd_no_dictionary` | `ZstdCompressor` | 16 | Zstd frame, internal checksum |
| `uncompressed_table` | Compression disabled | `uncompressed_table` | `{'enabled': false}` | n/a | **no** CompressionInfo.db; **has** CRC.db |
| `short_final_chunk` | Short final chunk | `short_final_chunk` | `LZ4Compressor` | 4 | last chunk covers < 4096 uncompressed bytes |
| `incompressible_uncompressed_chunk` | Uncompressed-chunk fallback | `incompressible_uncompressed_chunk` | `LZ4Compressor` (`min_compress_ratio=1.0`) | 4 | high-entropy blobs -> chunks stored RAW |

`min_compress_ratio = 1.0` on `incompressible_uncompressed_chunk` sets
`maxCompressedLength = chunk_length = 4096`; LZ4 cannot shrink the random blobs,
so Cassandra writes each full chunk RAW (`compressed_len == raw_chunk_len ==
4096`). With the DEFAULT `min_compress_ratio = 0`, `maxCompressedLength` is
`Integer.MAX_VALUE` (0x7FFFFFFF) and the raw fallback never fires.

## Committed (text) vs gitignored (binary) artifacts

Per the corpus convention, the binary `*.db` components are **gitignored**.
Only text reference artifacts are committed:

- `nb-1-big-Data.db.jsonl` — `sstabledump -l` golden (one partition per table)
- `nb-1-big-Statistics.db.txt` — `sstablemetadata` output (records `Compressor:` + ratio)
- `nb-1-big-TOC.txt` — component manifest (shows CompressionInfo.db vs CRC.db)
- `nb-1-big-Digest.crc32` — component-level digest
- `nb-1-big-CompressionInfo.db.txt` — decoded chunk map sidecar (algorithm,
  chunk_length, max_compressed_length, total_uncompressed_length, chunk_count,
  per-chunk offset/on-disk-length/compressed-length/raw-uncompressed-length/
  raw-stored, plus the short-final and raw-stored-count invariants)

To regenerate the binary `*.db` files (and refresh all text artifacts):

```bash
bash test-data/scripts/generate-compression-parity.sh
```

Table-UUID directory names change on every regeneration (Cassandra assigns a
fresh table UUID per CREATE TABLE); the logical fixtures are deterministic.

## CompressionInfo.db format (nb / Cassandra 5.0)

Big-endian, per `CompressionMetadata.writeHeader()`:

```
u16 name_len | name bytes | u32 option_count | (u16+bytes key, u16+bytes val) * option_count
u32 chunk_length | u32 max_compressed_length | u64 total_uncompressed_length
u32 chunk_count | u64 chunk_offset[chunk_count]
```

Per-chunk CRC32 lives inline in Data.db after each compressed chunk (not in
CompressionInfo.db). On-disk chunk length = `next_offset - offset`; compressed
payload length = on-disk length - 4 (the trailing CRC word).
