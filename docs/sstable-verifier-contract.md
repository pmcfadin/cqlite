# SSTable Verifier Contract (epic #970, issue #1000)

CQLite ships a verifier that **enforces** a stable contract for Cassandra 5.0
SSTables — both the `nb`/`big` (`BigFormat`) and the `da`/`bti` (`BtiFormat`)
layouts — across healthy *and* corrupted inputs. The contract guarantees that a
structurally corrupt SSTable **fails loudly with a stable error class and the
failing component name**, and is never reported as an apparently-successful
zero-row result.

- Library API: `cqlite_core::storage::sstable::verify::verify_sstable`
- CLI: `cqlite verify <dir> --mode <quick|full> --out <text|json>`
- Source: `cqlite-core/src/storage/sstable/verify.rs`
- Enforcement test: `cqlite-core/tests/issue_1000_verifier.rs`
  (manifest-driven against `test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml`)

## Library API

```rust
use cqlite_core::storage::sstable::verify::{verify_sstable, VerifyMode, VerifyReport};

let report: VerifyReport =
    verify_sstable(dir, VerifyMode::Full, &config, platform).await?;

if !report.is_ok() {
    for f in &report.findings {
        eprintln!("[{}] {}: {}", f.class.code(), f.component, f.detail);
    }
}
```

`verify_sstable` returns `Err` **only** for environmental problems (the
directory cannot be read, or contains no `*-Data.db`). *Data* corruption is
reported as `VerifyFinding`s inside an `Ok(VerifyReport)` so the caller can
serialize the full picture (e.g. as a CI artifact). Use `VerifyReport::is_ok()`
to branch.

## Modes (QUICK and FULL are distinct)

A QUICK pass MUST NOT be presented as a FULL pass — they validate different
surfaces.

### QUICK — metadata-only structural checks

1. **Component presence + `TOC.txt` completeness.** Every component listed in
   `TOC.txt` must exist on disk, and every real component present on disk must
   be listed in `TOC.txt` (a dropped TOC line is corruption). `Data.db` is
   always required.
2. **`Digest.crc32` vs `Data.db`.** The recorded decimal-ASCII CRC32 (IEEE) must
   equal the CRC32 of the entire `Data.db` file.
3. **`CompressionInfo.db` parse + chunk-offset bounds.** The file must parse
   (an unknown algorithm already fail-fasts, #1001) and every declared chunk
   offset must be in-bounds for `Data.db` (a single corrupted offset is still
   ascending yet points past EOF — caught here).
4. **Index structure.**
   - BIG: `Index.db` is walked entry-by-entry; a mid-stream parse error,
     leftover trailing bytes, or a zero-entry result on a non-empty file is
     corruption.
   - BTI: `Partitions.db` is walked from its footer root; for every partition
     whose payload is a `RowsOffset`, the per-partition row-index entry is
     resolved from `Rows.db`.

### FULL — QUICK plus content-touching checks

5. **Inline `Data.db` chunk CRC validation (#998).** Every chunk's trailing
   4-byte CRC32 is verified (no decompression — decode is covered by the scan).
6. **`Statistics.db` parse** (TOC-header sanity + reader open) and, for BIG,
   **`Summary.db` parse**.
7. **Full row scan.** A complete scan exercises LZ4/Snappy/Deflate/Zstd
   decompression via the stitch path. The BTI partition count recovered from
   `Partitions.db` is cross-checked against the distinct partitions decoded from
   `Data.db` to catch a footer flip that silently *under-counts* partitions.

## No silent empty results on corruption (#1000)

Two production read paths previously masked structural corruption:

- **BIG `Index.db`** — `index_reader` stops its parse loop at the first
  malformed entry and returns the partitions parsed so far. The full-scan path
  then falls back to a whole-`Data.db` scan, so a bit-flipped `Index.db` looked
  healthy. The verifier's BIG index check walks every entry and hard-fails.
- **BTI `Partitions.db`/`Rows.db`** — a full BTI scan reads `Data.db` directly
  and never touches the tries, so a corrupt/truncated trie is invisible to a
  scan. The verifier walks the tries explicitly and cross-checks the partition
  count against the scan.

A related production bug fixed under this epic: the stitch path
(`stitch_all_chunks`) did not honour Cassandra's incompressible-chunk rule
(`compressedLength >= maxCompressedLength` ⇒ chunk stored raw), so the
`incompressible` healthy fixture failed to scan. It now passes the raw bytes
through, matching `ChunkDecompressor`.

## Error classes (`VerifyErrorClass`)

These codes are part of the contract; callers and CI may match on them.

| Code | Meaning |
|------|---------|
| `MissingComponent` | A TOC-listed or structurally-required component is absent / unlisted. |
| `DigestMismatch` | `Digest.crc32` ≠ CRC32(`Data.db`). |
| `CompressionInfoCorrupt` | `CompressionInfo.db` failed to parse / unsupported algorithm. |
| `ChunkOffsetOutOfBounds` | A `CompressionInfo.db` chunk offset points past `Data.db`. |
| `ChunkDecompressionError` | Inline chunk CRC mismatch or a chunk failed to decompress. |
| `UnexpectedEof` | A component was truncated and a required read hit EOF. |
| `IndexEntryCorrupt` | BIG `Index.db` is structurally corrupt. |
| `StatisticsHeaderCorrupt` | `Statistics.db` header / body is corrupt. |
| `SummaryCorrupt` | `Summary.db` is truncated / unreadable. |
| `BtiRootPointerCorrupt` | BTI `Partitions.db` root pointer / node is corrupt. |
| `BtiTrieCorrupt` | BTI `Rows.db` trie is truncated / corrupt. |
| `RowScanFailed` | Full scan failed for a reason not classified above. |

## Corruption fixtures → checks

Driven by `test-data/datasets/corruption/test_comp_corrupt/corruption-manifest.yml`.

| Fixture | Failing component | Error class |
|---------|-------------------|-------------|
| `data_db_bit_flip` | `Data.db` | `ChunkDecompressionError` (inline CRC) |
| `data_db_truncation` | `Data.db` | `UnexpectedEof` (+ `ChunkOffsetOutOfBounds`, `DigestMismatch`) |
| `compression_info_bad_offset` | `CompressionInfo.db` | `CompressionInfoCorrupt` |
| `index_db_bit_flip_big` | `Index.db` | `IndexEntryCorrupt` |
| `bti_partitions_footer_flip` | `Partitions.db` | `BtiRootPointerCorrupt` |
| `bti_rows_truncation` | `Rows.db` | `BtiTrieCorrupt` |
| `statistics_db_header_damage` | `Statistics.db` | `StatisticsHeaderCorrupt` |
| `summary_db_truncation` | `Summary.db` | `SummaryCorrupt` |
| `toc_missing_component` | `TOC.txt` (drops `Statistics.db`) | `MissingComponent` |
| `digest_crc32_mismatch` | `Digest.crc32` | `DigestMismatch` |

Healthy fixtures (`lz4`, `snappy`, `deflate`, `zstd`, `short_final`,
`incompressible`, `uncompressed`) all PASS FULL verification.

## CLI

```bash
# FULL verify, human-readable
cqlite verify ./.../test_comp/lz4_table-<uuid> --mode full --out text

# QUICK verify, JSON for CI artifacts (exit code 2 on failure)
cqlite verify ./.../test_comp_corrupt/digest_crc32_mismatch --mode quick --out json
```

Exit codes: `0` = verified, `2` = verification failed (findings printed), `1` =
usage / environment error.
