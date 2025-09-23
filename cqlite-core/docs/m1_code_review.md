# M1 Code Review (cqlite-core)
We have some big problems. A code review has found some huge code flaws. We need to fix them. Assign subagents to research, fix and test problems I will show you. Here is one: 

## Critical
- `src/storage/sstable/reader.rs:290`: Fabricates a legacy header when real parsing fails, allowing corrupt/unsupported SSTables to masquerade as valid files. x

- `src/storage/sstable/reader.rs:3036`: Builds component paths from `path.file_stem()` ("Data"), so `*-Index.db`/`*-Summary.db`/`*-Statistics.db` are never found; index-derived operations are dead. x

- `src/storage/sstable/index_reader.rs:248`: Treats Index.db rows as `0x0010` + digest and hard-codes offsets to zero—partition lookups always return the start of Data.db. x

- `src/storage/sstable/reader.rs:826`: Guesses Cassandra 5 header size from string lengths; the data seek lands inside the header on real SSTables, corrupting every block read. x

- `src/storage/sstable/summary_reader.rs:254`: Assumes a fictitious header/entry layout for Summary.db; token-range logic always consumes junk. x

## High
- `src/storage/sstable/reader.rs:975`: Only searches for `nb-1-big-CompressionInfo.db`, `CompressionInfo.db`, or `Data-CompressionInfo.db`; misses real component filenames and silently skips compression metadata. x

- `src/storage/sstable/reader.rs:1125`: Expects `index_offset`/`bloom_filter_offset` in Data.db header properties; Cassandra stores these in separate components, so eager index/bloom loading never fires. 

- `src/storage/sstable/mod.rs:126`: Filters for `.sst` extension; Cassandra SSTables are `*-Data.db`, so no on-disk tables ever load.

- `src/parser/header.rs:70`: Mislabels magic numbers and "skips 25 bytes" before reading version; we mis-detect Cassandra formats and feed garbage into the header parser.

- `src/storage/sstable/reader.rs:950` & `src/storage/sstable/reader.rs:1728`: Heavy `println!/eprintln!` usage in block read/decompress paths spams stdout and torpedoes async performance.

## Medium
- `src/lib.rs:6`: Blanket `#![allow(clippy::all)]` hides real correctness regressions; re-enable linting and address violations.

- `src/storage/sstable/reader.rs:517`: Reports cache hit rate without tracking hits/misses; always returns 0.0 and misleads operators.

- `src/storage/sstable/reader.rs:1733`: Reads entire "large" compressed blocks into RAM; no chunked decompress, no guardrail beyond 64 MB.

- `src/storage/sstable/index_reader.rs:220`: Copies every partition digest into a `HashMap<Vec<u8>, usize>`; large tables explode memory/time—consider `Arc<[u8]>` or borrowed keys.

## Gaps & Next Steps
- Add golden-path tests over real Cassandra 5 artifacts to exercise `get`, `scan`, `lookup_partition_with_index`, and Summary/Index integration.
- Replace heuristic header parsing with spec-driven decoding shared across Data.db, Index.db, and Summary.db readers.
- After fixing component lookup, wire integration tests in `tests/` to assert partition lookups, range scans, and decompression succeed against fixtures.
