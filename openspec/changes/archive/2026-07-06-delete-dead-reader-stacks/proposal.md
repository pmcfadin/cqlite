## Why

The SSTable reader has accumulated dead and duplicated machinery that inflates every future
fix and misleads auditors. The July 2026 read-path performance audit
(`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic G, child **G1** / issue #1597)
verified:

1. `SchemaAwareReader` — constructed only in tests; zero production consumers.
2. `ChunkedDataReader` — zero `src/` consumers.
3. `StreamingDecompressor` / `CompressionReader::read_streaming` (and the rest of
   `CompressionReader`'s streaming half) — zero consumers.
4. A **duplicate `CompressionInfo` parser**: the reader-open path parses the SAME
   `CompressionInfo.db` file **twice** with two different parsers — once via the legacy
   `compression::CompressionInfo::parse_binary` (inside `detect_and_initialize_compression`,
   only to learn the algorithm) and once via the modern
   `compression_info::CompressionInfo::parse` (for the chunk metadata).
5. The legacy detection path issues ~25 `exists()` generation-probing `stat` calls per open
   (`get_standard_compression_patterns`), when the CompressionInfo.db name is already known
   deterministically from `SsTableDescriptor`.
6. `CompressionReader` collapses to a plain algorithm field once its dead streaming half is gone.

This is **Epic G / child G1 (#1597)**, the audit's Wave 2.5 one-batch dead-code purge. Routing is
**design-driven** (a reader-surface consolidation with structural latitude — no external oracle
dictates the reader shape), so it goes through OpenSpec under the **standing owner Seam-1 approval
(2026-07-06 drain directive)** authorizing the audit's locked G-series deletions. Its *correctness
guardrail* is oracle-driven: the deletions MUST NOT change any read result — the 33-table
`sstabledump` parity harness stays green byte-for-byte.

Milestone: **v0.14 perf wave** (M7 read-path program). Target: one CompressionInfo parse per open,
O(1) component probing, and a smaller reader surface for every downstream fix.

## What Changes

- **Delete `SchemaAwareReader`** (`schema_aware_reader.rs`, its `_test.rs`, the `pub use`/`mod`
  wiring, and every test-only consumer). Semver-visible: the public re-export
  `cqlite_core::storage::sstable::SchemaAwareReader` is removed.
- **Delete `ChunkedDataReader`** (`chunked_data_reader.rs`, its `mod` wiring, and its integration
  tests). Semver-visible: `cqlite_core::storage::sstable::chunked_data_reader` is removed.
- **Delete `StreamingDecompressor`** + `ChunkedDecompressionConfig` and collapse `CompressionReader`
  to a single `algorithm` field (dropping `read_streaming`, `read`, `with_block_size`, `block_size`,
  and the `buffer`/`block_size` fields). Semver-visible: those public items are removed.
- **Consolidate to ONE CompressionInfo parser.** Delete the legacy duplicate
  `compression::CompressionInfo` (+ `ChunkInfo`, `parse`, `parse_binary`, `normalize_algorithm_name`)
  and the unwired `cqlite-cli/src/commands/test_compression.rs` (its only remaining consumer). The
  reader-open path parses `CompressionInfo.db` exactly once via
  `compression_info::CompressionInfo::parse` and derives the `CompressionReader` algorithm from that
  single parsed result.
- **Replace the ~25 `exists()` generation-probe loop** with the `SsTableDescriptor`-derived O(1)
  lookup that `load_compression_info_metadata` already performs; delete the dead
  `detect_and_initialize_compression` detection helpers.
- **A one-parse-per-open work counter** (extending the existing `read_work_counters` zero-in-release
  pattern) so a real reader-open test proves `CompressionInfo.db` is parsed exactly once.

## Non-goals

- **No change to any read RESULT.** Byte-for-byte 33-table parity is a hard guardrail, not a knob.
- **Not the wider Epic G consolidation** (G2 one decode plane, G3 one `PartitionLocator`, G4 legacy
  `TombstoneMerger` confinement) — separate children. This change deletes only the G1-scoped dead
  stacks and the duplicate CompressionInfo parse.
- **Not deleting `extract_sstable_base_name`** — it remains the CRC.db sidecar-lookup helper used by
  `load_crc_reader`.
- **No new external crate dependency**; no change to the no-heuristics posture (algorithm and
  component names come from authoritative `CompressionInfo.db` + `SsTableDescriptor`, never inferred
  from byte content).
- **Pre-`na` formats remain out of scope** and are not reintroduced.

## Impact

- **Public surface:** removes `SchemaAwareReader`, `chunked_data_reader`, `StreamingDecompressor`,
  `ChunkedDecompressionConfig`, `compression::CompressionInfo`/`ChunkInfo`, and the streaming half of
  `CompressionReader` — recorded as a semver-visible removal in the changelog.
- **No-heuristics mandate:** unaffected/strengthened — the legacy heuristic detection path is deleted.
- **Bindings (Python/Node/CLI):** unchanged behavior; opens do less work.
- **Memory/perf:** one CompressionInfo parse and one `exists()` probe per open instead of two parses
  and ~25 probes.
