## Why

The July 2026 read-path performance audit (`docs/reports/read-path-performance-audit-2026-07-01.md`,
§Epic G, finding G3; child of Epic G #1519, Wave 4 capstone) found partition-location logic spread
across `IndexReader` + `SummaryReader` + `promoted_index_reader` + the BTI trie, each with its own
entry points. Every C-epic fix (B4 key cache #1570, C5 range short-circuit #1576) had to be applied
per-path, and the duplicate BIG presence helpers (`get_with_spec_readers`, `get_with_schema_context`,
`lookup_partition_with_schema_context`) drifted from the live point path. This change consolidates
partition location behind ONE format-tagged façade so those cross-cutting fixes are written once and
the duplicate entry points are deleted.

Facts that constrain the scope:

- The live point path is `SSTableReader::get_with_resolution` (`reader/data_access/mod.rs:198`), which
  runs C5 (`partition_key_out_of_range`), then branches BTI→`bti_point_lookup` (bloom-skip, trie
  authoritative) vs BIG→`big_get_with_resolution` (bloom-first, then `Index.db`).
- BIG resolution is `lookup_partition_with_index` (`reader/partition_lookup.rs:26`), which already
  carries the B4 key cache and returns `(offset, size)` (writer emits `size=0`).
- BTI resolution is `lookup_partition_via_bti_trie` / `_encoded` (`partition_lookup.rs:137/191`), also
  B4-cached, returning an uncompressed offset only.
- Candidate pruning is `prune_candidates` (`storage/sstable/mod.rs:1160`), calling
  `might_contain_partition{,_encoded}`.
- `get_with_spec_readers` / `get_with_schema_context` / `lookup_partition_with_schema_context`
  (`partition_lookup.rs:490-788`) have ZERO production callers (only in-crate and `tests/` callers) —
  they duplicate the bloom→Index.db ordering and can be deleted.

## What Changes

- **Add one format-tagged façade** `SSTableReader::locate(key) -> Result<Option<(u64, u32)>>`
  (new `reader/partition_locator.rs`): runs the C5 range short-circuit once, then dispatches BIG
  (Summary→Index.db) vs BTI (trie walk). B4 key-cache and C5 short-circuit are reached only through the
  façade so both formats share one copy.
- **Migrate the point path and candidate pruning onto it.** `get_with_resolution` and `prune_candidates`
  resolve partition offsets via `locate` / `locate_encoded`; each format keeps its exact post-resolution
  handling (BIG bloom-first + index-miss→scan fallback; BTI bloom-skip + trie-authoritative).
- **Delete the now-unreachable duplicate entry points** (`get_with_spec_readers`,
  `get_with_schema_context`, `lookup_partition_with_schema_context`) after proving zero production
  callers; adjust the tests that referenced them onto the façade or the surviving `get`.
- **Split the over-threshold files as touched (#1116):** carve `index_reader.rs`'s parse tree into a
  `index_reader/` submodule and BTI point/seek decoders out of `data_access/bti.rs`, so both touched
  files shrink.

## Non-goals

- **Bounded Index.db mode is DEFERRED (owner decision).** The Summary-bounded on-disk `Index.db`
  binary-search mode (avoid whole-file materialization at `index_reader.rs:140-186`/`:337-340`) is a
  separate follow-up issue under Epic G. This change is façade consolidation only. The façade is
  designed so a bounded BIG resolver can slot in behind `locate` later without touching callers, but it
  is NOT built here.
- **No behavior change.** Identical offsets, identical negatives, identical error classification;
  per-format bloom ordering (BIG bloom-first, BTI bloom-skip) preserved bit-for-bit; the 33-table
  golden parity harness stays green.
- **No on-disk format or writer change.** Read-path only.
- **Promoted-index clustering narrowing is out of scope.** `locate` returns the partition offset only;
  the within-partition `IndexInfo`/`Rows.db` clustering seek stays a downstream step keyed off that
  offset.
