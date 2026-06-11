# BTI (da) Read Support — Scoping Document

**Created**: 2026-06-10  
**Issue**: #657 (VG5 — da foundation)  
**Epic candidate**: GitHub issue #660  
**Status**: SCOPING — da read support not yet implemented

---

## Executive Summary

Cassandra 5.0 ships two on-disk SSTable formats:

- **BIG** (`nb`/`oa` version letters) — classic B-tree-style index (Index.db + Summary.db)
- **BTI** (`da` version letter) — trie-indexed format (Partitions.db + Rows.db)

CQLite currently reads BIG-format SSTables (nb read path is complete; oa BIG read path is in-progress). This document scopes the work needed to add full BTI read support for the `da` format letter, which is the only BTI version letter used by Cassandra 5.0 (`BtiFormat.java:287`).

**VG5 (issue #657)** delivered the foundation:
- `da` recognised and routed by `FormatDetector` → `V5x("da")` (not `Unknown`)
- `Partitions.db`/`Rows.db` classified as `SSTableComponent::Partitions`/`::Rows`
- `SSTableReader::open` returns `Error::UnsupportedFormat` early (not a confusing parse failure)
- `bti/` module has skeleton types and node-type dispatch (`#647`/`#651`)

This document describes the remaining work for full end-to-end BTI reading.

---

## Authority

All claims below are derived from the Cassandra 5.0.8 source:

| File | Role |
|------|------|
| `BtiFormat.java` | Format definition, version gates |
| `PartitionIndex.java` / `PartitionIndexBuilder.java` | Partitions.db reader/writer |
| `RowIndexReader.java` / `RowIndexWriter.java` | Rows.db reader/writer |
| `TrieNode.java` | Node type encoding (nibble dispatch) |
| `TrieIndexEntry.java` | Per-partition row index entry |
| `ByteComparable.java` / `ByteSource.java` | ByteComparable key encoding |
| `SortedTableWriter.java` | Data.db write-time chain |
| `BtiTableReader.java` | End-to-end read path |

Local copy: `docs/cassandra-5.0-src/` (or `~/local_projects/cassandra`).

---

## Architecture

### Partitions.db — partition trie

`Partitions.db` is a compacted ByteComparable trie where each leaf node references a partition's position in `Data.db`.

Key concepts:
1. **ByteComparable encoding** (`ByteComparable.java`): partition keys are transformed to a byte-comparable form so trie order equals partition key order. CQLite's `bti/encoder.rs` has a skeleton; it needs to implement all CQL type encodings (int, text, UUID, blob, composite keys, …).
2. **Trie node types** (`TrieNode.java`): 16 subtypes dispatched by the high nibble of the first byte. `bti/parser.rs` already implements all 16 nibble types (`#647`/`#651`). Remaining work: wire the decoded nodes into a trie-walk that returns a `Data.db` offset for a given partition key.
3. **Root location**: The trie root is at the end of `Partitions.db`. The last 8 bytes hold the root offset; reading works backwards. (`PartitionIndex.java:82-95`)
4. **Pointer encoding**: child pointers are backward deltas (child appears earlier in file). `bti/parser.rs` already decodes these correctly.

**Remaining work** (Partitions.db):
- [ ] Implement `PartitionsParser::find_partition(key: &[u8])` that walks the trie to locate a partition's data offset.
- [ ] Implement `PartitionsParser::iter_partitions()` for full-scan support (needed for `SELECT *`).
- [ ] Implement ByteComparable encoding for all CQL primitive and composite key types.
- [ ] Unit tests: round-trip encoding + trie-walk against real `da-2-bti-Partitions.db`.

### Rows.db — per-partition row index

`Rows.db` holds per-partition row index entries that allow locating rows within a (possibly large) partition in `Data.db` without scanning from the start.

Key concepts from `RowIndexReader.java`:
1. **Index entry format**: Each entry is a `TrieIndexEntry` (`TrieIndexEntry.java`), containing:
   - Data file offset (`long`)
   - Deletion time for the indexed row range (as a `DeletionTime`) — contains `localDeletionTime` (uint32) and `markedForDeleteAt` (int64)
   - `FLAG_OPEN_MARKER` bit (indicates whether a range tombstone is open at this position)
2. **Trie structure**: Same nibble-dispatch trie as Partitions.db, but keyed on ByteComparable-encoded clustering keys.
3. **Optional**: Small partitions (below `column_index_size_in_kb` threshold, default 64 KB) may not have a Rows.db entry at all — the row is read directly from Data.db at the partition offset from Partitions.db.

**Remaining work** (Rows.db):
- [ ] Implement `RowsParser::find_row(partition_offset, clustering_key)` for point lookups.
- [ ] Implement `RowsParser::iter_rows(partition_offset)` for scanning a partition.
- [ ] Parse `TrieIndexEntry` including `DeletionTime` and `FLAG_OPEN_MARKER`.
- [ ] Handle the "no row index" case for small partitions.

### Data.db — row payload

BTI-format `Data.db` shares the same row-level binary format as BIG `Data.db` (`V5CompressedLegacy` for `nb`/`oa`). The row cell state machine (`row_cell_state_machine.rs`) and the V5 compressed legacy parser should be reusable.

**Remaining work** (Data.db):
- [ ] Confirm that `row_cell_state_machine.rs` parses `da`-format row payloads correctly (expected: yes, the payload format is shared).
- [ ] Wire the Data.db reader into the BTI read path: after Partitions.db gives a data offset, seek to that position in Data.db and invoke the existing row parser.
- [ ] Handle `BtiVersionGates` (all `da` gates are `true`): `hasAccurateMinMax`, etc.

### CompressionInfo.db

Same as BIG format. `CompressionInfo.db` parsing already works; the existing `chunked_data_reader.rs` and `chunk_decompressor.rs` should be reusable without modification.

---

## Read Path Integration Points

The following TODOs exist in the codebase that must be resolved:

### `bti/parser.rs` — open range_query / iter TODO items

The `RowsParser` and `PartitionsParser` structs have placeholder bodies for:
- `range_query` — range scan over clustering keys
- partition iteration

These stubs must be replaced with real trie-walk implementations.

### `SSTableReader::open` — early BTI rejection (VG5)

`reader/mod.rs` line ~179 currently returns `Error::UnsupportedFormat` for all BTI files. This guard must be removed (or made conditional on a feature flag) once the BTI read path is wired.

### `BtiVersionGates` consumers

`version_gate.rs` exposes `BtiVersionGates { has_accurate_min_max, has_legacy_min_max }`. These are threaded through `VersionGates::from_path` and available to the reader. When the BTI read path lands, callers should use these gates (e.g., `hasAccurateMinMax == true` means the min/max per Statistics.db column is authoritative for `da`).

### Discovery service (`storage/sstable/mod.rs`)

The `SSTableManager` currently loads `Data.db` via `SSTableReader::open`. For BTI tables this will now return `UnsupportedFormat`. The manager needs to detect BTI format early and route to a `BtiTableReader` (to be created) instead.

---

## Implementation Plan

### Phase 1 — ByteComparable encoding (prerequisite)

Implement `bti/encoder.rs::encode(key_components, types) -> Vec<u8>` for:
- All CQL primitive types: int, bigint, varint, text, ascii, blob, boolean, timestamp, date, time, UUID, timeuuid, inet, decimal, duration
- Composite partition keys (concatenate component encodings with separator bytes per `ByteComparable.java:§2.4`)
- Clustering key encoding (includes ascending/descending ordering flags)

**Estimated size**: M (2–3 days; encoding rules are well-specified in `ByteSource.java`)

### Phase 2 — Partitions.db trie walk

Implement `PartitionsParser::find_partition` and `::iter_partitions` using the existing node-type dispatch from `#647`/`#651`.

**Estimated size**: M (2–3 days)

### Phase 3 — Rows.db row index

Implement `RowsParser::find_row` and `::iter_rows`, including `TrieIndexEntry` parsing with `DeletionTime` and `FLAG_OPEN_MARKER`.

**Estimated size**: M (2–3 days)

### Phase 4 — End-to-end wiring

- Replace the VG5 early-exit guard in `SSTableReader::open` (or create `BtiTableReader`).
- Wire `Partitions.db → Data.db` offset chain.
- Wire `Rows.db → Data.db` row-level seeks.
- Update `SSTableManager` to route BTI tables.
- Run 33 nb tables smoke test still green; `test_da` tables should flip from SKIP-PENDING to passing.

**Estimated size**: L (3–5 days; integration risk is higher)

### Phase 5 — Parity testing

- Generate JSONL goldens for `test_da` tables (via `sstabledump`; already done in `#654`).
- Add parity tests matching `test_da.simple_table`, `test_da.collection_table`, `test_da.ttl_table`.
- Extend smoke test to enforce `test_da` (remove from `SKIP_PENDING_KEYSPACES`).

**Estimated size**: S (1 day)

---

## Total Estimate

| Phase | Size | Risk |
|-------|------|------|
| Phase 1 — ByteComparable | M | Low (well-specified) |
| Phase 2 — Partitions.db trie | M | Medium (trie walk bugs) |
| Phase 3 — Rows.db row index | M | Medium |
| Phase 4 — E2E wiring | L | High (integration) |
| Phase 5 — Parity | S | Low |
| **Total** | **XL** | **Medium-High** |

Suggested sprint split: Phases 1–3 in one epic, Phase 4–5 in a follow-on.

---

## Known Unknowns / Risks

1. **ByteComparable ordering for exotic types**: `duration`, `decimal`, `inet` have non-trivial byte-comparable encodings. May need reference tests against Cassandra directly.
2. **Large partition handling**: Partitions with many rows use both Partitions.db (for the partition) AND Rows.db (for rows within). Interaction with the existing chunked decompressor is untested.
3. **Tombstone semantics**: `FLAG_OPEN_MARKER` in `TrieIndexEntry` affects how range tombstones are applied. The existing `tombstone_merger.rs` may need BTI-aware extension.
4. **Data.db format identity**: If BTI `da` Data.db uses a different row cell format than BIG `nb`/`oa`, the state machine will need updating. Expected to be the same (both use Cassandra 5.0 row encoding), but must verify with hex comparison.

---

## Files to Create / Modify

| File | Action |
|------|--------|
| `cqlite-core/src/storage/sstable/bti/encoder.rs` | Complete ByteComparable encoding |
| `cqlite-core/src/storage/sstable/bti/parser.rs` | Implement trie-walk, row index |
| `cqlite-core/src/storage/sstable/reader/mod.rs` | Remove VG5 early-exit guard |
| `cqlite-core/src/storage/sstable/reader/bti_reader.rs` | New: BtiTableReader |
| `cqlite-core/src/storage/sstable/mod.rs` | Route BTI tables in SSTableManager |
| `cqlite-core/tests/issue_NNN_bti_e2e.rs` | Parity tests against test_da fixtures |
| `test-data/scripts/smoke-test-all-tables.sh` | Move test_da from SKIP_PENDING to enforced |

---

## References

- `BtiFormat.java` — `org.apache.cassandra.io.sstable.format.bti`
- `PartitionIndex.java` — trie root offset at EOF, `find()` method
- `RowIndexReader.java` — `TrieIndexEntry`, `FLAG_OPEN_MARKER`, `DeletionTime`
- `TrieNode.java` — 16 node subtypes, nibble dispatch
- `ByteComparable.java` / `ByteSource.java` — key encoding rules
- `bti/parser.rs` — existing node-type dispatch (complete as of #647/#651)
- `bti/encoder.rs` — ByteComparable encoder skeleton
- `issue_653_version_gates_plumbing_test.rs` — BtiVersionGates tests
- `issue_657_da_foundation.rs` — VG5 foundation tests
