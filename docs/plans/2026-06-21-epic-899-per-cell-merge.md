# Epic #899 — per-element/per-cell merge representation (reader→merge→writer)

Foundation for compaction byte-parity. Makes the read→merge→write path per-element/per-cell
granular instead of whole-cell/row-timestamp granular. Scope = #899's 4 items ONLY; the 6 blocked
issues (#844/#887/#888/#846/#848/#845) stay open under epic #921.

Branch: `worktree-epic-899-per-cell-merge`. Gate: `scripts/agent-gate.sh`. Review: roborev per commit.
Known pre-existing gate failures (ignore): python-bindings PEP604/py3.9, flush-throughput flaky — diff vs origin/main.

## Critical files (cqlite-core/src)
- `storage/sstable/reader/parsing/v5_compressed_legacy.rs` — `parse_complex_column_inner` (5807), `parse_block_with_timestamps` (~3413), row emit (1270-1334)
- `storage/sstable/reader/data_access.rs` — `iterate_all_partitions_for_compaction` (1277), `stream_all_partitions_for_compaction` (1345), `drain_compaction_window` (1441), `stitch_and_parse_all_chunks_for_compaction` (1181), `parse_one_partition_with_timestamps`
- `storage/write_engine/merge.rs` — `CellData` (266), `ComplexDeletion` (329), `MergeEntry` (79), `value_to_row_data` (859), `reconcile_cluster` (1453), `merge_entry_to_mutation` (1624), `KWayMerger::merge` (1222), `estimate_entry_size` (470)
- `storage/write_engine/mutation.rs` — `Mutation` (63), `CellOperation` (205), `RangeTombstone` (164)
- `storage/sstable/writer/data_writer.rs` — `write_complex_column` (2056, LIVE sentinel 2069-2073), `write_complex_cell_header` (2165), `write_{set,map,list}_complex_cells` (2221/2272/2327), `write_complex_column_deletion` (2106), `MergedOp` (2943), `write_merged_cells` (1891)

## Tests to flip / extend
- `tests/issue_886_merge_entry_substrate.rs` — carry-only assertions flip to populated/consumed
- `tests/issue_819_differential_compaction.rs` — remove the per-cell writetime/TTL/LDT + `complex_cell_path_unobservable` downgrades (lines ~112-122, 393-396)
- `tests/issue_823_complex_column_merge.rs` — collection/complex-column merge coverage
- `compaction-parity/` — Java differential harness (byte gate); add overwrite+append collection fixtures

## Phase A (WI-1 + WI-2) — reader→merge per-element contract  [land together]
Replace the `(RowKey, Value, i64)` compaction emit with a struct that carries per-element cells +
real complex deletion. Compaction-only path — DO NOT touch `iterate_all_partitions`/`scan`/`get`
(those feed user reads + `WRITETIME(collection)`).

```rust
pub struct CompactionRow { pub key: RowKey, pub row_timestamp: i64, pub row_data: CompactionRowData }
pub enum CompactionRowData {
    Tombstone { deletion_time: i64, local_deletion_time: i32 },
    Live { simple: Vec<SimpleCell>, complex: Vec<ComplexColumn> },
}
pub struct ComplexColumn { pub column: String, pub complex_deletion: Option<(i64, i32)>, pub elements: Vec<ComplexElement> }
pub struct ComplexElement { pub cell_path: Vec<u8>, pub value: Option<Value>, pub timestamp: i64, pub ttl: Option<u32>, pub local_deletion_time: Option<i32>, pub is_deleted: bool }
```
- Reader: stop collapsing in `parse_complex_column_inner`; surface per-element + complex deletion.
- Update all compaction stream callers + `merge.rs` producer thread (748-761) / `build_merge_entry` (780).
- Merge: emit one `CellData` per element (populated `cell_path`/`ttl`/`local_deletion_time`/per-element `timestamp`); populate `MergeEntry.complex_deletions`. Re-key reconcile winners to `(String, Option<Vec<u8>>)`. Apply per-(column,cell_path) tie-break (tombstone beats live at equal ts, then value bytes). Drop complex deletion unless it strictly supersedes.
- Fix `estimate_entry_size` for cell_path bytes (#827 128MiB bound).

## Phase B (WI-3) — CellOperation model + merge_entry_to_mutation  [blockedBy A]
```rust
// add to CellOperation
WriteComplexElement { column: String, cell_path: Vec<u8>, value: Option<Value>, timestamp_micros: i64, ttl_seconds: Option<u32>, local_deletion_time: Option<i32> },
ComplexDeletion { column: String, marked_for_delete_at: i64, local_deletion_time: i32 },
```
- Rewrite `merge_entry_to_mutation`: group surviving `CellData` by `(column, cell_path)`, emit per-element ops + a `ComplexDeletion` op when `MergeEntry.complex_deletions` present.
- Re-evaluate `is_metadata_only_no_op` filter (1244): fully-deleted-collection deletions must now survive.

## Phase C (WI-4) — writer per-element emit + real complex deletion  [blockedBy B]
- Thread per-element timestamp/ttl/ldt through `MergedOp` (2943) instead of one `mop.timestamp_micros`.
- Replace hardcoded LIVE sentinel (2069-2073) with reconciled complex deletion when present, then emit surviving cells (new "deletion + cells" path).
- Per-element ts == row ts → keep `USE_ROW_TIMESTAMP` 0x08; != row ts → clear flag + explicit delta.
- Preserve source `cell_path` end-to-end (LIST TimeUUID must round-trip, not regenerate).

## Byte-format invariants (must hold for sstabledump + compaction-parity)
- DeletionTime order: `markedForDeleteAt` (unsigned VInt delta from min_timestamp, µs) then `localDeletionTime` (unsigned VInt delta from min_local_deletion_time, s). LIVE = (i64::MIN, i32::MAX).
- Element order: SET by serialized bytes; MAP by key bytes; LIST insertion order w/ 16-byte TimeUUID paths. Per-element ts must not reorder.
- Cell flags: IS_DELETED 0x01, IS_EXPIRING 0x02, HAS_EMPTY_VALUE 0x04, USE_ROW_TIMESTAMP 0x08, USE_ROW_TTL 0x10.
- All per-element deltas >= 0 against seeded baselines (`compute_baseline_min`); surviving element ts must not fall below baseline.
- Far-future LDT in [2^31,2^32) encoded as `as u32 as i32` wrapping — do NOT widen to i64.
- `PartitionEmitCounts.totalColumnsSet` must match Cassandra `Row.columnCount()` for non-frozen collections.

## Per-phase loop
TDD (failing test first) → implement via sstable-developer → `scripts/agent-gate.sh` green
→ commit → roborev review on commit → must pass before next phase. Final: differential parity + merge + close #899.
