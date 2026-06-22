## Merging, Tombstones, and Shadowing

Tombstones mark deletions at partition/row/cell levels (and ranges). This chapter explains how multiple SSTables and generations reconcile (shadowing), TTL expiry, and the effect of range tombstones.

### In this chapter you will learn
- Tombstone types and lifecycles
- Shadowing across SSTables/generations
- TTL expiry and gc_grace interactions
- Practical reconciliation rules

## Tombstone Types

- Partition, Row, Cell tombstones
- Range tombstones spanning clustering key intervals

## Reconciling Multiple Generations

Reconciliation applies Cassandra 5.0 semantics to select visible values.

Row-level handling ensures newer data can supersede older row tombstones when timestamps allow.

### Tombstone Tie-Breaking Hierarchy

When two cells share **equal timestamps**, `Cells.resolveRegular()` applies this precedence
(`Cells.java:79–128`, CASSANDRA-14592):

1. **Tombstone/expiring beats live cell** — any cell with a `localDeletionTime` wins over a live
   cell at the same timestamp.
2. **Pure tombstone beats expiring cell** — a hard delete wins over a TTL-expiring write.
3. **Higher `localDeletionTime` wins** — between two expiring cells or two tombstones.
4. **Lower TTL wins** — between two expiring cells with equal `localDeletionTime`.
5. **Value bytes** — final tiebreaker for live cells with identical timestamps.

### CQLite reconciliation behavior and divergences

CQLite's merge path (`cqlite-core/src/storage/write_engine/merge.rs`,
`reconcile_cluster`) implements only PART of the equal-timestamp hierarchy above.
What it actually compares, per column, is narrow — and the divergences below are
**not** an exhaustive enumeration of every place it differs from Cassandra. These
are documented honestly here and tracked in Appendix F.

- **What `reconcile_cluster` actually compares.** Per **`(column, cell_path)`**
  (epic #921; multi-cell collection/UDT elements reconcile independently — see
  "Compaction merge semantics" below), the winner is chosen by
  `cell_reconcile_replace`: (1) **strictly higher `timestamp`** wins; (2) at
  **equal** `timestamp`, a **cell tombstone beats a live OR expiring cell**
  (`is_cell_tombstone(candidate) && !is_cell_tombstone(existing)`), decided
  **before** any `localDeletionTime` compare (issue #848, parity Cassandra
  `a62c749`); (3) otherwise the **first-seen** cell is kept — and because inputs
  arrive in heap-routing order (`run_index` ascending = newest file first),
  first-seen means the newer file. Row-tombstone shadowing then drops any surviving
  cell whose `timestamp <= row_del` (the `<=` lets the row tombstone win at equal
  timestamp). That is the whole of the equal-timestamp logic.

- **What matches Cassandra at equal timestamp.** Because `is_cell_tombstone`
  distinguishes a cell tombstone from *any* non-tombstone (whether live or
  TTL-expiring), CQLite's check (2) makes a **cell tombstone beat both a live cell
  and an expiring cell** at equal timestamp — i.e. Cassandra's rule 1 (tombstone
  beats live) and rule 2 (pure tombstone beats expiring cell) are both honored.
  (Verified: #822 finding #13/#3 HOLDS.)

- **Equal-timestamp ordering BEYOND tombstone-vs-{live,expiring} — NOT implemented
  (further divergence).** Cassandra's `Cells.resolveRegular` also ranks
  non-tombstone cells: an **expiring cell beats a pure-live cell** (rule 1's
  expiring half — anything with a `localDeletionTime` wins over a plain live cell),
  **higher `localDeletionTime` wins** between two expiring cells or two tombstones
  (rule 3), and **lower TTL wins** between two equal-`localDeletionTime` expiring
  cells (rule 4). `reconcile_cluster` implements **none** of these: at equal
  timestamp it treats an expiring cell as live, so expiring-vs-pure-live, two
  tombstones differing only in `localDeletionTime`, or two expiring cells differing
  only in `localDeletionTime`/TTL all resolve by **first-seen** instead of by
  Cassandra's hierarchy. The merge `CellData` carries no `localDeletionTime`, so
  these comparisons are not even representable today. These are additional
  (currently-unhandled) divergences — NOT parity. Authority:
  `org.apache.cassandra.db.rows.Cells.resolveRegular`.

- **Live-cell value tie-break — CQLite divergence (#4/#21).** When two live cells
  tie on timestamp (and neither is a tombstone), Cassandra's
  `Cells.resolveRegular` keeps the cell with the **strictly-greater RAW value bytes**
  (unsigned lexicographic comparison on the raw value, skipping the VInt length
  prefix). **CQLite currently keeps the first-seen cell (newer file)** instead of
  comparing value bytes (`reconcile_cluster`: the `replace` predicate only fires for
  a higher timestamp or an equal-timestamp cell tombstone). This is a known
  divergence (ruled a FIX in issue #818; the fix is a follow-up). Authority:
  `org.apache.cassandra.db.rows.Cells.resolveRegular`.

- **Complex (collection/UDT) column merge — per-cell-path (RESOLVED in epic #921).**
  Cassandra merges complex columns **per cell-path** using the column's path
  comparator — signed `ShortType` for a UDT field index, `TimeUUIDType` for a list
  element, and the map key type for a map — applying shadow-before-purge per path.
  CQLite now reconciles complex columns per `(column, cell_path)`: `CellData`
  carries a `cell_path`, so disjoint elements of the same column survive and a
  same-key collision resolves by the higher per-cell timestamp (#844). UDT field
  paths are compared as **signed** `ShortType` and complex columns are matched
  **by name** across differing source headers (#888/#927). Complex deletion markers
  reconcile with strict-supersede + shadow-before-purge (#887). See "Compaction
  merge semantics" below.

## Compaction merge semantics

Epic #921 brought CQLite's compaction merge path
(`cqlite-core/src/storage/write_engine/merge.rs`, `reconcile_cluster`, and the
`merge_entry_to_mutation` rewrite) substantially closer to Cassandra's
`CompactionIterator` / `Cells#reconcile`. This section documents what the merge
**actually** does, verified against the code on this branch; each rule names the
function that implements it and (where given) the Cassandra parity commit.

### Per-(column, cell_path) reconciliation (#844)

`reconcile_cluster` keys per-cell winners by `(column, cell_path)`, not by whole
column. A simple cell has `cell_path == None` and behaves as before; each element
of a multi-cell collection or UDT carries its authoritative `cell_path` and
reconciles independently. **Disjoint elements** of the same column written in
different SSTables both survive; the **same `(column, cell_path)` key** resolves to
the cell with the **higher per-cell timestamp** (`cell_reconcile_replace`). On the
write-out side, `cells_to_cell_operations` emits one
`CellOperation::WriteComplexElement` per surviving element (preserving its
`cell_path`, `timestamp`, `ttl`, `local_deletion_time`, and authoritative
`is_deleted`), so the elements round-trip rather than collapsing to a whole-column
value.

### UDT cell-path ordering and match-by-name (#888 / #927)

A UDT field-index cell path is a 2-byte **signed** `ShortType` value, so a field
index in `[32768, 65535]` is negative as `i16` and must sort **before** the
positive indices. `compare_cell_paths(a, b, is_udt=true)` decodes both paths with
`i16::from_be_bytes` and compares as signed; collection (non-UDT) paths keep
plain lexicographic byte ordering (parity Cassandra `d14c96b8` / `5e636f9`).
Complex columns are matched **by name** (`ComplexDeletion.column`,
`udt_declared_field_names` resolving a `Value::Udt` literal's fields to their
declared index by name) rather than by header identity, so two sources whose
serialization headers differ still merge the same logical column. Non-frozen UDT
multi-cell data is now read and written end-to-end (#927).

### Complex-deletion reconciliation (#887)

Complex (collection/UDT) deletion markers reconcile in a dedicated stage (Step 2b
of `reconcile_cluster`) that runs **after** per-cell winner resolution and
**before** the row-tombstone and gc_grace filters (parity Cassandra `bd244649` +
`f66fa14f`):

- **Strict supersede.** Per complex column (matched by name) the active deletion
  is the one with the greatest `marked_for_delete_at`; a candidate supersedes only
  when its `marked_for_delete_at` is **strictly greater** — **equal** timestamps do
  **not** supersede.
- **Shadow before purge.** For the surviving deletion on a column, every per-element
  winner of that column whose timestamp is `<= marked_for_delete_at` is **shadowed
  (dropped) before** the marker itself is purged, so a later purge of the marker can
  never resurrect a covered element. An element strictly newer than
  `marked_for_delete_at` survives.

**Row-tombstone interaction.** A row tombstone at `row_del` shadows only timestamps
`<= row_del`. In `merge_entry_to_mutation`, a carried complex-deletion marker whose
`marked_for_delete_at` is **strictly greater** than `row_del` covers a range the row
tombstone does not (including elements in SSTables outside this compaction), so it is
**preserved and emitted** as a `CellOperation::ComplexDeletion` alongside the
`DeleteRow`. A marker with `marked_for_delete_at <= row_del` is fully covered and is
dropped.

### Tombstone-vs-expiring (TTL) tie-break (#848)

At **equal timestamp** a cell **tombstone beats an expiring (TTL) cell**, and this is
decided **before** the `localDeletionTime` compare. `cell_reconcile_replace` compares
timestamps first, then returns `is_cell_tombstone(candidate) &&
!is_cell_tombstone(existing)`; because `is_cell_tombstone` treats an expiring cell as
non-tombstone (it carries a real value plus a TTL, not a `CellTombstone`), the single
rule subsumes both tombstone-beats-live and tombstone-beats-expiring (parity Cassandra
`a62c749`). The further equal-timestamp ranking among non-tombstone cells
(expiring-beats-live, higher-`localDeletionTime`, lower-TTL) is still **not**
implemented — see the divergence note above and Appendix F.

### gc_grace / gcBefore purging during compaction (#845)

A tombstone whose on-disk `localDeletionTime` is **strictly less than** `gcBefore` is
purged from the output (Step 3c of `reconcile_cluster`, parity Cassandra `8d47ebb2`).
Key invariants, verified in `compute_gc_before` and `reconcile_cluster`:

- **Clock and cutoff.** `localDeletionTime` is the GC clock in **seconds**;
  `gcBefore = now_secs - gc_grace_seconds`. When the table declares no
  `gc_grace_seconds`, CQLite falls back to Cassandra's table **default of 864000
  seconds** (10 days). An invalid (unparseable or negative) declared value returns
  `None`, disabling purging (a strict no-op — garbage metadata never drops data).
- **Unsigned LDT.** `localDeletionTime` is read **unsigned** (`i64::from(ldt as u32)`)
  so a far-future LDT with bit 31 set is not mistaken for an ancient negative `i32`.
- **`LDT == 0` is unknown.** A zero LDT is the "not surfaced" placeholder and the
  tombstone is **retained** (never purge on unknown LDT — the no-heuristics mandate).
- **No resurrection.** Purge runs **after** the complex-deletion shadow stage and the
  row-tombstone / dropped-column filters, so a now-redundant marker is dropped only
  once everything it covered within the compaction is already gone.
- **Overlap safety.** Purging happens **only** on an overlap-safe (full/major)
  compaction that spans every SSTable for the table. `merge_partition_rows` collapses
  the effective `gc_before_secs` to `None` for a partial/background compaction
  (`KWayMerger::with_purge_safe` / the `purge_safe` flag), so partial compactions
  **retain** tombstones and cannot resurrect data shadowed in a non-included
  overlapping SSTable. In the CLI this is opt-in via `--major` / `--purge-tombstones`.

### Writer invariants surfaced by the merge path

The compaction rewrite exposed several writer invariants that must hold or the
delta-encoded SSTable would be corrupt:

- **Stats/baseline must fold every emitted field.** The Statistics.db baselines
  (`min_timestamp`, `min_local_deletion_time`, `min_ttl`) are folded over
  `ComplexDeletion` (`marked_for_delete_at` + `local_deletion_time`),
  `WriteComplexElement` (per-element `timestamp` / `ttl` / `local_deletion_time`), and
  per-cell `Delete` (`op_cell_local_deletion_time`) in
  `SSTableWriter::write_partition` (`writer/mod.rs`). A marker or element whose
  timestamp/LDT lies below an un-folded baseline would underflow the unsigned delta.
- **Per-op deletion timestamp shadowing is uniform** across the normal and shadowed
  writer paths.
- **Per-cell `Delete` localDeletionTime is preserved** through the merge→writer path
  (`cells_to_cell_operations` threads the source cell tombstone's own LDT into
  `CellOperation::Delete { local_deletion_time }`), avoiding GC-clock drift that would
  purge a surviving tombstone too early — or keep it too long — in a later compaction.
- **The WAL has three backward-compatible record layouts** (the WAL has no per-record
  version field and bincode is positional): (A) pre-#764 (no mutation-level
  `local_deletion_time`, old `Delete` op shape), (B) post-#764 / pre-#921
  (mutation-level LDT present, old `Delete` op shape), and (C) current
  (`CellOperation::Delete` carries `local_deletion_time`). `deserialize_mutation`
  attempts most-recent-first and falls back, mapping older layouts to `None` LDTs.

> **Reference: Cassandra parity.** Per-cell reconcile and the tombstone-vs-expiring
> tie-break mirror `org.apache.cassandra.db.rows.Cells#reconcile` (commit `a62c749`);
> complex-deletion strict-supersede + shadow-before-purge mirror commits `bd244649`
> and `f66fa14f`; UDT signed-short paths + match-by-name mirror `d14c96b8` / `5e636f9`;
> gc_grace purging mirrors `8d47ebb2`.

### Clustering order: empty vs valued under DESC

For a reversed (DESC) clustering column, an **empty** clustering value sorts
**after** a valued one; the empty-vs-valued comparison is routed through the
column's reversed-ness rather than compared as raw bytes. CQLite implements this in
`ClusteringKey::compare`
(`cqlite-core/src/storage/write_engine/mutation.rs`): the per-column ordering from
`compare_values` (which orders an empty/`Null` value `Less` than a valued one) is
`.reverse()`d when `ClusteringOrder::Desc`, so under DESC the empty value compares
`Greater` (sorts last). This matches Cassandra's reversed-type clustering
comparison.

## Range Tombstones

Range tombstones delete clustering intervals; readers must compare timestamps against range bounds during reconciliation.

> **Warning: range tombstones during compaction.** As of epic #921, range
> tombstones are **not** applied or emitted end-to-end in the compaction merge path.
> The V5CompressedLegacy reader only **skips** range markers (it does not surface
> them to the merger, except on the dedicated delta-scan path), and the writer does
> not persist a surviving range marker through `merge_entry_to_mutation`. Tracked in
> #933 — see Appendix F.

## Tombstone Timeline Diagram

![Tombstone timeline](diagrams/tombstone-timeline)
- Alt text: Timeline showing writes, tombstones, and TTL expiry with shadowing
- Caption: Newer values can shadow older tombstones; TTLs create time-bound deletions

## Key Takeaways
- Newest wins by timestamp; at **equal timestamp**, tombstones (and expiring cells) always beat
  live cells (`Cells.java:94`, CASSANDRA-14592). Within equal-timestamp tombstones: pure
  tombstone beats expiring cell; then higher `localDeletionTime`; then lower TTL.
- Range tombstones apply only within their intervals and while active.
- TTL expiry can surface as synthetic tombstones.

### Complexity Notes
- Merge per row: sorting values is O(k log k) where k is the number of versions; single-pass reconciliation after sort is O(k).
- Range tombstone filtering: O(n × t) worst-case (n entries, t tombstones) but typically reduced by time-sorted early exits.

### References
- Cassandra 5.0.8 (pinned):
  - `Cells.java` (tombstone reconciliation L79–L128) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/rows/Cells.java#L79-L128
  - `DeletionTime.supersedes()` (partition/row tombstone precedence L158–L161) — https://github.com/apache/cassandra/blob/cassandra-5.0.8/src/java/org/apache/cassandra/db/DeletionTime.java#L158-L161
  - Rows/tombstones package — https://github.com/apache/cassandra/tree/cassandra-5.0.8/src/java/org/apache/cassandra/db/rows
  
For implementation details, see Appendix C.


