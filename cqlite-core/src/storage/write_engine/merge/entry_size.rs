//! Heap-size estimation for one `MergeEntry` (issue #2820, roborev round 2).
//!
//! # Why this is not a wildcard match any more
//!
//! Two budgets are denominated in this estimate: `RunReader`'s read-ahead buffer
//! (`RunReader::DEFAULT_BUFFER_SIZE`, pre-dating #2820) and the egress batcher's
//! `BATCH_EMIT_BYTES_MERGE` byte budget (#2820). The previous estimator ended in
//! `_ => 32, // Default estimate for complex types`, so every nested `Value` —
//! `List`, `Set`, `Map`, `Tuple`, `Udt`, `Frozen`, `Json`, `Tombstone` — was
//! counted as 32 bytes however large it actually was. A batch of large nested
//! payloads could therefore hold hundreds of megabytes and never reach a 1 MiB
//! threshold: the byte bound was bypassable for exactly the row shapes most
//! likely to be large, and BOTH budgets were affected.
//!
//! Worse than the undercount itself, the wildcard **failed open for variants
//! nobody had considered**: a new `Value` variant inherited 32 bytes silently,
//! with no compile error and no failing test. The match over `Value` here is
//! therefore **EXHAUSTIVE — there is no `_` arm, and none may be added**. A new
//! `Value` variant is a compile error at this site, which forces whoever adds it
//! to decide its size (the same reasoning that holds the `channel_depth`
//! entry-unit invariant up, one module over).
//!
//! # Bounded, non-recursive traversal (the #1625 precedent)
//!
//! Nesting is walked with an explicit worklist rather than recursion — a
//! `Value` graph is attacker-shaped data, and a recursive walk over it is a
//! stack-overflow primitive. Because traversal state is not on the call stack
//! there is no depth cap, so a large scalar buried arbitrarily deep is counted
//! at its real size instead of collapsing to a floor. Worst-case WORK is bounded
//! instead, by node count: past [`MAX_ESTIMATE_NODES`] the estimate **fails
//! CLOSED**, returning `usize::MAX`.
//!
//! Failing closed is the only safe direction for both consumers, and both take
//! it correctly: the batcher's `pending_bytes.saturating_add(..)` immediately
//! exceeds any budget and flushes, and `RunReader::refill_buffer` stops
//! read-ahead after the current entry. An unmeasurable size must never take the
//! permissive branch (a 32-byte guess for an arbitrarily large nested value is
//! exactly that shape).
//!
//! # Every container costs THREE things (roborev round 4)
//!
//! The undercount above was the first of three instances of ONE class:
//! *a container's ELEMENT ALLOCATION is not counted, only its header and its
//! element payloads.* The three were the `_ => 32` wildcard, `range_deletion`'s
//! uncounted `ClusteringKey` bounds, and `add_clustering_key`'s uncounted
//! `Vec<(String, Value)>` element array (a `(String, Value)` slot is ~8x the
//! payload of a `BigInt` clustering component, so a wide scalar clustering key
//! underestimated by roughly the whole array).
//!
//! So every container this module walks is accounted in three parts:
//!
//! 1. its **header** — `size_of::<Vec<_>>()` etc., or already inside the
//!    enclosing `size_of` when the container is an inline field;
//! 2. its **element array** — `capacity() × size_of::<Element>()`, via
//!    [`SizeAcc::add_element_array`]. `capacity()`, not `len()`: capacity is
//!    what is allocated, and over-counting is the safe direction. `Element` is
//!    the container's own slot type — `(Value, Value)` for a `Map`,
//!    `UdtField` for a UDT, `(String, Value)` for a clustering key;
//! 3. each element's own **heap payload** — the `String`/`Bytes` bytes, and
//!    nested `Value`s via the worklist.
//!
//! A field that owns heap through something other than a `Vec` needs the same
//! three-part treatment: `RowKey`'s `Arc<[u8]>` bounds inside a
//! `Value::Tombstone` are counted as [`ARC_CONTROL_BLOCK`] + bytes, having
//! previously been swallowed by a flat `size_of + 16`.
//!
//! # Unit convention
//!
//! Bytes of heap *owned by* the entry, plus the entry's own `size_of`. The
//! per-variant scalar figures are carried over from the previous estimator
//! VERBATIM, so this change is strictly additive: an entry with no nested value
//! and no range deletion estimates byte-identically to before, which is why the
//! #827 flat-payload memory fixture cannot move. It is an ESTIMATE (container
//! overheads are approximated, and a nested element's inline slot is counted by
//! both its parent's slot arithmetic and its own arm), and every approximation
//! errs LARGE — the safe direction for a budget.

#[cfg(feature = "write-support")]
use super::model::{MergeEntry, RowData};
#[cfg(feature = "write-support")]
use crate::storage::write_engine::mutation::{ClusteringBound, ClusteringKey};
#[cfg(feature = "write-support")]
use crate::types::Value;
#[cfg(feature = "write-support")]
use smallvec::SmallVec;
#[cfg(feature = "write-support")]
use std::mem::size_of;

/// Upper bound on value nodes one entry's estimate may visit before it fails
/// closed (`usize::MAX`). Matches `Memtable::MAX_ESTIMATE_NODES` (issue #1625):
/// far beyond any legitimate row's node count, yet keeps the walk cheap.
#[cfg(feature = "write-support")]
pub(super) const MAX_ESTIMATE_NODES: usize = 1_000_000;

/// Bytes of `Arc` control block (strong + weak counters) in front of an
/// `Arc<[u8]>`'s payload — the allocation a `RowKey` owns beyond its bytes.
#[cfg(feature = "write-support")]
const ARC_CONTROL_BLOCK: usize = 2 * size_of::<usize>();

/// Estimate the heap bytes one `MergeEntry` occupies.
///
/// Covers every variable-sized field of `MergeEntry`: partition key bytes, the
/// clustering key's element array plus column names and values, live cells
/// (column name, value, cell path), complex-deletion markers, and — new in
/// #2820 — the clustering bounds carried by `range_deletion`, whose
/// `ClusteringKey` columns were not counted at all. The remaining fields
/// (`run_index`, `timestamp`, `row_deletion`, `partition_deletion`,
/// `row_liveness`) are fixed-size and already inside `size_of::<MergeEntry>()`
/// (`row_liveness` is `Copy` and owns no heap).
///
/// Container accounting follows the three-part rule in the module doc
/// (header + element array + element payloads).
#[cfg(feature = "write-support")]
pub(super) fn estimate_entry_size(entry: &MergeEntry) -> usize {
    let mut acc = SizeAcc::new(size_of::<MergeEntry>());

    acc.add(entry.key.key.len());

    if let Some(ck) = entry.clustering_key.as_ref() {
        acc.add_clustering_key(ck);
    }

    match &entry.row_data {
        RowData::Live { cells } => {
            for cell in cells {
                acc.add(size_of::<super::model::CellData>());
                acc.add(cell.column.len());
                // Epic #899: per-element cells carry cell-path bytes; count them
                // so the streaming buffer's memory accounting stays accurate
                // against the 128 MiB bound (#827).
                acc.add(cell.cell_path.as_ref().map_or(0, |p| p.len()));
                acc.add_value(&cell.value);
            }
        }
        RowData::Tombstone { .. } => acc.add(16),
    }

    // Epic #899: complex-deletion markers carried on the entry also occupy
    // memory; account for their column-name + fixed-size fields.
    for cd in &entry.complex_deletions {
        acc.add(size_of::<super::model::ComplexDeletion>());
        acc.add(cd.column.len());
    }

    // Issue #2820: a range tombstone's bounds own `ClusteringKey` columns —
    // unbounded heap that the previous estimator ignored entirely.
    if let Some(rt) = entry.range_deletion.as_ref() {
        for bound in [&rt.start, &rt.end] {
            match bound {
                ClusteringBound::Inclusive(ck) | ClusteringBound::Exclusive(ck) => {
                    acc.add_clustering_key(ck)
                }
                ClusteringBound::Bottom | ClusteringBound::Top => {}
            }
        }
    }

    acc.finish()
}

/// Saturating byte accumulator with a shared node budget across every value
/// walked for ONE entry (so an entry made of many medium collections is bounded
/// the same way a single huge one is).
#[cfg(feature = "write-support")]
struct SizeAcc {
    total: usize,
    visited: usize,
    /// Set once the node budget is exhausted; makes [`Self::finish`] fail closed.
    exhausted: bool,
}

#[cfg(feature = "write-support")]
impl SizeAcc {
    fn new(base: usize) -> Self {
        Self {
            total: base,
            visited: 0,
            exhausted: false,
        }
    }

    fn add(&mut self, bytes: usize) {
        self.total = self.total.saturating_add(bytes);
    }

    /// `ClusteringKey.columns: Vec<(String, Value)>` — header, ELEMENT ARRAY,
    /// then each element's own heap payload (issue #2820, roborev round 4).
    ///
    /// The element array is the half that was missing: a `(String, Value)` slot
    /// is `size_of::<String>()` + `size_of::<Value>()` bytes, ~8x the 8-byte
    /// payload of a `BigInt` component, so a row with many small clustering
    /// components underestimated by roughly the whole array — bypassing both
    /// the batch byte budget and read-ahead. Sized from `capacity()`, not
    /// `len()`: capacity is what is actually allocated, and over-counting is
    /// the safe direction for a budget.
    fn add_clustering_key(&mut self, ck: &ClusteringKey) {
        self.add(size_of::<Vec<(String, Value)>>());
        self.add_element_array(ck.columns.capacity(), size_of::<(String, Value)>());
        for (name, value) in &ck.columns {
            self.add(name.len());
            self.add_value(value);
        }
    }

    /// `count` element slots of `elem_size` bytes each — the heap array a
    /// container owns, distinct from its header and from the elements' own
    /// payloads. Saturating in BOTH operations: an attacker-shaped `capacity`
    /// must not wrap the product (and `usize::MAX` makes `+`/`*` a debug panic).
    fn add_element_array(&mut self, count: usize, elem_size: usize) {
        self.add(count.saturating_mul(elem_size));
    }

    /// The measured estimate, or `usize::MAX` when the node budget was exhausted.
    fn finish(self) -> usize {
        if self.exhausted {
            usize::MAX
        } else {
            self.total
        }
    }

    /// Would enqueuing `incoming` more children push this entry's total node
    /// count past the cap? Checked BEFORE enqueuing, so a single flat collection
    /// with far more than [`MAX_ESTIMATE_NODES`] elements fails closed WITHOUT
    /// first growing the worklist proportional to its element count (#1625).
    fn would_exceed_cap(&self, pending: usize, incoming: usize) -> bool {
        self.visited
            .saturating_add(pending)
            .saturating_add(incoming)
            > MAX_ESTIMATE_NODES
    }

    /// Walk one `Value` iteratively, adding its heap bytes.
    ///
    /// The match is EXHAUSTIVE by design (module doc): no `_` arm, so a new
    /// `Value` variant fails the build here rather than silently inheriting a
    /// permissive size.
    fn add_value(&mut self, value: &Value) {
        // Stack-backed worklist: 32 inline slots cover normal rows, so the hot
        // path walks with zero heap allocation; only deep/wide values spill.
        let mut worklist: SmallVec<[&Value; 32]> = SmallVec::new();
        worklist.push(value);

        while let Some(v) = worklist.pop() {
            self.visited += 1;
            if self.visited > MAX_ESTIMATE_NODES {
                self.exhausted = true;
                return;
            }

            match v {
                Value::Null => {}
                // Zero HEAP bytes: the sentinel's payload is the empty buffer
                // and its 1-byte type tag is inline in the `Value` slot, which
                // this estimator counts through the enclosing container
                // (issue #3805).
                Value::Empty(_) => {}
                Value::Boolean(_) | Value::TinyInt(_) => self.add(1),
                Value::SmallInt(_) => self.add(2),
                Value::Integer(_) | Value::Float32(_) | Value::Date(_) => self.add(4),
                Value::BigInt(_)
                | Value::Counter(_)
                | Value::Timestamp(_)
                | Value::Time(_)
                | Value::Float(_) => self.add(8),
                Value::Uuid(_) => self.add(16),
                Value::Duration { .. } => self.add(20),
                Value::Text(s) => self.add(s.len().saturating_add(size_of::<String>())),
                Value::Blob(b) | Value::Inet(b) | Value::Varint(b) => {
                    self.add(b.len().saturating_add(size_of::<Vec<u8>>()))
                }
                Value::Decimal { unscaled, .. } => self.add(
                    unscaled
                        .len()
                        .saturating_add(4)
                        .saturating_add(size_of::<Vec<u8>>()),
                ),
                Value::Tombstone(info) => {
                    // Base figure carried over VERBATIM so a tombstone with no
                    // range bounds estimates byte-identically to before.
                    self.add(size_of::<crate::types::TombstoneInfo>().saturating_add(16));
                    // Issue #2820 (roborev round 4): the `RowKey(Arc<[u8]>)`
                    // range bounds own UNBOUNDED bytes the flat figure above
                    // ignored — the same class as the clustering-key array.
                    for bound in [&info.range_start, &info.range_end] {
                        if let Some(rk) = bound.as_ref() {
                            self.add(rk.0.len().saturating_add(ARC_CONTROL_BLOCK));
                        }
                    }
                }
                Value::Json(json) => {
                    self.add(size_of::<serde_json::Value>());
                    self.add_json(json);
                    if self.exhausted {
                        return;
                    }
                }
                Value::List(items) | Value::Set(items) | Value::Tuple(items) => {
                    self.add(size_of::<Vec<Value>>());
                    self.add_element_array(items.capacity(), size_of::<Value>());
                    if self.would_exceed_cap(worklist.len(), items.len()) {
                        self.exhausted = true;
                        return;
                    }
                    worklist.extend(items.iter());
                }
                Value::Map(entries) => {
                    self.add(size_of::<Vec<(Value, Value)>>());
                    self.add_element_array(entries.capacity(), size_of::<(Value, Value)>());
                    // Each entry enqueues both a key and a value.
                    let incoming = entries.len().saturating_mul(2);
                    if self.would_exceed_cap(worklist.len(), incoming) {
                        self.exhausted = true;
                        return;
                    }
                    for (k, val) in entries {
                        worklist.push(k);
                        worklist.push(val);
                    }
                }
                Value::Udt(udt) => {
                    self.add(size_of::<crate::types::UdtValue>());
                    self.add(udt.type_name.len());
                    self.add(udt.keyspace.len());
                    self.add_element_array(
                        udt.fields.capacity(),
                        size_of::<crate::types::UdtField>(),
                    );
                    if self.would_exceed_cap(worklist.len(), udt.fields.len()) {
                        self.exhausted = true;
                        return;
                    }
                    for field in &udt.fields {
                        self.add(field.name.len());
                        if let Some(fv) = field.value.as_ref() {
                            worklist.push(fv);
                        }
                    }
                }
                Value::Frozen(inner) => {
                    self.add(size_of::<Value>());
                    if self.would_exceed_cap(worklist.len(), 1) {
                        self.exhausted = true;
                        return;
                    }
                    worklist.push(inner);
                }
            }
        }
    }

    /// Walk a JSON document iteratively, on the SAME node budget.
    ///
    /// Deliberately not `json.to_string().len()` (what `Memtable`'s estimator
    /// does): serialising an arbitrarily large document to measure it allocates
    /// the very memory this estimate exists to bound.
    fn add_json(&mut self, root: &serde_json::Value) {
        let mut worklist: SmallVec<[&serde_json::Value; 32]> = SmallVec::new();
        worklist.push(root);

        while let Some(v) = worklist.pop() {
            self.visited += 1;
            if self.visited > MAX_ESTIMATE_NODES {
                self.exhausted = true;
                return;
            }

            match v {
                serde_json::Value::Null => self.add(4),
                serde_json::Value::Bool(_) => self.add(5),
                serde_json::Value::Number(_) => self.add(size_of::<serde_json::Number>()),
                serde_json::Value::String(s) => {
                    self.add(s.len().saturating_add(size_of::<String>()))
                }
                serde_json::Value::Array(items) => {
                    self.add(size_of::<Vec<serde_json::Value>>());
                    self.add_element_array(items.capacity(), size_of::<serde_json::Value>());
                    if self.would_exceed_cap(worklist.len(), items.len()) {
                        self.exhausted = true;
                        return;
                    }
                    worklist.extend(items.iter());
                }
                serde_json::Value::Object(map) => {
                    self.add(size_of::<serde_json::Map<String, serde_json::Value>>());
                    if self.would_exceed_cap(worklist.len(), map.len()) {
                        self.exhausted = true;
                        return;
                    }
                    for (k, val) in map {
                        self.add(k.len().saturating_add(size_of::<String>()));
                        self.add(size_of::<serde_json::Value>());
                        worklist.push(val);
                    }
                }
            }
        }
    }
}
