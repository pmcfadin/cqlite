//! Per-element / per-cell compaction read contract (epic #899, Phase A).
//!
//! The compaction read path historically emitted `(RowKey, ScanRow, i64)` per row,
//! collapsing every non-frozen collection / UDT into a single nested [`Value`]
//! with one row-level timestamp. That representation cannot reconcile two
//! SSTables that wrote DISJOINT elements of the same multi-cell column (each
//! element has its own timestamp / ttl / local-deletion-time / cell-path), nor
//! can it carry a real per-column complex deletion (Cassandra's
//! `markedForDeleteAt` + `localDeletionTime` marker written ahead of a
//! multi-cell column's elements).
//!
//! [`CompactionRow`] replaces that tuple on the **compaction-only** read path. It
//! preserves the on-disk per-element granularity so the k-way merge can perform
//! byte-faithful per-`(column, cell_path)` reconciliation. The user-facing read
//! path (`scan` / `get` / `iterate_all_partitions` / `WRITETIME(collection)`) is
//! UNCHANGED — it still uses the collapsed [`Value`] representation.
//!
//! Byte-format invariants this representation must preserve (see
//! `docs/sstables-definitive-guide/` Ch.5 + Appendix B):
//! - Element order is the on-disk order (SET by serialized bytes, MAP by key
//!   bytes, LIST insertion order with 16-byte TimeUUID paths). Per-element
//!   timestamps must NOT reorder elements.
//! - A complex deletion's `LIVE` sentinel is `(i64::MIN, i32::MAX)`; a real
//!   deletion carries `(markedForDeleteAt µs, localDeletionTime s)`.
//! - Far-future local deletion times in `[2^31, 2^32)` are preserved as the
//!   wrapping `as u32 as i32` value — never widened to i64.

use crate::types::{RowKey, ScanRow, TombstoneType, Value};

/// One row surfaced by the compaction read path, carrying per-element complex
/// cells and the real per-column complex deletion (epic #899, Phase A).
///
/// This is the compaction-only counterpart of the old `(RowKey, ScanRow, i64)`
/// tuple. `row_timestamp` is the row-level write timestamp (for a tombstone it
/// is `markedForDeleteAt`); `row_data` holds either a row tombstone or the live
/// simple + complex cells.
#[derive(Debug, Clone, PartialEq)]
pub struct CompactionRow {
    /// Partition key bytes (token derived downstream).
    pub key: RowKey,
    /// Row-level write timestamp in microseconds (for a tombstone:
    /// `markedForDeleteAt`).
    pub row_timestamp: i64,
    /// Row payload: tombstone or live cells.
    pub row_data: CompactionRowData,
}

impl CompactionRow {
    /// Build a [`CompactionRow`] from the legacy collapsed `(RowKey, ScanRow,
    /// timestamp)` representation (the non-V5 compaction fallback path).
    ///
    /// This loses per-element complex granularity (the legacy fallback has none
    /// to begin with): a live `ScanRow::Row` becomes simple cells, a
    /// `ScanRow::Marker(Value::Tombstone(RowTombstone))` becomes a row tombstone,
    /// any other marker becomes a single `value` cell. The V5CompressedLegacy
    /// path bypasses this and builds per-element rows directly.
    ///
    /// Issue #1334: the reader carries every row through the single [`ScanRow`]
    /// carrier — this consumer disassembles that same carrier (no `Value::Map`
    /// bifurcation).
    pub fn from_legacy_value(key: RowKey, row: ScanRow, row_timestamp: i64) -> Self {
        let row_data = match row {
            // A live row's interned cells become simple cells.
            ScanRow::Row(entries) => {
                let simple = entries
                    .into_iter()
                    .map(|(k, v)| {
                        let timestamp = match &v {
                            Value::Tombstone(info) => info.deletion_time,
                            _ => row_timestamp,
                        };
                        SimpleCell {
                            column: k.to_string(),
                            value: v,
                            timestamp,
                            ttl: None,
                            local_deletion_time: None,
                        }
                    })
                    .collect();
                CompactionRowData::Live {
                    simple,
                    complex: Vec::new(),
                    // A live `ScanRow::Row` never carries a coexisting row deletion
                    // (a row tombstone arrives as a `ScanRow::Marker`, handled below).
                    row_deletion: None,
                    row_liveness: RowLiveness::default(),
                }
            }
            // A row tombstone marker becomes a row tombstone.
            ScanRow::Marker(Value::Tombstone(info))
                if info.tombstone_type == TombstoneType::RowTombstone =>
            {
                CompactionRowData::Tombstone {
                    deletion_time: info.deletion_time,
                    local_deletion_time: 0,
                    // The legacy collapsed-value fallback has no clustering capture
                    // (the clustering prefix is not surfaced on this path), so the
                    // tombstone lands in the partition's `None` clustering bucket
                    // exactly as before (#912 carries clustering only on the V5
                    // per-element path).
                    clustering: Vec::new(),
                }
            }
            // A raw undecoded fallback row collapses to a single `value` cell
            // carrying the raw bytes as a blob — the exact pre-#1334 shape a bare
            // `Value::Blob` produced in this legacy collapsed-value fallback.
            ScanRow::RawRow(bytes) => CompactionRowData::Live {
                simple: vec![SimpleCell {
                    column: "value".to_string(),
                    value: Value::Blob(bytes.into()),
                    timestamp: row_timestamp,
                    ttl: None,
                    local_deletion_time: None,
                }],
                complex: Vec::new(),
                row_deletion: None,
                row_liveness: RowLiveness::default(),
            },
            // Any other marker (null row, cell tombstone, …) collapses to a single
            // `value` cell, exactly as the pre-#1334 fallback did.
            ScanRow::Marker(other) => CompactionRowData::Live {
                simple: vec![SimpleCell {
                    column: "value".to_string(),
                    value: other,
                    timestamp: row_timestamp,
                    ttl: None,
                    local_deletion_time: None,
                }],
                complex: Vec::new(),
                row_deletion: None,
                row_liveness: RowLiveness::default(),
            },
        };
        CompactionRow {
            key,
            row_timestamp,
            row_data,
        }
    }
}

/// A clustering bound of a range-tombstone marker surfaced on the compaction
/// read path (issue #933).
///
/// Reader-native counterpart of
/// [`crate::storage::write_engine::mutation::ClusteringBound`]; kept here so the
/// compaction read contract does not depend on the write-engine types. Each
/// bound carries its clustering-prefix `(name, value)` pairs (possibly a PREFIX
/// shorter than the full clustering arity). An open bound (the writer emits these
/// as an inclusive bound with zero clustering values) is [`Self::Bottom`] /
/// [`Self::Top`].
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionBound {
    /// Inclusive bound (the clustering prefix is part of the deletion range).
    Inclusive(Vec<(String, Value)>),
    /// Exclusive bound (the clustering prefix is NOT part of the deletion range).
    Exclusive(Vec<(String, Value)>),
    /// Before all clustering keys (start of partition).
    Bottom,
    /// After all clustering keys (end of partition).
    Top,
}

/// Primary-key (row-marker) liveness surfaced on a live compaction row so a
/// READ consumer can apply Cassandra's row-visibility rule (issue #2374/#2789).
///
/// A row is visible to a `SELECT` iff it has at least one live data cell OR a
/// LIVE primary-key liveness marker (`HAS_TIMESTAMP`, whose TTL — if any — has
/// not expired). Compaction (the WRITE path) never SERIALIZES these fields — it
/// retains an expired marker within gc_grace for byte-parity, and
/// `merge_entry_to_mutation` does not read `row_liveness` at all (the emitted row
/// marker is derived from `MergeEntry::timestamp` and the surviving cells) — so the
/// consumer of the values is the read side (Flight `do_get` / cross-generation read
/// merge).
///
/// Since #3094 the write path does gate one BRANCH on
/// [`Self::marker_survives_floor`]: `apply_partition_shadowing`'s
/// `!has_data && !marker_live` arm. That branch cannot move an emitted byte. It is
/// reachable only when no data cell survives the partition floor, and in that case
/// the reconciled `MergeEntry::timestamp` — `merge::reconcile`'s max over the
/// surviving cells, INCLUDING the clustering pseudo-cells, whose timestamp is the
/// marker's own — is itself at/below the floor, so the arm concludes exactly what the
/// pre-#3094 row-timestamp test concluded. The #3094 shape (a cell tombstone written
/// after the deletion) never reaches it at all: `apply_partition_shadowing`'s
/// `is_data` test is BY NAME, so that tombstone survives the floor as data and
/// `has_data` stays `true` — there `marker_survives_floor` only selects the carried,
/// never-serialized `row_liveness`.
///
/// No-heuristics (#28): every field comes from the authoritative on-disk row header,
/// never inferred.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RowLiveness {
    /// Whether the row carried a primary-key liveness marker (`HAS_TIMESTAMP` —
    /// i.e. it was INSERTed, not created implicitly by a data-cell UPDATE).
    pub has_marker: bool,
    /// Marker expiry (epoch seconds) when the liveness marker carries a TTL
    /// (`HAS_TTL`); `None` when the marker is live-forever (no TTL). Only
    /// meaningful when `has_marker` is `true`. Reinterpreted UNSIGNED upstream
    /// so a post-2038 expiry is not wrapped negative.
    pub expires_at_seconds: Option<i64>,
    /// The marker's authoritative WRITE timestamp in microseconds (the row
    /// header liveness `timestamp`), when present. Populated from the on-disk
    /// row header only (no-heuristics, #28) — never inferred. Carries the
    /// last-write-wins key for [`Self::merge`] so a cross-generation fold keeps
    /// the NEWER liveness marker outright (a newer TTL'd marker can supersede an
    /// older live-forever one, and vice-versa) instead of a most-permissive
    /// union. `None` when `has_marker` is `false`.
    pub marker_timestamp: Option<i64>,
}

impl RowLiveness {
    /// `true` when the primary-key liveness marker is present AND still live at
    /// `now_secs` (no TTL, or its expiry is strictly in the future). Matches
    /// `Cell.isLive`: live iff `now < localExpirationTime`.
    pub fn marker_live_at(&self, now_secs: i64) -> bool {
        self.has_marker && self.expires_at_seconds.is_none_or(|s| s > now_secs)
    }

    /// Whether a reconciled row's primary-key liveness marker survives a DELETION
    /// FLOOR — the `markedForDeleteAt` (µs) of a covering partition (or range)
    /// deletion — and may therefore still be carried forward.
    ///
    /// Cassandra judges this on the MARKER's OWN write timestamp: `BTreeRow.filter`
    /// (`cassandra-5.0.8`) computes
    /// `if (activeDeletion.deletes(newInfo.timestamp())) newInfo = LivenessInfo.EMPTY;`
    /// and `DeletionTime.deletes(long ts)` is `ts <= markedForDeleteAt`. So the
    /// marker survives iff [`marker_timestamp`](Self::marker_timestamp) is strictly
    /// greater than `floor`.
    ///
    /// `row_timestamp` — the RECONCILED ROW timestamp (the max over the row's
    /// surviving cells) — is therefore consulted ONLY on the marker-LESS arm
    /// (`marker_timestamp == None`, i.e. `has_marker == false`): there is no marker
    /// timestamp to judge, so the row-level test stays the sole condition and
    /// marker-less behaviour is unchanged. (Purging a marker-less tombstone-only row
    /// is tracked separately as issue #3121.)
    ///
    /// Deliberately NOT an `&&` of both tests. `row_timestamp` ALONE is the #3094
    /// defect: a cell TOMBSTONE written AFTER the deletion raises the reconciled row
    /// timestamp above `floor` even when the marker itself is covered, so a deleted
    /// marker was carried forward and resurrected an all-null phantom row out of a
    /// deleted partition — reaching every consumer that decides row visibility from
    /// the merged entry alone (Flight's `producer.rs::entry_to_row`, shared by its
    /// row-stream and pushed-down-aggregate producers). But keeping `row_timestamp`
    /// as an ADDITIONAL necessary condition on the marker-present arm is equally
    /// non-Cassandra in the other direction: `BTreeRow.filter` never consults the
    /// row's cell timestamps when deciding the liveness marker's fate, so a marker
    /// strictly newer than `floor` whose data cells all predate it must still keep
    /// the key-only row VISIBLE. Conjoining the two tests would HIDE that row, and
    /// is unobservable today only because of an UNASSERTED cross-module invariant
    /// (`merge::reconcile` folds `row_timestamp` over the surviving clustering
    /// pseudo-cells too, whose timestamp is the marker's own, so
    /// `row_timestamp >= marker_timestamp` happens to hold). Splitting the arms
    /// removes the dependency on that invariant instead of documenting it: a future
    /// cleanup that excludes pseudo-cells from that fold can no longer silently
    /// hide a row Cassandra returns.
    pub fn marker_survives_floor(&self, row_timestamp: i64, floor: i64) -> bool {
        match self.marker_timestamp {
            // Marker present: judged on its OWN write timestamp, exactly as
            // `BTreeRow.filter` does — nothing about the row's cells participates.
            Some(marker_ts) => marker_ts > floor,
            // No marker timestamp: the row-level test is the only evidence there is.
            None => row_timestamp > floor,
        }
    }

    /// Fold two liveness markers across generations by Cassandra
    /// last-write-wins on the marker WRITE timestamp (issue #2374/#2789): the
    /// marker with the higher [`marker_timestamp`](Self::marker_timestamp) wins
    /// OUTRIGHT — its expiry / live-forever state is taken as-is, so a newer
    /// TTL'd marker supersedes an older live-forever one (and vice-versa). Later
    /// expiry is only a TIE-BREAK when the timestamps compare equal (or both are
    /// absent). A generation with no marker contributes nothing, so the surviving
    /// marker (if any) is carried through unchanged.
    ///
    /// This replaces the former most-permissive union, which unconditionally let
    /// a live-forever marker win regardless of write order and diverged from
    /// Cassandra for a reverse-timestamp reinsertion (older live-forever, newer
    /// expired TTL): the union reported the row VISIBLE where Cassandra keeps the
    /// newer expired liveness and HIDES it.
    pub fn merge(self, other: RowLiveness) -> RowLiveness {
        match (self.has_marker, other.has_marker) {
            (false, false) => RowLiveness::default(),
            (true, false) => self,
            (false, true) => other,
            (true, true) => {
                // Both generations carry a marker: last-write-wins on the
                // authoritative write timestamp; later expiry breaks a tie.
                let self_wins = match (self.marker_timestamp, other.marker_timestamp) {
                    (Some(a), Some(b)) if a != b => a > b,
                    // Equal (or one/both absent) timestamps → tie-break on the
                    // later expiry, treating live-forever (`None`) as the latest.
                    _ => match (self.expires_at_seconds, other.expires_at_seconds) {
                        (None, _) => true,
                        (_, None) => false,
                        (Some(a), Some(b)) => a >= b,
                    },
                };
                if self_wins {
                    self
                } else {
                    other
                }
            }
        }
    }
}

/// Live-or-tombstone payload of a [`CompactionRow`].
#[derive(Debug, Clone, PartialEq)]
pub enum CompactionRowData {
    /// A complete range tombstone (issue #933): the paired start + end bounds of
    /// a clustering-range delete, with the authoritative deletion timestamps.
    ///
    /// The reader pairs the on-disk start/end bound markers (or boundary markers)
    /// into one self-contained range so the compaction merge can shadow covered
    /// cells AND re-emit the surviving marker to the output SSTable. `deletion_time`
    /// is `markedForDeleteAt` (microseconds); `local_deletion_time` is the GC-grace
    /// clock (seconds, carried as the wrapping `as u32 as i32` for far-future LDTs).
    RangeMarker {
        /// Start bound of the deleted clustering range.
        start: CompactionBound,
        /// End bound of the deleted clustering range.
        end: CompactionBound,
        /// `markedForDeleteAt` in microseconds.
        deletion_time: i64,
        /// `localDeletionTime` in seconds (GC-grace clock).
        local_deletion_time: i32,
    },
    /// Partition-level tombstone (whole-partition delete) carrying its
    /// authoritative timestamps (issue #1072).
    ///
    /// Surfaced by the compaction read path as a synthetic carrier row (no
    /// clustering) so the cross-generation merge can apply the partition deletion
    /// as the OUTERMOST floor — shadowing every older cell/row/range/complex
    /// marker across ALL merge sources — and re-emit the surviving partition
    /// tombstone to the output SSTable. Without this carrier a newer partition
    /// tombstone in one SSTable failed to shadow older live rows in another,
    /// resurrecting deleted partitions. `deletion_time` is `markedForDeleteAt`
    /// (microseconds); `local_deletion_time` is the GC-grace clock (seconds,
    /// carried as the wrapping `as u32 as i32` for far-future LDTs).
    PartitionDelete {
        /// `markedForDeleteAt` in microseconds.
        deletion_time: i64,
        /// `localDeletionTime` in seconds (GC-grace clock).
        local_deletion_time: i32,
    },
    /// Row tombstone (whole-row delete) carrying its authoritative timestamps.
    Tombstone {
        /// `markedForDeleteAt` in microseconds.
        deletion_time: i64,
        /// `localDeletionTime` in seconds (GC-grace clock).
        local_deletion_time: i32,
        /// Clustering columns `(name, value)` in schema order identifying which
        /// clustering row this tombstone deletes (#912). On disk a row tombstone
        /// still carries its clustering prefix; capturing it here lets the merge
        /// route the tombstone into its own clustering bucket instead of
        /// collapsing every row tombstone (and the static row) into the single
        /// `None` bucket. Empty for an unclustered table (the partition's single
        /// row) and for the legacy collapsed-value fallback.
        clustering: Vec<(String, Value)>,
    },
    /// Live row: simple (single-cell) columns plus complex (multi-cell)
    /// columns with their per-element cells and optional complex deletion.
    Live {
        /// Simple, single-cell columns (incl. clustering columns surfaced as
        /// cells, and cell tombstones for deleted simple columns).
        simple: Vec<SimpleCell>,
        /// Complex (non-frozen collection / UDT) columns, each with its
        /// per-element cells and optional complex deletion.
        complex: Vec<ComplexColumn>,
        /// Row-level deletion that COEXISTS with the surviving live cells
        /// (issue #932). `Some((markedForDeleteAt µs, localDeletionTime s))`
        /// when this row carried `HAS_DELETION` AND still has surviving cells
        /// (the cells the merge kept are strictly newer than the deletion). The
        /// deletion is preserved so it keeps shadowing older cells of OTHER
        /// columns in SSTables not part of a partial compaction. `None` for a
        /// plain live row with no row deletion. A row whose ONLY payload is the
        /// deletion (no surviving cells) is a [`Self::Tombstone`], not a `Live`
        /// with this field set.
        row_deletion: Option<(i64, i32)>,
        /// Primary-key (row-marker) liveness of this row (issue #2374/#2789),
        /// carried so a READ consumer can hide a row whose only content is an
        /// EXPIRED liveness marker plus already-tombstoned cells. Carry-only for
        /// reads — the compaction WRITE path ignores it (byte-parity preserved).
        row_liveness: RowLiveness,
    },
}

/// A single-cell (simple) column value with its write metadata.
///
/// Cell tombstones for simple columns are represented by `value` holding a
/// `Value::Tombstone(CellTombstone)` (matching the legacy compaction stream).
#[derive(Debug, Clone, PartialEq)]
pub struct SimpleCell {
    /// Column name.
    pub column: String,
    /// Decoded cell value (or `Value::Tombstone` for a cell delete).
    pub value: Value,
    /// Effective cell write timestamp in microseconds (cell-own timestamp when
    /// present, else the row liveness timestamp).
    pub timestamp: i64,
    /// TTL in seconds when the cell is expiring (`None` otherwise).
    pub ttl: Option<u32>,
    /// `localDeletionTime` in seconds for an expiring / tombstone cell
    /// (`None` when not applicable).
    pub local_deletion_time: Option<i32>,
}

/// A complex (non-frozen collection / UDT) column: its per-element cells plus
/// an optional complex deletion marker covering elements written at or before
/// `marked_for_delete_at`.
#[derive(Debug, Clone, PartialEq)]
pub struct ComplexColumn {
    /// Column name.
    pub column: String,
    /// `Some((markedForDeleteAt µs, localDeletionTime s))` when a real complex
    /// deletion is present; `None` for the `LIVE` sentinel (no overwrite).
    pub complex_deletion: Option<(i64, i32)>,
    /// Per-element cells in on-disk order (epic #899 substrate / contract).
    pub elements: Vec<ComplexElement>,
    /// The whole-collection `Value` the reader collapses this column into
    /// (`Value::List` / `Value::Set` / `Value::Map`), EXACTLY as the
    /// pre-Phase-A read path produced it (SET/LIST element tombstones dropped,
    /// MAP null/tombstoned entries kept as `(key, Null)`, empty/overwritten
    /// collections kept as the empty collection).
    ///
    /// PHASE A NEUTRALITY (roborev #863, Finding 3): the merge OUTPUT path uses
    /// this collapsed value so the (untouched) writer emits byte-identical bytes
    /// to pre-Phase-A. The per-element `elements` ride alongside as the Phase-C
    /// foundation and are asserted by the reader-contract tests; per-element
    /// writer emit is Phase C.
    pub collapsed_value: Value,
}

/// A single element of a complex column (a list/set member, or a map entry).
#[derive(Debug, Clone, PartialEq)]
pub struct ComplexElement {
    /// Raw cell-path bytes that identify this element (the serialized element
    /// for a SET, the key for a MAP, the 16-byte TimeUUID for a LIST). Must be
    /// preserved byte-for-byte so the writer can round-trip it.
    pub cell_path: Vec<u8>,
    /// Decoded element value (`None` for a tombstoned or empty-value element,
    /// e.g. SET members which store the element in the path with an empty
    /// value).
    pub value: Option<Value>,
    /// Decoded element key for a MAP entry (the map key parsed from the
    /// `cell_path`). `None` for LIST / SET / UDT elements. Used to reconstruct a
    /// whole `Value::Map` for the writer-facing mutation while reconcile still
    /// keys on the raw `cell_path` bytes (epic #899, Phase A bridge).
    pub decoded_key: Option<Value>,
    /// Per-element write timestamp in microseconds (element-own when present,
    /// else the row liveness timestamp).
    pub timestamp: i64,
    /// TTL in seconds when the element is expiring (`None` otherwise).
    pub ttl: Option<u32>,
    /// `localDeletionTime` in seconds for an expiring / deleted element
    /// (`None` when not applicable). Far-future values in `[2^31, 2^32)` are
    /// kept as the wrapping `as u32 as i32` representation.
    pub local_deletion_time: Option<i32>,
    /// Whether this element carries the IS_DELETED (0x01) flag (an
    /// element-level tombstone).
    pub is_deleted: bool,
    /// Whether the on-disk cell carried the HAS_EMPTY_VALUE (0x04) flag.
    ///
    /// `true` for a SET member (whose value lives in the `cell_path`, not the
    /// cell value) and for any genuinely empty-value element. The compaction
    /// writer uses THIS flag — not the decoded [`value`](Self::value) — to decide
    /// whether to emit an on-disk value, so a SET element round-trips byte-for-
    /// byte (its decoded member is reconstructed from `cell_path`, never written
    /// as a cell value). Distinct from `is_deleted`: an empty-value live element
    /// is not a tombstone.
    pub has_empty_value: bool,
}

#[cfg(test)]
mod row_liveness_tests {
    use super::RowLiveness;

    fn marker(ts: i64, expires_at_seconds: Option<i64>) -> RowLiveness {
        RowLiveness {
            has_marker: true,
            expires_at_seconds,
            marker_timestamp: Some(ts),
        }
    }

    /// Issue #2374/#2789: the cross-generation fold is Cassandra last-write-wins
    /// on the marker WRITE timestamp, NOT a most-permissive union. A newer
    /// EXPIRED-TTL marker supersedes an older live-forever marker for a key-only
    /// row → the row is HIDDEN once `now` passes the newer marker's expiry.
    ///
    /// Pre-fix (union) this returned live-forever → `marker_live_at` true → the
    /// read path wrongly reported the row VISIBLE.
    #[test]
    fn newer_expired_ttl_supersedes_older_live_forever() {
        // gen A: INSERT live-forever @ts=200; gen B: INSERT ... USING TTL @ts=300
        // whose marker has since EXPIRED (expiry at epoch second 1_000).
        let gen_a = marker(200, None);
        let gen_b = marker(300, Some(1_000));

        let merged = gen_a.merge(gen_b);
        // The NEWER (ts=300) expired marker wins outright.
        assert_eq!(merged.marker_timestamp, Some(300));
        assert_eq!(merged.expires_at_seconds, Some(1_000));
        // Row is HIDDEN once now > expiry.
        assert!(
            !merged.marker_live_at(2_000),
            "newer expired-TTL marker must hide the key-only row (timestamp-LWW)"
        );
        // Fold order must not matter.
        let merged_rev = gen_b.merge(gen_a);
        assert_eq!(merged_rev.marker_timestamp, Some(300));
        assert!(!merged_rev.marker_live_at(2_000));
    }

    /// Reverse of the above: a newer live-forever marker supersedes an older
    /// expired-TTL one → the key-only row is VISIBLE.
    #[test]
    fn newer_live_forever_supersedes_older_expired_ttl() {
        // gen A: expired-TTL @ts=200 (expiry epoch second 500); gen B: live-forever @ts=300.
        let gen_a = marker(200, Some(500));
        let gen_b = marker(300, None);

        let merged = gen_a.merge(gen_b);
        assert_eq!(merged.marker_timestamp, Some(300));
        assert_eq!(merged.expires_at_seconds, None);
        assert!(
            merged.marker_live_at(2_000),
            "newer live-forever marker must keep the key-only row visible (timestamp-LWW)"
        );
        // Fold order must not matter.
        assert!(gen_b.merge(gen_a).marker_live_at(2_000));
    }

    /// Equal timestamps → tie-break on the later expiry (live-forever latest).
    #[test]
    fn equal_timestamps_tie_break_on_later_expiry() {
        let live_forever = marker(300, None);
        let ttl = marker(300, Some(500));
        assert_eq!(live_forever.merge(ttl).expires_at_seconds, None);
        assert_eq!(ttl.merge(live_forever).expires_at_seconds, None);

        let earlier = marker(300, Some(400));
        let later = marker(300, Some(900));
        assert_eq!(earlier.merge(later).expires_at_seconds, Some(900));
        assert_eq!(later.merge(earlier).expires_at_seconds, Some(900));
    }

    /// Issue #3094: `marker_survives_floor` judges the marker on its OWN write
    /// timestamp, so a cell tombstone that raises the RECONCILED ROW timestamp
    /// above a covering deletion cannot resurrect a marker the deletion covers.
    ///
    /// Cassandra authority (`cassandra-5.0.8`): `BTreeRow.filter` does
    /// `if (activeDeletion.deletes(newInfo.timestamp())) newInfo =
    /// LivenessInfo.EMPTY;`, and `DeletionTime.deletes(long ts)` is
    /// `ts <= markedForDeleteAt` — hence the equal-timestamp case below is DELETED.
    #[test]
    fn marker_survives_floor_uses_the_markers_own_timestamp() {
        // The #3094 shape: marker @100 <= floor 500 < reconciled row ts 1_000 (raised
        // by a cell tombstone written AFTER the deletion). The marker is DELETED.
        assert!(
            !marker(100, None).marker_survives_floor(1_000, 500),
            "a marker covered by the floor must not be resurrected by a newer cell \
             tombstone raising the reconciled row timestamp (#3094)"
        );
        // A marker strictly NEWER than the floor survives (the #2374/#2789 key-only
        // row that coexists with an older partition deletion stays VISIBLE).
        assert!(marker(900, None).marker_survives_floor(1_000, 500));
        // Equal timestamps: the deletion wins (`ts <= markedForDeleteAt`).
        assert!(!marker(500, None).marker_survives_floor(1_000, 500));
        // The row timestamp is NOT a second necessary condition once a marker is
        // present: `BTreeRow.filter` decides the marker's fate from `newInfo
        // .timestamp()` alone and never consults the row's cell timestamps, so a
        // marker strictly newer than the floor keeps the key-only row VISIBLE even
        // when every data cell it coexists with is at/below the floor. (Conjoining
        // the two tests would hide a row Cassandra returns; that this shape cannot
        // currently reach here is an unasserted invariant of `merge::reconcile`'s
        // row-ts fold, not a property of this rule.)
        assert!(marker(900, None).marker_survives_floor(500, 500));
        // MARKER-LESS row (`marker_timestamp == None`): unchanged behaviour — the
        // row-level test is the sole condition (#3121 tracks purging such a row).
        assert!(RowLiveness::default().marker_survives_floor(1_000, 500));
        assert!(!RowLiveness::default().marker_survives_floor(500, 500));
    }

    /// A generation with no marker contributes nothing.
    #[test]
    fn absent_marker_carries_the_present_one_through() {
        let present = marker(300, Some(500));
        assert_eq!(present.merge(RowLiveness::default()), present);
        assert_eq!(RowLiveness::default().merge(present), present);
        assert_eq!(
            RowLiveness::default().merge(RowLiveness::default()),
            RowLiveness::default()
        );
    }
}
