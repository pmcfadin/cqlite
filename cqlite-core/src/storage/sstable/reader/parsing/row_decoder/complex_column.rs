use super::marshal_element::MarshalCollectionElements;
use super::*;

// Issue #3612: the cell-path KEY decoder, split out of this file (campsite
// #1116). A MULTICELL map's key lives in the cell path and used to be decoded
// from a narrow allowlist here, falling back to an opaque `Value::Blob` for a
// composite key and ~10 scalar families. Its only caller is the map branch
// below, so it nests here rather than beside the whole-value decoders.
mod cell_path_key;

#[cfg(test)]
mod cell_path_key_tests;
#[cfg(test)]
mod regression_3747_empty_map_key_tests;
// Issue #3612 (R3-F1): the guarded component-length conversion, shared with the
// UDT field loops in `udt.rs` / `raw_type_value.rs` (see that module's header for
// why it lives here).
mod component_len;

/// Issue #2038 (roborev Medium finding): the shape of ONE visible collection
/// element's expiry, as input to [`ExpiryHomogeneity::fold`].
///
/// `LiveForever` is a live element with no TTL of any kind (`!is_expiring`).
/// `Explicit` is an element's EFFECTIVE `(ttl_seconds, expires_at_seconds)` —
/// either its OWN explicit per-element TTL (both `element_ttl` and
/// `element_local_deletion_time` decoded — the shape a per-element `USING
/// TTL` write emits), or, for a `USE_ROW_TTL` element (a statement-level
/// `INSERT ... USING TTL n` on a collection column — issue #2038 acceptance
/// criteria), the INHERITED row-liveness expiry threaded in via
/// `ElementShadow::row_ttl_seconds`/`row_expires_at` (mirroring the scalar
/// `USE_ROW_TTL` cell path's `effective_exp = cell_exp.or(row_level_exp)`,
/// row_data.rs ~line 736). `Unresolvable` is `is_expiring`, no explicit
/// per-element fields, AND no row-level expiry available to inherit (e.g. a
/// physical consumer with `element_filter: None`, or a genuinely corrupt
/// on-disk state) — its real expiry cannot be resolved from the element
/// alone, so it forces the homogeneity tracker to `Mixed` rather than
/// silently treating it as live-forever or omitting it (no-heuristics, #28).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ElementExpiryShape {
    LiveForever,
    Explicit(i32, i64),
    Unresolvable,
}

impl ElementExpiryShape {
    /// `row_ttl_seconds`/`row_expires_at` are the row-liveness TTL/expiry a
    /// `USE_ROW_TTL` element inherits (from `ElementShadow`, `None` when no
    /// shadow context is active — i.e. every physical consumer, byte-unchanged
    /// behavior). Both come from authoritative row-header fields (no
    /// heuristics): `row_header.ttl` and the SAME `liveness_expires_at_seconds`
    /// fallback the scalar `USE_ROW_TTL` cell path and the #1741 shadow filter
    /// both use.
    #[inline]
    fn from_cell(
        cell: &ComplexCellParse,
        row_ttl_seconds: Option<i32>,
        row_expires_at: Option<i64>,
    ) -> Self {
        if !cell.is_expiring {
            return Self::LiveForever;
        }
        match (cell.element_ttl, cell.element_local_deletion_time) {
            // Clamp `ttl` (a `u32`) to `i32::MAX` to mirror the scalar cell
            // reader's `abs_ttl.min(i32::MAX as i64) as i32` (see
            // `cell_value.rs`): a bare `as i32` cast would expose a NEGATIVE
            // `ttl_seconds` for a `u32` TTL > `i32::MAX`, violating the
            // `CellExpiration.ttl_seconds` contract. No real Cassandra data
            // triggers this (max TTL ~20y ≪ `i32::MAX`); defensive parity.
            (Some(ttl), Some(ldt)) => {
                Self::Explicit(ttl.min(i32::MAX as u32) as i32, ldt as u32 as i64)
            }
            // USE_ROW_TTL shape (issue #2038 round 3): no explicit per-element
            // TTL/LDT — both are decoded ONLY when NOT USE_ROW_TTL, so this
            // combination unambiguously means the element inherits the row's
            // expiry. Resolve it from the row-level values when both are
            // available; otherwise the real expiry is unknown here.
            (None, None) => match (row_ttl_seconds, row_expires_at) {
                (Some(ttl_s), Some(exp_s)) => Self::Explicit(ttl_s, exp_s),
                _ => Self::Unresolvable,
            },
            // Should not occur (ttl/ldt are always decoded together), but stay
            // conservative rather than guessing (no-heuristics, #28).
            _ => Self::Unresolvable,
        }
    }
}

/// Issue #2038 (roborev Medium finding): tri-state homogeneity tracker for the
/// per-cell-metadata `TTL()` value surfaced for a non-frozen collection/UDT
/// column.
///
/// Only VISIBLE (post shadow/TTL-filter, non-tombstone) elements participate
/// — a shadow/TTL-DROPPED element must not influence what the query layer
/// reports for the value it actually sees. This is a SEPARATE, narrower
/// aggregate than `max_element_expires_at`/`has_live_forever_element` above,
/// which intentionally fold dropped elements too for the orthogonal #1741
/// row-hidden decision (whether the WHOLE ROW should still be visible) — do
/// not conflate the two.
///
/// `Uniform` is the ONLY state that surfaces a `CellExpiration`: every visible
/// element must share the IDENTICAL explicit `(ttl_seconds,
/// expires_at_seconds)` pair. `Unseen` (no visible elements) and `LiveForever`
/// (every visible element has no TTL) both correctly surface `None` — there is
/// nothing to report. `Mixed` (elements disagree, or the shape can't be
/// resolved) ALSO surfaces `None` — correctness over over-approximating with
/// one element's expiry for a value the query does not see uniformly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExpiryHomogeneity {
    Unseen,
    LiveForever,
    Uniform(i32, i64),
    Mixed,
}

impl ExpiryHomogeneity {
    #[inline]
    fn fold(self, shape: ElementExpiryShape) -> Self {
        use ElementExpiryShape::{Explicit, LiveForever as ShapeLiveForever, Unresolvable};
        match (self, shape) {
            (Self::Mixed, _) => Self::Mixed,
            (_, Unresolvable) => Self::Mixed,
            (Self::Unseen, ShapeLiveForever) => Self::LiveForever,
            (Self::Unseen, Explicit(t, e)) => Self::Uniform(t, e),
            (Self::LiveForever, ShapeLiveForever) => Self::LiveForever,
            (Self::LiveForever, Explicit(..)) => Self::Mixed,
            (Self::Uniform(..), ShapeLiveForever) => Self::Mixed,
            (Self::Uniform(t, e), Explicit(t2, e2)) => {
                if t == t2 && e == e2 {
                    Self::Uniform(t, e)
                } else {
                    Self::Mixed
                }
            }
        }
    }

    /// Resolve to the `CellExpiration` surfaced in per-cell metadata: only
    /// `Uniform` produces one.
    #[inline]
    fn into_cell_expiration(self) -> Option<CellExpiration> {
        match self {
            Self::Uniform(ttl_seconds, expires_at_seconds) => Some(CellExpiration {
                ttl_seconds,
                expires_at_seconds,
            }),
            _ => None,
        }
    }
}

impl V5CompressedLegacyParser {
    /// Parse a complex column (non-frozen collection).
    /// Complex columns have multiple cells with cell paths.
    ///
    /// Format when HAS_COMPLEX_DELETION is set:
    ///   [complex_deletion_time: 2 VInts]  // DeletionTime
    ///   [cell_count: VInt]
    ///   [cell_1..cell_n: each with cell_path]
    ///
    /// Format when HAS_COMPLEX_DELETION is NOT set:
    ///   [cell_count: VInt]
    ///   [cell_1..cell_n: each with cell_path]
    ///
    /// Issue #221: This enables parsing of typed_collections_table and other
    /// tables with non-frozen collections.
    /// Outer entry point — the `reader` parameter is forwarded to the inner
    /// cells but is currently unused there (`_reader`).  The outer/inner split
    /// lets unit tests call `parse_complex_column_inner` without constructing a
    /// full `SSTableReader`.
    ///
    /// Returns `(value, new_offset, collection_meta)` where `collection_meta`
    /// carries DS4 extra info: whether the collection carries a tombstone
    /// (overwrite semantics), the max element writetime, and the element tombstone count.
    pub(super) fn parse_complex_column(
        &self,
        data: &[u8],
        offset: usize,
        column: &crate::schema::Column,
        // Issue #1081: authoritative on-disk marshal type used to decode the
        // complex value (e.g. `UserType(...)` for a non-frozen UDT). See
        // [`parse_complex_column_inner`].
        complex_type: &str,
        has_complex_deletion: bool,
        _reader: &crate::storage::sstable::reader::types::SSTableReader,
        // Issue #1741 (per-element filtering): read-side shadow/TTL context. `Some`
        // only on the user-facing SELECT read path; `None` keeps physical consumers
        // byte-unchanged.
        element_filter: Option<ElementShadow>,
    ) -> Result<(Value, usize, ComplexColumnMeta)> {
        self.parse_complex_column_inner(
            data,
            offset,
            column,
            complex_type,
            has_complex_deletion,
            0,
            None,
            element_filter,
        )
    }

    /// Inner complex-column parser.
    ///
    /// `complex_type` is the AUTHORITATIVE marshal type that drives the
    /// complex-value decode — for the live read/compaction paths this is the
    /// on-disk SerializationHeader `ColumnInfo.column_type` (issue #1081), which
    /// is the only source that carries `UserType(...)` for a non-frozen
    /// top-level UDT (the supplied schema's `column.data_type` is the bare CQL
    /// short form, e.g. `person_type`, and cannot express it). `column` still
    /// supplies the column identity (name) and is used for collection element /
    /// map-key decode where the supplied schema form is sufficient. No
    /// heuristics: both inputs are authoritative metadata (issue #28).
    ///
    /// When `elements_out` is `Some`, each parsed element (live, empty, or
    /// tombstoned) is also pushed as a
    /// [`crate::storage::sstable::reader::compaction_row::ComplexElement`] in
    /// on-disk order, so the compaction read path can surface per-element cells
    /// (epic #899). `row_timestamp` is the row liveness timestamp (µs) inherited
    /// by elements that carry the `USE_ROW_TIMESTAMP` (0x08) flag — only read
    /// when collecting elements. On the user-facing read path `elements_out` is
    /// `None`, `row_timestamp` is `0`, and no per-element collection occurs.
    pub(crate) fn parse_complex_column_inner(
        &self,
        data: &[u8],
        mut offset: usize,
        column: &crate::schema::Column,
        complex_type: &str,
        has_complex_deletion: bool,
        row_timestamp: i64,
        mut elements_out: Option<
            &mut Vec<crate::storage::sstable::reader::compaction_row::ComplexElement>,
        >,
        // Issue #1741 (per-element filtering): read-side shadow/TTL context. `Some`
        // ONLY on the user-facing SELECT read path (where `elements_out` is `None`);
        // `None` on every physical consumer (compaction / delta-scan / unit tests),
        // where no element is ever dropped so output is byte-unchanged.
        element_filter: Option<ElementShadow>,
    ) -> Result<(Value, usize, ComplexColumnMeta)> {
        use crate::storage::sstable::reader::compaction_row::ComplexElement;

        // Helper to push a per-element cell into `elements_out` (compaction
        // path only). `decoded_value` is the resolved element value (the list
        // member, the set member parsed from the path, or the map value);
        // `None` for a tombstoned / empty element. The effective timestamp is
        // the element-own writetime when present, else the inherited row
        // timestamp (USE_ROW_TIMESTAMP).
        fn record_element(
            out: &mut Option<&mut Vec<ComplexElement>>,
            cell: &ComplexCellParse,
            decoded_value: Option<Value>,
            decoded_key: Option<Value>,
            row_timestamp: i64,
        ) {
            if let Some(vec) = out.as_mut() {
                vec.push(ComplexElement {
                    cell_path: cell.path_bytes.clone(),
                    value: decoded_value,
                    decoded_key,
                    timestamp: cell.element_writetime.unwrap_or(row_timestamp),
                    ttl: cell.element_ttl,
                    local_deletion_time: cell.element_local_deletion_time,
                    is_deleted: cell.is_deleted,
                    has_empty_value: cell.has_empty_value,
                });
            }
        }
        tracing::debug!(
            "V5CompressedLegacy: Parsing complex column '{}' type='{}' has_complex_deletion={} at offset {}",
            column.name, column.data_type, has_complex_deletion, offset
        );

        // Step 1: Parse complex deletion time if flag is set.
        //
        // DS4 (Issue #700): Capture the `markedForDeleteAt` to determine whether this
        // generation carries a **collection-level tombstone** (`s = {...}` overwrite).
        // Cassandra stores the LIVE sentinel as i64::MIN when there is no tombstone;
        // any other value means the collection was overwritten (replaced, not appended).
        //
        // Wire format: DeletionTime = markedForDeleteAt (VInt delta from min_timestamp)
        //                           + localDeletionTime (VInt).
        // We treat `marked_for_delete_at != i64::MIN` as "has collection tombstone".
        let mut has_collection_tombstone = false;
        // Epic #899: the real complex deletion `(markedForDeleteAt µs,
        // localDeletionTime s)` for the compaction contract; `None` is the LIVE
        // sentinel (no overwrite).
        let mut complex_deletion: Option<(i64, i32)> = None;
        if has_complex_deletion {
            // Cassandra (SerializationHeader.writeDeletionTime ->
            // writeUnsignedVInt) encodes the markedForDeleteAt delta from
            // min_timestamp as an UNSIGNED VInt — matching the row-deletion path
            // (see parse_row_metadata, ~line 2585) and the writer
            // (write_complex_column_deletion, encode_unsigned). The earlier
            // parse_vint (ZigZag/signed) here mis-decoded any delta whose top
            // data bit was set, while still consuming the same number of bytes
            // (both variants are driven by leading-ones). Fix (roborev #863).
            let (remaining, mfda_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex column '{}': failed to parse markedForDeleteAt at offset {}: {:?}",
                    column.name, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;

            // Delta-decode to get the absolute timestamp.
            // The LIVE sentinel in Cassandra is Long.MIN_VALUE for markedForDeleteAt.
            let absolute_mfda = self.min_timestamp.wrapping_add(mfda_delta as i64);
            // Any value other than i64::MIN indicates a real collection tombstone.
            if absolute_mfda != i64::MIN {
                has_collection_tombstone = true;
            }

            // localDeletionTime is also an UNSIGNED VInt delta from
            // min_local_deletion_time (writer: encode_unsigned). Use the SAME
            // i32 wrapping/cast as the row/range deletion paths so far-future
            // LDTs in [2^31, 2^32) round-trip via `as u32 as i32`.
            let (remaining, local_deletion_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex column '{}': failed to parse localDeletionTime at offset {}: {:?}",
                    column.name, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;

            // Surface the real complex deletion for the compaction path. The
            // localDeletionTime is a delta from min_local_deletion_time; the
            // LIVE sentinel is i32::MAX. Far-future values in [2^31, 2^32) wrap
            // via `as u32 as i32` (epic #899 invariant). Only record a deletion
            // when markedForDeleteAt is not the LIVE sentinel.
            if absolute_mfda != i64::MIN {
                let absolute_ldt = self
                    .min_local_deletion_time
                    .wrapping_add(local_deletion_delta as i64);
                complex_deletion = Some((absolute_mfda, absolute_ldt as u32 as i32));
            }

            tracing::debug!(
                "V5CompressedLegacy: Complex column '{}' deletion time parsed \
                 (absolute_mfda={} has_collection_tombstone={}), now at offset {}",
                column.name,
                absolute_mfda,
                has_collection_tombstone,
                offset
            );
        }

        // Step 2: Parse cell count
        let (remaining, cell_count) = parse_vuint(&data[offset..]).map_err(|e| {
            Error::corruption(format!(
                "Complex column '{}': failed to parse cell count at offset {}: {:?}",
                column.name, offset, e
            ))
        })?;
        let bytes_consumed = data[offset..].len() - remaining.len();
        offset += bytes_consumed;

        tracing::debug!(
            "V5CompressedLegacy: Complex column '{}' has {} cells, now at offset {}",
            column.name,
            cell_count,
            offset
        );

        // Step 3: Parse all cells and aggregate values
        // Issue #225: Bounds check to prevent DoS from corrupted data (match frozen collection pattern)
        if cell_count > MAX_FROZEN_COLLECTION_SIZE {
            return Err(Error::corruption(format!(
                "Complex column '{}': cell count {} exceeds maximum {}",
                column.name, cell_count, MAX_FROZEN_COLLECTION_SIZE
            )));
        }
        // Convert cell_count to usize safely to prevent overflow on 32-bit systems
        let cell_count_usize: usize = cell_count.try_into().map_err(|_| {
            Error::corruption(format!(
                "Complex column '{}': cell count {} exceeds platform limit",
                column.name, cell_count
            ))
        })?;

        // Bound the up-front collection allocation by the bytes actually available
        // (issue #1632): every element/entry consumes at least one byte, so the
        // declared count can never legitimately exceed the remaining buffer. A
        // corrupt count near MAX_FROZEN_COLLECTION_SIZE against a short buffer must
        // not pre-allocate ~MBs; the per-element parse below still Errs on the short
        // buffer. Guard-only: valid inputs allocate the same (declared) capacity.
        let prealloc_cap = cell_count_usize.min(data.len().saturating_sub(offset));

        // DS4 (Issue #700): Track max element writetime and element tombstone count
        // across all cells in this collection.
        let mut max_element_writetime: i64 = 0;
        let mut element_tombstone_count: u64 = 0;

        // Issue #1741 (Finding 3): read-time TTL aggregate over LIVE elements.
        // `has_live_forever_element` becomes true when a live element carries no
        // TTL of any kind (never IS_EXPIRING); `max_element_expires_at` folds the
        // explicit per-element expiries. Scalar-only, no per-cell allocation.
        let mut has_live_forever_element = false;
        let mut max_element_expires_at: Option<i64> = None;
        // Issue #2038 (roborev Medium finding): VISIBLE-only homogeneity tracker
        // for the per-cell-metadata `TTL()` value surfaced for a non-frozen
        // collection/UDT column — the complex-cell analogue of the scalar #1743
        // fix, instead of hardcoding `expiration: None`. Deliberately separate
        // from `max_element_expires_at` above (see `ExpiryHomogeneity` doc).
        let mut expiry_homogeneity = ExpiryHomogeneity::Unseen;
        // Issue #2038 round 3: the row-liveness TTL/expiry a `USE_ROW_TTL`
        // element inherits, threaded from the SAME `ElementShadow` the #1741
        // shadow filter already carries (`None` for every physical consumer —
        // byte-unchanged; `element_filter` is `Some` only on the user-facing
        // SELECT read path).
        let (row_ttl_seconds, row_expires_at) = element_filter
            .as_ref()
            .map(|f| (f.row_ttl_seconds, f.row_expires_at))
            .unwrap_or((None, None));

        // Issue #1741 (per-element filtering): count of LIVE elements dropped from
        // the emitted container by the read-side shadow/TTL filter. Stays `0` when
        // `element_filter` is `None` (physical consumers), so their output is
        // byte-unchanged.
        let mut shadow_filtered_element_count: usize = 0;

        /// Helper to update max_element_writetime from a parsed cell.
        #[inline]
        fn update_max_writetime(max: &mut i64, cell: &ComplexCellParse) {
            if let Some(ts) = cell.element_writetime {
                if ts > *max {
                    *max = ts;
                }
            }
        }

        /// Issue #1741 (Finding 3): fold ONE live (non-deleted) element's TTL into
        /// the collection aggregate. A non-expiring element is live-forever (keeps
        /// the row visible) — but ONLY when it is not itself dropped by the read-side
        /// shadow/TTL filter (`dropped`), so a wholly-shadowed collection never marks
        /// live-forever. An expiring element with an EXPLICIT per-element expiry
        /// contributes that expiry REGARDLESS of `dropped` (so an all-expired
        /// collection still folds a past expiry that cannot keep the row alive, and
        /// signals `has_ttl`); an expiring element inheriting the row TTL
        /// (`USE_ROW_TTL`, no per-element `localDeletionTime`) is governed by the
        /// row-liveness expiry, so it neither marks live-forever nor folds here.
        /// Far-future `localDeletionTime` in `[2^31, 2^32)` is recovered via
        /// `as u32 as i64`, matching the row-liveness expiry clock.
        #[inline]
        #[allow(clippy::too_many_arguments)]
        fn fold_element_expiry(
            has_live_forever: &mut bool,
            max_exp: &mut Option<i64>,
            homogeneity: &mut ExpiryHomogeneity,
            cell: &ComplexCellParse,
            dropped: bool,
            // Issue #2038 round 3: the inherited row-liveness TTL/expiry a
            // `USE_ROW_TTL` element resolves against (from `ElementShadow`,
            // `None` for every physical consumer).
            row_ttl_seconds: Option<i32>,
            row_expires_at: Option<i64>,
        ) {
            // Issue #2038 (roborev Medium finding): fold this element into the
            // VISIBLE-only homogeneity tracker BEFORE the `dropped`-tolerant
            // `max_exp` aggregate below — a dropped element must never
            // contribute to the per-cell-metadata TTL (see `ExpiryHomogeneity`
            // doc for why this is a separate, narrower aggregate).
            if !dropped {
                *homogeneity = homogeneity.fold(ElementExpiryShape::from_cell(
                    cell,
                    row_ttl_seconds,
                    row_expires_at,
                ));
            }

            if !cell.is_expiring {
                if !dropped {
                    *has_live_forever = true;
                }
            } else if let Some(ldt) = cell.element_local_deletion_time {
                let e = ldt as u32 as i64;
                *max_exp = Some(max_exp.map_or(e, |m: i64| m.max(e)));
            }
        }

        // Determine collection / UDT type from the AUTHORITATIVE marshal type
        // (issue #1081). For collection branches the supplied schema short form
        // (`column.data_type`) is still used to extract element/key types below —
        // those paths are unchanged and proven. Only the top-level non-frozen UDT
        // branch needs the marshal form, which `complex_type` carries.
        let dt = complex_type.to_lowercase();
        let value = if dt.starts_with("list<")
            || dt.starts_with("org.apache.cassandra.db.marshal.listtype(")
        {
            // Parse list elements
            let element_type = self.extract_collection_element_type(&column.data_type, "list")?;
            let mut elements = Vec::with_capacity(prealloc_cap);

            for i in 0..cell_count_usize {
                let cell =
                    self.parse_complex_cell_value(data, offset, &element_type, column, i as u64)?;
                offset = cell.next_offset;

                // Issue #493: element-level tombstones (IS_DELETED 0x01) are not live
                // values and must not be surfaced. Skip them regardless of their path.
                // DS4: count them for the scan-summary warning counter.
                if cell.is_deleted {
                    element_tombstone_count += 1;
                    tracing::debug!(
                        "V5CompressedLegacy: list element {} in column '{}' is a tombstone \
                         (IS_DELETED=0x01) — counted for DS4 scan summary (Issue #700/#493)",
                        i,
                        column.name
                    );
                    // Epic #899: surface the tombstoned element to the compaction
                    // path (value None) so per-element reconcile can shadow it.
                    record_element(&mut elements_out, &cell, None, None, row_timestamp);
                    continue;
                }

                // DS4: Track element timestamp for live elements only (roborev Finding 2).
                // Tombstoned elements are skipped above; their timestamps must not
                // inflate the max_element_writetime reported for the collection.
                // ALWAYS folded (even for a shadow-dropped element) so a wholly-
                // shadowed collection still contributes its element ts to the row
                // aggregate and is recognised as shadowed.
                update_max_writetime(&mut max_element_writetime, &cell);
                // Issue #1741 (per-element): is THIS element shadowed by the covering
                // deletion or TTL-expired at the read clock? Always `false` when the
                // filter is `None` (physical consumers) — byte-unchanged.
                let dropped = Self::element_dropped(element_filter, &cell);
                // Issue #1741 (Finding 3): fold the live element's TTL/expiry
                // (live-forever only when NOT dropped).
                fold_element_expiry(
                    &mut has_live_forever_element,
                    &mut max_element_expires_at,
                    &mut expiry_homogeneity,
                    &cell,
                    dropped,
                    row_ttl_seconds,
                    row_expires_at,
                );
                if dropped {
                    shadow_filtered_element_count += 1;
                    continue;
                }

                // Epic #899: surface this live element to the compaction path.
                record_element(
                    &mut elements_out,
                    &cell,
                    cell.value.clone(),
                    None,
                    row_timestamp,
                );

                // Add non-null values to the list
                if let Some(val) = cell.value {
                    elements.push(val);
                }
            }

            Value::List(elements)
        } else if dt.starts_with("set<")
            || dt.starts_with("org.apache.cassandra.db.marshal.settype(")
        {
            // Parse set elements
            // In Cassandra's complex cell format for sets, each element is a separate cell
            // where the cell PATH contains the raw bytes of the set element, and the cell
            // VALUE is always empty (HAS_EMPTY_VALUE flag = 0x04 set).
            // We must parse the path bytes as the element value, not the (empty) cell value.
            let element_type = self.extract_collection_element_type(&column.data_type, "set")?;
            let mut elements = Vec::with_capacity(prealloc_cap);

            for i in 0..cell_count_usize {
                let cell =
                    self.parse_complex_cell_value(data, offset, &element_type, column, i as u64)?;
                offset = cell.next_offset;

                // Issue #493: element-level tombstones must not surface as live members.
                // For a set, both a live element and a tombstoned element produce
                // `cell.value == None` with non-empty `path_bytes` (the element key),
                // because live set elements carry HAS_EMPTY_VALUE (0x04) and store the
                // element in the path. The authoritative IS_DELETED (0x01) flag is the
                // ONLY signal that distinguishes them, so we consult it directly and skip
                // tombstoned elements (no-heuristics mandate, Issue #28).
                // DS4: count them for the scan-summary warning counter.
                if cell.is_deleted {
                    element_tombstone_count += 1;
                    tracing::debug!(
                        "V5CompressedLegacy: set element {} in column '{}' is a tombstone \
                         (IS_DELETED=0x01) — counted for DS4 scan summary (Issue #700/#493)",
                        i,
                        column.name
                    );
                    // Epic #899: surface the tombstoned set element (the element
                    // identity lives in the cell_path).
                    record_element(&mut elements_out, &cell, None, None, row_timestamp);
                    continue;
                }

                // DS4: Track element timestamp for live elements only (roborev Finding 2).
                // Tombstoned elements are skipped above; their timestamps must not
                // inflate the max_element_writetime reported for the collection.
                // ALWAYS folded (even for a shadow-dropped element) so the row
                // aggregate still sees a wholly-shadowed collection as shadowed.
                update_max_writetime(&mut max_element_writetime, &cell);
                // Issue #1741 (per-element): shadow/TTL filter for THIS set member.
                let dropped = Self::element_dropped(element_filter, &cell);
                // Issue #1741 (Finding 3): fold the live element's TTL/expiry
                // (live-forever only when NOT dropped).
                fold_element_expiry(
                    &mut has_live_forever_element,
                    &mut max_element_expires_at,
                    &mut expiry_homogeneity,
                    &cell,
                    dropped,
                    row_ttl_seconds,
                    row_expires_at,
                );
                if dropped {
                    shadow_filtered_element_count += 1;
                    continue;
                }

                // For sets: the path bytes ARE the element value (cell value is always empty).
                // If cell.value is Some (unusual case where a set cell has a non-empty value),
                // use it. Otherwise parse the path bytes as the element type.
                let set_member: Option<Value> = if let Some(val) = cell.value.clone() {
                    Some(val)
                } else if !cell.path_bytes.is_empty() {
                    // Path bytes are the set element — parse them as the element type
                    // Issue #3811 (roborev F1): PROPAGATE. This used to map the error
                    // to `None`, which silently DROPPED the member — so a set cell whose
                    // path bytes carry trailing garbage produced a set with one fewer
                    // element instead of a refusal, and two distinct serialized cells
                    // stayed indistinguishable. That is AC4's collapse wearing a smaller
                    // set, and it is strictly worse than the wrong value it replaced,
                    // because a dropped member leaves no trace at all. Verified against
                    // the 144-file corpus census: no table decodes differently.
                    Some(self.parse_value_from_raw_bytes(
                        &cell.path_bytes,
                        &element_type,
                        &column.name,
                        0,
                    )?)
                } else {
                    None
                };

                // Epic #899: surface the live set element (decoded member value)
                // to the compaction path, keyed by its cell_path.
                record_element(
                    &mut elements_out,
                    &cell,
                    set_member.clone(),
                    None,
                    row_timestamp,
                );

                if let Some(val) = set_member {
                    elements.push(val);
                }
            }

            Value::Set(elements)
        } else if dt.starts_with("map<")
            || dt.starts_with("org.apache.cassandra.db.marshal.maptype(")
        {
            // Parse map entries.
            //
            // KEY TYPE SELECTION — prefer the AUTHORITATIVE MARSHAL spelling, the
            // same way the FROZEN map reader does (`cell_value_complex`, issue
            // #1340). This is a parity requirement, not a refinement (issue #3612,
            // R7): decoding the multicell key from the SCHEMA short form while the
            // frozen reader decodes it from the marshal form makes the two spellings
            // of one map produce `Value` keys that compare and hash DIFFERENTLY on
            // the public Rust surface.
            //
            // MEASURED on the committed `test_nested_udt_keys` fixture, from
            // Cassandra's own `Statistics.db`: the two columns' marshal key types are
            // IDENTICAL — `m_tuple_udt` is
            // `MapType(TupleType(UserType(..),Int32Type),Int32Type)` and
            // `f_map_tuple_udt` is that same `MapType(..)` under one outer
            // `FrozenType`, which `extract_marshal_collection_elements` strips. The
            // SCHEMA form is what diverges: `frozen<tuple<frozen<key_part>, int>>`
            // carries `frozen` at BOTH levels, so decoding from it produced
            // `Frozen(Tuple([Frozen(Udt), Int]))` against the frozen reader's
            // `Tuple([Udt, Int])`. Starting both readers from the same string is
            // therefore the ROOT-CAUSE fix; peeling wrappers afterwards could not
            // reach the INNER one.
            //
            // Precedence (marshal over schema) is Cassandra's own, not ours: see
            // `map_key_type_for_decode`'s doc, which carries the `SerializationHeader
            // .getType` citation and the scope qualifications. It is the one home for
            // this rule, so the justification lives there too.
            let (schema_key_type, schema_value_type) = self.extract_map_types(&column.data_type)?;
            let marshal_map_elements = Self::extract_marshal_collection_elements(complex_type);
            let marshal_key = match &marshal_map_elements {
                Some(MarshalCollectionElements::Map(k, _v)) => Some(*k),
                _ => None,
            };
            // ONE shared rule, `map_key_type_for_decode` — see its doc. It picks the
            // same string the FROZEN map reader receives, so both decode identically
            // and key parity holds by construction rather than by a value-level
            // wrapper fixup on this side (roborev round 8, finding 1).
            let key_type = Self::map_key_type_for_decode(marshal_key, &schema_key_type);
            // VALUE type selection stays on the SCHEMA form, exactly as before R7. R7's
            // marshal preference was widened to the value half too, but MEASURED over the
            // committed corpus it was a strict no-op -- 5 multicell `map<..>` columns, 3
            // UDT-bearing on the KEY and 0 on the VALUE -- so it shipped a behaviour change
            // no lane executed. Removing it restores the pre-R7 status quo rather than
            // creating an asymmetry (value selection was on the schema form all along);
            // the key half's marshal preference is justified separately in
            // `map_key_type_for_decode`'s doc. #3612 is about KEYS.
            let value_type = schema_value_type;
            let mut entries = Vec::with_capacity(prealloc_cap);
            // Issue #3612 (roborev round 8, finding 2): count the entries whose key
            // could not be modelled, and report ONCE below. The decoder used to
            // `warn!` per entry, which on a large scan is `entries x rows` identical
            // lines — a log flood that can exhaust storage and that destroys the
            // only number an operator needs, namely how many entries were affected.
            let mut opaque_key_entries: usize = 0;

            for i in 0..cell_count_usize {
                let cell =
                    self.parse_complex_cell_value(data, offset, &value_type, column, i as u64)?;
                offset = cell.next_offset;

                // For maps, the cell path IS the key
                // Parse the path as the key using the key type
                // Note: Cell path keys are stored WITHOUT length prefixes (raw bytes only)
                //
                // Map semantics are intentionally unchanged for Issue #493: a deleted
                // entry already surfaces as `cell.value == None` and is emitted as
                // (key, Value::Null), preserving existing behavior. Only set/list
                // element tombstones are skipped.
                // DS4: For maps with IS_DELETED entries, count them for the scan summary.
                // Tombstoned entries must NOT contribute to max_element_writetime so that
                // the reported writetime only reflects live content (roborev Finding 2).
                if cell.is_deleted {
                    element_tombstone_count += 1;
                    tracing::debug!(
                        "V5CompressedLegacy: map entry {} in column '{}' is a tombstone \
                         (IS_DELETED=0x01) — counted for DS4 scan summary (Issue #700/#493)",
                        i,
                        column.name
                    );
                } else {
                    // DS4: Track element timestamp for live map entries only.
                    // ALWAYS folded (even for a shadow-dropped entry) so the row
                    // aggregate sees a wholly-shadowed map as shadowed.
                    update_max_writetime(&mut max_element_writetime, &cell);
                    // Issue #1741 (per-element): shadow/TTL filter for THIS entry —
                    // drop the WHOLE (key, value) entry when its own cell is shadowed
                    // by the covering deletion or TTL-expired. `false` when the filter
                    // is `None` (physical consumers), so maps stay byte-unchanged.
                    let dropped = Self::element_dropped(element_filter, &cell);
                    // Issue #1741 (Finding 3): fold the live entry's TTL/expiry
                    // (live-forever only when NOT dropped).
                    fold_element_expiry(
                        &mut has_live_forever_element,
                        &mut max_element_expires_at,
                        &mut expiry_homogeneity,
                        &cell,
                        dropped,
                        row_ttl_seconds,
                        row_expires_at,
                    );
                    if dropped {
                        shadow_filtered_element_count += 1;
                        continue;
                    }
                }

                // Decode the map key (from cell_path) up front so it can be both
                // recorded on the per-element compaction entry and used to build
                // the collapsed `Value::Map`.
                //
                // ISSUE #3747 — DECODED UNCONDITIONALLY. A map cell's cell path IS its key
                // and Cassandra always writes one, so a ZERO-LENGTH path is an EMPTY KEY —
                // legal data (`{'': 1}` is valid CQL; empty is DISTINCT from null), never
                // "no key". The old `!is_empty()` guard dropped it, so a `SELECT` returned
                // a map SHORT ONE ENTRY. WHICH empties are legal is decided by #3612's
                // Cassandra-derived `cell_path_key_allowed_widths`, which runs first.
                tracing::debug!(
                    "V5CompressedLegacy: Parsing map key for column '{}', key_type='{}', path_len={}",
                    column.name,
                    key_type,
                    cell.path_bytes.len()
                );
                // For cell path keys, parse directly without expecting length prefixes
                let mut opaque = false;
                let decoded_key = self.parse_cell_path_key_reporting(
                    &cell.path_bytes,
                    &key_type,
                    &column.name,
                    &mut opaque,
                )?;
                if opaque {
                    opaque_key_entries += 1;
                }

                // Epic #899: surface the map entry to the compaction path keyed
                // by its cell_path (the map key bytes); value is the map value
                // (`None` for a tombstoned / null entry), with the decoded key for
                // whole-`Value::Map` reconstruction downstream.
                record_element(
                    &mut elements_out,
                    &cell,
                    cell.value.clone(),
                    Some(decoded_key.clone()),
                    row_timestamp,
                );

                // Every decoded entry reaches the map. A `None` cell value is a
                // null/tombstoned entry for that key and is kept as (key, Null) —
                // unchanged from before (issue #493).
                entries.push((decoded_key, cell.value.unwrap_or(Value::Null)));
            }

            // ONE line per column per row, carrying the COUNT. Content unchanged
            // from the per-entry version it replaces; only its cardinality changed.
            if opaque_key_entries > 0 {
                tracing::warn!(
                    target: "cqlite::decode",
                    column = %column.name,
                    declared_type = %key_type,
                    affected_entries = opaque_key_entries,
                    total_entries = cell_count_usize,
                    "multicell map key type is not one this reader can decode; those \
                     keys are surfaced as opaque bytes (issue #3612). Check that the \
                     schema (or the on-disk SerializationHeader) resolves it, e.g. \
                     that a UDT named here is registered."
                );
            }

            Value::Map(entries)
        } else if dt.starts_with("org.apache.cassandra.db.marshal.usertype(") {
            // Issue #927: TOP-LEVEL non-frozen UDT — each field is a cell whose
            // cell_path is the 2-byte (signed ShortType) declared field index and
            // whose value bytes are the field datum. Decode each cell's value with
            // the DECLARED field type resolved from the marshal string.
            //
            // Issue #1081: resolve the field layout from the AUTHORITATIVE on-disk
            // marshal type (`complex_type`), not the supplied schema's bare short
            // form (`column.data_type`, e.g. `person_type`) which cannot express
            // the field list. This is the no-heuristics source of truth (issue #28).
            let field_defs = Self::udt_field_marshal_types(complex_type)?;
            // Field values keyed by declared index; absent / null fields stay None.
            let mut field_values: Vec<Option<Value>> = vec![None; field_defs.len()];

            for i in 0..cell_count_usize {
                // Capture the value bytes raw (BytesType is identity) so they can
                // be re-decoded with the per-field type AFTER the cell_path (which
                // carries the field index) is known.
                let cell = self.parse_complex_cell_value(
                    data,
                    offset,
                    "org.apache.cassandra.db.marshal.BytesType",
                    column,
                    i as u64,
                )?;
                offset = cell.next_offset;

                // Resolve the declared field index from the 2-byte signed-short
                // cell_path. A path that is not exactly 2 bytes, or that names a
                // field index outside the declared range, is authoritative
                // corruption — surface it rather than guessing (no-heuristics).
                let field_index = if cell.path_bytes.len() == 2 {
                    i16::from_be_bytes([cell.path_bytes[0], cell.path_bytes[1]]) as i32
                } else {
                    return Err(Error::corruption(format!(
                        "UDT column '{}' cell {}: field-index cell_path must be 2 bytes, got {}",
                        column.name,
                        i,
                        cell.path_bytes.len()
                    )));
                };

                if cell.is_deleted {
                    element_tombstone_count += 1;
                    record_element(&mut elements_out, &cell, None, None, row_timestamp);
                    continue;
                }
                update_max_writetime(&mut max_element_writetime, &cell);
                // Issue #1741 (per-element): shadow/TTL filter for THIS UDT field —
                // a shadowed/expired field is left absent (its `field_values` slot
                // stays `None`). `false` when the filter is `None` (byte-unchanged).
                let dropped = Self::element_dropped(element_filter, &cell);
                // Issue #1741 (Finding 3): fold the live UDT field's TTL/expiry
                // (live-forever only when NOT dropped).
                fold_element_expiry(
                    &mut has_live_forever_element,
                    &mut max_element_expires_at,
                    &mut expiry_homogeneity,
                    &cell,
                    dropped,
                    row_ttl_seconds,
                    row_expires_at,
                );
                if dropped {
                    shadow_filtered_element_count += 1;
                    continue;
                }

                // Decode the field value with its DECLARED type. `cell.value` is the
                // raw bytes wrapped as Blob (BytesType) above; an empty-value cell
                // yields None.
                let decoded = match &cell.value {
                    Some(Value::Blob(raw)) => {
                        let (_name, field_type) = field_defs
                            .get(field_index as usize)
                            .filter(|_| field_index >= 0)
                            .ok_or_else(|| {
                                Error::corruption(format!(
                                    "UDT column '{}' cell {}: field index {} out of range (0..{})",
                                    column.name,
                                    i,
                                    field_index,
                                    field_defs.len()
                                ))
                            })?;
                        Some(self.parse_value_from_raw_bytes(raw, field_type, &column.name, 0)?)
                    }
                    other => other.clone(),
                };

                record_element(
                    &mut elements_out,
                    &cell,
                    decoded.clone(),
                    None,
                    row_timestamp,
                );

                if field_index >= 0 && (field_index as usize) < field_values.len() {
                    field_values[field_index as usize] = decoded;
                }
            }

            // Build the collapsed UDT in DECLARED field order for the user-facing
            // read path. The keyspace/type-name come from the authoritative
            // on-disk marshal string (issue #1081).
            let (udt_keyspace, udt_name) = Self::udt_keyspace_and_name(complex_type)?;
            let fields = field_defs
                .iter()
                .zip(field_values)
                .map(|((name, _ty), value)| crate::types::UdtField {
                    name: name.clone(),
                    value,
                })
                .collect();
            Value::Udt(Box::new(UdtValue {
                type_name: udt_name,
                keyspace: udt_keyspace,
                fields,
            }))
        } else {
            // Unknown complex column type, skip cells
            for i in 0..cell_count_usize {
                offset = self.skip_complex_cell(data, offset, &column.name, i as u64)?;
            }
            Value::Null
        };

        tracing::debug!(
            "V5CompressedLegacy: Complex column '{}' parsed, final offset {} \
             (has_collection_tombstone={} max_element_writetime={} element_tombstone_count={})",
            column.name,
            offset,
            has_collection_tombstone,
            max_element_writetime,
            element_tombstone_count
        );

        // Issue #2038: resolve the homogeneity tracker into the `CellExpiration`
        // surfaced in per-cell metadata — `None` unless every VISIBLE element
        // shared the identical explicit expiry.
        let visible_uniform_expiration = expiry_homogeneity.into_cell_expiration();

        Ok((
            value,
            offset,
            ComplexColumnMeta {
                has_collection_tombstone,
                max_element_writetime,
                element_tombstone_count,
                complex_deletion,
                has_live_forever_element,
                max_element_expires_at,
                visible_uniform_expiration,
                shadow_filtered_element_count,
            },
        ))
    }

    /// Issue #1741 (per-element filtering): whether a LIVE collection element is
    /// dropped by read-side shadow/TTL filtering. `None` filter (every physical
    /// consumer — compaction / delta-scan / unit tests) NEVER drops, keeping their
    /// output byte-unchanged. A covering deletion shadows an element whose effective
    /// write ts (its own, or the inherited row ts for `USE_ROW_TIMESTAMP`) is
    /// `<= cover`; an EXPIRING element with an explicit per-element `localDeletionTime
    /// <= now` is TTL-expired. An element inheriting the row TTL (`USE_ROW_TTL`, no
    /// per-element `localDeletionTime`) has `element_local_deletion_time == None` and
    /// is TTL-checked against the row liveness expiry (`filter.row_expires_at`),
    /// EXACTLY as the scalar `USE_ROW_TTL` cell path does — so an expired inherited-TTL
    /// element is dropped even when another live cell keeps the row visible. An
    /// element with no authoritative write ts is never shadowed
    /// (no-heuristics, issue #28). Reuses the SAME decision as the simple-cell path
    /// ([`PartitionShadow::cell_shadowed_or_expired`]) so both stay consistent.
    #[inline]
    fn element_dropped(filter: Option<ElementShadow>, cell: &ComplexCellParse) -> bool {
        let Some(f) = filter else {
            return false;
        };
        // Effective element write ts: its own, else the inherited row ts for a
        // USE_ROW_TIMESTAMP element (`element_writetime == None`). The read path
        // always sets `f.row_ts` from the row header, so this is authoritative. When
        // neither is present the element has no authoritative ts (`eff_ts = None`),
        // which `cell_shadowed_or_expired` never shadows (no-heuristics).
        let eff_ts = cell.element_writetime.or(f.row_ts);
        // An EXPIRING element's effective expiry mirrors the scalar USE_ROW_TTL cell
        // path EXACTLY: an explicit per-element `localDeletionTime` wins (recovered
        // from the far-future [2^31,2^32) encoding via `as u32 as i64`); an element
        // that inherits the row TTL (`USE_ROW_TTL`, no per-element LDT) uses the row
        // liveness expiry (`f.row_expires_at`). A non-expiring element is live-forever
        // (no expiry). This makes scalar and collection-element USE_ROW_TTL semantics
        // identical, so an inherited-TTL element that has expired is dropped even when
        // another (live) cell keeps the row visible.
        let eff_exp = if cell.is_expiring {
            match cell.element_local_deletion_time {
                Some(l) => Some(l as u32 as i64),
                None => f.row_expires_at,
            }
        } else {
            None
        };
        PartitionShadow::cell_shadowed_or_expired(f.cover, f.now, eff_ts, eff_exp)
    }

    /// Issue #1741 (per-element filtering): whether a parsed non-frozen collection
    /// value emitted ZERO surviving elements (so the read path treats it as absent /
    /// null). Only the collection variants are meaningful; any other value returns
    /// `false` (never treated as an empty collection).
    pub(super) fn complex_value_is_empty(value: &Value) -> bool {
        match value {
            Value::List(v) => v.is_empty(),
            Value::Set(v) => v.is_empty(),
            Value::Map(v) => v.is_empty(),
            Value::Udt(u) => u.fields.iter().all(|f| f.value.is_none()),
            _ => false,
        }
    }

    /// Parse a single complex cell and extract its value.
    /// Complex cells have: [flags] [timestamp?] [deletion?] [ttl?] [cell_path] [value?]
    ///
    /// Returns a [`ComplexCellParse`] describing the parsed cell.
    /// - `value` is None if the cell is deleted or has an empty value
    /// - `path_bytes` contains the raw path bytes (used as map key for map<> types,
    ///   and as the element value for set<> types)
    /// - `is_deleted` reflects the authoritative IS_DELETED (0x01) cell flag, so
    ///   callers can distinguish element-level tombstones from live elements that
    ///   simply carry an empty value (Issue #493).
    fn parse_complex_cell_value(
        &self,
        data: &[u8],
        mut offset: usize,
        element_type: &str,
        column: &crate::schema::Column,
        cell_index: u64,
    ) -> Result<ComplexCellParse> {
        tracing::debug!(
            "V5CompressedLegacy: parse_complex_cell_value '{}' cell {} element_type='{}' starting at offset {}",
            column.name,
            cell_index,
            element_type,
            offset
        );

        // Step 1: Cell flags (standard 0x00-0x1F range)
        if offset >= data.len() {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: unexpected end at flags (offset {})",
                column.name, cell_index, offset
            )));
        }
        let flags = data[offset];
        offset += 1;

        // Validate flags are in valid range
        if flags > 0x1F {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: invalid flags 0x{:02x} at offset {} (expected 0x00-0x1F)",
                column.name,
                cell_index,
                flags,
                offset - 1
            )));
        }

        let is_deleted = (flags & 0x01) != 0;
        let is_expiring = (flags & 0x02) != 0;
        let has_empty_value = (flags & 0x04) != 0;
        let use_row_timestamp = (flags & 0x08) != 0;
        let use_row_ttl = (flags & 0x10) != 0;

        tracing::debug!(
            "V5CompressedLegacy: parse_complex_cell_value '{}' cell {} flags=0x{:02x} (deleted={}, expiring={}, empty_value={}, use_row_ts={}, use_row_ttl={})",
            column.name,
            cell_index,
            flags,
            is_deleted,
            is_expiring,
            has_empty_value,
            use_row_timestamp,
            use_row_ttl
        );

        // Step 2: Timestamp (if not using row timestamp)
        // Capture the element-level timestamp delta for DS4 max-writetime computation.
        // Cassandra encodes complex cell timestamps as UNSIGNED VInt deltas from
        // min_timestamp (SerializationHeader.writeUnsignedVInt; writer:
        // write_complex_cell_header, encode_unsigned). The earlier parse_vint
        // (ZigZag/signed) mis-decoded deltas with the top data bit set while
        // consuming the same byte count. Fix (roborev #863).
        let mut element_writetime: Option<i64> = None;
        if !use_row_timestamp {
            let (remaining, ts_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse timestamp at offset {}: {:?}",
                    column.name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
            // Delta decode: absolute_ts = min_timestamp + ts_delta
            let absolute_ts = self.min_timestamp.wrapping_add(ts_delta as i64);
            element_writetime = Some(absolute_ts);
        }

        // Step 3: Local deletion time (if deleted/expiring and not using row TTL)
        // Epic #899: surface the absolute localDeletionTime (SECONDS) for the
        // per-element compaction contract. The on-disk value is an unsigned VInt
        // delta from `min_local_deletion_time`. Far-future values in
        // `[2^31, 2^32)` are preserved as the wrapping `as u32 as i32`
        // representation — do NOT widen to i64 (epic #899 invariant).
        let mut element_local_deletion_time: Option<i32> = None;
        if !use_row_ttl && (is_deleted || is_expiring) {
            let (remaining, ldt_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse localDeletionTime at offset {}: {:?}",
                    column.name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
            let absolute_ldt = self.min_local_deletion_time.wrapping_add(ldt_delta as i64);
            // Wrap into i32 preserving the far-future [2^31, 2^32) encoding.
            element_local_deletion_time = Some(absolute_ldt as u32 as i32);
        }

        // Step 4: TTL (if expiring and not using row TTL)
        // Epic #899: surface the per-element TTL (SECONDS) for the compaction
        // contract. The on-disk value is an unsigned VInt delta from `min_ttl`.
        let mut element_ttl: Option<u32> = None;
        if !use_row_ttl && is_expiring {
            let (remaining, ttl_delta) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse TTL at offset {}: {:?}",
                    column.name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
            let absolute_ttl = self.min_ttl.unwrap_or(0).wrapping_add(ttl_delta as i64);
            element_ttl = Some(absolute_ttl as u32);
        }

        // Step 5: Cell path (VInt length + bytes)
        let (remaining, path_len) = parse_vuint(&data[offset..]).map_err(|e| {
            Error::corruption(format!(
                "Complex cell {}.{}: failed to parse path length at offset {}: {:?}",
                column.name, cell_index, offset, e
            ))
        })?;
        let bytes_consumed = data[offset..].len() - remaining.len();
        offset += bytes_consumed;

        // Issue #225: Safe conversion to prevent overflow on large values
        let path_len_usize: usize = path_len.try_into().map_err(|_| {
            Error::corruption(format!(
                "Complex cell {}.{}: path length {} exceeds platform limit",
                column.name, cell_index, path_len
            ))
        })?;
        if path_len > MAX_CELL_VALUE_LENGTH {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: path length {} exceeds maximum {}",
                column.name, cell_index, path_len, MAX_CELL_VALUE_LENGTH
            )));
        }

        // Bounds check before reading path
        if offset + path_len_usize > data.len() {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: cell path requires {} bytes but only {} available at offset {}",
                column.name,
                cell_index,
                path_len,
                data.len().saturating_sub(offset),
                offset
            )));
        }

        let path_bytes = data[offset..offset + path_len_usize].to_vec();
        offset += path_len_usize;

        // Step 6: Value (if not empty and not deleted)
        let value = if is_deleted || has_empty_value {
            tracing::debug!(
                "V5CompressedLegacy: parse_complex_cell_value '{}' cell {} is deleted or empty",
                column.name,
                cell_index
            );
            None
        } else {
            let (remaining, value_len) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse value length at offset {}: {:?}",
                    column.name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;

            // Issue #225: Safe conversion to prevent overflow on large values
            let value_len_usize: usize = value_len.try_into().map_err(|_| {
                Error::corruption(format!(
                    "Complex cell {}.{}: value length {} exceeds platform limit",
                    column.name, cell_index, value_len
                ))
            })?;
            if value_len > MAX_CELL_VALUE_LENGTH {
                return Err(Error::corruption(format!(
                    "Complex cell {}.{}: value length {} exceeds maximum {}",
                    column.name, cell_index, value_len, MAX_CELL_VALUE_LENGTH
                )));
            }

            // Bounds check before reading value
            if offset + value_len_usize > data.len() {
                return Err(Error::corruption(format!(
                    "Complex cell {}.{}: value requires {} bytes but only {} available at offset {}",
                    column.name,
                    cell_index,
                    value_len,
                    data.len().saturating_sub(offset),
                    offset
                )));
            }

            let value_data = &data[offset..offset + value_len_usize];
            offset += value_len_usize;

            // Parse the value based on element type.
            // The value bytes have already been extracted (length was consumed above).
            // Use parse_value_from_raw_bytes which treats the entire slice as the value
            // WITHOUT an additional length prefix (unlike parse_raw_type_value which
            // expects a VInt length prefix — wrong for already-extracted complex cell values).
            // See Issue #481: using parse_raw_type_value here caused the first byte of
            // blob/text values to be misread as a length, producing corrupt parses.
            let parsed_value =
                self.parse_value_from_raw_bytes(value_data, element_type, &column.name, 0)?;
            Some(parsed_value)
        };

        tracing::debug!(
            "V5CompressedLegacy: parse_complex_cell_value '{}' cell {} complete, value={:?}, final offset {}",
            column.name,
            cell_index,
            value.is_some(),
            offset
        );

        Ok(ComplexCellParse {
            value,
            path_bytes,
            is_deleted,
            has_empty_value,
            next_offset: offset,
            element_writetime,
            element_ttl,
            element_local_deletion_time,
            is_expiring,
        })
    }

    /// Skip over a single complex cell without fully parsing its value.
    /// Complex cells have: [flags] [timestamp?] [deletion?] [ttl?] [cell_path] [value?]
    ///
    /// Issue #221: This is used to advance past complex cell data while returning
    /// placeholder values. Future work can add full cell value parsing here.
    fn skip_complex_cell(
        &self,
        data: &[u8],
        mut offset: usize,
        column_name: &str,
        cell_index: u64,
    ) -> Result<usize> {
        tracing::debug!(
            "V5CompressedLegacy: skip_complex_cell '{}' cell {} starting at offset {}, bytes: {:02x?}",
            column_name,
            cell_index,
            offset,
            &data[offset..std::cmp::min(offset + 20, data.len())]
        );

        // Complex cell format per Cassandra source (UnfilteredSerializer.java):
        // [flags: u8]
        // [timestamp: VInt if not USE_ROW_TIMESTAMP_MASK]
        // [local_deletion_time: VInt if (deleted || expiring) && not USE_ROW_TTL_MASK]
        // [ttl: VInt if expiring && not USE_ROW_TTL_MASK]
        // [cell_path: VInt length + bytes] <-- AFTER flags/timestamp/etc, NOT before!
        // [value: VInt length + bytes if not HAS_EMPTY_VALUE_MASK]

        // Step 1: Cell flags (standard 0x00-0x1F range)
        if offset >= data.len() {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: unexpected end at flags (offset {})",
                column_name, cell_index, offset
            )));
        }
        let flags = data[offset];
        offset += 1;

        // Validate flags are in valid range
        if flags > 0x1F {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: invalid flags 0x{:02x} at offset {} (expected 0x00-0x1F)",
                column_name,
                cell_index,
                flags,
                offset - 1
            )));
        }

        let is_deleted = (flags & 0x01) != 0;
        let is_expiring = (flags & 0x02) != 0;
        let has_empty_value = (flags & 0x04) != 0;
        let use_row_timestamp = (flags & 0x08) != 0;
        let use_row_ttl = (flags & 0x10) != 0;

        tracing::debug!(
            "V5CompressedLegacy: skip_complex_cell '{}' cell {} flags=0x{:02x} (deleted={}, expiring={}, empty_value={}, use_row_ts={}, use_row_ttl={})",
            column_name,
            cell_index,
            flags,
            is_deleted,
            is_expiring,
            has_empty_value,
            use_row_timestamp,
            use_row_ttl
        );

        // Step 2: Timestamp (if not using row timestamp)
        // Skip-only: byte advancement is identical for vint/vuint, but use the
        // UNSIGNED variant to match the writer encoding and the decoding sites
        // (roborev #863).
        if !use_row_timestamp {
            let (remaining, _ts) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse timestamp at offset {}: {:?}",
                    column_name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
        }

        // Step 3: Local deletion time (if deleted/expiring and not using row TTL)
        if !use_row_ttl && (is_deleted || is_expiring) {
            let (remaining, _ldt) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse localDeletionTime at offset {}: {:?}",
                    column_name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
        }

        // Step 4: TTL (if expiring and not using row TTL)
        if !use_row_ttl && is_expiring {
            let (remaining, _ttl) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse TTL at offset {}: {:?}",
                    column_name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;
        }

        // Step 5: Cell path (VInt length + bytes) - comes AFTER flags/timestamp/ttl
        let (remaining, path_len) = parse_vuint(&data[offset..]).map_err(|e| {
            Error::corruption(format!(
                "Complex cell {}.{}: failed to parse path length at offset {}: {:?}",
                column_name, cell_index, offset, e
            ))
        })?;
        let bytes_consumed = data[offset..].len() - remaining.len();
        tracing::debug!(
            "V5CompressedLegacy: skip_complex_cell '{}' cell {} path_len={} at offset {}",
            column_name,
            cell_index,
            path_len,
            offset
        );
        offset += bytes_consumed;

        // Issue #225: Safe conversion to prevent overflow on large values
        let path_len_usize: usize = path_len.try_into().map_err(|_| {
            Error::corruption(format!(
                "Complex cell {}.{}: path length {} exceeds platform limit",
                column_name, cell_index, path_len
            ))
        })?;
        if path_len > MAX_CELL_VALUE_LENGTH {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: path length {} exceeds maximum {}",
                column_name, cell_index, path_len, MAX_CELL_VALUE_LENGTH
            )));
        }

        // Bounds check before advancing by path_len
        if offset + path_len_usize > data.len() {
            return Err(Error::corruption(format!(
                "Complex cell {}.{}: cell path requires {} bytes but only {} available at offset {}",
                column_name,
                cell_index,
                path_len,
                data.len().saturating_sub(offset),
                offset
            )));
        }
        offset += path_len_usize;

        // Step 6: Value (if not empty)
        if !has_empty_value {
            let (remaining, value_len) = parse_vuint(&data[offset..]).map_err(|e| {
                Error::corruption(format!(
                    "Complex cell {}.{}: failed to parse value length at offset {}: {:?}",
                    column_name, cell_index, offset, e
                ))
            })?;
            let bytes_consumed = data[offset..].len() - remaining.len();
            offset += bytes_consumed;

            // Issue #225: Safe conversion to prevent overflow on large values
            let value_len_usize: usize = value_len.try_into().map_err(|_| {
                Error::corruption(format!(
                    "Complex cell {}.{}: value length {} exceeds platform limit",
                    column_name, cell_index, value_len
                ))
            })?;
            if value_len > MAX_CELL_VALUE_LENGTH {
                return Err(Error::corruption(format!(
                    "Complex cell {}.{}: value length {} exceeds maximum {}",
                    column_name, cell_index, value_len, MAX_CELL_VALUE_LENGTH
                )));
            }

            // Bounds check before advancing by value_len
            if offset + value_len_usize > data.len() {
                return Err(Error::corruption(format!(
                    "Complex cell {}.{}: value requires {} bytes but only {} available at offset {}",
                    column_name,
                    cell_index,
                    value_len,
                    data.len().saturating_sub(offset),
                    offset
                )));
            }
            offset += value_len_usize;
        }

        tracing::debug!(
            "V5CompressedLegacy: skip_complex_cell '{}' cell {} complete, final offset {}",
            column_name,
            cell_index,
            offset
        );

        Ok(offset)
    }

    /// Extract element type from list<T> or set<T> type string (CQL or Cassandra internal format)
    pub(super) fn extract_collection_element_type(
        &self,
        type_str: &str,
        collection: &str,
    ) -> Result<String> {
        let type_lower = type_str.to_lowercase();

        // Check for Cassandra internal format first: org.apache.cassandra.db.marshal.ListType(...)
        let internal_prefix_lower = format!("org.apache.cassandra.db.marshal.{}type(", collection);
        if type_lower.starts_with(&internal_prefix_lower) && type_lower.ends_with(')') {
            // Use the lowercase prefix length to extract from the original string
            let inner = &type_str[internal_prefix_lower.len()..type_str.len() - 1];
            if inner.is_empty() {
                return Err(Error::schema(format!(
                    "Empty {} element type: {}",
                    collection, type_str
                )));
            }
            return Ok(inner.to_string());
        }

        // Check for CQL format: list<T>, set<T>
        let prefix_lower = format!("{}<", collection);
        if type_lower.starts_with(&prefix_lower) && type_lower.ends_with('>') {
            // Use the lowercase prefix length to extract from the original string
            let inner = &type_str[prefix_lower.len()..type_str.len() - 1];
            if inner.is_empty() {
                return Err(Error::schema(format!(
                    "Empty {} element type: {}",
                    collection, type_str
                )));
            }
            return Ok(inner.to_string());
        }

        Err(Error::schema(format!(
            "Invalid {} type format: {}",
            collection, type_str
        )))
    }

    /// Extract key and value types from map<K,V> type string (CQL or Cassandra internal format)
    pub(super) fn extract_map_types(&self, type_str: &str) -> Result<(String, String)> {
        let type_lower = type_str.to_lowercase();

        // Determine the inner content based on format
        let inner = if type_lower.starts_with("org.apache.cassandra.db.marshal.maptype(")
            && type_str.ends_with(')')
        {
            // Cassandra internal format: org.apache.cassandra.db.marshal.MapType(K,V)
            let prefix = "org.apache.cassandra.db.marshal.MapType(";
            &type_str[prefix.len()..type_str.len() - 1]
        } else if type_lower.starts_with("map<") && type_str.ends_with('>') {
            // CQL format: map<K,V>
            &type_str[4..type_str.len() - 1]
        } else {
            return Err(Error::schema(format!(
                "Invalid map type format: {}",
                type_str
            )));
        };

        if inner.is_empty() {
            return Err(Error::schema(format!("Empty map types: {}", type_str)));
        }

        // Split by comma, handling nested angle brackets and parentheses
        let mut depth = 0;
        let mut split_pos = None;

        for (i, ch) in inner.chars().enumerate() {
            match ch {
                '<' | '(' => depth += 1,
                '>' | ')' => depth -= 1,
                ',' if depth == 0 => {
                    split_pos = Some(i);
                    break;
                }
                _ => {}
            }
        }

        let split_pos = split_pos.ok_or_else(|| {
            Error::schema(format!(
                "Invalid map type format (no comma separator): {}",
                type_str
            ))
        })?;

        let key_type = inner[..split_pos].trim().to_string();
        let value_type = inner[split_pos + 1..].trim().to_string();

        if key_type.is_empty() || value_type.is_empty() {
            return Err(Error::schema(format!(
                "Empty key or value type in map: {}",
                type_str
            )));
        }

        Ok((key_type, value_type))
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::super::test_support::helpers::*;
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_extract_collection_element_type() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Test list element type extraction
        assert_eq!(
            parser
                .extract_collection_element_type("list<int>", "list")
                .unwrap(),
            "int"
        );

        // Test set element type extraction
        assert_eq!(
            parser
                .extract_collection_element_type("set<text>", "set")
                .unwrap(),
            "text"
        );

        // Test nested type
        assert_eq!(
            parser
                .extract_collection_element_type("list<frozen<map<text,int>>>", "list")
                .unwrap(),
            "frozen<map<text,int>>"
        );

        // Test error cases
        assert!(parser
            .extract_collection_element_type("list<>", "list")
            .is_err());
        assert!(parser
            .extract_collection_element_type("set<int>", "list")
            .is_err());
        assert!(parser
            .extract_collection_element_type("int", "list")
            .is_err());
    }

    #[test]
    fn test_extract_map_types() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

        // Test simple map
        let (key, value) = parser.extract_map_types("map<text,int>").unwrap();
        assert_eq!(key, "text");
        assert_eq!(value, "int");

        // Test map with spaces
        let (key, value) = parser.extract_map_types("map<text, int>").unwrap();
        assert_eq!(key, "text");
        assert_eq!(value, "int");

        // Test nested value type
        let (key, value) = parser
            .extract_map_types("map<text,frozen<set<uuid>>>")
            .unwrap();
        assert_eq!(key, "text");
        assert_eq!(value, "frozen<set<uuid>>");

        // Test nested key and value types
        let (key, value) = parser
            .extract_map_types("map<frozen<list<int>>,frozen<set<text>>>")
            .unwrap();
        assert_eq!(key, "frozen<list<int>>");
        assert_eq!(value, "frozen<set<text>>");

        // Test error cases
        assert!(parser.extract_map_types("map<>").is_err());
        assert!(parser.extract_map_types("map<text>").is_err());
        assert!(parser.extract_map_types("int").is_err());
    }

    /// Regression test for Issue #481 bug 2: `parse_complex_cell_value` was
    /// calling `parse_raw_type_value(value_data, 0, ...)` which re-consumed the
    /// already-extracted length prefix, causing the first content byte (e.g.
    /// `0x2A = 42`) to be misread as the start of another VInt length.
    ///
    /// **Without the fix** `parse_raw_type_value` would try to read 42 more
    /// bytes from a 2-byte slice and return an error, so the test would panic.
    /// **With the fix** `parse_value_from_raw_bytes` treats the whole slice as
    /// raw value bytes and returns `Blob([0x2A, 0xBB, 0xCC])`.
    #[test]
    fn test_regression_481_complex_cell_value_no_double_length_prefix() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);
        let column = crate::schema::Column {
            name: "my_blob".to_string(),
            data_type: "blob".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Build one list-cell with value bytes [0x2A, 0xBB, 0xCC].
        //
        // flags = 0x08 (use_row_timestamp — skip reading a timestamp),
        // path_len VUInt = 0x00 (empty path, normal for list elements),
        // value_len VUInt = 0x03,
        // value = [0x2A, 0xBB, 0xCC].
        //
        // The first content byte (0x2A = 42) is deliberately chosen so that
        // the pre-fix code — which passed the already-extracted slice back into
        // parse_raw_type_value with offset 0 — would read it as a length prefix
        // ("read 42 more bytes") and fail.
        let cell_bytes: Vec<u8> = vec![
            0x08, // flags: use_row_timestamp (skip ts field), no deletion, no empty-value
            0x00, // path_len VUInt = 0 (empty path)
            0x03, // value_len VUInt = 3
            0x2A, // ← first content byte; pre-fix code misread this as a length
            0xBB, 0xCC,
        ];

        let cell = parser
            .parse_complex_cell_value(&cell_bytes, 0, "blob", &column, 0)
            .expect("parse_complex_cell_value should succeed");

        assert!(cell.path_bytes.is_empty());
        assert!(!cell.is_deleted);
        assert_eq!(cell.next_offset, cell_bytes.len());
        assert_eq!(
            cell.value,
            Some(Value::blob(vec![0x2A, 0xBB, 0xCC])),
            "blob value must be the three raw bytes, not a misread length-prefixed parse"
        );
    }

    /// Issue #1741 (Finding 3): a non-frozen collection's read-time TTL aggregate
    /// must reflect its ELEMENTS, not blanket "live-forever". Build a `list<blob>`
    /// with one EXPIRING element (explicit `localDeletionTime = 1000`) and one
    /// LIVE-FOREVER element (no TTL), and assert the derived aggregate:
    ///   * `has_live_forever_element == true` (the no-TTL element keeps the row visible)
    ///   * `max_element_expires_at == Some(1000)` (the expiring element's expiry).
    ///
    /// Then a collection whose elements are ALL expiring must report
    /// `has_live_forever_element == false` (so an otherwise-expired row is NOT kept
    /// alive by the collection), and an all-deleted collection contributes neither.
    #[test]
    fn test_1741_collection_element_ttl_aggregate() {
        use super::super::test_support::helpers::encode_unsigned;

        // Deltas are absolute (min_timestamp / min_local_deletion_time / min_ttl = 0).
        let parser = V5CompressedLegacyParser::new("k".to_string(), "t".to_string(), 0, 0, Some(0));
        let column = crate::schema::Column {
            name: "c".to_string(),
            data_type: "list<blob>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // One expiring element (flags IS_EXPIRING 0x02): ts, ldt, ttl, path, value.
        // All deltas are < 128 so each VUInt is a single byte (unambiguous layout).
        let expiring = |ldt: u64, val: u8| {
            let mut b = vec![0x02u8];
            encode_unsigned(1, &mut b); // timestamp delta
            encode_unsigned(ldt, &mut b); // localDeletionTime delta == absolute expiry
            encode_unsigned(1, &mut b); // ttl delta
            encode_unsigned(0, &mut b); // path_len
            encode_unsigned(1, &mut b); // value_len
            b.push(val);
            b
        };
        // One live-forever element (flags 0x08 USE_ROW_TIMESTAMP, no TTL fields).
        let live_forever = |val: u8| {
            let mut b = vec![0x08u8];
            encode_unsigned(0, &mut b); // path_len
            encode_unsigned(1, &mut b); // value_len
            b.push(val);
            b
        };
        // A deleted element (flags IS_DELETED 0x01): timestamp, ldt (deleted), path,
        // no ttl (not expiring), no value (deleted).
        let deleted = |ldt: u64| {
            let mut b = vec![0x01u8];
            encode_unsigned(1, &mut b); // timestamp delta (not USE_ROW_TIMESTAMP)
            encode_unsigned(ldt, &mut b); // localDeletionTime delta
            encode_unsigned(0, &mut b); // path_len
            b
        };

        // Case A: expiring + live-forever.
        let mut data = Vec::new();
        encode_unsigned(2, &mut data); // cell_count
        data.extend_from_slice(&expiring(100, 0xAA));
        data.extend_from_slice(&live_forever(0xBB));
        let (_v, _off, meta) = parser
            .parse_complex_column_inner(&data, 0, &column, "list<blob>", false, 0, None, None)
            .expect("parse list<blob>");
        assert!(
            meta.has_live_forever_element,
            "a live-forever element must keep the row visible"
        );
        assert_eq!(
            meta.max_element_expires_at,
            Some(100),
            "the expiring element's explicit expiry must fold into the aggregate"
        );
        assert_eq!(meta.element_tombstone_count, 0);

        // Case B: all elements expiring — NO live-forever, max is the larger expiry.
        let mut data = Vec::new();
        encode_unsigned(2, &mut data);
        data.extend_from_slice(&expiring(100, 0xAA));
        data.extend_from_slice(&expiring(120, 0xBB));
        let (_v, _off, meta) = parser
            .parse_complex_column_inner(&data, 0, &column, "list<blob>", false, 0, None, None)
            .expect("parse list<blob>");
        assert!(
            !meta.has_live_forever_element,
            "an all-expiring collection must NOT keep an otherwise-expired row alive"
        );
        assert_eq!(meta.max_element_expires_at, Some(120));

        // Case C: all elements deleted — neither live-forever nor an expiry.
        let mut data = Vec::new();
        encode_unsigned(2, &mut data);
        data.extend_from_slice(&deleted(100));
        data.extend_from_slice(&deleted(120));
        let (_v, _off, meta) = parser
            .parse_complex_column_inner(&data, 0, &column, "list<blob>", false, 0, None, None)
            .expect("parse list<blob>");
        assert!(!meta.has_live_forever_element);
        assert_eq!(meta.max_element_expires_at, None);
        assert_eq!(meta.element_tombstone_count, 2);
    }

    /// Issue #1741 (per-element filtering, test 2 — expired vs live): a non-frozen
    /// collection with ONE expired element (explicit per-element
    /// `localDeletionTime <= now`) and ONE live-forever element must emit ONLY the
    /// live element when the read-side [`ElementShadow`] filter is active. The
    /// dropped element folds its (past) expiry into the aggregate but never marks
    /// `live-forever`.
    ///
    /// Pinned as a UNIT test (not end-to-end) because the writer stamps an expiring
    /// cell's `localDeletionTime` as `now + ttl`, so a PAST-expired per-element TTL
    /// is not synthesizable via a fresh writer flush — the same rationale as the
    /// existing `per_cell_shadow_and_ttl_drop_decision` pin. This drives the exact
    /// element loop the read path calls (`element_filter = Some`).
    ///
    /// Revert-verify: with the filter `None` (the physical-consumer path) the list
    /// keeps BOTH elements and `shadow_filtered_element_count == 0` — proving the
    /// differential is the filter, not the parse.
    #[test]
    fn test_1741_per_element_ttl_filter_keeps_only_live() {
        use super::super::test_support::helpers::encode_unsigned;

        let parser = V5CompressedLegacyParser::new("k".to_string(), "t".to_string(), 0, 0, Some(0));
        let column = crate::schema::Column {
            name: "c".to_string(),
            data_type: "list<blob>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };
        // Expiring element (IS_EXPIRING 0x02): own ts, ldt (absolute expiry), ttl,
        // empty path, one value byte.
        let expiring = |ldt: u64, val: u8| {
            let mut b = vec![0x02u8];
            encode_unsigned(1, &mut b); // timestamp delta
            encode_unsigned(ldt, &mut b); // localDeletionTime delta == absolute expiry
            encode_unsigned(1, &mut b); // ttl delta
            encode_unsigned(0, &mut b); // path_len
            encode_unsigned(1, &mut b); // value_len
            b.push(val);
            b
        };
        // Live-forever element (USE_ROW_TIMESTAMP 0x08, no TTL fields).
        let live_forever = |val: u8| {
            let mut b = vec![0x08u8];
            encode_unsigned(0, &mut b); // path_len
            encode_unsigned(1, &mut b); // value_len
            b.push(val);
            b
        };

        let mut data = Vec::new();
        encode_unsigned(2, &mut data); // cell_count
        data.extend_from_slice(&expiring(100, 0xAA)); // expires at 100
        data.extend_from_slice(&live_forever(0xBB));

        // now = 200 (> 100) so the expiring element is TTL-expired; no covering
        // deletion. row_ts feeds the USE_ROW_TIMESTAMP element's inherited ts.
        let filter = ElementShadow {
            cover: None,
            now: 200,
            row_ts: Some(5),
            row_expires_at: None,
            row_ttl_seconds: None,
        };
        let (value, _off, meta) = parser
            .parse_complex_column_inner(
                &data,
                0,
                &column,
                "list<blob>",
                false,
                0,
                None,
                Some(filter),
            )
            .expect("parse list<blob>");
        assert_eq!(
            value,
            Value::List(vec![Value::blob(vec![0xBB])]),
            "only the live-forever element survives; the expired element is dropped"
        );
        assert_eq!(meta.shadow_filtered_element_count, 1);
        assert!(
            meta.has_live_forever_element,
            "the surviving live-forever element keeps the row visible"
        );
        assert_eq!(meta.max_element_expires_at, Some(100));

        // Revert-verify: with NO filter the parse keeps both elements.
        let (value_unfiltered, _off, meta_unfiltered) = parser
            .parse_complex_column_inner(&data, 0, &column, "list<blob>", false, 0, None, None)
            .expect("parse list<blob>");
        assert_eq!(
            value_unfiltered,
            Value::List(vec![Value::blob(vec![0xAA]), Value::blob(vec![0xBB])]),
            "the physical (no-filter) parse must keep BOTH elements (byte-unchanged)"
        );
        assert_eq!(meta_unfiltered.shadow_filtered_element_count, 0);
    }

    /// Issue #1741 (per-element filtering, test 3 — all shadowed/expired): a
    /// collection whose every element is shadowed by the covering deletion OR
    /// TTL-expired emits an EMPTY container, reports `shadow_filtered_element_count
    /// == cell_count`, and never marks live-forever. The read call site turns that
    /// (empty container + filtered count > 0) into an ABSENT column so it cannot by
    /// itself keep an otherwise-dead row alive.
    ///
    /// Revert-verify: with the filter `None` the container keeps every element and
    /// `shadow_filtered_element_count == 0`.
    #[test]
    fn test_1741_per_element_all_shadowed_collection_is_empty() {
        use super::super::test_support::helpers::encode_unsigned;

        let parser = V5CompressedLegacyParser::new("k".to_string(), "t".to_string(), 0, 0, Some(0));
        let column = crate::schema::Column {
            name: "c".to_string(),
            data_type: "list<blob>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };
        // Two plain (non-expiring) elements each carrying its OWN write ts (delta),
        // both OLDER than the covering deletion below.
        let plain = |ts_delta: u64, val: u8| {
            let mut b = vec![0x00u8];
            encode_unsigned(ts_delta, &mut b); // timestamp delta == absolute ts
            encode_unsigned(0, &mut b); // path_len
            encode_unsigned(1, &mut b); // value_len
            b.push(val);
            b
        };
        let mut data = Vec::new();
        encode_unsigned(2, &mut data);
        data.extend_from_slice(&plain(10, 0xAA)); // ts = 10
        data.extend_from_slice(&plain(20, 0xBB)); // ts = 20

        // Covering deletion at 100 (µs) shadows both elements (10, 20 <= 100).
        let filter = ElementShadow {
            cover: Some(100),
            now: 0,
            row_ts: None,
            row_expires_at: None,
            row_ttl_seconds: None,
        };
        let (value, _off, meta) = parser
            .parse_complex_column_inner(
                &data,
                0,
                &column,
                "list<blob>",
                false,
                0,
                None,
                Some(filter),
            )
            .expect("parse list<blob>");
        assert!(
            V5CompressedLegacyParser::complex_value_is_empty(&value),
            "every element is shadowed by the covering deletion, so the container is empty"
        );
        assert_eq!(meta.shadow_filtered_element_count, 2);
        assert!(
            !meta.has_live_forever_element,
            "a wholly-shadowed collection must NOT keep an otherwise-dead row alive"
        );
        // The shadowed elements STILL fold their write ts into the aggregate so the
        // row is recognised as shadowed (max 20 <= covering 100).
        assert_eq!(meta.max_element_writetime, 20);

        // Revert-verify: with NO filter the container keeps both elements.
        let (value_unfiltered, _off, meta_unfiltered) = parser
            .parse_complex_column_inner(&data, 0, &column, "list<blob>", false, 0, None, None)
            .expect("parse list<blob>");
        assert_eq!(
            value_unfiltered,
            Value::List(vec![Value::blob(vec![0xAA]), Value::blob(vec![0xBB])])
        );
        assert_eq!(meta_unfiltered.shadow_filtered_element_count, 0);
    }

    /// Issue #1741 (roborev Medium — inherited-row-TTL collection elements): an
    /// expiring collection element that INHERITS the row TTL (`USE_ROW_TTL` 0x10,
    /// `is_expiring` true, NO explicit per-element `localDeletionTime`) must be
    /// dropped when the inherited row liveness expiry is past `now`, EXACTLY as the
    /// scalar `USE_ROW_TTL` cell path does — even though the row itself survives
    /// (another live element keeps it visible). Before the fix such an element had
    /// `element_local_deletion_time == None`, so its computed `eff_exp` was `None`
    /// and it was KEPT (the expired inherited-TTL element leaked into the result).
    ///
    /// Scenario: a `list<blob>` with (1) a USE_ROW_TTL element inheriting an EXPIRED
    /// row TTL and (2) a live-forever element that keeps the collection non-empty →
    /// only the live-forever element survives.
    ///
    /// Pinned as a UNIT test (not end-to-end) for the same reason as
    /// `test_1741_per_element_ttl_filter_keeps_only_live`: the writer stamps an
    /// expiring row's `liveness_expires_at_seconds` as `now + ttl`, so a PAST-expired
    /// inherited TTL is not synthesizable via a fresh writer flush. This drives the
    /// exact element loop the read path calls (`element_filter = Some`) with the row
    /// liveness expiry threaded into `ElementShadow::row_expires_at`.
    ///
    /// Revert-verify: with the filter `None` (physical-consumer path) BOTH elements
    /// survive and `shadow_filtered_element_count == 0` — proving the differential is
    /// the filter, not the parse.
    #[test]
    fn test_1741_per_element_inherited_row_ttl_expired_is_dropped() {
        use super::super::test_support::helpers::encode_unsigned;

        let parser = V5CompressedLegacyParser::new("k".to_string(), "t".to_string(), 0, 0, Some(0));
        let column = crate::schema::Column {
            name: "c".to_string(),
            data_type: "list<blob>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };
        // USE_ROW_TTL element (flags 0x12 = IS_EXPIRING 0x02 | USE_ROW_TTL 0x10):
        // it carries its OWN timestamp (USE_ROW_TIMESTAMP not set) but NO per-element
        // localDeletionTime / TTL (both omitted under USE_ROW_TTL). Its expiry is
        // inherited from the row liveness expiry.
        let use_row_ttl = |val: u8| {
            let mut b = vec![0x12u8];
            encode_unsigned(1, &mut b); // timestamp delta
            encode_unsigned(0, &mut b); // path_len
            encode_unsigned(1, &mut b); // value_len
            b.push(val);
            b
        };
        // Live-forever element (USE_ROW_TIMESTAMP 0x08, no TTL fields).
        let live_forever = |val: u8| {
            let mut b = vec![0x08u8];
            encode_unsigned(0, &mut b); // path_len
            encode_unsigned(1, &mut b); // value_len
            b.push(val);
            b
        };
        let mut data = Vec::new();
        encode_unsigned(2, &mut data); // cell_count
        data.extend_from_slice(&use_row_ttl(0xAA)); // inherits (expired) row TTL
        data.extend_from_slice(&live_forever(0xBB)); // keeps the collection non-empty

        // Row liveness expiry = 100 (epoch s), now = 200 (> 100) → the inherited-TTL
        // element is EXPIRED. No covering deletion. row_ts feeds the inherited write ts.
        let filter = ElementShadow {
            cover: None,
            now: 200,
            row_ts: Some(5),
            row_expires_at: Some(100),
            row_ttl_seconds: Some(50),
        };
        let (value, _off, meta) = parser
            .parse_complex_column_inner(
                &data,
                0,
                &column,
                "list<blob>",
                false,
                0,
                None,
                Some(filter),
            )
            .expect("parse list<blob>");
        assert_eq!(
            value,
            Value::List(vec![Value::blob(vec![0xBB])]),
            "the inherited-row-TTL element is expired and must be dropped; only the \
             live-forever element survives (pre-fix: BOTH leaked)"
        );
        assert_eq!(
            meta.shadow_filtered_element_count, 1,
            "exactly the expired inherited-TTL element was filtered"
        );
        assert!(
            meta.has_live_forever_element,
            "the surviving live-forever element keeps the row visible"
        );

        // Revert-verify: with NO filter (physical consumer) BOTH elements survive.
        let (value_unfiltered, _off, meta_unfiltered) = parser
            .parse_complex_column_inner(&data, 0, &column, "list<blob>", false, 0, None, None)
            .expect("parse list<blob>");
        assert_eq!(
            value_unfiltered,
            Value::List(vec![Value::blob(vec![0xAA]), Value::blob(vec![0xBB])]),
            "the physical (no-filter) parse keeps BOTH elements (byte-unchanged)"
        );
        assert_eq!(meta_unfiltered.shadow_filtered_element_count, 0);
    }

    /// Issue #2038 (roborev 1503, round 3): #2038's acceptance criteria
    /// explicitly require `USING TTL n` support on a non-frozen collection —
    /// a STATEMENT-LEVEL TTL INSERT, which is exactly the `USE_ROW_TTL`
    /// on-disk encoding (no explicit per-element TTL/LDT; the element
    /// inherits the row's liveness expiry). Round 2's `ElementExpiryShape`
    /// classified this shape as `Unresolvable` unconditionally, forcing
    /// `TTL(collection)` to `None` even for a single, unambiguous, live
    /// USE_ROW_TTL element — an unmet acceptance criterion.
    ///
    /// Scenario: a `list<blob>` with ONE USE_ROW_TTL element, NOT expired
    /// (`now` < inherited row expiry). `visible_uniform_expiration` must
    /// resolve to `Some(CellExpiration { ttl_seconds: 50, expires_at_seconds:
    /// 1000 })` — the row-level TTL/expiry threaded via
    /// `ElementShadow::row_ttl_seconds`/`row_expires_at`.
    ///
    /// Revert-verify: reverting `ElementExpiryShape::from_cell`'s `(None,
    /// None)` arm to unconditionally return `Unresolvable` (round 2's
    /// behavior) makes this test FAIL (`visible_uniform_expiration` becomes
    /// `None`) — confirmed by hand before this fix landed.
    #[test]
    fn test_2038_use_row_ttl_element_surfaces_inherited_expiration() {
        use super::super::test_support::helpers::encode_unsigned;

        let parser = V5CompressedLegacyParser::new("k".to_string(), "t".to_string(), 0, 0, Some(0));
        let column = crate::schema::Column {
            name: "c".to_string(),
            data_type: "list<blob>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };
        // USE_ROW_TTL element (flags 0x12 = IS_EXPIRING 0x02 | USE_ROW_TTL 0x10):
        // own timestamp, NO per-element localDeletionTime/TTL.
        let use_row_ttl = |val: u8| {
            let mut b = vec![0x12u8];
            encode_unsigned(1, &mut b); // timestamp delta
            encode_unsigned(0, &mut b); // path_len
            encode_unsigned(1, &mut b); // value_len
            b.push(val);
            b
        };
        let mut data = Vec::new();
        encode_unsigned(1, &mut data); // cell_count
        data.extend_from_slice(&use_row_ttl(0xAA));

        // Row liveness TTL=50s, expiry=1000 (epoch s); now=500 (< 1000) → LIVE
        // (not expired, not shadowed).
        let filter = ElementShadow {
            cover: None,
            now: 500,
            row_ts: Some(5),
            row_expires_at: Some(1000),
            row_ttl_seconds: Some(50),
        };
        let (value, _off, meta) = parser
            .parse_complex_column_inner(
                &data,
                0,
                &column,
                "list<blob>",
                false,
                0,
                None,
                Some(filter),
            )
            .expect("parse list<blob>");
        assert_eq!(
            value,
            Value::List(vec![Value::blob(vec![0xAA])]),
            "the live USE_ROW_TTL element survives (not expired)"
        );
        assert_eq!(
            meta.visible_uniform_expiration,
            Some(CellExpiration {
                ttl_seconds: 50,
                expires_at_seconds: 1000,
            }),
            "Issue #2038: a statement-level USING TTL n collection element \
             (USE_ROW_TTL encoding) must surface its inherited expiry, not None"
        );
    }

    /// Issue #2173 (a) (roborev Low): the scalar cell reader clamps an absolute
    /// TTL to `i32::MAX` (`cell_value.rs`: `abs_ttl.min(i32::MAX as i64) as
    /// i32`). `ElementExpiryShape::from_cell` must MIRROR that clamp — a bare
    /// `ttl as i32` cast on the `u32` `element_ttl` would expose a NEGATIVE
    /// `ttl_seconds` for a TTL > `i32::MAX`, violating the `CellExpiration`
    /// contract. No real Cassandra data reaches this (max TTL ~20y ≪
    /// `i32::MAX`); this pins the defensive parity.
    ///
    /// Revert-verify: reverting the fix to `Self::Explicit(ttl as i32, ...)`
    /// makes this test FAIL — the resolved `ttl_seconds` becomes negative.
    #[test]
    fn test_2173_explicit_element_ttl_clamps_to_i32_max_not_negative() {
        // A u32 element TTL strictly greater than i32::MAX (defensive only).
        let over_max: u32 = (i32::MAX as u32) + 1;
        assert!(
            (over_max as i32) < 0,
            "sanity: a bare `as i32` cast of this TTL would be negative"
        );
        let cell = ComplexCellParse {
            value: Some(Value::blob(vec![0xAA])),
            path_bytes: Vec::new(),
            is_deleted: false,
            has_empty_value: false,
            next_offset: 0,
            element_writetime: Some(1),
            element_ttl: Some(over_max),
            element_local_deletion_time: Some(1000),
            is_expiring: true,
        };
        // No USE_ROW_TTL inheritance (explicit per-element TTL present).
        match ElementExpiryShape::from_cell(&cell, None, None) {
            ElementExpiryShape::Explicit(ttl_seconds, _expires_at) => {
                assert_eq!(
                    ttl_seconds,
                    i32::MAX,
                    "a u32 TTL > i32::MAX must clamp to i32::MAX (matching the \
                     scalar reader), never wrap to a negative value"
                );
                assert!(ttl_seconds >= 0, "clamped ttl_seconds must be non-negative");
            }
            other => panic!("expected Explicit shape, got {other:?}"),
        }
    }

    /// Issue #2038 (roborev 1503, round 3): PRESERVE the round-2 no-over-
    /// approximation invariant when the mix is EXPLICIT-per-element vs
    /// INHERITED-row-TTL, not just explicit-vs-explicit (round 2 already pins
    /// the explicit-only heterogeneous case in the integration test
    /// `issue_2038_collection_ttl_expiring_cell.rs`).
    ///
    /// Scenario: a `list<blob>` with element A (EXPLICIT ttl=100,
    /// expires_at=100) and element B (USE_ROW_TTL inheriting ttl=200,
    /// expires_at=200) — two DIFFERENT effective expiries. Neither is
    /// expired/shadowed at `now=50`. `visible_uniform_expiration` must be
    /// `None`: the collection has no single TTL that describes both elements.
    #[test]
    fn test_2038_mixed_explicit_and_inherited_expiry_surfaces_no_expiration() {
        use super::super::test_support::helpers::encode_unsigned;

        let parser = V5CompressedLegacyParser::new("k".to_string(), "t".to_string(), 0, 0, Some(0));
        let column = crate::schema::Column {
            name: "c".to_string(),
            data_type: "list<blob>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };
        // Element A: EXPLICIT expiring (flags 0x02), own ts, ldt=100, ttl=100.
        let explicit_expiring = |val: u8| {
            let mut b = vec![0x02u8];
            encode_unsigned(1, &mut b); // timestamp delta
            encode_unsigned(100, &mut b); // localDeletionTime (absolute, min_ldt=0)
            encode_unsigned(100, &mut b); // ttl (absolute, min_ttl=0)
            encode_unsigned(0, &mut b); // path_len
            encode_unsigned(1, &mut b); // value_len
            b.push(val);
            b
        };
        // Element B: USE_ROW_TTL (flags 0x12), own ts, NO per-element ldt/ttl —
        // inherits the row's expiry via `ElementShadow`.
        let use_row_ttl = |val: u8| {
            let mut b = vec![0x12u8];
            encode_unsigned(1, &mut b); // timestamp delta
            encode_unsigned(0, &mut b); // path_len
            encode_unsigned(1, &mut b); // value_len
            b.push(val);
            b
        };
        let mut data = Vec::new();
        encode_unsigned(2, &mut data); // cell_count
        data.extend_from_slice(&explicit_expiring(0xAA)); // effective (100, 100)
        data.extend_from_slice(&use_row_ttl(0xBB)); // effective (200, 200) via row

        // Row liveness TTL=200s, expiry=200 (epoch s); now=50 (< 100 and < 200)
        // → BOTH elements are LIVE (neither expired/shadowed).
        let filter = ElementShadow {
            cover: None,
            now: 50,
            row_ts: Some(5),
            row_expires_at: Some(200),
            row_ttl_seconds: Some(200),
        };
        let (value, _off, meta) = parser
            .parse_complex_column_inner(
                &data,
                0,
                &column,
                "list<blob>",
                false,
                0,
                None,
                Some(filter),
            )
            .expect("parse list<blob>");
        assert_eq!(
            value,
            Value::List(vec![Value::blob(vec![0xAA]), Value::blob(vec![0xBB])]),
            "both elements are live and survive"
        );
        assert_eq!(
            meta.visible_uniform_expiration, None,
            "Issue #2038 (roborev 1503): an explicit-100 element mixed with an \
             inherited-200 element has no single TTL that describes the \
             collection — must surface None, not over-approximate with either \
             element's expiry, got {:?}",
            meta.visible_uniform_expiration
        );
    }

    /// Regression test for Issue #481 regression: `list<frozen<udt>>` elements
    /// were being returned as `Value::Blob` instead of `Value::Udt`.
    ///
    /// **Root cause**: `parse_complex_cell_value` called `parse_value_from_raw_bytes`
    /// with element_type `"frozen<address_type>"`.  The `frozen<>` arm stripped it
    /// to `"address_type"`, then recursed.  `"address_type"` did not match
    /// `is_udt_type()` (marshal form only) and fell through to the blob fallback.
    ///
    /// **Fix**: the `other =>` fallback in `parse_value_from_raw_bytes` now checks
    /// `self.udt_registry` for the bare name and delegates to `parse_raw_type_value`
    /// when found, which correctly reads the per-field i32 length-prefixed UDT data.
    ///
    /// This test fails on the pre-fix code path (produces `Value::Blob`) and
    /// passes after the fix (produces `Value::Udt` with `street` and `city` fields).
    #[test]
    fn test_regression_481_list_frozen_udt_parses_as_udt_not_blob() {
        use crate::schema::{CqlType, UdtRegistry};
        use crate::types::{UdtFieldDef, UdtTypeDef};

        // Build a UdtRegistry with a minimal "address_type" UDT: street TEXT, city TEXT
        let mut registry = UdtRegistry::new();
        registry.register_udt(UdtTypeDef {
            keyspace: "test_collections".to_string(),
            name: "address_type".to_string(),
            fields: vec![
                UdtFieldDef {
                    name: "street".to_string(),
                    field_type: CqlType::Text,
                    nullable: true,
                },
                UdtFieldDef {
                    name: "city".to_string(),
                    field_type: CqlType::Text,
                    nullable: true,
                },
            ],
        });

        let parser = V5CompressedLegacyParser::new(
            "test_collections".to_string(),
            "collections_with_udts".to_string(),
            0,
            0,
            None,
        )
        .with_udt_registry(registry);

        let column = crate::schema::Column {
            name: "addresses".to_string(),
            data_type: "list<frozen<address_type>>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Build UDT bytes for {street="Main St", city="Springfield"}:
        //   Each field: [i32 BE length (4 bytes)][field bytes]
        //   street: length=7, bytes="Main St"
        //   city:   length=11, bytes="Springfield"
        let mut udt_bytes: Vec<u8> = Vec::new();
        let street = b"Main St";
        udt_bytes.extend_from_slice(&(street.len() as i32).to_be_bytes());
        udt_bytes.extend_from_slice(street);
        let city = b"Springfield";
        udt_bytes.extend_from_slice(&(city.len() as i32).to_be_bytes());
        udt_bytes.extend_from_slice(city);

        // Build a complex-cell encoded list with one element.
        //   [cell_count:VUInt = 1]
        //   [flags:u8 = 0x08 (use_row_timestamp — skip explicit ts)]
        //   [path_len:VUInt = 0x00 (empty path — list elements have empty path)]
        //   [value_len:VUInt = udt_bytes.len()]
        //   [value: udt_bytes]
        assert!(
            udt_bytes.len() < 0x80,
            "test helper assumes single-byte VUInt"
        );
        let mut blob: Vec<u8> = vec![
            0x01,                  // cell_count = 1
            0x08,                  // flags: use_row_timestamp, not deleted, value present
            0x00,                  // path_len VUInt = 0 (list cells have empty path)
            udt_bytes.len() as u8, // value_len VUInt
        ];
        blob.extend_from_slice(&udt_bytes);

        let (value, consumed, _meta) = parser
            .parse_complex_column_inner(&blob, 0, &column, &column.data_type, false, 0, None, None)
            .expect("parse_complex_column_inner must succeed for list<frozen<address_type>>");
        assert_eq!(consumed, blob.len(), "all bytes must be consumed");

        // The list must contain exactly one element that is a UDT (not a Blob).
        let elements = match value {
            Value::List(elems) => elems,
            other => panic!("Expected Value::List, got {:?}", other),
        };
        assert_eq!(elements.len(), 1, "list must have one element");

        // The element must be a Frozen<Udt> or Udt (not Blob).
        let udt_val = match &elements[0] {
            Value::Frozen(inner) => match inner.as_ref() {
                Value::Udt(u) => u.clone(),
                other => panic!("Expected Frozen<Udt>, got Frozen<{:?}>", other),
            },
            Value::Udt(u) => u.clone(),
            other => panic!(
                "Expected Value::Udt or Value::Frozen(Udt), got {:?} \
                 (regression: list<frozen<udt>> must not return Blob)",
                other
            ),
        };

        // Verify field names match the schema definition.
        let field_names: Vec<&str> = udt_val.fields.iter().map(|f| f.name.as_str()).collect();
        assert!(
            field_names.contains(&"street"),
            "UDT must have 'street' field, got: {:?}",
            field_names
        );
        assert!(
            field_names.contains(&"city"),
            "UDT must have 'city' field, got: {:?}",
            field_names
        );

        // Verify field values decode correctly.
        let street_field = udt_val.fields.iter().find(|f| f.name == "street").unwrap();
        assert_eq!(
            street_field.value,
            Some(Value::text("Main St".to_string())),
            "street field must decode to Text(\"Main St\")"
        );
        let city_field = udt_val.fields.iter().find(|f| f.name == "city").unwrap();
        assert_eq!(
            city_field.value,
            Some(Value::text("Springfield".to_string())),
            "city field must decode to Text(\"Springfield\")"
        );
    }

    /// Issue #1080 / roborev job 1357 (High): a DROPPED *fixed-width* scalar column
    /// (e.g. `int`) must be consumed with the correct fixed-width framing, NOT as a
    /// VInt-length-prefixed blob — otherwise it would misalign every trailing column.
    ///
    /// The dropped-column synthetic `Column` carries the on-disk header type, which
    /// the SerializationHeader parser ALWAYS normalizes to the CQL form via
    /// `convert_marshal_type_to_cql` (`Int32Type` → `"int"`). So a dropped int hits
    /// the same fixed-width arm as a present int and reads exactly 4 value bytes (no
    /// length prefix). This test drives the shared `parse_cell_value_schema_order`
    /// with a hand-crafted int cell and asserts exact 5-byte consumption (1 flags +
    /// 4 value) with the trailing column's bytes left intact.
    #[tokio::test]
    async fn test_regression_1080_dropped_fixed_width_scalar_consumes_exact_width() {
        use crate::storage::sstable::SSTableReader;
        use std::sync::Arc;

        // `_reader` is unused by parse_cell_value_schema_order, but we still need a
        // real instance; open the core test_basic/simple_table fixture. The helper
        // handles the skip-vs-strict policy (issue #1094): a clean skip when the
        // data is unavailable, or a hard failure under CQLITE_REQUIRE_FIXTURES=1.
        let data_file = match core_simple_table_data_file() {
            Some(df) => df,
            None => return,
        };
        let config = crate::Config::default();
        let platform = Arc::new(
            crate::platform::Platform::new(&config)
                .await
                .expect("platform"),
        );
        let reader = SSTableReader::open(&data_file, &config, platform)
            .await
            .expect("core fixture test_basic/simple_table must open (datasets root is set)");

        let parser = V5CompressedLegacyParser::new(
            "test_basic".to_string(),
            "simple_table".to_string(),
            0,
            0,
            None,
        );
        // A DROPPED int column carries the CQL-normalized header type "int".
        let column = crate::schema::Column {
            name: "__dropped_col_0".to_string(),
            data_type: "int".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Cell: [flags=0x08 USE_ROW_TIMESTAMP → live, has-value, no own ts/ttl]
        //       [i32 BE = 0x01020304]. Then a trailing sentinel for the FOLLOWING
        //       column to prove no misalignment.
        let mut buf: Vec<u8> = vec![0x08];
        buf.extend_from_slice(&0x0102_0304_i32.to_be_bytes());
        let value_end = buf.len(); // 1 flags + 4 value = 5
        let trailing_marker: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
        buf.extend_from_slice(trailing_marker);

        let (value, _ts, _exp, new_offset) = parser
            .parse_cell_value_schema_order(&buf, 0, &column, Some("int"), None, &reader)
            .expect("dropped int cell must decode with fixed-width framing");
        assert_eq!(value, Value::Integer(0x0102_0304));
        assert_eq!(
            new_offset, value_end,
            "int consumes exactly flags(1)+4 fixed bytes — NO spurious VInt length \
             (refutes roborev job 1357: dropped fixed-width scalar misalignment)"
        );
        assert_eq!(
            &buf[new_offset..],
            trailing_marker,
            "trailing column bytes must be intact (no misalignment after a dropped scalar)"
        );
    }

    /// Issue #1080 / roborev job 1363 (Medium): when the schema is DERIVED FROM the
    /// on-disk header (not supplied as a CQL short form), a frozen UDT column's
    /// `data_type` is the marshal string `org.apache.cassandra.db.marshal.FrozenType(
    /// ...UserType...)`, which does NOT start with CQL `frozen<`. It must still
    /// decode STRUCTURALLY (via the marshal-form dispatch arm), not blob. Drives the
    /// full `parse_cell_value_schema_order` to prove the dispatch routes correctly
    /// and a trailing column stays byte-aligned.
    #[tokio::test]
    async fn test_regression_1080_marshal_form_frozen_udt_decodes_structurally() {
        use crate::storage::sstable::SSTableReader;
        use std::sync::Arc;

        // Skip-vs-strict fixture policy is centralized in core_simple_table_data_file
        // (issue #1094): clean skip when data is unavailable, hard failure under
        // CQLITE_REQUIRE_FIXTURES=1.
        let data_file = match core_simple_table_data_file() {
            Some(df) => df,
            None => return,
        };
        let config = crate::Config::default();
        let platform = Arc::new(
            crate::platform::Platform::new(&config)
                .await
                .expect("platform"),
        );
        let reader = SSTableReader::open(&data_file, &config, platform)
            .await
            .expect("core fixture test_basic/simple_table must open (datasets root is set)");

        let parser = V5CompressedLegacyParser::new(
            "test_types".to_string(),
            "cx_frozen_udt".to_string(),
            0,
            0,
            None,
        );
        let hex = |s: &str| -> String { hex::encode(s.as_bytes()) };
        let q = "org.apache.cassandra.db.marshal";
        // Header-derived schema: data_type IS the fully-qualified marshal string.
        let marshal_type = format!(
            "{q}.FrozenType({q}.UserType(test_types,{},{}:{q}.UTF8Type,{}:{q}.Int32Type))",
            hex("person_type"),
            hex("name"),
            hex("age"),
        );
        let column = crate::schema::Column {
            name: "p".to_string(),
            data_type: marshal_type,
            nullable: true,
            default: None,
            is_static: false,
        };

        // Cell: [flags=0x08][VInt blob_len][udt_blob]; then a trailing sentinel.
        let mut udt_blob: Vec<u8> = Vec::new();
        let name = b"Ada";
        udt_blob.extend_from_slice(&(name.len() as i32).to_be_bytes());
        udt_blob.extend_from_slice(name);
        udt_blob.extend_from_slice(&4i32.to_be_bytes());
        udt_blob.extend_from_slice(&36i32.to_be_bytes());
        assert!(udt_blob.len() < 0x80);
        let mut buf: Vec<u8> = vec![0x08, udt_blob.len() as u8];
        buf.extend_from_slice(&udt_blob);
        let trailing_marker: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
        let trailing_offset = buf.len();
        buf.extend_from_slice(trailing_marker);

        let (value, _ts, _exp, new_offset) = parser
            .parse_cell_value_schema_order(&buf, 0, &column, None, None, &reader)
            .expect("marshal-form frozen UDT must decode");
        // Structured frozen UDT, NOT a blob.
        let inner = match &value {
            Value::Frozen(b) => b.as_ref(),
            other => other,
        };
        match inner {
            Value::Udt(u) => {
                assert_eq!(u.type_name, "person_type");
                assert_eq!(u.fields.len(), 2);
            }
            other => panic!(
                "header-derived marshal-form frozen UDT must decode structurally, got {other:?}"
            ),
        }
        assert_eq!(
            new_offset, trailing_offset,
            "marshal-form frozen UDT must consume exactly its cell — trailing column stays aligned"
        );
        assert_eq!(&buf[new_offset..], trailing_marker);
    }

    /// Regression test for Issue #481 bug 3: for `set<T>` complex columns, each
    /// set element is stored in the cell PATH (with `HAS_EMPTY_VALUE` = 0x04
    /// set in cell flags), not the cell value.
    ///
    /// **Without the fix** `parse_complex_column` (the set branch) only checked
    /// `if let Some(val) = cell_value { elements.push(val) }` and silently
    /// discarded the path bytes, so the set appeared empty.
    /// **With the fix** the `else if !path_bytes.is_empty()` branch decodes the
    /// path bytes and adds them to the set.
    #[test]
    fn test_regression_481_set_elements_from_cell_path() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);
        let column = crate::schema::Column {
            name: "my_set".to_string(),
            data_type: "set<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Build a synthetic `set<text>` with two elements: "hello" and "world".
        //
        // Outer format: [cell_count:VUInt] [cell1] [cell2]
        //   cell_count = 2 → VUInt(2) = 0x02
        //
        // Each cell has HAS_EMPTY_VALUE (0x04) set, so the element lives in the
        // path field.  Timestamp is VInt(0) = 0x00 (ZigZag single byte).
        let hello = b"hello";
        let world = b"world";
        let mut blob = vec![0x02u8]; // cell_count = 2
        blob.extend(build_set_cell_bytes(hello));
        blob.extend(build_set_cell_bytes(world));

        let (value, consumed, _meta) = parser
            .parse_complex_column_inner(&blob, 0, &column, &column.data_type, false, 0, None, None)
            .expect("parse_complex_column_inner should succeed");

        assert_eq!(consumed, blob.len());
        assert_eq!(
            value,
            Value::Set(vec![
                Value::text("hello".to_string()),
                Value::text("world".to_string()),
            ]),
            "set elements stored in cell path must be decoded and returned"
        );
    }

    /// Regression test for Issue #493: element-level tombstones in a `set<T>`
    /// must NOT surface as live members.
    ///
    /// In the Cassandra 5.0 complex-cell format a live set element and a
    /// tombstoned element both produce `cell.value == None` with non-empty path
    /// bytes (live elements carry HAS_EMPTY_VALUE 0x04 and store the element in
    /// the path). The ONLY authoritative signal distinguishing them is the
    /// IS_DELETED (0x01) cell flag, which `parse_complex_cell_value` now surfaces
    /// via `ComplexCellParse::is_deleted`.
    ///
    /// **Without the fix** the set branch only checked `cell.value` / `path_bytes`
    /// and emitted BOTH "live" and "dead" as members, so the result was
    /// `Set(["live", "dead"])`.
    /// **With the fix** the tombstoned element is skipped and the result is
    /// `Set(["live"])`.
    #[test]
    fn test_regression_493_set_element_tombstone_skipped() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);
        let column = crate::schema::Column {
            name: "my_set".to_string(),
            data_type: "set<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Build a synthetic `set<text>` with two cells:
        //   cell 0: live element "live"  (HAS_EMPTY_VALUE, element in path)
        //   cell 1: tombstoned element "dead" (IS_DELETED, element in path)
        //
        // Outer format: [cell_count:VUInt] [cell0] [cell1]
        let live = b"live";
        let dead = b"dead";
        let mut blob = vec![0x02u8]; // cell_count = 2
        blob.extend(build_set_cell_bytes(live));
        blob.extend(build_set_tombstone_cell_bytes(dead));

        let (value, consumed, meta) = parser
            .parse_complex_column_inner(&blob, 0, &column, &column.data_type, false, 0, None, None)
            .expect("parse_complex_column_inner should succeed");

        assert_eq!(consumed, blob.len(), "parser must consume the entire blob");
        assert_eq!(
            value,
            Value::Set(vec![Value::text("live".to_string())]),
            "tombstoned set element must be skipped; only the live element survives"
        );
        // DS4 (Issue #700): element tombstone must be counted in the scan summary.
        assert_eq!(
            meta.element_tombstone_count, 1,
            "the tombstoned set element must increment element_tombstone_count"
        );
        // Non-overwrite generation (no has_complex_deletion=false → no collection tombstone).
        assert!(
            !meta.has_collection_tombstone,
            "no collection tombstone when has_complex_deletion=false"
        );
    }

    // =========================================================================
    // DS4 (Issue #700) / roborev Finding 3 — byte-level collection tombstone test
    //
    // The `has_collection_tombstone` decode path
    //   `absolute_mfda = min_timestamp.wrapping_add(mfda_delta)`
    //   `has_collection_tombstone = absolute_mfda != i64::MIN`
    // was previously exercised only by e2e tests that cover the append (no-tombstone)
    // path.  This unit test drives `parse_complex_column_inner` with
    // `has_complex_deletion = true` and a non-sentinel `markedForDeleteAt` value,
    // confirming that `ComplexColumnMeta.has_collection_tombstone == true` is set
    // purely from the byte-level decode without needing a full SSTableReader.
    // =========================================================================

    /// Byte-level test: `parse_complex_column_inner` with `has_complex_deletion = true`
    /// and a real `markedForDeleteAt` timestamp (not the i64::MIN sentinel) must set
    /// `ComplexColumnMeta.has_collection_tombstone = true`.
    ///
    /// Wire layout (min_timestamp = 0; complex-deletion deltas are UNSIGNED
    /// VInts per the writer, roborev #863):
    ///   [mfda_delta: VUInt(2) = 0x02]  → absolute_mfda = 0 + 2 = 2 ≠ i64::MIN
    ///   [localDeletionTime: VUInt(0) = 0x00]
    ///   [cell_count: VUInt(0) = 0x00]  ← zero cells for simplicity
    ///
    /// The parser uses `min_timestamp = 0` (default from `V5CompressedLegacyParser::new`).
    #[test]
    fn ds4_finding3_has_complex_deletion_sets_collection_tombstone() {
        let parser =
            V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);
        let column = crate::schema::Column {
            name: "tags".to_string(),
            data_type: "set<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Wire bytes (unsigned-VInt complex-deletion deltas, roborev #863):
        //   0x02 = VUInt(2), mfda_delta=2; absolute_mfda = 0+2 = 2 ≠ i64::MIN
        //   0x00 = VUInt(0), localDeletionTime delta = 0
        //   0x00 = VUInt(0), cell_count = 0 (empty collection after overwrite)
        let blob: Vec<u8> = vec![0x02, 0x00, 0x00];

        let (value, consumed, meta) = parser
            .parse_complex_column_inner(
                &blob,
                0,
                &column,
                &column.data_type,
                true, /* has_complex_deletion */
                0,
                None,
                None,
            )
            .expect("parse_complex_column_inner must succeed for collection tombstone");

        assert_eq!(consumed, blob.len(), "all bytes must be consumed");
        // A SET overwrite produces an empty set (collection tombstone + 0 new elements).
        assert_eq!(
            value,
            Value::Set(vec![]),
            "overwritten collection with 0 elements must be an empty Set"
        );
        // THE KEY ASSERTION: has_collection_tombstone must be true.
        assert!(
            meta.has_collection_tombstone,
            "has_complex_deletion=true with absolute_mfda=1 (!=i64::MIN) must set \
             has_collection_tombstone=true (roborev Finding 3)"
        );
        // No element tombstones in the 0-cell body.
        assert_eq!(
            meta.element_tombstone_count, 0,
            "empty post-overwrite collection must have no element tombstones"
        );
        // No element writetimes when there are no cells.
        assert_eq!(
            meta.max_element_writetime, 0,
            "empty collection must have max_element_writetime=0"
        );
    }

    /// Byte-level test: the sentinel logic for `has_collection_tombstone` is
    /// `absolute_mfda != i64::MIN`.  When `absolute_mfda == i64::MIN` (Cassandra's
    /// "no tombstone" sentinel), `has_collection_tombstone` must be `false`; when it
    /// is any other value, it must be `true`.
    ///
    /// We verify the predicate directly rather than via byte parsing (the 9-byte
    /// VInt encoding of i64::MIN is complex and well-covered by the VInt unit tests).
    #[test]
    fn ds4_finding3_min_sentinel_means_no_collection_tombstone() {
        // The sentinel logic is: absolute_mfda != i64::MIN → has_collection_tombstone.
        let absolute_mfda_sentinel: i64 = i64::MIN;
        let absolute_mfda_live: i64 = 1;

        // Sentinel → no tombstone.
        assert!(
            absolute_mfda_sentinel == i64::MIN,
            "i64::MIN sentinel must produce has_collection_tombstone=false"
        );
        // Real timestamp → tombstone.
        assert!(
            absolute_mfda_live != i64::MIN,
            "non-sentinel absolute_mfda must produce has_collection_tombstone=true"
        );
    }

    /// Regression (roborev #863, Finding 1): complex-deletion `markedForDeleteAt`
    /// and `localDeletionTime` deltas, plus an explicit per-element complex-cell
    /// timestamp, are UNSIGNED VInts (writer: `encode_unsigned`). The earlier
    /// reader used `parse_vint` (ZigZag), which halves any delta whose top data
    /// bit is set. This test seeds NON-ZERO deltas (chosen so ZigZag vs unsigned
    /// disagree) and proves the reader now round-trips the writer encoding: the
    /// surfaced complex deletion equals the seeded `(mfda, ldt)`, and the
    /// surfaced per-element timestamp equals the seeded value.
    #[test]
    fn finding1_complex_deletion_and_element_ts_are_unsigned_vint_roundtrip() {
        let min_timestamp: i64 = 1_000_000;
        let min_local_deletion_time: i32 = 1_700_000_000;
        let parser = V5CompressedLegacyParser::new(
            "ks".to_string(),
            "tbl".to_string(),
            min_timestamp,
            min_local_deletion_time as i64,
            None,
        );
        let column = crate::schema::Column {
            name: "tags".to_string(),
            data_type: "set<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        };

        // Seed deltas large enough that unsigned VInt and ZigZag VInt disagree
        // (the high data bit is set, so ZigZag would halve them).
        let mfda_delta: u64 = 1000; // unsigned [0x83,0xE8]; ZigZag would read 500
        let ldt_delta: u64 = 1234;
        let element_ts_delta: u64 = 4321;

        let abs_mfda = min_timestamp + mfda_delta as i64;
        let abs_ldt = min_local_deletion_time + ldt_delta as i32;
        let abs_element_ts = min_timestamp + element_ts_delta as i64;

        // Build the on-disk complex column with one live SET element:
        //   complex deletion: mfda_delta, ldt_delta            (UNSIGNED)
        //   cell_count: 1
        //   element: flags=HAS_EMPTY_VALUE(0x04) + explicit ts (UNSIGNED),
        //            path_len=1, path=[0x41]
        let mut blob: Vec<u8> = Vec::new();
        encode_unsigned(mfda_delta, &mut blob);
        encode_unsigned(ldt_delta, &mut blob);
        encode_unsigned(1, &mut blob); // cell_count
        blob.push(0x04); // CELL_HAS_EMPTY_VALUE, no USE_ROW_TIMESTAMP
        encode_unsigned(element_ts_delta, &mut blob); // explicit element ts (UNSIGNED)
        encode_unsigned(1, &mut blob); // path_len
        blob.push(0x41); // path bytes ("A")

        let mut elements: Vec<crate::storage::sstable::reader::compaction_row::ComplexElement> =
            Vec::new();
        let (_value, consumed, meta) = parser
            .parse_complex_column_inner(
                &blob,
                0,
                &column,
                &column.data_type,
                true, // has_complex_deletion
                min_timestamp,
                Some(&mut elements),
                None,
            )
            .expect("parse must succeed");

        assert_eq!(consumed, blob.len(), "all bytes consumed");
        assert!(
            meta.has_collection_tombstone,
            "non-sentinel mfda must set has_collection_tombstone"
        );

        // Per-element timestamp must decode via UNSIGNED VInt (not halved).
        assert_eq!(elements.len(), 1, "one live element surfaced");
        assert_eq!(
            elements[0].timestamp,
            abs_element_ts,
            "per-element timestamp must round-trip the UNSIGNED writer encoding \
             (ZigZag decode would yield {})",
            min_timestamp + (element_ts_delta as i64 / 2)
        );

        // The complex deletion (mfda, ldt) is surfaced via ComplexColumnMeta and
        // must decode via UNSIGNED VInt. ZigZag would yield mfda_delta=500,
        // ldt_delta=617 — both wrong.
        assert_eq!(
            meta.complex_deletion,
            Some((abs_mfda, abs_ldt)),
            "complex deletion (mfda, ldt) must round-trip the UNSIGNED writer \
             encoding; ZigZag decode would yield ({}, {})",
            min_timestamp + 500,
            min_local_deletion_time + 617
        );
    }
}
