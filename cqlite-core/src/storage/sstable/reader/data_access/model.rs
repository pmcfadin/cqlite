//! Shared model types and free helpers for the data-access read paths.
//!
//! This module holds the small, path-agnostic pieces the BTI, sequential, and
//! compaction read paths all build on: the clustering-slice model, the BTI
//! lookup-step decision, table-id matching, token-order sorting, and the
//! `scan_for_key` invocation counter. Each read-path submodule (`bti`,
//! `sequential`, `compaction`) reaches these via `super::model::*`.

use crate::types::{CellWriteMetadata, TableId, Value};
use crate::util::cassandra_murmur3::cassandra_murmur3_token;
#[cfg(not(feature = "tombstones"))]
use crate::Result;
use crate::RowKey;

/// A single-column clustering-key restriction pushed down to a within-partition
/// seek (Issue #954, Epic #951).
///
/// Carries the decoded clustering bound `Value`(s) for the FIRST clustering
/// column (multi-column prefixes are a documented follow-up). Each bound is one
/// clustering component; an empty `Vec` means that side is OPEN (unbounded). The
/// `_inclusive` flags distinguish `>=`/`<=` (inclusive) from `>`/`<` (exclusive)
/// — they are advisory for byte-window selection (block selection is
/// over-inclusive by block granularity; the post-scan `evaluate_leaf` backstop
/// applies the exact bound), so a slightly wider byte window never changes the
/// final result.
///
/// `ck = v` is represented as `start == end == [v]`, both inclusive.
#[derive(Debug, Clone)]
pub struct ClusteringSlice {
    /// Lower bound clustering component(s); empty = unbounded below.
    pub start: Vec<Value>,
    /// Whether the lower bound is inclusive (`>=`/`=`) vs exclusive (`>`).
    pub start_inclusive: bool,
    /// Upper bound clustering component(s); empty = unbounded above.
    pub end: Vec<Value>,
    /// Whether the upper bound is inclusive (`<=`/`=`) vs exclusive (`<`).
    pub end_inclusive: bool,
}

/// The within-partition row-body byte window selected by the BTI row index for a
/// clustering slice (Issue #954). Offsets are RELATIVE to the partition start —
/// the same domain the parser sees for `window[within..]`.
#[cfg(not(feature = "tombstones"))]
#[derive(Debug, Clone, Copy)]
pub(super) struct ClusteringRowWindow {
    /// First byte of the row body to parse (inclusive), relative to partition
    /// start. `0` decodes from the partition body start (used when statics exist).
    pub(super) body_start_rel: usize,
    /// Exclusive end of the row body to parse, relative to partition start;
    /// `usize::MAX` means "to the partition end" (clamped by the caller).
    pub(super) body_end_rel: usize,
}

/// Length of the all-`0xFF` sentinel used to represent an OPEN upper clustering
/// bound (+∞) for byte-comparable block selection (Issue #954). Any separator in
/// `Rows.db` is shorter or sorts below an all-`0xFF` run of this length, so it
/// reliably selects through the last block. 64 bytes comfortably exceeds any
/// realistic single-column clustering separator width.
#[cfg(not(feature = "tombstones"))]
pub(super) const MAX_OSS50_BOUND_SENTINEL_LEN: usize = 64;

/// Normalize a CQL [`ClusteringSlice`] into the `(physical_lower, physical_upper)`
/// byte-comparable bounds that [`select_row_index_blocks_for_range`] consumes
/// (issue #954 High-severity correctness fix).
///
/// [`select_row_index_blocks_for_range`] selects blocks purely in **physical**
/// (on-disk, byte-comparable) order — the order `Rows.db` separators are stored
/// in. The encoder ([`encode_clustering_bound_oss50_with_order`]) already inverts
/// every byte of a DESC component (`ReversedType` / `ByteSource.invert`), so for a
/// DESC first clustering column an ASCENDING byte order corresponds to a
/// DESCENDING CQL value order. The consequence:
///
/// - **ASC** (`is_reversed[0] == false`): CQL `start` (the `>=`/`>` lower value
///   bound) IS the physical-lower bound and CQL `end` (the `<=`/`<` upper value
///   bound) IS the physical-upper bound. No swap.
/// - **DESC** (`is_reversed[0] == true`): the CQL lower-value bound encodes to the
///   physically GREATER bytes and the CQL upper-value bound to the physically
///   SMALLER bytes, so the roles SWAP. A CQL `ck >= v` (lower) selects the rows
///   with the largest values, which sit at the physical-LOW byte side through
///   `enc(v)`; a CQL `ck < v` (upper) selects the physical-HIGH side. The open
///   sentinels swap accordingly: an open CQL lower (`-∞` value) maps to physical
///   `+∞` and an open CQL upper (`+∞` value) maps to physical `-∞`.
///
/// Block selection is over-inclusive by block granularity and the post-scan
/// `evaluate_leaf` backstop re-applies the exact CQL bound by VALUE, so the
/// inclusivity of each bound does not need separate handling here — the physical
/// window only has to be a SUPERSET of the matching rows. (Swapping the roles is
/// what guarantees the superset for DESC; the previous code built
/// `[enc(lower), +∞]` for DESC, which excluded the matching low-byte rows and the
/// backstop could not recover rows that were never decoded.)
///
/// Returns:
/// - `Ok(Some((lower, upper)))` — usable physical bounds.
/// - `Ok(None)` — a bound's type is not byte-comparable-encodable here, so the
///   narrowing is unsafe and the caller must decode the whole partition.
/// - `Err(_)` — never (kept `Result` for call-site symmetry); reserved.
#[cfg(not(feature = "tombstones"))]
pub(super) fn physical_byte_bounds_for_slice(
    slice: &ClusteringSlice,
    is_reversed: &[bool],
) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    use crate::storage::sstable::bti::encode_clustering_bound_oss50_with_order;

    // The physical-low sentinel: empty sorts before every separator (-∞).
    let neg_inf = Vec::<u8>::new();
    // The physical-high sentinel: an all-0xFF run sorts after every separator (+∞).
    let pos_inf = || vec![0xFFu8; MAX_OSS50_BOUND_SENTINEL_LEN];

    // Encode a closed CQL bound to its physical byte-comparable form; `None`
    // bubbles up as an un-encodable-bound fallback at the call site.
    let encode = |values: &[Value]| -> Option<Vec<u8>> {
        encode_clustering_bound_oss50_with_order(values, is_reversed).ok()
    };

    // The FIRST clustering column's order decides the value↔byte direction. A
    // missing entry (no schema / fewer entries) is ascending, matching the encoder.
    let first_desc = is_reversed.first().copied().unwrap_or(false);

    // CQL lower bound (`>=`/`>` / equality lower) → its physical byte image.
    let cql_lower_bytes = if slice.start.is_empty() {
        None // open CQL lower (value -∞)
    } else {
        match encode(&slice.start) {
            Some(b) => Some(b),
            None => return Ok(None),
        }
    };
    // CQL upper bound (`<=`/`<` / equality upper) → its physical byte image.
    let cql_upper_bytes = if slice.end.is_empty() {
        None // open CQL upper (value +∞)
    } else {
        match encode(&slice.end) {
            Some(b) => Some(b),
            None => return Ok(None),
        }
    };

    let (phys_lower, phys_upper) = if first_desc {
        // DESC: CQL upper-value bound → physical-lower bytes; CQL lower-value bound
        // → physical-upper bytes. Open CQL upper → physical -∞; open CQL lower →
        // physical +∞.
        let lower = cql_upper_bytes.unwrap_or(neg_inf);
        let upper = cql_lower_bytes.unwrap_or_else(pos_inf);
        (lower, upper)
    } else {
        // ASC: CQL lower-value bound → physical-lower bytes; CQL upper-value bound
        // → physical-upper bytes (the original mapping).
        let lower = cql_lower_bytes.unwrap_or(neg_inf);
        let upper = cql_upper_bytes.unwrap_or_else(pos_inf);
        (lower, upper)
    };

    Ok(Some((phys_lower, phys_upper)))
}

/// Counter of `scan_for_key` invocations, used by tests to prove the BTI
/// point-lookup path never falls through to a sequential scan (issue #831).
///
/// Incremented at the top of [`SSTableReader::scan_for_key`] and read via
/// [`SSTableReader::scan_for_key_call_count`]. The increment is a single
/// `Relaxed` atomic add on a cold path, so the runtime cost is negligible; it is
/// not gated behind `cfg(test)` because integration tests in the `tests/`
/// directory compile against the library crate without its `test` cfg.
///
/// [`SSTableReader::scan_for_key`]: crate::storage::sstable::SSTableReader
/// [`SSTableReader::scan_for_key_call_count`]: crate::storage::sstable::SSTableReader::scan_for_key_call_count
pub(crate) static SCAN_FOR_KEY_CALLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Compare two table IDs, handling both qualified (keyspace.table) and unqualified (table) formats.
///
/// This function allows flexible matching:
/// - "keyspace.table" matches "keyspace.table" (exact match)
/// - "table" matches "keyspace.table" (unqualified matches qualified)
/// - "keyspace.table" matches "table" (qualified matches unqualified)
/// - "table" matches "table" (exact match)
///
/// This is necessary because:
/// - Dataset mode SSTables store qualified table_ids (e.g., "test_basic.simple_table")
/// - Queries can use either qualified ("test_basic.simple_table") or unqualified ("simple_table") names
/// - Production SSTables may use unqualified table_ids
pub(in crate::storage::sstable::reader) fn table_ids_match(
    entry_table_id: &TableId,
    query_table_id: &TableId,
) -> bool {
    let entry_name = entry_table_id.name();
    let query_name = query_table_id.name();

    // Fast path: exact match
    if entry_name == query_name {
        return true;
    }

    // Extract unqualified table names for comparison
    let entry_unqualified = if let Some(dot_pos) = entry_name.rfind('.') {
        &entry_name[dot_pos + 1..]
    } else {
        entry_name
    };

    let query_unqualified = if let Some(dot_pos) = query_name.rfind('.') {
        &query_name[dot_pos + 1..]
    } else {
        query_name
    };

    // Match if unqualified names are the same
    entry_unqualified == query_unqualified
}

/// Stricter table-id match used by the BTI point-lookup guard (issue #831 review).
///
/// [`table_ids_match`] matches on the unqualified table name, so it treats
/// `ks_a.users` and `ks_b.users` as equal — fine for index lookups that are
/// already scoped to one table, but too permissive as a defensive guard against
/// a fully-qualified wrong-keyspace query. When BOTH ids are qualified
/// (`keyspace.table`), this requires exact `keyspace.table` equality; it only
/// falls back to the permissive unqualified match when one side lacks a
/// keyspace (preserving qualified-vs-unqualified flexibility).
pub(super) fn table_ids_match_strict(entry_table_id: &TableId, query_table_id: &TableId) -> bool {
    let entry_qualified = entry_table_id.name().contains('.');
    let query_qualified = query_table_id.name().contains('.');
    if entry_qualified && query_qualified {
        entry_table_id.name() == query_table_id.name()
    } else {
        table_ids_match(entry_table_id, query_table_id)
    }
}

/// Seek-path table-id consistency check (issue #1284, hardened per #1284 review).
///
/// The single-partition SEEK (`bti_decompress_and_parse_target_all`) builds
/// `entry_table_id` from THIS reader's AUTHORITATIVE serialization header
/// (`{header.keyspace}.{header.table_name}`). `query_table_id` is the query's id.
///
/// `fully_qualified_match` records HOW the manager resolved this reader for the
/// query (`resolve_reader_list`):
///   - `true`  — the query's fully-qualified `keyspace.table` key matched the
///     reader map EXACTLY (path-authoritative: this reader genuinely IS the
///     queried table), or the query was unqualified.
///   - `false` — the query was fully qualified but reached this reader via the
///     bare/unqualified NAME fallback (its keyspace did NOT match any map key).
///
/// [`table_ids_match_strict`] additionally requires the *keyspace* to match when
/// BOTH ids are fully qualified — the correct `get()` defensive guard (#831). For
/// a seek it falsely REJECTS every row when the reader's HEADER keyspace differs
/// from the query keyspace even though resolution was an exact FQ match (e.g. a
/// writer-produced header, or a header whose embedded keyspace is not the
/// path-derived one), silently zeroing the result and forcing a full-scan
/// fallback (#1284).
///
/// The relaxation (accept on a consistent TABLE NAME despite a header-keyspace
/// divergence) is therefore gated on `fully_qualified_match`:
///   - Exact FQ match (or unqualified query): the reader is authoritatively the
///     target table, so a header-keyspace divergence is benign — accept when the
///     table names agree, reject a different table name (the #831 wrong-table
///     guard survives via [`table_ids_match`]). This is #1284's actual goal.
///   - FQ query reached via the bare-name FALLBACK: the keyspace genuinely did
///     NOT match, so a relaxed name-only check could surface rows from ANOTHER
///     keyspace whose table name collides — keep STRICT keyspace matching
///     ([`table_ids_match_strict`]) and REJECT the wrong-keyspace rows.
///
/// This is no-heuristic: it relies only on the authoritative header identity, the
/// query id, and the authoritative resolution mode — never on guessing.
///
/// Used by BOTH the single-partition SEEK (`bti_collect_partition_rows`,
/// `not(tombstones)` only) and the `get()` POINT-LOOKUP decoder
/// (`bti_decompress_and_parse_target`, all feature builds, issue #1321). The
/// point-lookup caller exists in every build, so this helper is NOT gated.
pub(super) fn table_header_consistent_for_seek(
    entry_table_id: &TableId,
    query_table_id: &TableId,
    fully_qualified_match: bool,
) -> bool {
    // Only relax to the name-only consistency check for a fully-qualified query
    // when resolution was an EXACT fully-qualified match (the reader is
    // path-authoritatively the queried table). A fully-qualified query that
    // reached this reader via the bare-name fallback keeps STRICT keyspace
    // matching, so it can never return rows from a different keyspace whose table
    // name collides (#1284 review correctness fix).
    if fully_qualified_match && query_table_id.name().contains('.') {
        table_ids_match(entry_table_id, query_table_id)
    } else {
        table_ids_match_strict(entry_table_id, query_table_id)
    }
}

/// Per-iteration decision for the BTI chunk-targeted point-lookup loop.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum BtiLookupStep {
    /// The full partition-key prefix is buffered and matches the queried key —
    /// parse the partition.
    Parse,
    /// The header/key prefix straddles a chunk boundary and is not yet fully
    /// buffered — read the next chunk before parsing (chunk-targeted path only).
    PullNextChunk,
    /// Treat the partition as absent: either the buffered key prefix does not
    /// match, or a whole-section window is structurally too short to grow.
    Absent,
}

/// Decide what the BTI point-lookup loop should do for the current window state.
///
/// Pure so the chunk-straddle control flow is unit-testable without a real
/// multi-chunk BTI fixture (DataOffset partitions are narrow and fit in one
/// chunk, so a boundary-straddling header cannot be produced by the available
/// fixtures). Crucially, when the key prefix is not yet buffered on the
/// chunk-targeted path this returns [`BtiLookupStep::PullNextChunk`] — it must
/// NOT lead to parsing a truncated header (issue #831 review).
pub(super) fn bti_lookup_step(
    key_prefix_available: bool,
    key_matches: bool,
    chunk_targeted: bool,
) -> BtiLookupStep {
    if key_prefix_available {
        if key_matches {
            BtiLookupStep::Parse
        } else {
            BtiLookupStep::Absent
        }
    } else if chunk_targeted {
        BtiLookupStep::PullNextChunk
    } else {
        BtiLookupStep::Absent
    }
}

/// Sort a result slice in ascending Cassandra token order.
///
/// The authoritative ordering for SSTable partitions is ascending Murmur3 token, with
/// equal-token ties broken by raw key bytes (lexicographic). This matches the on-disk
/// physical order (spec §5, Appendix B §313) and the write engine's `PartitionPosition::cmp`.
///
/// Computes each key's token once to avoid O(n log n) recomputation inside the comparator.
pub(super) fn sort_by_token_order(results: &mut Vec<(RowKey, Value)>) {
    // Map to (token, RowKey, Value), sort, then reassemble.
    let mut tagged: Vec<(i64, RowKey, Value)> = results
        .drain(..)
        .map(|(k, v)| {
            let t = cassandra_murmur3_token(k.as_bytes());
            (t, k, v)
        })
        .collect();
    tagged.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    results.extend(tagged.into_iter().map(|(_, k, v)| (k, v)));
}

/// Sort `(RowKey, Value, CellMeta)` triples by Cassandra Murmur3 token order.
pub(super) fn sort_by_token_order_with_meta(
    results: &mut Vec<(
        RowKey,
        Value,
        std::collections::HashMap<String, CellWriteMetadata>,
    )>,
) {
    let mut tagged: Vec<(
        i64,
        RowKey,
        Value,
        std::collections::HashMap<String, CellWriteMetadata>,
    )> = results
        .drain(..)
        .map(|(k, v, m)| {
            let t = cassandra_murmur3_token(k.as_bytes());
            (t, k, v, m)
        })
        .collect();
    tagged.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    results.extend(tagged.into_iter().map(|(_, k, v, m)| (k, v, m)));
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // table_ids_match tests
    // =========================================================================

    #[test]
    fn test_table_ids_match_strict_keyspace_aware() {
        let a = TableId::new("ks_a.users".to_string());
        let b = TableId::new("ks_b.users".to_string());
        // Both qualified, different keyspace, same table name → must NOT match
        // (the permissive helper would match these).
        assert!(table_ids_match(&a, &b), "permissive helper matches on name");
        assert!(
            !table_ids_match_strict(&a, &b),
            "strict guard must reject a wrong-keyspace same-name query"
        );
        // Both qualified, identical → match.
        let a2 = TableId::new("ks_a.users".to_string());
        assert!(table_ids_match_strict(&a, &a2));
        // One side unqualified → fall back to permissive name match.
        let unq = TableId::new("users".to_string());
        assert!(table_ids_match_strict(&a, &unq));
        assert!(table_ids_match_strict(&unq, &a));
    }

    /// Issue #1284 (hardened per review): the SEEK consistency check relaxes to a
    /// name-only match across a header-keyspace divergence ONLY when resolution
    /// was an EXACT fully-qualified match. A fully-qualified query that reached
    /// the reader via the bare-name FALLBACK keeps STRICT keyspace matching, so a
    /// wrong-keyspace query can never return another keyspace's same-named rows.
    /// A different table name always rejects (the #831 wrong-table guard).
    #[test]
    fn test_table_header_consistent_for_seek_gates_on_resolution_mode() {
        // The seek builds `entry` from the SSTable's authoritative header.
        let header = TableId::new("ks_a.users".to_string());

        // 1. EXACT fully-qualified match, header keyspace MATCHES, same table:
        //    ACCEPT (seek engages — the genuine #1284 goal, no regression).
        let query_same = TableId::new("ks_a.users".to_string());
        assert!(
            table_header_consistent_for_seek(&header, &query_same, /*fq_match=*/ true),
            "exact FQ match with a consistent header keyspace+table must accept (seek engages)"
        );

        // 1b. EXACT fully-qualified match, header keyspace DIFFERS but resolution
        //     was path-authoritative (the benign #1284 divergence): ACCEPT on the
        //     consistent table name. The strict guard alone would wrongly reject.
        let query_div_exact = TableId::new("ks_b.users".to_string());
        assert!(
            !table_ids_match_strict(&header, &query_div_exact),
            "precondition: the strict guard rejects a different-keyspace same-name query"
        );
        assert!(
            table_header_consistent_for_seek(&header, &query_div_exact, /*fq_match=*/ true),
            "Issue #1284: an EXACT fully-qualified resolution must accept rows when only the \
             header keyspace diverges and the table name is consistent"
        );

        // 2. Fully-qualified query, header keyspace DIFFERS, reached via the
        //    bare-name FALLBACK (fq_match = false), SAME table name: REJECT — the
        //    keyspaces genuinely differ, so accepting would surface another
        //    keyspace's same-named rows (#1284 review correctness fix).
        let query_other_ks = TableId::new("ks_b.users".to_string());
        assert!(
            !table_header_consistent_for_seek(&header, &query_other_ks, /*fq_match=*/ false),
            "Issue #1284 review: a fully-qualified query resolved via the bare-name fallback must \
             NOT accept rows from a reader whose header keyspace differs (no wrong-keyspace rows)"
        );

        // 3. DIFFERENT table name: REJECT regardless of resolution mode (the #831
        //    wrong-table guard must survive the relaxation).
        let query_wrong_table = TableId::new("ks_b.accounts".to_string());
        assert!(
            !table_header_consistent_for_seek(&header, &query_wrong_table, /*fq_match=*/ true),
            "Issue #831: a genuinely different table name must still be rejected (exact match)"
        );
        assert!(
            !table_header_consistent_for_seek(
                &header,
                &query_wrong_table,
                /*fq_match=*/ false
            ),
            "Issue #831: a genuinely different table name must still be rejected (fallback)"
        );

        // 4. UNqualified query: defers to the strict guard's permissive name match
        //    (resolution mode irrelevant — there is no keyspace to mismatch).
        let query_unqualified = TableId::new("users".to_string());
        assert!(table_header_consistent_for_seek(
            &header,
            &query_unqualified,
            /*fq_match=*/ false
        ));
        let query_unqualified_wrong = TableId::new("accounts".to_string());
        assert!(!table_header_consistent_for_seek(
            &header,
            &query_unqualified_wrong,
            /*fq_match=*/ true
        ));
    }

    /// Issue #1321: the `get()` POINT-LOOKUP decoder
    /// (`bti_decompress_and_parse_target`) now applies the SAME
    /// resolution-mode-aware guard the SEEK path adopted in #1284 — replacing its
    /// previous unconditional `table_ids_match_strict`. This exercises the exact
    /// row-acceptance predicate the get path evaluates per emitted partition:
    ///   - ACCEPT: an EXACT fully-qualified resolution with a consistent table name
    ///     even when the reader's header keyspace diverges (the #1321 goal);
    ///   - REJECT (fallback): a fully-qualified query that reached the reader via
    ///     the bare-name fallback whose header keyspace differs — no wrong-keyspace
    ///     rows on get() either;
    ///   - REJECT (different table): a genuinely different table name (#831).
    #[test]
    fn test_get_point_lookup_guard_gates_on_resolution_mode() {
        // The get() decoder builds `entry` (`tid`) from the parser-emitted partition,
        // whose table id is this reader's authoritative serialization header.
        let header = TableId::new("ks_a.users".to_string());

        // ACCEPT: exact FQ match, header keyspace diverges, same table name. The old
        // strict-only get guard would WRONGLY reject this (the #1321 false rejection).
        let query_div_exact = TableId::new("ks_b.users".to_string());
        assert!(
            !table_ids_match_strict(&header, &query_div_exact),
            "precondition: the old get() guard (strict) rejects this divergence"
        );
        assert!(
            table_header_consistent_for_seek(&header, &query_div_exact, /*fq_match=*/ true),
            "Issue #1321: get() must accept rows when resolution was an exact FQ match \
             and only the header keyspace diverges"
        );

        // REJECT (fallback): fully-qualified query resolved via the bare-name
        // fallback, header keyspace differs — must NOT surface another keyspace's rows.
        assert!(
            !table_header_consistent_for_seek(&header, &query_div_exact, /*fq_match=*/ false),
            "Issue #1321: a fully-qualified get() resolved via the bare-name fallback must \
             still REJECT a wrong-keyspace reader (strict keyspace match preserved)"
        );

        // REJECT (different table): a genuinely different table never matches,
        // regardless of resolution mode (#831 wrong-table guard survives).
        let query_wrong_table = TableId::new("ks_a.accounts".to_string());
        assert!(
            !table_header_consistent_for_seek(&header, &query_wrong_table, /*fq_match=*/ true),
            "Issue #831: get() must still reject a different table name (exact match)"
        );
        assert!(
            !table_header_consistent_for_seek(
                &header,
                &query_wrong_table,
                /*fq_match=*/ false
            ),
            "Issue #831: get() must still reject a different table name (fallback)"
        );

        // Sanity: the existing strict default (fq_match=false) the unchanged per-reader
        // `get()` callers pass is exactly today's behavior for a consistent FQ query.
        let query_same = TableId::new("ks_a.users".to_string());
        assert!(table_header_consistent_for_seek(
            &header,
            &query_same,
            /*fq_match=*/ false
        ));
    }

    #[test]
    fn test_bti_lookup_step_decision() {
        // Key prefix buffered and matches → parse.
        assert_eq!(bti_lookup_step(true, true, true), BtiLookupStep::Parse);
        assert_eq!(bti_lookup_step(true, true, false), BtiLookupStep::Parse);
        // Key prefix buffered but does NOT match → absent (prefix collision).
        assert_eq!(bti_lookup_step(true, false, true), BtiLookupStep::Absent);
        assert_eq!(bti_lookup_step(true, false, false), BtiLookupStep::Absent);
        // Key prefix NOT yet buffered (header straddles a chunk boundary):
        //  - chunk-targeted path MUST pull the next chunk, never parse a
        //    truncated header (issue #831 review regression);
        assert_eq!(
            bti_lookup_step(false, false, true),
            BtiLookupStep::PullNextChunk
        );
        //  - whole-section fallback cannot grow → absent.
        assert_eq!(bti_lookup_step(false, false, false), BtiLookupStep::Absent);
    }

    // =========================================================================
    // physical_byte_bounds_for_slice — DESC clustering normalization (issue #954)
    // =========================================================================

    /// Build a [`ClusteringSlice`] over a single integer clustering column.
    #[cfg(not(feature = "tombstones"))]
    fn int_slice(
        start: Option<i64>,
        start_inclusive: bool,
        end: Option<i64>,
        end_inclusive: bool,
    ) -> ClusteringSlice {
        ClusteringSlice {
            start: start
                .map(|v| vec![Value::Integer(v as i32)])
                .unwrap_or_default(),
            start_inclusive,
            end: end
                .map(|v| vec![Value::Integer(v as i32)])
                .unwrap_or_default(),
            end_inclusive,
        }
    }

    /// The physical byte image of a single-int clustering bound under an order.
    #[cfg(not(feature = "tombstones"))]
    fn enc_int(v: i32, reversed: bool) -> Vec<u8> {
        crate::storage::sstable::bti::encode_clustering_bound_oss50_with_order(
            &[Value::Integer(v)],
            &[reversed],
        )
        .expect("int encodes")
    }

    /// ASC: the physical bounds are the CQL bounds, no swap.
    #[cfg(not(feature = "tombstones"))]
    #[test]
    fn physical_bounds_asc_no_swap() {
        // ck >= 100 AND ck < 110
        let slice = int_slice(Some(100), true, Some(110), false);
        let (lower, upper) = physical_byte_bounds_for_slice(&slice, &[false])
            .expect("ok")
            .expect("encodable");
        assert_eq!(lower, enc_int(100, false), "ASC physical-lower = enc(100)");
        assert_eq!(upper, enc_int(110, false), "ASC physical-upper = enc(110)");
        // The physical range is well-ordered (lower <= upper) so block selection
        // returns a non-empty window for an in-range slice.
        assert!(lower <= upper, "ASC physical bounds must be ordered");
    }

    /// DESC: the CQL lower/upper roles SWAP into physical order, and the result is
    /// still a well-ordered `[phys_lower, phys_upper]` (the bug produced a
    /// REVERSED, empty-selecting range).
    #[cfg(not(feature = "tombstones"))]
    #[test]
    fn physical_bounds_desc_swaps_roles() {
        // ck >= 100 AND ck < 110 on a DESC column.
        let slice = int_slice(Some(100), true, Some(110), false);
        let (lower, upper) = physical_byte_bounds_for_slice(&slice, &[true])
            .expect("ok")
            .expect("encodable");
        // Physical-lower comes from the CQL UPPER bound (enc_desc(110)); physical-
        // upper from the CQL LOWER bound (enc_desc(100)).
        assert_eq!(
            lower,
            enc_int(110, true),
            "DESC physical-lower must come from the CQL upper bound (110)"
        );
        assert_eq!(
            upper,
            enc_int(100, true),
            "DESC physical-upper must come from the CQL lower bound (100)"
        );
        // The whole point: under DESC the swapped bounds are well-ordered. With the
        // un-swapped (buggy) mapping the range would be [enc_desc(100),
        // enc_desc(110)] which is REVERSED (enc_desc(100) > enc_desc(110)) and
        // `select_row_index_blocks_for_range` returns EMPTY → dropped rows.
        assert!(
            lower < upper,
            "DESC swapped physical bounds must be ordered (lower < upper); \
             got lower={lower:?} upper={upper:?}"
        );
        assert!(
            enc_int(100, true) > enc_int(110, true),
            "sanity: under DESC, enc(100) sorts AFTER enc(110) in physical bytes — \
             the un-swapped mapping would build a reversed (empty) range"
        );
    }

    /// DESC single-bound `ck >= v` (open CQL upper): the matching values (v and
    /// larger) all sort to the physical-LOW byte side (DESC inverts bytes), so the
    /// physical window is `[-∞, enc_desc(v)]`. (The buggy un-swapped code built
    /// `[enc_desc(v), +∞]`, which EXCLUDED exactly those low-byte matching rows.)
    #[cfg(not(feature = "tombstones"))]
    #[test]
    fn physical_bounds_desc_lower_bound_only() {
        // ck >= 290, open above.
        let slice = int_slice(Some(290), true, None, false);
        let (lower, upper) = physical_byte_bounds_for_slice(&slice, &[true])
            .expect("ok")
            .expect("encodable");
        assert_eq!(
            lower,
            Vec::<u8>::new(),
            "DESC `ck >= 290`: open CQL upper → physical -∞ (the matching large \
             values sort to the LOW physical-byte side)"
        );
        assert_eq!(
            upper,
            enc_int(290, true),
            "DESC `ck >= 290`: physical-upper = enc_desc(290) (the boundary value)"
        );
        assert!(lower < upper, "must be ordered");
        // Crucial: enc_desc(290) sorts ABOVE enc_desc(299), so [-∞, enc_desc(290)]
        // includes enc_desc(299) — the buggy [enc_desc(290), +∞] would NOT.
        assert!(
            enc_int(299, true) < enc_int(290, true),
            "sanity: larger DESC value has smaller bytes"
        );
        assert!(
            enc_int(299, true).as_slice() <= upper.as_slice(),
            "the physical window must include enc_desc(299), a matching row"
        );
    }

    /// DESC single-bound `ck < v` (open CQL lower): the matching values (smaller
    /// than v) sort to the physical-HIGH byte side, so the physical window is
    /// `[enc_desc(v), +∞]`.
    #[cfg(not(feature = "tombstones"))]
    #[test]
    fn physical_bounds_desc_upper_bound_only() {
        // ck < 20, open below.
        let slice = int_slice(None, false, Some(20), false);
        let (lower, upper) = physical_byte_bounds_for_slice(&slice, &[true])
            .expect("ok")
            .expect("encodable");
        assert_eq!(
            lower,
            enc_int(20, true),
            "DESC `ck < 20`: physical-lower = enc_desc(20) (the boundary value)"
        );
        assert_eq!(
            upper,
            vec![0xFFu8; MAX_OSS50_BOUND_SENTINEL_LEN],
            "DESC `ck < 20`: open CQL lower → physical +∞ sentinel"
        );
        assert!(lower < upper, "must be ordered");
        // A matching small value (ck=0) has the LARGEST DESC bytes, inside the
        // window; the buggy [-∞, enc_desc(20)] mapping would exclude it.
        assert!(
            enc_int(0, true).as_slice() >= lower.as_slice(),
            "the physical window must include enc_desc(0), a matching row"
        );
    }

    /// DESC equality `ck = v`: start == end == [v]; physical bounds collapse to
    /// [enc_desc(v), enc_desc(v)] (a single-point range, identical to ASC since
    /// the swap of equal endpoints is a no-op).
    #[cfg(not(feature = "tombstones"))]
    #[test]
    fn physical_bounds_desc_equality_is_point() {
        let slice = int_slice(Some(150), true, Some(150), true);
        let (lower, upper) = physical_byte_bounds_for_slice(&slice, &[true])
            .expect("ok")
            .expect("encodable");
        assert_eq!(lower, enc_int(150, true));
        assert_eq!(upper, enc_int(150, true));
        assert_eq!(lower, upper, "equality is a single physical point");
    }

    /// Absent schema / empty `is_reversed` is treated as ASC (no swap), matching
    /// the encoder's default.
    #[cfg(not(feature = "tombstones"))]
    #[test]
    fn physical_bounds_no_order_defaults_ascending() {
        let slice = int_slice(Some(5), true, Some(50), false);
        let (lower, upper) = physical_byte_bounds_for_slice(&slice, &[])
            .expect("ok")
            .expect("encodable");
        assert_eq!(lower, enc_int(5, false));
        assert_eq!(upper, enc_int(50, false));
        assert!(lower < upper);
    }

    #[test]
    fn test_table_ids_match_exact() {
        // Exact match cases
        let id1 = TableId::new("simple_table".to_string());
        let id2 = TableId::new("simple_table".to_string());
        assert!(table_ids_match(&id1, &id2));

        let id3 = TableId::new("test_basic.simple_table".to_string());
        let id4 = TableId::new("test_basic.simple_table".to_string());
        assert!(table_ids_match(&id3, &id4));
    }

    #[test]
    fn test_table_ids_match_qualified_vs_unqualified() {
        // Qualified matches unqualified
        let qualified = TableId::new("test_basic.simple_table".to_string());
        let unqualified = TableId::new("simple_table".to_string());

        assert!(table_ids_match(&qualified, &unqualified));
        assert!(table_ids_match(&unqualified, &qualified));
    }

    #[test]
    fn test_table_ids_match_different_keyspaces() {
        // Different keyspaces but same table name - should match on table name
        let id1 = TableId::new("keyspace1.users".to_string());
        let id2 = TableId::new("keyspace2.users".to_string());

        assert!(
            table_ids_match(&id1, &id2),
            "Same table name should match across keyspaces"
        );
    }

    #[test]
    fn test_table_ids_match_completely_different() {
        // Completely different tables - should not match
        let id1 = TableId::new("users".to_string());
        let id2 = TableId::new("orders".to_string());

        assert!(!table_ids_match(&id1, &id2));

        let id3 = TableId::new("test.users".to_string());
        let id4 = TableId::new("test.orders".to_string());

        assert!(!table_ids_match(&id3, &id4));
    }

    #[test]
    fn test_table_ids_match_edge_cases() {
        // Table names with dots (unusual but possible)
        let id1 = TableId::new("schema.table.subtable".to_string());
        let id2 = TableId::new("subtable".to_string());

        assert!(
            table_ids_match(&id1, &id2),
            "Should match on last component"
        );
    }

    #[test]
    fn test_table_ids_match_empty() {
        // Empty table IDs
        let id1 = TableId::new("".to_string());
        let id2 = TableId::new("".to_string());

        assert!(table_ids_match(&id1, &id2), "Empty IDs should match");
    }

    // =========================================================================
    // Key comparison tests
    // =========================================================================

    #[test]
    fn test_row_key_comparison() {
        let key1 = RowKey::new(vec![1, 2, 3]);
        let key2 = RowKey::new(vec![1, 2, 3]);
        let key3 = RowKey::new(vec![1, 2, 4]);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert!(key1 < key3);
    }

    #[test]
    fn test_row_key_ordering() {
        let key_a = RowKey::new(vec![0x01]);
        let key_b = RowKey::new(vec![0x02]);
        let key_c = RowKey::new(vec![0x01, 0x00]); // Longer but starts with 0x01

        assert!(key_a < key_b);
        assert!(key_a < key_c); // Shorter prefix comes first in lexicographic order
    }

    // =========================================================================
    // Value tests
    // =========================================================================

    #[test]
    fn test_value_blob_creation() {
        let data = vec![1, 2, 3, 4, 5];
        let value = Value::Blob(data.clone());

        if let Value::Blob(v) = value {
            assert_eq!(v, data);
        } else {
            panic!("Expected Value::Blob");
        }
    }
}
