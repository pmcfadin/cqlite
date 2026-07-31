//! Unit tests for [`super::model`] — the shared, path-agnostic data-access
//! helpers (clustering-slice normalization, the BTI lookup-step decision,
//! table-id matching, row-key ordering).
//!
//! Split out of `model.rs` under the campsite rule (epic #1116): the test matrix
//! is several times the size of the code it covers, and `model.rs` was already
//! over the 800-line source target.

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
        !table_header_consistent_for_seek(&header, &query_wrong_table, /*fq_match=*/ false),
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
        !table_header_consistent_for_seek(&header, &query_wrong_table, /*fq_match=*/ false),
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

/// `bytes` as the PHYSICAL-LOW bound: Cassandra's `LT_NEXT_COMPONENT`
/// terminator (`ClusteringPrefix.Kind.INCL_START_BOUND`) appended.
#[cfg(not(feature = "tombstones"))]
fn lo(mut bytes: Vec<u8>) -> Vec<u8> {
    bytes.push(OSS50_LT_NEXT_COMPONENT);
    bytes
}

/// `bytes` as the PHYSICAL-HIGH bound: Cassandra's `GT_NEXT_COMPONENT`
/// terminator (`ClusteringPrefix.Kind.INCL_END_BOUND`) appended.
#[cfg(not(feature = "tombstones"))]
fn hi(mut bytes: Vec<u8>) -> Vec<u8> {
    bytes.push(OSS50_GT_NEXT_COMPONENT);
    bytes
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
    assert_eq!(
        lower,
        lo(enc_int(100, false)),
        "ASC physical-lower = enc(100) + LT_NEXT_COMPONENT"
    );
    assert_eq!(
        upper,
        hi(enc_int(110, false)),
        "ASC physical-upper = enc(110) + GT_NEXT_COMPONENT"
    );
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
        lo(enc_int(110, true)),
        "DESC physical-lower must come from the CQL upper bound (110)"
    );
    assert_eq!(
        upper,
        hi(enc_int(100, true)),
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
        lo(Vec::<u8>::new()),
        "DESC `ck >= 290`: open CQL upper → physical -∞ (the matching large \
             values sort to the LOW physical-byte side); a lone LT_NEXT_COMPONENT \
             still sorts below every separator, which all begin with 0x40"
    );
    assert_eq!(
        upper,
        hi(enc_int(290, true)),
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
        hi(enc_int(299, true)).as_slice() <= upper.as_slice(),
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
        lo(enc_int(20, true)),
        "DESC `ck < 20`: physical-lower = enc_desc(20) (the boundary value)"
    );
    assert_eq!(
        upper,
        hi(vec![0xFFu8; MAX_OSS50_BOUND_SENTINEL_LEN]),
        "DESC `ck < 20`: open CQL lower → physical +∞ sentinel"
    );
    assert!(lower < upper, "must be ordered");
    // A matching small value (ck=0) has the LARGEST DESC bytes, inside the
    // window; the buggy [-∞, enc_desc(20)] mapping would exclude it.
    assert!(
        lo(enc_int(0, true)).as_slice() >= lower.as_slice(),
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
    assert_eq!(lower, lo(enc_int(150, true)));
    assert_eq!(upper, hi(enc_int(150, true)));
    assert_eq!(
        lower[..lower.len() - 1],
        upper[..upper.len() - 1],
        "equality is a single physical point, bracketed by the two Cassandra \
             bound terminators"
    );
    assert!(lower < upper);
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
    assert_eq!(lower, lo(enc_int(5, false)));
    assert_eq!(upper, hi(enc_int(50, false)));
    assert!(lower < upper);
}

/// Issue #3032 — a bound naming only a PROPER PREFIX of a multi-component
/// clustering key must BRACKET every full clustering that extends it.
///
/// `WHERE bucket = 'bo'` on `PRIMARY KEY (pk, bucket, seq)` yields
/// `start == end == [Text("bo")]`, a one-component bound against two-component
/// `Rows.db` separators. Without Cassandra's `GT_NEXT_COMPONENT` terminator the
/// physical upper bound is the bare prefix, which sorts BELOW every
/// `('bo', seq)` separator — so `strict_ceiling` returned the FIRST `bo` block
/// and the slice was truncated to it (12 of 60 rows on the
/// `test_da/multiclustering_table` fixture).
///
/// The literal separator bytes below are the real Cassandra 5.0.2-written ones
/// from that fixture's `Rows.db` (`da-2-bti-Rows.db`, pk=1), not this encoder's
/// output.
#[cfg(not(feature = "tombstones"))]
#[test]
fn physical_bounds_bracket_extensions_of_a_prefix_bound() {
    // The real on-disk separators for ('bo', 12), ('bo', 30), ('bo', 48) and the
    // next bucket's ('charlie-extended-bucket', 5).
    let bo_12 = vec![0x40u8, 0x62, 0x6f, 0x00, 0x40, 0x80, 0x00, 0x00, 0x0c];
    let bo_48 = vec![0x40u8, 0x62, 0x6f, 0x00, 0x40, 0x80, 0x00, 0x00, 0x30];
    let charlie_5 = vec![
        0x40u8, 0x63, 0x68, 0x61, 0x72, 0x6c, 0x69, 0x65, 0x2d, 0x65, 0x78, 0x74, 0x65, 0x6e, 0x64,
        0x65, 0x64, 0x2d, 0x62, 0x75, 0x63, 0x6b, 0x65, 0x74, 0x00, 0x40, 0x80, 0x00, 0x00, 0x05,
    ];

    let slice = ClusteringSlice {
        start: vec![Value::text("bo".to_string())],
        start_inclusive: true,
        end: vec![Value::text("bo".to_string())],
        end_inclusive: true,
    };
    // Two clustering columns, both ASC.
    let (lower, upper) = physical_byte_bounds_for_slice(&slice, &[false, false])
        .expect("ok")
        .expect("encodable");

    assert_eq!(
        lower,
        vec![0x40u8, 0x62, 0x6f, 0x00, OSS50_LT_NEXT_COMPONENT],
        "the physical-low bound is the 'bo' prefix + LT_NEXT_COMPONENT"
    );
    assert_eq!(
        upper,
        vec![0x40u8, 0x62, 0x6f, 0x00, OSS50_GT_NEXT_COMPONENT],
        "the physical-high bound is the 'bo' prefix + GT_NEXT_COMPONENT"
    );

    for sep in [&bo_12, &bo_48] {
        assert!(
            lower.as_slice() <= sep.as_slice(),
            "the low bound must sort at/below every ('bo', seq) separator: \
                 {lower:02x?} vs {sep:02x?}"
        );
        assert!(
            sep.as_slice() < upper.as_slice(),
            "the high bound must sort ABOVE every ('bo', seq) separator — this is \
                 the property whose absence truncated the slice: {sep:02x?} vs \
                 {upper:02x?}"
        );
    }
    // ...and it must NOT reach into the next bucket, so the window stays narrow.
    assert!(
        upper.as_slice() < charlie_5.as_slice(),
        "the high bound must still sort below the NEXT bucket's separators"
    );
    // Without the terminator the upper bound would sort below the very first
    // extension — the exact regression this pins.
    let bare_upper = vec![0x40u8, 0x62, 0x6f, 0x00];
    assert!(
        bare_upper.as_slice() < bo_12.as_slice(),
        "sanity: a bare prefix sorts BELOW its own extensions, so it can never \
             bound them from above"
    );
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
    let value = Value::blob(data.clone());

    if let Value::Blob(v) = value {
        assert_eq!(v, data);
    } else {
        panic!("Expected Value::Blob");
    }
}
