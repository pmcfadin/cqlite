//! Unit tests for [`super`] — `verify_location.rs`'s pure range/rendering
//! logic (issue #4194).
//!
//! Carried in a sibling file wired via
//! `#[cfg(test)] #[path = "verify_location_tests.rs"] mod tests;` to keep the
//! production module's SOURCE line count down (campsite rule, epic
//! #1116/#1135). That module is currently 839 lines — OVER the 800-line
//! source threshold, disclosed under this PR's `CQLITE_ALLOW_FILE_GROWTH=1`
//! opt-out, not under it: the `format_location_compact` fix for roborev job
//! 131 pushed it past 800, and splitting the module by responsibility is
//! tracked as its own campsite-rule follow-up rather than done inline here.
//! `#[path]` keeps this a CHILD module of `verify_location`, so
//! `use super::*` still reaches its private items unchanged.

use super::*;

fn entries(pairs: &[(u64, &[u8])]) -> Vec<BoundaryEntry> {
    pairs
        .iter()
        .map(|(o, k)| (*o, Some(Arc::from(*k))))
        .collect()
}

/// `Resolved { keys, truncated: 0 }` — the shape every case in this file
/// expects except the dedicated cap test.
fn resolved(keys: Vec<KeyRef>) -> PartitionResolution {
    PartitionResolution::Resolved { keys, truncated: 0 }
}

#[test]
fn intersects_a_single_partition() {
    let e = entries(&[(0, b"k0"), (100, b"k1"), (200, b"k2")]);
    // Damaged range [100, 150) falls entirely inside k1's extent [100,200).
    let res = resolve_partitions((100, 150), &e, 300, LogicalLenSource::Declared);
    assert_eq!(res, resolved(vec![KeyRef::from_raw(b"k1")]));
}

#[test]
fn intersects_two_adjacent_partitions_when_the_range_spans_the_boundary() {
    let e = entries(&[(0, b"k0"), (100, b"k1"), (200, b"k2")]);
    // [90, 110) straddles k0's end and k1's start.
    let res = resolve_partitions((90, 110), &e, 300, LogicalLenSource::Declared);
    assert_eq!(
        res,
        resolved(vec![KeyRef::from_raw(b"k0"), KeyRef::from_raw(b"k1")])
    );
}

#[test]
fn touching_but_not_overlapping_is_not_an_intersection() {
    let e = entries(&[(0, b"k0"), (100, b"k1")]);
    // [100, 150) starts exactly where k0 ends — no overlap with k0.
    let res = resolve_partitions((100, 150), &e, 300, LogicalLenSource::Declared);
    assert_eq!(res, resolved(vec![KeyRef::from_raw(b"k1")]));
}

#[test]
fn last_partition_extent_is_bounded_by_logical_len() {
    let e = entries(&[(0, b"k0"), (100, b"k1")]);
    let res = resolve_partitions((250, 260), &e, 300, LogicalLenSource::Declared);
    assert_eq!(res, resolved(vec![KeyRef::from_raw(b"k1")]));
    let res_past_end = resolve_partitions((300, 310), &e, 300, LogicalLenSource::Declared);
    assert_eq!(res_past_end, resolved(vec![]));
}

#[test]
fn no_intersection_resolves_to_an_empty_resolved_set() {
    let e = entries(&[(1000, b"k0")]);
    let res = resolve_partitions((0, 10), &e, 2000, LogicalLenSource::Declared);
    assert_eq!(res, resolved(vec![]));
}

#[test]
fn resolved_set_is_capped_and_names_the_truncated_count() {
    // MAX_RESOLVED_KEYS + 10 boundary entries, all intersecting one huge
    // damaged range — the shape a badly truncated Data.db produces.
    let mut pairs: Vec<(u64, Vec<u8>)> = Vec::new();
    for i in 0..(MAX_RESOLVED_KEYS + 10) {
        pairs.push((i as u64 * 10, format!("k{i:04}").into_bytes()));
    }
    let e: Vec<BoundaryEntry> = pairs
        .iter()
        .map(|(o, k)| (*o, Some(Arc::from(k.as_slice()))))
        .collect();
    let logical_len = (MAX_RESOLVED_KEYS as u64 + 11) * 10;
    let res = resolve_partitions(
        (0, logical_len),
        &e,
        logical_len,
        LogicalLenSource::Declared,
    );
    match res {
        PartitionResolution::Resolved { keys, truncated } => {
            assert_eq!(keys.len(), MAX_RESOLVED_KEYS);
            assert_eq!(truncated, 10);
        }
        other => panic!("expected a capped Resolved set, got {other:?}"),
    }
}

#[test]
fn duplicate_raw_keys_do_not_inflate_the_truncated_count() {
    // roborev round-3 LOW finding: a boundary source naming the SAME raw
    // key at two different offsets (defensive-only in practice) must not
    // consume two cap slots or count as two omissions.
    let e: Vec<BoundaryEntry> = vec![
        (0, Some(Arc::from(b"dup".as_slice()))),
        (10, Some(Arc::from(b"dup".as_slice()))),
        (20, Some(Arc::from(b"unique".as_slice()))),
    ];
    let res = resolve_partitions((0, 30), &e, 30, LogicalLenSource::Declared);
    match res {
        PartitionResolution::Resolved { keys, truncated } => {
            assert_eq!(truncated, 0, "no key exceeds MAX_RESOLVED_KEYS here");
            let mut hexes: Vec<&str> = keys.iter().map(|k| k.key_hex.as_str()).collect();
            hexes.sort();
            hexes.dedup();
            assert_eq!(
                hexes.len(),
                keys.len(),
                "the resolved set must already be deduped by raw key identity: {keys:?}"
            );
        }
        other => panic!("expected Resolved, got {other:?}"),
    }
}

#[test]
fn an_unknown_intersecting_key_unresolves_the_whole_finding() {
    let e: Vec<BoundaryEntry> = vec![(0u64, Some(Arc::from(b"k0".as_slice()))), (100u64, None)];
    let res = resolve_partitions((100, 150), &e, 300, LogicalLenSource::Declared);
    assert_eq!(
        res,
        PartitionResolution::Unresolved(PARTITION_KEY_UNAVAILABLE.to_string())
    );
}

// format_location's PARTITION branches (roborev job 102 LOW, upgraded:
// `--out text` is the DEFAULT for both `verify` and `sweep`, so these two
// strings ARE the disclosures this change exists to make, and neither had
// any coverage. A regression dropping the `+N more, capped` suffix would
// have let a partial list read as complete with a green suite.
#[test]
fn format_location_discloses_a_capped_partition_list() {
    let loc = Location {
        component: "Data.db".to_string(),
        byte_offset: 0x28000,
        byte_len: 4,
        anchor: PhysicalAnchor::DeclaredRecord,
        chunk_index: Some(1),
        partitions: PartitionResolution::Resolved {
            keys: vec![KeyRef::from_raw(b"k0")],
            truncated: 842,
        },
    };
    let out = format_location(&loc);
    assert!(
        out.contains("(+842 more, capped)"),
        "a capped list MUST say it is incomplete: {out}"
    );
    // …and the declared-record disclosure rides along on the same line.
    assert!(
        out.contains("declared offset 0x28000") && out.contains("does not fit"),
        "capped rendering must not lose the anchor disclosure: {out}"
    );
}

#[test]
fn format_location_discloses_an_unresolved_cause() {
    let loc = Location {
        component: "Data.db".to_string(),
        byte_offset: 64,
        byte_len: 16,
        anchor: PhysicalAnchor::DamagedExtent,
        chunk_index: Some(0),
        partitions: PartitionResolution::Unresolved(BOUNDARY_SOURCE_UNREADABLE.to_string()),
    };
    let out = format_location(&loc);
    assert!(
        out.contains("partitions unresolved") && out.contains(BOUNDARY_SOURCE_UNREADABLE),
        "an unresolved location MUST name its cause rather than render an empty list: {out}"
    );
    assert!(
        !out.contains("declared offset"),
        "a damaged extent must not be labelled declared: {out}"
    );
}

#[test]
fn format_location_names_an_empty_resolved_set_without_claiming_partitions() {
    let loc = Location {
        component: "Data.db".to_string(),
        byte_offset: 64,
        byte_len: 16,
        anchor: PhysicalAnchor::DamagedExtent,
        chunk_index: None,
        partitions: PartitionResolution::Resolved {
            keys: vec![],
            truncated: 0,
        },
    };
    let out = format_location(&loc);
    assert!(
        out.contains("0 partitions"),
        "an empty resolved set reads as an affirmative zero, never a blank: {out}"
    );
}

#[test]
fn resolve_location_fails_closed_on_a_damaged_boundary_source() {
    // A `Rejected` source carries NO entries at all now: the former
    // `(healthy: false, entries: Some(..))` pair could express "distrusted yet
    // still handed its entries", a state nothing was stopping a future caller
    // from resolving against.
    let loc = resolve_location(
        "Data.db",
        64,
        16,
        PhysicalAnchor::DamagedExtent,
        Some(0),
        &BoundarySource::Rejected(BOUNDARY_SOURCE_UNREADABLE.to_string()),
        (0, 16384),
        16384,
        LogicalLenSource::Declared,
    );
    assert_eq!(
        loc.partitions,
        PartitionResolution::Unresolved(BOUNDARY_SOURCE_UNREADABLE.to_string())
    );
}

#[test]
fn resolve_location_resolves_when_the_boundary_source_is_healthy() {
    let e = entries(&[(0, b"k0")]);
    let loc = resolve_location(
        "Data.db",
        64,
        16,
        PhysicalAnchor::DamagedExtent,
        Some(0),
        &BoundarySource::Trusted(&e),
        (0, 100),
        16384,
        LogicalLenSource::Declared,
    );
    assert_eq!(loc.partitions, resolved(vec![KeyRef::from_raw(b"k0")]));
    assert_eq!(loc.component, "Data.db");
    assert_eq!(loc.byte_offset, 64);
    assert_eq!(loc.byte_len, 16);
    assert_eq!(loc.chunk_index, Some(0));
}

#[test]
fn hex_encode_matches_lower_case_pairs() {
    assert_eq!(hex_encode(&[0x00, 0xab, 0xff]), "00abff");
}

// ---------------------------------------------------------------------------
// Boundary-source validation (roborev blocker #2 + I1, issue #4194)
//
// Each of these fails WITHOUT the validation in `resolve_partitions` /
// `first_order_violation`: the pre-fix code let a corrupt `data_offset`
// collapse its own extent to `[huge, huge)` (never reported, never counted
// in `truncated`, never `Unresolved` — a silent drop) AND widen its former
// left neighbour's extent to the next real partition's start (clean bytes
// attributed to the wrong key, with full confidence).
// ---------------------------------------------------------------------------

/// The exact post-sort shape a bit flip in a non-leading byte of a BIG
/// `Index.db` `position` vint produces: every entry still parses, so the
/// bogus `+2^24` position simply sorts to the END of the list.
#[test]
fn an_out_of_bounds_boundary_offset_is_refused_rather_than_silently_dropped() {
    let logical_len = 300u64;
    let e = entries(&[(0, b"k0"), (100, b"k1"), (100 + (1 << 24), b"k2")]);
    let res = resolve_partitions((100, 150), &e, logical_len, LogicalLenSource::Declared);
    match res {
        PartitionResolution::Unresolved(cause) => {
            assert!(
                cause.contains(BOUNDARY_ENTRY_OFFSET_OUT_OF_BOUNDS),
                "the refusal must NAME the out-of-bounds offset as its cause: {cause}"
            );
            assert!(
                cause.contains("16777316") && cause.contains("300"),
                "the cause must carry BOTH the offending position and the logical length so \
                 the operator can see the inconsistency: {cause}"
            );
        }
        PartitionResolution::Resolved { keys, truncated } => panic!(
            "a boundary source declaring a position past the logical length must be REFUSED, \
             not resolved against: got keys={keys:?} truncated={truncated}. Pre-fix this \
             returned Resolved([k1]) — k2 silently dropped and k1's extent widened over k2's \
             real bytes."
        ),
    }
}

/// A position EXACTLY at the declared logical length is the same degenerate
/// `[len, len)` extent, so `>` would have left this one value silently
/// dropped — the bounds check is `>=`.
#[test]
fn a_boundary_offset_exactly_at_the_logical_length_is_also_refused() {
    let e = entries(&[(0, b"k0"), (300, b"k1")]);
    match resolve_partitions((0, 300), &e, 300, LogicalLenSource::Declared) {
        PartitionResolution::Unresolved(cause) => assert!(
            cause.contains(BOUNDARY_ENTRY_OFFSET_OUT_OF_BOUNDS),
            "cause: {cause}"
        ),
        other => panic!("expected a refusal for a position at the logical end, got {other:?}"),
    }
}

/// The in-bounds case must be entirely unaffected — the validation refuses a
/// corrupt source, it does not make a healthy one unresolvable.
#[test]
fn a_boundary_offset_just_below_the_logical_length_still_resolves() {
    let e = entries(&[(0, b"k0"), (299, b"k1")]);
    assert_eq!(
        resolve_partitions((299, 300), &e, 300, LogicalLenSource::Declared),
        resolved(vec![KeyRef::from_raw(b"k1")])
    );
}

#[test]
fn first_order_violation_accepts_a_strictly_ascending_sequence() {
    assert_eq!(
        first_order_violation(&entries(&[(0, b"k0"), (1, b"k1"), (2, b"k2")])),
        None
    );
    assert_eq!(first_order_violation(&entries(&[(7, b"only")])), None);
    assert_eq!(first_order_violation(&[]), None);
}

#[test]
fn first_order_violation_names_a_descending_entry() {
    // Entry 2 declares a position BELOW entry 1's — the on-disk parse order
    // the format guarantees is strictly ascending (guide Ch.6).
    assert_eq!(
        first_order_violation(&entries(&[(0, b"k0"), (200, b"k1"), (100, b"k2")])),
        Some(2)
    );
}

#[test]
fn first_order_violation_treats_two_equal_positions_as_a_violation() {
    // STRICT ascent: two partitions sharing a start offset give the earlier
    // one the extent `[s, s)`, which intersects nothing — the same silent
    // drop the out-of-bounds check above refuses, reached by a different
    // corruption. A non-strict `<` test would wave this through.
    assert_eq!(
        first_order_violation(&entries(&[(0, b"k0"), (100, b"k1"), (100, b"k2")])),
        Some(2)
    );
}

#[test]
fn a_rejected_boundary_source_propagates_its_own_cause_verbatim() {
    // I1: a rejection's cause must reach the operator intact, not be
    // flattened into one generic "unavailable" string.
    let cause = format!("{BOUNDARY_SOURCE_OPEN_FAILED}: /x/nb-1-big-Index.db: Permission denied");
    let loc = resolve_location(
        "Data.db",
        64,
        16,
        PhysicalAnchor::DamagedExtent,
        Some(0),
        &BoundarySource::Rejected(cause.clone()),
        (0, 16384),
        16384,
        LogicalLenSource::Declared,
    );
    assert_eq!(loc.partitions, PartitionResolution::Unresolved(cause));
    let rendered = format_location(&loc);
    assert!(
        rendered.contains("Permission denied"),
        "the operator-visible text MUST name the real I/O cause, not just \
         'boundary source unavailable': {rendered}"
    );
}

// `format_location_compact` and the `Display` that uses it (roborev job 131
// MEDIUM, issue #4194). `VerifyReport::summary_line()` joins every finding's
// `Display` into ONE line and `cqlite verify --out text` prints each finding's
// FULL location again two lines below, so rendering the full form in `Display`
// made that line run to kilobytes and duplicated the detail verbatim. These
// cases pin both halves of the fix: the enumeration is GONE from the compact
// form, and the byte anchor is STILL THERE (a compact form that dropped the
// anchor too would leave `parsing_errors` consumers with nothing to go on).
#[test]
fn format_location_compact_keeps_the_anchor_and_drops_the_key_list() {
    let loc = Location {
        component: "Data.db".to_string(),
        byte_offset: 0x1006,
        byte_len: 4,
        anchor: PhysicalAnchor::DamagedExtent,
        chunk_index: Some(7),
        partitions: PartitionResolution::Resolved {
            keys: vec![KeyRef::from_raw(b"k0"), KeyRef::from_raw(b"k1")],
            truncated: 842,
        },
    };
    let compact = format_location_compact(&loc);
    let full = format_location(&loc);

    // The anchor — component, chunk, physical range — survives.
    assert!(
        compact.contains("Data.db")
            && compact.contains("chunk 7")
            && compact.contains("offset 0x1006"),
        "the compact form MUST keep the byte anchor: {compact}"
    );
    // The COUNT survives, including the capped disclosure…
    assert!(
        compact.contains("2 partition(s)") && compact.contains("(+842 more, capped)"),
        "the compact form MUST keep the partition count and cap disclosure: {compact}"
    );
    // …but the enumeration does NOT. `6b30`/`6b31` are hex("k0")/hex("k1").
    assert!(
        !compact.contains("6b30") && !compact.contains("6b31"),
        "the compact form MUST NOT enumerate partition keys: {compact}"
    );
    // The full renderer is unchanged and still enumerates — the two forms are
    // genuinely different, so this test cannot pass vacuously by both being
    // compact.
    assert!(
        full.contains("6b30") && full.contains("6b31"),
        "format_location MUST still enumerate: {full}"
    );
}

#[test]
fn verify_finding_display_does_not_enumerate_partition_keys() {
    // Struct literal, not `VerifyFinding::new` — that constructor is private to
    // `verify.rs` and this is a child module of `verify_location`.
    let finding = VerifyFinding {
        class: VerifyErrorClass::ChunkOffsetOutOfBounds,
        component: "Data.db".to_string(),
        detail: "chunk offset out of bounds".to_string(),
        location: Some(Location {
            component: "Data.db".to_string(),
            byte_offset: 0x1006,
            byte_len: 4,
            anchor: PhysicalAnchor::DamagedExtent,
            chunk_index: Some(7),
            partitions: PartitionResolution::Resolved {
                keys: vec![KeyRef::from_raw(b"k0")],
                truncated: 0,
            },
        }),
    };
    let shown = finding.to_string();
    assert!(
        shown.contains("location:") && shown.contains("1 partition(s)"),
        "Display MUST still carry a location anchor: {shown}"
    );
    assert!(
        !shown.contains("6b30"),
        "Display feeds summary_line(); it MUST NOT enumerate partition keys: {shown}"
    );
}

// The uncompressed-truncation misdiagnosis (roborev job 133 MEDIUM, #4194).
// IDENTICAL inputs, differing ONLY in the provenance of `logical_len`, must
// name DIFFERENT suspect components — that is the whole content of the fix, so
// asserting both arms in one case keeps them from drifting apart.
#[test]
fn a_boundary_entry_past_a_measured_data_db_len_blames_data_db_not_the_index() {
    // `k1` starts at 200, but Data.db is only 150 bytes: a truncation.
    let e = entries(&[(0, b"k0"), (200, b"k1")]);

    let truncated = resolve_partitions((0, 150), &e, 150, LogicalLenSource::MeasuredDataDbLength);
    let cause = match &truncated {
        PartitionResolution::Unresolved(c) => c.clone(),
        other => panic!("a measured length past the last entry must refuse: {other:?}"),
    };
    assert!(
        cause.contains(DATA_DB_SHORTER_THAN_BOUNDARY_SOURCE),
        "a truncated Data.db MUST be named as the suspect: {cause}"
    );
    assert!(
        !cause.contains(BOUNDARY_ENTRY_OFFSET_OUT_OF_BOUNDS),
        "an intact boundary source MUST NOT be blamed for a Data.db truncation: {cause}"
    );
    assert!(
        cause.contains("150"),
        "the refusal must name the actual Data.db length: {cause}"
    );

    // Same entries, same length — but DECLARED. Now the boundary source really
    // does contradict itself, so the original cause is the correct one.
    let declared = resolve_partitions((0, 150), &e, 150, LogicalLenSource::Declared);
    let dcause = match &declared {
        PartitionResolution::Unresolved(c) => c.clone(),
        other => panic!("a declared length past the last entry must refuse: {other:?}"),
    };
    assert!(
        dcause.contains(BOUNDARY_ENTRY_OFFSET_OUT_OF_BOUNDS),
        "a declared-length violation MUST still blame the boundary source: {dcause}"
    );
    assert!(
        !dcause.contains(DATA_DB_SHORTER_THAN_BOUNDARY_SOURCE),
        "a declared-length violation is not a truncation: {dcause}"
    );

    // The two causes are genuinely different text — this case cannot pass
    // vacuously by both arms rendering the same string.
    assert_ne!(cause, dcause, "the two provenances must be distinguishable");
}
