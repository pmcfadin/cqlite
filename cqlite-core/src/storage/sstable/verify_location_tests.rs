//! Unit tests for [`super`] — `verify_location.rs`'s pure range/rendering
//! logic (issue #4194).
//!
//! Carried in a sibling file wired via
//! `#[cfg(test)] #[path = "verify_location_tests.rs"] mod tests;` so the
//! production module stays under the 800-line source threshold (campsite
//! rule, epic #1116/#1135) while the added BIG boundary-source validation
//! lands. `#[path]` keeps this a CHILD module of `verify_location`, so
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
    let res = resolve_partitions((100, 150), &e, 300);
    assert_eq!(res, resolved(vec![KeyRef::from_raw(b"k1")]));
}

#[test]
fn intersects_two_adjacent_partitions_when_the_range_spans_the_boundary() {
    let e = entries(&[(0, b"k0"), (100, b"k1"), (200, b"k2")]);
    // [90, 110) straddles k0's end and k1's start.
    let res = resolve_partitions((90, 110), &e, 300);
    assert_eq!(
        res,
        resolved(vec![KeyRef::from_raw(b"k0"), KeyRef::from_raw(b"k1")])
    );
}

#[test]
fn touching_but_not_overlapping_is_not_an_intersection() {
    let e = entries(&[(0, b"k0"), (100, b"k1")]);
    // [100, 150) starts exactly where k0 ends — no overlap with k0.
    let res = resolve_partitions((100, 150), &e, 300);
    assert_eq!(res, resolved(vec![KeyRef::from_raw(b"k1")]));
}

#[test]
fn last_partition_extent_is_bounded_by_logical_len() {
    let e = entries(&[(0, b"k0"), (100, b"k1")]);
    let res = resolve_partitions((250, 260), &e, 300);
    assert_eq!(res, resolved(vec![KeyRef::from_raw(b"k1")]));
    let res_past_end = resolve_partitions((300, 310), &e, 300);
    assert_eq!(res_past_end, resolved(vec![]));
}

#[test]
fn no_intersection_resolves_to_an_empty_resolved_set() {
    let e = entries(&[(1000, b"k0")]);
    let res = resolve_partitions((0, 10), &e, 2000);
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
    let res = resolve_partitions((0, logical_len), &e, logical_len);
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
    let res = resolve_partitions((0, 30), &e, 30);
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
    let res = resolve_partitions((100, 150), &e, 300);
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
    let e = entries(&[(0, b"k0")]);
    let loc = resolve_location(
        "Data.db",
        64,
        16,
        PhysicalAnchor::DamagedExtent,
        Some(0),
        false, // boundary source damaged
        (0, 16384),
        Some(&e),
        16384,
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
        true,
        (0, 100),
        Some(&e),
        16384,
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
