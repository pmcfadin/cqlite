//! Corruption-location resolution for [`VerifyFinding`] (issue #4194).
//!
//! This module owns exactly one piece of logic: given a `Data.db` byte range a
//! finding is anchored to, intersect it against a boundary source's own decoded
//! partition extents to name which partitions the damage touches. It is a pure,
//! closed-interval computation — **no byte-pattern scanning of `Data.db` is ever
//! performed here** (no-heuristics mandate, issue #28; mechanically enforced by
//! `scripts/tests/test_verify_location_no_resync_scan.sh`).
//!
//! # Coordinate spaces (read this before touching call sites)
//!
//! `Index.db`'s `data_offset` and the BTI trie's resolved partition positions
//! both address the **decompressed (logical)** `Data.db` byte stream — Cassandra
//! opens a compressed `Data.db` through a reader that presents a virtual
//! uncompressed view, and both boundary sources were written against that view.
//! For an uncompressed table logical and physical coordinates coincide.
//! [`resolve_partitions`] therefore always operates on a *logical* damaged
//! range; callers computing a chunk's *physical* on-disk byte range (for
//! [`Location::byte_offset`]/[`Location::byte_len`], which are reported for a
//! human to go find the bytes) additionally derive the matching logical range
//! before calling in — see `verify.rs`'s call sites for the per-check
//! derivation (compressed: `chunk_index * chunk_length`; uncompressed: the
//! physical offset, unchanged).
//!
//! [`VerifyFinding`]: super::verify::VerifyFinding
//! [`Location::byte_offset`]: Location::byte_offset
//! [`Location::byte_len`]: Location::byte_len

/// Where a [`VerifyFinding`] anchored to a `Data.db` byte range is located, and
/// which partitions that range intersects.
///
/// [`VerifyFinding`]: super::verify::VerifyFinding
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    /// The component the byte range belongs to — always `"Data.db"` today (the
    /// only component with a chunk/offset grid a finding can anchor to).
    pub component: String,
    /// Physical (on-disk) start of the damaged range in `component`.
    pub byte_offset: u64,
    /// Physical (on-disk) length of the damaged range in `component`.
    pub byte_len: u64,
    /// The chunk index the damage falls in, when `component` has a chunk grid
    /// (compressed `CompressionInfo.db` chunks, or the fixed-size `CRC.db`
    /// grid). `None` for a finding with no chunk grid.
    pub chunk_index: Option<usize>,
    /// The partitions whose `Data.db` extent intersects the damaged range.
    pub partitions: PartitionResolution,
}

/// The outcome of resolving [`Location::partitions`] against a boundary source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionResolution {
    /// The boundary source was healthy and every intersecting partition's raw
    /// key was recoverable. Empty when no boundary entry intersects the
    /// damaged range (legitimate — e.g. the range falls entirely past the last
    /// known partition).
    Resolved(Vec<KeyRef>),
    /// The boundary source could not be trusted (or a needed partition key was
    /// unavailable) — the cause is named, never a guess and never a silent
    /// empty list standing in for "unknown".
    Unresolved(String),
}

/// One partition key, as recovered from the boundary source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRef {
    /// The raw partition key's bytes, lower-case hex. Always populated when
    /// the key is known.
    pub key_hex: String,
    /// The key decoded through a schema, when one was available. `verify` and
    /// `sweep` take no mandatory `--schema` today, so this is commonly `None`
    /// (parity with `sstable-salvage`'s manifest `key`/`key_hex` split).
    pub rendered: Option<String>,
}

impl KeyRef {
    fn from_raw(raw: &[u8]) -> Self {
        Self {
            key_hex: hex_encode(raw),
            rendered: None,
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

/// The cause named when the boundary source (`Index.db` / `Partitions.db`)
/// itself is the corrupt component in the same report — every OTHER location
/// in that report is poisoned by this cause (design.md §D2).
pub const BOUNDARY_SOURCE_UNREADABLE: &str = "boundary-source-unreadable";

/// A partition key was needed to resolve a location but was not recoverable
/// from the data available at resolution time (e.g. a BTI `DataOffset` leaf
/// whose raw key is only recoverable through a FULL-mode `Data.db` scan, and
/// QUICK mode never scans). Named rather than silently dropping the leaf from
/// the resolved set, which would under-report the intersecting partitions.
pub const PARTITION_KEY_UNAVAILABLE: &str =
    "partition key unavailable for an intersecting boundary entry (requires a full-mode scan)";

/// `true` when the half-open ranges `[a.0, a.1)` and `[b.0, b.1)` overlap.
fn ranges_intersect(a: (u64, u64), b: (u64, u64)) -> bool {
    a.0 < b.1 && b.0 < a.1
}

/// Intersect `damaged` (a closed-open `[start, end)` range in `Data.db`
/// LOGICAL/decompressed offset space) against `boundary_entries` — one
/// `(data_offset, raw_key)` pair per partition the boundary source (`Index.db`
/// or the BTI `Partitions.db` trie) names, in ANY order. `raw_key` is `None`
/// when the entry's identity is not yet known (see [`PARTITION_KEY_UNAVAILABLE`]).
///
/// `logical_len` bounds the LAST entry's extent (there is no "next entry" to
/// derive it from) — the boundary source's own declared total logical length
/// (`CompressionInfo::data_length` when compressed, the physical `Data.db`
/// length when not).
///
/// This is a closed-interval test only — no `Data.db` bytes are read or
/// scanned here (design.md §D1/§D6, issue #28).
pub fn resolve_partitions(
    damaged: (u64, u64),
    boundary_entries: &[(u64, Option<Vec<u8>>)],
    logical_len: u64,
) -> PartitionResolution {
    let mut sorted: Vec<&(u64, Option<Vec<u8>>)> = boundary_entries.iter().collect();
    sorted.sort_by_key(|(offset, _)| *offset);

    let mut hits: Vec<KeyRef> = Vec::new();
    let mut unknown = false;
    for (i, (start, key)) in sorted.iter().enumerate() {
        let end = sorted
            .get(i + 1)
            .map(|(next_start, _)| *next_start)
            .unwrap_or(logical_len);
        let extent = (*start, end.max(*start));
        if ranges_intersect(damaged, extent) {
            match key {
                Some(k) => hits.push(KeyRef::from_raw(k)),
                None => unknown = true,
            }
        }
    }

    if unknown {
        return PartitionResolution::Unresolved(PARTITION_KEY_UNAVAILABLE.to_string());
    }
    hits.sort_by(|a, b| a.key_hex.cmp(&b.key_hex));
    hits.dedup_by(|a, b| a.key_hex == b.key_hex);
    PartitionResolution::Resolved(hits)
}

/// Build a [`Location`] for a chunk/offset-anchored finding, fail-closed on a
/// damaged boundary source (design.md §D2): when `boundary_source_healthy` is
/// `false`, `partitions` is always `Unresolved(BOUNDARY_SOURCE_UNREADABLE)` —
/// the boundary source is either fully trusted or not trusted at all, never
/// partially.
#[allow(clippy::too_many_arguments)]
pub fn resolve_location(
    component: &str,
    byte_offset: u64,
    byte_len: u64,
    chunk_index: Option<usize>,
    boundary_source_healthy: bool,
    damaged_logical: (u64, u64),
    boundary_entries: Option<&[(u64, Option<Vec<u8>>)]>,
    logical_len: u64,
) -> Location {
    let partitions = if !boundary_source_healthy {
        PartitionResolution::Unresolved(BOUNDARY_SOURCE_UNREADABLE.to_string())
    } else {
        match boundary_entries {
            Some(entries) => resolve_partitions(damaged_logical, entries, logical_len),
            None => PartitionResolution::Unresolved(
                "boundary source unavailable for location resolution".to_string(),
            ),
        }
    };
    Location {
        component: component.to_string(),
        byte_offset,
        byte_len,
        chunk_index,
        partitions,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(pairs: &[(u64, &[u8])]) -> Vec<(u64, Option<Vec<u8>>)> {
        pairs
            .iter()
            .map(|(o, k)| (*o, Some(k.to_vec())))
            .collect()
    }

    #[test]
    fn intersects_a_single_partition() {
        let e = entries(&[(0, b"k0"), (100, b"k1"), (200, b"k2")]);
        // Damaged range [100, 150) falls entirely inside k1's extent [100,200).
        let res = resolve_partitions((100, 150), &e, 300);
        assert_eq!(
            res,
            PartitionResolution::Resolved(vec![KeyRef::from_raw(b"k1")])
        );
    }

    #[test]
    fn intersects_two_adjacent_partitions_when_the_range_spans_the_boundary() {
        let e = entries(&[(0, b"k0"), (100, b"k1"), (200, b"k2")]);
        // [90, 110) straddles k0's end and k1's start.
        let res = resolve_partitions((90, 110), &e, 300);
        assert_eq!(
            res,
            PartitionResolution::Resolved(vec![
                KeyRef::from_raw(b"k0"),
                KeyRef::from_raw(b"k1"),
            ])
        );
    }

    #[test]
    fn touching_but_not_overlapping_is_not_an_intersection() {
        let e = entries(&[(0, b"k0"), (100, b"k1")]);
        // [100, 150) starts exactly where k0 ends — no overlap with k0.
        let res = resolve_partitions((100, 150), &e, 300);
        assert_eq!(
            res,
            PartitionResolution::Resolved(vec![KeyRef::from_raw(b"k1")])
        );
    }

    #[test]
    fn last_partition_extent_is_bounded_by_logical_len() {
        let e = entries(&[(0, b"k0"), (100, b"k1")]);
        let res = resolve_partitions((250, 260), &e, 300);
        assert_eq!(
            res,
            PartitionResolution::Resolved(vec![KeyRef::from_raw(b"k1")])
        );
        let res_past_end = resolve_partitions((300, 310), &e, 300);
        assert_eq!(res_past_end, PartitionResolution::Resolved(vec![]));
    }

    #[test]
    fn no_intersection_resolves_to_an_empty_resolved_set() {
        let e = entries(&[(1000, b"k0")]);
        let res = resolve_partitions((0, 10), &e, 2000);
        assert_eq!(res, PartitionResolution::Resolved(vec![]));
    }

    #[test]
    fn an_unknown_intersecting_key_unresolves_the_whole_finding() {
        let e = vec![(0u64, Some(b"k0".to_vec())), (100u64, None)];
        let res = resolve_partitions((100, 150), &e, 300);
        assert_eq!(
            res,
            PartitionResolution::Unresolved(PARTITION_KEY_UNAVAILABLE.to_string())
        );
    }

    #[test]
    fn resolve_location_fails_closed_on_a_damaged_boundary_source() {
        let e = entries(&[(0, b"k0")]);
        let loc = resolve_location(
            "Data.db",
            64,
            16,
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
        let loc = resolve_location("Data.db", 64, 16, Some(0), true, (0, 100), Some(&e), 16384);
        assert_eq!(
            loc.partitions,
            PartitionResolution::Resolved(vec![KeyRef::from_raw(b"k0")])
        );
        assert_eq!(loc.component, "Data.db");
        assert_eq!(loc.byte_offset, 64);
        assert_eq!(loc.byte_len, 16);
        assert_eq!(loc.chunk_index, Some(0));
    }

    #[test]
    fn hex_encode_matches_lower_case_pairs() {
        assert_eq!(hex_encode(&[0x00, 0xab, 0xff]), "00abff");
    }
}
