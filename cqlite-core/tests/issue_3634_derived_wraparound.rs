//! Issue #3634: `ScanTokenBound`'s ring-wrapping must be DERIVED from its two
//! endpoints, exactly as Cassandra derives it, rather than carried as a third
//! caller-supplied field that can disagree with them.
//!
//! ## Oracle
//!
//! The expectations here are transcribed from the PINNED Cassandra source, never
//! from CQLite's prior behaviour — CQLite's prior behaviour is the defect:
//!
//! ```text
//! git show cassandra-5.0.8:src/java/org/apache/cassandra/dht/Range.java
//! ```
//!
//! * `Range.isWrapAround(T left, T right)` — `return left.compareTo(right) >= 0;`
//! * `Range.contains(T left, T right, T point)` — the wrapping arm returns
//!   `point.compareTo(left) > 0 || right.compareTo(point) >= 0`, the
//!   non-wrapping arm `point.compareTo(left) > 0 && right.compareTo(point) >= 0`.
//!
//! Both are transcribed below over `i64` (CQLite's Murmur3 token domain); nothing
//! Java-specific is imported, only the membership rule.
//!
//! ## The shape this exists for
//!
//! `start_excl > end_incl` with the old `wraparound: false` was the one triple
//! Cassandra cannot express, and CQLite answered it with the EMPTY SET where
//! Cassandra answers `t > start || t <= end` — an empty scan result is
//! indistinguishable from a range that legitimately holds no partitions.

use cqlite_core::storage::sstable::reader::ScanTokenBound;

/// `Range.isWrapAround(left, right)` at `cassandra-5.0.8`, verbatim:
/// `left.compareTo(right) >= 0`.
fn cassandra_is_wrap_around(left: i64, right: i64) -> bool {
    left >= right
}

/// `Range.contains(left, right, point)` at `cassandra-5.0.8`, verbatim, with
/// `compareTo` spelled as the `i64` comparison it reduces to for tokens.
fn cassandra_contains(left: i64, right: i64, point: i64) -> bool {
    if cassandra_is_wrap_around(left, right) {
        // (1) a < k ⇒ true; (2) k <= b ⇒ true; (3) b < k <= a ⇒ false.
        point > left || right >= point
    } else {
        // The range (a, b] where a < b.
        point > left && right >= point
    }
}

/// Every endpoint pair the grid runs, including the previously-divergent
/// `start > end` shape, the `start == end` full ring (#2228) and the widest
/// non-wrapping pair the domain admits.
const SHAPES: &[(i64, i64)] = &[
    (10, 20),             // ordinary non-wrapping
    (100, -100),          // THE divergent shape: start > end
    (5, 5),               // full ring, #2228
    (i64::MIN, i64::MIN), // full ring at the domain floor
    (i64::MAX, i64::MAX), // full ring at the domain ceiling
    (i64::MIN, i64::MAX), // widest non-wrapping pair
    (i64::MAX, i64::MIN), // widest wrapping pair
    (-1, 0),
    (0, -1),
];

/// The probes for one shape: the domain extremes, both endpoints, and each
/// endpoint's ±1 neighbours (saturating, so the extremes stay in-domain).
fn probes(start_excl: i64, end_incl: i64) -> Vec<i64> {
    let mut v = vec![i64::MIN, i64::MAX, 0, -1, 1];
    for anchor in [start_excl, end_incl] {
        v.push(anchor.saturating_sub(1));
        v.push(anchor);
        v.push(anchor.saturating_add(1));
    }
    v
}

/// AC2: `contains` agrees with `Range.contains` on every shape, INCLUDING the
/// shape the flag-carrying form got wrong.
#[test]
fn contains_agrees_with_cassandra_range_contains() {
    for &(start_excl, end_incl) in SHAPES {
        let bound = ScanTokenBound {
            start_excl,
            end_incl,
        };
        for token in probes(start_excl, end_incl) {
            assert_eq!(
                bound.contains(token),
                cassandra_contains(start_excl, end_incl, token),
                "({start_excl}, {end_incl}] at token {token} must match \
                 Range.contains at cassandra-5.0.8"
            );
        }
    }
}

/// AC1: wrapping is the `>=` predicate, not a field — so no construction can
/// disagree with its own endpoints.
#[test]
fn is_wraparound_agrees_with_cassandra_is_wrap_around() {
    for &(start_excl, end_incl) in SHAPES {
        let bound = ScanTokenBound {
            start_excl,
            end_incl,
        };
        assert_eq!(
            bound.is_wraparound(),
            cassandra_is_wrap_around(start_excl, end_incl),
            "({start_excl}, {end_incl}] must wrap iff left >= right"
        );
    }
}

/// The regression proper: `(100, -100]` used to be constructible as the EMPTY
/// SET. It must now hold both ring segments and exclude only the gap between
/// them — stated as absolutes, so a future reintroduction of the flag cannot
/// pass this by agreeing with a broken oracle.
#[test]
fn the_previously_divergent_shape_is_no_longer_the_empty_set() {
    let bound = ScanTokenBound {
        start_excl: 100,
        end_incl: -100,
    };
    assert!(bound.is_wraparound(), "start > end wraps");
    for token in [i64::MIN, -1000, -100, i64::MAX, 101] {
        assert!(
            bound.contains(token),
            "token {token} is in (100, -100] under Range.contains"
        );
    }
    for token in [-99, 0, 99, 100] {
        assert!(
            !bound.contains(token),
            "token {token} is in the excluded gap of (100, -100]"
        );
    }
}

/// AC3: the `#2228` full ring still admits EVERY token — now because equal
/// endpoints wrap under `>=`, not because of an early-return special case. Pinned
/// at the domain boundaries, where the derivation is least obvious.
#[test]
fn equal_endpoints_still_admit_every_token() {
    for start in [i64::MIN, -1, 0, 1, 42, i64::MAX] {
        let bound = ScanTokenBound {
            start_excl: start,
            end_incl: start,
        };
        assert!(bound.is_wraparound(), "({start}, {start}] wraps under `>=`");
        for token in probes(start, start) {
            assert!(
                bound.contains(token),
                "the full ring ({start}, {start}] must admit token {token} (#2228)"
            );
            assert_eq!(
                bound.contains(token),
                cassandra_contains(start, start, token),
                "and Cassandra says the same"
            );
        }
    }
}
