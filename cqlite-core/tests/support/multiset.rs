//! Order-insensitive **MULTISET** comparison for the "nothing was lost, nothing
//! was fabricated" assertions (issue #3782, roborev job 57 finding 1).
//!
//! # Why a multiset and not a set
//!
//! The #3782 measurement is `100 → 102 rows, 2 partitions LOST, 3 FABRICATED`:
//! the count goes UP while real data disappears. **Duplication is one of the
//! shapes that fabrication takes on this route** — the partition-HEADER arm
//! resynchronises by advancing one byte (declared gap #3928), so it can re-emit
//! a partition it has already emitted. A MEMBERSHIP test (`expected.contains(x)`
//! / `BTreeSet::difference`) is blind to exactly that: N duplicate copies of a
//! legitimate row are all "present in expected", so the check that carries this
//! change's headline claim would pass over the very defect it is named for.
//!
//! Comparing OCCURRENCE COUNTS answers it: a surplus copy has no matching
//! occurrence left in `expected` and is reported.
//!
//! The RED-capability of that difference is proved below rather than asserted in
//! prose — [`a_duplicated_item_is_surplus_where_a_membership_test_sees_nothing`]
//! feeds a duplicated result to BOTH rules and pins that the membership rule
//! says "clean" while this one does not. A stricter assertion nobody proved can
//! fail is not an improvement.
//!
//! Included with `#[path = "support/multiset.rs"] mod multiset;` from a
//! `cqlite-core/tests/*.rs` target root (or `"../support/multiset.rs"` from a
//! submodule directory).

#![allow(dead_code)]

use std::collections::BTreeMap;

/// Occurrence counts of `items`, keyed by the item itself.
pub fn multiset<T: Ord, I: IntoIterator<Item = T>>(items: I) -> BTreeMap<T, usize> {
    let mut counts: BTreeMap<T, usize> = BTreeMap::new();
    for item in items {
        *counts.entry(item).or_insert(0) += 1;
    }
    counts
}

/// Occurrences present in `got` beyond those `expected` accounts for —
/// FABRICATION, covering both a wholly new item and a **surplus duplicate of a
/// legitimate one**. `(item, surplus_count)` pairs, empty when `got` is a
/// sub-multiset of `expected`.
pub fn surplus<T: Ord + Clone>(
    got: &BTreeMap<T, usize>,
    expected: &BTreeMap<T, usize>,
) -> Vec<(T, usize)> {
    got.iter()
        .filter_map(|(item, &n)| {
            let m = expected.get(item).copied().unwrap_or(0);
            (n > m).then(|| (item.clone(), n - m))
        })
        .collect()
}

/// Occurrences `expected` carries that `got` does not — LOSS. The mirror of
/// [`surplus`], so a caller can report both halves of the #3782 shape (some
/// lost, more fabricated) from one comparison.
pub fn deficit<T: Ord + Clone>(
    got: &BTreeMap<T, usize>,
    expected: &BTreeMap<T, usize>,
) -> Vec<(T, usize)> {
    surplus(expected, got)
}

/// Render `(item, count)` pairs for a panic message, `Debug`-formatted and
/// truncated so a whole corpus cannot flood the failure output.
pub fn describe<T: std::fmt::Debug>(pairs: &[(T, usize)]) -> String {
    const MAX: usize = 8;
    let shown: Vec<String> = pairs
        .iter()
        .take(MAX)
        .map(|(item, n)| format!("{item:?} x{n}"))
        .collect();
    if pairs.len() > MAX {
        format!("{} (+{} more)", shown.join(", "), pairs.len() - MAX)
    } else {
        shown.join(", ")
    }
}

/// RED-capability proof for the whole point of this module: a result that
/// DUPLICATES a legitimate row is fabrication, and the membership rule this
/// module replaces cannot see it.
#[test]
fn a_duplicated_item_is_surplus_where_a_membership_test_sees_nothing() {
    let expected = multiset(["a", "b"]);
    // 3 rows where the control had 2 — the #3782 "count goes UP" shape, built
    // entirely out of values that ARE legitimate.
    let got = multiset(["a", "a", "b"]);

    // The rule that was here before (roborev job 57 finding 1): every returned
    // row is a member of the expected set, so it reports CLEAN.
    assert!(
        got.keys().all(|k| expected.contains_key(k)),
        "the membership rule must be shown to pass here, or this proves nothing"
    );

    // The multiset rule reports the surplus copy.
    assert_eq!(surplus(&got, &expected), vec![("a", 1)]);
    assert!(deficit(&got, &expected).is_empty());
}

#[test]
fn a_wholly_new_item_is_surplus_and_a_dropped_one_is_deficit() {
    let expected = multiset(["a", "b"]);
    let got = multiset(["a", "z"]);
    assert_eq!(surplus(&got, &expected), vec![("z", 1)]);
    assert_eq!(deficit(&got, &expected), vec![("b", 1)]);
}

#[test]
fn an_identical_multiset_reports_neither_surplus_nor_deficit() {
    // Same items, different ORDER and with a legitimate repeat: equal.
    let expected = multiset(["b", "a", "a"]);
    let got = multiset(["a", "b", "a"]);
    assert!(surplus(&got, &expected).is_empty());
    assert!(deficit(&got, &expected).is_empty());
    assert_eq!(expected, got);
}

#[test]
fn a_missing_copy_of_a_repeated_item_is_deficit_not_absence() {
    // `expected` legitimately carries "a" twice; one copy lost is LOSS even
    // though "a" is still present — the mirror of the duplication blindness.
    let expected = multiset(["a", "a", "b"]);
    let got = multiset(["a", "b"]);
    assert!(surplus(&got, &expected).is_empty());
    assert_eq!(deficit(&got, &expected), vec![("a", 1)]);
    assert_eq!(describe(&deficit(&got, &expected)), "\"a\" x1");
}
