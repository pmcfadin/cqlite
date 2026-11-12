//! Directed equal-timestamp tie properties (issue #4497, AC 1: "equal-timestamp ties,
//! the #498 tiebreak class"), each resolved against CASSANDRA, not against CQLite.
//!
//! For every tie class the property is that the real merge over exactly two contenders,
//! in BOTH orders, equals the merge of the winner Cassandra's rule names on its own
//! (`resolves_to`), which implies `merge(x, y) == merge(y, x)`. Cassandra's reconcile is commutative by
//! contract (`Cells.reconcile` javadoc, `db/rows/Cells.java:59`) and its tie-break is
//! total, so ANY failure here is a CQLite-vs-Cassandra divergence.
//!
//! Cassandra 5.0.8 `Cells.resolveRegular` (`db/rows/Cells.java:79-129`) orders cells with
//! EQUAL timestamp by, in turn: (1) tombstone/expiring beats live; (2) tombstone beats
//! expiring; (3) both expiring (or both tombstone): larger `localDeletionTime`; (4) both
//! expiring: LOWER ttl; (5) otherwise `compareValues(left, right) >= 0` — the LARGER
//! value. CQLite's shared rule (`reconcile_rules::cell_wins`) implements only (2) and
//! the tombstone half of (1); everything else keeps the first-seen cell, so the winner
//! depends on stream order.
//!
//! Row/range/partition deletions: `DeletionTime.supersedes` (`db/DeletionTime.java:158`)
//! is `mfda > other.mfda || (mfda == other.mfda && ldt > other.ldt)` — equal timestamps
//! are broken by the LARGER `localDeletionTime`.
//!
//! Classes that FAIL today are `#[ignore]`d with the tracking marker and are NOT made to
//! pass here (product code is out of scope for this change); run them with
//! `cargo test ... -- --include-ignored`.

use proptest::prelude::*;

use super::generators::{CellSpec, Spec};
use super::laws::{merge, norm};

fn commutes(expire: bool, x: &Spec, y: &Spec) -> Result<(), TestCaseError> {
    let xy = norm(&merge(
        expire,
        &[std::slice::from_ref(x), std::slice::from_ref(y)],
    ));
    let yx = norm(&merge(
        expire,
        &[std::slice::from_ref(y), std::slice::from_ref(x)],
    ));
    prop_assert_eq!(xy, yx);
    Ok(())
}

/// Asserts the merge of `x` and `y`, in BOTH input orders, equals the merge of the
/// contender Cassandra picks on its own. Commutativity alone would accept a rule that is
/// deterministic but wrong (always the smaller value, say); naming the expected winner
/// pins the Cassandra rule itself.
fn resolves_to(expire: bool, x: &Spec, y: &Spec, winner: &Spec) -> Result<(), TestCaseError> {
    let expected = norm(&merge(expire, &[std::slice::from_ref(winner)]));
    for (a, b) in [(x, y), (y, x)] {
        let got = norm(&merge(
            expire,
            &[std::slice::from_ref(a), std::slice::from_ref(b)],
        ));
        prop_assert_eq!(&got, &expected, "merge({:?}, {:?})", a, b);
    }
    Ok(())
}

/// Cassandra 5.0.8 `Cells.resolveRegular` (`db/rows/Cells.java:79-129`) for two cells with
/// EQUAL timestamps, transcribed rule by rule. `Spec::Cell::expiring` is `(ttl, ldt)`.
fn cassandra_cell_winner<'a>(left: &'a Spec, right: &'a Spec) -> &'a Spec {
    // The cell payload is the text `v{value}` (generators.rs); Cassandra compares the
    // serialized value bytes (UTF8Type is bytewise).
    fn payload(v: u8) -> Vec<u8> {
        format!("v{v}").into_bytes()
    }
    // (expiring-or-tombstone, tombstone, localDeletionTime, ttl, value bytes)
    fn facts(s: &Spec) -> (bool, bool, i64, u32, Vec<u8>) {
        match s {
            Spec::Cell(c) => match c.expiring {
                Some((ttl, ldt)) => (true, false, i64::from(ldt), ttl, payload(c.value)),
                None => (false, false, i64::MAX, 0, payload(c.value)),
            },
            Spec::CellTomb { ldt, .. } => (true, true, i64::from(*ldt), 0, Vec::new()),
            other => panic!("not a cell contender: {other:?}"),
        }
    }
    let (l_et, l_tomb, l_ldt, l_ttl, l_val) = facts(left);
    let (r_et, r_tomb, r_ldt, r_ttl, r_val) = facts(right);
    if l_et || r_et {
        if l_et != r_et {
            return if l_et { left } else { right };
        }
        if l_tomb != r_tomb {
            return if l_tomb { left } else { right };
        }
        if l_ldt != r_ldt {
            return if l_ldt > r_ldt { left } else { right };
        }
        if !l_tomb && l_ttl != r_ttl {
            return if l_ttl < r_ttl { left } else { right };
        }
    }
    if l_val >= r_val {
        left
    } else {
        right
    }
}

/// `DeletionTime.supersedes` (`db/DeletionTime.java:158`) at equal timestamps: the larger
/// local deletion time wins.
fn larger_ldt<'a>(x: &'a Spec, y: &'a Spec, ldt: impl Fn(&Spec) -> i32) -> &'a Spec {
    if ldt(x) >= ldt(y) {
        x
    } else {
        y
    }
}

fn cell(value: u8, expiring: Option<(u32, i32)>) -> Spec {
    Spec::Cell(CellSpec {
        ck: 0,
        col: 0,
        ts: 3,
        value,
        expiring,
    })
}

proptest! {
    #![proptest_config(super::proptest_cfg::config("reconcile-ties"))]

    /// Resolved class (CQLite agrees with Cassandra): a tombstone beats a live cell and an
    /// expiring cell at the same timestamp, whatever the LDT/ttl/value of the loser.
    #[test]
    fn tie_tombstone_beats_live_and_expiring(
        tomb_ldt in 400i32..406,
        v in 0u8..3,
        expiring in prop::option::of((1u32..4, 997i32..1004)),
        expire in any::<bool>(),
    ) {
        let t = Spec::CellTomb { ck: 0, col: 0, ts: 3, ldt: tomb_ldt };
        resolves_to(expire, &t, &cell(v, expiring), &t)?;
    }

    /// Resolved class: equal-timestamp ROW / RANGE / PARTITION deletions with the SAME
    /// local deletion time are identical, so order cannot matter.
    #[test]
    fn tie_identical_deletions_commute(ts in 1i64..4, l in 400i32..406) {
        for (x, y) in [
            (Spec::RowDel { ck: 0, ts, ldt: l }, Spec::RowDel { ck: 0, ts, ldt: l }),
            (Spec::RangeDel { lo: 0, hi: 1, ts, ldt: l }, Spec::RangeDel { lo: 0, hi: 1, ts, ldt: l }),
            (Spec::PartDel { ts, ldt: l }, Spec::PartDel { ts, ldt: l }),
        ] {
            commutes(false, &x, &y)?;
        }
    }

    /// Cassandra rule (5): two LIVE cells, equal timestamp, different values — the larger
    /// value wins, regardless of arrival order.
    #[test]
    #[ignore = "#4530 equal-ts live cells with different values: winner depends on stream order (Cells.java:128)"]
    fn tie_live_vs_live_different_values(v1 in 0u8..3, v2 in 0u8..3) {
        let (x, y) = (cell(v1, None), cell(v2, None));
        resolves_to(false, &x, &y, cassandra_cell_winner(&x, &y))?;
    }

    /// Cassandra rule (1): at equal timestamp an EXPIRING cell beats a LIVE one
    /// (CASSANDRA-14592), regardless of arrival order.
    #[test]
    #[ignore = "#4530 equal-ts expiring vs live: winner depends on stream order (Cells.java:93-98)"]
    fn tie_expiring_vs_live(v1 in 0u8..3, v2 in 0u8..3, ttl in 1u32..4, ldt in 997i32..1004) {
        let (x, y) = (cell(v1, Some((ttl, ldt))), cell(v2, None));
        resolves_to(false, &x, &y, cassandra_cell_winner(&x, &y))?;
    }

    /// Cassandra rules (3)+(4): two EXPIRING cells at equal timestamp — larger LDT, then
    /// lower TTL, then larger value.
    #[test]
    #[ignore = "#4530 equal-ts expiring vs expiring: winner depends on stream order (Cells.java:111-125)"]
    fn tie_expiring_vs_expiring(
        v1 in 0u8..3, v2 in 0u8..3,
        t1 in 1u32..4, t2 in 1u32..4,
        l1 in 997i32..1004, l2 in 997i32..1004,
    ) {
        let (x, y) = (cell(v1, Some((t1, l1))), cell(v2, Some((t2, l2))));
        resolves_to(false, &x, &y, cassandra_cell_winner(&x, &y))?;
    }

    /// Cassandra rule (3): two cell TOMBSTONES at equal timestamp — larger LDT wins.
    #[test]
    #[ignore = "#4530 equal-ts cell tombstones with different LDT: winner depends on stream order (Cells.java:111-118)"]
    fn tie_tombstone_vs_tombstone_different_ldt(l1 in 400i32..406, l2 in 400i32..406) {
        let t = |ldt| Spec::CellTomb { ck: 0, col: 0, ts: 3, ldt };
        let (x, y) = (t(l1), t(l2));
        resolves_to(false, &x, &y, cassandra_cell_winner(&x, &y))?;
    }

    /// `DeletionTime.supersedes`: equal-timestamp ROW deletions — larger LDT wins.
    #[test]
    #[ignore = "#4530 equal-ts row deletions with different LDT: winner depends on stream order (DeletionTime.java:158)"]
    fn tie_row_deletion_different_ldt(l1 in 400i32..406, l2 in 400i32..406) {
        let d = |ldt| Spec::RowDel { ck: 0, ts: 3, ldt };
        let (x, y) = (d(l1), d(l2));
        resolves_to(false, &x, &y, larger_ldt(&x, &y, |s| match s { Spec::RowDel { ldt, .. } => *ldt, _ => unreachable!() }))?;
    }

    /// Same, RANGE tombstones over the same range. CQLite agrees with Cassandra here (the
    /// coalescer is order-independent), so this is a live regression guard, not a finding.
    #[test]
    fn tie_range_deletion_different_ldt(l1 in 400i32..406, l2 in 400i32..406) {
        let d = |ldt| Spec::RangeDel { lo: 0, hi: 1, ts: 3, ldt };
        let (x, y) = (d(l1), d(l2));
        resolves_to(false, &x, &y, larger_ldt(&x, &y, |s| match s { Spec::RangeDel { ldt, .. } => *ldt, _ => unreachable!() }))?;
    }

    /// Same, PARTITION deletions.
    #[test]
    #[ignore = "#4530 equal-ts partition deletions with different LDT: winner depends on stream order (DeletionTime.java:158)"]
    fn tie_partition_deletion_different_ldt(l1 in 400i32..406, l2 in 400i32..406) {
        let d = |ldt| Spec::PartDel { ts: 3, ldt };
        let (x, y) = (d(l1), d(l2));
        resolves_to(false, &x, &y, larger_ldt(&x, &y, |s| match s { Spec::PartDel { ldt, .. } => *ldt, _ => unreachable!() }))?;
    }
}
