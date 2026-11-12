//! Directed equal-timestamp tie properties (issue #4497, AC 1: "equal-timestamp ties,
//! the #498 tiebreak class"), each resolved against CASSANDRA, not against CQLite.
//!
//! For every tie class the property is commutativity of the real merge over exactly two
//! contenders: `merge(x, y) == merge(y, x)`. Cassandra's reconcile is commutative by
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
        commutes(expire, &t, &cell(v, expiring))?;
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
    #[ignore = "#TBD equal-ts live cells with different values: winner depends on stream order (Cells.java:128)"]
    fn tie_live_vs_live_different_values(v1 in 0u8..3, v2 in 0u8..3) {
        commutes(false, &cell(v1, None), &cell(v2, None))?;
    }

    /// Cassandra rule (1): at equal timestamp an EXPIRING cell beats a LIVE one
    /// (CASSANDRA-14592), regardless of arrival order.
    #[test]
    #[ignore = "#TBD equal-ts expiring vs live: winner depends on stream order (Cells.java:93-98)"]
    fn tie_expiring_vs_live(v1 in 0u8..3, v2 in 0u8..3, ttl in 1u32..4, ldt in 997i32..1004) {
        commutes(false, &cell(v1, Some((ttl, ldt))), &cell(v2, None))?;
    }

    /// Cassandra rules (3)+(4): two EXPIRING cells at equal timestamp — larger LDT, then
    /// lower TTL, then larger value.
    #[test]
    #[ignore = "#TBD equal-ts expiring vs expiring: winner depends on stream order (Cells.java:111-125)"]
    fn tie_expiring_vs_expiring(
        v1 in 0u8..3, v2 in 0u8..3,
        t1 in 1u32..4, t2 in 1u32..4,
        l1 in 997i32..1004, l2 in 997i32..1004,
    ) {
        commutes(false, &cell(v1, Some((t1, l1))), &cell(v2, Some((t2, l2))))?;
    }

    /// Cassandra rule (3): two cell TOMBSTONES at equal timestamp — larger LDT wins.
    #[test]
    #[ignore = "#TBD equal-ts cell tombstones with different LDT: winner depends on stream order (Cells.java:111-118)"]
    fn tie_tombstone_vs_tombstone_different_ldt(l1 in 400i32..406, l2 in 400i32..406) {
        let t = |ldt| Spec::CellTomb { ck: 0, col: 0, ts: 3, ldt };
        commutes(false, &t(l1), &t(l2))?;
    }

    /// `DeletionTime.supersedes`: equal-timestamp ROW deletions — larger LDT wins.
    #[test]
    #[ignore = "#TBD equal-ts row deletions with different LDT: winner depends on stream order (DeletionTime.java:158)"]
    fn tie_row_deletion_different_ldt(l1 in 400i32..406, l2 in 400i32..406) {
        let d = |ldt| Spec::RowDel { ck: 0, ts: 3, ldt };
        commutes(false, &d(l1), &d(l2))?;
    }

    /// Same, RANGE tombstones over the same range. CQLite agrees with Cassandra here (the
    /// coalescer is order-independent), so this is a live regression guard, not a finding.
    #[test]
    fn tie_range_deletion_different_ldt(l1 in 400i32..406, l2 in 400i32..406) {
        let d = |ldt| Spec::RangeDel { lo: 0, hi: 1, ts: 3, ldt };
        commutes(false, &d(l1), &d(l2))?;
    }

    /// Same, PARTITION deletions.
    #[test]
    #[ignore = "#TBD equal-ts partition deletions with different LDT: winner depends on stream order (DeletionTime.java:158)"]
    fn tie_partition_deletion_different_ldt(l1 in 400i32..406, l2 in 400i32..406) {
        let d = |ldt| Spec::PartDel { ts: 3, ldt };
        commutes(false, &d(l1), &d(l2))?;
    }
}
