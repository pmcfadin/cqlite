//! The three reconciliation laws over the REAL merge (issue #4497, AC 1).
//!
//! `merge(x, y)` is `KWayMerger::merge_partition_rows(x ++ y)` — the production
//! per-partition reconcile (cell winners, row/range/partition deletions, TTL expiry,
//! range + partition shadowing) — and its output is a valid input again, which is what
//! makes associativity and idempotence statable.
//!
//! The streams come from the `Clean` profile: every equal-timestamp collision is one
//! that CQLite and Cassandra both order (tombstone beats live and expiring). The
//! AMBIGUOUS ties (`Wild` profile) have their own directed properties in `ties.rs`, so
//! a divergence there cannot hide behind, or be mistaken for, a law failure here.
//!
//! Oracle: Cassandra 5.0.8 documents the cell law outright — `Cells.reconcile` javadoc:
//! "This method is commutative over it's cells arguments: reconcile(a, b, n) ==
//! reconcile(b, a, n)" (`db/rows/Cells.java:59`) — and `DeletionTime.supersedes`
//! (`db/DeletionTime.java:158`) is a total order on `(markedForDeleteAt, localDeletionTime)`.

use proptest::prelude::*;

use super::generators::{arb_stream, build_all, merger, Profile, Spec};
use crate::storage::write_engine::merge::{MergeEntry, RowData};

/// Order-insensitive rendering of a merge result. Cell vectors are sorted (their order is
/// an artifact of hashing, not a semantic); everything else is compared verbatim, entry
/// order included (it is part of the output contract). `run_index` is excluded: it is
/// provenance, not content.
pub(super) fn norm(rows: &[MergeEntry]) -> Vec<String> {
    rows.iter()
        .map(|e| {
            let data = match &e.row_data {
                RowData::Live { cells } => {
                    let mut c: Vec<String> = cells.iter().map(|c| format!("{c:?}")).collect();
                    c.sort();
                    format!("Live{c:?}")
                }
                other => format!("{other:?}"),
            };
            let mut cd: Vec<String> = e
                .complex_deletions
                .iter()
                .map(|d| format!("{d:?}"))
                .collect();
            cd.sort();
            format!(
                "{:?}|{:?}|{}|{}|{:?}|{:?}|{:?}|{:?}|{:?}",
                e.key,
                e.clustering_key,
                e.timestamp,
                data,
                cd,
                e.range_deletion,
                e.row_deletion,
                e.partition_deletion,
                e.row_liveness
            )
        })
        .collect()
}

/// `merge(x ++ y)` on specs.
pub(super) fn merge(expire: bool, parts: &[&[Spec]]) -> Vec<MergeEntry> {
    let rows: Vec<MergeEntry> = parts.iter().flat_map(|p| build_all(p)).collect();
    merger(expire)
        .merge_partition_rows(rows)
        .expect("merge_partition_rows must not fail on generated streams")
}

/// Re-feed a merge result as an input stream.
fn merge_entries(expire: bool, parts: Vec<Vec<MergeEntry>>) -> Vec<MergeEntry> {
    merger(expire)
        .merge_partition_rows(parts.into_iter().flatten().collect())
        .expect("merge_partition_rows must not fail on a merge result")
}

fn dup(rows: &[MergeEntry]) -> Vec<MergeEntry> {
    rows.to_vec()
}

proptest! {
    #![proptest_config(super::proptest_cfg::config("reconcile-laws"))]

    /// merge(a, b) == merge(b, a)
    #[test]
    fn merge_is_commutative(
        a in arb_stream(Profile::Clean),
        b in arb_stream(Profile::Clean),
        expire in any::<bool>(),
    ) {
        let ab = norm(&merge(expire, &[&a, &b]));
        let ba = norm(&merge(expire, &[&b, &a]));
        prop_assert_eq!(ab, ba);
    }

    /// merge(merge(a, b), c) == merge(a, merge(b, c))
    #[test]
    fn merge_is_associative(
        a in arb_stream(Profile::Clean),
        b in arb_stream(Profile::Clean),
        c in arb_stream(Profile::Clean),
        expire in any::<bool>(),
    ) {
        let left = merge_entries(expire, vec![merge(expire, &[&a, &b]), build_all(&c)]);
        let right = merge_entries(expire, vec![build_all(&a), merge(expire, &[&b, &c])]);
        prop_assert_eq!(norm(&left), norm(&right));
    }

    /// merge(a, a) == a, for a an already-reconciled stream (and merge(a, a) == merge(a)
    /// for a raw one).
    #[test]
    fn merge_is_idempotent(
        a in arb_stream(Profile::Clean),
        expire in any::<bool>(),
    ) {
        let once = merge(expire, &[&a]);
        let twice_raw = merge(expire, &[&a, &a]);
        prop_assert_eq!(norm(&once), norm(&twice_raw));
        let twice = merge_entries(expire, vec![dup(&once), dup(&once)]);
        prop_assert_eq!(norm(&once), norm(&twice));
    }
}

proptest! {
    #![proptest_config(super::proptest_cfg::config("reconcile-laws-wild"))]

    /// Idempotence does not depend on how ambiguous the ties are: duplicating a stream
    /// keeps the same first-seen winner. Run over the `Wild` profile, whose ambiguous ties
    /// (value / expiring / LDT) make commutativity fail in `ties.rs`.
    #[test]
    fn merge_is_idempotent_even_with_ambiguous_ties(
        a in arb_stream(Profile::Wild),
        expire in any::<bool>(),
    ) {
        prop_assert_eq!(norm(&merge(expire, &[&a])), norm(&merge(expire, &[&a, &a])));
    }
}

/// The generator must actually exercise every stream feature the issue lists (AC 1);
/// otherwise a law could pass over streams that never contain the hard cases.
#[test]
fn generator_covers_every_stream_feature() {
    use proptest::strategy::{Strategy, ValueTree};
    use proptest::test_runner::{Config, RngAlgorithm, RngSeed, TestRunner};
    let mut runner = TestRunner::new_with_rng(
        Config {
            rng_seed: RngSeed::Fixed(4497),
            ..Config::default()
        },
        proptest::test_runner::TestRng::from_seed(RngAlgorithm::default(), &[7u8; 32]),
    );
    let strat = arb_stream(Profile::Clean);
    let mut seen = [false; 7];
    for _ in 0..400 {
        let stream = strat.new_tree(&mut runner).expect("tree").current();
        for s in &stream {
            match s {
                Spec::Cell(c) if c.expiring.is_some() => seen[0] = true,
                Spec::Cell(_) => seen[1] = true,
                Spec::CellTomb { .. } => seen[2] = true,
                Spec::RowDel { .. } => seen[3] = true,
                Spec::RangeDel { .. } => seen[4] = true,
                Spec::PartDel { .. } => seen[5] = true,
            }
        }
        // an equal-timestamp collision on the same (ck, col) slot between a tombstone and a cell
        for x in &stream {
            for y in &stream {
                if let (Spec::CellTomb { ck, col, ts, .. }, Spec::Cell(c)) = (x, y) {
                    if (*ck, *col, *ts) == (c.ck, c.col, c.ts) {
                        seen[6] = true;
                    }
                }
            }
        }
    }
    assert_eq!(
        seen,
        [true; 7],
        "[expiring, live, cell tombstone, row del, range del, partition del, equal-ts tombstone/cell collision]"
    );
}
