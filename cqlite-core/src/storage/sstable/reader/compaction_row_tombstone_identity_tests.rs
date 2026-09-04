//! Issue #3809 (Finding 1): the SCALAR cases of
//! [`CompactionRowData::require_tombstone_clustering_identity`] ([`super`] =
//! `compaction_row`) — the invariant that a ROW DELETION may never be emitted
//! having LOST the clustering identity of the row it deletes.
//!
//! These cases pin the PREDICATE. They cannot see WHICH ARMS call it, because it
//! takes two counts and knows nothing about the row shape: that half — the pure
//! `Tombstone` arm, the #932 `Live { row_deletion: Some(..) }` coexistence arm,
//! and the deliberately-unvalidated no-row-deletion boundary — is pinned in
//! `parsing/row_decoder/compaction_build_identity_tests.rs`.
//!
//! Split out of `compaction_row.rs` to keep that source under the campsite-rule
//! size limit (epic #1116); included via
//! `#[cfg(test)] #[path = "compaction_row_tombstone_identity_tests.rs"] mod ...;`
//! there. In-crate rather than an integration test because the invariant is
//! `pub(crate)` (#3366) — the BYTE-LEVEL case, on real Cassandra-written bytes,
//! is `cqlite-core/tests/issue_3809_tombstone_clustering_identity.rs`.

use super::CompactionRowData;
use crate::Error;

const KS: &str = "test_tomb";
const TBL: &str = "static_with_tombstones";

/// REFUSAL. A non-static row on a clustered table that recovered FEWER
/// clustering values than the schema declares must be refused, never emitted
/// as `Tombstone { clustering: [] }`.
///
/// Oracle: `cassandra-5.0.8` `db/Clustering.java` — `Serializer.serialize`
/// asserts `clustering.size() == types.size()` and `deserialize` reads exactly
/// `types.size()` values, so a partial clustering is not a writable shape.
///
/// The arities are 7 and 3 ON PURPOSE: the boilerplate `#912`/`#3809` in the
/// message already contains the digits `1`, `2`, `3`, `8`, `9` and `0`, so a
/// needle like `"2"` would be satisfied by an issue number and would assert
/// nothing about the arities (#3809 review). `7` appears nowhere else, and the
/// needles below are PHRASES, not digits.
#[test]
fn a_short_clustering_on_a_non_static_clustered_row_is_refused() {
    let err = CompactionRowData::require_tombstone_clustering_identity(KS, TBL, false, 7, 3)
        .expect_err("7 declared clustering columns and 3 recovered must be REFUSED (#3809)");

    // Discriminate on the VARIANT and its CONTRACT, never on message text
    // (#28): re-deriving the same input reproduces the defect, so no retry can
    // help and compaction must stop.
    assert!(
        matches!(err, Error::Corruption(_)),
        "the refusal must be a Corruption, got {err:?}"
    );
    assert!(
        !err.is_recoverable(),
        "the refusal must be non-recoverable so compaction stops, got {err:?}"
    );

    let msg = err.to_string();
    for needle in [
        KS,
        TBL,
        "declares 7",
        "only 3",
        "3809",
        "ON-DISK BYTES MAY BE SOUND",
    ] {
        assert!(
            msg.contains(needle),
            "the refusal must name {needle:?} (keyspace.table, declared vs \
             recovered arity, the issue, and that a fire indicates a reader \
             defect rather than damaged data); got: {msg}"
        );
    }
    // The message is operator-facing: a multi-line `format!` string literal
    // without `\` continuations leaks runs of source indentation into it.
    assert!(
        !msg.contains("  "),
        "the diagnostic must be single-spaced, got: {msg:?}"
    );
}

/// A STATIC row's `[]` is CORRECT input at every declared arity. Discriminating:
/// with the `is_static` exemption removed, `declared >= 1, recovered 0` refuses.
///
/// Oracle: `cassandra-5.0.8` `db/Clustering.java:102,124` — `Clustering.EMPTY`
/// and `Clustering.STATIC_CLUSTERING` differ by `kind()`, a distinction this
/// `Vec<(String, Value)>` cannot carry, and a static row has no clustering
/// prefix on disk at all.
#[test]
fn a_static_row_keeps_its_empty_clustering() {
    for declared in [0usize, 1, 2, 5] {
        CompactionRowData::require_tombstone_clustering_identity(KS, TBL, true, declared, 0)
            .unwrap_or_else(|e| {
                panic!("a static row's empty clustering is CORRECT (declared={declared}): {e:?}")
            });
    }
}

/// A table with NO clustering columns: `[]` is the complete and only clustering
/// it has.
///
/// `(0, 0)` is the real shape and passes with or without the retained
/// `declared_clustering_columns == 0` exemption (`0 < 0` is false), so it is an
/// acceptance case and NOT a control — stating that rather than mislabelling it
/// is the point (#3809 review). `(0, 1)` is the discriminating case: it is
/// unreachable from the builder (which stops at the first gap, so
/// `recovered <= declared`), and it is what keeps this table shape correct if
/// the predicate is ever tightened to the arity-TOTAL `!=` that
/// `Clustering.java`'s own assert is — i.e. it fails once the exemption is
/// dropped AND the predicate tightened.
#[test]
fn a_table_with_no_clustering_columns_keeps_its_empty_clustering() {
    for recovered in [0usize, 1] {
        CompactionRowData::require_tombstone_clustering_identity(
            "test_basic",
            "users",
            false,
            0,
            recovered,
        )
        .unwrap_or_else(|e| {
            panic!("no clustering columns declared is CORRECT (recovered={recovered}): {e:?}")
        });
    }
}

/// A COMPLETE clustering is accepted at every arity, so the guard fires on the
/// incomplete case and nothing else.
#[test]
fn a_complete_clustering_is_accepted() {
    for arity in [1usize, 2, 3, 16] {
        CompactionRowData::require_tombstone_clustering_identity(KS, TBL, false, arity, arity)
            .unwrap_or_else(|e| panic!("a complete clustering (arity {arity}) must pass: {e:?}"));
    }
}
