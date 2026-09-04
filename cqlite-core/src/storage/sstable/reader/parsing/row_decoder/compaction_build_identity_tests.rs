//! Issue #3809 (Finding 1, review round 2): the row-BUILD site of the
//! clustering-identity invariant — that
//! `V5CompressedLegacyParser::build_compaction_row_data` refuses an incomplete
//! clustering for EVERY non-static row carrying a ROW DELETION, on BOTH of the
//! arms that carry one:
//!
//! * a PURE row tombstone (`CompactionRowData::Tombstone`), and
//! * a row deletion COEXISTING with cells that survived it (issue #932 —
//!   `CompactionRowData::Live { row_deletion: Some(..) }`).
//!
//! The scalar cases of the invariant itself live in
//! `reader/compaction_row_tombstone_identity_tests.rs`; those pin the predicate
//! but CANNOT see which arms call it, because the predicate takes two counts and
//! knows nothing about the row shape. Only these cases — through the private
//! builder — can, which is why they exist as a child module of `compaction`
//! rather than beside the predicate.
//!
//! # Why both arms, from the consumer rather than from CQLite's behaviour (#3042)
//!
//! `merge::producer_iter_convert::extract_clustering_key_from_compaction` maps an
//! incomplete clustering to `None` on BOTH arms — reading `Tombstone.clustering`
//! for a pure tombstone, and the `simple` cells for a `Live` row — and
//! `build_merge_entry` attaches `row_deletion` to the entry either way. In the
//! `None` bucket (shared with the partition's STATIC row) `fold_row_deletions`
//! then adopts that `deletion_time` as the WHOLE group's row deletion and
//! `shadow_by_row_deletion` drops every cell at or below it. The two arms are
//! therefore the same defect, and validating one of them is not the invariant
//! this change publishes.
//!
//! # Cassandra authority (pinned `cassandra-5.0.8`, plus the guide)
//!
//! * `db/Clustering.java` — `Serializer.serialize` asserts
//!   `clustering.size() == types.size()`; `deserialize` reads exactly
//!   `types.size()` values. A partial clustering is not a writable shape.
//! * A row carries ONE clustering prefix, written from the row flags BEFORE the
//!   `row_size`, the `deletion` VInt pair (flag `0x10`) and the cell data
//!   (`db/rows/UnfilteredSerializer.java`; field order tabulated in
//!   `docs/sstables-definitive-guide/chapters/05-data-db-format.md`, "Row
//!   Structure"). So a row whose deletion coexists with surviving cells carries
//!   the same FULL clustering a pure tombstone does — the shape refused below is
//!   one Cassandra never writes, which is the point: it can only arise from a
//!   CQLite decoder / column-resolution regression.
//!
//! No BYTE route to the refusal exists today (`parse_clustering_prefix` is
//! arity-total on all four `ClusteringPrefix.Kind` arms), so these are scalar
//! cases over the builder, exactly like the predicate's own. Were a byte route
//! ever found, this stops being defence in depth and becomes a live defect.
//!
//! RED control, run by hand and recorded because a green suite proves nothing
//! about the hoist on its own: with the invariant call moved back INSIDE the
//! `if !has_simple_data && !has_complex_data` branch (its pre-hoist position),
//! `a_coexisting_row_deletion_with_an_incomplete_clustering_is_refused` is the
//! ONE case that fails, and it fails having built exactly the defect shape —
//! `Live { simple: [v], row_deletion: Some((9000, 1700000000)) }` with no
//! clustering at all.

use super::*;

use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use crate::storage::sstable::reader::compaction_row::CompactionRowData;
use crate::Error;

const KS: &str = "test_tomb";
const TBL: &str = "coexisting_row_deletion";

/// `(pk int, ck0 int, ..., s text static, v text, PRIMARY KEY (pk, ck0, ...))`
/// with `clustering_arity` clustering columns.
fn schema(clustering_arity: usize) -> TableSchema {
    let col = |name: &str, ty: &str, is_static: bool| Column {
        name: name.into(),
        data_type: ty.into(),
        nullable: true,
        default: None,
        is_static,
    };
    let mut columns = vec![col("pk", "int", false)];
    let mut clustering_keys = Vec::with_capacity(clustering_arity);
    for i in 0..clustering_arity {
        clustering_keys.push(ClusteringColumn {
            name: ck_name(i),
            data_type: "int".into(),
            position: i,
            order: Default::default(),
        });
        columns.push(col(&ck_name(i), "int", false));
    }
    columns.push(col("s", "text", true));
    columns.push(col("v", "text", false));
    TableSchema {
        keyspace: KS.into(),
        table: TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys,
        columns,
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn ck_name(i: usize) -> String {
    format!("ck{i}")
}

/// A row header. `marked_for_delete_at` + `local_deletion_time` are what make it
/// a ROW DELETION (`RowHeader::is_row_tombstone`, keyed on the `HAS_DELETION`
/// flag's `local_deletion_time`).
fn header(row_deletion: bool) -> RowHeader {
    RowHeader {
        timestamp: Some(4_000),
        ttl: None,
        liveness_expires_at_seconds: None,
        local_deletion_time: row_deletion.then_some(1_700_000_000),
        marked_for_delete_at: row_deletion.then_some(9_000),
        header_size: 0,
        row_size_vint_len: 0,
        missing_columns_bitmap: None,
        max_data_cell_timestamp: None,
        max_data_cell_expires_at: None,
        has_live_forever_data_cell: false,
        has_deleted_data_cell: false,
    }
}

/// The cells a decoded row surfaces: `recovered_clustering` clustering
/// pseudo-cells (#229, in schema order) plus, when `surviving_cell`, one
/// NON-primary-key data cell — which is what sends the row down the coexistence
/// arm instead of the pure-tombstone one.
fn cells(recovered_clustering: usize, surviving_cell: bool) -> RowCells {
    let mut out: RowCells = Vec::new();
    for i in 0..recovered_clustering {
        out.push((Arc::from(ck_name(i).as_str()), Value::Integer(i as i32)));
    }
    if surviving_cell {
        out.push((Arc::from("v"), Value::text("survivor")));
    }
    out
}

fn build(
    schema: &TableSchema,
    cells: RowCells,
    row_deletion: bool,
    is_static: bool,
) -> Result<CompactionRowData> {
    let parser = V5CompressedLegacyParser::new(KS.to_string(), TBL.to_string(), 0, 0, None);
    let hdr = Some(header(row_deletion));
    let row_ts = row_write_timestamp(&hdr);
    parser.build_compaction_row_data(cells, None, HashMap::new(), &hdr, row_ts, schema, is_static)
}

/// Assert the refusal by VARIANT and CONTRACT, never by message text (#28).
fn assert_refused(err: Error) {
    assert!(
        matches!(err, Error::Corruption(_)),
        "the refusal must be a Corruption, got {err:?}"
    );
    assert!(
        !err.is_recoverable(),
        "the refusal must be non-recoverable so compaction stops rather than \
         retrying an input that reproduces it exactly, got {err:?}"
    );
}

/// THE FINDING. A non-static clustered row whose row deletion COEXISTS with a
/// surviving data cell, and whose clustering is incomplete, must be REFUSED —
/// not emitted as `Live { row_deletion: Some(..) }` with an unidentifiable
/// clustering. Both an EMPTY clustering (arity 1, nothing recovered) and a SHORT
/// one (arity 2, one value recovered) are covered.
#[test]
fn a_coexisting_row_deletion_with_an_incomplete_clustering_is_refused() {
    for (arity, recovered) in [(1usize, 0usize), (2, 0), (2, 1), (3, 2)] {
        let err = build(&schema(arity), cells(recovered, true), true, false).expect_err(&format!(
            "arity {arity} with {recovered} recovered clustering value(s) plus a \
             surviving cell must be REFUSED (#3809 Finding 1)"
        ));
        assert_refused(err);
    }
}

/// DISCRIMINATION for the case above: the very same shape with a COMPLETE
/// clustering is accepted AND lands on the coexistence arm
/// (`Live { row_deletion: Some(..) }`), so the refusal above is a statement about
/// THAT arm and not about the pure-tombstone one.
#[test]
fn a_coexisting_row_deletion_with_a_complete_clustering_stays_live() {
    for arity in [1usize, 2, 3] {
        let built = build(&schema(arity), cells(arity, true), true, false)
            .unwrap_or_else(|e| panic!("a complete clustering (arity {arity}) must pass: {e:?}"));
        match built {
            CompactionRowData::Live {
                row_deletion,
                simple,
                ..
            } => {
                assert_eq!(
                    row_deletion,
                    Some((9_000, 1_700_000_000)),
                    "the coexisting row deletion must be carried (issue #932)"
                );
                assert!(
                    simple.iter().any(|c| c.column == "v"),
                    "the surviving cell must be carried, not dropped"
                );
            }
            other => panic!(
                "a row deletion with a surviving cell must be Live, got {other:?} \
                 (arity {arity})"
            ),
        }
    }
}

/// The PURE tombstone arm still refuses: hoisting the check above the branch
/// must not have moved it OFF the arm it was written for.
#[test]
fn a_pure_row_tombstone_with_an_incomplete_clustering_is_still_refused() {
    for (arity, recovered) in [(1usize, 0usize), (2, 1)] {
        let err = build(&schema(arity), cells(recovered, false), true, false).expect_err(&format!(
            "a pure tombstone at arity {arity} with {recovered} recovered must be REFUSED"
        ));
        assert_refused(err);
    }
}

/// A COMPLETE pure tombstone is emitted as `Tombstone` carrying its clustering
/// in schema order (#912), unchanged by the hoist.
#[test]
fn a_pure_row_tombstone_with_a_complete_clustering_keeps_its_clustering() {
    let built = build(&schema(2), cells(2, false), true, false)
        .unwrap_or_else(|e| panic!("a complete pure tombstone must pass: {e:?}"));
    match built {
        CompactionRowData::Tombstone {
            deletion_time,
            local_deletion_time,
            clustering,
        } => {
            assert_eq!(deletion_time, 9_000);
            assert_eq!(local_deletion_time, 1_700_000_000);
            assert_eq!(
                clustering,
                vec![
                    ("ck0".to_string(), Value::Integer(0)),
                    ("ck1".to_string(), Value::Integer(1)),
                ],
                "the clustering prefix must be rebuilt in SCHEMA order (#912)"
            );
        }
        other => panic!("a row deletion with no surviving cell must be a Tombstone, got {other:?}"),
    }
}

/// THE STATED BOUNDARY (issue #3809 AC4): a row carrying NO row deletion is NOT
/// validated, even with an incomplete clustering. It has no `deletion_time`, so
/// it can never become the `None` bucket's row deletion and can shadow nothing —
/// the harm the invariant exists for does not arise — and refusing it would red a
/// whole compaction read over a shape whose worst outcome is the pre-#912
/// unclustered reconciliation of its own cells.
///
/// This is a RECORDED DECISION, not an accident: it fails if the guard is ever
/// widened to every row without that consequence argument being revisited.
#[test]
fn a_row_with_no_row_deletion_is_not_validated() {
    for (arity, recovered) in [(1usize, 0usize), (2, 1), (3, 0)] {
        let built =
            build(&schema(arity), cells(recovered, true), false, false).unwrap_or_else(|e| {
                panic!(
                    "a row with NO row deletion must not be validated here \
                     (arity {arity}, recovered {recovered}): {e:?}"
                )
            });
        match built {
            CompactionRowData::Live { row_deletion, .. } => assert_eq!(
                row_deletion, None,
                "no HAS_DELETION was set, so no row deletion may be carried"
            ),
            other => panic!("expected Live, got {other:?}"),
        }
    }
}

/// The `is_static` EXEMPTION survives the hoist, on BOTH arms: a static row's
/// clustering is `[]` at every arity, whether or not a cell survives its row
/// deletion.
///
/// Oracle: `cassandra-5.0.8` `db/Clustering.java:102,124` — `Clustering.EMPTY`
/// and `Clustering.STATIC_CLUSTERING` differ by `kind()`, a distinction this
/// `Vec<(String, Value)>` cannot carry, and a static row has no clustering prefix
/// on disk at all (`UnfilteredSerializer` writes none when `IS_STATIC` is set).
#[test]
fn a_static_row_with_a_row_deletion_keeps_its_empty_clustering() {
    for arity in [1usize, 2, 5] {
        for surviving_cell in [false, true] {
            build(&schema(arity), cells(0, surviving_cell), true, true).unwrap_or_else(|e| {
                panic!(
                    "a STATIC row's empty clustering is CORRECT input \
                     (arity {arity}, surviving_cell {surviving_cell}): {e:?}"
                )
            });
        }
    }
}

/// A table with NO clustering columns: `[]` is the complete and only clustering
/// it has, on both arms.
#[test]
fn a_table_with_no_clustering_columns_is_accepted_on_both_arms() {
    for surviving_cell in [false, true] {
        build(&schema(0), cells(0, surviving_cell), true, false).unwrap_or_else(|e| {
            panic!("no clustering columns declared is CORRECT (surviving_cell {surviving_cell}): {e:?}")
        });
    }
}
