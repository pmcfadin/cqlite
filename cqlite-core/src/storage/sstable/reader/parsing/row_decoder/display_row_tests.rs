//! Direct unit tests for the row DISPLAY decisions the static-content rule rests on
//! (issue #3095): [`row_is_visible`], [`build_display_row_read_path`], and — because
//! the same decision is what the sliding driver's partition close consults — the
//! `complete` flag of `TimestampPolicy::on_partition_close`.
//!
//! These existed only end-to-end before, which is how the "count a suppressed marker
//! as a row" defect survived a review round.
//!
//! Cassandra grounding (pinned `cassandra-5.0.8`, quoted where it decides a case):
//! `db/transform/Filter.java` purges the static row and the clustering rows as
//! SEPARATE transformations (`applyToStatic` vs
//! `applyToRow(row) = row.purge(PURGE_ALL, nowInSec, enforceStrictLiveness)`), and
//! `db/transform/FilteredRows.java`'s
//! `isEmpty() { return staticRow().isEmpty() && !hasNext(); }` shows `hasNext()`
//! counts clustering rows only.

use super::*;

use crate::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};

/// `(pk int, ck int, s text static, v text, PRIMARY KEY (pk, ck))`.
fn schema() -> TableSchema {
    let col = |name: &str, ty: &str, is_static: bool| Column {
        name: name.into(),
        data_type: ty.into(),
        nullable: true,
        default: None,
        is_static,
    };
    TableSchema {
        keyspace: "ks".into(),
        table: "t".into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "int".into(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            col("pk", "int", false),
            col("ck", "int", false),
            col("s", "text", true),
            col("v", "text", false),
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn cells(pairs: &[(&str, Value)]) -> RowCells {
    pairs
        .iter()
        .map(|(n, v)| (Arc::from(*n), v.clone()))
        .collect()
}

/// A row header. `marked_for_delete_at` + `local_deletion_time` make it a ROW
/// TOMBSTONE (`RowHeader::is_row_tombstone`).
fn header(timestamp: Option<i64>, row_tombstone: bool) -> RowHeader {
    RowHeader {
        timestamp,
        ttl: None,
        liveness_expires_at_seconds: None,
        local_deletion_time: row_tombstone.then_some(1_700_000_000),
        marked_for_delete_at: row_tombstone.then_some(2_000),
        header_size: 0,
        row_size_vint_len: 0,
        missing_columns_bitmap: None,
        max_data_cell_timestamp: None,
        max_data_cell_expires_at: None,
        has_live_forever_data_cell: false,
    }
}

// ---------------------------------------------------------------------------
// row_is_visible
// ---------------------------------------------------------------------------

/// The predicate is TOTAL over `ScanRow` and matches what user-facing consumers
/// actually surface: a decoded row and a raw undecoded row are rows; a MARKER is not
/// (`integrity::filter_tombstone` keeps `Row`/`RawRow` and drops a row-tombstone
/// marker; `build_row_from_scan`'s `into_cells` suppresses a marker, issue #505).
#[test]
fn row_is_visible_is_true_exactly_for_rows() {
    assert!(row_is_visible(&ScanRow::Row(cells(&[(
        "v",
        Value::text("x")
    )]))));
    assert!(row_is_visible(&ScanRow::RawRow(vec![1, 2, 3])));
    // Every marker shape a decoder can emit is NOT a row.
    assert!(!row_is_visible(&ScanRow::Marker(Value::Null)));
    assert!(!row_is_visible(&ScanRow::Marker(Value::Tombstone(
        Box::new(crate::types::TombstoneInfo {
            deletion_time: 2_000,
            tombstone_type: crate::types::TombstoneType::RowTombstone,
            local_deletion_time: 1_700_000_000,
            ttl: None,
            range_start: None,
            range_end: None,
        })
    ))));
    // An EMPTY `Row` cannot occur (`build_display_row` maps it to a marker), but the
    // predicate must still be total and must not claim it is invisible for the wrong
    // reason — visibility is decided by the VARIANT, never by cell count.
    assert!(row_is_visible(&ScanRow::Row(Vec::new())));
}

/// The pairing that matters: `build_display_row` maps a PURE row tombstone to a
/// marker, and `row_is_visible` then reports it invisible. This is the exact
/// composition the static rule uses for `partition.hasNext()`.
#[test]
fn a_pure_row_tombstone_builds_a_marker_and_is_invisible() {
    let schema = schema();
    let built = build_display_row(
        cells(&[("ck", Value::Integer(1))]),
        Some(&header(None, true)),
        &schema,
    );
    assert!(
        matches!(built, ScanRow::Marker(_)),
        "a row tombstone with only its clustering pseudo-cell is a marker, got {built:?}"
    );
    assert!(!row_is_visible(&built));
}

// ---------------------------------------------------------------------------
// build_display_row_read_path
// ---------------------------------------------------------------------------

/// THE #3095 B1 case: statics must NOT revive a row-tombstoned clustering row.
///
/// Cassandra purges a clustering row from ITSELF (`Filter.applyToRow` →
/// `BTreeRow.purge` → `null` when its own liveness, deletion and cell btree are all
/// empty/live); the static row is a separate `Row` delivered by
/// `BaseRowIterator.staticRow()` and can never be in that btree.
#[test]
fn statics_do_not_revive_a_row_tombstoned_clustering_row() {
    let schema = schema();
    let statics = cells(&[("s", Value::text("surviving"))]);
    let built = build_display_row_read_path(
        cells(&[("ck", Value::Integer(1))]),
        &statics,
        Some(&header(None, true)),
        &schema,
    );
    assert!(
        matches!(built, ScanRow::Marker(_)),
        "the row tombstone must stay a MARKER — injecting the static cell first would \
         make `row_has_non_key_cell` true and surface a phantom live row, which is the \
         #3095 B1 defect. Got {built:?}"
    );
    assert!(!row_is_visible(&built));
}

/// The historical INJECT-FIRST order (what physical consumers still use) is what made
/// that phantom row: asserted here so the two orders are pinned side by side and the
/// read/physical seam is not "fixed" by accidentally unifying them.
#[test]
fn the_physical_inject_first_order_still_produces_the_live_row() {
    let schema = schema();
    let statics = cells(&[("s", Value::text("surviving"))]);
    let mut c = cells(&[("ck", Value::Integer(1))]);
    merge_static_cells(&mut c, &statics);
    let built = build_display_row(c, Some(&header(None, true)), &schema);
    assert!(
        matches!(built, ScanRow::Row(_)),
        "physical consumers (compaction, verify, delta-scan) keep the historical \
         order and must be BYTE-UNCHANGED by #3095; if this ever becomes a Marker, \
         their output changed. Got {built:?}"
    );
}

/// A SURVIVING clustering row receives the static values, clustering-row-wins.
#[test]
fn a_surviving_row_receives_the_static_values() {
    let schema = schema();
    let statics = cells(&[("s", Value::text("from-static-row"))]);
    let built = build_display_row_read_path(
        cells(&[("ck", Value::Integer(1)), ("v", Value::text("live"))]),
        &statics,
        Some(&header(Some(1_000), false)),
        &schema,
    );
    let ScanRow::Row(out) = built else {
        panic!("a live row must stay a row: {built:?}");
    };
    let get = |n: &str| {
        out.iter()
            .find(|(name, _)| name.as_ref() == n)
            .map(|(_, v)| v.clone())
    };
    assert_eq!(get("v"), Some(Value::text("live")));
    assert_eq!(get("s"), Some(Value::text("from-static-row")));
}

/// The DISJOINTNESS contract [`merge_static_cells`] documents, asserted rather than
/// assumed: the injected static names and a clustering row's own names are disjoint by
/// construction (a static cell's column has `is_static == true`, a clustering row's
/// cells are the clustering pseudo-cells plus `is_static == false` columns), so the
/// merge is a plain APPEND and no de-duplication is performed.
///
/// This is exactly why [`build_display_row_read_path`] must decide the row-tombstone
/// question BEFORE appending: the append cannot "lose" to an existing cell, so on a
/// pure row tombstone it would unconditionally manufacture a non-key cell.
///
/// The out-of-contract input (a clustering row that already carries the static column
/// — the write-side #1074 shape) is pinned here as APPEND, not overwrite, so the real
/// behaviour is recorded instead of a hoped-for one.
#[test]
fn the_static_merge_is_a_plain_append_over_disjoint_names() {
    let schema = schema();
    let statics = cells(&[("s", Value::text("from-static-row"))]);
    // In-contract (disjoint): exactly one `s`, taken from the static row.
    let built = build_display_row_read_path(
        cells(&[("ck", Value::Integer(1)), ("v", Value::text("live"))]),
        &statics,
        Some(&header(Some(1_000), false)),
        &schema,
    );
    let ScanRow::Row(out) = built else {
        panic!("expected a row: {built:?}");
    };
    let s: Vec<_> = out
        .iter()
        .filter(|(n, _)| n.as_ref() == "s")
        .map(|(_, v)| v.clone())
        .collect();
    assert_eq!(s, vec![Value::text("from-static-row")]);

    // Out-of-contract (#1074 shape): the merge APPENDS — both cells are present, and
    // which one a name-keyed consumer surfaces is that consumer's rule, not this one's.
    let built = build_display_row_read_path(
        cells(&[("ck", Value::Integer(1)), ("s", Value::text("own"))]),
        &statics,
        Some(&header(Some(1_000), false)),
        &schema,
    );
    let ScanRow::Row(out) = built else {
        panic!("expected a row: {built:?}");
    };
    let s: Vec<_> = out
        .iter()
        .filter(|(n, _)| n.as_ref() == "s")
        .map(|(_, v)| v.clone())
        .collect();
    assert_eq!(
        s,
        vec![Value::text("own"), Value::text("from-static-row")],
        "merge_static_cells appends without a membership check (its documented \
         disjointness contract); the row's own cell keeps its position"
    );
}

/// With NO static values there is nothing to inject and the row is unchanged — the
/// partition-without-a-static-row case.
#[test]
fn no_static_values_leaves_the_row_unchanged() {
    let schema = schema();
    let built = build_display_row_read_path(
        cells(&[("ck", Value::Integer(1)), ("v", Value::text("live"))]),
        &Vec::new(),
        Some(&header(Some(1_000), false)),
        &schema,
    );
    let ScanRow::Row(out) = built else {
        panic!("expected a row: {built:?}");
    };
    assert_eq!(out.len(), 2, "no phantom static cell was added: {out:?}");
}

// ---------------------------------------------------------------------------
// TimestampPolicy::on_partition_close's `complete` flag
// ---------------------------------------------------------------------------

/// Build a read-path parser (`read_shadowing = true`) for the policy under test.
fn read_parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new("ks".into(), "t".into(), 0, 0, None).with_read_shadowing(true)
}

/// A STRUCTURALLY-complete partition with a live static row and no clustering row
/// synthesizes exactly ONE row.
#[test]
fn on_partition_close_synthesizes_one_row_when_complete() {
    let parser = read_parser();
    let schema = schema();
    let mut policy = super::TimestampPolicy::new(&parser);
    policy.seed_static_for_test(RowKey::new(1i32.to_be_bytes().to_vec()), &schema, 1_234);
    let mut pending = Vec::new();
    policy.on_partition_close(&schema, &mut pending, true);
    assert_eq!(pending.len(), 1, "exactly one static-only row: {pending:?}");
    let (_, _, row, ts) = &pending[0];
    assert!(
        matches!(row, ScanRow::Row(_)),
        "the synthesized row is a real row, not a marker: {row:?}"
    );
    assert_eq!(
        *ts, 1_234,
        "it reports the STATIC row's own write timestamp"
    );
}

/// An INCOMPLETE (truncated / unparseable-tail) partition synthesizes NOTHING: "this
/// partition yielded no clustering row" is not knowable from a partially-observed
/// body, so the rule fails closed rather than inventing a row.
#[test]
fn on_partition_close_synthesizes_nothing_when_incomplete() {
    let parser = read_parser();
    let schema = schema();
    let mut policy = super::TimestampPolicy::new(&parser);
    policy.seed_static_for_test(RowKey::new(1i32.to_be_bytes().to_vec()), &schema, 1_234);
    let mut pending = Vec::new();
    policy.on_partition_close(&schema, &mut pending, false);
    assert!(
        pending.is_empty(),
        "a truncated partition must not synthesize a row: {pending:?}"
    );
}

/// A PHYSICAL parser (`read_shadowing = false`) never synthesizes: compaction,
/// `verify` and delta-scan must see exactly the on-disk unfiltereds.
#[test]
fn on_partition_close_synthesizes_nothing_for_a_physical_consumer() {
    let parser = V5CompressedLegacyParser::new("ks".into(), "t".into(), 0, 0, None);
    let schema = schema();
    let mut policy = super::TimestampPolicy::new(&parser);
    policy.seed_static_for_test(RowKey::new(1i32.to_be_bytes().to_vec()), &schema, 1_234);
    let mut pending = Vec::new();
    policy.on_partition_close(&schema, &mut pending, true);
    assert!(
        pending.is_empty(),
        "a physical consumer must get no synthesized row: {pending:?}"
    );
}

/// Once the partition has produced a VISIBLE clustering row, the held static row is
/// correctly NOT emitted separately (Cassandra's N > 0 branch: N rows, statics on
/// each, no extra row).
#[test]
fn on_partition_close_synthesizes_nothing_after_a_visible_row() {
    let parser = read_parser();
    let schema = schema();
    let mut policy = super::TimestampPolicy::new(&parser);
    policy.seed_static_for_test(RowKey::new(1i32.to_be_bytes().to_vec()), &schema, 1_234);
    policy.note_visible_row_for_test();
    let mut pending = Vec::new();
    policy.on_partition_close(&schema, &mut pending, true);
    assert!(
        pending.is_empty(),
        "with a visible clustering row there is no separate static row: {pending:?}"
    );
}

/// `on_partition_open` RESETs partition-scoped state (issue #3095 B2): after it, a
/// previous partition's static values and its "had a visible row" flag are gone, so a
/// LATER static-only partition still synthesizes its row.
#[test]
fn on_partition_open_resets_partition_scoped_state() {
    let parser = read_parser();
    let schema = schema();
    let mut policy = super::TimestampPolicy::new(&parser);
    // Partition A: a static row AND a visible clustering row.
    policy.seed_static_for_test(RowKey::new(1i32.to_be_bytes().to_vec()), &schema, 1_000);
    policy.note_visible_row_for_test();
    let mut pending = Vec::new();
    policy.on_partition_close(&schema, &mut pending, true);
    assert!(pending.is_empty(), "A owes nothing: {pending:?}");

    // Partition B opens: state must be clean, and B's own static row must synthesize.
    policy.on_partition_open(
        RowKey::new(2i32.to_be_bytes().to_vec()),
        None,
        &schema,
        &mut pending,
    );
    assert!(pending.is_empty(), "opening a partition emits nothing");
    policy.seed_static_for_test(RowKey::new(2i32.to_be_bytes().to_vec()), &schema, 2_000);
    policy.on_partition_close(&schema, &mut pending, true);
    assert_eq!(
        pending.len(),
        1,
        "B is static-only and must synthesize its row — an un-reset \
         `emitted_clustering_row` from A would have suppressed it: {pending:?}"
    );
    assert_eq!(pending[0].3, 2_000, "with B's OWN static write timestamp");
}
