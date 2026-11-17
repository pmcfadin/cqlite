//! Issue #4581 — a compacted row's primary-key liveness marker is written VERBATIM.
//!
//! `merge_entry_to_mutation` hands the reconciled marker to the writer as `Mutation::row_marker`.
//! Before #4581 an empty `Live` entry reached the writer as a pure-pk insert and was stamped
//! with the mutation timestamp, no TTL, and (for a TTL) a `now + ttl` expiry. Cassandra
//! (`UnfilteredSerializer.serializeRowBody`) writes the marker's own timestamp, TTL and
//! `localExpirationTime`. Asserted on `merge_row_group`'s `RowWrite` (what the serializer
//! consumes) and on the serialized row header, with a deletion that shadows the marker.

use super::super::*;
use crate::storage::write_engine::mutation::{
    CellOperation, MarkerMode, Mutation, PartitionKey, RowMarker, TableId,
};
use crate::types::Value;

fn mutation(ts: i64, ops: Vec<CellOperation>, marker: Option<RowMarker>) -> Mutation {
    let mut m = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        ops,
        ts,
        None,
    );
    m.marker_mode = MarkerMode::Explicit(marker);
    m
}

fn ttl_marker() -> RowMarker {
    RowMarker {
        timestamp_micros: 1_000,
        ttl_seconds: Some(3),
        local_expiration_time: Some(1_700_000_003),
    }
}

/// The marker's ts, TTL and expiry reach the row write unchanged — the expiry is NOT `now + ttl`.
#[test]
fn a_marker_only_row_carries_the_markers_ts_ttl_and_expiry() {
    let schema = super::support::create_test_schema();
    let m = mutation(1_000, vec![], Some(ttl_marker()));
    let row = DataWriter::merge_row_group(&[&m], &schema, false, None).expect("a row");
    assert_eq!(row.liveness_ts, Some(1_000));
    assert_eq!(row.ttl_seconds, Some(3));
    assert_eq!(row.liveness_ldt, Some(1_700_000_003));
}

/// A live-forever marker: no TTL, no expiry — and the marker's own ts wins over the mutation's.
#[test]
fn the_marker_timestamp_wins_over_the_mutation_timestamp() {
    let schema = super::support::create_test_schema();
    let marker = RowMarker {
        timestamp_micros: 1_000,
        ttl_seconds: None,
        local_expiration_time: None,
    };
    let cell = CellOperation::Write {
        column: "name".into(),
        value: Value::text("v".to_string()),
    };
    // marker@1000, cell@2000 (INSERT then UPDATE): the row header keeps 1000, not the cell's 2000.
    let m = mutation(2_000, vec![cell], Some(marker));
    let row = DataWriter::merge_row_group(&[&m], &schema, false, None).expect("a row");
    assert_eq!(
        (row.liveness_ts, row.ttl_seconds, row.liveness_ldt),
        (Some(1_000), None, None)
    );
}

/// A marker at or below the covering deletion is shadowed (`DeletionTime.deletes`: `ts <=
/// markedForDeleteAt`); a strictly newer one survives beside it.
#[test]
fn a_covering_deletion_shadows_the_marker() {
    let schema = super::support::create_test_schema();
    let marker = |ts| RowMarker {
        timestamp_micros: ts,
        ttl_seconds: None,
        local_expiration_time: None,
    };
    for (marker_ts, survives) in [(1_000, false), (1_001, true)] {
        let m = mutation(marker_ts, vec![], Some(marker(marker_ts)));
        let row = DataWriter::merge_row_group(&[&m], &schema, false, Some(1_000));
        assert_eq!(
            row.is_some_and(|r| r.liveness_ts.is_some()),
            survives,
            "marker@{marker_ts}"
        );
    }
}

/// The serialized header: flags `HAS_TIMESTAMP | HAS_TTL`, then the ts / TTL / expiry deltas against
/// the stats baselines — the expiry delta is the MARKER's, so two different `now`s give the same bytes.
#[test]
fn the_serialized_row_header_uses_the_markers_expiry() {
    let schema = super::support::create_test_schema();
    let write = || {
        let mut stats = super::support::create_test_stats();
        stats.min_timestamp = 1_000;
        stats.min_ttl = 3;
        stats.min_local_deletion_time = 1_700_000_000;
        let mut w = DataWriter::new(stats);
        w.write_row(&mutation(1_000, vec![], Some(ttl_marker())), &schema)
            .expect("write_row");
        w.finish().expect("finish")
    };
    let bytes = write();
    // flags 0x0c = HAS_TIMESTAMP (0x04) | HAS_TTL (0x08); row size; prev size; ts delta 0;
    // ttl delta 0; ldt delta 3 (= 1_700_000_003 - 1_700_000_000).
    assert_eq!(bytes[0], 0x0c, "flags: {bytes:02x?}");
    assert_eq!(
        &bytes[3..6],
        &[0x00, 0x00, 0x03],
        "ts/ttl/ldt deltas: {bytes:02x?}"
    );
}
