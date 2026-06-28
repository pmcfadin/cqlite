//! data_writer tests, group 6/6 (issue #1118 split).
//! Relocated verbatim from the original inline `mod tests`.

#![allow(unused_imports)]

use super::super::*;
use super::support::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, CqlType, KeyColumn, TableSchema};
use crate::storage::serialization::types::TypeSerializer;
use crate::storage::write_engine::mutation::{CellOperation, ClusteringKey, PartitionKey, TableId};
use crate::types::UdtValue;
use std::collections::HashMap;

/// Issue #887: a row-tombstone mutation that ALSO carries a per-element
/// `WriteComplexElement` whose OWN `timestamp_micros` STRICTLY exceeds the
/// row-tombstone time must NOT have that element shadowed out. Per-element writes
/// carry INDEPENDENT timestamps; the row tombstone covers only `timestamp <=
/// row_del`, so a live element with `elem_ts > row_del` survives.
#[test]
fn merge_row_group_keeps_strictly_newer_complex_element_on_row_tombstone() {
    let schema = complex_column_schema();
    const ROW_DEL: i64 = 100;
    const SHADOWED_ELEM_TS: i64 = 100; // <= ROW_DEL: fully shadowed, dropped
    const LIVE_ELEM_TS: i64 = 300; // > ROW_DEL: must survive

    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![
            CellOperation::DeleteRow,
            CellOperation::WriteComplexElement {
                column: "tags".to_string(),
                cell_path: vec![0u8; 16],
                value: Some(Value::Text("shadowed".to_string())),
                timestamp_micros: SHADOWED_ELEM_TS,
                ttl_seconds: None,
                local_deletion_time: None,
                is_deleted: false,
            },
            CellOperation::WriteComplexElement {
                column: "tags".to_string(),
                cell_path: vec![1u8; 16],
                value: Some(Value::Text("live".to_string())),
                timestamp_micros: LIVE_ELEM_TS,
                ttl_seconds: None,
                local_deletion_time: None,
                is_deleted: false,
            },
        ],
        ROW_DEL,
        None,
    );

    let row = DataWriter::merge_row_group(&[&mutation], &schema, false, None)
        .expect("a row tombstone + surviving complex element must produce a row");

    assert!(
        row.row_deletion.is_some(),
        "the row tombstone must be preserved"
    );
    assert_eq!(row.row_deletion.map(|(ts, _)| ts), Some(ROW_DEL));

    let kept: Vec<i64> = row
        .complex_element_ops
        .iter()
        .filter_map(|mop| match mop.op {
            CellOperation::WriteComplexElement {
                timestamp_micros, ..
            } => Some(*timestamp_micros),
            _ => None,
        })
        .collect();
    assert_eq!(
        kept,
        vec![LIVE_ELEM_TS],
        "the element whose OWN timestamp ({LIVE_ELEM_TS}) strictly exceeds row_del \
             ({ROW_DEL}) must survive while the boundary element ({SHADOWED_ELEM_TS}) \
             stays shadowed"
    );
}

/// Issue #887, complement: a complex-deletion marker FULLY COVERED by the row
/// tombstone (`mfda <= row_del`) is shadowed out — redundant with the row
/// tombstone. Mirrors the strict boundary (`equal` does NOT survive).
#[test]
fn merge_row_group_drops_fully_covered_complex_deletion_on_row_tombstone() {
    let schema = complex_column_schema();
    const ROW_DEL: i64 = 300;

    for covered_mfda in [ROW_DEL - 100, ROW_DEL] {
        let mutation = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            None,
            vec![
                CellOperation::DeleteRow,
                CellOperation::ComplexDeletion {
                    column: "tags".to_string(),
                    marked_for_delete_at: covered_mfda,
                    local_deletion_time: 1_700_000_000,
                },
            ],
            ROW_DEL,
            None,
        );

        let row = DataWriter::merge_row_group(&[&mutation], &schema, false, None)
            .expect("the row tombstone alone still produces a row");
        assert!(row.row_deletion.is_some());
        assert!(
            row.complex_element_ops.is_empty(),
            "a marker with mfda ({covered_mfda}) <= row_del ({ROW_DEL}) is fully \
                 covered by the row tombstone and must be dropped"
        );
    }
}

/// Issue #921 (roborev HIGH): the `deletion_ts` shadow boundary for per-element
/// complex writes must apply on the NORMAL (non-shadowed) merge path too, not only
/// the row-tombstone rescue path. Here a partition/range tombstone supplies
/// `deletion_ts` via `shadow_floor`, but the carrying mutation's row timestamp is
/// ABOVE the floor, so it is NOT classified shadowed (takes the normal path). A
/// `WriteComplexElement` whose OWN `timestamp_micros <= deletion_ts` must still be
/// DROPPED (it is covered by the partition/range tombstone and would otherwise
/// resurrect once that tombstone is purged); an element with `ts > deletion_ts` in
/// the SAME mutation must survive. Boundary: `> deletion_ts` survives, `<=` shadowed
/// (equal-ts tombstone wins, #498) — identical to the rescue path.
#[test]
fn merge_row_group_shadows_complex_element_on_normal_path_by_floor() {
    let schema = complex_column_schema();
    const FLOOR: i64 = 200; // partition/range tombstone deletion_ts
    const MUTATION_ROW_TS: i64 = 500; // > FLOOR => NORMAL path (not shadowed)
    const COVERED_ELEM_TS: i64 = 200; // == FLOOR: covered, must drop (equal-ts loses)
    const BELOW_ELEM_TS: i64 = 150; // < FLOOR: covered, must drop
    const LIVE_ELEM_TS: i64 = 350; // > FLOOR: survives

    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![
            CellOperation::WriteComplexElement {
                column: "tags".to_string(),
                cell_path: vec![0u8; 16],
                value: Some(Value::Text("covered-eq".to_string())),
                timestamp_micros: COVERED_ELEM_TS,
                ttl_seconds: None,
                local_deletion_time: None,
                is_deleted: false,
            },
            CellOperation::WriteComplexElement {
                column: "tags".to_string(),
                cell_path: vec![1u8; 16],
                value: Some(Value::Text("covered-below".to_string())),
                timestamp_micros: BELOW_ELEM_TS,
                ttl_seconds: None,
                local_deletion_time: None,
                is_deleted: false,
            },
            CellOperation::WriteComplexElement {
                column: "tags".to_string(),
                cell_path: vec![2u8; 16],
                value: Some(Value::Text("live".to_string())),
                timestamp_micros: LIVE_ELEM_TS,
                ttl_seconds: None,
                local_deletion_time: None,
                is_deleted: false,
            },
        ],
        MUTATION_ROW_TS,
        None,
    );

    let row = DataWriter::merge_row_group(&[&mutation], &schema, false, Some(FLOOR))
        .expect("a surviving live element must still produce a row");

    let kept: Vec<i64> = row
        .complex_element_ops
        .iter()
        .filter_map(|mop| match mop.op {
            CellOperation::WriteComplexElement {
                timestamp_micros, ..
            } => Some(*timestamp_micros),
            _ => None,
        })
        .collect();
    assert_eq!(
        kept,
        vec![LIVE_ELEM_TS],
        "on the NORMAL path (mutation row ts {MUTATION_ROW_TS} > floor {FLOOR}), \
             elements at ts <= floor ({COVERED_ELEM_TS}, {BELOW_ELEM_TS}) must be \
             shadowed by the partition/range tombstone; only ts > floor \
             ({LIVE_ELEM_TS}) survives"
    );
    assert_eq!(
        row.liveness_ts,
        Some(LIVE_ELEM_TS),
        "row liveness comes only from the surviving live element"
    );
}

/// Issue #921 (roborev MEDIUM): the `deletion_ts` shadow boundary for complex
/// DELETION markers must apply on the NORMAL merge path too. A partition/range
/// tombstone supplies `deletion_ts` via `shadow_floor`; the carrying mutation's row
/// timestamp is ABOVE the floor (normal path). A `ComplexDeletion` whose own
/// `marked_for_delete_at <= deletion_ts` is FULLY COVERED and must be DROPPED
/// (redundant dead marker); one with `mfda > deletion_ts` must be retained.
/// Boundary matches the rescue path exactly (`equal` is shadowed).
#[test]
fn merge_row_group_drops_covered_complex_deletion_on_normal_path_by_floor() {
    let schema = complex_column_schema();
    const FLOOR: i64 = 300; // partition/range tombstone deletion_ts
    const MUTATION_ROW_TS: i64 = 700; // > FLOOR => NORMAL path

    // Covered markers (mfda <= floor) must be dropped on the normal path.
    for covered_mfda in [FLOOR - 100, FLOOR] {
        let mutation = Mutation::new(
            TableId::new("test_ks", "test_table"),
            PartitionKey::single("id", Value::Integer(1)),
            None,
            vec![CellOperation::ComplexDeletion {
                column: "tags".to_string(),
                marked_for_delete_at: covered_mfda,
                local_deletion_time: 1_700_000_000,
            }],
            MUTATION_ROW_TS,
            None,
        );
        let row = DataWriter::merge_row_group(&[&mutation], &schema, false, Some(FLOOR));
        assert!(
            row.is_none_or(|r| r.complex_element_ops.is_empty()),
            "on the NORMAL path (row ts {MUTATION_ROW_TS} > floor {FLOOR}), a marker \
                 with mfda ({covered_mfda}) <= floor is fully covered and must be dropped"
        );
    }

    // A marker strictly above the floor must be retained on the normal path.
    const LIVE_MFDA: i64 = 500; // > FLOOR
    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: LIVE_MFDA,
            local_deletion_time: 1_700_000_000,
        }],
        MUTATION_ROW_TS,
        None,
    );
    let row = DataWriter::merge_row_group(&[&mutation], &schema, false, Some(FLOOR))
        .expect("a strictly-newer marker must produce a row");
    assert!(
        row.complex_element_ops.iter().any(|mop| matches!(
            mop.op,
            CellOperation::ComplexDeletion { column, marked_for_delete_at, .. }
                if column == "tags" && *marked_for_delete_at == LIVE_MFDA
        )),
        "a marker with mfda ({LIVE_MFDA}) > floor ({FLOOR}) must survive on the \
             normal path"
    );
}

/// Issue #887: SHADOW-BEFORE-PURGE in the direct writer merge path — the analogue
/// of `reconcile_cluster` Step 2b. A surviving `ComplexDeletion(col, mfda)` marker
/// must shadow every `WriteComplexElement` of THE SAME COLUMN whose own
/// `timestamp_micros <= mfda`, while elements with `ts > mfda` survive. This MUST
/// hold on BOTH the NORMAL path and the SHADOWED (row-tombstone) rescue path.
#[test]
fn merge_row_group_shadows_complex_element_against_surviving_marker() {
    let schema = complex_column_schema();
    const MFDA: i64 = 300;
    const COVERED_ELEM_TS: i64 = 200; // <= MFDA: shadowed by the marker
    const LIVE_ELEM_TS: i64 = 500; // > MFDA: survives

    let covered_elem = || CellOperation::WriteComplexElement {
        column: "tags".to_string(),
        cell_path: vec![0u8; 16],
        value: Some(Value::Text("covered".to_string())),
        timestamp_micros: COVERED_ELEM_TS,
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };
    let live_elem = || CellOperation::WriteComplexElement {
        column: "tags".to_string(),
        cell_path: vec![1u8; 16],
        value: Some(Value::Text("live".to_string())),
        timestamp_micros: LIVE_ELEM_TS,
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };
    let marker = || CellOperation::ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: MFDA,
        local_deletion_time: 1_700_000_000,
    };

    let assert_shadowed = |row: &RowWrite<'_>, scenario: &str| {
        let kept_marker = row.complex_element_ops.iter().any(|mop| {
            matches!(
                mop.op,
                CellOperation::ComplexDeletion {
                    marked_for_delete_at,
                    ..
                } if *marked_for_delete_at == MFDA
            )
        });
        assert!(
            kept_marker,
            "{scenario}: the surviving complex-deletion marker (mfda={MFDA}) must be emitted"
        );
        let kept_elems: Vec<i64> = row
            .complex_element_ops
            .iter()
            .filter_map(|mop| match mop.op {
                CellOperation::WriteComplexElement {
                    timestamp_micros, ..
                } => Some(*timestamp_micros),
                _ => None,
            })
            .collect();
        assert_eq!(
            kept_elems,
            vec![LIVE_ELEM_TS],
            "{scenario}: the covered element@{COVERED_ELEM_TS} (<= mfda={MFDA}) must be \
                 shadowed; only the live element@{LIVE_ELEM_TS} (> mfda) survives"
        );
    };

    // --- Normal path: no row tombstone. mfda alone shadows the element. ---
    let normal = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![marker(), covered_elem(), live_elem()],
        COVERED_ELEM_TS,
        None,
    );
    let row = DataWriter::merge_row_group(&[&normal], &schema, false, None)
        .expect("normal path must produce a row");
    assert!(
        row.row_deletion.is_none(),
        "normal path carries no row tombstone"
    );
    assert_shadowed(&row, "normal path");

    // --- Shadowed (row-tombstone) rescue path: DeleteRow@100, mfda=300>100. ---
    const ROW_DEL: i64 = 100;
    let shadowed = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![
            CellOperation::DeleteRow,
            marker(),
            covered_elem(),
            live_elem(),
        ],
        ROW_DEL,
        None,
    );
    let row = DataWriter::merge_row_group(&[&shadowed], &schema, false, None)
        .expect("shadowed path must produce a row");
    assert_eq!(
        row.row_deletion.map(|(ts, _)| ts),
        Some(ROW_DEL),
        "shadowed path preserves the row tombstone"
    );
    assert_shadowed(&row, "shadowed (row-tombstone) path");
}

/// Issue #887: when EVERY live complex element of a mutation is shadowed by a
/// same-column `ComplexDeletion` (every element `timestamp_micros <= mfda`), and
/// there is NO other live contributor, the row must NOT carry a LIVE row timestamp
/// — it is a marker-only / deletion row. Liveness from OTHER sources (a surviving
/// element or a simple-cell `Write`) must still keep the row live.
#[test]
fn merge_row_group_drops_liveness_when_all_complex_elements_shadowed() {
    let schema = complex_column_schema();
    const MFDA: i64 = 300;
    const SHADOWED_ELEM_TS: i64 = 200; // <= MFDA: shadowed by the marker
    const LIVE_ELEM_TS: i64 = 500; // > MFDA: survives

    let marker = || CellOperation::ComplexDeletion {
        column: "tags".to_string(),
        marked_for_delete_at: MFDA,
        local_deletion_time: 1_700_000_000,
    };
    let shadowed_elem = || CellOperation::WriteComplexElement {
        column: "tags".to_string(),
        cell_path: vec![0u8; 16],
        value: Some(Value::Text("shadowed".to_string())),
        timestamp_micros: SHADOWED_ELEM_TS,
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };
    let live_elem = || CellOperation::WriteComplexElement {
        column: "tags".to_string(),
        cell_path: vec![1u8; 16],
        value: Some(Value::Text("live".to_string())),
        timestamp_micros: LIVE_ELEM_TS,
        ttl_seconds: None,
        local_deletion_time: None,
        is_deleted: false,
    };

    // --- Case 1: marker + ONLY shadowed element, no other live contributor. ---
    let all_shadowed = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![marker(), shadowed_elem()],
        SHADOWED_ELEM_TS,
        None,
    );
    let row = DataWriter::merge_row_group(&[&all_shadowed], &schema, false, None)
        .expect("the surviving marker alone still produces a row");
    assert!(
        row.complex_element_ops
            .iter()
            .all(|mop| !matches!(mop.op, CellOperation::WriteComplexElement { .. })),
        "the shadowed element must be purged; only the marker survives"
    );
    assert_eq!(
        row.liveness_ts, None,
        "a marker-only row carries NO liveness (shadowed element must not leak one)"
    );

    // --- Case 2: a SURVIVING element (ts > mfda) keeps the row live. ---
    let one_survives = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![marker(), shadowed_elem(), live_elem()],
        LIVE_ELEM_TS,
        None,
    );
    let row = DataWriter::merge_row_group(&[&one_survives], &schema, false, None)
        .expect("a surviving element must produce a row");
    assert_eq!(
        row.liveness_ts,
        Some(LIVE_ELEM_TS),
        "a surviving element (ts {LIVE_ELEM_TS} > mfda {MFDA}) must keep the row live"
    );

    // --- Case 3: an explicit simple-cell Write is an independent live source. ---
    let mut simple_schema = complex_column_schema();
    simple_schema.columns.push(Column {
        name: "v".to_string(),
        data_type: "text".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    });
    let with_simple_write = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![
            CellOperation::ComplexDeletion {
                column: "tags".to_string(),
                marked_for_delete_at: MFDA,
                local_deletion_time: 1_700_000_000,
            },
            CellOperation::WriteComplexElement {
                column: "tags".to_string(),
                cell_path: vec![0u8; 16],
                value: Some(Value::Text("shadowed".to_string())),
                timestamp_micros: SHADOWED_ELEM_TS,
                ttl_seconds: None,
                local_deletion_time: None,
                is_deleted: false,
            },
            CellOperation::Write {
                column: "v".to_string(),
                value: Value::Text("alive".to_string()),
            },
        ],
        SHADOWED_ELEM_TS,
        None,
    );
    let row = DataWriter::merge_row_group(&[&with_simple_write], &simple_schema, false, None)
        .expect("a simple write must produce a row");
    assert_eq!(
        row.liveness_ts,
        Some(SHADOWED_ELEM_TS),
        "a simple-cell Write is an independent live source and must keep the row live \
             even when every complex element is shadowed"
    );
}

/// Issue #921 (roborev Finding 1): in the #927 mixed-stream reconcile a column
/// may carry BOTH a whole-column write and a per-element edit. The reconcile
/// compares the whole op's timestamp to the per-element stream's MAX timestamp.
/// That max must be the element's OWN `timestamp_micros`, NOT the enclosing
/// mutation's row timestamp. Here a per-element edit's own timestamp is NEWER
/// than a whole-column write, but its enclosing mutation's row timestamp is
/// OLDER. The element must still WIN (be retained, whole op dropped).
#[test]
fn merge_row_group_element_own_ts_wins_mixed_stream_over_older_row_ts() {
    let schema = complex_column_schema();
    // Whole-column write at ts 500.
    const WHOLE_TS: i64 = 500;
    // Per-element edit: OWN element ts 900 (newer than the whole op) but its
    // enclosing mutation carries an OLDER row timestamp of 100.
    const ELEM_OWN_TS: i64 = 900;
    const MUTATION_ROW_TS: i64 = 100;

    let whole = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "tags".to_string(),
            value: Value::Text("whole".to_string()),
        }],
        WHOLE_TS,
        None,
    );
    let per_elem = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::WriteComplexElement {
            column: "tags".to_string(),
            cell_path: vec![0u8; 16],
            value: Some(Value::Text("new-elem".to_string())),
            timestamp_micros: ELEM_OWN_TS,
            ttl_seconds: None,
            local_deletion_time: None,
            is_deleted: false,
        }],
        MUTATION_ROW_TS,
        None,
    );

    let row = DataWriter::merge_row_group(&[&whole, &per_elem], &schema, false, None)
        .expect("mixed stream must produce a row");

    // The element's OWN ts (900) exceeds the whole-column write (500), so the
    // per-element stream wins regardless of the older enclosing row ts (100).
    assert!(
        !row.ops
            .iter()
            .any(|m| merged_op_column(m.op) == Some("tags")),
        "the whole-column write must lose: the per-element edit's OWN timestamp \
             ({ELEM_OWN_TS}) exceeds it ({WHOLE_TS}), even though its mutation row \
             timestamp ({MUTATION_ROW_TS}) is older"
    );
    assert_eq!(
        row.complex_element_ops.len(),
        1,
        "the newer per-element edit (own ts {ELEM_OWN_TS}) must be retained"
    );
    assert!(
        row.complex_element_ops.iter().any(|mop| matches!(
            mop.op,
            CellOperation::WriteComplexElement { column, timestamp_micros, .. }
                if column == "tags" && *timestamp_micros == ELEM_OWN_TS
        )),
        "the surviving element must be the newer per-element edit"
    );
}

/// Issue #921 (roborev, ComplexDeletion analogue of element-own-ts-wins): a
/// `ComplexDeletion(col, mfda)` carried by an OLDER metadata/tombstone mutation
/// (row ts 100) must NOT be shadowed by a whole-column write/delete (ts 500)
/// whose timestamp is below the marker's `marked_for_delete_at` (900). The #927
/// mixed-stream reconcile compares `MergedOp.timestamp_micros`; the marker must
/// carry its OWN mfda there so the per-element/marker stream WINS, retaining the
/// collection tombstone (otherwise covered elements survive/resurrect). Covers
/// the NORMAL (non-shadowed) merge path.
#[test]
fn merge_row_group_complex_deletion_mfda_wins_mixed_stream_over_older_row_ts() {
    let schema = complex_column_schema();
    // Whole-column write to `tags` at ts 500.
    const WHOLE_TS: i64 = 500;
    // ComplexDeletion marker: OWN mfda 900 (newer than the whole op) but carried
    // by a mutation whose row timestamp is an OLDER 100.
    const MARKER_MFDA: i64 = 900;
    const MUTATION_ROW_TS: i64 = 100;
    const MARKER_LDT: i32 = 1_700_000_000;

    let whole = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "tags".to_string(),
            value: Value::Text("whole".to_string()),
        }],
        WHOLE_TS,
        None,
    );
    let marker = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: MARKER_MFDA,
            local_deletion_time: MARKER_LDT,
        }],
        MUTATION_ROW_TS,
        None,
    );

    let row = DataWriter::merge_row_group(&[&whole, &marker], &schema, false, None)
        .expect("mixed stream must produce a row");

    // The marker's OWN mfda (900) exceeds the whole-column write (500), so the
    // marker stream wins regardless of the older enclosing row ts (100).
    assert!(
        !row.ops
            .iter()
            .any(|m| merged_op_column(m.op) == Some("tags")),
        "the whole-column write must lose: the ComplexDeletion's OWN mfda \
             ({MARKER_MFDA}) exceeds it ({WHOLE_TS}), even though its mutation row \
             timestamp ({MUTATION_ROW_TS}) is older"
    );
    // The collection tombstone must be retained with its UNCHANGED mfda/ldt bytes.
    assert!(
        row.complex_element_ops.iter().any(|mop| matches!(
            mop.op,
            CellOperation::ComplexDeletion {
                column,
                marked_for_delete_at,
                local_deletion_time,
            } if column == "tags"
                && *marked_for_delete_at == MARKER_MFDA
                && *local_deletion_time == MARKER_LDT
        )),
        "the ComplexDeletion marker must survive with its emitted mfda/ldt unchanged"
    );
}

/// Issue #921 (roborev, ComplexDeletion analogue, SHADOWED rescue path): the same
/// independence on the row-tombstone rescue path. A row tombstone at ts 100 carries
/// a `ComplexDeletion(col, mfda=900)`; a separate whole-column write to `col` at
/// ts 500 must NOT shadow the marker via the #927 reconcile, because the marker
/// carries its OWN mfda (900) as the comparison timestamp. The marker survives the
/// row tombstone (mfda > row_del) AND wins the mixed-stream reconcile.
#[test]
fn merge_row_group_complex_deletion_mfda_wins_mixed_stream_shadowed_path() {
    let schema = complex_column_schema();
    const ROW_DEL: i64 = 100;
    const WHOLE_TS: i64 = 500;
    const MARKER_MFDA: i64 = 900;
    const MARKER_LDT: i32 = 1_700_000_000;

    // Row-tombstone mutation (ts == ROW_DEL) that ALSO carries the marker. Its row
    // timestamp is shadowed by its own deletion, exercising the rescue path.
    let mut tombstone = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: MARKER_MFDA,
            local_deletion_time: MARKER_LDT,
        }],
        ROW_DEL,
        None,
    );
    tombstone.operations.push(CellOperation::DeleteRow);

    // A separate whole-column write at ts 500 (> row_del, < mfda).
    let whole = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "tags".to_string(),
            value: Value::Text("whole".to_string()),
        }],
        WHOLE_TS,
        None,
    );

    let row = DataWriter::merge_row_group(&[&tombstone, &whole], &schema, false, None)
        .expect("row tombstone + surviving marker must produce a row");

    assert!(
        !row.ops
            .iter()
            .any(|m| merged_op_column(m.op) == Some("tags")),
        "the whole-column write must lose on the shadowed path too: the marker's \
             OWN mfda ({MARKER_MFDA}) exceeds the whole op ts ({WHOLE_TS})"
    );
    assert!(
        row.complex_element_ops.iter().any(|mop| matches!(
            mop.op,
            CellOperation::ComplexDeletion {
                column,
                marked_for_delete_at,
                local_deletion_time,
            } if column == "tags"
                && *marked_for_delete_at == MARKER_MFDA
                && *local_deletion_time == MARKER_LDT
        )),
        "the rescued ComplexDeletion marker must survive the #927 reconcile with \
             its emitted mfda/ldt unchanged"
    );
}

/// Issue #921 (roborev Finding 2): a winning whole-column `Delete` for column A
/// drops A's per-element ops via the #927 mixed-stream reconcile. The deferred
/// complex-element liveness fold must NOT then let A's dropped (live) element
/// keep the row alive — even though another complex column B keeps
/// `complex_element_ops` non-empty (so the fold runs). The result is a
/// deletion-only column A: no liveness leaks from it.
#[test]
fn merge_row_group_no_liveness_from_element_dropped_by_whole_column_delete() {
    let schema = two_complex_column_schema();
    // Column A (`tags`): a live per-element write at ts 300, shadowed by a NEWER
    // whole-column Delete at ts 500 (delete wins the #927 reconcile, drops A's
    // element). A contributes NO liveness.
    const A_ELEM_TS: i64 = 300;
    const A_DELETE_TS: i64 = 500;
    // Column B (`notes`): an unrelated whole-column Delete keeps the row a
    // deletion (B's per-element stream is empty; B does not contribute either).
    // Crucially, column B keeps a per-element op so `complex_element_ops` is
    // non-empty and the liveness fold executes.
    const B_ELEM_TS: i64 = 200; // shadowed by B's own marker below
    const B_MFDA: i64 = 400; // > B_ELEM_TS: B's element is fully shadowed too

    let a_elem = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::WriteComplexElement {
            column: "tags".to_string(),
            cell_path: vec![0u8; 16],
            value: Some(Value::Text("a-live".to_string())),
            timestamp_micros: A_ELEM_TS,
            ttl_seconds: None,
            local_deletion_time: None,
            is_deleted: false,
        }],
        A_ELEM_TS,
        None,
    );
    let a_delete = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Delete {
            column: "tags".to_string(),
            local_deletion_time: None,
        }],
        A_DELETE_TS,
        None,
    );
    // Column B: a marker + a fully-shadowed element so B keeps a per-element op
    // (the surviving marker) without contributing any liveness of its own.
    let b_marker_and_elem = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![
            CellOperation::ComplexDeletion {
                column: "notes".to_string(),
                marked_for_delete_at: B_MFDA,
                local_deletion_time: 1_700_000_000,
            },
            CellOperation::WriteComplexElement {
                column: "notes".to_string(),
                cell_path: vec![1u8; 16],
                value: Some(Value::Text("b-shadowed".to_string())),
                timestamp_micros: B_ELEM_TS,
                ttl_seconds: None,
                local_deletion_time: None,
                is_deleted: false,
            },
        ],
        B_ELEM_TS,
        None,
    );

    let row = DataWriter::merge_row_group(
        &[&a_elem, &a_delete, &b_marker_and_elem],
        &schema,
        false,
        None,
    )
    .expect("the surviving deletes/marker still produce a row");

    // Column A's element was dropped by the winning whole-column Delete; no
    // liveness must leak from it. Column B contributes none either. The row is a
    // deletion-only row.
    assert_eq!(
        row.liveness_ts, None,
        "a live element of column A dropped by a winning whole-column Delete must \
             NOT keep the row live (Finding 2)"
    );
    // Sanity: A's element op did not survive; the whole-column Delete did.
    assert!(
        !row.complex_element_ops.iter().any(|mop| matches!(
            mop.op,
            CellOperation::WriteComplexElement { column, .. } if column == "tags"
        )),
        "column A's per-element write must be dropped by the winning whole-column Delete"
    );
    assert!(
        row.ops.iter().any(|mop| matches!(
            mop.op,
            CellOperation::Delete { column, .. } if column == "tags"
        )),
        "column A's whole-column Delete must survive"
    );
}

/// Issue #921 (roborev High): a winning whole-column LIVE `Write` for column A
/// drops A's per-element ops via the #927 mixed-stream reconcile. The OLD
/// candidate-list + exclusion-set fold only excluded columns whose stream was
/// dropped by a NON-live (Delete) winner, so a dropped LIVE element could still
/// fold liveness — and it folded the ENCLOSING MUTATION's row timestamp, which
/// can be NEWER than the surviving write even though the dropped element's OWN
/// timestamp is OLDER. The row would then carry a liveness timestamp from an
/// element that no longer exists. After the fix, complex-element liveness is
/// derived from the FINAL surviving `complex_element_ops`, so a dropped element
/// contributes nothing and the row's liveness comes only from the surviving LIVE
/// whole-column write.
#[test]
fn merge_row_group_no_liveness_from_element_dropped_by_whole_column_live_write() {
    let schema = two_complex_column_schema();
    // Column A (`tags`): a live per-element write whose OWN timestamp (300) is
    // OLDER than the winning whole-column Write (500), but whose ENCLOSING
    // mutation carries a NEWER row timestamp (1000). The whole-column Write wins
    // the #927 reconcile (500 >= elem_max 300) and drops A's element.
    const A_ELEM_TS: i64 = 300;
    const A_WRITE_TS: i64 = 500;
    const A_ELEM_MUTATION_ROW_TS: i64 = 1000; // > A_WRITE_TS: the trap for the old fold
                                              // Column B (`notes`): a marker + fully-shadowed element so B keeps a
                                              // per-element op (the surviving marker) and `complex_element_ops` is
                                              // non-empty, exercising the liveness derivation. B contributes no liveness.
    const B_ELEM_TS: i64 = 200;
    const B_MFDA: i64 = 400; // > B_ELEM_TS: B's element is fully shadowed

    let a_elem = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::WriteComplexElement {
            column: "tags".to_string(),
            cell_path: vec![0u8; 16],
            value: Some(Value::Text("a-live".to_string())),
            timestamp_micros: A_ELEM_TS,
            ttl_seconds: None,
            local_deletion_time: None,
            is_deleted: false,
        }],
        A_ELEM_MUTATION_ROW_TS,
        None,
    );
    let a_write = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "tags".to_string(),
            value: Value::Text("a-whole".to_string()),
        }],
        A_WRITE_TS,
        None,
    );
    let b_marker_and_elem = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![
            CellOperation::ComplexDeletion {
                column: "notes".to_string(),
                marked_for_delete_at: B_MFDA,
                local_deletion_time: 1_700_000_000,
            },
            CellOperation::WriteComplexElement {
                column: "notes".to_string(),
                cell_path: vec![1u8; 16],
                value: Some(Value::Text("b-shadowed".to_string())),
                timestamp_micros: B_ELEM_TS,
                ttl_seconds: None,
                local_deletion_time: None,
                is_deleted: false,
            },
        ],
        B_ELEM_TS,
        None,
    );

    let row = DataWriter::merge_row_group(
        &[&a_elem, &a_write, &b_marker_and_elem],
        &schema,
        false,
        None,
    )
    .expect("the surviving write/marker still produce a row");

    // A's per-element write was dropped by the winning whole-column LIVE Write;
    // its OWN timestamp (300) no longer exists in the output, so the only live
    // source is the surviving whole-column Write at 500. Liveness must be 500 —
    // NOT the dropped element's enclosing mutation row ts (1000), which the OLD
    // fold would have leaked.
    assert_eq!(
        row.liveness_ts,
        Some(A_WRITE_TS),
        "liveness must come from the surviving whole-column Write ({A_WRITE_TS}), \
             not the dropped per-element write's enclosing mutation row ts \
             ({A_ELEM_MUTATION_ROW_TS})"
    );
    // Sanity: A's element op did not survive; the whole-column Write did.
    assert!(
        !row.complex_element_ops.iter().any(|mop| matches!(
            mop.op,
            CellOperation::WriteComplexElement { column, .. } if column == "tags"
        )),
        "column A's per-element write must be dropped by the winning whole-column Write"
    );
    assert!(
        row.ops.iter().any(|mop| matches!(
            mop.op,
            CellOperation::Write { column, .. } if column == "tags"
        )),
        "column A's whole-column Write must survive"
    );
}

/// Issue #921 (roborev HIGH — symmetric counterpart to the per-element liveness
/// fix): a live WHOLE-COLUMN `Write` that LOSES the #927 mixed-stream reconcile to
/// a newer same-column complex stream must NOT leak row liveness. Here
/// `Write(tags)@500` competes with `ComplexDeletion(tags, mfda=900)`. The complex
/// marker stream wins (900 >= whole-column 500), so the whole-column Write is
/// DROPPED from `ops`, and the marker is a deletion (no live element) — there is
/// NO surviving live cell for `tags`. The row therefore carries NO liveness.
///
/// RED before the fix: whole-column liveness was folded INLINE at 500 during the
/// per-mutation loop, before reconcile decided the write loses, so the row emitted
/// a phantom live timestamp@500. GREEN after: liveness is derived from the FINAL
/// surviving `ops`, which no longer contain the dropped write.
#[test]
fn merge_row_group_no_liveness_when_whole_column_write_loses_to_complex_marker() {
    let schema = complex_column_schema();
    const WRITE_TS: i64 = 500;
    const MFDA: i64 = 900; // > WRITE_TS: the complex marker stream wins

    let write = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "tags".to_string(),
            value: Value::Text("whole".to_string()),
        }],
        WRITE_TS,
        None,
    );
    let marker = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::ComplexDeletion {
            column: "tags".to_string(),
            marked_for_delete_at: MFDA,
            local_deletion_time: 1_700_000_000,
        }],
        MFDA,
        None,
    );

    let row = DataWriter::merge_row_group(&[&write, &marker], &schema, false, None)
        .expect("the surviving complex-deletion marker still produces a row");

    // The whole-column Write lost the reconcile and was dropped; the only
    // surviving op is the deletion marker (no live cell). No phantom liveness.
    assert_eq!(
        row.liveness_ts, None,
        "a whole-column Write dropped by a winning complex marker must NOT leak \
             row liveness (no surviving live cell)"
    );
    assert!(
        !row.ops.iter().any(|mop| matches!(
            mop.op,
            CellOperation::Write { column, .. } if column == "tags"
        )),
        "the whole-column Write must be dropped by the winning complex stream"
    );
    assert!(
        row.complex_element_ops.iter().any(|mop| matches!(
            mop.op,
            CellOperation::ComplexDeletion { column, .. } if column == "tags"
        )),
        "the complex-deletion marker must survive"
    );
}

/// Issue #921: a surviving whole-column `Write` with NO competing complex winner
/// still sets row liveness at its own timestamp — the common case must be
/// unaffected by deriving liveness from final survivors.
#[test]
fn merge_row_group_surviving_whole_column_write_sets_liveness() {
    let schema = create_test_schema();
    const WRITE_TS: i64 = 700;

    let write = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text("alive".to_string()),
        }],
        WRITE_TS,
        None,
    );

    let row = DataWriter::merge_row_group(&[&write], &schema, false, None)
        .expect("a live whole-column write produces a row");

    assert_eq!(
        row.liveness_ts,
        Some(WRITE_TS),
        "a surviving whole-column write must set row liveness at its own timestamp"
    );
    assert!(
        row.ops.iter().any(|mop| matches!(
            mop.op,
            CellOperation::Write { column, .. } if column == "name"
        )),
        "the whole-column write must survive into ops"
    );
}

/// Issue #921: a pure primary-key insert (no ops, no tombstone payload) still
/// sets row liveness — the cell-less liveness source must be preserved when
/// whole-column liveness moves to a post-reconcile derivation.
#[test]
fn merge_row_group_pure_pk_insert_still_sets_liveness() {
    let schema = create_test_schema();
    const INSERT_TS: i64 = 1234;

    let insert = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![],
        INSERT_TS,
        None,
    );

    let row = DataWriter::merge_row_group(&[&insert], &schema, false, None)
        .expect("a pure primary-key insert produces a live row with no cells");

    assert_eq!(
        row.liveness_ts,
        Some(INSERT_TS),
        "a pure primary-key insert must keep its liveness timestamp"
    );
    assert!(
        row.ops.is_empty(),
        "a pure primary-key insert produces no cells"
    );
}

/// Issue #887: the all-shadowed case on the SHADOWED (row-tombstone) rescue path.
/// A row tombstone produces a row, but its surviving complex elements (rescue
/// path) deliberately contribute no liveness — so an all-shadowed complex column
/// must leave the row a pure tombstone (no LIVE timestamp).
#[test]
fn merge_row_group_row_tombstone_all_complex_shadowed_carries_no_liveness() {
    let schema = complex_column_schema();
    const ROW_DEL: i64 = 100;
    const MFDA: i64 = 300; // > ROW_DEL so the marker survives the row tombstone
    const COVERED_ELEM_TS: i64 = 250; // > ROW_DEL but <= MFDA: rescued then shadowed

    let mutation = Mutation::new(
        TableId::new("test_ks", "test_table"),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![
            CellOperation::DeleteRow,
            CellOperation::ComplexDeletion {
                column: "tags".to_string(),
                marked_for_delete_at: MFDA,
                local_deletion_time: 1_700_000_000,
            },
            CellOperation::WriteComplexElement {
                column: "tags".to_string(),
                cell_path: vec![0u8; 16],
                value: Some(Value::Text("covered".to_string())),
                timestamp_micros: COVERED_ELEM_TS,
                ttl_seconds: None,
                local_deletion_time: None,
                is_deleted: false,
            },
        ],
        ROW_DEL,
        None,
    );

    let row = DataWriter::merge_row_group(&[&mutation], &schema, false, None)
        .expect("a row tombstone + surviving marker must produce a row");
    assert_eq!(
        row.row_deletion.map(|(ts, _)| ts),
        Some(ROW_DEL),
        "the row tombstone is preserved"
    );
    assert!(
        row.complex_element_ops
            .iter()
            .all(|mop| !matches!(mop.op, CellOperation::WriteComplexElement { .. })),
        "the element@{COVERED_ELEM_TS} (<= mfda {MFDA}) is shadowed out; only the marker survives"
    );
    assert_eq!(
        row.liveness_ts, None,
        "a row-tombstone row whose complex elements are all shadowed must NOT carry \
             a LIVE row timestamp"
    );
}

/// The rendered marshal string for a `UdtTypeDef` must match the hand-written
/// `person_udt_marshal()` byte-for-byte AND round-trip through
/// `udt_declared_field_names` to the declared field order (issue #929).
#[test]
fn render_udt_marshal_matches_and_roundtrips() {
    let rendered = render_udt_marshal(&person_udt_def());
    assert_eq!(
        rendered,
        person_udt_marshal(),
        "rendered marshal must match the canonical hand-written form"
    );

    let names = udt_declared_field_names(&rendered)
        .expect("rendered marshal must parse via udt_declared_field_names");
    assert_eq!(
        names,
        vec!["name".to_string(), "age".to_string(), "email".to_string()],
        "field names must round-trip in declared order"
    );

    // And the renderer's output is recognized as a top-level UDT marshal.
    assert!(
        is_udt_marshal(&rendered.to_lowercase()),
        "rendered marshal must be recognized by is_udt_marshal"
    );
    assert!(
        is_complex_column(&rendered),
        "rendered marshal must be treated as a complex column"
    );
}

/// A bare UDT name resolves to its marshal string only when the registry
/// knows it; parameterized / marshal / unknown types are left untouched
/// (issue #929 top-level-bare-only scope).
#[test]
fn resolve_bare_udt_marshal_scope() {
    let reg = person_registry();

    // Bare known name -> rendered marshal.
    assert_eq!(
        resolve_bare_udt_marshal("person", "test_ks", &reg).as_deref(),
        Some(person_udt_marshal().as_str())
    );
    // Whitespace tolerated.
    assert_eq!(
        resolve_bare_udt_marshal("  person  ", "test_ks", &reg).as_deref(),
        Some(person_udt_marshal().as_str())
    );
    // Unquoted CQL identifiers are case-insensitive: a column declared with a
    // different case than the registered name still resolves (roborev #1005).
    assert_eq!(
        resolve_bare_udt_marshal("Person", "test_ks", &reg).as_deref(),
        Some(person_udt_marshal().as_str())
    );
    assert_eq!(
        resolve_bare_udt_marshal("PERSON", "test_ks", &reg).as_deref(),
        Some(person_udt_marshal().as_str())
    );

    // Unknown name / wrong keyspace -> no rewrite.
    assert!(resolve_bare_udt_marshal("unknown", "test_ks", &reg).is_none());
    assert!(resolve_bare_udt_marshal("person", "other_ks", &reg).is_none());

    // Primitives and parameterized types are not bare UDT names.
    assert!(resolve_bare_udt_marshal("text", "test_ks", &reg).is_none());
    assert!(resolve_bare_udt_marshal("list<person>", "test_ks", &reg).is_none());
    assert!(resolve_bare_udt_marshal("frozen<person>", "test_ks", &reg).is_none());

    // An already-marshalled type is left untouched.
    assert!(resolve_bare_udt_marshal(&person_udt_marshal(), "test_ks", &reg).is_none());

    // SCOPE (roborev #1011): a UDT whose fields include a nested UDT (directly,
    // or inside a collection/tuple/frozen) is NOT normalized at all — it stays
    // a single, self-consistent simple cell rather than a partially-degraded
    // complex column. A UDT with only primitive / collection-of-primitive
    // fields IS normalized.
    let mut nested_reg = UdtRegistry::new();
    nested_reg.register_udt(
        UdtTypeDef::new("test_ks".to_string(), "inner".to_string()).with_field(
            "x".to_string(),
            CqlType::Int,
            true,
        ),
    );
    // `with_nested` has a direct nested-UDT field -> skipped.
    nested_reg.register_udt(
        UdtTypeDef::new("test_ks".to_string(), "with_nested".to_string()).with_field(
            "i".to_string(),
            CqlType::Udt("inner".to_string(), vec![]),
            true,
        ),
    );
    // `with_nested_list` hides the nested UDT inside a list -> skipped.
    nested_reg.register_udt(
        UdtTypeDef::new("test_ks".to_string(), "with_nested_list".to_string()).with_field(
            "xs".to_string(),
            CqlType::List(Box::new(CqlType::Udt("inner".to_string(), vec![]))),
            true,
        ),
    );
    // `prim_coll` has only primitive / collection-of-primitive fields ->
    // normalized, with the collection field rendered fully.
    nested_reg.register_udt(
        UdtTypeDef::new("test_ks".to_string(), "prim_coll".to_string())
            .with_field("n".to_string(), CqlType::Text, true)
            .with_field(
                "xs".to_string(),
                CqlType::List(Box::new(CqlType::Int)),
                true,
            ),
    );
    assert!(
        resolve_bare_udt_marshal("with_nested", "test_ks", &nested_reg).is_none(),
        "UDT with a nested-UDT field must be skipped (single-cell fallback)"
    );
    assert!(
        resolve_bare_udt_marshal("with_nested_list", "test_ks", &nested_reg).is_none(),
        "UDT with a collection-of-UDT field must be skipped (single-cell fallback)"
    );
    let prim_coll = resolve_bare_udt_marshal("prim_coll", "test_ks", &nested_reg)
        .expect("primitive-only UDT resolves");
    assert!(
            prim_coll.contains(&format!(
                "{}:org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)",
                hex::encode(b"xs")
            )),
            "list<int> field must render as ListType(Int32Type), got: {prim_coll}"
        );
    assert!(
        !prim_coll.contains("BytesType"),
        "a primitive-only UDT must not contain BytesType, got: {prim_coll}"
    );

    // Defensive: even if a UDT named after a primitive is registered, a real
    // primitive column must NOT be rewritten into a UserType marshal.
    let mut shadow = UdtRegistry::new();
    shadow.register_udt(
        UdtTypeDef::new("test_ks".to_string(), "text".to_string()).with_field(
            "x".to_string(),
            CqlType::Int,
            true,
        ),
    );
    assert!(
        resolve_bare_udt_marshal("text", "test_ks", &shadow).is_none(),
        "primitive keyword must never resolve to a UDT marshal"
    );
}

/// A direct write of a BARE-name non-frozen UDT column WITH a populated
/// registry must, after schema normalization, decompose into per-field
/// complex cells at declared indices and round-trip through the reader,
/// even for a sparse / out-of-order literal (issue #929, mirroring the #927
/// sparse test but starting from a bare `person` name).
#[test]
fn bare_udt_with_registry_roundtrips_sparse_out_of_order() {
    // A schema whose UDT column is declared with the BARE name `person`.
    let mut schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![udt_column("addr", "person")],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    // Before normalization the bare name is NOT complex.
    assert!(
        !is_complex_column(&schema.columns[0].data_type),
        "bare name is not complex before normalization"
    );

    normalize_schema_udts(&mut schema, &person_registry());

    // After normalization the column carries the full marshal string and is
    // recognized as a complex column.
    assert_eq!(schema.columns[0].data_type, person_udt_marshal());
    assert!(is_complex_column(&schema.columns[0].data_type));

    let col = schema.columns[0].clone();
    let writer = DataWriter::new(create_test_stats());
    // Literal lists email THEN name (out of order) and OMITS age (sparse).
    let udt = Value::Udt(crate::types::UdtValue {
        type_name: "person".to_string(),
        keyspace: "test_ks".to_string(),
        fields: vec![
            udt_field("email", Some(Value::Text("a@b.com".to_string()))),
            udt_field("name", Some(Value::Text("Alice".to_string()))),
        ],
    });

    let row_ts = 1_005_000i64;
    let mut buf = Vec::new();
    writer
        .write_complex_column(&mut buf, &col, &udt, row_ts, None)
        .expect("normalized bare UDT must write as a complex column");

    let (_del_ts, _del_ldt, cells) = decode_complex_column(&buf);
    assert_eq!(cells.len(), 2, "two non-null fields => two cells");
    assert_eq!(
        cells[0].cell_path,
        0u16.to_be_bytes().to_vec(),
        "name idx 0"
    );
    assert_eq!(
        cells[1].cell_path,
        2u16.to_be_bytes().to_vec(),
        "email idx 2"
    );

    let parser = person_reader();
    let (value, _off, _meta) = parser
        .parse_complex_column_inner(&buf, 0, &col, &col.data_type, true, row_ts, None)
        .expect("reader must parse the UDT complex column");

    match value {
        Value::Udt(out) => {
            assert_eq!(out.type_name, "person");
            assert_eq!(out.keyspace, "test_ks");
            assert_eq!(out.fields.len(), 3, "all DECLARED fields present");
            assert_eq!(out.fields[0].name, "name");
            assert_eq!(out.fields[0].value, Some(Value::Text("Alice".to_string())));
            assert_eq!(out.fields[1].name, "age");
            assert_eq!(out.fields[1].value, None, "omitted field stays null");
            assert_eq!(out.fields[2].name, "email");
            assert_eq!(
                out.fields[2].value,
                Some(Value::Text("a@b.com".to_string()))
            );
        }
        other => panic!("expected Value::Udt, got {:?}", other),
    }
}

/// With NO registry a bare-name UDT column stays bare, is NOT detected as
/// complex, and a whole-`Value::Udt` write goes through the simple-cell path
/// without panicking (issue #929 documented fallback).
#[test]
fn bare_udt_without_registry_is_single_simple_cell() {
    let mut schema = TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![udt_column("addr", "person")],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    // No registry => no normalization (empty registry resolves nothing).
    normalize_schema_udts(&mut schema, &UdtRegistry::new());
    assert_eq!(
        schema.columns[0].data_type, "person",
        "with no matching registry entry the bare name is unchanged"
    );
    assert!(
        !is_complex_column(&schema.columns[0].data_type),
        "bare name without resolution is not a complex column"
    );

    // A whole-UDT write must not panic on the simple-cell path.
    let col = schema.columns[0].clone();
    let writer = DataWriter::new(create_test_stats());
    let udt = Value::Udt(crate::types::UdtValue {
        type_name: "person".to_string(),
        keyspace: "test_ks".to_string(),
        fields: vec![udt_field("name", Some(Value::Text("Alice".to_string())))],
    });
    let mut buf = Vec::new();
    let res = writer.write_cell(&mut buf, &col.name, &udt, 1_005_000);
    assert!(
        res.is_ok(),
        "bare-name fallback write must succeed as a simple cell: {res:?}"
    );
    assert!(!buf.is_empty(), "a simple cell must have been written");
}
