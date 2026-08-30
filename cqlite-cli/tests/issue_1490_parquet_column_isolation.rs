//! PER-COLUMN ISOLATION in the Parquet↔JSONL parity harness — issue #1490 (AD1)
//! round 7, finding 1, epic #1469.
//!
//! # The promise, and how the row projection broke it
//!
//! When one column's exported Arrow TYPE diverges, the harness defers THAT
//! column's values and compares every other column — that is what a
//! `KnownTypeGap` means, and the aggregate reports the deferral by name so it
//! cannot hide anything (`Failure::Unrunnable { column: Some(..) }`).
//!
//! The row projection used to decode EVERY column of the exported file anyway,
//! including the ones already blocked. `arrow_rows` deliberately has NO decoder
//! for a type the harness never declared valid (`UInt32`, `LargeList`,
//! `FixedSizeList`, …) — an accept-list broader than the decoder is a promise the
//! harness cannot keep — so a divergence INTO such a type failed the projection
//! WHOLESALE, and with it every unaffected column's value comparison. The
//! isolation the type stage had just computed was thrown away by the next stage.
//!
//! So: a blocked NON-KEY column is not decoded at all, and only an undecodable
//! KEY column — which is what ALIGNS the two sides' rows — blocks the comparison
//! whole.
//!
//! # Why these tests hand the projection a synthetic batch
//!
//! A real `cqlite export` cannot be made to emit `UInt32`: that is precisely
//! what the type check exists to catch, and the corpus's export is correct. The
//! property is therefore demonstrated against a hand-built `RecordBatch`
//! carrying an undecodable column, through `project_rows_for_test` — documented
//! test-support that a real run never calls. Each case pairs the isolation
//! assertion with the NEGATIVE control (the same batch with the column NOT
//! blocked, which must still fail loudly), so nothing here can pass vacuously.

#![cfg(feature = "state_machine")]

#[path = "support/parquet_parity/mod.rs"]
mod parquet_parity;

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use parquet_parity::canonical_jsonl::CanonicalValue;
use parquet_parity::cql_type::{parse_column, ColumnType};
use parquet_parity::golden_rows::GoldenRow;
use parquet_parity::{compare, project_rows_for_test, CaseOutcome, ParityCase, Row, SchemaCheck};

/// A case declaring `id int` (partition key), `age int` and `name text`. Only
/// its column/key DECLARATION is used: the projection never reads a fixture.
const CASE: ParityCase = ParityCase {
    keyspace: "test_isolation",
    table: "synthetic",
    schema: "da-test.cql",
    udts: &[],
    columns: &[("id", "int"), ("age", "int"), ("name", "text")],
    partition_key: &["id"],
    clustering: &[],
    schema_check: SchemaCheck::Synthetic {
        why: "a hand-built RecordBatch, not a corpus table — no committed schema declares it",
    },
    must_run: false,
    covers: "CONTROL for per-column isolation in the row projection",
    known_gap: None,
    known_type_gaps: &[],
};

fn columns() -> Vec<ColumnType> {
    CASE.columns
        .iter()
        .map(|(n, t)| parse_column(n, t, &[]).expect("declared type must parse"))
        .collect()
}

/// One row: `id` as `Int32` (decodable), `age` as `UInt32` (NO decoder exists),
/// `name` as `Utf8` (decodable) — the shape of a divergence into a type the
/// harness never declared valid.
fn batch_with_undecodable_age() -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("age", DataType::UInt32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![7i32])),
        Arc::new(UInt32Array::from(vec![41u32])),
        Arc::new(StringArray::from(vec!["ada"])),
    ];
    RecordBatch::try_new(Arc::new(schema), cols).expect("synthetic batch")
}

/// The premise both halves rest on: `UInt32` really is undecodable, so this is a
/// test about isolation and not about a decoder that quietly handles it.
#[test]
fn the_negative_control_an_unblocked_undecodable_column_still_fails_loudly() {
    let rendered = match project_rows_for_test(
        &CASE,
        &[batch_with_undecodable_age()],
        &columns(),
        &[],
        false,
    ) {
        Err(err) => format!("{err}"),
        Ok(_) => panic!("an UNBLOCKED undecodable Arrow type must fail the projection"),
    };
    assert!(
        rendered.contains("UInt32") && rendered.contains("no declared canonical rendering"),
        "the refusal must name the type it cannot render: {rendered}"
    );
}

/// THE finding: with the column BLOCKED by the type stage, the projection
/// succeeds and every SIBLING column still carries its value.
#[test]
fn a_blocked_undecodable_column_does_not_cancel_its_siblings() {
    let blocked = vec!["age".to_string()];
    let rows = project_rows_for_test(
        &CASE,
        &[batch_with_undecodable_age()],
        &columns(),
        &blocked,
        false,
    )
    .expect(
        "a column the TYPE stage already blocked must not be decoded, so its undecodable \
         Arrow type cannot cancel the other columns' value comparison",
    );
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(
        row.cell("name"),
        Some(&CanonicalValue::Text("ada".to_string())),
        "the unaffected sibling column must still be decoded and comparable"
    );
    assert_eq!(
        row.cell("id"),
        Some(&CanonicalValue::Int(7)),
        "the KEY column must still be decoded — it is what aligns the two sides' rows"
    );
    assert!(
        row.is_undecoded("age"),
        "the blocked column must be RECORDED as undecoded, not silently absent"
    );
    assert_eq!(
        row.cell("age"),
        None,
        "a blocked column must carry NO value: an `Absent` could compare EQUAL to a golden \
         absence and report coverage that never happened"
    );
}

/// A blocked column that IS decodable is skipped the same way — the rule is
/// "blocked", not "undecodable", so the two can never drift apart.
#[test]
fn a_blocked_but_decodable_column_is_skipped_too() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("age", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let cols: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![7i32])),
        Arc::new(Int32Array::from(vec![41i32])),
        Arc::new(StringArray::from(vec!["ada"])),
    ];
    let batch = RecordBatch::try_new(Arc::new(schema), cols).expect("synthetic batch");
    let rows = project_rows_for_test(&CASE, &[batch], &columns(), &["age".to_string()], false)
        .expect("projection must succeed");
    assert!(rows[0].is_undecoded("age"));
    assert_eq!(
        rows[0].cell("name"),
        Some(&CanonicalValue::Text("ada".to_string()))
    );
}

/// An undecodable KEY column DOES block the comparison whole — and says why,
/// because without the primary key the two sides' rows cannot be aligned at all.
#[test]
fn an_undecodable_key_column_blocks_the_comparison_whole() {
    let schema = Schema::new(vec![
        Field::new("id", DataType::UInt32, true),
        Field::new("age", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let cols: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from(vec![7u32])),
        Arc::new(Int32Array::from(vec![41i32])),
        Arc::new(StringArray::from(vec!["ada"])),
    ];
    let batch = RecordBatch::try_new(Arc::new(schema), cols).expect("synthetic batch");
    // Blocked, and still decoded: a blocked KEY column cannot be skipped.
    let rendered =
        match project_rows_for_test(&CASE, &[batch], &columns(), &["id".to_string()], false) {
            Err(err) => format!("{err}"),
            Ok(_) => panic!("an undecodable KEY column must block the projection"),
        };
    assert!(
        rendered.contains("PRIMARY-KEY column") && rendered.contains("cannot be aligned"),
        "the refusal must say WHY the whole comparison is blocked: {rendered}"
    );
}

/// When the type stage did not answer at all (`blocks_all_values`), no column's
/// values are compared, so none is decoded — and the projection still succeeds,
/// leaving the type stage's own refusal as the failure that is reported.
#[test]
fn a_type_stage_that_did_not_answer_blocks_every_non_key_column() {
    let rows = project_rows_for_test(
        &CASE,
        &[batch_with_undecodable_age()],
        &columns(),
        &[],
        true,
    )
    .expect("projection must succeed when every non-key column is blocked");
    assert!(rows[0].is_undecoded("age") && rows[0].is_undecoded("name"));
    assert_eq!(
        rows[0].cell("id"),
        Some(&CanonicalValue::Int(7)),
        "the KEY column is always decoded"
    );
}

/// BOOKKEEPING: if the compared set and the blocked set ever disagree, the
/// comparison REFUSES rather than compare an undecoded column against the
/// golden — an `Absent` could otherwise compare EQUAL to a golden absence and
/// report coverage that never happened.
#[test]
fn comparing_a_column_the_projection_skipped_is_refused() {
    let parquet: Vec<Row> = project_rows_for_test(
        &CASE,
        &[batch_with_undecodable_age()],
        &columns(),
        &["age".to_string()],
        false,
    )
    .expect("projection must succeed");

    let mut cells = std::collections::BTreeMap::new();
    cells.insert("id".to_string(), CanonicalValue::Int(7));
    cells.insert("age".to_string(), CanonicalValue::Absent);
    cells.insert("name".to_string(), CanonicalValue::Text("ada".to_string()));
    let golden = vec![GoldenRow {
        keys: vec![CanonicalValue::Int(7)],
        cells,
    }];

    // The columns the comparison is asked to cover WRONGLY include the blocked
    // one; `Stages::comparable_columns` removes it in a real run.
    let rendered = match compare(&CASE, &columns(), golden, parquet) {
        Err(err) => format!("{err}"),
        Ok(_) => panic!("comparing a column the projection skipped must be REFUSED"),
    };
    assert!(
        rendered.contains("did not decode") && rendered.contains("disagree"),
        "the refusal must name the bookkeeping disagreement: {rendered}"
    );
}

/// And the positive control for the same path: over the columns the projection
/// DID decode, the comparison runs and counts exactly those cells.
#[test]
fn the_sibling_columns_still_compare_and_are_counted() {
    let parquet = project_rows_for_test(
        &CASE,
        &[batch_with_undecodable_age()],
        &columns(),
        &["age".to_string()],
        false,
    )
    .expect("projection must succeed");

    let mut cells = std::collections::BTreeMap::new();
    cells.insert("id".to_string(), CanonicalValue::Int(7));
    cells.insert("name".to_string(), CanonicalValue::Text("ada".to_string()));
    let golden = vec![GoldenRow {
        keys: vec![CanonicalValue::Int(7)],
        cells,
    }];

    let comparable: Vec<ColumnType> = columns().into_iter().filter(|c| c.name != "age").collect();
    match compare(&CASE, &comparable, golden, parquet).expect("the siblings must compare") {
        CaseOutcome::Ran { rows, cells } => {
            assert_eq!((rows, cells), (1, 2), "id and name compared, age deferred");
        }
        CaseOutcome::Skipped(reason) => panic!("must not skip: {reason}"),
    }
}
