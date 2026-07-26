//! Tests for the conservative Arrow payload-byte estimator (issue #2825).
//!
//! Loaded via `#[path]` from `arrow_size.rs` so the production module stays
//! under the campsite file-size threshold (epic #1116).
//!
//! The load-bearing test here is [`estimate_is_conservative_across_shape_corpus`]:
//! it asserts `Σ estimate_arrow_row_bytes(..) >= arrow_payload_bytes(batch)` over
//! a corpus of row shapes. A future CQL type that `rows_to_record_batch` learns
//! to convert but the estimator does not model FAILS that test rather than
//! silently under-counting (spec Requirement 3).

use std::collections::HashMap;
use std::sync::Arc;

use super::*;
use crate::export::rows_to_record_batch;
use crate::query::{ColumnInfo, QueryRow};
use crate::schema::CqlType;
use crate::types::{DataType, UdtField, UdtValue, Value};
use crate::RowKey;

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

fn col(name: &str, data_type: DataType, cql_type: Option<CqlType>) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable: true,
        position: 0,
        table_name: None,
        cql_type,
    }
}

fn row(pairs: Vec<(&str, Value)>) -> QueryRow {
    let mut values: HashMap<Arc<str>, Value> = HashMap::new();
    for (name, value) in pairs {
        values.insert(Arc::from(name), value);
    }
    QueryRow::with_interned_values(RowKey::new(Vec::new()), values)
}

/// One named corpus entry: a projected column set plus the rows to convert.
struct Shape {
    name: &'static str,
    columns: Vec<ColumnInfo>,
    rows: Vec<QueryRow>,
}

fn text(s: &str) -> Value {
    Value::Text(s.as_bytes().to_vec().into())
}

fn blob(n: usize) -> Value {
    Value::Blob(vec![0xABu8; n].into())
}

/// `list<list<…<text>>>` nested `depth` levels deep.
fn nested_list_type(depth: usize) -> CqlType {
    let mut t = CqlType::Text;
    for _ in 0..depth {
        t = CqlType::List(Box::new(t));
    }
    t
}

/// The shape corpus required by spec Requirement 3: fixed-width columns, `text`,
/// `blob`, `list`/`set`, `map`, `tuple`/UDT, all-null rows, empty strings and
/// empty collections — plus the flat (`cql_type = None`) dispatch arms, which the
/// converter reaches through `build_string_array`/`build_list_array`/
/// `build_map_array`.
fn shape_corpus() -> Vec<Shape> {
    let mut shapes = Vec::new();

    shapes.push(Shape {
        name: "fixed-width scalars",
        columns: vec![
            col("b", DataType::Boolean, Some(CqlType::Boolean)),
            col("ti", DataType::TinyInt, Some(CqlType::TinyInt)),
            col("si", DataType::SmallInt, Some(CqlType::SmallInt)),
            col("i", DataType::Integer, Some(CqlType::Int)),
            col("bi", DataType::BigInt, Some(CqlType::BigInt)),
            col("f", DataType::Float32, Some(CqlType::Float)),
            col("d", DataType::Float, Some(CqlType::Double)),
            col("ts", DataType::Timestamp, Some(CqlType::Timestamp)),
            col("dt", DataType::Integer, Some(CqlType::Date)),
            col("tm", DataType::BigInt, Some(CqlType::Time)),
            col("u", DataType::Uuid, Some(CqlType::Uuid)),
            col("ct", DataType::BigInt, Some(CqlType::Counter)),
        ],
        rows: (0..64)
            .map(|i| {
                row(vec![
                    ("b", Value::Boolean(i % 2 == 0)),
                    ("ti", Value::TinyInt(i as i8)),
                    ("si", Value::SmallInt(i as i16)),
                    ("i", Value::Integer(i)),
                    ("bi", Value::BigInt(i64::from(i))),
                    ("f", Value::Float32(i as f32)),
                    ("d", Value::Float(f64::from(i))),
                    ("ts", Value::Timestamp(1_700_000_000_000 + i64::from(i))),
                    ("dt", Value::Date(19_000 + i)),
                    ("tm", Value::Time(i64::from(i) * 1_000_000)),
                    ("u", Value::Uuid([i as u8; 16])),
                    ("ct", Value::Counter(i64::from(i))),
                ])
            })
            .collect(),
    });

    shapes.push(Shape {
        name: "text narrow",
        columns: vec![col("t", DataType::Text, Some(CqlType::Text))],
        rows: (0..512)
            .map(|i| row(vec![("t", text(&format!("v{i}")))]))
            .collect(),
    });

    shapes.push(Shape {
        name: "text wide",
        columns: vec![col("t", DataType::Text, Some(CqlType::Text))],
        rows: (0..64)
            .map(|i| row(vec![("t", text(&"x".repeat(600 + i)))]))
            .collect(),
    });

    shapes.push(Shape {
        name: "text empty string",
        columns: vec![col("t", DataType::Text, Some(CqlType::Text))],
        rows: (0..8).map(|_| row(vec![("t", text(""))])).collect(),
    });

    shapes.push(Shape {
        name: "blob wide",
        columns: vec![col("b", DataType::Blob, Some(CqlType::Blob))],
        rows: (0..64).map(|i| row(vec![("b", blob(256 + i))])).collect(),
    });

    shapes.push(Shape {
        name: "blob single row",
        columns: vec![col("b", DataType::Blob, Some(CqlType::Blob))],
        rows: vec![row(vec![("b", blob(65_536))])],
    });

    shapes.push(Shape {
        name: "all null",
        columns: vec![
            col("t", DataType::Text, Some(CqlType::Text)),
            col("b", DataType::Blob, Some(CqlType::Blob)),
            col("i", DataType::Integer, Some(CqlType::Int)),
            col(
                "l",
                DataType::List,
                Some(CqlType::List(Box::new(CqlType::Int))),
            ),
        ],
        rows: (0..32)
            .map(|_| {
                row(vec![
                    ("t", Value::Null),
                    ("b", Value::Null),
                    ("i", Value::Null),
                    ("l", Value::Null),
                ])
            })
            .collect(),
    });

    shapes.push(Shape {
        name: "absent cells",
        columns: vec![
            col("t", DataType::Text, Some(CqlType::Text)),
            col("i", DataType::Integer, Some(CqlType::Int)),
        ],
        rows: (0..32).map(|_| row(vec![])).collect(),
    });

    shapes.push(Shape {
        name: "list<int>",
        columns: vec![col(
            "l",
            DataType::List,
            Some(CqlType::List(Box::new(CqlType::Int))),
        )],
        rows: (0..64)
            .map(|i| {
                row(vec![(
                    "l",
                    Value::List((0..=i).map(Value::Integer).collect()),
                )])
            })
            .collect(),
    });

    shapes.push(Shape {
        name: "set<text>",
        columns: vec![col(
            "s",
            DataType::Set,
            Some(CqlType::Set(Box::new(CqlType::Text))),
        )],
        rows: (0..48)
            .map(|i| {
                row(vec![(
                    "s",
                    Value::Set((0..=i).map(|j| text(&format!("e{j}"))).collect()),
                )])
            })
            .collect(),
    });

    shapes.push(Shape {
        name: "empty collections",
        columns: vec![
            col(
                "l",
                DataType::List,
                Some(CqlType::List(Box::new(CqlType::Int))),
            ),
            col(
                "m",
                DataType::Map,
                Some(CqlType::Map(
                    Box::new(CqlType::Text),
                    Box::new(CqlType::Int),
                )),
            ),
        ],
        rows: (0..16)
            .map(|_| {
                row(vec![
                    ("l", Value::List(Vec::new())),
                    ("m", Value::Map(Vec::new())),
                ])
            })
            .collect(),
    });

    // Nesting DEEPER than one level, empty at the leaf: `build_typed_value_array`
    // materializes one `ListArray` per DECLARED level whatever the value holds,
    // each carrying an empty 4-byte offsets buffer, so a per-value-only estimate
    // under-counts by ~4 bytes per level (review B2). Both the empty-collection
    // and the null spelling of the same cell.
    shapes.push(Shape {
        name: "deeply nested empty list",
        columns: vec![col("l", DataType::List, Some(nested_list_type(8)))],
        rows: vec![row(vec![("l", Value::List(Vec::new()))])],
    });

    shapes.push(Shape {
        name: "deeply nested null list",
        columns: vec![col("l", DataType::List, Some(nested_list_type(8)))],
        rows: (0..4)
            .map(|_| row(vec![("l", Value::Null)]))
            .chain(std::iter::once(row(vec![])))
            .collect(),
    });

    shapes.push(Shape {
        name: "map<text,list<text>> nested",
        columns: vec![col(
            "m",
            DataType::Map,
            Some(CqlType::Map(
                Box::new(CqlType::Text),
                Box::new(CqlType::List(Box::new(CqlType::Text))),
            )),
        )],
        rows: (0..24)
            .map(|i| {
                row(vec![(
                    "m",
                    Value::Map(
                        (0..=i)
                            .map(|j| {
                                (
                                    text(&format!("k{j}")),
                                    // Every other entry's list is EMPTY: the
                                    // declared element type's child array still
                                    // exists.
                                    Value::List(
                                        (0..(j % 3)).map(|e| text(&format!("e{e}"))).collect(),
                                    ),
                                )
                            })
                            .collect(),
                    ),
                )])
            })
            .collect(),
    });

    // A UDT whose DECLARED field list is empty (the unresolved-named-UDT case
    // `arrow_convert` documents): the converter takes the Utf8 fallback and
    // renders `{name: value, …}`, so every field NAME reaches the payload
    // (review B4).
    shapes.push(Shape {
        name: "udt with empty declared fields",
        columns: vec![col(
            "u",
            DataType::Udt,
            Some(CqlType::Udt("unresolved".into(), Vec::new())),
        )],
        rows: (0..16)
            .map(|i| {
                row(vec![(
                    "u",
                    Value::Udt(Box::new(UdtValue {
                        type_name: "unresolved".into(),
                        keyspace: "ks".into(),
                        fields: vec![
                            UdtField {
                                name: format!("a_long_field_name_{i}"),
                                value: Some(text(&"v".repeat(i as usize % 20))),
                            },
                            UdtField {
                                name: "second_field".into(),
                                value: Some(Value::Integer(i)),
                            },
                        ],
                    })),
                )])
            })
            .collect(),
    });

    shapes.push(Shape {
        name: "tuple with empty declared elements",
        columns: vec![col("tp", DataType::Tuple, Some(CqlType::Tuple(Vec::new())))],
        rows: (0..8)
            .map(|i| {
                row(vec![(
                    "tp",
                    Value::Tuple(vec![Value::Integer(i), text("tail")]),
                )])
            })
            .collect(),
    });

    shapes.push(Shape {
        name: "map<text,bigint>",
        columns: vec![col(
            "m",
            DataType::Map,
            Some(CqlType::Map(
                Box::new(CqlType::Text),
                Box::new(CqlType::BigInt),
            )),
        )],
        rows: (0..48)
            .map(|i| {
                row(vec![(
                    "m",
                    Value::Map(
                        (0..=i)
                            .map(|j| (text(&format!("k{j}")), Value::BigInt(i64::from(j))))
                            .collect(),
                    ),
                )])
            })
            .collect(),
    });

    shapes.push(Shape {
        name: "tuple<int,text>",
        columns: vec![col(
            "tp",
            DataType::Tuple,
            Some(CqlType::Tuple(vec![CqlType::Int, CqlType::Text])),
        )],
        rows: (0..64)
            .map(|i| {
                row(vec![(
                    "tp",
                    Value::Tuple(vec![Value::Integer(i), text(&"t".repeat(i as usize % 40))]),
                )])
            })
            .collect(),
    });

    shapes.push(Shape {
        name: "udt",
        columns: vec![col(
            "u",
            DataType::Udt,
            Some(CqlType::Udt(
                "addr".into(),
                vec![
                    ("street".into(), CqlType::Text),
                    ("zip".into(), CqlType::Int),
                ],
            )),
        )],
        rows: (0..64)
            .map(|i| {
                row(vec![(
                    "u",
                    Value::Udt(Box::new(UdtValue {
                        type_name: "addr".into(),
                        keyspace: "ks".into(),
                        fields: vec![
                            UdtField {
                                name: "street".into(),
                                value: Some(text(&"s".repeat(i as usize % 50))),
                            },
                            UdtField {
                                name: "zip".into(),
                                value: Some(Value::Integer(i)),
                            },
                        ],
                    })),
                )])
            })
            .collect(),
    });

    shapes.push(Shape {
        name: "frozen<list<text>>",
        columns: vec![col(
            "fl",
            DataType::Frozen,
            Some(CqlType::Frozen(Box::new(CqlType::List(Box::new(
                CqlType::Text,
            ))))),
        )],
        rows: (0..32)
            .map(|i| {
                row(vec![(
                    "fl",
                    Value::Frozen(Box::new(Value::List(
                        (0..=i).map(|j| text(&format!("f{j}"))).collect(),
                    ))),
                )])
            })
            .collect(),
    });

    shapes.push(Shape {
        name: "high-fidelity scalars",
        columns: vec![
            col("de", DataType::Blob, Some(CqlType::Decimal)),
            col("vi", DataType::Blob, Some(CqlType::Varint)),
            col("du", DataType::Text, Some(CqlType::Duration)),
            col("in", DataType::Text, Some(CqlType::Inet)),
            col("tu", DataType::Uuid, Some(CqlType::TimeUuid)),
        ],
        rows: (0..48)
            .map(|i| {
                row(vec![
                    (
                        "de",
                        Value::Decimal {
                            scale: 3,
                            unscaled: vec![0x01, 0x02, i as u8],
                        },
                    ),
                    ("vi", Value::Varint(vec![0x01, i as u8].into())),
                    (
                        "du",
                        Value::Duration {
                            months: i,
                            days: i,
                            nanos: i64::from(i) * 1_000,
                        },
                    ),
                    ("in", Value::Inet(vec![10, 0, 0, i as u8].into())),
                    ("tu", Value::Uuid([i as u8; 16])),
                ])
            })
            .collect(),
    });

    // Flat dispatch (no authoritative CQL type): the converter renders through
    // `build_string_array` / `build_list_array` / `build_map_array`.
    shapes.push(Shape {
        name: "flat text/blob/int",
        columns: vec![
            col("t", DataType::Text, None),
            col("b", DataType::Blob, None),
            col("i", DataType::Integer, None),
        ],
        rows: (0..64)
            .map(|i| {
                row(vec![
                    ("t", text(&"z".repeat(i as usize % 70))),
                    ("b", blob(i as usize % 90)),
                    ("i", Value::Integer(i)),
                ])
            })
            .collect(),
    });

    shapes.push(Shape {
        name: "flat list rendered",
        columns: vec![col("l", DataType::List, None)],
        rows: (0..48)
            .map(|i| {
                row(vec![(
                    "l",
                    Value::List(
                        (0..=i)
                            .map(|j| text(&format!("elem-{j}-{}", "p".repeat(j as usize % 12))))
                            .collect(),
                    ),
                )])
            })
            .collect(),
    });

    shapes.push(Shape {
        name: "flat map rendered",
        columns: vec![col("m", DataType::Map, None)],
        rows: (0..48)
            .map(|i| {
                row(vec![(
                    "m",
                    Value::Map(
                        (0..=i)
                            .map(|j| (text(&format!("k{j}")), blob(j as usize % 20)))
                            .collect(),
                    ),
                )])
            })
            .collect(),
    });

    shapes.push(Shape {
        name: "flat tuple rendered",
        columns: vec![col("tp", DataType::Tuple, None)],
        rows: (0..48)
            .map(|i| {
                row(vec![(
                    "tp",
                    Value::Tuple(vec![
                        Value::Integer(i),
                        text(&"q".repeat(i as usize % 30)),
                        Value::Uuid([i as u8; 16]),
                    ]),
                )])
            })
            .collect(),
    });

    // `Value::Json` through the flat `DataType::Json` arm — `json_render_bytes`
    // is the most intricate charging arm and was otherwise exercised only by the
    // fail-closed depth test (review N5).
    shapes.push(Shape {
        name: "flat json rendered",
        columns: vec![col("j", DataType::Json, None)],
        rows: (0..24)
            .map(|i| {
                row(vec![(
                    "j",
                    Value::Json(Box::new(serde_json::json!({
                        "id": i,
                        "name": "a\"quoted\"\u{1F600}name",
                        "tags": ["x", "yy", "zzz"],
                        "nested": {"flag": true, "none": null, "ratio": 1.5},
                        "empty_arr": [],
                        "empty_obj": {},
                    }))),
                )])
            })
            .collect(),
    });

    // Single-row batches: arrow's short-buffer length rounding is proportionally
    // largest here, so they are the tightest case for the per-slot slack.
    shapes.push(Shape {
        name: "single narrow row",
        columns: vec![
            col("i", DataType::Integer, Some(CqlType::Int)),
            col("t", DataType::Text, Some(CqlType::Text)),
        ],
        rows: vec![row(vec![("i", Value::Integer(1)), ("t", text("a"))])],
    });

    shapes.push(Shape {
        name: "single row with collections",
        columns: vec![
            col(
                "l",
                DataType::List,
                Some(CqlType::List(Box::new(CqlType::Text))),
            ),
            col(
                "m",
                DataType::Map,
                Some(CqlType::Map(
                    Box::new(CqlType::Text),
                    Box::new(CqlType::Int),
                )),
            ),
            col(
                "u",
                DataType::Udt,
                Some(CqlType::Udt(
                    "p".into(),
                    vec![("a".into(), CqlType::Int), ("b".into(), CqlType::Text)],
                )),
            ),
        ],
        rows: vec![row(vec![
            ("l", Value::List(vec![text("one"), text("two")])),
            ("m", Value::Map(vec![(text("k"), Value::Integer(7))])),
            (
                "u",
                Value::Udt(Box::new(UdtValue {
                    type_name: "p".into(),
                    keyspace: "ks".into(),
                    fields: vec![
                        UdtField {
                            name: "a".into(),
                            value: Some(Value::Integer(3)),
                        },
                        UdtField {
                            name: "b".into(),
                            value: Some(text("bee")),
                        },
                    ],
                })),
            ),
        ])],
    });

    shapes
}

// ---------------------------------------------------------------------------
// Requirement 3: the estimator is conservative across the shape corpus
// ---------------------------------------------------------------------------

/// `Σ estimate_arrow_row_bytes(..) >= arrow_payload_bytes(rows_to_record_batch(..))`
/// for every shape in the corpus — the conservatism contract.
///
/// A CQL type the converter handles but the estimator does not model shows up
/// here as an under-count and FAILS, so a future type addition cannot silently
/// introduce one (spec Requirement 3).
#[test]
fn estimate_is_conservative_across_shape_corpus() {
    for shape in shape_corpus() {
        let estimated: usize = shape
            .rows
            .iter()
            .map(|r| estimate_arrow_row_bytes(&shape.columns, r))
            .fold(0usize, |a, b| a.saturating_add(b));
        let batch = rows_to_record_batch(&shape.columns, &shape.rows)
            .unwrap_or_else(|e| panic!("shape '{}' failed to convert: {e}", shape.name));
        let realized = arrow_payload_bytes(&batch);
        assert!(
            estimated >= realized,
            "shape '{}': estimate {estimated} UNDER-COUNTS realized payload {realized}",
            shape.name
        );
        // Non-vacuity: the corpus must exercise real bytes, not empty batches.
        assert!(
            realized > 0 && batch.num_rows() == shape.rows.len(),
            "shape '{}' is vacuous: {realized} payload bytes, {} rows",
            shape.name,
            batch.num_rows()
        );
    }
}

/// The estimator must not be so loose that it is useless: it stays within a
/// bounded multiple of the realized payload, so the cap cuts batches near the
/// configured size rather than far short of it.
///
/// Covers the COLLECTION-heavy and MULTI-COLUMN shapes as well as the wide ones
/// (review B3): those are exactly where a slack charged per SLOT instead of per
/// COLUMN inflates the estimate — a `list<int>` cell of 1000 elements or a
/// 30-column fixed-width row is where an over-estimate turns into a real
/// batching regression, so the looseness is measured there, not only where it
/// amortizes away.
#[test]
fn estimate_is_within_a_bounded_multiple_of_the_payload() {
    // (shape, allowed multiple). The multiples are TIGHT — each is the smallest
    // whole number above the shape's measured ratio — so a regression in the
    // charging model shows up here rather than being absorbed.
    let cases: &[(&str, usize)] = &[
        ("text wide", 2),
        ("blob wide", 2),
        ("blob single row", 2),
        // Collection-heavy: the per-element charge is what must stay tight.
        ("list<int>", 2),
        ("set<text>", 2),
        ("map<text,bigint>", 2),
        ("map<text,list<text>> nested", 3),
        ("flat list rendered", 3),
        // Multi-column narrow: dominated by the per-COLUMN residual, which is
        // the term that must NOT scale with cell count.
        ("fixed-width scalars", 3),
        ("flat text/blob/int", 3),
        ("flat json rendered", 5),
        ("high-fidelity scalars", 4),
    ];
    for (name, multiple) in cases {
        let shape = shape_corpus()
            .into_iter()
            .find(|s| &s.name == name)
            .unwrap_or_else(|| panic!("missing corpus shape '{name}'"));
        let estimated: usize = shape
            .rows
            .iter()
            .map(|r| estimate_arrow_row_bytes(&shape.columns, r))
            .sum();
        let batch = rows_to_record_batch(&shape.columns, &shape.rows).expect("convert");
        let realized = arrow_payload_bytes(&batch);
        assert!(
            estimated <= realized.saturating_mul(*multiple),
            "shape '{name}': estimate {estimated} is more than {multiple}x the \
             realized payload {realized}"
        );
    }
}

/// The per-column residual is charged ONCE PER COLUMN, never per cell (review
/// B3): growing a collection cell's ELEMENT COUNT by 100x must grow the estimate
/// by roughly the elements' own Arrow cost, not by 100 slack charges.
///
/// Pinned as a ratio against the realized payload so it cannot be satisfied by
/// simply shrinking a constant: a per-slot slack of `S` would show up here as
/// `~S/4` extra bytes per `int` element.
#[test]
fn per_column_residual_does_not_scale_with_element_count() {
    let columns = vec![col(
        "l",
        DataType::List,
        Some(CqlType::List(Box::new(CqlType::Int))),
    )];
    let big = row(vec![(
        "l",
        Value::List((0..1000).map(Value::Integer).collect()),
    )]);
    let estimated = estimate_arrow_row_bytes(&columns, &big);
    let batch = rows_to_record_batch(&columns, std::slice::from_ref(&big)).expect("convert");
    let realized = arrow_payload_bytes(&batch);
    assert!(realized >= 4000, "vacuous: {realized} realized bytes");
    assert!(
        estimated >= realized,
        "estimate {estimated} under-counts {realized}"
    );
    // 1000 int elements: ~4 KB realized. A per-SLOT slack of 32 would put this
    // at ~9x.
    assert!(
        estimated <= realized.saturating_mul(3) / 2,
        "estimate {estimated} is more than 1.5x the realized payload {realized} \
         — the residual is scaling with element count, not column count"
    );
}

/// A wide fixed-width schema still lets the ROW-cap bind at the 4 MiB default:
/// the per-row estimate for 30 `int` columns must leave room for a full
/// 8192-row batch (review B3 — a per-slot slack put this at ~4,000 rows).
#[test]
fn a_wide_fixed_width_row_still_fits_a_full_default_batch() {
    const N_COLS: i32 = 30;
    const DEFAULT_CAP: usize = 4 * 1024 * 1024;
    const BATCH_ROWS: usize = 8192;
    let names: Vec<String> = (0..N_COLS).map(|i| format!("c{i}")).collect();
    let columns: Vec<ColumnInfo> = names
        .iter()
        .map(|n| col(n, DataType::Integer, Some(CqlType::Int)))
        .collect();
    let r = row(names
        .iter()
        .zip(0..N_COLS)
        .map(|(n, i)| (n.as_str(), Value::Integer(i)))
        .collect());
    let per_row = estimate_arrow_row_bytes(&columns, &r);
    assert!(
        per_row.saturating_mul(BATCH_ROWS) <= DEFAULT_CAP,
        "a {N_COLS}-column int row estimates {per_row} B, so the byte-cap would \
         cut at {} rows — below the {BATCH_ROWS}-row batch size, a throughput \
         regression on a narrow shape",
        DEFAULT_CAP / per_row.max(1)
    );
}

/// The conservatism property is DISCRIMINATING, not a tautology.
///
/// Spec Requirement 3 asks that a column type the converter handles but the
/// estimator does not model FAIL the property test. Two mechanisms enforce that
/// here. First, `column_shape`/`charge_cql`/`charge_flat` match [`CqlType`] and
/// [`DataType`] EXHAUSTIVELY with no wildcard arm, so a newly added variant is a
/// *compile* error before it can ever be under-counted. Second — proven below —
/// an estimator that models a type's CONTENT but forgets its Arrow structural
/// overhead (the exact failure mode of `Value::size_estimate`,
/// `memory::estimate_value_size` and `Memtable::estimate_value_size`, and the
/// likeliest shape of a careless future arm) UNDER-counts real corpus shapes and
/// so trips the assertion.
#[test]
fn the_conservatism_property_catches_a_content_only_estimator() {
    /// A deliberately unmodelled estimator: raw content bytes, zero Arrow
    /// structural overhead. Recursion is fine here — the corpus is shallow and
    /// this is test-only scaffolding, not the production walk.
    fn content_only(v: &Value) -> usize {
        match v {
            Value::Null | Value::Tombstone(_) => 0,
            Value::Boolean(_) | Value::TinyInt(_) => 1,
            Value::SmallInt(_) => 2,
            Value::Integer(_) | Value::Float32(_) | Value::Date(_) => 4,
            Value::BigInt(_)
            | Value::Counter(_)
            | Value::Float(_)
            | Value::Timestamp(_)
            | Value::Time(_) => 8,
            Value::Uuid(_) => 16,
            Value::Duration { .. } => 16,
            Value::Text(s) => s.len(),
            Value::Blob(b) => b.len(),
            Value::Varint(b) => b.len(),
            Value::Inet(b) => b.len(),
            Value::Decimal { unscaled, .. } => unscaled.len(),
            Value::Json(j) => j.to_string().len(),
            Value::List(items) | Value::Set(items) | Value::Tuple(items) => {
                items.iter().map(content_only).sum()
            }
            Value::Map(pairs) => pairs
                .iter()
                .map(|(k, v)| content_only(k) + content_only(v))
                .sum(),
            Value::Udt(u) => u
                .fields
                .iter()
                .filter_map(|f| f.value.as_ref())
                .map(content_only)
                .sum(),
            Value::Frozen(inner) => content_only(inner),
        }
    }

    let mut under_counted: Vec<&str> = Vec::new();
    for shape in shape_corpus() {
        let naive: usize = shape
            .rows
            .iter()
            .map(|r| {
                shape
                    .columns
                    .iter()
                    .filter_map(|c| r.values.get(c.name.as_str()))
                    .map(content_only)
                    .sum::<usize>()
            })
            .sum();
        let batch = rows_to_record_batch(&shape.columns, &shape.rows).expect("convert");
        if naive < arrow_payload_bytes(&batch) {
            under_counted.push(shape.name);
        }
    }
    assert!(
        !under_counted.is_empty(),
        "a content-only estimator under-counted NOTHING — the conservatism \
         property test cannot detect an unmodelled type and is a tautology"
    );
    // And the real estimator covers every one of those same shapes (the
    // property test above asserts this for the whole corpus).
    for name in &under_counted {
        let shape = shape_corpus()
            .into_iter()
            .find(|s| &s.name == name)
            .unwrap_or_else(|| panic!("missing shape '{name}'"));
        let estimated: usize = shape
            .rows
            .iter()
            .map(|r| estimate_arrow_row_bytes(&shape.columns, r))
            .sum();
        let batch = rows_to_record_batch(&shape.columns, &shape.rows).expect("convert");
        assert!(
            estimated >= arrow_payload_bytes(&batch),
            "shape '{name}' under-counted by the REAL estimator"
        );
    }
}

// ---------------------------------------------------------------------------
// Requirement 3: width sensitivity
// ---------------------------------------------------------------------------

/// Two rows differing only in one blob cell's length differ in estimate by at
/// least that content-byte difference — the estimator is width-driven, not a
/// per-row constant.
#[test]
fn variable_width_content_drives_the_estimate() {
    let columns = vec![col("b", DataType::Blob, Some(CqlType::Blob))];
    let small = row(vec![("b", blob(16))]);
    let large = row(vec![("b", blob(64 * 1024))]);
    let d = estimate_arrow_row_bytes(&columns, &large) - estimate_arrow_row_bytes(&columns, &small);
    assert!(
        d >= 64 * 1024 - 16,
        "estimate difference {d} is below the content difference"
    );
}

/// The same holds for `text`, and through the flat (untyped) dispatch.
#[test]
fn text_width_drives_the_estimate_on_both_dispatch_paths() {
    for cql in [Some(CqlType::Text), None] {
        let columns = vec![col("t", DataType::Text, cql.clone())];
        let small = row(vec![("t", text("ab"))]);
        let large = row(vec![("t", text(&"a".repeat(10_000)))]);
        let d =
            estimate_arrow_row_bytes(&columns, &large) - estimate_arrow_row_bytes(&columns, &small);
        assert!(
            d >= 10_000 - 2,
            "estimate difference {d} too small for {cql:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Requirement 3: pathological values fail closed
// ---------------------------------------------------------------------------

/// A collection whose element count exceeds the node budget saturates instead of
/// spinning — no panic, no unbounded work, and the returned width trips the cap.
#[test]
fn oversized_collection_fails_closed_to_a_saturated_width() {
    let columns = vec![col(
        "l",
        DataType::List,
        Some(CqlType::List(Box::new(CqlType::Int))),
    )];
    let huge = Value::List(
        (0..(MAX_ESTIMATE_NODES + 1) as i32)
            .map(Value::Integer)
            .collect(),
    );
    let r = row(vec![("l", huge)]);
    assert_eq!(estimate_arrow_row_bytes(&columns, &r), usize::MAX);
}

/// A value nested far deeper than any schema still terminates, returns a
/// saturated width, and never recurses (the walk is an explicit worklist — a
/// recursive one would blow the stack here instead of returning).
#[test]
fn deeply_nested_value_fails_closed_without_recursion() {
    let mut v = Value::Integer(1);
    for _ in 0..(MAX_ESTIMATE_NODES + 16) {
        v = Value::List(vec![v]);
    }
    let columns = vec![col("l", DataType::List, None)];
    let r = row(vec![("l", v)]);
    assert_eq!(estimate_arrow_row_bytes(&columns, &r), usize::MAX);
    // `Value`'s derived `Drop` IS recursive, so releasing a 65k-deep chain would
    // overflow the stack in the harness (not in the estimator, which already
    // returned). Leak the fixture rather than weaken the depth under test.
    std::mem::forget(r);
}

/// A row that mixes a saturating cell with ordinary ones still reports
/// `usize::MAX` — repeated additions of the fail-closed sentinel stay saturated
/// rather than wrapping to a small (and therefore cap-defeating) number.
#[test]
fn saturating_arithmetic_never_wraps() {
    let columns = vec![
        col("b0", DataType::Blob, Some(CqlType::Blob)),
        col(
            "l1",
            DataType::List,
            Some(CqlType::List(Box::new(CqlType::Int))),
        ),
        col("b2", DataType::Blob, Some(CqlType::Blob)),
        col("b3", DataType::Blob, Some(CqlType::Blob)),
    ];
    let r = row(vec![
        ("b0", blob(8)),
        (
            "l1",
            Value::List(
                (0..(MAX_ESTIMATE_NODES + 1) as i32)
                    .map(Value::Integer)
                    .collect(),
            ),
        ),
        ("b2", blob(8)),
        ("b3", blob(8)),
    ]);
    assert_eq!(estimate_arrow_row_bytes(&columns, &r), usize::MAX);
}

/// A `Frozen` chain deeper than the unwrap bound terminates without panicking.
#[test]
fn deep_frozen_chain_terminates() {
    let mut v = Value::Integer(1);
    for _ in 0..64 {
        v = Value::Frozen(Box::new(v));
    }
    let columns = vec![col("f", DataType::Integer, Some(CqlType::Int))];
    let r = row(vec![("f", v)]);
    // Terminates and returns a finite, non-zero width.
    let e = estimate_arrow_row_bytes(&columns, &r);
    assert!(e > 0 && e < usize::MAX);
}

// ---------------------------------------------------------------------------
// Projection / cell-resolution semantics
// ---------------------------------------------------------------------------

/// Only projected columns are charged: a value present in the row but absent
/// from `columns` never reaches the batch, so it must never reach the estimate.
#[test]
fn unprojected_values_are_not_charged() {
    let projected = vec![col("t", DataType::Text, Some(CqlType::Text))];
    let lean = row(vec![("t", text("abc"))]);
    let fat = row(vec![("t", text("abc")), ("other", blob(1_000_000))]);
    assert_eq!(
        estimate_arrow_row_bytes(&projected, &lean),
        estimate_arrow_row_bytes(&projected, &fat)
    );
}

/// An empty projection costs nothing.
#[test]
fn empty_projection_is_zero() {
    assert_eq!(
        estimate_arrow_row_bytes(&[], &row(vec![("t", text("x"))])),
        0
    );
}

// ---------------------------------------------------------------------------
// The payload oracle itself
// ---------------------------------------------------------------------------

/// `arrow_payload_bytes` counts buffer LENGTHS, so it is strictly at or below
/// `get_array_memory_size()` (which counts capacity) — the two currencies the
/// byte-cap keeps separate.
#[test]
fn payload_bytes_never_exceeds_reported_memory_size() {
    for shape in shape_corpus() {
        let batch = rows_to_record_batch(&shape.columns, &shape.rows).expect("convert");
        let payload = arrow_payload_bytes(&batch);
        let capacity = batch.get_array_memory_size();
        assert!(
            payload <= capacity,
            "shape '{}': payload {payload} exceeds reported memory {capacity}",
            shape.name
        );
    }
}
