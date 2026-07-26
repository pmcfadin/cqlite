//! The SHARED Arrow row-shape corpus (issues #2825 / #2821 / #2932).
//!
//! One corpus, two consumers, so neither can drift from the other:
//!
//! * `export::arrow_size_tests::estimate_is_conservative_across_shape_corpus`
//!   asserts the estimator contract
//!   `Sum estimate_arrow_row_bytes >= arrow_payload_bytes(batch)`.
//! * `cqlite_flight::batch_bytes_tests::the_capacity_bound_holds_over_the_shared_shape_corpus`
//!   asserts the PUBLISHED capacity conversion
//!   `get_array_memory_size() <= worst_case_batch_capacity_bytes(Sum estimate, nodes, 0)`.
//!
//! The second consumer is why this module exists at all rather than staying a
//! private test helper. Issue #2821 turned that capacity bound from a loose doc
//! claim into a **fail-closed runtime check**: a shape whose fixed per-array-node
//! cost exceeds `BATCH_BYTES_PER_COLUMN_SLACK` does not under-report a metric, it
//! terminates a live `do_get` with an internal error. A hand-written list of six
//! shapes in the flight crate is exactly how that gap appeared (#2932), so the
//! guard now runs over EVERY shape the estimator is validated against —
//! `FixedSizeBinary(16)` (uuid/timeuuid), boolean/decimal/varint/timestamp,
//! tuple/UDT (Struct), `set`, deeply nested collections, and the
//! `cql_type = None` flat dispatch arms that route through different builders
//! (`build_binary_array` / `build_list_array` / `build_map_array`).
//!
//! Adding a shape here therefore strengthens BOTH contracts at once; adding a CQL
//! type the converter handles without adding it here is the omission both tests
//! exist to catch.
//!
//! Compiled only under `cfg(test)` (for the in-crate estimator tests) or the
//! opt-in `arrow-shape-corpus` feature (for the `cqlite-flight` dev-dependency),
//! following the `fuzz` / `bench-internals` / `work-counters` precedent: a
//! default `cargo build -p cqlite-core` links none of it.

use std::collections::HashMap;
use std::sync::Arc;

use crate::query::{ColumnInfo, QueryRow};
use crate::schema::CqlType;
use crate::types::{DataType, UdtField, UdtValue, Value};
use crate::RowKey;

pub fn col(name: &str, data_type: DataType, cql_type: Option<CqlType>) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable: true,
        position: 0,
        table_name: None,
        cql_type,
    }
}

pub fn row(pairs: Vec<(&str, Value)>) -> QueryRow {
    let mut values: HashMap<Arc<str>, Value> = HashMap::new();
    for (name, value) in pairs {
        values.insert(Arc::from(name), value);
    }
    QueryRow::with_interned_values(RowKey::new(Vec::new()), values)
}

/// One named corpus entry: a projected column set plus the rows to convert.
pub struct Shape {
    /// Human-readable shape name, quoted in every failure message.
    pub name: &'static str,
    /// The projected column set (authoritative `ColumnInfo`, no heuristics).
    pub columns: Vec<ColumnInfo>,
    /// The rows to convert.
    pub rows: Vec<QueryRow>,
}

pub fn text(s: &str) -> Value {
    Value::Text(s.as_bytes().to_vec().into())
}

pub fn blob(n: usize) -> Value {
    Value::Blob(vec![0xABu8; n].into())
}

/// `list<list<…<text>>>` nested `depth` levels deep.
pub fn nested_list_type(depth: usize) -> CqlType {
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
pub fn shape_corpus() -> Vec<Shape> {
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

    // `cql_type` and `data_type` DISAGREE. `convert_column_to_array` dispatches
    // `Text`/`Ascii`/`Varchar` on `data_type` alone, so this column is built by
    // `build_binary_array`, not `build_string_array` — an estimate that routed
    // it to the typed TEXT arm would charge zero content for a `blob` value and
    // under-count the whole column.
    shapes.push(Shape {
        name: "text cql type over a blob data type",
        columns: vec![col("b", DataType::Blob, Some(CqlType::Text))],
        rows: (0..32).map(|i| row(vec![("b", blob(300 + i))])).collect(),
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

    // The 1-BYTE-MARGIN point of the whole `ARROW_COLUMN_SLACK_BYTES` derivation
    // (review nit): a flat `DataType::Map` with a SINGLE row whose cell is empty
    // realizes 17 bytes — map offsets `2 * 4`, the always-present null buffer
    // `ceil(1/8)`, and the key and value `Utf8` children's empty offsets buffers
    // `4 + 4` — against 18 charged. Every other flat-map shape has 48 rows, over
    // which the residual amortizes away, so a change to the constant would slip
    // through unnoticed without this shape.
    shapes.push(Shape {
        name: "flat map single empty row",
        columns: vec![col("m", DataType::Map, None)],
        rows: vec![row(vec![("m", Value::Map(Vec::new()))])],
    });

    // Same, with the cell ABSENT rather than empty: `build_map_array` takes its
    // null branch and still materializes both children.
    shapes.push(Shape {
        name: "flat map single absent row",
        columns: vec![col("m", DataType::Map, None)],
        rows: vec![row(vec![])],
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
