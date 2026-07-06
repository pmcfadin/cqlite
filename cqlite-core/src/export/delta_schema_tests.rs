//! Unit tests for `super::derive_delta_schema` (extracted per epic #1135).

use super::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use std::collections::HashMap;

// ------------------------------------------------------------------
// Helper: build the design's example table
//   t (pk int, ck text, val text, st text STATIC, PRIMARY KEY (pk, ck))
// ------------------------------------------------------------------
fn example_table() -> TableSchema {
    TableSchema {
        keyspace: "example_ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "text".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "val".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "st".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

// ------------------------------------------------------------------
// Snapshot test: schema for the design's example table
// ------------------------------------------------------------------

#[test]
fn snapshot_example_table_schema() {
    let table = example_table();
    let opts = DeltaSchemaOpts::default();
    let schema = derive_delta_schema(&table, &opts).expect("derive_delta_schema failed");

    // --- Verify field count ---
    // pk, ck, val, st, __op, __ts, __range_start, __range_end = 8
    assert_eq!(
        schema.fields().len(),
        8,
        "expected 8 fields, got {}",
        schema.fields().len()
    );

    // --- 1. pk: Int32, non-nullable ---
    let pk = schema.field_with_name("pk").expect("no pk field");
    assert_eq!(*pk.data_type(), ArrowDataType::Int32, "pk should be Int32");
    assert!(!pk.is_nullable(), "pk should be non-nullable");

    // --- 2. ck: Utf8, nullable (null for partition-scoped ops) ---
    let ck = schema.field_with_name("ck").expect("no ck field");
    assert_eq!(*ck.data_type(), ArrowDataType::Utf8, "ck should be Utf8");
    assert!(ck.is_nullable(), "ck should be nullable");

    // --- 3. val: Struct(nullable) { value: Utf8, writetime: i64, expires_at: i64|null } ---
    //    val has no `replaced` (it is not a collection)
    let val = schema.field_with_name("val").expect("no val field");
    assert!(val.is_nullable(), "val cell struct should be nullable");
    if let ArrowDataType::Struct(val_fields) = val.data_type() {
        assert_eq!(
            val_fields.len(),
            3,
            "val struct should have 3 fields (no replaced)"
        );
        let vf = val_fields
            .find("value")
            .expect("no value field in val struct");
        assert_eq!(*vf.1.data_type(), ArrowDataType::Utf8);
        assert!(
            vf.1.is_nullable(),
            "value should be nullable (cell tombstone = null)"
        );
        let wt = val_fields.find("writetime").expect("no writetime field");
        assert_eq!(*wt.1.data_type(), ArrowDataType::Int64);
        assert!(!wt.1.is_nullable(), "writetime should be non-nullable");
        let ea = val_fields.find("expires_at").expect("no expires_at field");
        assert_eq!(*ea.1.data_type(), ArrowDataType::Int64);
        assert!(ea.1.is_nullable(), "expires_at should be nullable");
    } else {
        panic!("val should be a Struct, got {:?}", val.data_type());
    }

    // --- 4. st: same struct shape as val (static but same cell struct) ---
    let st = schema.field_with_name("st").expect("no st field");
    assert!(st.is_nullable(), "st cell struct should be nullable");
    if let ArrowDataType::Struct(st_fields) = st.data_type() {
        assert_eq!(
            st_fields.len(),
            3,
            "st struct should have 3 fields (no replaced)"
        );
    } else {
        panic!("st should be a Struct");
    }

    // --- 5. __op: Dictionary(Int8, Utf8), non-nullable ---
    let op = schema.field_with_name("__op").expect("no __op field");
    assert!(!op.is_nullable(), "__op should be non-nullable");
    assert!(
        matches!(op.data_type(), ArrowDataType::Dictionary(key, val)
                if **key == ArrowDataType::Int8 && **val == ArrowDataType::Utf8),
        "__op should be Dictionary(Int8, Utf8), got {:?}",
        op.data_type()
    );

    // --- 6. __ts: Int64, nullable ---
    let ts = schema.field_with_name("__ts").expect("no __ts field");
    assert_eq!(*ts.data_type(), ArrowDataType::Int64);
    assert!(ts.is_nullable(), "__ts should be nullable");

    // --- 7. __range_start: Struct(nullable) { ck: Utf8, inclusive: Boolean } ---
    let rs = schema
        .field_with_name("__range_start")
        .expect("no __range_start field");
    assert!(rs.is_nullable(), "__range_start should be nullable");
    if let ArrowDataType::Struct(rs_fields) = rs.data_type() {
        assert_eq!(
            rs_fields.len(),
            2,
            "__range_start struct should have 2 fields (ck + inclusive)"
        );
        let ck_f = rs_fields.find("ck").expect("no ck in __range_start");
        assert_eq!(*ck_f.1.data_type(), ArrowDataType::Utf8);
        assert!(
            ck_f.1.is_nullable(),
            "ck in range bound should be nullable (prefix)"
        );
        let inc_f = rs_fields
            .find("inclusive")
            .expect("no inclusive in __range_start");
        assert_eq!(*inc_f.1.data_type(), ArrowDataType::Boolean);
        assert!(!inc_f.1.is_nullable(), "inclusive should be non-nullable");
    } else {
        panic!("__range_start should be a Struct");
    }

    // --- 8. __range_end: same shape as __range_start ---
    let re = schema
        .field_with_name("__range_end")
        .expect("no __range_end field");
    assert!(re.is_nullable(), "__range_end should be nullable");
    if let ArrowDataType::Struct(re_fields) = re.data_type() {
        assert_eq!(
            re_fields.len(),
            2,
            "__range_end struct should have 2 fields"
        );
    } else {
        panic!("__range_end should be a Struct");
    }
}

// ------------------------------------------------------------------
// Collection column: `replaced` field present ONLY for collections
// ------------------------------------------------------------------

#[test]
fn collection_column_has_replaced_field() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "with_collection".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "tags".to_string(),
                data_type: "set<text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let schema = derive_delta_schema(&table, &DeltaSchemaOpts::default()).expect("derive failed");

    // `tags` is a set — should have `replaced`.
    let tags = schema.field_with_name("tags").expect("no tags field");
    if let ArrowDataType::Struct(fields) = tags.data_type() {
        assert_eq!(
            fields.len(),
            4,
            "set column struct should have 4 fields (incl. replaced)"
        );
        assert!(
            fields.find("replaced").is_some(),
            "set column should have `replaced` field"
        );
    } else {
        panic!("tags should be Struct");
    }

    // `name` is text — should NOT have `replaced`.
    let name = schema.field_with_name("name").expect("no name field");
    if let ArrowDataType::Struct(fields) = name.data_type() {
        assert_eq!(
            fields.len(),
            3,
            "scalar column struct should have 3 fields (no replaced)"
        );
        assert!(
            fields.find("replaced").is_none(),
            "scalar column should NOT have `replaced`"
        );
    } else {
        panic!("name should be Struct");
    }
}

// ------------------------------------------------------------------
// frozen<list<text>> is NOT a collection (frozen = scalar)
// ------------------------------------------------------------------

#[test]
fn frozen_collection_is_scalar_no_replaced() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "frozen_test".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "frozen_list".to_string(),
                data_type: "frozen<list<text>>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let schema = derive_delta_schema(&table, &DeltaSchemaOpts::default()).expect("derive failed");

    let frozen_col = schema
        .field_with_name("frozen_list")
        .expect("no frozen_list field");
    if let ArrowDataType::Struct(fields) = frozen_col.data_type() {
        assert_eq!(
            fields.len(),
            3,
            "frozen<list> should be treated as scalar: 3 fields"
        );
        assert!(
            fields.find("replaced").is_none(),
            "frozen<list> should NOT have `replaced`"
        );
    } else {
        panic!("frozen_list should be Struct");
    }
}

// ------------------------------------------------------------------
// Collision detection: __op column → error
// ------------------------------------------------------------------

#[test]
fn column_collision_default_prefix() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "bad".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            // Legal CQL column name that collides with the envelope.
            Column {
                name: "__op".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let err = derive_delta_schema(&table, &DeltaSchemaOpts::default())
        .expect_err("expected collision error");

    match err {
        DeltaSchemaError::ColumnCollision { column, reserved } => {
            assert_eq!(column, "__op");
            assert_eq!(reserved, "__op");
        }
        other => panic!("expected ColumnCollision, got {:?}", other),
    }
}

#[test]
fn column_collision_ts_column() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "bad_ts".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "__ts".to_string(),
                data_type: "bigint".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let err = derive_delta_schema(&table, &DeltaSchemaOpts::default())
        .expect_err("expected collision error");

    assert!(
        matches!(err, DeltaSchemaError::ColumnCollision { .. }),
        "expected ColumnCollision"
    );
}

// ------------------------------------------------------------------
// Collision detection: key columns (partition / clustering) named __op
// ------------------------------------------------------------------

/// A partition key column named `__op` must trigger `ColumnCollision`.
///
/// Before the fix, the check only iterated `table.columns`, so a key column
/// with a reserved name escaped detection and produced a malformed Arrow
/// `Schema` with two fields named `__op`.
#[test]
fn partition_key_collision_default_prefix() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "pk_collision".to_string(),
        partition_keys: vec![KeyColumn {
            // Partition key named after the envelope discriminator.
            name: "__op".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        // `table.columns` deliberately does NOT contain a column named `__op`
        // (simulating a schema where keys are not duplicated in the columns
        // list), so the pre-fix check over `table.columns` alone would have
        // missed this collision entirely.
        columns: vec![Column {
            name: "__op".to_string(),
            data_type: "int".to_string(),
            nullable: false,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let err = derive_delta_schema(&table, &DeltaSchemaOpts::default())
        .expect_err("expected ColumnCollision for partition key named __op");

    match err {
        DeltaSchemaError::ColumnCollision { column, reserved } => {
            assert_eq!(column, "__op");
            assert_eq!(reserved, "__op");
        }
        other => panic!("expected ColumnCollision, got {:?}", other),
    }
}

/// A clustering key column named `__ts` must trigger `ColumnCollision`.
#[test]
fn clustering_key_collision_default_prefix() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "ck_collision".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            // Clustering key named after the envelope timestamp column.
            name: "__ts".to_string(),
            data_type: "text".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "__ts".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let err = derive_delta_schema(&table, &DeltaSchemaOpts::default())
        .expect_err("expected ColumnCollision for clustering key named __ts");

    match err {
        DeltaSchemaError::ColumnCollision { column, reserved } => {
            assert_eq!(column, "__ts");
            assert_eq!(reserved, "__ts");
        }
        other => panic!("expected ColumnCollision, got {:?}", other),
    }
}

// ------------------------------------------------------------------
// Collision detection: clustering key named `inclusive`
//
// The range-bound struct (`__range_start`/`__range_end`) appends a
// hardcoded `inclusive: bool` sub-field after one field per clustering
// column.  A clustering column literally named `inclusive` therefore
// produces two `inclusive` fields inside that struct.  This must fail at
// derivation time (before any output file is created), not late at
// `StructArray::try_new`.
// ------------------------------------------------------------------

/// A clustering key column named `inclusive` must trigger `ColumnCollision`
/// at derivation time, before any output bytes are produced.
#[test]
fn clustering_key_named_inclusive_collision() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "inclusive_ck".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            // Clustering key named after the range-bound reserved sub-field.
            name: "inclusive".to_string(),
            data_type: "text".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "inclusive".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let err = derive_delta_schema(&table, &DeltaSchemaOpts::default())
        .expect_err("expected ColumnCollision for clustering key named `inclusive`");

    match err {
        DeltaSchemaError::ColumnCollision { column, reserved } => {
            assert_eq!(column, "inclusive");
            assert_eq!(reserved, "inclusive");
        }
        other => panic!("expected ColumnCollision, got {:?}", other),
    }
}

/// A custom envelope prefix does NOT rename the range-bound `inclusive`
/// sub-field (it is hardcoded, not prefixed), so the collision must still
/// fire even under a custom prefix.
#[test]
fn clustering_key_named_inclusive_collision_custom_prefix() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "inclusive_ck_prefixed".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "inclusive".to_string(),
            data_type: "text".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "inclusive".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let opts = DeltaSchemaOpts::with_prefix("_cqlite_");
    let err = derive_delta_schema(&table, &opts)
        .expect_err("expected ColumnCollision for clustering key named `inclusive`");
    assert!(
        matches!(err, DeltaSchemaError::ColumnCollision { ref reserved, .. } if reserved == "inclusive"),
        "expected ColumnCollision on `inclusive`, got {:?}",
        err
    );
}

/// A NON-clustering (regular / partition-key) column named `inclusive` is
/// harmless — it never enters the range-bound struct — so it must NOT be
/// rejected.
#[test]
fn regular_column_named_inclusive_is_allowed() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "inclusive_regular".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "text".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            // Regular column named `inclusive` — not a clustering key,
            // so it never appears in the range-bound struct.
            Column {
                name: "inclusive".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let schema = derive_delta_schema(&table, &DeltaSchemaOpts::default())
        .expect("regular column named `inclusive` should be allowed");
    // The regular column is emitted as a cell struct.
    let inc = schema
        .field_with_name("inclusive")
        .expect("no inclusive field");
    assert!(
        matches!(inc.data_type(), ArrowDataType::Struct(_)),
        "regular `inclusive` column should be a cell Struct"
    );
}

// ------------------------------------------------------------------
// Configurable envelope_prefix changes the reserved names
// ------------------------------------------------------------------

#[test]
fn custom_prefix_avoids_collision() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "custom_prefix".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            // Column named `__op` — collides with default prefix but not
            // with the custom `_cqlite_` prefix.
            Column {
                name: "__op".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    // Default prefix → collision.
    assert!(derive_delta_schema(&table, &DeltaSchemaOpts::default()).is_err());

    // Custom prefix → success; envelope col is `_cqlite_op`, not `__op`.
    let opts = DeltaSchemaOpts::with_prefix("_cqlite_");
    let schema = derive_delta_schema(&table, &opts).expect("should succeed with custom prefix");

    // User column `__op` is present as a cell struct.
    let op_col = schema.field_with_name("__op").expect("no __op field");
    assert!(
        matches!(op_col.data_type(), ArrowDataType::Struct(_)),
        "__op should be a cell Struct under custom prefix"
    );

    // Envelope column is `_cqlite_op` (dictionary-encoded Utf8).
    let cqlite_op = schema
        .field_with_name("_cqlite_op")
        .expect("no _cqlite_op field");
    assert!(
        matches!(cqlite_op.data_type(), ArrowDataType::Dictionary(..)),
        "_cqlite_op should be Dictionary-encoded"
    );
}

// ------------------------------------------------------------------
// Custom prefix also changes the error message
// ------------------------------------------------------------------

#[test]
fn custom_prefix_collision_error_names_custom_reserved() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            // Column named `_cqlite_op` — collides with the custom prefix.
            Column {
                name: "_cqlite_op".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let opts = DeltaSchemaOpts::with_prefix("_cqlite_");
    let err = derive_delta_schema(&table, &opts).expect_err("expected collision");
    match err {
        DeltaSchemaError::ColumnCollision { column, reserved } => {
            assert_eq!(column, "_cqlite_op");
            assert_eq!(reserved, "_cqlite_op");
        }
        other => panic!("expected ColumnCollision, got {:?}", other),
    }
}

// ------------------------------------------------------------------
// Counter table rejection
// ------------------------------------------------------------------

#[test]
fn counter_table_rejected() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "counters".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "views".to_string(),
                data_type: "counter".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let err = derive_delta_schema(&table, &DeltaSchemaOpts::default())
        .expect_err("expected counter error");

    match &err {
        DeltaSchemaError::CounterTable {
            keyspace,
            table: tbl,
            columns,
        } => {
            assert_eq!(keyspace, "ks");
            assert_eq!(tbl, "counters");
            assert!(columns.contains("views"), "error should name 'views'");
        }
        other => panic!("expected CounterTable, got {:?}", other),
    }

    // Error message should be descriptive.
    let msg = err.to_string();
    assert!(
        msg.contains("counter"),
        "message should mention counter: {}",
        msg
    );
    assert!(
        msg.contains("ks.counters"),
        "message should name the table: {}",
        msg
    );
}

// ------------------------------------------------------------------
// Value types come from the #673 mapping (no duplicated mapping code)
// ------------------------------------------------------------------

#[test]
fn value_types_from_673_mapping() {
    // Verify that the Arrow type assigned to the `value` field of each cell
    // struct matches what `cql_type_to_arrow_data_type` returns for the
    // same CQL type.  This is the proof that we are reusing the #673 mapping
    // rather than forking a second CQL→Arrow mapping.
    let types_under_test = vec![
        ("bigint", CqlType::BigInt),
        ("boolean", CqlType::Boolean),
        ("float", CqlType::Float),
        ("double", CqlType::Double),
        ("uuid", CqlType::Uuid),
        ("timeuuid", CqlType::TimeUuid),
        ("timestamp", CqlType::Timestamp),
        ("date", CqlType::Date),
        ("time", CqlType::Time),
        ("blob", CqlType::Blob),
        ("inet", CqlType::Inet),
        ("decimal", CqlType::Decimal),
        ("varint", CqlType::Varint),
    ];

    for (type_str, expected_cql_type) in types_under_test {
        let table = TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            partition_keys: vec![KeyColumn {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "pk".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "col".to_string(),
                    data_type: type_str.to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let schema = derive_delta_schema(&table, &DeltaSchemaOpts::default())
            .unwrap_or_else(|e| panic!("derive failed for {}: {:?}", type_str, e));

        let col_field = schema.field_with_name("col").expect("no col field");
        if let ArrowDataType::Struct(struct_fields) = col_field.data_type() {
            let value_field = struct_fields
                .find("value")
                .expect("no value field in struct");
            let expected_arrow_type = cql_type_to_arrow_data_type(&expected_cql_type);
            assert_eq!(
                *value_field.1.data_type(),
                expected_arrow_type,
                "value Arrow type mismatch for CQL type '{}': expected {:?}, got {:?}",
                type_str,
                expected_arrow_type,
                value_field.1.data_type()
            );
        } else {
            panic!("col should be Struct for type {}", type_str);
        }
    }
}

// ------------------------------------------------------------------
// No-clustering-key table: range bounds have only `inclusive`
// ------------------------------------------------------------------

#[test]
fn no_clustering_keys_range_bound_has_only_inclusive() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "no_ck".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            nullable: false,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let schema = derive_delta_schema(&table, &DeltaSchemaOpts::default()).expect("derive failed");

    let rs = schema
        .field_with_name("__range_start")
        .expect("no __range_start");
    if let ArrowDataType::Struct(fields) = rs.data_type() {
        assert_eq!(
            fields.len(),
            1,
            "no-CK range bound should only have `inclusive`"
        );
        assert!(fields.find("inclusive").is_some());
    } else {
        panic!("__range_start should be Struct");
    }
}

// ------------------------------------------------------------------
// Multiple clustering keys: range bound has typed fields + inclusive
// ------------------------------------------------------------------

#[test]
fn multi_ck_range_bound_has_all_ck_fields() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "multi_ck".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![
            ClusteringColumn {
                name: "year".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: ClusteringOrder::Asc,
            },
            ClusteringColumn {
                name: "month".to_string(),
                data_type: "int".to_string(),
                position: 1,
                order: ClusteringOrder::Asc,
            },
        ],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "year".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "month".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let schema = derive_delta_schema(&table, &DeltaSchemaOpts::default()).expect("derive failed");

    let rs = schema
        .field_with_name("__range_start")
        .expect("no __range_start");
    if let ArrowDataType::Struct(fields) = rs.data_type() {
        // year + month + inclusive = 3 fields
        assert_eq!(fields.len(), 3);
        assert!(fields.find("year").is_some());
        assert!(fields.find("month").is_some());
        assert!(fields.find("inclusive").is_some());
        // Clustering fields are nullable (prefix bounds).
        let year_f = fields.find("year").unwrap();
        assert!(
            year_f.1.is_nullable(),
            "year in range bound should be nullable"
        );
    } else {
        panic!("__range_start should be Struct");
    }
}

// ------------------------------------------------------------------
// Map column has `replaced` field
// ------------------------------------------------------------------

#[test]
fn map_column_has_replaced_field() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "with_map".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "attrs".to_string(),
                data_type: "map<text, text>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let schema = derive_delta_schema(&table, &DeltaSchemaOpts::default()).expect("derive failed");
    let attrs = schema.field_with_name("attrs").expect("no attrs field");
    if let ArrowDataType::Struct(fields) = attrs.data_type() {
        assert_eq!(
            fields.len(),
            4,
            "map column should have 4 fields (incl. replaced)"
        );
        assert!(fields.find("replaced").is_some());
    } else {
        panic!("attrs should be Struct");
    }
}

// ------------------------------------------------------------------
// List column has `replaced` field
// ------------------------------------------------------------------

#[test]
fn list_column_has_replaced_field() {
    let table = TableSchema {
        keyspace: "ks".to_string(),
        table: "with_list".to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "events".to_string(),
                data_type: "list<bigint>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let schema = derive_delta_schema(&table, &DeltaSchemaOpts::default()).expect("derive failed");
    let events = schema.field_with_name("events").expect("no events field");
    if let ArrowDataType::Struct(fields) = events.data_type() {
        assert_eq!(
            fields.len(),
            4,
            "list column should have 4 fields (incl. replaced)"
        );
        assert!(fields.find("replaced").is_some());
    } else {
        panic!("events should be Struct");
    }
}
