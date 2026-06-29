//! Issue #1234 (P0, data loss) — a table whose ONLY regular column is a TOP-LEVEL
//! `frozen<UDT>` must not lose its partitions during compaction.
//!
//! ## The bug
//! `compact_sstables_with_registry` decoded a top-level `frozen<UserType(...)>`
//! regular column as if it were a NON-frozen, multi-cell per-field UDT (one cell
//! per field keyed by a 2-byte field-index cell_path). A frozen UDT is stored on
//! disk as a SINGLE frozen value cell, so the per-field decode found zero cells,
//! every row reconciled to empty, and the empty rows/partitions were purged →
//! an empty Data.db (`output_partitions=0`). Frozen collections (incl. ones
//! NESTING a frozen UDT) were already classified as single-value cells and so
//! survived — that is the passing contrast this test also pins.
//!
//! ## Discipline
//! This is a SELF-CONTAINED write-path regression: it drives CQLite's own
//! `WriteEngine` (write → flush TWO SSTables → `compact_sstables_with_registry`),
//! with the authoritative UDT metadata supplied via a `UdtRegistry`. It does NOT
//! depend on any Cassandra reference fixture or external dataset, so it runs in
//! CI without `CQLITE_DATASETS_ROOT`.
//!
//! Run:
//! ```bash
//! cargo test -p cqlite-core --features write-support \
//!   --test issue_1234_frozen_udt_compaction_dataloss -- --nocapture
//! ```

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::PathBuf;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::schema::{CqlType, UdtRegistry};
use cqlite_core::storage::write_engine::merge::{
    compact_sstables_with_registry, MergeStep, RowData,
};
use cqlite_core::storage::write_engine::{
    CellOperation, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::{UdtField, UdtTypeDef, UdtValue, Value};
use tempfile::TempDir;

const KS: &str = "frozen_udt_ks";

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(f)
}

// ── UDT registry (the authoritative metadata, #28) ──────────────────────────────

/// `person { name text, age int }`, plus `address { street text, zip int }` and
/// `employee { emp_name text, home frozen<address> }` (a frozen UDT NESTING a
/// frozen UDT). All registered in `KS`.
fn registry() -> UdtRegistry {
    let mut reg = UdtRegistry::new();
    reg.register_udt(
        UdtTypeDef::new(KS.to_string(), "person".to_string())
            .with_field("name".to_string(), CqlType::Text, true)
            .with_field("age".to_string(), CqlType::Int, true),
    );
    reg.register_udt(
        UdtTypeDef::new(KS.to_string(), "address".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("zip".to_string(), CqlType::Int, true),
    );
    reg.register_udt(
        UdtTypeDef::new(KS.to_string(), "employee".to_string())
            .with_field("emp_name".to_string(), CqlType::Text, true)
            .with_field(
                "home".to_string(),
                CqlType::Frozen(Box::new(CqlType::Udt(
                    "address".to_string(),
                    vec![
                        ("street".to_string(), CqlType::Text),
                        ("zip".to_string(), CqlType::Int),
                    ],
                ))),
                true,
            ),
    );
    reg
}

// ── Value builders ──────────────────────────────────────────────────────────────

fn person_value(name: &str, age: i32) -> Value {
    Value::Udt(UdtValue {
        type_name: "person".to_string(),
        keyspace: KS.to_string(),
        fields: vec![
            UdtField {
                name: "name".to_string(),
                value: Some(Value::Text(name.to_string())),
            },
            UdtField {
                name: "age".to_string(),
                value: Some(Value::Integer(age)),
            },
        ],
    })
}

fn address_value(street: &str, zip: i32) -> Value {
    Value::Udt(UdtValue {
        type_name: "address".to_string(),
        keyspace: KS.to_string(),
        fields: vec![
            UdtField {
                name: "street".to_string(),
                value: Some(Value::Text(street.to_string())),
            },
            UdtField {
                name: "zip".to_string(),
                value: Some(Value::Integer(zip)),
            },
        ],
    })
}

fn employee_value(name: &str, street: &str, zip: i32) -> Value {
    Value::Udt(UdtValue {
        type_name: "employee".to_string(),
        keyspace: KS.to_string(),
        fields: vec![
            UdtField {
                name: "emp_name".to_string(),
                value: Some(Value::Text(name.to_string())),
            },
            UdtField {
                name: "home".to_string(),
                value: Some(address_value(street, zip)),
            },
        ],
    })
}

// ── Schema builders ─────────────────────────────────────────────────────────────

fn col(name: &str, ty: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// `id int PRIMARY KEY, data <data_type>` — exactly ONE regular column.
fn single_regular_schema(table: &str, data_type: &str) -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: table.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![col("id", "int"), col("data", data_type)],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

// ── Harness: write two SSTables then compact via the real entry point ───────────

struct CompactOutcome {
    output_partitions: u64,
    output_rows: u64,
    /// Number of partitions in the compacted output whose `data` cell decodes to a
    /// NON-null value (data integrity: the frozen value was preserved, not blanked).
    output_data_cells: usize,
    _tmp: TempDir,
}

/// Write partitions `1..=half` into SSTable A and `half+1..=2*half` into SSTable B
/// through the `WriteEngine` (so each `data` cell goes through the real flush path),
/// then run `compact_sstables_with_registry` over both — the exact entry point the
/// bug lives in. `build_value(id)` yields the `data` column value for partition `id`.
fn write_two_then_compact(
    table: &str,
    data_type: &str,
    half: i32,
    build_value: impl Fn(i32) -> Value,
) -> CompactOutcome {
    let tmp = TempDir::new().expect("tempdir");
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    let schema = single_regular_schema(table, data_type);

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone())
        .with_udt_registry(registry());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    let table_id = TableId::new(KS, table);
    let write = |engine: &mut WriteEngine, id: i32, ts: i64| {
        let pk = PartitionKey::single("id", Value::Integer(id));
        let ops = vec![CellOperation::Write {
            column: "data".to_string(),
            value: build_value(id),
        }];
        engine
            .write(Mutation::new(table_id.clone(), pk, None, ops, ts, None))
            .expect("write mutation");
    };

    for id in 1..=half {
        write(&mut engine, id, 1_000);
    }
    let info_a = block_on(engine.flush())
        .expect("flush A")
        .expect("SSTable A info");
    assert_eq!(
        info_a.partition_count as i32, half,
        "SSTable A must hold {half} partitions"
    );

    for id in (half + 1)..=(2 * half) {
        write(&mut engine, id, 2_000);
    }
    let info_b = block_on(engine.flush())
        .expect("flush B")
        .expect("SSTable B info");
    assert_eq!(
        info_b.partition_count as i32, half,
        "SSTable B must hold {half} partitions"
    );

    let inputs: Vec<PathBuf> = vec![info_b.data_path.clone(), info_a.data_path.clone()];
    let out_dir = tmp.path().join("out");
    let report = block_on(compact_sstables_with_registry(
        inputs,
        &out_dir,
        &schema,
        12_340,
        None,
        None,
        true,
        Some(&registry()),
    ))
    .expect("compaction must succeed");

    // Read the compacted output back through a REGISTRY-AWARE merger (the #1234
    // entry point) and count how many partitions carry a non-null `data` cell.
    // This proves the frozen value was PRESERVED through compaction (not silently
    // blanked) — a stronger check than partition/row counts alone.
    let mut merger = KWayMerger::new_with_gc_and_registry(
        vec![report.output.data_path.clone()],
        &schema,
        None,
        None,
        Some(registry()),
    )
    .expect("registry-aware merger over compacted output");
    let mut output_data_cells = 0usize;
    loop {
        match merger.step().expect("merge step over compacted output") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for row in &rows {
                    if let RowData::Live { cells } = &row.row_data {
                        if cells
                            .iter()
                            .any(|c| c.column == "data" && !matches!(c.value, Value::Null))
                        {
                            output_data_cells += 1;
                        }
                    }
                }
            }
        }
    }

    CompactOutcome {
        output_partitions: report.stats.output_partitions,
        output_rows: report.stats.output_rows,
        output_data_cells,
        _tmp: tmp,
    }
}

// ════════════════════════════════════════════════════════════════════════════════
// FAILING CASES (before fix): top-level frozen<UDT> as the only regular column.
// ════════════════════════════════════════════════════════════════════════════════

/// **Primary repro** — `data frozen<person>`. On the buggy code the compaction
/// produced `output_partitions=0`. Every written partition must survive.
#[test]
fn top_level_frozen_udt_retains_all_partitions_after_compaction() {
    const HALF: i32 = 6;
    let out = write_two_then_compact("person_holder", "frozen<person>", HALF, |id| {
        person_value(&format!("person-{id}"), 20 + id)
    });

    assert_eq!(
        out.output_partitions,
        (2 * HALF) as u64,
        "DATA LOSS (#1234): a top-level frozen<person> column lost partitions during compaction \
         (output_partitions={}, expected {})",
        out.output_partitions,
        2 * HALF
    );
    assert_eq!(
        out.output_rows,
        (2 * HALF) as u64,
        "DATA LOSS (#1234): frozen<person> rows dropped (output_rows={}, expected {})",
        out.output_rows,
        2 * HALF
    );
    assert_eq!(
        out.output_data_cells,
        (2 * HALF) as usize,
        "DATA INTEGRITY (#1234): every compacted partition must carry its preserved \
         frozen<person> value (non-null data cells={}, expected {})",
        out.output_data_cells,
        2 * HALF
    );
}

/// Nested variant — `data frozen<employee>` (employee NESTS `frozen<address>`).
#[test]
fn nested_top_level_frozen_udt_retains_all_partitions_after_compaction() {
    const HALF: i32 = 5;
    let out = write_two_then_compact("employee_holder", "frozen<employee>", HALF, |id| {
        employee_value(&format!("emp-{id}"), &format!("{id} Main St"), 10_000 + id)
    });

    assert_eq!(
        out.output_partitions,
        (2 * HALF) as u64,
        "DATA LOSS (#1234): a top-level frozen<employee> (nested frozen<address>) column lost \
         partitions during compaction (output_partitions={}, expected {})",
        out.output_partitions,
        2 * HALF
    );
    assert_eq!(out.output_rows, (2 * HALF) as u64);
    assert_eq!(
        out.output_data_cells,
        (2 * HALF) as usize,
        "DATA INTEGRITY (#1234): every compacted partition must carry its preserved \
         frozen<employee> value (non-null data cells={}, expected {})",
        out.output_data_cells,
        2 * HALF
    );
}

// ════════════════════════════════════════════════════════════════════════════════
// PASSING CONTRAST: a frozen UDT nested inside a FROZEN COLLECTION already worked.
// Locks the boundary so a fix does not over-reach into the collection path.
// ════════════════════════════════════════════════════════════════════════════════

/// `data frozen<list<frozen<person>>>` — the frozen UDT appears only INSIDE a
/// frozen collection. This compacted correctly even on the buggy code; it must
/// keep doing so after the fix.
#[test]
fn frozen_udt_inside_frozen_collection_retains_all_partitions() {
    const HALF: i32 = 4;
    let out = write_two_then_compact(
        "people_list_holder",
        "frozen<list<frozen<person>>>",
        HALF,
        |id| {
            Value::List(vec![
                person_value(&format!("a-{id}"), id),
                person_value(&format!("b-{id}"), id + 100),
            ])
        },
    );

    assert_eq!(
        out.output_partitions,
        (2 * HALF) as u64,
        "contrast regression (#1234): frozen<list<frozen<person>>> must retain all partitions \
         (output_partitions={}, expected {})",
        out.output_partitions,
        2 * HALF
    );
    assert_eq!(out.output_rows, (2 * HALF) as u64);
    assert_eq!(
        out.output_data_cells,
        (2 * HALF) as usize,
        "contrast (#1234): frozen<list<frozen<person>>> data cells must survive (got {}, expected {})",
        out.output_data_cells,
        2 * HALF
    );
}
