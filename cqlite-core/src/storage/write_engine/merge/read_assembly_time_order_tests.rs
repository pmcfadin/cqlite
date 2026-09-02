//! Issue #3790 — merged-read collection ordering for `time` must agree with the
//! comparator (and therefore with the writer), including out-of-range negatives.
//!
//! Sibling file, not inline in `read_assembly.rs`, per the #1116 campsite rule —
//! that file is already over the source threshold, so a guard added inline would
//! red `file-size`.
//!
//! ## What this pins, and why it is not covered elsewhere
//!
//! `comparator_orders_by_raw_cell_path_bytes` used to include `time`, so merged
//! collection assembly sorted `time` element/map-key `cell_path`s by RAW UNSIGNED
//! BYTES. That was justified by "nanos are always non-negative, so byte order ==
//! numeric order" — a premise NOTHING enforces: no range check exists anywhere in
//! `cqlite-core/src`, decode is `map(be_i64, Value::Time)` and encode writes
//! `to_be_bytes()` verbatim, so a negative `Value::Time` is constructible, writable
//! and readable. For such a value the sign bit makes the leading byte `0xFF`, so
//! raw-byte order placed it ABOVE every non-negative value while
//! `ComparatorType::Custom("time")` (signed, matching `Value: PartialOrd` and hence
//! the writer/memtable) places it below. Merged reads and writes disagreed.
//!
//! #3790 removed `time` from that fast path. `inet` STAYS, because its shortcut is
//! unconditionally equivalent (`[u8]::cmp` over the very same serialized bytes).
//!
//! The in-range cases are pinned by the Cassandra-written golden
//! (`tests/issue_3790_collection_order_cassandra_golden.rs`), which cannot cover
//! this one: Cassandra's `TimeSerializer` validates `0..=86_399_999_999_999`, so no
//! Cassandra-written fixture can contain a negative `time`. This guard therefore
//! exercises the assembly path directly. Roborev jobs 46/47/49; range validation is
//! issue #3920.

use super::{assemble_read_cells, CellData};
use crate::schema::{Column, KeyColumn, TableSchema};
use crate::types::Value;
use crate::RowCells;
use std::collections::HashMap;

/// `RowCells` is an ordered slice of `(name, value)`, not a map — same helper the
/// inline tests in `read_assembly.rs` use.
fn get<'a>(cells: &'a RowCells, name: &str) -> Option<&'a Value> {
    cells
        .iter()
        .find(|(n, _)| n.as_ref() == name)
        .map(|(_, v)| v)
}

fn schema_with_time_keyed_collections() -> TableSchema {
    let col = |name: &str, ty: &str| Column {
        name: name.into(),
        data_type: ty.into(),
        nullable: true,
        default: None,
        is_static: false,
    };
    TableSchema {
        keyspace: "ks".into(),
        table: "t".into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: Vec::new(),
        columns: vec![
            col("id", "int"),
            col("tset", "set<time>"),
            col("tmap", "map<time, text>"),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// A multicell element whose `cell_path` is the serialized `time` (8-byte BE) — the
/// `cell_path` is what the assembler SORTS by, which is the property under test. The
/// element is also passed as the cell VALUE, mirroring the inline tests in
/// `read_assembly.rs`, because that is what the assembler surfaces for a set.
fn time_elem(column: &str, nanos: i64, value: Value) -> CellData {
    CellData {
        column: column.into(),
        value,
        timestamp: 1,
        ttl: None,
        cell_path: Some(nanos.to_be_bytes().to_vec()),
        local_deletion_time: None,
        is_complex_element: true,
        is_deleted: false,
        has_empty_value: false,
    }
}

fn assembled_time_paths(column: &str, cells: Vec<CellData>) -> Vec<i64> {
    let schema = schema_with_time_keyed_collections();
    let row = assemble_read_cells(cells, &schema, None).expect("assembly must succeed");
    let value =
        get(&row, column).unwrap_or_else(|| panic!("column {column} missing from assembled row"));
    // Recover the ORDER the assembler produced, as nanos.
    let items: &Vec<Value> = match value {
        Value::Set(v) | Value::List(v) => v,
        other => panic!("expected Set/List for {column}, got {other:?}"),
    };
    items
        .iter()
        .map(|v| match v {
            Value::Time(n) => *n,
            other => panic!("expected Value::Time element, got {other:?}"),
        })
        .collect()
}

/// The regression: a negative `time` set element must sort BELOW non-negatives,
/// matching the signed comparator and the writer. Under the removed raw-byte fast
/// path it sorted above them.
#[test]
fn negative_time_set_element_sorts_signed_not_by_raw_bytes() {
    let cells = vec![
        time_elem("tset", 0, Value::Time(0)),
        time_elem("tset", -1, Value::Time(-1)),
        time_elem("tset", 86_399_999_999_999, Value::Time(86_399_999_999_999)),
        time_elem("tset", -2, Value::Time(-2)),
    ];
    let got = assembled_time_paths("tset", cells);

    // Signed order. Raw UNSIGNED byte order would be [0, max, -2, -1] — the two
    // negatives last, because their leading byte is 0xFF.
    assert_eq!(
        got,
        vec![-2, -1, 0, 86_399_999_999_999],
        "merged-read set assembly must order time SIGNED (raw-byte order would put \
         the negatives last)"
    );
}

/// Same property for a `map<time, text>` key, which is the other ordering position.
#[test]
fn negative_time_map_key_sorts_signed_not_by_raw_bytes() {
    let cells = vec![
        time_elem("tmap", 10, Value::text("ten".to_string())),
        time_elem("tmap", -5, Value::text("neg".to_string())),
        time_elem("tmap", 0, Value::text("zero".to_string())),
    ];
    let schema = schema_with_time_keyed_collections();
    let row = assemble_read_cells(cells, &schema, None).expect("assembly must succeed");
    let value = get(&row, "tmap").expect("tmap missing");
    let pairs = match value {
        Value::Map(m) => m,
        other => panic!("expected Map, got {other:?}"),
    };
    let keys: Vec<i64> = pairs
        .iter()
        .map(|(k, _)| match k {
            Value::Time(n) => *n,
            other => panic!("expected Value::Time key, got {other:?}"),
        })
        .collect();
    assert_eq!(
        keys,
        vec![-5, 0, 10],
        "merged-read map assembly must order time keys SIGNED"
    );
}

/// In-range values are unaffected by the change — the property real data exercises,
/// and the one where signed and byte order coincide.
#[test]
fn in_range_time_order_is_unchanged() {
    let cells = vec![
        time_elem("tset", 86_399_999_999_999, Value::Time(86_399_999_999_999)),
        time_elem("tset", 0, Value::Time(0)),
        time_elem("tset", 45_296_000_000_007, Value::Time(45_296_000_000_007)),
        time_elem("tset", 3_600_000_000_000, Value::Time(3_600_000_000_000)),
    ];
    assert_eq!(
        assembled_time_paths("tset", cells),
        vec![0, 3_600_000_000_000, 45_296_000_000_007, 86_399_999_999_999]
    );
}
