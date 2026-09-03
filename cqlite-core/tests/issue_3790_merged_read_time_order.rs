//! Issue #3790 — merged-read collection ordering for `time` must match the ON-DISK
//! order, which is Cassandra's `BYTE_ORDER` (unsigned bytes), including for the
//! out-of-range negatives nothing validates.
//!
//! ## Why this test exists: it is a guard against a mistake that was actually made
//!
//! During #3790 the `time` comparator was changed to SIGNED and `time` was removed
//! from this path's raw-byte fast path, on the argument that the comparator should
//! agree with "the writer". **Both changes were wrong and were reverted.** The
//! argument verified the wrong writer: `Value::PartialOrd` is signed, but it does
//! NOT determine on-disk collection order — `data_writer/complex.rs` re-sorts every
//! non-list collection's cell paths through `schema_helpers::compare_cell_paths`
//! (`a.cmp(b)`, unsigned) immediately before writing, overriding whatever order the
//! memtable produced. So a signed read path REORDERS what the writer correctly wrote.
//!
//! The authority (never CQLite's own behaviour, #3041) — pinned `cassandra-5.0.8`,
//! `src/java/org/apache/cassandra/db/marshal/TimeType.java`:
//! `TimeType() { super(ComparisonType.BYTE_ORDER); }` — unsigned bytes of the 8-byte
//! big-endian nanos. CQLite's byte-parity-guarded writer already does exactly that.
//!
//! ## Why no fixture can cover this, and why that made it easy to get wrong twice
//!
//! Cassandra's `TimeSerializer` validates `0..=86_399_999_999_999`, so **no
//! Cassandra-written SSTable can contain a negative `time`** — the committed golden
//! (`issue_3790_collection_order_cassandra_golden.rs`) covers only in-range values,
//! where signed, unsigned and byte order all coincide. The entire disagreement lives
//! in values Cassandra cannot produce, so no observation settles it: the deciding
//! evidence has to be the RULE plus what our own writer does. This test encodes that
//! conclusion so it cannot be re-litigated by whichever review lands next.
//!
//! ## WHAT THIS DOES NOT EXERCISE — declared, not implied (roborev job 54)
//!
//! It calls `assemble_read_cells` on hand-built `CellData`, so it pins the
//! **merged-read assembly** order and **NOTHING ABOUT THE WRITER**. An earlier
//! revision named these cases `..._matching_the_writer`, which was an overclaim
//! twice over: the writer is never invoked here, and "the writer" is ambiguous
//! because CQLite has TWO collection write paths that DISAGREE for a negative
//! `time`:
//!
//! * per-element (`data_writer/complex.rs`, via `compare_cell_paths`) — unsigned
//!   raw cell-path bytes, which matches Cassandra and matches this test;
//! * whole-collection (`data_writer/complex.rs`, `write_complex_set` via
//!   `collection_order::compare_collection_elements`) — **signed** for
//!   `Value::Time`, and it emits cell paths in that order with no re-sort.
//!
//! So the whole-collection writer currently disagrees with Cassandra, with the
//! per-element writer, and with this test, for out-of-range negatives. That is a
//! pre-existing write-path parity defect, out of scope for a comparator fix
//! (on-disk byte ordering is compaction-parity territory) and filed as **#3935**.
//! An end-to-end write→read regression belongs there, where the rule is decided;
//! adding one here would pin one of two conflicting writer behaviours as correct
//! before that decision is made.
//!
//! ## What this catches, MEASURED — and what it does not
//!
//! Both halves of the reverted mistake were re-applied to check, rather than assumed:
//!
//! * comparator signed **and** `time` removed from the fast path (what actually
//!   happened): both negative cases **FAIL**. Caught.
//! * `time` removed from the fast path alone: **PASSES** — with the comparator
//!   unsigned the decode-and-compare route and the raw-byte route are equivalent,
//!   which is exactly the soundness condition that makes the fast path legitimate.
//! * comparator signed alone: **PASSES** — this path takes the raw-byte fast path and
//!   never consults the comparator, so a comparator-only change is INVISIBLE here.
//!   That half is guarded by the unit suite
//!   (`issue_3790_inet_time_value_ordering.rs::time_byte_order_places_negative_nanos_above_zero`).
//!
//! Stated because a test that quietly covers less than its name suggests is worse
//! than one that declares its scope: the two files are co-required, neither alone
//! pins the pair.
//!
//! Ungated on purpose (`write-support` is a DEFAULT feature), so `core-tests` in the
//! gate of record actually executes it — a `#![cfg(feature = "delta-scan")]`-style
//! gate would put it in a lane that runs nowhere (#3522).

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{assemble_read_cells, CellData};
use cqlite_core::types::Value;
use std::collections::HashMap;

fn schema() -> TableSchema {
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

/// `cell_path` is the serialized `time` (8-byte BE) — what the assembler sorts by.
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

fn column_of(row: &[(std::sync::Arc<str>, Value)], name: &str) -> Value {
    row.iter()
        .find(|(n, _)| n.as_ref() == name)
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| panic!("column {name} missing from assembled row"))
}

/// A negative `time` set element sorts ABOVE every non-negative one, because its sign
/// bit makes the leading byte `0xFF` and the order is UNSIGNED. Signed order would put
/// it first — that is the reverted mistake this pins.
#[test]
fn negative_time_set_element_sorts_by_unsigned_serialized_bytes() {
    let cells = vec![
        time_elem("tset", 0, Value::Time(0)),
        time_elem("tset", -1, Value::Time(-1)),
        time_elem("tset", 86_399_999_999_999, Value::Time(86_399_999_999_999)),
        time_elem("tset", -2, Value::Time(-2)),
    ];
    let row = assemble_read_cells(cells, &schema(), None).expect("assembly");
    let got: Vec<i64> = match column_of(&row, "tset") {
        Value::Set(v) | Value::List(v) => v
            .iter()
            .map(|x| match x {
                Value::Time(n) => *n,
                other => panic!("expected Value::Time, got {other:?}"),
            })
            .collect(),
        other => panic!("expected Set, got {other:?}"),
    };
    assert_eq!(
        got,
        vec![0, 86_399_999_999_999, -2, -1],
        "set<time> must order by UNSIGNED serialized bytes (negatives LAST, 0xFF \
         leading), matching Cassandra's TimeType BYTE_ORDER and the PER-ELEMENT \
         write path. Signed order would be [-2, -1, 0, max] — the reverted #3790 \
         mistake. NOTE the whole-collection writer is signed today (#3935); this \
         asserts the read side, not that writer."
    );
}

/// Same property for a `map<time, text>` key, the other ordering position.
#[test]
fn negative_time_map_key_sorts_by_unsigned_serialized_bytes() {
    let cells = vec![
        time_elem("tmap", 10, Value::text("ten".to_string())),
        time_elem("tmap", -5, Value::text("neg".to_string())),
        time_elem("tmap", 0, Value::text("zero".to_string())),
    ];
    let row = assemble_read_cells(cells, &schema(), None).expect("assembly");
    let keys: Vec<i64> = match column_of(&row, "tmap") {
        Value::Map(m) => m
            .iter()
            .map(|(k, _)| match k {
                Value::Time(n) => *n,
                other => panic!("expected Value::Time key, got {other:?}"),
            })
            .collect(),
        other => panic!("expected Map, got {other:?}"),
    };
    assert_eq!(
        keys,
        vec![0, 10, -5],
        "map<time, text> keys must order by UNSIGNED serialized bytes (negative last)"
    );
}

/// In-range values — the only ones real data contains, and where every candidate rule
/// agrees. Pinned so a future change to the negative case cannot quietly disturb them.
#[test]
fn in_range_time_order_is_ascending_by_nanos() {
    let cells = vec![
        time_elem("tset", 86_399_999_999_999, Value::Time(86_399_999_999_999)),
        time_elem("tset", 0, Value::Time(0)),
        time_elem("tset", 45_296_000_000_007, Value::Time(45_296_000_000_007)),
        time_elem("tset", 3_600_000_000_000, Value::Time(3_600_000_000_000)),
    ];
    let row = assemble_read_cells(cells, &schema(), None).expect("assembly");
    let got: Vec<i64> = match column_of(&row, "tset") {
        Value::Set(v) | Value::List(v) => v
            .iter()
            .map(|x| match x {
                Value::Time(n) => *n,
                other => panic!("expected Value::Time, got {other:?}"),
            })
            .collect(),
        other => panic!("expected Set, got {other:?}"),
    };
    assert_eq!(
        got,
        vec![0, 3_600_000_000_000, 45_296_000_000_007, 86_399_999_999_999]
    );
}
