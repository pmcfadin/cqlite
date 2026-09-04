//! Issue #3790 — merged-read collection ordering for `time` must match the ON-DISK
//! order, which is Cassandra's `BYTE_ORDER` (unsigned bytes), including for the
//! out-of-range negatives nothing validates.
//!
//! ## Why this test exists: it is a guard against a mistake that was actually made
//!
//! During #3790 the `time` comparator was changed to SIGNED and `time` was removed
//! from this path's raw-byte fast path, on the argument that the comparator should
//! agree with "the writer". **Both changes were wrong and were reverted.** The
//! argument verified the wrong writer: `Value::PartialOrd` was signed AT THE TIME
//! (#3935 has since made it BYTE_ORDER too), and it does NOT determine on-disk
//! collection order either way. **PRECISELY WHICH WRITER SORTS WHERE**
//! (an earlier revision of this paragraph said `complex.rs` re-sorts EVERY non-list
//! collection's cell paths, which is FALSE — only ONE of its two paths does):
//!
//! * PER-ELEMENT — `write_complex_column_per_element` sorts the supplied cells with
//!   `schema_helpers::compare_cell_paths` (`a.cmp(b)`, unsigned) immediately before
//!   writing, overriding whatever order the caller produced.
//! * WHOLE-COLUMN — `write_complex_column` does NOT re-sort cell paths; it orders the
//!   `Value`'s elements with `collection_order::compare_collection_elements` and emits
//!   the cell paths in THAT order. Which is why a signed `time` arm there wrote a
//!   non-Cassandra order for years (issue #3935) with no `compare_cell_paths` pass to
//!   catch it.
//!
//! Either way the on-disk order is unsigned/BYTE_ORDER, so a signed read path
//! REORDERS what the writer correctly wrote.
//!
//! The authority (never CQLite's own behaviour, #3041) — pinned `cassandra-5.0.8`,
//! `src/java/org/apache/cassandra/db/marshal/TimeType.java`:
//! `TimeType() { super(ComparisonType.BYTE_ORDER); }` — unsigned bytes of the 8-byte
//! big-endian nanos. CQLite's byte-parity-guarded writer already does exactly that.
//!
//! ## Why no fixture covers this
//!
//! **CORRECTED BY #3935 (measured against the pinned tag).** An earlier revision of
//! this comment said "Cassandra's `TimeSerializer` validates
//! `0..=86_399_999_999_999`, so no Cassandra-written SSTable can contain a negative
//! `time`". That is FALSE: Cassandra ACCEPTS, stores and `BYTE_ORDER`s an 8-byte
//! binary out-of-range `time`. The argument and its `TimeSerializer` citations are
//! written out ONCE, in `types::comparator::custom::compare_time` (`# CANONICAL
//! STATEMENT`); it is deliberately not restated here, so a future re-pin has one
//! paragraph to correct rather than four.
//!
//! The real reasons no fixture covers it: the committed golden
//! (`issue_3790_collection_order_cassandra_golden.rs`) holds only in-range values,
//! where signed, unsigned and byte order all coincide; and producing an out-of-range
//! one needs a binary-protocol write that bypasses the CQL string path, which no
//! committed generator does. The deciding evidence is therefore the RULE from the
//! pinned source. This test encodes that conclusion so it cannot be re-litigated by
//! whichever review lands next.
//!
//! ## WHAT THIS DOES NOT EXERCISE — declared, not implied (roborev job 54)
//!
//! It calls `assemble_read_cells` on hand-built `CellData`, so it pins the
//! **merged-read assembly** order and **NOTHING ABOUT THE WRITER**.
//!
//! Its two trailing `None` arguments are the projection filter (no projection —
//! assemble every column) and the UDT registry (issue #2339). The registry is
//! `None` because this fixture's schema declares only `set<time>` and
//! `map<time, text>`: no UDT reference to resolve, so a registry could not change
//! any ordering asserted below. A COMPOSITE element/key type WOULD need one and
//! fails closed without it. An earlier
//! revision named these cases `..._matching_the_writer`, which was an overclaim
//! twice over: the writer is never invoked here, and at the time "the writer" was
//! ambiguous, because CQLite had TWO collection write paths that DISAGREED for a
//! negative `time`:
//!
//! * per-element (`data_writer/complex.rs`, via `compare_cell_paths`) — unsigned
//!   raw cell-path bytes, which matches Cassandra and matches this test;
//! * whole-collection (`data_writer/complex.rs`, `write_set_complex_cells` /
//!   `write_map_complex_cells` via `collection_order::compare_collection_elements`)
//!   — **was signed** for `Value::Time`, emitting cell paths in that order with no
//!   re-sort.
//!
//! **#3935 FIXED that second path to `TimeType`'s BYTE_ORDER**, so both write paths
//! and this read path now agree for all inputs. The scope caveat above still stands
//! unchanged — this file still invokes no writer — and the write-side property is
//! pinned end to end by `issue_3935_collection_time_byte_order.rs`, which asserts
//! the two write paths emit the same on-disk order and that it is the rule-derived
//! one.
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
    let row = assemble_read_cells(cells, &schema(), None, None).expect("assembly");
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
         mistake. This asserts the READ side only; the whole-collection writer \
         was signed until #3935 corrected it, and the write side is pinned by \
         issue_3935_collection_time_byte_order.rs."
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
    let row = assemble_read_cells(cells, &schema(), None, None).expect("assembly");
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
    let row = assemble_read_cells(cells, &schema(), None, None).expect("assembly");
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
