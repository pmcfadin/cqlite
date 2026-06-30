//! END-TO-END serialize-path evidence for the recursive collection-order
//! comparator (issue #1296). These tests drive the REAL `serialize_value` write
//! path (not the comparator in isolation) and decode the emitted bytes to prove
//! the canonical on-disk ordering: frozen sets/maps canonicalize (sort) their
//! elements/keys bottom-up, while frozen lists PRESERVE insertion order.
//!
//! Extracted from `scenarios_3` to keep that file under the campsite test-size
//! threshold (epic #1135) and to group the #1296 serialize coverage together.

#![allow(unused_imports)]

use super::super::*;
use super::support::*;
use crate::types::Value;

/// Read a big-endian i32 from `b` at byte offset `at` (test decode helper).
fn read_be_i32(b: &[u8], at: usize) -> i32 {
    i32::from_be_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]])
}

/// END-TO-END wiring evidence (issue #1296): the REAL serialize path must emit
/// RECURSIVELY canonically ordered bytes for `set<frozen<set<int>>>` — every inner
/// frozen set sorted by the element comparator AND the outer set ordered by the
/// now-canonical inner bytes — even when BOTH levels are provided UNSORTED. This
/// is the bottom-up guarantee: the outer ordering is computed against the SORTED
/// inner, matching the (sorted) inner bytes actually written. A signed negative
/// inner element (`-1`) also proves the inner sort reaches the signed int leaf,
/// not raw two's-complement bytes (where `0xFFFF_FFFF` would sort last).
#[test]
fn serialize_nested_frozen_set_orders_recursively_bottom_up() {
    // Inner sets AND outer set deliberately in NON-canonical order.
    let value = Value::Set(vec![
        Value::Frozen(Box::new(Value::Set(vec![
            Value::Integer(1),
            Value::Integer(3),
        ]))),
        Value::Frozen(Box::new(Value::Set(vec![
            Value::Integer(5),
            Value::Integer(-1),
        ]))),
        Value::Frozen(Box::new(Value::Set(vec![
            Value::Integer(2),
            Value::Integer(1),
        ]))),
    ]);
    let bytes = serialize_value(&value).unwrap();

    // Decode set<frozen<set<int>>> back into Vec<Vec<i32>>.
    let mut pos = 0usize;
    let outer_count = read_be_i32(&bytes, pos);
    pos += 4;
    let mut decoded: Vec<Vec<i32>> = Vec::new();
    for _ in 0..outer_count {
        let elem_len = read_be_i32(&bytes, pos) as usize;
        pos += 4;
        let elem_end = pos + elem_len;
        let inner_count = read_be_i32(&bytes, pos);
        pos += 4;
        let mut inner: Vec<i32> = Vec::new();
        for _ in 0..inner_count {
            let int_len = read_be_i32(&bytes, pos) as usize;
            pos += 4;
            assert_eq!(int_len, 4);
            inner.push(read_be_i32(&bytes, pos));
            pos += 4;
        }
        assert_eq!(pos, elem_end, "inner frozen set length mismatch");
        decoded.push(inner);
    }

    // Inner {2,1}->{1,2}, {5,-1}->{-1,5} (signed); outer ordered by canonical inner:
    // {-1,5} < {1,2} < {1,3}.
    assert_eq!(decoded, vec![vec![-1, 5], vec![1, 2], vec![1, 3]]);
    // Each inner is ascending (signed) and the outer sequence is non-decreasing.
    for inner in &decoded {
        let mut sorted = inner.clone();
        sorted.sort_unstable();
        assert_eq!(inner, &sorted, "inner frozen set not canonically sorted");
    }
    let mut sorted_outer = decoded.clone();
    sorted_outer.sort();
    assert_eq!(decoded, sorted_outer, "outer set not canonically ordered");
}

/// END-TO-END (issue #1296): `set<tuple<int,int>>` ordered through the REAL
/// serialize path by the TUPLE comparator (field-by-field, signed), even when the
/// elements are provided UNSORTED and include a negative leading field.
#[test]
fn serialize_set_of_tuples_orders_by_tuple_comparator() {
    let value = Value::Set(vec![
        Value::Tuple(vec![Value::Integer(2), Value::Integer(9)]),
        Value::Tuple(vec![Value::Integer(-1), Value::Integer(0)]),
        Value::Tuple(vec![Value::Integer(1), Value::Integer(5)]),
        Value::Tuple(vec![Value::Integer(1), Value::Integer(2)]),
    ]);
    let bytes = serialize_value(&value).unwrap();

    let mut pos = 0usize;
    let outer_count = read_be_i32(&bytes, pos);
    pos += 4;
    let mut decoded: Vec<(i32, i32)> = Vec::new();
    for _ in 0..outer_count {
        let elem_len = read_be_i32(&bytes, pos) as usize;
        pos += 4;
        let elem_end = pos + elem_len;
        // tuple<int,int>: two length-prefixed i32 fields, no count prefix.
        let f0_len = read_be_i32(&bytes, pos) as usize;
        pos += 4;
        assert_eq!(f0_len, 4);
        let f0 = read_be_i32(&bytes, pos);
        pos += 4;
        let f1_len = read_be_i32(&bytes, pos) as usize;
        pos += 4;
        assert_eq!(f1_len, 4);
        let f1 = read_be_i32(&bytes, pos);
        pos += 4;
        assert_eq!(pos, elem_end, "tuple element length mismatch");
        decoded.push((f0, f1));
    }

    // Field 0 decides (signed: -1 < 1 < 2); equal field 0 falls to field 1 (2 < 5).
    assert_eq!(decoded, vec![(-1, 0), (1, 2), (1, 5), (2, 9)]);
}

/// END-TO-END (issue #1296): a `frozen<list<int>>` serializes its elements in
/// INSERTION order — the write path must NOT sort list elements (unlike a frozen
/// set/map, which canonicalize). Descending input `[3,1,2]` must round-trip as
/// `[3,1,2]`, NOT the sorted `[1,2,3]`. If the write path sorted list elements
/// (as it does for sets), this would decode to `[1,2,3]` and FAIL.
#[test]
fn serialize_frozen_list_preserves_insertion_order_not_sorted() {
    let value = Value::Frozen(Box::new(Value::List(vec![
        Value::Integer(3),
        Value::Integer(1),
        Value::Integer(2),
    ])));
    let bytes = serialize_value(&value).unwrap();

    // Decode list<int>: count prefix, then each element [be_i32 len][be_i32 value].
    let mut pos = 0usize;
    let count = read_be_i32(&bytes, pos);
    pos += 4;
    let mut decoded: Vec<i32> = Vec::new();
    for _ in 0..count {
        let int_len = read_be_i32(&bytes, pos) as usize;
        pos += 4;
        assert_eq!(int_len, 4);
        decoded.push(read_be_i32(&bytes, pos));
        pos += 4;
    }
    assert_eq!(pos, bytes.len(), "list length mismatch");
    // Insertion order preserved — NOT sorted to [1,2,3].
    assert_eq!(decoded, vec![3, 1, 2]);
}
