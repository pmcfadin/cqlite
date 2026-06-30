//! Recursive comparators for COMPOSITE SET-element / MAP-key types (issue #1296).
//!
//! Follow-up to the scalar comparator of #1275: for composite element/key types
//! Cassandra does NOT compare raw serialized bytes — it recurses into each
//! field/element's own `AbstractType.compare`. This module mirrors those
//! `compareCustom` implementations exactly, recursing back into the shared
//! dispatcher [`super::compare_collection_elements`] so the SCALAR leaves reuse
//! the #1275 signed/float/varint/decimal/uuid logic (no reimplementation).
//!
//! Oracle: Cassandra 5.0 `org.apache.cassandra.db.marshal`
//!   * `TupleType.compareCustom` / `UserType` (extends `TupleType`):
//!     field-by-field in declared order. Each field is `[int32 len][bytes]` where
//!     `len < 0` is a null; a null field sorts BEFORE a non-null one; two nulls
//!     at the same position are equal and comparison continues. The first
//!     non-equal field decides. When one tuple runs out of fields first (a
//!     prefix), the shorter sorts first.
//!   * `SetType`/`ListType.compareCustom` (frozen): element-by-element using the
//!     element type's comparator, then the shorter collection sorts first.
//!   * `MapType.compareCustom` (frozen): per entry compare KEY then VALUE using
//!     their respective comparators, then the shorter map sorts first.
//!
//! The decision is driven ENTIRELY by the `Value` variant — which carries the
//! authoritative CQL type metadata (a `Value::Tuple`/`Value::Udt`/nested
//! `Value::Set`/`Value::List`/`Value::Map`) — never by inspecting raw bytes
//! (no-heuristics, issue #28). A composite whose element/field variant is unknown
//! or mixed bottoms out at the dispatcher's serialized-byte fallback.

use super::compare_collection_elements;
use crate::types::{UdtValue, Value};
use std::cmp::Ordering;

/// Is this field-slot value a CQL null for comparison purposes? Both the explicit
/// `Value::Null` placeholder (tuple slots) and a missing UDT field are nulls.
fn is_null(v: &Value) -> bool {
    matches!(v, Value::Null)
}

/// `TupleType.compareCustom`: compare two tuples field-by-field in positional
/// order. A null field sorts before a non-null one; equal-position nulls continue;
/// the first non-equal field decides; a shorter tuple that is a prefix sorts first.
pub(super) fn compare_tuple(a: &[Value], b: &[Value]) -> Ordering {
    let common = a.len().min(b.len());
    for i in 0..common {
        // `get` is always Some here (i < common <= len); fall back to Null to stay
        // panic-free without an unwrap if that invariant ever changes.
        let ea = a.get(i).unwrap_or(&Value::Null);
        let eb = b.get(i).unwrap_or(&Value::Null);
        match (is_null(ea), is_null(eb)) {
            (true, true) => continue,
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            (false, false) => match compare_collection_elements(ea, eb) {
                Ordering::Equal => continue,
                other => return other,
            },
        }
    }
    // Prefix rule: the tuple that ran out of fields first sorts first.
    a.len().cmp(&b.len())
}

/// `UserType.compareCustom` (UserType extends TupleType): compare two UDT values
/// field-by-field in DECLARED order. A missing field value (`None`) — or an
/// explicit `Value::Null` — is a null, sorting before a non-null; the first
/// non-equal field decides; a shorter field list that is a prefix sorts first.
pub(super) fn compare_udt(a: &UdtValue, b: &UdtValue) -> Ordering {
    let common = a.fields.len().min(b.fields.len());
    for i in 0..common {
        let va = a.fields.get(i).and_then(|f| f.value.as_ref());
        let vb = b.fields.get(i).and_then(|f| f.value.as_ref());
        // A `None` field value OR a `Some(Value::Null)` both count as null.
        let na = va.is_none_or(is_null);
        let nb = vb.is_none_or(is_null);
        match (na, nb) {
            (true, true) => continue,
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            (false, false) => {
                // Both non-null by the check above, so `va`/`vb` are `Some`.
                match (va, vb) {
                    (Some(x), Some(y)) => match compare_collection_elements(x, y) {
                        Ordering::Equal => continue,
                        other => return other,
                    },
                    // Unreachable given na/nb above; keep the sort total.
                    _ => continue,
                }
            }
        }
    }
    a.fields.len().cmp(&b.fields.len())
}

/// `SetType`/`ListType.compareCustom` (frozen): compare two collections
/// element-by-element using the element comparator, then the shorter sorts first.
/// A `frozen<set<T>>` arrives as a `Value::Set` (sorted at serialize time) and a
/// `frozen<list<T>>` as a `Value::List` (insertion order); both compare the same
/// way here.
pub(super) fn compare_list_or_set(a: &[Value], b: &[Value]) -> Ordering {
    let common = a.len().min(b.len());
    for i in 0..common {
        let ea = a.get(i).unwrap_or(&Value::Null);
        let eb = b.get(i).unwrap_or(&Value::Null);
        match compare_collection_elements(ea, eb) {
            Ordering::Equal => continue,
            other => return other,
        }
    }
    a.len().cmp(&b.len())
}

/// `MapType.compareCustom` (frozen): per entry compare KEY then VALUE with their
/// respective comparators, in stored (key-sorted) order; the shorter map first.
pub(super) fn compare_map(a: &[(Value, Value)], b: &[(Value, Value)]) -> Ordering {
    let common = a.len().min(b.len());
    for i in 0..common {
        let (ka, val_a) = match a.get(i) {
            Some(e) => (&e.0, &e.1),
            None => (&Value::Null, &Value::Null),
        };
        let (kb, val_b) = match b.get(i) {
            Some(e) => (&e.0, &e.1),
            None => (&Value::Null, &Value::Null),
        };
        match compare_collection_elements(ka, kb) {
            Ordering::Equal => {}
            other => return other,
        }
        match compare_collection_elements(val_a, val_b) {
            Ordering::Equal => continue,
            other => return other,
        }
    }
    a.len().cmp(&b.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{UdtField, UdtValue, Value};

    fn tup(fields: Vec<Value>) -> Value {
        Value::Tuple(fields)
    }

    /// `set<tuple<int>>`: tuples order by their single int FIELD via the signed
    /// Int32Type comparator reached through recursion — `tuple(-1)` sorts BEFORE
    /// `tuple(1)` BEFORE `tuple(2)`, even though `-1`'s raw two's-complement bytes
    /// (`0xFFFF_FFFF`) sort LAST. Proves the recursion reaches the #1275 scalar
    /// signed-integer leaf rather than falling back to raw bytes.
    #[test]
    fn tuple_int_field_sorts_signed_not_raw_bytes() {
        let mut v = vec![
            tup(vec![Value::Integer(2)]),
            tup(vec![Value::Integer(-1)]),
            tup(vec![Value::Integer(1)]),
        ];
        v.sort_by(compare_collection_elements);
        assert_eq!(
            v,
            vec![
                tup(vec![Value::Integer(-1)]),
                tup(vec![Value::Integer(1)]),
                tup(vec![Value::Integer(2)]),
            ]
        );
    }

    /// `tuple<int,text>`: field priority — the FIRST field decides; the second is
    /// only a tie-break. `(1,"z")` < `(2,"a")` because field 0 (1 < 2) wins
    /// regardless of field 1; `(1,"a")` < `(1,"b")` falls through to field 1.
    #[test]
    fn tuple_multi_field_priority() {
        let a = tup(vec![Value::Integer(1), Value::Text("z".into())]);
        let b = tup(vec![Value::Integer(2), Value::Text("a".into())]);
        assert_eq!(compare_collection_elements(&a, &b), Ordering::Less);

        let c = tup(vec![Value::Integer(1), Value::Text("a".into())]);
        let d = tup(vec![Value::Integer(1), Value::Text("b".into())]);
        assert_eq!(compare_collection_elements(&c, &d), Ordering::Less);
    }

    /// `TupleType` prefix rule: a tuple that is a strict prefix of another (ran
    /// out of fields first) sorts BEFORE it. `(1)` < `(1, 0)`.
    #[test]
    fn tuple_prefix_sorts_first() {
        let short = tup(vec![Value::Integer(1)]);
        let long = tup(vec![Value::Integer(1), Value::Integer(0)]);
        assert_eq!(compare_collection_elements(&short, &long), Ordering::Less);
        assert_eq!(
            compare_collection_elements(&long, &short),
            Ordering::Greater
        );
    }

    /// `TupleType` null rule: a null field (`Value::Null`) sorts BEFORE a non-null
    /// one at the same position; two nulls are equal and comparison continues.
    #[test]
    fn tuple_null_field_sorts_first() {
        let null_first = tup(vec![Value::Null, Value::Integer(9)]);
        let has_value = tup(vec![Value::Integer(-5), Value::Integer(0)]);
        assert_eq!(
            compare_collection_elements(&null_first, &has_value),
            Ordering::Less
        );
        // Equal nulls in field 0 → field 1 decides (3 < 4).
        let a = tup(vec![Value::Null, Value::Integer(3)]);
        let b = tup(vec![Value::Null, Value::Integer(4)]);
        assert_eq!(compare_collection_elements(&a, &b), Ordering::Less);
    }

    /// `map<tuple<int>, text>`: map KEYS order by the tuple comparator (recursing
    /// into the signed int leaf), so `tuple(-1)` keys sort before `tuple(1)`.
    #[test]
    fn map_tuple_keys_order_by_tuple_comparator() {
        let mut keys = vec![
            tup(vec![Value::Integer(1)]),
            tup(vec![Value::Integer(-1)]),
            tup(vec![Value::Integer(0)]),
        ];
        keys.sort_by(compare_collection_elements);
        assert_eq!(
            keys,
            vec![
                tup(vec![Value::Integer(-1)]),
                tup(vec![Value::Integer(0)]),
                tup(vec![Value::Integer(1)]),
            ]
        );
    }

    /// `set<frozen<set<int>>>`: elements order by the inner set's element
    /// comparator RECURSIVELY. `{-1}` < `{1}` (signed int leaf), and `{1}` < `{1,2}`
    /// (prefix/length tiebreak). The frozen wrapper unwraps to a `Value::Set`.
    #[test]
    fn nested_frozen_set_orders_recursively() {
        let s_neg1 = Value::Frozen(Box::new(Value::Set(vec![Value::Integer(-1)])));
        let s_1 = Value::Frozen(Box::new(Value::Set(vec![Value::Integer(1)])));
        let s_1_2 = Value::Frozen(Box::new(Value::Set(vec![
            Value::Integer(1),
            Value::Integer(2),
        ])));
        // {-1} < {1}: inner int leaf is signed, not raw bytes.
        assert_eq!(compare_collection_elements(&s_neg1, &s_1), Ordering::Less);
        // {1} < {1,2}: equal first element, shorter sorts first.
        assert_eq!(compare_collection_elements(&s_1, &s_1_2), Ordering::Less);

        let mut v = vec![s_1_2.clone(), s_1.clone(), s_neg1.clone()];
        v.sort_by(compare_collection_elements);
        assert_eq!(v, vec![s_neg1, s_1, s_1_2]);
    }

    /// `frozen<map<int,int>>` element comparison: per entry compare KEY then VALUE.
    /// Equal first key (1) → value decides (1 < 2); a smaller key wins outright.
    #[test]
    fn frozen_map_orders_key_then_value() {
        let m_1_1 = Value::Map(vec![(Value::Integer(1), Value::Integer(1))]);
        let m_1_2 = Value::Map(vec![(Value::Integer(1), Value::Integer(2))]);
        let m_neg1 = Value::Map(vec![(Value::Integer(-1), Value::Integer(9))]);
        // Equal key 1 → value 1 < 2.
        assert_eq!(compare_collection_elements(&m_1_1, &m_1_2), Ordering::Less);
        // Key -1 < 1 (signed) decides regardless of value.
        assert_eq!(compare_collection_elements(&m_neg1, &m_1_1), Ordering::Less);
    }

    /// UDT element: compare field-by-field in DECLARED order, each field by its own
    /// type comparator. Field 0 (int, signed) decides; equal field 0 falls to
    /// field 1 (text). `{a:-1,b:"z"}` < `{a:1,b:"a"}` on the signed int field.
    #[test]
    fn udt_orders_field_by_field_declared_order() {
        fn udt(a: i32, b: &str) -> Value {
            Value::Udt(UdtValue {
                type_name: "t".into(),
                keyspace: "ks".into(),
                fields: vec![
                    UdtField {
                        name: "a".into(),
                        value: Some(Value::Integer(a)),
                    },
                    UdtField {
                        name: "b".into(),
                        value: Some(Value::Text(b.into())),
                    },
                ],
            })
        }
        // Field 0 signed: -1 < 1 even though raw bytes of -1 are larger.
        assert_eq!(
            compare_collection_elements(&udt(-1, "z"), &udt(1, "a")),
            Ordering::Less
        );
        // Equal field 0 → field 1 text decides.
        assert_eq!(
            compare_collection_elements(&udt(5, "a"), &udt(5, "b")),
            Ordering::Less
        );
    }

    /// UDT null rule: a `None` field value sorts BEFORE a present one at the same
    /// position (UserType inherits TupleType's null-first rule).
    #[test]
    fn udt_null_field_sorts_first() {
        fn udt(a: Option<i32>) -> Value {
            Value::Udt(UdtValue {
                type_name: "t".into(),
                keyspace: "ks".into(),
                fields: vec![UdtField {
                    name: "a".into(),
                    value: a.map(Value::Integer),
                }],
            })
        }
        assert_eq!(
            compare_collection_elements(&udt(None), &udt(Some(-100))),
            Ordering::Less
        );
        assert_eq!(
            compare_collection_elements(&udt(Some(-100)), &udt(None)),
            Ordering::Greater
        );
    }
}
