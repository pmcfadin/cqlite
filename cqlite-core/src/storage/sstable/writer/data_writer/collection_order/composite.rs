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
//!   * `SetType.compareCustom` (frozen): a frozen set is stored element-SORTED, so
//!     we compare the SORTED element sequences (sort each side with the same
//!     recursive comparator FIRST), then the shorter collection sorts first. The
//!     sort-first step is what canonicalizes nested inner sets BOTTOM-UP so the
//!     outer ordering matches the (sorted) inner bytes actually written (#1296).
//!   * `ListType.compareCustom` (frozen): INSERTION order preserved — element-by-
//!     element with NO sort, then the shorter list sorts first.
//!   * `MapType.compareCustom` (frozen): a frozen map is stored KEY-SORTED, so we
//!     compare the KEY-SORTED entry sequences (sort each side by key FIRST), then
//!     per entry compare KEY then VALUE, then the shorter map sorts first.
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

/// Element-wise compare of two ALREADY-ORDERED ref sequences: the first non-equal
/// element decides; otherwise the shorter (prefix) sequence sorts first.
fn compare_ordered_seq(a: &[&Value], b: &[&Value]) -> Ordering {
    let common = a.len().min(b.len());
    for i in 0..common {
        // `i < common <= len` so `get` is always Some; fall back to Null to stay
        // panic-free without an unwrap if that invariant ever changes.
        let ea = a.get(i).copied().unwrap_or(&Value::Null);
        let eb = b.get(i).copied().unwrap_or(&Value::Null);
        match compare_collection_elements(ea, eb) {
            Ordering::Equal => continue,
            other => return other,
        }
    }
    a.len().cmp(&b.len())
}

/// `SetType.compareCustom` (frozen): a `frozen<set<T>>` serializes its elements in
/// the element-type comparator's order (see `encoding.rs` Set arm), so its
/// canonical on-disk form is element-SORTED. We therefore compare the SORTED
/// element sequences: sort each side with the SAME recursive comparator FIRST,
/// then element-by-element with a shorter-first tiebreak. Sorting first is what
/// makes nested frozen sets order correctly BOTTOM-UP — the inner set is
/// canonicalized before the outer set orders by it — even when the in-memory
/// `Value::Set` is provided with its elements in a non-canonical order (#1296).
pub(super) fn compare_set(a: &[Value], b: &[Value]) -> Ordering {
    let mut sa: Vec<&Value> = a.iter().collect();
    let mut sb: Vec<&Value> = b.iter().collect();
    // PERF (correctness-first, #1296): each pairwise call re-sorts both inner
    // sets, so an OUTER sort that compares this set O(N log N) times re-sorts the
    // same inner collections O(N log N) times — correct but redundant. A future
    // optimization is to canonicalize (sort) each element ONCE before the outer
    // sort; intentionally out of scope here (no behavioral change).
    sa.sort_by(|x, y| compare_collection_elements(x, y));
    sb.sort_by(|x, y| compare_collection_elements(x, y));
    compare_ordered_seq(&sa, &sb)
}

/// `ListType.compareCustom` (frozen): a `frozen<list<T>>` serializes its elements
/// in INSERTION order (no sort), so compare element-by-element in stored order
/// with a shorter-first tiebreak — NEVER sorted.
pub(super) fn compare_list(a: &[Value], b: &[Value]) -> Ordering {
    let sa: Vec<&Value> = a.iter().collect();
    let sb: Vec<&Value> = b.iter().collect();
    compare_ordered_seq(&sa, &sb)
}

/// `MapType.compareCustom` (frozen): a `frozen<map<K,V>>` serializes its entries in
/// the KEY-type comparator's order (see `encoding.rs` Map arm), so its canonical
/// on-disk form is KEY-SORTED. We compare the KEY-SORTED entry sequences: sort each
/// side's entries by key with the SAME recursive comparator FIRST, then per entry
/// compare KEY then VALUE, with a shorter-first tiebreak. Sorting first makes
/// nested frozen maps order correctly BOTTOM-UP even when the in-memory
/// `Value::Map` is provided with its entries in a non-canonical order (#1296).
pub(super) fn compare_map(a: &[(Value, Value)], b: &[(Value, Value)]) -> Ordering {
    let mut sa: Vec<&(Value, Value)> = a.iter().collect();
    let mut sb: Vec<&(Value, Value)> = b.iter().collect();
    // PERF (correctness-first, #1296): each pairwise call re-sorts both inner
    // maps by key, so an OUTER sort that compares this map O(N log N) times
    // re-sorts the same inner collections O(N log N) times — correct but
    // redundant. A future optimization is to canonicalize (key-sort) each map
    // ONCE before the outer sort; intentionally out of scope here (no behavioral
    // change).
    sa.sort_by(|x, y| compare_collection_elements(&x.0, &y.0));
    sb.sort_by(|x, y| compare_collection_elements(&x.0, &y.0));
    let common = sa.len().min(sb.len());
    for i in 0..common {
        let (ka, val_a) = match sa.get(i) {
            Some(e) => (&e.0, &e.1),
            None => (&Value::Null, &Value::Null),
        };
        let (kb, val_b) = match sb.get(i) {
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
    sa.len().cmp(&sb.len())
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

    /// `compare_map` length/prefix tiebreak (DIRECT): when one map's entries are a
    /// prefix of another's, the SHORTER map sorts first. `{1:1}` < `{1:1, 2:2}`.
    #[test]
    fn map_shorter_sorts_first_on_length_tiebreak() {
        let short = vec![(Value::Integer(1), Value::Integer(1))];
        let long = vec![
            (Value::Integer(1), Value::Integer(1)),
            (Value::Integer(2), Value::Integer(2)),
        ];
        assert_eq!(compare_map(&short, &long), Ordering::Less);
        assert_eq!(compare_map(&long, &short), Ordering::Greater);
        assert_eq!(compare_map(&short, &short), Ordering::Equal);
    }

    /// `map<int, frozen<list<int>>>`: the VALUE side must RECURSE into the dispatcher.
    /// With keys equal, the list values decide: `[-1]` < `[1]` orders by the SIGNED
    /// int leaf (not raw two's-complement bytes, where `0xFFFF_FFFF` would sort
    /// last), and `[1]` < `[1,2]` is the value-length (prefix) tiebreak.
    #[test]
    fn map_composite_value_recurses_into_dispatcher() {
        // Equal key 7 → value list decides by signed int: [-1] < [1].
        let v_neg = vec![(Value::Integer(7), Value::List(vec![Value::Integer(-1)]))];
        let v_pos = vec![(Value::Integer(7), Value::List(vec![Value::Integer(1)]))];
        assert_eq!(compare_map(&v_neg, &v_pos), Ordering::Less);
        assert_eq!(compare_map(&v_pos, &v_neg), Ordering::Greater);

        // Equal key 7 → value-length tiebreak: [1] is a prefix of [1,2] → shorter first.
        let v_one = vec![(Value::Integer(7), Value::List(vec![Value::Integer(1)]))];
        let v_one_two = vec![(
            Value::Integer(7),
            Value::List(vec![Value::Integer(1), Value::Integer(2)]),
        )];
        assert_eq!(compare_map(&v_one, &v_one_two), Ordering::Less);
    }

    /// Bottom-up canonicalization at the comparator level: `set<frozen<set<int>>>`
    /// whose inner sets are provided UNSORTED still orders the OUTER correctly,
    /// because `compare_set` canonicalizes (sorts) each inner set before comparing.
    /// Inner `{2,1}` canonicalizes to `{1,2}`; `{1,2}` < `{1,3}` on element 1.
    #[test]
    fn nested_frozen_set_canonicalizes_unsorted_inner_before_outer() {
        let s_2_1 = Value::Frozen(Box::new(Value::Set(vec![
            Value::Integer(2),
            Value::Integer(1),
        ])));
        let s_1_3 = Value::Frozen(Box::new(Value::Set(vec![
            Value::Integer(1),
            Value::Integer(3),
        ])));
        // Canonical inner: {1,2} vs {1,3} → element 1 decides (2 < 3).
        assert_eq!(compare_collection_elements(&s_2_1, &s_1_3), Ordering::Less);
        assert_eq!(
            compare_collection_elements(&s_1_3, &s_2_1),
            Ordering::Greater
        );
    }

    /// UDT element: compare field-by-field in DECLARED order, each field by its own
    /// type comparator. Field 0 (int, signed) decides; equal field 0 falls to
    /// field 1 (text). `{a:-1,b:"z"}` < `{a:1,b:"a"}` on the signed int field.
    #[test]
    fn udt_orders_field_by_field_declared_order() {
        fn udt(a: i32, b: &str) -> Value {
            Value::Udt(Box::new(UdtValue {
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
            }))
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
            Value::Udt(Box::new(UdtValue {
                type_name: "t".into(),
                keyspace: "ks".into(),
                fields: vec![UdtField {
                    name: "a".into(),
                    value: a.map(Value::Integer),
                }],
            }))
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

    /// `frozen<list<T>>` PRESERVES INSERTION ORDER (it is NOT sorted) — the most
    /// error-prone part of the contract: sets/maps canonicalize (sort) but lists
    /// do not. `[3,1]` vs `[1,3]` is decided POSITIONALLY by element 0 (3 vs 1) →
    /// `Greater`. If `compare_list` sorted (like a set), both would canonicalize to
    /// `[1,3]` and the result would be `Equal` — so a `Greater`/`Less` (non-equal)
    /// result is exactly what proves no sort happens. The SAME elements as a SET
    /// DO compare equal, pinning the list-vs-set distinction.
    #[test]
    fn frozen_list_preserves_insertion_order_not_sorted() {
        let l_3_1 = vec![Value::Integer(3), Value::Integer(1)];
        let l_1_3 = vec![Value::Integer(1), Value::Integer(3)];
        // List: positional — element 0 (3 vs 1) decides → Greater (NOT sorted).
        assert_eq!(compare_list(&l_3_1, &l_1_3), Ordering::Greater);
        assert_eq!(compare_list(&l_1_3, &l_3_1), Ordering::Less);
        // Via the dispatcher (Value::List arm) — same positional result.
        assert_eq!(
            compare_collection_elements(&Value::List(l_3_1.clone()), &Value::List(l_1_3.clone())),
            Ordering::Greater
        );
        // CONTRAST: the SAME elements as a SET canonicalize (sort) to {1,3} on both
        // sides → Equal. A list of the same elements is NOT equal, proving lists are
        // ordered element-wise positionally while sets are reordered.
        assert_eq!(
            compare_collection_elements(&Value::Set(l_3_1.clone()), &Value::Set(l_1_3.clone())),
            Ordering::Equal
        );
        assert_ne!(
            compare_collection_elements(&Value::List(l_3_1), &Value::List(l_1_3)),
            Ordering::Equal
        );
    }

    /// `frozen<list<T>>` compared ELEMENT-WISE POSITIONALLY (not reordered): when
    /// element 0 ties, element 1 decides in stored position. `[1,3]` < `[1,9]` on
    /// element 1 (3 < 9), and the prefix `[1]` < `[1,0]` (shorter-first tiebreak) —
    /// note `[1,0]` would sort to `[0,1]` and flip if lists were sorted, so the
    /// `[1] < [1,0]` result also confirms positional (un-sorted) comparison.
    #[test]
    fn frozen_list_compared_elementwise_positionally() {
        let l_1_3 = vec![Value::Integer(1), Value::Integer(3)];
        let l_1_9 = vec![Value::Integer(1), Value::Integer(9)];
        assert_eq!(compare_list(&l_1_3, &l_1_9), Ordering::Less);
        // Prefix/length tiebreak, positional: [1] < [1,0].
        let l_1 = vec![Value::Integer(1)];
        let l_1_0 = vec![Value::Integer(1), Value::Integer(0)];
        assert_eq!(compare_list(&l_1, &l_1_0), Ordering::Less);
        assert_eq!(compare_list(&l_1_0, &l_1), Ordering::Greater);
    }
}
