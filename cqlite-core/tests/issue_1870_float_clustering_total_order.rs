//! Read/write agreement for the Cassandra/Java float total order on CLUSTERING
//! columns (issues #1870, #2010).
//!
//! Cassandra orders `float`/`double` clustering values with Java
//! `Double.compare`/`Float.compare`: **NaN sorts last** (every NaN bit-pattern
//! equal) and **`-0.0 < +0.0`**. This is a TOTAL order — required because the
//! writer's [`ClusteringKey`] is an `Ord` key (memtable `BTreeMap` placement +
//! compaction merge). A non-total order (IEEE `partial_cmp`) would let NaN
//! compare `Equal` to everything (transitivity violation → mis-placed/dropped
//! rows) and collapse `-0.0`/`+0.0`.
//!
//! Three comparators MUST agree for the same clustering values:
//!   1. `ClusteringKey::compare` (schema-aware write/compaction path),
//!   2. `ClusteringKey`'s `Ord::cmp` (schema-less memtable BTreeMap path),
//!   3. `ComparatorType::compare` for `Float`/`Float32` (schema-aware READ hot
//!      path), which must in turn agree with the reader's clustering-bound
//!      check via `Value::partial_cmp`.
//!
//! Before the fix, sites (1)/(2) and the `ComparatorType` read path used IEEE
//! `partial_cmp(..).unwrap_or(Equal)` (NaN → Equal, `-0.0 == +0.0`), so these
//! assertions failed; after routing them through `crate::float_cmp` they pass.

use std::cmp::Ordering;

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, TableSchema};
use cqlite_core::storage::write_engine::ClusteringKey;
use cqlite_core::types::{ComparatorType, Value};

/// A negative quiet NaN (`total_cmp` would sort this FIRST; Java sorts it last).
fn neg_nan_f64() -> f64 {
    f64::from_bits(0xFFF8_0000_0000_0000)
}
fn neg_nan_f32() -> f32 {
    f32::from_bits(0xFFC0_0000)
}

/// Single-`double`-clustering-column schema (ASC unless `desc`).
fn double_schema(desc: bool) -> TableSchema {
    single_clustering_schema("double", desc)
}
fn float_schema(desc: bool) -> TableSchema {
    single_clustering_schema("float", desc)
}
fn single_clustering_schema(data_type: &str, desc: bool) -> TableSchema {
    TableSchema {
        keyspace: "ks".to_string(),
        table: "t".to_string(),
        partition_keys: vec![],
        clustering_keys: vec![ClusteringColumn {
            name: "c".to_string(),
            data_type: data_type.to_string(),
            position: 0,
            order: if desc {
                ClusteringOrder::Desc
            } else {
                ClusteringOrder::Asc
            },
        }],
        columns: vec![],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn ck(v: Value) -> ClusteringKey {
    ClusteringKey::single("c", v)
}

// -------------------------------------------------------------------------
// (1) ClusteringKey::compare (schema-aware write/compaction path).
// -------------------------------------------------------------------------

#[test]
fn clustering_compare_double_nan_last_and_signed_zero() {
    let schema = double_schema(false);
    let nan = ck(Value::Float(neg_nan_f64()));
    let inf = ck(Value::Float(f64::INFINITY));
    let neg_zero = ck(Value::Float(-0.0));
    let pos_zero = ck(Value::Float(0.0));

    // NaN sorts after +Infinity (last).
    assert_eq!(nan.compare(&inf, &schema).unwrap(), Ordering::Greater);
    assert_eq!(inf.compare(&nan, &schema).unwrap(), Ordering::Less);
    // Two NaNs (any bit-pattern) are Equal.
    let nan2 = ck(Value::Float(f64::NAN));
    assert_eq!(nan.compare(&nan2, &schema).unwrap(), Ordering::Equal);
    // -0.0 < +0.0 (distinct, not IEEE-Equal).
    assert_eq!(
        neg_zero.compare(&pos_zero, &schema).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        pos_zero.compare(&neg_zero, &schema).unwrap(),
        Ordering::Greater
    );
}

#[test]
fn clustering_compare_float32_nan_last_and_signed_zero() {
    let schema = float_schema(false);
    let nan = ck(Value::Float32(neg_nan_f32()));
    let inf = ck(Value::Float32(f32::INFINITY));
    let neg_zero = ck(Value::Float32(-0.0));
    let pos_zero = ck(Value::Float32(0.0));

    assert_eq!(nan.compare(&inf, &schema).unwrap(), Ordering::Greater);
    assert_eq!(
        neg_zero.compare(&pos_zero, &schema).unwrap(),
        Ordering::Less
    );
    let nan2 = ck(Value::Float32(f32::NAN));
    assert_eq!(nan.compare(&nan2, &schema).unwrap(), Ordering::Equal);
}

/// DESC reverses the total order (NaN sorts FIRST under DESC).
#[test]
fn clustering_compare_double_desc_reverses() {
    let schema = double_schema(true);
    let nan = ck(Value::Float(f64::NAN));
    let inf = ck(Value::Float(f64::INFINITY));
    assert_eq!(nan.compare(&inf, &schema).unwrap(), Ordering::Less);
    let neg_zero = ck(Value::Float(-0.0));
    let pos_zero = ck(Value::Float(0.0));
    assert_eq!(
        neg_zero.compare(&pos_zero, &schema).unwrap(),
        Ordering::Greater
    );
}

/// The headline oracle: sorting mixed doubles via `ClusteringKey::compare`
/// yields `[-Inf, -0.0, +0.0, 1.0, +Inf, NaN]`.
#[test]
fn clustering_keys_sort_matches_oracle_double() {
    let schema = double_schema(false);
    let mut v = vec![
        ck(Value::Float(1.0)),
        ck(Value::Float(neg_nan_f64())),
        ck(Value::Float(-0.0)),
        ck(Value::Float(0.0)),
        ck(Value::Float(f64::NEG_INFINITY)),
        ck(Value::Float(f64::INFINITY)),
    ];
    v.sort_by(|a, b| a.compare(b, &schema).unwrap());
    let got: Vec<Value> = v.into_iter().map(|k| k.columns[0].1.clone()).collect();
    assert_eq!(got[0], Value::Float(f64::NEG_INFINITY));
    assert!(matches!(got[1], Value::Float(f) if f == 0.0 && f.is_sign_negative()));
    assert!(matches!(got[2], Value::Float(f) if f == 0.0 && f.is_sign_positive()));
    assert_eq!(got[3], Value::Float(1.0));
    assert_eq!(got[4], Value::Float(f64::INFINITY));
    assert!(matches!(got[5], Value::Float(f) if f.is_nan()));
}

// -------------------------------------------------------------------------
// (2) ClusteringKey Ord::cmp (schema-less memtable BTreeMap path).
// -------------------------------------------------------------------------

#[test]
fn clustering_ord_is_total_over_nan_and_signed_zero() {
    let nan = ck(Value::Float(f64::NAN));
    let one = ck(Value::Float(1.0));
    let neg_zero = ck(Value::Float(-0.0));
    let pos_zero = ck(Value::Float(0.0));

    // NaN is the maximum (sorts last), not Equal-to-everything.
    assert_eq!(nan.cmp(&one), Ordering::Greater);
    assert_eq!(one.cmp(&nan), Ordering::Less);
    assert_eq!(nan.cmp(&ck(Value::Float(f64::NAN))), Ordering::Equal);
    // Signed zeros distinct.
    assert_eq!(neg_zero.cmp(&pos_zero), Ordering::Less);

    // Total-order sanity in a BTreeSet: all distinct-by-order values retained,
    // NaN present exactly once and last.
    use std::collections::BTreeSet;
    let set: BTreeSet<ClusteringKey> = [
        ck(Value::Float(f64::NAN)),
        ck(Value::Float(f64::NAN)),
        ck(Value::Float(-0.0)),
        ck(Value::Float(0.0)),
        ck(Value::Float(1.0)),
    ]
    .into_iter()
    .collect();
    // NaN==NaN collapses the two NaNs; -0.0 and +0.0 are distinct ⇒ 4 entries.
    assert_eq!(set.len(), 4);
    let last = set.iter().next_back().unwrap();
    assert!(matches!(last.columns[0].1, Value::Float(f) if f.is_nan()));
}

// -------------------------------------------------------------------------
// (3) ComparatorType read hot path + read/write agreement.
// -------------------------------------------------------------------------

#[test]
fn comparator_float_double_matches_java_total_order() {
    let c = ComparatorType::Float; // CQL `double`
    assert_eq!(
        c.compare(&Value::Float(f64::NAN), &Value::Float(f64::INFINITY))
            .unwrap(),
        Ordering::Greater
    );
    assert_eq!(
        c.compare(&Value::Float(-0.0), &Value::Float(0.0)).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        c.compare(&Value::Float(f64::NAN), &Value::Float(f64::NAN))
            .unwrap(),
        Ordering::Equal
    );
}

#[test]
fn comparator_float32_matches_java_total_order() {
    let c = ComparatorType::Float32; // CQL `float`
    assert_eq!(
        c.compare(
            &Value::Float32(neg_nan_f32()),
            &Value::Float32(f32::INFINITY)
        )
        .unwrap(),
        Ordering::Greater
    );
    assert_eq!(
        c.compare(&Value::Float32(-0.0), &Value::Float32(0.0))
            .unwrap(),
        Ordering::Less
    );
    assert_eq!(
        c.compare(&Value::Float32(f32::NAN), &Value::Float32(f32::NAN))
            .unwrap(),
        Ordering::Equal
    );
}

/// The core read/write-agreement guarantee: for every pair of `double`/`float`
/// clustering values, the writer's `ClusteringKey::compare` (ASC), the
/// schema-aware reader's `ComparatorType::compare`, and the reader's
/// clustering-bound check (`Value::partial_cmp`) all yield the SAME ordering.
#[test]
fn read_write_comparators_agree_on_nan_and_signed_zero() {
    let doubles = [
        f64::NEG_INFINITY,
        -1.0,
        -0.0,
        0.0,
        1.0,
        f64::INFINITY,
        f64::NAN,
        neg_nan_f64(),
    ];
    let schema = double_schema(false);
    let cmp = ComparatorType::Float;
    for &a in &doubles {
        for &b in &doubles {
            let va = Value::Float(a);
            let vb = Value::Float(b);
            let write_ord = ck(va.clone()).compare(&ck(vb.clone()), &schema).unwrap();
            let read_ord = cmp.compare(&va, &vb).unwrap();
            let bound_ord = va.partial_cmp(&vb).unwrap();
            assert_eq!(
                write_ord, read_ord,
                "write vs read disagree for ({a:?}, {b:?})"
            );
            assert_eq!(
                write_ord, bound_ord,
                "write vs bound-check disagree for ({a:?}, {b:?})"
            );
        }
    }

    // Same for `float` (f32).
    let floats = [
        f32::NEG_INFINITY,
        -1.0,
        -0.0,
        0.0,
        1.0,
        f32::INFINITY,
        f32::NAN,
        neg_nan_f32(),
    ];
    let fschema = float_schema(false);
    let fcmp = ComparatorType::Float32;
    for &a in &floats {
        for &b in &floats {
            let va = Value::Float32(a);
            let vb = Value::Float32(b);
            let write_ord = ck(va.clone()).compare(&ck(vb.clone()), &fschema).unwrap();
            let read_ord = fcmp.compare(&va, &vb).unwrap();
            let bound_ord = va.partial_cmp(&vb).unwrap();
            assert_eq!(write_ord, read_ord, "f32 write vs read ({a:?}, {b:?})");
            assert_eq!(write_ord, bound_ord, "f32 write vs bound ({a:?}, {b:?})");
        }
    }
}
