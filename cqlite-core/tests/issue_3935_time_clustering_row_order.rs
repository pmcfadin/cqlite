//! Issue #3935 — the LAST TWO `time` ordering sites: `Value::PartialOrd` and the
//! write engine's `compare_values`, the second of which decides the PHYSICAL ROW
//! ORDER written to `Data.db` for a `time` CLUSTERING COLUMN.
//!
//! # The defect
//!
//! `time` ordering was SPLIT across the tree. The read/comparator side
//! (`types::comparator::custom::compare_time`) and every collection-order write
//! site used unsigned big-endian byte order — `TimeType`'s rule — while two
//! sites still used signed `i64` comparison:
//!
//! * `Value::PartialOrd`'s `Time` arm (`types::value_ord`), which the query-side
//!   comparator `select_executor::value_ops::try_compare_values` delegates to
//!   for same-variant operands;
//! * `write_engine::clustering_order::compare_values`'s `Time` arm, reached by BOTH
//!   `ClusteringKey::compare` (schema-aware) and `ClusteringKey`'s `Ord` (the
//!   memtable `BTreeMap` key order). `write_engine::merge` then sorts merged
//!   rows with `ClusteringKey::compare` "for output order", so this one decides
//!   the physical row order in the emitted `Data.db`.
//!
//! The split is the harm, and a PARTIAL fix would have DEEPENED it, so #3935
//! swept the class. This file pins the two sites this round changed, plus the
//! asymmetry that must NOT be "unified" away, plus the disk-reaching
//! consequence at the write surface.
//!
//! Range validation was considered and REFUSED (issue #3935, lead ruling): if
//! stock Cassandra can write a value, CQLite must read it, and Cassandra's own
//! binary `validate` accepts an out-of-range `time` — so a range check would
//! make CQLite reject data Cassandra created. The citations for that are written
//! out ONCE, in `types::comparator::custom::compare_time`
//! (`# CANONICAL STATEMENT`), and are deliberately not restated here.
//!
//! # Format authority — never a CQLite `file:line` (#3041)
//!
//! At the pinned tag `cassandra-5.0.8`:
//!
//! * `src/java/org/apache/cassandra/db/marshal/TimeType.java:48` —
//!   `private TimeType() {super(ComparisonType.BYTE_ORDER);}`, and
//!   `ComparisonType.BYTE_ORDER` resolves to `ByteBufferUtil.compareUnsigned`:
//!   UNSIGNED lexicographic comparison of the serialized bytes, here the 8-byte
//!   big-endian nanos-since-midnight long.
//! * `src/java/org/apache/cassandra/db/marshal/TimestampType.java:56` —
//!   `private TimestampType() {super(ComparisonType.CUSTOM);}`, whose
//!   `compareCustom` (`:69-71`) is exactly `return LongType.compareLongs(...)`,
//!   i.e. SIGNED. Cited because the asymmetry is load-bearing: `time` and
//!   `timestamp` are both 8-byte longs and do NOT share a comparator, so the
//!   `timestamp_*` cases below exist to stop a later change unifying the arms.
//!
//! # THE EXPECTED ORDER IS A HAND-DERIVED LITERAL, NOT A ROUND-TRIP
//!
//! A CQLite-write -> CQLite-read round-trip is invariant to a uniform ordering
//! error (CLAUDE.md, #3042). So `EXPECTED` is written out by hand from
//! `BYTE_ORDER` applied to the four serialized forms — each spelled out in a
//! comment and asserted against `to_be_bytes()` before use — and the write
//! surface case compares FILE OFFSETS in the raw `Data.db`, running no reader,
//! no comparator and no decode path. `OLD_SIGNED_ORDER` is carried as a NEGATIVE
//! CONTROL: every case asserts the two candidate sequences differ, so a green
//! result provably discriminates the two implementations rather than being
//! satisfiable by any total order.

use std::collections::HashMap;

use cqlite_core::schema::{
    ClusteringColumn, ClusteringOrder, Column, CqlType, KeyColumn, TableSchema,
};
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::comparator::ComparatorType;
use cqlite_core::types::Value;
use tempfile::TempDir;

const KS: &str = "issue_3935_rows_ks";
const TBL: &str = "time_clustered";
const CK: &str = "t";
const VAL_COL: &str = "v";
const TS: i64 = 1_700_000_000_000_000;

// ===========================================================================
// The four `time` values, and the ORDER THE RULE PUTS THEM IN.
//
//   T_NEG = -1_000_000_000     -> FF FF FF FF C4 65 36 00   (out of range)
//   T_LOW =   9_000_000_000     -> 00 00 00 02 18 71 1A 00   (00:00:09)
//   T_MID =  43_200_000_000_000 -> 00 00 27 4A 48 A7 80 00   (12:00:00)
//   T_MAX =  86_399_999_999_999 -> 00 00 4E 94 91 4E FF FF   (= DAYS.toNanos(1) - 1)
//
// UNSIGNED lexicographic on the leading byte alone settles it: 0x00 < 0xFF, so
// the three in-range values come first in numeric order and the out-of-range
// negative sorts LAST. Signed `i64::cmp` — the pre-#3935 behaviour at both
// sites — puts T_NEG FIRST.
// ===========================================================================

const T_NEG: i64 = -1_000_000_000;
const T_LOW: i64 = 9_000_000_000;
const T_MID: i64 = 43_200_000_000_000;
const T_MAX: i64 = 86_399_999_999_999;

/// The order `TimeType`'s BYTE_ORDER puts them in. Hand-derived from the rule —
/// NOT a snapshot of anything CQLite emitted (#3042).
const EXPECTED: [i64; 4] = [T_LOW, T_MID, T_MAX, T_NEG];

/// The order the pre-#3935 signed arms produced. NEGATIVE CONTROL only: it must
/// DIFFER from `EXPECTED`, else no case here can tell the two apart.
const OLD_SIGNED_ORDER: [i64; 4] = [T_NEG, T_LOW, T_MID, T_MAX];

/// The order handed to the write engine — deliberately neither `EXPECTED` nor
/// `OLD_SIGNED_ORDER`, so a writer that merely preserved insertion order fails
/// both.
const INSERTION_ORDER: [i64; 4] = [T_MID, T_NEG, T_MAX, T_LOW];

/// Assert the serialized forms and the discriminating power of the two
/// candidate orders, before anything below relies on either.
fn assert_premises() {
    assert_eq!(
        T_NEG.to_be_bytes(),
        [0xFF, 0xFF, 0xFF, 0xFF, 0xC4, 0x65, 0x36, 0x00]
    );
    assert_eq!(
        T_LOW.to_be_bytes(),
        [0x00, 0x00, 0x00, 0x02, 0x18, 0x71, 0x1A, 0x00]
    );
    assert_eq!(
        T_MID.to_be_bytes(),
        [0x00, 0x00, 0x27, 0x4A, 0x48, 0xA7, 0x80, 0x00]
    );
    assert_eq!(
        T_MAX.to_be_bytes(),
        [0x00, 0x00, 0x4E, 0x94, 0x91, 0x4E, 0xFF, 0xFF]
    );
    assert_eq!(T_MAX, 24 * 60 * 60 * 1_000_000_000_i64 - 1);
    assert!(
        T_NEG < 0,
        "T_NEG must be out of `time`'s range to discriminate"
    );
    assert_ne!(EXPECTED, OLD_SIGNED_ORDER);
    assert_ne!(EXPECTED, INSERTION_ORDER);
    assert_ne!(OLD_SIGNED_ORDER, INSERTION_ORDER);
}

// ===========================================================================
// SITE 2 — `Value::PartialOrd`.
// ===========================================================================

/// `Value::Time`'s `partial_cmp` is `TimeType`'s BYTE_ORDER: the out-of-range
/// negative sorts ABOVE every in-range value.
///
/// This is the arm the query-side comparator
/// (`select_executor::value_ops::try_compare_values`) delegates to for
/// same-variant operands — `Value::Time` returns `None` from `as_f64`, so it
/// never takes that function's numeric-coercion branch.
#[test]
fn value_partial_ord_orders_time_by_byte_order() {
    assert_premises();

    let mut sorted: Vec<i64> = INSERTION_ORDER.to_vec();
    sorted.sort_by(|a, b| {
        Value::Time(*a)
            .partial_cmp(&Value::Time(*b))
            .expect("Time vs Time is always comparable")
    });
    assert_eq!(
        sorted,
        EXPECTED.to_vec(),
        "Value::PartialOrd must order `time` by TimeType's BYTE_ORDER \
         (unsigned bytes of the 8-byte big-endian nanos), so the out-of-range \
         negative sorts LAST; signed i64 order would put it first"
    );

    // Pin the single discriminating pair directly, so a failure names the rule
    // rather than a whole sequence.
    assert_eq!(
        Value::Time(T_NEG).partial_cmp(&Value::Time(T_MAX)),
        Some(std::cmp::Ordering::Greater),
        "0xFF.. > 0x00.. as UNSIGNED bytes: the negative nanos is GREATER"
    );
}

/// `Value::Timestamp` must stay SIGNED. THE ASYMMETRY PIN: `TimestampType` is
/// `ComparisonType.CUSTOM` delegating to `LongType.compareLongs`, so a
/// pre-epoch negative millis sorts BELOW every non-negative one — the OPPOSITE
/// of `time`. This case is what stops a later change "unifying" the two arms.
#[test]
fn value_partial_ord_keeps_timestamp_signed() {
    let mut sorted: Vec<i64> = vec![1, -1, 0, i64::MIN];
    sorted.sort_by(|a, b| {
        Value::Timestamp(*a)
            .partial_cmp(&Value::Timestamp(*b))
            .expect("Timestamp vs Timestamp is always comparable")
    });
    assert_eq!(
        sorted,
        vec![i64::MIN, -1, 0, 1],
        "TimestampType is ComparisonType.CUSTOM -> LongType.compareLongs, i.e. \
         SIGNED: negatives sort BELOW non-negatives, unlike `time`"
    );

    // The same two operands that discriminate `time` must go the OTHER way for
    // `timestamp`, which is the whole content of the asymmetry.
    assert_eq!(
        Value::Timestamp(T_NEG).partial_cmp(&Value::Timestamp(T_MAX)),
        Some(std::cmp::Ordering::Less),
        "signed: a negative timestamp is LESS, where the same `time` is GREATER"
    );
}

// ===========================================================================
// SITE 6 — `write_engine::clustering_order::compare_values`, via `ClusteringKey`.
//
// `compare_values` is crate-internal (`pub(super)`), so both PUBLIC routes to
// it are covered:
// `ClusteringKey`'s `Ord` (memtable BTreeMap order) and the schema-aware
// `ClusteringKey::compare` (which `merge` uses for output row order).
// ===========================================================================

fn ck(nanos: i64) -> ClusteringKey {
    ClusteringKey::single(CK, Value::Time(nanos))
}

/// `ClusteringKey`'s `Ord` — the memtable `BTreeMap` key order — puts a `time`
/// clustering value in BYTE_ORDER.
#[test]
fn clustering_key_ord_orders_time_by_byte_order() {
    assert_premises();

    let mut sorted: Vec<i64> = INSERTION_ORDER.to_vec();
    sorted.sort_by(|a, b| ck(*a).cmp(&ck(*b)));
    assert_eq!(
        sorted,
        EXPECTED.to_vec(),
        "ClusteringKey's Ord (memtable BTreeMap key order) must order `time` by \
         TimeType's BYTE_ORDER"
    );
    assert_ne!(
        sorted,
        OLD_SIGNED_ORDER.to_vec(),
        "negative control: the pre-#3935 signed order must NOT be produced"
    );
}

/// The schema-aware `ClusteringKey::compare` — the route
/// `write_engine::merge` uses to sort merged rows "for output order", i.e. the
/// one that decides the physical `Data.db` row order.
#[test]
fn clustering_key_schema_compare_orders_time_by_byte_order() {
    assert_premises();
    let sch = schema();

    let mut sorted: Vec<i64> = INSERTION_ORDER.to_vec();
    sorted.sort_by(|a, b| {
        ck(*a)
            .compare(&ck(*b), &sch)
            .expect("time clustering keys are comparable")
    });
    assert_eq!(
        sorted,
        EXPECTED.to_vec(),
        "ClusteringKey::compare must order `time` by TimeType's BYTE_ORDER — \
         this is the comparator that decides physical Data.db row order"
    );
}

/// `time` clustering order is a STRICT TOTAL ORDER, which `ClusteringKey`
/// REQUIRES: it is a memtable `BTreeMap` key and a compaction merge key, and a
/// non-total order there silently loses rows (issues #1870/#2010).
///
/// Byte comparison of a FIXED 8-byte array is trivially total; this case is the
/// standing pin, exercised over the boundary values a signed/unsigned mixup
/// would break (`i64::MIN` = `0x80..`, `-1` = `0xFF..`, `0`, and the range max).
#[test]
fn clustering_key_time_order_is_a_strict_total_order() {
    let vals = [
        T_NEG,
        T_LOW,
        T_MID,
        T_MAX,
        0_i64,
        -1_i64,
        i64::MIN,
        i64::MAX,
    ];

    for &a in &vals {
        // Reflexive: every value equals itself.
        assert_eq!(
            ck(a).cmp(&ck(a)),
            std::cmp::Ordering::Equal,
            "reflexivity at {a}"
        );
        for &b in &vals {
            // Antisymmetric / total: exactly one of <, ==, > holds, and
            // reversing the operands reverses the answer.
            assert_eq!(
                ck(a).cmp(&ck(b)),
                ck(b).cmp(&ck(a)).reverse(),
                "antisymmetry at ({a}, {b})"
            );
            assert_eq!(
                ck(a).cmp(&ck(b)) == std::cmp::Ordering::Equal,
                a == b,
                "only equal values may compare Equal — ({a}, {b})"
            );
            for &c in &vals {
                // Transitive.
                if ck(a).cmp(&ck(b)) != std::cmp::Ordering::Greater
                    && ck(b).cmp(&ck(c)) != std::cmp::Ordering::Greater
                {
                    assert_ne!(
                        ck(a).cmp(&ck(c)),
                        std::cmp::Ordering::Greater,
                        "transitivity at ({a}, {b}, {c})"
                    );
                }
            }
        }
    }

    // And the order really is the unsigned-byte one over those boundaries:
    // 0x00.. < 0x7F.. < 0x80.. < 0xFF..
    let mut sorted = vals.to_vec();
    sorted.sort_by(|a, b| ck(*a).cmp(&ck(*b)));
    assert_eq!(
        sorted,
        vec![0, T_LOW, T_MID, T_MAX, i64::MAX, i64::MIN, T_NEG, -1],
        "unsigned byte order over the boundary values: non-negatives ascending, \
         then i64::MIN (0x80..), then the negatives ascending as unsigned"
    );
}

/// A `timestamp` clustering column must stay SIGNED at this site too — the
/// site-6 half of the asymmetry pin.
#[test]
fn clustering_key_keeps_timestamp_signed() {
    let tk = |ms: i64| ClusteringKey::single(CK, Value::Timestamp(ms));
    let mut sorted: Vec<i64> = vec![1, -1, 0, i64::MIN];
    sorted.sort_by(|a, b| tk(*a).cmp(&tk(*b)));
    assert_eq!(
        sorted,
        vec![i64::MIN, -1, 0, 1],
        "compare_values must keep `timestamp` SIGNED (TimestampType is \
         ComparisonType.CUSTOM -> LongType.compareLongs)"
    );
}

// ===========================================================================
// CONVERGENCE DIFFERENTIAL — every PUBLICLY REACHABLE `time` comparator must
// agree, for every ordered pair of the boundary values.
//
// This is the durable guard the split created the need for: fixing one site and
// not another is exactly what #3935 was, so the property under test is
// AGREEMENT, not any single site's answer. It is checked against the
// hand-derived BYTE_ORDER rule as well, so all sites agreeing on the WRONG
// answer still fails.
//
// SCOPE, declared: the three comparators reachable from outside the crate are
// covered. The two in-crate writer comparators
// (`data_writer/collection_order::compare_collection_elements` and
// `data_writer/marshal_comparator`) are `pub(crate)` and are pinned against
// each other by the in-crate differential
// `data_writer/tests/writer_comparator_differential.rs`, and against the
// on-disk bytes by `issue_3935_collection_time_byte_order.rs`.
// ===========================================================================

#[test]
fn all_publicly_reachable_time_comparators_agree() {
    assert_premises();
    let sch = schema();
    let cmp_ty =
        ComparatorType::from_cql_type(&CqlType::Time).expect("CqlType::Time maps to a comparator");

    let vals = [
        T_NEG,
        T_LOW,
        T_MID,
        T_MAX,
        0_i64,
        -1_i64,
        i64::MIN,
        i64::MAX,
    ];
    for &a in &vals {
        for &b in &vals {
            // The RULE, applied by hand: unsigned lexicographic comparison of
            // the serialized big-endian forms. Not read from any CQLite site.
            let rule = a.to_be_bytes().cmp(&b.to_be_bytes());

            let via_comparator = cmp_ty
                .compare(&Value::Time(a), &Value::Time(b))
                .expect("Custom(\"time\") compares two Time values");
            let via_partial_ord = Value::Time(a)
                .partial_cmp(&Value::Time(b))
                .expect("Time vs Time is comparable");
            let via_clustering_ord = ck(a).cmp(&ck(b));
            let via_clustering_compare = ck(a)
                .compare(&ck(b), &sch)
                .expect("time clustering keys are comparable");

            for (name, got) in [
                ("ComparatorType::compare", via_comparator),
                ("Value::partial_cmp", via_partial_ord),
                ("ClusteringKey::cmp", via_clustering_ord),
                ("ClusteringKey::compare", via_clustering_compare),
            ] {
                assert_eq!(
                    got, rule,
                    "{name} disagrees with TimeType's BYTE_ORDER for \
                     ({a}, {b}): got {got:?}, rule says {rule:?}"
                );
            }
        }
    }
}

// ===========================================================================
// THE DISK-REACHING CONSEQUENCE — DECLARED GAP, measured, not assumed.
//
// The chain the site-6 fix sits on is `compare_values` -> `ClusteringKey` ->
// `merge`'s "Sort merged rows by clustering key for output order" -> the bytes
// in `Data.db`. The sort is now BYTE_ORDER, and the case below was written to
// pin the resulting physical row order by FILE OFFSET in the raw file.
//
// IT IS NOT REACHABLE THROUGH CQLITE'S WRITE SURFACE TODAY, and that was
// MEASURED rather than predicted: `WriteEngine::flush` on a `time` clustering
// column returns
//   InvalidInput("Type mismatch or unsupported clustering type:
//                 value=Time(..), comparator=Custom(\"time\")")
// because `data_writer/encoding::serialize_value_for_clustering` — the ONE
// serializer on the write-out path, shared by flush AND compaction (its callers
// are `serialize_clustering_prefix_to_vec` and `rows.rs`) — has no `Custom` arm
// at all. So CQLite cannot WRITE a `time` (or `inet`) clustering column, and the
// refusal precedes any ordering effect on disk.
//
// The ordering fix is therefore CORRECT AND CURRENTLY LATENT at the byte level,
// and observable at the semantic level: `ClusteringKey::compare` also decides
// range-tombstone coverage (`encoding::range_tombstone_covers`), where a signed
// `time` comparison wrongly includes or excludes a row.
//
// Rather than invent a surface or weaken an assertion, the gap is PINNED: the
// case below asserts the fail-closed refusal and names what it blocks. Adding a
// `Custom("time")` arm to that serializer is a NEW WRITE CAPABILITY needing its
// own Cassandra byte-parity authority (the framing question — fixed-width raw 8
// bytes vs a vint length prefix, note the inconsistent `Date` arm beside it) and
// is out of #3935's ordering scope. When it lands, THIS CASE REDS, which is the
// prompt to replace it with a real row-order assertion: write the four rows,
// locate each value's 8-byte big-endian form in the raw `Data.db` and compare
// FILE OFFSETS — an oracle that runs no reader and no comparator (#3042), so it
// cannot be satisfied by a uniform writer+reader error. The sibling
// `issue_3935_collection_time_byte_order.rs` already does exactly that for the
// collection surfaces and its `on_disk_order`/`find_data_db` helpers are the
// pattern to copy.
// ===========================================================================

fn schema() -> TableSchema {
    let col = |name: &str, ty: &str| Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    };
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: CK.to_string(),
            data_type: "time".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![col("id", "int"), col(CK, "time"), col(VAL_COL, "int")],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Write one row per clustering value into ONE partition and flush, returning
/// the flush ERROR as a string.
///
/// Fails closed if the flush SUCCEEDS: that would mean the write path has gained
/// `time` clustering support and the declared gap above is stale.
fn write_rows_expecting_flush_error(clustering_values: &[i64]) -> String {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let temp = TempDir::new().expect("temp dir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    for (i, &nanos) in clustering_values.iter().enumerate() {
        let mutation = Mutation::new(
            TableId::new(KS, TBL),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single(CK, Value::Time(nanos))),
            vec![CellOperation::Write {
                column: VAL_COL.to_string(),
                // A distinct payload per row, so nothing can collapse them.
                value: Value::Integer(i as i32 + 1),
            }],
            TS,
            None,
        );
        engine
            .write(mutation)
            .expect("the memtable write itself succeeds");
    }

    match rt.block_on(engine.flush()) {
        Err(e) => e.to_string(),
        Ok(_) => panic!(
            "flush SUCCEEDED for a `time` clustering column. The declared gap in \
             this file is now STALE: `serialize_value_for_clustering` has gained \
             a Custom(\"time\") arm, so REPLACE this pin with the real on-disk \
             row-order assertion described above this case."
        ),
    }
}

/// DECLARED GAP PIN: CQLite's write path REFUSES a `time` clustering column, so
/// the byte-level row-order consequence of the site-6 fix is not observable
/// through the write surface yet. Asserts the refusal AND that it comes from the
/// clustering SERIALIZER (naming the unsupported comparator), so this case
/// cannot be satisfied by an unrelated write failure.
///
/// This reds the day a `Custom("time")` clustering arm is added — which is the
/// point: at that moment the byte-level row-order assertion described above
/// becomes writable and must replace this pin.
#[test]
fn a_time_clustering_column_is_refused_by_the_write_path_declared_gap() {
    assert_premises();

    let err = write_rows_expecting_flush_error(&INSERTION_ORDER);
    assert!(
        err.contains("unsupported clustering type"),
        "expected the clustering SERIALIZER's fail-closed refusal, got: {err}"
    );
    assert!(
        err.contains("Custom(\"time\")"),
        "the refusal must name Custom(\"time\") as the unsupported comparator, \
         so this pin cannot be satisfied by an unrelated write failure; got: {err}"
    );

    // Corroboration at the serializer's own level of abstraction: the sort that
    // WOULD decide the row order is already correct, so only the serializer is
    // missing. (Re-stated here because the case above proves a refusal, not a
    // correct order.)
    let sch = schema();
    let mut sorted: Vec<i64> = INSERTION_ORDER.to_vec();
    sorted.sort_by(|a, b| {
        ClusteringKey::single(CK, Value::Time(*a))
            .compare(&ClusteringKey::single(CK, Value::Time(*b)), &sch)
            .expect("comparison itself is supported")
    });
    assert_eq!(
        sorted,
        EXPECTED.to_vec(),
        "the row order the merge WOULD emit is already TimeType's BYTE_ORDER; \
         only `serialize_value_for_clustering` blocks it reaching disk"
    );
}
