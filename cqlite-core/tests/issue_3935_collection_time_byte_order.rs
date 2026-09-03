//! Issue #3935 — every writer path that lays a `set<time>` / `map<time,…>` down
//! must use the SAME on-disk element order, and that order must be `TimeType`'s
//! `ComparisonType.BYTE_ORDER`.
//!
//! Four write surfaces are covered here: the NON-FROZEN whole-collection and
//! per-element paths, the FROZEN whole-value path (`serialize_value`), and a
//! `set<time>`/`map<time,text>` FIELD OF A FROZEN UDT — the last of which is
//! ordered by a SECOND, independent comparator in `data_writer/udt_canon`.
//!
//! # The defect
//!
//! `data_writer/collection_order::compare_collection_elements` compared
//! `Value::Time` with signed `i64::cmp`, under a comment asserting that
//! "TimestampType/TimeType extend/share LongType". That is exactly half right:
//! `TimestampType` really is signed, `TimeType` is not. So CQLite had TWO
//! collection write paths that DISAGREED:
//!
//! * per-element (`complex.rs`, via `schema_helpers::compare_cell_paths`) —
//!   unsigned comparison of the raw serialized cell-path bytes, which IS
//!   `TimeType`'s rule;
//! * whole-collection (`complex.rs` `write_set_complex_cells` /
//!   `write_map_complex_cells`, via `compare_collection_elements`) — SIGNED, and
//!   it emits cell paths in that order with NO re-sort afterwards.
//!
//! They agree for every value in `time`'s valid range and invert for an
//! out-of-range NEGATIVE nanos. This file pins the agreement.
//!
//! A SECOND, independently-written comparator had the SAME defect:
//! `data_writer/udt_canon`'s `classify_comparator` put `TimeType` in its
//! `SignedInt` family beside `TimestampType`. It orders a frozen `SetType`'s
//! elements / a `MapType`'s keys from the DECLARED MARSHAL while canonicalizing
//! a UDT value, and for a collection field of a frozen UDT nothing re-sorts
//! afterwards — see the comment block above the `udt_nested_*` cases for the
//! chain and for why a TOP-LEVEL `frozen<set<time>>` masked it.
//!
//! # Format authority — never a CQLite `file:line` (#3041)
//!
//! At the pinned tag `cassandra-5.0.8`:
//!
//! * `src/java/org/apache/cassandra/db/marshal/TimeType.java:48` —
//!   `private TimeType() {super(ComparisonType.BYTE_ORDER);}`. `TimeType`
//!   declares no `validate` override.
//! * `ComparisonType.BYTE_ORDER` resolves to `ByteBufferUtil.compareUnsigned`:
//!   UNSIGNED lexicographic comparison of the serialized bytes — here the 8-byte
//!   big-endian nanos-since-midnight long.
//! * `src/java/org/apache/cassandra/db/marshal/TimestampType.java:56` —
//!   `super(ComparisonType.CUSTOM)`, whose `compareCustom` (`:69-71`) is exactly
//!   `return LongType.compareLongs(...)`, i.e. SIGNED. Cited to make the
//!   asymmetry explicit: the two temporal types do NOT share a comparator.
//!
//! For a NON-FROZEN collection the SET element (or MAP key) *is* the cell path,
//! so that comparator decides the physical cell order — which is what makes the
//! property observable in the emitted bytes at all.
//!
//! # THE EXPECTED ORDER IS A LITERAL DERIVED FROM THE RULE, NOT A ROUND-TRIP
//!
//! A CQLite-write -> CQLite-read round-trip is **invariant to a uniform
//! ordering error** (CLAUDE.md, #3042): writer and reader make the identical
//! mistake, the round-trip closes, and the test stays green. So this file does
//! NOT read an expected order back out of whatever CQLite produced. `EXPECTED`
//! below is written out by hand from `TimeType`'s BYTE_ORDER rule applied to the
//! four serialized forms, each of which is spelled out in a comment and asserted
//! against `to_be_bytes()` before it is used. The primary leg then locates those
//! byte patterns in the raw `Data.db` and compares their FILE OFFSETS — an
//! oracle that does not run CQLite's reader, its comparator, or any decode path.
//!
//! # WHY THERE IS NO CASSANDRA FIXTURE FOR THIS CASE — stated precisely
//!
//! Not because Cassandra rejects an out-of-range `time`: **it does not.**
//! `TimeType` has no `validate` override and
//! `src/java/org/apache/cassandra/serializers/TimeSerializer.java:71-75`
//! `validate` checks the SIZE ONLY (`if (accessor.size(value) != 8) throw ...`).
//! The range check `result < 0 || result >= TimeUnit.DAYS.toNanos(1)` lives ONLY
//! in `timeStringToLong` (`TimeSerializer.java:50`), the CQL string-literal /
//! JSON path. An 8-byte BINARY out-of-range `time` therefore passes Cassandra's
//! validation, is stored, and is ordered BYTE_ORDER.
//!
//! The actual reasons are: (a) the committed corpus contains no such value —
//! `test-data/datasets/sstables/test_comparator_order/` (issue #3790) holds only
//! in-range `time`s, where byte, unsigned and signed order all coincide, so it
//! cannot falsify either implementation; and (b) producing one needs a
//! BINARY-PROTOCOL write that bypasses the CQL string path, which no committed
//! generator script does. Generating such a fixture is a corpus task, not a
//! blocker on the rule: the rule is fully determined by the pinned source above,
//! and this file asserts each of CQLite's write surfaces against THAT rule
//! rather than against each other alone.
//!
//! Companion coverage, co-required and neither sufficient alone:
//! * `collection_order::tests::{time_negative_nanos_sorts_above_every_non_negative,
//!   timestamp_keeps_signed_order, in_range_time_order_is_unchanged}` — the
//!   comparator unit pins (RED-verified: reverting the arm to `x.cmp(y)` fails
//!   the first two; the third is the compatibility pin and passes under BOTH
//!   implementations by construction, which is exactly its job).
//! * `issue_3790_collection_order_cassandra_golden.rs` — the in-range order read
//!   out of real Cassandra-written bytes.
//! * `issue_3790_merged_read_time_order.rs` — the merged-READ assembly order.
//!
//! # RED-verified (measured, not asserted)
//!
//! There are TWO independent writer-side sorted-collection comparators, and each
//! was reverted separately with the rest of the tree unchanged. MEASURED in this
//! lane over the 10 cases of this file:
//!
//! * `collection_order`'s `time` arm reverted to `x.cmp(y)` — **5 FAIL, 5 pass**:
//!   FAIL `whole_collection_set_matches_byte_order`,
//!   `whole_collection_map_matches_byte_order` (the negative nanos lands FIRST
//!   instead of LAST), `both_write_paths_agree_on_element_order`, and the two
//!   FROZEN cases `frozen_set_matches_byte_order` /
//!   `frozen_map_matches_byte_order`. The two `udt_nested_*` cases stay GREEN,
//!   because that path does not consult this comparator.
//! * `udt_canon::classify_comparator` reverted to classifying `TimeType` as
//!   `SignedInt` — **2 FAIL, 8 pass**: FAIL exactly
//!   `udt_nested_set_matches_byte_order` and `udt_nested_map_matches_byte_order`,
//!   whose emitted on-disk order was the SIGNED sequence
//!   `[-1000000000, 9000000000, 43200000000000, 86399999999999]`. Nothing else
//!   moved, INCLUDING the frozen cases — a top-level `frozen<set<time>>` is
//!   re-sorted by `encoding.rs`, which is what MASKED this second defect.
//!
//! The two reverts fail DISJOINT case sets, so neither comparator's coverage is
//! standing in for the other's.
//!
//! * PASS under both — `per_element_path_matches_byte_order`, because that path
//!   never consulted either comparator; its passing under BOTH implementations
//!   IS the pre-existing divergence this issue is about, and it is what makes
//!   the agreement case non-vacuous.
//! * PASS under both — `in_range_only_collection_order_is_unmoved` and
//!   `udt_nested_in_range_order_is_unmoved`, by construction: they are the
//!   COMPATIBILITY pins, and a compatibility pin that reddened under the old
//!   comparator would be asserting the opposite of its own claim.
//!
//! A green ordering test that has never been shown to red proves nothing; so
//! does a test whose every case reds, which would mean it cannot tell the
//! unchanged half of the behaviour from the changed half.
//!
//! # Gate
//!
//! `#![cfg(feature = "write-support")]` is REQUIRED, not a narrowing:
//! `storage::write_engine` itself is `#[cfg(feature = "write-support")]`, so an
//! ungated target would break a `--no-default-features --all-targets` build.
//! `write-support` is a DEFAULT feature, so this target EXECUTES in the gate of
//! record's `core-tests` (`cargo test -p cqlite-core --features cli-helpers`,
//! which keeps defaults) rather than joining the compiles-everywhere /
//! executes-nowhere set (#3522).
//!
//! ```bash
//! cargo test -p cqlite-core --features cli-helpers \
//!   --test issue_3935_collection_time_byte_order -- --nocapture
//! ```

#![cfg(feature = "write-support")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::compaction_row::CompactionRowData;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::{UdtField, UdtValue, Value};
use cqlite_core::Config;
use tempfile::TempDir;

const KS: &str = "issue_3935_ks";
const TBL: &str = "times";
const SET_COL: &str = "tset";
const MAP_COL: &str = "tmap";
/// FROZEN (single-cell) collection columns. A frozen collection is NOT a complex
/// column (`schema_helpers::is_complex_column` returns false for `frozen<...>`),
/// so its whole value is serialized by `encoding.rs`'s `serialize_value` —
/// the SECOND caller the `collection_order` module doc names, and the one the
/// non-frozen cases above do not reach.
const FROZEN_SET_COL: &str = "fset";
const FROZEN_MAP_COL: &str = "fmap";
/// A `frozen<udt>` column whose single declared field is a `set<time>` /
/// `map<time,text>`. This is the reachable surface of the SECOND writer-side
/// sorted-collection comparator, `udt_canon::compare_for_marshal` (see the
/// module comment on the UDT cases below).
const UDT_SET_COL: &str = "uset";
const UDT_MAP_COL: &str = "umap";
const UDT_SET_TYPE: &str = "tset_udt";
const UDT_MAP_TYPE: &str = "tmap_udt";
const UDT_SET_FIELD: &str = "slots";
const UDT_MAP_FIELD: &str = "labels";
const TS: i64 = 1_700_000_000_000_000;

const MARSHAL: &str = "org.apache.cassandra.db.marshal.";

/// Lowercase hex of an ASCII string — the encoding Cassandra's `UserType(...)`
/// marshal uses for the type name and each field name. Hand-rolled so this
/// target needs no `hex` dev-dependency.
fn hex_ascii(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

/// The full `FrozenType(UserType(...))` marshal for a one-field UDT.
///
/// Spelled out as a marshal string rather than the CQL short form `frozen<t>`
/// ON PURPOSE: `udt_canon::canonicalize_udt_value` gates on the declared type
/// mentioning `UserType(` (a bare `frozen<t>` needs a `UdtRegistry` the write
/// engine is not given here), and the marshal is what a real SerializationHeader
/// carries anyway. `FrozenType(...)` also keeps the column a SIMPLE cell, which
/// is what routes it through the canonicalizer.
fn frozen_udt_marshal(type_name: &str, field_name: &str, field_marshal: &str) -> String {
    format!(
        "{MARSHAL}FrozenType({MARSHAL}UserType({KS},{},{}:{}))",
        hex_ascii(type_name),
        hex_ascii(field_name),
        field_marshal
    )
}

// ===========================================================================
// The four `time` values, and the ORDER THE RULE PUTS THEM IN.
//
// Serialized form is the 8-byte big-endian nanos long. Written out here so the
// expectation below is read off the BYTES, exactly as
// `ByteBufferUtil.compareUnsigned` reads them:
//
//   T_NEG = -1_000_000_000  -> FF FF FF FF C4 65 36 00   (out of range)
//   T_LOW =   9_000_000_000 -> 00 00 00 02 18 71 1A 00   (00:00:09)
//   T_MID =  43_200_000_000_000 -> 00 00 27 4A 48 A7 80 00 (12:00:00)
//   T_MAX =  86_399_999_999_999 -> 00 00 4E 94 91 4E FF FF (23:59:59.999999999,
//                                  = DAYS.toNanos(1) - 1, the largest valid)
//
// UNSIGNED lexicographic on the leading byte alone settles it: 0x00 < 0xFF, so
// the three in-range values come first in numeric order and the negative sorts
// LAST. Signed `i64::cmp` — the pre-#3935 behaviour — puts T_NEG FIRST, so the
// two implementations produce different sequences and this expectation
// discriminates them.
// ===========================================================================

const T_NEG: i64 = -1_000_000_000;
const T_LOW: i64 = 9_000_000_000;
const T_MID: i64 = 43_200_000_000_000;
const T_MAX: i64 = 86_399_999_999_999;

/// The order `TimeType`'s BYTE_ORDER puts them in. A hand-derived literal — NOT
/// a snapshot of anything CQLite emitted (#3042).
const EXPECTED: [i64; 4] = [T_LOW, T_MID, T_MAX, T_NEG];

/// The order the removed signed comparator produced. Used only as a negative
/// control: it must DIFFER from `EXPECTED`, else these cases cannot tell the two
/// implementations apart.
const OLD_SIGNED_ORDER: [i64; 4] = [T_NEG, T_LOW, T_MID, T_MAX];

/// The write order handed to the engine — deliberately neither `EXPECTED` nor
/// `OLD_SIGNED_ORDER`, so a writer that merely preserved insertion order would
/// fail both.
const INSERTION_ORDER: [i64; 4] = [T_MID, T_NEG, T_MAX, T_LOW];

/// Assert the serialized forms the whole expectation rests on, before using them.
fn assert_serialized_forms() {
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
    // T_MAX is `DAYS.toNanos(1) - 1`, the largest value the CQL string path
    // accepts; T_NEG is outside the range that path checks at all.
    assert_eq!(T_MAX, 24 * 60 * 60 * 1_000_000_000_i64 - 1);
    assert!(T_NEG < 0);
    // The two candidate orders must differ, or nothing below discriminates.
    assert_ne!(EXPECTED, OLD_SIGNED_ORDER);
    assert_ne!(EXPECTED, INSERTION_ORDER);
    assert_ne!(OLD_SIGNED_ORDER, INSERTION_ORDER);
}

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
        clustering_keys: vec![],
        columns: vec![
            col("id", "int"),
            col(SET_COL, "set<time>"),
            col(MAP_COL, "map<time, text>"),
            col(FROZEN_SET_COL, "frozen<set<time>>"),
            col(FROZEN_MAP_COL, "frozen<map<time, text>>"),
            col(
                UDT_SET_COL,
                &frozen_udt_marshal(
                    UDT_SET_TYPE,
                    UDT_SET_FIELD,
                    &format!("{MARSHAL}SetType({MARSHAL}TimeType)"),
                ),
            ),
            col(
                UDT_MAP_COL,
                &frozen_udt_marshal(
                    UDT_MAP_TYPE,
                    UDT_MAP_FIELD,
                    &format!("{MARSHAL}MapType({MARSHAL}TimeType,{MARSHAL}UTF8Type)"),
                ),
            ),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Write ONE mutation, flush, and return `(temp_dir, Data.db path)`.
///
/// Each call gets its OWN temp dir and therefore its own single-partition
/// SSTable, so the raw byte scan below is unambiguous: a value's 8-byte pattern
/// occurs once per file, which the scan asserts rather than assumes.
fn write_one(ops: Vec<CellOperation>) -> (TempDir, PathBuf) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let temp = TempDir::new().expect("temp dir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let sch = schema();

    let mutation = Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        ops,
        TS,
        None,
    );
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, sch);
    let mut engine = WriteEngine::new(config).expect("engine creation");
    engine.write(mutation).expect("write mutation");
    rt.block_on(engine.flush())
        .expect("flush")
        .expect("flush must produce a generation");
    rt.block_on(engine.close()).expect("close engine");

    let data_db = find_data_db(&data_dir);
    (temp, data_db)
}

/// The single flushed `*-Data.db`. Fails closed on zero or several — a scan over
/// the wrong file, or over none, must never read as a pass.
fn find_data_db(data_dir: &Path) -> PathBuf {
    let mut found: Vec<PathBuf> = Vec::new();
    let mut stack = vec![data_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
            {
                found.push(path);
            }
        }
    }
    assert_eq!(
        found.len(),
        1,
        "expected exactly one flushed *-Data.db under {data_dir:?}, found {found:?}"
    );
    found.remove(0)
}

// ===========================================================================
// LEG 1 (PRIMARY) — the raw emitted bytes.
//
// Locate each value's 8-byte big-endian serialized form in the `Data.db` and
// compare FILE OFFSETS. This runs no reader, no comparator and no decode path,
// so it cannot be satisfied by a uniform writer+reader error (#3042).
// ===========================================================================

/// File offsets of each value's serialized `time` pattern, in ascending offset
/// order — i.e. the physical order the writer emitted them in.
///
/// Each call site writes ONE collection column into its own SSTable, so every
/// pattern occurs exactly once. That is ASSERTED, not assumed: a second
/// occurrence (which is what a file holding both the SET and the MAP column
/// produces, since the two carry the same cell paths) is a hard failure rather
/// than a silent first-match.
fn on_disk_order(data_db: &Path, values: &[i64]) -> Vec<i64> {
    let bytes = std::fs::read(data_db).unwrap_or_else(|e| panic!("read {data_db:?}: {e}"));
    let mut located: Vec<(usize, i64)> = Vec::new();
    for &nanos in values {
        let needle = nanos.to_be_bytes();
        let hits: Vec<usize> = bytes
            .windows(8)
            .enumerate()
            .filter(|(_, w)| *w == needle.as_slice())
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            hits.len(),
            1,
            "the serialized form of time {nanos} ({needle:02X?}) must occur \
             EXACTLY ONCE in {data_db:?} for an offset comparison to be \
             meaningful; found {} occurrence(s) at {hits:?}",
            hits.len()
        );
        located.push((hits[0], nanos));
    }
    located.sort_by_key(|(offset, _)| *offset);
    located.into_iter().map(|(_, nanos)| nanos).collect()
}

// ===========================================================================
// LEG 2 (CORROBORATION) — the same order through the real reader.
//
// `SSTableReader::open` + `iterate_all_partitions_for_compaction` surfaces each
// complex column's per-element cells IN ON-DISK ORDER with their decoded values.
// Its job here is to confirm Leg 1's byte patterns are the values the cells
// really carry — it is NOT the oracle, for the #3042 reason in the file header.
// ===========================================================================

fn decoded_order(data_db: &Path, column: &str, expected_len: usize) -> Vec<i64> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    rt.block_on(async {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let reader = SSTableReader::open(data_db, &config, platform)
            .await
            .unwrap_or_else(|e| panic!("open {data_db:?}: {e}"));
        let sch = schema();
        let rows = reader
            .iterate_all_partitions_for_compaction(Some(&sch))
            .await
            .unwrap_or_else(|e| panic!("iterate {data_db:?}: {e}"));
        assert_eq!(rows.len(), 1, "expected the single written partition");
        let complex = match &rows[0].row_data {
            CompactionRowData::Live { complex, .. } => complex,
            other => panic!("partition did not decode as a live row: {other:?}"),
        };
        let col = complex
            .iter()
            .find(|c| c.column == column)
            .unwrap_or_else(|| {
                panic!(
                    "column {column} absent from the decoded complex columns \
                     {:?} — a present SSTable decoding to no elements is a hard \
                     failure, never a skip",
                    complex.iter().map(|c| &c.column).collect::<Vec<_>>()
                )
            });
        assert_eq!(
            col.elements.len(),
            expected_len,
            "{column} must decode to all {expected_len} written elements"
        );
        col.elements
            .iter()
            .map(|e| {
                // A MAP entry carries its key in `decoded_key`; a SET member
                // carries the element in the cell path, surfaced as `value`.
                // Panics rather than skipping: a shortened sequence would be a
                // vacuous pass.
                let v = e
                    .decoded_key
                    .as_ref()
                    .or(e.value.as_ref())
                    .unwrap_or_else(|| {
                        panic!("{column}: element decoded to neither a map key nor a value")
                    });
                match v {
                    Value::Time(n) => *n,
                    other => panic!("{column}: element decoded as {other:?}, expected Time"),
                }
            })
            .collect()
    })
}

// ===========================================================================
// Ops builders — the two write paths.
// ===========================================================================

/// WHOLE-COLLECTION path: ONE `Write` carrying the entire `Value::Set` /
/// `Value::Map`, which `write_set_complex_cells` / `write_map_complex_cells`
/// order via `compare_collection_elements` — the function this issue fixes.
///
/// Exactly one collection column per mutation, so the resulting SSTable carries
/// each cell path once (see `on_disk_order`).
fn whole_collection_ops(column: &str, values: &[i64]) -> Vec<CellOperation> {
    let value = if column == MAP_COL {
        Value::Map(
            values
                .iter()
                .map(|&n| (Value::Time(n), Value::text(format!("v{n}"))))
                .collect(),
        )
    } else {
        Value::Set(values.iter().map(|&n| Value::Time(n)).collect())
    };
    vec![CellOperation::Write {
        column: column.to_string(),
        value,
    }]
}

/// PER-ELEMENT path: one `WriteComplexElement` per element, whose ALREADY
/// SERIALIZED `cell_path` the writer orders via
/// `schema_helpers::compare_cell_paths` (unsigned raw bytes) — the path that was
/// already correct, here as the agreement partner.
fn per_element_ops(column: &str, values: &[i64]) -> Vec<CellOperation> {
    values
        .iter()
        .map(|&nanos| CellOperation::WriteComplexElement {
            column: column.to_string(),
            cell_path: nanos.to_be_bytes().to_vec(),
            value: if column == MAP_COL {
                Some(Value::text(format!("v{nanos}")))
            } else {
                None
            },
            timestamp_micros: TS,
            ttl_seconds: None,
            local_deletion_time: None,
            is_deleted: false,
        })
        .collect()
}

// ===========================================================================
// Cases.
// ===========================================================================

/// The whole-collection SET writer must emit BYTE_ORDER — the negative nanos
/// LAST, not first. This is the case the fix makes pass.
#[test]
fn whole_collection_set_matches_byte_order() {
    assert_serialized_forms();
    let (_temp, data_db) = write_one(whole_collection_ops(SET_COL, &INSERTION_ORDER));
    let raw = on_disk_order(&data_db, &INSERTION_ORDER);
    assert_eq!(
        raw,
        EXPECTED.to_vec(),
        "whole-collection set<time>: emitted order must be TimeType BYTE_ORDER \
         (TimeType.java:48). Signed order would be {OLD_SIGNED_ORDER:?}"
    );
    assert_ne!(
        raw,
        OLD_SIGNED_ORDER.to_vec(),
        "negative control: the emitted order must not be the pre-#3935 signed one"
    );
    // Corroboration: the bytes Leg 1 matched really are these elements.
    assert_eq!(decoded_order(&data_db, SET_COL, 4), EXPECTED.to_vec());
}

/// Same property for a `map<time, text>`, whose ordering value is the KEY.
#[test]
fn whole_collection_map_matches_byte_order() {
    assert_serialized_forms();
    let (_temp, data_db) = write_one(whole_collection_ops(MAP_COL, &INSERTION_ORDER));
    let raw = on_disk_order(&data_db, &INSERTION_ORDER);
    assert_eq!(
        raw,
        EXPECTED.to_vec(),
        "whole-collection map<time,text>: keys must be emitted in TimeType \
         BYTE_ORDER. Signed order would be {OLD_SIGNED_ORDER:?}"
    );
    assert_eq!(decoded_order(&data_db, MAP_COL, 4), EXPECTED.to_vec());
}

/// The per-element writer already ordered by raw unsigned cell-path bytes; pin
/// it so the agreement below cannot be reached by BOTH paths drifting together.
#[test]
fn per_element_path_matches_byte_order() {
    assert_serialized_forms();
    for col in [SET_COL, MAP_COL] {
        let (_temp, data_db) = write_one(per_element_ops(col, &INSERTION_ORDER));
        assert_eq!(
            on_disk_order(&data_db, &INSERTION_ORDER),
            EXPECTED.to_vec(),
            "{col}: per-element path — compare_cell_paths is unsigned raw \
             bytes, which IS TimeType BYTE_ORDER"
        );
        assert_eq!(decoded_order(&data_db, col, 4), EXPECTED.to_vec());
    }
}

/// THE ISSUE'S OWN ORACLE: the two write paths must emit the SAME element order.
///
/// Before the fix they diverged for the out-of-range negative — whole-collection
/// put it FIRST (signed), per-element LAST (byte order). The equality is
/// asserted TOGETHER WITH each side equalling the rule-derived `EXPECTED`, so
/// this case cannot be satisfied by both paths agreeing on a WRONG order.
#[test]
fn both_write_paths_agree_on_element_order() {
    assert_serialized_forms();
    for col in [SET_COL, MAP_COL] {
        let (_temp_whole, whole_db) = write_one(whole_collection_ops(col, &INSERTION_ORDER));
        let (_temp_elem, elem_db) = write_one(per_element_ops(col, &INSERTION_ORDER));

        let whole_raw = on_disk_order(&whole_db, &INSERTION_ORDER);
        let elem_raw = on_disk_order(&elem_db, &INSERTION_ORDER);
        assert_eq!(
            whole_raw, elem_raw,
            "issue #3935: for {col} the whole-collection and per-element write \
             paths must lay the collection down in the SAME on-disk order"
        );
        assert_eq!(
            whole_raw,
            EXPECTED.to_vec(),
            "{col}: …and that shared order must be TimeType BYTE_ORDER, not \
             merely shared"
        );

        let whole_decoded = decoded_order(&whole_db, col, 4);
        assert_eq!(whole_decoded, decoded_order(&elem_db, col, 4));
        assert_eq!(whole_decoded, EXPECTED.to_vec());
    }
}

/// COMPATIBILITY PIN: with the out-of-range negative removed, the emitted order
/// is the numeric one and is IDENTICAL under both the new and the removed
/// comparator — so this fix moved no in-range on-disk collection ordering.
#[test]
fn in_range_only_collection_order_is_unmoved() {
    assert_serialized_forms();
    let in_range = [T_MID, T_MAX, T_LOW];
    // Every in-range value has a 0x00 sign byte, which is why unsigned byte
    // order, unsigned numeric order and SIGNED numeric order coincide here.
    for &n in &in_range {
        assert_eq!(n.to_be_bytes()[0], 0x00);
    }
    let mut numeric = in_range.to_vec();
    numeric.sort_unstable();

    let (_temp, data_db) = write_one(whole_collection_ops(SET_COL, &in_range));
    assert_eq!(
        on_disk_order(&data_db, &in_range),
        numeric,
        "an all-in-range set<time> must be emitted in numeric order under BOTH \
         the BYTE_ORDER comparator and the removed signed one"
    );
    assert_eq!(decoded_order(&data_db, SET_COL, in_range.len()), numeric);
}

// ===========================================================================
// FROZEN (single-cell) collections — the SECOND caller of the one comparator.
//
// `collection_order`'s module doc claims `compare_collection_elements` is the
// SINGLE shared comparator for BOTH the non-frozen complex-cell path
// (`complex.rs`) AND the frozen `serialize_value` path (`encoding.rs`). The
// cases above exercise only the first. These exercise the second, so the claim
// is covered rather than asserted.
//
// A frozen collection is one simple cell whose whole value is the collection's
// wire form, so there are no cell paths to order: `encoding.rs`'s `Value::Set`
// / `Value::Map` arms sort the elements/entries themselves before serializing.
// ===========================================================================

/// FROZEN whole-value path: ONE `Write` of a `frozen<set<time>>` /
/// `frozen<map<time,text>>` column, which `serialize_value` orders via
/// `compare_collection_elements`.
///
/// The SET case passes a bare `Value::Set` and the MAP case a
/// `Value::Frozen(Value::Map(..))`, covering both spellings a frozen literal
/// arrives in (`serialize_value_into`'s `Value::Frozen` arm unwraps to the same
/// `Value::Map` arm).
fn frozen_ops(column: &str, values: &[i64]) -> Vec<CellOperation> {
    let value = if column == FROZEN_MAP_COL {
        Value::Frozen(Box::new(Value::Map(
            values
                .iter()
                .map(|&n| (Value::Time(n), Value::text(format!("v{n}"))))
                .collect(),
        )))
    } else {
        Value::Set(values.iter().map(|&n| Value::Time(n)).collect())
    };
    vec![CellOperation::Write {
        column: column.to_string(),
        value,
    }]
}

/// FROZEN-UDT path: ONE `Write` of a `frozen<udt>` whose single declared field
/// is a `set<time>` / `map<time,text>`.
///
/// This is the surface that reaches `udt_canon` — see
/// `udt_nested_set_matches_byte_order` for why nothing re-sorts afterwards.
fn udt_ops(column: &str, values: &[i64]) -> Vec<CellOperation> {
    let (type_name, field_name, field_value) = if column == UDT_MAP_COL {
        (
            UDT_MAP_TYPE,
            UDT_MAP_FIELD,
            Value::Map(
                values
                    .iter()
                    .map(|&n| (Value::Time(n), Value::text(format!("v{n}"))))
                    .collect(),
            ),
        )
    } else {
        (
            UDT_SET_TYPE,
            UDT_SET_FIELD,
            Value::Set(values.iter().map(|&n| Value::Time(n)).collect()),
        )
    };
    vec![CellOperation::Write {
        column: column.to_string(),
        value: Value::Frozen(Box::new(Value::Udt(Box::new(UdtValue {
            keyspace: KS.to_string(),
            type_name: type_name.to_string(),
            fields: vec![UdtField {
                name: field_name.to_string(),
                value: Some(field_value),
            }],
        })))),
    }]
}

// ===========================================================================
// LEG 1b (PRIMARY, for the frozen shapes) — parse the collection's own wire
// block out of the raw `Data.db`.
//
// A frozen collection (whether a top-level cell or a UDT field) is serialized
// as `[4-byte BE count]` followed by, per element, `[4-byte BE length][bytes]`
// (and, for a map, the value's own length-prefixed bytes after each key). So
// the element sequence is readable STRUCTURALLY from the file, with no reader,
// no comparator and no decode path — the #3042 requirement — and reading it
// this way also proves the located 8-byte patterns really are collection
// ELEMENTS rather than a coincidental byte run somewhere else in the file.
// ===========================================================================

/// The `time` elements/keys of the single frozen collection block of `count`
/// entries in `data_db`, in wire order.
///
/// Every candidate offset whose 4-byte BE word equals `count` is parsed
/// strictly; a candidate that does not parse as `count` length-8 entries is
/// rejected. EXACTLY ONE candidate must parse — asserted, not assumed, so an
/// ambiguous file fails loudly instead of silently taking a first match.
fn frozen_block_order(data_db: &Path, count: usize, is_map: bool) -> Vec<i64> {
    let bytes = std::fs::read(data_db).unwrap_or_else(|e| panic!("read {data_db:?}: {e}"));
    let be32 = |at: usize| -> Option<i64> {
        let w: [u8; 4] = bytes.get(at..at + 4)?.try_into().ok()?;
        Some(i32::from_be_bytes(w) as i64)
    };
    let mut parses: Vec<(usize, Vec<i64>)> = Vec::new();
    for start in 0..bytes.len() {
        if be32(start) != Some(count as i64) {
            continue;
        }
        let mut at = start + 4;
        let mut nanos: Vec<i64> = Vec::with_capacity(count);
        for _ in 0..count {
            // Key/element: must be a length-8 `time`.
            if be32(at) != Some(8) {
                break;
            }
            let Some(raw) = bytes.get(at + 4..at + 12) else {
                break;
            };
            let Ok(word) = <[u8; 8]>::try_from(raw) else {
                break;
            };
            nanos.push(i64::from_be_bytes(word));
            at += 12;
            if is_map {
                // Map value: `v<nanos>` ASCII text, length-prefixed.
                let Some(len) = be32(at) else { break };
                if !(1..=64).contains(&len) {
                    break;
                }
                let len = len as usize;
                let Some(val) = bytes.get(at + 4..at + 4 + len) else {
                    break;
                };
                if !val.iter().all(|b| b.is_ascii_graphic()) {
                    break;
                }
                at += 4 + len;
            }
        }
        if nanos.len() == count {
            parses.push((start, nanos));
        }
    }
    assert_eq!(
        parses.len(),
        1,
        "expected EXACTLY ONE parsable {}-entry frozen collection block in \
         {data_db:?} (a zero or ambiguous parse makes the order unreadable, \
         never a pass); found {:?}",
        count,
        parses.iter().map(|(o, n)| (o, n)).collect::<Vec<_>>()
    );
    parses.remove(0).1
}

/// FROZEN `frozen<set<time>>` — the frozen `serialize_value` caller of
/// `compare_collection_elements` (issue #3935, rust-reviewer gap I1).
#[test]
fn frozen_set_matches_byte_order() {
    assert_serialized_forms();
    let (_temp, data_db) = write_one(frozen_ops(FROZEN_SET_COL, &INSERTION_ORDER));
    assert_eq!(
        on_disk_order(&data_db, &INSERTION_ORDER),
        EXPECTED.to_vec(),
        "frozen<set<time>>: `serialize_value`'s Value::Set arm must order \
         elements by TimeType BYTE_ORDER (TimeType.java:48). Signed order would \
         be {OLD_SIGNED_ORDER:?}"
    );
    // Independent structural read of the collection's own wire block.
    assert_eq!(
        frozen_block_order(&data_db, EXPECTED.len(), false),
        EXPECTED.to_vec(),
        "frozen<set<time>>: the serialized [count][len][elem].. block must \
         carry the elements in BYTE_ORDER"
    );
}

/// FROZEN `frozen<map<time,text>>`, whose ordering value is the KEY. Passed as
/// `Value::Frozen(Value::Map(..))` so the `Value::Frozen` unwrap arm is covered
/// too.
#[test]
fn frozen_map_matches_byte_order() {
    assert_serialized_forms();
    let (_temp, data_db) = write_one(frozen_ops(FROZEN_MAP_COL, &INSERTION_ORDER));
    assert_eq!(
        on_disk_order(&data_db, &INSERTION_ORDER),
        EXPECTED.to_vec(),
        "frozen<map<time,text>>: keys must be emitted in TimeType BYTE_ORDER. \
         Signed order would be {OLD_SIGNED_ORDER:?}"
    );
    assert_eq!(
        frozen_block_order(&data_db, EXPECTED.len(), true),
        EXPECTED.to_vec()
    );
}

// ===========================================================================
// THE SECOND WRITER-SIDE SORTED-COLLECTION COMPARATOR (rust-reviewer B1).
//
// `data_writer/udt_canon` carries its OWN comparator, driven by the declared
// MARSHAL rather than by the `Value` variant: `classify_comparator` +
// `compare_for_marshal`, used by `sort_sorted_collection` to order a frozen
// `SetType`'s elements / a `MapType`'s keys while canonicalizing a UDT value
// against its declared type. It classified `TimeType` as `SignedInt` alongside
// `TimestampType` — the SAME defect #3935 fixed in `collection_order`.
//
// WHY IT REACHES DISK (the part that makes it a defect and not a dead branch):
//
//   * a `frozen<udt>` column is a SIMPLE cell (`is_complex_column` is false for
//     `FrozenType(...)`), so `complex.rs` calls `canonicalize_udt_value` and
//     then serializes the returned value;
//   * `serialize_value` routes `Value::Udt` to `TypeSerializer::serialize_udt`,
//     which serializes each field with `serialize_typed_value` ->
//     `serialize_set` / `serialize_map` -> `serialize_collection_elements`, a
//     bare `for elem in elements` with NO sort;
//   * so for a UDT FIELD, `sort_sorted_collection`'s order is the on-disk order.
//
// A TOP-LEVEL `frozen<set<time>>` MASKS this, because `encoding.rs`'s
// `Value::Set` arm re-sorts with `compare_collection_elements` — which is why
// `frozen_set_matches_byte_order` above passed both before and after the
// `udt_canon` fix, and why this case is the one that pins it.
// ===========================================================================

/// A `set<time>` field of a frozen UDT must be emitted in `TimeType`'s
/// BYTE_ORDER — the `udt_canon` comparator's own fix (rust-reviewer B1).
#[test]
fn udt_nested_set_matches_byte_order() {
    assert_serialized_forms();
    let (_temp, data_db) = write_one(udt_ops(UDT_SET_COL, &INSERTION_ORDER));
    let raw = on_disk_order(&data_db, &INSERTION_ORDER);
    assert_eq!(
        raw,
        EXPECTED.to_vec(),
        "frozen<udt> field set<time>: `udt_canon::sort_sorted_collection` is \
         the ONLY sort on this path (serialize_collection_elements does not \
         re-sort), so it must order by TimeType BYTE_ORDER (TimeType.java:48). \
         Signed order would be {OLD_SIGNED_ORDER:?}"
    );
    assert_ne!(
        raw,
        OLD_SIGNED_ORDER.to_vec(),
        "negative control: the emitted order must not be the signed one"
    );
    assert_eq!(
        frozen_block_order(&data_db, EXPECTED.len(), false),
        EXPECTED.to_vec(),
        "the UDT field's serialized [count][len][elem].. block must carry the \
         elements in BYTE_ORDER"
    );
}

/// Same for a `map<time,text>` field of a frozen UDT, whose ordering value is
/// the KEY (`sort_sorted_collection` keyed on the entry key).
#[test]
fn udt_nested_map_matches_byte_order() {
    assert_serialized_forms();
    let (_temp, data_db) = write_one(udt_ops(UDT_MAP_COL, &INSERTION_ORDER));
    assert_eq!(
        on_disk_order(&data_db, &INSERTION_ORDER),
        EXPECTED.to_vec(),
        "frozen<udt> field map<time,text>: keys must be emitted in TimeType \
         BYTE_ORDER. Signed order would be {OLD_SIGNED_ORDER:?}"
    );
    assert_eq!(
        frozen_block_order(&data_db, EXPECTED.len(), true),
        EXPECTED.to_vec()
    );
}

/// COMPATIBILITY PIN for the `udt_canon` comparator: with the out-of-range
/// negative removed, a UDT-nested `set<time>` is emitted in numeric order —
/// identical under BOTH the corrected BYTE_ORDER classification and the removed
/// `SignedInt` one. So that fix moved no in-range on-disk ordering either.
#[test]
fn udt_nested_in_range_order_is_unmoved() {
    assert_serialized_forms();
    let in_range = [T_MID, T_MAX, T_LOW];
    for &n in &in_range {
        assert_eq!(n.to_be_bytes()[0], 0x00);
    }
    let mut numeric = in_range.to_vec();
    numeric.sort_unstable();

    let (_temp, data_db) = write_one(udt_ops(UDT_SET_COL, &in_range));
    assert_eq!(
        on_disk_order(&data_db, &in_range),
        numeric,
        "an all-in-range UDT-nested set<time> must be emitted in numeric order \
         under BOTH classifications"
    );
    assert_eq!(frozen_block_order(&data_db, in_range.len(), false), numeric);
}
