//! `frozen<…>`-spelled CELL-PATH KEYS: the REACHABILITY claim (#4104), and the
//! record of the two pins that were deleted to get here (#3847/#3805).
//!
//! A campsite split of `cell_path_key_tests.rs` (#1135), which sits at its
//! 1500-line threshold. Kept under `complex_column` rather than beside the rest of
//! the #3847 key work in `frozen_map`, because `parse_cell_path_key` is
//! `pub(super)` HERE: a sibling module cannot call it.
//!
//! # THE ORACLE IS CASSANDRA'S GRAMMAR, NOT ITS BYTES
//!
//! `CQL3Type.Raw::freeze()` is the base implementation and does nothing but throw
//! (`cassandra-5.0.8:src/java/org/apache/cassandra/cql3/CQL3Type.java:647-651`):
//!
//! ```text
//!         public Raw freeze()
//!         {
//!             String message = String.format("frozen<> is only allowed on collections, tuples, and user-defined types (got %s)", this);
//!             throw new InvalidRequestException(message);
//!         }
//! ```
//!
//! and `cassandra-5.0.8:src/antlr/Parser.g:1853-1859` routes every `frozen<…>`
//! through it, turning the `InvalidRequestException` into a recognition error.
//! Only `RawCollection` (`:777`), `RawVector` (`:916`), `RawUT` (`:958`) and
//! `RawTuple` (`:1037`) override it. So no table can carry `frozen<int>`, no
//! serialization header can spell `FrozenType(Int32Type)`, and **no
//! Cassandra-written bytes for this input exist BY CONSTRUCTION.**
//!
//! # WHAT WAS DELETED, AND WHY IT COULD NOT BE FIXED INSTEAD
//!
//! #3805/#4017 CROSS-LANE COLLISION, RULED BY THE LEAD ON PR #4033: this module's
//! only case (`an_empty_frozen_spelled_fixed_width_key_is_also_preserved_opaquely`,
//! asserting `Blob(b"")` + `opaque_out`) and #3805's opposite pin (asserting
//! `Empty(Int)`) were BOTH DELETED. Under #28, where Cassandra has no behaviour
//! CQLite must not invent one — so both answers were inventions and neither was
//! "the" value to fix the other to. The correct behaviour is REFUSAL, and #4104
//! implements it at the two metadata entry points.
//!
//! Deleted here in the same spirit, and NOT relocated: every frozen-SCALAR case in
//! `cell_path_key_tests.rs` (the finding-B1 width table for
//! `frozen<int>`/`frozen<inet>`/`frozen<uuid>`/`frozen<smallint>`/`Frozen<BIGINT>`
//! and their `FrozenType(…)` marshal twins, and the three `frozen<blob>`
//! declared-blob cases). Each pinned an outcome for a string that can no longer
//! arrive; keeping one would re-assert an invention in a new place.
//!
//! # WHAT THIS MODULE PINS INSTEAD
//!
//! Not a decode outcome — a REACHABILITY claim. The declared key type handed to
//! `parse_cell_path_key_reporting` is produced by `map_key_type_for_decode`, which
//! returns exactly one of two things (see its doc): the `Statistics.db` marshal
//! form, when it is UDT-bearing, or the schema's CQL short form. Those are the two
//! metadata entry points #4104 gates, so driving BOTH producers with every
//! frozen-scalar spelling and finding each refuses is what makes the decoder's
//! former frozen-scalar tolerance dead rather than merely unused.
//!
//! # WHAT SURVIVES UNTOUCHED
//!
//! #4017's DOOR-2 fix — keying the decode-succeeded-with-`Null` check on the
//! PEELED probe rather than on `decoded`. That is a VALUE-side peel and remains
//! load-bearing for the LEGAL frozen families (a `frozen<absent_udt>` key comes
//! back as `Frozen(Blob)`), which #3805's admission gate never intercepts. Its
//! argument, and the four defects one root cause produced, are documented in
//! `row_decoder::frozen_map`. What #4104 removed is the TYPE-STRING peel in
//! `cell_path_key_allowed_widths`, `cell_path_key_declares_blob` and
//! `cell_path_key_cql_type`, each of which could only ever change an answer for a
//! frozen scalar.
//!
//! No dataset or feature-flag dependency: both producers are pure functions of a
//! type string, so these run in every build and lane and cannot pass vacuously on
//! an empty corpus.

use crate::schema::{validate_marshal_frozen, CqlType};

const MARSHAL: &str = "org.apache.cassandra.db.marshal";

/// The CQL short forms `CQL3Type.Native` admits, each of which is carried by
/// `RawType` and therefore reaches the throwing base `freeze()`.
const NATIVE_SCALARS: &[&str] = &[
    "ascii",
    "bigint",
    "blob",
    "boolean",
    "counter",
    "date",
    "decimal",
    "double",
    "duration",
    "float",
    "inet",
    "int",
    "smallint",
    "text",
    "time",
    "timestamp",
    "timeuuid",
    "tinyint",
    "uuid",
    "varchar",
    "varint",
];

/// The marshal classes those scalars are recorded as in a SerializationHeader.
const NATIVE_SCALAR_MARSHALS: &[&str] = &[
    "AsciiType",
    "LongType",
    "BytesType",
    "BooleanType",
    "CounterColumnType",
    "SimpleDateType",
    "DecimalType",
    "DoubleType",
    "DurationType",
    "FloatType",
    "InetAddressType",
    "Int32Type",
    "ShortType",
    "UTF8Type",
    "TimeType",
    "TimestampType",
    "TimeUUIDType",
    "ByteType",
    "UUIDType",
    "VarcharType",
    "IntegerType",
];

/// NO FROZEN-SCALAR KEY TYPE CAN REACH THE CELL-PATH KEY DECODER.
///
/// `map_key_type_for_decode` hands `parse_cell_path_key_reporting` either the
/// header's marshal form or the schema's CQL short form and nothing else, so it is
/// sufficient — and necessary — to show that BOTH producers refuse every
/// frozen-scalar spelling. Asserting a decode outcome instead would be asserting
/// an invention; this asserts an impossibility.
///
/// Every spelling is checked against BOTH producers rather than against the one
/// that "owns" it, because the two spellings are not partitioned by producer: a
/// hand-written schema can carry a marshal class name, and `cell_path_key_cql_type`
/// routes on `contains(marshal package)` for exactly that reason.
#[test]
fn no_frozen_scalar_key_type_can_reach_the_cell_path_key_decoder() {
    assert_eq!(
        (NATIVE_SCALARS.len(), NATIVE_SCALAR_MARSHALS.len()),
        (21, 21),
        "case floor: an emptied list would make this test pass having checked nothing"
    );

    // ── Producer 1: the schema's CQL short form (`CqlType::parse`) ──
    for scalar in NATIVE_SCALARS {
        for spelling in [
            format!("frozen<{scalar}>"),
            // Case-insensitively: CQL type keywords are.
            format!("FROZEN<{}>", scalar.to_uppercase()),
            // Nested, and at the positions a MAP KEY actually occupies.
            format!("frozen<frozen<{scalar}>>"),
        ] {
            assert!(
                CqlType::parse(&spelling).is_err(),
                "`{spelling}` must be refused by the schema entry point, so it can \
                 never become a cell-path key type"
            );
        }
    }

    // ── Producer 2: the Statistics.db SerializationHeader marshal form ──
    for marshal in NATIVE_SCALAR_MARSHALS {
        for spelling in [
            format!("{MARSHAL}.FrozenType({MARSHAL}.{marshal})"),
            format!("FrozenType({marshal})"),
            // The frozen wrapper on the KEY of a multicell map — the exact shape
            // `map_key_type_for_decode` forwards after stripping ONE outer marker.
            format!(
                "{MARSHAL}.MapType({MARSHAL}.FrozenType({MARSHAL}.{marshal}),{MARSHAL}.Int32Type)"
            ),
        ] {
            assert!(
                validate_marshal_frozen(&spelling).is_err(),
                "`{spelling}` must be refused by the header entry point, so it can \
                 never become a cell-path key type"
            );
        }
    }
}

/// The refusal is a NARROWING, not a ban on the keyword: every LEGAL frozen key
/// type still passes both producers.
///
/// Without this half, deleting the frozen arm outright would also pass — and a
/// frozen UDT map key is the single most common frozen key there is (10 of the
/// corpus's 60 `FrozenType(` occurrences wrap a `UserType`).
#[test]
fn a_legal_frozen_key_type_still_passes_both_producers() {
    for cql in [
        "frozen<list<int>>",
        "frozen<set<text>>",
        "frozen<map<text, int>>",
        "frozen<tuple<int, text>>",
        "frozen<collide>",
        "frozen<test_udt_collision.collide>",
    ] {
        assert!(
            CqlType::parse(cql).is_ok(),
            "`{cql}` is declarable CQL and must reach the decoder"
        );
    }
    for marshal in [
        format!("{MARSHAL}.FrozenType({MARSHAL}.SetType({MARSHAL}.Int32Type))"),
        format!("{MARSHAL}.FrozenType({MARSHAL}.ListType({MARSHAL}.Int32Type))"),
        format!("{MARSHAL}.FrozenType({MARSHAL}.MapType({MARSHAL}.UTF8Type,{MARSHAL}.Int32Type))"),
        format!("{MARSHAL}.FrozenType({MARSHAL}.TupleType({MARSHAL}.Int32Type))"),
        format!("{MARSHAL}.FrozenType({MARSHAL}.UserType(ks,6e,66:{MARSHAL}.Int32Type))"),
        format!(
            "{MARSHAL}.MapType({MARSHAL}.FrozenType({MARSHAL}.UserType(ks,6e,66:{MARSHAL}.Int32Type)),{MARSHAL}.Int32Type)"
        ),
    ] {
        assert!(
            validate_marshal_frozen(&marshal).is_ok(),
            "`{marshal}` is a type Cassandra writes and must reach the decoder"
        );
    }
}
