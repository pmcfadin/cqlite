//! The #4158 invariant on the PASS-THROUGH route (#4104, roborev job 121).
//!
//! `TableSchema::data_type` is a `String`, so a supplied schema can declare a
//! column type that is ALREADY a marshal string. [`super::render_marshal_type`]
//! returns such a string verbatim — deliberately (#929) — which meant an input
//! spelling `FrozenType(<scalar>)` was written into `Statistics.db` without ever
//! meeting the guard that exists to prevent exactly that spelling.
//!
//! Every expectation here derives from Apache Cassandra, never from CQLite's
//! prior output: only four marshal types carry the `includeFrozenType`
//! branch that PRINTS the wrapper — `ListType.java:195-207`,
//! `SetType.java:185-197`, `MapType.java:310-321`, `UserType.java:436-448`
//! (pinned `cassandra-5.0.8`) — and `CQL3Type.Raw::freeze()` throws for every
//! type that does not override it (`CQL3Type.java:647-651`). The corpus-level
//! oracle for the same rule is
//! `tests/issue_4158_frozen_wrapper_cassandra_oracle.rs`.

use super::{cql_type_to_marshal_type, cql_type_to_marshal_type_or_bytes};

const P: &str = "org.apache.cassandra.db.marshal.";

/// A `FrozenType(...)` marshal string around `inner`, package-qualified at both
/// levels exactly as `AbstractType::toString()` writes it.
fn frozen(inner: &str) -> String {
    format!("{P}FrozenType({P}{inner})")
}

/// The four wrappable heads, as pass-through strings the writer must emit
/// UNCHANGED — a narrowing here would be an over-refusal of a header Cassandra
/// really does write (the corpus has `MapType`, `ListType`, `UserType`,
/// `SetType` inners).
#[test]
fn the_four_wrappable_heads_pass_through_verbatim() {
    for inner in [
        "ListType(org.apache.cassandra.db.marshal.Int32Type)",
        "SetType(org.apache.cassandra.db.marshal.UTF8Type)",
        "MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)",
        "UserType(test_oa,61646472,6331:org.apache.cassandra.db.marshal.UTF8Type)",
    ] {
        let spelling = frozen(inner);
        let emitted = cql_type_to_marshal_type(&spelling)
            .unwrap_or_else(|e| panic!("Cassandra writes FrozenType({inner}); refusing it \
                                        would make a real table unwritable: {e}"));
        assert_eq!(
            emitted, spelling,
            "an already-marshaled type is emitted BYTE FOR BYTE — the marshal grammar is \
             case-sensitive, and a case-folded pass-through is a defect this file has had \
             before (a lowercased `usertype(...)`, #4158)"
        );
    }
}

/// THE DEFECT: a pass-through freezing a type Cassandra cannot wrap must be
/// REFUSED, not emitted.
///
/// Before the post-condition in [`cql_type_to_marshal_type`] every one of these
/// was returned verbatim and stamped into `Statistics.db`.
#[test]
fn a_passthrough_freezing_a_non_wrappable_type_is_refused() {
    for inner in [
        // scalars — the #4158 report's own shape, and the CQL3Type rule's core
        "Int32Type",
        "BytesType",
        "UTF8Type",
        // already-frozen / never-wrapped parametric types
        "TupleType(org.apache.cassandra.db.marshal.Int32Type)",
        "VectorType(org.apache.cassandra.db.marshal.FloatType , 3)",
        // a wrapper cannot nest: everything under one prints with
        // `ignoreFreezing = true` and prints no wrapper of its own
        "FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))",
    ] {
        let spelling = frozen(inner);
        let err = match cql_type_to_marshal_type(&spelling) {
            Err(e) => e.to_string(),
            Ok(emitted) => panic!(
                "the writer emitted '{emitted}', a FrozenType(...) wrapper no Cassandra \
                 writer can print (only ListType/SetType/MapType/UserType carry the \
                 includeFrozenType branch)"
            ),
        };
        assert!(
            err.contains(&spelling),
            "the refusal must name the string it refused: {err}"
        );
        assert!(
            err.contains("CQL3Type.java:647-651"),
            "the refusal must carry its Cassandra citation: {err}"
        );
    }
}

/// The post-condition is on the string EMITTED, so a pass-through reached
/// through the collection recursion is caught at whatever depth it sits — the
/// property a per-arm check could not have.
#[test]
fn a_nested_passthrough_freezing_a_scalar_is_refused_too() {
    for declared in [
        format!("list<{}>", frozen("Int32Type")),
        format!("map<text, {}>", frozen("Int32Type")),
        format!("tuple<int, {}>", frozen("Int32Type")),
    ] {
        assert!(
            cql_type_to_marshal_type(&declared).is_err(),
            "a FrozenType(<scalar>) nested inside '{declared}' still reaches the header, \
             so it must still be refused"
        );
    }
}

/// Control: the CQL spellings the writer BUILDS itself are untouched by the
/// post-condition — it refuses only what Cassandra could not have printed.
#[test]
fn the_postcondition_leaves_the_writer_s_own_renderings_alone() {
    for (declared, expected) in [
        (
            "frozen<list<int>>",
            format!("{P}FrozenType({P}ListType({P}Int32Type))"),
        ),
        (
            "frozen<set<text>>",
            format!("{P}FrozenType({P}SetType({P}UTF8Type))"),
        ),
        ("frozen<int>", format!("{P}Int32Type")),
        ("frozen<tuple<int>>", format!("{P}TupleType({P}Int32Type)")),
        ("list<int>", format!("{P}ListType({P}Int32Type)")),
    ] {
        let emitted = cql_type_to_marshal_type(declared)
            .unwrap_or_else(|e| panic!("'{declared}' must still render: {e}"));
        assert_eq!(emitted, expected, "rendering of '{declared}'");
    }
}

/// The UDT-FIELD path's disposition, stated in
/// [`cql_type_to_marshal_type_or_bytes`]'s doc and pinned here: it DEGRADES where
/// the column path refuses.
///
/// That renderer is infallible by signature, and the invariant is about what is
/// EMITTED — `BytesType` is a legal spelling, so the impossible wrapper is
/// DROPPED rather than written, and the field keeps the header/value
/// self-consistency the direct-write blob path relies on (#929/#1011).
#[test]
fn a_udt_field_freezing_a_scalar_takes_the_declared_bytestype_degradation() {
    let emitted = cql_type_to_marshal_type_or_bytes(&frozen("Int32Type"));
    assert_eq!(
        emitted,
        format!("{P}BytesType"),
        "the UDT-field renderer degrades; what it must NOT do is emit the wrapper"
    );
    assert!(
        !emitted.contains("FrozenType"),
        "the degradation exists to DROP the impossible wrapper: {emitted}"
    );
}
