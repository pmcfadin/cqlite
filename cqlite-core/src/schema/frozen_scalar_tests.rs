//! Pinned refusals for `frozen<scalar>` at BOTH metadata entry points (#4104).
//!
//! # Every expectation below is derived from Cassandra, never from CQLite
//!
//! `cassandra-5.0.8:src/java/org/apache/cassandra/cql3/CQL3Type.java:647-651`:
//!
//! ```text
//!         public Raw freeze()
//!         {
//!             String message = String.format("frozen<> is only allowed on collections, tuples, and user-defined types (got %s)", this);
//!             throw new InvalidRequestException(message);
//!         }
//! ```
//!
//! That is the BASE `CQL3Type.Raw` implementation, and `cassandra-5.0.8:src/antlr/
//! Parser.g:1853-1859` routes every `frozen<…>` through it:
//!
//! ```text
//!     | K_FROZEN '<' f=comparatorType '>'
//!       { try { $t = f.freeze(); } catch (InvalidRequestException e) { addRecognitionError(e.getMessage()); } }
//! ```
//!
//! The overrides — and therefore the entire ACCEPT set asserted here — are
//! `RawCollection` (`:777`), `RawVector` (`:916`), `RawUT` (`:958`) and `RawTuple`
//! (`:1037`). Nothing here records what CQLite used to do; the two lanes that
//! pinned a *decode result* for `frozen<int>` (#3847/PR #4017 -> `Value::blob(b"")`,
//! #3805/PR #4033 -> `Value::Empty(Int)`) were both deleted by the `REQ-3805-14`
//! ruling precisely because they had no oracle.

use super::*;
use crate::schema::CqlType;

/// Every native scalar spelling of `frozen<…>` is REFUSED by the CQL type parser.
///
/// The list is `CQL3Type.Native`'s constants (`CQL3Type.java`'s `Native` enum), each
/// of which is carried by `RawType` and therefore reaches the throwing base
/// `freeze()`. `varchar` is the `UTF8Type` alias `TypeParser` resolves.
#[test]
fn a_frozen_native_scalar_is_refused_by_the_cql_type_parser() {
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
    assert_eq!(
        NATIVE_SCALARS.len(),
        21,
        "case floor: an emptied or truncated list would pass vacuously"
    );
    for scalar in NATIVE_SCALARS {
        let spelling = format!("frozen<{scalar}>");
        let err = CqlType::parse(&spelling)
            .err()
            .unwrap_or_else(|| panic!("`{spelling}` is not declarable CQL and must be refused"));
        let msg = err.to_string();
        assert!(
            msg.contains("CQL3Type.java:647-651"),
            "the refusal must cite its oracle, got: {msg}"
        );
        assert!(
            msg.contains(*scalar),
            "the refusal must name the type that cannot be frozen, got: {msg}"
        );
    }
}

/// Case-insensitivity is Cassandra's, not a convenience: CQL type keywords are
/// case-insensitive, so `FROZEN<INT>` is the same declaration and the same refusal.
#[test]
fn the_cql_refusal_is_case_insensitive() {
    for spelling in ["FROZEN<INT>", "Frozen<Int>", "frozen< int >"] {
        assert!(
            CqlType::parse(spelling).is_err(),
            "`{spelling}` is the same non-declarable type as `frozen<int>`"
        );
    }
}

/// A frozen scalar is refused WHEREVER it appears — as a map key, a map value, a
/// collection element, a tuple field, and nested inside another `frozen<>`.
///
/// Cassandra gets this for free: `comparatorType` is one grammar rule, so the
/// `freeze()` call is reached identically at every position (`Parser.g:1853-1859`).
#[test]
fn a_frozen_scalar_is_refused_at_every_position() {
    for spelling in [
        "map<frozen<int>, int>",
        "map<int, frozen<int>>",
        "list<frozen<int>>",
        "set<frozen<text>>",
        "tuple<int, frozen<int>>",
        "frozen<frozen<int>>",
        "frozen<map<frozen<int>, int>>",
        "list<list<frozen<blob>>>",
    ] {
        assert!(
            CqlType::parse(spelling).is_err(),
            "`{spelling}` embeds a frozen scalar and must be refused"
        );
    }
}

/// The ACCEPT set — the four `freeze()` overrides — must still parse.
///
/// This is the half that makes the refusal a rule rather than a ban on the keyword.
#[test]
fn frozen_over_a_collection_tuple_or_udt_still_parses() {
    for spelling in [
        // RawCollection (`CQL3Type.java:777`)
        "frozen<list<int>>",
        "frozen<set<text>>",
        "frozen<map<text, int>>",
        // RawTuple (`:1037`)
        "frozen<tuple<int, text>>",
        // RawUT (`:958`) — both CQLite spellings of an unresolved UDT reference
        "frozen<address_type>",
        "frozen<address>",
        "frozen<ks.address_type>",
        // An already-frozen collection: `RawCollection::freeze` returns a frozen
        // RawCollection, so freezing it again is legal.
        "frozen<frozen<list<int>>>",
        // Nested, both ways round.
        "map<frozen<list<int>>, int>",
        "list<frozen<address_type>>",
    ] {
        assert!(
            CqlType::parse(spelling).is_ok(),
            "`{spelling}` is declarable CQL and must parse"
        );
    }
}

/// A VECTOR is freezable, and `CqlType` cannot model one — so the gate has to
/// decide it by SPELLING or it refuses declarable CQL.
///
/// `RawVector::freeze` (`CQL3Type.java:916-920`) returns `this`; a vector is
/// implicitly frozen (`isImplicitlyFrozen`, `:632-635`). `CqlType` has no `Vector`
/// variant, so `vector<float, 3>` parses to `Custom` — the same arm that carries an
/// unresolved UDT reference — and an `is_udt_identifier`-only rule would have
/// refused `frozen<vector<float, 3>>` because the name contains `<`.
///
/// Pinned in BOTH spellings, because the marshal allowlist naming `VectorType` and
/// the CQL rule are one rule and must not disagree.
#[test]
fn a_frozen_vector_is_accepted_in_both_spellings() {
    for spelling in [
        "frozen<vector<float, 3>>",
        "frozen<VECTOR<float,3>>",
        "list<frozen<vector<float, 3>>>",
    ] {
        assert!(
            CqlType::parse(spelling).is_ok(),
            "`{spelling}` is declarable CQL — RawVector overrides freeze()"
        );
    }
    const P: &str = "org.apache.cassandra.db.marshal.";
    assert!(
        validate_marshal_frozen(&format!("{P}FrozenType({P}VectorType({P}FloatType,3))")).is_ok()
    );
}

/// The header sometimes prefixes a comparator with a structural `[` or `(`
/// (roborev jobs 43/48), and `convert_marshal_type_to_cql` strips both. The gate
/// must strip them too: a normalization one reader applies and another does not is
/// how two readers form two opinions about one string.
#[test]
fn the_header_gate_strips_the_same_structural_prefixes_the_converter_does() {
    const P: &str = "org.apache.cassandra.db.marshal.";
    for accepted in [
        format!("{P}FrozenType([{P}SetType({P}Int32Type))"),
        format!("{P}FrozenType(({P}SetType({P}Int32Type)))"),
        format!("[{P}FrozenType({P}SetType({P}Int32Type))"),
    ] {
        assert!(
            validate_marshal_frozen(&accepted).is_ok(),
            "`{accepted}` freezes a SetType under a structural prefix"
        );
    }
    // Stripping the prefix must not smuggle a scalar through.
    for refused in [
        format!("{P}FrozenType([{P}Int32Type)"),
        format!("{P}FrozenType(({P}Int32Type))"),
    ] {
        assert!(
            validate_marshal_frozen(&refused).is_err(),
            "`{refused}` still freezes a scalar"
        );
    }
}

/// A QUOTED custom class is a `RawType` too, so it is refused.
///
/// `Parser.g:1861-1864` builds a `STRING_LITERAL` type as
/// `CQL3Type.Raw.from(new CQL3Type.Custom($s.text))`, i.e. a `RawType`, which does
/// not override `freeze()`. This is the one case that distinguishes CQLite's
/// `Custom` UDT-reference carrier from a genuine custom class.
#[test]
fn a_frozen_quoted_custom_class_is_refused() {
    for spelling in [
        "frozen<'org.apache.cassandra.db.marshal.Int32Type'>",
        "frozen<foo<bar>>",
    ] {
        assert!(
            CqlType::parse(spelling).is_err(),
            "`{spelling}` is a RawType/unknown parameterised type and must be refused"
        );
    }
}

/// The MEMBERSHIP statement itself, one assertion per `CqlType` variant class.
///
/// Asserted directly because [`frozen_inner_supports_freezing`] is the single
/// source of the accept set for both entry points, and an exhaustive `match` with
/// no `_` arm is only half the guard — the other half is that each arm answers the
/// way Cassandra's override set says.
#[test]
fn the_membership_set_is_cassandras_override_set() {
    for freezable in [
        CqlType::List(Box::new(CqlType::Int)),
        CqlType::Set(Box::new(CqlType::Int)),
        CqlType::Map(Box::new(CqlType::Int), Box::new(CqlType::Int)),
        CqlType::Tuple(vec![CqlType::Int]),
        CqlType::Udt("address_type".to_string(), vec![]),
        CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Int)))),
        CqlType::Custom("udt:address_type".to_string()),
        CqlType::Custom("address_type".to_string()),
        // A vector: `CqlType` has no variant for it, and `RawVector` overrides
        // `freeze()` (`CQL3Type.java:916-920`).
        CqlType::Custom("vector<float, 3>".to_string()),
    ] {
        assert!(
            frozen_inner_supports_freezing(&freezable),
            "{freezable:?} overrides freeze() in Cassandra"
        );
    }
    for scalar in [
        CqlType::Boolean,
        CqlType::TinyInt,
        CqlType::SmallInt,
        CqlType::Int,
        CqlType::BigInt,
        CqlType::Counter,
        CqlType::Float,
        CqlType::Double,
        CqlType::Decimal,
        CqlType::Text,
        CqlType::Ascii,
        CqlType::Varchar,
        CqlType::Blob,
        CqlType::Timestamp,
        CqlType::Date,
        CqlType::Time,
        CqlType::Uuid,
        CqlType::TimeUuid,
        CqlType::Inet,
        CqlType::Duration,
        CqlType::Varint,
        // A `Custom` that cannot name a UDT is a quoted custom class, i.e. a
        // `RawType`.
        CqlType::Custom("'org.apache.cassandra.db.marshal.Int32Type'".to_string()),
        CqlType::Custom("foo<bar>".to_string()),
    ] {
        assert!(
            !frozen_inner_supports_freezing(&scalar),
            "{scalar:?} reaches the throwing base freeze()"
        );
    }
}

// ══════════════════ THE SECOND ENTRY POINT: the SerializationHeader ══════════════

/// `FrozenType(<scalar>)` in a `Statistics.db` SerializationHeader is refused.
///
/// No Cassandra writer can emit it: the header records `column.type`, and no column
/// can have been declared `frozen<int>` in the first place (`CQL3Type.java:647-651`).
/// Both the canonical package-qualified spelling and the bare simple name are
/// pinned, because `convert_marshal_type_to_cql` accepts both.
#[test]
fn a_frozen_scalar_serialization_header_type_is_refused() {
    const P: &str = "org.apache.cassandra.db.marshal.";
    for scalar in [
        "Int32Type",
        "LongType",
        "UTF8Type",
        "BytesType",
        "UUIDType",
        "InetAddressType",
        "SimpleDateType",
        "DecimalType",
        "BooleanType",
    ] {
        for spelling in [
            format!("{P}FrozenType({P}{scalar})"),
            format!("FrozenType({scalar})"),
            // A frozen scalar as a MULTICELL map's key type.
            format!("{P}MapType({P}FrozenType({P}{scalar}),{P}Int32Type)"),
            // …as a collection element.
            format!("{P}ListType({P}FrozenType({P}{scalar}))"),
            // …and as a UDT FIELD, the position a leading-prefix check misses
            // because `convert_marshal_type_to_cql` returns a UserType-bearing
            // string verbatim.
            format!("{P}UserType(ks,6e,66:{P}FrozenType({P}{scalar}))"),
        ] {
            let err = validate_marshal_frozen(&spelling).err().unwrap_or_else(|| {
                panic!("no Cassandra writer can emit `{spelling}`; it must be refused")
            });
            assert!(
                err.to_string().contains("CQL3Type.java:647-651"),
                "the refusal must cite its oracle, got: {err}"
            );
        }
    }
}

/// The header ACCEPT set — measured on the real corpus, and it is exactly the
/// override set.
///
/// A census of every `FrozenType(` occurrence in the 310 `Statistics.db`/`Data.db`
/// files this box holds found four inner heads and no others: `MapType` (25),
/// `ListType` (16), `UserType` (10), `SetType` (9). The recipe is in this module's
/// parent doc. `TupleType`/`VectorType`/nested `FrozenType` are admitted from the
/// override set rather than from that census — an absence in one corpus is not an
/// impossibility.
#[test]
fn a_frozen_collection_tuple_udt_or_vector_header_type_is_accepted() {
    const P: &str = "org.apache.cassandra.db.marshal.";
    for spelling in [
        format!("{P}FrozenType({P}MapType({P}Int32Type,{P}Int32Type))"),
        format!("{P}FrozenType({P}ListType({P}Int32Type))"),
        format!("{P}FrozenType({P}SetType({P}Int32Type))"),
        format!("{P}FrozenType({P}UserType(ks,6e,66:{P}Int32Type))"),
        format!("{P}FrozenType({P}TupleType({P}Int32Type,{P}UTF8Type))"),
        format!("{P}FrozenType({P}VectorType({P}FloatType,3))"),
        format!("{P}FrozenType({P}FrozenType({P}SetType({P}Int32Type)))"),
        // The frozen wrapper on a map KEY, which is where a frozen UDT really lands.
        format!("{P}MapType({P}FrozenType({P}UserType(ks,6e,66:{P}Int32Type)),{P}Int32Type)"),
        // No frozen wrapper at all.
        format!("{P}MapType({P}Int32Type,{P}Int32Type)"),
        format!("{P}Int32Type"),
        String::new(),
    ] {
        assert!(
            validate_marshal_frozen(&spelling).is_ok(),
            "`{spelling}` is a type Cassandra can and does write"
        );
    }
}

/// The header gate FAILS CLOSED on every shape it cannot read, rather than
/// admitting it.
///
/// An unbalanced parenthesis and an empty inner are both unmeasurable, and a
/// non-canonical package is a class this crate has no authority over — the package
/// rule `row_decoder::udt::marshal_name` enforces for the same reason (#28,
/// roborev job 76: a third-party `com.acme.Int32Type` decoded as CQL `int`).
#[test]
fn the_header_gate_fails_closed_on_what_it_cannot_read() {
    const P: &str = "org.apache.cassandra.db.marshal.";
    for spelling in [
        // Unbalanced.
        format!("{P}FrozenType({P}SetType({P}Int32Type)"),
        "FrozenType(".to_string(),
        // Empty inner.
        format!("{P}FrozenType()"),
        // A foreign package wearing a freezable simple name.
        "com.acme.FrozenType(com.acme.SetType(com.acme.Int32Type))".to_string(),
        format!("{P}FrozenType(com.acme.SetType(com.acme.Int32Type))"),
        // A head that is neither freezable nor a known scalar.
        format!("{P}FrozenType({P}ReversedType({P}Int32Type))"),
        format!("{P}FrozenType({P}CompositeType({P}Int32Type))"),
    ] {
        assert!(
            validate_marshal_frozen(&spelling).is_err(),
            "`{spelling}` is not a type Cassandra can write and must not be admitted"
        );
    }
}
