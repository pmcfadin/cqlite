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
//! Parser.g:1853-1860` routes every `frozen<…>` through it:
//!
//! ```text
//!     | K_FROZEN '<' f=comparatorType '>'
//!       { try { $t = f.freeze(); } catch (InvalidRequestException e) { addRecognitionError(e.getMessage()); } }
//! ```
//!
//! The overrides — and therefore the entire ACCEPT set asserted here — are
//! `RawCollection` (`:773`), `RawVector` (`:916`), `RawUT` (`:958`) and `RawTuple`
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
/// `freeze()` call is reached identically at every position (`Parser.g:1853-1860`).
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
        // RawCollection (`CQL3Type.java:773`)
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

/// A VECTOR is freezable CQL, and its HEADER spelling carries NO wrapper — so the
/// two gates give the same declared type two different answers, on purpose.
///
/// `RawVector::freeze` (`CQL3Type.java:915-919`) returns `this`, so
/// `frozen<vector<float, 3>>` is declarable and the CQL gate must accept it. But
/// `VectorType.toString(ignoreFreezing)` is `getClass().getName() +
/// stringifyVectorParameters(..)` (`VectorType.java:339-342`) with no
/// `includeFrozenType` branch, so Apache Cassandra writes the BARE
/// `VectorType(FloatType , 3)` and can never write `FrozenType(VectorType(..))`.
///
/// THE DEFECT THIS PINS (#4158 review, blocker C): the marshal allowlist admitted
/// `VectorType` as a `FrozenType(` inner "because a vector is freezable", which
/// made the read gate accept a byte string the same PR's writer had just proved
/// impossible. Freezable and frozen-WRAPPED are different questions.
#[test]
fn a_frozen_vector_is_declarable_cql_but_never_a_frozen_wrapped_header() {
    // The full ACCEPT/REFUSE sets for the spelling live in
    // `a_complete_vector_spelling_is_still_accepted` /
    // `an_incomplete_vector_spelling_is_refused`; this case pins the CROSS-SPELLING
    // relationship, which is what the two halves of one rule get wrong.
    assert!(
        CqlType::parse("frozen<vector<float, 3>>").is_ok(),
        "declarable CQL: RawVector overrides freeze() and returns this"
    );
    const P: &str = "org.apache.cassandra.db.marshal.";
    // The spelling Cassandra DOES write for that column — accepted.
    assert!(
        validate_marshal_frozen(&format!("{P}VectorType({P}FloatType , 3)")).is_ok(),
        "the bare VectorType spelling is what Cassandra writes for a frozen vector"
    );
    // The spelling Cassandra CANNOT write — refused.
    let err = validate_marshal_frozen(&format!("{P}FrozenType({P}VectorType({P}FloatType , 3))"))
        .err()
        .unwrap_or_else(|| {
            panic!(
                "no Cassandra writer prints FrozenType(VectorType(..)): \
                 VectorType.toString has no includeFrozenType branch \
                 (VectorType.java:339-342)"
            )
        });
    let msg = err.to_string();
    assert!(
        msg.contains("includeFrozenType") && msg.contains("UserType.java:436-447"),
        "the refusal must cite the WRITER rule it applied (the includeFrozenType \
         branch and the four classes that carry it), got: {err}"
    );
}

/// An INCOMPLETE `vector<..>` spelling is NOT declarable, so it is NOT freezable —
/// and since #4149 the refusal comes from TWO different gates, so the PARTITION is
/// pinned rather than just the outcome.
///
/// roborev job 108 (the original defect): `is_vector_spelling` matched only the head
/// keyword, so every spelling below was granted freezability — a permission
/// Cassandra does not grant, which is this issue's own defect pointed the other way.
///
/// WHAT #4149 CHANGED. `CqlType::parse` now resolves a `vector<..>` spelling through
/// `schema::vector_type::cql_vector_kind` BEFORE the `Custom` fall-through, so a
/// MALFORMED spelling is an `Err` from the vector type parser and never reaches the
/// frozen gate at all. The refusal is therefore still total, but the two halves cite
/// their own oracles, and asserting one citation for all 17 cases would now be
/// asserting something false. Both lists are kept in ONE test because the property is
/// the UNION: no incomplete vector spelling is admitted by either gate.
///
/// The accepted grammar is `Parser.g:1916-1919`
/// (`K_VECTOR '<' comparatorType ',' INTEGER '>'`, i.e. EXACTLY two arguments),
/// `Lexer.g:337-339` (`INTEGER : '-'? DIGIT+`, so digits with at most a leading
/// `-`, and `Integer.parseInt` bounds it to a Java `int`) and
/// `VectorType.java:89-90` (`if (dimension <= 0) throw ... "vectors may only have
/// positive dimensions"`). Every case below violates exactly one of those.
#[test]
fn an_incomplete_vector_spelling_is_refused() {
    // Refused by the SHARED vector type parser (#4149): the spelling IS a
    // `vector<..>`, so `cql_vector_kind` owns it, and its parameters are illegal.
    const REFUSED_BY_THE_VECTOR_PARSER: &[&str] = &[
        // The three roborev job 108 named.
        "frozen<vector<int>>",       // no dimension — one argument, not two
        "frozen<vector<>>",          // no arguments at all
        "frozen<vector<int, nope>>", // dimension is not an INTEGER
        // `VectorType.java:89-90`: the dimension must be POSITIVE. `Lexer.g` admits
        // the `-` spelling, so this is a real reachable string, not a straw man.
        "frozen<vector<int, 0>>",
        "frozen<vector<int, -3>>",
        // `Integer.parseInt` throws past the Java `int` bound (2147483647), so this
        // is a parse error in Cassandra rather than a large vector.
        "frozen<vector<int, 2147483648>>",
        // `INTEGER` is `'-'? DIGIT+` — no `+`, no separators, no radix, no float.
        "frozen<vector<int, +3>>",
        "frozen<vector<int, 3.0>>",
        "frozen<vector<int, 0x3>>",
        "frozen<vector<int, 1_000>>",
        // Arity: three arguments, and an EMPTY one.
        "frozen<vector<int, text, 3>>",
        "frozen<vector<int, , 3>>",
        "frozen<vector<, 3>>",
        "frozen<vector<int, 3,>>",
        // An empty element with a valid dimension.
        "frozen<vector< , 3>>",
        // Unbalanced — the parameter list does not terminate at the type's end.
        "frozen<vector<int, 3>>>",
    ];
    // Refused by the FROZEN gate: not a `vector<..>` at all, so it lands in `Custom`
    // and this module's own membership predicate decides it.
    const REFUSED_BY_THE_FROZEN_GATE: &[&str] = &[
        // The head keyword must be `vector`, not merely end in it.
        "frozen<myvector<int, 3>>",
    ];
    assert_eq!(
        REFUSED_BY_THE_VECTOR_PARSER.len() + REFUSED_BY_THE_FROZEN_GATE.len(),
        17,
        "case floor: an emptied or truncated list would pass vacuously"
    );
    for spelling in REFUSED_BY_THE_VECTOR_PARSER {
        let err = CqlType::parse(spelling)
            .err()
            .unwrap_or_else(|| panic!("`{spelling}` is not declarable CQL and must be refused"));
        assert!(
            err.to_string().contains("malformed vector type"),
            "`{spelling}` must be refused BY THE VECTOR TYPE PARSER, naming the type \
             it could not read, got: {err}"
        );
    }
    for spelling in REFUSED_BY_THE_FROZEN_GATE {
        let err = CqlType::parse(spelling)
            .err()
            .unwrap_or_else(|| panic!("`{spelling}` is not declarable CQL and must be refused"));
        assert!(
            err.to_string().contains("CQL3Type.java:647-651"),
            "the refusal must cite its oracle, got: {err}"
        );
    }
}

/// `frozen<vector<float, 3>>` parses to `Frozen(Vector(Float, 3))` AT THIS HEAD.
///
/// The sibling accept-set tests below only assert `is_ok()`, and they were written
/// when this spelling parsed to `Frozen(Custom("vector<float, 3>"))` — so they would
/// have stayed green through #4149's variant change without noticing that the
/// permission had moved from the `Custom` arm to a brand-new one. This case pins the
/// SHAPE, which is what makes the `CqlType::Vector` arm of
/// [`frozen_inner_supports_freezing`] the thing under test.
#[test]
fn a_frozen_vector_parses_to_a_frozen_vector_variant() {
    let parsed = CqlType::parse("frozen<vector<float, 3>>").expect(
        "frozen<vector<float, 3>> is declarable CQL — RawVector::freeze() returns \
         `this` (CQL3Type.java:915-919)",
    );
    assert_eq!(
        parsed,
        CqlType::Frozen(Box::new(CqlType::Vector(Box::new(CqlType::Float), 3))),
        "the inner must be the #4149 `CqlType::Vector` variant, not a `Custom` \
         spelling carrier"
    );
}

/// A frozen SCALAR nested in a VECTOR ELEMENT is refused as of #4149 — the gap this
/// module's header used to DECLARE as out of scope (issue #4154).
///
/// It was out of scope because `CqlType::parse` could not descend into a vector: the
/// whole `vector<..>` spelling landed in `Custom` and the element was never parsed.
/// #4149's arm parses the element through `parse_with_depth`, which re-enters the
/// frozen gate — so the refusal now reaches this position for free. Pinned here so
/// the coverage cannot silently regress if the element recursion is ever changed;
/// #4154 stays open for whoever audits the remaining positions.
#[test]
fn a_frozen_scalar_in_a_vector_element_is_refused_since_4149() {
    for spelling in [
        "frozen<vector<frozen<int>, 3>>",
        "vector<frozen<int>, 3>",
        "frozen<vector<vector<frozen<text>, 2>, 3>>",
    ] {
        let err = CqlType::parse(spelling)
            .err()
            .unwrap_or_else(|| panic!("`{spelling}` freezes a scalar and must be refused"));
        assert!(
            err.to_string().contains("CQL3Type.java:647-651"),
            "the refusal must cite the frozen-scalar oracle, got: {err}"
        );
    }
}

/// OVER-refusal is as much a defect as under-refusal: a COMPLETE vector spelling
/// must still be accepted.
///
/// Without this half, deleting the vector arm outright would satisfy the test
/// above — and refusing `frozen<vector<float, 3>>` would be refusing declarable
/// CQL, which is worse than the hole job 108 found.
#[test]
fn a_complete_vector_spelling_is_still_accepted() {
    const DECLARABLE: &[&str] = &[
        "frozen<vector<float, 3>>",
        // CQL type keywords are case-insensitive, and whitespace is free.
        "frozen<VECTOR<float,3>>",
        "frozen<vector < float , 3 >>",
        // `Integer.parseInt("03")` is 3; a leading zero is legal.
        "frozen<vector<float, 03>>",
        // The element is a `comparatorType`, so it may itself carry top-level
        // commas — which is why the arity check has to split on `<>` DEPTH.
        "frozen<vector<map<int, text>, 3>>",
        "frozen<vector<tuple<int, text>, 2>>",
        "frozen<vector<vector<float, 3>, 2>>",
        // The Java `int` bound itself is legal.
        "frozen<vector<float, 2147483647>>",
        // And at the positions a vector actually occupies.
        "list<frozen<vector<float, 3>>>",
        "map<frozen<vector<float, 3>>, int>",
    ];
    assert_eq!(
        DECLARABLE.len(),
        10,
        "case floor: an emptied list would make this test assert nothing"
    );
    for spelling in DECLARABLE {
        assert!(
            CqlType::parse(spelling).is_ok(),
            "`{spelling}` is declarable CQL — RawVector overrides freeze() \
             (CQL3Type.java:915-919) — and must not be refused"
        );
    }
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
        // A vector, in BOTH carriers: the #4149 variant, and the `Custom` spelling
        // that carried it before #4149 (now unreachable from `CqlType::parse`, but
        // still a representable `CqlType` this predicate must answer consistently).
        // `RawVector` overrides `freeze()` and returns `this`
        // (`CQL3Type.java:915-919`).
        CqlType::Vector(Box::new(CqlType::Float), 3),
        CqlType::Vector(
            Box::new(CqlType::Map(
                Box::new(CqlType::Int),
                Box::new(CqlType::Text),
            )),
            2,
        ),
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
        // An INCOMPLETE vector is not declarable, so it is not freezable either —
        // asserted here too, at the membership predicate, and not only through
        // `CqlType::parse` (roborev job 108).
        CqlType::Custom("vector<int>".to_string()),
        CqlType::Custom("vector<int, 0>".to_string()),
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

/// The header ACCEPT set — the four classes whose `toString(boolean)` can PRINT a
/// `FrozenType(` wrapper, and nothing else.
///
/// Not "the freezable types": this gate reads bytes a Cassandra WRITER produced, so
/// its set is the writer's (`FROZEN_WRAPPABLE_MARSHAL_SIMPLE_NAMES`, derived
/// whole-tree at `cassandra-5.0.8`), which is also the set the write path uses
/// (`stats_writer::marshal::FREEZE_WRAPPED_HEADS`). The corpus agrees without
/// establishing it: `MapType` 25, `ListType` 16, `UserType` 10, `SetType` 9 over
/// 144 `Statistics.db`, no `TupleType`, no `VectorType`, no nesting.
#[test]
fn a_frozen_collection_or_udt_header_type_is_accepted() {
    const P: &str = "org.apache.cassandra.db.marshal.";
    for spelling in [
        format!("{P}FrozenType({P}MapType({P}Int32Type,{P}Int32Type))"),
        format!("{P}FrozenType({P}ListType({P}Int32Type))"),
        format!("{P}FrozenType({P}SetType({P}Int32Type))"),
        format!("{P}FrozenType({P}UserType(ks,6e,66:{P}Int32Type))"),
        // The frozen wrapper on a map KEY, which is where a frozen UDT really lands.
        format!("{P}MapType({P}FrozenType({P}UserType(ks,6e,66:{P}Int32Type)),{P}Int32Type)"),
        // ONE level of wrapping under a MULTICELL parent is exactly the corpus shape.
        format!("{P}ListType({P}FrozenType({P}SetType({P}Int32Type)))"),
        // No frozen wrapper at all: a tuple and a vector are written BARE.
        format!("{P}TupleType({P}Int32Type,{P}UTF8Type)"),
        format!("{P}VectorType({P}FloatType , 3)"),
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

/// The three wrapper shapes that are GRAMMATICAL, even declarable, and still
/// unwritable — so the gate refuses them (#4158 review, blocker C).
///
/// Each is unprintable for its own reason at the pinned tag, and the reasons are
/// what make this a narrowing rather than a preference:
///  * `FrozenType(TupleType(..))` — `TupleType.toString()` is
///    `getClass().getName() + stringifyTypeParameters(types, true)`
///    (`TupleType.java:557-560`): no `includeFrozenType` branch, and it forces
///    `ignoreFreezing` on its own components. A CQL tuple is already frozen.
///  * `FrozenType(VectorType(..))` — `VectorType.toString(boolean)`
///    (`VectorType.java:339-342`) likewise prints no wrapper.
///  * `FrozenType(FrozenType(..))` — each of the four wrapping types passes
///    `ignoreFreezing || !isMultiCell` to its parameters (`MapType.java:317` et al),
///    so anything already under a wrapper prints with `ignoreFreezing = true` and
///    wraps nothing.
///
/// `frozen<tuple<..>>` and `frozen<vector<..>>` stay DECLARABLE — see
/// `a_frozen_vector_is_declarable_cql_but_never_a_frozen_wrapped_header` and the
/// CQL accept sets above. Only the on-disk spelling is refused.
#[test]
fn a_wrapper_cassandra_cannot_print_is_refused_even_though_the_cql_is_declarable() {
    const P: &str = "org.apache.cassandra.db.marshal.";
    let unwritable = [
        format!("{P}FrozenType({P}TupleType({P}Int32Type,{P}UTF8Type))"),
        format!("{P}FrozenType({P}VectorType({P}FloatType , 3))"),
        format!("{P}FrozenType({P}FrozenType({P}SetType({P}Int32Type)))"),
        // At depth, too: the gate scans EVERY FrozenType( occurrence.
        format!("{P}MapType({P}FrozenType({P}TupleType({P}Int32Type)),{P}Int32Type)"),
        format!("{P}UserType(ks,6e,66:{P}FrozenType({P}VectorType({P}FloatType , 3)))"),
    ];
    assert_eq!(
        unwritable.len(),
        5,
        "case floor: an emptied list would make this test assert nothing"
    );
    for spelling in unwritable {
        let err = validate_marshal_frozen(&spelling).err().unwrap_or_else(|| {
            panic!(
                "`{spelling}` is not printable by any Cassandra writer and must be \
                 refused — see FROZEN_WRAPPABLE_MARSHAL_SIMPLE_NAMES"
            )
        });
        assert!(
            err.to_string().contains("includeFrozenType"),
            "the refusal must cite the writer rule it applied, got: {err}"
        );
    }
    // The corresponding CQL declarations are still accepted: this is a narrowing of
    // the BYTE gate, not a ban on the CQL keyword.
    for cql in [
        "frozen<tuple<int, text>>",
        "frozen<vector<float, 3>>",
        "frozen<frozen<set<int>>>",
    ] {
        assert!(
            CqlType::parse(cql).is_ok(),
            "`{cql}` remains declarable CQL (CQL3Type.java freeze() overrides)"
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

// ══════════════ OVER-REFUSAL: the gate never refuses a byte Cassandra wrote ══════

/// THE NARROWING'S OWN GUARD (#4158 review, blocker C): every `FrozenType(...)`
/// string Apache Cassandra actually wrote into the corpus is ACCEPTED by
/// [`validate_marshal_frozen`].
///
/// A widening can only be fail-open; a NARROWING can be fail-closed on real data,
/// and no other test here can see that — the accept/refuse sets above are hand-written
/// spellings, so they are evidence about the rule and not about Cassandra's bytes.
/// This case is the complement: the strings come from Cassandra-written
/// `Statistics.db` files, and the ONLY expectation is "the read gate does not refuse
/// them". It is a UNIT test because `validate_marshal_frozen` is `pub(crate)` and no
/// integration test can reach it; `tests/issue_4158_frozen_wrapper_cassandra_oracle.rs`
/// censuses the same bytes from the outside for the WRITER side.
///
/// Fixture-gated per repo doctrine: SKIPs when no `*-Statistics.db` is reachable
/// (hard-fails under `CQLITE_REQUIRE_FIXTURES=1`), and treats "files present but
/// nothing RECOGNISED" as a failure rather than a pass.
#[test]
fn the_header_gate_never_refuses_a_frozentype_cassandra_wrote() {
    let mut files = 0usize;
    let mut recognised = 0usize;
    let mut unreadable = 0usize;
    let mut refused: Vec<String> = Vec::new();

    for root in corpus_roots() {
        for path in statistics_db_files(&root) {
            let Ok(bytes) = std::fs::read(&path) else {
                continue;
            };
            files += 1;
            let hay = String::from_utf8_lossy(&bytes);
            for (idx, _) in hay.match_indices("FrozenType(") {
                match balanced_marshal_string(&hay[idx..]) {
                    // Truncated by the surrounding binary, not a Cassandra string.
                    None => unreadable += 1,
                    Some(spelling) => {
                        recognised += 1;
                        if let Err(e) = validate_marshal_frozen(spelling) {
                            refused.push(format!("{}: {spelling} — {e}", path.display()));
                        }
                    }
                }
            }
        }
    }

    if files == 0 {
        assert!(
            !require_fixtures(),
            "CQLITE_REQUIRE_FIXTURES=1 but no *-Statistics.db was reachable; the \
             over-refusal guard cannot be measured"
        );
        eprintln!(
            "[#4158] SKIP: no *-Statistics.db under CQLITE_DATASETS_ROOT or the \
             checkout; over-refusal guard not measured"
        );
        return;
    }

    assert!(
        refused.is_empty(),
        "the read gate REFUSED {} FrozenType(...) string(s) Apache Cassandra wrote \
         — an over-refusal, which is what narrowing the allowlist risks: {:?}",
        refused.len(),
        refused
    );
    // Affirmative zero: an unmeasured guard must not read like a clean one.
    assert!(
        recognised > 0,
        "{files} Statistics.db file(s) scanned but 0 complete FrozenType(...) \
         string(s) RECOGNISED ({unreadable} truncated) — the guard would be vacuous"
    );
    eprintln!(
        "[#4158] over-refusal guard: {recognised} Cassandra-written FrozenType(...) \
         string(s) RECOGNISED over {files} Statistics.db file(s), 0 refused \
         ({unreadable} truncated by the surrounding binary and not checked)"
    );
}

/// `true` when an absent corpus must FAIL rather than SKIP.
fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

/// EVERY candidate corpus root, never a preferred one: neither the exported root
/// nor the checkout is a superset of the other (#3220), so both are walked.
fn corpus_roots() -> Vec<std::path::PathBuf> {
    let mut roots = Vec::new();
    if let Ok(env_root) = std::env::var("CQLITE_DATASETS_ROOT") {
        let p = std::path::PathBuf::from(env_root);
        if p.is_dir() {
            roots.push(p);
        }
    }
    let checkout = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|w| w.join("test-data").join("datasets"));
    match checkout {
        Some(p) if p.is_dir() && !roots.contains(&p) => roots.push(p),
        _ => {}
    }
    roots
}

/// Every `*-Statistics.db` under `root`, recursively.
fn statistics_db_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
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
                .is_some_and(|n| n.ends_with("-Statistics.db"))
            {
                out.push(path);
            }
        }
    }
    out
}

/// The complete `FrozenType(...)` marshal string starting at `at`, or `None` when
/// it does not close inside the marshal grammar — which is how a match that is
/// really binary noise (or a string the surrounding buffer truncated) is EXCLUDED
/// rather than asserted about.
///
/// The accepted alphabet is exactly what Cassandra's own printers emit: class
/// names and keyspace names (`[A-Za-z0-9_.]`), the `,` / `:` / space separators
/// (`stringifyTypeParameters`, `stringifyUserTypeParameters`,
/// `stringifyVectorParameters`\'s `" , "`) and the parentheses.
fn balanced_marshal_string(at: &str) -> Option<&str> {
    let mut depth = 0usize;
    for (idx, ch) in at.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&at[..=idx]);
                }
            }
            c if c.is_ascii_alphanumeric() => {}
            '.' | '_' | ',' | ':' | ' ' | '-' => {}
            // Anything else means this is not a Cassandra-printed type string.
            _ => return None,
        }
    }
    None
}
