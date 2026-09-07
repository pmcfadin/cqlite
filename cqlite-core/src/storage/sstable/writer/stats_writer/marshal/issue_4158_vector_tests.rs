//! `vector<element, n>` in the SerializationHeader — issue #4158.
//!
//! Two properties, and they are different questions:
//!
//! 1. **The SPELLING is Cassandra's**, asserted against Cassandra-WRITTEN bytes
//!    (`test-data/fixtures/issue_4114/**/nb-1-big-Statistics.db`, committed by
//!    #4149) and never against CQLite's own prior output. A CQLite-written +
//!    CQLite-read round trip is invariant to a uniform framing error (#3042), so it
//!    could not have caught the `BytesType` this arm replaces.
//! 2. **The string CQLite emits is the string CQLite's READER classifies
//!    correctly.** #4149 made the read path derive a vector's on-disk layout from
//!    its ELEMENT (a fixed-element vector is written UNPREFIXED). If the writer
//!    emitted a spelling the reader classified differently, the mismatch would be a
//!    framing divergence no round trip can see. So the round trip asserted here is
//!    of the CLASSIFICATION, not of bytes.

use super::*;
use crate::parser::repair_clustering::{resolve_clustering_value_layout, ClusteringValueLayout};
use crate::schema::vector_type::{cassandra_fixed_element_width, marshal_vector_kind};
use crate::schema::CqlType;

const Q: &str = "org.apache.cassandra.db.marshal.";

fn marshal(cql: &str) -> String {
    cql_type_to_marshal_type(cql).expect("a renderable marshal type")
}

/// Every Cassandra-written `Statistics.db` under `test-data/fixtures/issue_4114`.
/// Git-committed, so a missing one is a FAILURE, never a skip.
fn cassandra_vector_headers() -> Vec<(std::path::PathBuf, Vec<u8>)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a parent directory")
        .join("test-data/fixtures/issue_4114/test_vector");
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&root).unwrap_or_else(|e| {
        panic!(
            "{} is a git-committed fixture directory: {e}",
            root.display()
        )
    }) {
        let dir = entry.expect("a readable directory entry").path();
        let stats = dir.join("nb-1-big-Statistics.db");
        if stats.is_file() {
            let bytes = std::fs::read(&stats).expect("a readable Statistics.db");
            out.push((stats, bytes));
        }
    }
    assert!(
        out.len() >= 4,
        "case floor: #4149 committed 4 vector fixtures under {}; found {} — an \
         emptied fixture set would make the byte oracle below assert nothing",
        root.display(),
        out.len()
    );
    out
}

/// THE BYTE ORACLE. `VectorType.toString(boolean)` is
/// `getClass().getName() + TypeParser.stringifyVectorParameters(elementType, ignoreFreezing, dimension)`
/// (`VectorType.java:338-342`), and `stringifyVectorParameters` is
/// `"(" + type.toString(ignoreFreezing) + " , " + dimension + ")"`
/// (`TypeParser.java:239-242`) — so SPACE comma SPACE, and the element is
/// package-qualified. Every dimension below is one Cassandra actually wrote into a
/// committed fixture header (`vector_exact` = 3, `vector_pk_only` = 1 and 384).
#[test]
fn cassandra_written_vector_header_is_reproduced() {
    let headers = cassandra_vector_headers();
    let mut matched = 0usize;
    for dimension in [1usize, 3, 384] {
        let emitted = marshal(&format!("vector<float, {dimension}>"));
        assert_eq!(
            emitted,
            format!("{Q}VectorType({Q}FloatType , {dimension})"),
            "the rendered spelling must be Cassandra's, spacing included"
        );
        let needle = emitted.as_bytes();
        let found = headers
            .iter()
            .any(|(_, bytes)| bytes.windows(needle.len()).any(|w| w == needle));
        assert!(
            found,
            "'{emitted}' does not occur verbatim in any Cassandra-written \
             Statistics.db under test-data/fixtures/issue_4114 — the emitted spelling \
             is not the one Cassandra wrote"
        );
        matched += 1;
    }
    assert_eq!(matched, 3, "all three attested dimensions must be checked");
}

/// The BytesType this arm replaces: the defect #4158 names. Asserted as a
/// non-equality so a regression to the fallback is caught even if the spelling
/// assertions above are ever loosened.
#[test]
fn a_vector_column_no_longer_degrades_to_bytestype() {
    for cql in ["vector<float, 3>", "vector<int, 2>", "vector<text, 5>"] {
        assert_ne!(
            marshal(cql),
            format!("{Q}BytesType"),
            "{cql} must not reach the unknown-type fallback"
        );
    }
}

/// No `FrozenType(` wrapper, ever: `VectorType.toString(boolean)` has no
/// `includeFrozenType` branch, so `frozen<vector<..>>` and `vector<..>` are one
/// Cassandra type with one spelling — matching CQL, where `RawVector::freeze()`
/// returns `this` (`CQL3Type.java:915-919`).
#[test]
fn a_frozen_vector_renders_as_the_bare_vectortype() {
    let bare = marshal("vector<float, 3>");
    assert_eq!(marshal("frozen<vector<float, 3>>"), bare);
    assert!(
        !takes_frozen_type_wrapper(&bare),
        "VectorType must not be wrapper-eligible"
    );
    // And under a MULTICELL parent, where a frozen collection WOULD keep its
    // wrapper, a vector still does not gain one.
    assert_eq!(
        marshal("list<frozen<vector<float, 3>>>"),
        format!("{Q}ListType({Q}VectorType({Q}FloatType , 3))")
    );
}

/// The separator is `" , "`, which is NOT the separator collections use.
/// `stringifyVectorParameters` (`TypeParser.java:239-242`) spells `" , "`;
/// `stringifyTypeParameters` joins with a bare `,`. Pinned because a single
/// normalising `trim`/`replace` anywhere would silently unify them and the reader
/// tolerates both (`TypeParser.getVectorParameters` calls `skipBlankAndComma()`),
/// so nothing else would notice.
#[test]
fn the_vector_separator_is_space_comma_space_unlike_a_collection() {
    assert!(marshal("vector<float, 3>").ends_with("FloatType , 3)"));
    assert_eq!(
        marshal("map<text, int>"),
        format!("{Q}MapType({Q}UTF8Type,{Q}Int32Type)"),
        "a collection's parameters are joined with a BARE comma"
    );
}

/// A PARAMETRIC element is spelled, not refused: Cassandra's element is any
/// `comparatorType` (`Parser.g:1916-1919`) and the recursion is the same one every
/// other subtype takes.
#[test]
fn a_parametric_vector_element_is_rendered_by_the_shared_recursion() {
    assert_eq!(
        marshal("vector<map<int, text>, 3>"),
        format!("{Q}VectorType({Q}MapType({Q}Int32Type,{Q}UTF8Type) , 3)")
    );
    assert_eq!(
        marshal("vector<vector<float, 3>, 2>"),
        format!("{Q}VectorType({Q}VectorType({Q}FloatType , 3) , 2)")
    );
    // An already-marshal element passes through verbatim, case preserved (#929).
    let user = format!("{Q}UserType(ks,706572736f6e,6e616d65:{Q}UTF8Type)");
    assert_eq!(
        marshal(&format!("vector<{user}, 2>")),
        format!("{Q}VectorType({user} , 2)")
    );
}

/// THE REFUSAL. An element name this converter resolves to no marshal class would
/// otherwise take the `_ => BytesType` fallback — and inside a vector that is not a
/// lost type NAME but a wrong value WIDTH
/// (`VectorType.valueLengthIfFixed() = element.valueLengthIfFixed() * dimension`,
/// `VectorType.java:94-96`), stamped into `Statistics.db`. Refused BY NAME.
#[test]
fn an_unresolvable_vector_element_is_refused_by_name() {
    for (cql, element) in [
        ("vector<address_type, 3>", "address_type"),
        ("vector<frozen<address_type>, 3>", "address_type"),
        ("list<vector<person, 2>>", "person"),
        ("vector<map<int, person>, 2>", "person"),
        ("frozen<vector<nope, 7>>", "nope"),
    ] {
        let err = cql_type_to_marshal_type(cql)
            .err()
            .unwrap_or_else(|| panic!("'{cql}' has an unresolvable element and must be refused"));
        let msg = err.to_string();
        assert!(
            msg.contains(element),
            "the refusal must NAME the element it could not spell ('{element}'), got: {msg}"
        );
        assert!(
            msg.contains("TypeParser.java:239-242"),
            "the refusal must cite its Cassandra oracle, got: {msg}"
        );
    }
}

/// The refusal is SCOPED to a vector element: the same unresolvable name OUTSIDE
/// one keeps its documented `BytesType` degradation (#929), unchanged. Without this
/// half, "refuse the unknown name" would silently have become a whole-converter
/// policy change affecting every unresolved UDT column.
#[test]
fn the_bytestype_fallback_outside_a_vector_is_untouched() {
    for cql in [
        "address_type",
        "list<person>",
        "frozen<person>",
        "unknown_t",
    ] {
        assert!(
            cql_type_to_marshal_type(cql).is_ok(),
            "'{cql}' must keep its pre-#4158 total behaviour"
        );
    }
    assert_eq!(marshal("frozen<person>"), format!("{Q}BytesType"));
    assert_eq!(
        marshal("list<person>"),
        format!("{Q}ListType({Q}BytesType)")
    );
}

/// A MALFORMED vector is refused by the ONE shared parser (`schema::vector_type`),
/// propagated rather than degraded — so a bad dimension or a third parameter can
/// never reach the header as `BytesType`.
#[test]
fn a_malformed_vector_is_refused_by_the_shared_parser() {
    for cql in [
        "vector<float>",          // one parameter
        "vector<float, 0>",       // VectorType.java:89-90
        "vector<float, -3>",      // ditto
        "vector<float, nope>",    // not a decimal integer
        "vector<float, text, 3>", // no third parameter exists
        "vector<, 3>",            // empty element
        "vector<float, 2>>",      // trailing text after the parameters
    ] {
        let err = cql_type_to_marshal_type(cql)
            .err()
            .unwrap_or_else(|| panic!("'{cql}' is not a legal vector type and must be refused"));
        assert!(
            err.to_string().contains("malformed vector type"),
            "'{cql}' must be refused by the shared vector parser, got: {err}"
        );
    }
}

/// The infallible UDT-FIELD disposition keeps `render_udt_marshal`'s declared
/// `BytesType` degradation (#929/#1011) while still spelling what it can.
#[test]
fn the_udt_field_disposition_degrades_instead_of_propagating() {
    assert_eq!(
        cql_type_to_marshal_type_or_bytes("vector<float, 3>"),
        format!("{Q}VectorType({Q}FloatType , 3)")
    );
    assert_eq!(
        cql_type_to_marshal_type_or_bytes("vector<address_type, 3>"),
        format!("{Q}BytesType"),
        "a field the registry-less renderer cannot represent takes its documented \
         degradation rather than propagating a refusal it cannot carry"
    );
}

// ════════════════ FIX 2b: WRITER/READER CLASSIFICATION AGREEMENT ════════════════

/// The string the writer emits must be the string the reader CLASSIFIES the way the
/// declared type means — element and dimension recovered, and the value LAYOUT
/// (`Fixed(element_width * dimension)` iff the element is fixed, else `Variable`)
/// resolved identically to the declared CQL type's own answer.
///
/// This is the #4149 interaction and the reason it is asserted at the level of
/// CLASSIFICATION rather than bytes: #4149 fixed `resolve_clustering_value_layout`
/// treating every `VectorType` as vint-length-prefixed when Cassandra writes a
/// FIXED-element vector UNPREFIXED. A writer emitting a spelling that classifier
/// read differently would be a framing divergence invisible to any round trip
/// (#3042) — both halves would agree with each other and with nothing else.
#[test]
fn the_emitted_string_is_classified_by_the_reader_as_the_declared_type() {
    // (declared CQL, element marshal simple name, expected layout)
    let cases: &[(&str, &str, ClusteringValueLayout)] = &[
        (
            "vector<float, 3>",
            "FloatType",
            ClusteringValueLayout::Fixed(12),
        ),
        (
            "vector<float, 1>",
            "FloatType",
            ClusteringValueLayout::Fixed(4),
        ),
        (
            "vector<float, 384>",
            "FloatType",
            ClusteringValueLayout::Fixed(1536),
        ),
        (
            "vector<int, 2>",
            "Int32Type",
            ClusteringValueLayout::Fixed(8),
        ),
        (
            "vector<double, 4>",
            "DoubleType",
            ClusteringValueLayout::Fixed(32),
        ),
        (
            "vector<boolean, 5>",
            "BooleanType",
            ClusteringValueLayout::Fixed(5),
        ),
        (
            "vector<uuid, 2>",
            "UUIDType",
            ClusteringValueLayout::Fixed(32),
        ),
        // Variable-element vectors: Cassandra frames these element-by-element, so
        // the layout must NOT become a fixed width.
        (
            "vector<text, 3>",
            "UTF8Type",
            ClusteringValueLayout::Variable,
        ),
        (
            "vector<blob, 3>",
            "BytesType",
            ClusteringValueLayout::Variable,
        ),
        (
            "vector<varint, 3>",
            "IntegerType",
            ClusteringValueLayout::Variable,
        ),
        // The four types whose LOGICAL fixed size disagrees with Cassandra's
        // framing (roborev job 111 on #4114): they must classify Variable.
        (
            "vector<tinyint, 3>",
            "ByteType",
            ClusteringValueLayout::Variable,
        ),
        (
            "vector<smallint, 3>",
            "ShortType",
            ClusteringValueLayout::Variable,
        ),
        (
            "vector<date, 3>",
            "SimpleDateType",
            ClusteringValueLayout::Variable,
        ),
        (
            "vector<time, 3>",
            "TimeType",
            ClusteringValueLayout::Variable,
        ),
    ];
    assert_eq!(
        cases.len(),
        14,
        "case floor: an emptied table would make this differential assert nothing"
    );
    for (declared, element_simple, want_layout) in cases {
        let emitted = marshal(declared);

        // (a) the reader's shared MARSHAL probe recovers both parameters.
        let args = marshal_vector_kind(&emitted)
            .into_args(&emitted)
            .unwrap_or_else(|e| panic!("'{emitted}' must be a well-formed vector type: {e}"))
            .unwrap_or_else(|| panic!("'{emitted}' must be recognised as a vector type"));
        assert_eq!(args.element, format!("{Q}{element_simple}"));

        // (b) the DECLARED type's own parameters, from the CQL side of the same
        // shared parser, agree with what the emitted marshal string yields.
        let declared_type = CqlType::parse(declared).expect("a declarable vector type");
        let CqlType::Vector(declared_element, declared_dimension) = &declared_type else {
            panic!("'{declared}' must parse to CqlType::Vector, got {declared_type:?}");
        };
        assert_eq!(args.dimension, *declared_dimension);

        // (c) the FRAMING classifier (#4149) answers for the emitted string exactly
        // what the declared element's own width rule says.
        let layout = resolve_clustering_value_layout(&emitted).unwrap_or_else(|| {
            panic!("'{emitted}' must be classifiable by the read path, not UNKNOWN")
        });
        assert_eq!(
            layout, *want_layout,
            "'{declared}' -> '{emitted}' must classify as {want_layout:?}"
        );
        match cassandra_fixed_element_width(declared_element) {
            Some(width) => assert_eq!(
                layout,
                ClusteringValueLayout::Fixed(width * declared_dimension),
                "a fixed element makes the vector fixed at element_width * dimension \
                 (VectorType.java:94-96)"
            ),
            None => assert_eq!(
                layout,
                ClusteringValueLayout::Variable,
                "a variable element makes the vector variable-length"
            ),
        }
    }
}

/// The classification agreement must also hold for the exact bytes CASSANDRA wrote,
/// not only for what CQLite emits — otherwise (c) above could be two halves of one
/// mistake agreeing with each other (#3042). The emitted string and the
/// fixture-extracted string are compared to each other AND classified separately.
#[test]
fn the_cassandra_written_string_classifies_the_same_way_as_the_emitted_one() {
    let needle = format!("{Q}VectorType(");
    let mut checked = 0usize;
    for (path, bytes) in cassandra_vector_headers() {
        let mut from = 0usize;
        while let Some(rel) = bytes[from..]
            .windows(needle.len())
            .position(|w| w == needle.as_bytes())
        {
            let start = from + rel;
            let close = bytes[start..]
                .iter()
                .position(|b| *b == b')')
                .map(|p| start + p + 1)
                .unwrap_or_else(|| panic!("{}: a VectorType( with no ')'", path.display()));
            let cassandra =
                std::str::from_utf8(&bytes[start..close]).expect("a marshal type string is ASCII");
            let args = marshal_vector_kind(cassandra)
                .into_args(cassandra)
                .expect("Cassandra wrote a well-formed vector type")
                .expect("recognised as a vector type");
            // Cassandra only ever wrote FloatType vectors into these fixtures.
            assert_eq!(args.element, format!("{Q}FloatType"));
            assert_eq!(
                marshal(&format!("vector<float, {}>", args.dimension)),
                cassandra,
                "{}: CQLite must emit the byte-identical string",
                path.display()
            );
            assert_eq!(
                resolve_clustering_value_layout(cassandra),
                Some(ClusteringValueLayout::Fixed(4 * args.dimension)),
                "{}: a FloatType vector is fixed at 4 * dimension, UNPREFIXED",
                path.display()
            );
            checked += 1;
            from = close;
        }
    }
    assert!(
        checked >= 4,
        "case floor: the fixtures carry at least 4 Cassandra-written VectorType \
         headers; only {checked} were classified"
    );
}
