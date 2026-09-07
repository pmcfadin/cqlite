//! The ONE parser for a `vector` type's two declared parameters — element type
//! and dimension (issue #4114).
//!
//! # Why this is shared rather than repeated
//!
//! Four independent type-string parsers plus the STATS clustering-layout resolver
//! all have to learn `vector`, and every one of them needs the SAME two answers:
//! which element type, and what dimension. Repeating the dimension rules (reject
//! `0`, reject non-numeric, reject an overflowing product) five times is five
//! chances to disagree, and the disagreement would be silent — the defect #4114
//! exists to remove.
//!
//! # Authority (pinned `cassandra-5.0.8`, never CQLite's own code — #3041)
//!
//! * The marshal spelling is written by `TypeParser.stringifyVectorParameters`
//!   (`TypeParser.java:239-242`):
//!   `"(" + type.toString(ignoreFreezing) + " , " + dimension + ")"`, reached from
//!   `VectorType.toString` (`VectorType.java:339-342`) as
//!   `getClass().getName() + stringifyVectorParameters(…)`. So Cassandra WRITES
//!   `org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType , 3)`
//!   — note the spaces around the comma.
//! * Cassandra's own READER tolerates whitespace variation:
//!   `TypeParser.getVectorParameters` (`TypeParser.java:244-263`) calls
//!   `skipBlankAndComma()` between the two parameters. So this parser TRIMS rather
//!   than matching Cassandra's exact output spacing — matching the literal `" , "`
//!   would refuse a legal spelling another writer may emit.
//! * Parameter ORDER is (element type, dimension) — same function.
//! * `dimension <= 0` is rejected by Cassandra at construction
//!   (`VectorType.java:89-90`, `InvalidRequestException`: "vectors may only have
//!   positive dimensions"), so a `0` dimension is a MALFORMED TYPE here, never an
//!   empty vector. There is NO upper bound in the type itself
//!   (`Guardrails.vectorDimensions` is a soft, disabled-by-default cluster
//!   setting — `Config.java:924-925`), so none is imposed here either; the only
//!   rejected large value is one whose byte width overflows `usize`.
//! * The CQL3 surface spelling is `vector<float, 3>` (`CQL3Type.java:589,:938`).
//!
//! # No-heuristics (#28)
//!
//! The dimension is read from the DECLARED type and from nowhere else. Nothing in
//! this module looks at a value byte, and no width is ever inferred from how many
//! bytes happen to be available.

use crate::error::{Error, Result};

/// The VALUE side: `vector<float, n>`'s fixed-width, prefix-free byte layout.
///
/// A child of this module rather than a sibling elsewhere so that everything
/// about the vector type — its declared parameters AND the layout those
/// parameters imply — has ONE home, reachable from both the SSTable row decoder
/// and the schema-aware value parsers without either owning it.
pub(crate) mod vector_value;

/// Width in bytes of one `float` (CQL) / `FloatType` (marshal) element.
///
/// `FloatType.valueLengthIfFixed()` returns 4 (`FloatType.java:148-152`), and
/// `VectorType`'s fixed width is `elementType.valueLengthIfFixed() * dimension`
/// (`VectorType.java:94-96`).
pub(crate) const FLOAT_ELEMENT_WIDTH: usize = 4;

/// The two declared parameters of a `vector` type, as borrowed substrings of the
/// type string they came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VectorTypeArgs<'a> {
    /// The element type, verbatim and trimmed (a CQL short form such as `float`
    /// for the CQL spelling, or a marshal class name such as
    /// `org.apache.cassandra.db.marshal.FloatType` for the marshal spelling).
    pub element: &'a str,
    /// The dimension `n`, guaranteed `>= 1`.
    pub dimension: usize,
}

/// The content between `vector<` and its matching final `>`, or `None` when
/// `type_str` is not a CQL-spelled vector.
///
/// The keyword is matched case-insensitively because CQL type keywords are
/// (`SET<TEXT>` == `set<text>`), which the sibling parsers in
/// [`super::cql_type_parser`] already rely on.
pub(crate) fn cql_vector_inner(type_str: &str) -> Option<&str> {
    let trimmed = type_str.trim();
    let head = trimmed.get(..7)?;
    if !head.eq_ignore_ascii_case("vector<") {
        return None;
    }
    trimmed[7..].strip_suffix('>')
}

/// The content between a `VectorType(` constructor and its MATCHING close paren,
/// or `None` when `type_str` is not a marshal-spelled vector.
///
/// The constructor name is matched EXACTLY on its package-stripped simple name
/// (`VectorType`), the same way the sibling marshal resolvers in
/// `parser::repair_clustering` and `parser::enhanced_statistics_parser` identify a
/// constructor: Java class names are case-sensitive, so a lowercased spelling is
/// not a class reference and must not be accepted here.
pub(crate) fn marshal_vector_inner(type_str: &str) -> Option<&str> {
    let trimmed = type_str.trim();
    let open = trimmed.find('(')?;
    let ctor = trimmed[..open].trim();
    let simple = ctor.rsplit('.').next().unwrap_or(ctor);
    if simple != "VectorType" {
        return None;
    }
    matched_paren_body(&trimmed[open + 1..])
}

/// The prefix of `after_open` up to (not including) the `)` that matches the `(`
/// already consumed by the caller. `None` on unbalanced parentheses.
fn matched_paren_body(after_open: &str) -> Option<&str> {
    let mut depth = 1usize;
    for (idx, ch) in after_open.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&after_open[..idx]);
                }
            }
            _ => {}
        }
    }
    None
}

/// Split a vector type's argument list into its (element type, dimension) pair.
///
/// `inner` is the already-extracted argument list — the output of
/// [`cql_vector_inner`] or [`marshal_vector_inner`]. `type_str` is the full type
/// string, used only so a refusal NAMES the type that was refused.
///
/// Splitting is on the LAST TOP-LEVEL comma rather than the first, because the
/// ELEMENT may itself be a parameterised type carrying top-level-looking commas
/// inside its own brackets — those are at depth > 0 and are skipped — while the
/// dimension never contains one. Requiring exactly two top-level arguments would
/// be equivalent; taking the last is stated because the order is fixed by
/// `TypeParser.getVectorParameters` (element first, dimension last).
pub(crate) fn split_vector_args<'a>(inner: &'a str, type_str: &str) -> Result<VectorTypeArgs<'a>> {
    let mut depth = 0usize;
    let mut split_at: Option<usize> = None;
    for (idx, ch) in inner.char_indices() {
        match ch {
            '(' | '<' => depth += 1,
            ')' | '>' => {
                depth = depth.saturating_sub(1);
            }
            ',' if depth == 0 => split_at = Some(idx),
            _ => {}
        }
    }
    let Some(comma) = split_at else {
        return Err(malformed(
            type_str,
            "expected two parameters (element type, dimension) separated by a comma",
        ));
    };
    let element = inner[..comma].trim();
    let dimension_raw = inner[comma + 1..].trim();
    if element.is_empty() {
        return Err(malformed(type_str, "the element type parameter is empty"));
    }
    let dimension = parse_vector_dimension(dimension_raw, type_str)?;
    Ok(VectorTypeArgs { element, dimension })
}

/// Parse a vector's dimension parameter.
///
/// Rejects — each with a named error, never a fallback value:
/// * an empty parameter, or one that is not PLAIN DECIMAL DIGITS — including the
///   signed spellings Cassandra's own reader would tolerate but its writer can
///   never emit (see the body for the pinned citation and why the divergence is
///   one-directional);
/// * `0` and any negative spelling — `VectorType.java:89-90` rejects
///   `dimension <= 0`, so a zero-dimension vector does not exist and a
///   zero-length value is an error rather than an empty vector;
/// * a dimension whose byte width would overflow `usize` (see
///   [`vector_byte_width`]).
pub(crate) fn parse_vector_dimension(raw: &str, type_str: &str) -> Result<usize> {
    if raw.is_empty() {
        return Err(malformed(type_str, "the dimension parameter is empty"));
    }
    // DIGITS ONLY, checked explicitly rather than left to `str::parse`, which
    // ACCEPTS a leading `+` (`"+3".parse::<usize>() == Ok(3)`) — measured, and it
    // is what the sibling test caught.
    //
    // This is deliberately STRICTER THAN CASSANDRA'S OWN READER, which would
    // accept `+3`: `TypeParser.readNextIdentifier` treats `+`, `-`, `.`, `_` and
    // `&` as identifier characters (`TypeParser.java:578-583`) and
    // `getVectorParameters` hands the result to `Integer.parseInt`
    // (`:255-258`), which tolerates a leading sign. The divergence is safe and
    // one-directional: Cassandra's WRITER concatenates the `int` dimension
    // directly (`stringifyVectorParameters`, `TypeParser.java:239-242`), so no
    // Cassandra-written type string can carry a sign, a `.` or a `_` in this
    // parameter. Refusing them therefore rejects nothing Cassandra wrote, while
    // accepting them would mean two spellings of one dimension reaching the
    // decode path — and a dimension IS the width, so an unnormalised spelling is
    // a width nobody declared (#28).
    if !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(malformed(
            type_str,
            &format!(
                "dimension '{raw}' is not a plain decimal integer (digits only: a                  sign, a decimal point, whitespace or a non-ASCII digit is not a                  dimension Cassandra's writer can emit)"
            ),
        ));
    }
    let dimension: usize = raw.parse().map_err(|_| {
        malformed(
            type_str,
            &format!("dimension '{raw}' is not a non-negative decimal integer"),
        )
    })?;
    if dimension == 0 {
        return Err(malformed(
            type_str,
            "dimension 0 is not a legal vector dimension (Cassandra rejects \
             dimension <= 0: VectorType.java:89-90)",
        ));
    }
    Ok(dimension)
}

/// The total on-disk byte width of a FIXED-width vector: `element_width *
/// dimension`, or `None` when that product overflows `usize`.
///
/// `VectorType.java:94-96`:
/// `valueLengthIfFixed = elementType.valueLengthIfFixed() * dimension`.
pub(crate) fn vector_byte_width(element_width: usize, dimension: usize) -> Option<usize> {
    element_width.checked_mul(dimension)
}

fn malformed(type_str: &str, why: &str) -> Error {
    Error::schema(format!("malformed vector type '{type_str}': {why}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PKG: &str = "org.apache.cassandra.db.marshal.";

    #[test]
    fn marshal_spelling_cassandra_writes_is_parsed() {
        // The exact string `VectorType.toString` produces, spaces and all.
        let ty = format!("{PKG}VectorType({PKG}FloatType , 3)");
        let inner = marshal_vector_inner(&ty).expect("a VectorType constructor");
        let args = split_vector_args(inner, &ty).expect("well-formed parameters");
        assert_eq!(args.element, format!("{PKG}FloatType"));
        assert_eq!(args.dimension, 3);
    }

    #[test]
    fn whitespace_around_the_comma_is_tolerated_in_both_directions() {
        // `TypeParser.skipBlankAndComma` tolerates variation on READ, so a writer
        // that omits the spaces Cassandra emits must still parse.
        for ty in [
            format!("{PKG}VectorType({PKG}FloatType,3)"),
            format!("{PKG}VectorType({PKG}FloatType , 3)"),
            format!("{PKG}VectorType( {PKG}FloatType ,  3 )"),
        ] {
            let inner = marshal_vector_inner(&ty).expect("a VectorType constructor");
            let args = split_vector_args(inner, &ty).expect("well-formed parameters");
            assert_eq!(args.element, format!("{PKG}FloatType"), "{ty}");
            assert_eq!(args.dimension, 3, "{ty}");
        }
    }

    #[test]
    fn a_bare_constructor_name_is_accepted_and_a_foreign_one_is_not() {
        // `TypeParser.getAbstractType` resolves an unqualified name against the
        // marshal package, so the bare spelling is the same type.
        assert!(marshal_vector_inner("VectorType(FloatType , 2)").is_some());
        // Case matters: a lowercased string is not a Java class reference.
        assert!(marshal_vector_inner("vectortype(FloatType , 2)").is_none());
        // A different constructor is not a vector.
        assert!(marshal_vector_inner(&format!("{PKG}ListType({PKG}FloatType)")).is_none());
        // Not parameterised at all.
        assert!(marshal_vector_inner(&format!("{PKG}FloatType")).is_none());
    }

    #[test]
    fn a_parameterised_element_keeps_its_own_commas() {
        // The element's commas are at depth > 0; only the LAST top-level comma
        // separates the dimension.
        let ty = format!("{PKG}VectorType({PKG}TupleType({PKG}Int32Type,{PKG}UTF8Type) , 7)");
        let inner = marshal_vector_inner(&ty).expect("a VectorType constructor");
        let args = split_vector_args(inner, &ty).expect("well-formed parameters");
        assert_eq!(
            args.element,
            format!("{PKG}TupleType({PKG}Int32Type,{PKG}UTF8Type)")
        );
        assert_eq!(args.dimension, 7);
    }

    #[test]
    fn cql_spelling_is_case_insensitive_and_whitespace_tolerant() {
        for ty in [
            "vector<float, 3>",
            "VECTOR<FLOAT,3>",
            "  vector< float , 3 >",
        ] {
            let inner = cql_vector_inner(ty).expect("a CQL vector spelling");
            let args = split_vector_args(inner, ty).expect("well-formed parameters");
            assert!(args.element.eq_ignore_ascii_case("float"), "{ty}");
            assert_eq!(args.dimension, 3, "{ty}");
        }
        assert!(cql_vector_inner("list<float>").is_none());
        assert!(cql_vector_inner("vector").is_none());
        // A nested element keeps its own angle brackets.
        let ty = "vector<frozen<tuple<int, text>>, 4>";
        let inner = cql_vector_inner(ty).expect("a CQL vector spelling");
        let args = split_vector_args(inner, ty).expect("well-formed parameters");
        assert_eq!(args.element, "frozen<tuple<int, text>>");
        assert_eq!(args.dimension, 4);
    }

    #[test]
    fn a_zero_dimension_is_refused_by_name_never_read_as_an_empty_vector() {
        let err = parse_vector_dimension("0", "vector<float, 0>")
            .expect_err("Cassandra rejects dimension <= 0");
        let msg = err.to_string();
        assert!(
            msg.contains("vector<float, 0>") && msg.contains("dimension 0"),
            "{msg}"
        );
    }

    #[test]
    fn a_non_numeric_or_negative_dimension_is_refused_by_name() {
        // Every one of these is a MALFORMED dimension, not a value to salvage: a
        // fallback would put a made-up width on the decode path (#28).
        for raw in ["", "n", "3.5", "-1", "+3", "0x3", "3 3", "\u{ff13}"] {
            let err = match parse_vector_dimension(raw, "vector<float, ?>") {
                Ok(n) => panic!("dimension {raw:?} must be refused, got {n}"),
                Err(e) => e.to_string(),
            };
            assert!(
                err.contains("vector<float, ?>"),
                "the refusal must name the type it refused: {err}"
            );
        }
    }

    #[test]
    fn an_overflowing_dimension_is_refused_rather_than_wrapped() {
        // The declared dimension parses, but its byte width does not fit.
        assert_eq!(vector_byte_width(FLOAT_ELEMENT_WIDTH, 3), Some(12));
        assert_eq!(vector_byte_width(FLOAT_ELEMENT_WIDTH, usize::MAX), None);
        assert_eq!(vector_byte_width(0, usize::MAX), Some(0));
    }

    #[test]
    fn a_missing_comma_or_empty_element_is_refused_by_name() {
        let err = split_vector_args("FloatType", "VectorType(FloatType)")
            .expect_err("a vector needs two parameters")
            .to_string();
        assert!(err.contains("two parameters"), "{err}");
        let err = split_vector_args(" , 3", "vector< , 3>")
            .expect_err("an empty element type is malformed")
            .to_string();
        assert!(err.contains("element type parameter is empty"), "{err}");
    }
}
