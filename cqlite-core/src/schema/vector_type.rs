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

/// The three answers a vector-type probe can give (issue #4114, roborev job 109).
///
/// Two-valued `Option` was a DEFECT, not a simplification: `None` meant both "this
/// is not a vector" (so the caller's other type arms apply) and "this IS a vector
/// but its parameter list is unparseable" (so the caller must FAIL CLOSED). Every
/// caller took the first reading, so a malformed `VectorType(` reached a generic
/// fallback — in `enhanced_statistics_parser::marshal_type` the
/// `other => other.to_lowercase()` one, which restored exactly the blob/phantom-vint
/// framing #4114 exists to remove.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorInner<'a> {
    /// `type_str` does not claim to be a vector type.
    NotAVector,
    /// `type_str` is a vector type; the payload is its raw parameter list, still to
    /// be split by [`split_vector_args`].
    Args(&'a str),
    /// `type_str` CLAIMS to be a vector type but its parameter list could not be
    /// extracted. The payload says why, for the refusal message.
    Malformed(&'static str),
}

impl<'a> VectorInner<'a> {
    /// Collapse the probe into the answer a caller actually needs:
    /// `Ok(None)` = not a vector, `Ok(Some(args))` = a well-formed vector,
    /// `Err` = a vector that must fail closed, with `type_str` named.
    ///
    /// Both malformed states — an unextractable parameter list AND a parameter list
    /// that does not split into (element, dimension) — come out as `Err`, so no
    /// caller can accidentally handle only one of them.
    pub(crate) fn into_args(self, type_str: &'a str) -> Result<Option<VectorTypeArgs<'a>>> {
        match self {
            VectorInner::NotAVector => Ok(None),
            VectorInner::Args(inner) => split_vector_args(inner, type_str).map(Some),
            VectorInner::Malformed(why) => Err(malformed(type_str, why)),
        }
    }
}

/// Probe `type_str` for the CQL spelling `vector<element, n>`.
///
/// The keyword is matched case-insensitively because CQL type keywords are
/// (`SET<TEXT>` == `set<text>`), which the sibling parsers in
/// [`super::cql_type_parser`] already rely on.
///
/// A bare `vector` with NO parameter list is [`VectorInner::NotAVector`], NOT
/// malformed: `vector` is not a reserved word in CQL (`CQL3Type.java:589,:938`
/// recognises it only WITH parameters), so an unparameterised `vector` can be a
/// legitimate UDT name and must stay available to the caller's UDT arm. Once a `<`
/// follows, no other type can be meant, so an unterminated parameter list is
/// [`VectorInner::Malformed`].
pub(crate) fn cql_vector_kind(type_str: &str) -> VectorInner<'_> {
    let trimmed = type_str.trim();
    let (keyword, rest) = split_leading_identifier(trimmed);
    if !keyword.eq_ignore_ascii_case("vector") {
        return VectorInner::NotAVector;
    }
    let rest = rest.trim_start();
    let Some(after_open) = rest.strip_prefix('<') else {
        // No parameter list: a UDT may legitimately be named `vector`.
        return VectorInner::NotAVector;
    };
    match matched_bracket_body(after_open, '<', '>') {
        Some((inner, tail)) if tail.trim().is_empty() => VectorInner::Args(inner),
        Some(_) => {
            VectorInner::Malformed("trailing text after the closing '>' of the type parameters")
        }
        None => VectorInner::Malformed("the type parameters are not terminated by a matching '>'"),
    }
}

/// Probe `type_str` for the marshal spelling `VectorType(element , n)`.
///
/// The constructor name is matched EXACTLY on its package-stripped simple name
/// (`VectorType`), the same way the sibling marshal resolvers in
/// `parser::repair_clustering` and `parser::enhanced_statistics_parser` identify a
/// constructor: Java class names are case-sensitive, so a lowercased spelling is
/// not a class reference and must not be accepted here.
///
/// Unlike the CQL probe, a `VectorType` with no usable parameter list is
/// [`VectorInner::Malformed`] rather than `NotAVector`: `VectorType` is a Java class
/// name, nothing else can be spelled that way, and `VectorType.getInstance` has no
/// parameterless form (`TypeParser.getVectorParameters`, `TypeParser.java:244-263`,
/// requires both parameters). So there is no honest reading of it other than "a
/// broken vector type".
pub(crate) fn marshal_vector_kind(type_str: &str) -> VectorInner<'_> {
    let trimmed = type_str.trim();
    let (ctor, rest) = split_leading_identifier(trimmed);
    let simple = ctor.rsplit('.').next().unwrap_or(ctor);
    if simple != "VectorType" {
        return VectorInner::NotAVector;
    }
    let rest = rest.trim_start();
    let Some(after_open) = rest.strip_prefix('(') else {
        return VectorInner::Malformed(
            "a VectorType constructor with no '(' parameter list (Cassandra has no \
             parameterless VectorType)",
        );
    };
    match matched_bracket_body(after_open, '(', ')') {
        Some((inner, tail)) if tail.trim().is_empty() => VectorInner::Args(inner),
        Some(_) => {
            VectorInner::Malformed("trailing text after the closing ')' of the parameter list")
        }
        None => VectorInner::Malformed("the parameter list is not terminated by a matching ')'"),
    }
}

/// Split off the leading Java/CQL type identifier — the run of characters legal in
/// a (possibly package-qualified) type name — and return `(identifier, rest)`.
///
/// `.` is part of the identifier so a package-qualified class name comes back
/// whole; the caller package-strips. `_` and `$` are legal Java identifier
/// characters. Nothing else is consumed, so the very next character decides
/// whether a parameter list follows.
fn split_leading_identifier(value: &str) -> (&str, &str) {
    let end = value
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '$'))
        .unwrap_or(value.len());
    (&value[..end], &value[end..])
}

/// The body between an ALREADY-CONSUMED `open` bracket and its MATCHING `close`,
/// plus whatever follows that `close`. `None` on unbalanced brackets.
fn matched_bracket_body(after_open: &str, open: char, close: char) -> Option<(&str, &str)> {
    let mut depth = 1usize;
    for (idx, ch) in after_open.char_indices() {
        if ch == open {
            depth += 1;
        } else if ch == close {
            depth -= 1;
            if depth == 0 {
                return Some((&after_open[..idx], &after_open[idx + close.len_utf8()..]));
            }
        }
    }
    None
}

/// Split a vector type's argument list into its (element type, dimension) pair.
///
/// `inner` is the already-extracted argument list — the [`VectorInner::Args`]
/// payload from [`cql_vector_kind`] or [`marshal_vector_kind`], normally reached
/// via [`VectorInner::into_args`] rather than called directly. `type_str` is the
/// full type string, used only so a refusal NAMES the type that was refused.
///
/// EXACTLY ONE top-level comma is required, and that is not a restatement of
/// "split on the last one" — the two differ, measurably. This function used to take
/// the LAST top-level comma and fold everything before it into the element, so
/// `vector<int, text, 3>` parsed as element `int, text` (which then degraded to a
/// `CqlType::Custom` carrier) with dimension `3`, ADMITTING a spelling Cassandra
/// refuses. Caught by the frozen-scalar gate's own accept/refuse pins (#4104): it
/// was refused before `CqlType::Vector` existed and silently admitted after.
///
/// Cassandra permits no third parameter, in either spelling:
/// * CQL — `Parser.g:1916-1919`,
///   `vector_type : K_VECTOR '<' t1=comparatorType ',' d=INTEGER '>'`;
/// * marshal — `TypeParser.getVectorParameters` (`TypeParser.java:244-263`) parses
///   ONE type, then `skipBlankAndComma()`, then ONE identifier, and then
///   `if (str.charAt(idx) != ')') throw new IllegalStateException()`.
///
/// Commas INSIDE the element's own brackets are at depth > 0 and are not counted,
/// so `vector<map<int, text>, 3>` and `VectorType(MapType(Int32Type,UTF8Type) , 3)`
/// are unaffected. Order is fixed by the same functions: element first, dimension
/// last.
pub(crate) fn split_vector_args<'a>(inner: &'a str, type_str: &str) -> Result<VectorTypeArgs<'a>> {
    let mut depth = 0usize;
    let mut split_at: Option<usize> = None;
    let mut top_level_commas = 0usize;
    for (idx, ch) in inner.char_indices() {
        match ch {
            '(' | '<' => depth += 1,
            ')' | '>' => {
                depth = depth.saturating_sub(1);
            }
            ',' if depth == 0 => {
                top_level_commas += 1;
                split_at = Some(idx);
            }
            _ => {}
        }
    }
    if top_level_commas > 1 {
        return Err(malformed(
            type_str,
            &format!(
                "expected exactly two parameters (element type, dimension) but found \
                 {} top-level comma(s): Cassandra's grammar admits no third parameter \
                 (Parser.g:1916-1919; TypeParser.getVectorParameters requires ')' after \
                 the dimension, TypeParser.java:244-263)",
                top_level_commas
            ),
        ));
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
                "dimension '{raw}' is not a plain decimal integer (digits only: a \
                 sign, a decimal point, whitespace or a non-ASCII digit is not a \
                 dimension Cassandra's writer can emit)"
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
    // The doc comment above promises this function rejects a dimension whose BYTE
    // WIDTH would overflow, and before this it only rejected zero (roborev job 113).
    // On a 64-bit target any `dimension > usize::MAX / 4` was accepted into
    // `CqlType::Vector` and failed much later, at decode — a promise the code did not
    // keep, and a late failure where an early one was available. Validated through the
    // SAME helper the decoders use, so the parser and the decode path cannot disagree
    // about what is representable.
    // Cassandra's dimension is a Java `int`: `getVectorParameters` hands the token to
    // `Integer.parseInt` (`TypeParser.java:255-258`), so `Integer.MAX_VALUE` is the
    // hard ceiling and NO Cassandra-written type string can exceed it (roborev job
    // 114). This is the same one-directional argument the digits-only check above
    // rests on — Cassandra's WRITER concatenates the `int` directly
    // (`stringifyVectorParameters`, `:239-242`) — so refusing a larger value rejects
    // nothing Cassandra wrote, while accepting one would admit an IMPOSSIBLE schema
    // and hand a multi-gigabyte declared width to the decode path.
    if dimension > i32::MAX as usize {
        return Err(malformed(
            type_str,
            "the dimension exceeds Cassandra's Integer.MAX_VALUE ceiling",
        ));
    }
    // NOTE the ORDER: the Integer.MAX_VALUE ceiling above runs FIRST, and on a 64-bit
    // target it SUBSUMES this width guard (usize::MAX/4 is 4.6e18, far above
    // i32::MAX = 2.1e9, so nothing can reach here overflowing). This guard is NOT dead
    // code though: on a 32-bit target usize::MAX/4 is 1_073_741_823, BELOW i32::MAX, so
    // a dimension in (1.07e9, 2.1e9] is legal per Cassandra and still overflows `4 * n`
    // — and there this is the only thing standing between that and a wrapped byte
    // count. Both guards are load-bearing; which one reports is target-dependent, so
    // no test may pin the message.
    //
    // Checked at the FLOAT element width, which is element-SPECIFIC and deliberately
    // so: this parser sees only the dimension token, never the element, so there is no
    // generally-correct width to check against — a check at width 1 would be
    // vacuously true. `float` is the only element #4114 decodes, and every other
    // element is refused by name at `require_float_element` before any width is
    // computed, so no unchecked path exists today. A future element type wider than 4
    // bytes must add its own check; it cannot inherit this one.
    if vector_byte_width(FLOAT_ELEMENT_WIDTH, dimension).is_none() {
        return Err(malformed(
            type_str,
            "the dimension's byte width overflows the addressable range",
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
    // `dimension == 0` is REFUSED, not multiplied (issue #4114, roborev job 111).
    // Cassandra cannot even CONSTRUCT a zero-dimension vector — `VectorType.java:89-90`
    // throws `InvalidRequestException("vectors may only have positive dimensions")` —
    // and an empty vector VALUE is `MarshalException("Invalid empty vector value")`
    // (`:365-368` via `:515-517`). Without this guard `0` multiplied cleanly to a width
    // of 0 and an EMPTY buffer then decoded as a successful `Value::List([])`: a value
    // Cassandra says cannot exist, produced without error. `CqlType::Vector` is
    // publicly constructible, so the string parser's own `n > 0` check is NOT
    // sufficient — a programmatically built `Vector(Float, 0)` bypasses it entirely,
    // which is why the guard belongs HERE, at the shared width helper every decode
    // entry point funnels through, rather than at one caller.
    if dimension == 0 {
        return None;
    }
    element_width.checked_mul(dimension)
}

/// The width Cassandra's `valueLengthIfFixed()` reports for a vector ELEMENT, or
/// `None` when that element is variable-length in Cassandra's FRAMING.
///
/// Deliberately NOT `CqlType::fixed_size()` (issue #4114, roborev job 111).
/// `fixed_size()` answers "how many bytes does one logical value occupy", which is a
/// DIFFERENT question from "does Cassandra frame this without a length prefix", and
/// it disagrees with the framing authority on five types: it reports `TinyInt(1)`,
/// `SmallInt(2)`, `Date(4)`, `Time(8)` and `Inet(16)` as fixed, while
/// `parser/repair_clustering.rs` — whose classification is derived from
/// `valueLengthIfFixed()` and pinned by tests — treats `InetAddressType`, `TimeType`
/// and `SimpleDateType` as VARIABLE, and its own tests assert tinyint and date
/// clusterings are vint-prefixed. (`fixed_size()`'s `Inet` arm even concedes the point
/// in a comment: IPv4 is 4 bytes, IPv6 is 16.)
///
/// That pre-existing disagreement is NOT corrected here — `fixed_size()` has no
/// callers other than its own recursion, so re-plumbing it is a separate change with
/// its own blast radius. What this function fixes is the vector arm's DEPENDENCE on
/// it: a vector must not inherit a framing claim from a method that does not answer
/// the framing question. Only element types whose Cassandra framing is KNOWN fixed
/// are listed, so an unlisted element yields `None` (variable) rather than a guess
/// (#28).
pub(crate) fn cassandra_fixed_element_width(element: &crate::schema::CqlType) -> Option<usize> {
    use crate::schema::CqlType as T;
    match element {
        T::Boolean => Some(1),
        T::Int => Some(4),
        T::Float => Some(4),
        T::BigInt | T::Counter | T::Double | T::Timestamp => Some(8),
        T::Uuid | T::TimeUuid => Some(16),
        // Everything else — including TinyInt, SmallInt, Date, Time and Inet — is
        // either variable in Cassandra's framing or not a scalar this function
        // speaks for. `None` means "not known fixed", never "zero width".
        _ => None,
    }
}

fn malformed(type_str: &str, why: &str) -> Error {
    Error::schema(format!("malformed vector type '{type_str}': {why}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PKG: &str = "org.apache.cassandra.db.marshal.";

    /// `into_args` for a string that MUST be a well-formed vector.
    fn marshal_args<'a>(ty: &'a str) -> VectorTypeArgs<'a> {
        marshal_vector_kind(ty)
            .into_args(ty)
            .expect("a well-formed vector type")
            .expect("a vector type")
    }

    fn cql_args<'a>(ty: &'a str) -> VectorTypeArgs<'a> {
        cql_vector_kind(ty)
            .into_args(ty)
            .expect("a well-formed vector type")
            .expect("a vector type")
    }

    #[test]
    fn marshal_spelling_cassandra_writes_is_parsed() {
        // The exact string `VectorType.toString` produces, spaces and all.
        let ty = format!("{PKG}VectorType({PKG}FloatType , 3)");
        let args = marshal_args(&ty);
        assert_eq!(args.element, format!("{PKG}FloatType"));
        assert_eq!(args.dimension, 3);
    }

    /// Regression, roborev job 109 (issue #4114): a `VectorType` whose parameter
    /// list cannot be EXTRACTED must be distinguishable from "not a vector type".
    ///
    /// The two used to collapse into `None`, so every caller read a malformed vector
    /// as "some other type" and sent it down a generic fallback — in
    /// `enhanced_statistics_parser::marshal_type` the `other => other.to_lowercase()`
    /// one, which restores the blob/phantom-vint framing #4114 exists to remove. The
    /// type STRINGS here are literals rather than fixture-derived on purpose: a
    /// malformed type string is a text-parsing question, not a byte-framing one, so
    /// no Cassandra-written bytes can express it (Cassandra's writer cannot emit one).
    #[test]
    fn a_malformed_vector_type_is_malformed_not_notavector() {
        for ty in [
            // Unmatched open paren — the case that reached the lowercase fallback.
            format!("{PKG}VectorType({PKG}FloatType , 3"),
            // A `VectorType` claim with no parameter list at all.
            format!("{PKG}VectorType"),
            "VectorType".to_string(),
            // Trailing text after the matched close paren.
            format!("{PKG}VectorType({PKG}FloatType , 3) , 4"),
        ] {
            assert!(
                matches!(marshal_vector_kind(&ty), VectorInner::Malformed(_)),
                "'{ty}' claims to be a VectorType and must be MALFORMED, not NotAVector"
            );
            let err = marshal_vector_kind(&ty)
                .into_args(&ty)
                .expect_err("a malformed vector type must fail closed")
                .to_string();
            assert!(
                err.contains(ty.trim()),
                "the refusal must name the type it refused: {err}"
            );
        }
    }

    /// The CQL-spelling half of the same defect class. `vector` WITHOUT parameters
    /// stays `NotAVector` — it is not a reserved word, so it can be a UDT name — but
    /// once a `<` follows, nothing else can be meant and an unterminated parameter
    /// list must fail closed rather than fall through to the UDT/`Custom` arm.
    #[test]
    fn an_unterminated_cql_vector_is_malformed_but_a_bare_vector_is_a_udt_name() {
        for ty in ["vector<float, 3", "vector<float, 3>>", "VECTOR<float"] {
            assert!(
                matches!(cql_vector_kind(ty), VectorInner::Malformed(_)),
                "'{ty}' must be MALFORMED"
            );
            assert!(cql_vector_kind(ty).into_args(ty).is_err(), "{ty}");
        }
        for ty in ["vector", "  vector  ", "vector_column", "vectorish<int>"] {
            assert!(
                matches!(cql_vector_kind(ty), VectorInner::NotAVector),
                "'{ty}' must stay available to the caller's UDT arm"
            );
            assert!(
                matches!(cql_vector_kind(ty).into_args(ty), Ok(None)),
                "{ty}"
            );
        }
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
            let args = marshal_args(&ty);
            assert_eq!(args.element, format!("{PKG}FloatType"), "{ty}");
            assert_eq!(args.dimension, 3, "{ty}");
        }
    }

    #[test]
    fn a_bare_constructor_name_is_accepted_and_a_foreign_one_is_not() {
        // `TypeParser.getAbstractType` resolves an unqualified name against the
        // marshal package, so the bare spelling is the same type.
        assert_eq!(marshal_args("VectorType(FloatType , 2)").dimension, 2);
        // Case matters: a lowercased string is not a Java class reference.
        assert!(matches!(
            marshal_vector_kind("vectortype(FloatType , 2)"),
            VectorInner::NotAVector
        ));
        // A different constructor is not a vector.
        assert!(matches!(
            marshal_vector_kind(&format!("{PKG}ListType({PKG}FloatType)")),
            VectorInner::NotAVector
        ));
        // A non-parameterised FOREIGN type is not a vector either.
        assert!(matches!(
            marshal_vector_kind(&format!("{PKG}FloatType")),
            VectorInner::NotAVector
        ));
    }

    #[test]
    fn a_parameterised_element_keeps_its_own_commas() {
        // The element's commas are at depth > 0; only the LAST top-level comma
        // separates the dimension.
        let ty = format!("{PKG}VectorType({PKG}TupleType({PKG}Int32Type,{PKG}UTF8Type) , 7)");
        let args = marshal_args(&ty);
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
            let args = cql_args(ty);
            assert!(args.element.eq_ignore_ascii_case("float"), "{ty}");
            assert_eq!(args.dimension, 3, "{ty}");
        }
        assert!(matches!(
            cql_vector_kind("list<float>"),
            VectorInner::NotAVector
        ));
        // A nested element keeps its own angle brackets.
        let ty = "vector<frozen<tuple<int, text>>, 4>";
        let args = cql_args(ty);
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

    // ── roborev job 111 ────────────────────────────────────────────────────────

    /// A zero dimension is REFUSED at the shared width helper, so no decode entry
    /// point can be reached with it.
    ///
    /// `CqlType::Vector` is publicly constructible, so the string parser's `n > 0`
    /// check does not cover a programmatically built `Vector(Float, 0)`. Before this
    /// guard, `0` multiplied cleanly to width 0 and an EMPTY buffer decoded as a
    /// successful `Value::List([])` — a value Cassandra says cannot exist
    /// (`VectorType.java:89-90` refuses n <= 0 at construction; `:365-368` throws
    /// `MarshalException("Invalid empty vector value")`).
    /// roborev job 113: an overflowing dimension is refused AT PARSE TIME, not only
    /// later at width computation.
    ///
    /// TWO guards now stand between a raw token and `CqlType::Vector`, and WHICH ONE
    /// fires is TARGET-DEPENDENT — which is why this asserts REFUSAL rather than a
    /// message. Measured:
    ///
    /// | target | `usize::MAX / 4`      | `i32::MAX`    | width guard reachable? |
    /// |--------|-----------------------|---------------|------------------------|
    /// | 64-bit | 4_611_686_018_427_387_903 | 2_147_483_647 | NO — the ceiling always fires first |
    /// | 32-bit | 1_073_741_823         | 2_147_483_647 | YES — (1.07e9, 2.1e9] passes the ceiling and overflows `4 * n` |
    ///
    /// So on 64-bit the Integer.MAX_VALUE ceiling subsumes the width guard, while on
    /// 32-bit the width guard is the only thing standing between a legal-per-Cassandra
    /// dimension and a wrapped byte count. BOTH are load-bearing; neither is dead.
    ///
    /// An earlier version of this test asserted the message contained "overflow" and
    /// broke the moment the ceiling landed (it fired first and said
    /// "Integer.MAX_VALUE" instead) — one unit test failing FOUR gate components,
    /// since core-tests / write-tests / legacy-heuristics / feature-iso-delta-scan all
    /// run this lib. Pinning a message that guard ORDERING can change was the mistake.
    #[test]
    fn an_overflowing_dimension_is_refused_at_parse_time_not_only_at_width() {
        let ty = "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType , n)";

        // A dimension whose 4*n width cannot be represented must be REFUSED at parse
        // time by SOME guard. Which one is target-dependent, so do not pin the text.
        let width_overflow = (usize::MAX / FLOAT_ELEMENT_WIDTH) + 1;
        assert!(
            parse_vector_dimension(&width_overflow.to_string(), ty).is_err(),
            "a dimension whose byte width cannot be represented must be refused at \
             PARSE time, by whichever guard applies on this target"
        );
        assert!(
            parse_vector_dimension(&usize::MAX.to_string(), ty).is_err(),
            "usize::MAX must be refused"
        );

        // The representable ones still parse.
        for ok in ["1", "3", "384", "4096"] {
            assert!(
                parse_vector_dimension(ok, ty).is_ok(),
                "{ok} is representable and must parse"
            );
        }
    }

    #[test]
    fn zero_dimension_has_no_width() {
        assert_eq!(
            vector_byte_width(4, 0),
            None,
            "a zero dimension must have NO width, not a width of 0 — a width of 0 \
             lets an empty buffer decode as an empty vector"
        );
        // Any element width, same answer: the refusal is on the DIMENSION.
        for w in [1usize, 2, 4, 8, 16] {
            assert_eq!(vector_byte_width(w, 0), None, "element width {w}, n=0");
        }
        // And a positive dimension still computes.
        assert_eq!(vector_byte_width(4, 1), Some(4));
        assert_eq!(vector_byte_width(4, 3), Some(12));
        assert_eq!(vector_byte_width(4, 384), Some(1536));
        // Overflow still refused (the pre-existing checked_mul contract).
        assert_eq!(vector_byte_width(4, usize::MAX), None);
    }

    /// The element width used for a vector's framing is Cassandra's
    /// `valueLengthIfFixed()`, NOT `CqlType::fixed_size()`.
    ///
    /// `fixed_size()` reports TinyInt/SmallInt/Date/Time/Inet as fixed, while the
    /// framing authority (`parser/repair_clustering.rs`, derived from
    /// `valueLengthIfFixed()` and pinned by its own tests) treats
    /// `InetAddressType`/`TimeType`/`SimpleDateType` as VARIABLE and asserts tinyint
    /// and date clusterings are vint-prefixed. A vector must not inherit a framing
    /// claim from a method that answers a different question.
    #[test]
    fn framing_width_is_not_fixed_size() {
        use crate::schema::CqlType as T;

        // KNOWN-FIXED elements, with Cassandra's widths.
        for (ty, want) in [
            (T::Boolean, 1usize),
            (T::Int, 4),
            (T::Float, 4),
            (T::BigInt, 8),
            (T::Counter, 8),
            (T::Double, 8),
            (T::Timestamp, 8),
            (T::Uuid, 16),
            (T::TimeUuid, 16),
        ] {
            assert_eq!(
                cassandra_fixed_element_width(&ty),
                Some(want),
                "{ty:?} must be fixed at {want} bytes"
            );
        }

        // THE REGRESSION: the five types `fixed_size()` gets wrong for framing.
        // Each of these returns Some(..) from fixed_size(), so a vector arm built on
        // that method claimed a fixed width Cassandra does not use.
        for ty in [T::TinyInt, T::SmallInt, T::Date, T::Time, T::Inet] {
            assert_eq!(
                cassandra_fixed_element_width(&ty),
                None,
                "{ty:?} is VARIABLE in Cassandra framing and must NOT report a fixed width"
            );
            assert!(
                ty.fixed_size().is_some(),
                "{ty:?} is expected to still report a logical fixed_size() — this test \
                 exists precisely because the two answers differ"
            );
        }

        // Genuinely variable elements stay None.
        for ty in [T::Text, T::Blob, T::Decimal, T::Duration, T::Varint] {
            assert_eq!(cassandra_fixed_element_width(&ty), None, "{ty:?}");
        }
    }
}
