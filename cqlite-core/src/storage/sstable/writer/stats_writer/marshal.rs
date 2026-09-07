//! CQL type name → Cassandra internal marshal-type conversion.
//!
//! Used when writing the SERIALIZATION_HEADER component of Statistics.db.

// DECLARED ABOVE THE FIRST `use` ON PURPOSE (issue #1714): this file's comments
// quote Java method bodies (`VectorType.toString`, `stringifyVectorParameters`)
// that carry braces, and the mod-reachability guard tracks inline-`mod` scope by
// counting braces as it walks the file — a late top-level `mod` is then resolved
// against a phantom scope directory. Same hazard, same remedy, as
// `schema::cql_type_parser::frozen_scalar` (#4153).
#[cfg(test)]
mod issue_4158_vector_tests;

use crate::error::{Error, Result};
use crate::schema::vector_type::cql_vector_kind;

/// Convert a CQL type name to Cassandra internal marshal type.
///
/// This is the reverse of `convert_marshal_type_to_cql` in enhanced_statistics_parser.rs.
/// Used when writing the SERIALIZATION_HEADER component of Statistics.db.
///
/// Handles:
/// - Primitive types: text, int, bigint, uuid, etc.
/// - Collections: list<T>, set<T>, map<K,V>
/// - Frozen wrappers: frozen<list<T>>, frozen<map<K,V>>
/// - Tuples: tuple<T1, T2, ...>
///
/// - Vectors: `vector<element, n>` (issue #4158) — see the arm in
///   [`render_marshal_type`] for the pinned derivation and for the element types
///   it REFUSES by name.
///
/// This is Cassandra's `AbstractType::toString(false)` (issue #4158): the entry
/// point is always the un-frozen, "print the `FrozenType(...)` wrapper where it
/// belongs" case, and [`render_marshal_type`] carries the `ignoreFreezing` flag
/// Cassandra threads through the type tree.
///
/// Returns `Err` only where a truthful spelling cannot be produced (the vector arm;
/// issue #4158). Every other type keeps its pre-existing total behaviour, including
/// the `_ => BytesType` fallback for an unresolvable bare name (#929) — widening
/// that fallback into a refusal is NOT this function's job and would change the
/// documented degradation of every unresolved UDT column.
pub(crate) fn cql_type_to_marshal_type(cql_type: &str) -> Result<String> {
    render_marshal_type(cql_type, false, false)
}

/// The UDT-FIELD-path disposition of [`cql_type_to_marshal_type`]: identical, except
/// that a refusal degrades to `BytesType` instead of propagating.
///
/// Used ONLY by `data_writer::schema_helpers`, whose `UserType(...)` renderers are
/// infallible by contract and already DECLARE this exact degradation for a field
/// they cannot fully represent (`render_udt_marshal`'s scope note; issues
/// #929/#1011): the direct-write value path writes such a field as an opaque
/// length-prefixed blob, so `BytesType` keeps the header and the value bytes
/// SELF-CONSISTENT, which is the property that renderer is documented to preserve.
/// #4158 changes the COLUMN path's contract, not that one.
///
/// Two of its three call sites are provably unreachable-with-an-error, and that is
/// stated rather than assumed: the ONLY refusal `cql_type_to_marshal_type` can
/// produce is the vector one, and `render_field_marshal` gives `CqlType::Vector` its
/// own arm, so its `Custom`/scalar tails never see a vector. The reachable site is
/// `render_udt_marshal` (the registry-LESS renderer), where a `vector<..>` field
/// whose element resolves to no marshal class takes the declared degradation.
pub(crate) fn cql_type_to_marshal_type_or_bytes(cql_type: &str) -> String {
    cql_type_to_marshal_type(cql_type)
        .unwrap_or_else(|_| "org.apache.cassandra.db.marshal.BytesType".to_string())
}

/// Marshal type names (without the `org.apache.cassandra.db.marshal.` prefix)
/// whose `toString(boolean)` carries the
/// `includeFrozenType = !ignoreFreezing && !isMultiCell` branch — i.e. the ONLY
/// types that can appear as the immediate inner of a `FrozenType(` in a header
/// Apache Cassandra wrote (pinned `cassandra-5.0.8`: `ListType.java:195-207`,
/// `SetType.java:185-197`, `MapType.java:310-321`, `UserType.java:436-448`).
///
/// Nothing else has that branch: every scalar inherits
/// `AbstractType.toString() == getClass().getName()` (`AbstractType.java:741`)
/// with `toString(boolean) == toString()` (`AbstractType.java:466`), and
/// `TupleType.toString()` is `getClass().getName() + stringifyTypeParameters(types, true)`
/// (`TupleType.java:557-560`) — a CQL tuple is already frozen and is never
/// wrapped. `CQL3Type.Raw::freeze()` correspondingly throws for every
/// non-collection/tuple/UDT/vector (`CQL3Type.java:647-651`).
const FREEZE_WRAPPED_HEADS: [&str; 4] = ["ListType(", "SetType(", "MapType(", "UserType("];

/// True iff Cassandra would print a `FrozenType(...)` wrapper around the
/// already-rendered marshal string `rendered`.
///
/// Matching is EXACT-CASE on purpose: the marshal grammar is case-sensitive
/// (`TypeParser` resolves class names verbatim), so a differently-cased inner is
/// not a type Cassandra can parse and must not be handed a wrapper that would
/// make the header unparseable in a NEW way.
pub(crate) fn takes_frozen_type_wrapper(rendered: &str) -> bool {
    match rendered.strip_prefix("org.apache.cassandra.db.marshal.") {
        Some(rest) => FREEZE_WRAPPED_HEADS.iter().any(|h| rest.starts_with(h)),
        None => false,
    }
}

/// Render `cql_type` as a Cassandra marshal string, mirroring
/// `AbstractType::toString(boolean ignoreFreezing)`.
///
/// `ignore_freezing` is Cassandra's flag: a parent passes
/// `ignoreFreezing || !isMultiCell` to its subtypes, so every child of a FROZEN
/// (never-multicell) collection/UDT — and every component of a `TupleType`,
/// which always passes `true` — drops its own `FrozenType(...)` wrapper, while a
/// child of a MULTICELL collection keeps it. Corpus-attested both ways:
/// `LIST<FROZEN<address_type>>` → `ListType(FrozenType(UserType(...)))`, but the
/// `address` field INSIDE `FrozenType(UserType(...,contact_info,...))` is the
/// bare `UserType(...)`.
/// `refuse_unknown` is set ONLY inside a vector element (issue #4158) and is
/// inherited by every nested call. It flips the unknown-bare-name tail from the
/// documented `BytesType` degradation (#929) to a NAMED refusal, because a vector's
/// element type is what decides the value's on-disk WIDTH: `VectorType`'s
/// `valueLengthIfFixed()` is `elementType.valueLengthIfFixed() * dimension` when the
/// element is fixed and `VARIABLE_LENGTH` otherwise (`VectorType.java:94-96`). So a
/// wrong element type there is not a lost type name, it is a wrong FRAMING stamped
/// into `Statistics.db` — the reader derives the vector's layout from exactly this
/// string (`parser::repair_clustering::resolve_clustering_value_layout`, #4149).
/// Outside a vector element the flag is `false` and the fallback is untouched.
fn render_marshal_type(
    cql_type: &str,
    ignore_freezing: bool,
    refuse_unknown: bool,
) -> Result<String> {
    // Already a marshal string (e.g. a UDT column normalized to UserType(...),
    // or a column type read back from an input SSTable's SerializationHeader):
    // return it verbatim. The marshal grammar is case-sensitive (UserType,
    // Int32Type, ...), so this MUST happen before the lowercasing below, and the
    // original-case string MUST be preserved. Without this, an already-marshaled
    // type would fall through to BytesType, advertising the wrong type in the
    // header while Data.db carries the real (e.g. complex UDT) cells (#929).
    let raw = cql_type.trim();
    if raw
        .to_lowercase()
        .starts_with("org.apache.cassandra.db.marshal.")
    {
        return Ok(raw.to_string());
    }

    let prefix = "org.apache.cassandra.db.marshal.";

    // Handle parameterized types: list<T>, set<T>, map<K,V>, frozen<T>, tuple<T1,T2>.
    //
    // Every wrapper is stripped from the ORIGINAL-CASE string (CQL type names
    // are case-insensitive but the marshal names nested inside are NOT — see
    // `strip_cql_wrapper`), and the inner is re-entered through this same
    // function so the `ignore_freezing` flag propagates exactly as Cassandra
    // propagates it.
    //
    // A bare `list`/`set`/`map` is MULTICELL, so it never prints a wrapper for
    // itself (that is the `frozen<..>` arm's job) and passes `ignore_freezing`
    // through unchanged (`ignoreFreezing || !isMultiCell` with `isMultiCell`).
    if let Some(inner) = strip_cql_wrapper(raw, "list") {
        return Ok(format!(
            "{prefix}ListType({})",
            render_marshal_type(inner, ignore_freezing, refuse_unknown)?
        ));
    }
    if let Some(inner) = strip_cql_wrapper(raw, "set") {
        return Ok(format!(
            "{prefix}SetType({})",
            render_marshal_type(inner, ignore_freezing, refuse_unknown)?
        ));
    }
    if let Some(inner) = strip_cql_wrapper(raw, "map") {
        let args = split_cql_type_args(inner);
        if args.len() == 2 {
            return Ok(format!(
                "{prefix}MapType({},{})",
                render_marshal_type(args[0], ignore_freezing, refuse_unknown)?,
                render_marshal_type(args[1], ignore_freezing, refuse_unknown)?
            ));
        }
        // Malformed map type — fall through to BytesType
    }
    if let Some(inner) = strip_cql_wrapper(raw, "frozen") {
        // `frozen<T>` is `T.freeze()`: never multicell, so its subtypes are
        // stringified with `ignoreFreezing = true` (which also makes `freeze()`
        // idempotent — a nested `frozen<..>` collapses).
        let rendered = render_marshal_type(inner, true, refuse_unknown)?;
        // Print the wrapper only where Cassandra's own writer can: not when a
        // frozen/tuple parent already suppressed it, and never around a type
        // that has no `includeFrozenType` branch. Emitting
        // `FrozenType(<scalar>)` — e.g. `FrozenType(BytesType)` for a `frozen<udt>`
        // whose name no `UdtRegistry` could resolve — advertises a type no
        // Cassandra writer can produce (#4158). Dropping the wrapper leaves the
        // legal spelling of the inner, which for an unresolved UDT name is the
        // same `BytesType` degradation the BARE unresolved name already takes
        // (#929: no registry => single simple cell).
        if ignore_freezing || !takes_frozen_type_wrapper(&rendered) {
            return Ok(rendered);
        }
        return Ok(format!("{prefix}FrozenType({rendered})"));
    }
    if let Some(inner) = strip_cql_wrapper(raw, "tuple") {
        // `TupleType.toString()` has no `includeFrozenType` branch and passes
        // `true` for its components (`TupleType.java:557-560`).
        let args = split_cql_type_args(inner);
        let components = args
            .iter()
            .map(|a| render_marshal_type(a, true, refuse_unknown))
            .collect::<Result<Vec<String>>>()?;
        return Ok(format!("{prefix}TupleType({})", components.join(",")));
    }

    // ═══ `vector<element, n>` — issue #4158, the FAIL-OPEN this arm removes ═══
    //
    // Without it a vector column reached the `_ => BytesType` fallback below and
    // CQLite stamped `org.apache.cassandra.db.marshal.BytesType` into the
    // SerializationHeader — the same defect class as the `FrozenType(BytesType)`
    // bug this file already fixed: a WRONG type recorded in `Statistics.db`, and one
    // that compiles silently because the fallback is over a `&str`, not a `CqlType`.
    //
    // THE SPELLING, DERIVED AT THE PINNED TAG (`cassandra-5.0.8`):
    //   VectorType.java:338-342
    //     public String toString(boolean ignoreFreezing) {
    //         return getClass().getName()
    //              + TypeParser.stringifyVectorParameters(elementType, ignoreFreezing, dimension);
    //     }
    //   TypeParser.java:239-242
    //     public static String stringifyVectorParameters(AbstractType<?> type, boolean ignoreFreezing, int dimension) {
    //         return "(" + type.toString(ignoreFreezing) + " , " + dimension + ")";
    //     }
    // So: the package-qualified class name, then `(`, the ELEMENT's own
    // `toString(ignoreFreezing)`, then the literal `" , "` (SPACE comma SPACE — not
    // the bare `,` that `stringifyTypeParameters` uses for collections), the decimal
    // dimension, `)`.
    //
    // BYTE ORACLE, Cassandra-written, not CQLite output: the header of
    // `test-data/fixtures/issue_4114/test_vector/vector_exact-*/nb-1-big-Statistics.db`
    // carries, at a VInt length of 0x59 = 89,
    //   org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType , 3)
    // and `vector_pk_only-*` carries the same shape at dimension 1 and 384. Pinned in
    // `issue_4158_vector_marshal_parity::cassandra_written_vector_header_is_reproduced`.
    //
    // NO `FrozenType(` WRAPPER, EVER: `VectorType.toString(boolean)` has no
    // `includeFrozenType` branch (it is the two lines quoted above), so a vector is
    // never wrapped — which is why `VectorType(` is absent from
    // [`FREEZE_WRAPPED_HEADS`] and `frozen<vector<float, 3>>` renders as the bare
    // `VectorType(FloatType , 3)`. That matches CQL: `RawVector::freeze()` returns
    // `this` (`CQL3Type.java:915-919`), so `frozen<vector<..>>` and `vector<..>` are
    // the SAME Cassandra type with one spelling.
    //
    // THE PARAMETERS COME FROM THE ONE SHARED PARSER (`schema::vector_type`), never
    // from a second `<`/`>` walk here: `Ok(None)` is "not a vector" and falls
    // through, and a MALFORMED vector is its `Err`, propagated — so a bad dimension
    // or a third parameter is refused BY NAME instead of degrading to `BytesType`.
    // The ELEMENT is rendered by THIS SAME recursion at the SAME `ignore_freezing`
    // — `stringifyVectorParameters` passes the flag straight through
    // (`type.toString(ignoreFreezing)`), unlike `stringifyTypeParameters(types, true)`
    // which a tuple forces to `true`. For a scalar element the flag provably cannot
    // matter (`AbstractType.toString(boolean)` is `return this.toString();`,
    // `AbstractType.java:466-469`), but it is threaded rather than assumed away so a
    // parametric element behaves like every other subtype in the tree.
    //
    // `refuse_unknown = true` for the element and everything under it: see that
    // parameter's doc. It converts the ONE untruthful outcome — the unknown-bare-name
    // `BytesType` degradation — into a named refusal, and it is what makes this arm's
    // claim total rather than "total except when the element is a UDT nobody
    // registered".
    if let Some(args) = cql_vector_kind(raw).into_args(raw)? {
        return Ok(format!(
            "{prefix}VectorType({} , {})",
            render_marshal_type(args.element.trim(), ignore_freezing, true)?,
            args.dimension
        ));
    }

    // Primitive types. CQL type names are case-insensitive, and the parser may
    // preserve original case from CQL files (e.g. "SET<TEXT>"), so match on a
    // lowercased copy.
    //
    // The fallback is UNCHANGED (#929): an unresolvable bare name renders as
    // `BytesType`, which is the documented degradation for a UDT column with no
    // `UdtRegistry` in play. Only the VECTOR arm above refuses, and only because a
    // vector's element type is what decides the value's on-disk WIDTH.
    match primitive_marshal_name(&raw.to_lowercase()) {
        Some(name) => Ok(format!("{prefix}{name}")),
        // Fallback: use BytesType for unknown types — UNLESS this name is (inside)
        // a vector element, where it would be a wrong WIDTH rather than a lost name.
        None if refuse_unknown => Err(Error::unsupported_format(format!(
            "cannot write a truthful SerializationHeader type for a vector whose \
             element type is or contains '{raw}': this converter resolves no marshal \
             class for that name (it has no UDT registry), and Cassandra spells a \
             vector as VectorType(<element>.toString(ignoreFreezing) , <dimension>) \
             (TypeParser.stringifyVectorParameters, TypeParser.java:239-242, reached \
             from VectorType.toString, VectorType.java:338-342). Substituting the \
             BytesType fallback would record a wrong element type — and therefore a \
             wrong value width, VectorType.java:94-96 — in Statistics.db (issue \
             #4158), so this is refused by name rather than guessed (issue #28)"
        ))),
        None => Ok(format!("{prefix}BytesType")),
    }
}

/// The marshal SIMPLE NAME of a native CQL scalar, or `None` if `lower` (already
/// lowercased) names no scalar this converter knows.
///
/// Split out of [`render_marshal_type`]'s tail so the vector arm can ASK whether an
/// element is a known scalar without having to recognise the `_ => BytesType`
/// fallback after the fact — `blob` legitimately renders to `BytesType`, so the
/// rendered string cannot distinguish "blob" from "unknown". The mapping itself is
/// unchanged.
fn primitive_marshal_name(lower: &str) -> Option<&'static str> {
    Some(match lower {
        "text" | "varchar" => "UTF8Type",
        "int" => "Int32Type",
        "bigint" => "LongType",
        "smallint" => "ShortType",
        "tinyint" => "ByteType",
        "float" => "FloatType",
        "double" => "DoubleType",
        "boolean" => "BooleanType",
        "blob" => "BytesType",
        "uuid" => "UUIDType",
        "timeuuid" => "TimeUUIDType",
        "timestamp" => "TimestampType",
        "date" => "SimpleDateType",
        "time" => "TimeType",
        "duration" => "DurationType",
        "inet" => "InetAddressType",
        "ascii" => "AsciiType",
        "decimal" => "DecimalType",
        "varint" => "IntegerType",
        "counter" => "CounterColumnType",
        _ => return None,
    })
}

/// Strip a CQL wrapper type like `list<inner>` and return the inner string.
/// Returns None if `cql_type` does not start with `wrapper<`.
///
/// The wrapper keyword is matched CASE-INSENSITIVELY (CQL type names are
/// case-insensitive; a schema may carry `SET<TEXT>`), but the slice returned is
/// of the ORIGINAL-CASE input. That matters: an inner that is itself an
/// `org.apache.cassandra.db.marshal.*` string must reach the recursion with its
/// case intact, because the marshal grammar is case-sensitive — lowercasing
/// first turned `list<...UserType(...)>` into an unparseable
/// `ListType(...usertype(...))` (#4158).
fn strip_cql_wrapper<'a>(cql_type: &'a str, wrapper: &str) -> Option<&'a str> {
    if !cql_type.is_char_boundary(wrapper.len()) {
        return None;
    }
    let (head, tail) = cql_type.split_at(wrapper.len());
    if !head.eq_ignore_ascii_case(wrapper) {
        return None;
    }
    let rest = tail.strip_prefix('<')?;
    // Find the matching closing '>' (handling nested angle brackets)
    let mut depth = 1;
    for (i, ch) in rest.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => {
                depth -= 1;
                if depth == 0 {
                    return Some(rest[..i].trim());
                }
            }
            _ => {}
        }
    }
    None
}

/// Split CQL type arguments at top-level commas (respecting nested angle brackets).
/// E.g. `"int, map<text, int>"` → `["int", "map<text, int>"]`
fn split_cql_type_args(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, ch) in s.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => {
                result.push(s[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = s[start..].trim();
    if !last.is_empty() {
        result.push(last);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `cql_type_to_marshal_type` returns `Result` since #4158 (the vector arm
    /// refuses rather than guesses); these cases are all types it must render.
    fn marshal(cql: &str) -> String {
        cql_type_to_marshal_type(cql).expect("a renderable marshal type")
    }

    #[test]
    fn test_marshal() {
        assert_eq!(marshal("text"), "org.apache.cassandra.db.marshal.UTF8Type");
        assert_eq!(marshal("int"), "org.apache.cassandra.db.marshal.Int32Type");
        assert_eq!(
            marshal("bigint"),
            "org.apache.cassandra.db.marshal.LongType"
        );
        assert_eq!(marshal("uuid"), "org.apache.cassandra.db.marshal.UUIDType");
        assert_eq!(marshal("blob"), "org.apache.cassandra.db.marshal.BytesType");
        assert_eq!(
            marshal("timestamp"),
            "org.apache.cassandra.db.marshal.TimestampType"
        );
        assert_eq!(
            marshal("boolean"),
            "org.apache.cassandra.db.marshal.BooleanType"
        );
        assert_eq!(
            marshal("varint"),
            "org.apache.cassandra.db.marshal.IntegerType"
        );
        // Unknown type falls back to BytesType
        assert_eq!(
            marshal("unknown_type"),
            "org.apache.cassandra.db.marshal.BytesType"
        );

        // Already-marshaled strings pass through verbatim, case preserved (#929).
        // This is what a normalized bare-UDT column carries, and what columns
        // read back from an input SSTable's SerializationHeader look like.
        let user_type =
            "org.apache.cassandra.db.marshal.UserType(ks,706572736f6e,6e616d65:org.apache.cassandra.db.marshal.UTF8Type)";
        assert_eq!(marshal(user_type), user_type);
        let int_type = "org.apache.cassandra.db.marshal.Int32Type";
        assert_eq!(marshal(int_type), int_type);
        // Whitespace around a marshal string is trimmed but case is preserved.
        assert_eq!(
            marshal("  org.apache.cassandra.db.marshal.UTF8Type  "),
            "org.apache.cassandra.db.marshal.UTF8Type"
        );

        // Collection types
        assert_eq!(
            marshal("list<int>"),
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)"
        );
        assert_eq!(
            marshal("set<text>"),
            "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)"
        );
        assert_eq!(
            marshal("map<text, int>"),
            "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)"
        );

        // Frozen and nested
        assert_eq!(
            marshal("frozen<list<int>>"),
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))"
        );

        // Tuple
        assert_eq!(
            marshal("tuple<int, text>"),
            "org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.UTF8Type)"
        );
    }
}

/// Issue #4158 — Cassandra-attested `FrozenType(...)` wrapper eligibility.
///
/// ORACLE (pinned `cassandra-5.0.8`, NOT CQLite's own output): only four
/// marshal types carry the `includeFrozenType = !ignoreFreezing && !isMultiCell`
/// branch that emits the wrapper, so only these four can EVER appear as the
/// immediate inner of a `FrozenType(`:
///
/// * `ListType.toString(boolean)`  — `ListType.java:195-207`
/// * `SetType.toString(boolean)`   — `SetType.java:185-197`
/// * `MapType.toString(boolean)`   — `MapType.java:310-321`
/// * `UserType.toString(boolean)`  — `UserType.java:436-448`
///
/// Everything else cannot be wrapped:
/// * every scalar inherits `AbstractType.toString() == getClass().getName()`
///   (`AbstractType.java:741-744`) and `toString(boolean) == toString()`
///   (`AbstractType.java:466-469`) — no wrapper branch at all;
/// * `TupleType.toString()` is `getClass().getName() + stringifyTypeParameters(types, true)`
///   (`TupleType.java:557-560`) — it never emits the wrapper for itself and
///   always stringifies its components with `ignoreFreezing = true`.
///
/// Corroborated by the corpus census (`test-data` / `CQLITE_DATASETS_ROOT`): of
/// every `FrozenType(` Apache Cassandra wrote across the corpus the immediate
/// inner head is only ever MapType / ListType / UserType / SetType — never a
/// scalar and never TupleType. The end-to-end oracle lives in
/// `cqlite-core/tests/issue_4158_frozen_wrapper_cassandra_oracle.rs`.
#[cfg(test)]
mod frozen_wrapper_cassandra_parity {
    use super::*;

    const Q: &str = "org.apache.cassandra.db.marshal.";

    /// See the sibling helper in `mod tests`: #4158 made the converter fallible.
    fn marshal(cql: &str) -> String {
        cql_type_to_marshal_type(cql).expect("a renderable marshal type")
    }

    /// A `frozen<...>` whose inner is a bare name this string converter cannot
    /// resolve (a UDT with no `UdtRegistry` in play) fell through to the
    /// `_ => BytesType` arm and was then WRAPPED, emitting
    /// `FrozenType(BytesType)` — a spelling no Cassandra writer can produce
    /// (`BytesType` is `blob`, a scalar; `CQL3Type.Raw::freeze()` throws for
    /// every non-collection/tuple/UDT/vector — `CQL3Type.java:647-651`).
    /// The wrapper must be DROPPED, leaving the legal scalar spelling — the
    /// same documented degradation a BARE unresolved UDT name already takes
    /// (issue #929: no registry => single simple cell).
    #[test]
    fn frozen_unresolvable_name_never_wraps_a_scalar() {
        assert_eq!(
            marshal("frozen<person>"),
            format!("{Q}BytesType"),
            "frozen<unresolved-udt> must degrade to the same legal spelling as \
             the bare unresolved name, never FrozenType(<scalar>)"
        );
        // The bare-name fallback it must agree with.
        assert_eq!(
            marshal("person"),
            marshal("frozen<person>"),
            "the frozen and bare unresolved-name fallbacks must agree"
        );
    }

    /// `frozen<scalar>` is not even expressible in CQL, but the converter is
    /// reachable with one (a hand-built schema, an exporter round-trip). It must
    /// never fabricate the impossible wrapper.
    #[test]
    fn frozen_scalar_never_wraps() {
        for (cql, expected) in [
            ("frozen<int>", "Int32Type"),
            ("frozen<text>", "UTF8Type"),
            ("frozen<blob>", "BytesType"),
            ("frozen<uuid>", "UUIDType"),
            ("frozen<timestamp>", "TimestampType"),
        ] {
            assert_eq!(
                marshal(cql),
                format!("{Q}{expected}"),
                "{cql} must render as the bare scalar"
            );
        }
    }

    /// `TupleType.toString()` (`TupleType.java:557-560`) has NO
    /// `includeFrozenType` branch, so Cassandra never wraps a tuple — a CQL
    /// tuple is already frozen. Corpus-confirmed: every `TupleType(` Cassandra
    /// wrote in the corpus is unwrapped, and no `FrozenType(TupleType` exists
    /// anywhere in it.
    #[test]
    fn frozen_tuple_never_wraps() {
        let expected = format!("{Q}TupleType({Q}Int32Type,{Q}UTF8Type)");
        assert_eq!(marshal("tuple<int, text>"), expected);
        assert_eq!(
            marshal("frozen<tuple<int, text>>"),
            expected,
            "frozen<tuple<..>> and tuple<..> are the SAME Cassandra type and \
             share one spelling"
        );
    }

    /// `TupleType` stringifies its components with `ignoreFreezing = true`, and
    /// a frozen collection/UserType propagates `ignoreFreezing || !isMultiCell`
    /// to ITS subtypes (`stringifyTypeParameters(..., ignoreFreezing || !isMultiCell)`).
    /// So a frozen type nested inside an already-frozen (or tuple) parent loses
    /// its own wrapper. Corpus-attested for the UserType case: inside
    /// `FrozenType(UserType(test_collections,contact_info,...))` the nested
    /// `address` field is the BARE `UserType(...)`, not `FrozenType(UserType(...))`.
    #[test]
    fn ignore_freezing_propagates_into_frozen_and_tuple_children() {
        assert_eq!(
            marshal("tuple<int, frozen<list<int>>>"),
            format!("{Q}TupleType({Q}Int32Type,{Q}ListType({Q}Int32Type))"),
            "a tuple component is stringified with ignoreFreezing = true"
        );
        assert_eq!(
            marshal("frozen<list<frozen<set<int>>>>"),
            format!("{Q}FrozenType({Q}ListType({Q}SetType({Q}Int32Type)))"),
            "a frozen child of a frozen parent drops its own wrapper"
        );
    }

    /// The REGRESSION half: every spelling Apache Cassandra itself wrote into
    /// the corpus must be reproduced byte-for-byte. Each expectation below is
    /// the exact string extracted from a Cassandra-written `Statistics.db`
    /// (`test_collections/frozen_collections_table-*`,
    /// `test_collections/typed_collections_table-*`), never from CQLite output.
    #[test]
    fn cassandra_written_spellings_are_unchanged() {
        // FROZEN<SET<TEXT>> / FROZEN<LIST<INT>> / FROZEN<MAP<TEXT,TEXT>> and a
        // non-frozen SET<TEXT>, all from frozen_collections_table's header.
        assert_eq!(
            marshal("frozen<set<text>>"),
            format!("{Q}FrozenType({Q}SetType({Q}UTF8Type))")
        );
        assert_eq!(
            marshal("frozen<list<int>>"),
            format!("{Q}FrozenType({Q}ListType({Q}Int32Type))")
        );
        assert_eq!(
            marshal("frozen<map<text, text>>"),
            format!("{Q}FrozenType({Q}MapType({Q}UTF8Type,{Q}UTF8Type))")
        );
        assert_eq!(marshal("set<text>"), format!("{Q}SetType({Q}UTF8Type)"));
        // A non-frozen (multicell) collection propagates ignoreFreezing = false,
        // so a FROZEN element KEEPS its wrapper. Corpus-attested by
        // collections_with_udts: `LIST<FROZEN<address_type>>` ->
        // `ListType(FrozenType(UserType(...)))`.
        assert_eq!(
            marshal("list<frozen<set<int>>>"),
            format!("{Q}ListType({Q}FrozenType({Q}SetType({Q}Int32Type)))"),
            "a frozen element of a MULTICELL collection keeps its wrapper"
        );
        // An already-marshaled UserType inner keeps the wrapper (the resolved
        // frozen-UDT column shape asserted against Cassandra bytes by
        // `frozen_udt_column_renders_frozentype_usertype_cassandra_parity`).
        let user_type = format!("{Q}UserType(ks,706572736f6e,6e616d65:{Q}UTF8Type)");
        assert_eq!(
            marshal(&format!("frozen<{user_type}>")),
            format!("{Q}FrozenType({user_type})")
        );
    }
}
