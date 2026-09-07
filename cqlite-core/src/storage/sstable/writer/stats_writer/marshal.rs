//! CQL type name → Cassandra internal marshal-type conversion.
//!
//! Used when writing the SERIALIZATION_HEADER component of Statistics.db.

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
/// This is Cassandra's `AbstractType::toString(false)` (issue #4158): the entry
/// point is always the un-frozen, "print the `FrozenType(...)` wrapper where it
/// belongs" case, and [`render_marshal_type`] carries the `ignoreFreezing` flag
/// Cassandra threads through the type tree.
pub(crate) fn cql_type_to_marshal_type(cql_type: &str) -> String {
    render_marshal_type(cql_type, false)
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
fn render_marshal_type(cql_type: &str, ignore_freezing: bool) -> String {
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
        return raw.to_string();
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
        return format!(
            "{prefix}ListType({})",
            render_marshal_type(inner, ignore_freezing)
        );
    }
    if let Some(inner) = strip_cql_wrapper(raw, "set") {
        return format!(
            "{prefix}SetType({})",
            render_marshal_type(inner, ignore_freezing)
        );
    }
    if let Some(inner) = strip_cql_wrapper(raw, "map") {
        let args = split_cql_type_args(inner);
        if args.len() == 2 {
            return format!(
                "{prefix}MapType({},{})",
                render_marshal_type(args[0], ignore_freezing),
                render_marshal_type(args[1], ignore_freezing)
            );
        }
        // Malformed map type — fall through to BytesType
    }
    if let Some(inner) = strip_cql_wrapper(raw, "frozen") {
        // `frozen<T>` is `T.freeze()`: never multicell, so its subtypes are
        // stringified with `ignoreFreezing = true` (which also makes `freeze()`
        // idempotent — a nested `frozen<..>` collapses).
        let rendered = render_marshal_type(inner, true);
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
            return rendered;
        }
        return format!("{prefix}FrozenType({rendered})");
    }
    if let Some(inner) = strip_cql_wrapper(raw, "tuple") {
        // `TupleType.toString()` has no `includeFrozenType` branch and passes
        // `true` for its components (`TupleType.java:557-560`).
        let args = split_cql_type_args(inner);
        let components: Vec<String> = args.iter().map(|a| render_marshal_type(a, true)).collect();
        return format!("{prefix}TupleType({})", components.join(","));
    }

    // Primitive types. CQL type names are case-insensitive, and the parser may
    // preserve original case from CQL files (e.g. "SET<TEXT>"), so match on a
    // lowercased copy.
    match raw.to_lowercase().as_str() {
        "text" | "varchar" => format!("{prefix}UTF8Type"),
        "int" => format!("{prefix}Int32Type"),
        "bigint" => format!("{prefix}LongType"),
        "smallint" => format!("{prefix}ShortType"),
        "tinyint" => format!("{prefix}ByteType"),
        "float" => format!("{prefix}FloatType"),
        "double" => format!("{prefix}DoubleType"),
        "boolean" => format!("{prefix}BooleanType"),
        "blob" => format!("{prefix}BytesType"),
        "uuid" => format!("{prefix}UUIDType"),
        "timeuuid" => format!("{prefix}TimeUUIDType"),
        "timestamp" => format!("{prefix}TimestampType"),
        "date" => format!("{prefix}SimpleDateType"),
        "time" => format!("{prefix}TimeType"),
        "duration" => format!("{prefix}DurationType"),
        "inet" => format!("{prefix}InetAddressType"),
        "ascii" => format!("{prefix}AsciiType"),
        "decimal" => format!("{prefix}DecimalType"),
        "varint" => format!("{prefix}IntegerType"),
        "counter" => format!("{prefix}CounterColumnType"),
        // Fallback: use BytesType for unknown types
        _ => format!("{prefix}BytesType"),
    }
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

    #[test]
    fn test_cql_type_to_marshal_type() {
        assert_eq!(
            cql_type_to_marshal_type("text"),
            "org.apache.cassandra.db.marshal.UTF8Type"
        );
        assert_eq!(
            cql_type_to_marshal_type("int"),
            "org.apache.cassandra.db.marshal.Int32Type"
        );
        assert_eq!(
            cql_type_to_marshal_type("bigint"),
            "org.apache.cassandra.db.marshal.LongType"
        );
        assert_eq!(
            cql_type_to_marshal_type("uuid"),
            "org.apache.cassandra.db.marshal.UUIDType"
        );
        assert_eq!(
            cql_type_to_marshal_type("blob"),
            "org.apache.cassandra.db.marshal.BytesType"
        );
        assert_eq!(
            cql_type_to_marshal_type("timestamp"),
            "org.apache.cassandra.db.marshal.TimestampType"
        );
        assert_eq!(
            cql_type_to_marshal_type("boolean"),
            "org.apache.cassandra.db.marshal.BooleanType"
        );
        assert_eq!(
            cql_type_to_marshal_type("varint"),
            "org.apache.cassandra.db.marshal.IntegerType"
        );
        // Unknown type falls back to BytesType
        assert_eq!(
            cql_type_to_marshal_type("unknown_type"),
            "org.apache.cassandra.db.marshal.BytesType"
        );

        // Already-marshaled strings pass through verbatim, case preserved (#929).
        // This is what a normalized bare-UDT column carries, and what columns
        // read back from an input SSTable's SerializationHeader look like.
        let user_type =
            "org.apache.cassandra.db.marshal.UserType(ks,706572736f6e,6e616d65:org.apache.cassandra.db.marshal.UTF8Type)";
        assert_eq!(cql_type_to_marshal_type(user_type), user_type);
        let int_type = "org.apache.cassandra.db.marshal.Int32Type";
        assert_eq!(cql_type_to_marshal_type(int_type), int_type);
        // Whitespace around a marshal string is trimmed but case is preserved.
        assert_eq!(
            cql_type_to_marshal_type("  org.apache.cassandra.db.marshal.UTF8Type  "),
            "org.apache.cassandra.db.marshal.UTF8Type"
        );

        // Collection types
        assert_eq!(
            cql_type_to_marshal_type("list<int>"),
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)"
        );
        assert_eq!(
            cql_type_to_marshal_type("set<text>"),
            "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)"
        );
        assert_eq!(
            cql_type_to_marshal_type("map<text, int>"),
            "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)"
        );

        // Frozen and nested
        assert_eq!(
            cql_type_to_marshal_type("frozen<list<int>>"),
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))"
        );

        // Tuple
        assert_eq!(
            cql_type_to_marshal_type("tuple<int, text>"),
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
            cql_type_to_marshal_type("frozen<person>"),
            format!("{Q}BytesType"),
            "frozen<unresolved-udt> must degrade to the same legal spelling as \
             the bare unresolved name, never FrozenType(<scalar>)"
        );
        // The bare-name fallback it must agree with.
        assert_eq!(
            cql_type_to_marshal_type("person"),
            cql_type_to_marshal_type("frozen<person>"),
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
                cql_type_to_marshal_type(cql),
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
        assert_eq!(cql_type_to_marshal_type("tuple<int, text>"), expected);
        assert_eq!(
            cql_type_to_marshal_type("frozen<tuple<int, text>>"),
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
            cql_type_to_marshal_type("tuple<int, frozen<list<int>>>"),
            format!("{Q}TupleType({Q}Int32Type,{Q}ListType({Q}Int32Type))"),
            "a tuple component is stringified with ignoreFreezing = true"
        );
        assert_eq!(
            cql_type_to_marshal_type("frozen<list<frozen<set<int>>>>"),
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
            cql_type_to_marshal_type("frozen<set<text>>"),
            format!("{Q}FrozenType({Q}SetType({Q}UTF8Type))")
        );
        assert_eq!(
            cql_type_to_marshal_type("frozen<list<int>>"),
            format!("{Q}FrozenType({Q}ListType({Q}Int32Type))")
        );
        assert_eq!(
            cql_type_to_marshal_type("frozen<map<text, text>>"),
            format!("{Q}FrozenType({Q}MapType({Q}UTF8Type,{Q}UTF8Type))")
        );
        assert_eq!(
            cql_type_to_marshal_type("set<text>"),
            format!("{Q}SetType({Q}UTF8Type)")
        );
        // A non-frozen (multicell) collection propagates ignoreFreezing = false,
        // so a FROZEN element KEEPS its wrapper. Corpus-attested by
        // collections_with_udts: `LIST<FROZEN<address_type>>` ->
        // `ListType(FrozenType(UserType(...)))`.
        assert_eq!(
            cql_type_to_marshal_type("list<frozen<set<int>>>"),
            format!("{Q}ListType({Q}FrozenType({Q}SetType({Q}Int32Type)))"),
            "a frozen element of a MULTICELL collection keeps its wrapper"
        );
        // An already-marshaled UserType inner keeps the wrapper (the resolved
        // frozen-UDT column shape asserted against Cassandra bytes by
        // `frozen_udt_column_renders_frozentype_usertype_cassandra_parity`).
        let user_type = format!("{Q}UserType(ks,706572736f6e,6e616d65:{Q}UTF8Type)");
        assert_eq!(
            cql_type_to_marshal_type(&format!("frozen<{user_type}>")),
            format!("{Q}FrozenType({user_type})")
        );
    }
}
