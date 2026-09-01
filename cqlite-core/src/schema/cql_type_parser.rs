//! CQL type-string parsing for [`CqlType`].
//!
//! Houses [`CqlType::parse`] and its `split_top_level_types` helper, along with
//! the small inherent accessors (`fixed_size`, `is_collection`). Extracted from
//! `schema/mod.rs` (issue #1134, source-split doctrine) with no behavior change.

use super::CqlType;
use crate::error::{Error, Result};

/// Maximum allowed CQL type nesting depth. Mirrors
/// [`crate::parser::complex_types::ComplexTypeParser`] (`max_depth = 32`).
///
/// Without this bound, a hostile or malformed schema with pathological nesting
/// (e.g. `frozen<` × 50_000) recurses until the thread stack overflows and,
/// under `panic = "abort"`, aborts the whole process instead of returning an
/// error (issue #1690). The guard is `depth > MAX_NESTING_DEPTH`, where `depth`
/// is 0 for the outermost type and increments once per nesting level. A leaf
/// reached at exactly depth 32 (i.e. 32 levels of collection/frozen nesting) is
/// therefore the last allowed depth; a 33rd level returns `Err`.
const MAX_NESTING_DEPTH: usize = 32;

impl CqlType {
    fn split_top_level_types(type_str: &str) -> Result<Vec<&str>> {
        let mut parts = Vec::new();
        let mut depth = 0usize;
        let mut start = 0usize;

        for (index, ch) in type_str.char_indices() {
            match ch {
                '<' => depth += 1,
                '>' => {
                    if depth == 0 {
                        return Err(Error::schema(format!(
                            "Invalid nested type syntax: {}",
                            type_str
                        )));
                    }
                    depth -= 1;
                }
                ',' if depth == 0 => {
                    parts.push(type_str[start..index].trim());
                    start = index + ch.len_utf8();
                }
                _ => {}
            }
        }

        if depth != 0 {
            return Err(Error::schema(format!(
                "Unbalanced nested type syntax: {}",
                type_str
            )));
        }

        parts.push(type_str[start..].trim());
        Ok(parts.into_iter().filter(|part| !part.is_empty()).collect())
    }

    /// Parse CQL type string into structured type
    pub fn parse(type_str: &str) -> Result<Self> {
        #[cfg(test)]
        crate::schema::work_counters::record_parse_call();
        Self::parse_with_depth(type_str, 0)
    }

    /// Recursive body of [`CqlType::parse`], threading the current nesting
    /// `depth` so recursion is bounded at [`MAX_NESTING_DEPTH`] (issue #1690).
    /// `depth` is 0 at the top level and increments by one for each nested type.
    fn parse_with_depth(type_str: &str, depth: usize) -> Result<Self> {
        if depth > MAX_NESTING_DEPTH {
            return Err(Error::schema(format!(
                "type nesting too deep (max {})",
                MAX_NESTING_DEPTH
            )));
        }

        let type_str = type_str.trim();

        // CQL type keywords are case-insensitive (`SET<TEXT>` == `set<text>`),
        // so match collection/frozen/tuple prefixes case-insensitively. Matching
        // only lowercase here previously left uppercase collections to fall
        // through to a bare `Custom("SET<TEXT>")`, which both broke type-aware
        // handling and confused UDT-reference validation (roborev job 51).
        fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
            s.get(..prefix.len())
                .filter(|head| head.eq_ignore_ascii_case(prefix))
                .map(|_| &s[prefix.len()..])
        }

        // Handle frozen types
        if let Some(inner) = strip_prefix_ci(type_str, "frozen<") {
            if let Some(inner) = inner.strip_suffix('>') {
                return Ok(CqlType::Frozen(Box::new(Self::parse_with_depth(
                    inner,
                    depth + 1,
                )?)));
            }
        }

        // Handle collection types
        if let Some(inner) = strip_prefix_ci(type_str, "list<") {
            if let Some(inner) = inner.strip_suffix('>') {
                return Ok(CqlType::List(Box::new(Self::parse_with_depth(
                    inner,
                    depth + 1,
                )?)));
            }
        }

        if let Some(inner) = strip_prefix_ci(type_str, "set<") {
            if let Some(inner) = inner.strip_suffix('>') {
                return Ok(CqlType::Set(Box::new(Self::parse_with_depth(
                    inner,
                    depth + 1,
                )?)));
            }
        }

        if let Some(inner) = strip_prefix_ci(type_str, "map<") {
            if let Some(inner) = inner.strip_suffix('>') {
                let parts = Self::split_top_level_types(inner)?;
                if parts.len() != 2 {
                    return Err(Error::schema(format!("Invalid map type: {}", type_str)));
                }
                return Ok(CqlType::Map(
                    Box::new(Self::parse_with_depth(parts[0].trim(), depth + 1)?),
                    Box::new(Self::parse_with_depth(parts[1].trim(), depth + 1)?),
                ));
            }
        }

        // Handle tuple types
        if let Some(inner) = strip_prefix_ci(type_str, "tuple<") {
            if let Some(inner) = inner.strip_suffix('>') {
                let parts = Self::split_top_level_types(inner)?;
                let mut types = Vec::new();
                for part in parts {
                    types.push(Self::parse_with_depth(part.trim(), depth + 1)?);
                }
                return Ok(CqlType::Tuple(types));
            }
        }

        // Handle UDT types - format: udt_name or keyspace.udt_name
        // But first check if it's not a primitive type in uppercase
        let lowercase_type = type_str.to_lowercase();
        let is_primitive = matches!(
            lowercase_type.as_str(),
            "boolean"
                | "bool"
                | "tinyint"
                | "smallint"
                | "int"
                | "integer"
                | "bigint"
                | "long"
                | "counter"
                | "float"
                | "double"
                | "decimal"
                | "text"
                | "varchar"
                | "ascii"
                | "blob"
                | "timestamp"
                | "date"
                | "time"
                | "uuid"
                | "timeuuid"
                | "inet"
                | "duration"
                | "varint"
        );

        if !is_primitive
            && type_str
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '.')
            && !type_str.chars().all(|c| c.is_ascii_lowercase())
        {
            // This might be a UDT name - store as custom type for now
            // Full validation requires UDT registry context
            return Ok(CqlType::Custom(format!("udt:{}", type_str)));
        }

        // Primitive types
        match type_str.to_lowercase().as_str() {
            "boolean" | "bool" => Ok(CqlType::Boolean),
            "tinyint" => Ok(CqlType::TinyInt),
            "smallint" => Ok(CqlType::SmallInt),
            "int" | "integer" => Ok(CqlType::Int),
            "bigint" | "long" => Ok(CqlType::BigInt),
            "counter" => Ok(CqlType::Counter),
            "float" => Ok(CqlType::Float),
            "double" => Ok(CqlType::Double),
            "decimal" => Ok(CqlType::Decimal),
            "text" | "varchar" => Ok(CqlType::Text),
            "ascii" => Ok(CqlType::Ascii),
            "blob" => Ok(CqlType::Blob),
            "timestamp" => Ok(CqlType::Timestamp),
            "date" => Ok(CqlType::Date),
            "time" => Ok(CqlType::Time),
            "uuid" => Ok(CqlType::Uuid),
            "timeuuid" => Ok(CqlType::TimeUuid),
            "inet" => Ok(CqlType::Inet),
            "duration" => Ok(CqlType::Duration),
            "varint" => Ok(CqlType::Varint),
            _ => Ok(CqlType::Custom(type_str.to_string())),
        }
    }

    /// Get the expected byte size for fixed-size types
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            CqlType::Boolean => Some(1),
            CqlType::TinyInt => Some(1),
            CqlType::SmallInt => Some(2),
            CqlType::Int => Some(4),
            CqlType::BigInt => Some(8),
            CqlType::Counter => Some(8),
            CqlType::Float => Some(4),
            CqlType::Double => Some(8),
            CqlType::Timestamp => Some(8),
            CqlType::Date => Some(4),
            CqlType::Time => Some(8),
            CqlType::Uuid | CqlType::TimeUuid => Some(16),
            CqlType::Inet => Some(16), // IPv6, IPv4 is variable
            // Variable size types
            CqlType::Text
            | CqlType::Ascii
            | CqlType::Varchar
            | CqlType::Blob
            | CqlType::Decimal
            | CqlType::Duration
            | CqlType::Varint => None,
            // Collections and complex types are variable
            CqlType::List(_)
            | CqlType::Set(_)
            | CqlType::Map(_, _)
            | CqlType::Tuple(_)
            | CqlType::Udt(_, _) => None,
            CqlType::Frozen(inner) => inner.fixed_size(),
            CqlType::Custom(_) => None,
        }
    }

    /// Check if this type is a collection
    pub fn is_collection(&self) -> bool {
        matches!(
            self,
            CqlType::List(_) | CqlType::Set(_) | CqlType::Map(_, _)
        )
    }

    /// Render this type as its canonical CQL short form — the INVERSE of
    /// [`CqlType::parse`], which is why it lives in this module.
    ///
    /// # Why this exists (issue #3722)
    ///
    /// The consolidated UDT-field decoder
    /// (`storage::sstable::reader::parsing::row_decoder::udt_field`) has to hand
    /// COLLECTION ELEMENT types to payload parsers that take a type `&str`
    /// (`parse_frozen_list_value_raw`, `parse_frozen_map_value_raw`,
    /// `parse_tuple_elements_raw`), and `CqlType` has no `Display`/`to_string`.
    ///
    /// # DRIFT NOTICE — this is the FOURTH such renderer in the tree
    ///
    /// Three private, near-duplicate copies already exist and are deliberately
    /// NOT refactored onto this one here (that is a separate change):
    ///
    /// * `parser::complex_types::ComplexTypeParser::format_cql_type` — **BUGGY**:
    ///   it has no `Varint` arm, so a `varint` falls through its `_` arm and
    ///   renders as `"text"`.
    /// * `schema::udt_registry::UdtRegistry::format_cql_type` (CREATE TYPE output)
    /// * `query::result::format_cql_type` (`ColumnInfo::to_json`'s `cql_type`)
    ///
    /// A follow-up issue tracks collapsing all four (and fixing the `Varint`
    /// bug); until then, a new short form must be added HERE and in those three.
    ///
    /// # Output conventions
    ///
    /// * Total over `CqlType` — no wildcard arm, pinned by
    ///   `#[deny(clippy::wildcard_enum_match_arm)]`, so a new variant is a
    ///   COMPILE error here rather than a silently wrong type string.
    /// * `map<k,v>` / `tuple<a,b>` carry NO space after the comma: the output is
    ///   consumed by the marshal/short-form element extractors
    ///   (`extract_map_types` / `extract_tuple_element_types`), which have not
    ///   been verified to tolerate one. The three renderers above emit
    ///   `map<{k}, {v}>` because their output is for humans.
    /// * `Udt(name, fields)` renders to the bare `name`: the inline field defs
    ///   CANNOT be carried in a type string, so a caller that can decode a UDT
    ///   structurally must do so and never route it through here. (A rendered
    ///   name is still resolvable when a `UdtRegistry` holds that type.)
    #[deny(clippy::wildcard_enum_match_arm)]
    pub(crate) fn to_cql_string(&self) -> String {
        match self {
            CqlType::Boolean => "boolean".to_string(),
            CqlType::TinyInt => "tinyint".to_string(),
            CqlType::SmallInt => "smallint".to_string(),
            CqlType::Int => "int".to_string(),
            CqlType::BigInt => "bigint".to_string(),
            CqlType::Counter => "counter".to_string(),
            CqlType::Float => "float".to_string(),
            CqlType::Double => "double".to_string(),
            CqlType::Decimal => "decimal".to_string(),
            CqlType::Text => "text".to_string(),
            CqlType::Ascii => "ascii".to_string(),
            CqlType::Varchar => "varchar".to_string(),
            CqlType::Blob => "blob".to_string(),
            CqlType::Timestamp => "timestamp".to_string(),
            CqlType::Date => "date".to_string(),
            CqlType::Time => "time".to_string(),
            CqlType::Uuid => "uuid".to_string(),
            CqlType::TimeUuid => "timeuuid".to_string(),
            CqlType::Inet => "inet".to_string(),
            CqlType::Duration => "duration".to_string(),
            CqlType::Varint => "varint".to_string(),
            CqlType::List(inner) => format!("list<{}>", inner.to_cql_string()),
            CqlType::Set(inner) => format!("set<{}>", inner.to_cql_string()),
            CqlType::Map(key, value) => {
                format!("map<{},{}>", key.to_cql_string(), value.to_cql_string())
            }
            CqlType::Tuple(elements) => {
                let rendered: Vec<String> = elements.iter().map(|e| e.to_cql_string()).collect();
                format!("tuple<{}>", rendered.join(","))
            }
            CqlType::Frozen(inner) => format!("frozen<{}>", inner.to_cql_string()),
            CqlType::Udt(name, _fields) => name.clone(),
            CqlType::Custom(name) => name.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cql_type_parsing() {
        assert_eq!(CqlType::parse("text").unwrap(), CqlType::Text);
        assert_eq!(CqlType::parse("bigint").unwrap(), CqlType::BigInt);

        match CqlType::parse("list<int>").unwrap() {
            CqlType::List(inner) => assert_eq!(*inner, CqlType::Int),
            _ => panic!("Expected List type"),
        }

        match CqlType::parse("map<text, bigint>").unwrap() {
            CqlType::Map(key, value) => {
                assert_eq!(*key, CqlType::Text);
                assert_eq!(*value, CqlType::BigInt);
            }
            _ => panic!("Expected Map type"),
        }

        match CqlType::parse("tuple<text, list<int>, map<text, text>>").unwrap() {
            CqlType::Tuple(fields) => {
                assert_eq!(fields.len(), 3);
                assert_eq!(fields[0], CqlType::Text);
                assert_eq!(fields[1], CqlType::List(Box::new(CqlType::Int)));
                assert_eq!(
                    fields[2],
                    CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Text))
                );
            }
            _ => panic!("Expected Tuple type"),
        }
    }

    /// Regression for #1160: CQL type names are case-insensitive in Cassandra,
    /// so mixed/upper-case primitives must resolve to their primitive variant
    /// rather than leaking through the UDT-detection branch as
    /// `Custom("udt:...")`. `varint` was previously missing from the
    /// `is_primitive` guard, so only its upper/mixed-case spellings regressed.
    #[test]
    fn test_uppercase_primitives_are_not_udts() {
        // The exact regression case from #1160.
        assert_eq!(CqlType::parse("VARINT").unwrap(), CqlType::Varint);
        assert_eq!(CqlType::parse("Varint").unwrap(), CqlType::Varint);
        assert_eq!(CqlType::parse("varint").unwrap(), CqlType::Varint);

        // Spot-check other spellings to lock the case-insensitive contract and
        // guard against future `is_primitive`/`match`-arm drift.
        assert_eq!(CqlType::parse("INT").unwrap(), CqlType::Int);
        assert_eq!(CqlType::parse("BigInt").unwrap(), CqlType::BigInt);
        assert_eq!(CqlType::parse("TEXT").unwrap(), CqlType::Text);
        assert_eq!(CqlType::parse("UUID").unwrap(), CqlType::Uuid);
        assert_eq!(CqlType::parse("Duration").unwrap(), CqlType::Duration);

        // A genuine UDT reference must still be treated as one.
        assert_eq!(
            CqlType::parse("MyType").unwrap(),
            CqlType::Custom("udt:MyType".to_string())
        );
    }

    /// Issue #1690 (P0 safety): a hostile/malformed schema with pathological
    /// nesting must NOT stack-overflow (which under `panic = "abort"` aborts the
    /// whole process). It must return `Err` instead. The recursion is bounded at
    /// [`MAX_NESTING_DEPTH`], so this errors long before the stack is exhausted.
    #[test]
    fn test_adversarial_deep_nesting_returns_err_not_abort() {
        let s = "frozen<".repeat(50_000) + "int" + &">".repeat(50_000);
        assert!(
            CqlType::parse(&s).is_err(),
            "pathological nesting must return Err, not abort"
        );
    }

    /// The depth bound is exact: a leaf reached at depth == [`MAX_NESTING_DEPTH`]
    /// (i.e. `MAX_NESTING_DEPTH` levels of `frozen<...>` around a leaf) is the
    /// last allowed depth and must still parse to the identical `CqlType` it did
    /// before the guard existed; one level deeper must error.
    #[test]
    fn test_nesting_depth_boundary_is_exact() {
        // 32 levels of frozen nesting: last allowed. Must parse and produce the
        // unchanged nested structure (Frozen x32 around Int).
        let depth = MAX_NESTING_DEPTH; // 32 — the last allowed nesting level.
        let ok_str = "frozen<".repeat(depth) + "int" + &">".repeat(depth);
        let mut parsed =
            CqlType::parse(&ok_str).expect("nesting at the depth bound must still parse");

        let mut frozen_levels = 0usize;
        while let CqlType::Frozen(inner) = parsed {
            parsed = *inner;
            frozen_levels += 1;
        }
        assert_eq!(frozen_levels, depth, "all frozen levels must be preserved");
        assert_eq!(parsed, CqlType::Int, "the leaf type must be unchanged");

        // 33 levels: one past the bound. Must error with a clear message.
        let bad_str = "frozen<".repeat(depth + 1) + "int" + &">".repeat(depth + 1);
        let err = CqlType::parse(&bad_str).expect_err("one level past the bound must error");
        let msg = err.to_string();
        assert!(
            msg.contains("nesting") || msg.contains("deep"),
            "error message must mention nesting/depth, got: {msg}"
        );
    }
    /// Issue #3722: the canonical renderer must round-trip through
    /// [`CqlType::parse`] for every short form and every composite shape, so the
    /// strings the UDT-field decoder hands to the element payload parsers are
    /// ones this codebase can parse back.
    #[test]
    fn to_cql_string_round_trips_through_parse() {
        let cases = vec![
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
            CqlType::Blob,
            CqlType::Timestamp,
            CqlType::Date,
            CqlType::Time,
            CqlType::Uuid,
            CqlType::TimeUuid,
            CqlType::Inet,
            CqlType::Duration,
            CqlType::Varint,
            CqlType::List(Box::new(CqlType::Int)),
            CqlType::Set(Box::new(CqlType::Text)),
            CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int)),
            CqlType::Tuple(vec![CqlType::Int, CqlType::Text]),
            CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Uuid)))),
        ];
        for case in cases {
            let rendered = case.to_cql_string();
            let reparsed = CqlType::parse(&rendered)
                .unwrap_or_else(|e| panic!("re-parsing {rendered:?} failed: {e}"));
            assert_eq!(reparsed, case, "round-trip mismatch via {rendered:?}");
        }
    }

    /// `Varchar` renders faithfully (and re-parses to `Text`, which is what
    /// `CqlType::parse` maps both spellings to). Pinned separately because it is
    /// the one variant whose round-trip is deliberately NOT an identity, and
    /// because the sibling renderer in `parser::complex_types` gets `Varint`
    /// wrong for exactly this fall-through reason.
    #[test]
    fn to_cql_string_varchar_and_varint_are_not_confused() {
        assert_eq!(CqlType::Varchar.to_cql_string(), "varchar");
        assert_eq!(CqlType::parse("varchar").unwrap(), CqlType::Text);
        assert_eq!(CqlType::Varint.to_cql_string(), "varint");
        assert_eq!(CqlType::parse("varint").unwrap(), CqlType::Varint);
    }

    /// Composite forms carry no space after the comma (see the doc comment on
    /// [`CqlType::to_cql_string`]): the element extractors consume this output.
    #[test]
    fn to_cql_string_composites_have_no_space_after_comma() {
        let map = CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int));
        assert_eq!(map.to_cql_string(), "map<text,int>");
        let tuple = CqlType::Tuple(vec![CqlType::Int, CqlType::Text, CqlType::Uuid]);
        assert_eq!(tuple.to_cql_string(), "tuple<int,text,uuid>");
    }

    /// A UDT renders to its bare name; the inline field defs cannot be carried in
    /// a type string, which is why the UDT-field decoder recurses structurally
    /// instead of rendering a `Udt` (issue #3722).
    #[test]
    fn to_cql_string_udt_renders_bare_name() {
        let udt = CqlType::Udt(
            "address_type".to_string(),
            vec![("street".to_string(), CqlType::Text)],
        );
        assert_eq!(udt.to_cql_string(), "address_type");
    }
}
