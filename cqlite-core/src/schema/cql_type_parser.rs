//! CQL type-string parsing for [`CqlType`].
//!
//! Houses [`CqlType::parse`] and its `split_top_level_types` helper, along with
//! the small inherent accessors (`fixed_size`, `is_collection`). Extracted from
//! `schema/mod.rs` (issue #1134, source-split doctrine) with no behavior change.

use super::frozen_scalar::{frozen_inner_supports_freezing, refuse_frozen_scalar_cql};
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

        // ═══ GATE 1 OF 2: `frozen<>` IS NOT DECLARABLE OVER A SCALAR ═══
        //
        // `CQL3Type.Raw::freeze()` is the base implementation and does nothing but
        // throw ("frozen<> is only allowed on collections, tuples, and user-defined
        // types"), and the grammar routes every `frozen<…>` through it — so a
        // `frozen<scalar>` column, map key, element or UDT field cannot exist and
        // there are no Cassandra-written bytes for one. The rule, its citation and
        // the corpus census live in ONE place (`schema::frozen_scalar`), shared with
        // the SerializationHeader gate; issue #4104.
        //
        // Sited HERE, at the metadata entry point, and never in a decoder: the
        // refusal is upstream of decode by design.
        if let Some(inner) = strip_prefix_ci(type_str, "frozen<") {
            if let Some(inner) = inner.strip_suffix('>') {
                let parsed = Self::parse_with_depth(inner, depth + 1)?;
                if !frozen_inner_supports_freezing(&parsed) {
                    return Err(refuse_frozen_scalar_cql(type_str, inner.trim()));
                }
                return Ok(CqlType::Frozen(Box::new(parsed)));
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
}
