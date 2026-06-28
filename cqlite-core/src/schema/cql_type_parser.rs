//! CQL type-string parsing for [`CqlType`].
//!
//! Houses [`CqlType::parse`] and its `split_top_level_types` helper, along with
//! the small inherent accessors (`fixed_size`, `is_collection`). Extracted from
//! `schema/mod.rs` (issue #1134, source-split doctrine) with no behavior change.

use super::CqlType;
use crate::error::{Error, Result};

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
                return Ok(CqlType::Frozen(Box::new(Self::parse(inner)?)));
            }
        }

        // Handle collection types
        if let Some(inner) = strip_prefix_ci(type_str, "list<") {
            if let Some(inner) = inner.strip_suffix('>') {
                return Ok(CqlType::List(Box::new(Self::parse(inner)?)));
            }
        }

        if let Some(inner) = strip_prefix_ci(type_str, "set<") {
            if let Some(inner) = inner.strip_suffix('>') {
                return Ok(CqlType::Set(Box::new(Self::parse(inner)?)));
            }
        }

        if let Some(inner) = strip_prefix_ci(type_str, "map<") {
            if let Some(inner) = inner.strip_suffix('>') {
                let parts = Self::split_top_level_types(inner)?;
                if parts.len() != 2 {
                    return Err(Error::schema(format!("Invalid map type: {}", type_str)));
                }
                return Ok(CqlType::Map(
                    Box::new(Self::parse(parts[0].trim())?),
                    Box::new(Self::parse(parts[1].trim())?),
                ));
            }
        }

        // Handle tuple types
        if let Some(inner) = strip_prefix_ci(type_str, "tuple<") {
            if let Some(inner) = inner.strip_suffix('>') {
                let parts = Self::split_top_level_types(inner)?;
                let mut types = Vec::new();
                for part in parts {
                    types.push(Self::parse(part.trim())?);
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
}
