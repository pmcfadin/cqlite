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
pub(crate) fn cql_type_to_marshal_type(cql_type: &str) -> String {
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

    // Normalize to lowercase for case-insensitive matching.
    // CQL type names are case-insensitive, and the parser may preserve
    // original case from CQL files (e.g., "SET<TEXT>" instead of "set<text>").
    let trimmed = cql_type.trim().to_lowercase();
    let trimmed = trimmed.as_str();
    let prefix = "org.apache.cassandra.db.marshal.";

    // Handle parameterized types: list<T>, set<T>, map<K,V>, frozen<T>, tuple<T1,T2>
    if let Some(inner) = strip_cql_wrapper(trimmed, "list") {
        return format!("{prefix}ListType({})", cql_type_to_marshal_type(inner));
    }
    if let Some(inner) = strip_cql_wrapper(trimmed, "set") {
        return format!("{prefix}SetType({})", cql_type_to_marshal_type(inner));
    }
    if let Some(inner) = strip_cql_wrapper(trimmed, "map") {
        let args = split_cql_type_args(inner);
        if args.len() == 2 {
            return format!(
                "{prefix}MapType({},{})",
                cql_type_to_marshal_type(args[0]),
                cql_type_to_marshal_type(args[1])
            );
        }
        // Malformed map type — fall through to BytesType
    }
    if let Some(inner) = strip_cql_wrapper(trimmed, "frozen") {
        return format!("{prefix}FrozenType({})", cql_type_to_marshal_type(inner));
    }
    if let Some(inner) = strip_cql_wrapper(trimmed, "tuple") {
        let args = split_cql_type_args(inner);
        let components: Vec<String> = args.iter().map(|a| cql_type_to_marshal_type(a)).collect();
        return format!("{prefix}TupleType({})", components.join(","));
    }

    // Primitive types
    match trimmed {
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
fn strip_cql_wrapper<'a>(cql_type: &'a str, wrapper: &str) -> Option<&'a str> {
    let pattern = format!("{}<", wrapper);
    if let Some(rest) = cql_type.strip_prefix(&pattern) {
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
