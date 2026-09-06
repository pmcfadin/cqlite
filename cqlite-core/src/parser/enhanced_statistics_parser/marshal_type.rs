//! Cassandra marshal-type string handling and `ColumnInfo` construction.
//!
//! These helpers convert Cassandra internal `org.apache.cassandra.db.marshal.*`
//! comparator/type strings (as they appear in the SerializationHeader of
//! Statistics.db) into CQL type names, and build the partition/clustering
//! `ColumnInfo` vectors used by schema discovery.

use crate::schema::cql_type_parser::frozen_scalar::validate_marshal_frozen;

/// Extract inner type from parameterized type string with proper parenthesis matching
///
/// Given a string that starts AFTER the opening parenthesis of a wrapper type,
/// returns the content up to (but not including) the matching closing parenthesis.
///
/// Example: For input "ListType(Int32Type))" (after stripping "FrozenType("),
/// returns Some("ListType(Int32Type)") - the content before the MATCHING close paren.
fn extract_inner_type(type_with_close_paren: &str) -> Option<&str> {
    let mut depth = 1; // We're already inside one opening paren (the wrapper type)
    for (idx, ch) in type_with_close_paren.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    // Return None if extracted string is empty (malformed input like ")")
                    if idx == 0 {
                        return None;
                    }
                    return Some(&type_with_close_paren[..idx]);
                }
            }
            _ => {}
        }
    }
    None // Unmatched parentheses
}

/// Split a type argument list on top-level commas, ignoring nested parentheses
fn split_type_arguments(input: &str) -> Vec<&str> {
    let mut args = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (idx, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                if depth > 0 {
                    depth -= 1;
                } else {
                    tracing::warn!(
                        "Unmatched closing parenthesis at position {} in type arguments: '{}'",
                        idx,
                        input
                    );
                }
            }
            ',' if depth == 0 => {
                let part = input[start..idx].trim();
                if !part.is_empty() {
                    args.push(part);
                }
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }

    let tail = input[start..].trim();
    if !tail.is_empty() {
        args.push(tail);
    }

    args
}

/// Detect whether a clustering comparator class denotes descending order.
///
/// Issue #759: Cassandra encodes a DESC clustering column by wrapping its
/// comparator class in `org.apache.cassandra.db.marshal.ReversedType(...)`.
/// This is the documented, authoritative signal for descending order
/// (definitive guide Ch.7 / Appendix B) — no heuristics. We mirror the
/// leading-paren stripping that `convert_marshal_type_to_cql` performs (the
/// header sometimes prefixes types with `(`), then check for the `ReversedType(`
/// prefix with or without the marshal package namespace.
pub(super) fn is_reversed_comparator(marshal_type: &str) -> bool {
    let mut value = marshal_type.trim();
    // Strip the optional leading wrapping paren(s) the header adds to the
    // top-level type (matches `convert_marshal_type_to_cql`'s normalization).
    while value.starts_with('(') && value.ends_with(')') && value.len() > 2 {
        value = value[1..value.len() - 1].trim();
    }
    // Some accepted header encodings prefix the comparator list with a `[`
    // (e.g. `[org.apache.cassandra.db.marshal.ReversedType(...)`); strip a
    // leading bracket so the ReversedType detection matches that form too
    // (roborev job 43).
    let value = value.trim_start_matches('[').trim_start();
    value.starts_with("org.apache.cassandra.db.marshal.ReversedType(")
        || value.starts_with("ReversedType(")
}

/// ═══ GATE 2 OF 2: the `Statistics.db` SerializationHeader type parser ═══
///
/// [`convert_marshal_type_to_cql`] with the `frozen<scalar>` refusal applied, and
/// THE ONLY WAY OUT OF THIS MODULE for a header type string. The converter itself
/// is private and infallible so that it can recurse; the gate sits here, on the
/// whole string, exactly once — which also means it descends into a
/// `UserType(…)`-bearing string that the converter returns verbatim.
///
/// A `FrozenType(<scalar>)` header is not something a Cassandra writer can emit:
/// the header records `column.type`, and `CQL3Type.Raw::freeze()` throws for every
/// non-collection/tuple/UDT, so no such column can have been declared. Refusing it
/// fail-closed is the no-heuristics answer (#28) — see [`validate_marshal_frozen`]
/// for the citation, the override set, and the corpus census. Issue #4104.
pub(super) fn convert_marshal_type_to_cql_checked(
    marshal_type: &str,
) -> crate::error::Result<String> {
    validate_marshal_frozen(marshal_type)?;
    Ok(convert_marshal_type_to_cql(marshal_type))
}

/// [`convert_marshal_type_to_cql_checked`] for the two callers whose error channel
/// is an `Option`: it LOGS the refusal (which carries the
/// `CQL3Type.java:647-651` citation) and answers `None`.
///
/// The marker-search SerializationHeader parsers in `serialization_header::mod`
/// have no per-column error type to carry a message, and both already treat "this
/// offset does not hold a readable header" as a single outcome. Giving them a
/// one-expression form keeps the gate at the SAME site as the UTF-8 check instead
/// of adding a second failure ladder beside it. Issue #4104.
pub(super) fn convert_marshal_type_to_cql_logged(marshal_type: &str) -> Option<String> {
    match convert_marshal_type_to_cql_checked(marshal_type) {
        Ok(t) => Some(t),
        Err(e) => {
            tracing::error!("Refusing SerializationHeader type '{marshal_type}': {e}");
            None
        }
    }
}

/// Convert Cassandra internal marshal type to CQL type name.
///
/// PRIVATE and infallible by design: it recurses, so gating here would re-scan the
/// string at every level. Every caller outside this module goes through
/// [`convert_marshal_type_to_cql_checked`], which is what makes gate 2 unmissable.
fn convert_marshal_type_to_cql(marshal_type: &str) -> String {
    fn strip_wrapping_parens(mut value: &str) -> &str {
        loop {
            // Also strip a leading structural `[` the header may prefix the
            // comparator list with (e.g. `[org...ReversedType(...)`), so the
            // wrapper-prefix checks below match the bracketed form too and the
            // inner CQL type is derived correctly (roborev job 48). This mirrors
            // the same normalization in `is_reversed_comparator`.
            let trimmed = value.trim().trim_start_matches('[').trim_start();
            if trimmed.starts_with('(') && trimmed.ends_with(')') && trimmed.len() > 2 {
                value = &trimmed[1..trimmed.len() - 1];
            } else {
                return trimmed;
            }
        }
    }

    fn strip_namespace(type_name: &str) -> &str {
        type_name.rsplit('.').next().unwrap_or(type_name)
    }

    fn strip_type_suffix(name: &str) -> &str {
        name.trim_end_matches("Type")
    }

    let mut cleaned = strip_wrapping_parens(marshal_type);

    // Special case: Preserve UserType definitions unchanged
    // UserType contains critical metadata (keyspace, type name, field definitions) that must
    // reach the parser intact. Converting it to a simplified CQL type would lose this information.
    if cleaned.contains("org.apache.cassandra.db.marshal.UserType(") {
        return marshal_type.to_string();
    }

    // Normalize known wrappers by recursively converting inner types
    // Use extract_inner_type() for proper parenthesis matching (fixes nested types)
    for prefix in [
        "org.apache.cassandra.db.marshal.ReversedType(",
        "ReversedType(",
    ] {
        if let Some(params_with_close) = cleaned.strip_prefix(prefix) {
            if let Some(inner) = extract_inner_type(params_with_close) {
                return convert_marshal_type_to_cql(inner);
            }
        }
    }

    for prefix in ["org.apache.cassandra.db.marshal.FrozenType(", "FrozenType("] {
        if let Some(params_with_close) = cleaned.strip_prefix(prefix) {
            if let Some(inner) = extract_inner_type(params_with_close) {
                return format!("frozen<{}>", convert_marshal_type_to_cql(inner));
            }
        }
    }

    for prefix in ["org.apache.cassandra.db.marshal.ListType(", "ListType("] {
        if let Some(params_with_close) = cleaned.strip_prefix(prefix) {
            if let Some(inner) = extract_inner_type(params_with_close) {
                return format!("list<{}>", convert_marshal_type_to_cql(inner));
            }
        }
    }

    for prefix in ["org.apache.cassandra.db.marshal.SetType(", "SetType("] {
        if let Some(params_with_close) = cleaned.strip_prefix(prefix) {
            if let Some(inner) = extract_inner_type(params_with_close) {
                return format!("set<{}>", convert_marshal_type_to_cql(inner));
            }
        }
    }

    for prefix in ["org.apache.cassandra.db.marshal.MapType(", "MapType("] {
        if let Some(params_with_close) = cleaned.strip_prefix(prefix) {
            if let Some(inner) = extract_inner_type(params_with_close) {
                let args = split_type_arguments(inner);
                if args.len() == 2 {
                    let key = convert_marshal_type_to_cql(args[0]);
                    let value = convert_marshal_type_to_cql(args[1]);
                    return format!("map<{}, {}>", key, value);
                } else if args.len() == 1 {
                    let value = convert_marshal_type_to_cql(args[0]);
                    return format!("map<text, {}>", value);
                }
            }
        }
    }

    cleaned = strip_wrapping_parens(cleaned);
    let base = strip_type_suffix(strip_namespace(cleaned)).trim_end_matches(')');

    // Map common types to CQL equivalents
    match base {
        "UTF8" => "text".to_string(),
        "Int32" => "int".to_string(),
        "Integer" => "int".to_string(),
        "Long" => "bigint".to_string(),
        "Short" => "smallint".to_string(),
        "Byte" => "tinyint".to_string(),
        "SimpleDate" => "date".to_string(),
        "Timestamp" => "timestamp".to_string(),
        "Boolean" => "boolean".to_string(),
        "Decimal" => "decimal".to_string(),
        "Float" => "float".to_string(),
        "Double" => "double".to_string(),
        "Bytes" => "blob".to_string(),
        "Ascii" => "ascii".to_string(),
        "InetAddress" => "inet".to_string(),
        "UUID" => "uuid".to_string(),
        "TimeUUID" => "timeuuid".to_string(),
        "Duration" => "duration".to_string(),
        "Time" => "time".to_string(),
        "Counter" | "CounterColumn" => "counter".to_string(),
        other => other.to_lowercase(),
    }
}

/// Construct ColumnInfo entries for partition key definitions found in SerializationHeader
pub(super) fn build_partition_key_columns(
    partition_types: &[String],
) -> crate::error::Result<Vec<super::super::header::ColumnInfo>> {
    if partition_types.is_empty() {
        return Ok(Vec::new());
    }

    let total = partition_types.len();
    partition_types
        .iter()
        .enumerate()
        .map(|(idx, marshal_type)| {
            let cql_type = convert_marshal_type_to_cql_checked(marshal_type)?;
            let name = if total == 1 {
                match cql_type.as_str() {
                    "uuid" | "timeuuid" => "id".to_string(),
                    _ => "partition_key".to_string(),
                }
            } else {
                format!("partition_key_{}", idx)
            };

            Ok(super::super::header::ColumnInfo {
                name,
                column_type: cql_type,
                is_primary_key: true,
                key_position: Some(idx as u16),
                is_static: false,
                is_clustering: false,
                clustering_reversed: false,
            })
        })
        .collect()
}

/// Construct ColumnInfo entries for clustering key definitions found in SerializationHeader
pub(super) fn build_clustering_key_columns(
    clustering_types: &[String],
) -> crate::error::Result<Vec<super::super::header::ColumnInfo>> {
    if clustering_types.is_empty() {
        return Ok(Vec::new());
    }

    let total = clustering_types.len();
    clustering_types
        .iter()
        .enumerate()
        .map(|(idx, marshal_type)| {
            // Issue #759: a DESC clustering column is encoded by wrapping its
            // comparator class in `ReversedType(...)`. Detect that authoritative
            // signal here, BEFORE `convert_marshal_type_to_cql` unwraps it, so
            // schema discovery can report `ClusteringOrder::Desc`. The CQL type
            // continues to come from `convert_marshal_type_to_cql`, which strips
            // `ReversedType` so the inner type's deserialization is undisturbed.
            let clustering_reversed = is_reversed_comparator(marshal_type);
            let cql_type = convert_marshal_type_to_cql_checked(marshal_type)?;
            let name = if total == 1 {
                "clustering_key".to_string()
            } else {
                format!("clustering_key_{}", idx)
            };

            Ok(super::super::header::ColumnInfo {
                name,
                column_type: cql_type,
                is_primary_key: true,
                key_position: Some(idx as u16),
                is_static: false,
                is_clustering: true,
                clustering_reversed,
            })
        })
        .collect()
}

/// Build ColumnInfo vectors from parsed type strings.
pub(super) fn build_column_infos(
    partition_types: &[String],
    clustering_types: &[String],
) -> crate::error::Result<(
    Vec<super::super::header::ColumnInfo>,
    Vec<super::super::header::ColumnInfo>,
)> {
    let partition_key_columns = build_partition_key_columns(partition_types)?;
    let clustering_key_columns = build_clustering_key_columns(clustering_types)?;

    tracing::debug!(
        "Constructed ColumnInfo entries from SerializationHeader: {} partition keys, {} clustering keys",
        partition_key_columns.len(),
        clustering_key_columns.len()
    );

    Ok((partition_key_columns, clustering_key_columns))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Issue #759: ReversedType wrapping is the authoritative DESC signal.
    #[test]
    fn test_is_reversed_comparator() {
        // Fully-qualified and short forms both denote DESC.
        assert!(is_reversed_comparator(
            "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)"
        ));
        assert!(is_reversed_comparator("ReversedType(Int32Type)"));
        // The header sometimes prefixes the top-level type with '(' — still DESC.
        assert!(is_reversed_comparator(
            "(org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.SimpleDateType))"
        ));
        // ...and sometimes with a structural '[' (roborev job 43) — still DESC.
        assert!(is_reversed_comparator(
            "[org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)"
        ));
        assert!(is_reversed_comparator("[ReversedType(Int32Type)"));
        // Non-reversed comparators are ASC.
        assert!(!is_reversed_comparator(
            "org.apache.cassandra.db.marshal.UTF8Type"
        ));
        assert!(!is_reversed_comparator("Int32Type"));
        // ReversedType only counts as the outer wrapper, not nested as an arg.
        assert!(!is_reversed_comparator(
            "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type))"
        ));
    }

    // Issue #759: ReversedType drives ClusteringColumn order without disturbing
    // the inner type's CQL conversion (used for deserialization).
    #[test]
    fn test_build_clustering_key_columns_reversed_order() {
        let clustering_types = vec![
            "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)".to_string(),
            "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)".to_string(),
        ];

        let cols = build_clustering_key_columns(&clustering_types)
            .expect("every type here is one Cassandra writes");
        assert_eq!(cols.len(), 3);

        // DESC columns flag clustering_reversed; inner CQL type is preserved.
        assert!(cols[0].clustering_reversed);
        assert_eq!(cols[0].column_type, "timestamp");
        assert!(!cols[1].clustering_reversed);
        assert_eq!(cols[1].column_type, "text");
        assert!(cols[2].clustering_reversed);
        assert_eq!(cols[2].column_type, "int");

        // Never leak the ReversedType wrapper into the resolved CQL type.
        for col in &cols {
            assert!(!col.column_type.contains("Reversed"));
            assert!(col.is_clustering);
        }
    }

    /// Regression (roborev job 48): a bracket-prefixed reversed comparator must
    /// be DESC *and* resolve to the correct inner CQL type (not `timestamptype`).
    #[test]
    fn test_build_clustering_key_columns_bracket_prefixed_reversed() {
        let clustering_types = vec![
            "[org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimestampType)"
                .to_string(),
        ];
        let cols = build_clustering_key_columns(&clustering_types)
            .expect("every type here is one Cassandra writes");
        assert_eq!(cols.len(), 1);
        assert!(
            cols[0].clustering_reversed,
            "bracket-prefixed ReversedType is DESC"
        );
        assert_eq!(
            cols[0].column_type, "timestamp",
            "inner CQL type must resolve through the bracket, got {}",
            cols[0].column_type
        );
    }

    #[test]
    fn test_marshal_type_conversion() {
        // Simple types should be converted to CQL names
        assert_eq!(
            convert_marshal_type_to_cql("org.apache.cassandra.db.marshal.Int32Type"),
            "int"
        );
        assert_eq!(
            convert_marshal_type_to_cql("org.apache.cassandra.db.marshal.UTF8Type"),
            "text"
        );
        assert_eq!(
            convert_marshal_type_to_cql("org.apache.cassandra.db.marshal.UUIDType"),
            "uuid"
        );
        assert_eq!(
            convert_marshal_type_to_cql("org.apache.cassandra.db.marshal.TimestampType"),
            "timestamp"
        );
        assert_eq!(
            convert_marshal_type_to_cql("org.apache.cassandra.db.marshal.DecimalType"),
            "decimal"
        );
        assert_eq!(
            convert_marshal_type_to_cql("org.apache.cassandra.db.marshal.SimpleDataType"),
            "simpledata"
        );

        // UserType should be preserved unchanged (contains critical metadata)
        let udt = "org.apache.cassandra.db.marshal.UserType(test_collections,616464726573735f74797065,737472656574:org.apache.cassandra.db.marshal.UTF8Type,63697479:org.apache.cassandra.db.marshal.UTF8Type)";
        assert_eq!(
            convert_marshal_type_to_cql(udt),
            udt,
            "UserType definitions must be preserved to retain keyspace, type name, and field metadata"
        );

        // Frozen UserType should also be preserved
        let frozen_udt = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(test_collections,616464726573735f74797065,737472656574:org.apache.cassandra.db.marshal.UTF8Type))";
        assert!(
            convert_marshal_type_to_cql(frozen_udt).contains("UserType("),
            "UserType inside FrozenType should be preserved"
        );

        // List of frozen UDT should preserve the UserType
        let list_udt = "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.UserType(test_collections,616464726573735f74797065,737472656574:org.apache.cassandra.db.marshal.UTF8Type)))";
        assert!(
            convert_marshal_type_to_cql(list_udt).contains("UserType("),
            "UserType inside List should be preserved"
        );
    }
}
