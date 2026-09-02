//! Parse a Cassandra **marshal TYPE STRING** into CQLite schema types (issue #3631).
//!
//! Split out of `udt.rs` under the campsite rule (epic #1116: that file is more than
//! 1.5x the 800-line source target) along a responsibility boundary that was already
//! there: everything here turns a `SerializationHeader` type STRING into a
//! [`UdtTypeDef`] / [`CqlType`], while `udt.rs` decodes UDT **value bytes**.
//!
//! # Format authority — the pinned tag, never CQLite's own tables (#3041)
//!
//! The marshal-name -> CQL-type mapping is
//! `cassandra-5.0.8:src/java/org/apache/cassandra/cql3/CQL3Type.java`'s `Native`
//! enum, which is the single place Cassandra binds a CQL native type to its
//! `AbstractType` implementation:
//!
//! | CQL native | marshal class     | CQL native | marshal class      |
//! |------------|-------------------|------------|--------------------|
//! | ascii      | `AsciiType`       | int        | `Int32Type`        |
//! | bigint     | `LongType`        | smallint   | `ShortType`        |
//! | blob       | `BytesType`       | text       | `UTF8Type`         |
//! | boolean    | `BooleanType`     | time       | `TimeType`         |
//! | counter    | `CounterColumnType` | timestamp | `TimestampType`   |
//! | date       | `SimpleDateType`  | timeuuid   | `TimeUUIDType`     |
//! | decimal    | `DecimalType`     | tinyint    | `ByteType`         |
//! | double     | `DoubleType`      | uuid       | `UUIDType`         |
//! | duration   | `DurationType`    | varchar    | `UTF8Type`         |
//! | float      | `FloatType`       | varint     | `IntegerType`      |
//! | inet       | `InetAddressType` |            |                    |
//!
//! Three names are NOT in that enum and are mapped anyway, each from its own
//! authority at the same tag:
//!
//! * `DateType` -> `timestamp`. `DateType.asCQL3Type()` returns
//!   `CQL3Type.Native.TIMESTAMP` — it is the LEGACY 8-byte millis type, NOT CQL
//!   `date`. It is listed here because a suffix match on `DateType` also matches
//!   `SimpleDateType` (the real `date`), which is how a `date` field could have been
//!   decoded as an 8-byte timestamp.
//! * `VarcharType` -> `text`. An alias `TypeParser` resolves to `UTF8Type`.
//! * `LexicalUUIDType` -> `uuid`. It has no `asCQL3Type` override, but
//!   `LexicalUUIDType.Serializer extends UUIDSerializer` and
//!   `valueLengthIfFixed() == 16`: the VALUE layout is a UUID's, and only the
//!   comparison order differs.
//!
//! `EmptyType`, `VectorType(...)`, `CompositeType(...)`, `DynamicCompositeType(...)`
//! and `PartitionerDefinedOrder` are deliberately NOT mapped — [`CqlType`] has no
//! variant that can express them, so they stay [`CqlType::Custom`] and the DECODER
//! refuses them by name (issue #3631 criterion 5) rather than guessing.

use super::super::*;

impl V5CompressedLegacyParser {
    /// Parse a UDT type string to extract the UDT definition.
    /// Cassandra encodes UDTs as:
    /// `UserType(keyspace,hex_name,field1_hex:type1,field2_hex:type2,...)`
    ///
    /// Example:
    /// ```text
    /// org.apache.cassandra.db.marshal.UserType(
    ///   test_collections,
    ///   616464726573735f74797065,    // hex("address_type")
    ///   737472656574:UTF8Type,        // street:UTF8Type
    ///   63697479:UTF8Type,            // city:UTF8Type
    ///   ...
    /// )
    /// ```
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn parse_udt_type_definition(
        type_str: &str,
    ) -> Result<UdtTypeDef> {
        Self::parse_udt_type_definition_with_depth(type_str, 0)
    }

    /// Internal implementation of parse_udt_type_definition with recursion depth tracking.
    pub(super) fn parse_udt_type_definition_with_depth(
        type_str: &str,
        depth: usize,
    ) -> Result<UdtTypeDef> {
        // Check recursion depth to prevent stack overflow
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::schema(format!(
                "UDT nesting depth {} exceeds maximum {}. Type string: {}",
                depth,
                MAX_TYPE_NESTING_DEPTH,
                type_str.chars().take(100).collect::<String>()
            )));
        }

        // Find the UserType(...) portion (case-insensitive). Match ONLY the
        // fully-qualified marshal marker — the single shape real SerializationHeaders
        // carry, and the same marker the nested-field decoder
        // (`parse_cassandra_type_with_depth`) keys on. Keeping top-level and nested
        // parsing on the same qualified marker avoids the partial-bare-support
        // inconsistency that would blob nested UDT fields (roborev jobs 1359/1361).
        let start_marker = "org.apache.cassandra.db.marshal.UserType(";
        let type_lower = type_str.to_lowercase();
        let start_marker_lower = start_marker.to_lowercase();
        let start_idx = type_lower
            .find(&start_marker_lower)
            .ok_or_else(|| Error::schema(format!("Not a UserType: {}", type_str)))?;

        // Find the matching close paren (handling nested types)
        let inner_start = start_idx + start_marker.len();
        let mut paren_depth = 1;
        let mut end_idx = inner_start;
        let chars: Vec<char> = type_str[inner_start..].chars().collect();

        for (i, c) in chars.iter().enumerate() {
            match c {
                '(' => paren_depth += 1,
                ')' => {
                    paren_depth -= 1;
                    if paren_depth == 0 {
                        end_idx = inner_start + i;
                        break;
                    }
                }
                _ => {}
            }
        }

        if paren_depth != 0 {
            return Err(Error::schema(format!(
                "Unbalanced parentheses in UserType: {}",
                type_str
            )));
        }

        let inner = &type_str[inner_start..end_idx];

        // Split by comma, but respect nested parentheses
        let parts = Self::split_type_args(inner)?;
        if parts.len() < 2 {
            return Err(Error::schema(format!(
                "UserType requires at least keyspace and name: {}",
                inner
            )));
        }

        // First part is keyspace
        let keyspace = parts[0].trim();
        if keyspace.is_empty() {
            return Err(Error::schema("UDT keyspace cannot be empty"));
        }
        let keyspace = keyspace.to_string();

        // Second part is hex-encoded type name
        let udt_name = Self::decode_hex_name(parts[1].trim())?;

        // Remaining parts are field definitions: hex_name:type
        let mut udt_def = UdtTypeDef::new(keyspace, udt_name);
        for field_def in parts.iter().skip(2) {
            let field_def = field_def.trim();
            if field_def.is_empty() {
                continue;
            }

            // Split on first colon (field name is before, type is after)
            if let Some(colon_idx) = field_def.find(':') {
                let field_name_hex = &field_def[..colon_idx];
                let field_type_str = &field_def[colon_idx + 1..];

                let field_name = Self::decode_hex_name(field_name_hex)?;
                // Use depth-aware version to track recursion through UDT fields
                let field_type = Self::parse_cassandra_type_with_depth(field_type_str, depth)?;

                udt_def = udt_def.with_field(field_name, field_type, true);
            } else {
                return Err(Error::schema(format!(
                    "Invalid UDT field definition (missing colon): {}",
                    field_def
                )));
            }
        }

        Ok(udt_def)
    }

    /// Split type arguments by comma, respecting nested parentheses.
    pub(super) fn split_type_args(s: &str) -> Result<Vec<String>> {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut depth = 0;

        for c in s.chars() {
            match c {
                '(' => {
                    depth += 1;
                    current.push(c);
                }
                ')' => {
                    depth -= 1;
                    current.push(c);
                }
                ',' if depth == 0 => {
                    parts.push(current.clone());
                    current.clear();
                }
                _ => current.push(c),
            }
        }

        if !current.is_empty() {
            parts.push(current);
        }

        Ok(parts)
    }

    /// Decode a hex-encoded name (e.g., "616464726573735f74797065" -> "address_type")
    pub(super) fn decode_hex_name(hex: &str) -> Result<String> {
        let bytes = hex::decode(hex)
            .map_err(|e| Error::schema(format!("Invalid hex-encoded UDT name '{}': {}", hex, e)))?;
        String::from_utf8(bytes)
            .map_err(|e| Error::schema(format!("Invalid UTF-8 in UDT name '{}': {}", hex, e)))
    }

    /// Parse a Cassandra type string into a CqlType.
    /// Handles: UTF8Type, Int32Type, ListType(...), SetType(...), MapType(...), UserType(...), FrozenType(...)
    #[allow(dead_code)]
    fn parse_cassandra_type(type_str: &str) -> Result<CqlType> {
        Self::parse_cassandra_type_with_depth(type_str, 0)
    }

    /// Internal implementation of parse_cassandra_type with recursion depth tracking.
    pub(super) fn parse_cassandra_type_with_depth(type_str: &str, depth: usize) -> Result<CqlType> {
        // Check recursion depth to prevent stack overflow
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::schema(format!(
                "Type nesting depth {} exceeds maximum {}. Type string: {}",
                depth,
                MAX_TYPE_NESTING_DEPTH,
                type_str.chars().take(100).collect::<String>()
            )));
        }

        let type_str = type_str.trim();

        // Handle FrozenType wrapper
        if type_str.starts_with("org.apache.cassandra.db.marshal.FrozenType(") {
            let inner_start = "org.apache.cassandra.db.marshal.FrozenType(".len();
            let inner = Self::extract_inner_parens(&type_str[inner_start..])?;
            let inner_type = Self::parse_cassandra_type_with_depth(&inner, depth + 1)?;
            return Ok(CqlType::Frozen(Box::new(inner_type)));
        }

        // Handle UserType (nested UDT)
        if type_str.starts_with("org.apache.cassandra.db.marshal.UserType(") {
            let udt_def = Self::parse_udt_type_definition_with_depth(type_str, depth + 1)?;
            let fields: Vec<(String, CqlType)> = udt_def
                .fields
                .into_iter()
                .map(|f| (f.name, f.field_type))
                .collect();
            return Ok(CqlType::Udt(udt_def.name, fields));
        }

        // Handle collection types
        if type_str.starts_with("org.apache.cassandra.db.marshal.ListType(") {
            let inner_start = "org.apache.cassandra.db.marshal.ListType(".len();
            let inner = Self::extract_inner_parens(&type_str[inner_start..])?;
            let elem_type = Self::parse_cassandra_type_with_depth(&inner, depth + 1)?;
            return Ok(CqlType::List(Box::new(elem_type)));
        }

        if type_str.starts_with("org.apache.cassandra.db.marshal.SetType(") {
            let inner_start = "org.apache.cassandra.db.marshal.SetType(".len();
            let inner = Self::extract_inner_parens(&type_str[inner_start..])?;
            let elem_type = Self::parse_cassandra_type_with_depth(&inner, depth + 1)?;
            return Ok(CqlType::Set(Box::new(elem_type)));
        }

        if type_str.starts_with("org.apache.cassandra.db.marshal.MapType(") {
            let inner_start = "org.apache.cassandra.db.marshal.MapType(".len();
            let inner = Self::extract_inner_parens(&type_str[inner_start..])?;
            let parts = Self::split_type_args(&inner)?;
            if parts.len() != 2 {
                return Err(Error::schema(format!(
                    "MapType requires exactly 2 type arguments: {}",
                    type_str
                )));
            }
            let key_type = Self::parse_cassandra_type_with_depth(&parts[0], depth + 1)?;
            let val_type = Self::parse_cassandra_type_with_depth(&parts[1], depth + 1)?;
            return Ok(CqlType::Map(Box::new(key_type), Box::new(val_type)));
        }

        // Handle primitive types
        Ok(match type_str {
            s if s.ends_with("UTF8Type") => CqlType::Text,
            s if s.ends_with("AsciiType") => CqlType::Ascii,
            s if s.ends_with("Int32Type") => CqlType::Int,
            s if s.ends_with("LongType") => CqlType::BigInt,
            s if s.ends_with("FloatType") => CqlType::Float,
            s if s.ends_with("DoubleType") => CqlType::Double,
            s if s.ends_with("BooleanType") => CqlType::Boolean,
            s if s.ends_with("UUIDType") || s.ends_with("TimeUUIDType") => CqlType::Uuid,
            s if s.ends_with("TimestampType") => CqlType::Timestamp,
            s if s.ends_with("DateType") || s.ends_with("SimpleDateType") => CqlType::Date,
            s if s.ends_with("TimeType") => CqlType::Time,
            s if s.ends_with("DecimalType") => CqlType::Decimal,
            s if s.ends_with("IntegerType") => CqlType::Varint,
            s if s.ends_with("BytesType") => CqlType::Blob,
            s if s.ends_with("InetAddressType") => CqlType::Inet,
            _ => CqlType::Custom(type_str.to_string()),
        })
    }

    /// Extract the contents inside parentheses, respecting nesting.
    pub(super) fn extract_inner_parens(s: &str) -> Result<String> {
        let mut depth = 1;
        let mut end_idx = 0;
        let chars: Vec<char> = s.chars().collect();

        for (i, c) in chars.iter().enumerate() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end_idx = i;
                        break;
                    }
                }
                _ => {}
            }
        }

        if depth != 0 {
            return Err(Error::schema(format!(
                "Unbalanced parentheses in type: {}",
                s
            )));
        }

        Ok(s[..end_idx].to_string())
    }
}
