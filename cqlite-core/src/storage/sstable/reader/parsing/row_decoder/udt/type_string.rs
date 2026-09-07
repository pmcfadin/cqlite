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
//!
//! # The PACKAGE is part of the identity (roborev job 76)
//!
//! Every name above is matched under the ONE package rule in
//! [`marshal_name`](super::marshal_name) — bare, or fully qualified under
//! `org.apache.cassandra.db.marshal`, and nothing else. A third-party class that
//! merely SHARES a simple name (`com.acme.Int32Type`, `com.acme.TupleType(...)`) is
//! refused with [`V5CompressedLegacyParser::foreign_marshal_package_error`], never
//! decoded as the native type it resembles: CQLite knows nothing about that class's
//! byte layout, and picking one from a name is a heuristic (#28).

use super::super::*;

// Issue #3631 / roborev job 68 finding 1: the marshal-form UDT field types that
// used to be routed to `Custom` (and thence, wrongly, to the nested-UDT decoder).
#[cfg(test)]
#[path = "regression_3631_marshal_field_types_tests.rs"]
mod regression_3631_marshal_field_types_tests;

// Issue #3631 / roborev job 76: the PACKAGE half of a marshal name's identity — a
// third-party class sharing a native simple name must be refused, not decoded.
#[cfg(test)]
#[path = "regression_3631_marshal_package_rule_tests.rs"]
mod regression_3631_marshal_package_rule_tests;

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

        // Locate + split the marshal `UserType(...)` arguments through the ONE
        // locator (`marshal_name.rs`), which validates the PACKAGE of the marker's
        // class name rather than substring-matching the qualified literal — a plain
        // `find` accepted `my.org.apache.cassandra.db.marshal.UserType(…)`, a
        // package SUFFIX (roborev job 76). `udt_field_marshal_types` shares it, so
        // the two `UserType(` consumers can no longer drift.
        let parts = Self::marshal_user_type_args(type_str)?;

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
        if let Some(args) = Self::marshal_parameterised_inner(type_str, "FrozenType") {
            let inner = Self::extract_inner_parens(args)?;
            let inner_type = Self::parse_cassandra_type_with_depth(&inner, depth + 1)?;
            return Ok(CqlType::Frozen(Box::new(inner_type)));
        }

        // Handle UserType (nested UDT)
        if Self::marshal_parameterised_inner(type_str, "UserType").is_some() {
            let udt_def = Self::parse_udt_type_definition_with_depth(type_str, depth + 1)?;
            let fields: Vec<(String, CqlType)> = udt_def
                .fields
                .into_iter()
                .map(|f| (f.name, f.field_type))
                .collect();
            return Ok(CqlType::Udt(udt_def.name, fields));
        }

        // Handle collection types
        if let Some(args) = Self::marshal_parameterised_inner(type_str, "ListType") {
            let inner = Self::extract_inner_parens(args)?;
            let elem_type = Self::parse_cassandra_type_with_depth(&inner, depth + 1)?;
            return Ok(CqlType::List(Box::new(elem_type)));
        }

        if let Some(args) = Self::marshal_parameterised_inner(type_str, "SetType") {
            let inner = Self::extract_inner_parens(args)?;
            let elem_type = Self::parse_cassandra_type_with_depth(&inner, depth + 1)?;
            return Ok(CqlType::Set(Box::new(elem_type)));
        }

        if let Some(args) = Self::marshal_parameterised_inner(type_str, "MapType") {
            let inner = Self::extract_inner_parens(args)?;
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

        // TupleType — STRUCTURAL, so it is PARSED and not name-matched: its marshal
        // string is `getClass().getName() + stringifyTypeParameters(types, true)`
        // (cassandra-5.0.8 TupleType.java:557), i.e. a parenthesised, comma-separated
        // component list. `depth + 1` like every other structural arm — a reset here
        // would make `MAX_TYPE_NESTING_DEPTH` bound nothing across a
        // tuple-inside-collection-inside-tuple chain.
        if let Some(args) = Self::marshal_parameterised_inner(type_str, "TupleType") {
            let inner = Self::extract_inner_parens(args)?;
            let parts = Self::split_type_args(&inner)?;
            if parts.is_empty() {
                return Err(Error::schema(format!(
                    "TupleType requires at least one component type: {}",
                    type_str
                )));
            }
            // `parts.len()` is bounded by the length of a type string that came out of
            // the SerializationHeader, not by a length field inside a value, so this
            // reserve cannot be inflated by a corrupt count.
            let mut components = Vec::with_capacity(parts.len());
            for part in &parts {
                components.push(Self::parse_cassandra_type_with_depth(part, depth + 1)?);
            }
            return Ok(CqlType::Tuple(components));
        }

        // VectorType — STRUCTURAL, so it is PARSED and not name-matched (issue
        // #4114). Its marshal string is
        // `getClass().getName() + stringifyVectorParameters(type, dimension)`
        // (`cassandra-5.0.8` VectorType.java:339-342 / TypeParser.java:239-242),
        // i.e. `VectorType(<element> , <n>)`. `depth + 1` like every other
        // structural arm.
        //
        // Before this arm the whole type collapsed to `CqlType::Custom`, which the
        // decoder refuses by name — honest, but it meant a `vector<float, n>` UDT
        // FIELD could not be read at all. The dimension is carried into
        // `CqlType::Vector` because the on-disk value has no element count and no
        // per-element framing for a fixed-width element: `n` is the only thing that
        // makes it parseable (#28).
        if let Some(args) = Self::marshal_parameterised_inner(type_str, "VectorType") {
            let inner = Self::extract_inner_parens(args)?;
            let parsed = crate::schema::vector_type::split_vector_args(&inner, type_str)?;
            let element = Self::parse_cassandra_type_with_depth(parsed.element, depth + 1)?;
            return Ok(CqlType::Vector(Box::new(element), parsed.dimension));
        }

        // ReversedType — a COMPARISON wrapper with no layout of its own:
        // `ReversedType.asCQL3Type()` and `getSerializer()` both delegate to
        // `baseType` (cassandra-5.0.8 ReversedType.java:138,144), so the value of a
        // `ReversedType(X)` is byte-for-byte the value of an `X`.
        if let Some(args) = Self::marshal_parameterised_inner(type_str, "ReversedType") {
            let inner = Self::extract_inner_parens(args)?;
            return Self::parse_cassandra_type_with_depth(&inner, depth + 1);
        }

        // THE PACKAGE RULE, at the ONE point every non-structural name reaches
        // (roborev job 76). A name qualified outside Cassandra's marshal package is
        // a third-party `AbstractType`, and it is refused HERE rather than passed
        // on as `Custom`, for a reason that is about WHERE THE CONTEXT LIVES: this
        // function is the only place that knows the string is a MARSHAL type
        // string, where a dotted name is a Java class name. A `CqlType::Custom`
        // payload has lost that context — it carries a marshal reference OR a
        // KEYSPACE-QUALIFIED UDT NAME (`test_ks.address`, a real form on the write
        // path) — so no downstream predicate can tell `acme.Int32Type` from
        // `ks.address` without guessing (#28). Structural forms reach this line
        // too: they match no arm above (each requires the qualified marshal
        // spelling), so `com.acme.TupleType(…)` is refused here by the same rule.
        if Self::marshal_simple_name(Self::marshal_head(type_str)).is_none() {
            return Err(Self::foreign_marshal_package_error(type_str));
        }

        // A native (non-parameterised) marshal type, else `Custom` — which the
        // DECODER refuses by name (issue #3631 criterion 5). Nothing here guesses
        // from bytes (#28): the mapping is keyed on the DECLARED marshal name only.
        Ok(Self::native_marshal_to_cql_type(type_str)
            .unwrap_or_else(|| CqlType::Custom(Self::canonical_marshal_class_name(type_str))))
    }

    /// The name Cassandra itself would resolve `type_str` to:
    /// `TypeParser.getAbstractType` (cassandra-5.0.8 TypeParser.java:450) loads
    /// `compareWith.contains(".") ? compareWith : "org.apache.cassandra.db.marshal." +
    /// compareWith`, so inside a marshal type string an UNQUALIFIED name IS a class in
    /// the marshal package.
    ///
    /// Recording the resolved form is what lets the decoder tell an unmappable marshal
    /// type from a UDT NAME: both are [`CqlType::Custom`], and only the qualified
    /// spelling is unambiguous (roborev job 68, finding 1). Every field type in a real
    /// `SerializationHeader` already arrives qualified, so this is the tolerated
    /// hand-written / `TypeParser`-legal bare spelling, normalised by Cassandra's own
    /// rule rather than by a guess about which names are class names.
    fn canonical_marshal_class_name(type_str: &str) -> String {
        let name = type_str.trim();
        if name.contains('.') {
            return name.to_string();
        }
        format!("{}{name}", Self::MARSHAL_PACKAGE)
    }

    /// The marshal-class-name -> [`CqlType`] table: the ONE place a Cassandra native
    /// `AbstractType` is bound to a CQLite type. `None` for a parameterised form (the
    /// structural arms above own those) and for a native type [`CqlType`] cannot
    /// express — see this module's header for the per-name authority and for the
    /// names deliberately left unmapped.
    ///
    /// # The name is resolved under the ONE package rule, then matched EXACTLY
    /// [`Self::marshal_simple_name`] (see `marshal_name.rs` for the pinned
    /// `TypeParser` authority) accepts a marshal name in EXACTLY TWO spellings — a
    /// bare simple name, or a fully-qualified class name under
    /// `org.apache.cassandra.db.marshal` — and yields the simple name, which is
    /// then the whole identity of the type.
    ///
    /// Two matching mistakes this closes, both of which shipped:
    ///
    /// * A **suffix** match (`ends_with`) is at once too loose and too tight.
    ///   `ends_with("DateType")` also matches `SimpleDateType` (the real CQL
    ///   `date`), so whether a `date` field decoded as an 8-byte `timestamp`
    ///   depended on arm ORDER.
    /// * Taking the text after the last `.` and ignoring the **package** decoded a
    ///   third-party `com.acme.Int32Type` as CQL `int` — an unknown class's bytes
    ///   read as if the class were known (roborev job 76). The rule now returns
    ///   `None` for it, and the type-string parser refuses it by name.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn native_marshal_to_cql_type(
        marshal_type: &str,
    ) -> Option<CqlType> {
        let name = marshal_type.trim();
        // A parameterised form is structural; the arms above are its only decoders.
        if name.contains('(') {
            return None;
        }
        // THE package rule (`marshal_name.rs`): `Some` only for a bare simple name
        // or the canonical `org.apache.cassandra.db.marshal.X` spelling, so a
        // third-party `com.acme.Int32Type` cannot reach the table below by simple
        // name alone.
        let simple = Self::marshal_simple_name(name)?;
        Some(match simple {
            // -- CQL3Type.Native, one arm per enum constant --------------------
            "AsciiType" => CqlType::Ascii,
            "LongType" => CqlType::BigInt,
            "BytesType" => CqlType::Blob,
            "BooleanType" => CqlType::Boolean,
            "CounterColumnType" => CqlType::Counter,
            "SimpleDateType" => CqlType::Date,
            "DecimalType" => CqlType::Decimal,
            "DoubleType" => CqlType::Double,
            "DurationType" => CqlType::Duration,
            "FloatType" => CqlType::Float,
            "InetAddressType" => CqlType::Inet,
            "Int32Type" => CqlType::Int,
            "ShortType" => CqlType::SmallInt,
            "UTF8Type" => CqlType::Text,
            "TimeType" => CqlType::Time,
            "TimestampType" => CqlType::Timestamp,
            "TimeUUIDType" => CqlType::TimeUuid,
            "ByteType" => CqlType::TinyInt,
            "UUIDType" => CqlType::Uuid,
            "IntegerType" => CqlType::Varint,
            // -- Not in that enum; each mapped from its own authority ----------
            // `DateType.asCQL3Type()` -> `TIMESTAMP`: the LEGACY 8-byte millis type.
            "DateType" => CqlType::Timestamp,
            // An alias `TypeParser` resolves to `UTF8Type`.
            "VarcharType" => CqlType::Text,
            // `LexicalUUIDType.Serializer extends UUIDSerializer`, 16 bytes fixed.
            "LexicalUUIDType" => CqlType::Uuid,
            // `LegacyTimeUUIDType extends AbstractTimeUUIDType<UUID>`: a timeuuid's
            // value layout, differing only in comparison.
            "LegacyTimeUUIDType" => CqlType::TimeUuid,
            _ => return None,
        })
    }

    /// Whether a [`CqlType::Custom`] payload is a **marshal type REFERENCE** — a
    /// Cassandra `AbstractType` class name, or a structural marshal fragment — rather
    /// than the NAME OF A UDT.
    ///
    /// `CqlType::Custom` carries both (see `crate::schema::is_udt_identifier`), and
    /// the typed decoder has to route them differently: a UDT name goes to the
    /// nested-UDT decoder, while a marshal reference has no field list and never
    /// will, so reporting it as a UDT with a missing field list misattributes the
    /// cause (roborev job 68, finding 1).
    ///
    /// The qualified package prefix is the discriminator because it is the single
    /// shape real `SerializationHeader`s carry — the same fact
    /// `marshal_is_top_level_frozen_udt` and `parse_udt_type_definition_with_depth`
    /// already key on (roborev jobs 1359/1361). This reads a DECLARED type string's
    /// own syntax; it infers nothing from value bytes (#28).
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn custom_is_marshal_type_reference(
        name: &str,
    ) -> bool {
        const PKG: &str = "org.apache.cassandra.";
        let name = name.trim();
        // A structural fragment: no UDT name can carry a parameter list.
        if name.contains('(') || name.contains(',') || name.contains('<') {
            return true;
        }
        // `str::get` yields `None` on a non-char-boundary index, so this cannot panic
        // on a multi-byte name.
        name.get(..PKG.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(PKG))
    }

    /// Extract the contents inside parentheses, respecting nesting.
    ///
    /// Indexed by `char_indices`, i.e. by BYTE offset. This used to collect a
    /// `Vec<char>` and slice `s[..i]` with the CHARACTER index `i`, which for any
    /// type string carrying a multi-byte character before the closing paren is
    /// either the wrong slice or — when the index lands mid-character — a PANIC
    /// (`extract_inner_parens("é)")` sliced `s[..1]` inside a 2-byte `é`). A
    /// SerializationHeader type string is attacker-influenced input, so the read
    /// path must not panic on it.
    pub(super) fn extract_inner_parens(s: &str) -> Result<String> {
        let mut depth = 1usize;
        for (offset, c) in s.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Ok(s[..offset].to_string());
                    }
                }
                _ => {}
            }
        }
        Err(Error::schema(format!(
            "Unbalanced parentheses in type: {}",
            s
        )))
    }
}
