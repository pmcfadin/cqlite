//! THE single UDT-field value decoder (issue #3722).
//!
//! # Why this module exists
//!
//! There used to be TWO shared UDT-field decoders with DIVERGENT arm sets, both
//! ending in `_ => Value::Blob`: `udt.rs`'s `parse_udt_field_value` (method) and
//! its `parse_simple_udt_field_value` (free fn). 14 CQL types fell through to an
//! opaque blob in the first; the second additionally dropped `date`, `inet`,
//! `frozen` and nested `udt` while being the ONLY one that handled `timeuuid`.
//! The drift was BIDIRECTIONAL, so the decoded type of a UDT field depended on
//! which route the value took through the reader. Both are deleted; this is the
//! only UDT-field decoder, and all 13 former call sites go through it.
//!
//! It lives in its own file because `udt.rs` was 1777 lines against the 800-line
//! campsite source target (epic #1116) — moving both decoders OUT is what shrinks
//! it.
//!
//! # What stops the arm sets diverging again
//!
//! [`V5CompressedLegacyParser::parse_udt_field_value`] is TOTAL over `CqlType`
//! with NO wildcard arm, pinned by `#[deny(clippy::wildcard_enum_match_arm)]`
//! (the same device `bindings/python/src/value_hashable.rs` uses for this defect
//! class, issue #3500). A new `CqlType` variant is therefore a COMPILE error
//! here instead of a silent blob on somebody's data. That is strictly stronger
//! than an equality test between two decoders' outputs: with one decoder,
//! equality is trivially true and proves nothing, while totality is the property
//! that actually prevents recurrence.
//!
//! # Deliberate differences from `parse_value_from_raw_bytes` (`raw_value.rs`)
//!
//! That function decodes an already-bounded value from a type STRING and looks
//! like a candidate to route through; it is not, in three respects:
//!
//! * `float` there widens to `Value::Float(f as f64)`; a UDT field must stay a
//!   lossless `Value::Float32` (issue #1884).
//! * its fixed-width arms are `data.len() < N` and SLICE; the arms here are
//!   strict `!= N`, so a 5-byte `int` or a 17-byte `uuid` field errors instead of
//!   silently decoding from a prefix. Loosening an existing corruption check is
//!   not a refactor.
//! * it takes a type STRING, and a `CqlType::Udt(name, fields)` cannot be
//!   rendered to one without DROPPING the inline field defs, which nothing
//!   downstream can recover. The `Udt` arm here recurses structurally instead.
//!
//! The `Custom(s)` arm — an unresolved marshal class or a registry UDT name — is
//! the one place a type string is genuinely all we have, and it routes there.
//!
//! # Collection element types
//!
//! The list/set/map/tuple arms delegate the payload framing to the existing
//! parsers in `frozen.rs`, which take ELEMENT types as separate `&str`s. Only the
//! element types are rendered (via [`CqlType::to_cql_string`]) — never an outer
//! `list<…>`/`map<…>` string — which bypasses `extract_map_types` /
//! `extract_tuple_element_types` entirely. Two consequences worth knowing:
//! element values are decoded by `parse_value_from_raw_bytes`, so they follow ITS
//! conventions (notably a `float` ELEMENT is `Value::Float(f64)`, unchanged from
//! every other frozen-collection path), and a UDT ELEMENT resolves by NAME
//! through the `UdtRegistry` rather than from inline field defs.

use super::*;

impl V5CompressedLegacyParser {
    /// Decode ONE UDT field value from the exact bytes of that field.
    ///
    /// `data` is the whole field value: the caller has already consumed the
    /// field's `[i32 BE len]` prefix and bounded the slice (a `-1` length is a
    /// null field and never reaches here; a `0` length goes through
    /// `create_empty_value_for_type` or arrives here as an empty slice).
    ///
    /// `depth` counts CQL type nesting, not bytes, and bounds this function's own
    /// recursion (`frozen<...>` chains) the same way the sibling decoders bound
    /// theirs.
    ///
    /// There is deliberately NO `_ =>` arm — see the module header.
    #[deny(clippy::wildcard_enum_match_arm)]
    pub(super) fn parse_udt_field_value(
        &self,
        data: &[u8],
        field_type: &CqlType,
        depth: usize,
    ) -> Result<Value> {
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::corruption(format!(
                "UDT field type nesting depth {} exceeds maximum {}",
                depth, MAX_TYPE_NESTING_DEPTH
            )));
        }
        match field_type {
            // ---------------------------------------------------------------
            // Text-ish: the whole slice IS the value; UTF-8 is a hard error.
            // ---------------------------------------------------------------
            CqlType::Text | CqlType::Ascii | CqlType::Varchar => {
                std::str::from_utf8(data)
                    .map_err(|e| Error::corruption(format!("Invalid UTF-8 in UDT field: {}", e)))?;
                Ok(Value::Text(
                    crate::storage::sstable::reader::value_borrow::borrow_active(data),
                ))
            }
            CqlType::Blob => Ok(Value::Blob(
                crate::storage::sstable::reader::value_borrow::borrow_active(data),
            )),
            CqlType::Inet => Ok(Value::Inet(
                crate::storage::sstable::reader::value_borrow::borrow_active(data),
            )),
            CqlType::Varint => Ok(Value::Varint(
                crate::storage::sstable::reader::value_borrow::borrow_active(data),
            )),

            // ---------------------------------------------------------------
            // Fixed-width integers/floats. Every length check is strict `!= N`.
            // ---------------------------------------------------------------
            CqlType::Boolean => {
                Self::require_len(data, 1, "Boolean")?;
                Ok(Value::Boolean(data[0] != 0))
            }
            CqlType::TinyInt => {
                Self::require_len(data, 1, "TinyInt")?;
                Ok(Value::TinyInt(data[0] as i8))
            }
            CqlType::SmallInt => {
                Self::require_len(data, 2, "SmallInt")?;
                Ok(Value::SmallInt(i16::from_be_bytes([data[0], data[1]])))
            }
            CqlType::Int => {
                Self::require_len(data, 4, "Int")?;
                Ok(Value::Integer(i32::from_be_bytes([
                    data[0], data[1], data[2], data[3],
                ])))
            }
            CqlType::BigInt => {
                Self::require_len(data, 8, "BigInt")?;
                Ok(Value::BigInt(Self::be_i64(data)))
            }
            // A UDT field can never BE a counter in Cassandra 5.0: `CREATE TYPE`
            // with a counter field is rejected server-side ("A user type cannot
            // contain counters"), so this arm is UNREACHABLE from
            // Cassandra-written data and is pinned by a unit test only, never by
            // a fixture. It exists so the match stays total. Note
            // `parse_value_from_raw_bytes` maps the STRING "counter" to
            // `Value::BigInt`; here the type is `CqlType::Counter`, which has its
            // own `Value` variant, so we use it.
            CqlType::Counter => {
                Self::require_len(data, 8, "Counter")?;
                Ok(Value::Counter(Self::be_i64(data)))
            }
            CqlType::Float => {
                Self::require_len(data, 4, "Float")?;
                // Issue #1884: keep the lossless f32 variant.
                Ok(Value::Float32(f32::from_bits(u32::from_be_bytes([
                    data[0], data[1], data[2], data[3],
                ]))))
            }
            CqlType::Double => {
                Self::require_len(data, 8, "Double")?;
                Ok(Value::Float(f64::from_bits(Self::be_i64(data) as u64)))
            }
            CqlType::Uuid | CqlType::TimeUuid => {
                // There is no distinct `Value::TimeUuid`; a timeuuid is a UUID
                // whose bytes encode the time. This is also the one arm the two
                // former decoders disagreed about in the OTHER direction (only
                // the free fn handled `TimeUuid`).
                Self::require_len(data, 16, "UUID")?;
                let uuid_bytes: [u8; 16] = data[0..16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;
                Ok(Value::Uuid(uuid_bytes))
            }

            // ---------------------------------------------------------------
            // Temporal.
            // ---------------------------------------------------------------
            CqlType::Timestamp => {
                Self::require_len(data, 8, "Timestamp")?;
                Ok(Value::Timestamp(Self::be_i64(data)))
            }
            // Cassandra stores a `date` as an UNSIGNED day count offset by 2^31:
            // `SimpleDateSerializer.dayToTimeInMillis(int days)` is
            // `Duration.ofDays(days + Integer.MIN_VALUE)`, i.e. real
            // days-since-epoch = stored + Integer.MIN_VALUE (authority:
            // `git show cassandra-5.0.8:src/java/org/apache/cassandra/serializers/
            // SimpleDateSerializer.java`). The pre-#3722 UDT-field arm did a bare
            // `u32 as i32` with NO offset and was wrong by 2^31 days; it was the
            // sole outlier among this tree's date decoders.
            CqlType::Date => {
                Self::require_len(data, 4, "Date")?;
                let stored = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(Value::Date(stored.wrapping_add(i32::MIN as u32) as i32))
            }
            CqlType::Time => {
                Self::require_len(data, 8, "Time")?;
                Ok(Value::Time(Self::be_i64(data)))
            }
            // DurationSerializer: three consecutive SIGNED VInts (months, days,
            // nanos) over the whole slice — there is NO outer `[VInt len]` here,
            // because the field's `[i32 BE len]` prefix already bounded `data`.
            CqlType::Duration => Self::parse_udt_field_duration(data),

            // ---------------------------------------------------------------
            // Numeric with a prefix: `[i32 BE scale][unscaled BigInteger]`.
            // ---------------------------------------------------------------
            CqlType::Decimal => {
                if data.len() < 4 {
                    return Err(Error::corruption(format!(
                        "Decimal field requires at least 4 bytes for the scale, got {}",
                        data.len()
                    )));
                }
                Ok(Value::Decimal {
                    scale: i32::from_be_bytes([data[0], data[1], data[2], data[3]]),
                    unscaled: data[4..].to_vec(),
                })
            }

            // ---------------------------------------------------------------
            // Collections/tuple: delegate the payload framing to `frozen.rs`,
            // passing ONLY the rendered ELEMENT types (module header).
            // ---------------------------------------------------------------
            CqlType::List(element) => {
                let (value, _) = self.parse_frozen_list_value_raw(
                    data,
                    0,
                    &element.to_cql_string(),
                    "udt field",
                    depth + 1,
                )?;
                Ok(value)
            }
            CqlType::Set(element) => {
                let (value, _) = self.parse_frozen_set_value_raw(
                    data,
                    0,
                    &element.to_cql_string(),
                    "udt field",
                    depth + 1,
                )?;
                Ok(value)
            }
            CqlType::Map(key, value) => {
                let (decoded, _) = self.parse_frozen_map_value_raw(
                    data,
                    0,
                    &key.to_cql_string(),
                    &value.to_cql_string(),
                    "udt field",
                    depth + 1,
                )?;
                Ok(decoded)
            }
            CqlType::Tuple(element_types) => {
                let rendered: Vec<String> =
                    element_types.iter().map(|t| t.to_cql_string()).collect();
                let mut offset = 0usize;
                let elements = self.parse_tuple_elements_raw(
                    data,
                    &mut offset,
                    data.len(),
                    &rendered,
                    "udt field",
                    depth + 1,
                )?;
                Ok(Value::Tuple(elements))
            }

            // ---------------------------------------------------------------
            // Composite.
            // ---------------------------------------------------------------
            CqlType::Frozen(inner) => Ok(Value::Frozen(Box::new(self.parse_udt_field_value(
                data,
                inner,
                depth + 1,
            )?))),
            // Recurse STRUCTURALLY on the inline field defs. Rendering the name
            // and re-resolving it would drop them (module header).
            CqlType::Udt(name, field_defs) => {
                let mut nested_def = UdtTypeDef::new("".to_string(), name.clone());
                for (field_name, nested_type) in field_defs {
                    nested_def =
                        nested_def.with_field(field_name.clone(), nested_type.clone(), true);
                }
                let dummy_column = crate::schema::Column {
                    name: name.clone(),
                    data_type: "udt".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                };
                let (value, _) = self.parse_udt_value(data, 0, &nested_def, &dummy_column)?;
                Ok(value)
            }
            // An UNRESOLVED type string — a marshal class, or a UDT name to look
            // up in the registry. This is the only arm where a string is all we
            // have, and it is the one place a genuinely unknown type may still
            // land on that function's blob fallback.
            CqlType::Custom(type_str) => {
                self.parse_value_from_raw_bytes(data, type_str, "udt field", depth + 1)
            }
        }
    }

    /// Strict fixed-width field length check: EXACTLY `expected` bytes.
    ///
    /// Not `<`: a wrong-length fixed-width field is corruption, and decoding from
    /// a prefix would hide it.
    fn require_len(data: &[u8], expected: usize, type_name: &str) -> Result<()> {
        if data.len() != expected {
            return Err(Error::corruption(format!(
                "{} field requires {} byte{}, got {}",
                type_name,
                expected,
                if expected == 1 { "" } else { "s" },
                data.len()
            )));
        }
        Ok(())
    }

    /// Big-endian i64 from the first 8 bytes. Callers check the length first.
    fn be_i64(data: &[u8]) -> i64 {
        i64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ])
    }

    /// `duration` field: three consecutive SIGNED VInts over the whole slice.
    ///
    /// `months`/`days` are `i32` in Cassandra's `DurationType`, so an encoded
    /// value outside the i32 range is REJECTED rather than truncated by `as i32`
    /// (same rule as the frozen-element decoder, issue #1632 item b).
    fn parse_udt_field_duration(data: &[u8]) -> Result<Value> {
        let mut pos = 0usize;
        let mut next = |component: &str| -> Result<i64> {
            let (remaining, raw) = parse_vint(&data[pos..]).map_err(|e| {
                Error::corruption(format!(
                    "Duration field: failed to parse {}: {:?}",
                    component, e
                ))
            })?;
            pos = data.len() - remaining.len();
            Ok(raw)
        };
        let months = next("months")?;
        let days = next("days")?;
        let nanos = next("nanos")?;

        let months = i32::try_from(months)
            .map_err(|_| Error::corruption("Duration field: months out of i32 range"))?;
        let days = i32::try_from(days)
            .map_err(|_| Error::corruption("Duration field: days out of i32 range"))?;
        Ok(Value::Duration {
            months,
            days,
            nanos,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::helpers::*;
    use super::*;

    fn parser() -> V5CompressedLegacyParser {
        V5CompressedLegacyParser::new("test_ks".to_string(), "test_table".to_string(), 0, 0, None)
    }

    fn decode(field_type: &CqlType, data: &[u8]) -> Value {
        parser()
            .parse_udt_field_value(data, field_type, 0)
            .unwrap_or_else(|e| panic!("decode of {field_type:?} failed: {e}"))
    }

    /// Cassandra's `VIntCoding` signed encoding: zigzag, then unsigned VInt.
    fn encode_signed(value: i64, buf: &mut Vec<u8>) {
        encode_unsigned(((value << 1) ^ (value >> 63)) as u64, buf);
    }

    // ---------------------------------------------------------------------
    // CONTROLS: the two arms that legitimately produce their pre-#3722 value.
    // A "fix" that blanket-stops emitting Blob, or that reroutes `int`, breaks
    // these.
    // ---------------------------------------------------------------------

    #[test]
    fn control_blob_field_stays_blob() {
        assert_eq!(
            decode(&CqlType::Blob, &[0xDE, 0xAD]),
            Value::blob(vec![0xDE, 0xAD])
        );
    }

    #[test]
    fn control_int_field_stays_integer() {
        assert_eq!(
            decode(&CqlType::Int, &7i32.to_be_bytes()),
            Value::Integer(7)
        );
    }

    // ---------------------------------------------------------------------
    // Per-type arms (issue #3722): every one of these previously fell through
    // to `_ => Value::Blob` in at least one of the two decoders.
    // ---------------------------------------------------------------------

    #[test]
    fn tinyint_field_decodes_to_tinyint() {
        assert_eq!(decode(&CqlType::TinyInt, &[0xFF]), Value::TinyInt(-1));
    }

    #[test]
    fn smallint_field_decodes_to_smallint() {
        assert_eq!(
            decode(&CqlType::SmallInt, &(-2i16).to_be_bytes()),
            Value::SmallInt(-2)
        );
    }

    #[test]
    fn bigint_field_decodes_to_bigint() {
        assert_eq!(
            decode(&CqlType::BigInt, &(-9i64).to_be_bytes()),
            Value::BigInt(-9)
        );
    }

    /// A UDT cannot contain a counter in Cassandra 5.0 (`CREATE TYPE` with a
    /// counter field is rejected: `InvalidRequest ... "A user type cannot
    /// contain counters"`), so this arm is UNREACHABLE from Cassandra-written
    /// data and can only ever be pinned by a unit test — never by a fixture. It
    /// exists so the match stays TOTAL over `CqlType`.
    #[test]
    fn counter_field_decodes_to_counter() {
        assert_eq!(
            decode(&CqlType::Counter, &5i64.to_be_bytes()),
            Value::Counter(5)
        );
    }

    /// Issue #1884: a `float` field must stay a LOSSLESS `Float32`, never be
    /// widened to `Value::Float(f64)`.
    #[test]
    fn float_field_decodes_to_float32_not_float64() {
        let v = decode(&CqlType::Float, &0.1f32.to_be_bytes());
        assert_eq!(v, Value::Float32(0.1f32));
        assert!(
            !matches!(v, Value::Float(_)),
            "a float field must not widen to Value::Float(f64)"
        );
    }

    #[test]
    fn double_field_decodes_to_float() {
        assert_eq!(
            decode(&CqlType::Double, &0.5f64.to_be_bytes()),
            Value::Float(0.5)
        );
    }

    #[test]
    fn boolean_field_decodes_to_boolean() {
        assert_eq!(decode(&CqlType::Boolean, &[1]), Value::Boolean(true));
    }

    #[test]
    fn decimal_field_decodes_to_decimal() {
        let mut data = 2i32.to_be_bytes().to_vec();
        data.extend_from_slice(&[0x01, 0x2C]); // unscaled 300, scale 2 => 3.00
        assert_eq!(
            decode(&CqlType::Decimal, &data),
            Value::Decimal {
                scale: 2,
                unscaled: vec![0x01, 0x2C]
            }
        );
    }

    #[test]
    fn varint_field_decodes_to_varint() {
        assert_eq!(
            decode(&CqlType::Varint, &[0x01, 0x00]),
            Value::varint(vec![0x01, 0x00])
        );
    }

    #[test]
    fn text_ascii_and_varchar_fields_decode_to_text() {
        for t in [CqlType::Text, CqlType::Ascii, CqlType::Varchar] {
            assert_eq!(decode(&t, b"hi"), Value::text("hi"), "type {t:?}");
        }
    }

    #[test]
    fn timestamp_field_decodes_to_timestamp() {
        assert_eq!(
            decode(&CqlType::Timestamp, &1_700_000_000_000i64.to_be_bytes()),
            Value::Timestamp(1_700_000_000_000)
        );
    }

    /// Cassandra stores a `date` as an UNSIGNED day count offset by 2^31
    /// (`SimpleDateSerializer.dayToTimeInMillis`: `days + Integer.MIN_VALUE`),
    /// so the stored bytes for 1970-01-01 are `0x80000000`, not `0x00000000`.
    /// The pre-#3722 UDT-field arm applied no offset and was wrong by 2^31 days.
    #[test]
    fn date_field_applies_the_cassandra_epoch_offset() {
        assert_eq!(
            decode(&CqlType::Date, &[0x80, 0x00, 0x00, 0x00]),
            Value::Date(0),
            "stored 0x80000000 is days-since-epoch 0"
        );
        assert_eq!(
            decode(&CqlType::Date, &[0x80, 0x00, 0x00, 0x01]),
            Value::Date(1)
        );
        assert_eq!(
            decode(&CqlType::Date, &[0x00, 0x00, 0x00, 0x00]),
            Value::Date(i32::MIN)
        );
    }

    #[test]
    fn time_field_decodes_to_time() {
        assert_eq!(
            decode(&CqlType::Time, &123_456_789i64.to_be_bytes()),
            Value::Time(123_456_789)
        );
    }

    #[test]
    fn uuid_and_timeuuid_fields_decode_to_uuid() {
        let bytes = [7u8; 16];
        for t in [CqlType::Uuid, CqlType::TimeUuid] {
            assert_eq!(decode(&t, &bytes), Value::Uuid(bytes), "type {t:?}");
        }
    }

    #[test]
    fn inet_field_decodes_to_inet() {
        assert_eq!(
            decode(&CqlType::Inet, &[127, 0, 0, 1]),
            Value::inet(vec![127, 0, 0, 1])
        );
    }

    #[test]
    fn duration_field_decodes_to_duration() {
        let mut data = Vec::new();
        encode_signed(1, &mut data); // months
        encode_signed(2, &mut data); // days
        encode_signed(3_000, &mut data); // nanos
        assert_eq!(
            decode(&CqlType::Duration, &data),
            Value::Duration {
                months: 1,
                days: 2,
                nanos: 3_000
            }
        );
    }

    #[test]
    fn list_field_decodes_to_list() {
        assert_eq!(
            decode(
                &CqlType::List(Box::new(CqlType::Int)),
                &build_frozen_list_int(&[1, 2])
            ),
            Value::List(vec![Value::Integer(1), Value::Integer(2)])
        );
    }

    #[test]
    fn set_field_decodes_to_set() {
        assert_eq!(
            decode(
                &CqlType::Set(Box::new(CqlType::Text)),
                &build_frozen_list_text(&["a", "b"])
            ),
            Value::Set(vec![Value::text("a"), Value::text("b")])
        );
    }

    #[test]
    fn map_field_decodes_to_map() {
        assert_eq!(
            decode(
                &CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int)),
                &build_frozen_map_text_int(&[("k", 9)])
            ),
            Value::Map(vec![(Value::text("k"), Value::Integer(9))])
        );
    }

    #[test]
    fn tuple_field_decodes_to_tuple() {
        // [i32 len][bytes] per element, no count prefix.
        let mut data = 4i32.to_be_bytes().to_vec();
        data.extend_from_slice(&11i32.to_be_bytes());
        data.extend_from_slice(&2i32.to_be_bytes());
        data.extend_from_slice(b"hi");
        assert_eq!(
            decode(&CqlType::Tuple(vec![CqlType::Int, CqlType::Text]), &data),
            Value::Tuple(vec![Value::Integer(11), Value::text("hi")])
        );
    }

    /// A nested UDT must recurse STRUCTURALLY on the inline field defs — never
    /// be rendered to a bare type name, which would drop them.
    #[test]
    fn nested_udt_field_recurses_on_inline_field_defs() {
        let mut data = 4i32.to_be_bytes().to_vec();
        data.extend_from_slice(&42i32.to_be_bytes());
        let t = CqlType::Udt(
            "inner_type".to_string(),
            vec![("n".to_string(), CqlType::Int)],
        );
        match decode(&t, &data) {
            Value::Udt(udt) => {
                assert_eq!(udt.type_name, "inner_type");
                assert_eq!(udt.fields.len(), 1);
                assert_eq!(udt.fields[0].name, "n");
                assert_eq!(udt.fields[0].value, Some(Value::Integer(42)));
            }
            other => panic!("expected a structurally decoded UDT, got {other:?}"),
        }
    }

    #[test]
    fn frozen_field_wraps_the_inner_decode() {
        assert_eq!(
            decode(
                &CqlType::Frozen(Box::new(CqlType::Int)),
                &3i32.to_be_bytes()
            ),
            Value::Frozen(Box::new(Value::Integer(3)))
        );
    }

    /// `Custom` carries an unresolved type string (a marshal class, or a
    /// registry UDT name) and is the ONE arm that routes to the marshal/short-form
    /// resolver.
    #[test]
    fn custom_field_resolves_a_marshal_type_string() {
        assert_eq!(
            decode(
                &CqlType::Custom("org.apache.cassandra.db.marshal.Int32Type".to_string()),
                &8i32.to_be_bytes()
            ),
            Value::Integer(8)
        );
    }

    // ---------------------------------------------------------------------
    // The recurrence guard (issue #3722 AC5): NO non-blob CQL type may decode
    // to an opaque `Value::Blob`. One decoder + a total match makes the arm
    // sets unable to diverge; this asserts the OBSERVABLE consequence.
    // ---------------------------------------------------------------------

    #[test]
    fn no_non_blob_cql_type_decodes_to_an_opaque_blob() {
        let mut tuple_data = 4i32.to_be_bytes().to_vec();
        tuple_data.extend_from_slice(&1i32.to_be_bytes());
        let mut udt_data = 4i32.to_be_bytes().to_vec();
        udt_data.extend_from_slice(&1i32.to_be_bytes());
        let mut duration = Vec::new();
        encode_signed(0, &mut duration);
        encode_signed(0, &mut duration);
        encode_signed(0, &mut duration);

        let cases: Vec<(CqlType, Vec<u8>)> = vec![
            (CqlType::Boolean, vec![0]),
            (CqlType::TinyInt, vec![1]),
            (CqlType::SmallInt, vec![0, 1]),
            (CqlType::Int, 1i32.to_be_bytes().to_vec()),
            (CqlType::BigInt, 1i64.to_be_bytes().to_vec()),
            (CqlType::Counter, 1i64.to_be_bytes().to_vec()),
            (CqlType::Float, 1f32.to_be_bytes().to_vec()),
            (CqlType::Double, 1f64.to_be_bytes().to_vec()),
            (CqlType::Decimal, vec![0, 0, 0, 1, 5]),
            (CqlType::Text, b"a".to_vec()),
            (CqlType::Ascii, b"a".to_vec()),
            (CqlType::Varchar, b"a".to_vec()),
            (CqlType::Timestamp, 1i64.to_be_bytes().to_vec()),
            (CqlType::Date, vec![0x80, 0, 0, 0]),
            (CqlType::Time, 1i64.to_be_bytes().to_vec()),
            (CqlType::Uuid, vec![0u8; 16]),
            (CqlType::TimeUuid, vec![0u8; 16]),
            (CqlType::Inet, vec![127, 0, 0, 1]),
            (CqlType::Duration, duration),
            (CqlType::Varint, vec![1]),
            (
                CqlType::List(Box::new(CqlType::Int)),
                build_frozen_list_int(&[1]),
            ),
            (
                CqlType::Set(Box::new(CqlType::Text)),
                build_frozen_list_text(&["a"]),
            ),
            (
                CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int)),
                build_frozen_map_text_int(&[("k", 1)]),
            ),
            (CqlType::Tuple(vec![CqlType::Int]), tuple_data),
            (
                CqlType::Udt(
                    "inner_type".to_string(),
                    vec![("n".to_string(), CqlType::Int)],
                ),
                udt_data,
            ),
            (
                CqlType::Frozen(Box::new(CqlType::Int)),
                1i32.to_be_bytes().to_vec(),
            ),
            (
                CqlType::Custom("org.apache.cassandra.db.marshal.Int32Type".to_string()),
                1i32.to_be_bytes().to_vec(),
            ),
        ];

        for (field_type, data) in cases {
            let value = decode(&field_type, &data);
            assert!(
                !matches!(value, Value::Blob(_)),
                "{field_type:?} decoded to an opaque Value::Blob: {value:?}"
            );
        }
    }

    // ---------------------------------------------------------------------
    // Corruption checks: fixed-width arms are STRICT (`!= N`, not `< N`), so a
    // wrong-length field errors instead of silently decoding from a prefix.
    // ---------------------------------------------------------------------

    #[test]
    fn fixed_width_fields_reject_a_wrong_length() {
        let p = parser();
        for (t, data) in [
            (CqlType::Int, vec![0u8; 5]),
            (CqlType::Int, vec![0u8; 3]),
            (CqlType::BigInt, vec![0u8; 9]),
            (CqlType::Uuid, vec![0u8; 17]),
            (CqlType::TimeUuid, vec![0u8; 15]),
            (CqlType::SmallInt, vec![0u8; 3]),
            (CqlType::TinyInt, vec![0u8; 2]),
            (CqlType::Counter, vec![0u8; 7]),
            (CqlType::Float, vec![0u8; 5]),
            (CqlType::Double, vec![0u8; 7]),
            (CqlType::Boolean, vec![0u8; 2]),
            (CqlType::Date, vec![0u8; 5]),
            (CqlType::Time, vec![0u8; 9]),
            (CqlType::Timestamp, vec![0u8; 9]),
        ] {
            assert!(
                p.parse_udt_field_value(&data, &t, 0).is_err(),
                "{t:?} must reject a {}-byte field",
                data.len()
            );
        }
    }

    #[test]
    fn text_field_rejects_invalid_utf8() {
        let p = parser();
        assert!(p
            .parse_udt_field_value(&[0xFF, 0xFE], &CqlType::Text, 0)
            .is_err());
        assert!(p
            .parse_udt_field_value(&[0xFF, 0xFE], &CqlType::Varchar, 0)
            .is_err());
    }

    #[test]
    fn decimal_field_rejects_a_short_scale() {
        let p = parser();
        assert!(p
            .parse_udt_field_value(&[0, 0, 1], &CqlType::Decimal, 0)
            .is_err());
    }

    #[test]
    fn nesting_depth_is_bounded() {
        let p = parser();
        let err = p
            .parse_udt_field_value(&[0u8; 4], &CqlType::Int, MAX_TYPE_NESTING_DEPTH + 1)
            .expect_err("past the depth bound must error");
        assert!(err.to_string().contains("depth"), "got: {err}");
    }
}
