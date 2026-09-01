//! The SINGLE UDT-field value decoder (issue #3722).
//!
//! Placeholder header for the RED step: this file currently holds the OLD
//! `parse_udt_field_value` arms verbatim, moved out of `udt.rs`.

use super::*;

impl V5CompressedLegacyParser {
    /// Parse a UDT field value based on its CqlType.
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
            CqlType::Text | CqlType::Ascii => {
                std::str::from_utf8(data)
                    .map_err(|e| Error::corruption(format!("Invalid UTF-8 in UDT field: {}", e)))?;
                Ok(Value::Text(
                    crate::storage::sstable::reader::value_borrow::borrow_active(data),
                ))
            }
            CqlType::Int => {
                if data.len() != 4 {
                    return Err(Error::corruption(format!(
                        "Int field requires 4 bytes, got {}",
                        data.len()
                    )));
                }
                let v = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(Value::Integer(v))
            }
            CqlType::BigInt => {
                if data.len() != 8 {
                    return Err(Error::corruption(format!(
                        "BigInt field requires 8 bytes, got {}",
                        data.len()
                    )));
                }
                let v = i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok(Value::BigInt(v))
            }
            CqlType::Float => {
                if data.len() != 4 {
                    return Err(Error::corruption(format!(
                        "Float field requires 4 bytes, got {}",
                        data.len()
                    )));
                }
                let bits = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(Value::Float32(f32::from_bits(bits)))
            }
            CqlType::Double => {
                if data.len() != 8 {
                    return Err(Error::corruption(format!(
                        "Double field requires 8 bytes, got {}",
                        data.len()
                    )));
                }
                let bits = u64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok(Value::Float(f64::from_bits(bits)))
            }
            CqlType::Boolean => {
                if data.len() != 1 {
                    return Err(Error::corruption(format!(
                        "Boolean field requires 1 byte, got {}",
                        data.len()
                    )));
                }
                Ok(Value::Boolean(data[0] != 0))
            }
            CqlType::Uuid => {
                if data.len() != 16 {
                    return Err(Error::corruption(format!(
                        "UUID field requires 16 bytes, got {}",
                        data.len()
                    )));
                }
                let uuid_bytes: [u8; 16] = data[0..16]
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;
                Ok(Value::Uuid(uuid_bytes))
            }
            CqlType::Timestamp => {
                if data.len() != 8 {
                    return Err(Error::corruption(format!(
                        "Timestamp field requires 8 bytes, got {}",
                        data.len()
                    )));
                }
                let millis = i64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok(Value::Timestamp(millis))
            }
            CqlType::Date => {
                if data.len() != 4 {
                    return Err(Error::corruption(format!(
                        "Date field requires 4 bytes, got {}",
                        data.len()
                    )));
                }
                let days = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(Value::Date(days as i32))
            }
            CqlType::Blob => Ok(Value::Blob(
                crate::storage::sstable::reader::value_borrow::borrow_active(data),
            )),
            CqlType::Inet => Ok(Value::Inet(
                crate::storage::sstable::reader::value_borrow::borrow_active(data),
            )),
            CqlType::Frozen(inner) => {
                let inner_value = self.parse_udt_field_value(data, inner, depth + 1)?;
                Ok(Value::Frozen(Box::new(inner_value)))
            }
            CqlType::Udt(name, field_defs) => {
                let mut nested_def = UdtTypeDef::new("".to_string(), name.clone());
                for (field_name, field_type) in field_defs {
                    nested_def =
                        nested_def.with_field(field_name.clone(), field_type.clone(), true);
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
            _ => Ok(Value::Blob(
                crate::storage::sstable::reader::value_borrow::borrow_active(data),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::test_support::helpers::*;

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
        assert_eq!(decode(&CqlType::Int, &7i32.to_be_bytes()), Value::Integer(7));
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
            decode(&CqlType::Frozen(Box::new(CqlType::Int)), &3i32.to_be_bytes()),
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
            (CqlType::List(Box::new(CqlType::Int)), build_frozen_list_int(&[1])),
            (CqlType::Set(Box::new(CqlType::Text)), build_frozen_list_text(&["a"])),
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
