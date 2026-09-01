//! Unit tests for THE single UDT-field value decoder (issue #3722).
//!
//! A sibling module rather than an inline `mod tests`, because `udt_field.rs`
//! reached 823 lines against the 800-line campsite source target and the gate's
//! `file-size` ratchet correctly FAILed. Splitting the tests out is the cheaper
//! and more honest half of that split: the decoder itself is one `match` that
//! reads best whole, while these cases are independent of each other.
//! Convention follows `decoder_lockstep_tests.rs` / `partition_shadow_tests.rs`.
//!
//! The TOTALITY guard that stops the arm sets diverging again lives on the
//! decoder in `udt_field.rs`, NOT here: it is `#[deny(clippy::
//! wildcard_enum_match_arm)]`, a compile-time property, and these cases are
//! corroboration rather than the mechanism.

#[cfg(test)]
mod tests {

    // `super` is THIS file's module, so the decoder's own scope — everything
    // `udt_field.rs` sees via its `use super::*` — is one level further out.
    use super::super::test_support::helpers::*;
    use super::super::*;

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

    /// roborev round 3: `inet` is 4 bytes (IPv4) or 16 (IPv6) and nothing else.
    /// The pre-fix arms accepted ANY non-empty payload, so a malformed address
    /// reached `Value::Inet`. Both directions asserted — a guard that rejects
    /// VALID lengths would be worse than the leniency it replaced.
    #[test]
    fn inet_accepts_only_4_or_16_bytes() {
        let p = parser();
        assert!(
            matches!(
                p.parse_udt_field_value(&[192, 168, 1, 42], &CqlType::Inet, 0),
                Ok(Value::Inet(_))
            ),
            "a 4-byte IPv4 address must decode"
        );
        assert!(
            matches!(
                p.parse_udt_field_value(&[0u8; 16], &CqlType::Inet, 0),
                Ok(Value::Inet(_))
            ),
            "a 16-byte IPv6 address must decode"
        );
        for bad in [1usize, 3, 5, 8, 15, 17] {
            let data = vec![0u8; bad];
            assert!(
                p.parse_udt_field_value(&data, &CqlType::Inet, 0).is_err(),
                "a {bad}-byte inet payload must be rejected"
            );
        }
    }

    /// roborev round 4: `parse_nested_udt_from_registry` had NO depth parameter,
    /// so its recursive sites bypassed `MAX_TYPE_NESTING_DEPTH` and a chain of
    /// registry-resolved UDTs recursed to stack exhaustion. Reachable from a
    /// schema-less read of hostile bytes, since the chain comes from the marshal
    /// header.
    ///
    /// Asserted at the boundary rather than by building a deep chain: calling it
    /// AT the budget must work and one PAST it must error, which is what proves
    /// the guard is the thing firing rather than nested registry UDTs failing
    /// generally.
    #[test]
    fn registry_nested_udt_recursion_is_bounded() {
        use crate::schema::UdtRegistry;
        let p = parser();
        let registry = UdtRegistry::new();
        let mut def = crate::types::UdtTypeDef::new("test_ks".to_string(), "deep".to_string());
        def = def.with_field("a".to_string(), CqlType::Int, true);
        // One i32 field length of -1 (null) is a complete, valid payload.
        let data = (-1i32).to_be_bytes().to_vec();

        let at_budget =
            p.parse_nested_udt_from_registry(&data, &def, &registry, MAX_TYPE_NESTING_DEPTH);
        assert!(
            at_budget.is_ok(),
            "AT the depth budget must still decode, else the guard rejects valid \
             input: {at_budget:?}"
        );

        let past_budget =
            p.parse_nested_udt_from_registry(&data, &def, &registry, MAX_TYPE_NESTING_DEPTH + 1);
        assert!(
            past_budget.is_err(),
            "one PAST the depth budget must be a corruption error, not unbounded \
             recursion: {past_budget:?}"
        );
    }

    /// THE BEHAVIOURAL DEPTH GUARD — a real COLLECTION-MEDIATED chain.
    ///
    /// The depth family produced a roborev finding in THREE consecutive rounds
    /// (round 2: the inline-UDT arm reset to 0; round 4:
    /// `parse_nested_udt_from_registry` had no depth parameter; round 5: its field
    /// decodes still passed a literal 0). Per-site review stopped being the guard,
    /// so this builds a chain deeper than `MAX_TYPE_NESTING_DEPTH` and requires
    /// the decode to ERROR.
    ///
    /// THE NESTING GOES THROUGH A COLLECTION AT EVERY LEVEL, and that is the whole
    /// point. A first version chained UDT -> UDT directly and PASSED even with the
    /// defect reintroduced, because that path recurses through
    /// `parse_nested_udt_from_registry`'s own already-correct `depth + 1`. Only the
    /// collection-mediated shape roborev actually named —
    /// `UDT -> frozen<list<frozen<UDT>>> -> ...` — routes through the field
    /// decoder whose depth argument was the defect. A test that passes before and
    /// after proves nothing, so the shape is load-bearing, not incidental.
    #[test]
    fn a_collection_mediated_udt_chain_deeper_than_the_budget_errors() {
        use crate::schema::UdtRegistry;

        const CHAIN: usize = MAX_TYPE_NESTING_DEPTH * 3;
        let ks = "test_ks";

        let mut registry = UdtRegistry::new();
        for i in 0..CHAIN {
            // deep_i.f : frozen<list<frozen<deep_{i+1}>>>, the LAST one an int so
            // the chain terminates if the budget somehow never fires.
            let field_type = if i + 1 < CHAIN {
                CqlType::List(Box::new(CqlType::Udt(format!("deep{}", i + 1), Vec::new())))
            } else {
                CqlType::Int
            };
            registry.register_udt(
                crate::types::UdtTypeDef::new(ks.to_string(), format!("deep{i}")).with_field(
                    "f".to_string(),
                    field_type,
                    true,
                ),
            );
        }

        // Build the payload BOTTOM-UP so every level is well-formed:
        //   a UDT value  = [i32 field_len][field bytes]
        //   a frozen list = [i32 count=1][i32 elem_len][elem bytes]
        let mut payload = 7i32.to_be_bytes().to_vec(); // deep_{CHAIN-1}.f : int
        for _ in 0..CHAIN.saturating_sub(1) {
            let udt = {
                let mut v = (payload.len() as i32).to_be_bytes().to_vec();
                v.extend_from_slice(&payload);
                v
            };
            let mut list = 1i32.to_be_bytes().to_vec();
            list.extend_from_slice(&(udt.len() as i32).to_be_bytes());
            list.extend_from_slice(&udt);
            payload = list;
        }

        let parser = V5CompressedLegacyParser::new(ks.to_string(), "t".to_string(), 0, 0, None)
            .with_udt_registry(registry.clone());
        let root = registry
            .get_udt_qualified(ks, "deep0")
            .expect("deep0 was just registered");

        let mut root_bytes = (payload.len() as i32).to_be_bytes().to_vec();
        root_bytes.extend_from_slice(&payload);

        let got = parser.parse_nested_udt_from_registry(&root_bytes, root, &registry, 0);
        assert!(
            got.is_err(),
            "a {CHAIN}-deep collection-mediated UDT chain must be refused by \
             MAX_TYPE_NESTING_DEPTH ({MAX_TYPE_NESTING_DEPTH}); Ok means some call on \
             the path is restarting the budget — the defect three review rounds kept \
             finding: {got:?}"
        );
    }

    /// The depth guard over the routes the collection chain does NOT traverse:
    /// the ZERO-LENGTH field path and the FROZEN-inline path.
    ///
    /// roborev round 6's exact criticism of the chain test above was that it
    /// "only exercises depth zero and does not detect this" — the literal `0`/`1`
    /// depths on the empty-field and frozen-inline routes. So each route is driven
    /// AT the budget and one PAST it: at the budget must decode, past must error.
    /// That is what makes the failure attributable to the guard rather than to the
    /// route being broken generally.
    #[test]
    fn the_empty_and_frozen_routes_are_depth_bounded_too() {
        let p = parser();
        let inner = CqlType::Udt("inner_u".to_string(), vec![("a".to_string(), CqlType::Int)]);

        // Route 1: a ZERO-LENGTH field of a nested-UDT type.
        for (depth, want_ok) in [
            (MAX_TYPE_NESTING_DEPTH - 1, true),
            (MAX_TYPE_NESTING_DEPTH + 1, false),
        ] {
            let got = p.parse_udt_field_value(&[], &inner, depth);
            assert_eq!(
                got.is_ok(),
                want_ok,
                "empty nested-UDT field at depth {depth} (budget {MAX_TYPE_NESTING_DEPTH}) \
                 expected ok={want_ok}, got {got:?}"
            );
        }

        // Route 2: the FROZEN-inline path, which wraps and recurses.
        let frozen = CqlType::Frozen(Box::new(inner.clone()));
        for (depth, want_ok) in [
            (MAX_TYPE_NESTING_DEPTH - 2, true),
            (MAX_TYPE_NESTING_DEPTH + 1, false),
        ] {
            let data = (-1i32).to_be_bytes().to_vec();
            let got = p.parse_udt_field_value(&data, &frozen, depth);
            assert_eq!(
                got.is_ok(),
                want_ok,
                "frozen<inner_u> at depth {depth} (budget {MAX_TYPE_NESTING_DEPTH}) \
                 expected ok={want_ok}, got {got:?}"
            );
        }
    }

    /// roborev round 7: a ZERO-LENGTH null field and a `-1` null field must be ONE
    /// representation, because `UdtField::value`'s `None` MEANS null.
    ///
    /// This issue's own empty-value fix made `Value::Null` reachable as a decoded
    /// field value, and wrapping it in `Some` created a second spelling of null:
    /// derived `PartialEq`/`Hash` on `UdtValue` then told the two apart, while the
    /// collection comparator treated them as equal. Asserted on the NORMALIZER so
    /// the property is pinned wherever a field is constructed.
    #[test]
    fn a_decoded_null_collapses_to_the_none_spelling_of_null() {
        assert_eq!(
            V5CompressedLegacyParser::udt_field_value(Value::Null),
            None,
            "a decoded Value::Null must become UdtField::value = None, or a \
             zero-length null and a -1 null hash differently"
        );
        // Control: a real value must survive untouched.
        assert_eq!(
            V5CompressedLegacyParser::udt_field_value(Value::Integer(7)),
            Some(Value::Integer(7)),
            "a non-null value must pass through unchanged"
        );
        assert_eq!(
            V5CompressedLegacyParser::udt_field_value(Value::text(String::new())),
            Some(Value::text(String::new())),
            "an EMPTY STRING is a value, not a null — text is one of the four types \
             with a genuine empty instance"
        );
    }
}
