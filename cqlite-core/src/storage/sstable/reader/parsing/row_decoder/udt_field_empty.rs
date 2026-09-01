//! The ZERO-LENGTH arm of THE UDT-field decoder (issue #3722).
//!
//! A field whose `[i32 BE len]` prefix is `0` used to bypass the consolidated
//! decoder entirely: both UDT readers called a `create_empty_value_for_type`
//! helper whose match had a `_ => Value::blob(Vec::new())` fallback, so an empty
//! `varint`, `decimal`, `time`, `inet`, `tuple` or nested `udt` field decoded to
//! an opaque `Value::Blob`. That falsified this issue's headline property — no
//! `Value::Blob` for a field whose declared type is not `blob` — for every empty
//! value. The helper is gone; [`super::udt_field`] dispatches here instead, so
//! there is ONE decoder for a UDT field whatever its length.
//!
//! # What an empty value MEANS (authority: pinned Cassandra 5.0.8 source)
//!
//! An empty (0-length) value is legal for most CQL types — `Int32Serializer`
//! `validate` accepts "4 or 0 byte int" — and Cassandra's serializers answer it
//! with **null** for everything except the three genuinely variable-width types.
//! Read with
//! `git show cassandra-5.0.8:src/java/org/apache/cassandra/serializers/<F>.java`:
//!
//! * `Int32Serializer.java:30` — `deserialize` is
//!   `accessor.isEmpty(value) ? null : accessor.toInt(value)`. `Boolean`(32),
//!   `Byte`(30), `Short`(30), `Long`(30), `Float`(30), `Double`(30), `UUID`(31),
//!   `Timestamp`(137), `SimpleDate`(50), `Time`(32) and `Duration`(61) are the
//!   same shape. `TimeUUIDSerializer` is `UUIDSerializer`.
//! * **NOT only the fixed-width types**, which is the part worth writing down:
//!   `IntegerSerializer.java:33` (varint), `DecimalSerializer.java:33` and
//!   `InetAddressSerializer.java:34` also return `null` for an empty buffer. An
//!   empty `varint` is therefore NOT "the empty-bytes varint" and not `0`.
//!   `db/marshal/IntegerType.java:90`, `DecimalType.java:75` and
//!   `InetAddressType.java:51` each override `isEmptyValueMeaningless()` to
//!   `true`, which `AbstractType.java:789` turns into `null`.
//! * `BytesSerializer.java:36` returns the buffer itself and
//!   `AbstractTextSerializer` decodes it to the empty string, so `blob`, `text`,
//!   `ascii` and `varchar` are the ONLY types with a genuine empty instance.
//!
//! Composite types split rather than deserialize: `TupleType.split`
//! (`db/marshal/TupleType.java:203`) walks `while (!isEmptyFromOffset(...))`, so
//! an empty buffer yields NO components and the remaining ones "remain null"
//! (its own comment, line 231) — an all-null tuple, and an all-null UDT via
//! `UserType extends TupleType`.

use super::*;

impl V5CompressedLegacyParser {
    /// The value of a UDT field (or collection element) whose payload is ZERO
    /// bytes. Total over `CqlType` with NO wildcard arm, pinned by
    /// `#[deny(clippy::wildcard_enum_match_arm)]` for the same reason
    /// [`V5CompressedLegacyParser::parse_udt_field_value`] is: a new `CqlType`
    /// variant must be a compile error here, not a silent blob.
    ///
    /// Per-type semantics and their Cassandra sources are in the module header.
    #[deny(clippy::wildcard_enum_match_arm)]
    pub(super) fn empty_udt_field_value(
        &self,
        field_type: &CqlType,
        depth: usize,
    ) -> Result<Value> {
        match field_type {
            // The three types with a genuine empty instance.
            CqlType::Text | CqlType::Ascii | CqlType::Varchar => Ok(Value::text(String::new())),
            CqlType::Blob => Ok(Value::blob(Vec::new())),

            // Empty deserializes to NULL — fixed-width and self-delimiting alike
            // (see the module header for the per-serializer line references).
            CqlType::Boolean
            | CqlType::TinyInt
            | CqlType::SmallInt
            | CqlType::Int
            | CqlType::BigInt
            | CqlType::Counter
            | CqlType::Float
            | CqlType::Double
            | CqlType::Uuid
            | CqlType::TimeUuid
            | CqlType::Timestamp
            | CqlType::Date
            | CqlType::Time
            | CqlType::Duration
            | CqlType::Varint
            | CqlType::Decimal
            | CqlType::Inet => Ok(Value::Null),

            // Cassandra does not WRITE a zero-length collection — a frozen empty
            // collection is the 4-byte count `0`, and `ListSerializer.validate`
            // (`serializers/ListSerializer.java:74`) throws "Not enough bytes to
            // read a list" on an empty buffer (`Set`:77, `Map`:88 likewise). So
            // this shape is unreachable from Cassandra-written bytes, and the
            // pre-#3722 lenient reading — the empty collection — is KEPT rather
            // than converted into an error: turning a value that used to read
            // into a failed row is a behaviour change this fix does not need.
            CqlType::List(_) => Ok(Value::List(Vec::new())),
            CqlType::Set(_) => Ok(Value::Set(Vec::new())),
            CqlType::Map(_, _) => Ok(Value::Map(Vec::new())),

            // Composites: zero components, the rest null (`TupleType.split`).
            CqlType::Tuple(element_types) => {
                Ok(Value::Tuple(vec![Value::Null; element_types.len()]))
            }
            CqlType::Udt(name, field_defs) => {
                // Same route as the non-empty `Udt` arm, so an empty nested UDT
                // keeps the real keyspace and the shared depth budget: with no
                // bytes, every field comes back null.
                self.parse_inline_udt_value(&[], name, field_defs, depth + 1)
            }
            CqlType::Frozen(inner) => Ok(Value::Frozen(Box::new(self.parse_udt_field_value(
                &[],
                inner,
                depth + 1,
            )?))),

            // An UNRESOLVED type string is the one arm that cannot say what an
            // empty value of it IS without resolving it, so it keeps the SAME
            // single route a non-empty payload takes (module header of
            // `udt_field`): a `blob` fallback there is the documented carve-out
            // for an unknown type, not a decoded non-`blob` type turning into a
            // blob.
            CqlType::Custom(type_str) => {
                self.parse_value_from_raw_bytes(&[], type_str, "udt field", depth + 1)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parser() -> V5CompressedLegacyParser {
        V5CompressedLegacyParser::new("test_ks".to_string(), "test_table".to_string(), 0, 0, None)
    }

    /// Every non-`blob` type — arbitrary-length ones included — must decode an
    /// empty payload to something that is NOT a `Value::Blob`. This is issue
    /// #3722's headline property at the length-0 branch that used to bypass the
    /// decoder.
    #[test]
    fn no_non_blob_type_decodes_an_empty_field_to_a_blob() {
        let p = parser();
        let types = vec![
            CqlType::Text,
            CqlType::Ascii,
            CqlType::Varchar,
            CqlType::Boolean,
            CqlType::TinyInt,
            CqlType::SmallInt,
            CqlType::Int,
            CqlType::BigInt,
            CqlType::Counter,
            CqlType::Float,
            CqlType::Double,
            CqlType::Uuid,
            CqlType::TimeUuid,
            CqlType::Timestamp,
            CqlType::Date,
            CqlType::Time,
            CqlType::Duration,
            CqlType::Varint,
            CqlType::Decimal,
            CqlType::Inet,
            CqlType::List(Box::new(CqlType::Int)),
            CqlType::Set(Box::new(CqlType::Text)),
            CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Int)),
            CqlType::Tuple(vec![CqlType::Int, CqlType::Text]),
            CqlType::Udt("inner_u".to_string(), vec![("a".to_string(), CqlType::Int)]),
            CqlType::Frozen(Box::new(CqlType::Int)),
        ];
        for t in types {
            let value = p
                .parse_udt_field_value(&[], &t, 0)
                .unwrap_or_else(|e| panic!("empty {t:?} field must decode: {e}"));
            assert!(
                !matches!(value, Value::Blob(_)),
                "empty {t:?} field decoded to a Blob: {value:?}"
            );
            if let Value::Frozen(inner) = &value {
                assert!(
                    !matches!(**inner, Value::Blob(_)),
                    "empty frozen<int> field decoded to a Frozen(Blob)"
                );
            }
        }
    }

    /// The three types WITH a genuine empty instance keep it; everything else
    /// fixed-width or self-delimiting is null (Cassandra serializer semantics,
    /// module header).
    #[test]
    fn empty_values_follow_the_cassandra_serializers() {
        let p = parser();
        for t in [CqlType::Text, CqlType::Ascii, CqlType::Varchar] {
            assert_eq!(
                p.parse_udt_field_value(&[], &t, 0).expect("text-ish"),
                Value::text(""),
                "{t:?}"
            );
        }
        assert_eq!(
            p.parse_udt_field_value(&[], &CqlType::Blob, 0)
                .expect("blob"),
            Value::blob(Vec::new())
        );
        for t in [
            CqlType::Int,
            CqlType::BigInt,
            CqlType::Boolean,
            CqlType::Uuid,
            CqlType::Timestamp,
            CqlType::Date,
            CqlType::Time,
            CqlType::Duration,
            // Arbitrary-length, and STILL null: IntegerSerializer:33,
            // DecimalSerializer:33, InetAddressSerializer:34.
            CqlType::Varint,
            CqlType::Decimal,
            CqlType::Inet,
        ] {
            assert_eq!(
                p.parse_udt_field_value(&[], &t, 0).expect("empty scalar"),
                Value::Null,
                "{t:?}"
            );
        }
    }

    /// An empty composite has the right SHAPE: a tuple keeps one null per
    /// declared element, a UDT one null per declared field (`TupleType.split`).
    #[test]
    fn empty_composites_are_all_null_with_the_declared_arity() {
        let p = parser();
        assert_eq!(
            p.parse_udt_field_value(&[], &CqlType::Tuple(vec![CqlType::Int, CqlType::Text]), 0)
                .expect("tuple"),
            Value::Tuple(vec![Value::Null, Value::Null])
        );
        let udt_type = CqlType::Udt(
            "inner_u".to_string(),
            vec![
                ("a".to_string(), CqlType::Int),
                ("b".to_string(), CqlType::Text),
            ],
        );
        match p
            .parse_udt_field_value(&[], &udt_type, 0)
            .expect("empty udt")
        {
            Value::Udt(udt) => {
                assert_eq!(udt.type_name, "inner_u");
                assert_eq!(udt.keyspace, "test_ks");
                assert_eq!(udt.fields.len(), 2);
                assert!(udt.fields.iter().all(|f| f.value.is_none()));
            }
            other => panic!("expected a Udt, got {other:?}"),
        }
    }
}
