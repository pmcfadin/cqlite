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
            CqlType::Udt(name, field_defs) if !field_defs.is_empty() => {
                // Inline definition present: decode it directly, so an empty
                // nested UDT keeps the real keyspace and the shared depth budget
                // (with no bytes, every field comes back null).
                self.parse_inline_udt_value(&[], name, field_defs, depth + 1)
            }
            // NAMED WITHOUT ITS DEFINITION — the registry-backed shape. Routed
            // through the SAME resolver the non-empty path uses
            // (`resolve_named_udt_value`), because these were two separate
            // implementations and this one still produced an empty `Value::Udt`
            // a round after the other was fixed (roborev round 6 on #3722). An
            // all-null UDT with its DECLARED fields is the right answer here, not
            // a UDT with no fields at all.
            CqlType::Udt(name, _) => self.resolve_named_udt_value(&[], name, depth),
            // A zero-length `frozen<T>` whose INNER type reads empty as null is
            // NULL — not `Frozen(Null)`. Propagated here, at the value, rather
            // than special-cased in the field normalizer: `Frozen(Null)` would
            // otherwise survive as `Some(Frozen(Null))` while a `-1` field is
            // `None`, which is the very equality/hash inconsistency #3722's null
            // normalization exists to remove (roborev, at the certified head).
            // Fixing it here also covers collection ELEMENTS, which never reach
            // the field normalizer at all. A non-null inner value keeps its
            // `Frozen(...)` wrapper.
            CqlType::Frozen(inner) => {
                // No wildcard match arm: this function carries
                // `#[deny(clippy::wildcard_enum_match_arm)]` for its `CqlType`
                // totality, and that deny covers every match in it — including one
                // on `Value`. An `if` keeps the null check without adding a
                // wildcard the guard would (correctly) refuse.
                let decoded = self.parse_udt_field_value(&[], inner, depth + 1)?;
                if matches!(decoded, Value::Null) {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Frozen(Box::new(decoded)))
                }
            }

            // `Custom` now means what it says: a type string this reader could not
            // resolve. It no longer carries primitive marshal forms, because
            // `parse_cassandra_type_with_depth` names ShortType/ByteType/
            // VarcharType/DurationType directly (issue #3722), so both SPELLINGS
            // arrive here as the same concrete `CqlType` and there is nothing to
            // re-normalize at this site.
            //
            // An earlier revision DID normalize here, by suffix-matching the
            // marshal name. It was deleted rather than hardened: roborev showed it
            // misclassified a registry UDT named e.g. `udt:ShortType` as a
            // primitive, and the fix for that would have been a namespace check —
            // a rarer delimiter on a channel that should not have been shared.
            // Naming the types at the parser removes the channel instead.
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

    /// The two SPELLINGS must resolve to the SAME `CqlType`, so nothing
    /// downstream can decode them differently.
    ///
    /// This test has been through three shapes, and the history is the point.
    /// roborev found an empty `smallint` field ERRORING under the marshal
    /// spelling while returning `Value::Null` under CQL-short. The first fix
    /// normalized the marshal string inside the empty arm and this test asserted
    /// the two DECODES agreed. roborev then found that normalization
    /// misclassifying a registry UDT named `udt:ShortType`, and separately found
    /// the same divergence on the NON-empty path — the family regenerating one
    /// site over.
    ///
    /// So the property was moved UPSTREAM: `parse_cassandra_type_with_depth` now
    /// names `ShortType`/`ByteType`/`VarcharType`/`DurationType`, and the two
    /// spellings are the same `CqlType` before any decoder runs. That is what
    /// this test asserts now — the equality of the TYPES, not of two decode
    /// results — because with one type there is only one decode and a
    /// decode-comparison would be trivially true.
    #[test]
    fn both_spellings_resolve_to_the_same_cql_type() {
        const M: &str = "org.apache.cassandra.db.marshal.";
        let pairs = [
            (format!("{M}ShortType"), CqlType::SmallInt),
            (format!("{M}ByteType"), CqlType::TinyInt),
            (format!("{M}VarcharType"), CqlType::Text),
            (format!("{M}DurationType"), CqlType::Duration),
            // NOT missing, and asserted so nobody "fixes" it: LexicalUUIDType
            // ends with `UUIDType` and already resolves on that arm.
            (format!("{M}LexicalUUIDType"), CqlType::Uuid),
        ];
        for (marshal, want) in pairs {
            let got = Self_parse_cassandra(&marshal);
            assert_eq!(
                got, want,
                "{marshal} must resolve to {want:?}; a `Custom` here means the two \
                 spellings diverge before any decoder runs, which is the defect \
                 family #3722 closed at the parser"
            );
        }

        // CounterColumnType is DELIBERATELY still `Custom` — a counter cell holds
        // a CounterContext, not a raw i64, and Cassandra refuses `counter` as a
        // UDT field anyway. Pinned so the exclusion is a decision, not a gap.
        let counter = Self_parse_cassandra(&format!("{M}CounterColumnType"));
        assert!(
            matches!(counter, CqlType::Custom(_)),
            "CounterColumnType is deliberately left unresolved here; if that \
             changed, re-read the comment at the arm before accepting it"
        );
    }

    /// And the decode must still be Blob-free for both, which is AC1's property
    /// at the length-0 branch regardless of how the type was spelled.
    #[test]
    fn an_empty_field_of_either_spelling_is_never_a_blob() {
        let p = parser();
        const M: &str = "org.apache.cassandra.db.marshal.";
        for marshal in [
            format!("{M}ShortType"),
            format!("{M}ByteType"),
            format!("{M}DurationType"),
        ] {
            let t = Self_parse_cassandra(&marshal);
            let got = p
                .parse_udt_field_value(&[], &t, 0)
                .unwrap_or_else(|e| panic!("empty {marshal} must decode, not error: {e}"));
            assert!(
                !matches!(got, Value::Blob(_)),
                "empty {marshal} decoded to Value::Blob"
            );
        }
    }

    /// Helper: resolve a marshal string the way a schema-less read does.
    #[allow(non_snake_case)]
    fn Self_parse_cassandra(type_str: &str) -> CqlType {
        V5CompressedLegacyParser::parse_cassandra_type(type_str)
            .unwrap_or_else(|e| panic!("marshal type must parse: {e}"))
    }

    /// roborev round 3: a field length below `-1` in the inline-UDT route used to
    /// reach `field_len as usize` (~1.8e19 for `-5`), after which the bounds
    /// check `current_offset + field_len > data.len()` OVERFLOWS — a panic in a
    /// debug build. Reachable from hostile bytes via the structural nested-UDT
    /// route, so it must be a corruption ERROR and never a panic.
    #[test]
    fn a_negative_inline_udt_field_length_errors_rather_than_panicking() {
        let p = parser();
        let fields = vec![("a".to_string(), CqlType::Int)];
        for bad_len in [-5i32, -2, i32::MIN] {
            let mut data = Vec::new();
            data.extend_from_slice(&bad_len.to_be_bytes());
            let got = p.parse_inline_udt_value(&data, "inner_u", &fields, 0);
            assert!(
                got.is_err(),
                "field length {bad_len} must be a corruption error, got {got:?}"
            );
        }
        // Control: -1 is the LEGAL negative length (null), and must still work —
        // otherwise the guard above would be rejecting valid input.
        let mut null_data = Vec::new();
        null_data.extend_from_slice(&(-1i32).to_be_bytes());
        assert!(
            p.parse_inline_udt_value(&null_data, "inner_u", &fields, 0)
                .is_ok(),
            "-1 is a NULL field and must still decode"
        );
    }
}
