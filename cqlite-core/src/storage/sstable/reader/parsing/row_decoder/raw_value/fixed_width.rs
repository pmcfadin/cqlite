//! Fixed-width scalar arms of the bounded raw-value decoder (campsite split of
//! `raw_value.rs`, epic #1116 / issue #3723).
//!
//! Every value here is decoded from a slice a caller has already bounded by an
//! explicit `[i32 BE len]` element/field prefix, so `data.len()` IS the value's
//! DECLARED length. The width guard therefore belongs in these arms — never in
//! a call-site framing walk over element lengths (issue #3612 removed exactly
//! that shape: a validator that must know every decoder's framing and is
//! silently wrong the moment one is added).
//!
//! # Authority: the pinned `cassandra-5.0.8` serializers
//!
//! Read at the tag ref, e.g.
//! `git show cassandra-5.0.8:src/java/org/apache/cassandra/serializers/Int32Serializer.java`.
//! Cassandra's `validate()` splits into three families:
//!
//! | serializer (`cassandra-5.0.8`) | `validate()` | Cassandra admits |
//! |---|---|---|
//! | `Int32Serializer` | `size != 4 && !isEmpty` | 4 or 0 |
//! | `LongSerializer`, `CounterSerializer` (extends `LongSerializer`) | `size != 8 && !isEmpty` | 8 or 0 |
//! | `FloatSerializer` | `size != 4 && !isEmpty` | 4 or 0 |
//! | `DoubleSerializer` | `size != 8 && !isEmpty` | 8 or 0 |
//! | `UUIDSerializer` | `size != 16 && !isEmpty` | 16 or 0 |
//! | `TimeUUID.Serializer` (`utils/TimeUUID.java`) | returns on `isEmpty`, then `size != 16` | 16 or 0 |
//! | `TimestampSerializer` | `size != 8 && !isEmpty` | 8 or 0 |
//! | `BooleanSerializer` | `size > 1` | 1 or 0 |
//! | `ShortSerializer` | `size != 2` | 2 ONLY |
//! | `ByteSerializer` | `size != 1` | 1 ONLY |
//! | `SimpleDateSerializer` | `size != 4` | 4 ONLY |
//! | `TimeSerializer` | `size != 8` | 8 ONLY |
//!
//! There is no `TimeUUIDSerializer.java` at this tag; `timeuuid` validates
//! through `TimeUUID.Serializer`, which additionally checks the UUID version —
//! a value check this decoder does not perform (out of scope here).
//!
//! # AC2 decision: a ZERO-length fixed-width element is REFUSED
//!
//! Cassandra's "or 0" half is a legal encoding whose `deserialize` returns Java
//! `null` (e.g. `Int32Serializer.deserialize` — `isEmpty ? null : toInt`). This
//! decoder deliberately does NOT admit it, i.e. the rule applied here is
//! `len == N` for every type in the table (and `len == 1` for `boolean`), which
//! is NARROWER than Cassandra for the six "or 0" types. Reasoning:
//!
//! 1. **Null is carried out-of-band in every framing that reaches this
//!    function, and 0 is not it.** A tuple field spells null `-1`
//!    (`parse_tuple_elements_raw` yields `Value::Null` before dispatching here);
//!    a frozen collection element spells it `-1` too and CQLite refuses that as
//!    a negative length, exactly as `CollectionSerializer.readNonNullValue`
//!    does. So a `len == 0` element is not "the null case" in this position —
//!    it is a zero-length payload for a type that has no zero-length value.
//! 2. **Admitting it would require inventing a value.** There is no `Value`
//!    that means "Cassandra deserialized this element to Java null" inside a
//!    `Value::List`/`Value::Map` here; producing `Value::Null` would be a NEW
//!    behaviour in the LOOSENING direction, covered by no oracle in this repo,
//!    on a function every value read in the crate traverses.
//! 3. **It is already the shipped behaviour.** Every fixed-width arm refused an
//!    empty slice before issue #3723 (`data.len() < N` / `data.is_empty()`), so
//!    keeping the refusal makes this change purely a TIGHTENING: no encoding
//!    that decoded before stops decoding, and no encoding that errored before
//!    starts decoding.
//! 4. It agrees with the single-width top-level table #3612 introduced for
//!    cell-path keys (`int => &[4]`, no `0`).
//!
//! The refusal is loud and named (`Error::FixedWidthLengthMismatch`), never a
//! silently substituted value. **If a real Cassandra-written fixture is ever
//! found carrying a zero-length fixed-width element in this position, the fix
//! is to loosen the six "or 0" arms to yield `Value::Null` — not to reinstate
//! the `< N` guard**, which is what let a wrong length through in the first
//! place.

use super::*;

impl V5CompressedLegacyParser {
    /// The EXACT byte width this decoder admits for a fixed-width CQL short
    /// form in the bounded element/field position, or `None` when `cql_short`
    /// is not a fixed-width type (issue #3723).
    ///
    /// Widths come from the pinned `cassandra-5.0.8` serializer table in this
    /// module's header. The name set here is the closed set
    /// [`Self::decode_fixed_width_raw`] decodes;
    /// `nested_fixed_width_length_tests.rs` pins the two together so
    /// they cannot drift.
    pub(super) fn fixed_width_admissible_width(cql_short: &str) -> Option<usize> {
        let width = match cql_short {
            // Int32Serializer
            "int" => 4,
            // LongSerializer / CounterSerializer
            "bigint" | "counter" => 8,
            // BooleanSerializer
            "boolean" => 1,
            // UUIDSerializer / TimeUUID.Serializer
            "uuid" | "timeuuid" => 16,
            // FloatSerializer
            "float" => 4,
            // DoubleSerializer
            "double" => 8,
            // ShortSerializer (`short` is the marshal alias `ShortType`)
            "smallint" | "short" => 2,
            // ByteSerializer (`byte` is the marshal alias `ByteType`)
            "tinyint" | "byte" => 1,
            // TimestampSerializer
            "timestamp" => 8,
            // SimpleDateSerializer
            "date" => 4,
            // TimeSerializer
            "time" => 8,
            _ => return None,
        };
        Some(width)
    }

    /// Decode one fixed-width scalar from a fully bounded slice.
    ///
    /// `cql_short` is the canonical lowercase CQL short form (already
    /// normalized from any marshal form by
    /// [`Self::primitive_marshal_to_cql_short`]).
    ///
    /// Issue #3723: the declared length must EQUAL the admissible width. The
    /// pre-#3723 guards were `data.len() < N`, so an over-long element read its
    /// first `N` bytes and DISCARDED the remainder silently — which made
    /// `[count=1][len=4][4B]` and `[count=1][len=5][5B]` decode to the same
    /// `frozen<list<int>>` value, collapsing two distinct on-disk cell paths
    /// onto one map key.
    pub(super) fn decode_fixed_width_raw(
        cql_short: &str,
        data: &[u8],
        column_name: &str,
    ) -> Result<Value> {
        let expected = Self::fixed_width_admissible_width(cql_short).ok_or_else(|| {
            // Unreachable by construction: the only caller is the dispatch arm
            // in `raw_value.rs`, whose guard is `admissible_width(..).is_some()`.
            // Reported rather than panicked so a future caller cannot turn a
            // programming slip into an abort.
            Error::internal(format!(
                "decode_fixed_width_raw called with non-fixed-width type '{}' for '{}'",
                cql_short, column_name
            ))
        })?;
        if data.len() != expected {
            return Err(Error::FixedWidthLengthMismatch {
                cql_type: cql_short.to_string(),
                context: column_name.to_string(),
                expected,
                actual: data.len(),
            });
        }

        // Past this point the slice length is EXACTLY `expected`, so every index
        // below is in bounds without a further check.
        match cql_short {
            "int" => Ok(Value::Integer(i32::from_be_bytes([
                data[0], data[1], data[2], data[3],
            ]))),
            "bigint" | "counter" => Ok(Value::BigInt(i64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]))),
            "boolean" => Ok(Value::Boolean(data[0] != 0)),
            "uuid" | "timeuuid" => {
                let uuid: [u8; 16] = data
                    .try_into()
                    .map_err(|_| Error::corruption("UUID byte conversion failed"))?;
                Ok(Value::Uuid(uuid))
            }
            "float" => {
                // CQL `float` is `Value::Float32`, not the f64 `Value::Float`; the column
                // path and both UDT field decoders already agree (roborev round 10 F1).
                let f = f32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok(Value::Float32(f))
            }
            "double" => Ok(Value::Float(f64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]))),
            "smallint" | "short" => Ok(Value::SmallInt(i16::from_be_bytes([data[0], data[1]]))),
            "tinyint" | "byte" => Ok(Value::TinyInt(data[0] as i8)),
            "timestamp" => Ok(Value::Timestamp(i64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]))),
            "date" => {
                let stored = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let days_since_epoch = stored.wrapping_add(i32::MIN as u32) as i32;
                Ok(Value::Date(days_since_epoch))
            }
            "time" => Ok(Value::Time(i64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]))),
            other => Err(Error::internal(format!(
                "decode_fixed_width_raw has an admissible width for '{}' but no decode arm (for '{}')",
                other, column_name
            ))),
        }
    }
}
