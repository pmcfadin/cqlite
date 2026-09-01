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
//! The list/set/map/tuple arms delegate to `udt_field_collection.rs`, which
//! decodes each element STRUCTURALLY from its `&CqlType` (never from a rendered
//! type string, which would drop a UDT element's inline field defs — the same
//! reason the `Udt` FIELD arm recurses) and requires the field's bytes to be
//! consumed EXACTLY. The byte framing itself is the single implementation in
//! `frozen_framing.rs`, parameterized by an element-decode callback. See that
//! module header for both properties in full.

use super::*;

impl V5CompressedLegacyParser {
    /// Decode ONE UDT field value from the exact bytes of that field.
    ///
    /// `data` is the whole field value: the caller has already consumed the
    /// field's `[i32 BE len]` prefix and bounded the slice. A `-1` length is a
    /// null field and never reaches here; a `0` length arrives as an EMPTY slice
    /// and is decoded by [`Self::empty_udt_field_value`] — see that module for
    /// what Cassandra's serializers make of an empty buffer.
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
        // A ZERO-LENGTH field is a decode in its own right, not a degenerate
        // one: Cassandra permits an empty value for most types and its
        // serializers answer it with null (`udt_field_empty`). Dispatching it
        // HERE, rather than at each call site, is what makes this the only
        // UDT-field decoder for empty fields too — the pre-#3722 call sites
        // answered length 0 from a helper whose fallback was `Value::Blob`.
        if data.is_empty() {
            return self.empty_udt_field_value(field_type, depth);
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
            // `inet` is 4 bytes (IPv4) or 16 (IPv6) and nothing else:
            // `InetAddressSerializer.validate` rejects every other length
            // ("Expected 4 or 16 byte inetaddress"). The pre-#3722 arms accepted
            // ANY non-empty payload, so a malformed address reached
            // `Value::Inet` (roborev round 3, #3722). Checked here rather than
            // left to a consumer, for the same reason the fixed-width arms are
            // strict `!= N`: the field's own length prefix bounds it exactly, so
            // a wrong length is corruption and not something to pass along.
            CqlType::Inet => {
                if data.len() != 4 && data.len() != 16 {
                    return Err(Error::corruption(format!(
                        "Inet field requires 4 bytes (IPv4) or 16 (IPv6), got {}",
                        data.len()
                    )));
                }
                Ok(Value::Inet(
                    crate::storage::sstable::reader::value_borrow::borrow_active(data),
                ))
            }
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
            // Collections/tuple: `udt_field_collection.rs` — structural
            // element decode over the one framing impl (module header).
            // ---------------------------------------------------------------
            CqlType::List(element) => self.parse_udt_field_sequence(data, element, false, depth),
            CqlType::Set(element) => self.parse_udt_field_sequence(data, element, true, depth),
            CqlType::Map(key, value) => self.parse_udt_field_map(data, key, value, depth),
            CqlType::Tuple(element_types) => self.parse_udt_field_tuple(data, element_types, depth),

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
            //
            // `parse_inline_udt_value` is the one nested-inline-UDT decoder: it
            // threads `depth + 1` (so this path is bounded by the SAME
            // `MAX_TYPE_NESTING_DEPTH` budget as every other recursion here) and
            // stamps `self.keyspace` on the resulting `Value::Udt`. The
            // pre-#3722 arm built a throwaway `UdtTypeDef` with an EMPTY
            // keyspace and re-entered at depth 0, so a UDT reached this way both
            // restarted the nesting budget and carried a DIFFERENT public
            // identity (`_keyspace` in the bindings; part of `Udt` equality and
            // hashing, issue #3504) from the same UDT nested directly.
            CqlType::Udt(name, field_defs) if !field_defs.is_empty() => {
                self.parse_inline_udt_value(data, name, field_defs, depth + 1)
            }
            // An EMPTY `field_defs` does not mean "a UDT with no fields" — it
            // means the type was NAMED WITHOUT ITS DEFINITION, which is how a
            // registry-backed UDT arrives. ONE resolver, shared with the
            // zero-length path in `udt_field_empty.rs`: the two arms were
            // separate implementations and roborev found the empty one still
            // producing an empty `Value::Udt` a round after the non-empty one was
            // fixed. Two implementations of one resolution is the drift this
            // issue exists to remove.
            CqlType::Udt(name, _) => self.resolve_named_udt_value(data, name, depth),
            // An UNRESOLVED type string — a marshal class, or a UDT name to look
            // up in the registry. This is the only arm where a string is all we
            // have, and it is the one place a genuinely unknown type may still
            // land on that function's blob fallback.
            CqlType::Custom(type_str) => {
                self.parse_value_from_raw_bytes(data, type_str, "udt field", depth + 1)
            }
        }
    }

    /// Decode a UDT that was NAMED without its field definitions, by resolving
    /// `name` through the `UdtRegistry`.
    ///
    /// Shared by the non-empty arm above and by the zero-length arm in
    /// [`super::udt_field_empty`], so the two cannot answer differently — which
    /// they did, for one review round.
    ///
    /// Falls back to a zero-field inline decode only when no definition exists
    /// anywhere: that is a genuinely unresolvable type, and inventing fields for
    /// it would be worse than reporting what was actually named.
    pub(super) fn resolve_named_udt_value(
        &self,
        data: &[u8],
        name: &str,
        depth: usize,
    ) -> Result<Value> {
        match self
            .udt_registry
            .as_ref()
            .and_then(|r| r.get_udt_qualified(&self.keyspace, name))
        {
            Some(def) => {
                let registry = self
                    .udt_registry
                    .as_ref()
                    .ok_or_else(|| Error::corruption("UDT registry vanished"))?;
                self.parse_nested_udt_from_registry(data, def, registry, depth + 1)
            }
            None => self.parse_inline_udt_value(data, name, &[], depth + 1),
        }
    }

    /// Normalize a decoded field value into `UdtField::value`.
    ///
    /// `UdtField::value` is an `Option<Value>` whose `None` MEANS null, so a
    /// decoded `Value::Null` must collapse to `None`. Otherwise a zero-length null
    /// field (which decodes to `Value::Null`, per Cassandra's serializers) and a
    /// `-1` null field (which is `None`) are TWO representations of the same thing,
    /// and derived `PartialEq`/`Hash` on `UdtValue` treat them as different — even
    /// though the collection comparator considers them equivalent (roborev round 7
    /// on #3722; introduced by this issue's own empty-value fix, which is what made
    /// `Value::Null` reachable as a field value at all).
    pub(super) fn udt_field_value(decoded: Value) -> Option<Value> {
        match decoded {
            Value::Null => None,
            other => Some(other),
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

    /// A UDT field's bytes are EXACTLY its value's bytes: the caller already
    /// consumed the field's `[i32 BE len]` prefix, so anything left over after a
    /// well-formed value is corruption, not a value to accept.
    ///
    /// This is the variable-length counterpart of [`Self::require_len`]'s strict
    /// `!= N`, and the consistency is deliberate: ONE rule for "the field's bytes
    /// are exactly this value's bytes", whatever the type.
    pub(super) fn require_full_consumption(
        consumed: usize,
        field_len: usize,
        type_name: &str,
    ) -> Result<()> {
        if consumed != field_len {
            return Err(Error::corruption(format!(
                "{} field: consumed {} of {} bytes; trailing bytes in a length-bounded UDT field are corruption",
                type_name, consumed, field_len
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

        // Same exact-consumption rule as the collection arms and the strict
        // fixed-width `!= N` checks (see `require_full_consumption`).
        Self::require_full_consumption(pos, data.len(), "Duration")?;

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
