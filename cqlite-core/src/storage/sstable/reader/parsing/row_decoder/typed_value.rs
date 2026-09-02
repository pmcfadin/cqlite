//! Decode a bounded byte slice whose type is a **declared `CqlType`** (issue #3631),
//! and own the ONE implementation of the bounded-decode exhaustion rule.
//!
//! ## Why this module exists
//!
//! Two decode paths used to match a CLOSED SET of primitive types and fall back to
//! `Value::Blob` for everything else, while the schema naming the real type was in
//! hand and unread:
//!
//! * `parse_simple_udt_field_value` (`udt.rs`) — a COLLECTION-typed field of a
//!   frozen UDT (`frozen<map<text,int>>`, `frozen<list<…>>`, `frozen<set<…>>`)
//!   surfaced to callers as bytes. Instance B of #3631.
//! * `parse_cell_path_key` (`cell_path_key.rs`) — a non-frozen map's cell-path key.
//!   Instance A, fixed THERE by #3612 / PR #3736, not here.
//!
//! Issue #28 (no-heuristics) forbids that silent degradation: authoritative
//! metadata only, and where the metadata is present it must be USED. A
//! `tracing::debug!` is not a diagnostic a caller can see, so a type this decoder
//! genuinely cannot express is an explicit `Error` naming it.
//!
//! ## Format authority (never CQLite's own prior output)
//!
//! Every structured shape below is the **frozen / "multi-cell-free"** serialization
//! Cassandra writes for a value nested inside another value, read at the pinned tag:
//!
//! * Collections — `cassandra-5.0.8:src/java/org/apache/cassandra/serializers/CollectionSerializer.java`.
//!   `writeCollectionSize` is `output.putInt(elements)` (a 4-byte BE i32 count, NOT a
//!   vint), and `writeValue` writes `putInt(size)` then the bytes, with `putInt(-1)`
//!   for a null element; `readValue` treats any negative size as null. A `list`/`set`
//!   is `[i32 count]` then that many values; a `map` is `[i32 count]` then that many
//!   KEY, VALUE pairs, each independently length-prefixed.
//! * Tuples — `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/TupleType.java`
//!   (`buildValue` / `split`): each component is `[i32 size][bytes]`, `-1` for null,
//!   and a tuple may be written with FEWER components than the type declares
//!   (trailing components are then absent, i.e. null).
//! * UDTs — `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/UserType.java`,
//!   which extends `TupleType`: identical per-field `[i32 size][bytes]` framing. That
//!   is already implemented by `parse_nested_udt_from_registry` /
//!   `parse_inline_udt_value`, which this module delegates to so there is ONE UDT
//!   field-framing implementation.
//!
//! Cross-checked against the committed Cassandra-5.0.2-written fixture
//! `test-data/fixtures/issue_3504/` and its `sstabledump` golden, where
//! `udt_hashable_shapes` row 3's `stn` renders the nested `frozen<map<text,int>>`
//! field as `{"a": 1}` from the on-disk bytes
//! `00000001 00000001 61 00000004 00000001`.

use super::*;

// Issue #3631: the blob-fallback arm's replacement, at the DECODER's own level.
// Covers acceptance criteria 4 and 5 with bytes derived from cassandra-5.0.8 source;
// the fixture-backed parity oracle is
// `cqlite-core/tests/issue_3631_structured_values_not_blobs.rs`.
#[cfg(test)]
#[path = "regression_3631_typed_value_tests.rs"]
mod regression_3631_typed_value_tests;

// Issue #3631: the per-scalar width / empty-means-null / short-form tables, split
// out under the campsite rule (epic #1116).
mod scalar_rules;

impl V5CompressedLegacyParser {
    /// Assert a bounded decode consumed its entire slice.
    ///
    /// # THE one implementation of this rule (issues #3811 + #3820/#3631)
    /// #3811 landed `require_fully_consumed_raw` beside the type-STRING decoder and
    /// recorded, in its own doc comment, that #3820 was adding a second copy over
    /// `&CqlType` and that *"there must be ONE implementation of this rule, not two"*.
    /// This is that one implementation: `require_fully_consumed_raw` is GONE and all of
    /// its call sites — `raw_value.rs`'s bounded wrapper, `cell_value_complex.rs`'s two
    /// frozen-UDT columns, `udt.rs`'s nested-UDT arms and `udt/inline.rs` — name this
    /// function, with the SAME arguments and the SAME message. The type-string and
    /// `CqlType` sides therefore share one error class, which is what the #3811 note
    /// asked for: a caller matching on the message must not have to know which layer
    /// refused.
    ///
    /// `consumed` is what the decoder reports it read; `len` is the exact extent the
    /// caller handed it. Anything short is `cassandra-5.0.8` `TupleType.split` rule 2
    /// or rule 4 — a partial component-length prefix, or trailing bytes after the last
    /// declared component — and Cassandra throws `MarshalException` for both. A
    /// genuinely SHORT encoding (rule 1, omitted trailing components) leaves
    /// `consumed == len` and stays ACCEPTED.
    ///
    /// Discarding the leftover bytes silently is the framing-error-MASKING class that
    /// let #3002's `Rows.db` root-base defect hide behind a compensating encoder
    /// defect: two errors that cancel are undetectable unless something insists the
    /// accounting balances.
    ///
    /// `subject` names the thing being decoded (a column, a field, a nested type) and
    /// `type_desc` its declared type, both for the message only.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn require_fully_consumed(
        consumed: usize,
        len: usize,
        subject: &str,
        type_desc: &str,
    ) -> Result<()> {
        if consumed == len {
            return Ok(());
        }
        if consumed < len {
            // Wording deliberately SHARED with `cell_path_key.rs`'s consumption
            // refusal ("decoded only N of M byte(s)"): it is the same rule, and a
            // caller matching on the message must not have to know which of the
            // layers refused.
            return Err(Error::corruption(format!(
                "Bounded value '{}' of type '{}' decoded only {} of {} byte(s); the whole \
             slice must be the value (trailing bytes, or a partial trailing component \
             header, are corruption — Cassandra TupleType.split rules 2 and 4)",
                subject, type_desc, consumed, len
            )));
        }
        Err(Error::corruption(format!(
        "Bounded value '{}' (type '{}'): decoder reported {} bytes consumed but only {} were available",
        subject, type_desc, consumed, len
    )))
    }
}

/// The number of leading bytes an `[i32 BE]` length or count occupies —
/// `TypeSizes.INT_SIZE` in `CollectionSerializer`.
const I32_LEN: usize = 4;

impl V5CompressedLegacyParser {
    /// Parse a UDT field value from its declared `CqlType`, carrying an explicit
    /// nesting `depth`.
    ///
    /// # THE one per-field entry point for every UDT field loop (issue #3631)
    /// All the field loops route here — `udt.rs`'s `parse_udt_value`, its
    /// registry-resolved decoder and `udt/inline.rs`'s inline decoder, plus
    /// `raw_type_value.rs`'s two marshal-string loops. Each used to carry its own
    /// ~100-line dispatch over the field's `CqlType`, and three consecutive review
    /// rounds on this issue each found one more of those copies with one of the same
    /// three defects (a reset nesting counter, a discarded consumed-byte count, a
    /// `Value::Blob` fallback for an unresolved UDT name). One implementation is what
    /// makes the three properties hold BY CONSTRUCTION instead of per call site.
    ///
    /// # There is deliberately NO zero-depth wrapper
    /// A `depth = 0` overload is the thing a caller inside a decode picks by accident,
    /// and that is precisely how the counter came to be reset at every frozen hop. An
    /// entry point genuinely at the root — a test, or a column-level decode — writes the
    /// `0` at the call site, where a reviewer can see it.
    ///
    /// The scalar arms below are kept BYTE-FOR-BYTE as they were before #3631 (they
    /// encode measured, shipped behaviour — e.g. `CqlType::Float` surfaces
    /// `Value::Float32`, which the type-string decoder spells as a widened
    /// `Value::Float`; unifying the two is a separate change). Each validates its exact
    /// width, so each consumes the whole slice. What #3631 replaced is the trailing
    /// `_ =>` arm, which used to hand back `Value::Blob` for EVERY remaining type —
    /// including a `frozen<map<text,int>>` field whose declared type was in hand and
    /// unread. That silent degradation is what #28 forbids, and it made a value HASHABLE
    /// in the Python binding that should not have been (#3500).
    ///
    /// Depth is checked on entry and threaded outward: a field reached from INSIDE a
    /// collection, tuple or another UDT must not restart the counter, because the UDT
    /// decoders recurse back through here — so a reset at any UDT boundary makes
    /// `MAX_TYPE_NESTING_DEPTH` bound nothing across alternating collection/UDT layers,
    /// and a cyclic `UdtRegistry` (which the registry type permits even though CQL does
    /// not) then recurses until the stack is exhausted.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn parse_simple_udt_field_value_at(
        &self,
        data: &[u8],
        field_type: &CqlType,
        depth: usize,
    ) -> Result<Value> {
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::corruption(format!(
                "UDT field: type nesting depth {depth} exceeds maximum {MAX_TYPE_NESTING_DEPTH}"
            )));
        }
        // An EMPTY field (`[i32 0]`, i.e. `ByteBufferUtil.EMPTY_BYTE_BUFFER`) is decided
        // by ONE rule, in the typed decoder — see `empty_is_a_value`, which carries the
        // pinned-tag citations. The arms below each require their exact width, so
        // without this redirect an empty `int` field ERRORED here while Cassandra reads
        // it as null. Text, blob and the collections keep their own empty semantics,
        // which the typed decoder spells identically (`Text("")`, empty blob, empty
        // collection).
        if data.is_empty() {
            return self.parse_typed_value(data, field_type, "UDT field", depth);
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
            CqlType::Boolean => {
                if data.len() != 1 {
                    return Err(Error::corruption(format!(
                        "Boolean field requires 1 byte, got {}",
                        data.len()
                    )));
                }
                Ok(Value::Boolean(data[0] != 0))
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
            CqlType::Uuid | CqlType::TimeUuid => {
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
            CqlType::Blob => Ok(Value::Blob(
                crate::storage::sstable::reader::value_borrow::borrow_active(data),
            )),
            // Issue #3631: every remaining declared type is decoded from that
            // declared type — collections, tuples and nested UDTs structurally, the
            // remaining scalars through the shared type-string decoder — or reported
            // as an explicit `Error` naming the type. NEVER a silent `Value::Blob`.
            other => self.parse_typed_value(data, other, "UDT field", depth),
        }
    }

    /// Read the `[i32 BE]` at `pos`, bounds-checked.
    fn read_i32_at(data: &[u8], pos: usize, ctx: &str, what: &str) -> Result<i32> {
        let end = pos.checked_add(I32_LEN).ok_or_else(|| {
            Error::corruption(format!("{ctx}: offset overflow reading {what} length"))
        })?;
        if end > data.len() {
            return Err(Error::corruption(format!(
                "{ctx}: need {I32_LEN} bytes for the {what} length at offset {pos}, only {} available",
                data.len().saturating_sub(pos)
            )));
        }
        Ok(i32::from_be_bytes([
            data[pos],
            data[pos + 1],
            data[pos + 2],
            data[pos + 3],
        ]))
    }

    /// Read one `[i32 size][bytes]` element starting at `*pos`, advancing `*pos`.
    ///
    /// `Ok(None)` is a NULL element: `CollectionSerializer.readValue` returns null
    /// for any negative size, and `TupleType.split` does the same.
    fn read_sized_element<'d>(
        data: &'d [u8],
        pos: &mut usize,
        ctx: &str,
        what: &str,
    ) -> Result<Option<&'d [u8]>> {
        let size = Self::read_i32_at(data, *pos, ctx, what)?;
        *pos += I32_LEN;
        if size < 0 {
            return Ok(None);
        }
        let size = size as usize;
        let end = pos.checked_add(size).ok_or_else(|| {
            Error::corruption(format!("{ctx}: offset overflow reading {what} body"))
        })?;
        if end > data.len() {
            return Err(Error::corruption(format!(
                "{ctx}: {what} declares {size} bytes at offset {pos} but only {} remain",
                data.len().saturating_sub(*pos)
            )));
        }
        let body = &data[*pos..end];
        *pos = end;
        Ok(Some(body))
    }

    /// Read the leading `[i32 BE]` element count of a frozen collection, rejecting a
    /// negative count and one beyond `MAX_FROZEN_COLLECTION_SIZE`.
    ///
    /// A ZERO-LENGTH `data` is the empty collection, not corruption: a UDT field
    /// written with `[i32 size] == 0` carries `ByteBufferUtil.EMPTY_BYTE_BUFFER`, and
    /// this decoder's callers pass `&[]` for exactly that case.
    ///
    /// This is the one reading here NOT taken from Cassandra's own serializer, and it is
    /// stated rather than dressed up: `CollectionType` does NOT override
    /// `AbstractType.isEmptyValueMeaningless`, so Cassandra would `compose` an empty
    /// buffer, and `MapSerializer.deserialize` underflows on one — i.e. Cassandra never
    /// WRITES a zero-length frozen collection (an empty one is the 4 bytes `[i32 0]`).
    /// CQLite has always read it as the empty collection, and that behaviour is
    /// PRESERVED deliberately: it is lenient, it is not a blob, and tightening it is not
    /// this issue's subject (#3631 criterion 5 is about silent degradation to
    /// `Value::Blob`).
    fn read_collection_count(data: &[u8], ctx: &str) -> Result<(usize, usize)> {
        if data.is_empty() {
            // No header to consume either, so the cursor starts (and ends) at 0.
            return Ok((0, 0));
        }
        let count = Self::read_i32_at(data, 0, ctx, "collection element count")?;
        if count < 0 {
            return Err(Error::corruption(format!(
                "{ctx}: negative frozen collection element count {count}"
            )));
        }
        let count = count as u64;
        if count > MAX_FROZEN_COLLECTION_SIZE {
            return Err(Error::corruption(format!(
                "{ctx}: frozen collection element count {count} exceeds the {MAX_FROZEN_COLLECTION_SIZE} safety limit"
            )));
        }
        Ok((count as usize, I32_LEN))
    }

    /// Capacity to reserve for a declared element `count`, capped by what `data`
    /// could possibly hold.
    ///
    /// `count` is attacker-controlled (it is read out of the value being decoded), so
    /// reserving it directly lets a 4-byte corrupt header ask for the full
    /// `MAX_FROZEN_COLLECTION_SIZE` allocation. Every item costs at least its own
    /// `[i32 size]` prefix, so `data.len() / min_bytes_per_item` is a hard upper
    /// bound on how many can actually be present; a genuine collection reserves
    /// exactly, and a lying header reserves what its own bytes justify.
    fn bounded_capacity(count: usize, data_len: usize, min_bytes_per_item: usize) -> usize {
        count.min(data_len / min_bytes_per_item)
    }

    /// Decode `data` — the COMPLETE value, with no outer length prefix — as the
    /// declared `ty`, requiring EVERY byte to be accounted for.
    ///
    /// This is the `CqlType`-driven sibling of `parse_value_from_raw_bytes` (which is
    /// driven by a marshal / CQL type STRING). It never degrades to `Value::Blob` for
    /// a non-`blob` declared type: a type it cannot express is an explicit `Error`.
    ///
    /// # Consumption is part of the decode SIGNATURE, not a per-arm checklist
    /// Every caller — and every future caller — inherits the exhaustion rule, because
    /// the only way to decode a `CqlType` here is through this function, and the only
    /// way to get a value WITHOUT the assert is to ask
    /// [`Self::parse_typed_value_reporting`] for the consumed length and take
    /// responsibility for it explicitly. Review round 1 of this issue added the check
    /// to the collection arms, round 2 found the UDT arms still bypassing it; a per-arm
    /// checklist does not close.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn parse_typed_value(
        &self,
        data: &[u8],
        ty: &CqlType,
        ctx: &str,
        depth: usize,
    ) -> Result<Value> {
        let (value, consumed) = self.parse_typed_value_reporting(data, ty, ctx, depth)?;
        Self::require_fully_consumed(consumed, data.len(), ctx, Self::typed_value_label(ty))?;
        Ok(value)
    }

    /// [`Self::parse_typed_value`] WITHOUT the exhaustion assert, reporting how many
    /// bytes of `data` the decode actually consumed.
    ///
    /// `Ok((value, consumed))` where `consumed <= data.len()`. Only two kinds of
    /// caller may use this instead of the asserting entry point above: the arms
    /// BELOW, which thread the count outward, and a caller that has a
    /// Cassandra-derived reason for a short read — of which there is exactly one, a
    /// UDT/tuple whose trailing components are omitted, and even there
    /// `TupleType.split` requires the decode to have ended exactly at the end of the
    /// buffer, which is what the assert in the wrapper checks.
    fn parse_typed_value_reporting(
        &self,
        data: &[u8],
        ty: &CqlType,
        ctx: &str,
        depth: usize,
    ) -> Result<(Value, usize)> {
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::corruption(format!(
                "{ctx}: type nesting depth {depth} exceeds maximum {MAX_TYPE_NESTING_DEPTH}"
            )));
        }
        match ty {
            // ── Structured shapes (the #3631 subject) ───────────────────────────
            CqlType::Frozen(inner) => {
                // `frozen<X>` is serialized EXACTLY as `X` in a nested position; the
                // wrapper is a type-system marker. Mirrors the `Frozen` arms in
                // `parse_nested_udt_from_registry` and `parse_value_from_raw_bytes`,
                // which also surface `Value::Frozen`.
                //
                // It consumes NO nesting level, and that is load-bearing rather than
                // tidy. The limit must count FRAMING layers — the things that make the
                // decoder recurse over bytes — and `frozen` adds none. Counting it
                // charged the canonical spellings in this repo's own corpus up to five
                // levels per logical layer (`frozen<set<frozen<tuple<frozen<udt>,int>>>>`
                // is the fixture's `stn`), which put real Cassandra-written data within
                // one level of a FALSE REFUSAL. Termination does not depend on it: every
                // recursion through a UDT boundary or a collection element still
                // increments, so any cycle is strictly increasing and bounded.
                let (inner_value, consumed) =
                    self.parse_typed_value_reporting(data, inner, ctx, depth)?;
                Ok((Value::Frozen(Box::new(inner_value)), consumed))
            }
            CqlType::List(element) => {
                let (elements, consumed) =
                    self.parse_typed_collection_elements(data, element, ctx, depth)?;
                Ok((Value::List(elements), consumed))
            }
            CqlType::Set(element) => {
                let (elements, consumed) =
                    self.parse_typed_collection_elements(data, element, ctx, depth)?;
                Ok((Value::Set(elements), consumed))
            }
            CqlType::Map(key_type, value_type) => {
                let (count, header) = Self::read_collection_count(data, ctx)?;
                // A map ENTRY costs two `[i32 size]` prefixes, so 8 bytes minimum.
                let mut entries = Vec::with_capacity(Self::bounded_capacity(count, data.len(), 8));
                let mut pos = header;
                for i in 0..count {
                    let key_ctx = format!("{ctx}: map entry {i} key");
                    let key = match Self::read_sized_element(data, &mut pos, &key_ctx, "map key")? {
                        // `MapSerializer` writes a key with `writeValue`, so a
                        // negative length is representable — but a null key is not a
                        // legal Cassandra map entry, so it is corruption, not a Null.
                        None => {
                            return Err(Error::corruption(format!(
                                "{key_ctx}: null map key (negative length) is not a legal map entry"
                            )))
                        }
                        Some(bytes) => {
                            self.parse_typed_value(bytes, key_type, &key_ctx, depth + 1)?
                        }
                    };
                    let value_ctx = format!("{ctx}: map entry {i} value");
                    let value =
                        match Self::read_sized_element(data, &mut pos, &value_ctx, "map value")? {
                            // A negative length is REPRESENTABLE and is corruption, not a
                            // `Null` — the same rule as the key above, and it used to
                            // differ. Verified first-hand at the pinned tag:
                            // `cassandra-5.0.8:src/java/org/apache/cassandra/serializers/MapSerializer.java`
                            // `deserialize` reads BOTH halves of every entry with
                            // `readNonNullValue`, and
                            // `CollectionSerializer.readNonNullValue` throws
                            // `MarshalException("Null value read when not allowed")` when
                            // `readValue` returned null, which it does for any `size < 0`.
                            // So Cassandra itself refuses this byte pattern; surfacing it
                            // as `Value::Null` invented a value no writer can produce.
                            None => {
                                return Err(Error::corruption(format!(
                                    "{value_ctx}: null map value (negative length) is not a \
                                     legal map entry — Cassandra's MapSerializer reads \
                                     entry values with readNonNullValue"
                                )))
                            }
                            Some(bytes) => {
                                self.parse_typed_value(bytes, value_type, &value_ctx, depth + 1)?
                            }
                        };
                    entries.push((key, value));
                }
                Ok((Value::Map(entries), pos))
            }
            CqlType::Tuple(element_types) => {
                if element_types.is_empty() {
                    return Err(Error::schema(format!(
                        "{ctx}: tuple type declares no components"
                    )));
                }
                let mut elements = Vec::with_capacity(element_types.len());
                let mut pos = 0usize;
                for (i, element_type) in element_types.iter().enumerate() {
                    // `TupleType.split` stops at the end of the buffer: a tuple may
                    // be written with fewer components than declared, and the
                    // trailing components are then absent (null).
                    if pos >= data.len() {
                        elements.push(Value::Null);
                        continue;
                    }
                    let element_ctx = format!("{ctx}: tuple component {i}");
                    elements.push(
                        match Self::read_sized_element(
                            data,
                            &mut pos,
                            &element_ctx,
                            "tuple component",
                        )? {
                            None => Value::Null,
                            Some(bytes) => self.parse_typed_value(
                                bytes,
                                element_type,
                                &element_ctx,
                                depth + 1,
                            )?,
                        },
                    );
                }
                // A tuple written with FEWER components than declared is legal —
                // `TupleType.split` returns early on `position == length` and the loop
                // above null-pads — but bytes left after the LAST declared component
                // are trailing garbage (`if (position < length) throw`), which the
                // caller's one exhaustion assert refuses.
                Ok((Value::Tuple(elements), pos))
            }
            // A UDT nested inside a collection / tuple / another UDT. Delegated so
            // the per-field `[i32 size][bytes]` framing has ONE implementation.
            //
            // `Custom` carries a UDT NAME *or* a marshal type reference the type-string
            // parser had no `CqlType` for (`EmptyType`, `VectorType(...)`, a
            // third-party `AbstractType`), so only the former may be routed here:
            // an unroutable marshal name falls through to the `scalar` arm below and is
            // refused as an undecodable DECLARED TYPE, which is what it is. Routing it
            // here reported "nested user-defined type … field list is not available",
            // misattributing the cause (roborev job 68, finding 1).
            CqlType::Custom(name) if !Self::custom_is_marshal_type_reference(name) => {
                self.parse_typed_udt(data, name, &[], ctx, depth)
            }
            CqlType::Udt(name, inline_fields) => {
                self.parse_typed_udt(data, name, inline_fields, ctx, depth)
            }

            // ── Scalars ─────────────────────────────────────────────────────────
            // Delegated to the type-STRING decoder by naming the declared type: one
            // closed `CqlType` -> canonical CQL short form mapping, no guessing, and
            // one implementation of each scalar's byte layout.
            scalar => {
                let short = Self::cql_scalar_short_form(scalar).ok_or_else(|| {
                    // The no-silent-blob boundary (#3631 acceptance criterion 5).
                    Error::unsupported_format(format!(
                        "{ctx}: cannot decode declared type {scalar:?} — CQLite has no \
                         decoding rule for it, and returning the raw bytes as a blob \
                         would silently discard the declared type (issue #3631 / #28)"
                    ))
                })?;
                // An EMPTY buffer is NULL for every scalar whose Cassandra serializer
                // guards `accessor.isEmpty(value) ? null : …` — see
                // `Self::empty_is_a_value` for the per-serializer reading. The old
                // `create_empty_value_for_type` handed back an empty BLOB here.
                if data.is_empty() && !Self::empty_is_a_value(short) {
                    return Ok((Value::Null, 0));
                }
                if let Some(width) = Self::fixed_scalar_width(short) {
                    // Any length other than the exact width is refused rather than
                    // truncated: the delegate bounds-checks with `<`, so it would read
                    // `width` bytes out of a longer frame and drop the tail.
                    if data.len() != width {
                        return Err(Error::corruption(format!(
                            "{ctx}: declared type '{short}' is {width} bytes wide (or \
                             empty, meaning null) but the framed value is {} bytes; \
                             accepting it would silently discard {} trailing byte(s) \
                             (issue #3631)",
                            data.len(),
                            data.len().saturating_sub(width)
                        )));
                    }
                }
                // The delegate is the ASSERTING short name, so on `Ok` it has ALREADY
                // required `consumed == data.len()` (#3811). That is what makes
                // reporting `data.len()` here a VERIFIED claim rather than an assumed
                // one — including for `duration`, the one scalar whose decode can stop
                // short, whose arm reports where its third VInt actually ended. #3631's
                // predecessor re-walked those VInts here; that walk is folded out for
                // the same reason `cell_path_key.rs`'s was.
                let value = self.parse_value_from_raw_bytes(data, short, ctx, depth)?;
                Ok((value, data.len()))
            }
        }
    }

    /// The `[i32 count]` + `[i32 size][bytes]`× body shared by `list` and `set`,
    /// reporting the offset it reached so the caller's one exhaustion assert can see
    /// a zero-count header with a payload behind it.
    fn parse_typed_collection_elements(
        &self,
        data: &[u8],
        element_type: &CqlType,
        ctx: &str,
        depth: usize,
    ) -> Result<(Vec<Value>, usize)> {
        let (count, header) = Self::read_collection_count(data, ctx)?;
        // One element costs at least its own `[i32 size]` prefix, so 4 bytes.
        let mut elements = Vec::with_capacity(Self::bounded_capacity(count, data.len(), I32_LEN));
        let mut pos = header;
        for i in 0..count {
            let element_ctx = format!("{ctx}: element {i}");
            elements.push(
                match Self::read_sized_element(data, &mut pos, &element_ctx, "element")? {
                    // Cassandra rejects null collection elements on write
                    // (`CollectionSerializer.readNonNullValue` throws), so a negative
                    // length here is corruption rather than a Null element.
                    None => {
                        return Err(Error::corruption(format!(
                            "{element_ctx}: null collection element (negative length) is not legal"
                        )))
                    }
                    Some(bytes) => {
                        self.parse_typed_value(bytes, element_type, &element_ctx, depth + 1)?
                    }
                },
            );
        }
        Ok((elements, pos))
    }

    /// Decode a nested UDT named by the declared type, preferring the authoritative
    /// `UdtRegistry` and falling back to the inline field list carried by
    /// `CqlType::Udt` (issue #239). An unresolvable name is an explicit `Error`: the
    /// bytes cannot be interpreted without a field list, and handing back a blob
    /// would discard the declared type name (#3631 criterion 5).
    ///
    /// # Why the reported consumption is `data.len()`, VERIFIED not assumed
    /// A UDT's field loop STOPS as soon as fewer than four bytes remain, treating the
    /// undeclared remainder as omitted trailing fields — so a bare `Value` return would
    /// hide both trailing garbage AND an incomplete 1-3 byte field-length prefix. Since
    /// #3811 both delegates below END with `Self::require_fully_consumed(current_offset,
    /// data.len(), …)`, so an `Ok` from either is a PROOF that the loop reached
    /// `data.len()`.
    /// `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/TupleType.java`
    /// `split` (which `UserType extends TupleType` inherits and calls) is explicit
    /// about all three cases: `if (position == length) return Arrays.copyOfRange(...)`
    /// — omitted trailing components are legal ONLY when the decode ended exactly at
    /// the end of the buffer; `if (position + 4 > length) throw new
    /// MarshalException("Not enough bytes to read %dth component")` — a partial length
    /// prefix is corruption; and, after the loop, `if (position < length) throw ...
    /// "but got more"` — trailing bytes are corruption.
    fn parse_typed_udt(
        &self,
        data: &[u8],
        name: &str,
        inline_fields: &[(String, CqlType)],
        ctx: &str,
        depth: usize,
    ) -> Result<(Value, usize)> {
        // Crossing a UDT boundary CONSUMES a level: both decoders below recurse back
        // into `parse_simple_udt_field_value_at`, so passing `depth + 1` (never `0`
        // or `1`) is what makes ONE limit hold across alternating collection/UDT
        // nesting. Each decoder re-checks the limit on entry, so an over-deep value is
        // refused at the boundary that exceeded it.
        if let Some(registry) = self.udt_registry.as_ref() {
            // `get_udt_qualified` owns "udt:" + keyspace-qualifier normalization
            // (issues #239 / #2807).
            if let Some(def) = registry.get_udt_qualified(&self.keyspace, name) {
                let value = self.parse_nested_udt_from_registry(data, def, depth + 1)?;
                return Ok((value, data.len()));
            }
        }
        if !inline_fields.is_empty() {
            let value = self.parse_inline_udt_value(data, name, inline_fields, depth + 1)?;
            return Ok((value, data.len()));
        }
        Err(Error::unsupported_format(format!(
            "{ctx}: nested user-defined type '{name}' is declared but its field list is \
             not available (absent from the UDT registry and carrying no inline \
             fields), so its bytes cannot be decoded; returning them as a blob would \
             silently discard the declared type (issue #3631 / #28)"
        )))
    }
}
