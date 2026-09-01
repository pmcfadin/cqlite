//! Decode a bounded byte slice whose type is a **declared `CqlType`** (issue #3631).
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
//!   Instance A, fixed there by delegating to the type-STRING decoder.
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

// Issue #3631: the two blob-fallback arms — a NON-frozen map's cell-path key and a
// collection-typed field of a frozen UDT. Covers acceptance criteria 4 and 5 at the
// decoder's own level, with bytes derived from cassandra-5.0.8 source; the
// fixture-backed parity oracle is `tests/issue_3631_structured_values_not_blobs.rs`.
#[cfg(test)]
#[path = "regression_3631_typed_value_tests.rs"]
mod regression_3631_typed_value_tests;

/// The number of leading bytes an `[i32 BE]` length or count occupies —
/// `TypeSizes.INT_SIZE` in `CollectionSerializer`.
const I32_LEN: usize = 4;

impl V5CompressedLegacyParser {
    /// Parse a UDT field value without requiring an `SSTableReader`.
    ///
    /// The scalar arms below are kept BYTE-FOR-BYTE as they were before issue
    /// #3631 (they encode measured, shipped behaviour — e.g. `CqlType::Float`
    /// surfaces `Value::Float32`, which the type-string decoder spells as a widened
    /// `Value::Float`; unifying the two is a separate change). What #3631 replaced is
    /// the trailing `_ =>` arm, which used to hand back `Value::Blob` for EVERY
    /// remaining type — including a `frozen<map<text,int>>` / `list` / `set` field of
    /// a frozen UDT, whose declared type was in hand and unread. That silent
    /// degradation is what #28 (no-heuristics) forbids, and it made a value
    /// HASHABLE in the Python binding that should not have been (#3500).
    pub(super) fn parse_simple_udt_field_value(
        &self,
        data: &[u8],
        field_type: &CqlType,
    ) -> Result<Value> {
        self.parse_simple_udt_field_value_at(data, field_type, 0)
    }

    /// `parse_simple_udt_field_value` carrying an explicit nesting `depth`.
    ///
    /// Issue #3631 (roborev BLOCKER 2). The 0-depth wrapper above is the entry point
    /// for every pre-existing caller. A field reached from INSIDE a collection, tuple
    /// or another UDT must not restart the counter: the two UDT decoders recurse back
    /// through here, so a reset at each UDT boundary makes
    /// `MAX_TYPE_NESTING_DEPTH` bound nothing across alternating collection/UDT
    /// layers, and a cyclic `UdtRegistry` then recurses until the stack is exhausted.
    pub(super) fn parse_simple_udt_field_value_at(
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
    /// this decoder's callers pass `&[]` for exactly that case (the same rule
    /// `create_empty_value_for_type` already applies for List/Set/Map).
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

    /// Every byte of a bounded slice must be accounted for (roborev BLOCKER 3 on
    /// #3631).
    ///
    /// The caller framed `data` as exactly one value, so bytes left over after the
    /// declared structure is read are not "extra" — they are evidence that the frame
    /// and the type disagree. Discarding them silently is the framing-error-MASKING
    /// class that let #3002's `Rows.db` root-base defect hide behind a compensating
    /// encoder defect: two errors that cancel are undetectable unless something
    /// insists the accounting balances. The sharpest case is a zero-count collection
    /// with a payload behind it, which otherwise decodes as a cheerful empty
    /// collection.
    fn require_fully_consumed(pos: usize, data: &[u8], ctx: &str) -> Result<()> {
        if pos != data.len() {
            return Err(Error::corruption(format!(
                "{ctx}: decoded {pos} of {} bytes — {} trailing byte(s) unaccounted \
                 for; the declared type and the framed value disagree, and silently \
                 discarding them would mask a framing error (issue #3631)",
                data.len(),
                data.len().saturating_sub(pos)
            )));
        }
        Ok(())
    }

    /// The EXACT serialized width of a fixed-width scalar, or `None` when the type is
    /// variable-width (`text`, `blob`, `varint`, `decimal`, `duration`, `inet`).
    ///
    /// Authority: `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/`, whose
    /// fixed-width `AbstractType.validate` implementations accept the exact width OR
    /// an EMPTY buffer and reject everything else (e.g. `Int32Type.validate`:
    /// `if (bytes.remaining() != 4 && bytes.remaining() != 0) throw`). The
    /// type-string decoder this module delegates to bounds-checks with `<`, so it
    /// happily reads a 4-byte int out of a 9-byte frame and drops the rest — the same
    /// silent-discard class as above, one level down.
    fn fixed_scalar_width(short: &str) -> Option<usize> {
        Some(match short {
            "boolean" | "tinyint" => 1,
            "smallint" => 2,
            "int" | "float" | "date" => 4,
            "bigint" | "counter" | "double" | "time" | "timestamp" => 8,
            "uuid" | "timeuuid" => 16,
            _ => return None,
        })
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
    /// declared `ty`.
    ///
    /// This is the `CqlType`-driven sibling of `parse_value_from_raw_bytes` (which is
    /// driven by a marshal / CQL type STRING). It never degrades to `Value::Blob` for
    /// a non-`blob` declared type: a type it cannot express is an explicit `Error`.
    pub(super) fn parse_typed_value(
        &self,
        data: &[u8],
        ty: &CqlType,
        ctx: &str,
        depth: usize,
    ) -> Result<Value> {
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
                let inner_value = self.parse_typed_value(data, inner, ctx, depth + 1)?;
                Ok(Value::Frozen(Box::new(inner_value)))
            }
            CqlType::List(element) => Ok(Value::List(
                self.parse_typed_collection_elements(data, element, ctx, depth)?,
            )),
            CqlType::Set(element) => Ok(Value::Set(
                self.parse_typed_collection_elements(data, element, ctx, depth)?,
            )),
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
                            None => Value::Null,
                            Some(bytes) => {
                                self.parse_typed_value(bytes, value_type, &value_ctx, depth + 1)?
                            }
                        };
                    entries.push((key, value));
                }
                Self::require_fully_consumed(pos, data, ctx)?;
                Ok(Value::Map(entries))
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
                // A tuple written with FEWER components than declared is legal
                // (`TupleType.split` stops at the end of the buffer, and the loop
                // above null-pads), but bytes left after the LAST declared component
                // are trailing garbage.
                Self::require_fully_consumed(pos, data, ctx)?;
                Ok(Value::Tuple(elements))
            }
            // A UDT nested inside a collection / tuple / another UDT. Delegated so
            // the per-field `[i32 size][bytes]` framing has ONE implementation.
            CqlType::Custom(name) => self.parse_typed_udt(data, name, &[], ctx, depth),
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
                if let Some(width) = Self::fixed_scalar_width(short) {
                    // Cassandra accepts the exact width or an EMPTY buffer; the
                    // delegate below checks `<`, so it would silently drop the tail
                    // of an oversized frame.
                    if !data.is_empty() && data.len() != width {
                        return Err(Error::corruption(format!(
                            "{ctx}: declared type '{short}' is {width} bytes wide but \
                             the framed value is {} bytes; accepting it would \
                             silently discard {} trailing byte(s) (issue #3631)",
                            data.len(),
                            data.len().saturating_sub(width)
                        )));
                    }
                }
                self.parse_value_from_raw_bytes(data, short, ctx, depth)
            }
        }
    }

    /// The `[i32 count]` + `[i32 size][bytes]`× body shared by `list` and `set`.
    fn parse_typed_collection_elements(
        &self,
        data: &[u8],
        element_type: &CqlType,
        ctx: &str,
        depth: usize,
    ) -> Result<Vec<Value>> {
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
        Self::require_fully_consumed(pos, data, ctx)?;
        Ok(elements)
    }

    /// Decode a nested UDT named by the declared type, preferring the authoritative
    /// `UdtRegistry` and falling back to the inline field list carried by
    /// `CqlType::Udt` (issue #239). An unresolvable name is an explicit `Error`: the
    /// bytes cannot be interpreted without a field list, and handing back a blob
    /// would discard the declared type name (#3631 criterion 5).
    fn parse_typed_udt(
        &self,
        data: &[u8],
        name: &str,
        inline_fields: &[(String, CqlType)],
        ctx: &str,
        depth: usize,
    ) -> Result<Value> {
        // Crossing a UDT boundary CONSUMES a level: both decoders below recurse back
        // into `parse_simple_udt_field_value_at`, so passing `depth + 1` (never `0`
        // or `1`) is what makes ONE limit hold across alternating collection/UDT
        // nesting — roborev BLOCKER 2 on #3631. Each decoder re-checks the limit on
        // entry, so an over-deep value is refused at the boundary that exceeded it.
        if let Some(registry) = self.udt_registry.as_ref() {
            // `get_udt_qualified` owns "udt:" + keyspace-qualifier normalization
            // (issues #239 / #2807).
            if let Some(def) = registry.get_udt_qualified(&self.keyspace, name) {
                return self.parse_nested_udt_from_registry_at(data, def, registry, depth + 1);
            }
        }
        if !inline_fields.is_empty() {
            return self.parse_inline_udt_value(data, name, inline_fields, depth + 1);
        }
        Err(Error::unsupported_format(format!(
            "{ctx}: nested user-defined type '{name}' is declared but its field list is \
             not available (absent from the UDT registry and carrying no inline \
             fields), so its bytes cannot be decoded; returning them as a blob would \
             silently discard the declared type (issue #3631 / #28)"
        )))
    }

    /// The canonical CQL short form of a SCALAR `CqlType`, or `None` when `ty` is not
    /// a scalar this decoder can name.
    ///
    /// The returned strings are the arm labels of `parse_value_from_raw_bytes`, which
    /// implements each scalar's on-disk byte layout. Deliberately exhaustive over
    /// `CqlType` with no wildcard, so a NEW variant is a compile error here rather
    /// than a silent blob at run time.
    fn cql_scalar_short_form(ty: &CqlType) -> Option<&'static str> {
        Some(match ty {
            CqlType::Boolean => "boolean",
            CqlType::TinyInt => "tinyint",
            CqlType::SmallInt => "smallint",
            CqlType::Int => "int",
            CqlType::BigInt => "bigint",
            CqlType::Counter => "counter",
            CqlType::Float => "float",
            CqlType::Double => "double",
            CqlType::Decimal => "decimal",
            CqlType::Text => "text",
            CqlType::Ascii => "ascii",
            CqlType::Varchar => "varchar",
            CqlType::Blob => "blob",
            CqlType::Timestamp => "timestamp",
            CqlType::Date => "date",
            CqlType::Time => "time",
            CqlType::Uuid => "uuid",
            CqlType::TimeUuid => "timeuuid",
            CqlType::Inet => "inet",
            CqlType::Duration => "duration",
            CqlType::Varint => "varint",
            // Structured types are handled by `parse_typed_value`'s own arms and must
            // never reach the scalar delegation.
            CqlType::List(_)
            | CqlType::Set(_)
            | CqlType::Map(_, _)
            | CqlType::Tuple(_)
            | CqlType::Udt(_, _)
            | CqlType::Frozen(_)
            | CqlType::Custom(_) => return None,
        })
    }
}
