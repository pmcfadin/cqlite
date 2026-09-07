//! Issue #3811 — the CONSUMPTION-REPORTING twin of
//! [`V5CompressedLegacyParser::parse_value_from_raw_bytes`], plus the one
//! assert that every bounded caller of that function now inherits.
//!
//! # Why this file exists at all
//!
//! `parse_value_from_raw_bytes` is documented as a BOUNDED decoder — *"The
//! entire `data` slice IS the value"* — but it returned a bare `Result<Value>`,
//! so **no caller could check the claim even if it wanted to**, and several of
//! its own arms threw away the count their callee already reported. Two
//! distinct serialized values therefore collapsed to one `Value`: trailing
//! bytes after a complete UDT, and a partial 1-3 byte component-length prefix,
//! were both silently accepted (demonstrated in
//! `docs/round-artifacts/issue-3811-defect-demonstration.md`).
//!
//! # The oracle — `cassandra-5.0.8` `TupleType.split`, IN ITS OWN ORDER
//!
//! `src/java/org/apache/cassandra/db/marshal/TupleType.java`, static
//! `split(...)`; `UserType extends TupleType`, so a UDT value is split by
//! exactly this method. The ORDER is the whole content, because it is what
//! separates a legal omission from a corruption and the two are one byte apart:
//!
//! 1. `position == length` before component `i` ⇒ **LEGAL** short return;
//!    components `i..n` are absent (implicit null).
//! 2. else `position + 4 > length` ⇒ throw
//!    `"Not enough bytes to read %dth component"`.
//! 3. `position + size > length` ⇒ throw, same message.
//! 4. after the loop, `position < length` ⇒ throw
//!    `"Expected N values ... but got more"`.
//!
//! Rules 2 and 4 share ONE observable in CQLite: the decoders treat 1-3 leftover
//! bytes as "trailing fields omitted" and `break` WITHOUT advancing past them, so
//! the reported consumption is short by exactly those bytes — the same signal as
//! trailing garbage. A single `consumed == data.len()` comparison at the bounded
//! caller therefore refuses both while leaving rule 1 (a genuinely short
//! encoding, where `consumed == data.len()`) accepted.
//!
//! # Shape, and the escape hatch this deliberately does NOT reproduce
//!
//! `complex_column/cell_path_key.rs`'s `decode_reporting_consumption` returns
//! `Ok((value, None))` where `None` means "this arm consumes the whole slice by
//! construction, nothing to compare". **That `None` is an opt-out a new arm
//! inherits by accident**, and that file already declares the resulting drift
//! against itself (`:485-500`: a manual obligation that "no test enforces").
//! Here every arm returns a REAL `usize`; arms that consume the whole slice by
//! construction return `data.len()` EXPLICITLY, so a newly added arm cannot fall
//! into a branch that skips the check — it has to state a number.

// One level deeper than `raw_value.rs`, so the `row_decoder` glob is `super::super`.
use super::super::*;
// Issue #3847: the ONE statement of which widths a fixed-width scalar admits and
// what an EMPTY buffer means. Shared with `udt.rs`'s two scalar field decoders.
use super::fixed_width::{self, FixedWidthCell};

impl V5CompressedLegacyParser {
    /// Consumption-reporting form of
    /// [`Self::parse_value_from_raw_bytes`](super::V5CompressedLegacyParser::parse_value_from_raw_bytes):
    /// returns the decoded value AND the number of bytes of `data` it actually
    /// read.
    ///
    /// **This is NOT a general escape hatch, and the docs used to say it was.**
    /// It is `pub(super)` inside `raw_value::reporting`, so `super` is `raw_value`
    /// and NO other `row_decoder` child — not `frozen.rs`, not `complex_column.rs`,
    /// not `cell_path_key.rs` — can name it. Every bounded caller outside this
    /// module therefore reaches the asserting short name and CANNOT opt out, which
    /// is stronger than the naming convention #3811's AC2 asked for. If a caller
    /// elsewhere ever genuinely needs a short read, widening this visibility is the
    /// deliberate, reviewable act — do not do it to silence a refusal.
    ///
    /// - Variable-width types (text, blob, varint, decimal, inet): consume the full slice
    /// - Fixed-width types (int, bigint, uuid, etc.): consume exactly their width
    /// - Nested collections: use the bounded sub-format `[i32 BE count][i32 BE len][bytes]...`
    pub(super) fn parse_value_from_raw_bytes_reporting(
        &self,
        data: &[u8],
        type_str: &str,
        column_name: &str,
        depth: usize,
    ) -> Result<(Value, usize)> {
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::corruption(format!(
                "Frozen element '{}': recursion depth {} exceeds maximum {}",
                column_name, depth, MAX_TYPE_NESTING_DEPTH
            )));
        }
        // Issue #1081: scalar marshal forms (e.g.
        // `org.apache.cassandra.db.marshal.Int32Type` / `BooleanType`) reach this
        // function for multicell-UDT field values, which resolve their field
        // types from the authoritative on-disk `UserType(...)` marshal string.
        // The match below only enumerates short forms plus a handful of text
        // marshal aliases, so a bare scalar marshal type would otherwise fall
        // through to the blob default. Normalize a primitive marshal type to its
        // canonical CQL short form (via the existing authoritative marshal→CqlType
        // mapping, no heuristics) and re-dispatch. Composite/UDT marshal forms
        // (UserType/ListType/MapType/SetType/etc.) are left untouched here — they
        // are handled by the dedicated arms below — so this only rewrites scalars.
        if type_str.contains("org.apache.cassandra.db.marshal.") {
            if let Some(short) = Self::primitive_marshal_to_cql_short(type_str) {
                return self.parse_value_from_raw_bytes_reporting(data, short, column_name, depth);
            }
        }

        // Preserve the ORIGINAL-CASE type string. Below, the `match` scrutinee is
        // `type_str.to_lowercase()` and each `type_str if ...` arm binding SHADOWS
        // the function parameter with the lowercased string. The collection/tuple/
        // frozen extraction helpers slice their element/inner types out of the
        // string they are handed, so if we passed the lowercased binding the nested
        // element marshal type would come back lowercased (e.g. `...int32type`) and
        // would NOT re-normalize via the CASE-SENSITIVE `primitive_marshal_to_cql_short`
        // suffix match, wrongly falling through to blob. The marshal-form arms below
        // therefore extract from `raw_type_str` (original case) so nested element
        // marshal types keep their case. The CQL-short-form arms are unaffected
        // because their inner types are already canonical lowercase.
        let raw_type_str = type_str;
        let normalized_type = type_str.to_lowercase();
        match normalized_type.as_str() {
            "text"
            | "varchar"
            | "ascii"
            | "org.apache.cassandra.db.marshal.utf8type"
            | "org.apache.cassandra.db.marshal.asciitype"
            | "org.apache.cassandra.db.marshal.varchartype" => {
                // Issue #1644 (K5 stage 2): validate in place, borrow if possible.
                std::str::from_utf8(data).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': invalid UTF-8 in text value: {}",
                        column_name, e
                    ))
                })?;
                Ok((
                    Value::Text(crate::storage::sstable::reader::value_borrow::borrow_active(data)),
                    data.len(),
                ))
            }
            "blob" | "bytes" => Ok((
                Value::Blob(crate::storage::sstable::reader::value_borrow::borrow_active(data)),
                data.len(),
            )),
            "int" => match Self::require_fixed_width(data, 4, "int", column_name)? {
                FixedWidthCell::Null => Ok((Value::Null, 0)),
                FixedWidthCell::Bytes => Ok((
                    Value::Integer(i32::from_be_bytes([data[0], data[1], data[2], data[3]])),
                    4,
                )),
            },
            "bigint" | "counter" => {
                match Self::require_fixed_width(data, 8, "bigint", column_name)? {
                    FixedWidthCell::Null => Ok((Value::Null, 0)),
                    FixedWidthCell::Bytes => Ok((
                        Value::BigInt(i64::from_be_bytes([
                            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                        ])),
                        8,
                    )),
                }
            }
            "boolean" => match Self::require_fixed_width(data, 1, "boolean", column_name)? {
                FixedWidthCell::Null => Ok((Value::Null, 0)),
                FixedWidthCell::Bytes => Ok((Value::Boolean(data[0] != 0), 1)),
            },
            "uuid" | "timeuuid" => {
                match Self::require_fixed_width(data, 16, "UUID", column_name)? {
                    FixedWidthCell::Null => Ok((Value::Null, 0)),
                    FixedWidthCell::Bytes => {
                        let uuid: [u8; 16] = data[..16]
                            .try_into()
                            .map_err(|_| Error::corruption("UUID byte conversion failed"))?;
                        Ok((Value::Uuid(uuid), 16))
                    }
                }
            }
            "float" => match Self::require_fixed_width(data, 4, "float", column_name)? {
                FixedWidthCell::Null => Ok((Value::Null, 0)),
                FixedWidthCell::Bytes => {
                    // CQL `float` is `Value::Float32`, not the f64 `Value::Float`; the column
                    // path and both UDT field decoders already agree (roborev round 10 F1).
                    let f = f32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                    Ok((Value::Float32(f), 4))
                }
            },
            "double" => match Self::require_fixed_width(data, 8, "double", column_name)? {
                FixedWidthCell::Null => Ok((Value::Null, 0)),
                FixedWidthCell::Bytes => Ok((
                    Value::Float(f64::from_be_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ])),
                    8,
                )),
            },
            "smallint" | "short" => {
                match Self::require_fixed_width(data, 2, "smallint", column_name)? {
                    FixedWidthCell::Null => Ok((Value::Null, 0)),
                    FixedWidthCell::Bytes => {
                        Ok((Value::SmallInt(i16::from_be_bytes([data[0], data[1]])), 2))
                    }
                }
            }
            "tinyint" | "byte" => {
                match Self::require_fixed_width(data, 1, "tinyint", column_name)? {
                    FixedWidthCell::Null => Ok((Value::Null, 0)),
                    FixedWidthCell::Bytes => Ok((Value::TinyInt(data[0] as i8), 1)),
                }
            }
            "timestamp" => match Self::require_fixed_width(data, 8, "timestamp", column_name)? {
                FixedWidthCell::Null => Ok((Value::Null, 0)),
                FixedWidthCell::Bytes => Ok((
                    Value::Timestamp(i64::from_be_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ])),
                    8,
                )),
            },
            "date" => match Self::require_fixed_width(data, 4, "date", column_name)? {
                FixedWidthCell::Null => Ok((Value::Null, 0)),
                FixedWidthCell::Bytes => {
                    let stored = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                    let days_since_epoch = stored.wrapping_add(i32::MIN as u32) as i32;
                    Ok((Value::Date(days_since_epoch), 4))
                }
            },
            "time" => match Self::require_fixed_width(data, 8, "time", column_name)? {
                FixedWidthCell::Null => Ok((Value::Null, 0)),
                FixedWidthCell::Bytes => Ok((
                    Value::Time(i64::from_be_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ])),
                    8,
                )),
            },
            "duration" => {
                // Issue #1081: in this function the entire `data` slice IS the value
                // (the element/cell length prefix already bounded it) — there is NO
                // outer `[VInt len]` prefix. Decode three consecutive SIGNED VInts
                // directly over `data`: months, days, nanos (Cassandra
                // DurationSerializer). Contrast `parse_raw_type_value`'s duration arm,
                // which reads an outer `[VInt len]` first because its framing differs.
                let (remaining, months) = parse_vint(data).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse duration months: {:?}",
                        column_name, e
                    ))
                })?;
                let pos = data.len() - remaining.len();

                let (remaining, days) = parse_vint(&data[pos..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse duration days: {:?}",
                        column_name, e
                    ))
                })?;
                let pos = data.len() - remaining.len();

                let (remaining, nanos) = parse_vint(&data[pos..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen element '{}': failed to parse duration nanos: {:?}",
                        column_name, e
                    ))
                })?;
                // Issue #3811: three consecutive VInts need not fill the slice, so
                // report where they actually ended rather than assuming `data.len()`.
                let consumed = data.len() - remaining.len();

                // months/days are i32 in Cassandra's DurationType. Reject
                // (rather than silently truncate via `as i32`) any encoded value
                // outside the i32 range so a corrupt encoding errors instead of
                // wrapping (issue #1632, item b).
                let months = i32::try_from(months).map_err(|_| {
                    Error::corruption(format!(
                        "Frozen element '{}': duration months out of i32 range",
                        column_name
                    ))
                })?;
                let days = i32::try_from(days).map_err(|_| {
                    Error::corruption(format!(
                        "Frozen element '{}': duration days out of i32 range",
                        column_name
                    ))
                })?;
                Ok((
                    Value::Duration {
                        months,
                        days,
                        nanos,
                    },
                    consumed,
                ))
            }
            "varint" => Ok((
                Value::Varint(crate::storage::sstable::reader::value_borrow::borrow_active(data)),
                data.len(),
            )),
            "decimal" => {
                if data.len() < 4 {
                    return Err(Error::corruption(format!(
                        "Frozen element '{}': decimal too short ({} bytes)",
                        column_name,
                        data.len()
                    )));
                }
                let scale = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let unscaled = data[4..].to_vec();
                Ok((Value::Decimal { scale, unscaled }, data.len()))
            }
            "inet" => Ok((
                Value::Inet(crate::storage::sstable::reader::value_borrow::borrow_active(data)),
                data.len(),
            )),
            // Nested list/set/map inside a bounded element (e.g. map<text, list<int>>).
            //
            // Issue #1081: the guards accept BOTH the CQL short form (`list<...>`)
            // and the authoritative Cassandra marshal form
            // (`org.apache.cassandra.db.marshal.ListType(...)`). Multicell-UDT field
            // values resolve their field types from the on-disk `UserType(...)`
            // marshal string, so a collection-typed UDT field arrives here in marshal
            // form and would otherwise fall through to the blob default. The
            // extraction helpers (`extract_collection_element_type` / `extract_map_types`)
            // already accept marshal forms; we extract from `raw_type_str`
            // (original case) so the returned nested element marshal type keeps its
            // case and re-normalizes correctly on recursion (see note above).
            //
            // Issue #3811: `parse_frozen_*_value_raw` reports the offset it actually
            // reached; the count was previously dropped on the floor here
            // (`let (val, _) = …`), which is census finding A.
            type_str
                if type_str.starts_with("list<")
                    || type_str.starts_with("org.apache.cassandra.db.marshal.listtype(") =>
            {
                let element_type = self.extract_collection_element_type(raw_type_str, "list")?;
                self.parse_frozen_list_value_raw(data, 0, &element_type, column_name, depth + 1)
            }
            type_str
                if type_str.starts_with("set<")
                    || type_str.starts_with("org.apache.cassandra.db.marshal.settype(") =>
            {
                let element_type = self.extract_collection_element_type(raw_type_str, "set")?;
                self.parse_frozen_set_value_raw(data, 0, &element_type, column_name, depth + 1)
            }
            type_str
                if type_str.starts_with("map<")
                    || type_str.starts_with("org.apache.cassandra.db.marshal.maptype(") =>
            {
                let (key_type, value_type) = self.extract_map_types(raw_type_str)?;
                self.parse_frozen_map_value_raw(
                    data,
                    0,
                    &key_type,
                    &value_type,
                    column_name,
                    depth + 1,
                )
            }
            // Nested tuple inside a frozen collection element.
            // The caller (read_frozen_element) has already extracted the raw element bytes
            // into `data`, so there is no outer VUInt length here — just the sequence of
            // [i32 BE len][bytes] fields as written by serialize_value for Value::Tuple.
            // Issue #1081: also accept the marshal form `TupleType(...)`, extracting
            // element types from the original-case `raw_type_str`.
            type_str
                if type_str.starts_with("tuple<")
                    || type_str.starts_with("org.apache.cassandra.db.marshal.tupletype(") =>
            {
                let element_types = self.extract_tuple_element_types(raw_type_str)?;
                if element_types.is_empty() {
                    return Err(Error::schema(format!(
                        "Nested tuple element '{}': empty tuple type",
                        column_name
                    )));
                }
                let mut off = 0usize;
                let blob_end = data.len();
                let elements = self.parse_tuple_elements_raw(
                    data,
                    &mut off,
                    blob_end,
                    &element_types,
                    column_name,
                    depth + 1,
                )?;
                // Issue #3811 (census finding F): report where the element loop
                // ACTUALLY stopped, not the declared extent. A tuple whose trailing
                // components are absent leaves `off == blob_end` (TupleType.split
                // rule 1, legal); a partial component-length prefix leaves it SHORT,
                // and the bounded caller's assert turns that into the refusal
                // Cassandra makes at rule 2.
                Ok((Value::Tuple(elements), off))
            }
            // `vector<element, n>` / `VectorType(element , n)` — issue #4114.
            //
            // Sited BEFORE the UDT and unknown-type arms, which is the whole point:
            // a vector reaching either of those was DEGRADED — the unknown-type arm
            // returns `Value::Blob(data)` over what are exactly `4 * n` raw
            // big-endian binary32 bytes (`VectorType.java:94-96`, `:445-460`: no
            // length prefix, no element count, no per-element framing). That blob is
            // the right LENGTH here, so unlike the vint-framed sibling defect it
            // does not desync the row — it silently returns the WRONG TYPE, which is
            // the misdecode #4114 exists to remove.
            //
            // Reached with a vector when a multicell/frozen UDT field or a
            // collection element is declared as one: those field types come from the
            // on-disk `UserType(...)` marshal string, so the spelling arriving here
            // is `org.apache.cassandra.db.marshal.VectorType(…FloatType , 3)`.
            //
            // `data` is EXACTLY the value (the outer `[i32 len]` framing already
            // delimited it), so the exact-width rule applies and the consumed count
            // is the declared width — which the bounded caller's fully-consumed
            // assert then re-checks.
            type_str
                if type_str.starts_with("vector<")
                    || type_str
                        .starts_with("org.apache.cassandra.db.marshal.vectortype(")
                    || type_str.starts_with("vectortype(") =>
            {
                // Parse from `raw_type_str` (ORIGINAL case), never the lowercased
                // match binding: `marshal_vector_kind` matches the Java class name
                // `VectorType` CASE-SENSITIVELY, and the element it yields
                // (`FloatType`) is resolved case-sensitively too. This is the same
                // trap this function's header documents for the collection arms.
                let is_cql_spelling = type_str.starts_with("vector<");
                let kind = if is_cql_spelling {
                    crate::schema::vector_type::cql_vector_kind(raw_type_str)
                } else {
                    crate::schema::vector_type::marshal_vector_kind(raw_type_str)
                };
                // A malformed vector type is an ERROR naming the type, never a
                // fall-through to the blob arm below (roborev job 109).
                let args = kind.into_args(raw_type_str)?.ok_or_else(|| {
                    Error::corruption(format!(
                        "'{column_name}': type '{raw_type_str}' matched the vector \
                         arm but is not a vector type"
                    ))
                })?;
                // The element type comes from the DECLARED type via the ONE marshal
                // name authority (or `CqlType::parse` for the CQL spelling) — never
                // from the bytes (#28).
                let element = if is_cql_spelling {
                    crate::schema::CqlType::parse(args.element)?
                } else {
                    Self::native_marshal_to_cql_type(args.element).ok_or_else(|| {
                        Error::unsupported_format(format!(
                            "'{column_name}': vector element type '{}' in \
                             '{raw_type_str}' is not a recognised Cassandra scalar \
                             marshal type, so its width is unknown; refused rather \
                             than decoded as a blob (issue #4114)",
                            args.element
                        ))
                    })?
                };
                // AC4: an element type CQLite does not implement is refused BY NAME.
                let value = crate::schema::vector_type::vector_value::decode_framed_float_vector(
                    data,
                    &element,
                    args.dimension,
                    column_name,
                )?;
                Ok((value, data.len()))
            }
            // Issue #1081: accept BOTH the CQL short form (`frozen<...>`) and the
            // authoritative Cassandra marshal form
            // (`org.apache.cassandra.db.marshal.FrozenType(...)`). Collection/UDT
            // fields inside a multicell UDT must be frozen, and their field types
            // resolve from the on-disk `UserType(...)` marshal string where a frozen
            // field is spelled `FrozenType(...)` — e.g. `frozen<list<int>>` arrives
            // as `FrozenType(ListType(Int32Type))` and `frozen<some_udt>` as
            // `FrozenType(UserType(...))`. Without this arm those bypass the frozen
            // handling and fall through to the blob default. `extract_frozen_inner_type`
            // accepts both forms; we extract from `raw_type_str` (original case) so the
            // inner marshal type keeps its case and re-routes to the marshal
            // collection/UDT/scalar arms above on recursion.
            type_str
                if type_str.starts_with("frozen<")
                    || type_str.starts_with("org.apache.cassandra.db.marshal.frozentype(") =>
            {
                let inner_type = self.extract_frozen_inner_type(raw_type_str)?;
                let (inner, consumed) = self.parse_value_from_raw_bytes_reporting(
                    data,
                    &inner_type,
                    column_name,
                    depth + 1,
                )?;
                Ok((Value::Frozen(Box::new(inner)), consumed))
            }
            // UDT (User-Defined Type): delegate to parse_raw_type_value which has the full
            // UDT parsing logic including field count validation and nested type resolution.
            // The raw bytes representation is identical between the two function conventions.
            //
            // Issue #3811 AC3, census finding B: this arm — and its registry-resolved
            // sibling below — used to spell this `let (val, _offset) = …`, discarding
            // the very count `parse_raw_type_value` publishes. That discard is what
            // made trailing garbage and a partial component-length prefix both
            // silently acceptable.
            other if Self::is_udt_type(other) => {
                self.parse_raw_type_value(data, 0, type_str, column_name, depth)
            }
            other => {
                // Check if it's a short UDT name in the registry (e.g., "address_type").
                // This handles the case where parse_value_from_raw_bytes is called recursively
                // from the frozen<> arm with the stripped inner type (e.g., frozen<address_type>
                // → "address_type"). Since parse_raw_type_value already has a registry-lookup
                // fallback that correctly handles bare UDT names, we delegate there.
                // The byte-level encoding is identical: UDT fields use 4-byte i32 length prefixes
                // with no overall cell-level length prefix, so parse_raw_type_value offset=0 is
                // correct for already-extracted cell value bytes.
                // See Issue #481 regression fix.
                if let Some(ref registry) = self.udt_registry {
                    // ORIGINAL case (`type_str`), not the lowercased `other` match
                    // binding: the delegation below re-looks-up with `type_str` and
                    // `get_udt_qualified` bottoms out in a case-SENSITIVE map get, so
                    // a lowercased probe would fire this guard on keys the callee
                    // cannot resolve (and miss ones it can). The callee would then
                    // fall into its "parse as blob" path, which reads a VInt length
                    // prefix off UDT bytes — and since #3811 that returns a SHORT
                    // consumption, so what used to be a silently wrong `Value::Blob`
                    // becomes a refused (and therefore truncated) row. Same reasoning,
                    // same wording as `complex_column/cell_path_key.rs`'s probe.
                    if registry
                        .get_udt_qualified(&self.keyspace, type_str)
                        .is_some()
                    {
                        tracing::debug!(
                            "parse_value_from_raw_bytes: type '{}' for '{}' resolved as UDT via registry, delegating to parse_raw_type_value",
                            type_str,
                            column_name,
                        );
                        return self.parse_raw_type_value(data, 0, type_str, column_name, depth);
                    }
                }
                // Truly unknown type: fall back to blob.
                //
                // Issue #3811: this arm consumes the whole slice BY CONSTRUCTION, and
                // it says so with an explicit `data.len()` rather than an opt-out
                // sentinel. (The blob-degrade class itself is census finding G /
                // issue #3631 and is deliberately NOT this issue's subject.)
                tracing::debug!(
                    "parse_value_from_raw_bytes: unknown type '{}' for '{}', treating as blob ({} bytes)",
                    other,
                    column_name,
                    data.len()
                );
                Ok((
                    Value::Blob(crate::storage::sstable::reader::value_borrow::borrow_active(data)),
                    data.len(),
                ))
            }
        }
    }

    /// Issue #3811 (census finding F): a CELL-level frozen collection or tuple
    /// must consume its declared blob exactly.
    ///
    /// These decoders return `blob_end` as their offset because that offset is a
    /// STREAM POSITION the row loop advances by — it is not a consumption
    /// report, and it must keep being `blob_end`. The consequence, until now, was
    /// that bytes left inside the declared blob were *unobservable*: a
    /// `frozen<list<int>>` cell whose blob is four bytes longer than its elements
    /// require decoded clean and the row stayed byte-aligned, so nothing
    /// downstream ever noticed. Cassandra refuses it —
    /// `cassandra-5.0.8:src/java/org/apache/cassandra/serializers/ListSerializer.java:135`
    /// (`"Unexpected extraneous bytes after list value"`; the identical guard is
    /// `SetSerializer.java:127-128` and `MapSerializer.java:147`), and for tuples
    /// `TupleType.split` rule 4.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn require_frozen_extent(
        actual_end: usize,
        blob_end: usize,
        kind: &str,
        column_name: &str,
    ) -> Result<()> {
        if actual_end == blob_end {
            return Ok(());
        }
        // Symmetric with `require_fully_consumed` above: an OVER-read is
        // unreachable today (every element loop is bounded by `blob_end`), but a
        // `saturating_sub` would render it as "0 extraneous byte(s)", which is a
        // false statement in a corruption message rather than a missing one.
        if actual_end > blob_end {
            return Err(Error::corruption(format!(
                "Frozen {} '{}': decoded to offset {} but the declared blob ends at {} \
                 (over-read of {} byte(s))",
                kind,
                column_name,
                actual_end,
                blob_end,
                actual_end - blob_end
            )));
        }
        Err(Error::corruption(format!(
            "Frozen {} '{}': decoded to offset {} but the declared blob ends at {} \
             ({} extraneous byte(s) inside the cell value; Cassandra refuses this)",
            kind,
            column_name,
            actual_end,
            blob_end,
            blob_end - actual_end
        )))
    }

    /// Issue #3811 (census finding D): a fixed-width scalar needs its own width
    /// — or nothing at all.
    ///
    /// # The composed rule, which is not readable from this function alone
    ///
    /// The OVER-width half of finding D is enforced by the caller's consumption
    /// assert, not here: these arms report their exact width `n`, so a slice of
    /// `n + k` bytes leaves `k` unconsumed and
    /// [`require_fully_consumed`] refuses it. That is what stops a 5-byte
    /// declared `int` decoding from its first four bytes.
    ///
    /// Composed, the accepted set is therefore `{n, 0}`: `len == 0` ⇒
    /// [`FixedWidthCell::Null`] here and the arm reports **`0`** consumed, so the
    /// consumption assert passes; `len == n` ⇒ the value; `len` in `1..n` ⇒ `Err`
    /// here; `len == n + k` ⇒ consumed `n` ≠ `n + k` ⇒ `Err` there.
    ///
    /// # Issue #3847: the `0` is Cassandra's, and BOTH halves are load-bearing
    ///
    /// Until #3847 the composed set was exactly `{n}` — this guard was
    /// `data.len() < n`, which refuses the empty buffer, and the empty buffer is a
    /// LEGAL fixed-width value meaning `null` for every one of the twelve
    /// fixed-width scalars. The oracle, with its per-type table and the reason
    /// `deserialize()` rather than `validate()` governs a read path, is
    /// `docs/round-artifacts/issue-3847-cassandra-oracle.md` (pinned
    /// `cassandra-5.0.8`), restated in
    /// [`super::fixed_width`] — this path's table for that rule. Note it is ONE OF
    /// TWO in the repository: the typed/UDT path answers the same question from
    /// `typed_value/scalar_rules.rs::empty_is_a_value` since #3631. See that
    /// module's SCOPE note; nothing enforces the two staying in agreement.
    ///
    /// Relaxing this guard ALONE would be a defect and not the fix: the arm would
    /// then index `data[0]` on an empty slice, and would report `n` consumed
    /// against a `0`-length slice, so the consumption assert would refuse the
    /// value just admitted. Every caller of this function MUST short-circuit
    /// [`FixedWidthCell::Null`] to `(Value::Null, 0)`.
    fn require_fixed_width(
        data: &[u8],
        n: usize,
        what: &str,
        column_name: &str,
    ) -> Result<FixedWidthCell> {
        fixed_width::admissible_at_least(data, n).ok_or_else(|| {
            Error::corruption(format!(
                "Frozen element '{}': need {} byte(s) for {}, got {}",
                column_name,
                n,
                what,
                data.len()
            ))
        })
    }
}
