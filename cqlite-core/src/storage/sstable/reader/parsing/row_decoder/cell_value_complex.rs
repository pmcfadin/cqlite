//! Complex value-decode ladder for the V5CompressedLegacy per-cell decoder.
//!
//! Campsite split of `cell_value.rs` (issue #1795): this file owns the
//! frozen / tuple / non-frozen-collection / marshal-UDT / `varint` / unknown-scalar
//! decode ladder ([`CellKind::Complex`]); the scalar arms live in
//! `cell_value_scalar.rs` and the flag/conditional-field parsing in `cell_value.rs`.
//!
//! The blob/`varint` fall-throughs route their length prefix through
//! [`super::V5CompressedLegacyParser::read_vint_length_prefixed_bytes`] for
//! overflow-safe bounds checks (issue #1795).

use super::marshal_element::MarshalCollectionElements;
use super::*;

impl V5CompressedLegacyParser {
    /// Decode a live cell value for a [`CellKind::Complex`] column: frozen types,
    /// tuples, non-frozen collections, marshal-form frozen UDTs, CQL `varint`, and
    /// the unknown-scalar blob fall-through. `lowered` is the already-lowercased
    /// declared type string. Advances `offset` past the consumed value bytes.
    pub(super) fn decode_complex_cell_value(
        &self,
        data: &[u8],
        offset: &mut usize,
        lowered: &str,
        column: &crate::schema::Column,
        header_type: Option<&str>,
        reader: &crate::storage::sstable::reader::types::SSTableReader,
    ) -> Result<Value> {
        let mut off = *offset;
        let type_str: &str = lowered;
        let value = if type_str.starts_with("frozen<") {
            // Frozen types: unwrap inner type and route to appropriate parser
            let inner_type = self.extract_frozen_inner_type(type_str)?;

            tracing::debug!(
                "V5CompressedLegacy: Parsing frozen type '{}' -> inner type '{}'",
                type_str,
                inner_type
            );

            // Issue #1340: extract the AUTHORITATIVE marshal element type(s)
            // from the on-disk SerializationHeader marshal type (`header_type`).
            // When an element is a `frozen<UDT>`, threading the marshal type lets
            // it decode to a typed `Value::Frozen(Value::Udt)` registry-free
            // (precedence: header marshal → registry → Blob, no byte-pattern
            // inference — no-heuristics #28). Extracted once per frozen cell
            // (before the element loop); the result borrows from `header_type`,
            // so the per-element loop is allocation-free.
            let marshal_elems = header_type.and_then(Self::extract_marshal_collection_elements);
            // Shared for list & set (both are `Sequence`): the borrowed element
            // marshal type, or `None` for a map / absent / mismatched marshal.
            let sequence_marshal_elem = match &marshal_elems {
                Some(MarshalCollectionElements::Sequence(m)) => Some(*m),
                _ => None,
            };

            // Route to appropriate frozen collection parser
            let (inner_value, new_offset) = if inner_type.starts_with("list<") {
                let schema_elem = self.extract_collection_element_type(&inner_type, "list")?;
                let element_type =
                    Self::prefer_udt_marshal_element(sequence_marshal_elem, &schema_elem);
                self.parse_frozen_list_value(data, off, element_type, column)?
            } else if inner_type.starts_with("set<") {
                let schema_elem = self.extract_collection_element_type(&inner_type, "set")?;
                let element_type =
                    Self::prefer_udt_marshal_element(sequence_marshal_elem, &schema_elem);
                self.parse_frozen_set_value(data, off, element_type, column)?
            } else if inner_type.starts_with("map<") {
                let (schema_key, schema_val) = self.extract_map_types(&inner_type)?;
                let (marshal_key, marshal_val) = match &marshal_elems {
                    Some(MarshalCollectionElements::Map(k, v)) => (Some(*k), Some(*v)),
                    _ => (None, None),
                };
                // Same shared rule as the MULTICELL map reader uses, so the two
                // cannot form two opinions about a map key's decode type (issue
                // #3612, roborev round 8 finding 1). A NO-OP here in both branches
                // by construction: this side's marshal key never carries the outer
                // `FrozenType` (Cassandra omits it inside a frozen collection) and
                // its schema branch is untouched — wired anyway so the rule has ONE
                // home.
                let key_type = Self::map_key_type_for_decode(marshal_key, &schema_key);
                let value_type = Self::prefer_udt_marshal_element(marshal_val, &schema_val);
                self.parse_frozen_map_value(data, off, &key_type, value_type, column)?
            } else if Self::is_udt_type(&column.data_type) {
                // Frozen UDT - parse using UDT parser
                // The column.data_type contains the full Cassandra type string including UserType
                tracing::debug!(
                    "V5CompressedLegacy: Parsing frozen UDT column '{}' type='{}'",
                    column.name,
                    column.data_type
                );

                // Parse UDT definition from the type string
                let udt_def = Self::parse_udt_type_definition(&column.data_type)?;

                // First read the VInt-prefixed blob length
                let (remaining, blob_len_raw) = parse_vuint(&data[off..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen UDT '{}': failed to parse blob length: {:?}",
                        column.name, e
                    ))
                })?;
                if blob_len_raw > MAX_CELL_VALUE_LENGTH {
                    return Err(Error::corruption(format!(
                        "Frozen UDT '{}': blob_len {} exceeds maximum {}",
                        column.name, blob_len_raw, MAX_CELL_VALUE_LENGTH
                    )));
                }
                let blob_len = blob_len_raw as usize;
                let bytes_consumed = data[off..].len() - remaining.len();
                off += bytes_consumed;

                if blob_len > data.len().saturating_sub(off) {
                    return Err(Error::corruption(format!(
                        "Frozen UDT '{}': need {} bytes but only {} available",
                        column.name,
                        blob_len,
                        data.len() - off
                    )));
                }

                // Parse UDT value from the blob
                let udt_data = &data[off..off + blob_len];
                // The trailing `0` is the ROOT nesting depth, written here on purpose:
                // this is a COLUMN-level decode, so it genuinely starts the count.
                // `parse_udt_value` has no zero-depth overload precisely so that a
                // caller INSIDE a decode cannot pick one by accident (issue #3631).
                let (udt_value, n) = self.parse_udt_value(udt_data, 0, &udt_def, column, 0)?;
                // #3811 (finding C): `parse_udt_value` REPORTS; this caller dropped it,
                // so a frozen UDT blob with trailing bytes or a partial trailing field
                // header was accepted where the collection and tuple paths refuse.
                Self::require_fully_consumed(n, udt_data.len(), &column.name, "frozen UDT")?;
                off += blob_len;

                (udt_value, off)
            } else if let Some(udt_def) = self
                .udt_registry
                .as_ref()
                .and_then(|reg| reg.get_udt_qualified(&self.keyspace, &inner_type).cloned())
            {
                // frozen<short_udt_name>: look up the concrete UDT definition in the
                // registry (Issue #502).  This handles type strings like
                // `frozen<person>` where "person" is a registered UDT rather than a
                // collection or a full marshal-format UserType string.
                tracing::debug!(
                    "V5CompressedLegacy: Resolving frozen UDT '{}' via registry for column '{}'",
                    inner_type,
                    column.name,
                );

                // Read VUInt-prefixed blob length (same framing as tuple and
                // marshal-format UDT cells).
                let (remaining, blob_len_raw) = parse_vuint(&data[off..]).map_err(|e| {
                    Error::corruption(format!(
                        "Frozen UDT '{}' (column '{}'): failed to parse blob length: {:?}",
                        inner_type, column.name, e
                    ))
                })?;
                if blob_len_raw > MAX_CELL_VALUE_LENGTH {
                    return Err(Error::corruption(format!(
                        "Frozen UDT '{}' (column '{}'): blob_len {} exceeds maximum {}",
                        inner_type, column.name, blob_len_raw, MAX_CELL_VALUE_LENGTH
                    )));
                }
                let blob_len = blob_len_raw as usize;
                let len_bytes_consumed = data[off..].len() - remaining.len();
                off += len_bytes_consumed;

                if blob_len > data.len().saturating_sub(off) {
                    return Err(Error::corruption(format!(
                        "Frozen UDT '{}' (column '{}'): need {} bytes but only {} available",
                        inner_type,
                        column.name,
                        blob_len,
                        data.len() - off
                    )));
                }

                let udt_data = &data[off..off + blob_len];
                // The trailing `0` is the ROOT nesting depth, written here on purpose:
                // this is a COLUMN-level decode, so it genuinely starts the count.
                // `parse_udt_value` has no zero-depth overload precisely so that a
                // caller INSIDE a decode cannot pick one by accident (issue #3631).
                let (udt_value, n) = self.parse_udt_value(udt_data, 0, &udt_def, column, 0)?;
                // #3811 (finding C): `parse_udt_value` REPORTS; this caller dropped it,
                // so a frozen UDT blob with trailing bytes or a partial trailing field
                // header was accepted where the collection and tuple paths refuse.
                Self::require_fully_consumed(n, udt_data.len(), &column.name, "frozen UDT")?;
                off += blob_len;

                (udt_value, off)
            } else if let Some(ht) =
                header_type.filter(|ht| Self::marshal_is_top_level_frozen_udt(ht))
            {
                // Issue #1080: NO UdtRegistry is wired and the supplied schema
                // short form `frozen<person_type>` carries no field defs, but
                // the AUTHORITATIVE on-disk SerializationHeader marshal type for
                // this column is the full
                // `FrozenType(UserType(ks,hexname,field:Type,...))`. Decode the
                // UDT STRUCTURALLY from that header type (no guessing, issue #28)
                // rather than dropping the column (which also broke the Err→break
                // loop, silently losing all trailing columns).
                self.decode_frozen_udt_from_header_type(data, off, ht, column)?
            } else {
                // Detect bare identifiers that look like unregistered UDT names.
                // A bare identifier has no '<' (not a container or tuple) and does not
                // match any known CQL primitive type.  If we reach this branch with
                // such an identifier it means the UDT was not in the registry — return
                // an actionable schema error rather than silently producing a Blob.
                //
                // Legitimate fall-through types handled below:
                //   • tuple<...>  (contains '<')
                //   • known primitives: int, text, uuid, boolean, blob, float, double,
                //     decimal, varint, bigint, counter, timestamp, date, time, duration,
                //     inet, smallint, tinyint, varchar, ascii, timeuuid
                const KNOWN_PRIMITIVES: &[&str] = &[
                    "int",
                    "bigint",
                    "counter",
                    "smallint",
                    "tinyint",
                    "text",
                    "varchar",
                    "ascii",
                    "uuid",
                    "timeuuid",
                    "boolean",
                    "blob",
                    "float",
                    "double",
                    "decimal",
                    "varint",
                    "timestamp",
                    "date",
                    "time",
                    "duration",
                    "inet",
                ];
                let is_container = inner_type.contains('<');
                let is_primitive = KNOWN_PRIMITIVES.contains(&inner_type.as_str());
                if !is_container && !is_primitive {
                    // Bare identifier that is neither a container nor a primitive —
                    // this is an unregistered UDT name.
                    return Err(Error::schema(format!(
                        "frozen<{inner}>: UDT '{inner}' not found in registry for keyspace '{}'; \
                         register it before reading",
                        self.keyspace,
                        inner = inner_type,
                    )));
                }
                // Non-collection / primitive frozen type — recurse normally.
                // The recursive call now returns 4 elements; we only need value + offset.
                let mut inner_column = column.clone();
                inner_column.data_type = inner_type.clone();
                let (inner_val, _inner_ts, _inner_exp, inner_off) = self
                    .parse_cell_value_schema_order(
                        data,
                        off,
                        &inner_column,
                        None,
                        // Frozen-inner recursion: resolve the tag locally from
                        // `inner_column.data_type` (bounded, off the per-cell scan
                        // hot path).
                        None,
                        reader,
                    )?;
                (inner_val, inner_off)
            };

            off = new_offset;

            // Wrap in Frozen
            Value::Frozen(Box::new(inner_value))
        } else if type_str.starts_with("tuple<") {
            // Tuple types: parse fixed number of elements
            self.parse_tuple_value(data, &mut off, type_str, column)?
        }
        // Non-frozen collections: list, set, map
        // TODO(Issue #162, Task 3): Multi-cell collection parsing
        //
        // Collections in V5CompressedLegacy are stored as MULTIPLE CELLS with path identifiers,
        // NOT as single blob values. The current single-cell parser cannot handle this.
        //
        // Format (from sstabledump analysis):
        //   {"name": "scores", "deletion_info": {...}},  // Collection tombstone
        //   {"name": "scores", "path": ["uuid1"], "value": 23},  // Element 1
        //   {"name": "scores", "path": ["uuid2"], "value": 99},  // Element 2
        //
        // Required implementation:
        //   1. Parse cell path (clustering key bytes) for each collection element
        //   2. Detect collection tombstone cell (has deletion_info, no path/value)
        //   3. Read N element cells (each with path + value)
        //   4. Aggregate elements into Value::List/Set/Map based on column type
        //   5. Handle different path encodings:
        //      - list<T>: path is UUID bytes (timeuuid for ordering)
        //      - set<T>: path is serialized element value (key), value is empty
        //      - map<K,V>: path is serialized key, value is serialized value
        //
        // This is a fundamental architectural change requiring cell-level parsing
        // before column-level aggregation. For now, return stub to unblock downstream work.
        else if type_str.starts_with("list<")
            || type_str.starts_with("set<")
            || type_str.starts_with("map<")
        {
            warn!(
                "V5CompressedLegacy: Non-frozen collection '{}' type '{}' requires multi-cell parsing (not yet implemented). \
                 Collections are stored as multiple cells with path identifiers, requiring cell-level aggregation. \
                 Returning empty collection as placeholder. See Issue #162 Task 3 for implementation plan.",
                column.name, column.data_type
            );

            // Return empty collection based on type
            if type_str.starts_with("list<") {
                Value::List(Vec::new())
            } else if type_str.starts_with("set<") {
                Value::Set(Vec::new())
            } else {
                Value::Map(Vec::new())
            }
        }
        // Issue #1080 / roborev job 1363: marshal-form frozen UDT. When the
        // schema is DERIVED FROM the on-disk header (rather than supplied as a
        // CQL short form), `column.data_type` is the authoritative marshal
        // string `org.apache.cassandra.db.marshal.FrozenType(...UserType...)`,
        // which does NOT start with CQL `frozen<` and so misses the arm above.
        // Decode it structurally from that marshal type (same authoritative
        // path as the supplied-schema header fallback) instead of blobbing it.
        // `marshal_is_top_level_frozen_udt` accepts ONLY a top-level
        // `FrozenType(UserType(...))` (NOT a frozen collection that contains a
        // UDT, e.g. `FrozenType(ListType(UserType(...)))` — roborev 1365), and a
        // non-frozen top-level UDT is routed to the complex branch by
        // `is_complex_column`, so reaching here means a single-cell frozen UDT
        // → wrap in `Value::Frozen` (consistent with the CQL `frozen<` arm).
        else if Self::marshal_is_top_level_frozen_udt(&column.data_type) {
            let (udt_value, new_offset) =
                self.decode_frozen_udt_from_header_type(data, off, &column.data_type, column)?;
            off = new_offset;
            Value::Frozen(Box::new(udt_value))
        }
        // Cassandra 5.0 `vector<element, n>` (issue #4114). Sited BEFORE the blob
        // fall-through, which is exactly what used to swallow it: a fixed-width
        // vector value carries NO length prefix, so the fall-through read the first
        // float's leading byte as a vint length and — depending on that ONE BYTE OF
        // USER DATA — either errored blaming the data or returned a WRONG VALUE at
        // exit 0 (`.drive-issue-4114/silent-misdecode-measurement.md`).
        //
        // `type_str` is the lowercased DECLARED type. Only the CQL spelling is
        // matched here because every header marshal type reaching a column's
        // `data_type` has already been normalized by the ONE conversion point,
        // `enhanced_statistics_parser::marshal_type::convert_marshal_type_to_cql`
        // (which now emits `vector<…, n>`); a marshal-spelled vector inside a UDT
        // field instead travels the `CqlType` route (`type_string.rs` ->
        // `typed_value.rs`).
        else if type_str.starts_with("vector<") {
            let args = crate::schema::vector_type::cql_vector_inner(type_str)
                .ok_or_else(|| {
                    Error::schema(format!(
                        "Cell '{}': malformed vector type '{}' (unterminated type parameters)",
                        column.name, type_str
                    ))
                })
                .and_then(|inner| crate::schema::vector_type::split_vector_args(inner, type_str))?;
            // AC4: the dimension comes from the type, and an element type CQLite
            // does not implement is refused BY NAME rather than decoded as
            // something else (#28).
            let element = crate::schema::CqlType::parse(args.element)?;
            crate::schema::vector_type::vector_value::require_float_element(
                &element,
                args.dimension,
            )?;
            crate::schema::vector_type::vector_value::decode_float_vector_at(
                data,
                &mut off,
                &column.name,
                args.dimension,
            )?
        }
        // CQL `varint`: [VInt len][big-endian two's-complement bytes]. Decode to
        // Value::Varint, matching the block path (issue #1885). Without this arm the
        // value fell through to the blob default and was mis-typed as Value::Blob.
        else if type_str == "varint" {
            let bytes = Self::read_vint_length_prefixed_bytes(data, &mut off, column, "varint")?;
            // Issue #1644: adapt the #1885 varint arm to the Bytes-backed enum
            // via the `Value::varint` constructor (preserves #1885's owned-copy
            // semantics; this arm was not part of #1644's zero-copy borrow scope).
            Value::varint(bytes.to_vec())
        }
        // Default: treat as VInt-length-prefixed blob (unknown scalar type).
        else {
            let bytes = Self::read_vint_length_prefixed_bytes(data, &mut off, column, "blob")?;
            // Issue #1644 (K5 stage 2): the complex blob default shares the scalar
            // blob arm's zero-copy borrow (mirrors the pre-split shared closure).
            Value::Blob(crate::storage::sstable::reader::value_borrow::borrow_active(bytes))
        };

        *offset = off;
        Ok(value)
    }
}
