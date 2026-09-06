//! The two CELL-PATH-BEARING multicell collection cell writers — SET and MAP.
//!
//! Campsite split out of `complex.rs` (epic #1116) by issue #4106, which had to
//! grow `write_set_complex_cells` and could not grow a file already ~200 lines
//! over the ratchet ceiling. The responsibility line is Cassandra's own: these
//! are the two collections whose CELL PATH carries a component of the declared
//! type (`nameComparator()` — the keys type for a `MapType`, the elements type
//! for a `SetType`, `schema/ColumnMetadata.java:457-467` at `cassandra-5.0.8`),
//! so both serialize their path through the schema-aware
//! [`super::cell_path`] and both must thread the column's declared type down to
//! reach it. `write_list_complex_cells` stays in `complex.rs`: a LIST's cell path
//! is a generated 16-byte TimeUUID, not a declared-type component, so it needs no
//! declared type and shares nothing with these two.
//!
//! `use super::*` pulls the shared writer types, serialization/schema helpers,
//! flag constants and crate imports re-exported from `data_writer/mod.rs`. No
//! emitted bytes change from the move.

use super::*;

impl DataWriter {
    /// Write SET complex cells.
    ///
    /// SET elements: cell_path = serialized element value, cell value = empty (HAS_EMPTY_VALUE).
    /// Elements are ordered by the element type's Cassandra `SetType` comparator (#1275).
    ///
    /// `set_data_type` is the COLUMN's DECLARED type, threaded down for exactly
    /// the reason the MAP sibling threads its own: the cell path is the one write
    /// position where an empty-buffer sentinel is legal, and legality there
    /// depends on the declared ELEMENT type, which a bare `Value` cannot supply
    /// (#4106; the shared doctrine is in [`super::cell_path`]'s module header).
    pub(super) fn write_set_complex_cells(
        &self,
        buf: &mut Vec<u8>,
        value: &Value,
        set_data_type: &str,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
        now_seconds: i32,
    ) -> Result<()> {
        let elements = match value {
            Value::Set(elements) => elements,
            _ => {
                return Err(Error::InvalidInput(format!(
                    "Expected Set value for complex SET column, got {:?}",
                    value
                )))
            }
        };

        // Order by the element type's Cassandra `SetType` comparator (#1275, see
        // collection_order: SIGNED numerics, unsigned-byte otherwise) decided from
        // the element `Value`s. Null is rejected by the cell-path serializer.
        let mut ordered: Vec<&Value> = elements.iter().collect();
        ordered.sort_by(|a, b| compare_collection_elements(a, b));
        let mut serialized: Vec<Vec<u8>> = Vec::with_capacity(ordered.len());
        for element in &ordered {
            let mut path = Vec::new();
            serialize_set_cell_path_element_into(element, set_data_type, &mut path)?;
            serialized.push(path);
        }

        encode_unsigned(serialized.len() as u64, buf); // cell count
        for path_bytes in &serialized {
            // Cell header: flags + optional TTL fields
            self.write_complex_cell_header(
                buf,
                CELL_HAS_EMPTY_VALUE,
                timestamp_micros,
                ttl_seconds,
                now_seconds,
            )?;

            // Cell path: serialized element value
            encode_unsigned(path_bytes.len() as u64, buf);
            buf.extend_from_slice(path_bytes);
            // No value bytes (HAS_EMPTY_VALUE flag set)
        }

        Ok(())
    }

    /// Write MAP complex cells.
    ///
    /// MAP entries: cell_path = serialized key, cell value = serialized value.
    /// Entries are sorted by their serialized key byte representation for Cassandra compatibility.
    ///
    /// `map_data_type` is the COLUMN's DECLARED type (e.g. `map<int, int>`),
    /// threaded down so the cell path can admit an empty-buffer sentinel against
    /// the declared KEY type (#3805; doctrine in [`super::cell_path`]). The cell
    /// VALUE deliberately keeps the type-blind [`serialize_value_into`], which
    /// REFUSES a sentinel: a zero-byte map VALUE is not a sentinel, it is the
    /// empty value of the value type — or, with `HAS_EMPTY_VALUE`, a null.
    pub(super) fn write_map_complex_cells(
        &self,
        buf: &mut Vec<u8>,
        value: &Value,
        map_data_type: &str,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
        now_seconds: i32,
    ) -> Result<()> {
        let entries = match value {
            Value::Map(entries) => entries,
            _ => {
                return Err(Error::InvalidInput(format!(
                    "Expected Map value for complex MAP column, got {:?}",
                    value
                )))
            }
        };

        // Order by the KEY type's Cassandra `MapType` comparator (#1275, see
        // collection_order: SIGNED numerics so negative keys sort -1 before 0/1,
        // unsigned-byte otherwise) from the key `Value`s. Null keys rejected inline.
        let mut ordered: Vec<&(Value, Value)> = entries.iter().collect();
        ordered.sort_by(|a, b| compare_collection_elements(&a.0, &b.0));

        // Reusable per-entry scratch (issue #1672): one alloc for the whole map,
        // not a Vec-of-Vecs holding every key/value.
        encode_unsigned(ordered.len() as u64, buf); // cell count
        let mut key_scratch = Vec::new();
        let mut val_scratch = Vec::new();
        for (key, val) in ordered {
            if matches!(key, Value::Null) {
                return Err(Error::InvalidInput(
                    "MAP keys cannot be null (CQL semantics)".to_string(),
                ));
            }

            // Cell header: flags + optional TTL fields
            self.write_complex_cell_header(buf, 0, timestamp_micros, ttl_seconds, now_seconds)?;

            // Cell path: serialized key. SCHEMA-AWARE, because this is the one
            // position an empty-buffer sentinel may occupy (issue #3805) and its
            // tag must be validated against the DECLARED key type.
            key_scratch.clear();
            serialize_map_cell_path_key_into(key, map_data_type, &mut key_scratch)?;
            encode_unsigned(key_scratch.len() as u64, buf);
            buf.extend_from_slice(&key_scratch);

            // Cell value: serialized value
            val_scratch.clear();
            serialize_value_into(val, &mut val_scratch)?;
            encode_unsigned(val_scratch.len() as u64, buf);
            buf.extend_from_slice(&val_scratch);
        }

        Ok(())
    }
}
