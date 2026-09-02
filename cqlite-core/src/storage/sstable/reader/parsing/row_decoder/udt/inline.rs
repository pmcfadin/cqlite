//! Issue #3811 — the INLINE-field UDT decoder, split out of `udt.rs` (campsite
//! rule, epic #1116: that file is more than twice the 800-line source target)
//! and given the consumption contract census finding C left open.
//!
//! # Why the check lives INSIDE this function rather than at its call sites
//!
//! `parse_inline_udt_value` is reached from **14 call sites** across `udt.rs`
//! and `raw_type_value.rs`, and EVERY one of them hands it a
//! `&data[off..off + field_len]` — an exactly-bounded UDT component carved by
//! `checked_component_len`. There is no caller that passes a longer slice and
//! wants a short read. Enforcing `consumed == data.len()` at those 14 sites
//! would be precisely the "every caller must remember to ask" shape issue
//! #3811's AC2 forbids; enforcing it here, at the one place the count exists,
//! is what makes a NEW call site inherit the rule for free.
//!
//! # The oracle, and what stays LEGAL
//!
//! `cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/TupleType.java`
//! static `split(...)` (`UserType extends TupleType`). Rule 1 governs the
//! accepting case and is deliberately preserved: `position == length` before a
//! component is a **legal short return** — the trailing fields are simply absent
//! and read as implicit null, which is how a UDT that gained fields after the
//! row was written still decodes. Only `position < length` after the loop
//! (rule 4, trailing bytes) or 1–3 stray bytes with a component still to read
//! (rule 2, a partial component-length header, which this loop `break`s on
//! WITHOUT advancing past) are corruption. Both surface as the same observable:
//! a consumed count short of `data.len()`. This is therefore a CONSUMPTION
//! COMPARISON, never an "all declared fields must be present" assertion.

use super::super::*;

impl V5CompressedLegacyParser {
    /// Parse a UDT using inline field definitions from CqlType::Udt
    /// Used when we have inline type info but no registry entry (Issue #239)
    ///
    /// This handles the case where a UDT contains a nested UDT field, and the
    /// nested UDT's field definitions are available inline in the CqlType structure
    /// (parsed from the Statistics.db type string) rather than from the UdtRegistry.
    pub(in crate::storage::sstable::reader::parsing::row_decoder) fn parse_inline_udt_value(
        &self,
        data: &[u8],
        type_name: &str,
        inline_fields: &[(String, CqlType)],
        depth: usize,
    ) -> Result<Value> {
        if depth > MAX_TYPE_NESTING_DEPTH {
            return Err(Error::corruption(format!(
                "UDT nesting depth {} exceeds maximum {}",
                depth, MAX_TYPE_NESTING_DEPTH
            )));
        }

        let mut current_offset = 0;
        let mut fields = Vec::with_capacity(inline_fields.len());

        for (field_name, field_type) in inline_fields {
            // Check bounds for field length (4 bytes BE i32)
            if current_offset + 4 > data.len() {
                // Trailing fields are implicit null
                while fields.len() < inline_fields.len() {
                    let remaining_field = &inline_fields[fields.len()];
                    fields.push(UdtField {
                        name: remaining_field.0.clone(),
                        value: None,
                    });
                }
                break;
            }

            // Read field length (4 bytes big-endian i32)
            let field_len = i32::from_be_bytes([
                data[current_offset],
                data[current_offset + 1],
                data[current_offset + 2],
                data[current_offset + 3],
            ]);
            current_offset += 4;

            let field_value = if field_len == -1 {
                // Null field
                None
            } else if field_len == 0 {
                // Empty value, through THE ONE UDT-field decoder (#3722). This used to
                // call `parse_simple_udt_field_value`, one of the two divergent decoders
                // that issue removed, whose fallback answered most types with an opaque
                // `Value::Blob`. `udt_field_value` then collapses a decoded
                // `Value::Null` to `None`, so a 0-length null and a -1 null are ONE
                // representation.
                let value = self.parse_udt_field_value(&[], field_type, depth)?;
                Self::udt_field_value(value)
            } else {
                let field_len =
                    Self::checked_component_len(field_len, field_name, current_offset, data.len())?;

                let field_data = &data[current_offset..current_offset + field_len];
                current_offset += field_len;

                // Handle nested UDTs using inline field definitions (Issue #239)
                let value = match field_type {
                    CqlType::Udt(nested_name, nested_fields) if !nested_fields.is_empty() => {
                        // Recursively parse nested UDT using its inline fields
                        self.parse_inline_udt_value(
                            field_data,
                            nested_name,
                            nested_fields,
                            depth + 1,
                        )?
                    }
                    CqlType::Frozen(inner) => match inner.as_ref() {
                        CqlType::Udt(nested_name, nested_fields) if !nested_fields.is_empty() => {
                            // Frozen nested UDT - unwrap and parse. `depth + 2`: this
                            // manual unwrap consumes BOTH the `Frozen` and the `Udt`,
                            // which is what the consolidated decoder charges when it
                            // walks the same shape itself (roborev, #3722).
                            let inner_value = self.parse_inline_udt_value(
                                field_data,
                                nested_name,
                                nested_fields,
                                depth + 2,
                            )?;
                            Value::Frozen(Box::new(inner_value))
                        }
                        _ => {
                            // Other frozen types, through THE ONE decoder (#3722).
                            // `depth + 1`, NOT + 2: only the `Frozen` is consumed here.
                            // The inner type is a NON-UDT handed to the consolidated
                            // decoder, which charges its own levels. Charging two
                            // rejected otherwise-valid values one level early, and was
                            // my own error porting this during the main merge (roborev).
                            let inner_value =
                                self.parse_udt_field_value(field_data, inner, depth + 1)?;
                            Value::Frozen(Box::new(inner_value))
                        }
                    },
                    _ => self.parse_udt_field_value(field_data, field_type, depth)?,
                };
                Self::udt_field_value(value)
            };

            fields.push(UdtField {
                name: field_name.clone(),
                value: field_value,
            });
        }

        // Issue #3811 (census finding C): `data` IS the whole UDT value, so the
        // field loop must have reached its end. See the module header for why this
        // is here and not at the 14 call sites, and for why rule 1 still passes.
        Self::require_fully_consumed_raw(current_offset, data.len(), type_name, "inline UDT")?;
        Ok(Value::Udt(Box::new(UdtValue {
            type_name: type_name.to_string(),
            keyspace: self.keyspace.clone(),
            fields,
        })))
    }
}
