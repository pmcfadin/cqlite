//! Collection / tuple / UDT Arrow builders (feature = "arrow").
//!
//! Split out of `arrow_convert` (epic #1116 file-size split, issue #3096 Phase 0a)
//! with no behaviour change. Two families live here:
//!
//! * the **recursive, typed** element builders reached from
//!   [`build_typed_value_array`](super::arrow_typed_value::build_typed_value_array)
//!   when the authoritative `CqlType` is a `list`/`set`, `map`, `tuple` or UDT —
//!   they recurse back through that dispatcher for their element types;
//! * the **flat** (`DataType`-dispatched) `List`/`Map` builders used when no
//!   authoritative CQL type is available, which stringify their elements.
//!
//! Scalar builders live in [`super::arrow_builders_scalar`].

use super::arrow_convert::ArrowConvertError;
use super::arrow_convert_util::{
    checked_offset, checked_string_offsets, unwrap_frozen_value, Cells,
};
use super::arrow_schema::cql_type_to_arrow_data_type;
use super::arrow_typed_value::build_typed_value_array;
use crate::query::ColumnInfo;
use crate::schema::CqlType;
use crate::types::Value;
use crate::util::value_fmt::ValueFormatter;
use arrow::array::{ArrayRef, ListArray, MapArray, StringArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType as ArrowDataType, Field, Fields};
use std::sync::Arc;

// =========================================================================
// Recursive, typed collection builders (authoritative CqlType path)
// =========================================================================

// ----------------------------------------------------------------
// List and Set (recursive): element type dispatches back here.
// Arrow has no dedicated Set type; Set maps to List.
// ----------------------------------------------------------------
pub(super) fn build_typed_list_or_set_array(
    inner: &CqlType,
    values: &[Option<&Value>],
) -> Result<ArrayRef, ArrowConvertError> {
    let element_type = cql_type_to_arrow_data_type(inner);
    let item_field = Arc::new(Field::new("item", element_type, true));

    // Collect flat elements for all list/set values,
    // recording offsets so we can reconstruct the list structure.
    let mut offsets: Vec<i32> = vec![0];
    let mut flat_elements: Vec<Option<&Value>> = Vec::new();
    let mut null_bitmap: Vec<bool> = Vec::new();

    for opt in values {
        let v = unwrap_frozen_value(*opt);
        match v {
            Some(Value::List(items)) | Some(Value::Set(items)) => {
                null_bitmap.push(true);
                for item in items {
                    flat_elements.push(Some(item));
                }
                offsets.push(checked_offset(flat_elements.len())?);
            }
            Some(Value::Null) | None => {
                null_bitmap.push(false);
                offsets.push(checked_offset(flat_elements.len())?);
            }
            Some(other) => {
                return Err(ArrowConvertError::InvalidValue(format!(
                    "expected List/Set value, got {:?}",
                    other
                )));
            }
        }
    }

    // Recursively build the flat element array using the inner type.
    let elements_array = build_typed_value_array(inner, &flat_elements)?;

    let offset_buffer = OffsetBuffer::new(offsets.into());
    let null_buffer = NullBuffer::from(null_bitmap);

    Ok(Arc::new(ListArray::new(
        item_field,
        offset_buffer,
        elements_array,
        Some(null_buffer),
    )))
}

// ----------------------------------------------------------------
// Map: recursively typed keys and values.
//
// Arrow Map is represented as:
//   Map<Struct("entries") { key: K (non-nullable), value: V (nullable) }>
//
// We collect flat (key, value) pairs from all rows, track per-row
// offsets, recursively build the key and value arrays via the same
// recursive builder, then assemble a MapArray.
//
// Null key policy: a Value::Null in the key position is an error
// (Arrow MapArray requires non-nullable keys).  We return an error
// clearly rather than silently skip the entry.
// ----------------------------------------------------------------
pub(super) fn build_typed_map_array(
    key_type: &CqlType,
    val_type: &CqlType,
    values: &[Option<&Value>],
) -> Result<ArrayRef, ArrowConvertError> {
    let key_arrow = cql_type_to_arrow_data_type(key_type);
    let val_arrow = cql_type_to_arrow_data_type(val_type);

    let mut offsets: Vec<i32> = vec![0];
    let mut flat_keys: Vec<Option<&Value>> = Vec::new();
    let mut flat_vals: Vec<Option<&Value>> = Vec::new();
    let mut null_bitmap: Vec<bool> = Vec::new();

    for opt in values {
        let v = unwrap_frozen_value(*opt);
        match v {
            Some(Value::Map(pairs)) => {
                null_bitmap.push(true);
                for (k, val) in pairs {
                    // Keys must be non-nullable in Arrow MapArray.
                    if matches!(k, Value::Null) {
                        return Err(ArrowConvertError::InvalidValue(
                            "null key in map is not allowed in Arrow MapArray".to_string(),
                        ));
                    }
                    flat_keys.push(Some(k));
                    flat_vals.push(Some(val));
                }
                offsets.push(checked_offset(flat_keys.len())?);
            }
            Some(Value::Null) | None => {
                null_bitmap.push(false);
                offsets.push(checked_offset(flat_keys.len())?);
            }
            Some(other) => {
                return Err(ArrowConvertError::InvalidValue(format!(
                    "expected Map value, got {:?}",
                    other
                )));
            }
        }
    }

    // Recursively build the flat key and value arrays.
    let key_array = build_typed_value_array(key_type, &flat_keys)?;
    let val_array = build_typed_value_array(val_type, &flat_vals)?;

    // Build the entries StructArray (no validity buffer: all non-null).
    let struct_fields = Fields::from(vec![
        Field::new("key", key_arrow, false),
        Field::new("value", val_arrow, true),
    ]);
    let entries_array = StructArray::new(struct_fields.clone(), vec![key_array, val_array], None);

    let map_field = Arc::new(Field::new(
        "entries",
        ArrowDataType::Struct(struct_fields),
        false,
    ));
    let offset_buffer = OffsetBuffer::new(offsets.into());
    let null_buffer = NullBuffer::from(null_bitmap);

    Ok(Arc::new(MapArray::new(
        map_field,
        offset_buffer,
        entries_array,
        Some(null_buffer),
        false,
    )))
}

// ----------------------------------------------------------------
// Tuple<A, B, …>: Arrow Struct with positional field names.
//
// For each field position i, we collect per-row child values by
// indexing into the `Value::Tuple` element vector.  Rows whose
// tuple is shorter than the schema position, or whose top-level
// value is Null/absent, contribute None for that child position.
//
// Zero-field tuples (degenerate) fall back to Utf8.
// ----------------------------------------------------------------
pub(super) fn build_typed_tuple_array(
    element_types: &[CqlType],
    values: &[Option<&Value>],
) -> Result<ArrayRef, ArrowConvertError> {
    if element_types.is_empty() {
        // Degenerate case: no fields → Utf8 fallback. Still fail closed
        // on a wrong top-level variant; only a Tuple (or null) is valid.
        let arr: Vec<Option<String>> = values
            .iter()
            .map(|opt| match unwrap_frozen_value(*opt) {
                Some(Value::Null) | None => Ok(None),
                Some(v @ Value::Tuple(_)) => Ok(Some(ValueFormatter::format_value(v))),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "expected Tuple value, got {:?}",
                    other
                ))),
            })
            .collect::<Result<Vec<Option<String>>, ArrowConvertError>>()?;
        checked_string_offsets(&arr)?;
        return Ok(Arc::new(StringArray::from(arr)));
    }

    let n_rows = values.len();
    let n_fields = element_types.len();

    // Unwrap Frozen at the value level before inspecting tuples.
    let unwrapped: Vec<Option<&Value>> =
        values.iter().map(|opt| unwrap_frozen_value(*opt)).collect();

    // Fail closed: a non-null top-level value that is not a Tuple is a
    // type mismatch, not a null.  Mirror the scalar arms rather than
    // silently coercing the whole struct row's children to null.
    for v in unwrapped.iter() {
        match v {
            Some(Value::Tuple(_)) | Some(Value::Null) | None => {}
            Some(other) => {
                return Err(ArrowConvertError::InvalidValue(format!(
                    "expected Tuple value, got {:?}",
                    other
                )));
            }
        }
    }

    // Build a null bitmap: true = row is non-null (valid struct).
    let null_bitmap: Vec<bool> = unwrapped
        .iter()
        .map(|v| !matches!(v, Some(Value::Null) | None))
        .collect();

    // For each schema field position, build a Vec<Option<&Value>>
    // by pulling out the element at that position.
    //
    // IMPORTANT: absent/null positions must contribute `Some(&Value::Null)`
    // rather than `None`.  The scalar type builders use `?` on
    // `unwrap_frozen_value` which silently drops `None` entries via
    // `flatten()`, producing a shorter child array than the struct
    // expects.  Arrow's StructArray::new() panics on length mismatch.
    // Using `Some(&Value::Null)` keeps every row represented while
    // the builder treats it as a null element.
    let null_sentinel = Value::Null;
    let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(n_fields);
    for (field_idx, element_type) in element_types.iter().enumerate() {
        let child_values: Vec<Option<&Value>> = (0..n_rows)
            .map(|row_idx| {
                match unwrapped[row_idx] {
                    Some(Value::Tuple(items)) => {
                        // Missing trailing positions → null sentinel.
                        Some(
                            items
                                .get(field_idx)
                                .map(|v| v as &Value)
                                .unwrap_or(&null_sentinel),
                        )
                    }
                    // Null/absent row → null sentinel (wrong variants
                    // already failed closed above).
                    _ => Some(&null_sentinel),
                }
            })
            .collect();
        let child_arr = build_typed_value_array(element_type, &child_values)?;
        child_arrays.push(child_arr);
    }

    let struct_fields: Fields = Fields::from(
        element_types
            .iter()
            .enumerate()
            .map(|(i, t)| Field::new(format!("field_{i}"), cql_type_to_arrow_data_type(t), true))
            .collect::<Vec<_>>(),
    );

    let null_buffer = NullBuffer::from(null_bitmap);
    Ok(Arc::new(StructArray::new(
        struct_fields,
        child_arrays,
        Some(null_buffer),
    )))
}

// ----------------------------------------------------------------
// Udt: Arrow Struct with the UDT's schema field names.
//
// The CQL type carries the schema field order and types.  For each
// schema field, we look up the matching UdtField by name in each
// row's Value::Udt.  Missing fields and fields whose value is None
// (unset) become null in the child array.
//
// Zero-field UDTs fall back to Utf8.
// ----------------------------------------------------------------
pub(super) fn build_typed_udt_array(
    udt_fields: &[(String, CqlType)],
    values: &[Option<&Value>],
) -> Result<ArrayRef, ArrowConvertError> {
    if udt_fields.is_empty() {
        // Degenerate case (incl. unresolved named UDTs, which carry an
        // empty field list): Utf8 fallback. Still fail closed on a wrong
        // top-level variant; only a Udt (or null) is valid.
        let arr: Vec<Option<String>> = values
            .iter()
            .map(|opt| match unwrap_frozen_value(*opt) {
                Some(Value::Null) | None => Ok(None),
                Some(v @ Value::Udt(_)) => Ok(Some(ValueFormatter::format_value(v))),
                Some(other) => Err(ArrowConvertError::InvalidValue(format!(
                    "expected Udt value, got {:?}",
                    other
                ))),
            })
            .collect::<Result<Vec<Option<String>>, ArrowConvertError>>()?;
        checked_string_offsets(&arr)?;
        return Ok(Arc::new(StringArray::from(arr)));
    }

    let n_rows = values.len();

    // Unwrap Frozen at the value level before inspecting UDTs.
    let unwrapped: Vec<Option<&Value>> =
        values.iter().map(|opt| unwrap_frozen_value(*opt)).collect();

    // Fail closed: a non-null top-level value that is not a Udt is a
    // type mismatch, not a null.  Mirror the scalar arms rather than
    // silently coercing the whole struct row's children to null.
    for v in unwrapped.iter() {
        match v {
            Some(Value::Udt(_)) | Some(Value::Null) | None => {}
            Some(other) => {
                return Err(ArrowConvertError::InvalidValue(format!(
                    "expected Udt value, got {:?}",
                    other
                )));
            }
        }
    }

    // Build a null bitmap: true = row is non-null (valid struct).
    let null_bitmap: Vec<bool> = unwrapped
        .iter()
        .map(|v| !matches!(v, Some(Value::Null) | None))
        .collect();

    // For each schema field, build a child array by looking up the
    // field by name in each row's UdtValue.
    //
    // IMPORTANT: absent/null positions must contribute `Some(&Value::Null)`
    // rather than `None`.  See the Tuple arm above for the explanation.
    let null_sentinel = Value::Null;
    let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(udt_fields.len());
    for (field_name, field_type) in udt_fields.iter() {
        let child_values: Vec<Option<&Value>> = (0..n_rows)
            .map(|row_idx| match unwrapped[row_idx] {
                Some(Value::Udt(udt_val)) => {
                    // Look up by field name; null sentinel if absent or unset.
                    Some(
                        udt_val
                            .fields
                            .iter()
                            .find(|f| &f.name == field_name)
                            .and_then(|f| f.value.as_ref().map(|v| v as &Value))
                            .unwrap_or(&null_sentinel),
                    )
                }
                // Null/absent row → null sentinel (wrong variants
                // already failed closed above).
                _ => Some(&null_sentinel),
            })
            .collect();
        let child_arr = build_typed_value_array(field_type, &child_values)?;
        child_arrays.push(child_arr);
    }

    let struct_fields: Fields = Fields::from(
        udt_fields
            .iter()
            .map(|(field_name, field_type)| {
                Field::new(
                    field_name.as_str(),
                    cql_type_to_arrow_data_type(field_type),
                    true,
                )
            })
            .collect::<Vec<_>>(),
    );

    let null_buffer = NullBuffer::from(null_bitmap);
    Ok(Arc::new(StructArray::new(
        struct_fields,
        child_arrays,
        Some(null_buffer),
    )))
}

/// Custom / unknown CQL types: the `Utf8` textual fallback.
pub(super) fn build_typed_custom_array(
    values: &[Option<&Value>],
) -> Result<ArrayRef, ArrowConvertError> {
    let arr: Vec<Option<String>> = values
        .iter()
        .map(|opt| match opt {
            Some(Value::Null) | None => None,
            Some(v) => Some(ValueFormatter::format_value(v)),
        })
        .collect();
    checked_string_offsets(&arr)?;
    Ok(Arc::new(StringArray::from(arr)))
}

// =========================================================================
// Flat (DataType-dispatched) collection builders
// =========================================================================

pub(super) fn build_list_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    // For lists/sets, we serialize elements as strings for simplicity
    let mut offsets: Vec<i32> = vec![0];
    let mut values: Vec<Option<String>> = Vec::new();
    let mut null_bitmap: Vec<bool> = Vec::new();

    for cell in cells {
        match unwrap_frozen_value(*cell) {
            Some(Value::List(items)) | Some(Value::Set(items)) => {
                null_bitmap.push(true);
                for item in items {
                    values.push(Some(ValueFormatter::format_value(item)));
                }
                offsets.push(checked_offset(values.len())?);
            }
            Some(Value::Null) | None => {
                null_bitmap.push(false);
                offsets.push(checked_offset(values.len())?);
            }
            Some(other) => {
                return Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected List/Set value, got {:?}",
                    col.name, other
                )));
            }
        }
    }

    checked_string_offsets(&values)?;
    let values_array = Arc::new(StringArray::from(values)) as ArrayRef;
    let field = Arc::new(Field::new("item", ArrowDataType::Utf8, true));
    let offset_buffer = OffsetBuffer::new(offsets.into());
    let null_buffer = NullBuffer::from(null_bitmap);

    Ok(Arc::new(ListArray::new(
        field,
        offset_buffer,
        values_array,
        Some(null_buffer),
    )))
}

pub(super) fn build_map_array(
    col: &ColumnInfo,
    cells: Cells,
) -> Result<ArrayRef, ArrowConvertError> {
    // For maps, serialize key-value pairs as structs
    let mut offsets: Vec<i32> = vec![0];
    let mut keys: Vec<Option<String>> = Vec::new();
    let mut values: Vec<Option<String>> = Vec::new();
    let mut null_bitmap: Vec<bool> = Vec::new();

    for cell in cells {
        match unwrap_frozen_value(*cell) {
            Some(Value::Map(pairs)) => {
                null_bitmap.push(true);
                for (k, v) in pairs {
                    keys.push(Some(ValueFormatter::format_value(k)));
                    values.push(Some(ValueFormatter::format_value(v)));
                }
                offsets.push(checked_offset(keys.len())?);
            }
            Some(Value::Null) | None => {
                null_bitmap.push(false);
                offsets.push(checked_offset(keys.len())?);
            }
            Some(other) => {
                return Err(ArrowConvertError::InvalidValue(format!(
                    "column '{}': expected Map value, got {:?}",
                    col.name, other
                )));
            }
        }
    }

    // Build struct array for entries
    checked_string_offsets(&keys)?;
    checked_string_offsets(&values)?;
    let key_array = Arc::new(StringArray::from(keys)) as ArrayRef;
    let value_array = Arc::new(StringArray::from(values)) as ArrayRef;

    let struct_fields = Fields::from(vec![
        Field::new("key", ArrowDataType::Utf8, false),
        Field::new("value", ArrowDataType::Utf8, true),
    ]);

    let entries_array = StructArray::new(struct_fields.clone(), vec![key_array, value_array], None);

    let map_field = Arc::new(Field::new(
        "entries",
        ArrowDataType::Struct(struct_fields),
        false,
    ));
    let offset_buffer = OffsetBuffer::new(offsets.into());
    let null_buffer = NullBuffer::from(null_bitmap);

    Ok(Arc::new(MapArray::new(
        map_field,
        offset_buffer,
        entries_array,
        Some(null_buffer),
        false,
    )))
}
