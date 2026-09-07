//! FROZEN collection element framing (issue #2339).
//!
//! A frozen collection is stored as ONE opaque value produced by Cassandra's
//! `CollectionSerializer.serialize()` → `pack()`. Verified against the pinned
//! `cassandra-5.0.8` tag
//! (`src/java/org/apache/cassandra/serializers/CollectionSerializer.java`):
//!
//! ```text
//! writeCollectionSize(out, elements)  =>  out.putInt(elements)   // 4-byte i32 BE
//! writeValue(out, value)              =>  out.putInt(-1)         // null
//!                                     =>  out.putInt(size); bytes
//! ```
//!
//! So the body is `i32-BE count` followed by `count` (or `2 * count` for a map)
//! `i32-BE length`-prefixed values. That is NOT the framing
//! [`super::value_parsing`] uses for a NON-frozen (multicell) collection cell,
//! which is VInt-length-prefixed — the two shapes are genuinely different on
//! disk and must not share a decoder (`AbstractType.writeValue` vs the
//! multicell per-element cell layout).
//!
//! Nullability follows the pinned deserializers exactly:
//!   * `ListSerializer.deserialize` uses `readValue` and appends `null` for a
//!     `-1` length, so a frozen LIST element may be null.
//!   * `SetSerializer`/`MapSerializer.deserialize` use `readNonNullValue`, which
//!     raises `MarshalException` on `-1`, so a null set element or map key/value
//!     is CORRUPT and fails closed here (never silently coerced).
//!
//! Tuple/UDT field framing is the same 4-byte i32-BE shape
//! (`TupleType.buildValue`'s `accessor.putInt`) and already lives in
//! [`super::value_parsing::parse_tuple_value_with`] /
//! `parse_udt_value_with`; this module covers only the three COLLECTION kinds.

use crate::{types::ComparatorType, Error, Result, Value};

/// A caller's depth-tracking value decoder, handed to
/// [`parse_frozen_inner_with`] so the recursion (and its `#1632` depth budget)
/// stays owned by ONE place instead of being duplicated per frozen kind.
type DepthAwareDecoder<'a> = &'a dyn Fn(&[u8], &ComparatorType, usize) -> Result<Value>;

/// Upper bound on capacity pre-allocated from a declared element/entry count — a
/// corrupt huge count must not pre-allocate gigabytes (issue #1632). Mirrors
/// `value_parsing::REASONABLE_COLLECTION_CAPACITY`.
const REASONABLE_COLLECTION_CAPACITY: usize = 4096;

/// Read a 4-byte big-endian `i32` at `offset`, advancing it.
fn read_i32_be(data: &[u8], offset: &mut usize, what: &str) -> Result<i32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| Error::corruption(format!("Frozen {what}: length offset overflow")))?;
    if end > data.len() {
        return Err(Error::corruption(format!(
            "Frozen {what}: not enough bytes for a 4-byte length (need {end}, have {})",
            data.len()
        )));
    }
    let v = i32::from_be_bytes([
        data[*offset],
        data[*offset + 1],
        data[*offset + 2],
        data[end - 1],
    ]);
    *offset = end;
    Ok(v)
}

/// Read the `i32-BE` element/entry count (`writeCollectionSize`).
fn read_count(data: &[u8], offset: &mut usize, what: &str) -> Result<usize> {
    let count = read_i32_be(data, offset, &format!("{what} element count"))?;
    if count < 0 {
        return Err(Error::corruption(format!(
            "Frozen {what}: negative element count {count}"
        )));
    }
    Ok(count as usize)
}

/// Read one `i32-BE`-length-prefixed value body, returning `None` for the `-1`
/// null marker (`CollectionSerializer.readValue`).
fn read_element<'a>(data: &'a [u8], offset: &mut usize, what: &str) -> Result<Option<&'a [u8]>> {
    let len = read_i32_be(data, offset, what)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    let end = offset
        .checked_add(len)
        .ok_or_else(|| Error::corruption(format!("Frozen {what}: length offset overflow")))?;
    if end > data.len() {
        return Err(Error::corruption(format!(
            "Frozen {what}: declared {len} bytes but only {} remain",
            data.len() - *offset
        )));
    }
    let body = &data[*offset..end];
    *offset = end;
    Ok(Some(body))
}

/// Decode a frozen `list<E>` body: `i32-BE count` + `count` × (`i32-BE len` +
/// bytes), where `-1` is a null element (`ListSerializer.deserialize`).
pub(crate) fn parse_frozen_list_value_with<F>(
    data: &[u8],
    element_comparator: &ComparatorType,
    parse_element: F,
) -> Result<Value>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    Ok(Value::List(decode_elements(
        data,
        element_comparator,
        parse_element,
        "list",
        true,
    )?))
}

/// Decode a frozen `set<E>` body — same framing as a list, but a `-1` element is
/// CORRUPT (`SetSerializer.deserialize` uses `readNonNullValue`).
pub(crate) fn parse_frozen_set_value_with<F>(
    data: &[u8],
    element_comparator: &ComparatorType,
    parse_element: F,
) -> Result<Value>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    Ok(Value::Set(decode_elements(
        data,
        element_comparator,
        parse_element,
        "set",
        false,
    )?))
}

/// Decode a frozen `map<K, V>` body: `i32-BE count` + `count` × (key, value),
/// each `i32-BE len`-prefixed. Neither a key nor a value may be null
/// (`MapSerializer.deserialize` uses `readNonNullValue` for both).
pub(crate) fn parse_frozen_map_value_with<F>(
    data: &[u8],
    key_comparator: &ComparatorType,
    value_comparator: &ComparatorType,
    parse_element: F,
) -> Result<Value>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    let mut offset = 0usize;
    let count = read_count(data, &mut offset, "map")?;
    let mut entries = Vec::with_capacity(count.min(REASONABLE_COLLECTION_CAPACITY));
    for i in 0..count {
        let key_body =
            read_element(data, &mut offset, &format!("map key {i}"))?.ok_or_else(|| {
                Error::corruption(format!("Frozen map key {i}: null key is not permitted"))
            })?;
        let key = parse_element(key_body, key_comparator)?;
        let val_body =
            read_element(data, &mut offset, &format!("map value {i}"))?.ok_or_else(|| {
                Error::corruption(format!("Frozen map value {i}: null value is not permitted"))
            })?;
        let val = parse_element(val_body, value_comparator)?;
        entries.push((key, val));
    }
    reject_trailing(data, offset, "map")?;
    Ok(Value::Map(entries))
}

/// Shared list/set element loop. `nulls_allowed` distinguishes
/// `ListSerializer`'s `readValue` from `SetSerializer`'s `readNonNullValue`.
fn decode_elements<F>(
    data: &[u8],
    element_comparator: &ComparatorType,
    parse_element: F,
    kind: &str,
    nulls_allowed: bool,
) -> Result<Vec<Value>>
where
    F: Fn(&[u8], &ComparatorType) -> Result<Value>,
{
    let mut offset = 0usize;
    let count = read_count(data, &mut offset, kind)?;
    let mut out = Vec::with_capacity(count.min(REASONABLE_COLLECTION_CAPACITY));
    for i in 0..count {
        let what = format!("{kind} element {i}");
        match read_element(data, &mut offset, &what)? {
            Some(body) => out.push(parse_element(body, element_comparator)?),
            None if nulls_allowed => out.push(Value::Null),
            None => {
                return Err(Error::corruption(format!(
                    "Frozen {what}: null element is not permitted"
                )))
            }
        }
    }
    reject_trailing(data, offset, kind)?;
    Ok(out)
}

/// Fail closed on bytes left over after `count` elements — `MapSerializer`'s
/// `validate` raises `MarshalException("Unexpected extraneous bytes …")`, so a
/// short declared count over a longer buffer is corrupt, not a partial decode.
fn reject_trailing(data: &[u8], offset: usize, kind: &str) -> Result<()> {
    if offset != data.len() {
        return Err(Error::corruption(format!(
            "Frozen {kind}: {} extraneous bytes after the declared elements",
            data.len() - offset
        )));
    }
    Ok(())
}

/// Decode the INNER type of a `frozen<...>` with FROZEN framing (issue #2339).
///
/// A frozen COLLECTION is one opaque value produced by Cassandra's
/// `CollectionSerializer.serialize()`: `i32-BE count` + `i32-BE length`-prefixed
/// elements. That is a DIFFERENT framing from the VInt-prefixed NON-frozen
/// (multicell) collection shape [`super::value_parsing`]'s
/// `parse_{list,set,map}_value_with` decode, so `frozen<map<..>>` /
/// `frozen<set<..>>` / `frozen<list<..>>` must dispatch here or they mis-decode
/// (e.g. a `set<frozen<map<text,int>>>` element reads as an EMPTY map).
///
/// Every OTHER inner type — tuple, UDT, scalar, a nested `frozen` — already
/// decodes correctly through `parse`: tuple/UDT field framing is the same 4-byte
/// i32-BE shape (`TupleType.buildValue`), and a scalar is its own serialized form
/// either way. Dispatch is on the DECLARED comparator only, never a byte pattern
/// (no-heuristics, issue #28).
///
/// `parse` is the caller's depth-tracking recursion, and `max_depth` its budget,
/// so the guard against a pathological nesting depth (issue #1632) stays owned by
/// one place.
pub(crate) fn parse_frozen_inner_with(
    value_data: &[u8],
    inner: &ComparatorType,
    depth: usize,
    max_depth: usize,
    parse: DepthAwareDecoder<'_>,
) -> Result<Value> {
    if depth > max_depth {
        return Err(Error::corruption(format!(
            "Value decode recursion depth {depth} exceeds maximum {max_depth}"
        )));
    }
    match inner {
        ComparatorType::List(element) => {
            parse_frozen_list_value_with(value_data, element, |d, c| parse(d, c, depth + 1))
        }
        ComparatorType::Set(element) => {
            parse_frozen_set_value_with(value_data, element, |d, c| parse(d, c, depth + 1))
        }
        ComparatorType::Map(key, value) => {
            parse_frozen_map_value_with(value_data, key, value, |d, c| parse(d, c, depth + 1))
        }
        other => parse(value_data, other, depth),
    }
}
