//! Composite (frozen tuple / UDT / nested frozen collection) collection
//! key/element handling for [`super`], the merged-read assembler (issue #2339).
//!
//! A non-frozen `set` whose element — or `map` whose key — is an opaque composite
//! keeps that element's IDENTITY in the cell's authoritative `cell_path`, not in
//! its value (a set cell's value is empty; sstabledump prints `"value":""`). This
//! module owns the two things the assembler must do with those bytes:
//!
//! 1. [`decode_composite`] — DECODE them into a typed `Value::Tuple`/`Value::Udt`/
//!    nested collection, reusing the canonical structural value deserializer
//!    (`comparator_value_parsing::parse_value_with_comparator`), the same decoder
//!    the single-generation read path uses. Never a second structural decoder.
//! 2. [`compare_composite`] — ORDER them the way Cassandra does.
//!
//! ## Why ordering needs its own comparator (measured, not assumed)
//!
//! Cassandra writes a complex column's cells in `cellPathComparator()` order,
//! which for a collection is the declared element/key TYPE's comparator — NOT
//! unsigned byte order of the serialized form. Verified against Cassandra-written
//! bytes: in `test_collections.collections_with_udts` partition `c2a32d80…` the
//! `contacts set<frozen<contact_info>>` cells are on disk in the order
//! `harrisvalerie@example.net`, `sandra59@example.org` (component-wise text
//! order), whereas RAW byte comparison of the same two cell paths puts
//! `sandra59` FIRST — the first component's 4-byte i32-BE LENGTH prefix
//! (`0x14` vs `0x19`) dominates the byte comparison before any text is reached.
//! The same inversion shows in `test_nested_udt_keys.nested_udt_keys`, where a
//! 2-element frozen collection precedes a 1-element one on disk while its
//! `i32-BE` count prefix would sort it second.
//!
//! So the semantics below are transcribed from the pinned `cassandra-5.0.8`
//! comparators, applied to the DECODED values (which the assembler needs anyway):
//!
//! * `TupleType.compareCustom` (`UserType` extends `TupleType`): component-wise
//!   with each component's own comparator; a NULL component sorts BEFORE a
//!   non-null one; when one side runs out, the sides are EQUAL iff every
//!   remaining component of the other is null; an empty value sorts first.
//! * `CollectionType.compareListOrSet`: element-wise over `min(sizeL, sizeR)`,
//!   then `Integer.compare(sizeL, sizeR)`.
//! * `MapType.compareMaps`: `(key, value)`-wise over `min(sizeL, sizeR)`, then
//!   `Integer.compare(sizeL, sizeR)`.
//!
//! Scalars delegate to [`ComparatorType::compare`], the single owner of per-type
//! scalar ordering — EXCEPT `inet` and `time`, whose Cassandra types declare
//! `ComparisonType.BYTE_ORDER` while that method's fall-through compares FORMATTED
//! STRINGS ([`compare_byte_order_custom`], roborev F3). Dispatch is on the DECLARED
//! comparator only, never a byte pattern (no-heuristics, issue #28).

#![cfg(feature = "write-support")]

use std::cmp::Ordering;

use crate::storage::sstable::reader::parsing::comparator_value_parsing::parse_value_with_comparator;
use crate::types::{ComparatorType, Value};
use crate::{Error, Result};

/// Peel `Frozen` wrappers off a comparator.
pub(super) fn unwrap_frozen_comparator(cmp: &ComparatorType) -> &ComparatorType {
    match cmp {
        ComparatorType::Frozen(inner) => unwrap_frozen_comparator(inner),
        other => other,
    }
}

/// Peel `Value::Frozen` wrappers off a decoded value (the deserializer wraps a
/// `frozen<...>` result, so a decoded element/key is `Frozen(Udt(..))`).
fn unwrap_frozen_value(value: &Value) -> &Value {
    match value {
        Value::Frozen(inner) => unwrap_frozen_value(inner),
        other => other,
    }
}

/// Decode an OPAQUE COMPOSITE collection key/element from its authoritative
/// `cell_path` identity bytes into a typed `Value` (issue #2339).
///
/// Before #2339 the merged-read arm FAILED CLOSED here, which made a correctness
/// outcome flip on SSTable generation count: one generation decoded via the
/// single-generation arm, two errored the whole request.
///
/// A composite whose declared type never resolved to a STRUCTURE — a `Custom` UDT
/// reference with no entry in the table's `UdtRegistry` — has no field list to
/// decode into, so it still fails closed with a clear error naming the column and
/// the declared type. Emitting opaque bytes instead would break the typed Arrow
/// builder one layer deeper (roborev 1632).
pub(super) fn decode_composite(
    column: &str,
    kind: &str,
    bytes: &[u8],
    cmp: &ComparatorType,
) -> Result<Value> {
    if let Some(name) = first_unresolved_custom(cmp) {
        return Err(Error::unsupported_format(format!(
            "column '{column}': {kind} type '{name}' did not resolve to a structure \
             — a UDT reference with no definition in the table's UDT registry has no \
             field list to decode into; failing closed rather than emitting opaque \
             bytes (issues #28/#2339)"
        )));
    }
    parse_value_with_comparator(bytes, cmp)
}

/// The name of the first `Custom` comparator node ANYWHERE in `cmp`'s tree that
/// `custom_scalar::decode_custom_scalar` cannot decode, or `None` when every
/// `Custom` node is one it recognises.
///
/// **Why this is RECURSIVE, and why a top-level-only check was a silent-wrong-value
/// bug (roborev job 52, G2).** The guard in [`decode_composite`] originally tested
/// only the comparator that `unwrap_frozen_comparator` returns, i.e. the top level.
/// An unresolved UDT reference NESTED inside a tuple, UDT or collection — say
/// `set<frozen<tuple<frozen<unregistered_udt>, int>>>` — leaves `Custom` at a nested
/// position, so the guard passed and the decoder's `_ =>` arm turned that field into
/// an opaque `Value::Blob`. A multi-generation read then emitted AND SORTED a
/// plausible-looking wrong value instead of failing closed, which is exactly the
/// class the no-heuristics mandate forbids (#28; the same shape as #3612's
/// "the blob fallback must not swallow a recognised-but-unhandled composite").
///
/// The admitted set is **`time` / `inet` / `json`** because that is precisely what
/// `custom_scalar::decode_custom_scalar` matches; every other name falls to its
/// `_ =>` arm and becomes a `Blob`. Read from that function rather than assumed —
/// if it gains an arm, this list must gain it too, or a decodable type starts
/// failing closed.
fn first_unresolved_custom(cmp: &ComparatorType) -> Option<&str> {
    match cmp {
        ComparatorType::Custom(name) => {
            if matches!(name.as_str(), "time" | "inet" | "json") {
                None
            } else {
                Some(name.as_str())
            }
        }
        ComparatorType::Frozen(inner) => first_unresolved_custom(inner),
        ComparatorType::List(elem) | ComparatorType::Set(elem) => {
            first_unresolved_custom(elem)
        }
        ComparatorType::Map(key, val) => {
            first_unresolved_custom(key).or_else(|| first_unresolved_custom(val))
        }
        ComparatorType::Tuple(fields) => fields.iter().find_map(first_unresolved_custom),
        ComparatorType::Udt {
            field_comparators, ..
        } => field_comparators
            .iter()
            .find_map(|(_, c)| first_unresolved_custom(c)),
        _ => None,
    }
}

/// Order two decoded composite keys/elements exactly as Cassandra's type
/// comparator does (see the module doc for the pinned-source transcription).
///
/// `Err` on a value/comparator shape mismatch the declared metadata should never
/// produce — surfaced rather than silently mis-ordered.
pub(super) fn compare_composite(
    left: &Value,
    right: &Value,
    cmp: &ComparatorType,
) -> Result<Ordering> {
    let cmp = unwrap_frozen_comparator(cmp);
    let left = unwrap_frozen_value(left);
    let right = unwrap_frozen_value(right);
    match cmp {
        // `TupleType.compareCustom` — `UserType` extends `TupleType`, so a UDT and
        // a tuple order identically (component-wise, nulls first).
        ComparatorType::Tuple(field_cmps) => {
            let (l, r) = (tuple_fields(left, cmp)?, tuple_fields(right, cmp)?);
            compare_components(&l, &r, field_cmps)
        }
        ComparatorType::Udt {
            field_comparators, ..
        } => {
            let (l, r) = (udt_fields(left, cmp)?, udt_fields(right, cmp)?);
            let field_cmps: Vec<&ComparatorType> =
                field_comparators.iter().map(|(_, c)| c).collect();
            compare_components_ref(&l, &r, &field_cmps)
        }
        // `CollectionType.compareListOrSet`: element-wise, then size.
        ComparatorType::List(elem) | ComparatorType::Set(elem) => {
            let (l, r) = (sequence(left, cmp)?, sequence(right, cmp)?);
            for (a, b) in l.iter().zip(r.iter()) {
                match compare_composite(a, b, elem)? {
                    Ordering::Equal => {}
                    other => return Ok(other),
                }
            }
            Ok(l.len().cmp(&r.len()))
        }
        // `MapType.compareMaps`: (key, value)-wise, then size.
        ComparatorType::Map(key_cmp, val_cmp) => {
            let (l, r) = (entries(left, cmp)?, entries(right, cmp)?);
            for ((lk, lv), (rk, rv)) in l.iter().zip(r.iter()) {
                match compare_composite(lk, rk, key_cmp)? {
                    Ordering::Equal => {}
                    other => return Ok(other),
                }
                match compare_composite(lv, rv, val_cmp)? {
                    Ordering::Equal => {}
                    other => return Ok(other),
                }
            }
            Ok(l.len().cmp(&r.len()))
        }
        // `inet` / `time` are ordered by the SERIALIZED FORM's unsigned byte order,
        // NOT by `ComparatorType::compare` (roborev F3) — see
        // [`compare_byte_order_custom`].
        ComparatorType::Custom(name) if name == "inet" || name == "time" => {
            compare_byte_order_custom(name, left, right, cmp)
        }
        // Every other scalar routes through the single owner of scalar ordering.
        scalar => scalar.compare(left, right),
    }
}

/// Order an `inet` / `time` COMPONENT of a composite the way Cassandra does:
/// unsigned byte order of the serialized form.
///
/// Both types declare `ComparisonType.BYTE_ORDER` at the pinned `cassandra-5.0.8`
/// tag — verbatim:
///
/// ```text
/// InetAddressType() {super(ComparisonType.BYTE_ORDER);} // singleton
/// private TimeType()  {super(ComparisonType.BYTE_ORDER);} // singleton
/// ```
///
/// `ComparatorType::compare`'s fall-through for these two names is
/// `compare_custom`, which compares the values' FORMATTED STRINGS — a genuinely
/// different order for `inet` (`9.0.0.1` precedes `10.0.0.1` by address bytes, the
/// REVERSE of their dotted-quad text order), so a `tuple`/UDT carrying an `inet`
/// component was ordered differently from Cassandra.
///
/// SCOPE (deliberate, issue #2339): this fixes the COMPOSITE path only.
/// `ComparatorType::compare`'s own `inet`/`time` arms are a PRE-EXISTING defect
/// that also affects the SCALAR collection path (where `read_assembly` works
/// around it by sorting those elements on raw `cell_path` bytes — see
/// `comparator_orders_by_raw_cell_path_bytes`), so rewriting the central
/// comparator is a separate change with its own blast radius.
///
/// Serialized forms (`custom_scalar::decode_custom_scalar`, the decoder that
/// produced these values):
/// * `inet` — the raw address bytes, so unsigned byte order IS a byte compare of
///   `Value::Inet`'s payload (Rust slice `Ord` is unsigned lexicographic then
///   length, matching `ByteBufferUtil.compareUnsigned`).
/// * `time` — an 8-byte big-endian `i64`, so unsigned byte order is `u64` order of
///   the same bits (identical to `i64` order for the non-negative nanoseconds-of-day
///   a valid value carries, and still faithful to BYTE_ORDER if one is not).
///
/// A value whose shape contradicts the declared type is an `Err`, never a silent
/// mis-order — the rule the rest of this module follows.
fn compare_byte_order_custom(
    name: &str,
    left: &Value,
    right: &Value,
    cmp: &ComparatorType,
) -> Result<Ordering> {
    // Nulls first, exactly as `ComparatorType::compare` does — the `List`/`Set`/
    // `Map` arms above descend into elements without a null pre-check.
    match (left.is_null(), right.is_null()) {
        (true, true) => return Ok(Ordering::Equal),
        (true, false) => return Ok(Ordering::Less),
        (false, true) => return Ok(Ordering::Greater),
        (false, false) => {}
    }
    match (name, left, right) {
        ("inet", Value::Inet(l), Value::Inet(r)) => Ok(l.as_ref().cmp(r.as_ref())),
        ("time", Value::Time(l), Value::Time(r)) => Ok((*l as u64).cmp(&(*r as u64))),
        _ => Err(shape_error(left, cmp)),
    }
}

/// `TupleType.compareCustom`'s component loop over already-decoded components.
///
/// * a NULL component sorts BEFORE a non-null one (`sizeL < 0` ⇒ `-1`);
/// * when one side runs out of components, the two are EQUAL iff every remaining
///   component of the other side is NULL, else the side with content is GREATER;
/// * an EMPTY value therefore sorts first, matching the `isEmpty` pre-check.
fn compare_components(
    left: &[Value],
    right: &[Value],
    field_cmps: &[ComparatorType],
) -> Result<Ordering> {
    let refs: Vec<&ComparatorType> = field_cmps.iter().collect();
    compare_components_ref(left, right, &refs)
}

fn compare_components_ref(
    left: &[Value],
    right: &[Value],
    field_cmps: &[&ComparatorType],
) -> Result<Ordering> {
    let common = left.len().min(right.len()).min(field_cmps.len());
    for i in 0..common {
        let (l, r) = (&left[i], &right[i]);
        match (l.is_null(), r.is_null()) {
            (true, true) => continue,
            (true, false) => return Ok(Ordering::Less),
            (false, true) => return Ok(Ordering::Greater),
            (false, false) => {}
        }
        match compare_composite(l, r, field_cmps[i])? {
            Ordering::Equal => {}
            other => return Ok(other),
        }
    }
    let l_rest_null = left[common.min(left.len())..].iter().all(Value::is_null);
    let r_rest_null = right[common.min(right.len())..].iter().all(Value::is_null);
    match (l_rest_null, r_rest_null) {
        (true, true) => Ok(Ordering::Equal),
        (true, false) => Ok(Ordering::Less),
        (false, true) => Ok(Ordering::Greater),
        // Both sides still carry non-null components beyond the comparable
        // prefix; only reachable when `field_cmps` is SHORTER than both decoded
        // component lists, i.e. the declared type contradicts the bytes.
        (false, false) => Ok(left.len().cmp(&right.len())),
    }
}

fn shape_error(value: &Value, cmp: &ComparatorType) -> Error {
    Error::Schema(format!(
        "composite collection key/element: decoded {value:?} does not match the \
         declared comparator {cmp:?}"
    ))
}

fn tuple_fields(value: &Value, cmp: &ComparatorType) -> Result<Vec<Value>> {
    match value {
        Value::Tuple(fields) => Ok(fields.clone()),
        other => Err(shape_error(other, cmp)),
    }
}

fn udt_fields(value: &Value, cmp: &ComparatorType) -> Result<Vec<Value>> {
    match value {
        Value::Udt(udt) => Ok(udt
            .fields
            .iter()
            .map(|f| f.value.clone().unwrap_or(Value::Null))
            .collect()),
        other => Err(shape_error(other, cmp)),
    }
}

fn sequence(value: &Value, cmp: &ComparatorType) -> Result<Vec<Value>> {
    match value {
        Value::List(items) | Value::Set(items) => Ok(items.clone()),
        other => Err(shape_error(other, cmp)),
    }
}

fn entries(value: &Value, cmp: &ComparatorType) -> Result<Vec<(Value, Value)>> {
    match value {
        Value::Map(pairs) => Ok(pairs.clone()),
        other => Err(shape_error(other, cmp)),
    }
}
