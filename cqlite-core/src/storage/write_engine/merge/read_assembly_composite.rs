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
//! Scalar leaves delegate to [`ComparatorType::compare`], resolved from the
//! DECLARED (schema) leaf type — the single owner of per-type scalar ordering.
//! There is deliberately NO second scalar-ordering path here: two ordering
//! authorities for one type is precisely the divergence class issue #2339 exists
//! to remove, and a schema-driven dispatch is the no-heuristics direction (#28)
//! where a runtime-`Value`-variant dispatch is not.
//!
//! Three leaf types — `varint`, `decimal` and `uuid`/`timeuuid` — have no
//! Cassandra-compatible ordering in that single owner yet (issue #4063), so
//! [`compare_composite`] REFUSES an ordering decision on them instead of
//! inheriting a known-wrong answer; the citations and the reasoning are on
//! [`divergent_leaf`]. Refusal keeps ONE ordering authority: a locally-correct
//! comparator here would be the second one, which is the very class above.
//!
//! `inet` and `time` need no special case: `types::comparator::custom` orders both
//! by their serialized form's unsigned byte order, which IS their Cassandra
//! `ComparisonType.BYTE_ORDER` (fixture-backed against the Cassandra-written
//! `test_comparator_order` corpus, #3790/#3935).
//!
//! Dispatch is on the DECLARED comparator only, never a byte pattern (#28).

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
    reject_trailing_bytes(column, kind, bytes, cmp)?;
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

/// Refuse a composite cell path whose framing does not consume the WHOLE slice.
///
/// **Why this is required here and not inside the shared decoder (roborev job 60).**
/// `parse_value_with_comparator`'s tuple/UDT helpers stop once every DECLARED field
/// is read and ignore whatever follows, because their other callers hand them element
/// bytes already bounded by an outer length prefix — trailing bytes cannot occur
/// there. **A cell path has no such outer bound: the whole slice IS the key.** So two
/// DISTINCT cell paths that share a prefix decoded to the SAME logical map key / set
/// element, which in a map is a duplicate-key hazard and in a set collapses two
/// members into one.
///
/// The rule is the one `cell_path_key.rs` already states for the single-generation
/// reader, and it is applied here verbatim so the two arms agree: **`Err` only where
/// Cassandra's own `validate`/`split` throws** — and `TupleType.validate` at the
/// pinned `cassandra-5.0.8` tag throws on trailing bytes after a composite's
/// components. Such input is corrupt on Cassandra's own terms, so refusing it adds no
/// availability risk for data Cassandra itself would have read.
///
/// Only the TOP-LEVEL framing is walked, which is where the unbounded slice is; a
/// nested value is bounded by its own `i32` length prefix. Field bytes are never
/// inspected — the walk is over the framing alone (no-heuristics, #28) — and a length
/// that would run past the end is itself a refusal rather than a panic, since this
/// slice is attacker-controlled SSTable content.
fn reject_trailing_bytes(
    column: &str,
    kind: &str,
    bytes: &[u8],
    cmp: &ComparatorType,
) -> Result<()> {
    let arity = match unwrap_frozen_comparator(cmp) {
        ComparatorType::Tuple(fields) => fields.len(),
        ComparatorType::Udt {
            field_comparators, ..
        } => field_comparators.len(),
        // Only tuple/UDT framing is fixed-arity and therefore checkable this way.
        // A frozen collection carries its own leading count, and a scalar leaf is
        // width-validated by its own decoder.
        _ => return Ok(()),
    };

    let mut off = 0usize;
    for _ in 0..arity {
        // A composite with FEWER encoded components than declared is legal —
        // Cassandra's `compareCustom` treats an omitted suffix as all-null — so
        // running out of input here is not an error, only a stop.
        if off == bytes.len() {
            return Ok(());
        }
        let Some(len_bytes) = bytes.get(off..off + 4) else {
            return Err(trailing_error(column, kind, bytes.len(), off));
        };
        off += 4;
        let raw = i32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]);
        if raw < 0 {
            // Negative length == a null component; it consumes no value bytes.
            continue;
        }
        let Some(next) = off.checked_add(raw as usize) else {
            return Err(trailing_error(column, kind, bytes.len(), off));
        };
        if next > bytes.len() {
            return Err(trailing_error(column, kind, bytes.len(), off));
        }
        off = next;
    }

    if off != bytes.len() {
        return Err(trailing_error(column, kind, bytes.len(), off));
    }
    Ok(())
}

fn trailing_error(column: &str, kind: &str, total: usize, consumed: usize) -> Error {
    Error::corruption(format!(
        "column '{column}': {kind} cell path is {total} bytes but its declared \
         composite framing consumes {consumed} — trailing or truncated bytes after the \
         components. Cassandra's TupleType.validate throws on this, so the key is \
         corrupt on its own terms; decoding it from a prefix would let two distinct \
         cell paths collapse to one logical key (issues #28/#2339)"
    ))
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
        ComparatorType::List(elem) | ComparatorType::Set(elem) => first_unresolved_custom(elem),
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
/// produce — surfaced rather than silently mis-ordered — and on a scalar leaf whose
/// central comparator is KNOWN to diverge from Cassandra's (see
/// [`divergent_leaf`]), which is refused rather than ordered wrongly.
///
/// `column` is carried only to NAME the column in those refusals, exactly as the
/// decode-side refusals in this module do; it never influences the ordering.
pub(super) fn compare_composite(
    column: &str,
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
            compare_components(column, &l, &r, field_cmps)
        }
        ComparatorType::Udt {
            field_comparators, ..
        } => {
            let (l, r) = (udt_fields(left, cmp)?, udt_fields(right, cmp)?);
            let field_cmps: Vec<&ComparatorType> =
                field_comparators.iter().map(|(_, c)| c).collect();
            compare_components_ref(column, &l, &r, &field_cmps)
        }
        // `CollectionType.compareListOrSet`: element-wise, then size.
        ComparatorType::List(elem) | ComparatorType::Set(elem) => {
            let (l, r) = (sequence(left, cmp)?, sequence(right, cmp)?);
            for (a, b) in l.iter().zip(r.iter()) {
                match compare_composite(column, a, b, elem)? {
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
                match compare_composite(column, lk, rk, key_cmp)? {
                    Ordering::Equal => {}
                    other => return Ok(other),
                }
                match compare_composite(column, lv, rv, val_cmp)? {
                    Ordering::Equal => {}
                    other => return Ok(other),
                }
            }
            Ok(l.len().cmp(&r.len()))
        }
        // Every scalar leaf delegates to the CENTRAL comparator, resolved from the
        // DECLARED (schema) leaf type. `cmp` here is the frozen-unwrapped declared
        // comparator for this component, so this is a schema-driven dispatch, never
        // an inspection of the runtime value (no-heuristics, #28).
        //
        // `inet`/`time` come along for free: `types::comparator::custom` orders both
        // by their serialized form's unsigned byte order — `compare_inet` is a
        // `[u8]` compare of the raw address and `compare_time` a compare of
        // `i64::to_be_bytes` — which is `ComparisonType.BYTE_ORDER` verbatim, backed
        // by the Cassandra-written `test_comparator_order` fixtures (#3790/#3935).
        //
        // ONE authority, deliberately. An earlier revision of this arm called the
        // WRITE path's `collection_order::compare_collection_elements` instead; that
        // was wrong. It dispatches on the runtime `Value` VARIANT while this one
        // dispatches on the SCHEMA type, so keeping both would leave two ordering
        // authorities for one type — the divergence class #2339 exists to remove.
        //
        // GAP (tracked as #4063), and what this arm DOES about it: three leaf types
        // have no Cassandra-compatible ordering in `ComparatorType::compare`, so an
        // ordering DECISION on one of them is REFUSED here rather than answered
        // wrongly (see [`divergent_leaf`] for the per-type citations). Returning the
        // central comparator's answer would emit a plausible-looking collection in
        // the wrong order — a silent-wrong-value outcome, which is the class #28
        // forbids and the same shape as the nested-unresolved-UDT bug this module
        // already fails closed on.
        //
        // Fixing the central arms is deliberately NOT done here: it is a convergence
        // on `collection_order::scalar` (which already implements all three under its
        // own Cassandra citations) with its own blast radius, tracked as #4063. A
        // local correct comparator would be a SECOND ordering authority for the same
        // types — the divergence class #2339 exists to remove — so refusal, not a
        // second path, is the only option that keeps ONE authority.
        _ => {
            if let Some(leaf) = divergent_leaf(cmp) {
                return Err(divergent_leaf_error(column, leaf));
            }
            cmp.compare(left, right)
        }
    }
}

/// A scalar leaf type whose CENTRAL `ComparatorType::compare` arm is KNOWN not to
/// implement Cassandra's ordering, with the pinned-source citation for WHY
/// (issue #4063).
///
/// This is a REFUSAL list, not a second comparator: [`compare_composite`] fails
/// closed for these leaves instead of ordering them. It exists because the
/// alternative — inheriting `ComparatorType::compare`'s answer — returns a
/// plausible-looking collection in an order Cassandra would not produce, and a
/// merged read has no way for the caller to notice.
///
/// The divergences, read from `types::comparator` rather than assumed, against the
/// pinned `cassandra-5.0.8` comparators:
///
/// * `varint` — `compare_varint` is `Bytes::cmp`, i.e. raw UNSIGNED bytes, where
///   Cassandra `IntegerType.compare` orders by SIGNED two's-complement magnitude
///   (so `-1`, body `0xFF`, sorts ABOVE `0` here — reversed).
/// * `decimal` — `compare_decimal` normalizes unequal scales by comparing
///   `format!("{:?}.{}", unscaled, scale)` STRINGS, self-described in source as
///   "For now, simple string comparison"; Cassandra `DecimalType.compare` compares
///   NUMERICALLY.
/// * `uuid` — `compare_uuid` is `Uuid::cmp` (raw bytes), where Cassandra `UUIDType`
///   compares version first, then a v1 timestamp, then the tail. `CqlType::TimeUuid`
///   maps onto this SAME `ComparatorType::Uuid` variant, so a `timeuuid` leaf is
///   refused here too — and Cassandra's `TimeUUIDType` order differs again, so
///   inheriting would be wrong for both spellings.
///
/// Dispatch is on the DECLARED comparator, never on the runtime value (#28).
fn divergent_leaf(cmp: &ComparatorType) -> Option<(&'static str, &'static str)> {
    match cmp {
        ComparatorType::Varint => Some((
            "varint",
            "IntegerType (signed two's-complement magnitude), where compare_varint is raw \
             unsigned byte order",
        )),
        ComparatorType::Decimal => Some((
            "decimal",
            "DecimalType (numeric across scales), where compare_decimal falls back to a \
             formatted-string comparison for unequal scales",
        )),
        ComparatorType::Uuid => Some((
            "uuid/timeuuid",
            "UUIDType (version, then v1 timestamp, then tail), where compare_uuid is raw \
             byte order",
        )),
        _ => None,
    }
}

/// The refusal [`compare_composite`] returns for a [`divergent_leaf`].
///
/// Names the COLUMN, the offending LEAF TYPE and issue #4063, in the same style as
/// this module's decode-side refusals, so an operator can tell which column and
/// which type to look at without reading source.
fn divergent_leaf_error(column: &str, (leaf, citation): (&'static str, &'static str)) -> Error {
    Error::unsupported_format(format!(
        "column '{column}': refusing to ORDER a composite collection key/element \
         whose '{leaf}' leaf has no Cassandra-compatible ordering yet — the central \
         ComparatorType::compare arm for '{leaf}' diverges from Cassandra's {citation} \
         (tracked as issue #4063). Failing closed rather than returning a \
         plausible-looking collection in the wrong order (issues #28/#2339)"
    ))
}

/// `TupleType.compareCustom`'s component loop over already-decoded components.
///
/// * a NULL component sorts BEFORE a non-null one (`sizeL < 0` ⇒ `-1`);
/// * when one side runs out of components, the two are EQUAL iff every remaining
///   component of the other side is NULL, else the side with content is GREATER;
/// * an EMPTY value therefore sorts first, matching the `isEmpty` pre-check.
fn compare_components(
    column: &str,
    left: &[Value],
    right: &[Value],
    field_cmps: &[ComparatorType],
) -> Result<Ordering> {
    let refs: Vec<&ComparatorType> = field_cmps.iter().collect();
    compare_components_ref(column, left, right, &refs)
}

fn compare_components_ref(
    column: &str,
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
        match compare_composite(column, l, r, field_cmps[i])? {
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
