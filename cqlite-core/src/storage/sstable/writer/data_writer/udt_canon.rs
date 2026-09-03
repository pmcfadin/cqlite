//! Schema-aware canonicalization of frozen-UDT (and UDT-bearing frozen
//! collection/tuple) VALUES against the column's authoritative on-disk marshal
//! type (roborev #1020 Finding 1).
//!
//! A `frozen<udt>` regular column advertises the full
//! `FrozenType(UserType(...))` SerializationHeader marshal (issue #1020). The
//! simple-cell value path, however, serialized a `Value::Frozen(Value::Udt(...))`
//! from the LITERAL field list — so a SPARSE, OUT-OF-ORDER, or EXTRA-FIELD UDT
//! literal wrote bytes that did not match the declared header (corrupting
//! reads/compaction). This module reorders/pads/validates a UDT value against the
//! DECLARED field order parsed from the marshal BEFORE serialization, so the
//! emitted field bytes always follow declared order with a `-1` (absent) length
//! marker for every missing declared field and an error for any unknown field
//! (no-heuristics, issue #28). It recurses through `FrozenType`/collection/tuple
//! wrappers so nested UDT values (e.g. inside `frozen<list<frozen<address>>>`)
//! are canonicalized too.
//!
//! The absent-field encoding (`-1` i32 length) is verified against the Cassandra
//! wire format: `serialize_udt` (types.rs) writes a 4-byte BE i32 length per
//! declared field, `-1` for null, and the reader
//! (`row_decoder::udt::parse_udt_value`) reads `-1` back as a null
//! field. Canonicalizing the literal to declared order + `None` padding therefore
//! makes the value bytes self-consistent with the declared `UserType(...)` header.
//!
//! Part of the `data_writer` responsibility split (issue #1118). `use super::*`
//! provides the crate imports and sibling helpers.

use super::*;
use crate::types::{UdtField, UdtValue};
use std::borrow::Cow;
use std::cmp::Ordering;

const MARSHAL_PREFIX: &str = "org.apache.cassandra.db.marshal.";

/// Canonicalize a frozen-UDT (or UDT-bearing frozen collection/tuple) VALUE
/// against the column's authoritative `data_type` marshal (roborev #1020
/// Finding 1).
///
/// Returns a `Cow<Value>` whose every UDT — at any depth reachable through
/// `FrozenType`/`ListType`/`SetType`/`MapType`/`TupleType` — has its fields in
/// DECLARED order, missing declared fields padded with `None`, and any unknown
/// literal field rejected with an error. When `data_type` does NOT reference a
/// UDT (a primitive, or a frozen collection of primitives), or is not a marshal
/// string, the BORROW is returned UNCHANGED (`Cow::Borrowed`, no clone) — the
/// existing serialization path is byte-identical for those (roborev #1020
/// Finding 2: the previous owned-`Value` return cloned every non-UDT cell on
/// the write/compaction hot path).
pub(crate) fn canonicalize_udt_value<'a>(
    data_type: &str,
    value: &'a Value,
) -> Result<Cow<'a, Value>> {
    // Fast path: no UDT anywhere in the declared type → nothing to canonicalize.
    if !references_user_type(data_type) {
        return Ok(Cow::Borrowed(value));
    }
    Ok(Cow::Owned(canonicalize_value_for_marshal(
        data_type.trim(),
        value,
    )?))
}

/// Canonicalize a STATIC column's value against the column's declared
/// `data_type` resolved from `schema` by name (roborev #1020 Finding 1, static
/// path). A column not found in `schema` (defensive) or a non-UDT column returns
/// the BORROW unchanged (`Cow::Borrowed`, no clone — roborev #1020 Finding 2).
pub(crate) fn canonicalize_static_value<'a>(
    schema: &TableSchema,
    column: &str,
    value: &'a Value,
) -> Result<Cow<'a, Value>> {
    match schema.columns.iter().find(|c| c.name == column) {
        Some(col) => canonicalize_udt_value(&col.data_type, value),
        None => Ok(Cow::Borrowed(value)),
    }
}

/// True iff `marshal` mentions a `UserType(` anywhere (ASCII case-insensitive).
/// Used as the cheap gate so non-UDT columns skip canonicalization entirely.
fn references_user_type(marshal: &str) -> bool {
    const TARGET: &[u8] = b"usertype(";
    let bytes = marshal.as_bytes();
    if bytes.len() < TARGET.len() {
        return false;
    }
    bytes
        .windows(TARGET.len())
        .any(|w| w.iter().zip(TARGET).all(|(a, b)| a.eq_ignore_ascii_case(b)))
}

/// Recursively canonicalize `value` against the marshal type `ty`.
fn canonicalize_value_for_marshal(ty: &str, value: &Value) -> Result<Value> {
    let ty = ty.trim();

    // An already-serialized opaque value (a `Blob` carrying the frozen wire bytes,
    // or `Null`) is passed through UNCHANGED, regardless of the declared marshal.
    // This is the COMPACTION read path: a frozen collection/UDT cell decoded from
    // an input SSTable comes back as a `Value::Blob` of its raw, already-declared-
    // order wire bytes. It is NOT a structured `List`/`Map`/`Udt` literal to
    // reorder; re-canonicalizing it is impossible (no fields to reorder) and
    // wrong (the bytes are already canonical). Only a structured literal from a
    // direct write needs reordering/padding (no-heuristics: we never reinterpret
    // opaque bytes — issue #28).
    if matches!(value, Value::Blob(_) | Value::Null) {
        return Ok(value.clone());
    }

    // Unwrap a Value::Frozen so the inner value is matched against the inner
    // marshal type, then re-wrap. The marshal may or may not carry an explicit
    // FrozenType wrapper at this level (an inner UDT field is spelled bare), so
    // peel the value's Frozen regardless and recurse on the (possibly frozen-)
    // stripped marshal.
    if let Value::Frozen(inner) = value {
        let inner_marshal = strip_frozen_marshal(ty);
        let canon = canonicalize_value_for_marshal(inner_marshal, inner)?;
        return Ok(Value::Frozen(Box::new(canon)));
    }

    let ty = strip_frozen_marshal(ty);

    if is_user_type_marshal(ty) {
        return canonicalize_udt(ty, value);
    }
    if let Some(elem) = collection_element_marshal(ty, "ListType") {
        return canonicalize_seq(value, elem, SeqKind::List);
    }
    if let Some(elem) = collection_element_marshal(ty, "SetType") {
        return canonicalize_seq(value, elem, SeqKind::Set);
    }
    if let Some((k, v)) = map_kv_marshal(ty) {
        return canonicalize_map(value, k, v);
    }
    if let Some(components) = tuple_component_marshals(ty) {
        return canonicalize_tuple(value, &components);
    }

    // Primitive (or a type with no UDT inside) LEAF. roborev #1020 Finding 1:
    // validate the Value variant against the declared marshal before letting it
    // flow into serialization. A `frozen<person>` header declares e.g.
    // `age:Int32Type`; without this check a `Value::BigInt` in that slot would be
    // serialized with inferred 8-byte LongType bytes, producing a cell whose bytes
    // disagree with the advertised `UserType(...)` header. We REJECT (never coerce
    // — no-heuristics, issue #28).
    validate_primitive_leaf(ty, value)?;
    Ok(value.clone())
}

/// Whether a list/set value being canonicalized is declared as a `ListType` or
/// `SetType`, so a `Value::List` in a `SetType` slot (and vice versa) is
/// rejected rather than silently re-tagged (roborev #1020 Finding 1).
#[derive(Clone, Copy)]
enum SeqKind {
    List,
    Set,
}

/// Validate a PRIMITIVE leaf `value` against its declared primitive marshal
/// `ty` (roborev #1020 Finding 1). The accepted `Value` variant(s) per marshal
/// mirror the authoritative `TypeSerializer` mapping in
/// `storage/serialization/types.rs` (`serialize_primitive`/`serialize_text`/…),
/// so a value that passes here is exactly one that serializer will accept with
/// the bytes the declared on-disk type expects. An unknown / non-primitive
/// marshal (a collection/tuple/UDT is handled by the recursive callers before
/// this point) is left unvalidated — there is no primitive-byte mismatch to
/// guard. A `Value::Null` never reaches here (the UDT/collection callers map a
/// null field to absence first).
fn validate_primitive_leaf(ty: &str, value: &Value) -> Result<()> {
    let Some(name) = primitive_marshal_name(ty) else {
        // Not a known primitive marshal (e.g. a bare CQL type string, or a marshal
        // this mapping does not enumerate): nothing to validate here.
        return Ok(());
    };
    let ok = match name {
        // 4-byte BE signed int.
        "Int32Type" => matches!(value, Value::Integer(_)),
        // 8-byte BE signed long; counter shares the LongType representation.
        "LongType" => matches!(value, Value::BigInt(_) | Value::Counter(_)),
        "CounterColumnType" => matches!(value, Value::Counter(_) | Value::BigInt(_)),
        // 1-byte signed (tinyint) / 2-byte BE signed (smallint).
        "ByteType" => matches!(value, Value::TinyInt(_)),
        "ShortType" => matches!(value, Value::SmallInt(_)),
        // IEEE-754 4-byte (float) / 8-byte (double).
        "FloatType" => matches!(value, Value::Float32(_)),
        "DoubleType" => matches!(value, Value::Float(_)),
        "BooleanType" => matches!(value, Value::Boolean(_)),
        // Text family — all carry a UTF-8 `Value::Text`.
        "UTF8Type" | "AsciiType" => matches!(value, Value::Text(_)),
        // blob.
        "BytesType" => matches!(value, Value::Blob(_)),
        // 16-byte UUID family.
        "UUIDType" | "TimeUUIDType" | "LexicalUUIDType" => matches!(value, Value::Uuid(_)),
        // 8-byte BE millis-since-epoch.
        "TimestampType" => matches!(value, Value::Timestamp(_)),
        // 4-byte day count (SimpleDateType is the `date` CQL type).
        "SimpleDateType" => matches!(value, Value::Date(_)),
        // 8-byte BE nanos-since-midnight.
        "TimeType" => matches!(value, Value::Time(_)),
        // months/days/nanos vints.
        "DurationType" => matches!(value, Value::Duration { .. }),
        // arbitrary-precision integer (CQL `varint`).
        "IntegerType" => matches!(value, Value::Varint(_)),
        "DecimalType" => matches!(value, Value::Decimal { .. }),
        "InetAddressType" => matches!(value, Value::Inet(_)),
        // A primitive marshal not in this table: do not guess.
        _ => return Ok(()),
    };
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidInput(format!(
            "frozen-UDT leaf value {value:?} does not match declared marshal type \
             {MARSHAL_PREFIX}{name}; value/type mismatch is rejected (no coercion, issue #28)"
        )))
    }
}

/// The bare marshal type name (e.g. `Int32Type`) iff `ty` is exactly a
/// `org.apache.cassandra.db.marshal.<Name>` with NO parenthesized arguments
/// (i.e. a primitive). A parameterized marshal (`ListType(...)`, `UserType(...)`,
/// etc.) returns `None` — those are structural types handled by the recursion.
fn primitive_marshal_name(ty: &str) -> Option<&str> {
    let ty = ty.trim();
    let rest = ty.strip_prefix(MARSHAL_PREFIX)?;
    if rest.is_empty() || rest.contains('(') || rest.contains(')') {
        return None;
    }
    Some(rest)
}

/// Canonicalize a UDT value against a `UserType(...)` marshal: emit fields in
/// declared order, pad missing declared fields with `None`, recurse into each
/// declared field's type, and REJECT any literal field not in the declared set.
fn canonicalize_udt(user_type_marshal: &str, value: &Value) -> Result<Value> {
    let udt = match value {
        Value::Udt(udt) => udt,
        other => {
            return Err(Error::InvalidInput(format!(
                "expected a UDT value for declared type {user_type_marshal}, got {other:?}"
            )))
        }
    };

    let declared = parse_user_type_fields(user_type_marshal)?;

    // Reject unknown literal fields (no-heuristics, issue #28).
    for lit in &udt.fields {
        if !declared.iter().any(|(name, _)| name == &lit.name) {
            return Err(Error::InvalidInput(format!(
                "UDT literal field '{}' is not a declared field of {}",
                lit.name, user_type_marshal
            )));
        }
    }

    let mut fields = Vec::with_capacity(declared.len());
    for (name, field_marshal) in &declared {
        let lit_value = udt
            .fields
            .iter()
            .find(|f| &f.name == name)
            .and_then(|f| f.value.as_ref());
        let value = match lit_value {
            Some(v) if !matches!(v, Value::Null) => {
                Some(canonicalize_value_for_marshal(field_marshal, v)?)
            }
            // Missing OR explicitly-null declared field → absent (`-1` on the wire).
            _ => None,
        };
        fields.push(UdtField {
            name: name.clone(),
            value,
        });
    }

    Ok(Value::Udt(Box::new(UdtValue {
        type_name: udt.type_name.clone(),
        keyspace: udt.keyspace.clone(),
        fields,
    })))
}

fn canonicalize_seq(value: &Value, elem_marshal: &str, kind: SeqKind) -> Result<Value> {
    // roborev #1020 Finding 1: a `ListType` marshal must carry a `Value::List`
    // and a `SetType` marshal a `Value::Set`. A list/set are NOT
    // interchangeable on the wire — a `Value::Set` written into a `ListType`
    // column (or vice versa) sorts/structures differently from what the declared
    // type advertises. REJECT the kind mismatch rather than re-tag (no-heuristics,
    // issue #28).
    let elems = match (kind, value) {
        (SeqKind::List, Value::List(e)) => e,
        (SeqKind::Set, Value::Set(e)) => e,
        (SeqKind::List, Value::Set(_)) => {
            return Err(Error::InvalidInput(
                "declared ListType but value is a Set; list/set kind mismatch is rejected \
                 (no coercion, issue #28)"
                    .to_string(),
            ))
        }
        (SeqKind::Set, Value::List(_)) => {
            return Err(Error::InvalidInput(
                "declared SetType but value is a List; list/set kind mismatch is rejected \
                 (no coercion, issue #28)"
                    .to_string(),
            ))
        }
        (_, other) => {
            return Err(Error::InvalidInput(format!(
                "expected a list/set value for a collection type, got {other:?}"
            )))
        }
    };
    let mut out = Vec::with_capacity(elems.len());
    for e in elems {
        out.push(canonicalize_value_for_marshal(elem_marshal, e)?);
    }
    Ok(match kind {
        SeqKind::List => Value::List(out),
        SeqKind::Set => {
            // A frozen `SetType` is a SORTED collection: Cassandra stores its
            // elements ordered by the element AbstractType comparator. That
            // comparator is type-dependent and is NOT unsigned-byte order for
            // every type (notably SIGNED integers: Int32Type/LongType/ByteType/
            // ShortType compare as signed, so `-1` sorts before `0`). The writer's
            // simple-cell path emits set elements in iteration order, so a
            // multi-element frozen set written unsorted would produce NON-Cassandra
            // bytes. Sort using the comparator DERIVED FROM `elem_marshal` (issue
            // #1020 roborev follow-up; #28 no-heuristics — fail-closed on any
            // element type whose comparator we cannot implement confidently).
            sort_sorted_collection(out, elem_marshal, |e| e)?
        }
    })
}

/// Return `items` reordered by the element/key AbstractType comparator DERIVED
/// FROM `key_marshal`, so the on-disk order of a frozen SORTED collection
/// (`SetType` elements, `MapType` keys) matches Cassandra exactly. `key` projects
/// the comparable value out of each item (identity for a set element, the entry
/// KEY for a map).
///
/// The comparator is type-aware (see [`compare_for_marshal`]): SIGNED integer
/// marshals compare as signed numerics (so `-1 < 0`), byte-ordered marshals
/// compare by their unsigned serialized bytes, and any type whose comparator we
/// cannot implement confidently is FAIL-CLOSED with an error (no-heuristics, issue
/// #28). The serialized bytes are precomputed once per item (so a serialization
/// error surfaces deterministically and we never re-serialize per comparison); the
/// comparator receives both the live `Value` and its bytes. A stable sort keeps
/// the relative input order of items that compare equal.
fn sort_sorted_collection<T>(
    items: Vec<T>,
    key_marshal: &str,
    key: impl Fn(&T) -> &Value,
) -> Result<Value>
where
    Value: SortedCollection<T>,
{
    // roborev #1020 Finding 2 (REGRESSION guard): ordering is a no-op for a
    // collection with fewer than 2 elements — there is no pair to compare — so
    // skip comparator classification/validation ENTIRELY and return the
    // (already element-canonicalized) collection unchanged. Without this, the
    // round-6 fail-closed comparator wrongly REJECTS a perfectly valid singleton
    // such as `frozen<map<uuid, frozen<person>>>` (one entry) purely because
    // UUIDType has no implemented comparator. Fail-closed behavior is preserved
    // only where an actual sort comparison is required (`len >= 2`).
    if items.len() < 2 {
        return Ok(Value::from_sorted(items));
    }
    let mut keyed: Vec<(Vec<u8>, T)> = Vec::with_capacity(items.len());
    for item in items {
        let bytes = serialize_value(key(&item))?;
        keyed.push((bytes, item));
    }
    // Surface any unsupported-comparator error BEFORE sorting (sort_by cannot
    // return an error). A single representative probe is insufficient — collection
    // elements can mix variants — so the byte/value comparator is invoked for every
    // pair; instead, validate the comparator is implementable for this marshal up
    // front against each item's value.
    for (_, item) in &keyed {
        comparator_supported_for(key_marshal, key(item))?;
    }
    let mut sort_err: Option<Error> = None;
    keyed.sort_by(|a, b| {
        match compare_for_marshal(key_marshal, key(&a.1), &a.0, key(&b.1), &b.0) {
            Ok(ord) => ord,
            Err(e) => {
                if sort_err.is_none() {
                    sort_err = Some(e);
                }
                Ordering::Equal
            }
        }
    });
    if let Some(e) = sort_err {
        return Err(e);
    }
    Ok(Value::from_sorted(
        keyed.into_iter().map(|(_, item)| item).collect(),
    ))
}

/// Comparator family for a frozen sorted-collection key/element marshal.
enum CompareKind {
    /// Compare by the UNSIGNED order of the serialized wire bytes. Correct for
    /// byte-ordered Cassandra `AbstractType`s (UTF8Type/AsciiType/BytesType,
    /// InetAddressType, SimpleDateType — whose epoch is shifted so byte order is
    /// value order — BooleanType) AND for composite frozen UDT/tuple/collection
    /// elements (Cassandra orders those by their serialized bytes here too).
    UnsignedBytes,
    /// Compare as a SIGNED numeric of the given width: Int32Type→i32, LongType/
    /// CounterColumnType/TimestampType→i64, ByteType→i8, ShortType→i16.
    /// Unsigned big-endian byte order disagrees with the Cassandra comparator for
    /// these (e.g. `-1` = 0xFFFFFFFF would sort AFTER `0`), so they are compared on
    /// the decoded signed value.
    ///
    /// `TimeType` is deliberately NOT in this family — see the arms below.
    SignedInt,
}

/// Classify the comparator for a key/element marshal `ty`. A non-primitive marshal
/// (UDT/tuple/list/set/map element of a sorted collection) is byte-ordered. A
/// primitive marshal maps to its AbstractType comparator family; a primitive whose
/// comparator we cannot implement confidently returns `None` (caller fails closed).
fn classify_comparator(ty: &str) -> Option<CompareKind> {
    let Some(name) = primitive_marshal_name(ty) else {
        // Composite frozen element (UDT/tuple/collection): byte-ordered.
        return Some(CompareKind::UnsignedBytes);
    };
    match name {
        // Signed integers — Cassandra compares the decoded signed value.
        //
        // `TimestampType` belongs here and `TimeType` does NOT: authority, pinned
        // `cassandra-5.0.8`, `db/marshal/TimestampType.java:56`
        // `super(ComparisonType.CUSTOM)`, whose `compareCustom` (`:69-71`) is
        // exactly `return LongType.compareLongs(...)` — SIGNED. Conflating the two
        // temporal types was issue #3935.
        "Int32Type" | "LongType" | "ByteType" | "ShortType" | "CounterColumnType"
        | "TimestampType" => Some(CompareKind::SignedInt),
        // Byte-ordered AbstractTypes: unsigned serialized-byte order == comparator.
        // SimpleDateType is byte-ordered (epoch shifted by 2^31 at serialization).
        //
        // `TimeType` is BYTE_ORDER, not signed (issue #3935). Authority, pinned
        // `cassandra-5.0.8`: `db/marshal/TimeType.java:48`
        // `private TimeType() {super(ComparisonType.BYTE_ORDER);}`, i.e.
        // `ByteBufferUtil.compareUnsigned` over the serialized 8-byte big-endian
        // nanos-since-midnight long. Cassandra ACCEPTS, stores and BYTE_ORDERs an
        // out-of-range (negative) binary `time`, whose leading byte >= `0x80` then
        // sorts ABOVE every in-range value — so range validation would not make the
        // signed and byte orders agree. That argument, with its `TimeSerializer`
        // citations, is written out ONCE, in
        // `types::comparator::custom::compare_time`; do not restate it here.
        //
        // The two orders coincide for every NON-NEGATIVE `i64`, so no in-range
        // on-disk ordering moved. This comparator is the ONLY sort for a UDT's
        // `SetType`/`MapType` field: `serialization/types.rs`
        // `serialize_collection_elements` does not re-sort.
        "UTF8Type" | "AsciiType" | "BytesType" | "InetAddressType" | "BooleanType"
        | "SimpleDateType" | "TimeType" => Some(CompareKind::UnsignedBytes),
        // FAIL-CLOSED (no-heuristics, issue #28; tracked for #1254): types whose
        // Cassandra comparator is non-trivial and NOT plain unsigned-byte order:
        //   UUIDType/TimeUUIDType/LexicalUUIDType — version- and time-field-aware,
        //     not raw byte order;
        //   IntegerType (varint) / DecimalType — sign+magnitude/scale aware;
        //   FloatType/DoubleType — total-order with NaN/sign handling;
        //   DurationType — not a sortable AbstractType.
        _ => None,
    }
}

/// Ensure the comparator for `ty` can be applied to `value` (fail-closed up front).
fn comparator_supported_for(ty: &str, value: &Value) -> Result<()> {
    match classify_comparator(ty) {
        Some(CompareKind::UnsignedBytes) => Ok(()),
        Some(CompareKind::SignedInt) => {
            // Confirm the live value is one of the signed-int variants we decode.
            signed_value(value).map(|_| ())
        }
        None => Err(unsupported_comparator_err(ty)),
    }
}

fn unsupported_comparator_err(ty: &str) -> Error {
    Error::InvalidInput(format!(
        "frozen sorted-collection key/element type '{ty}' has no comparator implemented in the \
         canonicalizer; ordering it by raw serialized bytes could produce NON-Cassandra bytes, so \
         it is rejected rather than guessed (no-heuristics, issue #28; tracked for follow-up #1254)"
    ))
}

/// Decode the SIGNED i128 value of a signed-integer `Value`, or an error if the
/// variant is not one of the signed-int variants (Integer/BigInt/Counter/Timestamp/
/// TinyInt/SmallInt). Widening to `i128` makes all four widths comparable in one
/// ordering.
///
/// `Value::Time` is deliberately ABSENT (issue #3935): `TimeType` is
/// `ComparisonType.BYTE_ORDER`, so a `time` element/key is compared by its
/// serialized bytes via [`CompareKind::UnsignedBytes`] and never decoded here.
fn signed_value(value: &Value) -> Result<i128> {
    match value {
        Value::Integer(n) => Ok(*n as i128),
        Value::BigInt(n) | Value::Counter(n) | Value::Timestamp(n) => Ok(*n as i128),
        Value::TinyInt(n) => Ok(*n as i128),
        Value::SmallInt(n) => Ok(*n as i128),
        other => Err(Error::InvalidInput(format!(
            "expected a signed-integer value for a signed-comparator key/element type, got {other:?}"
        ))),
    }
}

/// Compare two key/element values for the marshal `ty`, given each value and its
/// precomputed serialized bytes. SIGNED-int marshals compare the decoded signed
/// values; byte-ordered marshals compare the unsigned serialized bytes.
fn compare_for_marshal(
    ty: &str,
    a_val: &Value,
    a_bytes: &[u8],
    b_val: &Value,
    b_bytes: &[u8],
) -> Result<Ordering> {
    match classify_comparator(ty) {
        Some(CompareKind::UnsignedBytes) => Ok(a_bytes.cmp(b_bytes)),
        Some(CompareKind::SignedInt) => Ok(signed_value(a_val)?.cmp(&signed_value(b_val)?)),
        None => Err(unsupported_comparator_err(ty)),
    }
}

/// Maps the sorted item vector back to the right `Value` collection variant so
/// `sort_sorted_collection` can serve both the frozen-set element path
/// (`T = Value`) and the frozen-map entry path (`T = (Value, Value)`).
trait SortedCollection<T> {
    fn from_sorted(items: Vec<T>) -> Value;
}

impl SortedCollection<Value> for Value {
    fn from_sorted(items: Vec<Value>) -> Value {
        Value::Set(items)
    }
}

impl SortedCollection<(Value, Value)> for Value {
    fn from_sorted(items: Vec<(Value, Value)>) -> Value {
        Value::Map(items)
    }
}

fn canonicalize_map(value: &Value, key_marshal: &str, val_marshal: &str) -> Result<Value> {
    let entries = match value {
        Value::Map(m) => m,
        other => {
            return Err(Error::InvalidInput(format!(
                "expected a map value for a MapType, got {other:?}"
            )))
        }
    };
    let mut out = Vec::with_capacity(entries.len());
    for (k, v) in entries {
        out.push((
            canonicalize_value_for_marshal(key_marshal, k)?,
            canonicalize_value_for_marshal(val_marshal, v)?,
        ));
    }
    // A frozen `MapType` is a SORTED collection: Cassandra stores its entries
    // ordered by the KEY's AbstractType comparator. That comparator is type-
    // dependent and is NOT unsigned-byte order for every key type (notably SIGNED
    // integers, where `-1` must sort before `0`). The writer's simple-cell path
    // emits entries in iteration order, so a multi-entry frozen map written with
    // unsorted keys would produce NON-Cassandra bytes. Sort by the comparator
    // DERIVED FROM `key_marshal` (issue #1020 roborev follow-up; #28 no-heuristics
    // — fail-closed on any key type whose comparator we cannot implement
    // confidently).
    sort_sorted_collection(out, key_marshal, |(k, _)| k)
}

fn canonicalize_tuple(value: &Value, components: &[String]) -> Result<Value> {
    let fields = match value {
        Value::Tuple(f) => f,
        other => {
            return Err(Error::InvalidInput(format!(
                "expected a tuple value for a TupleType, got {other:?}"
            )))
        }
    };
    // Reject arity mismatch (no-heuristics, issue #28): a tuple value with fewer
    // elements than the declared TupleType would omit trailing components (and
    // never write their `-1` absent markers); more elements would emit field
    // bytes with no matching declared component type → malformed wire bytes for a
    // UDT-bearing frozen tuple column. The typed tuple serializer
    // (serialization/types.rs `serialize_tuple`) rejects the same mismatch, so we
    // reject here too rather than pad/truncate.
    if fields.len() != components.len() {
        return Err(Error::InvalidInput(format!(
            "tuple field count mismatch for a TupleType: expected {}, got {}",
            components.len(),
            fields.len()
        )));
    }
    let mut out = Vec::with_capacity(fields.len());
    for (f, comp) in fields.iter().zip(components.iter()) {
        out.push(canonicalize_value_for_marshal(comp, f)?);
    }
    Ok(Value::Tuple(out))
}

// ── Marshal-type parsing (ASCII case-insensitive marker, original-case slices) ──

/// Strip a leading `FrozenType(...)` wrapper, returning the inner marshal type.
/// Idempotent on a non-frozen type. Slices from the original-case string so
/// nested marshal type names keep their case.
fn strip_frozen_marshal(ty: &str) -> &str {
    let ty = ty.trim();
    if let Some(inner) = marshal_inner(ty, "FrozenType") {
        return inner.trim();
    }
    ty
}

/// True iff `ty` is a top-level `UserType(...)` marshal.
fn is_user_type_marshal(ty: &str) -> bool {
    marshal_inner(ty, "UserType").is_some()
}

/// Return the inner (between the matching parens) of `<marker>(...)` if `ty`
/// starts with `org.apache.cassandra.db.marshal.<marker>(` (case-insensitive on
/// the qualified prefix + marker), else `None`.
fn marshal_inner<'a>(ty: &'a str, marker: &str) -> Option<&'a str> {
    let head = format!("{MARSHAL_PREFIX}{marker}(");
    let lower = ty.to_lowercase();
    if !lower.starts_with(&head.to_lowercase()) {
        return None;
    }
    let inner_start = head.len();
    let bytes = ty.as_bytes();
    let mut depth = 1usize;
    let mut i = inner_start;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&ty[inner_start..i]);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Element marshal of a `ListType(...)` / `SetType(...)`.
fn collection_element_marshal<'a>(ty: &'a str, marker: &str) -> Option<&'a str> {
    marshal_inner(ty, marker).map(str::trim)
}

/// `(key, value)` marshals of a `MapType(K,V)`.
fn map_kv_marshal(ty: &str) -> Option<(&str, &str)> {
    let inner = marshal_inner(ty, "MapType")?;
    let parts = split_top_level(inner);
    if parts.len() == 2 {
        Some((parts[0].trim(), parts[1].trim()))
    } else {
        None
    }
}

/// Component marshals of a `TupleType(T1,T2,...)`.
fn tuple_component_marshals(ty: &str) -> Option<Vec<String>> {
    let inner = marshal_inner(ty, "TupleType")?;
    Some(
        split_top_level(inner)
            .into_iter()
            .map(|s| s.trim().to_string())
            .collect(),
    )
}

/// Parse the declared `(field_name, field_marshal_type)` pairs of a
/// `UserType(...)` marshal (or `FrozenType(UserType(...))`), preserving declared
/// order. Field names are hex-decoded; the type is the original-case marshal
/// substring after the first colon. The first two args (keyspace, hex-name) are
/// skipped.
fn parse_user_type_fields(user_type_marshal: &str) -> Result<Vec<(String, String)>> {
    let stripped = strip_frozen_marshal(user_type_marshal);
    let inner = marshal_inner(stripped, "UserType").ok_or_else(|| {
        Error::InvalidInput(format!("not a UserType marshal: {user_type_marshal}"))
    })?;
    let parts = split_top_level(inner);
    let mut out = Vec::with_capacity(parts.len().saturating_sub(2));
    for field_def in parts.iter().skip(2) {
        let field_def = field_def.trim();
        if field_def.is_empty() {
            continue;
        }
        let colon = field_def.find(':').ok_or_else(|| {
            Error::InvalidInput(format!("invalid UDT field (missing colon): {field_def}"))
        })?;
        let name_bytes = hex::decode(&field_def[..colon]).map_err(|e| {
            Error::InvalidInput(format!(
                "invalid hex UDT field name '{}': {e}",
                &field_def[..colon]
            ))
        })?;
        let name = String::from_utf8(name_bytes)
            .map_err(|e| Error::InvalidInput(format!("invalid UTF-8 UDT field name: {e}")))?;
        out.push((name, field_def[colon + 1..].trim().to_string()));
    }
    Ok(out)
}

/// Split top-level comma-separated args, respecting nested parens.
fn split_top_level(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    let last = &s[start..];
    if !last.trim().is_empty() {
        parts.push(last);
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;

    const KS: &str = "test_ks";

    // person { first_name text, last_name text, age int }
    fn person_marshal() -> String {
        format!(
            "{p}FrozenType({p}UserType({KS},706572736f6e,\
             66697273745f6e616d65:{p}UTF8Type,6c6173745f6e616d65:{p}UTF8Type,616765:{p}Int32Type))",
            p = MARSHAL_PREFIX
        )
    }

    fn udt(fields: Vec<(&str, Option<Value>)>) -> Value {
        Value::Frozen(Box::new(Value::Udt(Box::new(UdtValue {
            type_name: "person".into(),
            keyspace: KS.into(),
            fields: fields
                .into_iter()
                .map(|(name, value)| UdtField {
                    name: name.into(),
                    value,
                })
                .collect(),
        }))))
    }

    fn declared_order(v: &Value) -> Vec<(String, Option<Value>)> {
        match v {
            Value::Frozen(inner) => declared_order(inner),
            Value::Udt(u) => u
                .fields
                .iter()
                .map(|f| (f.name.clone(), f.value.clone()))
                .collect(),
            other => panic!("expected frozen UDT, got {other:?}"),
        }
    }

    #[test]
    fn out_of_order_literal_is_reordered_to_declared_order() {
        // Literal: age, last_name, first_name (reversed). Must become declared
        // order: first_name, last_name, age.
        let v = udt(vec![
            ("age", Some(Value::Integer(36))),
            ("last_name", Some(Value::Text("Lovelace".into()))),
            ("first_name", Some(Value::Text("Ada".into()))),
        ]);
        let canon = canonicalize_udt_value(&person_marshal(), &v).unwrap();
        let order = declared_order(canon.as_ref());
        assert_eq!(
            order,
            vec![
                ("first_name".into(), Some(Value::Text("Ada".into()))),
                ("last_name".into(), Some(Value::Text("Lovelace".into()))),
                ("age".into(), Some(Value::Integer(36))),
            ]
        );
    }

    #[test]
    fn sparse_literal_pads_missing_declared_fields_with_none() {
        // Literal only carries `age`; first_name + last_name must be padded None
        // (serialize_udt then writes the `-1` absent marker for them).
        let v = udt(vec![("age", Some(Value::Integer(85)))]);
        let canon = canonicalize_udt_value(&person_marshal(), &v).unwrap();
        let order = declared_order(canon.as_ref());
        assert_eq!(
            order,
            vec![
                ("first_name".into(), None),
                ("last_name".into(), None),
                ("age".into(), Some(Value::Integer(85))),
            ]
        );
    }

    #[test]
    fn extra_unknown_field_is_rejected() {
        let v = udt(vec![
            ("first_name", Some(Value::Text("Ada".into()))),
            ("nickname", Some(Value::Text("Countess".into()))),
        ]);
        let err = canonicalize_udt_value(&person_marshal(), &v).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("nickname") && msg.contains("not a declared field"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn non_udt_column_passes_through_unchanged() {
        // A frozen<list<int>> column carries a CQL type (no `UserType(` marker)
        // and the fast path returns the value unchanged.
        let v = Value::Frozen(Box::new(Value::List(vec![
            Value::Integer(3),
            Value::Integer(1),
            Value::Integer(2),
        ])));
        let canon = canonicalize_udt_value("frozen<list<int>>", &v).unwrap();
        // Finding 2: the non-UDT fast path borrows (no clone).
        assert!(matches!(canon, Cow::Borrowed(_)));
        assert_eq!(canon.as_ref(), &v);
    }

    #[test]
    fn opaque_blob_value_passes_through_unchanged_for_udt_marshal() {
        // COMPACTION path: a frozen<list<frozen<person>>> cell decoded from an
        // input SSTable comes back as an opaque Value::Blob of its already-
        // canonical wire bytes. The canonicalizer must pass it through unchanged
        // even though the declared marshal references a UserType — it has no
        // structured fields to reorder, and reinterpreting opaque bytes is
        // forbidden (no-heuristics, issue #28).
        let list_marshal = format!(
            "{p}FrozenType({p}ListType({p}UserType({KS},706572736f6e,\
             66697273745f6e616d65:{p}UTF8Type,6c6173745f6e616d65:{p}UTF8Type,616765:{p}Int32Type)))",
            p = MARSHAL_PREFIX
        );
        let blob = Value::blob(vec![0, 0, 0, 1, 0, 0, 0, 3, 65, 100, 97]);
        let canon = canonicalize_udt_value(&list_marshal, &blob).unwrap();
        assert_eq!(canon.as_ref(), &blob, "opaque blob must be byte-identical");
    }

    #[test]
    fn tuple_wrong_arity_is_rejected() {
        // A frozen<tuple<int, text>> declared as a 2-component TupleType. A value
        // with 1 element (missing) and a value with 3 elements (extra) must BOTH
        // error rather than silently truncating/padding to malformed wire bytes.
        let tuple_marshal = format!(
            "{p}FrozenType({p}TupleType({p}Int32Type,{p}UTF8Type))",
            p = MARSHAL_PREFIX
        );

        let too_few = Value::Frozen(Box::new(Value::Tuple(vec![Value::Integer(7)])));
        let err = canonicalize_udt_value(&tuple_marshal, &too_few);
        // Fast path: a primitive-only tuple has no `UserType(` marker, so
        // canonicalize_udt_value short-circuits and returns Ok unchanged. Drive
        // the marshal recursion directly to exercise the arity guard.
        let _ = err;
        let components = vec![
            format!("{MARSHAL_PREFIX}Int32Type"),
            format!("{MARSHAL_PREFIX}UTF8Type"),
        ];
        let too_few_inner = Value::Tuple(vec![Value::Integer(7)]);
        let msg = format!(
            "{}",
            canonicalize_tuple(&too_few_inner, &components).unwrap_err()
        );
        assert!(
            msg.contains("expected 2, got 1") && msg.contains("mismatch"),
            "unexpected error: {msg}"
        );

        let too_many_inner = Value::Tuple(vec![
            Value::Integer(7),
            Value::Text("x".into()),
            Value::Text("extra".into()),
        ]);
        let msg = format!(
            "{}",
            canonicalize_tuple(&too_many_inner, &components).unwrap_err()
        );
        assert!(
            msg.contains("expected 2, got 3") && msg.contains("mismatch"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn udt_bearing_frozen_tuple_correct_arity_canonicalizes() {
        // A frozen<tuple<int, frozen<person>>>: the nested UDT component (2nd
        // element) must be reordered to declared order, and the correct-arity
        // tuple canonicalizes without error.
        let tuple_marshal = format!(
            "{p}FrozenType({p}TupleType({p}Int32Type,{p}UserType({KS},706572736f6e,\
             66697273745f6e616d65:{p}UTF8Type,6c6173745f6e616d65:{p}UTF8Type,616765:{p}Int32Type)))",
            p = MARSHAL_PREFIX
        );
        let person = Value::Udt(Box::new(UdtValue {
            type_name: "person".into(),
            keyspace: KS.into(),
            // out-of-order fields
            fields: vec![
                UdtField {
                    name: "age".into(),
                    value: Some(Value::Integer(28)),
                },
                UdtField {
                    name: "first_name".into(),
                    value: Some(Value::Text("Grace".into())),
                },
            ],
        }));
        let v = Value::Frozen(Box::new(Value::Tuple(vec![Value::Integer(1), person])));
        let canon = canonicalize_udt_value(&tuple_marshal, &v).unwrap();
        let Value::Frozen(inner) = canon.as_ref() else {
            panic!("expected frozen tuple");
        };
        let Value::Tuple(items) = inner.as_ref() else {
            panic!("expected tuple");
        };
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], Value::Integer(1));
        let order = declared_order(&items[1]);
        assert_eq!(
            order,
            vec![
                ("first_name".into(), Some(Value::Text("Grace".into()))),
                ("last_name".into(), None),
                ("age".into(), Some(Value::Integer(28))),
            ]
        );
    }

    #[test]
    fn nested_udt_inside_frozen_list_is_canonicalized() {
        // A frozen<list<frozen<person>>> column: the nested person ELEMENT must be
        // reordered to declared order too.
        let list_marshal = format!(
            "{p}FrozenType({p}ListType({p}UserType({KS},706572736f6e,\
             66697273745f6e616d65:{p}UTF8Type,6c6173745f6e616d65:{p}UTF8Type,616765:{p}Int32Type)))",
            p = MARSHAL_PREFIX
        );
        let element = Value::Udt(Box::new(UdtValue {
            type_name: "person".into(),
            keyspace: KS.into(),
            // out-of-order element fields
            fields: vec![
                UdtField {
                    name: "age".into(),
                    value: Some(Value::Integer(41)),
                },
                UdtField {
                    name: "first_name".into(),
                    value: Some(Value::Text("Alan".into())),
                },
            ],
        }));
        let v = Value::Frozen(Box::new(Value::List(vec![element])));
        let canon = canonicalize_udt_value(&list_marshal, &v).unwrap();
        let Value::Frozen(inner) = canon.as_ref() else {
            panic!("expected frozen list");
        };
        let Value::List(items) = inner.as_ref() else {
            panic!("expected list");
        };
        let order = declared_order(&items[0]);
        assert_eq!(
            order,
            vec![
                ("first_name".into(), Some(Value::Text("Alan".into()))),
                ("last_name".into(), None),
                ("age".into(), Some(Value::Integer(41))),
            ]
        );
    }

    #[test]
    fn wrong_primitive_field_type_is_rejected() {
        // person.age declares Int32Type. A BigInt in that slot would serialize as
        // 8-byte LongType bytes, disagreeing with the declared header. roborev
        // #1020 Finding 1: reject the leaf-type mismatch (no coercion).
        let v = udt(vec![
            ("first_name", Some(Value::Text("Ada".into()))),
            ("age", Some(Value::BigInt(36))), // wrong: BigInt in an Int32Type slot
        ]);
        let err = canonicalize_udt_value(&person_marshal(), &v).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("Int32Type") && msg.contains("does not match declared marshal"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn correct_primitive_field_types_canonicalize() {
        // The matching variants (Text for UTF8Type, Integer for Int32Type) pass
        // leaf validation and canonicalize to declared order.
        let v = udt(vec![
            ("age", Some(Value::Integer(36))),
            ("first_name", Some(Value::Text("Ada".into()))),
            ("last_name", Some(Value::Text("Lovelace".into()))),
        ]);
        let canon = canonicalize_udt_value(&person_marshal(), &v).unwrap();
        let order = declared_order(canon.as_ref());
        assert_eq!(
            order,
            vec![
                ("first_name".into(), Some(Value::Text("Ada".into()))),
                ("last_name".into(), Some(Value::Text("Lovelace".into()))),
                ("age".into(), Some(Value::Integer(36))),
            ]
        );
    }

    // A frozen<set<frozen<person>>> marshal — used to exercise the list/set kind
    // guard while still tripping the `references_user_type` fast-path gate.
    fn set_of_person_marshal() -> String {
        format!(
            "{p}FrozenType({p}SetType({p}UserType({KS},706572736f6e,\
             66697273745f6e616d65:{p}UTF8Type,6c6173745f6e616d65:{p}UTF8Type,616765:{p}Int32Type)))",
            p = MARSHAL_PREFIX
        )
    }

    fn list_of_person_marshal() -> String {
        format!(
            "{p}FrozenType({p}ListType({p}UserType({KS},706572736f6e,\
             66697273745f6e616d65:{p}UTF8Type,6c6173745f6e616d65:{p}UTF8Type,616765:{p}Int32Type)))",
            p = MARSHAL_PREFIX
        )
    }

    fn person_value() -> Value {
        Value::Udt(Box::new(UdtValue {
            type_name: "person".into(),
            keyspace: KS.into(),
            fields: vec![UdtField {
                name: "first_name".into(),
                value: Some(Value::Text("Ada".into())),
            }],
        }))
    }

    #[test]
    fn list_value_in_set_type_slot_is_rejected() {
        // A SetType marshal carrying a Value::List must error (roborev #1020
        // Finding 1: list/set kind mismatch is rejected, not silently re-tagged).
        let v = Value::Frozen(Box::new(Value::List(vec![person_value()])));
        let err = canonicalize_udt_value(&set_of_person_marshal(), &v).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("SetType") && msg.contains("List"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn set_value_in_list_type_slot_is_rejected() {
        let v = Value::Frozen(Box::new(Value::Set(vec![person_value()])));
        let err = canonicalize_udt_value(&list_of_person_marshal(), &v).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("ListType") && msg.contains("Set"),
            "unexpected error: {msg}"
        );
    }

    // A frozen<map<text, frozen<person>>> marshal: UDT-bearing map value so it
    // trips the `references_user_type` fast-path gate and exercises key sorting.
    fn map_text_to_person_marshal() -> String {
        format!(
            "{p}FrozenType({p}MapType({p}UTF8Type,{p}FrozenType({p}UserType({KS},706572736f6e,\
             66697273745f6e616d65:{p}UTF8Type,6c6173745f6e616d65:{p}UTF8Type,616765:{p}Int32Type))))",
            p = MARSHAL_PREFIX
        )
    }

    fn named_person(first: &str) -> Value {
        Value::Udt(Box::new(UdtValue {
            type_name: "person".into(),
            keyspace: KS.into(),
            fields: vec![UdtField {
                name: "first_name".into(),
                value: Some(Value::text(first)),
            }],
        }))
    }

    #[test]
    fn multi_entry_frozen_map_is_sorted_by_serialized_key_bytes() {
        // Keys inserted in REVERSED order ("c","b","a"). Cassandra's MapType is a
        // sorted collection keyed by the key AbstractType comparator (UTF8Type =
        // unsigned serialized-byte order), so the canonical output must be a < b < c.
        let v = Value::Frozen(Box::new(Value::Map(vec![
            (Value::Text("c".into()), named_person("Carol")),
            (Value::Text("b".into()), named_person("Bob")),
            (Value::Text("a".into()), named_person("Ada")),
        ])));
        let canon = canonicalize_udt_value(&map_text_to_person_marshal(), &v).unwrap();
        let Value::Frozen(inner) = canon.as_ref() else {
            panic!("expected frozen map");
        };
        let Value::Map(entries) = inner.as_ref() else {
            panic!("expected map, got {inner:?}");
        };
        let keys: Vec<&str> = entries
            .iter()
            .map(|(k, _)| match k {
                Value::Text(s) => std::str::from_utf8(s).unwrap_or_default(),
                other => panic!("expected text key, got {other:?}"),
            })
            .collect();
        assert_eq!(
            keys,
            vec!["a", "b", "c"],
            "map keys must be sorted by serialized bytes"
        );
        // Values must stay paired with their keys after the sort, and the nested
        // UDT must still be canonicalized to declared field order.
        let first_for = |entries: &[(Value, Value)], want_key: &str| {
            let (_, person) = entries
                .iter()
                .find(|(k, _)| matches!(k, Value::Text(s) if s == want_key))
                .unwrap();
            declared_order(person)
        };
        assert_eq!(
            first_for(entries, "a"),
            vec![
                ("first_name".into(), Some(Value::Text("Ada".into()))),
                ("last_name".into(), None),
                ("age".into(), None),
            ]
        );
    }

    // frozen<set<text>> marshal (no UDT) would skip the fast-path gate, so use a
    // frozen<set<frozen<person>>> to exercise element sorting through the gate.
    #[test]
    fn multi_element_frozen_set_is_sorted_by_serialized_element_bytes() {
        // Elements inserted in REVERSED first_name order. Set elements sort by the
        // element AbstractType comparator = unsigned serialized-byte order of the
        // full frozen-UDT wire bytes (which here begins with the first_name field
        // length+bytes), so canonical order is Ada < Bob < Carol.
        let v = Value::Frozen(Box::new(Value::Set(vec![
            named_person("Carol"),
            named_person("Bob"),
            named_person("Ada"),
        ])));
        let canon = canonicalize_udt_value(&set_of_person_marshal(), &v).unwrap();
        let Value::Frozen(inner) = canon.as_ref() else {
            panic!("expected frozen set");
        };
        let Value::Set(items) = inner.as_ref() else {
            panic!("expected set, got {inner:?}");
        };
        let firsts: Vec<String> = items
            .iter()
            .map(|p| match declared_order(p).into_iter().next() {
                Some((_, Some(Value::Text(s)))) => String::from_utf8_lossy(&s).into_owned(),
                other => panic!("unexpected first field: {other:?}"),
            })
            .collect();
        assert_eq!(
            firsts,
            vec!["Ada".to_string(), "Bob".to_string(), "Carol".to_string()],
            "set elements must be sorted by serialized element bytes"
        );
    }

    // A frozen<map<int, frozen<person>>> marshal: an Int32Type KEY (a SIGNED
    // integer) exercises the type-aware comparator. Negative keys MUST sort before
    // 0 by signed-numeric order, NOT by unsigned big-endian byte order.
    fn map_int_to_person_marshal() -> String {
        format!(
            "{p}FrozenType({p}MapType({p}Int32Type,{p}FrozenType({p}UserType({KS},706572736f6e,\
             66697273745f6e616d65:{p}UTF8Type,6c6173745f6e616d65:{p}UTF8Type,616765:{p}Int32Type))))",
            p = MARSHAL_PREFIX
        )
    }

    #[test]
    fn frozen_map_with_negative_int_keys_sorts_signed_not_unsigned() {
        // Keys inserted OUT OF ORDER, mixing negatives and positives. Cassandra's
        // Int32Type comparator is SIGNED: -5 < -1 < 0 < 2 < 7. Unsigned big-endian
        // byte order would WRONGLY put 0,2,7 (0x00.., 0x02.., 0x07..) before -5,-1
        // (0xFF..) — this test guards against that regression.
        let v = Value::Frozen(Box::new(Value::Map(vec![
            (Value::Integer(7), named_person("Seven")),
            (Value::Integer(-1), named_person("MinusOne")),
            (Value::Integer(0), named_person("Zero")),
            (Value::Integer(-5), named_person("MinusFive")),
            (Value::Integer(2), named_person("Two")),
        ])));
        let canon = canonicalize_udt_value(&map_int_to_person_marshal(), &v).unwrap();
        let Value::Frozen(inner) = canon.as_ref() else {
            panic!("expected frozen map");
        };
        let Value::Map(entries) = inner.as_ref() else {
            panic!("expected map, got {inner:?}");
        };
        let keys: Vec<i32> = entries
            .iter()
            .map(|(k, _)| match k {
                Value::Integer(n) => *n,
                other => panic!("expected int key, got {other:?}"),
            })
            .collect();
        assert_eq!(
            keys,
            vec![-5, -1, 0, 2, 7],
            "int map keys must be sorted by SIGNED numeric order (-N..-1,0,1..N), not unsigned bytes"
        );
    }

    // frozen<set<int>> is not UDT-bearing, so it skips the references_user_type
    // fast-path gate; exercise the signed-int SET element comparator by driving
    // canonicalize_seq directly (the same path canonicalize_udt_value reaches for a
    // UDT-bearing set whose elements are signed ints would use).
    #[test]
    fn frozen_set_with_negative_int_elements_sorts_signed_not_unsigned() {
        let elem_marshal = format!("{MARSHAL_PREFIX}Int32Type");
        let set = Value::Set(vec![
            Value::Integer(3),
            Value::Integer(-2),
            Value::Integer(0),
            Value::Integer(-10),
            Value::Integer(1),
        ]);
        let canon = canonicalize_seq(&set, &elem_marshal, SeqKind::Set).unwrap();
        let Value::Set(items) = canon else {
            panic!("expected set, got {canon:?}");
        };
        let elems: Vec<i32> = items
            .iter()
            .map(|e| match e {
                Value::Integer(n) => *n,
                other => panic!("expected int element, got {other:?}"),
            })
            .collect();
        assert_eq!(
            elems,
            vec![-10, -2, 0, 1, 3],
            "int set elements must be sorted by SIGNED numeric order, not unsigned bytes"
        );
    }

    #[test]
    fn frozen_set_bigint_negative_elements_sort_signed() {
        // LongType is also signed; -1i64 (0xFFFF..) must sort before 0.
        let elem_marshal = format!("{MARSHAL_PREFIX}LongType");
        let set = Value::Set(vec![
            Value::BigInt(5),
            Value::BigInt(-1),
            Value::BigInt(-100),
            Value::BigInt(0),
        ]);
        let canon = canonicalize_seq(&set, &elem_marshal, SeqKind::Set).unwrap();
        let Value::Set(items) = canon else {
            panic!("expected set");
        };
        let elems: Vec<i64> = items
            .iter()
            .map(|e| match e {
                Value::BigInt(n) => *n,
                other => panic!("expected bigint, got {other:?}"),
            })
            .collect();
        assert_eq!(elems, vec![-100, -1, 0, 5]);
    }

    #[test]
    fn frozen_set_unsupported_key_type_is_fail_closed() {
        // UUIDType has a version/time-aware comparator we do NOT implement; the
        // canonicalizer must REJECT (fail-closed) rather than sort by raw bytes
        // (no-heuristics, issue #28; tracked for #1254). Two elements are required
        // so the sort path actually evaluates the comparator.
        let elem_marshal = format!("{MARSHAL_PREFIX}UUIDType");
        let set = Value::Set(vec![Value::Uuid([1u8; 16]), Value::Uuid([2u8; 16])]);
        let err = canonicalize_seq(&set, &elem_marshal, SeqKind::Set).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("UUIDType") && msg.contains("no comparator implemented"),
            "unexpected error: {msg}"
        );
    }

    // A frozen<map<uuid, frozen<person>>> marshal: a UUIDType KEY whose
    // comparator is NOT implemented (fail-closed for len >= 2). Used to verify the
    // roborev #1020 Finding 2 regression guard: a SINGLE-entry map must NOT trip
    // the unsupported-comparator error (no ordering decision is needed), while a
    // 2+-entry map still fails closed (behavior preserved).
    fn map_uuid_to_person_marshal() -> String {
        format!(
            "{p}FrozenType({p}MapType({p}UUIDType,{p}FrozenType({p}UserType({KS},706572736f6e,\
             66697273745f6e616d65:{p}UTF8Type,6c6173745f6e616d65:{p}UTF8Type,616765:{p}Int32Type))))",
            p = MARSHAL_PREFIX
        )
    }

    #[test]
    fn single_entry_uuid_keyed_map_canonicalizes_without_error() {
        // roborev #1020 Finding 2 REGRESSION GUARD: a one-entry
        // frozen<map<uuid, frozen<person>>> needs no ordering decision, so the
        // unsupported-UUIDType comparator must NOT be consulted. The round-6 fix
        // wrongly errored here; this must now succeed and reorder the nested UDT.
        let v = Value::Frozen(Box::new(Value::Map(vec![(
            Value::Uuid([7u8; 16]),
            named_person("Solo"),
        )])));
        let canon = canonicalize_udt_value(&map_uuid_to_person_marshal(), &v)
            .expect("single-entry uuid-keyed map must canonicalize without a comparator error");
        let Value::Frozen(inner) = canon.as_ref() else {
            panic!("expected frozen map");
        };
        let Value::Map(entries) = inner.as_ref() else {
            panic!("expected map, got {inner:?}");
        };
        assert_eq!(entries.len(), 1);
        assert!(matches!(&entries[0].0, Value::Uuid(u) if u == &[7u8; 16]));
        // The nested UDT value is still canonicalized to declared field order.
        assert_eq!(
            declared_order(&entries[0].1),
            vec![
                ("first_name".into(), Some(Value::Text("Solo".into()))),
                ("last_name".into(), None),
                ("age".into(), None),
            ]
        );
    }

    #[test]
    fn empty_uuid_keyed_map_canonicalizes_without_error() {
        // Zero entries: also a no-op for ordering — must not consult the
        // unsupported comparator (roborev #1020 Finding 2 regression guard).
        let v = Value::Frozen(Box::new(Value::Map(vec![])));
        let canon = canonicalize_udt_value(&map_uuid_to_person_marshal(), &v)
            .expect("empty uuid-keyed map must canonicalize without a comparator error");
        let Value::Frozen(inner) = canon.as_ref() else {
            panic!("expected frozen map");
        };
        let Value::Map(entries) = inner.as_ref() else {
            panic!("expected map, got {inner:?}");
        };
        assert!(entries.is_empty());
    }

    #[test]
    fn multi_entry_uuid_keyed_map_still_fails_closed() {
        // 2+ entries DO require an ordering decision: the unsupported UUIDType
        // comparator must still fail closed (round-6 behavior preserved — NOT
        // weakened by the len<2 regression guard).
        let v = Value::Frozen(Box::new(Value::Map(vec![
            (Value::Uuid([1u8; 16]), named_person("One")),
            (Value::Uuid([2u8; 16]), named_person("Two")),
        ])));
        let err = canonicalize_udt_value(&map_uuid_to_person_marshal(), &v).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("UUIDType") && msg.contains("no comparator implemented"),
            "2+-entry uuid-keyed map must still fail closed: {msg}"
        );
    }

    #[test]
    fn single_entry_frozen_map_unchanged_by_sort() {
        // Sorting a single-entry map is a no-op: confirms the existing
        // single-entry UDT-map scenarios keep byte-identical output.
        let v = Value::Frozen(Box::new(Value::Map(vec![(
            Value::Text("only".into()),
            named_person("Solo"),
        )])));
        let canon = canonicalize_udt_value(&map_text_to_person_marshal(), &v).unwrap();
        let Value::Frozen(inner) = canon.as_ref() else {
            panic!("expected frozen map");
        };
        let Value::Map(entries) = inner.as_ref() else {
            panic!("expected map");
        };
        assert_eq!(entries.len(), 1);
        assert!(matches!(&entries[0].0, Value::Text(s) if s == "only"));
    }

    #[test]
    fn matching_collection_kind_canonicalizes() {
        // A SetType marshal with a Value::Set (and its nested UDT) canonicalizes.
        let v = Value::Frozen(Box::new(Value::Set(vec![person_value()])));
        let canon = canonicalize_udt_value(&set_of_person_marshal(), &v).unwrap();
        let Value::Frozen(inner) = canon.as_ref() else {
            panic!("expected frozen set");
        };
        let Value::Set(items) = inner.as_ref() else {
            panic!("expected set, got {inner:?}");
        };
        let order = declared_order(&items[0]);
        assert_eq!(
            order,
            vec![
                ("first_name".into(), Some(Value::Text("Ada".into()))),
                ("last_name".into(), None),
                ("age".into(), None),
            ]
        );
    }
}
