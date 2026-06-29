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
//! (`v5_compressed_legacy::udt::parse_udt_value`) reads `-1` back as a null
//! field. Canonicalizing the literal to declared order + `None` padding therefore
//! makes the value bytes self-consistent with the declared `UserType(...)` header.
//!
//! Part of the `data_writer` responsibility split (issue #1118). `use super::*`
//! provides the crate imports and sibling helpers.

use super::*;
use crate::types::{UdtField, UdtValue};

const MARSHAL_PREFIX: &str = "org.apache.cassandra.db.marshal.";

/// Canonicalize a frozen-UDT (or UDT-bearing frozen collection/tuple) VALUE
/// against the column's authoritative `data_type` marshal (roborev #1020
/// Finding 1).
///
/// Returns a `Value` whose every UDT — at any depth reachable through
/// `FrozenType`/`ListType`/`SetType`/`MapType`/`TupleType` — has its fields in
/// DECLARED order, missing declared fields padded with `None`, and any unknown
/// literal field rejected with an error. When `data_type` does NOT reference a
/// UDT (a primitive, or a frozen collection of primitives), or is not a marshal
/// string, the value is returned UNCHANGED (cheap, allocation-free clone of the
/// borrow via `Cow`-free passthrough) — the existing serialization path is
/// byte-identical for those.
pub(crate) fn canonicalize_udt_value(data_type: &str, value: &Value) -> Result<Value> {
    // Fast path: no UDT anywhere in the declared type → nothing to canonicalize.
    if !references_user_type(data_type) {
        return Ok(value.clone());
    }
    canonicalize_value_for_marshal(data_type.trim(), value)
}

/// Canonicalize a STATIC column's value against the column's declared
/// `data_type` resolved from `schema` by name (roborev #1020 Finding 1, static
/// path). A column not found in `schema` (defensive) or a non-UDT column returns
/// the value unchanged.
pub(crate) fn canonicalize_static_value(
    schema: &TableSchema,
    column: &str,
    value: &Value,
) -> Result<Value> {
    match schema.columns.iter().find(|c| c.name == column) {
        Some(col) => canonicalize_udt_value(&col.data_type, value),
        None => Ok(value.clone()),
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
        return canonicalize_seq(value, elem, Value::List);
    }
    if let Some(elem) = collection_element_marshal(ty, "SetType") {
        return canonicalize_seq(value, elem, Value::Set);
    }
    if let Some((k, v)) = map_kv_marshal(ty) {
        return canonicalize_map(value, k, v);
    }
    if let Some(components) = tuple_component_marshals(ty) {
        return canonicalize_tuple(value, &components);
    }

    // Primitive (or a type with no UDT inside): leave the value untouched.
    Ok(value.clone())
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

    Ok(Value::Udt(UdtValue {
        type_name: udt.type_name.clone(),
        keyspace: udt.keyspace.clone(),
        fields,
    }))
}

fn canonicalize_seq(
    value: &Value,
    elem_marshal: &str,
    wrap: impl Fn(Vec<Value>) -> Value,
) -> Result<Value> {
    let elems = match value {
        Value::List(e) | Value::Set(e) => e,
        other => {
            return Err(Error::InvalidInput(format!(
                "expected a list/set value for a collection type, got {other:?}"
            )))
        }
    };
    let mut out = Vec::with_capacity(elems.len());
    for e in elems {
        out.push(canonicalize_value_for_marshal(elem_marshal, e)?);
    }
    Ok(wrap(out))
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
    Ok(Value::Map(out))
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
    let mut out = Vec::with_capacity(fields.len());
    for (i, f) in fields.iter().enumerate() {
        let comp = components.get(i).map(String::as_str).unwrap_or("");
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
        Value::Frozen(Box::new(Value::Udt(UdtValue {
            type_name: "person".into(),
            keyspace: KS.into(),
            fields: fields
                .into_iter()
                .map(|(name, value)| UdtField {
                    name: name.into(),
                    value,
                })
                .collect(),
        })))
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
        let order = declared_order(&canon);
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
        let order = declared_order(&canon);
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
        assert_eq!(canon, v);
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
        let element = Value::Udt(UdtValue {
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
        });
        let v = Value::Frozen(Box::new(Value::List(vec![element])));
        let canon = canonicalize_udt_value(&list_marshal, &v).unwrap();
        let Value::Frozen(inner) = &canon else {
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
}
