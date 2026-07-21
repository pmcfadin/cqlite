//! Regression tests for issue #2807 — the DECODE surface for keyspace-qualified
//! UDT type names.
//!
//! Cassandra emits UDT column types keyspace-qualified (`frozen<ks.addr>`), which
//! the CQL parser now RETAINS in `column.data_type`. The registry is keyed by BARE
//! name, so a plain `get_udt(&self.keyspace, "ks.addr")` MISSES and the value
//! silently degrades to `Blob` (worse than the pre-fix hard parse failure). These
//! tests drive the reader's registry-backed decode fallback
//! ([`V5CompressedLegacyParser::parse_raw_type_value`], `pub(super)`, reachable
//! from this child module) with a QUALIFIED type name and assert the UDT
//! materializes as a STRUCTURED [`crate::Value::Udt`], never a `Blob` — proving the
//! lookups route through the qualifier-aware
//! [`crate::schema::UdtRegistry::get_udt_qualified`].
//!
//! These carry NO dataset/reader/feature-flag dependency: `parse_raw_type_value`
//! is a `pub(super)` method on a plainly-constructed parser, so they run in every
//! build/lane (never a vacuous 0-row skip).

use super::V5CompressedLegacyParser;
use crate::schema::{CqlType, UdtRegistry};
use crate::types::UdtTypeDef;
use crate::Value;

const KEYSPACE: &str = "cassandra_easy_stress";

/// Registry holding a bare-keyed `addr` UDT (street text, city text) in
/// `cassandra_easy_stress` — exactly how `udt_registry_from_cql` keys a
/// `CREATE TYPE cassandra_easy_stress.addr (...)`.
fn registry_with_addr() -> UdtRegistry {
    let mut reg = UdtRegistry::new();
    reg.register_udt(
        UdtTypeDef::new(KEYSPACE.to_string(), "addr".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("city".to_string(), CqlType::Text, true),
    );
    reg
}

/// Encode a frozen-context UDT body: each field is a 4-byte big-endian i32 length
/// prefix followed by the raw field bytes (text = raw UTF-8).
fn encode_udt(fields: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    for f in fields {
        out.extend_from_slice(&(f.len() as i32).to_be_bytes());
        out.extend_from_slice(f.as_bytes());
    }
    out
}

fn parser_with_registry() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new(KEYSPACE.to_string(), "t".to_string(), 0, 0, None)
        .with_udt_registry(registry_with_addr())
}

/// The core #2807 decode assertion: a KEYSPACE-QUALIFIED type name
/// (`cassandra_easy_stress.addr`) resolves the bare-keyed registry entry and
/// decodes to a structured `Value::Udt` — NOT a `Blob`.
#[test]
fn qualified_udt_type_name_decodes_to_struct_not_blob() {
    let parser = parser_with_registry();
    let data = encode_udt(&["main st", "nyc"]);

    let (value, _off) = parser
        .parse_raw_type_value(&data, 0, "cassandra_easy_stress.addr", "col", 0)
        .expect("qualified UDT type name must decode via the registry");

    match value {
        Value::Udt(udt) => {
            assert_eq!(udt.type_name, "addr", "resolved to the bare-keyed UDT");
            assert_eq!(udt.fields.len(), 2, "street + city materialized");
            assert_eq!(udt.fields[0].name, "street");
            assert_eq!(udt.fields[0].value, Some(Value::Text("main st".into())));
        }
        other => {
            panic!("qualified UDT must decode to Value::Udt, got {other:?} (Blob = the #2807 bug)")
        }
    }
}

/// The unqualified form must still decode identically (regression guard).
#[test]
fn unqualified_udt_type_name_still_decodes_to_struct() {
    let parser = parser_with_registry();
    let data = encode_udt(&["main st", "nyc"]);

    let (value, _off) = parser
        .parse_raw_type_value(&data, 0, "addr", "col", 0)
        .expect("bare UDT type name must still decode");

    assert!(
        matches!(value, Value::Udt(ref udt) if udt.type_name == "addr" && udt.fields.len() == 2),
        "unqualified `addr` must still resolve to the struct, got {value:?}"
    );
}

/// A genuinely-unregistered qualified reference must NOT falsely resolve: the
/// split keyspace has no such UDT, so decode must not produce the registered UDT
/// struct (fail-open, never a fabricated type). Fed the SAME 2-field payload the
/// positive test uses, so the ONLY variable is the keyspace qualifier.
#[test]
fn qualified_reference_to_unknown_udt_does_not_resolve() {
    let parser = parser_with_registry();
    let data = encode_udt(&["main st", "nyc"]);

    // `other_ks.addr` splits to keyspace `other_ks`, which holds no `addr`; the
    // non-regressive fallback (bare `other_ks.addr` in the default keyspace) also
    // misses, so this must NOT decode to the registered `addr` struct.
    let result = parser.parse_raw_type_value(&data, 0, "other_ks.addr", "col", 0);
    assert!(
        !matches!(result, Ok((Value::Udt(_), _))),
        "unknown-keyspace qualified reference must not resolve to a UDT, got {result:?}"
    );
}

/// Issue #2807 (addendum item 4): drive the SECOND silent-degradation site — the
/// nested-field registry fallback in `parse_nested_udt_from_registry` (udt.rs) —
/// through the decode surface. A three-level qualified nest
/// (`outer{ m: mid }`, `mid{ leaf }`, `leaf{ v: text }`) forces a qualified
/// `get_udt_qualified` lookup INSIDE the nested decoder, asserting the deepest
/// field materializes as structured text, never a `Blob`.
#[test]
fn nested_qualified_udt_field_decodes_via_nested_registry_fallback() {
    // A nested-UDT field typed as `Custom("udt:<qualified>")` — the exact shape
    // `CqlType::parse` yields for a qualified UDT reference.
    fn qualified_udt_field(qualified: &str) -> CqlType {
        CqlType::Custom(format!("udt:{qualified}"))
    }

    let mut reg = UdtRegistry::new();
    reg.register_udt(
        UdtTypeDef::new(KEYSPACE.to_string(), "leaf".to_string()).with_field(
            "v".to_string(),
            CqlType::Text,
            true,
        ),
    );
    reg.register_udt(
        UdtTypeDef::new(KEYSPACE.to_string(), "mid".to_string()).with_field(
            "leaf".to_string(),
            qualified_udt_field("cassandra_easy_stress.leaf"),
            true,
        ),
    );
    reg.register_udt(
        UdtTypeDef::new(KEYSPACE.to_string(), "outer".to_string()).with_field(
            "m".to_string(),
            qualified_udt_field("cassandra_easy_stress.mid"),
            true,
        ),
    );
    let parser = V5CompressedLegacyParser::new(KEYSPACE.to_string(), "t".to_string(), 0, 0, None)
        .with_udt_registry(reg);

    // Frame each level: one 4-byte-BE-prefixed field per UDT.
    let leaf_bytes = encode_udt(&["deep"]); // leaf{ v = "deep" }
    let mut mid_bytes = Vec::new();
    mid_bytes.extend_from_slice(&(leaf_bytes.len() as i32).to_be_bytes());
    mid_bytes.extend_from_slice(&leaf_bytes);
    let mut outer_bytes = Vec::new();
    outer_bytes.extend_from_slice(&(mid_bytes.len() as i32).to_be_bytes());
    outer_bytes.extend_from_slice(&mid_bytes);

    let (value, _off) = parser
        .parse_raw_type_value(&outer_bytes, 0, "cassandra_easy_stress.outer", "col", 0)
        .expect("qualified 3-level nested UDT must decode");

    // outer.m → mid.leaf → leaf.v == "deep", all resolved via qualified lookups.
    let outer = match value {
        Value::Udt(u) => u,
        other => panic!("outer must be Udt, got {other:?}"),
    };
    let mid = match outer.fields[0].value.as_ref() {
        Some(Value::Udt(u)) => u,
        other => panic!("outer.m must decode to a nested Udt, got {other:?} (Blob = the bug)"),
    };
    let leaf = match mid.fields[0].value.as_ref() {
        Some(Value::Udt(u)) => u,
        other => panic!("mid.leaf must decode to a nested Udt, got {other:?} (Blob = the bug)"),
    };
    assert_eq!(leaf.type_name, "leaf");
    assert_eq!(leaf.fields[0].value, Some(Value::Text("deep".into())));
}
