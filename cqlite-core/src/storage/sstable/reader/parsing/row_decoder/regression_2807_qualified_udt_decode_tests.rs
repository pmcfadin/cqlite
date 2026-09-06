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
use crate::error::Error;
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
///
/// # TIGHTENED to the POSITIVE outcome (issue #4070, AC3 — SITE A)
/// This case used to assert only `!matches!(result, Ok((Value::Udt(_), _)))`, which
/// the pre-#4070 `Value::Blob` degrade satisfied just as well as an `Err` — so it
/// could not distinguish "refused" from "silently handed back opaque bytes", and
/// #4070's change to this very site would NOT have turned it red. Its intent above
/// is unchanged and STRENGTHENED: never a fabricated type, and now never a silent
/// blob either. AC3 says this outcome must be NAMED, so the assertion names it.
#[test]
fn qualified_reference_to_unknown_udt_is_refused_not_degraded_to_blob() {
    let parser = parser_with_registry();
    let data = encode_udt(&["main st", "nyc"]);

    // `other_ks.addr` splits to keyspace `other_ks`, which holds no `addr`; the
    // non-regressive fallback (bare `other_ks.addr` in the default keyspace) also
    // misses. SITE A: a registry IS present and the name is absent from it.
    let err = match parser.parse_raw_type_value(&data, 0, "other_ks.addr", "col", 0) {
        Ok((value, _off)) => panic!(
            "an unresolvable qualified UDT reference must be REFUSED, not decoded — a \
             `Value::Blob` here is the #3631/#28 silent degradation #4070 removed, and a \
             `Value::Udt` would be a fabricated type. Got {value:?}"
        ),
        Err(e) => e,
    };
    assert!(
        matches!(err, Error::UnsupportedFormat(_)),
        "the refusal must be the `unsupported_format` class the CqlType-driven decoder \
         already uses for this state (typed_value.rs::parse_typed_udt) — a caller \
         matching on the message must not have to know which layer refused. Got {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("'other_ks.addr'") && msg.contains("field list is not available"),
        "the refusal must NAME the type that failed to resolve — that naming is the \
         whole diagnostic value of the site: {msg}"
    );
    assert!(
        msg.contains("absent from the UDT registry"),
        "a registry WAS supplied, so the message must say the schema lacks the type \
         rather than that no schema was supplied — the operator-facing half of the \
         distinction the two collapsed sites used to carry in `tracing::debug!`: {msg}"
    );
}

/// **SITE B** (issue #4070, AC3): the SAME unresolvable state with **NO UDT
/// registry at all** must also be refused, and must say so differently.
///
/// # This half was completely unpinned before #4070
/// Site B had no test of any kind: the no-registry branch produced its own
/// byte-identical `[VInt len]`-framed `Value::Blob`, so nothing observed which of
/// the two branches ran or what either returned. The two branches are now ONE
/// refusal path, and this case is what holds the surviving distinction — an
/// operator must be able to tell "your schema is missing this type" (site A above)
/// from "you supplied no schema at all" (here).
///
/// # Why this is a DECODER-level test and not an end-to-end one
/// The precondition is not constructible through the CLI: the schema loader fails
/// CLOSED on a column referencing an undefined UDT (`Column 'w' references
/// undefined UDT 'wide'`), and validation descends into collection element types
/// too, so there is no user-reachable route to a table whose declared UDT cannot be
/// resolved. Four constructions were tried during #4070's measurement and every one
/// was rejected at load. Building a fixture for a state the loader refuses to let a
/// user create would be inventing the state, not covering it — so this drives
/// `parse_raw_type_value` directly, the idiom this whole module already uses.
/// #4070's public-surface wiring evidence lives with AC1, on the route that IS
/// reachable (`cqlite-core/tests/issue_3722_udt_field_type_fidelity.rs`).
#[test]
fn unresolvable_udt_with_no_registry_is_refused_and_says_no_schema_was_supplied() {
    // Deliberately NO `.with_udt_registry(..)`: `self.udt_registry` is `None`.
    let parser = V5CompressedLegacyParser::new(KEYSPACE.to_string(), "t".to_string(), 0, 0, None);
    let data = encode_udt(&["main st", "nyc"]);

    let err = match parser.parse_raw_type_value(&data, 0, "addr", "col", 0) {
        Ok((value, _off)) => panic!(
            "with no UDT registry there is no field list to decode `addr` against, so \
             the decode must be REFUSED; a `Value::Blob` is the silent degradation \
             #4070 removed. Got {value:?}"
        ),
        Err(e) => e,
    };
    assert!(
        matches!(err, Error::UnsupportedFormat(_)),
        "same error class as site A and as the CqlType-driven decoder: {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("'addr'") && msg.contains("field list is not available"),
        "the refusal must name the unresolved type: {msg}"
    );
    assert!(
        msg.contains("no UDT registry is available at all"),
        "site B's cause must be DISTINGUISHABLE from site A's `absent from the UDT \
         registry`: no schema was supplied, which is a different thing for an operator \
         to fix: {msg}"
    );
    // The same name WITH a registry that holds it still decodes — so this case is
    // evidence about the missing registry specifically, not about `addr` being
    // undecodable or about a decoder that refuses everything.
    let (value, _off) = parser_with_registry()
        .parse_raw_type_value(&data, 0, "addr", "col", 0)
        .expect("CONTROL: with the registry present, `addr` must still decode");
    assert!(
        matches!(value, Value::Udt(ref u) if u.type_name == "addr" && u.fields.len() == 2),
        "CONTROL: `addr` decodes to the struct when the registry holds it, got {value:?}"
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
