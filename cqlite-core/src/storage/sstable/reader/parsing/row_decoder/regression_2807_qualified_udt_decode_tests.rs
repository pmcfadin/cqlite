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

/// A frozen-element body in the shape the trailing `_ =>` arm expects: a single-byte
/// unsigned VInt length (7 <= 0x7F) followed by that many bytes, then a SENTINEL byte
/// the decode must NOT consume — so the returned offset proves the arm framed the value
/// as `[VInt len][bytes]` rather than swallowing the rest of the slice.
fn vint_framed_body() -> Vec<u8> {
    let mut out = vec![0x07];
    out.extend_from_slice(b"main st");
    out.push(0xEE); // sentinel
    out
}

/// **CHARACTERIZATION TEST — measured CURRENT behaviour, NOT endorsed behaviour.**
/// Subject: **issue #4141**. Descoped from #4070 AC3 by lead ruling on
/// `REQUEST-4070-02` (option 4).
///
/// Pins what [`V5CompressedLegacyParser::parse_raw_type_value`]'s trailing `_ =>` arm
/// does **today** when a frozen element's declared type cannot be decoded: it parses a
/// `[VInt len]`-framed body and hands the raw bytes back as `Ok((Value::Blob(..), _))`,
/// silently discarding the declared type.
///
/// # This test does NOT endorse the blob fallback — it RECORDS it
/// Returning declared-but-undecodable bytes as an opaque blob is precisely what **#28**
/// forbids (authoritative metadata only; never a silent degrade), and precisely what
/// **#3631** and **#3722** removed from the `CqlType`-driven decoder next door —
/// `typed_value.rs`'s UDT path refuses this state with `Error::unsupported_format`.
/// These two sites are the surviving instance. A reader who finds a test asserting a
/// blob fallback must not read it as a specification: it is a tripwire.
///
/// # WHEN #4141 LANDS, THIS TEST SHOULD GO RED — that is its entire purpose
/// #4141 converges these sites with **site C** (`raw_value/reporting.rs`'s trailing
/// `other =>` arm) into ONE refusal. At that point every assertion below becomes false.
/// Whoever lands #4141 must therefore **observe** the behaviour change here and delete
/// this test as part of that change, rather than discover the change in production.
/// Do NOT repair a failure here by re-adding a blob degrade.
///
/// # Why #4070 did not fix it
/// #4070 AC3 made both sites refuse, and the change was correct and measured (zero
/// corpus hits over 147 `*-Data.db`, on an arm proved capable of firing — its sibling
/// containing arms fired 303x). It was descoped because the arm refuses **any**
/// unrecognised type string — case (c) below is not a UDT at all — while other routes
/// still blob the same type, and this site cannot tell the populations apart:
/// `is_udt_type` is constant `false` here (its own arm above already consumed every
/// string it accepts), so shape-based discrimination is unavailable, and a
/// marshal-prefix test was refused as a #28 violation. Fixing it properly means
/// changing `reporting.rs`'s trailing arm, which the TOP-LEVEL COLUMN path also uses —
/// deliberately out of scope for #4070.
#[test]
fn characterization_unresolvable_frozen_element_still_degrades_to_blob_pending_4141() {
    let data = vint_framed_body();
    let framed_len = 1 + 7; // VInt length byte + 7 body bytes; the sentinel stays

    // (a) A registry IS present and the name is ABSENT from it: `other_ks.addr` splits
    // to keyspace `other_ks`, which holds no `addr`, and the bare-name fallback misses
    // too. Sibling `qualified_reference_to_unknown_udt_does_not_resolve` asserts only
    // `!Ok(Udt)` here — which this blob satisfies just as well as a refusal would, which
    // is why that test cannot see this behaviour and this one exists.
    let (value, offset) = parser_with_registry()
        .parse_raw_type_value(&data, 0, "other_ks.addr", "col", 0)
        .expect("MEASURED TODAY: an unresolvable qualified UDT name does not refuse");
    assert!(
        matches!(value, Value::Blob(ref b) if b.as_ref() == b"main st"),
        "(a) registry present / name absent: measured behaviour is an opaque blob of \
         the declared bytes (the #28 degrade #4141 will remove), got {value:?}"
    );
    assert_eq!(
        offset, framed_len,
        "(a) the arm frames the value as [VInt len][bytes] and leaves the sentinel — \
         recording the FRAMING, so #4141 cannot change it unobserved"
    );

    // (b) NO registry at all. This population has never had a test of ANY kind: the
    // branch produced its own byte-identical blob, so nothing observed which of the two
    // branches ran or what either returned. A characterization test is still its first
    // coverage.
    let no_registry =
        V5CompressedLegacyParser::new(KEYSPACE.to_string(), "t".to_string(), 0, 0, None);
    let (value, offset) = no_registry
        .parse_raw_type_value(&data, 0, "addr", "col", 0)
        .expect("MEASURED TODAY: with no registry at all, the decode does not refuse");
    assert!(
        matches!(value, Value::Blob(ref b) if b.as_ref() == b"main st"),
        "(b) no registry: same opaque blob, so an operator who supplied no schema gets \
         the same undiagnosed bytes as one whose schema is incomplete, got {value:?}"
    );
    assert_eq!(offset, framed_len, "(b) same [VInt len][bytes] framing");

    // (c) An unrecognised NON-UDT marshal string. This is the population roborev was
    // actually worried about on #4070 and the one nothing has ever pinned: `EmptyType`
    // is a real Cassandra marshal type, matches no arm above, is not accepted by
    // `is_udt_type`, and is not a UDT name — yet it lands in the SAME arm and gets the
    // same blob. Whatever #4141 does here, it must do deliberately.
    let (value, offset) = parser_with_registry()
        .parse_raw_type_value(&data, 0, "EmptyType", "col", 0)
        .expect("MEASURED TODAY: an unrecognised non-UDT marshal string does not refuse");
    assert!(
        matches!(value, Value::Blob(ref b) if b.as_ref() == b"main st"),
        "(c) unrecognised non-UDT marshal string: indistinguishable from (a)/(b) at \
         this site — one arm, one outcome, no type awareness, got {value:?}"
    );
    assert_eq!(offset, framed_len, "(c) same [VInt len][bytes] framing");

    // CONTROL: the arm is not simply blobbing everything — a name the registry DOES
    // hold still decodes to a struct, so the three cases above are evidence about
    // unresolvable types specifically.
    let (control, _off) = parser_with_registry()
        .parse_raw_type_value(&encode_udt(&["main st", "nyc"]), 0, "addr", "col", 0)
        .expect("CONTROL: a resolvable UDT name must still decode");
    assert!(
        matches!(control, Value::Udt(ref u) if u.type_name == "addr" && u.fields.len() == 2),
        "CONTROL: `addr` decodes to the struct when the registry holds it, got {control:?}"
    );
}
