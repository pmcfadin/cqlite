//! Regression tests for issue #3612 — the DECODE surface for a MULTICELL map's
//! cell-path KEY.
//!
//! ## The defect
//! A non-frozen `map<K, V>` is multicell: every entry is its own cell and the KEY
//! lives in that cell's CELL PATH. `parse_cell_path_key` used to decode it from a
//! hand-maintained allowlist of six scalar families and return `Value::Blob` for
//! everything else, so a COMPOSITE key and ~10 further scalar families silently
//! surfaced as raw bytes — the same key the FROZEN spelling of that map, and
//! `sstabledump`, render structurally. The fix delegates to the shared structural
//! decoder. Full narrative, and the parity rules, in `cell_path_key.rs`'s header;
//! it is not restated here, because two copies drift.
//!
//! ## What these tests pin (each can fail on its own)
//! * **Composite keys decode structurally** (UDT, tuple), including a NULL field
//!   inside the composite — the case the committed fixture does not carry (see
//!   `cqlite-core/tests/issue_3612_multicell_map_composite_key.rs`).
//! * **The previously-broken scalar families decode** (boolean, decimal, float, …).
//! * **CASE IS PRESERVED on delegation.** `primitive_marshal_to_cql_short` is
//!   CASE-SENSITIVE (`s.ends_with("Int32Type")`), so handing it a lowercased string
//!   would fail every marshal-form normalization and land back in an opaque `Blob`
//!   — reintroducing the bug for the no-schema `Statistics.db` path. Several cases
//!   use ORIGINAL-CASE marshal spellings and would red if that regressed.
//! * **Width validation follows Cassandra's THREE-WAY serializer split**, not a
//!   uniform `!= N`: `N`-or-`0` (Int32/Long/Float/Double/UUID/Timestamp/Counter),
//!   strict `!= N` (Short/Byte/SimpleDate/Time), `> 1` (Boolean), and Inet at 0/4/16.
//!   Per-serializer citations are in `cell_path_key.rs`; both directions pinned below.
//! * **A `blob` key is a blob only by EXACT name**, so a foreign
//!   `…CustomBytesType` cannot silence the opaque-key diagnostic.
//! * **CROSS-SPELLING PARITY, for the shapes no fixture supplies.** A multicell key
//!   and the frozen spelling of the same map must present the IDENTICAL `Value`.
//!   The fixture-backed subjects live in
//!   `cqlite-core/tests/issue_3612_multicell_map_composite_key.rs`; the set-, list-
//!   and map-keyed cases are pinned HERE, from the two marshal spellings, because
//!   the corpus has no such column. (An earlier revision of this header said parity
//!   was not pinned here at all — true until round 8 added those.)
//!
//! These carry NO dataset/feature-flag dependency: `parse_cell_path_key` is a
//! `pub(super)` method on a plainly-constructed parser, so they run in every
//! build and lane and can never pass vacuously on an empty corpus.

use super::super::V5CompressedLegacyParser;
use crate::schema::{CqlType, UdtRegistry};
use crate::types::UdtTypeDef;
use crate::Value;

const KEYSPACE: &str = "test_udt_collision";
const MARSHAL: &str = "org.apache.cassandra.db.marshal";

/// The `collide` UDT of the committed #3504 fixture:
/// (`_type` text, `_keyspace` text, `__proto__` text, `real_field` int).
fn registry_with_collide() -> UdtRegistry {
    let mut reg = UdtRegistry::new();
    reg.register_udt(
        UdtTypeDef::new(KEYSPACE.to_string(), "collide".to_string())
            .with_field("_type".to_string(), CqlType::Text, true)
            .with_field("_keyspace".to_string(), CqlType::Text, true)
            .with_field("__proto__".to_string(), CqlType::Text, true)
            .with_field("real_field".to_string(), CqlType::Int, true),
    );
    reg
}

fn parser() -> V5CompressedLegacyParser {
    V5CompressedLegacyParser::new(KEYSPACE.to_string(), "udt_collide".to_string(), 0, 0, None)
        .with_udt_registry(registry_with_collide())
}

/// Encode a tuple/UDT body the way Cassandra's `TupleType.buildValue` does:
/// each component is `[i32 BE len][bytes]`, with `-1` marking a NULL component.
/// This is the framing `read_frozen_element` already feeds the same decoder, and
/// it is what remains of a CellPath once the outer `[VInt length]`
/// (`CollectionType.CollectionPathSerializer`) has been stripped by the caller.
fn encode_components(components: &[Option<&[u8]>]) -> Vec<u8> {
    let mut out = Vec::new();
    for c in components {
        match c {
            Some(bytes) => {
                out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                out.extend_from_slice(bytes);
            }
            None => out.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }
    out
}

/// The MARSHAL spelling of the `collide` UDT, exactly as an on-disk
/// `SerializationHeader` carries it. Built once: four tests need it, and a copy
/// per test is how one of them ends up spelling a field type differently.
fn udt_marshal_type() -> String {
    format!(
        "{MARSHAL}.UserType({KEYSPACE},{},{}:{MARSHAL}.UTF8Type,{}:{MARSHAL}.UTF8Type,\
         {}:{MARSHAL}.UTF8Type,{}:{MARSHAL}.Int32Type)",
        hex::encode("collide"),
        hex::encode("_type"),
        hex::encode("_keyspace"),
        hex::encode("__proto__"),
        hex::encode("real_field"),
    )
}

/// The fixture's map key: `{_type: "key-type-marker", _keyspace:
/// "key-keyspace-marker", __proto__: "key-proto-marker", real_field: 100}`.
fn collide_key_bytes() -> Vec<u8> {
    encode_components(&[
        Some(b"key-type-marker"),
        Some(b"key-keyspace-marker"),
        Some(b"key-proto-marker"),
        Some(&100i32.to_be_bytes()),
    ])
}

/// Peel any `Value::Frozen` wrappers (transparent at every render surface).
fn peel(v: &Value) -> &Value {
    match v {
        Value::Frozen(inner) => peel(inner),
        other => other,
    }
}

fn udt_of(v: &Value) -> &crate::types::UdtValue {
    match peel(v) {
        Value::Udt(u) => u,
        other => panic!("expected a structured Value::Udt cell-path key, got {other:?}"),
    }
}

fn field(u: &crate::types::UdtValue, name: &str) -> Option<Value> {
    u.fields
        .iter()
        .find(|f| f.name == name)
        .unwrap_or_else(|| panic!("UDT '{}' has no field '{name}'", u.type_name))
        .value
        .clone()
}

fn text(v: Option<Value>) -> String {
    match v {
        Some(Value::Text(s)) => String::from_utf8_lossy(&s).into_owned(),
        other => panic!("expected text, got {other:?}"),
    }
}

/// The four fixture field values, asserted in one place so a case cannot end up
/// silently weaker than its siblings.
fn assert_collide_fields(v: &Value) {
    let u = udt_of(v);
    assert_eq!(u.type_name, "collide");
    assert_eq!(u.keyspace, KEYSPACE);
    assert_eq!(u.fields.len(), 4, "all four declared fields materialize");
    assert_eq!(text(field(u, "_type")), "key-type-marker");
    assert_eq!(text(field(u, "_keyspace")), "key-keyspace-marker");
    assert_eq!(text(field(u, "__proto__")), "key-proto-marker");
    assert_eq!(
        field(u, "real_field"),
        Some(Value::Integer(100)),
        "an int field inside a composite cell-path key decodes as an int"
    );
}

// ---------------------------------------------------------------------------
// (a) COMPOSITE cell-path keys — the #3612 headline
// ---------------------------------------------------------------------------

/// A `frozen<udt>` map key — the schema spelling of the fixture's `cm` column.
#[test]
fn frozen_udt_cell_path_key_decodes_structurally_not_as_blob() {
    let value = parser()
        .parse_cell_path_key(&collide_key_bytes(), "frozen<collide>", "cm")
        .expect("a frozen<udt> cell-path key must decode");
    assert!(
        !matches!(peel(&value), Value::Blob(_)),
        "issue #3612: a composite cell-path key must not surface as raw bytes"
    );
    assert_collide_fields(&value);
}

/// The bare UDT name, as a registry-resolved key type.
#[test]
fn bare_udt_name_cell_path_key_decodes_structurally() {
    let value = parser()
        .parse_cell_path_key(&collide_key_bytes(), "collide", "cm")
        .expect("a bare UDT-named cell-path key must decode");
    assert_collide_fields(&value);
}

/// A NULL field INSIDE a composite key round-trips as a null field, not as a
/// zero-length blob and not by shifting the following components. The committed
/// Cassandra fixture's map keys carry no null component, so this case is pinned
/// here against Cassandra's `-1`-length convention (`TupleType.buildValue`).
#[test]
fn null_field_inside_a_composite_cell_path_key_round_trips_as_null() {
    let bytes = encode_components(&[
        None,
        Some(b"key-keyspace-marker"),
        None,
        Some(&7i32.to_be_bytes()),
    ]);
    let value = parser()
        .parse_cell_path_key(&bytes, "frozen<collide>", "cm")
        .expect("a composite cell-path key with null components must decode");
    let u = udt_of(&value);
    assert_eq!(u.fields.len(), 4);
    assert_eq!(field(u, "_type"), None, "a -1-length component is NULL");
    assert_eq!(text(field(u, "_keyspace")), "key-keyspace-marker");
    assert_eq!(field(u, "__proto__"), None);
    assert_eq!(
        field(u, "real_field"),
        Some(Value::Integer(7)),
        "components after a null keep their positions"
    );
}

/// The MARSHAL form of the same UDT — the shape the no-schema `Statistics.db`
/// path supplies. `real_field` decoding as an int (not a blob) is what pins the
/// CASE-PRESERVATION requirement: `Int32Type` only normalizes through the
/// case-SENSITIVE `primitive_marshal_to_cql_short`.
#[test]
fn marshal_form_usertype_cell_path_key_keeps_its_case_and_decodes_fields() {
    let marshal = udt_marshal_type();
    let value = parser()
        .parse_cell_path_key(&collide_key_bytes(), &marshal, "cm")
        .expect("a marshal-form UserType cell-path key must decode");
    assert_collide_fields(&value);
}

/// A `tuple<…>` map key: the other composite spelling, and the one whose
/// component framing is identical to a UDT's.
#[test]
fn tuple_cell_path_key_decodes_structurally() {
    let bytes = encode_components(&[Some(b"abc"), Some(&42i32.to_be_bytes())]);
    let value = parser()
        .parse_cell_path_key(&bytes, "tuple<text, int>", "tk")
        .expect("a tuple cell-path key must decode");
    match peel(&value) {
        Value::Tuple(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Value::text("abc"));
            assert_eq!(items[1], Value::Integer(42));
        }
        other => panic!("expected Value::Tuple, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// (b) SCALAR families the old allowlist omitted
// ---------------------------------------------------------------------------

#[test]
fn boolean_cell_path_key_decodes_as_boolean() {
    let p = parser();
    assert_eq!(
        p.parse_cell_path_key(&[0x01], "boolean", "bk").unwrap(),
        Value::Boolean(true)
    );
    assert_eq!(
        p.parse_cell_path_key(&[0x00], "boolean", "bk").unwrap(),
        Value::Boolean(false)
    );
}

#[test]
fn decimal_cell_path_key_decodes_as_decimal() {
    // scale=2, unscaled=12345 (varint) → 123.45
    let mut bytes = 2i32.to_be_bytes().to_vec();
    bytes.extend_from_slice(&[0x30, 0x39]);
    match parser()
        .parse_cell_path_key(&bytes, "decimal", "dk")
        .unwrap()
    {
        Value::Decimal { scale, unscaled } => {
            assert_eq!(scale, 2);
            assert_eq!(unscaled, vec![0x30, 0x39]);
        }
        other => panic!("expected Value::Decimal, got {other:?}"),
    }
}

#[test]
fn smallint_and_tinyint_cell_path_keys_decode() {
    let p = parser();
    assert_eq!(
        p.parse_cell_path_key(&(-300i16).to_be_bytes(), "smallint", "sk")
            .unwrap(),
        Value::SmallInt(-300)
    );
    assert_eq!(
        p.parse_cell_path_key(&[0xFF], "tinyint", "tk").unwrap(),
        Value::TinyInt(-1)
    );
}

/// ORIGINAL-CASE marshal spellings for the newly-reachable scalars. If the
/// delegated type string were lowercased these would each fall back to a blob.
#[test]
fn marshal_form_scalar_cell_path_keys_keep_their_case() {
    let p = parser();
    for (marshal, bytes, expected) in [
        (
            format!("{MARSHAL}.BooleanType"),
            vec![0x01u8],
            Value::Boolean(true),
        ),
        (
            format!("{MARSHAL}.ShortType"),
            (-300i16).to_be_bytes().to_vec(),
            Value::SmallInt(-300),
        ),
        (
            format!("{MARSHAL}.ByteType"),
            vec![0xFFu8],
            Value::TinyInt(-1),
        ),
        (
            format!("{MARSHAL}.DoubleType"),
            1.5f64.to_be_bytes().to_vec(),
            Value::Float(1.5),
        ),
        (
            format!("{MARSHAL}.TimeType"),
            42i64.to_be_bytes().to_vec(),
            Value::Time(42),
        ),
        (
            format!("{MARSHAL}.Int32Type"),
            77i32.to_be_bytes().to_vec(),
            Value::Integer(77),
        ),
    ] {
        let got = p
            .parse_cell_path_key(&bytes, &marshal, "k")
            .unwrap_or_else(|e| panic!("{marshal} must decode: {e}"));
        assert_eq!(
            got, expected,
            "{marshal}: a marshal-form cell-path key must normalize CASE-SENSITIVELY \
             (a lowercased delegation would land back in an opaque Blob)"
        );
    }
}

// ---------------------------------------------------------------------------
// The six previously-handled families still decode identically
// ---------------------------------------------------------------------------

#[test]
fn previously_handled_scalar_cell_path_keys_are_unchanged() {
    let p = parser();
    assert_eq!(
        p.parse_cell_path_key(b"hello", "text", "k").unwrap(),
        Value::text("hello")
    );
    assert_eq!(
        p.parse_cell_path_key(&[7u8; 16], "uuid", "k").unwrap(),
        Value::Uuid([7u8; 16])
    );
    assert_eq!(
        p.parse_cell_path_key(&5i32.to_be_bytes(), "int", "k")
            .unwrap(),
        Value::Integer(5)
    );
    assert_eq!(
        p.parse_cell_path_key(&9i64.to_be_bytes(), "bigint", "k")
            .unwrap(),
        Value::BigInt(9)
    );
    assert_eq!(
        p.parse_cell_path_key(&1_000i64.to_be_bytes(), "timestamp", "k")
            .unwrap(),
        Value::Timestamp(1_000)
    );
    // DATE: 4-byte UNSIGNED days with an Integer.MIN_VALUE offset, so the
    // stored form of day 0 is 0x80000000 (issue #3612 must not change this).
    assert_eq!(
        p.parse_cell_path_key(&0x8000_0000u32.to_be_bytes(), "date", "k")
            .unwrap(),
        Value::Date(0)
    );
}

/// A text key must still reject invalid UTF-8 rather than degrade to bytes.
#[test]
fn invalid_utf8_text_cell_path_key_is_an_error() {
    assert!(parser()
        .parse_cell_path_key(&[0xFF, 0xFE], "text", "k")
        .is_err());
}

// ---------------------------------------------------------------------------
// Exact-width validation (see the module header for the Cassandra authority)
// ---------------------------------------------------------------------------

#[test]
fn fixed_width_cell_path_keys_reject_a_wrong_width() {
    let p = parser();
    // Over-width: the whole slice IS the key, so trailing bytes are corruption.
    for (type_str, bytes) in [
        ("int", vec![0u8; 5]),
        ("bigint", vec![0u8; 9]),
        ("uuid", vec![0u8; 17]),
        ("timeuuid", vec![0u8; 15]),
        ("date", vec![0u8; 5]),
        ("timestamp", vec![0u8; 7]),
        ("boolean", vec![0u8; 2]),
        ("float", vec![0u8; 5]),
        ("double", vec![0u8; 9]),
        ("smallint", vec![0u8; 3]),
        ("tinyint", vec![0u8; 2]),
        ("time", vec![0u8; 9]),
    ] {
        assert!(
            p.parse_cell_path_key(&bytes, type_str, "k").is_err(),
            "{type_str}: {} bytes must be rejected, not silently decoded from a prefix",
            bytes.len()
        );
    }
    // Marshal spellings go through the same check.
    assert!(p
        .parse_cell_path_key(&[0u8; 5], &format!("{MARSHAL}.Int32Type"), "k")
        .is_err());
    // FROZEN-SPELLED fixed-width keys must be width-checked too (finding B1).
    // The classifier used to run on the RAW string, so `frozen<int>` took the
    // "contains '<' => variable width" branch, the dispatcher's frozen arm then
    // recursed to `"int"` and returned `None` consumption, and a 5-byte key
    // decoded `Integer` from `data[0..4]` silently. The composite drift guard
    // could not see this: it enumerates COMPOSITES only.
    for (type_str, bytes) in [
        ("frozen<int>", vec![0u8; 5]),
        ("frozen<int>", vec![0u8; 3]),
        ("Frozen<BIGINT>", vec![0u8; 9]),
        ("frozen<inet>", vec![0u8; 5]),
        ("frozen<uuid>", vec![0u8; 17]),
        ("frozen<smallint>", vec![0u8; 3]),
    ] {
        assert!(
            p.parse_cell_path_key(&bytes, type_str, "k").is_err(),
            "{type_str}: {} bytes must be refused — a frozen-spelled fixed-width \
             key must not bypass the width table (B1)",
            bytes.len()
        );
    }
    for marshal_inner in ["Int32Type", "LongType", "InetAddressType", "UUIDType"] {
        let t = format!("{MARSHAL}.FrozenType({MARSHAL}.{marshal_inner})");
        assert!(
            p.parse_cell_path_key(&[0u8; 5], &t, "k").is_err(),
            "{t}: 5 bytes must be refused (B1, marshal spelling)"
        );
    }
    // ...and the CORRECT widths still decode through the frozen spelling, so B1's
    // fix is a narrowing and not a ban.
    assert!(p
        .parse_cell_path_key(&7i32.to_be_bytes(), "frozen<int>", "k")
        .is_ok());
    assert!(p
        .parse_cell_path_key(&[127, 0, 0, 1], "frozen<inet>", "k")
        .is_ok());
    assert!(p
        .parse_cell_path_key(
            &7i32.to_be_bytes(),
            &format!("{MARSHAL}.FrozenType({MARSHAL}.Int32Type)"),
            "k"
        )
        .is_ok());
    // Variable-width families are unaffected.
    assert!(p.parse_cell_path_key(&[0u8; 5], "text", "k").is_ok());
    assert!(p.parse_cell_path_key(&[0u8; 5], "blob", "k").is_ok());
}

// ---------------------------------------------------------------------------
// A `blob` key is a blob BY DECLARATION, not by fallback (issue #3612 option B)
// ---------------------------------------------------------------------------

/// The one case where `Value::Blob` is the RIGHT answer. The opaque-value
/// DIAGNOSTIC below must distinguish "declared blob" from "undecoded", so this is
/// its control: a check keyed on the RESULT alone would fire on these. Note the
/// failure mode it guards is a SPURIOUS WARNING, not a rejection — the
/// undecodable case returns opaque bytes plus a `warn!` and never an `Err` (see
/// the module header's error-budget rule); an earlier revision of this comment
/// said "reject", which described a fail-closed check that no longer exists.
#[test]
fn a_declared_blob_cell_path_key_is_a_blob() {
    let p = parser();
    assert_eq!(
        p.parse_cell_path_key(&[1, 2, 3], "blob", "k").unwrap(),
        Value::blob(vec![1, 2, 3])
    );
    assert_eq!(
        p.parse_cell_path_key(&[1, 2, 3], "BLOB", "k").unwrap(),
        Value::blob(vec![1, 2, 3]),
        "the declared-blob test is case-insensitive, like the decode match itself"
    );
    assert_eq!(
        p.parse_cell_path_key(&[1, 2, 3], "bytes", "k").unwrap(),
        Value::blob(vec![1, 2, 3])
    );
    assert_eq!(
        p.parse_cell_path_key(&[1, 2, 3], &format!("{MARSHAL}.BytesType"), "k")
            .unwrap(),
        Value::blob(vec![1, 2, 3])
    );
    // A FROZEN-SPELLED blob keeps its wrapper, because this reader mirrors
    // `parse_value_from_raw_bytes` exactly — that is what makes it agree with the
    // frozen map reader for every spelling (roborev round 8). Before that the
    // multicell side peeled and re-applied a wrapper of its own, and these three
    // cases asserted the BARE value; the wrapper is the frozen reader's answer for
    // this string, so it is now the right expectation.
    //
    // CQL does not permit `frozen<blob>` as a map key (freezing applies to
    // composites), but if such a spelling ever reaches here it is still a DECLARED
    // blob and must not be misdiagnosed as an undecoded key — the diagnostic reads
    // a PEELED view, so the wrapper does not hide the `Blob` from it.
    assert_eq!(
        p.parse_cell_path_key(&[1, 2, 3], "frozen<blob>", "k")
            .unwrap(),
        Value::Frozen(Box::new(Value::blob(vec![1, 2, 3])))
    );
    // Case-INSENSITIVELY, because `parse_value_from_raw_bytes` routes off a
    // LOWERCASED guard: were the declared-blob test case-sensitive where the
    // decode is not, a `Frozen<BLOB>` would decode to a blob and then be
    // rejected as undecoded.
    assert_eq!(
        p.parse_cell_path_key(&[1, 2, 3], "Frozen<BLOB>", "k")
            .unwrap(),
        Value::Frozen(Box::new(Value::blob(vec![1, 2, 3])))
    );
    // A BARE, unqualified marshal name (a hand-written schema, or a marshal
    // string whose package prefix was stripped upstream): the declared-blob test
    // must recognise it, or the diagnostic below would fire on a CORRECT decode.
    assert_eq!(
        p.parse_cell_path_key(&[1, 2, 3], "BytesType", "k").unwrap(),
        Value::blob(vec![1, 2, 3])
    );
    // CQL's CUSTOM-type spelling is the marshal class name in SINGLE QUOTES; the
    // trailing quote defeats a naive `ends_with` suffix match.
    assert_eq!(
        p.parse_cell_path_key(
            &[1, 2, 3],
            "'org.apache.cassandra.db.marshal.BytesType'",
            "k"
        )
        .unwrap(),
        Value::blob(vec![1, 2, 3])
    );
    // Frozen-spelled marshal: same wrapper, same reason as the two above.
    assert_eq!(
        p.parse_cell_path_key(
            &[1, 2, 3],
            &format!("{MARSHAL}.FrozenType({MARSHAL}.BytesType)"),
            "k"
        )
        .unwrap(),
        Value::Frozen(Box::new(Value::blob(vec![1, 2, 3])))
    );
}

// ---------------------------------------------------------------------------
// A type this reader CANNOT MODEL: opaque bytes + a warning, NEVER an `Err`
// ---------------------------------------------------------------------------
//
// MEASURED in review round 1 (see the module header): an `Err` from this site is
// SWALLOWED by row assembly's complex-column `Err(e) => { debug!(); break; }`
// arm, which drops the column AND every later on-disk column from the row. A
// silently truncated row is more destructive than one opaque value, and
// Cassandra reads such a key without complaint, so the rule is: `Err` ONLY where
// Cassandra's own `validate`/`split` throws. A type CQLite merely cannot model
// is not that case.

/// A bare UDT name ABSENT from the registry reaches the shared decoder's opaque
/// default. It must surface as the opaque `Value::Blob` — NOT an `Err`, which
/// would truncate the row — leaving the rest of the row intact.
#[test]
fn unregistered_udt_name_cell_path_key_stays_opaque_without_erroring() {
    let got = parser()
        .parse_cell_path_key(&collide_key_bytes(), "absent_udt", "cm")
        .expect(
            "an unmodellable key type must NOT return Err: row assembly swallows it \
             into a silently truncated row (issue #3612 review round 1)",
        );
    assert_eq!(
        got,
        Value::blob(collide_key_bytes()),
        "the key surfaces as the raw cell-path bytes, unchanged"
    );
}

/// A parameterised marshal type CQLite models nowhere (here a custom
/// comparator) takes the same route: opaque, not an error.
#[test]
fn unmodelled_custom_marshal_cell_path_key_stays_opaque_without_erroring() {
    let declared = format!("{MARSHAL}.DynamicCompositeType(s=>{MARSHAL}.UTF8Type)");
    let got = parser()
        .parse_cell_path_key(&[1, 2, 3], &declared, "ck")
        .expect("an unmodellable marshal key type must NOT return Err");
    assert_eq!(got, Value::blob(vec![1, 2, 3]));
}

// ---------------------------------------------------------------------------
// COMPOSITE keys: trailing bytes are corruption (Cassandra `TupleType.split`)
// ---------------------------------------------------------------------------

/// Appending bytes to a UDT cell-path key must be REFUSED, not decoded from the
/// prefix — otherwise two distinct corrupted encodings yield the same logical
/// key. Cassandra 5.0.8 `TupleType.split` throws `"Expected N values … but got
/// more"` on exactly this input.
#[test]
fn trailing_bytes_after_a_composite_cell_path_key_are_rejected() {
    let p = parser();
    let mut corrupt = collide_key_bytes();
    corrupt.extend_from_slice(b"\xde\xad");
    let err = p
        .parse_cell_path_key(&corrupt, "frozen<collide>", "cm")
        .expect_err("trailing bytes after a composite key must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("trailing"), "got: {msg}");
    assert!(
        msg.contains("cm"),
        "the error must name the column, got: {msg}"
    );
    // The control: the SAME bytes without the appended pair still decode.
    assert_collide_fields(
        &p.parse_cell_path_key(&collide_key_bytes(), "frozen<collide>", "cm")
            .expect("the un-appended key is the control and must still decode"),
    );
}

/// Same for a tuple key, the other composite spelling.
#[test]
fn trailing_bytes_after_a_tuple_cell_path_key_are_rejected() {
    let mut corrupt = encode_components(&[Some(b"abc"), Some(&42i32.to_be_bytes())]);
    corrupt.push(0x00);
    let err = parser()
        .parse_cell_path_key(&corrupt, "tuple<text, int>", "tk")
        .expect_err("trailing bytes after a tuple key must be rejected");
    assert!(err.to_string().contains("trailing"), "got: {err}");
}

/// A SHORT composite encoding — fewer components present than declared, the
/// trailing fields absent — is LEGAL and must NOT be rejected. Cassandra's
/// `TupleType.split` returns early on `position == length`
/// (`Arrays.copyOfRange(components, 0, i)`), so refusing this would red on valid
/// Cassandra-written data.
#[test]
fn a_short_composite_cell_path_key_is_accepted() {
    let short = encode_components(&[Some(b"only-the-first-field")]);
    let value = parser()
        .parse_cell_path_key(&short, "frozen<collide>", "cm")
        .expect("a short composite encoding is legal per TupleType.split");
    let u = udt_of(&value);
    assert_eq!(text(field(u, "_type")), "only-the-first-field");
    assert_eq!(
        field(u, "real_field"),
        None,
        "components the encoding omits are absent, i.e. null"
    );
}

// ---------------------------------------------------------------------------
// `inet` is 4 OR 16 bytes — the one family the single-width table cannot express
// ---------------------------------------------------------------------------

/// Cassandra's `InetAddressSerializer.validate` delegates to
/// `InetAddress.getByAddress`, which accepts ONLY a 4-byte (IPv4) or 16-byte
/// (IPv6) address.
#[test]
fn inet_cell_path_key_accepts_only_4_or_16_bytes() {
    let p = parser();
    assert_eq!(
        p.parse_cell_path_key(&[127, 0, 0, 1], "inet", "k").unwrap(),
        Value::Inet(vec![127, 0, 0, 1].into()),
        "IPv4"
    );
    assert_eq!(
        p.parse_cell_path_key(&[0u8; 16], "inet", "k").unwrap(),
        Value::Inet(vec![0u8; 16].into()),
        "IPv6"
    );
    for bad in [1usize, 3, 5, 8, 15, 17] {
        let err = p
            .parse_cell_path_key(&vec![0u8; bad], "inet", "k")
            .expect_err("only 4 or 16 bytes is a valid inet");
        assert!(
            err.to_string().contains("4 or 16"),
            "the message must name BOTH accepted widths, got: {err}"
        );
    }
    // The marshal spelling goes through the same table.
    assert!(p
        .parse_cell_path_key(&[0u8; 5], &format!("{MARSHAL}.InetAddressType"), "k")
        .is_err());
}

#[test]
fn every_decodable_cell_path_key_type_decodes_without_erroring() {
    let p = parser();
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("text", b"x".to_vec()),
        ("ascii", b"x".to_vec()),
        ("int", 1i32.to_be_bytes().to_vec()),
        ("bigint", 1i64.to_be_bytes().to_vec()),
        ("boolean", vec![1]),
        ("tinyint", vec![1]),
        ("smallint", 1i16.to_be_bytes().to_vec()),
        ("float", 1.0f32.to_be_bytes().to_vec()),
        ("double", 1.0f64.to_be_bytes().to_vec()),
        ("decimal", vec![0, 0, 0, 0, 1]),
        ("varint", vec![1]),
        ("inet", vec![127, 0, 0, 1]),
        ("time", 1i64.to_be_bytes().to_vec()),
        ("date", 0x8000_0000u32.to_be_bytes().to_vec()),
        ("timestamp", 1i64.to_be_bytes().to_vec()),
        ("uuid", vec![0u8; 16]),
        ("timeuuid", vec![0u8; 16]),
        ("frozen<collide>", collide_key_bytes()),
        ("collide", collide_key_bytes()),
    ];
    for (type_str, bytes) in cases {
        p.parse_cell_path_key(&bytes, type_str, "k")
            .unwrap_or_else(|e| panic!("{type_str} must still decode, got: {e}"));
    }
}

// ---------------------------------------------------------------------------
// THE CLASS, not the arm: "two distinct corrupted paths, one logical key"
// ---------------------------------------------------------------------------
//
// Round 2 of review found the round-1 framing validator covered only tuple and
// UDT, so a frozen list/set/map key or a `duration` key still decoded from a
// PREFIX — two different byte strings yielding the SAME logical map key, which
// is the defect this whole check exists to close. Each case below appends bytes
// to a VALID encoding and asserts (1) the clean form decodes, (2) the appended
// form is REFUSED, and (3) — the property that actually matters — that the two
// are DISTINCT byte strings, so a silent accept really would be a collision.

/// Assert the collision shape for one declared type: `clean` decodes, `clean +
/// junk` is refused, and the two inputs differ. Written once so no per-decoder
/// case can end up quietly weaker than its siblings.
fn assert_no_prefix_collision(type_str: &str, clean: &[u8], junk: &[u8]) {
    let p = parser();
    let decoded_clean = p
        .parse_cell_path_key(clean, type_str, "k")
        .unwrap_or_else(|e| panic!("{type_str}: the clean encoding must decode, got: {e}"));
    let mut corrupt = clean.to_vec();
    corrupt.extend_from_slice(junk);
    assert_ne!(
        corrupt.as_slice(),
        clean,
        "{type_str}: the test is only meaningful if the two inputs differ"
    );
    match p.parse_cell_path_key(&corrupt, type_str, "k") {
        Ok(also) => panic!(
            "{type_str}: appending {} byte(s) was ACCEPTED and produced {:?} — the \
             clean encoding produced {decoded_clean:?}; two distinct cell paths now \
             collapse to one logical key",
            junk.len(),
            also
        ),
        Err(e) => {
            // Any of THREE layers may refuse, and which one does is not the
            // property under test — that the two byte strings do not collapse is.
            // The message is still constrained, so a refusal for an unrelated
            // reason (say a registry miss) cannot be mistaken for this one:
            //   * the consumption rule       -> "decoded only N of M byte(s)";
            //   * the caller's width table   -> "requires exactly N bytes";
            //   * the decoder's own element
            //     bounds check, which already
            //     rejected a dangling FULL
            //     4-byte header pre-change    -> "available in blob".
            let msg = e.to_string();
            assert!(
                msg.contains("decoded only")
                    || msg.contains("requires exactly")
                    || msg.contains("available in blob"),
                "{type_str}: the refusal must name a length/consumption problem, \
                 got: {msg}"
            );
        }
    }
}

/// `[i32 BE count]` then per element `[i32 BE len][bytes]` — the framing
/// `parse_frozen_sequence_value_raw` reads for a frozen list/set.
fn encode_sequence(elements: &[&[u8]]) -> Vec<u8> {
    let mut out = (elements.len() as i32).to_be_bytes().to_vec();
    for e in elements {
        out.extend_from_slice(&(e.len() as i32).to_be_bytes());
        out.extend_from_slice(e);
    }
    out
}

/// `[i32 BE count]` then per entry `[i32 len][key][i32 len][value]`.
fn encode_frozen_map(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut out = (entries.len() as i32).to_be_bytes().to_vec();
    for (k, v) in entries {
        out.extend_from_slice(&(k.len() as i32).to_be_bytes());
        out.extend_from_slice(k);
        out.extend_from_slice(&(v.len() as i32).to_be_bytes());
        out.extend_from_slice(v);
    }
    out
}

#[test]
fn frozen_list_cell_path_key_rejects_trailing_bytes() {
    let clean = encode_sequence(&[&1i32.to_be_bytes(), &2i32.to_be_bytes()]);
    assert_no_prefix_collision("frozen<list<int>>", &clean, b"\xde\xad");
}

#[test]
fn frozen_set_cell_path_key_rejects_trailing_bytes() {
    let clean = encode_sequence(&[b"a", b"b"]);
    assert_no_prefix_collision("frozen<set<text>>", &clean, b"\x00");
}

#[test]
fn frozen_map_cell_path_key_rejects_trailing_bytes() {
    let clean = encode_frozen_map(&[(b"k", &7i32.to_be_bytes())]);
    assert_no_prefix_collision("frozen<map<text, int>>", &clean, b"\xff\xff\xff");
}

#[test]
fn duration_cell_path_key_rejects_trailing_bytes() {
    // Three signed VInts: months=0, days=0, nanos=0 (each one byte under
    // Cassandra's zigzag VInt encoding).
    let clean = vec![0u8, 0, 0];
    assert_no_prefix_collision("duration", &clean, b"\x01");
}

#[test]
fn marshal_form_composite_cell_path_keys_reject_trailing_bytes_too() {
    // The marshal spellings take the same dispatch arms, so the check must not
    // be reachable only through the CQL short forms.
    let clean = encode_sequence(&[&1i32.to_be_bytes()]);
    assert_no_prefix_collision(
        &format!("{MARSHAL}.ListType({MARSHAL}.Int32Type)"),
        &clean,
        b"\x07",
    );
    let clean_map = encode_frozen_map(&[(b"k", &7i32.to_be_bytes())]);
    assert_no_prefix_collision(
        &format!("{MARSHAL}.MapType({MARSHAL}.UTF8Type,{MARSHAL}.Int32Type)"),
        &clean_map,
        b"\x07",
    );
}

// ---------------------------------------------------------------------------
// R2-F2: a PARTIAL trailing component-length header is corruption, not omission
// ---------------------------------------------------------------------------

/// The subtle half of the class. A composite decoder treats "fewer than 4 bytes
/// remain" as "the trailing components are omitted", which is LEGAL per
/// `TupleType.split`'s `position == length` early return — but only when nothing
/// remains at all. Appending 1-3 junk bytes hits that same early exit, so before
/// the consumption rule it produced the SAME decoded key as the clean short
/// encoding: a collision reachable by appending one byte.
#[test]
fn a_partial_trailing_component_header_is_rejected_for_a_udt_key() {
    let clean = encode_components(&[Some(b"only-the-first-field")]);
    for junk_len in 1..=3usize {
        assert_no_prefix_collision("frozen<collide>", &clean, &vec![0xABu8; junk_len]);
    }
}

#[test]
fn a_partial_trailing_component_header_is_rejected_for_a_tuple_key() {
    let clean = encode_components(&[Some(b"abc")]);
    for junk_len in 1..=3usize {
        assert_no_prefix_collision("tuple<text, int>", &clean, &vec![0x5Au8; junk_len]);
    }
}

/// A FULL 4-byte header with no body is also `pos < len` and must be refused —
/// the boundary immediately above the 1-3 byte case.
#[test]
fn a_dangling_component_length_header_is_rejected() {
    let clean = encode_components(&[Some(b"abc")]);
    assert_no_prefix_collision("tuple<text, int>", &clean, &7i32.to_be_bytes());
}

// ---------------------------------------------------------------------------
// The enumeration itself: every COMPOSITE spelling must report a consumption
// ---------------------------------------------------------------------------

/// The drift guard for the dispatch table. `decode_reporting_consumption` mirrors
/// `parse_value_from_raw_bytes`'s composite arms, and an arm added there but not
/// here would silently fall through to the `None` ("whole slice by construction")
/// default — i.e. straight back to a prefix decode. Rather than inspect the
/// private return value, this asserts the OBSERVABLE consequence for every
/// composite spelling the decoder supports: appending a byte must be refused.
///
/// DIRECTIONAL: the list is HAND-CURATED, so it proves the arms it names still
/// refuse; it cannot see an arm added to `parse_value_from_raw_bytes` and missing
/// from the dispatcher. That limit is stated once, at `decode_reporting_consumption`.
#[test]
fn every_composite_cell_path_key_spelling_is_consumption_checked() {
    let int_elem = encode_sequence(&[&1i32.to_be_bytes()]);
    let map_bytes = encode_frozen_map(&[(b"k", &7i32.to_be_bytes())]);
    let udt_bytes = collide_key_bytes();
    let tuple_bytes = encode_components(&[Some(b"abc"), Some(&42i32.to_be_bytes())]);
    let cases: Vec<(String, &[u8])> = vec![
        ("list<int>".to_string(), &int_elem),
        ("set<int>".to_string(), &int_elem),
        ("map<text, int>".to_string(), &map_bytes),
        ("tuple<text, int>".to_string(), &tuple_bytes),
        ("frozen<list<int>>".to_string(), &int_elem),
        ("frozen<set<int>>".to_string(), &int_elem),
        ("frozen<map<text, int>>".to_string(), &map_bytes),
        ("frozen<collide>".to_string(), &udt_bytes),
        ("collide".to_string(), &udt_bytes),
        (
            format!("{MARSHAL}.ListType({MARSHAL}.Int32Type)"),
            &int_elem,
        ),
        (format!("{MARSHAL}.SetType({MARSHAL}.Int32Type)"), &int_elem),
        (
            format!("{MARSHAL}.MapType({MARSHAL}.UTF8Type,{MARSHAL}.Int32Type)"),
            &map_bytes,
        ),
        (
            format!("{MARSHAL}.TupleType({MARSHAL}.UTF8Type,{MARSHAL}.Int32Type)"),
            &tuple_bytes,
        ),
        (
            format!("{MARSHAL}.FrozenType({MARSHAL}.ListType({MARSHAL}.Int32Type))"),
            &int_elem,
        ),
        ("duration".to_string(), &[0u8, 0, 0]),
        // The marshal spelling reaches the duration arm through a DIFFERENT
        // route (`primitive_marshal_to_cql_short` normalizes it inside
        // `parse_value_from_raw_bytes`, but this dispatcher sees the raw
        // string), so it is enumerated separately rather than assumed.
        (format!("{MARSHAL}.DurationType"), &[0u8, 0, 0]),
        // The marshal `UserType(..)` is a THIRD UDT route: a different inline
        // field loop in `raw_type_value.rs` from the registry-bare one above, and
        // the one the committed `cm` fixture key actually takes.
        (udt_marshal_type(), &udt_bytes),
    ];
    assert_eq!(
        cases.len(),
        17,
        "keep this count in step with the case list, so a deleted case is visible"
    );
    for (type_str, clean) in cases {
        assert_no_prefix_collision(&type_str, clean, b"\x01");
    }
}

// ---------------------------------------------------------------------------
// R3-F1: a component length below -1 must ERROR, never panic (untrusted bytes)
// ---------------------------------------------------------------------------
//
// A component is `[i32 BE len][bytes]` with `-1` meaning null. Three UDT field
// loops handled `-1` and `0` and then cast the remainder with a bare `as usize`,
// so `-2` became ~1.8e19 and the following `offset + len > data.len()` test
// OVERFLOWED — a debug-build panic, and in release a wrap after which the bounds
// test can pass and the slice index panics instead. CLAUDE.md forbids a reachable
// panic in a parser on untrusted bytes.
//
// Reachable from a cell-path key only since #3612 delegated the key to the
// structural decoder, which routes a registry-resolved bare name and a marshal
// `UserType(..)` through `parse_raw_type_value`. Every negative other than `-1`
// is now refused BEFORE conversion by `checked_component_len`.

/// Component bytes whose FIRST length header is `raw`, followed by three valid
/// components — so the poison is at the head and the rest is well-formed.
fn components_with_leading_len(raw: i32) -> Vec<u8> {
    let mut out = raw.to_be_bytes().to_vec();
    out.extend_from_slice(&encode_components(&[
        Some(b"key-keyspace-marker"),
        Some(b"key-proto-marker"),
        Some(&100i32.to_be_bytes()),
    ]));
    out
}

#[test]
fn a_component_length_below_minus_one_errors_and_never_panics() {
    let p = parser();
    // Both UDT spellings the cell-path route can take, and the boundary values.
    let marshal = udt_marshal_type();
    for spelling in ["collide", "frozen<collide>", marshal.as_str()] {
        for raw in [-2i32, -7, i32::MIN, -1_000_000] {
            let bytes = components_with_leading_len(raw);
            // The ONLY acceptable outcome is a returned error. A panic fails the
            // test by aborting it, which is the point of driving real bytes here.
            let Err(err) = p.parse_cell_path_key(&bytes, spelling, "cm") else {
                panic!("{spelling}: component length {raw} must be REFUSED, not decoded");
            };
            let msg = err.to_string();
            assert!(
                msg.contains("negative") || msg.contains("beyond data"),
                "{spelling}/{raw}: the error must name the length problem, got: {msg}"
            );
        }
    }
}

/// `-1` stays legal — the guard must not have swept the null marker up with the
/// corrupt values. This is the control that the fix is a narrowing, not a ban.
#[test]
fn a_minus_one_component_length_is_still_a_null_field() {
    let bytes = components_with_leading_len(-1);
    let value = parser()
        .parse_cell_path_key(&bytes, "frozen<collide>", "cm")
        .expect("-1 is the NULL marker and must still decode");
    let u = udt_of(&value);
    assert_eq!(field(u, "_type"), None, "-1 is null, not an error");
    assert_eq!(text(field(u, "_keyspace")), "key-keyspace-marker");
    assert_eq!(field(u, "real_field"), Some(Value::Integer(100)));
}

// ---------------------------------------------------------------------------
// The width table mirrors Cassandra's THREE-WAY serializer split, not a uniform
// `!= N` (issue #3612 round 3 addendum)
// ---------------------------------------------------------------------------
//
// !! REACHABILITY: THE EMPTY-KEY CASES BELOW ARE NOW REACHED BY A REAL READ
// !! (issue #3747). This note previously said the opposite; do not restore it.
//
// #3612 wrote them as UNIT-ONLY and that was accurate THEN: the sole production
// caller decoded a key only `if !cell.path_bytes.is_empty()`, so a zero-length
// path never reached `parse_cell_path_key` — and that branch DROPPED the entry,
// because `if let Some(key_value) = decoded_key` never fired. #3612 filed the
// swallow as #3747 instead of fixing it. #3747 removed the guard, so these cases
// now describe behaviour a `SELECT` and a compaction read exercise. They are the
// function-level half only: the wiring evidence is
// `cqlite-core/tests/issue_3747_empty_map_key.rs`, against a Cassandra-written
// fixture and its golden (a unit test is not wiring evidence on its own).
//
// That filter is PRE-EXISTING, not part of #3612, and it means a legal empty
// `text`/`blob` map key is silently dropped from query and compaction results —
// filed separately rather than changed here, since the filter governs every
// complex column.
//
// The per-serializer split is in `cell_path_key.rs`'s header (verified at the
// pinned `cassandra-5.0.8` tag); it is not restated here, because this file
// already carried a STALE copy saying "Inet THROWS on empty" — it returns early,
// so inet is `0/4/16`. Both directions of the asymmetry are pinned below, because
// only the pair is evidence.

/// The `N`-or-`0` family: a 0-byte key passes the WIDTH table (Cassandra's
/// serializer accepts an empty buffer) and is now PRESERVED, opaquely.
///
/// #3612 asserted an `Err` here and called the `0` allowance "a fidelity fix, not a
/// behaviour change" — true then: the decoder's length guard refused what the table
/// admitted. **#3747 changed that on purpose** — losing a key Cassandra accepts is
/// the data loss it exists to stop, and an untypeable key already had a policy:
/// opaque bytes plus `opaque_out`. The STRICT sibling alone now carries the other half.
#[test]
fn an_empty_key_of_an_n_or_zero_type_is_preserved_opaquely() {
    let p = parser();
    #[rustfmt::skip]
    let types = ["int","bigint","float","double","uuid","timeuuid","timestamp","counter","boolean"];
    for type_str in types {
        let mut opaque = false;
        let decoded = p
            .parse_cell_path_key_reporting(&[], type_str, "k", &mut opaque)
            .unwrap_or_else(|e| panic!("{type_str}: Cassandra accepts empty; keep it: {e}"));
        assert_eq!(
            decoded,
            Value::blob(Vec::new()),
            "{type_str}: preserved opaquely"
        );
        assert!(
            opaque,
            "{type_str}: opaque_out must be raised (gap goes to the log)"
        );
    }
}

/// The STRICT family — Short, Byte, SimpleDate, Time: a 0-byte key IS refused by
/// the WIDTH TABLE, because these four serializers alone have no `isEmpty`
/// allowance. This is the half that makes the three-way split load-bearing rather
/// than decorative. (`inet` is NOT one of them — see
/// `an_empty_inet_key_decodes_and_is_reachable_by_a_read`.)
/// Reached by a real read since #3747 removed the caller's empty-path guard;
/// see the REACHABILITY note above.
#[test]
fn an_empty_key_of_a_strict_type_is_refused_by_the_width_table() {
    let p = parser();
    for (type_str, width) in [
        ("smallint", "2"),
        ("tinyint", "1"),
        ("date", "4"),
        ("time", "8"),
    ] {
        let err = p
            .parse_cell_path_key(&[], type_str, "k")
            .expect_err("a strict fixed-width type admits no empty buffer");
        let msg = err.to_string();
        assert!(
            msg.contains("requires exactly") && msg.contains(width),
            "{type_str}: the WIDTH TABLE must refuse an empty key and name {width}, \
             got: {msg}"
        );
    }
}

/// `inet` is NOT a fifth strict case, and it is the ONE family where the empty
/// buffer decodes rather than merely passing the width table.
///
/// This carried the loudest UNIT-ONLY warning in the file, because its name reads
/// like a capability claim and no read could reach it. **No longer true**: #3747
/// removed the caller's empty-path guard, so a `SELECT` over `map<inet,…>` with an
/// empty key does reach this arm and does return an empty `Value::Inet`. Renamed
/// accordingly — a name asserting unreachability is worse than none once the code
/// has moved. It remains the only thing pinning the corrected `[0, 4, 16]` row.
///
/// `InetAddressSerializer.validate` RETURNS EARLY on empty
/// (`if (accessor.isEmpty(value)) return;`) and only then delegates to
/// `getByAddress`, so an empty `inet` is legal to Cassandra — and CQLite's inet
/// arm borrows the whole slice with no minimum, so it round-trips as an empty
/// `Value::Inet`. Pinned because THREE places in this diff previously called inet
/// "the fifth strict case", on the strength of a grep whose output line ran the
/// `isEmpty` test together with the `throw` from the `catch (UnknownHostException)`
/// block below it. Read whole methods, not greps of their `if`s.
#[test]
fn an_empty_inet_key_decodes_and_is_reachable_by_a_read() {
    let p = parser();
    assert_eq!(
        p.parse_cell_path_key(&[], "inet", "k").unwrap(),
        Value::Inet(Vec::new().into()),
        "Cassandra returns early on an empty inet, so it is a legal value"
    );
    // The two NON-empty widths still work, and nothing between or beyond does.
    assert!(p.parse_cell_path_key(&[127, 0, 0, 1], "inet", "k").is_ok());
    assert!(p.parse_cell_path_key(&[0u8; 16], "inet", "k").is_ok());
    for bad in [1usize, 3, 5, 15, 17] {
        assert!(
            p.parse_cell_path_key(&vec![0u8; bad], "inet", "k").is_err(),
            "inet: {bad} bytes is neither empty, IPv4 nor IPv6"
        );
    }
}

/// Over-width is still refused for EVERY fixed-width family, empty-allowance or
/// not — the `0` entries must not have widened the accepted set upwards.
#[test]
fn admitting_an_empty_width_did_not_widen_the_upper_bound() {
    let p = parser();
    for (type_str, over) in [
        ("int", 5usize),
        ("bigint", 9),
        ("float", 5),
        ("double", 9),
        ("uuid", 17),
        ("timestamp", 9),
        ("boolean", 2),
        ("smallint", 3),
        ("tinyint", 2),
        ("date", 5),
        ("time", 9),
        ("inet", 5),
    ] {
        assert!(
            p.parse_cell_path_key(&vec![0u8; over], type_str, "k")
                .is_err(),
            "{type_str}: {over} bytes must still be refused"
        );
    }
}

// ---------------------------------------------------------------------------
// R3-F2: the multicell key presents EXACTLY as the FROZEN spelling, per type
// ---------------------------------------------------------------------------
//
// The differential the earlier parity tests were missing: they covered only UDT
// keys, where both sides happen to agree because Cassandra's frozen-collection
// header omits the `FrozenType` marker and `prefer_udt_marshal_element` prefers
// that marshal string anyway (#1340). For a COLLECTION key the frozen side falls
// back to the SCHEMA spelling `frozen<set<int>>`, which IS frozen-spelled, and
// wraps — so an unconditional strip on the multicell side diverged.
//
// Both sides funnel a key through `parse_value_from_raw_bytes`, so measuring that
// function with the type string the frozen side would supply IS measuring the
// frozen side. Asserted as EQUALITY of the two, per type, so neither can drift.

#[test]
fn multicell_and_frozen_sides_present_every_composite_key_type_identically() {
    let p = parser();
    let udt = collide_key_bytes();
    let seq = encode_sequence(&[&1i32.to_be_bytes()]);
    let mp = encode_frozen_map(&[(b"k", &7i32.to_be_bytes())]);
    let tup = encode_components(&[Some(b"abc"), Some(&42i32.to_be_bytes())]);
    let udt_marshal = udt_marshal_type();
    // (the type string the FROZEN side hands its key decoder, the key bytes,
    //  whether a `Value::Frozen` wrapper is EXPECTED on both sides)
    let cases: Vec<(String, &[u8], bool)> = vec![
        (udt_marshal, &udt, false),
        ("frozen<set<int>>".into(), &seq, true),
        ("frozen<list<int>>".into(), &seq, true),
        ("frozen<map<text, int>>".into(), &mp, true),
        ("tuple<text, int>".into(), &tup, false),
        ("frozen<tuple<text, int>>".into(), &tup, true),
        (
            format!("{MARSHAL}.FrozenType({MARSHAL}.SetType({MARSHAL}.Int32Type))"),
            &seq,
            true,
        ),
    ];
    assert_eq!(cases.len(), 7, "keep in step with the case list");
    for (type_str, bytes, expect_wrapped) in cases {
        let frozen_side = p
            .parse_value_from_raw_bytes(bytes, &type_str, "k", 0)
            .unwrap_or_else(|e| panic!("{type_str}: frozen-side decode failed: {e}"));
        let multicell = p
            .parse_cell_path_key(bytes, &type_str, "k")
            .unwrap_or_else(|e| panic!("{type_str}: multicell decode failed: {e}"));
        assert_eq!(
            matches!(frozen_side, Value::Frozen(_)),
            expect_wrapped,
            "{type_str}: the FROZEN side's wrapper shape changed — re-derive the \
             expectation from a measurement before touching the multicell side"
        );
        assert_eq!(
            multicell, frozen_side,
            "{type_str}: the multicell key must present EXACTLY as the frozen \
             spelling (issue #3612 R3-F2)"
        );
    }
}

/// The consequence that makes it matter: `Value` equality/hashing distinguish
/// `Frozen(Set(..))` from `Set(..)`, so a divergence is observable on the public
/// Rust surface and not merely a shape detail.
#[test]
fn a_wrapper_divergence_would_be_observable_through_value_equality() {
    let inner = Value::Set(vec![Value::Integer(1)]);
    let wrapped = Value::Frozen(Box::new(inner.clone()));
    assert_ne!(inner, wrapped, "PartialEq distinguishes the two shapes");
    use std::collections::HashSet;
    let mut set: HashSet<String> = HashSet::new();
    set.insert(format!("{inner:?}"));
    assert!(
        !set.contains(&format!("{wrapped:?}")),
        "the two shapes are distinct as map keys too"
    );
}

// ---------------------------------------------------------------------------
// R8-F1: a COLLECTION-keyed multicell map presents its key exactly as the frozen
// spelling does — the class the tuple/UDT tests left open
// ---------------------------------------------------------------------------
//
// WHY A UNIT TEST AND NOT A FIXTURE, recorded so nobody "upgrades" it later.
// #3042 governs FRAMING/ENCODING properties, where a CQLite-written/CQLite-read
// round trip is invariant to a uniform error. This property is different in kind:
// that our decoder NORMALISES TWO TYPE SPELLINGS TO ONE PRESENTATION. Both
// spellings are INPUTS stated from the marshal grammar, the same bytes feed both
// sides, and there is no framing to get symmetrically wrong.
//
// And the corpus has no subject: MEASURED, the committed schemas contain no
// multicell set-, list- or map-keyed map at all (the 5 multicell map columns are
// keyed by text, UDT, UDT and tuple). If such a fixture is added, prefer it.
//
// The two spellings are Cassandra's own: a MULTICELL map key marshal KEEPS its
// `FrozenType` (such a key must be explicitly frozen) while the FROZEN spelling
// omits it (all inside a frozen collection is already frozen). Worked through in
// `map_key_type_for_decode`'s doc.

/// A frozen `set<frozen<collide>>` body: `[i32 count][i32 len][udt bytes]…`.
fn encode_set_of_collide() -> Vec<u8> {
    let member = collide_key_bytes();
    let mut out = 1i32.to_be_bytes().to_vec();
    out.extend_from_slice(&(member.len() as i32).to_be_bytes());
    out.extend_from_slice(&member);
    out
}

/// THE R8-F1 ASSERTION: the two readers' key-type spellings must decode to the
/// SAME `Value`, with equal `hash`, and with NO peeling on either side.
#[test]
fn set_of_udt_key_presents_identically_across_both_marshal_spellings() {
    let p = parser();
    let bytes = encode_set_of_collide();
    let schema = "frozen<set<frozen<collide>>>";

    // The two spellings Cassandra records, each passed through the ONE shared rule
    // exactly as its own reader does — the rule lives in the CALLER, so a test that
    // handed these strings straight to the decoder would model neither reader and
    // would fail for a reason unrelated to the property.
    let multicell_key_type = V5CompressedLegacyParser::map_key_type_for_decode(
        Some(&format!(
            "{MARSHAL}.FrozenType({MARSHAL}.SetType({}))",
            udt_marshal_type()
        )),
        schema,
    );
    let frozen_key_type = V5CompressedLegacyParser::map_key_type_for_decode(
        Some(&format!("{MARSHAL}.SetType({})", udt_marshal_type())),
        schema,
    );
    // The rule's whole job: two different recorded spellings, one decode type.
    assert_eq!(
        multicell_key_type, frozen_key_type,
        "the shared rule must normalize both recorded spellings to ONE string"
    );

    let multicell = p
        .parse_cell_path_key(&bytes, &multicell_key_type, "k")
        .expect("the multicell spelling must decode");
    let frozen = p
        .parse_value_from_raw_bytes(&bytes, &frozen_key_type, "k", 0)
        .expect("the frozen spelling must decode");

    // The property, unpeeled and both ways round.
    assert_eq!(
        multicell, frozen,
        "a set-of-UDT map key must present IDENTICALLY under both spellings \
         (issue #3612, roborev round 8 finding 1); multicell={multicell:?} frozen={frozen:?}"
    );
    assert_eq!(
        hash_of_value(&multicell),
        hash_of_value(&frozen),
        "equal keys must hash equally, or they are two entries in any hashed \
         projection"
    );

    // And it is a STRUCTURED set of a structured UDT, not an opaque blob — so the
    // equality above cannot be satisfied by both sides failing the same way.
    match &multicell {
        Value::Set(items) => {
            assert_eq!(items.len(), 1);
            let u = udt_of(&items[0]);
            assert_eq!(u.type_name, "collide");
        }
        other => panic!("expected a structured Value::Set, got {other:?}"),
    }
}

/// The same for a LIST-keyed and a MAP-keyed multicell map: the other two members
/// of this class, neither of which has a fixture either.
#[test]
fn list_and_map_keyed_multicell_keys_present_identically_too() {
    let p = parser();
    let list_bytes = encode_set_of_collide(); // same framing: [count][len][elem]
    for (multicell_marshal, frozen_marshal, schema, bytes) in [
        (
            format!(
                "{MARSHAL}.FrozenType({MARSHAL}.ListType({}))",
                udt_marshal_type()
            ),
            format!("{MARSHAL}.ListType({})", udt_marshal_type()),
            "frozen<list<frozen<collide>>>",
            list_bytes.clone(),
        ),
        (
            format!(
                "{MARSHAL}.FrozenType({MARSHAL}.MapType({MARSHAL}.UTF8Type,{}))",
                udt_marshal_type()
            ),
            format!(
                "{MARSHAL}.MapType({MARSHAL}.UTF8Type,{})",
                udt_marshal_type()
            ),
            "frozen<map<text, frozen<collide>>>",
            {
                let v = collide_key_bytes();
                let mut out = 1i32.to_be_bytes().to_vec();
                out.extend_from_slice(&1i32.to_be_bytes());
                out.extend_from_slice(b"k");
                out.extend_from_slice(&(v.len() as i32).to_be_bytes());
                out.extend_from_slice(&v);
                out
            },
        ),
    ] {
        let mc_type =
            V5CompressedLegacyParser::map_key_type_for_decode(Some(&multicell_marshal), schema);
        let fz_type =
            V5CompressedLegacyParser::map_key_type_for_decode(Some(&frozen_marshal), schema);
        assert_eq!(
            mc_type, fz_type,
            "{multicell_marshal}: rule must normalize both"
        );
        let multicell = p
            .parse_cell_path_key(&bytes, &mc_type, "k")
            .unwrap_or_else(|e| panic!("{multicell_marshal} must decode: {e}"));
        let frozen = p
            .parse_value_from_raw_bytes(&bytes, &fz_type, "k", 0)
            .unwrap_or_else(|e| panic!("{frozen_marshal} must decode: {e}"));
        assert_eq!(
            multicell, frozen,
            "{multicell_marshal}: must present identically to its frozen spelling"
        );
        assert_eq!(hash_of_value(&multicell), hash_of_value(&frozen));
    }
}

/// Hashing helper, local to this file so the assertions above can state `hash`
/// equality alongside `==` (the pair is the property; `==` alone would miss a
/// `Hash` impl that disagreed with `PartialEq`).
fn hash_of_value(v: &Value) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

// ---------------------------------------------------------------------------
// R9-F2: a FOREIGN type whose name merely ENDS IN `BytesType` is not a blob
// ---------------------------------------------------------------------------
//
// `cell_path_key_declares_blob` used to prefix a bare name with the canonical
// Cassandra package and ask the SUFFIX normalizer, so `com.acme.CustomBytesType`
// normalized to `blob`, the key was reported as a DECLARED blob, and the opaque-key
// warning was suppressed — in exactly the case the warning exists for. Deciding a
// type's identity from a name suffix is inference from a name pattern (#28); the
// closed set of exact names below is not.
//
// The property is observable without capturing logs: `declares_blob` is what
// separates "opaque because DECLARED blob" from "opaque because UNMODELLED", and
// only the latter is counted for the aggregated warning. So the test asserts the
// decode outcome for both, and that the foreign name still yields opaque bytes.

#[test]
fn a_foreign_type_ending_in_bytes_type_is_not_treated_as_a_declared_blob() {
    let p = parser();
    // Foreign, unmodelled, and merely SUFFIXED — must decode opaque (the decoder
    // never reaches its normalizer, since the name lacks the Cassandra package)
    // and must NOT be mistaken for a declared blob.
    for foreign in [
        "com.acme.CustomBytesType",
        "com.example.marshal.MyBytesType",
        "org.apache.cassandra.db.marshal.vendor.CustomBytesType",
    ] {
        let got = p
            .parse_cell_path_key(&[1, 2, 3], foreign, "k")
            .unwrap_or_else(|e| panic!("{foreign} must still decode opaquely: {e}"));
        assert_eq!(
            got,
            Value::blob(vec![1, 2, 3]),
            "{foreign}: an unmodelled type surfaces as opaque bytes"
        );
        assert!(
            !p.cell_path_key_declares_blob(foreign),
            "{foreign} merely ENDS IN `BytesType`; treating it as a declared blob \
             suppresses the very warning that discloses an unmodelled key (#3612 R9-F2)"
        );
    }
    // The exact names that ARE a declared blob still are — the fix is a narrowing,
    // not a ban, and these are the spellings the decoder itself accepts.
    for real in [
        "blob",
        "BLOB",
        "bytes",
        "BytesType",
        "org.apache.cassandra.db.marshal.BytesType",
        "'org.apache.cassandra.db.marshal.BytesType'",
        "frozen<blob>",
    ] {
        assert!(
            p.cell_path_key_declares_blob(real),
            "{real} is a declared blob spelling"
        );
    }
    // CASE-SENSITIVE for marshal names, deliberately: the decoder's normalizer is
    // case-sensitive, so a lowercased marshal name does NOT decode as a blob and
    // must not be reported as one, or the two would disagree.
    assert!(!p.cell_path_key_declares_blob("org.apache.cassandra.db.marshal.bytestype"));
    assert!(!p.cell_path_key_declares_blob("bytestype"));
}

/// R10-F1: a `float` map key keeps CQL type identity.
///
/// `float` keys became reachable when #3612 widened the cell-path allowlist, and
/// the shared arm was widening f32 to the f64 `Value::Float` — so such a key
/// compared UNEQUAL to the same float decoded on the ordinary column path or as a
/// UDT field, both of which produce `Value::Float32`. Pinned on BOTH SPELLINGS, since a map
/// key that cannot compare equal to its own value is the collapse-class #3612 exists for.
#[test]
fn float_cell_path_key_keeps_cql_type_identity() {
    let p = parser();
    let bytes = 1.5f32.to_be_bytes();
    for spelling in ["float", &format!("{MARSHAL}.FloatType")] {
        assert_eq!(
            p.parse_cell_path_key(&bytes, spelling, "k").unwrap(),
            Value::Float32(1.5),
            "{spelling}: a float key is Float32, not the f64 Float"
        );
    }
    // NO cross-spelling assertion here: for `float` the cell-path decoder DELEGATES to
    // `parse_value_from_raw_bytes`, so float parity holds BY DELEGATION and no assertion
    // could falsify it. The composite parity test is not its home either — it hands ONE
    // type string to both sides, and all seven of its cases are composites by construction.
    // `double` is untouched and REMAINS the f64 `Value::Float` — the fix narrows
    // `float` alone and must not have swept its neighbour up.
    assert_eq!(
        p.parse_cell_path_key(&9.876f64.to_be_bytes(), "double", "k")
            .unwrap(),
        Value::Float(9.876)
    );
}
