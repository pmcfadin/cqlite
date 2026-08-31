//! Regression tests for issue #3612 — the DECODE surface for a MULTICELL map's
//! cell-path KEY.
//!
//! ## The defect
//! A non-frozen `map<K, V>` is multicell: every entry is its own cell and the
//! KEY lives in that cell's CELL PATH. `V5CompressedLegacyParser::parse_cell_path_key`
//! used to decode that key from a hand-maintained allowlist of SIX scalar
//! families (text/varchar/ascii, uuid/timeuuid, int, bigint/counter, date,
//! timestamp) and returned `Value::Blob` for everything else. So a COMPOSITE key
//! (`frozen<udt>`, `tuple<…>`, a frozen collection) and ~10 further scalar
//! families (boolean, float, double, smallint, tinyint, decimal, varint, time,
//! duration, inet, …) silently surfaced as raw bytes — the same key rendered
//! structurally by the FROZEN spelling of the same map, and by Cassandra's own
//! `sstabledump`.
//!
//! The fix delegates to the structural decoder
//! [`V5CompressedLegacyParser::parse_value_from_raw_bytes`] — the very function
//! the SET branch already used for a set member's cell path — so the cell-path
//! key path gains the whole type ladder (nested collections, tuples, UDTs,
//! `frozen<…>`, and every scalar marshal form) instead of a second, narrower
//! copy of it.
//!
//! ## What these tests pin (and why each one can fail on its own)
//! * **Composite keys decode structurally** (UDT, tuple), including a NULL field
//!   inside the composite — the case the committed Cassandra fixture does not
//!   carry (see `cqlite-core/tests/issue_3612_multicell_map_composite_key.rs`).
//! * **The previously-broken scalar families decode** (boolean, decimal, …).
//! * **CASE IS PRESERVED on delegation.** `primitive_marshal_to_cql_short` is
//!   CASE-SENSITIVE (`s.ends_with("Int32Type")`). The pre-fix code computed
//!   `type_str.to_lowercase()` up front, so handing the LOWERCASED string to the
//!   structural decoder would fail every marshal-form normalization and land
//!   right back in an opaque `Blob` — reintroducing the bug for the no-schema
//!   `Statistics.db` path, where the key type arrives in marshal form. Several
//!   cases below therefore use ORIGINAL-CASE marshal spellings and would red if
//!   a future refactor re-lowercased the delegated type string.
//! * **Exact-width validation is preserved and generalized.** For a cell path the
//!   ENTIRE stripped slice IS the key, so an over-long slice is corruption;
//!   `parse_value_from_raw_bytes` only rejects UNDER-width (`< N`) because its
//!   other callers hand it already-length-bounded element bytes. Authority for
//!   exact width is Cassandra 5.0.8 itself, e.g.
//!   `org.apache.cassandra.serializers.Int32Serializer.validate`:
//!   `if (accessor.size(value) != 4 && !accessor.isEmpty(value)) throw new
//!   MarshalException(...)` — and the same `!= N` shape in
//!   `LongSerializer`/`UUIDSerializer`/`TimestampSerializer`/`FloatSerializer`/
//!   `DoubleSerializer`/`ShortSerializer`/`ByteSerializer`/`SimpleDateSerializer`/
//!   `TimeSerializer` (`BooleanSerializer` spells it `size > 1`).
//! * **A `blob` key is a legitimate blob**, not a fallback — pinned so the
//!   diagnostic that used to claim every unhandled key was "parsed as blob"
//!   cannot come back.
//!
//! These carry NO dataset/feature-flag dependency: `parse_cell_path_key` is a
//! `pub(super)` method on a plainly-constructed parser, so they run in every
//! build and lane and can never pass vacuously on an empty corpus.

use super::V5CompressedLegacyParser;
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
    let marshal = format!(
        "{MARSHAL}.UserType({KEYSPACE},{},{}:{MARSHAL}.UTF8Type,{}:{MARSHAL}.UTF8Type,\
         {}:{MARSHAL}.UTF8Type,{}:{MARSHAL}.Int32Type)",
        hex::encode("collide"),
        hex::encode("_type"),
        hex::encode("_keyspace"),
        hex::encode("__proto__"),
        hex::encode("real_field"),
    );
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
    match parser().parse_cell_path_key(&bytes, "decimal", "dk").unwrap() {
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
        p.parse_cell_path_key(&5i32.to_be_bytes(), "int", "k").unwrap(),
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
    // Variable-width families are unaffected.
    assert!(p.parse_cell_path_key(&[0u8; 5], "text", "k").is_ok());
    assert!(p.parse_cell_path_key(&[0u8; 5], "blob", "k").is_ok());
}

// ---------------------------------------------------------------------------
// A `blob` key is a blob BY DECLARATION, not by fallback
// ---------------------------------------------------------------------------

#[test]
fn a_declared_blob_cell_path_key_is_a_blob() {
    let p = parser();
    assert_eq!(
        p.parse_cell_path_key(&[1, 2, 3], "blob", "k").unwrap(),
        Value::blob(vec![1, 2, 3])
    );
    assert_eq!(
        p.parse_cell_path_key(&[1, 2, 3], &format!("{MARSHAL}.BytesType"), "k")
            .unwrap(),
        Value::blob(vec![1, 2, 3])
    );
}
