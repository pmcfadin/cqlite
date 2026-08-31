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
    // Variable-width families are unaffected.
    assert!(p.parse_cell_path_key(&[0u8; 5], "text", "k").is_ok());
    assert!(p.parse_cell_path_key(&[0u8; 5], "blob", "k").is_ok());
}

// ---------------------------------------------------------------------------
// A `blob` key is a blob BY DECLARATION, not by fallback (issue #3612 option B)
// ---------------------------------------------------------------------------

/// The one case where `Value::Blob` is the RIGHT answer. The fail-closed check
/// below must distinguish "declared blob" from "undecoded", so this is its
/// control: if the check ever keyed on the RESULT alone it would reject these.
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
    // CQL does not permit `frozen<blob>` as a map key (freezing applies to
    // composites), but if such a spelling ever reaches here it is still a
    // DECLARED blob and must not be misdiagnosed as an undecoded key.
    assert_eq!(
        p.parse_cell_path_key(&[1, 2, 3], "frozen<blob>", "k")
            .unwrap(),
        Value::blob(vec![1, 2, 3])
    );
    // Case-INSENSITIVELY, because `parse_value_from_raw_bytes` routes off a
    // LOWERCASED guard: were the declared-blob test case-sensitive where the
    // decode is not, a `Frozen<BLOB>` would decode to a blob and then be
    // rejected as undecoded.
    assert_eq!(
        p.parse_cell_path_key(&[1, 2, 3], "Frozen<BLOB>", "k")
            .unwrap(),
        Value::blob(vec![1, 2, 3])
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
    assert_eq!(
        p.parse_cell_path_key(
            &[1, 2, 3],
            &format!("{MARSHAL}.FrozenType({MARSHAL}.BytesType)"),
            "k"
        )
        .unwrap(),
        Value::blob(vec![1, 2, 3])
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
    ];
    assert_eq!(
        cases.len(),
        15,
        "keep this count in step with the case list, so a deleted case is visible"
    );
    for (type_str, clean) in cases {
        assert_no_prefix_collision(&type_str, clean, b"\x01");
    }
}
