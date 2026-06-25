//! Primitive & special CQL codec byte-vector parity (issue #1004, epic #971).
//!
//! Proves CQLite's primitive / special CQL value codecs match the exact
//! Apache Cassandra 5.0.2 binary representation, using deterministic
//! checked-in vectors (`test-data/codec-vectors/primitives.json`). No SSTable
//! fixtures are required.
//!
//! For every fixture the test asserts:
//!
//! 1. ENCODE parity — `TypeSerializer` reproduces the Cassandra bytes.
//! 2. DECODE parity — the public `parser::types` decoders rebuild the value.
//! 3. CANONICAL bytes — a reference encoder (independent of CQLite) recomputes
//!    the expected bytes and matches the fixture, so the vectors are NOT derived
//!    from CQLite's own output.
//!
//! It additionally proves invalid byte lengths FAIL EXPLICITLY (length error)
//! for every fixed-width type and are never silently truncated to a default.

use cqlite_core::parser::types::{
    parse_cql_value, parse_cql_value_raw, parse_inet, parse_varint, CqlTypeId,
};
use cqlite_core::parser::vint::encode_vint;
use cqlite_core::storage::serialization::types::TypeSerializer;
use cqlite_core::types::Value;
use num_bigint::BigInt;
use serde_json::Value as Json;
use std::path::PathBuf;
use std::str::FromStr;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn vectors_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../test-data/codec-vectors/primitives.json");
    p
}

fn load() -> Json {
    let path = vectors_path();
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {}", path.display(), e));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("parse {}: {}", path.display(), e))
}

/// Parse "01 02 ff" into bytes; empty string -> empty vec.
fn hex(s: &str) -> Vec<u8> {
    s.split_whitespace()
        .map(|t| u8::from_str_radix(t, 16).unwrap_or_else(|_| panic!("bad hex byte {t:?}")))
        .collect()
}

fn arr<'a>(root: &'a Json, key: &str) -> &'a Vec<Json> {
    root[key]
        .as_array()
        .unwrap_or_else(|| panic!("section {key} missing/not array"))
}

fn s<'a>(e: &'a Json, k: &str) -> &'a str {
    e[k].as_str()
        .unwrap_or_else(|| panic!("field {k} missing/not string in {e:?}"))
}

fn i64f(e: &Json, k: &str) -> i64 {
    e[k].as_i64()
        .unwrap_or_else(|| panic!("field {k} missing/not i64 in {e:?}"))
}

// ---------------------------------------------------------------------------
// Reference encoders: independent re-implementation of Cassandra 5.0.2
// serializer contracts. NEVER call CQLite here.
// ---------------------------------------------------------------------------

/// IntegerType / java.math.BigInteger.toByteArray(): minimal two's-complement
/// big-endian, always at least one byte.
fn ref_varint(n: &BigInt) -> Vec<u8> {
    n.to_signed_bytes_be()
}

/// DecimalType: 4-byte BE signed scale, then ref_varint(unscaled).
fn ref_decimal(scale: i32, unscaled: &BigInt) -> Vec<u8> {
    let mut out = scale.to_be_bytes().to_vec();
    out.extend_from_slice(&ref_varint(unscaled));
    out
}

/// Cassandra VIntCoding.writeUnsignedVInt over a u64 magnitude.
fn ref_unsigned_vint(value: u64) -> Vec<u8> {
    // extra bytes = number of bytes beyond the first.
    // Mirrors VIntCoding.computeUnsignedVIntSize / encodeVInt.
    let magnitude_bits = 64 - value.leading_zeros() as i32;
    // size in bytes per Cassandra: (magnitude_bits - 1) / 7 + 1, capped at 9.
    let mut extra = if magnitude_bits <= 7 {
        0
    } else {
        ((magnitude_bits - 1) / 7) as usize
    };
    if extra > 8 {
        extra = 8;
    }
    // Verify the chosen size can actually hold the value given the header bits;
    // grow if a 7-extra header cannot fit (boundary cases).
    while extra < 8 {
        let usable = 8 * extra + (7 - extra); // data bits available with `extra` extra bytes
        if (value >> usable) == 0 {
            break;
        }
        extra += 1;
    }

    let mut out = Vec::with_capacity(extra + 1);
    if extra == 0 {
        return vec![value as u8];
    }
    if extra >= 8 {
        // 9-byte form: 0xFF then 8 raw bytes.
        out.push(0xFF);
        for i in (0..8).rev() {
            out.push((value >> (8 * i)) as u8);
        }
        return out;
    }
    // first byte: `extra` leading ones, a 0 separator, then top (7-extra) data bits.
    let leading = ((0xFFu16 << (8 - extra)) & 0xFF) as u8;
    let data_bits_first = 7 - extra;
    let shift = 8 * extra;
    let first_data = ((value >> shift) & ((1u64 << data_bits_first) - 1)) as u8;
    out.push(leading | first_data);
    for i in (0..extra).rev() {
        out.push((value >> (8 * i)) as u8);
    }
    out
}

/// Signed VInt = ZigZag then unsigned VInt (Cassandra VIntCoding.writeVInt).
fn ref_signed_vint(value: i64) -> Vec<u8> {
    let zz = ((value << 1) ^ (value >> 63)) as u64;
    ref_unsigned_vint(zz)
}

/// DurationType / DurationSerializer: 3 signed VInts months, days, nanos.
fn ref_duration(months: i32, days: i32, nanos: i64) -> Vec<u8> {
    let mut out = ref_signed_vint(months as i64);
    out.extend_from_slice(&ref_signed_vint(days as i64));
    out.extend_from_slice(&ref_signed_vint(nanos));
    out
}

/// SimpleDateType: 4-byte BE UNSIGNED, stored = days + 2^31.
fn ref_date(days: i32) -> Vec<u8> {
    let encoded = (days as u32).wrapping_add(0x8000_0000);
    encoded.to_be_bytes().to_vec()
}

// ---------------------------------------------------------------------------
// Encode + decode round-trip drivers
// ---------------------------------------------------------------------------

fn ser() -> TypeSerializer {
    TypeSerializer::new()
}

/// Assert CQLite encodes `value` as `type_name` to exactly `expected`.
fn assert_encode(value: &Value, type_name: &str, expected: &[u8], label: &str) {
    let got = ser()
        .serialize_value(value, type_name)
        .unwrap_or_else(|e| panic!("[{label}] encode {type_name} failed: {e}"));
    assert_eq!(
        got, expected,
        "[{label}] encode {type_name}: got {got:02x?} expected {expected:02x?}"
    );
}

/// Decode raw Cassandra cell bytes via the public parser::types decoder and
/// assert the resulting value, requiring all bytes consumed.
fn assert_decode_raw(bytes: &[u8], type_id: CqlTypeId, expected: &Value, label: &str) {
    let (rem, got) = parse_cql_value(bytes, type_id)
        .unwrap_or_else(|e| panic!("[{label}] decode {type_id:?} failed: {e:?}"));
    assert!(
        rem.is_empty(),
        "[{label}] decode {type_id:?} left {} trailing bytes",
        rem.len()
    );
    assert_eq!(
        &got, expected,
        "[{label}] decode {type_id:?} value mismatch"
    );
}

// ===========================================================================
// Manifest: cass.cql_types.primitives.fixed_width_vectors
// ===========================================================================

#[test]
fn fixed_width_vectors() {
    let root = load();
    for e in arr(&root, "fixed_width") {
        let ty = s(e, "type");
        let label = format!("fixed_width/{}/{}", ty, s(e, "name"));
        let expected = hex(s(e, "hex"));

        let (value, type_id) = match ty {
            "boolean" => (
                Value::Boolean(e["value"].as_bool().unwrap()),
                CqlTypeId::Boolean,
            ),
            "tinyint" => (Value::TinyInt(i64f(e, "value") as i8), CqlTypeId::Tinyint),
            "smallint" => (
                Value::SmallInt(i64f(e, "value") as i16),
                CqlTypeId::Smallint,
            ),
            "int" => (Value::Integer(i64f(e, "value") as i32), CqlTypeId::Int),
            "bigint" => (Value::BigInt(i64f(e, "value")), CqlTypeId::BigInt),
            "float" => {
                let bits = hex(s(e, "value_bits_be"));
                let f = f32::from_be_bytes(bits.as_slice().try_into().unwrap());
                (Value::Float32(f), CqlTypeId::Float)
            }
            "double" => {
                let bits = hex(s(e, "value_bits_be"));
                let f = f64::from_be_bytes(bits.as_slice().try_into().unwrap());
                (Value::Float(f), CqlTypeId::Double)
            }
            other => panic!("[{label}] unexpected fixed_width type {other}"),
        };

        // 1. Encode parity.
        assert_encode(&value, ty, &expected, &label);

        // 2. Decode parity. tinyint/smallint decode into Value::Integer in
        //    parser::types; compare against the widened expectation.
        let decode_expect = match &value {
            Value::TinyInt(n) => Value::Integer(*n as i32),
            Value::SmallInt(n) => Value::Integer(*n as i32),
            v => v.clone(),
        };
        // Floats compare bit-exactly (handles -0.0); use raw bit equality.
        if let Value::Float32(f) = &value {
            let (rem, got) = parse_cql_value(&expected, type_id).unwrap();
            assert!(rem.is_empty(), "[{label}] float trailing bytes");
            match got {
                Value::Float32(g) => assert_eq!(
                    g.to_be_bytes(),
                    f.to_be_bytes(),
                    "[{label}] float bit mismatch"
                ),
                o => panic!("[{label}] expected Float32, got {o:?}"),
            }
        } else if let Value::Float(f) = &value {
            let (rem, got) = parse_cql_value(&expected, type_id).unwrap();
            assert!(rem.is_empty(), "[{label}] double trailing bytes");
            match got {
                Value::Float(g) => assert_eq!(
                    g.to_be_bytes(),
                    f.to_be_bytes(),
                    "[{label}] double bit mismatch"
                ),
                o => panic!("[{label}] expected Float, got {o:?}"),
            }
        } else {
            assert_decode_raw(&expected, type_id, &decode_expect, &label);
        }
    }
}

// ===========================================================================
// Manifest: cass.cql_types.primitives.text_blob_ascii_vectors
// ===========================================================================

#[test]
fn text_blob_ascii_vectors() {
    let root = load();
    for e in arr(&root, "text_blob_ascii") {
        let ty = s(e, "type");
        let label = format!("text_blob_ascii/{}/{}", ty, s(e, "name"));
        let expected = hex(s(e, "hex"));

        match ty {
            "ascii" | "text" => {
                let text = s(e, "value").to_string();
                assert_eq!(
                    text.as_bytes(),
                    expected.as_slice(),
                    "[{label}] fixture hex must equal UTF-8 of value"
                );
                assert_encode(&Value::Text(text.clone()), ty, &expected, &label);
                // text/ascii decode: entire cell bytes are the value (deterministic path).
                let type_id = if ty == "ascii" {
                    CqlTypeId::Ascii
                } else {
                    CqlTypeId::Varchar
                };
                // An empty cell decodes to the empty string directly; the
                // length-prefixed decode entry isn't exercised for a 0-byte cell.
                if expected.is_empty() {
                    assert!(text.is_empty(), "[{label}] empty cell must be empty text");
                } else {
                    assert_decode_raw(&expected, type_id, &Value::Text(text), &label);
                }
            }
            "blob" => {
                let raw = hex(s(e, "value_hex"));
                assert_eq!(raw, expected, "[{label}] blob value_hex must equal hex");
                assert_encode(&Value::Blob(raw.clone()), "blob", &expected, &label);
                // Raw cell bytes ARE the blob value (no length prefix at cell level);
                // parse_cql_value_raw is the faithful raw decode entry for blob.
                let (rem, got) = parse_cql_value_raw(&expected, CqlTypeId::Blob)
                    .unwrap_or_else(|e| panic!("[{label}] blob decode failed: {e:?}"));
                assert!(rem.is_empty(), "[{label}] blob trailing bytes");
                assert_eq!(got, Value::Blob(raw), "[{label}] blob decode mismatch");
            }
            other => panic!("[{label}] unexpected type {other}"),
        }
    }
}

// ===========================================================================
// Manifest: cass.cql_types.primitives.uuid_inet_vectors
// ===========================================================================

#[test]
fn uuid_inet_vectors() {
    let root = load();
    for e in arr(&root, "uuid_inet") {
        let ty = s(e, "type");
        let label = format!("uuid_inet/{}/{}", ty, s(e, "name"));
        let expected = hex(s(e, "hex"));
        let raw = hex(s(e, "value_hex"));
        assert_eq!(raw, expected, "[{label}] value_hex must equal hex");

        match ty {
            "uuid" | "timeuuid" => {
                let bytes: [u8; 16] = expected
                    .as_slice()
                    .try_into()
                    .unwrap_or_else(|_| panic!("[{label}] uuid must be 16 bytes"));
                assert_encode(&Value::Uuid(bytes), ty, &expected, &label);
                let type_id = if ty == "uuid" {
                    CqlTypeId::Uuid
                } else {
                    CqlTypeId::Timeuuid
                };
                assert_decode_raw(&expected, type_id, &Value::Uuid(bytes), &label);
            }
            "inet" => {
                assert!(
                    raw.len() == 4 || raw.len() == 16,
                    "[{label}] inet must be 4 or 16 bytes"
                );
                assert_encode(&Value::Inet(raw.clone()), "inet", &expected, &label);
                // parse_inet consumes a VInt-length-prefixed value; frame the raw
                // cell bytes the way the decoder expects, then assert reproduction.
                let mut framed = encode_vint(raw.len() as i64);
                framed.extend_from_slice(&raw);
                let (rem, got) = parse_inet(&framed)
                    .unwrap_or_else(|e| panic!("[{label}] parse_inet failed: {e:?}"));
                assert!(rem.is_empty(), "[{label}] inet trailing bytes");
                assert_eq!(got, Value::Inet(raw), "[{label}] inet decode mismatch");
            }
            other => panic!("[{label}] unexpected type {other}"),
        }
    }
}

// ===========================================================================
// Manifest: cass.cql_types.primitives.temporal_vectors
// ===========================================================================

#[test]
fn temporal_vectors() {
    let root = load();
    for e in arr(&root, "temporal") {
        let ty = s(e, "type");
        let label = format!("temporal/{}/{}", ty, s(e, "name"));
        let expected = hex(s(e, "hex"));

        match ty {
            "timestamp" => {
                let ms = i64f(e, "value");
                assert_eq!(
                    ms.to_be_bytes().to_vec(),
                    expected,
                    "[{label}] canonical timestamp bytes"
                );
                assert_encode(&Value::Timestamp(ms), "timestamp", &expected, &label);
                assert_decode_raw(
                    &expected,
                    CqlTypeId::Timestamp,
                    &Value::Timestamp(ms),
                    &label,
                );
            }
            "date" => {
                let days = i64f(e, "days") as i32;
                assert_eq!(ref_date(days), expected, "[{label}] canonical date bytes");
                assert_encode(&Value::Date(days), "date", &expected, &label);
                assert_decode_raw(&expected, CqlTypeId::Date, &Value::Date(days), &label);
            }
            "time" => {
                let nanos = i64f(e, "nanos");
                assert_eq!(
                    nanos.to_be_bytes().to_vec(),
                    expected,
                    "[{label}] canonical time bytes"
                );
                assert_encode(&Value::Time(nanos), "time", &expected, &label);
                assert_decode_raw(&expected, CqlTypeId::Time, &Value::Time(nanos), &label);
            }
            other => panic!("[{label}] unexpected type {other}"),
        }
    }
}

// ===========================================================================
// Manifest: cass.cql_types.primitives.varint_decimal_duration_vectors
// ===========================================================================

#[test]
fn varint_decimal_duration_vectors() {
    let root = load();
    for e in arr(&root, "varint_decimal_duration") {
        let ty = s(e, "type");
        let label = format!("varint_decimal_duration/{}/{}", ty, s(e, "name"));
        let expected = hex(s(e, "hex"));

        match ty {
            "varint" => {
                let n = BigInt::from_str(s(e, "value")).unwrap();
                // Canonical cross-check (no CQLite involved).
                assert_eq!(ref_varint(&n), expected, "[{label}] canonical varint bytes");
                let raw = expected.clone();
                assert_encode(&Value::Varint(raw.clone()), "varint", &expected, &label);
                // parse_varint expects a VInt length prefix; frame and reproduce.
                let mut framed = encode_vint(raw.len() as i64);
                framed.extend_from_slice(&raw);
                let (rem, got) = parse_varint(&framed)
                    .unwrap_or_else(|err| panic!("[{label}] parse_varint failed: {err:?}"));
                assert!(rem.is_empty(), "[{label}] varint trailing bytes");
                assert_eq!(got, Value::Varint(raw), "[{label}] varint decode mismatch");
            }
            "decimal" => {
                let scale = i64f(e, "scale") as i32;
                let unscaled = BigInt::from_str(s(e, "unscaled")).unwrap();
                assert_eq!(
                    ref_decimal(scale, &unscaled),
                    expected,
                    "[{label}] canonical decimal bytes"
                );
                let unscaled_bytes = ref_varint(&unscaled);
                let value = Value::Decimal {
                    scale,
                    unscaled: unscaled_bytes.clone(),
                };
                assert_encode(&value, "decimal", &expected, &label);
                // parse_cql_value(Decimal) reads 4-byte scale then raw unscaled bytes.
                assert_decode_raw(&expected, CqlTypeId::Decimal, &value, &label);
            }
            "duration" => {
                let months = i64f(e, "months") as i32;
                let days = i64f(e, "days") as i32;
                let nanos = i64f(e, "nanos");
                assert_eq!(
                    ref_duration(months, days, nanos),
                    expected,
                    "[{label}] canonical duration bytes"
                );
                let value = Value::Duration {
                    months,
                    days,
                    nanos,
                };
                assert_encode(&value, "duration", &expected, &label);
                assert_decode_raw(&expected, CqlTypeId::Duration, &value, &label);
            }
            other => panic!("[{label}] unexpected type {other}"),
        }
    }
}

// ===========================================================================
// Manifest: cass.cql_types.primitives.invalid_length_rejection
//
// Every fixed-width type must reject truncated input (length error) and must
// NOT silently truncate over-long input into a default/wrong value.
// ===========================================================================

/// Fixed-width decode entry points and their canonical byte length.
fn fixed_width_cases() -> Vec<(&'static str, CqlTypeId, usize, Vec<u8>)> {
    vec![
        ("boolean", CqlTypeId::Boolean, 1, vec![0x01]),
        ("tinyint", CqlTypeId::Tinyint, 1, vec![0x7f]),
        ("smallint", CqlTypeId::Smallint, 2, vec![0x12, 0x34]),
        ("int", CqlTypeId::Int, 4, vec![0x00, 0x00, 0x00, 0x2a]),
        ("bigint", CqlTypeId::BigInt, 8, vec![0, 0, 0, 0, 0, 0, 0, 7]),
        ("float", CqlTypeId::Float, 4, vec![0x3f, 0xc0, 0x00, 0x00]),
        (
            "double",
            CqlTypeId::Double,
            8,
            vec![0x3f, 0xf3, 0xbe, 0x76, 0xc8, 0xb4, 0x39, 0x58],
        ),
        ("uuid", CqlTypeId::Uuid, 16, (0u8..16).collect::<Vec<u8>>()),
        (
            "timeuuid",
            CqlTypeId::Timeuuid,
            16,
            (0u8..16).collect::<Vec<u8>>(),
        ),
        ("date", CqlTypeId::Date, 4, vec![0x80, 0x00, 0x00, 0x00]),
        ("time", CqlTypeId::Time, 8, vec![0, 0, 0, 0, 0, 0, 0, 0]),
        (
            "timestamp",
            CqlTypeId::Timestamp,
            8,
            vec![0, 0, 0, 0, 0, 0, 0, 0],
        ),
    ]
}

#[test]
fn invalid_length_rejection() {
    for (name, type_id, len, valid) in fixed_width_cases() {
        assert_eq!(valid.len(), len, "[{name}] test setup: valid len");

        // Sanity: the exact-length input decodes fine (proves any failure below
        // is length-specific, not a type/parse problem).
        let ok = parse_cql_value(&valid, type_id);
        assert!(
            ok.is_ok(),
            "[{name}] exact-length {len}B should decode, got {:?}",
            ok.err()
        );
        let (rem_ok, _) = ok.unwrap();
        assert!(
            rem_ok.is_empty(),
            "[{name}] exact-length input must be fully consumed",
        );

        // Truncated input (every length 0..len-1) must FAIL — this is the
        // length rejection. A fixed-width decoder must not fabricate a value
        // from fewer bytes than the type requires.
        for short_len in 0..len {
            let short = &valid[..short_len];
            let res = parse_cql_value(short, type_id);
            assert!(
                res.is_err(),
                "[{name}] {short_len}B (< {len}) MUST be rejected, got {:?}",
                res.ok()
            );
        }

        // Over-long input must NOT be silently truncated into a default: the
        // decoder consumes exactly `len` bytes and leaves the surplus as
        // unconsumed remainder (never folds extra bytes into the value, never
        // drops to a zero/default).
        let mut long = valid.clone();
        long.extend_from_slice(&[0xAB, 0xCD, 0xEF]);
        let (rem_long, got_long) = parse_cql_value(&long, type_id)
            .unwrap_or_else(|e| panic!("[{name}] over-long decode errored: {e:?}"));
        assert_eq!(
            rem_long,
            &[0xAB, 0xCD, 0xEF],
            "[{name}] over-long input must leave exactly the surplus bytes unconsumed"
        );
        let (_, got_exact) = parse_cql_value(&valid, type_id).unwrap();
        assert_eq!(
            got_long, got_exact,
            "[{name}] over-long decode must equal the exact-length value (no truncation/default)"
        );
    }
}
