//! Tests for the bounded raw-value decoder (`raw_value.rs` and its fixed-width
//! arms). Split out of `raw_value.rs` under the campsite rule (epic #1116 /
//! issue #3723).

#[allow(unused_imports)]
use super::test_support::helpers::*;
#[allow(unused_imports)]
use super::*;

#[test]
fn test_parse_value_from_raw_bytes_primitives() {
    let parser = V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

    // int
    let data = 42i32.to_be_bytes();
    let val = parser
        .parse_value_from_raw_bytes(&data, "int", "col", 0)
        .unwrap();
    assert_eq!(val, Value::Integer(42));

    // bigint
    let data = 123456789i64.to_be_bytes();
    let val = parser
        .parse_value_from_raw_bytes(&data, "bigint", "col", 0)
        .unwrap();
    assert_eq!(val, Value::BigInt(123456789));

    // text
    let data = b"hello";
    let val = parser
        .parse_value_from_raw_bytes(data, "text", "col", 0)
        .unwrap();
    assert_eq!(val, Value::text("hello".to_string()));

    // boolean true
    let val = parser
        .parse_value_from_raw_bytes(&[1], "boolean", "col", 0)
        .unwrap();
    assert_eq!(val, Value::Boolean(true));

    // boolean false
    let val = parser
        .parse_value_from_raw_bytes(&[0], "boolean", "col", 0)
        .unwrap();
    assert_eq!(val, Value::Boolean(false));

    // float (parse_value_from_raw_bytes promotes f32 to f64 via Float)
    let data = 1.5f32.to_be_bytes();
    let val = parser
        .parse_value_from_raw_bytes(&data, "float", "col", 0)
        .unwrap();
    match val {
        Value::Float(f) => assert!((f - 1.5).abs() < 0.001),
        other => panic!("Expected Float, got {:?}", other),
    }

    // double
    let data = 9.876f64.to_be_bytes();
    let val = parser
        .parse_value_from_raw_bytes(&data, "double", "col", 0)
        .unwrap();
    assert_eq!(val, Value::Float(9.876));

    // uuid (16 bytes)
    let uuid_bytes: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let val = parser
        .parse_value_from_raw_bytes(&uuid_bytes, "uuid", "col", 0)
        .unwrap();
    assert_eq!(val, Value::Uuid(uuid_bytes));

    // smallint
    let data = 1234i16.to_be_bytes();
    let val = parser
        .parse_value_from_raw_bytes(&data, "smallint", "col", 0)
        .unwrap();
    assert_eq!(val, Value::SmallInt(1234));

    // tinyint
    let val = parser
        .parse_value_from_raw_bytes(&[42], "tinyint", "col", 0)
        .unwrap();
    assert_eq!(val, Value::TinyInt(42));

    // blob
    let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let val = parser
        .parse_value_from_raw_bytes(&data, "blob", "col", 0)
        .unwrap();
    assert_eq!(val, Value::Blob(data.into()));

    // varint
    let data = vec![0x01, 0x00];
    let val = parser
        .parse_value_from_raw_bytes(&data, "varint", "col", 0)
        .unwrap();
    assert_eq!(val, Value::varint(vec![0x01, 0x00]));

    // inet (IPv4)
    let data = vec![127, 0, 0, 1];
    let val = parser
        .parse_value_from_raw_bytes(&data, "inet", "col", 0)
        .unwrap();
    assert_eq!(val, Value::inet(vec![127, 0, 0, 1]));

    // timestamp
    let data = 1704067200000i64.to_be_bytes();
    let val = parser
        .parse_value_from_raw_bytes(&data, "timestamp", "col", 0)
        .unwrap();
    assert_eq!(val, Value::Timestamp(1704067200000));

    // decimal
    let mut data = Vec::new();
    data.extend_from_slice(&2i32.to_be_bytes()); // scale
    data.extend_from_slice(&[0x01, 0xC8]); // unscaled = 456
    let val = parser
        .parse_value_from_raw_bytes(&data, "decimal", "col", 0)
        .unwrap();
    assert_eq!(
        val,
        Value::Decimal {
            scale: 2,
            unscaled: vec![0x01, 0xC8]
        }
    );
}

/// Issue #1081: a multicell-UDT field declared `duration` resolves its
/// field type from the authoritative on-disk `UserType(...)` marshal string
/// as `org.apache.cassandra.db.marshal.DurationType`. That bare scalar
/// marshal form must normalize to `"duration"` and decode the three
/// consecutive SIGNED VInts (months, days, nanos) that constitute the value
/// — NOT fall through to `Value::Blob`. In `parse_value_from_raw_bytes` the
/// entire slice IS the value (no outer `[VInt len]` prefix).
#[test]
fn test_parse_value_from_raw_bytes_duration_marshal() {
    let parser = V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

    // Build three SIGNED VInts (months=1, days=2, nanos=3) using the same
    // ZigZag-over-unsigned-VInt scheme `parse_vint` decodes. ZigZag:
    // 1 -> 2, 2 -> 4, 3 -> 6.
    let zigzag = |v: i64| ((v << 1) ^ (v >> 63)) as u64;
    let mut data = Vec::new();
    encode_unsigned(zigzag(1), &mut data); // months
    encode_unsigned(zigzag(2), &mut data); // days
    encode_unsigned(zigzag(3), &mut data); // nanos

    // Round-trip via the signed VInt parser to confirm the encoding before
    // exercising the decode arm under test.
    let (rem, m) = parse_vint(&data).unwrap();
    let consumed = data.len() - rem.len();
    let (rem2, d) = parse_vint(&data[consumed..]).unwrap();
    let consumed2 = data.len() - rem2.len();
    let (_rem3, n) = parse_vint(&data[consumed2..]).unwrap();
    assert_eq!(
        (m, d, n),
        (1, 2, 3),
        "hand-encoded duration vints round-trip"
    );

    // The bare scalar marshal form must normalize and decode to Duration,
    // NOT fall through to Blob.
    let val = parser
        .parse_value_from_raw_bytes(
            &data,
            "org.apache.cassandra.db.marshal.DurationType",
            "col",
            0,
        )
        .unwrap();
    assert_eq!(
        val,
        Value::Duration {
            months: 1,
            days: 2,
            nanos: 3
        }
    );

    // The canonical CQL short form must decode identically.
    let val_short = parser
        .parse_value_from_raw_bytes(&data, "duration", "col", 0)
        .unwrap();
    assert_eq!(val_short, val);
}

/// Issue #1632 (item b): the raw-value frozen `duration` arm must REJECT a
/// months/days VInt outside the i32 range instead of silently wrapping via
/// `as i32`. On pre-fix code `months as i32` wraps `i32::MAX + 1` to a
/// negative value and returns Ok; the guard turns it into an error.
#[test]
fn test_parse_value_from_raw_bytes_duration_months_out_of_i32_range_errors() {
    let parser = V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);
    let zigzag = |v: i64| ((v << 1) ^ (v >> 63)) as u64;

    // months = i32::MAX + 1 (overflows i32), days = 0, nanos = 0.
    let mut over = Vec::new();
    encode_unsigned(zigzag(i32::MAX as i64 + 1), &mut over);
    encode_unsigned(zigzag(0), &mut over);
    encode_unsigned(zigzag(0), &mut over);
    assert!(
        parser
            .parse_value_from_raw_bytes(&over, "duration", "col", 0)
            .is_err(),
        "months > i32::MAX must error, not wrap via `as i32`"
    );

    // days = i32::MIN - 1 (underflows i32), months = 0, nanos = 0.
    let mut under = Vec::new();
    encode_unsigned(zigzag(0), &mut under);
    encode_unsigned(zigzag(i32::MIN as i64 - 1), &mut under);
    encode_unsigned(zigzag(0), &mut under);
    assert!(
        parser
            .parse_value_from_raw_bytes(&under, "duration", "col", 0)
            .is_err(),
        "days < i32::MIN must error, not wrap via `as i32`"
    );
}

#[test]
fn test_parse_value_from_raw_bytes_nested_list() {
    let parser = V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

    let data = build_frozen_list_int(&[10, 20, 30]);
    let val = parser
        .parse_value_from_raw_bytes(&data, "list<int>", "col", 0)
        .unwrap();
    assert_eq!(
        val,
        Value::List(vec![
            Value::Integer(10),
            Value::Integer(20),
            Value::Integer(30)
        ])
    );
}

#[test]
fn test_parse_value_from_raw_bytes_nested_set() {
    let parser = V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

    let data = build_frozen_list_int(&[5, 15]);
    let val = parser
        .parse_value_from_raw_bytes(&data, "set<int>", "col", 0)
        .unwrap();
    assert_eq!(val, Value::Set(vec![Value::Integer(5), Value::Integer(15)]));
}

#[test]
fn test_parse_value_from_raw_bytes_nested_map() {
    let parser = V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

    let data = build_frozen_map_text_int(&[("alice", 1), ("bob", 2)]);
    let val = parser
        .parse_value_from_raw_bytes(&data, "map<text,int>", "col", 0)
        .unwrap();
    assert_eq!(
        val,
        Value::Map(vec![
            (Value::text("alice".to_string()), Value::Integer(1)),
            (Value::text("bob".to_string()), Value::Integer(2)),
        ])
    );
}

#[test]
fn test_parse_value_from_raw_bytes_frozen_wrapper() {
    let parser = V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

    let data = build_frozen_list_int(&[100, 200]);
    let val = parser
        .parse_value_from_raw_bytes(&data, "frozen<list<int>>", "col", 0)
        .unwrap();
    assert_eq!(
        val,
        Value::Frozen(Box::new(Value::List(vec![
            Value::Integer(100),
            Value::Integer(200)
        ])))
    );
}

/// Issue #1081 (FINDING 1): a multicell-UDT field whose declared type arrives
/// in Cassandra MARSHAL form for a COLLECTION — here
/// `ListType(UTF8Type)` — must decode to a real `Value::List`, NOT fall
/// through to the blob default. This also exercises the case-sensitivity
/// path: the lowercased match binding must NOT corrupt the original-case
/// nested element marshal type (`UTF8Type`), which would otherwise fail to
/// re-normalize and blob.
#[test]
fn test_parse_value_from_raw_bytes_marshal_list_utf8() {
    let parser = V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

    let data = build_frozen_list_text(&["alpha", "beta"]);
    let val = parser
        .parse_value_from_raw_bytes(
            &data,
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)",
            "udt_field",
            0,
        )
        .expect("marshal ListType(UTF8Type) must decode, not blob");
    assert_eq!(
        val,
        Value::List(vec![
            Value::text("alpha".to_string()),
            Value::text("beta".to_string()),
        ]),
        "marshal-form list field must produce a List of Text (not a Blob)"
    );
}

/// Issue #1081 (FINDING 1): a multicell-UDT field declared as a marshal-form
/// `MapType(UTF8Type, Int32Type)` must decode to a `Value::Map`. The
/// Int32Type VALUE proves the nested element marshal type keeps its original
/// case through the lowercased match arm (a lowercased `...int32type` would
/// fail the case-sensitive primitive normalizer and blob).
#[test]
fn test_parse_value_from_raw_bytes_marshal_map_utf8_int32() {
    let parser = V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

    // Same [count][i32 key_len][key][i32 val_len][i32 value] framing the
    // frozen map raw parser expects; the int value is a 4-byte i32.
    let data = build_frozen_map_text_int(&[("alice", 1), ("bob", 2)]);
    let val = parser
        .parse_value_from_raw_bytes(
            &data,
            "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)",
            "udt_field",
            0,
        )
        .expect("marshal MapType(UTF8Type, Int32Type) must decode, not blob");
    assert_eq!(
        val,
        Value::Map(vec![
            (Value::text("alice".to_string()), Value::Integer(1)),
            (Value::text("bob".to_string()), Value::Integer(2)),
        ]),
        "marshal-form map field must produce a Map of (Text, Integer) (not a Blob)"
    );
}

/// Issue #1081 (FINDING 1): a marshal-form `SetType(Int32Type)` field must
/// decode to a `Value::Set` rather than a blob.
#[test]
fn test_parse_value_from_raw_bytes_marshal_set_int32() {
    let parser = V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

    let data = build_frozen_list_int(&[7, 9]);
    let val = parser
        .parse_value_from_raw_bytes(
            &data,
            "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type)",
            "udt_field",
            0,
        )
        .expect("marshal SetType(Int32Type) must decode, not blob");
    assert_eq!(
        val,
        Value::Set(vec![Value::Integer(7), Value::Integer(9)]),
        "marshal-form set field must produce a Set of Integer (not a Blob)"
    );
}

/// Issue #1081 (FINDING — last type-tree gap): a multicell-UDT field declared
/// `frozen<list<text>>` resolves from the on-disk `UserType(...)` marshal string
/// as `FrozenType(ListType(UTF8Type))`. Before the fix the frozen arm in
/// `parse_value_from_raw_bytes` only matched the CQL `frozen<...>` form, so this
/// marshal form bypassed it and fell through to `Value::Blob`. It must now decode
/// to `Value::Frozen(List([Text, ...]))`. (Collection/UDT fields inside a UDT must
/// be frozen, so this marshal form is the common real case.)
#[test]
fn test_parse_value_from_raw_bytes_marshal_frozen_list_utf8() {
    let parser = V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

    let data = build_frozen_list_text(&["gamma", "delta"]);
    let val = parser
        .parse_value_from_raw_bytes(
            &data,
            "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type))",
            "udt_field",
            0,
        )
        .expect("marshal FrozenType(ListType(UTF8Type)) must decode, not blob");
    assert_eq!(
        val,
        Value::Frozen(Box::new(Value::List(vec![
            Value::text("gamma".to_string()),
            Value::text("delta".to_string()),
        ]))),
        "marshal-form frozen-list field must produce Frozen(List(Text)) (not a Blob)"
    );
}

#[test]
fn test_parse_raw_type_value_depth_guard() {
    let parser = V5CompressedLegacyParser::new("test".to_string(), "table".to_string(), 0, 0, None);

    // Directly calling with depth at limit should fail
    let data = 42i32.to_be_bytes();
    let result = parser.parse_raw_type_value(&data, 0, "int", "col", MAX_TYPE_NESTING_DEPTH + 1);
    assert!(result.is_err());
}
