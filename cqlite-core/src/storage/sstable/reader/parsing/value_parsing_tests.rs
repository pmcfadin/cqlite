use super::*;
use crate::parser::vint::encode_vint;

// ============================================================================
// Primitive Type Tests
// ============================================================================

#[test]
fn test_parse_boolean_true() {
    let data = [0x01];
    let result = parse_boolean_value(&data).unwrap();
    assert_eq!(result, Value::Boolean(true));
}

#[test]
fn test_parse_boolean_false() {
    let data = [0x00];
    let result = parse_boolean_value(&data).unwrap();
    assert_eq!(result, Value::Boolean(false));
}

#[test]
fn test_parse_tinyint() {
    let data = [0x2A]; // 42
    let result = parse_tinyint_value(&data).unwrap();
    assert_eq!(result, Value::TinyInt(42));
}

#[test]
fn test_parse_tinyint_negative() {
    let data = [0xD6]; // -42 in two's complement
    let result = parse_tinyint_value(&data).unwrap();
    assert_eq!(result, Value::TinyInt(-42));
}

#[test]
fn test_parse_smallint() {
    let data = [0x00, 0x2A]; // 42 big-endian
    let result = parse_smallint_value(&data).unwrap();
    assert_eq!(result, Value::SmallInt(42));
}

#[test]
fn test_parse_smallint_negative() {
    let data = [0xFF, 0xD6]; // -42 big-endian
    let result = parse_smallint_value(&data).unwrap();
    assert_eq!(result, Value::SmallInt(-42));
}

#[test]
fn test_parse_int() {
    let data = 42i32.to_be_bytes();
    let result = parse_int_value(&data).unwrap();
    assert_eq!(result, Value::Integer(42));
}

#[test]
fn test_parse_int_negative() {
    let data = (-2i32).to_be_bytes();
    let result = parse_int_value(&data).unwrap();
    assert_eq!(result, Value::Integer(-2));
}

#[test]
fn test_parse_bigint() {
    let data = 42i64.to_be_bytes();
    let result = parse_bigint_value(&data).unwrap();
    assert_eq!(result, Value::BigInt(42));
}

#[test]
fn test_parse_bigint_negative() {
    let data = (-9223372036854775807i64).to_be_bytes();
    let result = parse_bigint_value(&data).unwrap();
    assert_eq!(result, Value::BigInt(-9223372036854775807));
}

#[test]
fn test_parse_counter() {
    // Test with actual counter value from test data (Issue #272)
    let test_value: i64 = 422216548022666;
    let data = test_value.to_be_bytes();
    let result = parse_counter_value(&data).unwrap();
    assert_eq!(result, Value::Counter(test_value));
}

#[test]
fn test_parse_counter_negative() {
    let data = (-1234567890i64).to_be_bytes();
    let result = parse_counter_value(&data).unwrap();
    assert_eq!(result, Value::Counter(-1234567890));
}

#[test]
fn test_parse_counter_wrong_length() {
    let data = [0x00, 0x00, 0x00, 0x00]; // Only 4 bytes instead of 8
    let result = parse_counter_value(&data);
    assert!(result.is_err(), "Counter should reject 4-byte input");
}

#[test]
fn test_parse_text() {
    let data = b"hello";
    let result = parse_text_value(data).unwrap();
    assert_eq!(result, Value::Text("hello".to_string()));
}

#[test]
fn test_parse_text_unicode() {
    let data = "Hello 世界".as_bytes();
    let result = parse_text_value(data).unwrap();
    assert_eq!(result, Value::Text("Hello 世界".to_string()));
}

#[test]
fn test_parse_blob() {
    let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let result = parse_blob_value(&data).unwrap();
    assert_eq!(result, Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
}

#[test]
fn test_parse_uuid() {
    let uuid_bytes = [
        0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD,
        0xEF,
    ];
    let result = parse_uuid_value(&uuid_bytes).unwrap();
    assert_eq!(result, Value::Uuid(uuid_bytes));
}

#[test]
fn test_parse_date_epoch() {
    // Cassandra DATE for epoch (1970-01-01) is stored as i32::MIN (0x80000000)
    // which represents 0 days since epoch
    let stored = (0u32).wrapping_sub(i32::MIN as u32);
    let data = stored.to_be_bytes();
    let result = parse_date_value(&data).unwrap();
    assert_eq!(result, Value::Date(0));
}

#[test]
fn test_parse_date_positive() {
    // 100 days after epoch
    let days = 100i32;
    let stored = (days as u32).wrapping_sub(i32::MIN as u32);
    let data = stored.to_be_bytes();
    let result = parse_date_value(&data).unwrap();
    assert_eq!(result, Value::Date(100));
}

#[test]
fn test_parse_date_negative() {
    // 100 days before epoch
    let days = -100i32;
    let stored = (days as u32).wrapping_sub(i32::MIN as u32);
    let data = stored.to_be_bytes();
    let result = parse_date_value(&data).unwrap();
    assert_eq!(result, Value::Date(-100));
}

// ============================================================================
// Collection Tests
// ============================================================================

#[test]
fn test_parse_list_empty() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(0)); // 0 elements

    let result =
        parse_list_value_with(&data, &ComparatorType::Int, |d, _| parse_int_value(d)).unwrap();

    if let Value::List(elements) = result {
        assert_eq!(elements.len(), 0);
    } else {
        panic!("Expected List value");
    }
}

#[test]
fn test_parse_list_single_int() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(1)); // 1 element
    data.extend_from_slice(&encode_vint(4)); // element length
    data.extend_from_slice(&42i32.to_be_bytes());

    let result =
        parse_list_value_with(&data, &ComparatorType::Int, |d, _| parse_int_value(d)).unwrap();

    if let Value::List(elements) = result {
        assert_eq!(elements.len(), 1);
        assert_eq!(elements[0], Value::Integer(42));
    } else {
        panic!("Expected List value");
    }
}

#[test]
fn test_parse_list_multiple_ints() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(3)); // 3 elements
    for val in [1i32, 2, 3] {
        data.extend_from_slice(&encode_vint(4)); // each int is 4 bytes
        data.extend_from_slice(&val.to_be_bytes());
    }

    let result =
        parse_list_value_with(&data, &ComparatorType::Int, |d, _| parse_int_value(d)).unwrap();

    if let Value::List(elements) = result {
        assert_eq!(elements.len(), 3);
        assert_eq!(elements[0], Value::Integer(1));
        assert_eq!(elements[1], Value::Integer(2));
        assert_eq!(elements[2], Value::Integer(3));
    } else {
        panic!("Expected List value");
    }
}

#[test]
fn test_parse_list_text_elements() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(2)); // 2 elements

    let str1 = "hello";
    data.extend_from_slice(&encode_vint(str1.len() as i64));
    data.extend_from_slice(str1.as_bytes());

    let str2 = "world";
    data.extend_from_slice(&encode_vint(str2.len() as i64));
    data.extend_from_slice(str2.as_bytes());

    let result =
        parse_list_value_with(&data, &ComparatorType::Text, |d, _| parse_text_value(d)).unwrap();

    if let Value::List(elements) = result {
        assert_eq!(elements.len(), 2);
        assert_eq!(elements[0], Value::Text("hello".to_string()));
        assert_eq!(elements[1], Value::Text("world".to_string()));
    } else {
        panic!("Expected List value");
    }
}

#[test]
fn test_parse_set_int() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(2)); // 2 elements
    for val in [10i32, 20] {
        data.extend_from_slice(&encode_vint(4));
        data.extend_from_slice(&val.to_be_bytes());
    }

    let result =
        parse_set_value_with(&data, &ComparatorType::Int, |d, _| parse_int_value(d)).unwrap();

    if let Value::Set(elements) = result {
        assert_eq!(elements.len(), 2);
        assert_eq!(elements[0], Value::Integer(10));
        assert_eq!(elements[1], Value::Integer(20));
    } else {
        panic!("Expected Set value");
    }
}

#[test]
fn test_parse_map_text_int() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(2)); // 2 entries

    // Entry 1: "key1" -> 100
    let key1 = "key1";
    data.extend_from_slice(&encode_vint(key1.len() as i64));
    data.extend_from_slice(key1.as_bytes());
    data.extend_from_slice(&encode_vint(4));
    data.extend_from_slice(&100i32.to_be_bytes());

    // Entry 2: "key2" -> 200
    let key2 = "key2";
    data.extend_from_slice(&encode_vint(key2.len() as i64));
    data.extend_from_slice(key2.as_bytes());
    data.extend_from_slice(&encode_vint(4));
    data.extend_from_slice(&200i32.to_be_bytes());

    let result = parse_map_value_with(
        &data,
        &ComparatorType::Text,
        &ComparatorType::Int,
        |d, comp| match comp {
            ComparatorType::Text => parse_text_value(d),
            ComparatorType::Int => parse_int_value(d),
            _ => panic!("Unexpected comparator"),
        },
    )
    .unwrap();

    if let Value::Map(entries) = result {
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].0, Value::Text("key1".to_string()));
        assert_eq!(entries[0].1, Value::Integer(100));
        assert_eq!(entries[1].0, Value::Text("key2".to_string()));
        assert_eq!(entries[1].1, Value::Integer(200));
    } else {
        panic!("Expected Map value");
    }
}

#[test]
fn test_parse_map_empty() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(0)); // 0 entries

    let result = parse_map_value_with(
        &data,
        &ComparatorType::Text,
        &ComparatorType::Int,
        |d, comp| match comp {
            ComparatorType::Text => parse_text_value(d),
            ComparatorType::Int => parse_int_value(d),
            _ => panic!("Unexpected comparator"),
        },
    )
    .unwrap();

    if let Value::Map(entries) = result {
        assert_eq!(entries.len(), 0);
    } else {
        panic!("Expected Map value");
    }
}

#[test]
fn test_parse_map_nested() {
    // Map<text, list<int>>
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(1)); // 1 entry

    // Key: "numbers"
    let key = "numbers";
    data.extend_from_slice(&encode_vint(key.len() as i64));
    data.extend_from_slice(key.as_bytes());

    // Value: list<int> with [1, 2, 3]
    let mut list_data = Vec::new();
    list_data.extend_from_slice(&encode_vint(3)); // 3 elements
    for val in [1i32, 2, 3] {
        list_data.extend_from_slice(&encode_vint(4));
        list_data.extend_from_slice(&val.to_be_bytes());
    }
    data.extend_from_slice(&encode_vint(list_data.len() as i64));
    data.extend_from_slice(&list_data);

    let result = parse_map_value_with(
        &data,
        &ComparatorType::Text,
        &ComparatorType::List(Box::new(ComparatorType::Int)),
        |d, comp| match comp {
            ComparatorType::Text => parse_text_value(d),
            ComparatorType::List(inner) => {
                parse_list_value_with(d, inner, |inner_d, inner_comp| match inner_comp {
                    ComparatorType::Int => parse_int_value(inner_d),
                    _ => panic!("Unexpected inner comparator"),
                })
            }
            _ => panic!("Unexpected comparator"),
        },
    )
    .unwrap();

    if let Value::Map(entries) = result {
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, Value::Text("numbers".to_string()));
        if let Value::List(list_elements) = &entries[0].1 {
            assert_eq!(list_elements.len(), 3);
            assert_eq!(list_elements[0], Value::Integer(1));
            assert_eq!(list_elements[1], Value::Integer(2));
            assert_eq!(list_elements[2], Value::Integer(3));
        } else {
            panic!("Expected List value in map");
        }
    } else {
        panic!("Expected Map value");
    }
}

// ============================================================================
// Complex Type Tests
// ============================================================================

#[test]
fn test_parse_tuple_int_text() {
    // Cassandra tuple field lengths use 4-byte big-endian signed int32 (not VInt).
    let mut data = Vec::new();

    // Field 0: int = 42 (4 bytes)
    data.extend_from_slice(&4i32.to_be_bytes()); // 4-byte length prefix
    data.extend_from_slice(&42i32.to_be_bytes());

    // Field 1: text = "hello" (5 bytes)
    let text = "hello";
    data.extend_from_slice(&(text.len() as i32).to_be_bytes()); // 4-byte length prefix
    data.extend_from_slice(text.as_bytes());

    let field_comparators = vec![ComparatorType::Int, ComparatorType::Text];

    let result = parse_tuple_value_with(&data, &field_comparators, |d, comp| match comp {
        ComparatorType::Int => parse_int_value(d),
        ComparatorType::Text => parse_text_value(d),
        _ => panic!("Unexpected comparator"),
    })
    .unwrap();

    if let Value::Tuple(fields) = result {
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], Value::Integer(42));
        assert_eq!(fields[1], Value::Text("hello".to_string()));
    } else {
        panic!("Expected Tuple value");
    }
}

#[test]
fn test_parse_tuple_with_null() {
    // Cassandra uses 4-byte big-endian signed int32 for field lengths.
    // -1 means null field.
    let mut data = Vec::new();

    // Field 0: int = 42 (4 bytes)
    data.extend_from_slice(&4i32.to_be_bytes()); // length = 4
    data.extend_from_slice(&42i32.to_be_bytes());

    // Field 1: null text (-1 sentinel)
    data.extend_from_slice(&(-1i32).to_be_bytes()); // -1 = null

    let field_comparators = vec![ComparatorType::Int, ComparatorType::Text];

    let result = parse_tuple_value_with(&data, &field_comparators, |d, comp| match comp {
        ComparatorType::Int => parse_int_value(d),
        ComparatorType::Text => parse_text_value(d),
        _ => panic!("Unexpected comparator"),
    })
    .unwrap();

    if let Value::Tuple(fields) = result {
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], Value::Integer(42));
        assert_eq!(
            fields[1],
            Value::Null,
            "null field should decode to Value::Null"
        );
    } else {
        panic!("Expected Tuple value");
    }
}

#[test]
fn test_parse_udt_simple() {
    // Cassandra UDT field lengths use 4-byte big-endian signed int32 (not VInt).
    let mut data = Vec::new();

    // Field "id": int = 123 (4 bytes)
    data.extend_from_slice(&4i32.to_be_bytes()); // 4-byte length prefix
    data.extend_from_slice(&123i32.to_be_bytes());

    // Field "name": text = "Alice" (5 bytes)
    let name = "Alice";
    data.extend_from_slice(&(name.len() as i32).to_be_bytes()); // 4-byte length prefix
    data.extend_from_slice(name.as_bytes());

    let field_comparators = vec![
        ("id".to_string(), ComparatorType::Int),
        ("name".to_string(), ComparatorType::Text),
    ];

    let result = parse_udt_value_with(&data, &field_comparators, |d, comp| match comp {
        ComparatorType::Int => parse_int_value(d),
        ComparatorType::Text => parse_text_value(d),
        _ => panic!("Unexpected comparator"),
    })
    .unwrap();

    assert_eq!(result.fields.len(), 2);
    assert_eq!(result.fields[0].name, "id");
    assert_eq!(result.fields[0].value, Some(Value::Integer(123)));
    assert_eq!(result.fields[1].name, "name");
    assert_eq!(
        result.fields[1].value,
        Some(Value::Text("Alice".to_string()))
    );
}

#[test]
fn test_parse_udt_with_collection() {
    // Cassandra UDT field lengths use 4-byte big-endian signed int32 (not VInt).
    // Note: the list *elements* still use VInt lengths (CollectionSerializer format).
    let mut data = Vec::new();

    // Field "id": int = 456 (4 bytes)
    data.extend_from_slice(&4i32.to_be_bytes()); // 4-byte UDT field length prefix
    data.extend_from_slice(&456i32.to_be_bytes());

    // Field "tags": list<text> = ["tag1", "tag2"]
    let mut list_data = Vec::new();
    list_data.extend_from_slice(&encode_vint(2)); // 2 elements (VInt count per CollectionSerializer)
    for tag in ["tag1", "tag2"] {
        list_data.extend_from_slice(&encode_vint(tag.len() as i64)); // VInt element length
        list_data.extend_from_slice(tag.as_bytes());
    }
    // UDT field length prefix = 4-byte BE i32
    data.extend_from_slice(&(list_data.len() as i32).to_be_bytes());
    data.extend_from_slice(&list_data);

    let field_comparators = vec![
        ("id".to_string(), ComparatorType::Int),
        (
            "tags".to_string(),
            ComparatorType::List(Box::new(ComparatorType::Text)),
        ),
    ];

    let result = parse_udt_value_with(&data, &field_comparators, |d, comp| match comp {
        ComparatorType::Int => parse_int_value(d),
        ComparatorType::List(inner) => {
            parse_list_value_with(d, inner, |inner_d, inner_comp| match inner_comp {
                ComparatorType::Text => parse_text_value(inner_d),
                _ => panic!("Unexpected inner comparator"),
            })
        }
        _ => panic!("Unexpected comparator"),
    })
    .unwrap();

    assert_eq!(result.fields.len(), 2);
    assert_eq!(result.fields[0].name, "id");
    assert_eq!(result.fields[0].value, Some(Value::Integer(456)));
    assert_eq!(result.fields[1].name, "tags");
    if let Some(Value::List(tags)) = &result.fields[1].value {
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0], Value::Text("tag1".to_string()));
        assert_eq!(tags[1], Value::Text("tag2".to_string()));
    } else {
        panic!("Expected List value in UDT");
    }
}

// ============================================================================
// Error Cases
// ============================================================================

#[test]
fn test_parse_int_wrong_length() {
    let data = [0x00, 0x00, 0x2A]; // Only 3 bytes instead of 4
    let result = parse_int_value(&data);
    assert!(result.is_err());
}

#[test]
fn test_parse_text_invalid_utf8() {
    let data = [0xFF, 0xFE, 0xFD]; // Invalid UTF-8 sequence
    let result = parse_text_value(&data);
    assert!(result.is_err());
}

#[test]
fn test_parse_uuid_wrong_length() {
    let data = [
        0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56, 0x78, 0x90, 0xAB, 0xCD,
    ]; // Only 15 bytes
    let result = parse_uuid_value(&data);
    assert!(result.is_err());
}

#[test]
fn test_parse_list_truncated() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vint(2)); // Claims 2 elements
    data.extend_from_slice(&encode_vint(4)); // First element length
    data.extend_from_slice(&42i32.to_be_bytes());
    // Missing second element

    let result =
        parse_list_value_with(&data, &ComparatorType::Int, |d, _| parse_int_value(d)).unwrap();

    // Should successfully parse 1 element (stops when no more data)
    if let Value::List(elements) = result {
        assert_eq!(elements.len(), 1);
    } else {
        panic!("Expected List value");
    }
}

#[test]
fn test_parse_boolean_wrong_length() {
    let data = [0x01, 0x02]; // 2 bytes instead of 1
    let result = parse_boolean_value(&data);
    assert!(result.is_err());
}

#[test]
fn test_parse_date_wrong_length() {
    let data = [0x00, 0x00, 0x00]; // Only 3 bytes instead of 4
    let result = parse_date_value(&data);
    assert!(result.is_err());
}

// ============================================================================
// S2 Type System Verification Tests (Issue #624, Epic #622)
// ============================================================================

/// A-07: Tuple field with length >= 128 bytes uses 4-byte BE i32, not VInt.
/// Before this fix, parse_vint_length would read 0x00 as length=0 for a
/// 4-byte field-length header like [0x00, 0x00, 0x00, 0x80], corrupting data.
#[test]
fn s2_a07_tuple_field_128_bytes_cassandra_format() {
    // 128-byte blob field followed by int field = 99
    let blob_data: Vec<u8> = (0u8..=127u8).collect(); // 128 bytes
    let mut data = Vec::new();
    // 4-byte BE i32 length = 128 = [0x00, 0x00, 0x00, 0x80]
    data.extend_from_slice(&128i32.to_be_bytes());
    data.extend_from_slice(&blob_data);
    // Second field: int = 99
    data.extend_from_slice(&4i32.to_be_bytes());
    data.extend_from_slice(&99i32.to_be_bytes());

    let field_comparators = vec![ComparatorType::Blob, ComparatorType::Int];
    let result = parse_tuple_value_with(&data, &field_comparators, |d, comp| match comp {
        ComparatorType::Blob => parse_blob_value(d),
        ComparatorType::Int => parse_int_value(d),
        _ => panic!("Unexpected comparator"),
    })
    .unwrap();

    if let Value::Tuple(fields) = result {
        assert_eq!(fields.len(), 2);
        assert_eq!(
            fields[0],
            Value::Blob(blob_data),
            "128-byte blob should be intact"
        );
        assert_eq!(
            fields[1],
            Value::Integer(99),
            "int after 128-byte blob should be 99"
        );
    } else {
        panic!("Expected Tuple value");
    }
}

/// A-07/A-08: Tuple null field uses -1 sentinel (4-byte BE i32).
#[test]
fn s2_a08_tuple_null_field_minus_one_sentinel() {
    let mut data = Vec::new();
    data.extend_from_slice(&(-1i32).to_be_bytes()); // null
    data.extend_from_slice(&4i32.to_be_bytes()); // int length
    data.extend_from_slice(&7i32.to_be_bytes()); // int = 7

    let field_comparators = vec![ComparatorType::Int, ComparatorType::Int];
    let result =
        parse_tuple_value_with(&data, &field_comparators, |d, _| parse_int_value(d)).unwrap();

    if let Value::Tuple(fields) = result {
        assert_eq!(fields[0], Value::Null, "first field should be null");
        assert_eq!(fields[1], Value::Integer(7), "second field should be 7");
    } else {
        panic!("Expected Tuple value");
    }
}

/// A-08: UDT field with length >= 128 bytes uses 4-byte BE i32, not VInt.
#[test]
fn s2_a08_udt_field_128_bytes_cassandra_format() {
    let text_data: Vec<u8> = b"A".repeat(128);
    let mut data = Vec::new();
    data.extend_from_slice(&128i32.to_be_bytes()); // 4-byte BE i32 length
    data.extend_from_slice(&text_data);

    let field_comparators = vec![("data".to_string(), ComparatorType::Text)];
    let result =
        parse_udt_value_with(&data, &field_comparators, |d, _| parse_text_value(d)).unwrap();

    assert_eq!(result.fields.len(), 1);
    assert_eq!(
        result.fields[0].value,
        Some(Value::Text("A".repeat(128))),
        "128-char text field should be intact"
    );
}

/// A-08: UDT null field uses -1 sentinel (4-byte BE i32).
#[test]
fn s2_a08_udt_null_field_minus_one_sentinel() {
    let mut data = Vec::new();
    data.extend_from_slice(&(-1i32).to_be_bytes()); // null field

    let field_comparators = vec![("x".to_string(), ComparatorType::Int)];
    let result =
        parse_udt_value_with(&data, &field_comparators, |d, _| parse_int_value(d)).unwrap();

    assert_eq!(result.fields.len(), 1);
    assert_eq!(
        result.fields[0].value, None,
        "null field should have value=None"
    );
}

/// A-02: Date epoch bias - 0x80000000 on disk decodes to day 0 (1970-01-01).
#[test]
fn s2_a02_date_epoch_bias_0x80000000() {
    let data = 0x80000000u32.to_be_bytes();
    let result = parse_date_value(&data).unwrap();
    assert_eq!(
        result,
        Value::Date(0),
        "0x80000000 should decode to epoch day 0"
    );
}

/// A-02: Date 1 day after epoch = 0x80000001 on disk.
#[test]
fn s2_a02_date_one_day_after_epoch() {
    let data = 0x80000001u32.to_be_bytes();
    let result = parse_date_value(&data).unwrap();
    assert_eq!(result, Value::Date(1));
}

/// A-02: Date 1 day before epoch = 0x7FFFFFFF on disk.
#[test]
fn s2_a02_date_one_day_before_epoch() {
    let data = 0x7FFFFFFFu32.to_be_bytes();
    let result = parse_date_value(&data).unwrap();
    assert_eq!(result, Value::Date(-1));
}

// Issue #264: Blob fallback disabled test - type-specific parsers enforce strict validation
#[test]
fn test_value_parsing_blob_fallback_disabled() {
    // Test that type-specific parsers reject invalid data lengths
    // This validates strict type checking - no silent blob fallback

    // Smallint requires exactly 2 bytes
    let wrong_size_smallint = [0x00]; // 1 byte
    let result = parse_smallint_value(&wrong_size_smallint);
    assert!(result.is_err(), "Smallint should reject 1-byte input");

    // Bigint requires exactly 8 bytes
    let wrong_size_bigint = [0x00, 0x00, 0x00, 0x00]; // 4 bytes
    let result = parse_bigint_value(&wrong_size_bigint);
    assert!(result.is_err(), "Bigint should reject 4-byte input");

    // Tinyint requires exactly 1 byte
    let wrong_size_tinyint = [0x00, 0x01]; // 2 bytes
    let result = parse_tinyint_value(&wrong_size_tinyint);
    assert!(result.is_err(), "Tinyint should reject 2-byte input");

    // Note: Float and Double parsing is handled by the main parser (parse_value_by_type)
    // rather than standalone functions, so they are not tested here.
}
