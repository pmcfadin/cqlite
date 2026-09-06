use super::*;
use crate::parser::vint::encode_vuint;

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
    assert_eq!(result, Value::text("hello".to_string()));
}

#[test]
fn test_parse_text_unicode() {
    let data = "Hello 世界".as_bytes();
    let result = parse_text_value(data).unwrap();
    assert_eq!(result, Value::text("Hello 世界".to_string()));
}

#[test]
fn test_parse_blob() {
    let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let result = parse_blob_value(&data).unwrap();
    assert_eq!(result, Value::blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
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
    data.extend_from_slice(&encode_vuint(0)); // 0 elements

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
    data.extend_from_slice(&encode_vuint(1)); // 1 element
    data.extend_from_slice(&encode_vuint(4)); // element length
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
    data.extend_from_slice(&encode_vuint(3)); // 3 elements
    for val in [1i32, 2, 3] {
        data.extend_from_slice(&encode_vuint(4)); // each int is 4 bytes
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
    data.extend_from_slice(&encode_vuint(2)); // 2 elements

    let str1 = "hello";
    data.extend_from_slice(&encode_vuint(str1.len() as u64));
    data.extend_from_slice(str1.as_bytes());

    let str2 = "world";
    data.extend_from_slice(&encode_vuint(str2.len() as u64));
    data.extend_from_slice(str2.as_bytes());

    let result =
        parse_list_value_with(&data, &ComparatorType::Text, |d, _| parse_text_value(d)).unwrap();

    if let Value::List(elements) = result {
        assert_eq!(elements.len(), 2);
        assert_eq!(elements[0], Value::text("hello".to_string()));
        assert_eq!(elements[1], Value::text("world".to_string()));
    } else {
        panic!("Expected List value");
    }
}

#[test]
fn test_parse_set_int() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vuint(2)); // 2 elements
    for val in [10i32, 20] {
        data.extend_from_slice(&encode_vuint(4));
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
    data.extend_from_slice(&encode_vuint(2)); // 2 entries

    // Entry 1: "key1" -> 100
    let key1 = "key1";
    data.extend_from_slice(&encode_vuint(key1.len() as u64));
    data.extend_from_slice(key1.as_bytes());
    data.extend_from_slice(&encode_vuint(4));
    data.extend_from_slice(&100i32.to_be_bytes());

    // Entry 2: "key2" -> 200
    let key2 = "key2";
    data.extend_from_slice(&encode_vuint(key2.len() as u64));
    data.extend_from_slice(key2.as_bytes());
    data.extend_from_slice(&encode_vuint(4));
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
        assert_eq!(entries[0].0, Value::text("key1".to_string()));
        assert_eq!(entries[0].1, Value::Integer(100));
        assert_eq!(entries[1].0, Value::text("key2".to_string()));
        assert_eq!(entries[1].1, Value::Integer(200));
    } else {
        panic!("Expected Map value");
    }
}

#[test]
fn test_parse_map_empty() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vuint(0)); // 0 entries

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
    data.extend_from_slice(&encode_vuint(1)); // 1 entry

    // Key: "numbers"
    let key = "numbers";
    data.extend_from_slice(&encode_vuint(key.len() as u64));
    data.extend_from_slice(key.as_bytes());

    // Value: list<int> with [1, 2, 3]
    let mut list_data = Vec::new();
    list_data.extend_from_slice(&encode_vuint(3)); // 3 elements
    for val in [1i32, 2, 3] {
        list_data.extend_from_slice(&encode_vuint(4));
        list_data.extend_from_slice(&val.to_be_bytes());
    }
    data.extend_from_slice(&encode_vuint(list_data.len() as u64));
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
        assert_eq!(entries[0].0, Value::text("numbers".to_string()));
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
        assert_eq!(fields[1], Value::text("hello".to_string()));
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
        Some(Value::text("Alice".to_string()))
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
    list_data.extend_from_slice(&encode_vuint(2)); // 2 elements (VInt count per CollectionSerializer)
    for tag in ["tag1", "tag2"] {
        list_data.extend_from_slice(&encode_vuint(tag.len() as u64)); // VInt element length
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
        assert_eq!(tags[0], Value::text("tag1".to_string()));
        assert_eq!(tags[1], Value::text("tag2".to_string()));
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
    data.extend_from_slice(&encode_vuint(2)); // Claims 2 elements
    data.extend_from_slice(&encode_vuint(4)); // First element length
    data.extend_from_slice(&42i32.to_be_bytes());
    // Missing second element

    let result = parse_list_value_with(&data, &ComparatorType::Int, |d, _| parse_int_value(d));

    // A valid list holds EXACTLY `count` elements; a buffer that runs dry after
    // 1 of 2 declared elements is a truncated cell and must Err, not silently
    // return a short partial list (#1632).
    assert!(
        result.is_err(),
        "list declaring more elements than present must Err (truncated), not accept a partial"
    );
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
            Value::Blob(blob_data.into()),
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
        Some(Value::text("A".repeat(128))),
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

// ============================================================================
// Issue #1632 (hardening c/d): a corrupt, huge declared element/entry count must
// not pre-allocate gigabytes. The shared collection helpers clamp pre-allocation
// to min(count, REASONABLE_COLLECTION_CAPACITY); with a short buffer the first
// element/key length exceeds the data, so parsing returns Err promptly with
// bounded peak allocation (no OOM / panic).
// ============================================================================

#[test]
fn test_parse_list_huge_count_short_buffer_errors_bounded_alloc() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vuint(1u64 << 30)); // ~1 billion elements
    data.extend_from_slice(&encode_vuint(1000)); // first element claims 1000 bytes...
    data.extend_from_slice(&42i32.to_be_bytes()); // ...but only 4 remain

    let result = parse_list_value_with(&data, &ComparatorType::Int, |d, _| parse_int_value(d));
    assert!(
        result.is_err(),
        "huge count + short buffer must Err without a huge pre-allocation"
    );
}

#[test]
fn test_parse_map_huge_count_short_buffer_errors_bounded_alloc() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vuint(1u64 << 30)); // ~1 billion entries
    data.extend_from_slice(&encode_vuint(1000)); // first key claims 1000 bytes...
    data.extend_from_slice(&42i32.to_be_bytes()); // ...but only 4 remain

    let result = parse_map_value_with(&data, &ComparatorType::Int, &ComparatorType::Int, |d, _| {
        parse_int_value(d)
    });
    assert!(
        result.is_err(),
        "huge count + short buffer must Err without a huge pre-allocation"
    );
}

// Issue #1632 acceptance criterion (roborev's exact case): a huge declared count
// followed IMMEDIATELY by EOF (no element/entry bytes at all) is a truncated cell
// and must Err — the old buffer-terminated loop silently returned Ok(empty). A
// valid collection always carries exactly `count` fully-encoded elements, so
// requiring `count` decoded elements never rejects well-formed data.

#[test]
fn test_parse_list_huge_count_eof_after_count_errors() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vuint(1u64 << 30)); // ~1 billion elements, then EOF

    let result = parse_list_value_with(&data, &ComparatorType::Int, |d, _| parse_int_value(d));
    assert!(
        result.is_err(),
        "list: huge count then immediate EOF must Err (truncated), not Ok(empty)"
    );
}

#[test]
fn test_parse_set_huge_count_eof_after_count_errors() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vuint(1u64 << 30)); // ~1 billion elements, then EOF

    let result = parse_set_value_with(&data, &ComparatorType::Int, |d, _| parse_int_value(d));
    assert!(
        result.is_err(),
        "set: huge count then immediate EOF must Err (truncated), not Ok(empty)"
    );
}

#[test]
fn test_parse_map_huge_count_eof_after_count_errors() {
    let mut data = Vec::new();
    data.extend_from_slice(&encode_vuint(1u64 << 30)); // ~1 billion entries, then EOF

    let result = parse_map_value_with(&data, &ComparatorType::Int, &ComparatorType::Int, |d, _| {
        parse_int_value(d)
    });
    assert!(
        result.is_err(),
        "map: huge count then immediate EOF must Err (truncated), not Ok(empty)"
    );
}

// Issue #1632 (hardening a): the SSTableReader value-decode surface carries the
// identical `MAX_VALUE_NESTING_DEPTH` guard as the free-function comparator path
// (covered by `test_deeply_nested_frozen_type_errors_not_overflow`). This mirrors
// that test through the real `SSTableReader::parse_value_with_comparator_at_depth`
// method (entered at depth 0, as the schema path does): a deeply-nested
// `frozen<...>` type recurses on the SAME bytes at every level, so without the
// guard it would recurse to a stack overflow. It must return `Err`.
//
// `SSTableReader` has no lightweight constructor, so this opens a real (fetched)
// SSTable — the reader's own data content is irrelevant; the frozen recursion
// operates only on the passed-in slice. Fixture-gated: SKIP when absent.
#[tokio::test]
async fn test_1632_sstablereader_deeply_nested_frozen_errors_not_overflow() {
    let Some(reader) = open_simple_table_fixture_reader().await else {
        return; // SKIP: fixture absent (message already logged).
    };

    // 12 levels of frozen wrapping an int — exceeds MAX_VALUE_NESTING_DEPTH (10).
    let mut comparator = ComparatorType::Int;
    for _ in 0..12 {
        comparator = ComparatorType::Frozen(Box::new(comparator));
    }
    // Any body works: frozen recurses without consuming bytes.
    let data = vec![0x00, 0x00, 0x00, 0x2A];
    let result = reader.parse_value_with_comparator_at_depth(&data, &comparator, 0);
    assert!(
        result.is_err(),
        "12-level nested frozen type must Err via SSTableReader, not stack-overflow/abort"
    );
}

// Issue #1632 (Finding 2): the schema-path `frozen<...>` arm
// (`parse_value_with_schema_type`) must count the outer frozen layer just like
// the block path (`parse_value_with_comparator_at_depth`). Before the fix the
// schema path entered the inner comparator at depth 0, silently allowing ONE
// extra nested level past MAX_VALUE_NESTING_DEPTH. This asserts the two paths
// agree at BOTH the last-allowed depth (10 frozens → Ok) and one past it
// (11 frozens → Err). Fixture-gated: SKIP when the dataset is absent.
#[tokio::test]
async fn test_1632_frozen_depth_schema_path_symmetric_with_block_path() {
    let Some(reader) = open_simple_table_fixture_reader().await else {
        return; // SKIP: fixture absent (message already logged).
    };

    // # THE LEAF IS `list<int>`, NOT `int`, AND THE BOUNDARY IS UNCHANGED (#4104)
    //
    // `frozen<int>` is not declarable CQL — `CQL3Type.Raw::freeze()` throws for
    // every non-collection/tuple/UDT (`cassandra-5.0.8:src/java/org/apache/
    // cassandra/cql3/CQL3Type.java:647-651`) — so the schema path now refuses that
    // spelling at `CqlType::parse` and this test can no longer probe depth with it.
    // A frozen COLLECTION is the legal witness for the same property.
    //
    // The boundary does not move, and that is arithmetic rather than luck: `Frozen`
    // consumes no bytes and recurses at `depth + 1`, so `Frozen^n(List(Int))`
    // reaches the `List` arm at depth `n` on both paths — exactly where
    // `Frozen^n(Int)` reached the `Int` arm. A ZERO-COUNT list body is used so the
    // element decode (which would recurse to `n + 1`) is never entered and the
    // frozen layers remain the only thing being counted.
    //
    // Body: one VInt `0x00` = element count 0. Frozen consumes nothing, so the same
    // byte serves at every nesting level.
    let data = vec![0x00];

    // Build the equivalent block-path comparator for `n` frozen layers over
    // `list<int>`.
    let frozen_comparator = |n: usize| -> ComparatorType {
        let mut c = ComparatorType::List(Box::new(ComparatorType::Int));
        for _ in 0..n {
            c = ComparatorType::Frozen(Box::new(c));
        }
        c
    };
    // The type string `frozen<...<list<int>>...>` for the schema path.
    let frozen_type_string =
        |n: usize| -> String { "frozen<".repeat(n) + "list<int>" + &">".repeat(n) };

    // MAX_VALUE_NESTING_DEPTH is 10: 10 frozens is the last allowed depth, 11 exceeds it.
    for (n, expect_ok) in [(10usize, true), (11usize, false)] {
        let block = reader.parse_value_with_comparator_at_depth(&data, &frozen_comparator(n), 0);
        let schema = reader.parse_value_with_schema_type(&data, &frozen_type_string(n));
        assert_eq!(
            block.is_ok(),
            expect_ok,
            "block path at {n} frozen layers should be ok={expect_ok}"
        );
        assert_eq!(
            schema.is_ok(),
            block.is_ok(),
            "schema path must agree with block path at {n} frozen layers (symmetric depth counting)"
        );
    }
}

/// Open a reliably-present fetched fixture (`test_basic/simple_table`) as a real
/// `SSTableReader`. The reader's own content is irrelevant to these depth/frozen
/// tests — recursion operates only on the passed-in slice — but `SSTableReader`
/// has no lightweight constructor. Returns `None` (after logging a SKIP) when the
/// dataset is absent.
#[cfg(test)]
async fn open_simple_table_fixture_reader() -> Option<SSTableReader> {
    use crate::{Config, Platform};
    use std::path::PathBuf;
    use std::sync::Arc;

    let root = match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(r) if PathBuf::from(&r).is_dir() => PathBuf::from(r),
        _ => match PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|p| p.join("test-data/datasets"))
        {
            Some(p) if p.is_dir() => p,
            _ => {
                eprintln!("SKIP: datasets root absent.");
                return None;
            }
        },
    };
    let base = root.join("sstables/test_basic");
    let data_db = std::fs::read_dir(&base).ok().and_then(|rd| {
        rd.flatten().find_map(|e| {
            let name = e.file_name();
            let name = name.to_str()?;
            if name.starts_with("simple_table-") {
                let c = e.path().join("nb-1-big-Data.db");
                return c.is_file().then_some(c);
            }
            None
        })
    });
    let Some(path) = data_db else {
        eprintln!("SKIP: test_basic/simple_table Data.db absent.");
        return None;
    };

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform init"));
    Some(
        SSTableReader::open(&path, &config, platform)
            .await
            .expect("opening the fetched simple_table fixture should succeed"),
    )
}
