//! Unit tests for the DEPRECATED legacy `value_to_json` shaping used by
//! `Database::execute()` (see `database/json_value.rs`).
//!
//! In a sibling file because `database/json_value.rs` is over the campsite threshold
//! (#1116) and the `inet` refusal coverage added for issue #1452 would have
//! grown it.
//!
//! The property these pin is the one issue #1452 found missing: the legacy JSON
//! path and the native `value_to_napi` path must agree about a malformed cell.
//! A JSON `null` for a malformed `inet` length is indistinguishable from a
//! genuine NULL, so it is silent data loss, not a lenient fallback.

use super::*;

#[test]
#[allow(deprecated)]
fn test_value_to_json_primitives() {
    use cqlite_core::types::Value;

    assert_eq!(json_of(&Value::Null), serde_json::Value::Null);
    assert_eq!(
        json_of(&Value::Boolean(true)),
        serde_json::Value::Bool(true)
    );
    assert_eq!(json_of(&Value::Integer(42)), serde_json::json!(42));
    assert_eq!(
        json_of(&Value::text("hello".to_string())),
        serde_json::json!("hello")
    );
}

#[test]
#[allow(deprecated)]
fn test_value_to_json_uuid() {
    use cqlite_core::types::Value;

    let uuid_bytes = [
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
        0x00,
    ];
    let result = json_of(&Value::Uuid(uuid_bytes));

    if let serde_json::Value::String(s) = result {
        assert!(s.contains('-')); // UUID format with hyphens
    } else {
        panic!("Expected string for UUID");
    }
}

#[test]
#[allow(deprecated)]
fn test_value_to_json_collections() {
    use cqlite_core::types::Value;

    // List
    let list = Value::List(vec![Value::Integer(1), Value::Integer(2)]);
    assert_eq!(json_of(&list), serde_json::json!([1, 2]));

    // Map with string keys
    let map = Value::Map(vec![
        (Value::text("a".to_string()), Value::Integer(1)),
        (Value::text("b".to_string()), Value::Integer(2)),
    ]);
    let result = json_of(&map);
    assert!(result.is_object());
}

/// Render a value through the legacy JSON path, failing the test on a refusal.
#[allow(deprecated)]
fn json_of(value: &cqlite_core::types::Value) -> serde_json::Value {
    match value_to_json(value) {
        Ok(json) => json,
        Err(err) => panic!("value_to_json refused unexpectedly: {}", err.reason),
    }
}

/// A malformed `inet` length on the legacy `execute()` path is a typed
/// refusal, NOT a JSON `null` (issue #1452).
///
/// The regression this pins: `value_to_json` held a second, private 4/16
/// length dispatch whose fall-through arm was `serde_json::Value::Null`, so
/// `db.execute('SELECT ip FROM t')` on a 5-byte inet cell returned
/// `{ip: null}` — indistinguishable from a genuine NULL — while
/// `db.executeNative()` on the same cell raised. `specs/
/// binding-shared-scalar-math` forbids any non-error outcome for a
/// malformed length in EITHER binding, so the outcomes here are exactly
/// IPv4, IPv6 and a refusal carrying the #1451 contract identity.
#[test]
#[allow(deprecated)]
fn test_value_to_json_inet_malformed_length_refuses_instead_of_null() {
    use cqlite_core::types::Value;

    // The two well-formed lengths still render.
    assert_eq!(
        json_of(&Value::Inet(vec![192, 168, 1, 1].into())),
        serde_json::json!("192.168.1.1")
    );
    assert!(json_of(&Value::Inet(vec![0u8; 16].into())).is_string());

    // Every other length refuses. 5 is the concrete case from the finding;
    // 0/3/17 cover both sides of both legal widths.
    for len in [0usize, 3, 5, 17] {
        let refused = value_to_json(&Value::Inet(vec![7u8; len].into()));
        let err = match refused {
            Ok(json) => panic!("inet of {len} bytes must refuse, produced {json:?}"),
            Err(err) => err,
        };
        // The one canonical message, and the #1451 error-contract identity
        // for a DATA fault (never the INTERNAL default).
        assert!(
            err.reason.contains("expected 4 or 16"),
            "unexpected message: {}",
            err.reason
        );
        assert!(
            err.reason.contains("\0code=PARSE"),
            "missing contract code: {:?}",
            err.reason
        );
        assert!(
            err.reason.contains("\0category=Data"),
            "missing contract category: {:?}",
            err.reason
        );
    }
}

/// A malformed `inet` NESTED in a collection refuses too — the refusal is
/// not flattened into a null by the list/map/udt shaping (issue #1452).
#[test]
#[allow(deprecated)]
fn test_value_to_json_nested_malformed_inet_refuses() {
    use cqlite_core::types::Value;

    let bad = Value::Inet(vec![1, 2, 3, 4, 5].into());
    let nested = [
        Value::List(vec![bad.clone()]),
        Value::Set(vec![bad.clone()]),
        Value::Tuple(vec![bad.clone()]),
        Value::Map(vec![(Value::text("ip".to_string()), bad.clone())]),
        Value::Map(vec![(bad.clone(), Value::Integer(1))]),
        Value::Frozen(Box::new(bad)),
    ];
    for value in nested {
        assert!(
            value_to_json(&value).is_err(),
            "a nested malformed inet must refuse, not yield null: {value:?}"
        );
    }
}
