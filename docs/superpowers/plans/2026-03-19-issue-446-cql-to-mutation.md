# Issue #446: Connect CQL Parser to WriteEngine

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the existing CQL mutation parser (INSERT/UPDATE/DELETE) to the WriteEngine so `WriteEngine::execute("INSERT INTO ...")` actually works instead of returning a stub error.

**Architecture:** Create a new `cql_to_mutation.rs` module in the write_engine directory that converts parser AST types (`CqlInsert`, `CqlUpdate`, `CqlDelete`) to `Mutation` structs using the schema from `WriteEngineConfig`. The conversion is schema-aware: literal values are coerced to the correct `Value` variant based on column type from `TableSchema`. The stub `parse_cql_to_mutation()` in `mod.rs` is then replaced with calls to the parser + converter.

**Tech Stack:** Rust, nom (existing parser), CqlType/TableSchema (existing schema), feature-gated behind `write-support`

**References:**
- Stub: `cqlite-core/src/storage/write_engine/mod.rs:658-665`
- Parser: `cqlite-core/src/cql/mutation_parser.rs` (public fns: `parse_insert_statement`, `parse_update_statement`, `parse_delete_statement`)
- AST: `cqlite-core/src/cql/ast.rs` (CqlInsert, CqlUpdate, CqlDelete, CqlLiteral, CqlExpression)
- Mutation: `cqlite-core/src/storage/write_engine/mutation.rs` (Mutation, TableId, PartitionKey, ClusteringKey, CellOperation, Value)
- Schema: `cqlite-core/src/schema/mod.rs` (TableSchema, CqlType, Column, KeyColumn, ClusteringColumn)
- Types: `cqlite-core/src/types.rs` (Value enum)

---

### Task 1: Create `cql_to_mutation.rs` with CqlLiteral-to-Value Conversion

**Files:**
- Create: `cqlite-core/src/storage/write_engine/cql_to_mutation.rs`
- Modify: `cqlite-core/src/storage/write_engine/mod.rs` (add `mod cql_to_mutation;`)

The core building block: convert `CqlLiteral` values to `Value` using the target column's `CqlType` from the schema. This handles type coercion (e.g., `CqlLiteral::Integer(42)` → `Value::BigInt(42)` when column type is `bigint`).

- [ ] **Step 1: Write failing tests for literal-to-value conversion**

Create the module file with tests for primitive type conversions:

```rust
// cqlite-core/src/storage/write_engine/cql_to_mutation.rs

#[cfg(feature = "write-support")]
use crate::cql::ast::*;
#[cfg(feature = "write-support")]
use crate::schema::{CqlType, TableSchema};
#[cfg(feature = "write-support")]
use crate::storage::write_engine::mutation::*;
#[cfg(feature = "write-support")]
use crate::types::Value;
#[cfg(feature = "write-support")]
use crate::Error;

/// Convert a CqlLiteral to a Value, coercing to the target CQL type.
#[cfg(feature = "write-support")]
pub(crate) fn literal_to_value(literal: &CqlLiteral, target_type: &CqlType) -> Result<Value, Error> {
    todo!()
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;

    #[test]
    fn test_null_literal() {
        let result = literal_to_value(&CqlLiteral::Null, &CqlType::Text);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Null);
    }

    #[test]
    fn test_boolean_literal() {
        let result = literal_to_value(&CqlLiteral::Boolean(true), &CqlType::Boolean);
        assert_eq!(result.unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_integer_to_int() {
        let result = literal_to_value(&CqlLiteral::Integer(42), &CqlType::Int);
        assert_eq!(result.unwrap(), Value::Integer(42));
    }

    #[test]
    fn test_integer_to_bigint() {
        let result = literal_to_value(&CqlLiteral::Integer(42), &CqlType::BigInt);
        assert_eq!(result.unwrap(), Value::BigInt(42));
    }

    #[test]
    fn test_integer_to_smallint() {
        let result = literal_to_value(&CqlLiteral::Integer(42), &CqlType::SmallInt);
        assert_eq!(result.unwrap(), Value::SmallInt(42));
    }

    #[test]
    fn test_integer_to_tinyint() {
        let result = literal_to_value(&CqlLiteral::Integer(7), &CqlType::TinyInt);
        assert_eq!(result.unwrap(), Value::TinyInt(7));
    }

    #[test]
    fn test_integer_overflow_tinyint() {
        let result = literal_to_value(&CqlLiteral::Integer(999), &CqlType::TinyInt);
        assert!(result.is_err());
    }

    #[test]
    fn test_float_to_float() {
        let result = literal_to_value(&CqlLiteral::Float(3.14), &CqlType::Float);
        assert_eq!(result.unwrap(), Value::Float32(3.14_f32));
    }

    #[test]
    fn test_float_to_double() {
        let result = literal_to_value(&CqlLiteral::Float(3.14), &CqlType::Double);
        assert_eq!(result.unwrap(), Value::Float(3.14));
    }

    #[test]
    fn test_string_to_text() {
        let result = literal_to_value(&CqlLiteral::String("hello".into()), &CqlType::Text);
        assert_eq!(result.unwrap(), Value::Text("hello".into()));
    }

    #[test]
    fn test_string_to_varchar() {
        let result = literal_to_value(&CqlLiteral::String("hello".into()), &CqlType::Varchar);
        assert_eq!(result.unwrap(), Value::Text("hello".into()));
    }

    #[test]
    fn test_string_to_ascii() {
        let result = literal_to_value(&CqlLiteral::String("hello".into()), &CqlType::Ascii);
        assert_eq!(result.unwrap(), Value::Text("hello".into()));
    }

    #[test]
    fn test_uuid_literal() {
        let result = literal_to_value(
            &CqlLiteral::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            &CqlType::Uuid,
        );
        assert!(result.is_ok());
        if let Value::Uuid(bytes) = result.unwrap() {
            assert_eq!(bytes.len(), 16);
        } else {
            panic!("expected Uuid");
        }
    }

    #[test]
    fn test_blob_literal() {
        let result = literal_to_value(
            &CqlLiteral::Blob("0xdeadbeef".into()),
            &CqlType::Blob,
        );
        assert_eq!(result.unwrap(), Value::Blob(vec![0xde, 0xad, 0xbe, 0xef]));
    }

    #[test]
    fn test_integer_to_timestamp() {
        let result = literal_to_value(&CqlLiteral::Integer(1704067200000), &CqlType::Timestamp);
        assert_eq!(result.unwrap(), Value::Timestamp(1704067200000));
    }

    #[test]
    fn test_type_mismatch_error() {
        let result = literal_to_value(&CqlLiteral::Boolean(true), &CqlType::Int);
        assert!(result.is_err());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --package cqlite-core --features write-support cql_to_mutation -- --nocapture 2>&1 | head -30`
Expected: compilation error (todo!) or test failures

- [ ] **Step 3: Implement `literal_to_value` for primitives**

Replace `todo!()` with the full implementation:

```rust
#[cfg(feature = "write-support")]
pub(crate) fn literal_to_value(literal: &CqlLiteral, target_type: &CqlType) -> Result<Value, Error> {
    // Unwrap Frozen wrapper - frozen doesn't affect value conversion
    let target_type = match target_type {
        CqlType::Frozen(inner) => inner.as_ref(),
        other => other,
    };

    match literal {
        CqlLiteral::Null => Ok(Value::Null),

        CqlLiteral::Boolean(b) => match target_type {
            CqlType::Boolean => Ok(Value::Boolean(*b)),
            _ => Err(type_mismatch("boolean", target_type)),
        },

        CqlLiteral::Integer(i) => integer_to_value(*i, target_type),

        CqlLiteral::Float(f) => match target_type {
            CqlType::Float => Ok(Value::Float32(*f as f32)),
            CqlType::Double => Ok(Value::Float(*f)),
            _ => Err(type_mismatch("float", target_type)),
        },

        CqlLiteral::String(s) => match target_type {
            CqlType::Text | CqlType::Varchar | CqlType::Ascii => Ok(Value::Text(s.clone())),
            CqlType::Inet => parse_inet(s),
            _ => Err(type_mismatch("string", target_type)),
        },

        CqlLiteral::Uuid(s) => match target_type {
            CqlType::Uuid | CqlType::TimeUuid => parse_uuid(s),
            _ => Err(type_mismatch("uuid", target_type)),
        },

        CqlLiteral::Blob(s) => match target_type {
            CqlType::Blob => parse_blob(s),
            _ => Err(type_mismatch("blob", target_type)),
        },

        CqlLiteral::Collection(coll) => collection_to_value(coll, target_type),
        CqlLiteral::Tuple(elements) => tuple_to_value(elements, target_type),
        CqlLiteral::Udt(udt) => udt_to_value(udt, target_type),
    }
}

#[cfg(feature = "write-support")]
fn integer_to_value(i: i64, target_type: &CqlType) -> Result<Value, Error> {
    match target_type {
        CqlType::TinyInt => {
            let val = i8::try_from(i).map_err(|_| overflow_error(i, "tinyint"))?;
            Ok(Value::TinyInt(val))
        }
        CqlType::SmallInt => {
            let val = i16::try_from(i).map_err(|_| overflow_error(i, "smallint"))?;
            Ok(Value::SmallInt(val))
        }
        CqlType::Int => {
            let val = i32::try_from(i).map_err(|_| overflow_error(i, "int"))?;
            Ok(Value::Integer(val))
        }
        CqlType::BigInt | CqlType::Counter => Ok(Value::BigInt(i)),
        CqlType::Timestamp => Ok(Value::Timestamp(i)),
        CqlType::Float => Ok(Value::Float32(i as f32)),
        CqlType::Double => Ok(Value::Float(i as f64)),
        CqlType::Varint => Ok(Value::Varint(i.to_be_bytes().to_vec())),
        _ => Err(type_mismatch("integer", target_type)),
    }
}

#[cfg(feature = "write-support")]
fn parse_uuid(s: &str) -> Result<Value, Error> {
    // Parse UUID string "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" to 16 bytes
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return Err(Error::InvalidInput(format!("Invalid UUID: {}", s)));
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| Error::InvalidInput(format!("Invalid UUID hex: {}", s)))?;
    }
    Ok(Value::Uuid(bytes))
}

#[cfg(feature = "write-support")]
fn parse_blob(s: &str) -> Result<Value, Error> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for i in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i + 2], 16)
            .map_err(|_| Error::InvalidInput(format!("Invalid blob hex: {}", s)))?;
        bytes.push(byte);
    }
    Ok(Value::Blob(bytes))
}

#[cfg(feature = "write-support")]
fn parse_inet(s: &str) -> Result<Value, Error> {
    use std::net::IpAddr;
    let addr: IpAddr = s.parse()
        .map_err(|_| Error::InvalidInput(format!("Invalid inet address: {}", s)))?;
    match addr {
        IpAddr::V4(v4) => Ok(Value::Inet(v4.octets().to_vec())),
        IpAddr::V6(v6) => Ok(Value::Inet(v6.octets().to_vec())),
    }
}

#[cfg(feature = "write-support")]
fn collection_to_value(coll: &CqlCollectionLiteral, target_type: &CqlType) -> Result<Value, Error> {
    match (coll, target_type) {
        (CqlCollectionLiteral::List(items), CqlType::List(elem_type)) => {
            let values: Result<Vec<Value>, Error> = items.iter()
                .map(|item| literal_to_value(item, elem_type))
                .collect();
            Ok(Value::List(values?))
        }
        (CqlCollectionLiteral::Set(items), CqlType::Set(elem_type)) => {
            let values: Result<Vec<Value>, Error> = items.iter()
                .map(|item| literal_to_value(item, elem_type))
                .collect();
            Ok(Value::Set(values?))
        }
        (CqlCollectionLiteral::Map(entries), CqlType::Map(key_type, val_type)) => {
            let pairs: Result<Vec<(Value, Value)>, Error> = entries.iter()
                .map(|(k, v)| {
                    Ok((literal_to_value(k, key_type)?, literal_to_value(v, val_type)?))
                })
                .collect();
            Ok(Value::Map(pairs?))
        }
        _ => Err(type_mismatch("collection", target_type)),
    }
}

#[cfg(feature = "write-support")]
fn tuple_to_value(elements: &[CqlLiteral], target_type: &CqlType) -> Result<Value, Error> {
    match target_type {
        CqlType::Tuple(types) => {
            if elements.len() != types.len() {
                return Err(Error::InvalidInput(format!(
                    "Tuple has {} elements but type expects {}",
                    elements.len(), types.len()
                )));
            }
            let values: Result<Vec<Value>, Error> = elements.iter().zip(types.iter())
                .map(|(lit, typ)| literal_to_value(lit, typ))
                .collect();
            Ok(Value::Tuple(values?))
        }
        _ => Err(type_mismatch("tuple", target_type)),
    }
}

#[cfg(feature = "write-support")]
fn udt_to_value(udt: &CqlUdtLiteral, target_type: &CqlType) -> Result<Value, Error> {
    match target_type {
        CqlType::Udt(type_name, fields) => {
            let mut udt_fields = Vec::new();
            for (field_name, field_type) in fields {
                let value = udt.fields.iter()
                    .find(|(name, _)| name.name == *field_name)
                    .map(|(_, lit)| literal_to_value(lit, field_type))
                    .transpose()?;
                udt_fields.push(crate::types::UdtField {
                    name: field_name.clone(),
                    value,
                });
            }
            Ok(Value::Udt(crate::types::UdtValue {
                type_name: type_name.clone(),
                keyspace: String::new(),
                fields: udt_fields,
            }))
        }
        _ => Err(type_mismatch("udt", target_type)),
    }
}

#[cfg(feature = "write-support")]
fn type_mismatch(literal_type: &str, target: &CqlType) -> Error {
    Error::InvalidInput(format!(
        "Cannot convert {} literal to {:?}",
        literal_type, target
    ))
}

#[cfg(feature = "write-support")]
fn overflow_error(value: i64, target: &str) -> Error {
    Error::InvalidInput(format!(
        "Value {} overflows {}", value, target
    ))
}
```

- [ ] **Step 4: Register the module in mod.rs**

In `cqlite-core/src/storage/write_engine/mod.rs`, add:
```rust
#[cfg(feature = "write-support")]
mod cql_to_mutation;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --package cqlite-core --features write-support cql_to_mutation -v`
Expected: All 16 tests PASS

- [ ] **Step 6: Commit**

```bash
git add cqlite-core/src/storage/write_engine/cql_to_mutation.rs cqlite-core/src/storage/write_engine/mod.rs
git commit -m "feat(#446): add CqlLiteral-to-Value conversion with schema-aware coercion"
```

---

### Task 2: Add Collection and Edge Case Tests

**Files:**
- Modify: `cqlite-core/src/storage/write_engine/cql_to_mutation.rs`

Add tests for collections, tuples, UDTs, frozen types, and edge cases.

- [ ] **Step 1: Add collection and complex type tests**

```rust
#[test]
fn test_list_of_int() {
    let lit = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![
        CqlLiteral::Integer(1),
        CqlLiteral::Integer(2),
        CqlLiteral::Integer(3),
    ]));
    let result = literal_to_value(&lit, &CqlType::List(Box::new(CqlType::Int)));
    assert_eq!(result.unwrap(), Value::List(vec![
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
    ]));
}

#[test]
fn test_set_of_text() {
    let lit = CqlLiteral::Collection(CqlCollectionLiteral::Set(vec![
        CqlLiteral::String("a".into()),
        CqlLiteral::String("b".into()),
    ]));
    let result = literal_to_value(&lit, &CqlType::Set(Box::new(CqlType::Text)));
    assert_eq!(result.unwrap(), Value::Set(vec![
        Value::Text("a".into()),
        Value::Text("b".into()),
    ]));
}

#[test]
fn test_map_of_text_to_int() {
    let lit = CqlLiteral::Collection(CqlCollectionLiteral::Map(vec![
        (CqlLiteral::String("a".into()), CqlLiteral::Integer(1)),
    ]));
    let result = literal_to_value(&lit, &CqlType::Map(
        Box::new(CqlType::Text),
        Box::new(CqlType::Int),
    ));
    assert_eq!(result.unwrap(), Value::Map(vec![
        (Value::Text("a".into()), Value::Integer(1)),
    ]));
}

#[test]
fn test_frozen_list() {
    let lit = CqlLiteral::Collection(CqlCollectionLiteral::List(vec![
        CqlLiteral::Integer(1),
    ]));
    let result = literal_to_value(&lit, &CqlType::Frozen(Box::new(CqlType::List(Box::new(CqlType::Int)))));
    assert_eq!(result.unwrap(), Value::List(vec![Value::Integer(1)]));
}

#[test]
fn test_tuple() {
    let lit = CqlLiteral::Tuple(vec![
        CqlLiteral::Integer(1),
        CqlLiteral::String("hello".into()),
    ]);
    let result = literal_to_value(&lit, &CqlType::Tuple(vec![CqlType::Int, CqlType::Text]));
    assert_eq!(result.unwrap(), Value::Tuple(vec![
        Value::Integer(1),
        Value::Text("hello".into()),
    ]));
}

#[test]
fn test_tuple_wrong_arity() {
    let lit = CqlLiteral::Tuple(vec![CqlLiteral::Integer(1)]);
    let result = literal_to_value(&lit, &CqlType::Tuple(vec![CqlType::Int, CqlType::Text]));
    assert!(result.is_err());
}

#[test]
fn test_integer_to_varint() {
    let result = literal_to_value(&CqlLiteral::Integer(256), &CqlType::Varint);
    assert!(result.is_ok());
    if let Value::Varint(bytes) = result.unwrap() {
        assert!(!bytes.is_empty());
    } else {
        panic!("expected Varint");
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cargo test --package cqlite-core --features write-support cql_to_mutation -v`
Expected: All tests PASS (including new ones)

- [ ] **Step 3: Commit**

```bash
git add cqlite-core/src/storage/write_engine/cql_to_mutation.rs
git commit -m "test(#446): add collection, tuple, and edge case tests for literal conversion"
```

---

### Task 3: Implement INSERT-to-Mutation Conversion

**Files:**
- Modify: `cqlite-core/src/storage/write_engine/cql_to_mutation.rs`

Convert `CqlInsert` AST → `Mutation`. This requires schema lookup to identify which columns are partition keys, clustering keys, and regular columns.

- [ ] **Step 1: Write failing test for INSERT conversion**

Add a helper to build a test schema, then test INSERT conversion:

```rust
#[cfg(all(test, feature = "write-support"))]
fn test_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".into(),
        table: "test_tbl".into(),
        partition_keys: vec![
            crate::schema::KeyColumn {
                name: "id".into(),
                data_type: "uuid".into(),
                position: 0,
            },
        ],
        clustering_keys: vec![
            crate::schema::ClusteringColumn {
                name: "ts".into(),
                data_type: "timestamp".into(),
                position: 0,
                order: crate::schema::ClusteringOrder::Asc,
            },
        ],
        columns: vec![
            crate::schema::Column { name: "id".into(), data_type: "uuid".into(), nullable: false, default: None, is_static: false },
            crate::schema::Column { name: "ts".into(), data_type: "timestamp".into(), nullable: false, default: None, is_static: false },
            crate::schema::Column { name: "name".into(), data_type: "text".into(), nullable: true, default: None, is_static: false },
            crate::schema::Column { name: "age".into(), data_type: "int".into(), nullable: true, default: None, is_static: false },
        ],
        comments: std::collections::HashMap::new(),
    }
}

#[test]
fn test_insert_to_mutation() {
    let schema = test_schema();
    let insert = CqlInsert {
        table: CqlTable {
            keyspace: Some(CqlIdentifier { name: "test_ks".into(), quoted: false }),
            name: CqlIdentifier { name: "test_tbl".into(), quoted: false },
        },
        columns: vec![
            CqlIdentifier { name: "id".into(), quoted: false },
            CqlIdentifier { name: "ts".into(), quoted: false },
            CqlIdentifier { name: "name".into(), quoted: false },
            CqlIdentifier { name: "age".into(), quoted: false },
        ],
        values: CqlInsertValues::Values(vec![
            CqlExpression::Literal(CqlLiteral::Uuid("550e8400-e29b-41d4-a716-446655440000".into())),
            CqlExpression::Literal(CqlLiteral::Integer(1704067200000)),
            CqlExpression::Literal(CqlLiteral::String("Alice".into())),
            CqlExpression::Literal(CqlLiteral::Integer(30)),
        ]),
        if_not_exists: false,
        using: None,
    };
    let mutation = insert_to_mutation(&insert, &schema).unwrap();
    assert_eq!(mutation.table.keyspace, "test_ks");
    assert_eq!(mutation.table.table, "test_tbl");
    assert_eq!(mutation.partition_key.columns.len(), 1);
    assert_eq!(mutation.partition_key.columns[0].0, "id");
    assert!(mutation.clustering_key.is_some());
    assert_eq!(mutation.operations.len(), 2); // name + age (non-key columns)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --package cqlite-core --features write-support test_insert_to_mutation -v`
Expected: FAIL - `insert_to_mutation` not found

- [ ] **Step 3: Implement `insert_to_mutation`**

```rust
/// Convert a parsed CQL INSERT statement to a Mutation.
#[cfg(feature = "write-support")]
pub(crate) fn insert_to_mutation(
    insert: &CqlInsert,
    schema: &TableSchema,
) -> Result<Mutation, Error> {
    // Validate table name matches schema
    validate_table(&insert.table, schema)?;

    // Extract column names and values
    let values = match &insert.values {
        CqlInsertValues::Values(exprs) => exprs,
        CqlInsertValues::Json(_) => {
            return Err(Error::InvalidInput("JSON INSERT not yet supported".into()));
        }
    };

    if insert.columns.len() != values.len() {
        return Err(Error::InvalidInput(format!(
            "Column count ({}) does not match value count ({})",
            insert.columns.len(), values.len()
        )));
    }

    // Build column-name → Value pairs
    let mut column_values: Vec<(&str, Value)> = Vec::new();
    for (col, expr) in insert.columns.iter().zip(values.iter()) {
        let col_name = &col.name;
        let column_def = schema.get_column(col_name)
            .ok_or_else(|| Error::InvalidInput(format!("Unknown column: {}", col_name)))?;
        let cql_type = CqlType::parse(&column_def.data_type)?;
        let value = expression_to_value(expr, &cql_type)?;
        column_values.push((col_name.as_str(), value));
    }

    // Separate into partition key, clustering key, and regular columns
    let pk_names: Vec<String> = schema.ordered_partition_keys()
        .iter().map(|k| k.name.clone()).collect();
    let ck_names: Vec<String> = schema.ordered_clustering_keys()
        .iter().map(|k| k.name.clone()).collect();

    let mut pk_values = Vec::new();
    for pk_name in &pk_names {
        let val = column_values.iter()
            .find(|(name, _)| *name == pk_name.as_str())
            .ok_or_else(|| Error::InvalidInput(format!("Missing partition key column: {}", pk_name)))?;
        pk_values.push((pk_name.clone(), val.1.clone()));
    }

    let mut ck_values = Vec::new();
    for ck_name in &ck_names {
        let val = column_values.iter()
            .find(|(name, _)| *name == ck_name.as_str())
            .ok_or_else(|| Error::InvalidInput(format!("Missing clustering key column: {}", ck_name)))?;
        ck_values.push((ck_name.clone(), val.1.clone()));
    }

    // Regular columns become CellOperation::Write
    let operations: Vec<CellOperation> = column_values.iter()
        .filter(|(name, _)| !pk_names.iter().any(|pk| pk == *name) && !ck_names.iter().any(|ck| ck == *name))
        .map(|(name, value)| CellOperation::Write {
            column: name.to_string(),
            value: value.clone(),
        })
        .collect();

    // Extract timestamp from USING clause or use current time
    let timestamp_micros = extract_timestamp(&insert.using)?;
    let ttl_seconds = extract_ttl(&insert.using)?;

    Ok(Mutation::new(
        TableId::new(schema.keyspace.clone(), schema.table.clone()),
        PartitionKey { columns: pk_values },
        if ck_values.is_empty() { None } else { Some(ClusteringKey { columns: ck_values }) },
        operations,
        timestamp_micros,
        ttl_seconds,
    ))
}

/// Convert a CqlExpression to a Value (only supports literals for now).
#[cfg(feature = "write-support")]
fn expression_to_value(expr: &CqlExpression, target_type: &CqlType) -> Result<Value, Error> {
    match expr {
        CqlExpression::Literal(lit) => literal_to_value(lit, target_type),
        CqlExpression::Unary { operator: CqlUnaryOperator::Minus, operand } => {
            // Handle negative numbers: -42
            match operand.as_ref() {
                CqlExpression::Literal(CqlLiteral::Integer(i)) => {
                    literal_to_value(&CqlLiteral::Integer(-i), target_type)
                }
                CqlExpression::Literal(CqlLiteral::Float(f)) => {
                    literal_to_value(&CqlLiteral::Float(-f), target_type)
                }
                _ => Err(Error::InvalidInput("Only literal values are supported in mutations".into())),
            }
        }
        _ => Err(Error::InvalidInput(format!(
            "Only literal values are supported in mutations, got: {:?}", expr
        ))),
    }
}

#[cfg(feature = "write-support")]
fn validate_table(table: &CqlTable, schema: &TableSchema) -> Result<(), Error> {
    if let Some(ks) = &table.keyspace {
        if ks.name != schema.keyspace {
            return Err(Error::InvalidInput(format!(
                "Keyspace mismatch: statement references '{}' but schema is for '{}'",
                ks.name, schema.keyspace
            )));
        }
    }
    if table.name.name != schema.table {
        return Err(Error::InvalidInput(format!(
            "Table mismatch: statement references '{}' but schema is for '{}'",
            table.name.name, schema.table
        )));
    }
    Ok(())
}

#[cfg(feature = "write-support")]
fn extract_timestamp(using: &Option<CqlUsing>) -> Result<i64, Error> {
    if let Some(using) = using {
        if let Some(ts_expr) = &using.timestamp {
            if let CqlExpression::Literal(CqlLiteral::Integer(ts)) = ts_expr {
                return Ok(*ts);
            }
            return Err(Error::InvalidInput("USING TIMESTAMP must be an integer literal".into()));
        }
    }
    // Default: current time in microseconds
    Ok(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64)
}

#[cfg(feature = "write-support")]
fn extract_ttl(using: &Option<CqlUsing>) -> Result<Option<u32>, Error> {
    if let Some(using) = using {
        if let Some(ttl_expr) = &using.ttl {
            if let CqlExpression::Literal(CqlLiteral::Integer(ttl)) = ttl_expr {
                let ttl = u32::try_from(*ttl)
                    .map_err(|_| Error::InvalidInput(format!("Invalid TTL value: {}", ttl)))?;
                return Ok(Some(ttl));
            }
            return Err(Error::InvalidInput("USING TTL must be an integer literal".into()));
        }
    }
    Ok(None)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --package cqlite-core --features write-support test_insert_to_mutation -v`
Expected: PASS

- [ ] **Step 5: Add INSERT with USING TIMESTAMP and JSON INSERT error tests**

```rust
#[test]
fn test_insert_with_using_timestamp() {
    let schema = test_schema();
    let insert = CqlInsert {
        table: CqlTable {
            keyspace: None,
            name: CqlIdentifier { name: "test_tbl".into(), quoted: false },
        },
        columns: vec![
            CqlIdentifier { name: "id".into(), quoted: false },
            CqlIdentifier { name: "ts".into(), quoted: false },
            CqlIdentifier { name: "name".into(), quoted: false },
        ],
        values: CqlInsertValues::Values(vec![
            CqlExpression::Literal(CqlLiteral::Uuid("550e8400-e29b-41d4-a716-446655440000".into())),
            CqlExpression::Literal(CqlLiteral::Integer(1704067200000)),
            CqlExpression::Literal(CqlLiteral::String("Bob".into())),
        ]),
        if_not_exists: false,
        using: Some(CqlUsing {
            timestamp: Some(CqlExpression::Literal(CqlLiteral::Integer(1704067200000000))),
            ttl: None,
        }),
    };
    let mutation = insert_to_mutation(&insert, &schema).unwrap();
    assert_eq!(mutation.timestamp_micros, 1704067200000000);
}

#[test]
fn test_insert_json_unsupported() {
    let schema = test_schema();
    let insert = CqlInsert {
        table: CqlTable {
            keyspace: None,
            name: CqlIdentifier { name: "test_tbl".into(), quoted: false },
        },
        columns: vec![],
        values: CqlInsertValues::Json("{}".into()),
        if_not_exists: false,
        using: None,
    };
    let result = insert_to_mutation(&insert, &schema);
    assert!(result.is_err());
}

#[test]
fn test_insert_missing_partition_key() {
    let schema = test_schema();
    let insert = CqlInsert {
        table: CqlTable {
            keyspace: None,
            name: CqlIdentifier { name: "test_tbl".into(), quoted: false },
        },
        columns: vec![
            CqlIdentifier { name: "name".into(), quoted: false },
        ],
        values: CqlInsertValues::Values(vec![
            CqlExpression::Literal(CqlLiteral::String("Alice".into())),
        ]),
        if_not_exists: false,
        using: None,
    };
    let result = insert_to_mutation(&insert, &schema);
    assert!(result.is_err());
}
```

- [ ] **Step 6: Run all tests and commit**

Run: `cargo test --package cqlite-core --features write-support cql_to_mutation -v`

```bash
git add cqlite-core/src/storage/write_engine/cql_to_mutation.rs
git commit -m "feat(#446): implement INSERT-to-Mutation conversion with schema lookup"
```

---

### Task 4: Implement UPDATE-to-Mutation and DELETE-to-Mutation Conversion

**Files:**
- Modify: `cqlite-core/src/storage/write_engine/cql_to_mutation.rs`

- [ ] **Step 1: Write failing tests for UPDATE and DELETE**

```rust
#[test]
fn test_update_to_mutation() {
    let schema = test_schema();
    let update = CqlUpdate {
        table: CqlTable {
            keyspace: None,
            name: CqlIdentifier { name: "test_tbl".into(), quoted: false },
        },
        using: None,
        assignments: vec![
            CqlAssignment {
                column: CqlIdentifier { name: "name".into(), quoted: false },
                operator: CqlAssignmentOperator::Assign,
                value: CqlExpression::Literal(CqlLiteral::String("Updated".into())),
            },
        ],
        where_clause: CqlExpression::Binary {
            left: Box::new(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier { name: "id".into(), quoted: false })),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Literal(CqlLiteral::Uuid("550e8400-e29b-41d4-a716-446655440000".into()))),
            }),
            operator: CqlBinaryOperator::And,
            right: Box::new(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier { name: "ts".into(), quoted: false })),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(1704067200000))),
            }),
        },
        if_condition: None,
    };
    let mutation = update_to_mutation(&update, &schema).unwrap();
    assert_eq!(mutation.partition_key.columns.len(), 1);
    assert!(mutation.clustering_key.is_some());
    assert_eq!(mutation.operations.len(), 1);
    match &mutation.operations[0] {
        CellOperation::Write { column, value } => {
            assert_eq!(column, "name");
            assert_eq!(*value, Value::Text("Updated".into()));
        }
        _ => panic!("expected Write operation"),
    }
}

#[test]
fn test_delete_row_to_mutation() {
    let schema = test_schema();
    let delete = CqlDelete {
        columns: vec![], // Empty = full row delete
        table: CqlTable {
            keyspace: None,
            name: CqlIdentifier { name: "test_tbl".into(), quoted: false },
        },
        using: None,
        where_clause: CqlExpression::Binary {
            left: Box::new(CqlExpression::Column(CqlIdentifier { name: "id".into(), quoted: false })),
            operator: CqlBinaryOperator::Eq,
            right: Box::new(CqlExpression::Literal(CqlLiteral::Uuid("550e8400-e29b-41d4-a716-446655440000".into()))),
        },
        if_condition: None,
    };
    let mutation = delete_to_mutation(&delete, &schema).unwrap();
    assert_eq!(mutation.partition_key.columns.len(), 1);
    assert_eq!(mutation.operations.len(), 1);
    assert!(matches!(mutation.operations[0], CellOperation::DeleteRow));
}

#[test]
fn test_delete_columns_to_mutation() {
    let schema = test_schema();
    let delete = CqlDelete {
        columns: vec![
            CqlIdentifier { name: "name".into(), quoted: false },
            CqlIdentifier { name: "age".into(), quoted: false },
        ],
        table: CqlTable {
            keyspace: None,
            name: CqlIdentifier { name: "test_tbl".into(), quoted: false },
        },
        using: None,
        where_clause: CqlExpression::Binary {
            left: Box::new(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier { name: "id".into(), quoted: false })),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Literal(CqlLiteral::Uuid("550e8400-e29b-41d4-a716-446655440000".into()))),
            }),
            operator: CqlBinaryOperator::And,
            right: Box::new(CqlExpression::Binary {
                left: Box::new(CqlExpression::Column(CqlIdentifier { name: "ts".into(), quoted: false })),
                operator: CqlBinaryOperator::Eq,
                right: Box::new(CqlExpression::Literal(CqlLiteral::Integer(1704067200000))),
            }),
        },
        if_condition: None,
    };
    let mutation = delete_to_mutation(&delete, &schema).unwrap();
    assert_eq!(mutation.operations.len(), 2);
    assert!(matches!(&mutation.operations[0], CellOperation::Delete { column } if column == "name"));
    assert!(matches!(&mutation.operations[1], CellOperation::Delete { column } if column == "age"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --package cqlite-core --features write-support test_update_to_mutation test_delete -v`
Expected: FAIL

- [ ] **Step 3: Implement `update_to_mutation`**

```rust
/// Convert a parsed CQL UPDATE statement to a Mutation.
#[cfg(feature = "write-support")]
pub(crate) fn update_to_mutation(
    update: &CqlUpdate,
    schema: &TableSchema,
) -> Result<Mutation, Error> {
    validate_table(&update.table, schema)?;

    // Extract key values from WHERE clause
    let where_bindings = extract_where_bindings(&update.where_clause)?;
    let (pk_values, ck_values) = resolve_key_bindings(&where_bindings, schema)?;

    // Convert SET assignments to CellOperations
    let operations = update.assignments.iter()
        .map(|assign| {
            let column_def = schema.get_column(&assign.column.name)
                .ok_or_else(|| Error::InvalidInput(format!("Unknown column: {}", assign.column.name)))?;
            let cql_type = CqlType::parse(&column_def.data_type)?;
            let value = expression_to_value(&assign.value, &cql_type)?;
            Ok(CellOperation::Write {
                column: assign.column.name.clone(),
                value,
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;

    let timestamp_micros = extract_timestamp(&update.using)?;
    let ttl_seconds = extract_ttl(&update.using)?;

    Ok(Mutation::new(
        TableId::new(schema.keyspace.clone(), schema.table.clone()),
        PartitionKey { columns: pk_values },
        if ck_values.is_empty() { None } else { Some(ClusteringKey { columns: ck_values }) },
        operations,
        timestamp_micros,
        ttl_seconds,
    ))
}
```

- [ ] **Step 4: Implement `delete_to_mutation`**

```rust
/// Convert a parsed CQL DELETE statement to a Mutation.
#[cfg(feature = "write-support")]
pub(crate) fn delete_to_mutation(
    delete: &CqlDelete,
    schema: &TableSchema,
) -> Result<Mutation, Error> {
    validate_table(&delete.table, schema)?;

    let where_bindings = extract_where_bindings(&delete.where_clause)?;
    let (pk_values, ck_values) = resolve_key_bindings(&where_bindings, schema)?;

    let operations = if delete.columns.is_empty() {
        vec![CellOperation::DeleteRow]
    } else {
        delete.columns.iter()
            .map(|col| CellOperation::Delete { column: col.name.clone() })
            .collect()
    };

    let timestamp_micros = extract_timestamp(&delete.using)?;

    Ok(Mutation::new(
        TableId::new(schema.keyspace.clone(), schema.table.clone()),
        PartitionKey { columns: pk_values },
        if ck_values.is_empty() { None } else { Some(ClusteringKey { columns: ck_values }) },
        operations,
        timestamp_micros,
        None, // DELETE doesn't use TTL
    ))
}
```

- [ ] **Step 5: Implement `extract_where_bindings` and `resolve_key_bindings` helpers**

```rust
/// Extract column = value bindings from a WHERE clause.
/// Only supports AND-chained equality predicates (col = literal).
#[cfg(feature = "write-support")]
fn extract_where_bindings(expr: &CqlExpression) -> Result<Vec<(String, CqlExpression)>, Error> {
    let mut bindings = Vec::new();
    collect_equality_bindings(expr, &mut bindings)?;
    Ok(bindings)
}

#[cfg(feature = "write-support")]
fn collect_equality_bindings(
    expr: &CqlExpression,
    bindings: &mut Vec<(String, CqlExpression)>,
) -> Result<(), Error> {
    match expr {
        CqlExpression::Binary { left, operator: CqlBinaryOperator::And, right } => {
            collect_equality_bindings(left, bindings)?;
            collect_equality_bindings(right, bindings)?;
        }
        CqlExpression::Binary { left, operator: CqlBinaryOperator::Eq, right } => {
            if let CqlExpression::Column(col) = left.as_ref() {
                bindings.push((col.name.clone(), *right.clone()));
            } else {
                return Err(Error::InvalidInput(
                    "WHERE clause must use column = value format".into()
                ));
            }
        }
        _ => {
            return Err(Error::InvalidInput(
                "WHERE clause only supports AND-chained equality predicates".into()
            ));
        }
    }
    Ok(())
}

/// Resolve WHERE bindings into partition key and clustering key values.
#[cfg(feature = "write-support")]
fn resolve_key_bindings(
    bindings: &[(String, CqlExpression)],
    schema: &TableSchema,
) -> Result<(Vec<(String, Value)>, Vec<(String, Value)>), Error> {
    let pk_names: Vec<String> = schema.ordered_partition_keys()
        .iter().map(|k| k.name.clone()).collect();

    let mut pk_values = Vec::new();
    for pk_name in &pk_names {
        let (_, expr) = bindings.iter()
            .find(|(name, _)| name == pk_name)
            .ok_or_else(|| Error::InvalidInput(format!("Missing partition key in WHERE: {}", pk_name)))?;
        let pk_col = schema.partition_keys.iter().find(|k| &k.name == pk_name).unwrap();
        let cql_type = CqlType::parse(&pk_col.data_type)?;
        let value = expression_to_value(expr, &cql_type)?;
        pk_values.push((pk_name.clone(), value));
    }

    let ck_names: Vec<String> = schema.ordered_clustering_keys()
        .iter().map(|k| k.name.clone()).collect();

    let mut ck_values = Vec::new();
    for ck_name in &ck_names {
        if let Some((_, expr)) = bindings.iter().find(|(name, _)| name == ck_name) {
            let ck_col = schema.clustering_keys.iter().find(|k| &k.name == ck_name).unwrap();
            let cql_type = CqlType::parse(&ck_col.data_type)?;
            let value = expression_to_value(expr, &cql_type)?;
            ck_values.push((ck_name.clone(), value));
        }
    }

    Ok((pk_values, ck_values))
}
```

- [ ] **Step 6: Run all tests and commit**

Run: `cargo test --package cqlite-core --features write-support cql_to_mutation -v`

```bash
git add cqlite-core/src/storage/write_engine/cql_to_mutation.rs
git commit -m "feat(#446): implement UPDATE and DELETE to Mutation conversion"
```

---

### Task 5: Wire Up `parse_cql_to_mutation` in WriteEngine

**Files:**
- Modify: `cqlite-core/src/storage/write_engine/mod.rs` (replace stub at line 658-665)

- [ ] **Step 1: Write integration test for `WriteEngine::execute()`**

Add to the bottom of `cql_to_mutation.rs`:

```rust
#[test]
fn test_convert_cql_insert_string() {
    let schema = test_schema();
    let sql = "INSERT INTO test_ks.test_tbl (id, ts, name) VALUES (550e8400-e29b-41d4-a716-446655440000, 1704067200000, 'Alice')";
    let result = convert_cql_to_mutation(sql, &schema);
    assert!(result.is_ok(), "Failed: {:?}", result.err());
    let mutation = result.unwrap();
    assert_eq!(mutation.table.keyspace, "test_ks");
    assert_eq!(mutation.operations.len(), 1); // only 'name' is non-key
}

#[test]
fn test_convert_cql_update_string() {
    let schema = test_schema();
    let sql = "UPDATE test_ks.test_tbl SET name = 'Bob' WHERE id = 550e8400-e29b-41d4-a716-446655440000 AND ts = 1704067200000";
    let result = convert_cql_to_mutation(sql, &schema);
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_convert_cql_delete_string() {
    let schema = test_schema();
    let sql = "DELETE FROM test_ks.test_tbl WHERE id = 550e8400-e29b-41d4-a716-446655440000";
    let result = convert_cql_to_mutation(sql, &schema);
    assert!(result.is_ok(), "Failed: {:?}", result.err());
}

#[test]
fn test_convert_unsupported_statement() {
    let schema = test_schema();
    let sql = "SELECT * FROM test_ks.test_tbl";
    let result = convert_cql_to_mutation(sql, &schema);
    assert!(result.is_err());
}
```

- [ ] **Step 2: Implement `convert_cql_to_mutation` - the top-level bridge function**

```rust
/// Convert a CQL mutation statement string to a Mutation struct.
///
/// Supports INSERT, UPDATE, and DELETE statements.
/// The statement is parsed using the existing CQL parser, then converted
/// to a Mutation using the provided schema for type resolution.
#[cfg(feature = "write-support")]
pub(crate) fn convert_cql_to_mutation(
    statement: &str,
    schema: &TableSchema,
) -> Result<Mutation, Error> {
    let trimmed = statement.trim();
    let upper = trimmed.to_uppercase();

    if upper.starts_with("INSERT") {
        let insert = crate::cql::mutation_parser::parse_insert_statement(trimmed)?;
        insert_to_mutation(&insert, schema)
    } else if upper.starts_with("UPDATE") {
        let update = crate::cql::mutation_parser::parse_update_statement(trimmed)?;
        update_to_mutation(&update, schema)
    } else if upper.starts_with("DELETE") {
        let delete = crate::cql::mutation_parser::parse_delete_statement(trimmed)?;
        delete_to_mutation(&delete, schema)
    } else {
        Err(Error::InvalidInput(format!(
            "Unsupported mutation statement. Expected INSERT, UPDATE, or DELETE: {}",
            &trimmed[..trimmed.len().min(50)]
        )))
    }
}
```

- [ ] **Step 3: Replace the stub in `mod.rs`**

Replace lines 654-665 in `cqlite-core/src/storage/write_engine/mod.rs`:

Old:
```rust
fn parse_cql_to_mutation(&self, statement: &str) -> Result<Mutation> {
    // TODO: Full CQL parser integration in M5.0-8
    // For now, return error to indicate not implemented
    Err(Error::InvalidInput(format!(
        "CQL parsing not yet implemented: {}",
        statement
    )))
}
```

New:
```rust
fn parse_cql_to_mutation(&self, statement: &str) -> Result<Mutation> {
    cql_to_mutation::convert_cql_to_mutation(statement, &self.config.schema)
}
```

- [ ] **Step 4: Run all tests**

Run: `cargo test --package cqlite-core --features write-support cql_to_mutation -v`
Run: `cargo test --package cqlite-core --features write-support write_engine -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add cqlite-core/src/storage/write_engine/mod.rs cqlite-core/src/storage/write_engine/cql_to_mutation.rs
git commit -m "feat(#446): wire CQL parser to WriteEngine via convert_cql_to_mutation"
```

---

### Task 6: Clippy, Format, and Final Validation

**Files:**
- All modified files

- [ ] **Step 1: Run cargo fmt**

Run: `cargo fmt`

- [ ] **Step 2: Run clippy with CI flags**

Run: `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features`
Expected: No warnings

- [ ] **Step 3: Run full test suite**

Run: `env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test --package cqlite-core --features write-support`
Expected: All tests pass

- [ ] **Step 4: Fix any issues found**

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "chore(#446): clippy and formatting fixes"
```

---

## Verification Checklist

- [ ] `literal_to_value` handles all primitive CQL types, collections, tuples, UDTs, frozen
- [ ] `insert_to_mutation` separates PK/CK/regular columns correctly
- [ ] `update_to_mutation` extracts keys from WHERE, assignments from SET
- [ ] `delete_to_mutation` handles row deletes and column deletes
- [ ] `convert_cql_to_mutation` dispatches INSERT/UPDATE/DELETE correctly
- [ ] `parse_cql_to_mutation` stub replaced with real implementation
- [ ] USING TIMESTAMP and USING TTL supported
- [ ] Error messages are clear for: unknown columns, missing PK, type mismatches
- [ ] All code behind `#[cfg(feature = "write-support")]`
- [ ] `cargo fmt` clean
- [ ] `RUSTFLAGS="-D warnings" cargo clippy` clean
- [ ] All tests pass
