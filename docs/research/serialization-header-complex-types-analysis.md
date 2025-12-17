# SerializationHeader Complex Type Parsing Analysis

**Date**: 2025-12-17
**Analyst**: Claude (SSTable Developer)
**Related Issues**: #210 (Frozen collections), #163 (SerializationHeader parsing)
**Related Files**:
- `/Users/patrick/local_projects/cqlite/cqlite-core/src/parser/enhanced_statistics_parser.rs`
- `/Users/patrick/local_projects/cqlite/docs/research/serialization-header-format.md`

---

## Executive Summary

The current SerializationHeader parser in `enhanced_statistics_parser.rs` has a **critical flaw** in the `convert_marshal_type_to_cql()` function that causes it to fail for complex collection types. The function uses **naive string manipulation** (simple prefix matching and `trim_end_matches(')')`) which breaks when parsing nested or parameterized types.

**Impact**: 3 failing tables in test_collections keyspace
- `frozen_collections_table`
- `typed_collections_table`
- `nested_collections_table`

**Root Cause**: Type string parser does not properly handle:
1. Nested parentheses in parameterized types
2. Comma-separated arguments in maps and composite types
3. Recursive type parameter parsing

---

## Current Implementation Analysis

### Location
File: `/Users/patrick/local_projects/cqlite/cqlite-core/src/parser/enhanced_statistics_parser.rs`
Function: `convert_marshal_type_to_cql()` (lines 1109-1205)

### Current Approach

The function uses a **string prefix/suffix stripping strategy**:

```rust
fn convert_marshal_type_to_cql(marshal_type: &str) -> String {
    // Strip wrapping parentheses
    let mut cleaned = strip_wrapping_parens(marshal_type);

    // Try ReversedType wrapper
    for prefix in ["org.apache.cassandra.db.marshal.ReversedType(", "ReversedType("] {
        if let Some(inner) = cleaned.strip_prefix(prefix) {
            let inner = inner.trim_end_matches(')');  // ❌ BUG: Naive!
            return convert_marshal_type_to_cql(inner);
        }
    }

    // Try FrozenType wrapper
    for prefix in ["org.apache.cassandra.db.marshal.FrozenType(", "FrozenType("] {
        if let Some(inner) = cleaned.strip_prefix(prefix) {
            let inner = inner.trim_end_matches(')');  // ❌ BUG: Naive!
            return format!("frozen<{}>", convert_marshal_type_to_cql(inner));
        }
    }

    // Similar pattern for ListType, SetType, MapType...
}
```

### Why This Fails

#### Problem 1: `trim_end_matches(')')` is Not Context-Aware

**Example input**:
```
org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))
```

**What happens**:
1. Strip prefix `FrozenType(` → `org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))`
2. Call `trim_end_matches(')')` → **REMOVES BOTH CLOSING PARENS** → `org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type`
3. Recursive call tries to parse `org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type` (missing closing paren)
4. **Parser fails** because string is malformed

**Expected behavior**: Remove only the **matching** closing paren for the outer `FrozenType(...)`, leaving inner type intact.

#### Problem 2: `split_type_arguments()` Doesn't Handle Nesting

**Current implementation** (lines 1077-1106):
```rust
fn split_type_arguments(input: &str) -> Vec<&str> {
    let mut args = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (idx, ch) in input.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => if depth > 0 { depth -= 1; }
            ',' if depth == 0 => {  // Only split at top-level commas
                let part = input[start..idx].trim();
                if !part.is_empty() {
                    args.push(part);
                }
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    // Add final argument
    let tail = input[start..].trim();
    if !tail.is_empty() {
        args.push(tail);
    }
    args
}
```

This function **correctly** handles nested parens when splitting commas. However:
1. It's **only called for MapType** parsing (line 1166)
2. It's **NOT used** for FrozenType, ListType, SetType parsing
3. Those types use `trim_end_matches(')')` instead, which fails

#### Problem 3: Composite Type Parsing Missing

**Type string format**:
```
org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)
```

**Current behavior**: Falls through to simple type matcher, returns `"compositetype"` (wrong!)

**Expected**: Should parse as compound partition key with multiple component types.

#### Problem 4: UDT Parsing Not Implemented

**Type string format**:
```
org.apache.cassandra.db.marshal.UserType(my_keyspace,616464726573735F74797065,737472656574:org.apache.cassandra.db.marshal.UTF8Type,63697479:org.apache.cassandra.db.marshal.UTF8Type)
```

**Current behavior**: Falls through to simple type matcher, returns `"usertype"` (wrong!)

**Expected**: Should parse keyspace, decode hex type name, parse field definitions.

---

## Correct SerializationHeader Format (from Cassandra Source)

### Binary Layout

Based on `SerializationHeader.Serializer.serialize()` (SerializationHeader.java:544-553):

```java
public void serialize(Version version, Component header, DataOutputPlus out) throws IOException
{
    EncodingStats.serializer.serialize(header.stats, out);

    typeSerializer.serialize(header.keyType, out);
    typeSerializer.serializeList(header.clusteringTypes, out);

    writeColumnsWithTypes(header.staticColumns, out);
    writeColumnsWithTypes(header.regularColumns, out);
}
```

**Actual binary structure**:
```
[EncodingStats]
  ├── minTimestamp: VInt (delta from epoch)
  ├── minLocalDeletionTime: VInt (delta from epoch)
  └── minTTL: VInt (delta from epoch)

[KeyType] (AbstractTypeSerializer)
  ├── length: VInt
  └── typeString: UTF-8 bytes

[ClusteringTypes]
  ├── count: VInt
  └── [Type] * count (each using AbstractTypeSerializer)

[StaticColumns] (writeColumnsWithTypes)
  ├── count: VInt
  └── [Column] * count
      ├── nameLength: VInt
      ├── name: UTF-8 bytes
      ├── typeLength: VInt
      └── typeString: UTF-8 bytes

[RegularColumns] (same format as StaticColumns)
```

### AbstractTypeSerializer Format

From `AbstractTypeSerializer.java` (lines 36-39):

```java
public void serialize(AbstractType<?> type, DataOutputPlus out) throws IOException
{
    ByteBufferUtil.writeWithVIntLength(UTF8Type.instance.decompose(type.toString()), out);
}
```

**Key insight**: Types are serialized as **VInt-length-prefixed UTF-8 strings** of the Java `type.toString()` output.

This means:
- Collection types like `ListType` serialize as `"org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)"`
- The **entire type descriptor** is stored as a single string, not parsed into binary components

---

## Type String Grammar (from Cassandra TypeParser.java)

### Grammar Rules

**Simple Type**:
```
<namespace>.<ClassName>
Example: org.apache.cassandra.db.marshal.Int32Type
```

**Parameterized Type**:
```
<TypeName>(<Parameters>)
Example: ListType(Int32Type)
```

**Nested Type**:
```
<OuterType>(<InnerType>(<...>))
Example: FrozenType(ListType(Int32Type))
```

**Multi-Parameter Type (Map)**:
```
MapType(<KeyType>,<ValueType>)
Example: MapType(UTF8Type,Int32Type)
```

**Composite Type**:
```
CompositeType(<Type1>,<Type2>,...)
Example: CompositeType(UTF8Type,Int32Type,UUIDType)
```

**UDT Type**:
```
UserType(<keyspace>,<hex_type_name>,<hex_field1>:<field1_type>,<hex_field2>:<field2_type>,...)
Example: UserType(ks,616464726573735F74797065,737472656574:UTF8Type)
```

### Parsing Strategy

From `TypeParser.parse()` (TypeParser.java:60-165):

```java
public static <T> AbstractType<T> parse(String compareWith) throws SyntaxException, ConfigurationException
{
    // 1. Check for empty or special types
    if (compareWith == null || compareWith.isEmpty())
        return BytesType.instance;

    // 2. Extract type name and parameters
    Pair<String, String> name = getTypeName(compareWith);
    String className = name.left;
    String parameters = name.right;

    // 3. Lookup type class
    AbstractType<?> type = getAbstractType(className);

    // 4. If parameterized, parse parameters
    if (parameters != null)
        type = type.getSerializer().deserialize(parameters);

    return (AbstractType<T>) type;
}
```

Key helper: `getTypeName()` (lines 252-297)
- Finds matching parentheses using **depth tracking**
- Splits `"ClassName(params)"` into `("ClassName", "params")`
- Handles nested parens correctly

---

## Where Current Implementation Fails

### Test Case 1: Frozen List

**Input type string**:
```
org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))
```

**Expected output**: `"frozen<list<int>>"`

**Current execution**:
1. Strip prefix `FrozenType(` → `org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))`
2. `trim_end_matches(')')` → **Removes TWO parens** → `org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type`
3. Recursive call with malformed string
4. **Fails to match any prefix** (no trailing paren)
5. Falls through to simple type matcher
6. Returns `"listtype(org.apache.cassandra.db.marshal.int32type"` (wrong!)

### Test Case 2: Map Type

**Input type string**:
```
org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)
```

**Expected output**: `"map<text, int>"`

**Current execution**:
1. Strip prefix `MapType(` → `org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)`
2. `trim_end_matches(')')` → `org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type`
3. Call `split_type_arguments()` on **correctly stripped string**
4. Returns `["org.apache.cassandra.db.marshal.UTF8Type", "org.apache.cassandra.db.marshal.Int32Type"]`
5. Recursive calls convert each to `"text"` and `"int"`
6. Returns `"map<text, int>"` ✅ **SUCCESS**

**Why it works**: MapType parser calls `split_type_arguments()` AFTER stripping the closing paren.

### Test Case 3: Nested Frozen Map

**Input type string**:
```
org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)))
```

**Expected output**: `"frozen<map<text, list<int>>>"`

**Current execution**:
1. Strip prefix `FrozenType(` → `org.apache.cassandra.db.marshal.MapType(...MapType(...ListType(...)...)...))`
2. `trim_end_matches(')')` → **Removes ALL THREE closing parens** → `org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type`
3. Recursive call with malformed string
4. **Fails completely**

---

## Correct Implementation Approach

### Strategy 1: Proper Parameter Extraction

Instead of `trim_end_matches(')')`, use **paren-matching logic**:

```rust
fn extract_type_parameters(type_string: &str) -> Option<(&str, &str)> {
    // Find the opening paren
    let open_pos = type_string.find('(')?;

    // Find the MATCHING closing paren using depth tracking
    let mut depth = 0;
    let mut close_pos = None;

    for (idx, ch) in type_string[open_pos..].char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    close_pos = Some(open_pos + idx);
                    break;
                }
            }
            _ => {}
        }
    }

    let close_pos = close_pos?;

    // Extract type name and parameters
    let type_name = &type_string[..open_pos];
    let params = &type_string[open_pos + 1..close_pos];

    Some((type_name, params))
}
```

**Usage**:
```rust
// FrozenType parsing
if let Some(inner) = cleaned.strip_prefix("FrozenType") {
    if let Some((_, params)) = extract_type_parameters(inner) {
        return format!("frozen<{}>", convert_marshal_type_to_cql(params));
    }
}
```

### Strategy 2: Recursive Type Parser

Build a proper **recursive descent parser**:

```rust
pub enum CassandraType {
    Simple(String),                                  // Int32Type, UTF8Type
    List(Box<CassandraType>),                       // ListType(T)
    Set(Box<CassandraType>),                        // SetType(T)
    Map(Box<CassandraType>, Box<CassandraType>),   // MapType(K,V)
    Frozen(Box<CassandraType>),                     // FrozenType(T)
    Reversed(Box<CassandraType>),                   // ReversedType(T)
    Composite(Vec<CassandraType>),                  // CompositeType(T1,T2,...)
    Tuple(Vec<CassandraType>),                      // TupleType(T1,T2,...)
    UserDefined {
        keyspace: String,
        name: String,                               // Decoded from hex
        fields: Vec<(String, CassandraType)>,       // (name, type)
    },
}

impl CassandraType {
    pub fn parse(type_string: &str) -> Result<Self> {
        // Strip "org.apache.cassandra.db.marshal." prefix
        let s = type_string
            .strip_prefix("org.apache.cassandra.db.marshal.")
            .unwrap_or(type_string);

        // Check if parameterized
        if let Some((type_name, params)) = extract_type_parameters(s) {
            match type_name {
                "ListType" => {
                    let element = Self::parse(params)?;
                    Ok(CassandraType::List(Box::new(element)))
                }
                "SetType" => {
                    let element = Self::parse(params)?;
                    Ok(CassandraType::Set(Box::new(element)))
                }
                "MapType" => {
                    let args = split_type_arguments(params);
                    if args.len() != 2 {
                        return Err(Error::parse("MapType requires 2 args"));
                    }
                    let key = Self::parse(args[0])?;
                    let value = Self::parse(args[1])?;
                    Ok(CassandraType::Map(Box::new(key), Box::new(value)))
                }
                "FrozenType" => {
                    let inner = Self::parse(params)?;
                    Ok(CassandraType::Frozen(Box::new(inner)))
                }
                "ReversedType" => {
                    let inner = Self::parse(params)?;
                    Ok(CassandraType::Reversed(Box::new(inner)))
                }
                "CompositeType" => {
                    let args = split_type_arguments(params);
                    let types = args.iter()
                        .map(|s| Self::parse(s))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(CassandraType::Composite(types))
                }
                "TupleType" => {
                    let args = split_type_arguments(params);
                    let types = args.iter()
                        .map(|s| Self::parse(s))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(CassandraType::Tuple(types))
                }
                "UserType" => {
                    Self::parse_udt(params)
                }
                _ => Err(Error::unsupported(format!("Unknown type: {}", type_name)))
            }
        } else {
            // Simple type
            Ok(CassandraType::Simple(s.to_string()))
        }
    }

    pub fn to_cql_string(&self) -> String {
        match self {
            CassandraType::Simple(s) => {
                // Strip "Type" suffix and convert to lowercase
                let base = s.trim_end_matches("Type");
                match base {
                    "Int32" | "Integer" => "int".to_string(),
                    "UTF8" => "text".to_string(),
                    "Long" => "bigint".to_string(),
                    "UUID" => "uuid".to_string(),
                    "Timestamp" => "timestamp".to_string(),
                    // ... etc ...
                    _ => base.to_lowercase()
                }
            }
            CassandraType::List(inner) => {
                format!("list<{}>", inner.to_cql_string())
            }
            CassandraType::Set(inner) => {
                format!("set<{}>", inner.to_cql_string())
            }
            CassandraType::Map(key, value) => {
                format!("map<{}, {}>", key.to_cql_string(), value.to_cql_string())
            }
            CassandraType::Frozen(inner) => {
                format!("frozen<{}>", inner.to_cql_string())
            }
            CassandraType::Reversed(inner) => {
                // Reversed is a storage hint, not visible in CQL
                inner.to_cql_string()
            }
            CassandraType::Composite(types) => {
                // Composite is for compound partition keys
                // In CQL, this is represented as multiple columns
                // For now, return comma-separated list
                types.iter()
                    .map(|t| t.to_cql_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            }
            CassandraType::Tuple(types) => {
                let inner = types.iter()
                    .map(|t| t.to_cql_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("tuple<{}>", inner)
            }
            CassandraType::UserDefined { keyspace, name, .. } => {
                format!("{}.{}", keyspace, name)
            }
        }
    }
}
```

### Strategy 3: Use Existing `split_type_arguments()` Everywhere

The existing `split_type_arguments()` function (lines 1077-1106) **correctly handles nested parens**.

**Fix**: Use it for **ALL** parameterized types, not just MapType:

```rust
// FrozenType parsing - CORRECTED
for prefix in ["org.apache.cassandra.db.marshal.FrozenType(", "FrozenType("] {
    if let Some(params_with_paren) = cleaned.strip_prefix(prefix) {
        // Extract parameters up to matching closing paren
        if let Some(close_idx) = find_matching_paren(params_with_paren) {
            let params = &params_with_paren[..close_idx];
            return format!("frozen<{}>", convert_marshal_type_to_cql(params));
        }
    }
}

fn find_matching_paren(s: &str) -> Option<usize> {
    let mut depth = 1;  // We're already inside one opening paren
    for (idx, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(idx);
                }
            }
            _ => {}
        }
    }
    None
}
```

---

## Implementation Recommendations

### Option A: Minimal Fix (Quick, Low Risk)

**Target**: Fix only the `trim_end_matches(')')` bug

1. Add `find_matching_paren()` helper function
2. Replace all `trim_end_matches(')')` calls with proper paren matching
3. Add test cases for nested frozen types
4. Estimated effort: **2-3 hours**

**Pros**:
- Minimal code change
- Low regression risk
- Quick to implement

**Cons**:
- Still doesn't handle UDTs, Composite types, Vectors
- Parsing logic remains ad-hoc

### Option B: Full Type Parser (Robust, Future-Proof)

**Target**: Replace string manipulation with proper recursive parser

1. Create new module: `cqlite-core/src/parser/cassandra_type.rs`
2. Implement `CassandraType` enum and `parse()` method
3. Replace `convert_marshal_type_to_cql()` with `CassandraType::parse().to_cql_string()`
4. Add comprehensive test suite with all type variations
5. Estimated effort: **1-2 days**

**Pros**:
- Handles ALL Cassandra types (UDTs, vectors, composites)
- Maintainable and extensible
- Aligns with Cassandra's TypeParser architecture
- Enables future schema-aware parsing

**Cons**:
- More code to write and test
- Larger PR to review
- Potential for new bugs during refactor

### Option C: Hybrid Approach (Recommended)

**Target**: Fix current bugs + add proper type model

1. **Phase 1** (immediate): Add `find_matching_paren()` to fix frozen types
2. **Phase 2** (M2+): Implement full `CassandraType` parser in separate module
3. **Phase 3** (M3+): Migrate all type parsing to new module

**Pros**:
- Quick fix for current failures
- Incremental path to robust solution
- Can validate new parser against old before switching

**Cons**:
- Two implementations exist temporarily

---

## Test Cases for Validation

### Test Data Sources

1. **Real SSTable files**: `test-data/datasets/sstables/test_collections/`
   - `frozen_collections_table`: Frozen lists, sets, maps
   - `typed_collections_table`: Complex type combinations
   - `nested_collections_table`: Multi-level nesting

2. **Cassandra sstabledump output**: Ground truth for expected CQL types

### Unit Test Cases

```rust
#[test]
fn test_frozen_list_parsing() {
    let input = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))";
    assert_eq!(convert_marshal_type_to_cql(input), "frozen<list<int>>");
}

#[test]
fn test_frozen_map_parsing() {
    let input = "org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type))";
    assert_eq!(convert_marshal_type_to_cql(input), "frozen<map<text, int>>");
}

#[test]
fn test_nested_frozen_list_of_maps() {
    let input = "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)))";
    assert_eq!(convert_marshal_type_to_cql(input), "list<frozen<map<text, int>>>");
}

#[test]
fn test_composite_type_parsing() {
    let input = "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)";
    let result = convert_marshal_type_to_cql(input);
    // Should handle compound keys properly
    assert!(result.contains("text") && result.contains("int"));
}

#[test]
fn test_tuple_type_parsing() {
    let input = "org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UUIDType)";
    assert_eq!(convert_marshal_type_to_cql(input), "tuple<int, text, uuid>");
}
```

### Integration Test

```rust
#[test]
fn test_frozen_collections_table_schema_extraction() {
    let datasets_root = std::env::var("CQLITE_DATASETS_ROOT").expect("CQLITE_DATASETS_ROOT not set");
    let table_path = format!("{}/sstables/test_collections/frozen_collections_table-*/nb-1-big-Statistics.db", datasets_root);

    // Parse Statistics.db
    let stats = parse_statistics_file(&table_path).expect("Failed to parse Statistics.db");

    // Verify frozen list column type
    let frozen_list_col = stats.serialization_header_columns.iter()
        .find(|c| c.name == "col_frozen_list")
        .expect("col_frozen_list not found");
    assert_eq!(frozen_list_col.column_type, "frozen<list<int>>");

    // Verify frozen map column type
    let frozen_map_col = stats.serialization_header_columns.iter()
        .find(|c| c.name == "col_frozen_map")
        .expect("col_frozen_map not found");
    assert_eq!(frozen_map_col.column_type, "frozen<map<text, int>>");
}
```

---

## Cassandra Source Code References

### Key Files for Type Parsing

1. **TypeParser.java** (`/src/java/org/apache/cassandra/db/marshal/TypeParser.java`)
   - Lines 60-165: `parse()` method
   - Lines 252-297: `getTypeName()` - paren matching logic
   - Lines 670-684: `stringifyUserTypeParameters()` - UDT format

2. **AbstractTypeSerializer.java** (`/src/java/org/apache/cassandra/serializers/AbstractTypeSerializer.java`)
   - Lines 36-39: `serialize()` - stores type.toString()
   - Lines 48-52: `deserialize()` - calls TypeParser.parse()

3. **ListType.java** (`/src/java/org/apache/cassandra/db/marshal/ListType.java`)
   - Lines 202-214: `toString()` - includes FrozenType wrapper if !isMultiCell()

4. **MapType.java** (`/src/java/org/apache/cassandra/db/marshal/MapType.java`)
   - Lines 300-313: `toString()` - MapType(K,V) format

5. **UserType.java** (`/src/java/org/apache/cassandra/db/marshal/UserType.java`)
   - Lines 620-632: `toString()` - hex-encoded field names

### Cassandra TypeParser Algorithm

From `TypeParser.getTypeName()` (lines 252-297):

```java
private static Pair<String, String> getTypeName(String compareWith) throws SyntaxException
{
    boolean isParameterized = false;
    int depth = 0;
    StringBuilder name = new StringBuilder();
    StringBuilder params = null;

    for (int i = 0; i < compareWith.length(); i++)
    {
        char c = compareWith.charAt(i);
        switch (c)
        {
            case '(':
                isParameterized = true;
                if (depth == 0)
                {
                    params = new StringBuilder();
                }
                else
                {
                    params.append(c);
                }
                depth++;
                break;
            case ')':
                depth--;
                if (depth > 0)
                    params.append(c);
                else if (depth < 0)
                    throw new SyntaxException("Unexpected ')'");
                break;
            default:
                if (isParameterized)
                    params.append(c);
                else
                    name.append(c);
        }
    }

    if (depth != 0)
        throw new SyntaxException("Unmatched parentheses");

    return Pair.create(name.toString(), params == null ? null : params.toString());
}
```

**Key insight**: Cassandra uses **depth tracking with StringBuilder** to extract parameters without modifying the original string.

---

## Action Items

### Immediate (Next PR)

1. ✅ Document the bug (this file)
2. Implement `find_matching_paren()` helper
3. Fix `FrozenType`, `ListType`, `SetType` parsing
4. Add unit tests for nested frozen types
5. Validate against `frozen_collections_table`

### Short-term (M2)

1. Implement full `CassandraType` parser in new module
2. Add support for CompositeType, TupleType
3. Add basic UDT parsing (keyspace + name only)
4. Migrate existing code to use new parser

### Long-term (M3+)

1. Full UDT field parsing with hex decoding
2. VectorType support (Cassandra 5.0+)
3. Type validation against actual Data.db values
4. Schema inference from SerializationHeader alone (no external schema files)

---

## References

### Implementation Files (CQLite)

- `/Users/patrick/local_projects/cqlite/cqlite-core/src/parser/enhanced_statistics_parser.rs` (lines 1109-1205)
- `/Users/patrick/local_projects/cqlite/docs/research/serialization-header-format.md`
- `/Users/patrick/local_projects/cqlite/docs/sstables-definitive-guide/chapters/08-statistics-db.md`
- `/Users/patrick/local_projects/cqlite/docs/sstables-definitive-guide/ISSUE_162_LEARNINGS.md`

### Cassandra Source Files

- `/Users/patrick/local_projects/cassandra/src/java/org/apache/cassandra/db/SerializationHeader.java`
- `/Users/patrick/local_projects/cassandra/src/java/org/apache/cassandra/serializers/AbstractTypeSerializer.java`
- `/Users/patrick/local_projects/cassandra/src/java/org/apache/cassandra/db/marshal/TypeParser.java`

### Test Data

- `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_collections/frozen_collections_table-*/`
- `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_collections/typed_collections_table-*/`
- `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_collections/nested_collections_table-*/`

---

**Document Version**: 1.0
**Status**: Complete - Ready for implementation
**Reviewed By**: (Pending)
