# Issue #240: DATE Type Comparator Research Report

**Date**: 2026-01-06
**Focus**: Current state of DATE type handling in CQLite comparator system
**File Analyzed**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/types/comparator.rs`

---

## Executive Summary

**Current State**: DATE type is mapped to `ComparatorType::Custom("date")` fallback, lacking native comparison support.

**Impact**: DATE columns cannot be properly compared, ordered, or used in WHERE clauses with comparison operators.

**Solution Required**: Add `ComparatorType::Date` variant with proper i32 comparison logic (days since Unix epoch).

---

## 1. ComparatorType Enum Analysis

**Location**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/types/comparator.rs:16-67`

### Current Variants (46 total)

```rust
pub enum ComparatorType {
    // Primitive types (17)
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Counter,
    Float32,
    Float,
    Text,
    Blob,
    Timestamp,      // ✓ Temporal type (i64 - milliseconds)
    Uuid,
    Varint,
    Decimal,
    Duration,
    Json,

    // Collection types (3)
    List(Box<ComparatorType>),
    Set(Box<ComparatorType>),
    Map(Box<ComparatorType>, Box<ComparatorType>),

    // Complex types (4)
    Tuple(Vec<ComparatorType>),
    Udt { type_name: String, keyspace: Option<String>, field_comparators: Vec<(String, ComparatorType)> },
    Frozen(Box<ComparatorType>),
    Custom(String),  // ⚠️ DATE currently falls back here
}
```

### Missing Variants

**DATE is NOT present** as a dedicated variant. Other missing temporal types:
- `Date` - Days since epoch (i32) - **Issue #240 target**
- `Time` - Nanoseconds since midnight (i64)

---

## 2. CqlType::Date Mapping

### Primary Mapping (Line 129)

**Location**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/types/comparator.rs:129`

```rust
pub fn from_cql_type(cql_type: &CqlType) -> Result<Self> {
    let comparator = match cql_type {
        // ... other types ...
        CqlType::Timestamp => ComparatorType::Timestamp,  // ✓ Has native support
        // ...
        CqlType::Date => ComparatorType::Custom("date".to_string()),  // ⚠️ FALLBACK
        CqlType::Time => ComparatorType::Custom("time".to_string()),  // ⚠️ FALLBACK
        CqlType::Inet => ComparatorType::Custom("inet".to_string()),  // ⚠️ FALLBACK
    };
    Ok(comparator)
}
```

### Secondary Mapping with UDT Registry (Line 191)

**Location**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/types/comparator.rs:191`

```rust
fn from_cql_type_with_registry(
    cql_type: &CqlType,
    registry: &UdtRegistry,
    keyspace: &str,
) -> Result<Self> {
    let comparator = match cql_type {
        // ... other types ...
        CqlType::Date => ComparatorType::Custom("date".to_string()),  // ⚠️ SAME FALLBACK
        // ...
    };
    Ok(comparator)
}
```

**Observation**: Both code paths use identical fallback logic.

---

## 3. Reference Implementation Patterns

### 3.1 Timestamp (Similar Temporal Type)

**ComparatorType Variant**: Line 38
```rust
/// Timestamp comparator (chronological)
Timestamp,
```

**Mapping**: Line 84
```rust
CqlType::Timestamp => ComparatorType::Timestamp,
```

**Compare Method**: Lines 294, 510-517
```rust
// In compare() dispatch (line 294)
ComparatorType::Timestamp => self.compare_timestamp(left, right),

// Implementation (lines 510-517)
fn compare_timestamp(&self, left: &Value, right: &Value) -> Result<Ordering> {
    match (left, right) {
        (Value::Timestamp(l), Value::Timestamp(r)) => Ok(l.cmp(r)),  // i64 comparison
        _ => Err(Error::Schema(
            "Type mismatch: expected timestamp values".to_string(),
        )),
    }
}
```

**Type Name**: Line 362
```rust
ComparatorType::Timestamp => "timestamp",
```

**Ordering Support**: Line 391
```rust
ComparatorType::Timestamp => true,  // ✓ Supports ordering
```

### 3.2 Int (Similar i32 Type)

**ComparatorType Variant**: Line 24
```rust
/// 32-bit signed integer comparator
Int,
```

**Mapping**: Line 76
```rust
CqlType::Int => ComparatorType::Int,
```

**Compare Method**: Lines 287, 440-447
```rust
// In compare() dispatch (line 287)
ComparatorType::Int => self.compare_int(left, right),

// Implementation (lines 440-447)
fn compare_int(&self, left: &Value, right: &Value) -> Result<Ordering> {
    match (left.as_i32(), right.as_i32()) {  // Uses helper method
        (Some(l), Some(r)) => Ok(l.cmp(&r)),
        _ => Err(Error::Schema(
            "Type mismatch: expected int values".to_string(),
        )),
    }
}
```

**Type Name**: Line 355
```rust
ComparatorType::Int => "int",
```

**Ordering Support**: Line 384
```rust
ComparatorType::Int => true,  // ✓ Supports ordering
```

### 3.3 BigInt (i64 Comparison)

**Compare Method**: Lines 288, 449-456
```rust
fn compare_bigint(&self, left: &Value, right: &Value) -> Result<Ordering> {
    match (left.as_i64(), right.as_i64()) {
        (Some(l), Some(r)) => Ok(l.cmp(&r)),
        _ => Err(Error::Schema(
            "Type mismatch: expected bigint values".to_string(),
        )),
    }
}
```

### 3.4 Custom (Current DATE Fallback)

**Compare Method**: Lines 318, 786-791
```rust
fn compare_custom(&self, left: &Value, right: &Value) -> Result<Ordering> {
    // For custom types, we can only do equality comparison based on string representation
    let l_str = format!("{}", left);   // ⚠️ String comparison - WRONG for dates
    let r_str = format!("{}", right);
    Ok(l_str.cmp(&r_str))
}
```

**Problem**: String comparison of dates yields incorrect ordering:
- "2023-01-15" < "2023-02-01" ✓ (works by luck)
- "2023-12-01" < "2023-02-01" ✗ (wrong! string comparison)

---

## 4. Value::Date Support

**Location**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/types.rs`

### Value Enum Variant (Line 49)

```rust
/// Date (days since Unix epoch: 1970-01-01)
Date(i32),
```

**Storage Format**: Signed 32-bit integer representing days since Unix epoch (1970-01-01).

### Helper Method (Lines 571-576)

```rust
/// Try to convert this value to a date (days since epoch)
pub fn as_date(&self) -> Option<i32> {
    match self {
        Value::Date(d) => Some(*d),
        _ => None,
    }
}
```

**Observation**: `as_date()` already exists and follows the same pattern as `as_i32()` and `as_i64()`.

### Existing Comparison (Line 841)

```rust
(Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
```

**Location**: In `PartialOrd` implementation for `Value`.

**Status**: Direct i32 comparison already works at Value level, just not exposed via ComparatorType system.

---

## 5. SSTable Parsing Support

**Location**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`

### Type Detection (Line 2767)

```rust
s if s.ends_with("DateType") || s.ends_with("SimpleDateType") => CqlType::Date,
```

**Handles**: Both `org.apache.cassandra.db.marshal.DateType` and legacy `SimpleDateType`.

### Parsing Logic (Lines 3018-3027)

```rust
CqlType::Date => {
    if data.len() != 4 {
        return Err(Error::corruption(format!(
            "Date field requires 4 bytes, got {}",
            data.len()
        )));
    }
    let days = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    Ok(Value::Date(days as i32))  // ✓ Parses as i32
}
```

**Status**: Full parsing support exists. DATE values are correctly read from SSTables.

---

## 6. Test Data Availability

### Schema Files with DATE Columns

1. **basic-types.cql** (Line 23)
   ```sql
   CREATE TABLE simple_table (
       id UUID PRIMARY KEY,
       birth_date DATE,  -- ✓ DATE column
       -- ... other columns ...
   );
   ```
   **Path**: `/Users/patrick/local_projects/cqlite/test-data/schemas/basic-types.cql`

2. **time-series.cql**
   ```sql
   activity_date DATE,  -- ✓ DATE column
   ```
   **Path**: `/Users/patrick/local_projects/cqlite/test-data/schemas/time-series.cql`

**Status**: Test data exists with DATE columns for validation.

---

## 7. Display Implementation

**Location**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/types/comparator.rs:794-827`

### Current Behavior

```rust
impl std::fmt::Display for ComparatorType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // ... specific cases for List, Set, Map, Tuple, Udt, Frozen ...
            _ => write!(f, "{}", self.type_name()),  // Default case
        }
    }
}
```

**For DATE**: Would use `type_name()` fallback:
```rust
ComparatorType::Custom(name) => name,  // Returns "date"
```

**After Fix**: Would need explicit handling if added as variant:
```rust
ComparatorType::Date => "date",  // Line 355 in type_name()
```

---

## 8. Ordering Support

**Location**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/types/comparator.rs:379-409`

### Current Implementation

```rust
pub fn supports_ordering(&self) -> bool {
    match self {
        ComparatorType::Boolean
        | ComparatorType::TinyInt
        | ComparatorType::SmallInt
        | ComparatorType::Int
        | ComparatorType::BigInt
        | ComparatorType::Counter
        | ComparatorType::Float32
        | ComparatorType::Float
        | ComparatorType::Text
        | ComparatorType::Blob
        | ComparatorType::Timestamp  // ✓ Timestamp supports ordering
        | ComparatorType::Uuid
        | ComparatorType::Varint
        | ComparatorType::Decimal
        | ComparatorType::Duration
        | ComparatorType::Json => true,
        // ...
        ComparatorType::Custom(_) => false,  // ⚠️ DATE currently returns false
    }
}
```

**Impact**: DATE columns cannot be used in ORDER BY, range queries, or comparison operators.

---

## 9. Summary of Required Changes

### 9.1 Add ComparatorType::Date Variant

**File**: `cqlite-core/src/types/comparator.rs`
**After Line 38** (after Timestamp):

```rust
/// Date comparator (days since Unix epoch)
Date,
```

### 9.2 Update from_cql_type() Mapping

**Line 129** - Replace:
```rust
CqlType::Date => ComparatorType::Custom("date".to_string()),
```

With:
```rust
CqlType::Date => ComparatorType::Date,
```

### 9.3 Update from_cql_type_with_registry() Mapping

**Line 191** - Replace:
```rust
CqlType::Date => ComparatorType::Custom("date".to_string()),
```

With:
```rust
CqlType::Date => ComparatorType::Date,
```

### 9.4 Add Compare Dispatch

**After Line 294** (after Timestamp):

```rust
ComparatorType::Date => self.compare_date(left, right),
```

### 9.5 Implement compare_date() Method

**After Line 517** (after compare_timestamp):

```rust
fn compare_date(&self, left: &Value, right: &Value) -> Result<Ordering> {
    match (left.as_date(), right.as_date()) {
        (Some(l), Some(r)) => Ok(l.cmp(&r)),  // i32 comparison
        _ => Err(Error::Schema(
            "Type mismatch: expected date values".to_string(),
        )),
    }
}
```

### 9.6 Add type_name() Entry

**After Line 362** (after Timestamp):

```rust
ComparatorType::Date => "date",
```

### 9.7 Add Ordering Support

**Line 391** - Add to ordering list:

```rust
ComparatorType::Timestamp
| ComparatorType::Date  // ← Add this
| ComparatorType::Uuid
```

---

## 10. Comparison with Similar Types

| Type      | Variant | Storage | Compare Method    | Helper    | Ordering | Status |
|-----------|---------|---------|-------------------|-----------|----------|--------|
| Int       | ✓       | i32     | `compare_int`     | `as_i32`  | ✓        | Works  |
| BigInt    | ✓       | i64     | `compare_bigint`  | `as_i64`  | ✓        | Works  |
| Timestamp | ✓       | i64     | `compare_timestamp` | N/A     | ✓        | Works  |
| **Date**  | ✗       | i32     | `compare_custom`  | `as_date` | ✗        | **BROKEN** |
| Time      | ✗       | i64     | `compare_custom`  | `as_time` | ✗        | Broken |

**Pattern**: DATE should follow Int pattern (i32 storage, dedicated variant, proper comparison).

---

## 11. Risk Assessment

### Low Risk Areas

1. **Value::Date** - Already fully implemented and working
2. **SSTable Parsing** - DATE fields parse correctly
3. **Test Data** - Available with DATE columns
4. **Helper Method** - `as_date()` exists and follows conventions

### Medium Risk Areas

1. **Serialization** - ComparatorType is Serialize/Deserialize, adding variant changes wire format
2. **Pattern Matching** - Existing match statements need updating

### Zero Breaking Changes Expected

- Internal implementation detail
- No public API changes
- Backwards compatible (Custom("date") → Date variant)

---

## 12. Testing Strategy

### Unit Tests (comparator_test.rs)

Add tests for:
```rust
#[test]
fn test_date_comparison() {
    let comparator = ComparatorType::Date;
    let early = Value::Date(18000);   // ~2019-04-26
    let later = Value::Date(19000);   // ~2022-01-10

    assert_eq!(comparator.compare(&early, &later).unwrap(), Ordering::Less);
    assert_eq!(comparator.compare(&later, &early).unwrap(), Ordering::Greater);
    assert_eq!(comparator.compare(&early, &early).unwrap(), Ordering::Equal);
}

#[test]
fn test_date_ordering_support() {
    assert!(ComparatorType::Date.supports_ordering());
}

#[test]
fn test_date_type_name() {
    assert_eq!(ComparatorType::Date.type_name(), "date");
}
```

### Integration Tests

1. Query with DATE in WHERE clause: `WHERE birth_date > '2020-01-01'`
2. Query with DATE in ORDER BY: `ORDER BY birth_date DESC`
3. Query with DATE ranges: `WHERE birth_date BETWEEN '2020-01-01' AND '2020-12-31'`

---

## 13. Related Work

### Similar Issues

- **Issue #240** (this issue) - DATE type comparator
- **Potential Follow-up** - TIME type comparator (same pattern, i64)
- **Potential Follow-up** - INET type comparator (byte comparison)

### Cassandra Documentation

**Reference**: Apache Cassandra `DateType` uses:
- **Wire Format**: 4-byte signed integer (big-endian)
- **Semantics**: Days since Unix epoch (1970-01-01)
- **Range**: -2^31 to 2^31-1 (supports dates from ~5879611 BC to ~5879611 AD)
- **Comparison**: Numeric comparison (earlier dates < later dates)

**Source**: `org.apache.cassandra.db.marshal.DateType` in Cassandra 5.0 codebase

---

## 14. Implementation Checklist

- [ ] Add `Date` variant to `ComparatorType` enum (after Timestamp)
- [ ] Update `from_cql_type()` mapping (line 129)
- [ ] Update `from_cql_type_with_registry()` mapping (line 191)
- [ ] Add dispatch in `compare()` method (after line 294)
- [ ] Implement `compare_date()` method (after line 517)
- [ ] Add `type_name()` entry (after line 362)
- [ ] Add to `supports_ordering()` list (line 391)
- [ ] Write unit tests in `comparator_test.rs`
- [ ] Test with real SSTable data (simple_table with birth_date)
- [ ] Update any match statements that enumerate all variants
- [ ] Run full test suite: `env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test --package cqlite-core`
- [ ] Run clippy: `env RUSTFLAGS="-D warnings" cargo clippy --package cqlite-core`

---

## 15. Code References

### Files to Modify

1. **Primary**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/types/comparator.rs`
   - Lines to modify: 38, 129, 191, 294, 362, 391
   - New method: `compare_date()` after line 517

2. **Tests**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/types/comparator_test.rs`
   - Add DATE comparison tests

### Related Files (Reference Only)

- `/Users/patrick/local_projects/cqlite/cqlite-core/src/types.rs` - Value::Date definition (lines 49, 323, 571-576)
- `/Users/patrick/local_projects/cqlite/cqlite-core/src/schema/mod.rs` - CqlType::Date (line 185)
- `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs` - DATE parsing (lines 2767, 3018-3027)

---

## 16. Conclusion

**Current State**: DATE type falls back to string-based comparison via `ComparatorType::Custom("date")`, preventing proper chronological ordering.

**Solution**: Add dedicated `ComparatorType::Date` variant with i32 comparison, following the established pattern of Timestamp and Int types.

**Effort**: Small, low-risk change (~40 lines of code + tests).

**Impact**: Enables DATE columns to work correctly in:
- WHERE clauses with comparison operators
- ORDER BY clauses
- Range queries
- Query engine filters

**Next Steps**: Implement changes per checklist in Section 14.
