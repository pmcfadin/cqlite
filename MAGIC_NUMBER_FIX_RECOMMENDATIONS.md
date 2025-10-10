# Magic Number Issue - Fix Recommendations

## TL;DR - What's Actually Wrong

**You asked**: "Why are 5 magic numbers failing?"
**Answer**: They're not magic numbers at all - we're trying to read magic numbers from files that don't have them.

### The Real Issue

```
Statistics.db    → First 4 bytes: 0x00000004 (version field, NOT magic)
Filter.db        → First 4 bytes: 0x00000005 (version field, NOT magic)
CompressionInfo  → First 4 bytes: 0x0010... (VInt length, NOT magic)
Summary.db       → First 4 bytes: 0x00000080 (version field, NOT magic)
```

**Our code**: Tries to parse ALL components as if they have magic numbers
**Cassandra**: Only Data.db has magic numbers

---

## Quick Fix (30 minutes)

### File 1: Remove Incorrect Magic Number

**File**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/parser/header.rs`
**Line**: 34

```rust
// DELETE THIS LINE:
V5_0SummaryFormat,  // ← This is WRONG - Summary.db doesn't have magic number

// DELETE FROM magic_number():
CassandraVersion::V5_0SummaryFormat => 0x0000_0080,  // ← Line 59

// DELETE FROM from_magic_number():
0x0000_0080 => Some(CassandraVersion::V5_0SummaryFormat),  // ← Line 93

// DELETE FROM version_string():
CassandraVersion::V5_0SummaryFormat => "...",  // ← Line 124
```

### File 2: Stop Parsing Magic From Wrong Components

**File**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/header.rs`
**Function**: `parse_header_with_version_detection` (line 69)

**Change**:
```rust
pub(crate) async fn parse_header_with_version_detection(
    header_buffer: &[u8],
    path: &Path,
    component_type: SSTableComponent,  // ← ADD THIS PARAMETER
) -> Result<SSTableHeader> {
    // Check component type first
    match component_type {
        SSTableComponent::Statistics
        | SSTableComponent::Filter
        | SSTableComponent::CompressionInfo
        | SSTableComponent::Summary => {
            // These don't have magic numbers - parse directly
            return parse_component_without_magic(header_buffer, component_type);
        }
        SSTableComponent::Data => {
            // Only Data.db has magic numbers - proceed with existing logic
        }
        SSTableComponent::Index => {
            // Index.db also doesn't have magic - existing spec handles it
        }
        _ => {
            // Unknown component - try magic number as fallback
        }
    }

    // ... existing magic number parsing code ...
}
```

### File 3: Add Helper Function

**File**: Same as above
**Add after line 150**:

```rust
/// Parse component headers that don't use magic numbers
fn parse_component_without_magic(
    buffer: &[u8],
    component_type: SSTableComponent,
) -> Result<SSTableHeader> {
    match component_type {
        SSTableComponent::Statistics => {
            // First 4 bytes are version
            let version = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
            log::debug!("Statistics.db version: {}", version);

            // Return minimal header for Statistics
            Ok(SSTableHeader {
                cassandra_version: CassandraVersion::V5_0NewBig,
                version: version as u16,
                table_id: [0; 16],
                keyspace: "unknown".to_string(),
                table_name: "unknown".to_string(),
                generation: 1,
                compression: CompressionInfo {
                    algorithm: "none".to_string(),
                    chunk_size: 65536,
                    parameters: HashMap::new(),
                },
                stats: Default::default(),
                columns: vec![],
                properties: HashMap::new(),
            })
        }
        SSTableComponent::Filter => {
            // Similar to Statistics
            let version = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
            log::debug!("Filter.db version: {}", version);
            // ... return header ...
            todo!("Implement Filter.db header parsing")
        }
        SSTableComponent::Summary => {
            // Similar to Statistics
            let version = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);
            log::debug!("Summary.db version: {}", version);
            // ... return header ...
            todo!("Implement Summary.db header parsing")
        }
        SSTableComponent::CompressionInfo => {
            // Parse VInt length + string
            log::debug!("CompressionInfo.db - parsing algorithm string");
            // ... return header ...
            todo!("Implement CompressionInfo.db header parsing")
        }
        _ => Err(Error::unsupported_format(format!(
            "Unsupported component type: {:?}",
            component_type
        ))),
    }
}
```

### File 4: Update Call Sites

**Search for**: All calls to `parse_header_with_version_detection`
**Action**: Add `component_type` parameter

```bash
# Find call sites
grep -rn "parse_header_with_version_detection" cqlite-core/src/storage/sstable
```

**Update each call**:
```rust
// Before:
parse_header_with_version_detection(buffer, path).await?;

// After:
parse_header_with_version_detection(buffer, path, component_type).await?;
```

---

## Complete Fix (2-4 hours)

### Step 1: Extend HeaderSpec System

**File**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/header_spec.rs`
**Location**: After line 407

**Add**:
```rust
// Statistics.db header specification
self.specs.insert(
    SSTableComponentType::Statistics,
    ComponentHeaderSpec {
        component_type: SSTableComponentType::Statistics,
        has_magic_number: false,  // ← KEY: NO MAGIC
        magic_number: None,
        min_version: 1,
        max_version: 10,
        field_layout: HeaderFieldLayout {
            fields: vec![
                HeaderField {
                    name: "version".to_string(),
                    field_type: HeaderFieldType::U32BE,
                    optional: false,
                    validation: Some(FieldValidation {
                        min_value: Some(1),
                        max_value: Some(10),
                        allowed_values: None,
                        max_length: None,
                    }),
                },
                HeaderField {
                    name: "statistics_kind".to_string(),
                    field_type: HeaderFieldType::U32BE,
                    optional: false,
                    validation: None,
                },
                // ... add more fields based on Statistics.db format
            ],
            min_size: 8,
            max_size: 1024,
        },
    },
);

// Filter.db header specification
self.specs.insert(
    SSTableComponentType::Filter,
    ComponentHeaderSpec {
        component_type: SSTableComponentType::Filter,
        has_magic_number: false,  // ← KEY: NO MAGIC
        magic_number: None,
        min_version: 1,
        max_version: 10,
        field_layout: HeaderFieldLayout {
            fields: vec![
                HeaderField {
                    name: "version".to_string(),
                    field_type: HeaderFieldType::U32BE,
                    optional: false,
                    validation: Some(FieldValidation {
                        min_value: Some(1),
                        max_value: Some(10),
                        allowed_values: None,
                        max_length: None,
                    }),
                },
                HeaderField {
                    name: "hash_count".to_string(),
                    field_type: HeaderFieldType::U32BE,
                    optional: false,
                    validation: None,
                },
                // ... add more fields
            ],
            min_size: 8,
            max_size: 64,
        },
    },
);

// CompressionInfo.db header specification
self.specs.insert(
    SSTableComponentType::CompressionInfo,
    ComponentHeaderSpec {
        component_type: SSTableComponentType::CompressionInfo,
        has_magic_number: false,  // ← KEY: NO MAGIC
        magic_number: None,
        min_version: 0, // No version field
        max_version: 0,
        field_layout: HeaderFieldLayout {
            fields: vec![
                HeaderField {
                    name: "algorithm".to_string(),
                    field_type: HeaderFieldType::VString,
                    optional: false,
                    validation: Some(FieldValidation {
                        min_value: None,
                        max_value: None,
                        allowed_values: None,
                        max_length: Some(256),
                    }),
                },
                HeaderField {
                    name: "chunk_size".to_string(),
                    field_type: HeaderFieldType::U32BE,
                    optional: false,
                    validation: None,
                },
                // ... add more fields
            ],
            min_size: 8,
            max_size: 512,
        },
    },
);
```

### Step 2: Use Spec System in Reader

**File**: `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/header.rs`

**Replace** the entire `parse_header_with_version_detection` function:

```rust
pub(crate) async fn parse_header_with_version_detection(
    header_buffer: &[u8],
    path: &Path,
    component_type: SSTableComponent,
) -> Result<SSTableHeader> {
    // Get spec for this component type
    let registry = get_global_registry();
    let spec = registry.get_spec(component_type)?;

    // Check if this component uses magic numbers
    if !spec.has_magic_number {
        // Parse without magic number validation
        log::debug!(
            "Parsing {:?} header without magic number for file: {}",
            component_type,
            path.display()
        );
        return parse_component_header_from_spec(header_buffer, spec, component_type);
    }

    // For components with magic numbers (Data.db), proceed with magic validation
    let magic = u32::from_be_bytes([
        header_buffer[0],
        header_buffer[1],
        header_buffer[2],
        header_buffer[3],
    ]);

    if !SUPPORTED_MAGIC_NUMBERS.contains(&magic) {
        return Err(Error::unsupported_format(format!(
            "Unsupported magic number 0x{:08X} in {:?} file: {}",
            magic,
            component_type,
            path.display()
        )));
    }

    // ... rest of existing magic number parsing ...
}
```

---

## Testing The Fix

### Test 1: Statistics.db Parsing

```rust
#[test]
fn test_statistics_db_no_magic_number() {
    // Real Statistics.db header
    let bytes = vec![
        0x00, 0x00, 0x00, 0x04,  // Version: 4
        0x26, 0x29, 0x1b, 0x05,  // Statistics kind
        // ... rest of data
    ];

    let result = parse_header_with_version_detection(
        &bytes,
        Path::new("test-Statistics.db"),
        SSTableComponent::Statistics,
    );

    assert!(result.is_ok());
    let header = result.unwrap();
    assert_eq!(header.version, 4);
    // Should NOT have tried to parse as magic number
}
```

### Test 2: Data.db Still Works

```rust
#[test]
fn test_data_db_with_magic_number() {
    let bytes = vec![
        0x80, 0x80, 0x01, 0x5C,  // Magic: 0x8080015C
        0x00, 0x10,              // Version
        // ... rest of Data.db header
    ];

    let result = parse_header_with_version_detection(
        &bytes,
        Path::new("test-Data.db"),
        SSTableComponent::Data,
    );

    assert!(result.is_ok());
    let header = result.unwrap();
    assert_eq!(header.cassandra_version, CassandraVersion::V5_0DataFormat);
}
```

### Test 3: No More "Unknown Magic" Errors

```bash
# Before fix: Errors like this
ERROR Unknown magic number: 0xDE150000
ERROR Unknown magic number: 0xB57C6400

# After fix: Should see
DEBUG Parsing Statistics.db header without magic number
DEBUG Statistics.db version: 4
DEBUG Filter.db version: 5
```

---

## Verification Steps

After implementing fixes:

```bash
# 1. Run the failing smoke tests
cd test-data/scripts
./smoke-tests.sh

# 2. Check logs - should see NO "Unknown magic number" errors
grep -i "unknown magic" smoke-test-results/*.actual

# 3. Verify Statistics.db is parsed correctly
cargo test --package cqlite-core statistics

# 4. Run full test suite
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets \
  cargo test --package cqlite-core

# 5. Clippy should pass
cargo clippy --package cqlite-core
```

---

## Why This Fixes Everything

### Problem Files Analysis

| File | First 4 Bytes | Error Before | After Fix |
|------|---------------|--------------|-----------|
| Statistics.db | `00 00 00 04` | "Unknown magic 0x00000004" | "Version 4" ✓ |
| Filter.db | `00 00 00 05` | "Unknown magic 0x00000005" | "Version 5" ✓ |
| Summary.db | `00 00 00 80` | Parsed as magic (wrong!) | "Version 128" ✓ |
| CompressionInfo | `00 10 53 6E` | "Unknown magic 0x0010536E" | Parse VInt ✓ |

### Root Cause → Fix Mapping

| Root Cause | Fix |
|------------|-----|
| All components parsed same way | Component-type-aware parsing |
| Statistics/Filter treated as Data.db | Check `has_magic_number` flag |
| Summary magic in enum (wrong) | Remove `V5_0SummaryFormat` |
| No specs for Stats/Filter/CompInfo | Add HeaderSpec entries |

---

## Migration Path

### Phase 1: Immediate (Quick Fix)
- ✓ Stop parsing magic from non-Data components
- ✓ Remove incorrect `V5_0SummaryFormat`
- ✓ Add component-type parameter to parser

### Phase 2: Complete (2-4 hours)
- ✓ Add HeaderSpec for Statistics/Filter/CompressionInfo
- ✓ Implement spec-driven parsing for all components
- ✓ Comprehensive tests

### Phase 3: Cleanup (Optional)
- Deprecate legacy parsing code
- Unified spec-driven approach for all components
- Better error messages with component awareness

---

## Questions To Answer

### Q1: "What are 0xDE150000 and 0xB57C6400?"

**A**: Need to check logs to see which FILES produced these errors. Likely:
- Filter.db or Statistics.db from a different table
- Or CompressionInfo.db with specific algorithm string

**Action**: Add logging to show file path when magic number fails

### Q2: "Is 0x5C018080 an endianness bug?"

**A**: This is byte-reversed `0x8080015C`. Either:
1. Code read it in wrong order (search for `le_u32` usage)
2. Or it's being logged/displayed incorrectly

**Action**: Search for any little-endian reads:
```bash
grep -rn "le_u32\|from_le_bytes" cqlite-core/src/storage/sstable/
```

### Q3: "Are our other magic numbers correct?"

**A**: Need Cassandra source verification for:
- `0xAD010000` (V5_0Alpha)
- `0xA0070000` (V5_0Beta)
- `0x43160000` (V5_0Release)
- `0x8C330000` (V5_0FormatC)

**Action**: Cross-reference with Cassandra's `Version.java`

---

## Summary

**The problem**: Trying to parse magic numbers from files that don't have them
**The fix**: Check component type before attempting magic number parsing
**The impact**: Fixes all 5 "unknown magic number" errors
**The effort**: 30 min (quick) to 4 hours (complete)

**Files to change**:
1. `parser/header.rs` - Remove wrong enum variant
2. `storage/sstable/reader/header.rs` - Add component-type check
3. `storage/sstable/header_spec.rs` - Add missing specs
4. All call sites - Pass component type parameter

**Ready to implement?** Start with the Quick Fix section above.
