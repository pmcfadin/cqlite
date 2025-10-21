# Partition Key Length Limit Fix - Summary

## Problem
File: `cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs:229`

The V5CompressedLegacy parser had a hard-coded 100-byte partition key length limit that violated the no-heuristics mandate. This arbitrary limit would reject valid Cassandra partitions with:
- Composite partition keys (multiple columns)
- Text/VARCHAR partition keys
- Long UUIDs or other large key types

## Root Cause
Line 229 contained: `|| key_len > 100`

This was a HEURISTIC that conflicted with:
1. **Apache Cassandra Specification**: Partition keys can be up to 64KB (65,536 bytes)
2. **V5CompressedLegacy Format Reality**: Uses u8 for key_len field, max expressible is 255 bytes
3. **No-heuristics mandate (Issue #28)**: Should only reject keys that Cassandra itself would reject

## Solution
Replaced the arbitrary 100-byte limit with authoritative format constraints:

### Changes Made

1. **Added constant declarations** (lines 189-193):
   ```rust
   // Cassandra partition key size limits (used in header validation)
   // - CASSANDRA_MAX_KEY_SIZE: 64KB limit per Apache Cassandra specification
   // - FORMAT_MAX_KEY_SIZE: u8 max value - V5CompressedLegacy format limitation
   const CASSANDRA_MAX_KEY_SIZE: usize = 65536; // 64KB per Cassandra spec
   const FORMAT_MAX_KEY_SIZE: usize = 255; // u8 max value - format limitation
   ```

2. **Updated validation logic** (line 244):
   ```rust
   || key_len > FORMAT_MAX_KEY_SIZE.min(CASSANDRA_MAX_KEY_SIZE)
   ```
   
   This enforces the actual format limit (255 bytes) while documenting Cassandra's spec (64KB).

3. **Enhanced module documentation** (lines 7-13):
   Added section explaining partition key size constraints and format limitations.

4. **Improved inline comments** (lines 236-240):
   Clarified that the 255-byte limit is a V5CompressedLegacy format limitation, not Cassandra's limit.

## Technical Details

### Apache Cassandra Partition Key Limits
- **Specification Maximum**: 64KB (65,536 bytes) for partition keys and column names
- **Performance Recommendation**: Keep keys small due to O(N) routing and O(N log N) querying
- **Source**: Apache Cassandra documentation and community best practices

### V5CompressedLegacy Format Constraints
- **Actual Limitation**: u8 length field can only represent 0-255 bytes
- **Implication**: Tables with partition keys > 255 bytes cannot use V5CompressedLegacy format
- **Alternative**: Such tables would use V5_0NewBig or V5_0Bti formats with VInt-based length encoding

## Validation

### Compilation
✅ `cargo check --package cqlite-core --lib` - Success  
✅ `cargo clippy --package cqlite-core --lib -- -D warnings` - No warnings  
✅ `cargo fmt --all` - Formatted correctly

### Testing
The fix maintains backward compatibility while removing false rejections:
- Partition keys 0 bytes: Still rejected (invalid)
- Partition keys 1-100 bytes: Accepted (unchanged behavior)
- Partition keys 101-255 bytes: **NOW ACCEPTED** (was incorrectly rejected)
- Partition keys 256+ bytes: Rejected by format (u8 overflow, correct behavior)

### Code Review Checklist
- [x] Removed arbitrary heuristic (100-byte limit)
- [x] Applied authoritative constraints (Cassandra spec + format reality)
- [x] Documented rationale in code comments
- [x] Updated module-level documentation
- [x] Maintained no-heuristics mandate compliance
- [x] No performance regression (constants resolved at compile-time)
- [x] Clippy clean with `-D warnings`
- [x] Formatted with rustfmt

## Impact

### Before
- **Bug**: Partitions with keys 101-255 bytes would be rejected
- **Risk**: False negatives on valid Cassandra data
- **Example**: Composite key `(uuid, timestamp, text)` could easily exceed 100 bytes

### After
- **Correct**: Accept all partition keys the format can represent (0-255 bytes)
- **Documented**: Clear explanation of format limitation vs. Cassandra spec
- **Maintainable**: Self-documenting constants instead of magic numbers

## Files Modified
- `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader/parsing/v5_compressed_legacy.rs`
  - Lines 1-15: Enhanced module documentation
  - Lines 189-193: Added constant declarations
  - Lines 236-244: Updated validation logic and comments

## References
- Issue #28: No-heuristics mandate
- Apache Cassandra Documentation: Partition key size limits (64KB)
- V5_COMPRESSED_LEGACY_FORMAT_SPEC.md: Format specification
