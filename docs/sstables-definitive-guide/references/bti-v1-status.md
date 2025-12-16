# BTI (Big Trie Index) Status for v1 Release

**Investigation Date:** 2025-12-15
**Decision:** BTI is NOT blocking for v1 release
**Confidence Level:** HIGH

---

## Executive Summary

**Is BTI blocking for v1?** **NO**

**Key Finding:** All test data uses the legacy BIG format (`nb-` prefix). No BTI format SSTables (`bti-` or format identifier starting with `da`) exist in the test dataset. BTI support can be deferred to post-v1 without impacting current functionality.

---

## 1. Web Research: BTI Format in Cassandra 5.0

### What is BTI?

BTI (Big Trie-Indexed) is a new SSTable format introduced in Cassandra 5.0 via CEP-25. It offers:
- Trie-based primary index (2x better performance than legacy BIG format)
- Eliminates need for index summary and key cache
- More efficient for wide partitions (thousands/millions of rows)
- Better lookup efficiency with reduced index file sizes

### BTI vs BIG Format Status

**Default Format in Cassandra 5.0:** `big` (legacy format)
- BTI is **opt-in** via `cassandra.yaml` configuration
- Cassandra 5.0+ supports both `big` and `bti` formats
- BTI is marked as **experimental** in Cassandra 5.0
- Expected to become default in future versions

### Configuration

From `cassandra.yaml`:
```yaml
# Default is 'big' (commented out)
# selected_format: big
```

To enable BTI, users must explicitly configure:
```yaml
selected_format: bti
```

### File Naming Patterns

| Format | File Prefix | Format ID | Example Filename |
|--------|-------------|-----------|------------------|
| BIG (legacy) | `nb-` | **Headerless** - identified by filename only | `nb-1-big-Data.db` |
| BTI (trie-indexed) | Format-specific | `0x6461` (`da` in hex) | `ac-1-bti-Data.db` or similar |

> **Note (Issue #211):** NB format Data.db files are **headerless** - they have no magic number.
> The format is identified solely by the `nb-` filename prefix. The `0x6F61` ('oa') magic
> is for legacy OA format files, not NB format.

**Key Distinction:**
- BIG format: Uses `Index.db` and `Summary.db` for partition lookups
- BTI format: Uses `Partitions.db` and `Rows.db` trie indexes

---

## 2. Test Data Analysis

### Dataset Location
```
/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/
```

### Format Distribution

**Total SSTable Files:** 354
**BIG Format Files (`nb-`):** 354 (100%)
**BTI Format Files (`bti-`):** 0 (0%)

### File Pattern Analysis

All test tables use the BIG format with `nb-` prefix:
```
nb-1-big-Summary.db
nb-1-big-Index.db
nb-1-big-Data.db
nb-1-big-CompressionInfo.db
nb-1-big-Filter.db
nb-1-big-Statistics.db
```

### Test Dataset Coverage

All test datasets use legacy BIG format:
- `test_basic/` - BIG format only
- `test_collections/` - BIG format only
- `test_timeseries/` - BIG format only
- `test_wide_rows/` - BIG format only
- `system/` - BIG format only
- `system_auth/` - BIG format only
- `system_schema/` - BIG format only

**No BTI-format test data exists.**

---

## 3. CQLite BTI Implementation Status

### Code Structure

BTI implementation located at: `/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/bti/`

**Files:**
1. `mod.rs` - Module definition and core types (145 lines)
2. `parser.rs` - Partitions and Rows parsers (761 lines)
3. `node.rs` - Trie node structures (566 lines)
4. `encoder.rs` - Byte-comparable key encoding (1,208 lines)
5. `nodes.rs` - Additional node utilities

**Total LOC:** ~2,680 lines of BTI-specific code

### Implementation Completeness

#### ✅ Fully Implemented

1. **Type Definitions** (`mod.rs`)
   - BTI magic number detection (`0x6461`)
   - Format type enum (Big vs BTI)
   - BTI metadata structures
   - Configuration options

2. **Byte-Comparable Encoding** (`encoder.rs`)
   - Complete CEP-25 compliant encoder
   - All CQL type support (primitives, collections, UDTs, tuples)
   - Proper ordering semantics
   - Escape sequences for special bytes
   - Variable-length encoding
   - Comprehensive test coverage (25+ unit tests)

3. **Node Structures** (`node.rs`)
   - All 4 BTI node types (PayloadOnly, Single, Sparse, Dense)
   - Sized pointer encoding/decoding
   - Payload references
   - Trie navigation with cycle detection
   - Node validation
   - Full test coverage

4. **Parser Foundations** (`parser.rs`)
   - BTI header parsing
   - Partitions.db parser skeleton
   - Rows.db parser skeleton
   - Basic lookup logic
   - Node caching

#### ⚠️ Incomplete/TODO

**Found 4 TODO Comments:**

1. **Line 495** (RowsParser::parse_node_data):
   ```rust
   // TODO: Extract to common utility function
   ```
   - Impact: Code duplication between Partitions and Rows parsers
   - Severity: Low (code quality issue)

2. **Line 540** (RowsParser::range_query):
   ```rust
   // TODO: Implement proper range traversal
   ```
   - Impact: Range queries within partitions not implemented
   - Severity: **HIGH** - Core functionality gap

3. **Line 584** (PartitionIterator::next):
   ```rust
   // TODO: Implement proper trie traversal for iteration
   ```
   - Impact: Cannot iterate all partitions in BTI index
   - Severity: **HIGH** - Core functionality gap

4. **Line 619** (RowIterator::next):
   ```rust
   // TODO: Implement proper trie traversal for iteration
   ```
   - Impact: Cannot iterate all rows in BTI index
   - Severity: **HIGH** - Core functionality gap

### Critical Gaps

#### 1. Trie Traversal for Full Scans
- **Status:** Stub implementation only
- **Impact:** Cannot perform full table scans on BTI-format SSTables
- **Required For:** SELECT * queries without partition key

#### 2. Range Queries on Clustering Keys
- **Status:** Returns empty results
- **Impact:** Cannot perform range queries within partitions
- **Required For:** SELECT with clustering key ranges

#### 3. RowsParser Node Parsing
- **Status:** Simplified stub (line 493-512)
- **Impact:** Real BTI Rows.db files cannot be parsed correctly
- **Required For:** Wide partition lookups

### What Works vs What Doesn't

| Feature | Status | Notes |
|---------|--------|-------|
| BTI format detection | ✅ Works | Magic number checking |
| Byte-comparable encoding | ✅ Works | Full CEP-25 compliance |
| BTI header parsing | ✅ Works | Partitions.db and Rows.db |
| Trie node structures | ✅ Works | All 4 node types |
| Single partition lookup | ⚠️ Partial | Basic structure exists |
| Full table iteration | ❌ Not implemented | Returns empty |
| Range queries | ❌ Not implemented | Returns empty |
| Row index parsing | ❌ Stub only | Simplified implementation |

---

## 4. Component Type Recognition

### BTI Component Awareness

CQLite code **does recognize** BTI components:

**From `directory/types.rs`:**
```rust
pub enum SSTableComponent {
    // BIG format components
    Index,      // BIG format partition index
    Summary,    // BIG format index summary

    // BTI format components
    Partitions, // BTI partition trie index
    Rows,       // BTI row trie index (for wide partitions)

    // Shared components
    Data,
    Statistics,
    Filter,
    CompressionInfo,
    // ...
}
```

**Helper Methods:**
```rust
fn is_bti_specific(&self) -> bool {
    matches!(self, SSTableComponent::Partitions | SSTableComponent::Rows)
}

fn is_big_specific(&self) -> bool {
    matches!(self, SSTableComponent::Index | SSTableComponent::Summary)
}
```

**Conclusion:** Infrastructure exists to detect BTI files, but parsing is incomplete.

---

## 5. Real-World Usage Implications

### Current Cassandra 5.0 Deployment Reality

1. **Default Configuration:** BIG format is default
2. **Opt-In Required:** Users must explicitly enable BTI
3. **Experimental Status:** BTI marked experimental in C5.0
4. **Adoption Rate:** Likely very low in production (new format, experimental)

### Expected v1 User Base

**Primary Use Case:** Reading existing Cassandra SSTables

Most likely scenarios:
- ✅ Production clusters running Cassandra 3.x-5.0 with default config (BIG format)
- ✅ Upgraded 4.x → 5.0 clusters (SSTables remain BIG format until rewritten)
- ❌ Users who explicitly enabled experimental BTI format
- ❌ New Cassandra 5.0+ clusters with BTI explicitly configured

**Estimated BTI Exposure:** <5% of real-world use cases

---

## 6. v1 Release Recommendation

### Decision: Ship v1 WITHOUT BTI Support ✅

**Rationale:**

1. **Zero Test Coverage:** No BTI test data exists
2. **Zero Real Demand:** All test scenarios use BIG format
3. **Incomplete Implementation:** Critical gaps in iteration and range queries
4. **Low Production Adoption:** BTI is experimental and opt-in
5. **Clear Alternative:** Users with BTI SSTables can use native Cassandra tools

### Risk Assessment

**Shipping without BTI:**
- **Risk Level:** LOW
- **User Impact:** Minimal (affects <5% of potential users)
- **Workaround:** Users can export BTI SSTables to BIG format using Cassandra

**Shipping with incomplete BTI:**
- **Risk Level:** HIGH
- **User Impact:** Broken functionality, confusing errors
- **Technical Debt:** Untested code in production

### Recommended v1 Scope

**Include:**
- ✅ Full BIG format support (all test data uses this)
- ✅ BTI detection and graceful error message
- ✅ Documentation noting BTI as post-v1 feature

**Exclude:**
- ❌ BTI Partitions.db parsing
- ❌ BTI Rows.db parsing
- ❌ BTI trie traversal
- ❌ BTI iteration and range queries

### Suggested Error Handling

```rust
if format == "bti" {
    return Err(Error::UnsupportedFormat(
        "BTI format detected. BTI support is planned for v1.1+. \
         Current version supports BIG format only. \
         Please export SSTables to BIG format using: \
         nodetool upgradesstables --rewrite-all-sstables"
    ));
}
```

---

## 7. Post-v1 Roadmap

### v1.1+ BTI Completion Tasks

**High Priority:**
1. Generate BTI test data using Cassandra 5.0+ with `selected_format: bti`
2. Implement trie full traversal (PartitionIterator, RowIterator)
3. Complete RowsParser node parsing logic
4. Implement range query support
5. Add comprehensive integration tests

**Medium Priority:**
6. Optimize trie caching strategies
7. Benchmark BTI vs BIG read performance
8. Document BTI-specific tuning parameters

**Low Priority:**
9. BTI SSTable writing support (experimental feature)
10. Migration tooling (BIG → BTI conversion)

**Estimated Effort:** 2-3 weeks for full BTI read support

---

## 8. Documentation Requirements for v1

### User-Facing Documentation

**README.md** should include:

```markdown
## Supported SSTable Formats

CQLite v1.x supports the following Cassandra SSTable formats:

- ✅ **BIG format** (Cassandra 3.0-5.0 default)
  - File pattern: `nb-*-big-*.db`
  - Full read support including indexes, compression, statistics

- ⚠️ **BTI format** (Cassandra 5.0+ opt-in, experimental)
  - File pattern: `*-bti-*.db`, `Partitions.db`, `Rows.db`
  - Status: Planned for v1.1+
  - Workaround: Export to BIG format using `nodetool upgradesstables`

### Format Detection

CQLite automatically detects SSTable format and will provide clear
error messages if unsupported formats are encountered.
```

### CHANGELOG.md Entry

```markdown
## [1.0.0] - 2025-XX-XX

### Supported
- Full BIG format SSTable reading (Cassandra 3.x-5.0 default)
- Automatic format detection with helpful error messages

### Known Limitations
- BTI trie-indexed format (Cassandra 5.0+ experimental) not supported
- BTI support planned for v1.1 release
```

---

## 9. Testing Recommendations

### Before v1 Release

1. ✅ Verify all existing tests pass (using BIG format test data)
2. ✅ Add explicit BTI rejection test
3. ✅ Test error message clarity when BTI is detected
4. ✅ Document format detection logic

### For v1.1 (BTI Support)

1. Generate real BTI test data:
   ```bash
   # Configure Cassandra 5.0+
   echo "selected_format: bti" >> cassandra.yaml
   # Create test tables and flush to disk
   ```

2. Validate against native tools:
   ```bash
   sstabledump -d Partitions.db
   sstabledump -d Rows.db
   ```

3. Comprehensive test coverage:
   - Partition lookups via trie
   - Full table scans
   - Range queries
   - Wide partition handling

---

## 10. References

### Official Documentation
- [CEP-25: Trie-indexed SSTable format](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-25:+Trie-indexed+SSTable+format)
- [Apache Cassandra 5.0 BTI Format Documentation](https://github.com/apache/cassandra/blob/cassandra-5.0/src/java/org/apache/cassandra/io/sstable/format/bti/BtiFormat.md)
- [Cassandra 5.0 Features: Trie Memtables and SSTables](https://cassandra.apache.org/_/blog/Apache-Cassandra-5.0-Features-Trie-Memtables-and-Trie-Indexed-SSTables.html)
- [Cassandra Storage Engine Architecture](https://cassandra.apache.org/doc/latest/cassandra/architecture/storage-engine.html)

### Implementation References
- CQLite BTI Implementation: `cqlite-core/src/storage/sstable/bti/`
- Component Type Definitions: `cqlite-core/src/storage/sstable/directory/types.rs`
- Test Data Location: `test-data/datasets/sstables/`

---

## Appendix: BTI Detection Code

```rust
// From cqlite-core/src/storage/sstable/bti/mod.rs

/// BTI format magic number ("da" in hexspeak)
pub const BTI_MAGIC_NUMBER: u32 = 0x6461_0000;

/// Detect BTI format from magic number
pub fn detect_format(magic_number: u32) -> FormatType {
    match magic_number {
        BTI_MAGIC_NUMBER => FormatType::Bti,
        _ => FormatType::Big,
    }
}

/// Check if a magic number indicates BTI format
pub fn is_bti_format(magic_number: u32) -> bool {
    magic_number == BTI_MAGIC_NUMBER
}
```

**Usage in SSTable Reader:**
```rust
// Pseudo-code for format detection
let magic = read_magic_number(file)?;
if is_bti_format(magic) {
    return Err(Error::UnsupportedFormat(
        "BTI format not supported in v1.x"
    ));
}
```

---

## Conclusion

**BTI is NOT blocking for v1 release.**

The combination of:
1. Zero BTI test data
2. Experimental/opt-in status in Cassandra 5.0
3. Low real-world adoption
4. Incomplete implementation with critical gaps
5. Clear alternative path for users

Makes deferring BTI support to v1.1+ the correct engineering decision. This allows v1 to ship with high confidence in supported formats (BIG) while providing a clear roadmap for future BTI support when real user demand materializes.

**Recommendation:** Ship v1 with BIG format only, add BTI to v1.1 roadmap with proper test data generation.
