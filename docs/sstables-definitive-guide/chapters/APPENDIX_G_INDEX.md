# Appendix G: Compression Chunk Formats - Documentation Index

## Overview

Appendix G is a comprehensive guide to Cassandra 5.0 compression chunk formats. It consists of multiple documents, each serving a different purpose.

## Document Map

### 1. Main Specification
**File**: `appendix-g-compression-chunk-formats.md` (375 lines)

**Best for**: Comprehensive reference, understanding the complete format

**Covers**:
- CompressionInfo.db binary structure
- Compressed chunk layout in Data.db
- Algorithm-specific formats for LZ4, Snappy, Deflate, Zstd
- CRC checksum details
- Byte order summary table
- Decompression bomb protection
- Practical examples with hex dumps
- Related documentation links

**Start here if**: You need to understand how compression works in Cassandra 5.0

---

### 2. Quick Reference
**File**: `appendix-g-quick-reference.md` (210 lines)

**Best for**: Quick lookup, implementation decisions, common mistakes

**Covers**:
- One-minute algorithm summary table
- Critical byte order reference
- Rust code templates for decompression
- Common mistakes and fixes
- Algorithm class name normalization
- Testing procedures
- Troubleshooting guide

**Start here if**: You're implementing decompression and need to avoid bugs

---

### 3. Algorithm Reference
**File**: `appendix-g-algorithm-reference.md` (450 lines)

**Best for**: Implementation, code templates, step-by-step guides

**Covers**:
- Algorithm-by-algorithm byte layouts
- Rust code for each algorithm
- CompressionInfo.db parsing code
- CRC handling details
- Common bugs with fixes
- Testing template with example code
- Byte order comparison

**Start here if**: You're writing decompression code

---

### 4. Research Summary
**File**: `/docs/archive/issues/COMPRESSION_CHUNK_FORMAT_RESEARCH.md` (600+ lines)

**Best for**: Understanding the analysis, implementation details, Cassandra source mapping

**Covers**:
- Detailed analysis of each Cassandra compressor
- Exact Java source code references with line numbers
- Critical findings explained
- Verification points for implementation
- Files modified and implementation status
- Testing recommendations

**Start here if**: You want to understand WHY something is done a certain way

---

### 5. Implementation Summary
**File**: `/COMPRESSION_DOCUMENTATION_SUMMARY.md` (300 lines)

**Best for**: Executive overview, impact analysis, recommendations

**Covers**:
- Deliverables summary
- Key technical discoveries
- Implementation verification points
- Files modified
- Testing strategy
- Impact on CQLite

**Start here if**: You're a project manager or architect understanding scope

---

## Quick Navigation Matrix

| Need | Best Document |
|------|---------------|
| Algorithm byte layout | Algorithm Reference (#3) |
| Code template | Algorithm Reference (#3) |
| Quick lookup | Quick Reference (#2) |
| Full specification | Main Specification (#1) |
| Why decision made | Research Summary (#4) |
| Project overview | Implementation Summary (#5) |
| Byte order | Quick Reference (#2) or Algorithm Reference (#3) |
| Common mistakes | Quick Reference (#2) |
| Cassandra source code | Research Summary (#4) |
| Testing | Algorithm Reference (#3) |
| CRC handling | Algorithm Reference (#3) or Main Specification (#1) |
| Decompression bombs | Main Specification (#1) |

## Key Concepts Across Documents

### Byte Order / Size Prefixes

- **LZ4**: 4-byte **Little-Endian** size prefix in `Data.db` (critical — unusual!)
- **Snappy**: No size prefix — raw Snappy frame
- **Deflate**: No size prefix — raw Deflate stream
- **Zstd**: No size prefix — Zstd frame with internal content checksum

### CRC Checksum

Per-chunk CRC32 checksums live inline in `Data.db` immediately after each compressed chunk:
- Location: In `Data.db`, immediately after the compressed chunk bytes
- Position: `chunk_offset + compressed_length` (where `compressed_length` = `next_offset - offset - 4`)
- `CompressionInfo.db` stores **no** per-chunk CRCs — only chunk byte offsets
- Byte order: Big-Endian (Java `java.util.zip.CRC32`, IEEE polynomial)

### Decompression Bomb Protection

Covered in all documents:
- Limit: 128MB maximum decompressed size
- Validation: Both pre and post-decompression
- Implementation: In CQLite compression.rs

### Algorithm Size Prefixes

Only LZ4 has a size prefix in Data.db. Snappy, Deflate, and Zstd do not:
- **LZ4**: 4-byte little-endian uncompressed-length prefix
- **Snappy**: No prefix — raw Snappy frame
- **Deflate**: No prefix — raw Deflate stream
- **Zstd**: No prefix — Zstd frame with internal content checksum

## Reading Recommendations

### For Developers (30 minutes)

1. **Quick Reference** (5 min) - Get the essentials
2. **Algorithm Reference** (20 min) - Study code templates
3. **Main Specification** (5 min) - Fill in gaps

### For Code Reviewers (45 minutes)

1. **Quick Reference** (5 min) - Understand basics
2. **Algorithm Reference** (25 min) - Know what to check
3. **Research Summary** (15 min) - Understand reasoning

### For Architects (20 minutes)

1. **Implementation Summary** (10 min) - Overview
2. **Main Specification** (5 min) - Key technical points
3. **Research Summary** (5 min) - Background

### For Maintaining (variable)

1. **Algorithm Reference** (for code questions)
2. **Quick Reference** (for debugging)
3. **Research Summary** (for history/reasoning)

## Cross-References

Documents reference each other:

- Main Spec references Quick Reference for code templates
- Quick Reference references Main Spec for full details
- Algorithm Reference references both for detailed reference
- Research Summary references everything for authoritative sources
- Implementation Summary ties everything together

## Cassandra Source Code References

All documents cite Cassandra 5.0.8 source code:

```
/src/java/org/apache/cassandra/io/compress/
├── LZ4Compressor.java        — 4-byte LE prefix in compress() lines 118-134
├── SnappyCompressor.java     — no prefix, uncompress() lines 93-108
├── DeflateCompressor.java    — no prefix, uncompress() lines 199-221
├── ZstdCompressor.java       — no prefix, Zstd frame with checksum
├── CompressedSequentialWriter.java — per-chunk CRC write, flushData() lines 140-206
├── CompressionMetadata.java  — header layout, open() lines 76-112, writeHeader() lines 375-398
├── NoopCompressor.java       — passthrough compressor
└── ICompressor.java

/src/java/org/apache/cassandra/schema/   ← note: schema/, not io/compress/
└── CompressionParams.java    — DEFAULT_CHUNK_LENGTH = 16384 (line 47)

/src/java/org/apache/cassandra/io/sstable/format/big/
└── BigFormat.java            — hasMaxCompressedLength version gate (line 401)
```

## Document Maintenance

### When to Update

1. **Algorithm format changes**: Update all documents
2. **CQLite implementation changes**: Update code examples in Quick Ref and Algorithm Ref
3. **New findings**: Add to Research Summary
4. **Cassandra version updates**: Add to Research Summary

### Version Tracking

All documents reference Cassandra 5.0.8. Links are pinned to the `cassandra-5.0.8` tag.

## External Resources

### Cassandra 5.0 Source
- Repository: https://github.com/apache/cassandra
- Branch: cassandra-5.0.8 (pin to this tag for permalink stability)
- Path: /src/java/org/apache/cassandra/io/compress/

### CQLite Implementation
- Path: /cqlite-core/src/storage/sstable/compression.rs
- Related: /cqlite-core/src/storage/sstable/compression_info.rs

### Test Data
- Location: /test-data/datasets/sstables/
- Purpose: Real Cassandra 5.0 SSTables for validation

## Common Lookups

### "How do I extract the size prefix?"
See: **Algorithm Reference** > Algorithm-specific sections
Or: **Quick Reference** > Byte Swaps section

### "What's the byte order for LZ4?"
See: **Quick Reference** > Byte Order Comparison
Or: **Algorithm Reference** > LZ4 Format

### "What are common bugs?"
See: **Quick Reference** > Common Mistakes
Or: **Algorithm Reference** > Common Bugs and Fixes

### "Where's the CRC?"
See: **Main Specification** > CRC Checksum Format
Or: **Algorithm Reference** > CRC Checksum section

### "How do I test my implementation?"
See: **Algorithm Reference** > Testing Template
Or: **Main Specification** > Related Documentation

### "Why is this format different?"
See: **Research Summary** > Detailed analysis sections

## Document Statistics

| Document | Size | Lines | Code Examples | Tables |
|----------|------|-------|---------------|--------|
| Main Specification | 12 KB | 375 | 15 | 8 |
| Quick Reference | 6 KB | 210 | 10 | 5 |
| Algorithm Reference | 14 KB | 450 | 25 | 4 |
| Research Summary | 10 KB | 600+ | 5 | 3 |
| Implementation Summary | 8.8 KB | 300 | 2 | 3 |

**Total**: ~50 KB, ~1950 lines, 57+ code examples, 23 tables

## Feedback and Updates

If you find:
- **Errors**: Reference the Cassandra source code to verify
- **Omissions**: Check Research Summary for detailed analysis
- **Confusion**: Try another document from the matrix above
- **Updates needed**: Check against latest Cassandra 5.0 source

## See Also

- `/docs/sstables-definitive-guide/README.md` - Main guide index
- `/docs/sstables-definitive-guide/chapters/09-compressioninfo-and-chunking.md` - General compression background
- `/docs/sstables-definitive-guide/chapters/05-data-db-format.md` - Data.db structure
- `/docs/sstables-definitive-guide/chapters/appendix-b-encodings-cheat-sheet.md` - Encoding reference

---

**Last Updated**: June 2026
**Cassandra Version**: 5.0.8
**Status**: Verified against Cassandra 5.0.8 source (audit B5)
