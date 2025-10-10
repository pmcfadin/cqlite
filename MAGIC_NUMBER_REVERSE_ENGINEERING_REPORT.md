# Cassandra SSTable Magic Number Reverse Engineering Report

## Executive Summary

After comprehensive analysis of Cassandra's SSTable magic numbers, I've determined that **magic numbers are primarily opaque identifiers, NOT structured bit-encoded formats**. They serve as version/format signatures similar to file magic numbers (like PNG's `0x89504E47`), rather than bitwise-encoded metadata containers.

## 1. Pattern Analysis: Known Magic Numbers

### Current CQLite Implementation

From `/Users/patrick/local_projects/cqlite/cqlite-core/src/parser/header.rs`, lines 138-155:

```rust
pub const SUPPORTED_MAGIC_NUMBERS: &[u32] = &[
    0x6F61_0000, // Legacy 'oa' format
    0xAD01_0000, // Cassandra 5.0 Alpha
    0xA007_0000, // Cassandra 5.0 Beta
    0x4316_0000, // Cassandra 5.0 Release
    0x0040_0000, // Cassandra 5.0 'nb' (new big) format
    0x6461_0000, // Cassandra 5.0 BTI (Big Trie-Indexed) format
    0x8080_015c, // Cassandra 5.0 Data.db format
    0x0000_0080, // Cassandra 5.0 Summary.db format
    0x8c33_0000, // Cassandra 5.0 Format C
    0x4325_0000, // Cassandra 5.0 Format D
    0x4225_0000, // Cassandra 5.0 Format E (composite keys)
    0xEA22_0000, // Cassandra 5.0 Format F (TTL support)
    0xAF03_0000, // Cassandra 5.0 Format G (counters)
    0x2C00_0000, // Extended format variant A
    0xC302_0000, // Extended format variant B
    0xF81E_0000, // Extended format variant C
];
```

### Bit-Level Breakdown Table

| Magic Number | Binary | Byte 0 | Byte 1 | Byte 2 | Byte 3 | ASCII Interpretation | Family |
|--------------|--------|--------|--------|--------|--------|---------------------|---------|
| **0x6F61_0000** | 01101111 01100001 00000000 00000000 | 0x6F ('o') | 0x61 ('a') | 0x00 | 0x00 | "oa\0\0" | Legacy |
| **0x6461_0000** | 01100100 01100001 00000000 00000000 | 0x64 ('d') | 0x61 ('a') | 0x00 | 0x00 | "da\0\0" | BTI |
| **0x0040_0000** | 00000000 01000000 00000000 00000000 | 0x00 | 0x40 ('@') | 0x00 | 0x00 | "\0@\0\0" | NewBig |
| **0xAD01_0000** | 10101101 00000001 00000000 00000000 | 0xAD | 0x01 | 0x00 | 0x00 | (non-ASCII) | Alpha |
| **0xA007_0000** | 10100000 00000111 00000000 00000000 | 0xA0 | 0x07 | 0x00 | 0x00 | (non-ASCII) | Beta |
| **0x4316_0000** | 01000011 00010110 00000000 00000000 | 0x43 ('C') | 0x16 | 0x00 | 0x00 | "C\x16\0\0" | Release |
| **0x8080_015c** | 10000000 10000000 00000001 01011100 | 0x80 | 0x80 | 0x01 | 0x5C ('\\') | (mixed) | Data.db |
| **0x0000_0080** | 00000000 00000000 00000000 10000000 | 0x00 | 0x00 | 0x00 | 0x80 | "\0\0\0\x80" | Summary.db |
| **0x8c33_0000** | 10001100 00110011 00000000 00000000 | 0x8C | 0x33 ('3') | 0x00 | 0x00 | (mixed) | Format C |
| **0x4325_0000** | 01000011 00100101 00000000 00000000 | 0x43 ('C') | 0x25 ('%') | 0x00 | 0x00 | "C%\0\0" | Format D |
| **0x4225_0000** | 01000010 00100101 00000000 00000000 | 0x42 ('B') | 0x25 ('%') | 0x00 | 0x00 | "B%\0\0" | Format E |
| **0xEA22_0000** | 11101010 00100010 00000000 00000000 | 0xEA | 0x22 ('"') | 0x00 | 0x00 | (non-ASCII) | Format F |
| **0xAF03_0000** | 10101111 00000011 00000000 00000000 | 0xAF | 0x03 | 0x00 | 0x00 | (non-ASCII) | Format G |
| **0x2C00_0000** | 00101100 00000000 00000000 00000000 | 0x2C (',') | 0x00 | 0x00 | 0x00 | ",\0\0\0" | Variant A |
| **0xC302_0000** | 11000011 00000010 00000000 00000000 | 0xC3 | 0x02 | 0x00 | 0x00 | (non-ASCII) | Variant B |
| **0xF81E_0000** | 11111000 00011110 00000000 00000000 | 0xF8 | 0x1E | 0x00 | 0x00 | (non-ASCII) | Variant C |

### Unknown Magic Numbers (From Test Data)

From smoke test error logs:

| Magic Number | Binary | Byte 0 | Byte 1 | Byte 2 | Byte 3 | Status |
|--------------|--------|--------|--------|--------|--------|--------|
| **0xDE15_0000** | 11011110 00010101 00000000 00000000 | 0xDE | 0x15 | 0x00 | 0x00 | ❌ Unknown |
| **0xB57C_6400** | 10110101 01111100 01100100 00000000 | 0xB5 | 0x7C ('\|') | 0x64 ('d') | 0x00 | ❌ Unknown |
| **0x5732_0000** | 01010111 00110010 00000000 00000000 | 0x57 ('W') | 0x32 ('2') | 0x00 | 0x00 | ❌ Unknown |
| **0xD464_5400** | 11010100 01100100 01010100 00000000 | 0xD4 | 0x64 ('d') | 0x54 ('T') | 0x00 | ❌ Unknown |
| **0xC051_5C00** | 11000000 01010001 01011100 00000000 | 0xC0 | 0x51 ('Q') | 0x5C ('\\') | 0x00 | ❌ Unknown |

## 2. Structural Analysis: Opaque vs Encoded

### Evidence for ASCII-Based Encoding (Partial)

**Pattern 1: Two-Character Version Codes**

The most prominent pattern is ASCII character pairs representing version strings:

- `0x6F61_0000` = "**oa**\0\0" (Legacy format)
- `0x6461_0000` = "**da**\0\0" (BTI format)

From Cassandra's Java source (BigFormat.java), version strings like "oa", "nb", "ma" are defined as:

```java
public static final String current_version =
    DatabaseDescriptor.getStorageCompatibilityMode().isBefore(5)
    ? "nb" : "oa";
public static final String earliest_supported_version = "ma";
```

This confirms that **some** magic numbers encode two-letter version codes as ASCII in bytes 0-1.

**Pattern 2: Zero Padding**

Most known magic numbers have `0x0000` in bytes 2-3, suggesting:
- Bytes 0-1: Version identifier (sometimes ASCII, sometimes opaque)
- Bytes 2-3: Reserved/padding (often zero)

### Evidence AGAINST Structured Bit Encoding

**Observation 1: No Consistent Bitwise Patterns**

Comparing formats with similar features shows NO consistent bit flags:

| Format | Compression | CRC | BTI | Magic Number | Common Bits? |
|--------|------------|-----|-----|--------------|--------------|
| V5_0NewBig | Varies | No | No | 0x0040_0000 | ❌ |
| V5_0NewBigCrc | Varies | Yes | No | (not defined) | N/A |
| V5_0Bti | Varies | No | Yes | 0x6461_0000 | ❌ |
| Legacy | Varies | No | No | 0x6F61_0000 | ❌ |

No bit positions consistently indicate features like compression or CRC checksums.

**Observation 2: Non-Contiguous Magic Numbers**

If bits encoded features, we'd expect similar formats to have nearby magic numbers. Instead:

- 0x6F61_0000 (oa/Legacy) and 0x6461_0000 (da/BTI) differ by 0x0B00_0000
- 0x4316_0000 (Release) and 0x4325_0000 (Format D) differ by only 0x000F_0000

This suggests **ad-hoc assignment** rather than systematic encoding.

**Observation 3: Range-Based Detection**

From `header.rs` lines 70-112, CQLite uses **range matching**:

```rust
pub fn from_magic_number(magic: u32) -> Option<CassandraVersion> {
    match magic {
        // Legacy 'oa' format (range allows version variation in lower bytes)
        0x6F61_0000..=0x6F61_FFFF => Some(CassandraVersion::Legacy),

        // Cassandra 5.0 Alpha format
        0xAD01_0000..=0xAD01_FFFF => Some(CassandraVersion::V5_0Alpha),
        // ...
    }
}
```

The **use of ranges** (0x6F61_0000 to 0x6F61_FFFF) suggests:
- High 16 bits identify the format family
- Low 16 bits may vary (possibly for micro-versions or sub-formats)
- This is **namespace allocation**, not bit-field encoding

## 3. Analysis of Unknown Magic Numbers

### Hypothesis: Endianness Confusion?

Checking if unknowns are byte-swapped versions of known formats:

| Unknown | Byte-Swapped | Matches Known? |
|---------|-------------|----------------|
| 0xDE15_0000 | 0x0000_15DE | ❌ No |
| 0xB57C_6400 | 0x0064_7CB5 | ❌ No |
| 0x5732_0000 | 0x0000_3257 | ❌ No |
| 0xD464_5400 | 0x0054_64D4 | ❌ No |
| 0xC051_5C00 | 0x005C_51C0 | ❌ No |

**Conclusion**: Endianness errors do NOT explain unknown magic numbers.

### Hypothesis: Undocumented Cassandra Versions

Analyzing unknown magic numbers for ASCII patterns:

- **0xDE15_0000**: Bytes = [0xDE, 0x15, 0x00, 0x00] - non-printable, no ASCII pattern
- **0x5732_0000**: Bytes = [0x57 ('W'), 0x32 ('2'), 0x00, 0x00] - Could be "W2" version?
- **0xD464_5400**: Bytes = [0xD4, 0x64 ('d'), 0x54 ('T'), 0x00] - Mixed ASCII/non-ASCII
- **0xB57C_6400**: Bytes = [0xB5, 0x7C, 0x64 ('d'), 0x00] - Non-standard pattern
- **0xC051_5C00**: Bytes = [0xC0, 0x51 ('Q'), 0x5C ('\\'), 0x00] - Non-standard

**Likely Explanations**:

1. **Test data corruption**: The error logs show these from test SSTable files that may be corrupted
2. **Pre-release formats**: Alpha/beta versions not officially supported
3. **Alternative storage engines**: ScyllaDB or other Cassandra forks may use different magic numbers
4. **File type confusion**: May be Index.db, Summary.db, or other components masquerading as Data.db

From error log context: These appear in `nb-1-big-Data.db` files in test dataset, suggesting **"nb" (new big) format variants** that aren't yet catalogued.

## 4. Cassandra's Source Code Parsing Logic

From research of Cassandra's Java codebase:

### Version String Validation

From `Version.java`:

```java
private static final Pattern VALIDATION = Pattern.compile("[a-z]+");

public static boolean validate(String ver) {
    return ver != null && VALIDATION.matcher(ver).matches();
}
```

**Key Finding**: Cassandra validates version strings as **lowercase alphabetic sequences**, NOT as structured bit patterns.

### Magic Number Generation

From `BigFormat.java` analysis:

```java
public static final String current_version =
    DatabaseDescriptor.getStorageCompatibilityMode().isBefore(5)
        ? "nb" : "oa";
```

**Key Finding**: Version identifiers are **string constants** selected based on compatibility mode, not computed from bit operations.

### How Magic Numbers Are Used

Cassandra's parsing appears to use **simple equality checks** or **string prefix matching**:

- Read first 4 bytes as magic number
- Compare against known format signatures
- No bitwise operations for feature detection
- Version-specific features controlled by **separate metadata fields**, not magic number bits

## 5. Format Evolution Analysis

### Historical Progression

From web research (Cassandra SSTable Format Version Numbers Gist):

```
Version 0.x: b, c, d, e, f, g
Version 1.x: h, hb, hc, hd, he, hf
Version 2.x: ia, ib, ic, ja, jb
Version 3.x: ka, la, ma, mb, mc, md
Version 4.x: (unknown format codes)
Version 5.x: na, nb, oa, da (BTI)
```

### Pattern Evolution

**Generation 1** (0.x - 2.x): Single-letter codes
- `b`, `c`, `d` → Simple sequential

**Generation 2** (2.x - 3.x): Two-letter codes
- `ja`, `jb`, `ka`, `la`, `ma` → First letter increments for major version

**Generation 3** (5.x): Two-letter codes with semantic meaning
- `nb` = "new big" (explicit feature name)
- `oa` = Current production format (opaque code)
- `da` = BTI format (possibly "data" related?)

**Conclusion**: Version identifiers evolved from **sequential** → **alphabetic increment** → **mnemonic abbreviations**, NOT systematic bit encoding.

### Predicting Future Magic Numbers

Based on alphabetic progression:

**Cassandra 5.1 might use**:
- `ob` (following `oa`)
- `pa` (next major letter)
- `db` (BTI evolution after `da`)

**Cassandra 6.0 might use**:
- `qa` (next major version)
- `ra` (alternative)

**Format**: `0x[ASCII_BYTE_0][ASCII_BYTE_1]0000`

Example predictions:
- `ob` = 0x6F62_0000
- `pa` = 0x7061_0000
- `db` = 0x6462_0000
- `qa` = 0x7161_0000

## 6. Architectural Implications

### Current CQLite Implementation

From `/Users/patrick/local_projects/cqlite/cqlite-core/src/parser/header.rs`:

```rust
/// Parse magic number to version with proper format detection
pub fn from_magic_number(magic: u32) -> Option<CassandraVersion> {
    match magic {
        // Range-based detection allows version variation in lower 16 bits
        0x6F61_0000..=0x6F61_FFFF => Some(CassandraVersion::Legacy),
        0xAD01_0000..=0xAD01_FFFF => Some(CassandraVersion::V5_0Alpha),
        // ...
        _ => None,
    }
}
```

**Strengths**:
✅ Range-based matching handles minor version variations
✅ Clear enum mapping for each format
✅ Extensible for new formats

**Limitations**:
❌ Hardcoded magic numbers require code updates for new Cassandra versions
❌ Unknown formats immediately rejected (no fallback)
❌ No automatic detection of format features from magic number (because none exist)

### Recommended Architecture Changes

**Option 1: Keep Current Approach (Recommended)**

Magic numbers are opaque identifiers. Continue explicit enum-based matching:

```rust
pub const SUPPORTED_MAGIC_NUMBERS: &[u32] = &[
    // Add new formats as Cassandra releases them
    0x6F61_0000, // oa
    0x6461_0000, // da
    // Future: 0x6F62_0000, // ob (Cassandra 5.1?)
];
```

**Why**: Matches Cassandra's own architecture. No false assumptions about encoding.

**Option 2: Add Heuristic Fallback**

For unknown magic numbers, attempt ASCII decoding:

```rust
fn detect_version_heuristic(magic: u32) -> Option<String> {
    let bytes = magic.to_be_bytes();
    if bytes[0].is_ascii_lowercase() && bytes[1].is_ascii_lowercase()
       && bytes[2] == 0 && bytes[3] == 0 {
        Some(format!("{}{}", bytes[0] as char, bytes[1] as char))
    } else {
        None
    }
}
```

**Why**: Allows forward compatibility with future "XY" format versions.

**Risks**: May misidentify corrupted files as valid new formats.

**Option 3: Configuration-Based Format Registry**

Load magic numbers from external config:

```rust
// formats.toml
[[format]]
magic = 0x6F61_0000
version = "oa"
cassandra_version = "5.0"
features = ["compression", "bti"]
```

**Why**: Users can add support for new formats without code changes.

**Risks**: More complex, potential for misconfiguration.

## 7. Handling Unknown Magic Numbers

### Current Behavior

From error logs, CQLite rejects unknown magic numbers:

```
Error: Unsupported SSTable format: magic number 0xde150000 not recognized.
Supported formats: ["0x6f610000", "0xad010000", ...]
```

### Recommended Strategies

**Strategy 1: Graceful Degradation**

```rust
pub fn from_magic_number(magic: u32) -> Option<CassandraVersion> {
    match magic {
        // Known formats
        0x6F61_0000..=0x6F61_FFFF => Some(CassandraVersion::Legacy),
        // ...

        // Unknown format - attempt heuristic detection
        _ => {
            let bytes = magic.to_be_bytes();
            if bytes[0].is_ascii_lowercase() && bytes[1].is_ascii_lowercase()
               && bytes[2] == 0 && bytes[3] == 0 {
                log::warn!("Unknown version code: {}{}",
                          bytes[0] as char, bytes[1] as char);
                Some(CassandraVersion::Unknown(format!("{}{}",
                                                       bytes[0] as char,
                                                       bytes[1] as char)))
            } else {
                None
            }
        }
    }
}
```

**Strategy 2: Version Flag**

Add `--allow-unknown-formats` CLI flag:

```bash
cqlite --allow-unknown-formats --data-dir test-data/sstables
```

Internally, attempt best-effort parsing with warnings.

**Strategy 3: Format Database**

Maintain online database of known magic numbers:

```rust
async fn fetch_format_database() -> Result<HashMap<u32, FormatInfo>> {
    // Fetch from cqlite.dev/formats.json
    // Cache locally
}
```

Update periodically to support new Cassandra releases.

## 8. Visualization: Magic Number Families

### Alphabetic Family Tree

```
ASCII "Letter-Letter" Family (Structured)
├── 0x6F61_0000 ("oa") - Legacy/Production
├── 0x6461_0000 ("da") - BTI Format
├── 0x???? ("nb") - New Big (not in list, but referenced)
└── 0x???? ("ma", "la", "ka") - Historical versions

Non-ASCII Alpha/Beta Family (Opaque)
├── 0xAD01_0000 - Alpha
├── 0xA007_0000 - Beta
└── 0x4316_0000 - Release

Component-Specific Family
├── 0x8080_015c - Data.db specific
└── 0x0000_0080 - Summary.db specific

Test/Format Variants (Semi-Structured)
├── 0x4325_0000 - Format D (ASCII 'C')
├── 0x4225_0000 - Format E (ASCII 'B')
├── 0xEA22_0000 - Format F
├── 0xAF03_0000 - Format G
└── 0x8c33_0000 - Format C

Extended Variants (Opaque)
├── 0x2C00_0000 - Variant A
├── 0xC302_0000 - Variant B
└── 0xF81E_0000 - Variant C
```

### Bit Distribution Heatmap

Analysis of which bytes vary across formats:

```
Byte Position:  [0]        [1]        [2]        [3]
Variation:      ████████   ████████   ██░░░░░░   ██░░░░░░
                (High)     (High)     (Low)      (Low)
```

- **Bytes 0-1**: Highly variable (format identification)
- **Bytes 2-3**: Mostly zero (padding/reserved)

**Exceptions**: Some formats use non-zero bytes 2-3:
- 0x8080_015c (Data.db)
- 0x0000_0080 (Summary.db)
- 0xC051_5C00 (Unknown)

These appear to be **component-specific** or **non-standard** variants.

## 9. Final Recommendations

### Question 1: Are magic numbers opaque or structured?

**Answer**: **Hybrid, but predominantly opaque**

- **High 16 bits** (bytes 0-1): Format identifier, sometimes ASCII mnemonic, sometimes arbitrary
- **Low 16 bits** (bytes 2-3): Usually zero-padding, occasionally component-specific data
- **No systematic bit encoding** of features like compression, CRC, or BTI support

### Question 2: Should we parse magic numbers differently?

**Answer**: **No major changes needed, but add flexibility**

**Keep**:
- Enum-based version matching
- Range detection for version families
- Explicit format support list

**Add**:
- Heuristic detection for ASCII "XY\0\0" patterns
- Warning system for unknown but plausible formats
- Optional forward-compatibility mode

**Implementation**:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CassandraVersion {
    // Explicit versions
    Legacy,      // "oa"
    V5_0Bti,     // "da"
    // ...

    // Dynamic unknown version (forward compatibility)
    Unknown(u32), // Store raw magic number
}

impl CassandraVersion {
    pub fn from_magic_number_lenient(magic: u32) -> CassandraVersion {
        // Try known formats first
        if let Some(version) = Self::from_magic_number(magic) {
            return version;
        }

        // Attempt heuristic detection
        let bytes = magic.to_be_bytes();
        if bytes[0].is_ascii_lowercase() && bytes[1].is_ascii_lowercase()
           && bytes[2] == 0 && bytes[3] == 0 {
            log::warn!("Unknown SSTable version code: {}{} (0x{:08X}), attempting best-effort parsing",
                      bytes[0] as char, bytes[1] as char, magic);
            return CassandraVersion::Unknown(magic);
        }

        // Completely unrecognized
        log::error!("Unrecognized magic number: 0x{:08X}", magic);
        CassandraVersion::Unknown(magic)
    }
}
```

### Question 3: Can we predict unknown magic numbers?

**Answer**: **Partially yes, for ASCII-based formats**

- **0xDE15_0000**: Likely corrupt or non-Cassandra format (no ASCII pattern)
- **0x5732_0000**: Could be "W2" version (ASCII 'W', '2') - plausible future format
- **0xD464_5400**, **0xB57C_6400**, **0xC051_5C00**: Non-standard, likely corruption or alternative storage engine

**Action**: Add these to test suite as "suspicious but parseable" formats.

## 10. Validation Against Real Data

### Test Plan

To validate these findings:

1. **Collect all SSTable files from test data**:
```bash
find test-data/datasets/sstables -name "*Data.db" -exec hexdump -C {} \; | head -20
```

2. **Extract first 4 bytes from each**:
```bash
find test-data/datasets/sstables -name "*Data.db" -exec sh -c 'echo -n "{}: "; xxd -p -l 4 "{}"' \;
```

3. **Categorize magic numbers**:
- Known formats: Continue processing
- ASCII "XY" pattern: Log as unknown but attempt parsing
- Non-ASCII: Flag as corrupted/unsupported

4. **Compare against sstabledump**:
For files with unknown magic numbers, verify if Apache Cassandra's sstabledump can read them.

### Expected Outcomes

- **Known formats (oa, da, etc.)**: ✅ Parse successfully
- **Unknown ASCII formats (W2, etc.)**: ⚠️ Attempt parsing with warnings
- **Non-ASCII unknowns**: ❌ Reject as corrupted

---

## Appendix: Magic Number Quick Reference

### Decoding Guide

To manually decode a magic number:

1. **Convert to hex**: e.g., 1867939840 → 0x6F610000
2. **Split into bytes**: [0x6F, 0x61, 0x00, 0x00]
3. **Check for ASCII**: 0x6F='o', 0x61='a' → "oa"
4. **Look up version**: "oa" = Cassandra 5.0 Legacy format

### Code Snippet for Quick Analysis

```rust
fn analyze_magic_number(magic: u32) {
    let bytes = magic.to_be_bytes();
    println!("Magic: 0x{:08X}", magic);
    println!("Binary: {:08b} {:08b} {:08b} {:08b}",
             bytes[0], bytes[1], bytes[2], bytes[3]);
    println!("Hex bytes: [{:02X}, {:02X}, {:02X}, {:02X}]",
             bytes[0], bytes[1], bytes[2], bytes[3]);

    if bytes[0].is_ascii() && bytes[1].is_ascii() {
        println!("ASCII: \"{}{}{}{\"",
                bytes[0] as char,
                bytes[1] as char,
                if bytes[2].is_ascii() { bytes[2] as char } else { '?' },
                if bytes[3].is_ascii() { bytes[3] as char } else { '?' });
    }

    if bytes[2] == 0 && bytes[3] == 0 {
        println!("Structure: Version code (bytes 0-1) + zero padding");
    }
}
```

---

## Conclusion

Cassandra SSTable magic numbers are **opaque format identifiers** with **partial ASCII mnemonic encoding**, not structured bit-encoded metadata. They function like file signatures (PNG magic, JPEG SOI, etc.) rather than feature flags.

**Key Takeaway**: Treat magic numbers as **lookup keys** in a version registry, not as **data structures** to be parsed.

**Recommended Action**: Extend CQLite's format registry with new magic numbers as they're discovered, using heuristic ASCII detection as a forward-compatibility fallback.
