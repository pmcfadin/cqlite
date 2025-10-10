# Appendix C — Reference Walkthroughs with Code

In this appendix you will learn:
- An end-to-end `Data.db` read path using Cassandra concepts and components
- Where Index and Summary readers participate in the read path
- How to correlate types to parsing behavior

## Walkthrough: Data.db point read (Cassandra semantics)

Conceptually, a point read follows: Bloom → Index → Summary → Data. Cassandra defines serialization via `SerializationHeader` and marshaller types, while `IndexSummary` and `RowIndexEntry` guide seeks.

Pinned upstream anchors (5.0.0):
- `SSTableReader` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/SSTableReader.java`
- `IndexSummary` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/IndexSummary.java`
- `RowIndexEntry` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/RowIndexEntry.java`
- `SerializationHeader` — `https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/db/SerializationHeader.java`

Step-by-step (using `test-data/datasets/test_basic` as mental model):
1) Bloom filter: check partition key digest in `Filter.db` (negative → stop).
2) Summary: binary search in `Summary.db` over tokens to find nearest `index_offset`.
3) Index: scan `Index.db` from `index_offset` to find matching partition digest; read `RowIndexEntry`.
4) Data: seek to `Data.db` position from `RowIndexEntry`; read partition header, then row/cell payloads using `SerializationHeader`.

Tiny trimmed example (conceptual):
```
Summary entry: token=12345 index_offset=0x0000_2A10
Index entry: key_digest=ab..cd data_offset=0x0001_0030
Data read @0x0001_0030: partition header + row [len=0x12] ...
```

Row and cell serialization are defined by `rows.*` and the marshaller types (`db.marshal.*`).

Index and Summary provide navigation primitives: `IndexSummary` samples `RowIndexEntry` positions for efficient seeks into `Index.db` and then `Data.db`.

## Walkthrough: Header CRC32 Validation (Cassandra 5.0+)

Some Cassandra 5.0 SSTables prepend a 4-byte CRC32 checksum to protect header integrity. This walkthrough demonstrates detection and validation using real test data.

### Real Example: collection_clustering_table

```bash
$ hexdump -C nb-1-big-Data.db | head -3
00000000  71 16 00 00 f2 09 00 10  d7 51 56 cb c5 a7 45 56  |q........QV...EV|
00000010  93 05 be 61 38 94 68 bf  7f ff ff ff 80 00 01 00  |...a8.h.........|
00000020  f1 83 64 82 c9 1e c2 47  1d c2 47 1c 00 02 08 10  |..d....G..G.....|
```

**Analysis**:
- Offset 0x00: `71 16 00 00` → `0x71160000` (big-endian)
- Not a known magic number → detected as CRC32 checksum
- Offset 0x04: `f2 09 00 10` → starts with next data
- After checksum skip, offset 0x04 should have magic number

### Step-by-Step Validation

**Step 1: Read and detect**
```rust
use std::io::Read;

let mut file = File::open("nb-1-big-Data.db")?;
let mut first_4 = [0u8; 4];
file.read_exact(&mut first_4)?;

let potential_magic = u32::from_be_bytes(first_4);
// 0x71160000 - check known magic numbers

if CassandraVersion::from_magic_number(potential_magic).is_none() {
    println!("Detected header CRC32: 0x{:08x}", potential_magic);
    // This is a checksum, not a magic number
}
```

**Step 2: Read remaining header data**
```rust
// Read rest of header (example: 4KB buffer)
let mut header_data = vec![0u8; 4096];
file.read_exact(&mut header_data)?;

// Actual magic number should be at start of header_data
let actual_magic = u32::from_be_bytes([
    header_data[0], header_data[1],
    header_data[2], header_data[3]
]);

println!("Actual magic at offset 4: 0x{:08x}", actual_magic);
// Expected: 0xf2090010 or 0x00400000 (depends on byte order interpretation)
```

**Step 3: Compute and validate CRC32**
```rust
use crc32fast::Hasher;

let mut hasher = Hasher::new();
hasher.update(&header_data);
let computed_checksum = hasher.finalize();

let expected_checksum = potential_magic; // from first 4 bytes

if computed_checksum == expected_checksum {
    println!("✓ Header checksum valid");
    // Proceed with parsing header_data
} else {
    eprintln!("✗ Header checksum mismatch!");
    eprintln!("  Expected: 0x{:08x}", expected_checksum);
    eprintln!("  Computed: 0x{:08x}", computed_checksum);
    return Err(Error::HeaderCorruption);
}
```

**Step 4: Parse validated header**
```rust
// Now parse header_data (which starts with magic number)
let header = parse_sstable_header(&header_data)?;
println!("Header parsed successfully:");
println!("  Version: {:?}", header.cassandra_version);
println!("  Keyspace: {}", header.keyspace);
println!("  Table: {}", header.table_name);
```

### Test Data Verification

Using real test data from `test-data/datasets/sstables/test_collections/`:

| Table | First 4 Bytes | Is Checksum? | Actual Magic @ Offset 4 |
|-------|---------------|--------------|-------------------------|
| `collection_clustering_table` | `0x71160000` | ✅ Yes | After checksum |
| `empty_collections_table` | `0xf1185c00` | ✅ Yes | After checksum |
| `collection_table` | `0x8080015c` | ❌ No (magic) | N/A |
| `simple_table` | `0x00400000` | ❌ No (magic) | N/A |

### Error Case: Checksum Mismatch

Simulated corruption example:
```rust
// Flip one bit in header data to simulate corruption
header_data[100] ^= 0x01;

let computed = crc32fast::hash(&header_data);
// Now: computed != expected_checksum

// Error output:
// Header checksum mismatch for nb-1-big-Data.db
// Expected: 0x71160000
// Computed: 0x71160042
// Action:  Reject file, trigger repair
```

### Performance Considerations

**CRC32 computation cost**:
- Modern CPUs: ~1-2 GB/s (using `crc32fast` with SIMD)
- 4KB header: ~2-4 microseconds
- Negligible overhead for early corruption detection

**Trade-off**: Header validation cost << cost of parsing corrupt metadata

### Integration Pattern

```rust
pub async fn parse_header_with_validation(
    file_path: &Path
) -> Result<SSTableHeader> {
    let mut file = File::open(file_path)?;
    let mut buffer = vec![0u8; 8192]; // Read generous header size
    file.read_exact(&mut buffer)?;

    // Step 1: Detect checksum
    let first_4 = u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);

    let header_data = if CassandraVersion::from_magic_number(first_4).is_none() {
        // Has checksum prefix
        let expected_crc = first_4;
        let data = &buffer[4..]; // Skip checksum

        // Validate
        let computed_crc = crc32fast::hash(data);
        if computed_crc != expected_crc {
            return Err(Error::header_checksum_mismatch(
                file_path, expected_crc, computed_crc
            ));
        }

        data // Parse from offset 4
    } else {
        &buffer[..] // No checksum, parse from offset 0
    };

    // Step 2: Parse validated header
    parse_header_data(header_data)
}
```

## Key Takeaways
- Schema-aware decoding eliminates guesswork; comparators come from the schema.
- Index and Summary readers narrow reads before hitting `Data.db` bytes.
- **Header CRC32 validation** (Cassandra 5.0+) detects corruption before parsing.
- Use `crc32fast::hash()` for efficient checksum computation.
- Validate with small, trimmed output from real SSTables (e.g., `sstabledump | head -n 10`).

## References
- Cassandra 5.0: `SSTableReader`, `IndexSummary`, `RowIndexEntry`, `SerializationHeader` (see Source Map)
- CQLite: Header CRC32 validation in `cqlite-core/src/storage/sstable/reader/header.rs`
- CRC32 library: `crc32fast` crate (https://docs.rs/crc32fast/)

