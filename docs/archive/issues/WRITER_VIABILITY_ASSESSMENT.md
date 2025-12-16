# SSTable Writer Viability Assessment

**TL;DR: No. The current writer is not production-viable and will need a complete rewrite when you get to M5.**

---

## Critical Findings

### 1. Never Validated Against Cassandra

**Evidence:**
- All tests are internal round-trips (write → read back with CQLite reader)
- Validation code explicitly says: "This would require having Cassandra installed and accessible. For now, we'll do a basic round-trip test within CQLite" (`validation.rs:311-312`)
- Round-trip test comment: "TODO: Implement reader validation. For now, just check that the file was created successfully" (`validation.rs:347-348`)

**Translation:** The writer has **never** produced a file that Cassandra can actually read.

---

### 2. Known Compatibility Issues (From Your Own Docs)

Your compatibility analysis document (`docs/reports/validation-reports/WRITER_COMPATIBILITY_ANALYSIS.md`) identifies **9 CRITICAL issues**:

| Issue | Current (Wrong) | Required | Impact |
|-------|----------------|----------|--------|
| **File Layout** | Single `.sst` file | Multi-file (Data.db, Index.db, Summary.db, Statistics.db, Filter.db, CompressionInfo.db, TOC.txt) | Cassandra won't recognize it |
| **Endianness** | Little-endian | Big-endian | All numbers garbled |
| **Index Format** | Bincode serialization | BTI (Binary Tree Index) format | Index unreadable |
| **Bloom Filter** | Custom format | Cassandra bloom format | Filter rejected |
| **VInt Encoding** | Simple varint | Cassandra-specific VInt rules | Fields misaligned |
| **Compression** | Different parameters | Exact Cassandra format | Won't decompress |
| **Header Structure** | Custom | Exact 'oa' format | Cassandra rejects file |
| **Statistics** | Custom structure | Statistics.db format | Metadata missing |
| **Checksums** | CRC32 custom polynomial | Cassandra checksum impl | Corruption detection fails |

---

### 3. Type Mapping Hacks

**From `writer.rs:831-856`:**
```rust
crate::types::DataType::Set => DataType::LIST,      // Map to closest existing
crate::types::DataType::Tuple => DataType::LIST,    // Map to closest existing
crate::types::DataType::Udt => DataType::JSON,      // Map to closest existing
crate::types::DataType::Frozen => DataType::BLOB,   // Map to closest existing
crate::types::DataType::Tombstone => DataType::BLOB, // Map to closest existing
```

These are **placeholders**. You can't write a Cassandra SET as a LIST and expect Cassandra to read it correctly.

---

## Why Reading Knowledge Won't Transfer Easily

### What You Know From Building the Reader:

✅ Exact byte layout of Cassandra 5 'oa' format  
✅ VInt encoding rules  
✅ Compression block structure  
✅ Cell format and flags  
✅ Multi-file component layout (Data.db, Index.db, etc.)  
✅ BTI index structure  
✅ Bloom filter binary format  

### What's Different for Writing:

❌ **No room for defensive coding** - Must produce EXACTLY correct bytes  
❌ **No heuristics allowed** - Reader can guess; writer can't  
❌ **Must handle edge cases perfectly** - One wrong byte breaks the whole file  
❌ **Must generate indices correctly** - BTI tree construction is complex  
❌ **Must match Cassandra's checksums** - Different algorithm = corruption  
❌ **Must coordinate multiple file writes** - Transaction semantics across 7+ files  

### The Hard Parts:

1. **BTI Index Construction**: Cassandra uses a complex binary trie structure. You'd need to:
   - Build the tree in memory
   - Serialize it exactly as Cassandra does
   - Handle posting lists correctly
   - Match byte-for-byte with Cassandra's output

2. **Multi-File Coordination**: Writing a complete SSTable requires:
   ```
   Data.db          ← Primary data with exact cell format
   Index.db         ← BTI structure pointing into Data.db
   Summary.db       ← Sparse index sampling
   Statistics.db    ← Table-level stats in specific format
   Filter.db        ← Bloom filter binary
   CompressionInfo.db ← Block offsets and checksums
   TOC.txt          ← Component list
   ```
   All must be consistent. One offset wrong = corrupted SSTable.

3. **Partition/Clustering Key Ordering**: Cassandra has specific comparator logic. If you write keys in the wrong order, the file is invalid.

4. **Tombstone Markers**: Different tombstone types (row deletion, range deletion, TTL expiry) have specific encodings.

---

## Will You Need the Current Writer Code?

**No, for these reasons:**

### 1. Architecture is Wrong
Current writer is monolithic and tied to the write infrastructure (WAL, memtable, compaction). You'd need:
```rust
// M5 writer should be:
pub struct CassandraSSTableWriter {
    table_writer: DataFileWriter,      // Data.db
    index_builder: BTIBuilder,         // Index.db
    bloom_builder: BloomBuilder,       // Filter.db
    // ... one per component
}
```

### 2. Knowledge Transfer is Format, Not Code
What you learned building the reader:
- **Reusable**: Format specs, byte layouts, compression parameters
- **Not reusable**: Defensive parsing logic, error recovery, heuristics

You'd reference your reader code for format details, but write fresh code optimized for exact output generation.

### 3. Testing Requirements Are Different
- Reader: "Can I parse this weird/corrupted file?"
- Writer: "Does Cassandra accept my file? Can I bulk load it?"

You'd need integration tests with **actual Cassandra** running, trying to:
- `nodetool refresh` your generated SSTables
- Run `SELECT` queries against them
- Verify data matches what you wrote

---

## Recommendation: Pragmatic Approach

### Phase 1 (Now - M1/M2)
✅ **DELETE** the writer code from `cqlite-core`  
✅ Keep the compatibility analysis document as reference  
✅ Focus on reader maturity

**Why?**
- Keeping dead code suggests it works (it doesn't)
- Maintaining it is a distraction
- M5 is 3+ milestones away

### Phase 2 (M5 Planning)
When you're ready to build write support:

1. **Study Cassandra Source**: Look at:
   - `org.apache.cassandra.io.sstable.format.big.BigFormat`
   - `org.apache.cassandra.io.sstable.format.SSTableWriter`
   - `org.apache.cassandra.db.rows.UnfilteredRowIterator`

2. **Build Component Writers**: Start with simplest:
   ```
   Step 1: Data.db writer (no index, no bloom)
   Step 2: TOC.txt writer
   Step 3: Add Index.db (BTI)
   Step 4: Add Filter.db (Bloom)
   Step 5: Add Statistics.db
   Step 6: Add CompressionInfo.db
   Step 7: Add Summary.db
   ```

3. **Test Against Cassandra**:
   ```bash
   # Generate SSTable with CQLite
   cqlite write --schema schema.cql --data data.json --out /tmp/test/

   # Try to load in Cassandra
   docker exec cassandra nodetool refresh keyspace table

   # Query it
   docker exec cassandra cqlsh -e "SELECT * FROM keyspace.table"
   ```

4. **Iterate Until Compatible**: This is the **only** way to know it works.

---

## Answer to Your Question

> "Are we confident that the writer will be relevant after all the work we have done with the reader?"

**No. The writer is not relevant in its current form.**

**BUT:** The reader work is **essential** for building a correct writer. You've learned:
- Exact byte formats
- Compression details
- Index structures
- Bloom filter format

When you build M5, you'll reference your reader code constantly to ensure the writer produces what the reader expects. But you'll be writing new code from scratch, not reusing the current writer.

---

## Recommendation

**Delete the writer now.** Add this to your cleanup plan:

```bash
# Move to a branch for historical reference
git checkout -b archive/early-writer-attempt
git add cqlite-core/src/storage/sstable/writer.rs
git commit -m "Archive: Early writer implementation (not Cassandra-compatible)"
git push origin archive/early-writer-attempt

# Delete from main
git checkout main
git rm cqlite-core/src/storage/sstable/writer.rs
git rm cqlite-core/src/storage/batch_writer.rs
git rm cqlite-core/src/storage/wal.rs
git rm cqlite-core/src/storage/memtable.rs
git rm cqlite-core/src/storage/compaction.rs
git rm cqlite-core/src/storage/manifest.rs
git commit -m "Remove M5 write infrastructure (not M1/M2 scope)"
```

Keep the knowledge, lose the code.

---

**Bottom Line:** Building a correct Cassandra SSTable writer is **harder** than building a reader. The current code is not salvageable for production use. When you're ready for M5, start fresh with the format knowledge you've gained.

