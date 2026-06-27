# Context7 References for Rust Crates

Use Context7 MCP tool to fetch the latest documentation for these crates.

## bytes crate

**Context7 Library ID:** `/tokio-rs/bytes`

**Use for:**
- `Bytes` and `BytesMut` API
- Zero-copy buffer patterns
- Slicing and cloning semantics
- Buf and BufMut traits

**Example queries:**
- "Fetch bytes crate documentation using Context7 for /tokio-rs/bytes"
- "How does Bytes::slice work? Use Context7 /tokio-rs/bytes"
- "What's the difference between Bytes and BytesMut? Context7 /tokio-rs/bytes"

**Topics to explore:**
- Buffer management
- Reference counting
- Slicing without copying
- Conversion from Vec<u8>
- Integration with I/O operations

---

## tokio async runtime

**Context7 Library ID:** `/tokio-rs/tokio`

**Use for:**
- Async I/O with `tokio::fs` and `tokio::io`
- Task spawning and management
- Async file reading patterns
- `spawn_blocking` for CPU-intensive work
- Streams and futures

**Example queries:**
- "Fetch tokio documentation using Context7 for /tokio-rs/tokio"
- "How to read files asynchronously? Context7 /tokio-rs/tokio"
- "Best practices for spawn_blocking? Context7 /tokio-rs/tokio"

**Topics to explore:**
- File I/O (`tokio::fs::File`)
- Async read/write traits
- Task management
- Spawning blocking tasks
- Streams (`tokio_stream`)
- Synchronization primitives

---

## serde serialization

**Context7 Library ID:** `/serde-rs/serde`

**Use for:**
- Serialization framework patterns
- Derive macros
- Custom serializers
- JSON, CSV, Parquet integration (M3)

**Example queries:**
- "Fetch serde documentation using Context7 for /serde-rs/serde"
- "How to implement custom deserializer? Context7 /serde-rs/serde"
- "Serde zero-copy deserialization? Context7 /serde-rs/serde"

**Topics to explore:**
- Derive macros (`Serialize`, `Deserialize`)
- Custom serialization
- Zero-copy deserialization
- Visitor pattern
- Integration with output formats (M3)

---

## Related Crates (Not in Context7)

These crates may not be available via Context7. Refer to docs.rs:

### lz4 compression
**Docs:** https://docs.rs/lz4/
- Block compression/decompression
- Streaming compression
- Frame format

### snap (Snappy)
**Docs:** https://docs.rs/snap/
- Snappy compression
- Raw vs framed format

### flate2 (Deflate)
**Docs:** https://docs.rs/flate2/
- Deflate/gzip compression
- Compression levels
- Streaming interface

### nom parser combinators
**Docs:** https://docs.rs/nom/
- Binary parsing patterns
- VInt parsing
- Error handling
- Zero-copy parsing

## How to Use Context7

### In Claude Code

When you need latest documentation:
```
User: "How should I use Bytes::slice for zero-copy parsing?"
AI: "Let me fetch the latest bytes crate documentation..."
[Uses Context7 to fetch /tokio-rs/bytes]
[Provides answer based on latest docs]
```

### Explicitly Request

You can explicitly request documentation:
```
"Fetch bytes crate documentation using Context7 for /tokio-rs/bytes 
and show me zero-copy patterns"
```

### During Implementation

When implementing new features:
1. Request relevant Context7 docs
2. Review latest API patterns
3. Implement using current best practices
4. Reference docs in code comments

## Version Considerations

Context7 provides latest stable documentation. For version-specific needs:
- Check `Cargo.toml` for pinned versions
- Use docs.rs for specific version docs
- Test compatibility with our version

## PRD Alignment

These crates support:
- **M1:** Zero-copy reading (bytes, tokio)
- **M3:** Output formats (serde + format crates)
- **M4:** Language bindings (async patterns)
- **M6:** Performance targets (efficient I/O)

## Example Workflow

### Implementing New Async Reader

1. **Fetch Context7 docs:**
   ```
   "Fetch tokio and bytes documentation using Context7"
   ```

2. **Review patterns:**
   - Async file opening
   - Reading with `AsyncReadExt`
   - Buffer management with Bytes

3. **Implement:**
   ```rust
   use tokio::fs::File;
   use tokio::io::AsyncReadExt;
   use bytes::Bytes;
   
   async fn read_sstable(path: &Path) -> Result<Bytes> {
       // Pattern from Context7 docs
       let mut file = File::open(path).await?;
       let mut buffer = Vec::new();
       file.read_to_end(&mut buffer).await?;
       Ok(Bytes::from(buffer))
   }
   ```

4. **Validate:**
   - Check against Context7 best practices
   - Ensure zero-copy where possible
   - Verify async patterns

## Updating Dependencies

When updating Rust crates:
1. Fetch latest Context7 docs for breaking changes
2. Review changelog on docs.rs
3. Update code to new patterns
4. Run full test suite
5. Update skill documentation if patterns change

## Summary

Use Context7 for:
- ✅ bytes (`/tokio-rs/bytes`)
- ✅ tokio (`/tokio-rs/tokio`)
- ✅ serde (`/serde-rs/serde`)

Refer to docs.rs for:
- Compression crates (lz4, snap, flate2)
- Parser combinators (nom)
- Version-specific documentation

Always validate Context7 patterns against project's pinned versions in `Cargo.toml`.

