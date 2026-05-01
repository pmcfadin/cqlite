# Fix Index.db Multi-Partition Enumeration (Issue #445)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix IndexWriter to produce BIG-format Index.db that IndexReader can parse, enabling multi-partition roundtrip.

**Architecture:** The IndexWriter currently writes `[key_len:u16][raw_key_bytes]` but IndexReader expects `[0x0010:marker][16-byte MD5 digest]`. Fix the writer to emit the correct BIG format: `0x0010` marker + MD5 digest of partition key bytes + VInt offset + VInt promoted_size. Update writer unit tests to match the new format.

**Tech Stack:** Rust, md5 crate (already a dependency), nom (reader), Cassandra BIG Index.db format

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `cqlite-core/src/storage/sstable/writer/index_writer.rs` | Modify | Fix `write_entry()` to emit 0x0010 marker + MD5 digest |
| `cqlite-core/Cargo.toml` | Modify (if needed) | Ensure `md5` crate is available for writer |
| `cqlite-core/tests/write_read_roundtrip/index.rs` | No change | Existing roundtrip tests validate the fix |

---

### Task 1: Add md5 dependency check and fix IndexWriter format

**Files:**
- Modify: `cqlite-core/src/storage/sstable/writer/index_writer.rs:148-173` (write_entry method)
- Modify: `cqlite-core/Cargo.toml` (if md5 not already available)

- [ ] **Step 1: Verify md5 crate is available to index_writer**

Run: `grep 'md5' cqlite-core/Cargo.toml`

The md5 crate should already be a dependency (used by index_reader.rs). If not, add it.

- [ ] **Step 2: Write a failing unit test that validates the correct BIG format**

Add this test to `index_writer.rs` `mod tests`:

```rust
#[test]
fn test_big_format_marker_and_digest() {
    let mut writer = IndexWriter::new();
    let key = DecoratedKey::new(12345, vec![0x00, 0x00, 0x00, 0x2A]);
    writer.add_partition(&key, 0).unwrap();
    let bytes = writer.finish().unwrap();

    // BIG format: [0x0010:marker][16-byte MD5 digest][vint offset][vint promoted]
    // Total: 2 + 16 + 1 + 1 = 20 bytes
    assert_eq!(bytes.len(), 20);

    // Check marker
    assert_eq!(&bytes[0..2], &[0x00, 0x10], "Marker should be 0x0010");

    // Check MD5 digest matches
    let expected_digest = md5::compute(&[0x00, 0x00, 0x00, 0x2A]);
    assert_eq!(&bytes[2..18], expected_digest.as_slice(), "Should be MD5 of key bytes");

    // Check offset VInt(0) and promoted VInt(0)
    assert_eq!(bytes[18], 0x00, "Offset should be 0");
    assert_eq!(bytes[19], 0x00, "Promoted size should be 0");
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cargo test --package cqlite-core --lib -- storage::sstable::writer::index_writer::tests::test_big_format_marker_and_digest`
Expected: FAIL (currently writes key_len + raw bytes, not marker + digest)

- [ ] **Step 4: Fix write_entry() to produce correct BIG format**

Replace the `write_entry` method in `index_writer.rs`:

```rust
fn write_entry(&mut self, key: &DecoratedKey, data_offset: u64) -> Result<usize> {
    let start_len = self.buffer.len();

    // Write BIG format marker (0x0010)
    self.buffer
        .write_all(&0x0010u16.to_be_bytes())
        .map_err(|e| Error::Storage(format!("Failed to write marker: {}", e)))?;

    // Write MD5 digest of partition key bytes (16 bytes)
    let digest = md5::compute(&key.key);
    self.buffer
        .write_all(digest.as_slice())
        .map_err(|e| Error::Storage(format!("Failed to write key digest: {}", e)))?;

    // Write position (unsigned VInt encoded)
    encode_unsigned(data_offset, &mut self.buffer);

    // Write promoted index length (0 = no promoted index)
    encode_unsigned(0, &mut self.buffer);

    let bytes_written = self.buffer.len() - start_len;
    Ok(bytes_written)
}
```

Also add `use md5;` at the top of the file (or in the function scope if preferred).

- [ ] **Step 5: Run the new unit test to verify it passes**

Run: `cargo test --package cqlite-core --lib -- storage::sstable::writer::index_writer::tests::test_big_format_marker_and_digest`
Expected: PASS

- [ ] **Step 6: Update existing unit tests to match new format**

The existing unit tests hardcode byte offsets based on the old `[key_len:u16][raw_key]` format. They need updating to the new `[0x0010:marker][16-byte digest]` format. Each entry is now always exactly `2 (marker) + 16 (digest) + N (vint offset) + 1 (vint promoted) = 19 + N` bytes.

Key size changes:
- Old single entry (4-byte key, offset=0): 2+4+1+1 = 8 bytes
- New single entry (offset=0): 2+16+1+1 = 20 bytes
- Old single entry (4-byte key, offset=150): 2+4+2+1 = 9 bytes
- New single entry (offset=150): 2+16+2+1 = 21 bytes

Update all assertions in existing tests to use 20-byte base size instead of variable key-dependent sizes.

- [ ] **Step 7: Run all index_writer unit tests**

Run: `cargo test --package cqlite-core --lib -- storage::sstable::writer::index_writer::tests`
Expected: ALL PASS

- [ ] **Step 8: Commit**

```bash
git add cqlite-core/src/storage/sstable/writer/index_writer.rs
git commit -m "fix(index_writer): emit BIG format (0x0010 marker + MD5 digest) instead of raw key bytes

Fixes #445. The IndexWriter was producing [key_len:u16][raw_key_bytes]
but IndexReader expects [0x0010:marker][16-byte MD5 digest]. This caused
multi-partition enumeration to return too few entries because the reader
couldn't parse the writer's output."
```

---

### Task 2: Validate roundtrip tests pass

**Files:**
- Read-only: `cqlite-core/tests/write_read_roundtrip/index.rs`

- [ ] **Step 1: Run all index roundtrip tests**

Run: `cargo test --package cqlite-core --features write-support --test write_read_roundtrip -- index`
Expected: ALL 5 tests PASS (single partition, multiple partitions, write engine, large offsets, key digest)

- [ ] **Step 2: Run summary roundtrip test that depends on index**

Run: `cargo test --package cqlite-core --features write-support --test write_read_roundtrip -- summary::test_summary_offset_tracking_with_index`
Expected: PASS

- [ ] **Step 3: Run ALL roundtrip tests to check for regressions**

Run: `cargo test --package cqlite-core --features write-support --test write_read_roundtrip`
Expected: ALL PASS

---

### Task 3: Full build validation

- [ ] **Step 1: Run clippy with CI flags**

Run: `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features`
Expected: No warnings or errors

- [ ] **Step 2: Run cargo fmt check**

Run: `cargo fmt --check`
Expected: No formatting issues

- [ ] **Step 3: Run full test suite**

Run: `cargo test --workspace --all-features`
Expected: ALL PASS

- [ ] **Step 4: Commit any fixes from validation**

Only if clippy or fmt required changes.

---

### Task 4: Code review and push

- [ ] **Step 1: Review changes with rust-reviewer subagent**

Use the `rust-reviewer` subagent to review all changes for memory safety, code quality, and project conventions.

- [ ] **Step 2: Push to remote**

Run: `git push`
