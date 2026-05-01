# Issue #453: Fix test_component_binary_formats EncodingStats Assertion

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the failing `test_component_binary_formats` test by correcting unrealistic test metadata values that cause wrapping arithmetic in EncodingStats delta encoding.

**Architecture:** The SERIALIZATION_HEADER component of Statistics.db encodes three unsigned VInt deltas: `(minTimestamp - TIMESTAMP_EPOCH)`, `(minLocalDeletionTime - DELETION_TIME_EPOCH)`, `(minTTL - TTL_EPOCH)`. The writer is correct — the test uses metadata values far below the epoch constants, causing wrapping to huge u64 values. The fix is to use realistic metadata values and update assertions accordingly.

**Tech Stack:** Rust, `cqlite-core` crate, `write-support` feature flag

---

## Root Cause Analysis

The test sets `meta.min_timestamp = 1000000` (1 second after Unix epoch in microseconds). The SERIALIZATION_HEADER's EncodingStats computes:

```
delta = 1_000_000u64.wrapping_sub(1_442_880_000_000_000u64)
      = 18_446_742_630_829_552_616  (huge wrapped value)
```

`encode_vuint(18_446_742_630_829_552_616)` produces a 9-byte encoding starting with `0xFF`. The test asserts `0x00`, hence the failure.

Similarly, `min_local_deletion_time = 0` is below `DELETION_TIME_EPOCH = 1_442_880_000`, and `min_ttl = 100` is fine (TTL_EPOCH = 0, delta = 100).

**The writer is correct.** The test metadata is unrealistic. Fix: use timestamp values >= their respective epochs so deltas are small positive numbers.

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `cqlite-core/src/storage/sstable/writer/stats_writer.rs` | Modify (test only) | Fix test metadata values and assertions |

---

### Task 1: Fix test metadata to use realistic values

**Files:**
- Modify: `cqlite-core/src/storage/sstable/writer/stats_writer.rs:1123-1221`

- [ ] **Step 1: Update test metadata values**

In the `test_component_binary_formats` test, change the metadata setup from unrealistic values to values at or above the epoch constants:

```rust
// Old (unrealistic - causes wrapping):
let mut meta = StatisticsMetadata::new();
meta.min_timestamp = 1000000;
meta.max_timestamp = 2000000;
meta.min_local_deletion_time = 0;
meta.max_local_deletion_time = 0;
meta.min_ttl = 100;
meta.max_ttl = 200;

// New (realistic - above epoch baselines):
let mut meta = StatisticsMetadata::new();
meta.min_timestamp = 1442880000000000;  // == TIMESTAMP_EPOCH (delta = 0)
meta.max_timestamp = 1442880000000000 + 1000000;
meta.min_local_deletion_time = 1442880000;  // == DELETION_TIME_EPOCH (delta = 0)
meta.max_local_deletion_time = 1442880000;
meta.min_ttl = 0;  // == TTL_EPOCH (delta = 0)
meta.max_ttl = 200;
```

Using epoch values exactly produces delta = 0 for all three fields, so the first byte of each is `0x00`.

- [ ] **Step 2: Update the STATS component min_timestamp assertion**

The assertion at line 1212 checks the min_timestamp value in the STATS component (i64 BE at offset 84):

```rust
// Old:
assert_eq!(min_ts, 1000000, "Min timestamp should be preserved");

// New:
assert_eq!(min_ts, 1442880000000000, "Min timestamp should be preserved");
```

- [ ] **Step 3: Keep the SERIALIZATION_HEADER assertion as-is**

The existing assertion `assert_eq!(file_data[header_offset], 0x00, ...)` is now correct because all three deltas are 0, so the first vuint byte is indeed `0x00`. No change needed.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p cqlite-core --features write-support test_component_binary_formats -- --nocapture`
Expected: PASS

- [ ] **Step 5: Add a second assertion for the other two EncodingStats deltas**

To strengthen the test, verify all three deltas are encoded as `0x00`:

```rust
// Verify SERIALIZATION_HEADER component
// Should start with 3 unsigned VInts for EncodingStats deltas
// All should be 0x00 (vuint encoding of 0) since metadata values == epoch baselines
assert_eq!(
    file_data[header_offset], 0x00,
    "EncodingStats minTimestamp delta should be 0"
);
assert_eq!(
    file_data[header_offset + 1], 0x00,
    "EncodingStats minLocalDeletionTime delta should be 0"
);
assert_eq!(
    file_data[header_offset + 2], 0x00,
    "EncodingStats minTTL delta should be 0"
);
```

- [ ] **Step 6: Run the test again to verify all new assertions pass**

Run: `cargo test -p cqlite-core --features write-support test_component_binary_formats -- --nocapture`
Expected: PASS

### Task 2: Verify no regressions

- [ ] **Step 1: Run all stats_writer tests**

Run: `cargo test -p cqlite-core --features write-support stats_writer -- --nocapture`
Expected: All tests pass

- [ ] **Step 2: Run clippy**

Run: `RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features`
Expected: No warnings or errors

- [ ] **Step 3: Run cargo fmt**

Run: `cargo fmt`
Expected: No changes (or apply formatting)

- [ ] **Step 4: Run full test suite**

Run: `env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test --package cqlite-core`
Expected: All tests pass

### Task 3: Commit

- [ ] **Step 1: Commit the fix**

```bash
git add cqlite-core/src/storage/sstable/writer/stats_writer.rs
git commit -m "fix(#453): use realistic epoch-based metadata in test_component_binary_formats

Root cause: test used min_timestamp=1000000 (far below TIMESTAMP_EPOCH=1442880000000000),
causing wrapping_sub to produce huge u64 delta encoded as 0xFF instead of expected 0x00.

Fix: use epoch values as metadata baselines so EncodingStats deltas are 0.
Added assertions for all three EncodingStats delta bytes."
```
