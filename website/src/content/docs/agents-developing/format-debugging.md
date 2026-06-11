---
title: Format Debugging Workflow
description: Hex dumps, the definitive-guide chapters to consult, and appendix F for known limitations — how to debug SSTable binary format issues.
sidebar:
  label: Format debugging
  order: 6
---

Use this workflow when parsed output is wrong and you need to trace the problem to a
specific byte offset in a binary SSTable file.

## Step 1: Locate the definitive guide chapter

The SSTable format reference lives in `docs/sstables-definitive-guide/`. Start here
before reading source code.

| Problem area | Chapter to read |
|---|---|
| Wrong row data, flags, cell values | Ch.5: Data.db Format — rows, flags, V5CompressedLegacy |
| Partition lookup failures | Ch.6: Index.db/Summary.db — partition lookups |
| Compression block misalignment | Ch.9: CompressionInfo.db — chunk layout, checksums |
| BTI/trie index issues | Ch.17: BTI Formats |
| VInt encoding errors | Appendix B: Encoding Cheat Sheet — VInt, cell flags |
| "Does this format work yet?" | Appendix F: Known Limitations |

## Appendix F: known limitations

Before spending time debugging, check
`docs/sstables-definitive-guide/chapters/appendix-f-known-limitations.md`.

Current confirmed gaps (as of this writing):

- **da/BTI format**: BTI trie index parser is incomplete. da-keyspace tables are in
  the corpus but smoke-excluded (`SKIP-PENDING BTI`). Do not file bugs for da tables.
- **Set element tombstones** (issue #493): Tracked as out-of-scope for v0.9.1.
- **Counter tables**: Not supported.

If your parsing failure is in Appendix F, it is a known gap, not a regression.

## Step 2: Hex dump at the failing offset

```bash
# Dump 64 bytes starting at a specific offset
hexdump -C test-data/datasets/sstables/<keyspace>/<table>/<file>-Data.db \
  -s <offset> -n 64

# Dump the first 256 bytes (partition header area)
hexdump -C <file>-Data.db -n 256

# Find byte offset of a pattern (e.g., a known partition key)
grep -ob $'\x00\x10' <file>-Data.db | head  # not always useful for binary
```

## Step 3: Trace byte consumption through the row format

Cassandra 5.0 `nb`/`oa` row format sequence (from
[Ch.5](https://github.com/pmcfadin/cqlite/blob/main/docs/sstables-definitive-guide/)
and `UnfilteredSerializer.java`):

```
[1 byte:  row flags]
[0-1 byte: extended flags if bit 0x80 set]

--- Clustering prefix (tables with clustering columns only) ---
[VInt: header bitmap — 2 bits per column, batches of 32]
[bytes: column values for non-null/non-empty columns]

--- Row body ---
[VInt: row_size]
[VInt: prev_unfiltered_size]

[if HAS_TIMESTAMP (0x04):]
  [VInt: timestamp_delta from EncodingStats.minTimestamp]
  [if HAS_TTL (0x08):]
    [VInt: ttl_delta]
    [VInt: local_deletion_time_delta]

[if HAS_DELETION (0x10):]
  [VInt: deletion_timestamp_delta]
  [VInt: deletion_local_time_delta]

[if NOT HAS_ALL_COLUMNS (0x20 not set):]
  [VInt-encoded bitmap: which columns are present]

[for each present column:]
  [cell data — see Ch.5 cell format]
```

Row flags reference:

| Bit | Meaning |
|-----|---------|
| `0x01` | IS_MARKER (range tombstone) |
| `0x02` | reserved |
| `0x04` | HAS_TIMESTAMP |
| `0x08` | HAS_TTL |
| `0x10` | HAS_DELETION |
| `0x20` | HAS_ALL_COLUMNS |
| `0x40` | IS_STATIC |
| `0x80` | EXTENSION_FLAG |

## Step 4: Compare against Cassandra source

When CQLite behaviour diverges from the spec:

```bash
# Local Cassandra 5.0 source
grep -n "deserializeRowBody" ~/local_projects/cassandra/src/java/org/apache/cassandra/io/sstable/format/big/UnfilteredSerializer.java | head -20

# Remote: https://github.com/apache/cassandra/blob/cassandra-5.0.0/src/java/org/apache/cassandra/io/sstable/format/big/UnfilteredSerializer.java
```

Key Cassandra source files:

| File | Relevance |
|------|-----------|
| `io/sstable/format/big/UnfilteredSerializer.java` | Row/cell serialization — the ground truth |
| `db/rows/Cell.java` | Cell flags and encoding |
| `db/Clustering.java` | Clustering prefix encoding |
| `utils/vint/VIntCoding.java` | VInt encoding/decoding |

## Step 5: Add logging and reproduce

Add temporary logging to the parser:

```rust
// In v5_compressed_legacy.rs — for a single debugging session
eprintln!("[debug] offset={} flags={:#04x}", offset, flags);
```

Then run against a single table:

```bash
RUST_LOG=debug cargo test --package cqlite-integration-tests \
  --test fixture_specific_integration_tests -- --nocapture 2>&1 | head -100
```

Or use the CLI directly:

```bash
RUST_LOG=cqlite_core=debug cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables/test_basic/simple_table-<hash> \
  --query "SELECT * FROM test_basic.simple_table LIMIT 1" \
  --out json 2>&1
```

## Step 6: Validate the fix

After a fix, run parity on the affected table before running the full gate:

```bash
# Quick single-table check
cargo test --package cqlite-integration-tests \
  --test fixture_specific_integration_tests -- simple_table --nocapture

# Full gate (required before PR)
scripts/agent-gate.sh
```

## VInt encoding quick reference

Cassandra VInt (from Appendix B):

```
Value fits in 1 byte  (0..=127):      [0xxxxxxx]
Value fits in 2 bytes (128..=16383):  [10xxxxxx] [xxxxxxxx]
...
n leading 1-bits in first byte → (n+1) total bytes, value in remaining bits
```

Zig-zag encoded for signed values: `(n << 1) ^ (n >> 63)`.

```bash
# Decode a VInt at offset 42 using Python (cross-platform)
python3 -c "
with open('nb-1-big-Data.db', 'rb') as f:
    f.seek(42)
    b = f.read(9)
    # First byte determines length
    n_leading = bin(b[0]).lstrip('0b').count('1') if b[0] >= 0x80 else 0
    print(f'first byte: {b[0]:#04x}, extra bytes: {n_leading}')
    print(f'raw bytes: {b[:n_leading+1].hex()}')
"
```

## Compression block debugging

If the parser fails inside a compressed chunk:

```bash
# Check CompressionInfo.db for chunk offsets
hexdump -C <file>-CompressionInfo.db | head -20
```

Compression block layout (Ch.9):
- Header: magic, algorithm, chunk length, data length, number of chunks
- Body: array of chunk offsets (uncompressed offsets, 8 bytes each)
- Compressed data: each chunk independently compressed

The parser decompresses each chunk to a flat buffer before parsing rows. If rows
span chunk boundaries, the merge is done in `v5_compressed_legacy.rs` before the
row parser is called.
