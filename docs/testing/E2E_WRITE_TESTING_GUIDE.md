# E2E Write Path Testing Guide

How to verify that CQLite-written SSTables are compatible with Apache Cassandra 5.0.

## Overview

The write-path E2E test validates the full pipeline:

```
Mutations (JSONL) → CQLite CLI → Memtable → Flush → SSTable on disk
    → Export (Cassandra naming) → nodetool import → Cassandra 5 → cqlsh queries
```

This is the ultimate compatibility check. If Cassandra accepts our files and returns correct data, the SSTable format is correct.

## Prerequisites

- Rust toolchain (stable)
- Docker (for Cassandra 5 container)
- Python 3.9+ (for mutation generator scripts)
- ~2GB disk for Cassandra image

## Quick Start

### 1. Build CQLite with write support

```bash
cargo build --package cqlite-cli --features write-support --release
```

### 2. Generate mutations

Create a JSONL file with one mutation per line. Each mutation targets a specific table and writes column values:

```bash
python3 scripts/generate_e2e_mutations.py
```

This produces `mutations.jsonl`. See [Mutation Format](#mutation-format) below for the schema.

### 3. Write, flush, and export

```bash
WRITE_DIR=/tmp/cqlite-e2e
EXPORT_DIR=/tmp/cqlite-export
SCHEMA=test-data/schemas/basic-types.cql

# Write mutations to memtable
./target/release/cqlite \
  --writable \
  --write-dir $WRITE_DIR \
  --schema $SCHEMA \
  --mutations-file mutations.jsonl

# Flush memtable to SSTable
./target/release/cqlite \
  --writable \
  --write-dir $WRITE_DIR \
  --schema $SCHEMA \
  --flush

# Export with Cassandra-compatible filenames
./target/release/cqlite \
  --writable \
  --write-dir $WRITE_DIR \
  --schema $SCHEMA \
  export-sstable $EXPORT_DIR \
  --keyspace test_basic \
  --table simple_table \
  --skip-compact
```

The `--skip-compact` flag is required (compaction is not yet implemented).

### 4. Verify with sstabledump (optional but recommended)

Before loading into Cassandra, validate the SSTable format:

```bash
docker run --rm \
  -v $EXPORT_DIR:/data \
  cassandra:5.0 \
  /opt/cassandra/tools/bin/sstabledump /data/test_basic-simple_table-nb-1-big-Data.db
```

This should produce valid JSON output with all partitions and rows. If sstabledump fails, fix the format issue before proceeding to import.

### 5. Start Cassandra and create schema

```bash
# Start Cassandra 5
docker run -d --name cassandra-e2e -p 9042:9042 cassandra:5.0

# Wait for readiness
until docker exec cassandra-e2e cqlsh -e "SELECT now() FROM system.local" 2>/dev/null; do
  sleep 5
  echo "Waiting for Cassandra..."
done
echo "Cassandra is ready"

# Create keyspace and table
docker exec cassandra-e2e cqlsh -e "CREATE KEYSPACE IF NOT EXISTS test_basic WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"

docker exec cassandra-e2e cqlsh -e "CREATE TABLE IF NOT EXISTS test_basic.simple_table (
  id UUID PRIMARY KEY,
  name TEXT,
  age INT,
  salary BIGINT,
  height FLOAT,
  weight DOUBLE,
  active BOOLEAN,
  created TIMESTAMP,
  birth_date DATE,
  work_time TIME,
  description BLOB,
  account_balance DECIMAL,
  session_id TIMEUUID,
  ip_address INET,
  small_number TINYINT,
  medium_number SMALLINT,
  duration_val DURATION,
  varchar_field VARCHAR,
  ascii_field ASCII
) WITH compression = {'class': 'SnappyCompressor'};"
```

### 6. Import SSTables

Use `docker cp` to copy files into the container, then `nodetool import`:

```bash
# Find the table's data directory UUID
TABLE_DIR=$(docker exec cassandra-e2e ls /var/lib/cassandra/data/test_basic/ | grep simple_table)

# Copy SSTable files into Cassandra's data directory
docker cp $EXPORT_DIR/. cassandra-e2e:/var/lib/cassandra/data/test_basic/$TABLE_DIR/

# Import (the -t flag skips token verification — see Known Limitations)
docker exec cassandra-e2e nodetool import -t test_basic simple_table \
  /var/lib/cassandra/data/test_basic/$TABLE_DIR/
```

### 7. Verify data

```bash
# Row count
docker exec cassandra-e2e cqlsh -e "SELECT COUNT(*) FROM test_basic.simple_table;"

# Sample rows
docker exec cassandra-e2e cqlsh -e "SELECT id, name, age FROM test_basic.simple_table LIMIT 10;"

# All columns for a spot check
docker exec cassandra-e2e cqlsh -e "SELECT * FROM test_basic.simple_table LIMIT 5;"

# Aggregates (verify numeric types)
docker exec cassandra-e2e cqlsh -e "SELECT min(age), max(age), avg(age) FROM test_basic.simple_table;"

# Token range query
docker exec cassandra-e2e cqlsh -e "SELECT * FROM test_basic.simple_table WHERE token(id) > 0 LIMIT 5;"
```

### 8. Cleanup

```bash
docker stop cassandra-e2e && docker rm cassandra-e2e
rm -rf /tmp/cqlite-e2e /tmp/cqlite-export mutations.jsonl
```

## Mutation Format

Each line in the JSONL file is a JSON object with this structure:

```json
{
  "table": {
    "keyspace": "test_basic",
    "table": "simple_table"
  },
  "partition_key": [
    {"Uuid": [0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1]}
  ],
  "clustering_key": [],
  "operations": [
    {"Write": {"column": "name", "value": {"Text": "Alice"}}},
    {"Write": {"column": "age", "value": {"Integer": 30}}}
  ],
  "timestamp_micros": 1704067200000000
}
```

### Partition key types

```json
{"Uuid": [16 bytes]}
{"Text": "string"}
{"Integer": 42}
{"BigInt": 1234567890}
```

### Clustering key types

Same format as partition keys. Order must match the table's `PRIMARY KEY` definition.

### Value types

| CQL Type | JSON Format | Example |
|----------|-------------|---------|
| TEXT / VARCHAR / ASCII | `{"Text": "..."}` | `{"Text": "hello"}` |
| INT | `{"Integer": N}` | `{"Integer": 42}` |
| BIGINT | `{"BigInt": N}` | `{"BigInt": 1234567890}` |
| FLOAT | `{"Float32": N}` | `{"Float32": 3.14}` |
| DOUBLE | `{"Float": N}` | `{"Float": 3.14159}` |
| BOOLEAN | `{"Boolean": B}` | `{"Boolean": true}` |
| TIMESTAMP | `{"Timestamp": millis}` | `{"Timestamp": 1704067200000}` |
| DATE | `{"Date": days}` | `{"Date": 19723}` |
| TIME | `{"Time": nanos}` | `{"Time": 43200000000000}` |
| UUID | `{"Uuid": [16 bytes]}` | `{"Uuid": [0,0,...,0,1]}` |
| BLOB | `{"Blob": [bytes]}` | `{"Blob": [0,1,255]}` |
| TINYINT | `{"TinyInt": N}` | `{"TinyInt": 7}` |
| SMALLINT | `{"SmallInt": N}` | `{"SmallInt": 256}` |
| INET | `{"Inet": [bytes]}` | `{"Inet": [192,168,1,1]}` (IPv4) |
| DECIMAL | `{"Decimal": {"scale": N, "unscaled": [bytes]}}` | `{"Decimal": {"scale": 2, "unscaled": [39,16]}}` |
| DURATION | `{"Duration": {"months": M, "days": D, "nanos": N}}` | `{"Duration": {"months": 1, "days": 15, "nanos": 0}}` |

## Exported SSTable Components

A successful export produces these files:

| File | Purpose |
|------|---------|
| `{ks}-{table}-nb-{gen}-big-Data.db` | Row data (partitions, rows, cells) |
| `{ks}-{table}-nb-{gen}-big-Index.db` | Partition key → Data.db offset index |
| `{ks}-{table}-nb-{gen}-big-Filter.db` | Bloom filter for partition existence checks |
| `{ks}-{table}-nb-{gen}-big-Summary.db` | Sampled index for Index.db |
| `{ks}-{table}-nb-{gen}-big-Statistics.db` | SSTable metadata (min/max timestamps, etc.) |
| `{ks}-{table}-nb-{gen}-big-CompressionInfo.db` | Compression metadata |
| `{ks}-{table}-nb-{gen}-big-Digest.crc32` | CRC32 digest of Data.db |
| `{ks}-{table}-nb-{gen}-big-TOC.txt` | Table of contents listing all components |

All components must be present for Cassandra to accept the SSTable.

## Validation Tiers

### Tier 1: sstabledump format check

Validates binary format correctness without a running Cassandra cluster:

```bash
sstabledump /path/to/nb-1-big-Data.db
```

- **Pass**: Produces valid JSON with correct partition keys, column names, values
- **Fail**: Exception traces (CorruptSSTableException, ArrayIndexOutOfBoundsException, etc.)

### Tier 2: nodetool import

Validates all SSTable components work together:

```bash
nodetool import -t keyspace table /path/to/sstables/
```

- **Pass**: "Import completed successfully" (or silent success)
- **Fail**: CorruptSSTableException, checksum errors, format version errors

The `-t` flag skips token range verification (required due to Murmur3 hash mismatch — see Known Limitations).

### Tier 3: cqlsh query verification

Validates data integrity after import:

```sql
SELECT COUNT(*) FROM keyspace.table;           -- Row count
SELECT * FROM keyspace.table LIMIT 10;         -- Sample data
SELECT min(col), max(col) FROM keyspace.table;  -- Aggregates
```

## Troubleshooting

### sstabledump: "CorruptSSTableException: Corrupted Statistics.db"

Statistics.db checksums don't match. Each metadata component needs a CRC32 checksum. See issue #425.

### sstabledump: "version unsupported"

Missing CompressionInfo.db. Even uncompressed SSTables need this file. See issue #426.

### nodetool import: "Memory.allocate(0)" / AssertionError

Filter.db format wrong. Must be `[hashCount:i32][numLongs:i32][longs...]` (Cassandra OffHeapBitSet format), NOT `[hashCount:u32][bitCount:u64][words...]`. See issue #434.

### nodetool import: succeeds but wrong row count (e.g., 1 instead of 100)

Index.db format wrong. Must store raw partition key bytes with `[key_len:u16 BE][key_bytes]`, NOT MD5 digests. See issue #435.

### nodetool import: "RangeOwnHelper" token verification failure

Use the `-t` flag: `nodetool import -t keyspace table /path/`. This skips token range ownership verification. See Known Limitations.

### cqlsh: point lookup returns 0 rows but full scan works

Murmur3 token mismatch. CQLite's Rust Murmur3 crate produces different tokens than Cassandra's Java implementation. Full scans, aggregates, and token range queries all work. See Known Limitations.

### docker cp fails / volume mount issues

On macOS with Podman, volume mounts may fail silently. Use `docker cp` instead:

```bash
docker cp /local/path/. container_name:/container/path/
```

## Known Limitations

### Murmur3 token mismatch

CQLite uses a Rust `murmur3` crate that produces different hash tokens than Cassandra's Java `Murmur3Partitioner`. This means:

- **Point lookups** (`WHERE id = X`) return 0 rows because Cassandra routes to the wrong token range
- **Full table scans** (`SELECT * FROM table`) work correctly
- **Token range queries** (`WHERE token(id) > N`) work correctly
- **Aggregates** (`SELECT count(*), min(col)`) work correctly

**Workaround**: Use `nodetool import -t` (skip token verification) and verify data with full scans.

### No compaction

The `--skip-compact` flag is required for export. Compaction (merging multiple SSTables) is not yet implemented (M5.3 scope). Each flush produces a separate SSTable.

### Collection types (Phase 2)

`FROZEN` collections are fully supported as single-cell values (#433). Non-frozen collections (`LIST`, `SET`, `MAP`) require complex column (multi-cell) format which is not yet implemented (#435). Mutations with non-frozen collection values will write successfully but produce SSTables that Cassandra cannot read. See #436 for E2E validation tracking.

## CI Integration

The `cassandra-validation.yml` workflow automates this pipeline on every PR that touches write-path code:

```
.github/workflows/cassandra-validation.yml
```

It runs three tiers:
1. **Tier 1**: sstableloader acceptance tests (single partition, multiple partitions, wide partition, all types)
2. **Tier 2**: CQL query verification (SELECT *, WHERE, row count)
3. **Tier 3**: Stress tests (10K partitions, 1000-row wide partitions) — on push to main only

Triggered by changes to:
- `cqlite-core/src/storage/write_engine/**`
- `cqlite-core/src/storage/sstable/writer/**`
- `cqlite-core/tests/sstableloader_integration.rs`

## Historical Issues

Bugs discovered during E2E verification, for reference when debugging future issues:

| Issue | Component | Root Cause |
|-------|-----------|------------|
| #425 | Statistics.db | Missing CRC32 checksums per component |
| #426 | CompressionInfo.db | File not generated at all |
| #427 | Filenames | Wrong naming convention for Cassandra NB format |
| #428 | Data.db | Column bitmap used wrong format (PRESENT vs MISSING bitmask) |
| #429 | CompressionInfo.db | Omitted for uncompressed data (still required) |
| #430 | CLI | Schema selection picked wrong table (first alphabetically) |
| #431 | Data.db | Partition header: wrong key length encoding, wrong LIVE sentinel |
| #432 | Data.db | Cell value length: signed zigzag instead of unsigned VInt |
| #433 | Data.db | Columns not sorted alphabetically |
| #434 | Filter.db | Wrong binary format (8-byte bitCount instead of 4-byte numLongs) |
| #435 | Index.db | Stored MD5 digests instead of raw partition key bytes |
| #436 | Filter.db | Bloom filter bit_count not aligned to 64-bit word boundaries |
| #437 | Filter.db | expected_keys hardcoded to 1 instead of actual partition count |
