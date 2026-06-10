# SSTable Fixture Version-Coverage Matrix (S5, Issue #627)

**Generated**: 2026-06-09  
**Authority**: Cassandra 5.0.8 source + audit report B10 Part 2 (Ch.22)  
**Branch**: `verify/s5-version-gates-627`

---

## 1. Version-Letter Gate Definitions

Per `BigFormat.java:395-410` and `BtiFormat.java:321-418` (Cassandra 5.0.8).

### BIG format per-letter gates

| Gate (Java name) | ma | mb | mc | md | me | na | nb | oa |
|---|---|---|---|---|---|---|---|---|
| `hasCommitLogLowerBound` | N | Y | Y | Y | Y | Y | Y | Y |
| `hasCommitLogIntervals` | N | N | Y | Y | Y | Y | Y | Y |
| `hasAccurateMinMax` | N | N | N | Y | Y | Y | Y | **N** |
| `hasLegacyMinMax` | Y | Y | Y | Y | Y | Y | Y | **N** |
| `hasOriginatingHostId` | N | N | N | N | **Y** | N | Y | Y |
| `hasMaxCompressedLength` | N | N | N | N | N | Y | Y | Y |
| `hasPendingRepair` | N | N | N | N | N | Y | Y | Y |
| `hasIsTransient` | N | N | N | N | N | Y | Y | Y |
| `hasMetadataChecksum` | N | N | N | N | N | Y | Y | Y |
| `hasOldBfFormat` | Y | Y | Y | Y | Y | N | N | N |
| `hasImprovedMinMax` (**oa-only**) | N | N | N | N | N | N | **N** | **Y** |
| `hasPartitionLevelDeletionPresenceMarker` (**oa-only**) | N | N | N | N | N | N | **N** | **Y** |
| `hasKeyRange` (**oa-only**) | N | N | N | N | N | N | **N** | **Y** |
| `hasUIntDeletionTime` (**oa-only**) | N | N | N | N | N | N | **N** | **Y** |
| `hasTokenSpaceCoverage` (**oa-only**) | N | N | N | N | N | N | **N** | **Y** |

**Notes**:
- `hasOriginatingHostId` straddles letter boundaries: TRUE for `m[e-z]` OR `>= nb`
- `hasAccurateMinMax` and `hasLegacyMinMax` are deprecated in `oa` (both FALSE)
- `na < nb` in lexicographic order, so `na` does NOT satisfy `>= nb`

### BTI format gates (version `da` only)

| Gate | da |
|---|---|
| All BIG gates (except `hasOldBfFormat`) | Y |
| `hasOldBfFormat` | **N** |
| `hasAccurateMinMax` / `hasLegacyMinMax` | (not applicable — BTI has separate min/max) |

---

## 2. Fixture-Coverage Matrix

### 2a. Main test corpus (`test-data/datasets/sstables/`)

All 59 Data.db files in the main corpus use **`nb` version, `big` format, sequential integer IDs**.

| Version | Format | ID form | Tables | Keyspaces |
|---|---|---|---|---|
| `nb` | `big` | sequential int (`1`, `2`, `45`, `46`, `47`, `53`, `54`, `55`) | 33 primary + system tables | `test_basic`, `test_collections`, `test_timeseries`, `test_wide_rows`, `system`, `system_schema`, `system_auth` |
| `oa` | — | — | **NONE** | — |
| `da` | `bti` | — | **NONE** | — |

#### Complete table listing (primary test corpus, 33 tables)

| Keyspace | Table | Version | Format | SSTable ID |
|---|---|---|---|---|
| test_basic | composite_key_table | nb | big | 1 |
| test_basic | compression_test_table | nb | big | 1 |
| test_basic | counters | nb | big | 1 |
| test_basic | multi_partition_table | nb | big | 1 |
| test_basic | simple_table | nb | big | 1 |
| test_basic | static_columns_table | nb | big | 1 |
| test_basic | ttl_test_table | nb | big | 1 |
| test_basic | uncompressed_table | nb | big | 1 |
| test_collections | collection_clustering_table | nb | big | 1 |
| test_collections | collection_table | nb | big | 1 |
| test_collections | collections_with_udts | nb | big | 1 |
| test_collections | empty_collections_table | nb | big | 1 |
| test_collections | frozen_collections_table | nb | big | 1 |
| test_collections | large_collections_table | nb | big | 1 |
| test_collections | nested_collections_table | nb | big | 1 |
| test_collections | typed_collections_table | nb | big | 1 |
| test_timeseries | app_metrics | nb | big | 1 |
| test_timeseries | event_store | nb | big | 1 |
| test_timeseries | log_entries | nb | big | 1 |
| test_timeseries | sensor_data | nb | big | 1 |
| test_timeseries | stock_prices | nb | big | 1 |
| test_timeseries | tick_data | nb | big | 1 |
| test_timeseries | time_bucketed_counters | nb | big | 1 |
| test_timeseries | user_activity | nb | big | 1 |
| test_timeseries | user_sessions | nb | big | 1 |
| test_wide_rows | chat_messages | nb | big | 1 |
| test_wide_rows | document_versions | nb | big | 1 |
| test_wide_rows | large_blob_table | nb | big | 1 |
| test_wide_rows | many_columns_table | nb | big | 1 |
| test_wide_rows | multi_metric_timeseries | nb | big | 1 |
| test_wide_rows | product_catalog | nb | big | 1 |
| test_wide_rows | sparse_data_table | nb | big | 1 |
| test_wide_rows | wide_partition_table | nb | big | 1 |

**Observation**: The main corpus uses `nb` because Cassandra 5.0 writes `nb` by default
(`storage_compatibility_mode: CASSANDRA_4`). All fixture directory names also use
UUID-based SSTable table IDs (e.g., `simple_table-6aa08200a25111f0a3fef1a551383fb9`)
but the Data.db files inside use sequential integer IDs.

### 2b. Docker-generated fixtures (Cassandra 5.0.8)

These fixtures were generated in this S5 investigation task and stored under
`test-data/datasets/sstables/test_oa/` and `test-data/datasets/sstables/test_da/`.

#### Generation procedure

```bash
# Start Cassandra 5.0.8 container
docker run -d --name cqlite-oa-gen -p 19042:9042 cassandra:5.0

# Enable oa mode
docker exec cqlite-oa-gen sed -i \
  's/storage_compatibility_mode: CASSANDRA_4/storage_compatibility_mode: NONE/g' \
  /etc/cassandra/cassandra.yaml
docker restart cqlite-oa-gen

# Insert data and flush
docker exec cqlite-oa-gen cqlsh -e "INSERT INTO test_oa.simple_test ..."
docker exec cqlite-oa-gen nodetool flush test_oa simple_test
# => generates oa-2-big-Data.db

# Enable BTI format
docker exec cqlite-oa-gen sed -i \
  's|^#sstable:$|sstable:|; s|^#  selected_format: big$|  selected_format: bti|' \
  /etc/cassandra/cassandra.yaml
docker restart cqlite-oa-gen
docker exec cqlite-oa-gen nodetool flush test_da trie_test
# => generates da-2-bti-Data.db
```

#### Docker-generated fixture inventory

| Path | Version | Format | ID | Component files present |
|---|---|---|---|---|
| `test-data/datasets/sstables/test_oa/simple_test-5d108ac0.../oa-2-big-Data.db` | `oa` | `big` | 2 | Data.db, Statistics.db, CompressionInfo.db |
| `test-data/datasets/sstables/test_oa/time_series-84a60e70.../oa-1-big-Data.db` | `oa` | `big` | 1 | Data.db, Statistics.db |
| `test-data/datasets/sstables/test_da/trie_test-9584e770.../da-2-bti-Data.db` | `da` | `bti` | 2 | Data.db, Statistics.db |

---

## 3. Gate Coverage by Fixture Tier

> **Note:** `version_gate.rs` is a reference module; the CQLite parsers do not yet
> consume these gates. "TESTED" below means the gate *values* are unit-tested against
> `BigFormat.java`/`BtiFormat.java` — not that any parser enforces them. Wiring the
> gates into the read path is future work (see Section 4 and S6 of epic #622).

### Gates exercised by `nb` corpus (33 tables, 59 files)

| Gate | Status |
|---|---|
| `hasCommitLogLowerBound` | TESTED (TRUE for nb) |
| `hasCommitLogIntervals` | TESTED (TRUE for nb) |
| `hasAccurateMinMax` | TESTED (TRUE for nb) |
| `hasLegacyMinMax` | TESTED (TRUE for nb) |
| `hasOriginatingHostId` | TESTED (TRUE for nb, nb >= nb) |
| `hasMaxCompressedLength` | TESTED (TRUE for nb) |
| `hasPendingRepair` | TESTED (TRUE for nb) |
| `hasIsTransient` | TESTED (TRUE for nb) |
| `hasMetadataChecksum` | TESTED (TRUE for nb) |
| `hasOldBfFormat` | TESTED (FALSE for nb) |
| `hasImprovedMinMax` | TESTED FALSE only — no oa fixture in main corpus |
| `hasPartitionLevelDeletionPresenceMarker` | TESTED FALSE only |
| `hasKeyRange` | TESTED FALSE only |
| `hasUIntDeletionTime` | TESTED FALSE only |
| `hasTokenSpaceCoverage` | TESTED FALSE only |

### Gates exercised by Docker `oa` fixtures

All 5 oa-only gates verified TRUE via unit tests in `version_gate.rs`:
- `hasImprovedMinMax` — TRUE
- `hasPartitionLevelDeletionPresenceMarker` — TRUE
- `hasKeyRange` — TRUE
- `hasUIntDeletionTime` — TRUE
- `hasTokenSpaceCoverage` — TRUE

Also verified: `hasAccurateMinMax` and `hasLegacyMinMax` are FALSE for oa (deprecated).

### Gates exercised by Docker `da` fixture

All BTI gates verified TRUE, `hasOldBfFormat` verified FALSE, via unit tests.

---

## 4. What Remains Untested

| Gap | Reason | Tracking |
|---|---|---|
| **`oa` format reading** (Data.db decode) | CQLite parser supports `nb` only; `oa` format has different Statistics.db layout and new components (`KeyRange.db`, `TokenSpaceCoverage.db` not yet parsed) | Follow-up issue needed |
| **`da` (BTI) format reading** | BTI uses trie-based `Partitions.db`/`Rows.db` index; current reader uses BIG index (`Index.db`/`Summary.db`) | Follow-up issue needed |
| `straddle gate me..mz` at runtime | No `me`-versioned SSTables in corpus; verified via synthetic unit tests only | Unit tests sufficient |
| `na` version at runtime | No `na` SSTables in corpus; verified via synthetic unit tests only | Unit tests sufficient |
| UUID-based SSTable IDs at runtime | Corpus uses sequential int IDs in filenames (even though dir names are UUID-based); fixed parsing bug (see below) | Fixed in this PR |

---

## 5. Bug Fixed in This Investigation

**Bug**: `SSTableInfo::from_path` in `format_detector.rs` called `parts[1].parse::<u64>()`
which panics/errors for UUID-based SSTable IDs (e.g., `nb-6aa08200a25111f0a3fef1a551383fb9-big-Data.db`).

**Impact**: UUID IDs are the default in real Cassandra 5.0 (`uuid_sstable_identifiers_enabled: true`).
The test corpus happens to use sequential integer IDs, masking the bug.

**Fix**:
- Changed `generation: u64` to `sstable_id: String` in `SSTableInfo`
- Added `generation_numeric() -> Option<u64>` for backward compatibility
- Used right-to-left scan to locate the `big`/`bti` format segment

See: `cqlite-core/src/storage/sstable/format_detector.rs`

---

## 6. New Module: `version_gate.rs`

**Location**: `cqlite-core/src/storage/sstable/version_gate.rs`

Provides:
- `SsTableFormat` enum (`Big`, `Bti`) with filename parsing
- `SsTableDescriptor` struct with both sequential and UUID ID support
- `BigVersionGates` struct — 15 gates matching `BigFormat.java:395-410`
- `BtiVersionGates` struct — BTI gates matching `BtiFormat.java:321-418`
- `VersionGates` enum combining both via `from_path()`
- 50+ unit tests covering all gate values, straddle logic, corpus filenames, Docker-generated filenames

---

## 7. Corpus Summary

| Version letter | Format | Files (Data.db) | Source | Notes |
|---|---|---|---|---|
| `nb` | `big` | 59 | Main corpus (Cassandra 5.0.8, compat mode) | All 33 primary tables |
| `oa` | `big` | 2 | Docker-generated (5.0.8, `NONE` mode) | Verifies oa-only gates |
| `da` | `bti` | 1 | Docker-generated (5.0.8, BTI format) | Verifies BTI gates |
| `ma`–`me`, `na` | `big` | 0 | Not in corpus | Verified via synthetic unit tests only |
