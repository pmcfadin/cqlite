# SSTable Fixture Version-Coverage Matrix (S5, Issue #627)

**Generated**: 2026-06-10  
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

The main corpus uses **`nb` version, `big` format, sequential integer IDs** for the 33 primary tables.
The `oa` and `da` fixture sets were added by Issue #654 (VG2) and are now permanently part of
the corpus (shipped in release `datasets-v3`).

| Version | Format | ID form | Tables | Keyspaces |
|---|---|---|---|---|
| `nb` | `big` | sequential int (`1`, `2`, `45`, `46`, `47`, `53`, `54`, `55`) | 33 primary + system tables | `test_basic`, `test_collections`, `test_timeseries`, `test_wide_rows`, `system`, `system_schema`, `system_auth` |
| `oa` | `big` | sequential int (`2`) | **6 tables** | `test_oa` |
| `da` | `bti` | sequential int (`2`) | **3 tables** | `test_da` |

#### OA table inventory (`test_oa/`, 6 tables, Issue #654)

| Table | Description | sstabledump JSONL |
|---|---|---|
| `simple_table` | Simple primitive types | Yes |
| `collection_table` | Set, List, Map collections | Yes |
| `udt_table` | UDT with ≥128-byte `large_field` | Yes |
| `ttl_table` | Rows with TTL (86400s default) | Yes |
| `static_table` | Static column across clustering rows | Yes |
| `tombstone_table` | Range, row, and cell tombstones | Yes |

#### DA table inventory (`test_da/`, 3 tables, Issue #654)

| Table | Description | sstabledump JSONL |
|---|---|---|
| `simple_table` | Simple primitive types (BTI index) | Yes |
| `collection_table` | Collections (BTI index) | Yes |
| `ttl_table` | TTL rows (BTI index) | Yes |

**Note on sstabledump and BTI**: `sstabledump` from Cassandra 5.0.2 supports da/BTI format
(it reads the Data.db directly, not the trie indexes). JSONL goldens were generated
successfully for all 3 da tables.

**Note on CI enforcement (Issue #656 VG4)**: oa tables are now **enforced** in CI as of
VG4.  `smoke-test-all-tables.sh` includes `test_oa` in `KEYSPACES` (nb=33 + oa=6 = 39
enforced tables).  da/BTI tables remain SKIP-PENDING — see BTI read epic #660.

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

### 2b. Docker-generated fixtures (Cassandra 5.0.2, Issue #654 permanent corpus)

These fixtures were generated with Cassandra 5.0.2 using the procedure below and are now
permanently shipped as part of `datasets-v3` release. Schemas are in
`test-data/schemas/oa-test.cql` and `test-data/schemas/da-test.cql`.

#### Generation procedure

```bash
# Start Cassandra 5.0.2 container
docker run -d --name cqlite-oa-gen \
  -e MAX_HEAP_SIZE=1G -e HEAP_NEWSIZE=256m \
  -e CASSANDRA_CLUSTER_NAME=cqlite-oa-test \
  cassandra:5.0.2

# Wait for readiness (~60s), then enable oa mode
docker exec cqlite-oa-gen sed -i \
  's/storage_compatibility_mode: CASSANDRA_4/storage_compatibility_mode: NONE/g' \
  /etc/cassandra/cassandra.yaml
docker restart cqlite-oa-gen

# Create test_oa schema (test-data/schemas/oa-test.cql), insert data, flush+compact
docker cp test-data/schemas/oa-test.cql cqlite-oa-gen:/tmp/oa-schema.cql
docker exec cqlite-oa-gen cqlsh -f /tmp/oa-schema.cql
# ... insert data ...
docker exec cqlite-oa-gen nodetool flush test_oa
docker exec cqlite-oa-gen nodetool compact test_oa
# => generates oa-2-big-Data.db for each table

# Generate sstabledump JSONL golden files
docker exec cqlite-oa-gen bash -c '
for f in /var/lib/cassandra/data/test_oa/*/oa-*-big-Data.db; do
  /opt/cassandra/tools/bin/sstabledump "$f" | python3 -c "
import json, sys
for item in json.loads(sys.stdin.read()): print(json.dumps(item, separators=(\",\",\": \")))
" > "${f}.jsonl"
done'

# Enable BTI format for da
docker exec cqlite-oa-gen sed -i \
  's|#sstable:|sstable:|; s|#  selected_format: big|  selected_format: bti|' \
  /etc/cassandra/cassandra.yaml
docker restart cqlite-oa-gen

# Create test_da schema (test-data/schemas/da-test.cql), insert data, flush+compact
# ... same pattern as oa ...
# => generates da-2-bti-Data.db for each table

# Copy out and package
docker cp cqlite-oa-gen:/var/lib/cassandra/data/test_oa test-data/datasets/sstables/
docker cp cqlite-oa-gen:/var/lib/cassandra/data/test_da test-data/datasets/sstables/
tar -czf cassandra5-small-full-v3.tar.gz test-data/datasets/
gh release create datasets-v3 cassandra5-small-full-v3.tar.gz --title "Test datasets v3 (nb + oa + da)"
```

#### Permanent fixture inventory (Issue #654)

| Path | Version | Format | ID | Component files |
|---|---|---|---|---|
| `test_oa/simple_table-4b7cd050.../oa-2-big-Data.db` | `oa` | `big` | 2 | Data.db, Statistics.db, Index.db, Summary.db, Filter.db, CompressionInfo.db, Digest.crc32, TOC.txt |
| `test_oa/collection_table-4b892c60.../oa-2-big-Data.db` | `oa` | `big` | 2 | same |
| `test_oa/udt_table-4b9f7380.../oa-2-big-Data.db` | `oa` | `big` | 2 | same |
| `test_oa/ttl_table-4badf270.../oa-2-big-Data.db` | `oa` | `big` | 2 | same |
| `test_oa/static_table-4bba0060.../oa-2-big-Data.db` | `oa` | `big` | 2 | same |
| `test_oa/tombstone_table-4bc746d0.../oa-2-big-Data.db` | `oa` | `big` | 2 | same |
| `test_da/simple_table-de1be8b0.../da-2-bti-Data.db` | `da` | `bti` | 2 | Data.db, Statistics.db, **Partitions.db**, **Rows.db**, Filter.db, CompressionInfo.db, Digest.crc32, TOC.txt |
| `test_da/collection_table-de2c1550.../da-2-bti-Data.db` | `da` | `bti` | 2 | same |
| `test_da/ttl_table-de3b5790.../da-2-bti-Data.db` | `da` | `bti` | 2 | same |

**BTI index files**: da format uses `Partitions.db` and `Rows.db` (trie-based) instead of
`Index.db` and `Summary.db` (BIG format). The Data.db payload format is the same.

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

All 5 oa-only gates verified TRUE via unit tests in `version_gate.rs` AND via
fixture-level cargo parity tests in `cqlite-core/tests/issue_655_oa_read_gates.rs`:
- `hasImprovedMinMax` — TRUE
- `hasPartitionLevelDeletionPresenceMarker` — TRUE
- `hasKeyRange` — TRUE
- `hasUIntDeletionTime` — TRUE
- `hasTokenSpaceCoverage` — TRUE

Also verified: `hasAccurateMinMax` and `hasLegacyMinMax` are FALSE for oa (deprecated).

**VG4 (Issue #656)**: oa fixture reading is now **fixture-enforced** in CI:
- Cargo: `issue_655_oa_read_gates.rs` parity tests run against all 6 oa tables
- Smoke: `smoke-test-all-tables.sh` enforces `test_oa` (39 total tables)
- Python: `test_parity.py` `TestOaRowCountParity` + `TestOaValueParity` classes
- Node: `parity.test.js` VG4 oa row-count + value spot-check suites

### Gates exercised by Docker `da` fixture

All BTI gates verified TRUE, `hasOldBfFormat` verified FALSE, via unit tests.

---

## 4. What Remains Untested / Future Work

| Gap | Reason | Tracking |
|---|---|---|
| ~~**`oa` format reading** (Data.db decode)~~ | **RESOLVED in VG3 (#655)**. oa format is now read and enforced in CI (VG4 #656). | Closed |
| **`da` (BTI) format reading** | BTI uses trie-based `Partitions.db`/`Rows.db` index; current reader uses BIG index (`Index.db`/`Summary.db`). da tables remain SKIP-PENDING. | BTI read epic #660 |
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
| `nb` | `big` | 59 | Main corpus (Cassandra 5.0.2, compat mode) | All 33 primary tables; CI-enforced |
| `oa` | `big` | 6 | Issue #654 permanent corpus (5.0.2, `NONE` mode) | 6 tables; **CI-enforced (VG4 #656)** |
| `da` | `bti` | 3 | Issue #654 permanent corpus (5.0.2, BTI format) | 3 tables; SKIP-PENDING (BTI read epic #660) |
| `ma`–`me`, `na` | `big` | 0 | Not in corpus | Verified via synthetic unit tests only |

All fixtures packaged as `cassandra5-small-full-v3.tar.gz` in GitHub release `datasets-v3`
(SHA256: `69950feaaf45854e38c467087911d2d9772eeb459b6131adb676a342d7dfa983`).
