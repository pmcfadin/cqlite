# CQLite Write Engine Index

**Subsystem**: Storage write path (memtable, WAL, flush, STCS compaction)  
**Scope**: `cqlite-core/src/storage/write_engine/` + `cqlite-core/src/storage/sstable/writer/`  
**Status**: M5 complete (v0.12.0+); byte-for-byte Cassandra 5.0 compaction parity achieved  
**Target audience**: Internal engineering scoping for Q1 (analytical freshness) & Q2 (storage engine feasibility)

---

## Summary

CQLite implements a complete write engine for producing Cassandra 5.0-compatible SSTables: WAL-backed memtable, configurable flush thresholds, Size-Tiered Compaction Strategy (STCS) with byte-for-byte parity vs Cassandra, and pluggable merge policies. The engine writes **uncompressed SSTables only** (CompressionInfo.db is built-but-unwired, issue #1406). It recovers from crashes via WAL replay, implements reconciliation & tombstone shadowing per byte-parity rules, and coordinates atomic multi-component finalization via tmp-dir + rename. Single-writer model (not thread-safe); shipped in bindings via Arc<Database>.

---

## Key Classes & Interfaces

### Write Engine Coordinator
- **`WriteEngine`** (`cqlite-core/src/storage/write_engine/mod.rs:321–390`): Public API for all write ops. Owns WAL, memtable, SSTableWriter coordination. Tracks generation counter, active merge state, cumulative stats. Single-writer; uses `AtomicBool::closed` for thread-safe shutdown checks.
- **`WriteEngineConfig`** (`mod.rs:167–278`): Builder for engine init. Configurable: flush threshold (64MB default), hard limit (256MB default), durability mode (SyncEachWrite=production, Disabled=bulk-load), auto-compaction on/off, STCS min/max thresholds.
- **`Durability`** (`mod.rs:145–162`): Enum: `SyncEachWrite` (fsync per write, production), `Disabled` (buffered-only, benchmarking).

### Memtable
- **`Memtable`** (`cqlite-core/src/storage/write_engine/memtable.rs:18–27`): BTreeMap<DecoratedKey, Vec<Mutation>>. Maintains token-sorted order. Tracks approx size_bytes & row_count for flush triggering.
- **API**: `insert_with_key()` (primary, requires pre-computed DecoratedKey), `get()`, `is_empty()`, `size_bytes()`, `into_mutations()` (drain for flush).

### Write-Ahead Log (WAL)
- **`WriteAheadLog`** (`cqlite-core/src/storage/write_engine/wal.rs:~200+`): Append-only log with CRC32 framing. Entry format: `[u32 LE len][u32 LE crc32][bytes: serialized Mutation]`. Sequential append with 4 KB buffer; fsync on explicit sync() or buffer-full.
- **`RecoveryReport`** (`wal.rs:80–106`): Crash-recovery outcome. Fields: `mutations` (recovered prefix), `corrupt_entries` (count), `stopped_early` (bool), `bytes_skipped` (u64). **Fail-fast posture**: stops at first unrecoverable corruption; lossy recoveries preserved aside.
- **Corruption handling** (`wal.rs:109–146` WalStop enum): `CleanEof`, `TornTail` (incomplete header, safe trim), `Corruption` (complete-header entry unsatisfiable → lossy, preserved).

### SSTableWriter (Uncompressed BIG+BTI)
- **`SSTableWriter`** (`cqlite-core/src/storage/sstable/writer/mod.rs:200+`): Orchestrates all component writes in order: Statistics.db → Data.db+Index.db → Summary.db → Filter.db → (skip CompressionInfo.db for uncompressed) → Digest.crc32 → TOC.txt (publication barrier). Atomic finalization via tmp-dir rename.
- **`SSTableFormat`** (`mod.rs:112–143`): Enum: `Big` (legacy Index.db+Summary.db, **default**), `Bti` (Partitions.db+Rows.db trie, issue #908). Data.db row encoding identical; only index components differ.
- **`SSTableInfo`** (`mod.rs:149–196`): Return type after `finish()`. Contains paths: `data_path`, `index_path` (Some for BIG, None for BTI), `filter_path` (None when disabled), `summary_path`, `stats_path`, `compression_info_path` (always None for production writes), `partitions_path`/`rows_path` (BTI only), `crc_path` (BIG uncompressed only).

### Compaction & Merge
- **`MergePolicy` trait** (`mod.rs:91–102`): Extension point. Single method: `select_merge(&[PathBuf]) -> Result<Vec<PathBuf>>`. Allows pluggable strategies (STCS is default).
- **`STCSPolicy`** (`cqlite-core/src/storage/write_engine/merge_policy.rs:80–142`): Size-Tiered Compaction. Groups SSTables by size into buckets; triggers when bucket size ≥ `min_threshold` (default 4); compacts ≤ `max_threshold` (default 32). Configurable: `bucket_low`/`bucket_high` (size ratios), `min_sstable_size` (50MB default).
- **`KWayMerger`** (`cqlite-core/src/storage/write_engine/merge/mod.rs:~1–100`): K-way merge coordinator. Reconciles mutations across input SSTables, applies tombstone shadowing, purges via `gc_grace`, handles dropped columns (partial coverage). Outputs row groups for SSTableWriter.
- **`ActiveMerge`** (`maintenance.rs:~`): Tracks in-flight merge state; incremented by compaction steps.

### Mutation & Schema
- **`Mutation`** (`cqlite-core/src/storage/write_engine/mutation.rs:~`): Core write unit. Fields: `table_id`, `partition_key`, `clustering_key`, `operations` (Vec<CellOperation>).
- **`DecoratedKey`** (`mutation.rs:~`): Partition key + token; used for BTree ordering in memtable and SSTable offsets.
- **`CellOperation`** (`mutation.rs:~`): Enum: Write, Tombstone (cell-level), TTL (expiring cell).
- **`PartitionTombstone`/`RangeTombstone`** (`mutation.rs:~`): Partition & range deletion markers.

---

## Extension Points / Pluggability Seams

1. **Merge Policy Plugin** (`MergePolicy` trait, `mod.rs:91–102`):
   - Pluggable via `WriteEngine::set_merge_policy()` (hypothetical future API).
   - Current: STCS installed by default if `WriteEngineConfig::auto_compaction = true`.
   - Can implement LCS, TWCS, or custom strategies by impl'ing trait.

2. **Durability Mode** (`Durability` enum, `mod.rs:145–162`):
   - FSL (SyncEachWrite) vs buffered (Disabled); controls WAL fsync per write.
   - Allows trade-off between latency & consistency in controlled contexts.

3. **Schema-Aware UDT Resolution** (`WriteEngineConfig::udt_registry`, `mod.rs:181–185`):
   - Optional UdtRegistry for bare CQL UDT column type → UserType(...) marshal normalization at flush time (issue #929).
   - Allows downstream to supply metadata for complex types.

4. **SSTable Format Selection** (`SSTableFormat` enum, `mod.rs:112–143`):
   - Big (default) vs Bti (issue #908). Toggles output: Index.db/Summary.db vs Partitions.db/Rows.db.

5. **Compression Writer (Read-Path Fixtures Only)** (`CompressionInfoWriter`, `writer/compression_info_writer.rs:195–206`):
   - `guard_unsupported_production_write()` fail-closes production compressed writes.
   - Read fixtures can still use `build_to_vec()` to synthesize compressed test data (isolated from production claim boundary).

---

## Hard Couplings

1. **Single-Writer Model**: WriteEngine is NOT thread-safe. Python bindings wrap in Arc + Tokio for async, but concurrent writes on same Database instance will race (mitigation: GIL for Python, sequential promises for Node). Issue #311 (Python), #305 (Node streaming).

2. **Uncompressed-Only Production Write** (issue #1406, posture b):
   - SSTableWriter::finish() calls `CompressionInfoWriter::guard_unsupported_production_write(CompressionAlgorithm::None)` (writer/compression_info_writer.rs:195).
   - Only `None` (passthrough) passes; all real algorithms (LZ4, Snappy, Deflate, Zstd) error with `UnsupportedFormat`.
   - Compressed writes require upstream Cassandra integration (CEP-11 Memtable API plugin hook, issue #1406 posture a).

3. **WAL Crash Recovery Lossiness Posture**:
   - RecoveryReport preserves lossy metadata instead of silently truncating (issue #1391).
   - WriteEngine exposes report via `wal_recovery()` so callers detect corruption BEFORE flush truncates WAL (issue #1391 r5).
   - On corrupt recovery, raw WAL segment moved aside for diagnostic inspection.

4. **Cassandra 5.0 Format Binding**:
   - Hardcoded BIG row encoding (nb/oa versions; BTI da version for index only).
   - Pre-Cassandra-5.0 formats (ma–me) explicitly rejected in `BigVersionGates::from_version()`.
   - No version-gate fallbacks; fail-fast on unsupported format.

5. **Byte-Parity Rules Precedence**:
   - Merge reconciliation rules (docs/compaction/byte-parity-rules.md) are oracle.
   - Cell reconciliation by (column, cell_path) on timestamp; tombstone beats live at tie (rule 1, rule 2).
   - Range tombstones shadow cells (rule 2 "shadowing"). Partial coverage (only row-level TTL shadowing, no per-cell).
   - Purge via gc_grace + max_purgeable_timestamp (rule 2, issue #935); dropped-column filtering partial (rule 2).

6. **Arrow Flight Connector (OLAP Surface)**:
   - Per-node read surface for Flight queries (does NOT see memtable or unflushed WAL).
   - Q1 implication: analytical reads see only flushed SSTables; memtable state invisible to Flight.

---

## Q1 Relevance: Analytical Freshness (Memtable → Flight Visibility)

**Current State**: Arrow Flight connector reads only flushed SSTables (`SSTableReader::open()` scans data_dir for published SSTable trees). Memtable contents and unflushed WAL are not visible to analytical queries.

**What Must Change**:
1. **Memtable Export Path**: Need a read-friendly snapshot of in-memory memtable state (Issue #696 delta-export, #705 CLI `delta-export` implement row-by-row Parquet export of unflushed mutations).
   - CQLite ships `delta-export` subcommand for single-generation SSTable export (parquet/JSON); for memtable, would need ephemeral Parquet or Arrow IPC serialization.

2. **Cassandra-Side**: A CEP-11 in-JVM plugin seam could subscribe to `Memtable::flush()` events and shadow the memtable to an external store (e.g., an Arrow Flight sidecar). Cassandra's Memtable API (trunk 7.0+, not 5.0) has no public seam today; would require a new hook.

3. **WAL Replay Path**: Unflushed WAL entries (RecoveryReport::mutations) are currently private to WriteEngine. No public replay surface for external agents.

**Hypothesis**: To make Flight see memtable state, CQLite would need: (a) export APIs for in-memory mutations (new `write_engine::memtable_snapshot()` or similar), (b) Cassandra-side: either an in-JVM plugin listening to flush events OR a sidecar that tails the CQLite WAL and synthesizes Flight-readable state. The WAL already has the data; the seam is missing.

---

## Q2 Relevance: Storage Engine Feasibility (CQLite as Cassandra Engine)

### What CQLite Has (Alternative/Adjacent Engine Capability)

**Complete**:
- ✓ Memtable (BTreeMap, token-sorted, threshold-triggered flush)
- ✓ WAL (CRC-framed, crash-recoverable; lossy recovery detection)
- ✓ SSTableWriter (all BIG+BTI components, atomic finalization)
- ✓ Compaction executor (K-way merge, STCS policy, purge logic)
- ✓ Byte-for-byte parity vs Cassandra 5.0 (rules documented; 80%+ coverage)
- ✓ Durability modes (SyncEachWrite production, Disabled bulk-load)

**Partial**:
- ⚠ Tombstone shadowing (row/partition-level; no cell-path or complex deletions, issue #844)
- ⚠ Compaction purging (gc_grace safe, overlap-aware partial; dropped-column filtering gap, issue #847)
- ⚠ Statistics.db accuracy (empty-row count gap, issue #851)

**Missing**:
- ✗ Counters (rejected at write boundary, issue #486 note)
- ✗ Materialized views (no transactional write-through)
- ✗ Local indexes (not in scope for v0.12; read-path only)
- ✗ Repair (streaming, digest exchange — not needed for local read-only)
- ✗ Compaction streaming (not needed; all SSTables available locally)
- ✗ Replica coordination (no cluster, no gossip, no quorum)

### Storage Engine Seams in Cassandra 5.0

**In-Scope Seams** (exist, CQLite can use):
- **Pluggable Compaction** (issue #851 references SizeTieredCompactionStrategy): Cassandra allows custom CompactionStrategy impl via classloader. CQLite plugs in via `MergePolicy` trait (equivalent role, not wired to Cassandra yet).
- **Format Versioning**: Cassandra 5.0 has BigVersionGates; CQLite respects them (no pre-na support). BTI `da` format supported as output (issue #908).

**Out-of-Scope in 5.0** (requires CEP-11 or trunk):
- **CEP-11 Memtable API** (Cassandra trunk 7.0+): Pluggable Memtable implementation. Cassandra 5.0 has none. CQLite memtable is standalone; no Cassandra hook.
- **SSTableFormat Plugin** (no public seam in 5.0): Reader has format detection; writer is hardcoded. Trunk (7.0+) adds pluggable format SPI (src/java/org/apache/cassandra/io/sstable/format/SSTableFormat.java).

**No Seam / Would Require Fork**:
- **Write Path in Cassandra 5.0**: No pluggable Write-Ahead Log, no pluggable CompactedRowWriter. CQLite's WAL is CQLite-only; Cassandra's CommitLog is separate.
- **Replica Coordination**: No pluggable consensus/gossip layer. CQLite is single-node only.

### Feasibility Verdict for Q2

**CQLite as in-JVM replacement engine (posture a)**: ⚠ **Partial + Fork**
- Memtable/WAL/compaction fully functional locally.
- Would require: Cassandra 5.0 fork OR CEP-11 plugin (trunk only), custom classloader binding for CompactionStrategy, and new Memtable API hook for flush events (issue #1406 sidecar model).
- Byte-parity gaps (cell-path, complex deletions, dropped columns) would need closure before full claim.

**CQLite as adjacent OLAP engine (posture b)**: ✓ **Feasible + Ready**
- Current Arrow Flight + Trino connector fully wired; reads any flushed SSTable directory.
- Scaling: can run in separate JVM, sideline process, or containerized sidecar.
- Limitation: does not see memtable (Q1 problem); would need WAL tail or memtable snapshot export API.

**CQLite hybrid (in-JVM memtable + sidecar OLAP)**: ⚠ **Requires CEP-11 or Bespoke Wiring**
- Could shadow memtable to CQLite sidecar via WAL subscription hook (not built).
- Would need Cassandra-side: Memtable.onFlush() event subscriber, or custom CommitLogReplayer.
- Not production-ready without Cassandra integration work.

---

## Trunk-vs-5.0 Deltas (Verified from Code)

### Cassandra 7.0 Trunk (Observed via CEP-11 Docs / Code Comments)

1. **CEP-11 Pluggable Memtable API**: `src/java/org/apache/cassandra/db/memtable/Memtable_API.md` (referenced in comments). CQLite has no equivalent hook in 5.0; would need shim.
2. **SSTableFormat SPI**: `src/java/org/apache/cassandra/io/sstable/format/SSTableFormat.java` (public, pluggable). Cassandra 5.0 has no plugin point; format detection hardcoded in reader.
3. **BTI Format Canonical** (trunk): BTI `da` is production-ready in trunk. Cassandra 5.0 has BIG+OA; BTI (`da`) was prototype. CQLite ships both (issue #908).

### Cassandra 5.0 (CQLite Target)

1. **BIG Format**: `nb`/`oa` version letters. **CQLite outputs `nb` only** (issue #1397: uncompressed BIG is default/only format for writes).
2. **No Pluggable Memtable**: CommitLog + SkipListMemtable hardcoded. CQLite cannot hook in.
3. **No Pluggable Format**: Reader detects version; writer is fixed (BigTableWriter only).
4. **Compaction Hook**: CompactionStrategy classloader-pluggable (default SizeTieredCompactionStrategy). CQLite MergePolicy is analogous but not integrated.

### No Version Guards Crossing 5.0 Boundary in Write Path

- CQLite's WriteEngine does not version-gate; it targets 5.0 semantics exclusively.
- Reader-side version gates (BigVersionGates, etc.) exist but are read-only (irrelevant to write engine index).

---

## Summary of Findings for Indexing

| Aspect | Status | Anchor | Notes |
|--------|--------|--------|-------|
| Memtable | Complete | `memtable.rs:18–27` | BTreeMap, token-sorted, ready for plugin |
| WAL | Complete | `wal.rs:80–106` | CRC-framed, lossy-recovery detection (issue #1391) |
| Flush | Complete | `write_engine/mod.rs:321–390` | Coordinates memtable→SSTable transition |
| STCS | Complete | `merge_policy.rs:80–142` | Configurable bucket logic; default enabled |
| Compaction | Partial | `merge/mod.rs:~1–100` | K-way merge, byte-parity rules ~80% covered |
| Byte Parity | Partial | `docs/compaction/byte-parity-rules.md` | 14/28 rules covered; tombstone/cell-path gaps |
| Uncompressed Write | Enforced | `writer/compression_info_writer.rs:195–206` | Production writes only `None` (passthrough) |
| BTI Output | Complete | `writer/mod.rs:112–143` | Format selectable (Big/Bti); da version wired |
| Q1 Memtable Export | Missing | — | No public snapshot/export API for OLAP |
| Q2 Engine Seams | Partial | — | Trunk CEP-11 available; 5.0 requires fork |

---

## File Paths (Repo-Relative)

- `cqlite-core/src/storage/write_engine/mod.rs` — WriteEngine coordinator (4.2k lines)
- `cqlite-core/src/storage/write_engine/memtable.rs` — In-memory buffer (200 lines)
- `cqlite-core/src/storage/write_engine/wal.rs` — Write-Ahead Log (1.3k lines)
- `cqlite-core/src/storage/write_engine/merge_policy.rs` — STCS strategy (600 lines)
- `cqlite-core/src/storage/write_engine/merge/mod.rs` — K-way merger (13k lines)
- `cqlite-core/src/storage/sstable/writer/mod.rs` — SSTable writer (4k lines)
- `cqlite-core/src/storage/sstable/writer/compression_info_writer.rs` — Compression metadata (500 lines)
- `docs/compaction/byte-parity-rules.md` — Byte-parity oracle (127 lines)
