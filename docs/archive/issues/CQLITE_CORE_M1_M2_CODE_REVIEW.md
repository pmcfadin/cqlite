# CQLite-Core M1/M2 Scope Violation Report

**Reviewer:** Senior Rust Developer / Hardcore Cassandra Engineer  
**Date:** October 18, 2025  
**Review Target:** `cqlite-core/` against PRD M1 & M2 requirements  

---

## Executive Summary

**Bottom Line:** You have ~10,000+ lines of code that have nothing to do with M1 or M2. This is feature creep at its finest.

### What M1/M2 Actually Requires:
- **M1:** Read Cassandra 5 SSTables (format parsing, decompression, deserialization)
- **M2:** CLI tooling (not core library territory)

### What You Have Instead:
A database engine with write capabilities, compaction strategies, performance optimization frameworks, and Docker integrations. You're building M5 and M6 features before you've shipped M1.

---

## Scope Violation Breakdown (Visual)

```
┌─────────────────────────────────────────────────────────────────┐
│                    CQLITE-CORE SCOPE ANALYSIS                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ✅ M1 REQUIRED (Reading):                                       │
│     • SSTable format parsers         [~15,000 lines]             │
│     • Type system & schema           [~8,000 lines]              │
│     • Compression handlers           [~1,500 lines]              │
│     • Basic query execution          [~3,000 lines]              │
│                                      ──────────────              │
│                                      ~27,500 lines               │
│                                                                  │
│  ❌ OUT OF SCOPE (M5/M6):                                        │
│     • Write infrastructure (WAL, memtable, batch writer)         │
│     • Compaction strategies                                      │
│     • Performance optimization & benchmarks                      │
│     • Docker integration                                         │
│     • Tombstone merging (M3+)                                    │
│                                      ──────────────              │
│                                      ~10,000 lines               │
│                                                                  │
│  📊 RATIO: 73% in-scope / 27% out-of-scope                      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Milestone Mapping

| Milestone | PRD Description | Current State |
|-----------|----------------|---------------|
| **M1** | Core Reading Library | ✅ **Complete** (but polluted with M5/M6 code) |
| **M2** | CLI (REPL + one-shot) | 🔄 In Progress (separate crate, correct) |
| **M3** | Output Writers (JSON/CSV/Parquet) | ⚠️ Partially implemented (tombstones) |
| **M4** | Language Bindings | ❌ Not started |
| **M5** | **Write Support** | ⚠️ **Already implemented in core** ← PROBLEM |
| **M6** | Perf & Size Validation | ⚠️ **Already implemented** ← PROBLEM |

---

## Critical Violations (Delete or Feature-Gate These)

### 1. Write Infrastructure (M5 Territory - Not M1)

**Code Volume:** ~4,500 lines

| File | Lines | Status | Rationale |
|------|-------|--------|-----------|
| `storage/batch_writer.rs` | 543+ | ❌ **DELETE** | Batch writes are M5. You're reading, not writing. |
| `storage/wal.rs` | 377+ | ❌ **DELETE** | Write-Ahead Log is for durability of WRITES. M5. |
| `storage/memtable.rs` | 393+ | ❌ **DELETE** | In-memory write buffer. M1 reads from disk, not RAM. |
| `storage/manifest.rs` | 388+ | ❌ **DELETE** | Manifest management is for tracking SSTable lifecycle during writes/compaction. |
| `storage/sstable/writer.rs` | 959 | ❌ **DELETE** | SSTable writer is literally M5 "Write Support". |

**Impact:** These files are wired into `StorageEngine` (see lines 21-50 of `storage/mod.rs`). Every `StorageEngine` instance carries:
- A `BatchWriter`
- A `WriteAheadLog`
- A `MemTable`
- A `Manifest`
- A `CompactionManager`

**None of these are needed to read SSTables.**

---

### 2. Compaction Infrastructure (Operational, Not Reading)

**Code Volume:** 457 lines

| File | Lines | Status | Rationale |
|------|-------|--------|-----------|
| `storage/compaction.rs` | 457 | ❌ **DELETE** | Compaction is a background maintenance operation. M1 reads existing SSTables as-is. |

**Evidence:**
```rust:12:39:cqlite-core/src/storage/compaction.rs
/// Compaction strategy
#[derive(Debug, Clone)]
pub enum CompactionStrategy {
    SizeTiered { ... },
    Leveled { ... },
    TimeWindow { ... },
}
```

This is runtime database maintenance. You're not operating a Cassandra cluster, you're reading its data files.

---

### 3. Performance Optimization Infrastructure (M6 Territory)

**Code Volume:** ~3,500+ lines

| File | Lines | Status | Rationale |
|------|-------|--------|-----------|
| `benchmarks/mod.rs` + subdirs | ~2,000+ | ⚠️ **FEATURE-GATE** | Benchmarking is M6 "Perf & Size Validation". Gate behind `benchmarks` feature (already exists, but code is exposed). |
| `benchmarks/cassandra5/throughput_benchmarks.rs` | 813 | ⚠️ **FEATURE-GATE** | Same. |
| `benchmarks/cassandra5/compression_benchmarks.rs` | ? | ⚠️ **FEATURE-GATE** | Same. |
| `benchmarks/cassandra5/memory_benchmarks.rs` | ? | ⚠️ **FEATURE-GATE** | Same. |
| `benchmarks/cassandra5/zerocopy_benchmarks.rs` | ? | ⚠️ **FEATURE-GATE** | Same. |
| `performance_monitor.rs` | 546+ | ⚠️ **FEATURE-GATE** | Performance regression detection framework. M6. |
| `parser/m3_performance_benchmarks.rs` | 1,285 | ⚠️ **FEATURE-GATE** | Why is M3 benchmark code in parser? Move or delete. |
| `parser/performance_regression_framework.rs` | 822 | ⚠️ **FEATURE-GATE** | Regression framework. M6. |
| `query/optimized_executor.rs` | 1,045 | ⚠️ **FEATURE-GATE** | Query caching, parallel execution, plan optimization. M6. |
| `query/select_optimizer.rs` | 681+ | ⚠️ **FEATURE-GATE** | Cost-based optimization, index selection. M6. |

**Problem:** PRD M1/M2 says:
> "**Performance Targets**: Set after functional parity; goal: faster than native Cassandra bulk tools"

You're building the performance layer before you've established functional parity. Classic cart-before-horse.

---

### 4. Docker Integration (Test Infrastructure in Core Library)

**Code Volume:** 262 lines

| File | Lines | Status | Rationale |
|------|-------|--------|-----------|
| `docker/mod.rs` | 262 | ❌ **MOVE TO TEST UTILS** | Docker exec wrapper for cqlsh commands. This belongs in `tests/` or a separate `cqlite-testing` crate, NOT in core. |

**Evidence:**
```rust:3:18:cqlite-core/src/docker/mod.rs
/// Docker integration module for running cqlsh commands in Cassandra containers
use std::process::Command;

/// Represents a Docker container
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerContainer {
    pub id: String,
    pub name: String,
    pub image: String,
}
```

This is test infrastructure. Core library should have zero dependencies on Docker CLI.

---

### 5. Tombstone Merging (M3+ Feature)

**Code Volume:** 718 lines

| File | Lines | Status | Rationale |
|------|-------|--------|-----------|
| `storage/sstable/tombstone_merger.rs` | 718 | ⚠️ **DEFER TO M3** | Multi-generation tombstone resolution. PRD says "tombstones" is an M3+ feature (see Cargo.toml line 129). Feature-gate or remove. |

---

### 6. Questionable Query Engine Components

| File | Lines | Status | Rationale |
|------|-------|--------|-----------|
| `query/prepared.rs` | ? | ⚠️ **REVIEW** | Prepared statement cache. Is this needed for M2's "basic SELECT...WHERE"? |
| `query/planner.rs` | 900 | ⚠️ **REVIEW** | Query planner with parallelization and cost estimation. M2 needs basic execution, not advanced planning. |

---

## Public API Surface Area Issues

### Problem 1: `lib.rs` Exposes Write Capabilities

```rust:355:362:cqlite-core/src/lib.rs
/// Flush all pending writes to disk
pub async fn flush(&self) -> Result<()> {
    self.storage.flush().await
}

/// Perform manual compaction of storage files
pub async fn compact(&self) -> Result<()> {
    self.storage.compact().await
}
```

**Issue:** M1 is a reading library. Why does `Database` have `flush()` and `compact()` in its public API?

### Problem 2: `StorageEngine` Has Write Methods

```rust:233:271:cqlite-core/src/storage/mod.rs
pub async fn put(&self, table_id: &TableId, key: RowKey, value: Value) -> Result<()> {
    // ... write implementation
}

pub async fn delete(&self, table_id: &TableId, key: RowKey) -> Result<()> {
    // ... delete implementation
}
```

**Issue:** These are **public** methods on the storage engine. M1 is read-only.

### Problem 3: Query Engine Supports INSERT/UPDATE/DELETE/CREATE TABLE

Evidence from `query/mod.rs`:
```rust
pub enum QueryType {
    Select,
    Insert,
    Update,      // ← M5 territory
    Delete,      // ← M5 territory
    CreateTable, // ← M5 territory
    // ...
}
```

And from tests (`lib.rs:487-496`):
```rust
// Create table
db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    .await.unwrap();

// Insert data
db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    .await.unwrap();
```

**Issue:** Your query engine parses, plans, and executes write operations. That's M5 "Write Support", not M1 "Core Reading Library".

---

## Architectural Concern: StorageEngine Coupling

### The Core Problem

`StorageEngine` is supposed to be your SSTable reader. Instead, it's a full CRUD engine:

```rust:24:55:cqlite-core/src/storage/mod.rs
#[derive(Debug)]
pub struct StorageEngine {
    /// In-memory write buffer
    memtable: Arc<RwLock<memtable::MemTable>>,

    /// SSTable manager for persistent storage
    sstables: Arc<sstable::SSTableManager>,

    /// Write-ahead log for durability
    wal: Arc<wal::WriteAheadLog>,

    /// Compaction manager for background maintenance
    compaction: Arc<compaction::CompactionManager>,

    /// Manifest for metadata management
    manifest: Arc<manifest::Manifest>,

    /// Batch writer for efficient bulk operations
    batch_writer: Option<BatchWriter>,
    
    // ...
}
```

**What M1 needs:**
```rust
pub struct SSTableReader {
    /// SSTable files on disk
    sstables: Arc<sstable::SSTableManager>,
    
    /// Platform abstraction
    platform: Arc<Platform>,
    
    /// Optional schema for type-aware reading
    schema_registry: Option<Arc<RwLock<SchemaRegistry>>>,
}
```

**That's it.** No WAL, no memtable, no compaction, no manifest.

### Separation of Concerns Violation

Every time you instantiate a `StorageEngine` for **reading**, you pay for:
- WAL initialization and file handle
- Empty memtable allocation
- Compaction manager background thread spawn
- Manifest transaction log setup
- Batch writer buffers

This is like buying a 747 when you need a bicycle.

### Recommended Architecture

```
cqlite-core/
├── sstable_reader/     # Pure read-side (M1)
│   ├── format/
│   ├── compression/
│   └── manager.rs
│
└── (future) cqlite-writer/  # Write-side (M5)
    ├── memtable/
    ├── wal/
    ├── compaction/
    └── writer.rs
```

Keep read and write **completely separate**. You're not Cassandra itself—you don't need to replicate its monolithic architecture.

---

## Cargo.toml Feature Flags Analysis

### What You Have:
```toml:103:145:cqlite-core/Cargo.toml
[features]
# M2+ production defaults: Full query engine enabled
default = ["all-compression", "metrics", "experimental", "state_machine"]

# ... various features ...

experimental = []     # SSTable writing and other M1 functionality (enabled by default)
```

**Problems:**
1. **`experimental` enabled by default** - This gates SSTable writing (M5), yet it's in `default`. That's not experimental, that's production.
2. **`tombstones` feature exists but code isn't gated** - `tombstone_merger.rs` has no `#[cfg(feature = "tombstones")]`.
3. **`benchmarks` feature exists but modules are always compiled** - `benchmarks/mod.rs` has `#![cfg(feature = "benchmarks")]` but parent module still references it unconditionally.

---

## Quantitative Summary

### Estimated Out-of-Scope Code (Lines)

| Category | Files | Est. Lines | Disposition |
|----------|-------|------------|-------------|
| Write Infrastructure | 5 | ~2,660 | DELETE |
| Compaction | 1 | 457 | DELETE |
| Docker Integration | 1 | 262 | MOVE |
| Benchmarks | 8+ | ~3,500 | FEATURE-GATE |
| Optimization | 3 | ~2,400 | FEATURE-GATE |
| Tombstone Merging | 1 | 718 | DEFER/GATE |
| **TOTAL** | **19+** | **~10,000** | |

**That's ~10% of your codebase (100k total lines) that has nothing to do with M1/M2.**

---

## Recommendations (Priority Order)

### Phase 1: Immediate Deletions (Zero M1/M2 Value)
1. **Delete** `storage/batch_writer.rs`
2. **Delete** `storage/wal.rs`
3. **Delete** `storage/memtable.rs`
4. **Delete** `storage/manifest.rs`
5. **Delete** `storage/compaction.rs`
6. **Delete** `storage/sstable/writer.rs`
7. **Move** `docker/mod.rs` to `tests/helpers/` or new `cqlite-testing` crate

**Impact:** Will break `StorageEngine`. You'll need to refactor it to be a pure read-side component without write infrastructure.

### Phase 2: Feature-Gate Deferred Work
1. **Move benchmarks behind gate:**
   ```rust
   #[cfg(feature = "benchmarks")]
   pub mod benchmarks;
   ```
   Remove `benchmarks` from `default` features.

2. **Gate optimization components:**
   ```rust
   #[cfg(feature = "query-optimization")]
   pub mod optimized_executor;
   #[cfg(feature = "query-optimization")]  
   pub mod select_optimizer;
   ```
   Create new feature, leave it off by default.

3. **Gate tombstone merging:**
   ```rust
   #[cfg(feature = "tombstones")]
   pub mod tombstone_merger;
   ```
   Already have the feature, just enforce it.

4. **Remove `experimental` from default:**
   ```toml
   default = ["all-compression", "metrics", "state_machine"]
   ```

### Phase 3: API Cleanup
1. Remove `Database::flush()` and `Database::compact()` from public API
2. Make write-related methods in `StorageEngine` private or feature-gated
3. Document feature boundaries clearly

---

## M1 Definition of Done (What You Should Have)

Per PRD M1:
> **Core Reading Library** – Reads any Cassandra 5 SSTable; all CQL/UDT types; compression OK; 95% unit-test coverage

**Required Code:**
- ✅ SSTable format parsers (`parser/`, `storage/sstable/reader/`)
- ✅ Compression handlers (`storage/sstable/compression.rs`)
- ✅ Type system (`types.rs`, `types_enhanced.rs`, `schema/`)
- ✅ Basic query execution for M2 CLI (`query/executor.rs`, `query/select_executor.rs`)

**Not Required (But You Have):**
- ❌ Write infrastructure
- ❌ Compaction strategies
- ❌ Performance optimization frameworks
- ❌ Docker integration
- ❌ Advanced query planning with cost estimation

---

## Final Verdict

You're building a full-fledged database engine when the PRD asked for a library to read Cassandra files. This is scope creep, plain and simple.

**Options:**
1. **Nuclear:** Delete everything above. Ship M1 as a pure read library. Add writes in M5.
2. **Pragmatic:** Feature-gate aggressively. Make `default` features = M1 only. Let advanced users opt into writes/optimization.
3. **Status Quo:** Ship everything, accept that "M1" actually includes M3-M6 functionality, update PRD to match reality.

**My Recommendation:** Go pragmatic (#2). You've already written the code. Don't throw it away, but make it crystal clear what's stable (reading) vs experimental (writing/optimization).

---

## Action Items for Patrick

### Immediate (< 1 day)
- [ ] Review this report
- [ ] Decide on disposition: **Nuclear / Pragmatic / Status Quo**
- [ ] If Nuclear → Create deletion branch
- [ ] If Pragmatic → Continue to Phase 1 below

### Phase 1: Quick Wins (2-3 days)
- [ ] Move `docker/mod.rs` → `tests/helpers/docker.rs` or new crate
- [ ] Remove `benchmarks` from default features
- [ ] Remove `experimental` from default features  
- [ ] Create `query-optimization` feature for optimizer code
- [ ] Gate `tombstone_merger.rs` with `tombstones` feature
- [ ] Update CI to test `--no-default-features` builds

### Phase 2: Core Refactor (1 week)
- [ ] Extract write components to `cqlite-writer` crate:
  - `storage/batch_writer.rs`
  - `storage/wal.rs`
  - `storage/memtable.rs`
  - `storage/manifest.rs`
  - `storage/compaction.rs`
  - `storage/sstable/writer.rs`
- [ ] Refactor `StorageEngine` to be pure read-side (no WAL/memtable/manifest)
- [ ] Remove `Database::flush()` and `Database::compact()` from public API
- [ ] Make `StorageEngine::put/delete` private or move to `cqlite-writer`

### Phase 3: Documentation (2 days)
- [ ] Update `README.md` with clear feature boundaries
- [ ] Document which features enable which milestones
- [ ] Add "Roadmap" section showing M1-M6 progression
- [ ] Update PRD to reflect current implementation reality

### Phase 4: Validation (1 day)
- [ ] Run full test suite with `--no-default-features` + only M1 features
- [ ] Verify M1 scope compiles without write infrastructure
- [ ] Add CI job: "M1 Scope Validation"
- [ ] Measure crate size impact: default vs. read-only features

---

## Quick Reference: File Disposition Table

| File | Action | New Location / Feature Gate | Priority |
|------|--------|----------------------------|----------|
| `storage/batch_writer.rs` | MOVE | `cqlite-writer` crate | P1 |
| `storage/wal.rs` | MOVE | `cqlite-writer` crate | P1 |
| `storage/memtable.rs` | MOVE | `cqlite-writer` crate | P1 |
| `storage/manifest.rs` | MOVE | `cqlite-writer` crate | P1 |
| `storage/compaction.rs` | MOVE | `cqlite-writer` crate | P1 |
| `storage/sstable/writer.rs` | MOVE | `cqlite-writer` crate | P1 |
| `docker/mod.rs` | MOVE | `tests/helpers/` | P0 |
| `benchmarks/**` | GATE | `#[cfg(feature = "benchmarks")]` | P0 |
| `performance_monitor.rs` | GATE | `#[cfg(feature = "benchmarks")]` | P0 |
| `query/optimized_executor.rs` | GATE | `#[cfg(feature = "query-optimization")]` | P1 |
| `query/select_optimizer.rs` | GATE | `#[cfg(feature = "query-optimization")]` | P1 |
| `storage/sstable/tombstone_merger.rs` | GATE | `#[cfg(feature = "tombstones")]` | P2 |
| `parser/m3_performance_benchmarks.rs` | GATE | `#[cfg(feature = "benchmarks")]` | P1 |
| `parser/performance_regression_framework.rs` | GATE | `#[cfg(feature = "benchmarks")]` | P1 |

**Legend:**
- **P0** = Quick wins, minimal risk
- **P1** = High impact, moderate effort
- **P2** = Lower priority, defer if needed

---

**End of Report**

