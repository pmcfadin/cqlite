# CQLite M5 Write Support: Council of Elders Recommendation

**Date**: 2026-01-27
**Status**: FINAL RECOMMENDATION
**Council Members**: SSTable Format Expert, Rust Architecture Expert, Validation Expert, Database Internals Expert, CQL Type System Expert

---

## Executive Summary

After comprehensive analysis of six proposed architectures for M5 write support, the Council unanimously recommends **Option D: External-Sort SSTable Builder** with BIG format targeting.

**Key Decision**: CQLite will implement a WAL + Memtable + K-way Merge architecture that produces valid Cassandra 5.0 SSTables directly, with explicit user-controlled maintenance APIs suitable for embedded library deployment.

---

## 1. Options Evaluated

### Original Research Proposals

| Direction | Architecture | Verdict |
|-----------|--------------|---------|
| **A** | Segmented Log + SkipList Index | ❌ Unsorted Data.db ≠ Cassandra SSTable |
| **B** | Memory-Mapped Persistent B-tree | ❌ 300-500 lines unsafe, high complexity |
| **C** | Lazy LSM with Foreground Compaction | ❌ Unpredictable latency spikes |
| **D** | External-Sort SSTable Builder | ✅ **RECOMMENDED** |
| **E** | Ingest Log + Deterministic Export | ⚠️ Viable alternative, double-write cost |
| **F** | Java Golden Writer Bridge | ⚠️ Use as validation oracle only |

### Why Option D Wins

1. **Format Compliance**: Produces byte-correct Cassandra SSTables directly
2. **Memory Bounded**: Stays under 128MB with configurable thresholds
3. **Zero Unsafe Code**: Entire implementation in safe Rust
4. **Explicit Control**: User decides when I/O happens via `flush_run()` and `maintenance_step(budget)`
5. **Cassandra Compatible**: Every intermediate L0 file is a valid, loadable SSTable

---

## 2. Architecture Specification

### 2.1 High-Level Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Write()   │───▶│     WAL     │───▶│  Memtable   │───▶│ L0 SSTable  │
│  Mutation   │    │  (fsync)    │    │ (BTreeMap)  │    │   (valid)   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                │
                                                                ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Final     │◀───│   K-way     │◀───│    Merge    │◀───│  Multiple   │
│  SSTable    │    │   Merge     │    │   Policy    │    │ L0 SSTables │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

### 2.2 Core Components

```
cqlite-core/src/storage/
├── write_engine/
│   ├── mod.rs              # WriteEngine orchestration
│   ├── wal.rs              # Write-ahead log (crash recovery)
│   ├── memtable.rs         # BTreeMap + size tracking
│   ├── mutation.rs         # Mutation, DecoratedKey types
│   └── merge.rs            # K-way merge with BinaryHeap
├── sstable/
│   └── writer/
│       ├── mod.rs          # SSTableWriter coordinator
│       ├── data_writer.rs  # Data.db component
│       ├── index_writer.rs # Index.db component
│       ├── filter_writer.rs # Filter.db (Bloom)
│       ├── stats_writer.rs # Statistics.db
│       ├── summary_writer.rs # Summary.db
│       └── toc_writer.rs   # TOC.txt + checksums
└── serialization/
    ├── types.rs            # CQL type serializers
    ├── cell.rs             # Cell-level encoding
    └── row.rs              # Row serialization
```

### 2.3 Public API

```rust
impl Database {
    /// Insert a mutation (CQL INSERT/UPDATE equivalent)
    pub async fn write(&mut self, mutation: Mutation) -> Result<()>;

    /// Batch write for better throughput
    pub async fn batch_write(&mut self, mutations: Vec<Mutation>) -> Result<WriteResult>;

    /// Explicit flush memtable to L0 SSTable
    pub async fn flush_run(&mut self) -> Result<PathBuf>;

    /// Time-bounded maintenance (merge L0 files if needed)
    pub async fn maintenance_step(&mut self, budget: Duration) -> Result<MaintenanceReport>;

    /// Export all data to canonical SSTable for Cassandra ingestion
    pub async fn export_sstable(&mut self, output_dir: &Path) -> Result<ExportReport>;

    /// Get write statistics (memtable size, L0 count, WAL size)
    pub fn write_stats(&self) -> WriteStats;
}
```

---

## 3. Critical Format Requirements

### 3.1 Non-Negotiable Invariants

| Requirement | Description | Consequence if Violated |
|-------------|-------------|------------------------|
| **Partition Ordering** | By Murmur3 token, then key bytes | SSTable rejected by Cassandra |
| **Clustering Ordering** | By clustering comparator within partition | Query results incorrect |
| **Component Set** | Data.db, Index.db, Filter.db, Statistics.db, Summary.db, TOC.txt | SSTable unloadable |
| **Statistics First** | Write Statistics.db BEFORE Data.db | Delta encoding broken |
| **Index Offsets** | Must exactly match Data.db positions | Corrupt reads |

### 3.2 Format-Specific Gotchas (CRITICAL)

1. **VInt Encoding**: Must match Cassandra's `VIntCoding.java` exactly
2. **Row Size Measurement**: Measured from AFTER the VInt length bytes
3. **Compression CRC**: Trailing (after chunk data), not leading
4. **Summary.db Offsets**: Little-endian (only LE component!)
5. **UDT Field Lengths**: 4-byte big-endian i32, NOT VInt
6. **Date Encoding**: Unsigned with offset for lexicographic ordering

### 3.3 Component Generation Order

```
1. Collect metadata during memtable sort
2. Write Statistics.db (provides min_timestamp, min_ttl for delta encoding)
3. Write Data.db + Index.db (single pass, track offsets)
4. Write Summary.db (sample Index.db entries)
5. Write Filter.db (finalize Bloom filter)
6. Write CompressionInfo.db (if compressed)
7. Write Digest.crc32
8. Write TOC.txt (publication barrier - makes SSTable visible)
```

---

## 4. Type Serialization Complexity

### 4.1 Complexity Ranking

| Tier | Types | Effort |
|------|-------|--------|
| **Trivial** | boolean, int, bigint, float, double, timestamp, uuid, timeuuid | 1-2 days |
| **Moderate** | text (UTF-8), blob, inet, date, time | 2-3 days |
| **Complex** | varint, decimal, duration, list, set, map, tuple | 4-5 days |
| **Dangerous** | UDT (schema-ordered, 4-byte prefixes) | 3-4 days |
| **Deferred** | Counter (CounterContext with shards) | M6 |

### 4.2 Critical Type Gotchas

**UDT Encoding** (Most Dangerous):
```
[i32 BE field_len][field_bytes]  ← NOT VInt!
Field order = schema definition order (not alphabetical)
NULL = -1 (0xFFFFFFFF)
```

**Decimal Encoding**:
```
[i32 BE scale][VInt unscaled_len][unscaled_bytes]
Scale can be NEGATIVE
```

**Duration Encoding**:
```
[VInt months][VInt days][VInt nanos]  ← All signed VInts
```

---

## 5. Memory & Performance Budget

### 5.1 Memory Allocation

| Component | Budget | Notes |
|-----------|--------|-------|
| Memtable | 64 MB | Configurable flush threshold |
| WAL Buffer | 4 KB | Sequential append buffer |
| K-way Merge | k × 8 KB | Peek buffers per run |
| Output Buffer | 8 KB | Sequential write buffer |
| Compression | 256 KB | LZ4 worst-case expansion |
| **Total** | **< 128 MB** | Within CQLite target |

### 5.2 Amplification Targets

| Metric | Target | Acceptable Range |
|--------|--------|------------------|
| Write Amplification | 2-3x | 2-4x |
| Read Amplification | 2-4x | 1-6x |
| Space Amplification | 1.3x | 1.1-1.5x |

### 5.3 Performance Targets

| Operation | Target | Measurement |
|-----------|--------|-------------|
| Insert throughput | ≥1000 rows/sec | Single-threaded |
| Memtable flush | <10 ms/MB | 64 MB in <640 ms |
| K-way merge | ≥200 MB/sec | 10 runs × 64 MB |

---

## 6. Validation Strategy

### 6.1 Four-Layer Validation

```
Layer 1: Unit Tests (Component Writers)
    └── Test each component in isolation
    └── 85%+ coverage for writer modules

Layer 2: Integration Tests (Full SSTable Generation)
    └── Round-trip: write → read back → compare
    └── Golden file parity (vs Java CQLSSTableWriter)

Layer 3: E2E Tests (Cassandra Ingestion)
    └── sstableloader into real Cassandra 5.0
    └── Query verification via CQL
    └── Repair produces zero differences

Layer 4: Parity Tests (Reference Comparison)
    └── Semantic equivalence with Java-generated SSTables
    └── sstabledump output comparison
```

### 6.2 CI Pipeline

```yaml
# Fast path (every PR): ~15 min
- Unit tests (5 min)
- Integration tests (10 min)

# Full path (main branch): ~45 min
- Unit tests
- Integration tests
- Cassandra container E2E tests
- Performance regression detection
```

### 6.3 Validation Gate

**THE ULTIMATE TEST**: Every milestone MUST pass:
```bash
1. Generate SSTable with CQLite
2. Load into Cassandra 5.0 via sstableloader
3. Query via CQL and verify row counts + values
4. Run nodetool repair - expect zero differences
```

---

## 7. Implementation Roadmap

### Stage 0: Minimum Viable Writer (4-6 weeks)

**Scope**:
- BIG format only (no BTI)
- Simple types only (text, int, timestamp, uuid)
- Single partition key, optional clustering
- Uncompressed only
- No static columns, UDTs, or collections

**Deliverables**:
- `Database::write(mutation)` API
- `Database::flush_run()` → L0 SSTable
- WAL for crash recovery
- 8 Tier-1 tables validated

**Exit Criteria**:
- ✅ Cassandra 5.0 successfully loads generated SSTables
- ✅ Round-trip tests pass (write → read → compare)
- ✅ Row counts and values match

### Stage 1: Compression & Collections (3-4 weeks)

**Additions**:
- LZ4, Snappy, Deflate compression
- List, Set, Map (frozen and non-frozen)
- Multiple partition keys
- Static columns

**Exit Criteria**:
- ✅ All compression algorithms work
- ✅ 16 Tier-1 + Tier-2 tables validated
- ✅ 3-node cluster replication tested

### Stage 2: Maintenance & Full Schema (3-4 weeks)

**Additions**:
- `maintenance_step(budget)` API
- Merge policy (STCS)
- UDT support
- TTL and tombstones

**Exit Criteria**:
- ✅ All 33 test tables pass round-trip
- ✅ Deterministic latency within budget
- ✅ Crash recovery tested

### Stage 3: BTI Format (4-6 weeks, optional)

**Additions**:
- BTI trie-indexed writer
- Cassandra 5.1+ compatibility

**Exit Criteria**:
- ✅ BTI SSTables load in Cassandra 5.1
- ✅ Performance parity with BIG format

### Total Timeline: 14-20 weeks to production-ready BIG writer

---

## 8. Risk Matrix

| Risk | Severity | Mitigation |
|------|----------|------------|
| VInt encoding mismatch | 🔴 CRITICAL | 100% unit test coverage vs Cassandra reference |
| Index offset calculation | 🔴 CRITICAL | Track Data.db position precisely, integration tests |
| Wrong delta encoding baseline | 🔴 CRITICAL | Write Statistics.db BEFORE Data.db |
| UDT field ordering | 🟡 HIGH | Schema-driven serialization, explicit tests |
| Counter columns | 🟡 HIGH | Defer to M6 (complex CounterContext) |
| Compression chunk boundaries | 🟠 MEDIUM | Test against Cassandra decompression |

---

## 9. CQL Write Statement Support

### 9.1 Current State

CQLite's `cql/ast.rs` already has complete AST types for write statements:
- `CqlInsert`, `CqlUpdate`, `CqlDelete`, `CqlBatch` (fully defined)
- `CqlUsing` for TIMESTAMP/TTL support
- `CqlAssignmentOperator` for collection/counter operations

**Gap**: The `NomParser` in `cql/nom_backend.rs` contains placeholder stubs that need real implementations.

### 9.2 CQL → Mutation Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  CQL String │───▶│   Parser    │───▶│  Validator  │───▶│  Mutation   │
│  (INSERT..) │    │  (nom AST)  │    │  (schema)   │    │  (struct)   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                │
                                                                ▼
                                                         ┌─────────────┐
                                                         │ WriteEngine │
                                                         └─────────────┘
```

### 9.3 M5 Stage 0 CQL Scope

**Supported:**
```sql
INSERT INTO ks.table (id, name) VALUES (1, 'Alice');
INSERT INTO ks.table (id, name) VALUES (1, 'Bob') USING TIMESTAMP 1234567890;
INSERT INTO ks.table (id, name) VALUES (1, 'Carol') USING TTL 3600;
UPDATE ks.table SET name = 'Dave' WHERE id = 1;
DELETE FROM ks.table WHERE id = 1;
```

**Deferred to M5.1+:**
- `IF NOT EXISTS` / `IF` conditions (lightweight transactions)
- Collection mutations: `SET list = list + [value]`, `DELETE map['key']`
- Counter updates: `SET counter = counter + 1`
- `BEGIN BATCH ... APPLY BATCH`
- Prepared statements with bind variables (`?` placeholders)

### 9.4 API Design

**Unified execute() (Recommended):**
```rust
impl Database {
    /// Execute any CQL statement (SELECT, INSERT, UPDATE, DELETE)
    pub async fn execute(&self, cql: &str) -> Result<QueryResult> {
        let statement = self.parser.parse(cql)?;
        match statement {
            CqlStatement::Select(s) => self.execute_select(s).await,
            CqlStatement::Insert(i) => self.execute_insert(i).await,
            CqlStatement::Update(u) => self.execute_update(u).await,
            CqlStatement::Delete(d) => self.execute_delete(d).await,
            _ => Err(Error::UnsupportedStatement),
        }
    }
}

// Usage - same API for reads and writes
let rows = db.execute("SELECT * FROM users").await?;
let result = db.execute("INSERT INTO users (id) VALUES (1)").await?;
assert_eq!(result.rows_affected, 1);
```

**Why unified**: Matches familiar SQL patterns, simplifies Python/Node.js bindings.

### 9.5 Mutation Type

```rust
/// A mutation represents a write operation (INSERT, UPDATE, DELETE)
pub struct Mutation {
    pub table: TableId,
    pub partition_key: PartitionKey,
    pub clustering_key: Option<ClusteringKey>,
    pub operations: Vec<CellOperation>,
    pub timestamp_micros: i64,
    pub ttl_seconds: Option<u32>,
}

pub enum CellOperation {
    Write { column: String, value: Value },
    Delete { column: String },
    DeleteRow,
}
```

### 9.6 Parser Implementation Work

| Component | Effort | Notes |
|-----------|--------|-------|
| INSERT parser | 2-3 days | Reuse expression parsers from SELECT |
| UPDATE parser | 2-3 days | SET clause with assignments |
| DELETE parser | 1-2 days | Simpler than INSERT/UPDATE |
| USING clause | 1 day | TIMESTAMP and TTL |
| Schema validation | 2-3 days | Type checking, column existence |
| AST → Mutation | 2-3 days | Conversion layer |
| **Total** | ~2 weeks | Fits within Stage 0 timeline |

---

## 10. Deferred Items (M6+)

1. **Counter Columns**: Complex CounterContext with shards, read-before-write semantics
2. **BTI Format**: Trie implementation adds 12-16 weeks
3. **Background Compaction**: Optional daemon mode for server-like deployments
4. **WASM Support**: Write path in browser environment
5. **Lightweight Transactions**: IF NOT EXISTS, IF conditions (requires read-before-write)
6. **Prepared Statements**: Bind variables with `?` placeholders

---

## 10. Council Vote

| Expert | Vote | Notes |
|--------|------|-------|
| SSTable Format | ✅ Option D | "Only format-compliant option without converter" |
| Rust Architecture | ✅ Option D | "Zero unsafe, integrates with existing patterns" |
| Validation | ✅ Option D | "L0-as-SSTable simplifies validation" |
| Database Internals | ✅ Option D | "Best amplification trade-offs for embedded" |
| CQL Type System | ✅ Option D | "Schema-aware serialization works cleanly" |

**UNANIMOUS RECOMMENDATION: Option D (External-Sort SSTable Builder)**

---

## Appendix A: Dissenting Views & Alternatives

### Option E as Fallback

If Option D proves too complex for Stage 0, Option E (Ingest Log + Export) provides a simpler initial path:
- Log writes immediately (fast, simple)
- Export to SSTable on-demand
- Trade-off: Double write cost, poor read performance until export

**Council Position**: Start with Option D. Fall back to Option E only if Stage 0 takes >8 weeks.

### Java Bridge (Option F) Role

Use Cassandra's CQLSSTableWriter as a **validation oracle**, not as production path:
- Generate reference SSTables for comparison
- Validate format correctness
- Not suitable for embedded deployment (JVM dependency)

---

## Appendix B: Key Documentation References

- **SSTable Format**: `docs/sstables-definitive-guide/README.md`
- **Data.db Format**: `docs/sstables-definitive-guide/chapters/05-data-db-format.md`
- **Index.db Format**: `docs/sstables-definitive-guide/chapters/06-index-and-summary.md`
- **BTI Format**: `docs/sstables-definitive-guide/chapters/17-bti-format.md`
- **Encoding Rules**: `docs/sstables-definitive-guide/appendix-b-encoding-cheat-sheet.md`

---

## Appendix C: Expert Analysis Documents

The full expert analyses are available:
- SSTable Format Expert: Detailed format invariants, component order, gotchas
- Rust Architecture Expert: Memory model, data structures, API sketches
- Validation Expert: CI pipeline, test matrix, golden file strategy
- Database Internals Expert: Amplification analysis, crash recovery, embedded patterns
- CQL Type System Expert: Serialization complexity, encoding rules, edge cases

---

**Document Approved By**: Council of Elders
**Next Step**: Begin Stage 0 implementation with `write-support` feature flag
