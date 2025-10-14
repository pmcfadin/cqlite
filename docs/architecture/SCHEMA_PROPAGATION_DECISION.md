# Architecture Decision: Schema Propagation in Query Execution

**Date**: 2025-10-13
**Status**: Needs Senior Developer Review
**Context**: Issue #154 + E2E Query Testing Failures
**Priority**: BLOCKING - CI smoke tests failing

---

## Executive Summary

We've successfully fixed Issue #154 (header parsing) and removed unnecessary `legacy-heuristics` gates from V5.0 state machine creation. However, **e2e query testing has revealed a fundamental architecture gap**: schema information loaded at the Database level during ingestion is not propagated to the SSTable storage layer, causing all query executions to fail.

This document presents three architectural options to fix this gap, with detailed analysis of tradeoffs, implementation complexity, and long-term maintainability considerations.

---

## Background: What We Discovered

### The Working Parts ✅

1. **Schema Loading (Ingestion)**: Works perfectly
   - CLI `--schema` flag loads CQL schema files
   - `ingest()` function parses schema and stores in `Database`
   - Schema available to `QueryExecutor` via Database reference

2. **SSTable Storage Layer**: Works in isolation
   - Header parsing works (Issue #154 fixed)
   - Compression works
   - File I/O works
   - V5.0 state machine creates successfully

3. **Unit/Integration Tests**: All passing (750+ tests)
   - Tests work because they either:
     - Mock data (no real parsing needed), OR
     - Test storage layer in isolation (don't go through query execution)

### The Broken Part ❌

**E2E Query Execution** through the full stack fails:

```
Database (has schema ✓)
  ↓ execute("SELECT * FROM table")
QueryExecutor (has schema access ✓)
  ↓ storage.scan(table_id, None, None, None)  ← Schema NOT passed
StorageEngine::scan()
  ↓ sstables.scan(table_id, None, None, None)  ← Schema NOT passed
SSTableManager::scan()
  ↓ reader.scan(table_id, None, None, None)    ← Schema NOT passed
SSTableReader::scan()
  ↓ parse_block_entries(block_data)            ← Schema NOT available
RowCellStateMachine::parse()
  ❌ FAILS: Cannot parse partition keys without schema
```

### Error Manifestation

**Smoke Test Output**:
```
[DEBUG SSTableReader::parse_block_entries_with_state_machine] Modern V5.0 format without header schema
[DEBUG SSTableReader::parse_block_entries_with_state_machine] Using basic state machine for V5.0 format
[DEBUG SSTableReader::parse_block_entries_with_state_machine] State machine creation result: OK
❌ State machine processing error: Data corruption: Failed to parse partition key component count
🔄 Falling back to legacy parsing for remaining 16384 bytes
[DEBUG SSTableReader::parse_block_entries] State machine failed: Schema error: Non-schema key parsing requires legacy-heuristics feature for legacy compatibility.
Error: Failed to execute query: Schema error: Non-schema key parsing requires legacy-heuristics feature for legacy compatibility.
```

**What's Happening**:
1. State machine creates ✅ (we fixed this)
2. State machine tries to parse partition key from binary data
3. Partition key format is: `[component_count: vint][component1_len: vint][component1_data][component2_len...]`
4. Without schema, we don't know:
   - How many components to expect
   - What type each component is
   - How to deserialize each component's bytes
5. Parser fails with "Failed to parse partition key component count"
6. Falls back to legacy parser which also requires schema or `legacy-heuristics` feature
7. Query execution fails completely

---

## Current Architecture Details

### Layer 1: Database & Ingestion

**File**: `cqlite-core/src/database.rs`, `cqlite-core/src/ingestion/mod.rs`

**What it does**:
```rust
pub struct Database {
    storage: Arc<StorageEngine>,
    schema_manager: Arc<RwLock<SchemaManager>>,  // ← Schema stored here
    query_engine: Option<Arc<QueryEngine>>,
    // ...
}

impl Database {
    pub async fn execute(&self, query: &str) -> Result<QueryResult> {
        // Has access to schema_manager
        let engine = self.query_engine.as_ref()
            .ok_or_else(|| Error::unsupported("Query engine not available"))?;

        engine.execute(query, &self.schema_manager, &self.storage).await
    }
}
```

**Schema Source**: Loaded during ingestion from CQL files via:
```rust
pub async fn ingest(config: IngestionConfig) -> Result<IngestionResult> {
    // Parse schema from .cql files
    let schemas = parse_schemas(&config.schema_paths)?;

    // Store in SchemaManager
    schema_manager.register_schemas(schemas)?;

    // Return Database with schema_manager
    Ok(IngestionResult {
        database: Database::new(storage_engine, schema_manager, ...),
        // ...
    })
}
```

### Layer 2: Query Execution

**File**: `cqlite-core/src/query/select_executor.rs`

**What it does**:
```rust
impl SelectExecutor {
    async fn execute_sstable_scan(
        &self,
        table: &TableRef,
        predicates: &[Predicate],
        projection: &[String],
        context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        // Use StorageEngine's scan method to get all rows for the table
        let scan_results = self.storage.scan(table, None, None, None).await?;
        //                                   ^^^^^ TableId only - NO SCHEMA

        for (key, value) in scan_results {
            // key is RowKey (raw bytes or parsed components?)
            // value is Value (raw bytes)
            // Need to deserialize based on schema...
        }
    }
}
```

**Critical Observation**: `self.storage.scan()` only receives `table: &TableRef` (which contains table_id). Schema is NOT passed.

### Layer 3: Storage Engine

**File**: `cqlite-core/src/storage/mod.rs`

**Current Signature**:
```rust
impl StorageEngine {
    pub async fn scan(
        &self,
        table_id: &TableId,          // ← Only table identifier
        start_key: Option<&RowKey>,  // ← Raw key (no type info)
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        // Delegates to SSTableManager
        let sstable_results = self
            .sstables
            .scan(table_id, start_key, end_key, limit)  // ← No schema passed
            .await?;

        Ok(sstable_results)
    }
}
```

**Key Point**: This is a pure storage API - it knows nothing about schemas. It returns `(RowKey, Value)` which are essentially byte arrays.

### Layer 4: SSTable Manager

**File**: `cqlite-core/src/storage/sstable/manager.rs`

**Current Implementation**:
```rust
impl SSTableManager {
    pub async fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        let mut results = Vec::new();

        // Iterate through all SSTables
        for reader in self.readers.values() {
            let entries = reader.scan(table_id, start_key, end_key, limit).await?;
            //                         ^^^^^ No schema passed
            results.extend(entries);
        }

        Ok(results)
    }
}
```

### Layer 5: SSTable Reader (The Problem!)

**File**: `cqlite-core/src/storage/sstable/reader/data_access.rs`

**Current Signature**:
```rust
impl SSTableReader {
    pub async fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        // Sequential scan approach
        let all_entries = self.sequential_scan().await?;
        //                     ^^^^^^^^^^^^^^^^ Needs to parse binary data

        // Filter and return
        let filtered = all_entries
            .into_iter()
            .filter(|(entry_table_id, entry_key, _)| {
                *entry_table_id == *table_id
                // Can we even compare keys without schema?
            })
            .collect();

        Ok(filtered)
    }
}
```

### Layer 6: Block Entry Parsing (Where It Breaks!)

**File**: `cqlite-core/src/storage/sstable/reader/parsing/block_entries.rs`

**The Failing Code**:
```rust
impl SSTableReader {
    fn parse_block_entries_with_state_machine(
        &self,
        data: &[u8],
    ) -> Result<Vec<(TableId, RowKey, Value)>> {
        let mut offset = 0;

        while offset < data.len() {
            // Try to create state machine
            let has_schema = self.get_table_schema().is_some();
            //                    ^^^^^^^^^^^^^^^^^^
            // Returns None because header has no schema for NB format

            let state_machine = if has_schema {
                // With schema: can parse structured keys ✅
                RowCellStateMachine::new_with_schema(schema)
            } else {
                // Without schema: creates basic state machine ✅ (we fixed this)
                RowCellStateMachine::new()  // ← This now works!
            };

            // Parse partition key from binary
            let (partition_key, bytes_read) = state_machine.parse_partition_key(&data[offset..])?;
            //                                               ^^^^^^^^^^^^^^^^^^^^^
            // ❌ THIS FAILS - needs schema to parse key structure
        }
    }
}
```

**File**: `cqlite-core/src/storage/sstable/reader/parsing/key_parsing.rs` (Line 46-51)

**The Gate That Blocks Everything**:
```rust
pub(in crate::storage::sstable::reader) fn parse_row_key(
    &self,
    key_data: &[u8],
) -> Result<RowKey> {
    if let Some(schema) = self.get_table_schema() {
        // Schema available: parse structured key ✅
        self.parse_key_with_schema(key_data, &schema)
    } else {
        // No schema available
        #[cfg(feature = "legacy-heuristics")]
        {
            // Allow heuristic-based parsing (returns raw bytes)
            Ok(RowKey::new(key_data.to_vec()))
        }
        #[cfg(not(feature = "legacy-heuristics"))]
        {
            // BLOCKED: Can't parse without schema or feature flag ❌
            Err(Error::Schema(
                "Non-schema key parsing requires legacy-heuristics feature for legacy compatibility.".to_string()
            ))
        }
    }
}
```

---

## Why Didn't We See This Earlier?

### Unit Tests Pass Because...

1. **Mocked Data**:
   ```rust
   #[test]
   fn test_parse_partition_key() {
       let schema = TableSchema { /* explicit schema */ };
       let reader = SSTableReader::new_with_schema(schema);
       // ^^^^^^^^^^^^^^^^^^^^^^ Schema explicitly provided
       assert_eq!(reader.parse_key(&bytes), expected_key);
   }
   ```

2. **Storage Layer Isolation**:
   ```rust
   #[test]
   async fn test_sstable_reader() {
       let reader = SSTableReader::open(path, &config, platform).await?;
       let entries = reader.get_all_entries().await?;
       // Returns raw (RowKey, Value) - no parsing validation
       assert_eq!(entries.len(), 10);
   }
   ```

3. **No E2E Testing**:
   - Previous tests didn't go through full Database → Query → Storage → Parse stack
   - Each layer tested independently with controlled inputs
   - Integration tests used simplified scenarios or mocked components

### E2E Tests Expose the Gap

The `ci-one-shot-smoke.sh` script does TRUE end-to-end testing:
```bash
# Load real schema
export CQLITE_SCHEMA=test-data/schemas/basic-types.cql

# Execute real query through full stack
cqlite --schema $SCHEMA --dataset test_basic \
       --execute "SELECT * FROM test_basic.simple_table" \
       --format json
```

This goes through:
1. Parse CQL schema ✅
2. Load SSTables ✅
3. Parse SQL query ✅
4. Execute query (calls storage.scan()) ❌ **Schema not passed**
5. Parse binary SSTable data ❌ **Fails without schema**

---

## Three Architectural Solutions

### Option 1: Quick Fix - Allow Schema-less Fallback (2 hours)

**Approach**: Remove `legacy-heuristics` gate for modern V5.0 formats, allow raw key fallback

**Changes Required**:

**File 1**: `cqlite-core/src/storage/sstable/reader/parsing/key_parsing.rs`
```rust
pub(in crate::storage::sstable::reader) fn parse_row_key(
    &self,
    key_data: &[u8],
) -> Result<RowKey> {
    if let Some(schema) = self.get_table_schema() {
        // Schema available: parse structured key
        self.parse_key_with_schema(key_data, &schema)
    } else {
        // Modern V5.0 formats: allow raw key fallback
        // Note: This returns unparsed bytes, query layer must handle
        match self.header.cassandra_version {
            CassandraVersion::V5_0NewBig
            | CassandraVersion::V5_0Bti
            | CassandraVersion::V5_0DataFormat
            | CassandraVersion::V5_0FormatE
            | CassandraVersion::V5_0FormatF => {
                log::warn!(
                    "No schema available for V5.0 format - returning raw key data for key of length {}",
                    key_data.len()
                );
                Ok(RowKey::new(key_data.to_vec()))
            }
            _ => {
                // Legacy formats require feature flag
                #[cfg(feature = "legacy-heuristics")]
                {
                    Ok(RowKey::new(key_data.to_vec()))
                }
                #[cfg(not(feature = "legacy-heuristics"))]
                {
                    Err(Error::Schema(
                        "Schema-less parsing for legacy formats requires legacy-heuristics feature.".to_string()
                    ))
                }
            }
        }
    }
}
```

**File 2**: Similar changes in `parse_value()` methods

**What This Achieves**:
- ✅ Smoke tests pass (queries return data)
- ✅ Minimal code changes (2 files, ~30 lines)
- ✅ Fast to implement and test
- ✅ Unblocks CI for finding other issues

**Limitations**:
- ❌ Returns raw byte keys (not parsed into components)
- ❌ Query layer gets `RowKey::Raw(Vec<u8>)` instead of `RowKey::Parsed(components)`
- ❌ Filtering/comparison in query layer becomes harder
- ❌ Not production-ready (workaround only)

**Questions for Senior Developer**:
1. Is it acceptable to return raw keys temporarily to unblock testing?
2. Would query layer handle `RowKey::Raw` gracefully or break in unexpected ways?
3. Should we add a `RowKey::Raw` variant or use existing structure differently?

---

### Option 2: Proper Fix - Pass Schema Through Storage API (1-2 days)

**Approach**: Add schema parameter to all storage layer APIs

**Changes Required**:

**File 1**: `cqlite-core/src/storage/mod.rs`
```rust
impl StorageEngine {
    pub async fn scan(
        &self,
        table_id: &TableId,
        schema: Option<&TableSchema>,  // ← NEW: Optional schema parameter
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        // Pass schema through to SSTableManager
        let sstable_results = self
            .sstables
            .scan(table_id, schema, start_key, end_key, limit)  // ← Pass schema
            .await?;

        Ok(sstable_results)
    }
}
```

**File 2**: `cqlite-core/src/storage/sstable/manager.rs`
```rust
impl SSTableManager {
    pub async fn scan(
        &self,
        table_id: &TableId,
        schema: Option<&TableSchema>,  // ← NEW
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        let mut results = Vec::new();

        for reader in self.readers.values() {
            let entries = reader.scan(table_id, schema, start_key, end_key, limit).await?;
            //                                  ^^^^^^ Pass schema
            results.extend(entries);
        }

        Ok(results)
    }
}
```

**File 3**: `cqlite-core/src/storage/sstable/reader/data_access.rs`
```rust
impl SSTableReader {
    pub async fn scan(
        &self,
        table_id: &TableId,
        schema: Option<&TableSchema>,  // ← NEW
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        // Store schema for duration of scan
        let _guard = self.set_schema(schema);

        // Now parse_block_entries can access schema via self.get_schema()
        let all_entries = self.sequential_scan().await?;

        Ok(filtered)
    }

    // Helper to temporarily set schema
    fn set_schema(&self, schema: Option<&TableSchema>) -> SchemaGuard {
        // Use Arc<RwLock<Option<TableSchema>>> to allow temporary schema
        let mut current = self.runtime_schema.write().unwrap();
        let previous = current.clone();
        *current = schema.cloned();

        SchemaGuard {
            lock: &self.runtime_schema,
            previous,
        }
    }
}

// RAII guard to restore previous schema
struct SchemaGuard<'a> {
    lock: &'a RwLock<Option<TableSchema>>,
    previous: Option<TableSchema>,
}

impl<'a> Drop for SchemaGuard<'a> {
    fn drop(&mut self) {
        let mut current = self.lock.write().unwrap();
        *current = self.previous.take();
    }
}
```

**File 4**: `cqlite-core/src/query/select_executor.rs`
```rust
impl SelectExecutor {
    async fn execute_sstable_scan(
        &self,
        table: &TableRef,
        predicates: &[Predicate],
        projection: &[String],
        context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        // Look up schema from SchemaManager
        let schema = context.schema_manager
            .get_table_schema(&table.keyspace, &table.name)?;

        // Pass schema to storage layer
        let scan_results = self.storage.scan(
            table,
            Some(&schema),  // ← NEW: Pass schema
            None,
            None,
            None
        ).await?;

        // Now we get properly parsed keys!
        for (key, value) in scan_results {
            // key is RowKey::Parsed with components ✅
            // value is still raw, deserialize using schema
        }
    }
}
```

**Callsites to Update** (~20 locations):
1. `cqlite-core/src/query/select_executor.rs` - Query execution ✅
2. `cqlite-core/src/storage/mod.rs` - StorageEngine internal calls
3. `cqlite-core/tests/` - All integration tests
4. `cqlite-cli/src/` - CLI commands that scan
5. Any benchmarks that use storage API

**What This Achieves**:
- ✅ Production-ready solution
- ✅ Schema available exactly where needed
- ✅ Properly parsed keys and values
- ✅ Tests become explicit about schema requirements
- ✅ No global state (schema passed as parameter)

**Tradeoffs**:
- ❌ API signature changes (breaking change for storage layer)
- ❌ Need to update all callsites
- ❌ Tests need refactoring to pass schema
- ❌ More parameters to pass through (could use context struct?)

**Questions for Senior Developer**:
1. Should schema be `Option<&TableSchema>` or required `&TableSchema`?
2. Should we bundle parameters into a `ScanContext { schema, stats, ... }` struct?
3. How to handle schema lifecycle in SSTableReader (RAII guard vs store in struct)?
4. Should we make this change incrementally (add optional schema first, then make required)?

---

### Option 3: Clean Architecture - Schema Registry Pattern (2-3 days)

**Approach**: Central SchemaRegistry accessible to all layers via Arc reference

**New Architecture**:
```rust
// New struct: SchemaRegistry
pub struct SchemaRegistry {
    schemas: RwLock<HashMap<TableId, Arc<TableSchema>>>,
}

impl SchemaRegistry {
    pub fn register(&self, table_id: TableId, schema: TableSchema) {
        let mut schemas = self.schemas.write().unwrap();
        schemas.insert(table_id, Arc::new(schema));
    }

    pub fn get(&self, table_id: &TableId) -> Option<Arc<TableSchema>> {
        let schemas = self.schemas.read().unwrap();
        schemas.get(table_id).cloned()
    }
}
```

**Integration**:

**File 1**: `cqlite-core/src/database.rs`
```rust
pub struct Database {
    storage: Arc<StorageEngine>,
    schema_manager: Arc<RwLock<SchemaManager>>,
    schema_registry: Arc<SchemaRegistry>,  // ← NEW: Shared registry
    query_engine: Option<Arc<QueryEngine>>,
}

impl Database {
    pub async fn new(
        storage: StorageEngine,
        schema_manager: SchemaManager,
    ) -> Result<Self> {
        let schema_registry = Arc::new(SchemaRegistry::new());

        // Populate registry from SchemaManager
        for (table_id, schema) in schema_manager.all_schemas() {
            schema_registry.register(table_id, schema);
        }

        // Pass registry to storage engine
        let storage = Arc::new(storage.with_schema_registry(schema_registry.clone()));

        Ok(Database {
            storage,
            schema_manager: Arc::new(RwLock::new(schema_manager)),
            schema_registry,
            query_engine: Some(Arc::new(QueryEngine::new(...))),
        })
    }
}
```

**File 2**: `cqlite-core/src/storage/mod.rs`
```rust
pub struct StorageEngine {
    memtable: Arc<RwLock<MemTable>>,
    sstables: Arc<SSTableManager>,
    wal: Arc<RwLock<WriteAheadLog>>,
    schema_registry: Arc<SchemaRegistry>,  // ← NEW: Registry reference
}

impl StorageEngine {
    pub fn with_schema_registry(mut self, registry: Arc<SchemaRegistry>) -> Self {
        self.schema_registry = registry.clone();
        self.sstables = Arc::new(
            Arc::try_unwrap(self.sstables)
                .unwrap_or_else(|arc| (*arc).clone())
                .with_schema_registry(registry)
        );
        self
    }

    pub async fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        // No API change! ✅
        // Schema looked up internally
        let sstable_results = self
            .sstables
            .scan(table_id, start_key, end_key, limit)
            .await?;

        Ok(sstable_results)
    }
}
```

**File 3**: `cqlite-core/src/storage/sstable/reader/data_access.rs`
```rust
impl SSTableReader {
    pub async fn scan(
        &self,
        table_id: &TableId,
        start_key: Option<&RowKey>,
        end_key: Option<&RowKey>,
        limit: Option<usize>,
    ) -> Result<Vec<(RowKey, Value)>> {
        // Look up schema from registry
        let schema = self.schema_registry
            .get(table_id)
            .ok_or_else(|| Error::schema(format!("Schema not found for table {}", table_id)))?;

        // Store for duration of scan
        let _guard = self.set_runtime_schema(Some(&schema));

        // parse_block_entries now has schema access ✅
        let all_entries = self.sequential_scan().await?;

        Ok(filtered)
    }
}
```

**What This Achieves**:
- ✅ Clean separation of concerns (registry owns lifecycle)
- ✅ No API signature changes (schema lookup is internal)
- ✅ Tests unaffected (registry auto-populated in test fixtures)
- ✅ Proper schema lifecycle management
- ✅ Cache invalidation handled by registry
- ✅ Thread-safe with Arc<RwLock>

**Tradeoffs**:
- ❌ More complex (new component to manage)
- ❌ Arc/RwLock overhead (minimal but present)
- ❌ Need to handle registry population during ingestion
- ❌ Need to handle schema updates (ALTER TABLE scenarios)
- ❌ Longer implementation time

**Questions for Senior Developer**:
1. Is the added complexity worth the clean API?
2. Should SchemaRegistry be in Database or a separate global?
3. How to handle schema cache invalidation on ALTER TABLE?
4. Should registry support schema versioning for multi-version reads?
5. What's the performance impact of RwLock on hot read path?

---

## Comparison Matrix

| Criteria | Option 1: Quick Fix | Option 2: Pass Schema | Option 3: Registry |
|----------|-------------------|---------------------|-------------------|
| **Implementation Time** | 2 hours | 1-2 days | 2-3 days |
| **Code Changes** | ~2 files, 30 lines | ~15 files, ~200 lines | ~10 files, ~300 lines |
| **API Stability** | No breaks | Breaks storage API | No breaks |
| **Test Updates** | Minimal | ~20 callsites | Minimal |
| **Production Ready** | ❌ No (workaround) | ✅ Yes | ✅ Yes |
| **Maintainability** | ⚠️ Technical debt | ✅ Explicit | ✅ Clean |
| **Performance** | ✅ Fast (no overhead) | ✅ Fast (direct pass) | ⚠️ RwLock overhead |
| **Testability** | ⚠️ Hides issues | ✅ Explicit requirements | ✅ Test isolation |
| **Future ALTER TABLE** | ❌ Doesn't support | ⚠️ Need to re-pass | ✅ Registry handles |
| **Schema Caching** | N/A | N/A | ✅ Built-in |

---

## Recommended Phased Approach

My recommendation is **Option 1 now, then Option 2**:

### Phase 1: Unblock Testing (Option 1) - 2 hours

**Goal**: Get smoke tests passing to find other issues

**Implementation**:
1. Modify `key_parsing.rs` to allow raw key fallback for V5.0
2. Modify `value_parsing.rs` similarly
3. Run smoke tests - should pass ✅
4. Document limitation in code comments

**Acceptance Criteria**:
- Smoke tests pass
- Keys returned as raw bytes (not parsed)
- Query layer tolerates raw keys

### Phase 2: Production Fix (Option 2) - 1-2 days

**Goal**: Proper schema propagation for production use

**Implementation**:
1. Add `schema: Option<&TableSchema>` to storage APIs
2. Update query executor to pass schema
3. Update all callsites
4. Add integration tests validating parsed keys
5. Remove Option 1 workaround

**Acceptance Criteria**:
- All tests pass with proper schema
- Keys properly parsed into components
- No `legacy-heuristics` gates remaining

### Alternative: Go Straight to Option 3 - 2-3 days

**If** we have time and want the cleanest solution:
- Skip Option 1 entirely
- Implement SchemaRegistry properly
- No technical debt, no refactoring later

---

## Critical Questions for Decision

### Architecture Philosophy

1. **API Design**: Should storage layer be schema-aware or schema-agnostic?
   - Storage-agnostic (Option 3) = cleaner separation, but more complex
   - Storage-aware (Option 2) = explicit dependencies, easier to reason about

2. **Schema Lifecycle**: Who owns schema lifecycle?
   - Database/SchemaManager (current)
   - SchemaRegistry (Option 3)
   - Passed as parameter (Option 2)

3. **Testing Strategy**: What's our testing philosophy?
   - Tests should be explicit about requirements (favors Option 2)
   - Tests should be isolated and self-contained (favors Option 3)
   - Tests should pass quickly (favors Option 1 short-term)

### Technical Constraints

4. **Time Pressure**: How urgent is fixing CI?
   - Very urgent (use Option 1 to unblock)
   - Moderate (go straight to Option 2)
   - Not urgent (implement Option 3 properly)

5. **API Stability**: How much API churn can we tolerate?
   - High churn tolerance (Option 2 is fine)
   - Low churn tolerance (Option 3 to avoid double-refactoring)

6. **Performance**: What's acceptable overhead?
   - Zero overhead (Option 1 or 2)
   - Minimal RwLock overhead okay (Option 3)

### Future Considerations

7. **Schema Evolution**: Do we plan to support ALTER TABLE?
   - Yes → Option 3 (registry handles updates centrally)
   - No/Later → Option 2 (simpler for now)

8. **Multi-Version Reads**: Need MVCC schema versioning?
   - Yes → Option 3 (registry can track versions)
   - No → Option 2 (single version is fine)

9. **Distributed Queries**: Future sharding/distribution plans?
   - Yes → Option 3 (registry can be distributed)
   - No → Option 2 (local is fine)

---

## My Recommendation with Rationale

### Recommended: Phased Approach (Option 1 → Option 2)

**Why**:
1. **We're in debugging mode** - CI is broken, we need fast feedback
2. **Unknown unknowns** - Option 1 will reveal other issues quickly
3. **Pragmatic** - Option 1 gets us data, Option 2 gets us quality
4. **Low risk** - Option 1 is small, easy to undo if wrong

**Timeline**:
- **Today** (2 hours): Implement Option 1, get smoke tests passing
- **Tomorrow** (find other issues): Run full test suite, identify remaining problems
- **Next Week** (1-2 days): Implement Option 2 properly with lessons learned

**Risk Mitigation**:
- Option 1 is clearly marked as temporary (comments, TODO, issue tracking)
- Option 2 planned before Option 1 merged (no forgetting)
- Tests added for both workaround and proper fix

### Alternative: Go Straight to Option 2

**If** senior developers prefer no technical debt:
- Skip Option 1
- Implement Option 2 directly (1-2 days)
- Higher confidence in production quality
- Slower feedback loop but cleaner codebase

### Not Recommended: Option 3 Right Now

**Why not**:
- More complex than we need for current requirements
- No immediate need for schema versioning or caching
- Can refactor to Option 3 later if requirements emerge (YAGNI principle)
- Option 2 → Option 3 migration is straightforward (wrap existing API)

---

## Questions Needing Answers

Please provide guidance on:

### Priority 1 (Blocking)

1. **Which option should we implement?**
   - [ ] Option 1 only (quick fix)
   - [ ] Option 1 then Option 2 (phased)
   - [ ] Option 2 directly (skip workaround)
   - [ ] Option 3 (registry pattern)

2. **If Option 2**: Should schema parameter be optional or required?
   - [ ] `Option<&TableSchema>` (gradual migration)
   - [ ] `&TableSchema` (explicit requirement)

3. **API Design**: Should we bundle parameters into context struct?
   ```rust
   pub struct ScanContext<'a> {
       pub table_id: &'a TableId,
       pub schema: Option<&'a TableSchema>,
       pub start_key: Option<&'a RowKey>,
       pub end_key: Option<&'a RowKey>,
       pub limit: Option<usize>,
   }
   ```
   - [ ] Yes, use context struct (cleaner API, easier to extend)
   - [ ] No, keep individual parameters (simpler for now)

### Priority 2 (Important)

4. **Schema Lifecycle**: How should SSTableReader hold schema?
   - [ ] RAII guard (temporary, dropped after scan)
   - [ ] Struct field (stored during reader lifetime)
   - [ ] Arc reference (shared ownership)

5. **Error Handling**: What should happen if schema unavailable?
   - [ ] Return error (fail fast)
   - [ ] Return raw keys with warning (graceful degradation)
   - [ ] Use default/inferred schema (heuristics)

6. **Testing**: How should tests provide schema?
   - [ ] Explicit parameter in test setup
   - [ ] Test fixtures with schema registry
   - [ ] Mock schema manager

### Priority 3 (Nice to Have)

7. **Performance**: Should we cache schemas in SSTableReader?
   - [ ] Yes (optimize hot path)
   - [ ] No (keep simple, optimize later)

8. **Logging**: What level of schema-related logging?
   - [ ] Debug (verbose for development)
   - [ ] Info (key lifecycle events)
   - [ ] Warn (only when missing)

9. **Documentation**: Should we document schema propagation architecture?
   - [ ] Yes, create ADR (Architecture Decision Record)
   - [ ] Yes, update developer docs
   - [ ] No, code comments sufficient

---

## Next Steps Based on Decision

### If Option 1 Chosen:
1. Implement raw key fallback (2 hours)
2. Run smoke tests and document limitations
3. File issue for Option 2 implementation
4. Estimate Option 2 effort

### If Option 2 Chosen:
1. Define ScanContext struct or parameter list
2. Update storage API signatures
3. Update query executor callsites
4. Refactor tests
5. Integration test with full stack

### If Option 3 Chosen:
1. Design SchemaRegistry interface
2. Implement registry with tests
3. Integrate into Database
4. Update storage layer to use registry
5. Handle schema lifecycle edge cases

---

## Additional Context

### Related Issues
- **Issue #154**: Collections with UDTs header parsing (FIXED ✅)
- **CI Smoke Tests**: All failing with schema propagation gap (OPEN ❌)

### Files Modified So Far
- `cqlite-core/src/storage/sstable/reader/header.rs` (Issue #154 fix)
- `cqlite-core/src/storage/sstable/reader/parsing/block_entries.rs` (V5.0 state machine)
- `cqlite-core/src/parser/header.rs` (FormatE/F support)
- `cqlite-core/tests/issue_154_test.rs` (regression test)

### Commits
- `91c89e7`: Issue #154 fix (NB format header detection)
- `68c4214`: V5.0 state machine + FormatE/F (current HEAD)

### Test Status
- ✅ Unit tests: 750+ passing
- ✅ Integration tests: All passing
- ❌ E2E smoke tests: All failing (schema propagation)

---

**Prepared by**: Claude (AI Assistant)
**Date**: 2025-10-13
**Requires Review From**: Senior Developer (Architecture Decision)
**Timeline**: Needs decision today to unblock CI
