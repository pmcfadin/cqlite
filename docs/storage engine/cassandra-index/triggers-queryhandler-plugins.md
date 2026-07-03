# Cassandra Triggers, QueryHandler & Plugin Injection Points

**Subsystem:** Third-party code injection into Cassandra JVM (write-path augmentation, query interception, storage format, memtable, monitoring).

**Research relevance:** Q1 (freshness) — how to intercept reads to include memtable state; Q2 (feasibility) — whether an alternative storage engine can replace or run alongside BIG/BTI formats.

## Summary

Apache Cassandra 7.0 (trunk, as of 2026-07-03) provides multiple injection seams for third-party code via:
1. **Triggers** — mutation augmentation at coordinator (write-path, partition-scoped)
2. **Custom QueryHandler** — complete query interception via system property
3. **SSTableFormat plugin system** — pluggable storage format via ServiceLoader
4. **Memtable API (CEP-11)** — pluggable memtable implementation via config
5. **DiagnosticEvent service** — read/write-path event publishing with subscribers
6. **MBean registration** — JMX hooks for daemon/service lifecycle
7. **Listener/Subscriber patterns** — schema, endpoint state, failure detection

**Key insight:** Reads bypass custom QueryHandler (no hook). Memtable data flows through unified read path (CFS.selectMemtables + filter stack); no external read-time interception point exists. Formats are locked at table creation; no cross-format analytical reads without materialization.

## Key Classes & Interfaces (file:line anchors)

### Write Path (Mutation Augmentation)

| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| `ITrigger` | src/java/org/apache/cassandra/triggers/ITrigger.java:41 | Interface for partition-scoped mutation augmentation; `augment(Partition update) → Collection<Mutation>` |
| `TriggerExecutor` | src/java/org/apache/cassandra/triggers/TriggerExecutor.java:57 | Singleton; loads triggers from `$CASSANDRA_HOME/lib/triggers/` via CustomClassLoader; caches instances; executed from StorageProxy.cas (CAS writes only) |
| `CustomClassLoader` | src/java/org/apache/cassandra/triggers/CustomClassLoader.java | Classloader for jar files in trigger directory; reloadable via `reloadClasses()` |
| `TriggerMetadata` | src/java/org/apache/cassandra/schema/TriggerMetadata.java | Schema-defined trigger name → class mapping |

### Query Interception

| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| `QueryHandler` | src/java/org/apache/cassandra/cql3/QueryHandler.java:32 | Interface: parse, process, prepare, getPrepared, processPrepared, processBatch |
| `QueryProcessor` | src/java/org/apache/cassandra/cql3/QueryProcessor.java:~60 | Default impl; instantiated via `instance` singleton; custom handler injected via property |
| `ClientState` | src/java/org/apache/cassandra/service/ClientState.java | Loads custom handler from `CUSTOM_QUERY_HANDLER_CLASS` system property via FBUtilities.construct() |

### Storage Format Plugin

| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| `SSTableFormat<R,W>` | src/java/org/apache/cassandra/io/sstable/format/SSTableFormat.java:45 | Interface: version, reader/writer factories, components, scrubber |
| `SSTableFormat.Factory` | src/java/org/apache/cassandra/io/sstable/format/SSTableFormat.java (nested) | ServiceLoader-discovered; `name() → String` (e.g. "big", "bti") |
| `BigFormat.BigFormatFactory` | src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java | Fallback if no factories discovered |
| `DatabaseDescriptor.applySSTableFormats()` | src/java/org/apache/cassandra/config/DatabaseDescriptor.java:1974 | ServiceLoader discovery; validation; registration as ImmutableMap |

### Memtable Plugin (CEP-11)

| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| `Memtable` | src/java/org/apache/cassandra/db/memtable/Memtable.java:60 | Interface: Factory pattern; `create(commitLogLowerBound, metadataRef, owner) → Memtable` |
| `Memtable.Factory` | src/java/org/apache/cassandra/db/memtable/Memtable.java:77 | ServiceLoader or static FACTORY field; configured via `CREATE TABLE ... WITH memtable = '<config>'` |
| `SkipListMemtableFactory` | src/java/org/apache/cassandra/db/memtable/SkipListMemtableFactory.java | Default; registered in `memtable_configurations` (cassandra.yaml) |

### Daemon & Service Registration

| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| `CassandraDaemon` | src/java/org/apache/cassandra/service/CassandraDaemon.java:244 | Entry point; registers MBeans, listeners; calls StorageService.initServer() |
| `StorageService` | src/java/org/apache/cassandra/service/StorageService.java | Registers self as MBean; manages ring, bootstrap; owns queryHandler reference |
| `AuditLogManager` | src/java/org/apache/cassandra/audit/AuditLogManager.java | Singleton; initialized in CassandraDaemon.start() |

### Observability & Events

| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| `DiagnosticEventService` | src/java/org/apache/cassandra/diag/DiagnosticEventService.java:46 | Singleton; subscribers by event class/type; publish(DiagnosticEvent) broadcasts to all |
| `DiagnosticEvent` | src/java/org/apache/cassandra/diag/DiagnosticEvent.java | Base class; emitted during bootstrap, commit, flush, compaction, etc. |
| `SchemaChangeListener` | src/java/org/apache/cassandra/schema/SchemaChangeListener.java | Interface; DDL notifications; impl: StatementInvalidatingListener, AuthSchemaChangeListener |
| `IEndpointStateChangeSubscriber` | src/java/org/apache/cassandra/gms/IEndpointStateChangeSubscriber.java | Gossip state changes; ring events |

## Extension Points & Pluggability Seams

### 1. Triggers (Write-Path Mutation Augmentation)

**Config hook:** Create trigger via `CREATE TRIGGER` statement; class jar goes to `$CASSANDRA_HOME/lib/triggers/`

**Injection point:** `StorageProxy.cas()` line ~481 calls `TriggerExecutor.instance.execute(updates)` → runs ITrigger.augment() on each PartitionUpdate (CAS writes only; not on regular mutations via mutate() path)

**Limitations:**
- Single partition only (validated at coordinator)
- CAS writes only; regular writes bypass triggers
- Must be loaded at server init or via manual reload; no per-query override

**Hypothesis:** Cannot inject at non-CAS write path (`StorageProxy.mutate()` line ~975) without refactoring; triggers are not called during AsyncRepairHandler or streaming.

### 2. Custom QueryHandler (Query Interception)

**Config hook:** `-Dcassandra.custom_query_handler_class=<FQN>`

**Injection point:** `ClientState` static init loads class via `FBUtilities.construct()`, assigns to `cqlQueryHandler` field; every CQL request routed to handler.process()

**Methods interceptable:** parse, process, prepare, getPrepared, processPrepared, processBatch

**Hard coupling:** Handler must return ResultMessage; no streaming hookpoints; native transport (CQLMessageHandler → Dispatcher → ClientState.cqlQueryHandler) is the only entry. Thrift (legacy) has its own handler path (not CQL).

**Hypothesis:** Can hook *all* CQL queries (select, insert, etc.) but only at the entry point; internal phases (execution, filtering, ghosting) are opaque.

### 3. SSTableFormat Plugin System

**Discovery:** `DatabaseDescriptor.applySSTableFormats()` (line 1974) loads via `ServiceLoader<SSTableFormat.Factory>` with fallback to `BigFormat.BigFormatFactory`

**Registration:** Formats validated by name (lowercase letters), stored in ImmutableMap; config: `cassandra.yaml` → `sstable.format.selected_format` (default "big")

**Pluggability:** Implement `SSTableFormat.Factory`, list in `META-INF/services/org.apache.cassandra.io.sstable.format.SSTableFormat$Factory`; jar goes to classpath

**Table-level lock:** SSTableFormat chosen at table creation via CREATE TABLE; immutable. Cross-format reads require external orchestration (CQLite's strategy).

**Hypothesis:** Third-party format (e.g., column-oriented, compressed differently) can be loaded, but tables are single-format lifetime. No query-time format negotiation.

### 4. Memtable Plugin (CEP-11)

**Discovery:** Either via ServiceLoader `Memtable.Factory` or static FACTORY field in implementation

**Registration:** `cassandra.yaml` → `memtable_configurations.<name>` maps string config name to class; `CREATE TABLE ... WITH memtable='<name>'`

**Contract:** Factory must have no-arg constructor or static factory(Map<String, String>); creates Memtable instance per table

**Read path:** `ColumnFamilyStore` calls memtable.partitionIterator() and memtable.rowIterator() during read-phase filtering; no external hook.

**Hypothesis:** Can customize in-memory layout/encoding but reads still flow through standard filter stack (no read-path interception before ghosting).

### 5. DiagnosticEvent Subscribers

**Injection point:** `DiagnosticEventService.subscribe(Consumer<DiagnosticEvent>)` or by class/type; no configuration-based registration

**Events published:** Bootstrap, TombstoneScavenged, BootstrapMetricsEvent, etc.; published conditionally on `DatabaseDescriptor.diagnosticEventsEnabled()`

**Hookpoints:** Read/write operations publish events *after* mutation applied or after read returned; not during processing.

**Hypothesis:** Cannot inject logic that blocks or filters events; subscribers are async callbacks.

### 6. Listeners & Subscribers (Ad-Hoc Pattern)

| Listener | Registered | Phase |
|----------|-----------|-------|
| `SchemaChangeListener` | Schema.instance | DDL (table/keyspace/type changes) |
| `IEndpointStateChangeSubscriber` | Gossiper.instance | Gossip/ring events |
| `IFailureDetectionEventListener` | FailureDetector.instance | Failure detection |
| `IEndpointLifecycleSubscriber` | StorageService | Endpoint bootstrap/leave |
| `SSTableReadsListener` | CFS or query context (Issue #1542 draft) | Per-read metrics (not stable) |

**Hard coupling:** Each listener type has its own registration interface; no unified plugin API.

## Hard Couplings & Assumptions Breaking an Alternative Engine

1. **Partition key as Token routing** (line ~StorageProxy.performWrite) — writes routed by token; alternative engine must implement Partitioner interface
2. **Schema-defined tables** (`TableMetadata`) — all mutations/reads require schema; cannot bypass schema layer
3. **CommitLog at partition boundary** — writes logged before applying; recovery replays mutations; alternative must preserve log format or hook into CommitLog.recover()
4. **Memtable → SSTable durability contract** — flush() creates SSTableWriter; alternatives must implement Memtable.Flushing interface
5. **SSTableFormat enumeration at startup** — formats registered once at init; runtime format registration not supported
6. **Read-phase filter stack** (UnfilteredPartitionIterator → filter → ghosting → result) — no break-glass hook to bypass or replace
7. **QueryHandler is per-node singleton** — one handler instance for all CQL; no per-tenant/per-keyspace override
8. **Trigger validation in CAS path only** — StorageProxy.mutate() does not call TriggerExecutor; regular inserts cannot be augmented
9. **Thrift protocol bypass** — QueryHandler is native protocol only; Thrift (if enabled) routes through its own pipeline

## Q1 & Q2 Relevance

### Q1: Arrow Flight Connector Freshness (Reading Memtable + SSTables)

**Current gap:** Arrow Flight connector (external via CQLite) reads only SSTables on disk; in-flight memtable mutations invisible to analytical reader.

**Cassandra hookpoints to expose memtable:**
- None exist in 7.0 for read-side interception
- `DiagnosticEventService` publishes MutationApplied *after* memtable write; too late for read
- Custom `Memtable.Factory` can log mutations in-stream but no way to expose them to external process
- `QueryHandler.process()` sees all mutations *and* can return custom results, but only for CQL (not Arrow Flight protocol)

**Options within Cassandra 7.0:**
1. *Custom QueryHandler*: Intercept `SELECT` via QueryHandler, return Arrow Flight result. But no protocol wrapping.
2. *DiagnosticEvent subscriber*: Listen to MutationApplied, buffer in external store, join with SSTables. Requires dual writes.
3. *Memtable.Factory hook*: Implement custom factory that mirrors writes to external index. No read-side integration.
4. *SSTableReaderListener* (proposed #1542): Hook into CFS reads; can observe SSTable access but not memtable.

**Verdict for Q1:** Cassandra 7.0 has no integrated read-time hook to expose memtable data to external reader. CQLite's approach (external Arrow Flight serving SSTables only) is the only current seam; full memtable visibility requires:
- (a) A new Cassandra hook `ReadCommand.executeInternal()` that allows external interception, or
- (b) Custom QueryHandler + custom protocol layer, or
- (c) Memtable duplication to external store via DiagnosticEvent listener (stale-window race)

### Q2: CQLite as Alternative Storage Engine (Pluggability)

**Can CQLite replace BIG/BTI?** (Partially)
- ✅ `SSTableFormat` plugin seam exists; CQLite can register a format
- ❌ But: table format is immutable at creation; cannot migrate live tables between engines
- ❌ Mutations (writes) still route through Cassandra's CommitLog → Memtable → SSTableWriter; CQLite's write surface must conform to Memtable.Flushing contract
- ❌ Schema layer (TableMetadata, partition key, clustering, type decoding) is not pluggable; CQLite cannot override

**Can CQLite run alongside as OLAP engine?** (Yes, via external process)
- ✅ External Arrow Flight server (CQLite's current approach) is cleanest seam
- ✅ CQLite SSTableReader can parse BIG/BTI on filesystem
- ❌ Memtable freshness issue (Q1) requires application-side read merging or dual writes

**In-JVM plugin (CQLite as Memtable.Factory)?** (No practical benefit)
- ❌ Memtable interface requires Flushing → SSTableWriter; CQLite would still emit uncompressed SSTables (current claim boundary)
- ❌ Read path (partitionIterator) would just layer over same file I/O CQLite does externally
- ✅ Avoids JNI/FFI overhead but loses isolation; Cassandra process crash = data loss

**Verdict for Q2:** CQLite as external OLAP engine (current model) is feasible via SSTableFormat plugin + external reader. In-JVM replacement is blocked by CommitLog → Memtable contract. In-JVM OLTP replacement blocked by read-phase integration gaps.

## Trunk vs 5.0 Delta

| Feature | 5.0 | 7.0 (Trunk) | Impact |
|---------|-----|-----------|--------|
| CEP-11 Memtable API | No pluggable Memtable.Factory | Yes, via `memtable_configurations` in cassandra.yaml | Enables custom memtable implementations |
| Pluggable SSTableFormat | BIG only; no ServiceLoader | Yes, via ServiceLoader + fallback BigFormat | Enables custom storage formats |
| Custom QueryHandler system property | Yes (`cassandra.custom_query_handler_class`) | Yes, unchanged | Can intercept all CQL queries |
| Triggers | Yes (CAS path only) | Yes, unchanged | No write-path expansion in 5.0 either |
| DiagnosticEvent service | Yes (beta in 5.0) | Yes, stable | Event publishing unchanged |
| Read-path hooks | None | None | No improvement for Q1 |
| CommitLog format | Not pluggable | Not pluggable | Hard coupling remains |
| Partition routing via Token | Yes | Yes | Hard coupling remains |

**Inference:** Trunk (7.0) adds *storage-layer* extensibility (memtable, format) but not *read-layer* observability. Q1 remains unaddressed between versions.

## Summary Table: Injection Points Ranked by Accessibility

| Seam | Complexity | Footprint | Write? | Read? | Recommended for Q1 | Recommended for Q2 |
|------|-----------|-----------|--------|-------|-------------------|-------------------|
| Triggers (CAS only) | Low | 100 LOC | ✅ (CAS) | ✗ | No | No |
| Custom QueryHandler | Medium | Varies | ✅ (all CQL) | ✅ (all CQL) | Partial (protocol issue) | No |
| SSTableFormat plugin | High | 1K+ LOC | ✅ (format choice) | ✓ (format parsing) | No | ✅ (external reader) |
| Memtable.Factory | High | 2K+ LOC | ✅ (in-memory layout) | ✓ (memtable.iterator) | No | No |
| DiagnosticEvent listener | Low | 100 LOC | ✗ (async callback) | ✗ (async callback) | No (stale data) | No |
| Custom MBean | Low | 50 LOC | ✗ | ✗ | No | No |

---

**Next steps for research:**
1. Verify CustomPayloadMirroringQueryHandler (line ~1) as reference impl of custom handler
2. Check if Thrift protocol has parallel hooks (separate from native CQL)
3. Audit CEP-11 spec document for memtable lifecycle guarantee details
4. Trace read path (ReadCommand.executeInternal) to confirm no interception seam
