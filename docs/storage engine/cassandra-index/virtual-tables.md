# Cassandra Virtual Tables Subsystem Index

## Summary

The Virtual Tables API in Apache Cassandra (trunk, post-CEP-11) provides a pluggable mechanism for exposing system information and metadata via queryable CQL tables. Virtual tables are read-only by default; mutable variants allow controlled writes (e.g., cache invalidation, configuration updates). The subsystem is NOT integrated with memtable or SSTable readers—it has zero connection to the hot storage path. Virtual keyspaces must be manually registered at startup; third-party registration is possible but requires code changes to `CassandraDaemon.setupVirtualKeyspaces()`. No native paging, streaming, or Arrow/Flight integration exists.

## Key Classes & Interfaces

| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| `VirtualTable` | `db/virtual/VirtualTable.java:34` | Core interface: `select(partitionKey|dataRange, filters, limits)→UnfilteredPartitionIterator`, `apply(PartitionUpdate)`, `truncate()`. Sorted/indexing hints via optional methods. |
| `AbstractVirtualTable` | `db/virtual/AbstractVirtualTable.java:48` | Base read-only impl; subclasses override `data()` to supply `DataSet` (partition map). Translates query filters into row iteration. Throws `InvalidRequestException` on writes. |
| `AbstractMutableVirtualTable` | `db/virtual/AbstractMutableVirtualTable.java:51` | Extends `AbstractVirtualTable` with pluggable write handlers: `applyColumnUpdate()`, `applyRowDeletion()`, `applyPartitionDeletion()`, `applyRangeTombstone()`. Row/column updates decomposed via `ColumnValues`/`ColumnValue` PODs. |
| `AbstractLazyVirtualTable` | `db/virtual/AbstractLazyVirtualTable.java` | Caches `DataSet` in memory, rebuilds on demand (used by metrics, peers tables). |
| `VirtualKeyspace` | `db/virtual/VirtualKeyspace.java:32` | Container: name + `ImmutableCollection<VirtualTable>`, converts to `KeyspaceMetadata.virtual()`. Enforces no duplicate table names. |
| `VirtualKeyspaceRegistry` | `db/virtual/VirtualKeyspaceRegistry.java:31` | Singleton; `register(VirtualKeyspace)` → `ConcurrentHashMap<name, keyspace>` + `<TableId, table>`. Queries via `getTableNullable(id)`. |
| `VirtualMutation` | `db/virtual/VirtualMutation.java:54` | `IMutation` impl: routes writes to `VirtualTable.apply(PartitionUpdate)` per table. Accord-safe (non-transactional). |
| `SystemViewsKeyspace` | `db/virtual/SystemViewsKeyspace.java:29` | Singleton; hardcoded `system_views` keyspace with ~30 system tables (caches, clients, thread pools, metrics, peers, gossip, etc.). |
| `VirtualSchemaKeyspace` | `db/virtual/VirtualSchemaKeyspace.java:34` | Singleton; `system_schema` keyspace exposing virtual keyspaces/tables/columns via nested inner `AbstractVirtualTable` classes. |
| `CollectionVirtualTableAdapter` | `db/virtual/CollectionVirtualTableAdapter.java` | Wraps `Collection<E>` + `RowWalker<E>` into `VirtualTable`. Backward-compatible: used for thread-pool metrics. |
| `RemoteToLocalVirtualTable` | `db/virtual/RemoteToLocalVirtualTable.java:19` | Peer-to-peer virtual table: fetches non-local replica data on demand using gossip/tcm, merges into single result. **Only async read path; no memtable/SSTable exposure.** |

## Extension Points / Pluggability Seams

1. **Custom VirtualTable Registration** (weak): 
   - Implement `VirtualTable` or extend `AbstractVirtualTable` / `AbstractMutableVirtualTable`.
   - Wrap in `VirtualKeyspace(name, List.of(table1, table2, ...))`.
   - Register at startup via `VirtualKeyspaceRegistry.instance.register(keyspace)`.
   - **Seam location**: `CassandraDaemon.setupVirtualKeyspaces()` at `src/java/org/apache/cassandra/service/CassandraDaemon.java:640–651`. Hardcoded registration; no SPI/plugin mechanism. Extensions require code change.

2. **Mutable Virtual Tables** (configurable):
   - Extend `AbstractMutableVirtualTable`, override granular write methods: `applyColumnUpdate()`, `applyRowDeletion()`, etc.
   - Each method can throw `InvalidRequestException` to reject unsupported operations (e.g., cache tables reject partition deletes).
   - **Example**: `SettingsTable.java` allows column updates; cache-key tables only allow range deletions.

3. **Data Supply Abstraction** (template):
   - Implement `DataSet` interface: `isEmpty()`, `getPartition(key)`, `getPartitions(range)`.
   - Often backed by `AbstractDataSet` (NavigableMap-based); subclasses can use metrics, config, or remote peers.
   - `data()` can be regenerated per query (lazy) or cached; no lifecycle management.

4. **Query Filtering** (partial):
   - `VirtualTable.allowFilteringImplicitly()` (default true) auto-adds ALLOW FILTERING if needed.
   - `sorted()` hint (UNSORTED|ASC|DESC|SORTED) can optimize query planning.
   - `indexes()` returns `IndexRegistry.EMPTY` by default; no virtual table indexes wired.
   - Row/column filtering delegated to caller post-iteration; no pushdown.

## Hard Couplings

1. **Storage Engine Isolation**: Virtual tables have **zero coupling** to memtables, SSTables, or read-path storage:
   - `VirtualTable.select()` returns `UnfilteredPartitionIterator` directly; bypasses `Keyspace`/`Table`/`ColumnFamilyStore` read pipeline.
   - No access to `Memtable` / `SSTableReader` / `MemtableIndex` APIs.
   - **Implication (Q1)**: Analytics read on a node sees ONLY flushed SSTables via normal CQL reads. Virtual tables offer **no seam** to expose unflushed memtable data.

2. **Keyspace Metadata Coupling**:
   - Virtual tables must specify `TableMetadata` with `kind(Kind.VIRTUAL)`.
   - `TableMetadata.isVirtual()` check enforced in `AbstractVirtualTable` ctor; misconfiguration throws `IllegalArgumentException`.
   - Schema metadata baked into `KeyspaceMetadata.virtual()` at registration time; no dynamic schema updates post-registration.

3. **IMutation Routing**:
   - Write operations on virtual tables routed via `VirtualMutation` (custom `IMutation` impl).
   - `VirtualMutation.apply()` directly calls `VirtualKeyspaceRegistry.getTableNullable(id).apply(PartitionUpdate)`.
   - **Invariant**: Writes do NOT pass through normal Cassandra commit log, replication, or Accord consensus (marked `PotentialTxnConflicts.ALLOW`).

4. **Query Execution Path**:
   - `SinglePartitionReadCommand.LocalExecution::executeLocally()` at `src/java/org/apache/cassandra/db/SinglePartitionReadCommand.java:1569–1574` checks `VirtualKeyspaceRegistry` first, **short-circuits** read if table is virtual (no storage engine involvement).
   - Same for `PartitionRangeReadCommand` at `src/java/org/apache/cassandra/db/PartitionRangeReadCommand.java:651–657`.
   - **Implication (Q2)**: Virtual tables are read-only from storage perspective—writes are side-effect operations on in-memory state; no recovery or durability guarantees.

5. **Accord (Cluster Consensus) Exclusion**:
   - `VirtualMutation.potentialTxnConflicts()` returns `ALLOW`, exempting writes from Accord conflict detection.
   - Comment: "Accord doesn't support reading/writing virtual tables yet" (`VirtualMutation.java:182–189`).
   - **Implication**: Virtual table writes are non-transactional; safe only for idempotent operations (cache invalidation, flag toggles).

6. **LocalPartitioner Requirement**:
   - Virtual tables use `LocalPartitioner` (not token-based hashing).
   - Example: `SystemViewsKeyspace` builds tables with `.partitioner(new LocalPartitioner(...))`.
   - **Implication**: Virtual tables are node-local by definition; no cluster-wide partitioning or replication.

## Answers to Research Questions

### Q1: Freshness (Memtable Exposure to Analytical Readers)

**Status**: NOT SOLVED by Virtual Tables.
- Virtual tables have **zero access** to memtable or in-flight write state.
- No hook exists to wire a virtual table to a `Memtable` / `MemtableIndex`.
- Analytics query on a node through virtual tables + CQL sees ONLY flushed SSTables (→ incomplete snapshot during writes).
- **What's needed** (hypothetical seams):
  - Extend `VirtualTable` API with optional `UnfilteredPartitionIterator selectMemtable(...)` method.
  - Wire `ColumnFamilyStore` to expose `Memtable` reference for query time.
  - Memtable_API (CEP-11) provides pluggable memtable classes but NOT introspection; no "read from memtable" hook.
  - SSTableFormat API (pluggable storage format) similarly silent on analytics reads.

### Q2: Feasibility as Alternative/Adjacent Storage Engine

**Status**: NOT FEASIBLE via Virtual Tables alone.
- Virtual tables are **orthogonal** to storage engines—they read side-channel data, not user tables.
- Use of `LocalPartitioner` + in-memory `DataSet` makes them unsuitable for durable, replicated data.
- Writes are side-effect-only; no recovery log, durability, or Accord integration.
- **Seams that DO exist** (for alternative engines):
  - `SSTableFormat` API (pluggable format, but read-only from storage; `src/java/org/apache/cassandra/io/sstable/format/SSTableFormat.java`).
  - `Memtable` interface (pluggable, CEP-11; `src/java/org/apache/cassandra/db/memtable/Memtable_API.md`).
  - Storage engine lives in `Keyspace`/`ColumnFamilyStore`/`CompactionManager` (NOT modular); tightly coupled to WAL, hints, repair, replication.
- **Alternative storage as sidecar** (feasible via `RemoteToLocalVirtualTable` pattern):
  - An external storage (e.g., CQLite Arrow Flight) could register itself as a "remote peer virtual table."
  - `RemoteToLocalVirtualTable` already demonstrates network-based data fetching for metadata (peers, gossip).
  - Cassandra would see it as a virtual system table, not a replacement engine.

## Trunk vs Cassandra 5.0 Deltas

1. **Accord Integration** (trunk only):
   - Trunk: `VirtualMutation` references Accord conflict tracking; 5.0 lacks this.
   - 5.0 has simpler `IMutation` hierarchy; no Accord consensus.
   - Virtual table writes in 5.0 bypass fewer consensus layers but still non-transactional.

2. **CEP-11 Memtable API** (trunk only):
   - Trunk: Pluggable memtable framework; 5.0 hardcoded skiplist only.
   - No observable impact on virtual tables (both versions exclude memtable reads).

3. **SSTableFormat Pluggability** (trunk only):
   - Trunk: Custom SSTable formats supported.
   - 5.0: Only BIG format (na/nb/oa).
   - Virtual tables unaffected (read neither format natively).

4. **Virtual Keyspaces Registry Behavior**:
   - Trunk: Concurrent registration via `ConcurrentHashMap`; re-registration allowed (e.g., test cleanup).
   - 5.0: Likely identical (simple synchronization; no backward-compat concern).

## Hypotheses / Open Questions

- **Can a virtual table serve analytics without memtable data?** Yes, if analytics tolerate eventual consistency (flushed SSTables only). CQLite's Arrow Flight + Trino connector takes this trade: read-only, consistent snapshot, no replication overhead.
- **Could RemoteToLocalVirtualTable evolve into a full external-storage bridge?** Partially. It fetches remote replica state; extending it to call a sidecar (e.g., Arrow Flight server) is plausible but requires new verb/request handler wiring.
- **Is the Memtable_API missing a read seam by design?** Likely yes. CEP-11 focuses on write-path optimizations (trie, sharding); analytics readers were not in scope. A future "analytic memtable read" feature would need explicit SPI (not present).

## File List (Key Anchors)

- `src/java/org/apache/cassandra/db/virtual/VirtualTable.java` — interface, read ops
- `src/java/org/apache/cassandra/db/virtual/AbstractVirtualTable.java` — base read-only impl, DataSet
- `src/java/org/apache/cassandra/db/virtual/AbstractMutableVirtualTable.java` — write handlers, ColumnValues
- `src/java/org/apache/cassandra/db/virtual/VirtualKeyspace.java` — container, metadata wrapping
- `src/java/org/apache/cassandra/db/virtual/VirtualKeyspaceRegistry.java` — singleton registry
- `src/java/org/apache/cassandra/db/virtual/VirtualMutation.java` — write routing, IMutation impl
- `src/java/org/apache/cassandra/db/virtual/SystemViewsKeyspace.java` — hardcoded system_views tables
- `src/java/org/apache/cassandra/db/virtual/VirtualSchemaKeyspace.java` — hardcoded system_schema
- `src/java/org/apache/cassandra/db/virtual/RemoteToLocalVirtualTable.java` — peer-to-peer data fetch (no memtable)
- `src/java/org/apache/cassandra/service/CassandraDaemon.java:640–651` — registration hook, hardcoded init
- `src/java/org/apache/cassandra/db/SinglePartitionReadCommand.java:1569–1574` — query routing (virtual short-circuit)
- `src/java/org/apache/cassandra/db/PartitionRangeReadCommand.java:651–657` — scan routing (virtual short-circuit)
- `src/java/org/apache/cassandra/db/memtable/Memtable_API.md` — CEP-11 pluggable memtable (no read hook)
- `src/java/org/apache/cassandra/io/sstable/format/SSTableFormat.java` — pluggable format (storage-layer only)
