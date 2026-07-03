# TCM, Journal, and Accord: Storage Engine Seams in Apache Cassandra Trunk

## Summary

Apache Cassandra trunk (7.0-dev, post-6.0 merge) introduces two log-structured subsystems that establish storage patterns but do **not** constitute second storage engines in the traditional sense. (1) **TCM (Transactional Cluster Metadata)** maintains cluster topology and schema via a distributed consensus log persisted in the system keyspace. (2) **Accord** adds a transaction coordinator with its own **journal** (separate from Cassandra's commit log) for transaction state, using Cassandra's standard SSTable format for segment persistence. Both systems reinforce the "data = memtable + SSTables" visibility model. Query engines see only flushed SSTables; memtable contents are **not** exposed to external readers (e.g., Arrow Flight). For freshness (Q1), an external analytical reader must either force a flush or be extended to query the live memtable via a new seam. For alternative engines (Q2), pluggability exists for memtable and SSTableFormat, but ColumnFamilyStore's lifecycle ownership and the assumed-separate commit log create coupling; full replacement is harder than extension.

## Key Classes (file:line anchors)

**Cluster Metadata:**
- `ClusterMetadata` (src/java/org/apache/cassandra/tcm/ClusterMetadata.java:97) — Immutable snapshot of cluster state: memberships, token ranges, schema, Accord fast-path status.
- `ClusterMetadataService` (src/java/org/apache/cassandra/tcm/ClusterMetadataService.java:106) — Singleton orchestrator of TCM state machine; maintains Processor (consensus impl), LocalLog, MetadataSnapshots.
- `Processor` (src/java/org/apache/cassandra/tcm/Processor.java:27) — Interface: `commit(Entry.Id, Transformation, Epoch, Retry)` and `fetchLogAndWait(Epoch, Retry)`. Abstracts log consensus (Paxos vs. PaxosBacked vs. local).
- `AbstractLocalProcessor` (src/java/org/apache/cassandra/tcm/AbstractLocalProcessor.java:38) — Base impl; stores LocalLog; applies transformations to derive new ClusterMetadata.

**TCM Log & Storage:**
- `LocalLog` (src/java/org/apache/cassandra/tcm/log/LocalLog.java) — Client-side in-memory log of entries up to highest fetched epoch; watchers for catch-up.
- `LogStorage` (src/java/org/apache/cassandra/tcm/log/LogStorage.java:25) — Interface: append(), getLogState(), snapshots(). Implementations: `SystemKeyspaceStorage` (default, system keyspace table), `NoOpLogStorage` (in-memory only).
- `Entry` (src/java/org/apache/cassandra/tcm/log/Entry.java) — TCM log entry: Transformation + metadata.

**Generic Journal (used by Accord):**
- `Journal<K,V>` (src/java/org/apache/cassandra/journal/Journal.java:93) — Append-only log with key-based lookup, invalidation, compaction. Type params: K (key), V (record).
- `Segments` (src/java/org/apache/cassandra/journal/Segments.java) — Multi-segment indexed store; active segment + static segments.
- `Flusher` / `Compactor` (src/java/org/apache/cassandra/journal/Flusher.java:14, Compactor.java) — Background I/O workers; callbacks for durability checkpoints.

**Accord Transaction Journal:**
- `AccordJournal` (src/java/org/apache/cassandra/service/accord/journal/AccordJournal.java:91) — Implements `accord.api.Journal`; wraps `Journal<JournalKey, Object>`. Stores command changes, replayed on recovery.
- `CommandChanges` (src/java/org/apache/cassandra/service/accord/journal/CommandChanges.java:33) — Encodes Accord command state deltas for journal.
- `Replay` (src/java/org/apache/cassandra/service/accord/journal/Replay.java) — Replays journal entries during boot; ranges caught up from CMS log too.

**Pluggable Memtable (CEP-11, trunk):**
- `Memtable` (src/java/org/apache/cassandra/db/memtable/Memtable.java:60) — Interface: put(), rowIterator(), partitionIterator(), flush operations, lifecycle. Factory pattern for construction.
- `Memtable.Factory` (Memtable.java:77) — plugs into yaml config `memtable.configurations.<name>.class_name`; supports `writesAreDurable()` flag.

**Pluggable SSTableFormat (pre-trunk, but extended):**
- `SSTableFormat<R,W>` (src/java/org/apache/cassandra/io/sstable/format/SSTableFormat.java:45) — Interface: reader/writer factories, component metadata, validation.
- `SSTableReaderFactory` / `SSTableWriterFactory` (SSTableFormat.java:107–135) — Builder pattern; resource management on build().

**Listeners & Callbacks (extension points):**
- `ChangeListener` (src/java/org/apache/cassandra/tcm/listeners/ChangeListener.java) — Notified on ClusterMetadata updates (schema, topology, keyspaces).
- `LogListener` (src/java/org/apache/cassandra/tcm/listeners/LogListener.java) — Notified when TCM log entries are fetched; allows monitoring.
- `MetadataSnapshotListener` (src/java/org/apache/cassandra/tcm/listeners/MetadataSnapshotListener.java) — Triggered on snapshot checkpoints.

## Extension Points / Pluggability Seams

1. **Processor replacement** — Multiple consensus impls (AtomicLongBackedProcessor, PaxosBackedProcessor) via factory pattern. CMS nodes run Paxos; others use client mode.

2. **LogStorage backend** — Swap SystemKeyspaceStorage for custom impl (e.g., etcd, Consul) **if** DDL/DML is isolated into an interface. Current: tightly coupled to Cassandra schema + keyspace creation.

3. **Memtable factory** — CEP-11 fully pluggable. Config-driven instantiation: YAML specifies class + parameters. Implementations can opt-in to durability via `writesAreDurable()`.

4. **SSTableFormat & reader/writer** — Abstract factories allow alternative formats (e.g., Iceberg, Parquet). Components and versions discoverable via interface.

5. **Journal compactor/segment management** — Custom compactor impl (Compactor interface); Accord overrides with CommandChangeWriter for transaction-aware merge.

6. **TCM listeners** — Hooks for reactive behavior: new schema listeners, topology listeners, snapshot listeners. Used by Accord topology integration.

## Hard Couplings

1. **ColumnFamilyStore owns both memtable and SSTables** (src/java/org/apache/cassandra/db/ColumnFamilyStore.java:187). Query engine aggregates both. **External readers (Arrow Flight, CQLite) see only SSTables** because memtables aren't exported by the SSTableReader interface. **→ Q1 impact:** memtable data invisible to analytical reads.

2. **Commit log is separate, durability optional** — ColumnFamilyStore writes to commit log + memtable. Memtable can provide its own durability (CEP-11 flag), but commit log is **assumed to exist and be recoverable** as independent storage. **→ Alternative engine must handle durability separately.**

3. **TCM log uses system keyspace** (SystemKeyspaceStorage) — Metadata entries stored in `system.metadata_log` table. Requires working DDL/DML to create/manage the table. **Cannot replace with external log without modifying initialization logic.**

4. **Accord journal tied to AccordKeyspace** (AccordJournal.java:104-105) — Persists to `accord_system.journal` table. Range search indices via secondary indexes (SAI). **Couples Accord durability to Cassandra's sstable-based storage.**

5. **Memtable flush → SSTableWriter.Builder** — Lifecycle is non-negotiable: memtable fills, CFS triggers flush, Builder opens resources, writes, releases. No option to redirect writes to alternative storage.

6. **Recovery paths assume commit log + SSTables** — Startup replays commit log, loads SSTable indexes, applies TCM + Accord journal. Alternative formats require override at multiple points (CommitLogReplayer, SSTableLoader).

## Q1: Freshness (Memtable + SSTable Visibility)

**Current state:** Arrow Flight connector reads SSTables only. A fresh write lands in the memtable and becomes visible to clients (via local read), but an analytical reader on the same node sees stale data until flush.

**Where the gap is:**
- `ColumnFamilyStore.getAllIterables(ColumnFilter, ...)` returns memtable + SSTable readers (src/java/org/apache/cassandra/db/ColumnFamilyStore.java ~line 2400s).
- `SSTableReader` is the external contract exposed via `iter()` and range queries.
- Memtables are **internal to CFS**; no seam to export them via SSTableReader interface.

**Options to bridge (in priority order):**

a) **Extend CFS to export memtable + SSTable union** — Add a new method `getUnfilteredPartitionIterator(ColumnFilter, Epoch)` that aggregates live memtable + current SSTable snapshot. Requires holding CFS lock during iteration to prevent memtable flush mid-scan. ✓ Seam exists: CFS.Owner, Memtable.Owner interfaces. ✗ API surface expansion.

b) **Force flush before read** — Arrow Flight checks if stale data is acceptable; if not, calls `CFS.forceBlockingFlush()` before read. Simple, but kills analytical latency.

c) **Read-your-writes with timestamp bounds** — External reader passes `queryAt={timestamp}` filter; CFS skips memtable, returns SSTable snapshot as-of that timestamp (requires MVCC or commit log tail). Not yet wired.

**Trunk-vs-5.0:** No change in 5.0; same memtable/SSTable boundary exists.

## Q2: Feasibility of Alternative Storage Engine

### (a) In-JVM replacement (CQLite as Cassandra storage engine)

**Feasibility: Moderate difficulty.** Pluggable seams exist for memtable + SSTableFormat, but orchestration is hardcoded in CFS.

- ✓ Can replace Memtable.Factory → custom in-memory structure tied to CQLite write path.
- ✓ Can implement SSTableFormat → CQLite reads from SSTables; CQLite writes via SSTableWriter.
- ✗ **ColumnFamilyStore lifecycle not pluggable** — CFS creates/switches memtables, manages flush cycles, schedules compaction. Cannot redirect to CQLite's flush logic without subclassing CFS (fragile, upgrade-hostile).
- ✗ **Commit log assumed** — Cassandra writes to commit log even if memtable is durable. Recovery replays commit log + sstables. CQLite would need to participate in commit log protocol or disable it (loses CDC/PITR).
- ✗ **Compaction & STCS scheduling hardcoded** — CFS owns compaction strategy selection; not a Processor-like abstraction.

**Hypothesis:** Practical approach = "plugin memtable + custom compaction" not "replace engine wholesale."

### (b) Adjacent OLAP engine (CQLite as read-only sidecar)

**Feasibility: High.** Exploit SSTableFormat seam + Accord journal seam.

- ✓ CQLite reads SSTables directly via custom SSTableFormat reader (already done in CQLite).
- ✓ CQLite replays Accord journal from sidecar filesystem to track transaction state.
- ✓ Arrow Flight on CQLite node aggregates SSTables + reads from Accord journal to derive visible state.
- ✗ Still blind to live memtable (same as core Cassandra Arrow Flight).
- ✗ Accord journal on disk is only durably persisted after flush; live Accord command state lives in Cassandra's memtable-like structure (CommandStore in-memory).

**Hypothesis:** Viable for historical + recently-flushed data; real-time reads still require flush-to-SSTables.

## Trunk-vs-5.0 Deltas

| Feature | 5.0 | Trunk | Impact |
|---------|-----|-------|--------|
| **CEP-11: Pluggable Memtable API** | No (fixed SkipList) | Yes (SkipList/ShardedSkipList/Trie + custom) | Q2: enables custom memtable impl |
| **Accord Journal** | No (commit log only) | Yes (separate journal for transactions) | Q1/Q2: new log-structured store, but doesn't solve freshness |
| **TCM (Cluster Metadata)** | Partial (gossip-based) | Full (consensus log in system.metadata_log) | Q2: metadata persistence now log-based, not gossip-fragile |
| **SSTableFormat interface** | Yes (md5/nb/oa formats) | Yes (same + 'da' BTI) | Baseline; no pluggability change |

## Conclusions

**For Q1 (freshness):** Accord journal + TCM log do not solve memtable visibility. The seam must be at ColumnFamilyStore level (extend to export memtable union) or application level (force flush / read-your-writes discipline). Trunk has no new machinery here.

**For Q2 (feasibility):**
- **Alternative memtable:** ✓ Pluggable end-to-end (CEP-11, trunk).
- **Alternative SSTableFormat:** ✓ Reader/writer factories decouple implementation.
- **Full storage engine replacement:** ✗ ColumnFamilyStore lifecycle + commit log + recovery path coupling too tight; practical only as "custom memtable + standard compaction."
- **Read-only OLAP sidecar:** ✓ Viable via SSTableFormat + Accord journal read seams; limited to flushed data.

**Recommended scoping for CQLite:** Position as "alternative SSTableFormat + read-only OLAP query engine over SSTables + Accord journal," not as "replacement storage engine." Freshness requires upstream Cassandra work (expose memtable union) or application-side flush discipline.
