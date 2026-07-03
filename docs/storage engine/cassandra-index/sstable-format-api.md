# SSTable Format API — Pluggability Indexer

**Subsystem**: Apache Cassandra SSTableFormat pluggable API (trunk)  
**Scope**: `src/java/org/apache/cassandra/io/sstable/format/`  
**Key docs**: cassandra.yaml §1190–1214 (format configuration)

---

## Summary

Cassandra (trunk, 5.0+) uses a pluggable SSTableFormat API (SPI) to support alternative storage formats. The mechanism is:
- **Discovery**: `ServiceLoader<SSTableFormat.Factory>` loaded at startup (DatabaseDescriptor:1976)
- **Selection**: cassandra.yaml `sstable.selected_format` configures which format writes new SSTables
- **Supported formats**: `big` (legacy, available since 3.0) and `bti` (trie-indexed, introduced 5.0 via CEP-25)
- **Contract**: Each format implements SSTableFormat<R extends SSTableReader, W extends SSTableWriter> with reader/writer factories, component metadata, verification, scrubbing, and deletion

**Critical for Q1 (freshness)**: The API has **NO memtable snapshot hook**—only flushed SSTables are discoverable. For analytical reads to see live memtable + SSTable state, Cassandra would need a new seam in Flushing.FlushRunnable (line ~103 in Flushing.java) or a real-time read path that queries both. Trunk does not export this.

**Critical for Q2 (alternative engine feasibility)**: The SSTableFormat API is **format-only**; it does NOT encompass partitioner, replication, or lifecycle. A CQLite-like engine could read via a custom format implementation, but write would require wrapping SSTableWriter.Builder, and both would remain embedded in Cassandra's schema/memtable/compaction layers.

---

## Key Classes & Interfaces

### Core API

| Class | Responsibility | File:Line |
|-------|---|---|
| `SSTableFormat<R, W>` | Format contract: reader/writer factories, components, scrubber, deletion | format/SSTableFormat.java:45 |
| `SSTableFormat.Factory` | ServiceLoader-discoverable factory (name + getInstance) | format/SSTableFormat.java:196 |
| `AbstractSSTableFormat<R, W>` | Base class; stores name + options | format/AbstractSSTableFormat.java:24 |
| `SSTableFormat.SSTableReaderFactory<R, B>` | Creates SSTableReader instances; loadingBuilder + key range | format/SSTableFormat.java:107 |
| `SSTableFormat.SSTableWriterFactory<W, B>` | Creates SSTableWriter instances; size estimation | format/SSTableFormat.java:135 |
| `Version` | Abstract version descriptor; gates features via boolean flags (e.g., hasCommitLogLowerBound) | format/Version.java:34 |
| `Component.Type` | File component enum (DATA, INDEX, STATS, FILTER, DIGEST, CRC, TOC, COMPRESSION_INFO, CUSTOM) | (defined in SSTableFormat.Components.Types:154–174) |

### BigFormat (Legacy, 3.0+)

| Class | Responsibility | File:Line |
|-------|---|---|
| `BigFormat` | Extends AbstractSSTableFormat; manages big-format lifecycle | format/big/BigFormat.java:164 |
| `BigTableReaderFactory` | Opens BigTableReader + reads key ranges | format/big/BigTableReader.java |
| `BigTableReader` | Reads via Index.db (sampling) + Data.db; uses RowIndexEntry | format/big/BigTableReader.java |
| `BigTableWriter` | Writes sorted partitions to Data.db; builds Index.db in parallel | format/big/BigTableWriter.java |
| `BigVersion` | Versions: `ca`–`ob` (2.0–5.1); gates format evolution | format/big/BigVersion.java |

### BtiFormat (Trie-indexed, 5.0+)

| Class | Responsibility | File:Line |
|-------|---|---|
| `BtiFormat` | Extends AbstractSSTableFormat; manages bti-format lifecycle | format/bti/BtiFormat.java |
| `BtiTableReaderFactory` | Opens BtiTableReader; reads key ranges via PartitionIndex | format/bti/BtiTableReaderFactory.java |
| `BtiTableReader` | Reads via PartitionIndex (trie) + Data.db; TrieIndexEntry lookups | format/bti/BtiTableReader.java |
| `BtiTableWriter` | Writes sorted partitions; builds trie index on close | format/bti/BtiTableWriter.java |
| `BtiVersion` | Versions: `da`–`db`; all version gates in bti/BtiVersion.java | format/bti/BtiVersion.java |

### Shared Components

| Class | Responsibility | File:Line |
|-------|---|---|
| `SortedTableWriter` | Abstract sorted writer; partition iteration + observers | format/SortedTableWriter.java |
| `SortedTableScrubber` | Abstract scrubber; delegates to format-specific impl | format/SortedTableScrubber.java |
| `SortedTableVerifier` | Verifies structural integrity (checksums, keys, offsets) | format/SortedTableVerifier.java |
| `DataComponent` | Codec for Data.db; delegates compression to format | format/DataComponent.java |
| `StatsComponent` | Codec for Statistics.db (encoding stats, min/max, boundaries) | format/StatsComponent.java |

---

## Extension Points / Pluggability Seams

### 1. **Format Registration (ServiceLoader)**
   - **Trigger**: DatabaseDescriptor.applySSTableFormats() (config/DatabaseDescriptor.java:1974–1981)
   - **Mechanism**: `ServiceLoader<SSTableFormat.Factory> loader = ServiceLoader.load(SSTableFormat.Factory.class, DatabaseDescriptor.class.getClassLoader())`
   - **Config**: cassandra.yaml `sstable.format.<name>` (options map)
   - **Fallback**: If no loaders found, defaults to BigFormat.BigFormatFactory()
   - **Result**: Immutable `sstableFormats: Map<String, SSTableFormat>` + `selectedSSTableFormat` (DatabaseDescriptor.java:6102)
   - **Q2 relevance**: **Custom format = new Factory.getInstance()** can be injected via classpath (no code change needed), but CQLite would need to parse Cassandra's binary row format (schema-aware decoding from Data.db).

### 2. **Reader Creation (Builder Pattern)**
   - **Entry**: SSTableFormat.SSTableReaderFactory.loadingBuilder(descriptor, metadata, components)
   - **Contract**: Must open all resources (Index.db, Data.db, STATS, COMPRESSION_INFO, etc.) and return SSTableReader
   - **Resource cleanup**: On failure, builder must close all opened handles
   - **Q1 relevance**: Reader only opens flushed SSTable files; no memtable hook.

### 3. **Writer Creation (Builder Pattern)**
   - **Entry**: SSTableFormat.SSTableWriterFactory.builder(descriptor) + ILifecycleTransaction
   - **Contract**: Must accept sorted partitions (via startPartition/append/finish) and flush to disk atomically
   - **Observers**: Writers notify Index.Group.getFlushObserver() for secondary index writes (SSTableWriter.java:129–137)
   - **Transaction**: Lifecycle tracks file creation; failure rolls back via txn.abort()

### 4. **Component Declaration**
   - **Method**: SSTableFormat.allComponents() → Set<Component>
   - **Subset methods**: primaryComponents(), generatedOnLoadComponents(), mutableComponents(), uploadComponents(), batchComponents()
   - **Default components**: All formats declare DATA, STATS, COMPRESSION_INFO, DIGEST, CRC, TOC
   - **Format-specific**: BIG adds PRIMARY_INDEX + SUMMARY; BTI adds PARTITION_INDEX + PARTITION_SUMMARY
   - **CUSTOM**: Extensible via Component.Type.create("CUSTOM", null, true, null) for format-specific or strategy-specific metadata

### 5. **Versioning & Feature Gating**
   - **Entry**: SSTableFormat.getVersion(String versionString) → Version
   - **Method**: Version.isCompatible() / isCompatibleForStreaming() (virtual methods per format)
   - **Flags**: Each Version subclass implements ~15 boolean flags gating features (e.g., hasCommitLogLowerBound, hasUIntDeletionTime, hasTokenSpaceCoverage)
   - **Example**: BigVersion.ob gates `hasMetadataChecksum = true` but `hasUIntDeletionTime = false`; BtiVersion.da gates both true
   - **Q2 note**: Version gates are **READ-time only**; write always uses selectedSSTableFormat.getLatestVersion()

### 6. **Scrubbing & Verification**
   - **Entry**: SSTableFormat.getScrubber() → IScrubber
   - **Contract**: IScrubber.scrub() validates + repairs (or deletes) corrupt partitions
   - **Entry**: IVerifier.verify() / IMetadataVerifier.verify() (SortedTableVerifier)
   - **Format-specific**: BIG uses RowIndexEntry; BTI uses TrieIndexEntry; both check checksums, bloom filter, key ordering

### 7. **Deletion & Cache Invalidation**
   - **Entry**: SSTableFormat.delete(Descriptor) / deleteOrphanedComponents()
   - **Contract**: Must delete all files + invalidate in-memory resources (key cache entries, index summary)
   - **Example**: BigFormat.delete() iterates KeyCacheKey, removes matching entries (BigFormat.java:343–348)

---

## Hard Couplings (Constraints on Alternative Engines)

### 1. **Cassandra Row Format Coupling**
   - SSTableReader/Writer are coupled to Cassandra's UnfilteredRowIterator abstraction (db/rows/)
   - Data.db encodes partitions as: partition-key-header | static-row | clustered-rows | range-tombstones
   - Encoding uses Cassandra's varint/serialization context (schema-aware variable-width encoding)
   - **Impact**: Alternative storage engine must either:
     a. Decode Cassandra rows natively (parsing the binary format)
     b. Use Cassandra's own deserialization (SchemaKeyspace dependency)
     c. Bypass the format layer entirely (fork Cassandra or use a sidecar)

### 2. **Metadata Coupling**
   - All readers require TableMetadataRef (schema introspection at open time)
   - Version.correspondingMessagingVersion() ties SSTable version to Cassandra release (e.g., big.nb → MessagingService.VERSION_50)
   - Statistics.db requires EncodingStats (min timestamp, min TTL, clustered min/max keys) — schema-aware encoding
   - **Impact**: Cannot read SSTables without Cassandra's schema service; CQLite must inject or bypass.

### 3. **Lifecycle Transaction Coupling**
   - All writers accept ILifecycleTransaction (db/lifecycle/) — tracks in-flight SSTable creation + rollback
   - Descriptor.isTemporary flag gate whether writes use .tmp.* or final names
   - Transaction.trackNew() registers SSTable before any writes; failure aborts + deletes files
   - **Impact**: Alternative writer must either integrate with Cassandra's lifecycle or manage its own transactional state.

### 4. **Key Cache Coupling**
   - KeyCacheValueSerializer<R, T extends AbstractRowIndexEntry> required for each format
   - BigFormat uses RowIndexEntry; BTI uses TrieIndexEntry; both serialized to key cache
   - Cache invalidation on delete is format-specific (e.g., BigFormat iterates KeyCacheKey to find matches)
   - **Impact**: Alternative format must provide its own cache serialization or accept cache bypass.

### 5. **Component File Naming**
   - Descriptor.fileFor(Component) → File uses Component.dbName (e.g., "Data.db", "Index.db")
   - All files share prefix: `<keyspace>-<table>-<uuid>-<version>-<component>.<ext>`
   - TOC.txt lists all components for compaction/streaming
   - **Impact**: Cannot use alternative naming schemes; file discovery is Descriptor-driven.

### 6. **Partitioner Coupling**
   - IPartitioner (DHT) is passed to format at open time (SSTableReader.Builder)
   - Key ranges (readKeyRange) must respect partitioner's token order for streaming/compaction
   - **Impact**: Format is partitioner-agnostic but assumes partitioner-ordered keys on disk.

### 7. **Compression Coupling**
   - CompressionInfoComponent (if present) encodes LZ4/Snappy/Deflate/Zstd chunk metadata
   - Writer.setCompressionDictionaryManager() injects compression strategy
   - Reader opens CompressionMetadata to decompress Data.db chunks
   - **Impact**: Alternative compression schemes require new CompressionInfoComponent subclass.

### 8. **Memtable Write Path is Opaque**
   - Format factories only see sorted partitions via Flushing.FlushRunnable (db/memtable/Flushing.java)
   - No hook for "capture memtable state before flush" → analytical reads see only flushed SSTables
   - WAL (CommitLog) is separate; cannot reconstruct live memtable state from format layer
   - **Q1 critical**: This is the main blocker for "all node-local state" freshness goal.

---

## Q1 & Q2 Relevance

### Q1: **When DataFusion/Trino reads a node via CQLite's Arrow Flight connector, how to see memtable + SSTable state?**

**Current state**: Only flushed SSTables are visible to the format API.
- Memtable state is held in JVM memory (Memtable objects in ColumnFamilyStore.memtable + all.memtables)
- Flushing.FlushRunnable (db/memtable/Flushing.java) writes sorted partitions → SSTableWriter → disk
- **No seam exists** to snapshot memtable before flush or to expose unflushed partitions to readers

**Required changes in Cassandra** (trunk):
1. **Option A**: Add memtable snapshot hook
   - Add SSTableFormat.getMemtableSnapshot() → PartitionIterator
   - Call from ColumnFamilyStore or a new AnalyticalRead path
   - Cost: ~500 LOC, but couples format API to memtable layer (violates SoC)

2. **Option B**: CDC-style real-time export
   - Extend delta-scan (CommitLogManager + CDC) to replay pending mutations in-order
   - CQLite calls out-of-band API; Cassandra streams mutations since last SSTable flush
   - Cost: Requires CDC-like event registry + replay guarantees; already exists but needs export

3. **Option C**: Sidecar memtable monitor
   - External process attaches to CFS.memtable via reflection or JMX
   - Polls memtable state, merges with SSTable reads
   - Cost: ~200 LOC sidecar; fragile (GC pauses, memtable switches)

**Decision**: **Option B is only safe path** (event-driven). Memtable is mutable; Option C races; Option A couples core API.

---

### Q2: **How feasible is CQLite as (a) alternative/replacement storage engine, or (b) adjacent OLAP engine?**

**Feasibility: LOW for (a), MEDIUM for (b).**

#### (a) Alternative storage engine inside Cassandra

**Blockers**:
- SSTableFormat only handles **formatted data disk access**, not:
  - Replication strategy (replication_factor, token assignment)
  - Compaction strategy (STCS, LCS, UCS)
  - Consistency model (quorum reads, read-repair)
  - Memtable buffer & flush triggers
  - CommitLog + recovery
- To be a true storage engine, CQLite would need to replace ~40,000 LOC (CompactionStrategy, ColumnFamilyStore, Keyspace, ReplicationStrategy, ReadCommand.execute, etc.)
- Format layer is **only 5,000 LOC** of that

**Verdict**: CQLite-as-engine is **infeasible without forking Cassandra**. The seams don't extend to the storage engine layer; they only extend to format.

#### (b) Adjacent OLAP engine (read-only, parallel node scans)

**Feasibility: MEDIUM–HIGH.**

**Path**:
1. **Implement SSTableFormat (hard)**: CQLite must
   - Subclass AbstractSSTableFormat<CQLiteReader, CQLiteWriter>
   - Implement loadingBuilder() to open native binary parsers (Rust/JNI)
   - Parse Cassandra's Data.db, Index.db, STATS binary (schema-aware)
   - Provide Version gating (claim BIG/BTI versions as "readable")

2. **Register as Factory**: Via ServiceLoader in cqlite-java module
   - META-INF/services/org.apache.cassandra.io.sstable.format.SSTableFormat$Factory
   - Java wrapper calls native code (JNI or GraalVM native image)

3. **Limitations**:
   - Cannot write (SSTableWriter stub throws UnsupportedOperationException)
   - Only reads flushed SSTables (memtable state invisible)
   - Inherits Cassandra's schema/partitioner/metadata coupling (must call CassandraServer.Schema or parse cassandra.yaml)
   - No auto-compaction (CQLite sees static SSTable set at open time)

**Cost to integrate**:
- Format API implementation: ~1,500 LOC (Java wrapper)
- Binary format parsing: Already done (CQLite core, ~10k LOC)
- Testing: Parity against sstabledump for all 33 test tables
- CI: ~4 weeks for a beta (Q4 2026?)

**Advantages**:
- Cassandra core unchanged (no fork)
- Works with existing Cassandra deployments (no schema migration)
- Can serve Trino/DataFusion via Arrow Flight (independent process)
- Lineage: CQLite → SSTableFormat.Factory → Cassandra's query planner

**Risks**:
- BigFormat.java:343–348 invalidates key cache on reader construction — CQLite reader might hold stale cache entries
- VersionedSSTableFormat.correspondingMessagingVersion() must be gated (CQLite doesn't support streaming)
- Metadata.StatsMetadata min/max are **schema-required** for read correctness; CQLite must parse schema at load

---

## Trunk vs. 5.0 Deltas

| Feature | 5.0 | Trunk | Impact |
|---------|-----|-------|--------|
| **SSTableFormat API** | ✅ (core) | ✅ (same) | No change; API stable since 5.0 |
| **BTI Format** | ✅ (CEP-25, f16fb67) | ✅ | Both big + bti available; bti default from 5.1 onward |
| **Memtable API** | Pluggable (CEP-11) | ✅ Enhanced | CEP-11 landed in 5.0; trunk adds TrieMemtable (skip-list alternative) |
| **Format feature gating** | Via Version boolean flags | ✅ Same | Version.isCompatible() unchanged since 5.0 |
| **ServiceLoader discovery** | ✅ | ✅ | No change |
| **cassandra.yaml sstable section** | Lines 1190–1214 | ✅ Same | Configuration stable |
| **Key cache serialization** | BigFormat.KeyCacheValueSerializer | ✅ Same | BtiFormat adds BtiFormat.KeyCacheValueSerializer; no breaking changes |
| **Lifecycle transaction coupling** | ✅ ILifecycleTransaction | ✅ | No change; writers always require transaction |
| **Real-time memtable snapshot** | ❌ Not available | ❌ Not available | **Q1 blocker still unresolved** |

**Conclusion**: SSTableFormat API is **stable & backward-compatible** between 5.0 and trunk. No version gating needed for a format implementation targeting 5.0+.

---

## Hypotheses / Out-of-Scope

1. **Multi-format SSTable sets**: Can a single SSTable read mixed big + bti versions?
   - Yes; Descriptor.version → SSTableFormat.getVersion() is per-file, not per-format
   - Compaction pulls readers from different versions; no conflict

2. **Format hot-swap during read-only mode**:
   - Safe if new format is backward-compatible (reads old versions)
   - Cassandra validates readability via Version.isCompatibleForStreaming()

3. **CQLite + Cassandra data consistency**:
   - CQLite sees what's on disk (eventual consistency model)
   - Reads within 1 flush-cycle of last write; no strict serializability

---

## Conclusion

The SSTableFormat API is **minimal but sufficient** for read-only analytics engines (path Q2b). It is **insufficient** for storage engine replacement (path Q2a) due to lack of replication/compaction/consistency layer seams. **Q1 (freshness)** requires changes outside the format layer: either CDC-style event export or a memtable snapshot hook (both missing in trunk).

