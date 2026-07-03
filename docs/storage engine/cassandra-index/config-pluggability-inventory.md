# Cassandra Trunk Pluggability Inventory

## Summary

Cassandra trunk (post-6.0) provides multiple pluggability seams for storage-related components through YAML configuration and Java ServiceLoader mechanisms. **Key pluggable surfaces**: memtable implementations (CEP-11), SSTable formats (CEP-17), compressors, compression providers, compaction strategies, authenticators, and disk access patterns. **Critical limitation for Q1 (analytical freshness)**: no pluggability hook into the read-before-flush coordination—reads always see only flushed SSTables, not live memtable state. **For Q2 (alternative engine feasibility)**: the architecture assumes hardcoded lifecycle coupling (flush→compact→delete) and Java/Class.forName bootstrapping, not language-agnostic seams.

---

## Key Classes & Extension Points

### Memtable (CEP-11 Pluggable API)

**File**: `src/java/org/apache/cassandra/db/memtable/Memtable_API.md` (design)  
**Config**: `src/java/org/apache/cassandra/config/Config.java:190-192` (MemtableOptions)  
**Schema**: `src/java/org/apache/cassandra/schema/MemtableParams.java:51-258`

- **Interface**: `Memtable.Factory` — required entry point; implementations provide `factory(Map<String, String>)` method or static `FACTORY` field
- **Configuration**: `cassandra.yaml:818+` → memtable.configurations.{name}.class_name, parameters
- **Per-table override**: TableParams.MEMTABLE (CQL: `CREATE/ALTER TABLE ... WITH memtable = 'config_name'`)
- **Implementations provided**:
  - `SkipListMemtable` (legacy, default)
  - `ShardedSkipListMemtable` (shards parameter, parallelism; `src/java/org/apache/cassandra/db/memtable/ShardedSkipListMemtable.java`)
  - `TrieMemtable` (trie-based, off-heap friendly; `src/java/org/apache/cassandra/db/memtable/TrieMemtable.java`)
- **Extension mechanism**: Class.forName + reflection (method or field lookup); custom implementations packaged alongside, no ServiceLoader
- **Trunk-vs-5.0**: CEP-11 shipped in trunk; 5.0.x has single SkipListMemtable only

---

### SSTable Format (CEP-17 Pluggable API)

**File**: `src/java/org/apache/cassandra/io/sstable/SSTable_API.md` (design)  
**Config**: `src/java/org/apache/cassandra/config/Config.java:157-159` (SSTableConfig)  
**Format interface**: `src/java/org/apache/cassandra/io/sstable/format/SSTableFormat.java:45-150`  
**Bootstrap**: `src/java/org/apache/cassandra/config/DatabaseDescriptor.java` (ServiceLoader discovery + selected_format config)

- **Interface**: `SSTableFormat<R extends SSTableReader, W extends SSTableWriter>` + `SSTableFormat.Factory`
- **ServiceLoader discovery**: Java SPI mechanism discovers all `SSTableFormat.Factory` implementations on classpath
- **Configuration**:
  - `cassandra.yaml:sstable.selected_format` — default format for writes (default: `big`)
  - `cassandra.yaml:sstable.format.{format_name}.{param}` — format-specific parameters
  - Per-table selection: currently not in scope (all tables of a keyspace write same format)
- **Implementations provided**:
  - `BigFormat` (legacy, na/nb versions; `src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java:70+`)
  - `BtiFormat` (BTI, da versions, index structure optimized for analytical reads; `src/java/org/apache/cassandra/io/sstable/format/bti/`)
- **Component system**: `SSTableFormat.Components` registry; each format declares Components (DATA, SUMMARY, PRIMARY_INDEX, etc.)
- **Trunk-vs-5.0**: CEP-17 shipped in trunk; 5.0.x has BigFormat only

---

### Compressor & Compression Provider

**Files**:
- `src/java/org/apache/cassandra/config/Config.java:92` (compressor_providers map, ParameterizedClass)
- `src/java/org/apache/cassandra/config/Config.java:172` (commitlog_compression, ParameterizedClass)
- `src/java/org/apache/cassandra/io/compress/AbstractCompressionProvider.java:29-80`
- `src/java/org/apache/cassandra/io/compress/CompressorRegistry.java`

- **Provider interface**: `AbstractCompressionProvider` — loadable via ServiceLoader or explicit class_name in yaml
- **Configuration**:
  - `cassandra.yaml:compressor_providers` — map of provider name → {class_name, parameters}
  - `cassandra.yaml:commitlog_compression` — ParameterizedClass for commit log compression
  - `cassandra.yaml:hints_compression` — ParameterizedClass for hints compression (line 125-128 in cassandra.yaml)
  - Per-table: `TableParams.CompressionParams` (CQL: `WITH compression = {'sstable_compression': 'LZ4Compressor', 'chunk_length_kb': 64}`)
- **Built-in compressors**: LZ4Compressor, SnappyCompressor, DeflateCompressor, ZstdCompressor, NoopCompressor
- **Extension**: Class.forName + parameters map passed to init()
- **Trunk-vs-5.0**: compression provider architecture same; trunk expands configuration model

---

### Compaction Strategy

**File**: `src/java/org/apache/cassandra/schema/CompactionParams.java:49-150`

- **Interface**: `AbstractCompactionStrategy` — per-table configuration
- **Configuration**: `TableParams.COMPACTION` (CQL: `WITH compaction = {'class': 'SizeTieredCompactionStrategy', ...}`)
- **Built-in strategies**:
  - `SizeTieredCompactionStrategy` (default for legacy tables)
  - `LeveledCompactionStrategy`
  - `TimeWindowCompactionStrategy`
  - `UnifiedCompactionStrategy` (CASSANDRA-15265, newer unified approach)
- **Global default**: `cassandra.yaml:default_compaction` (ParameterizedClass; `src/java/org/apache/cassandra/config/Config.java:178`)
- **Extension**: Class.forName + reflection
- **Cluster scope**: compaction strategy cannot change without rolling restart; schema change broadcasts to cluster, fallback on mismatch

---

### Authenticator, Authorizer, Role Manager

**Files**: `src/java/org/apache/cassandra/config/Config.java:86-91`

- **authenticator**: ParameterizedClass (default: AllowAllAuthenticator)
  - Interface: `IAuthenticator` (custom login flow)
  - Implementations: AllowAllAuthenticator, PasswordAuthenticator, MutualTlsAuthenticator
  - `cassandra.yaml:authenticator.class_name` + parameters
- **authorizer**: ParameterizedClass (default: AllowAllAuthorizer)
  - Interface: `IAuthorizer` (permission checks)
  - Implementations: AllowAllAuthorizer, CassandraAuthorizer
- **role_manager**: ParameterizedClass (default: CassandraRoleManager)
  - Interface: `IRoleManager` (user/role management)
- **network_authorizer**: ParameterizedClass (default: AllowAllNetworkAuthorizer)
  - Interface: `INetworkAuthorizer` (datacenter access control)
- **cidr_authorizer**: ParameterizedClass (default: AllowAllCIDRAuthorizer)
  - Interface: `ICIDRAuthorizer` (CIDR-based access control)
- **Extension**: Class.forName + optional parameters map
- **Scope**: cluster-wide; must agree on all nodes

---

### Partitioner

**File**: `src/java/org/apache/cassandra/config/Config.java:161` (String, not ParameterizedClass)

- **Configuration**: `cassandra.yaml:partitioner` (bare class name; e.g., `org.apache.cassandra.dht.Murmur3Partitioner`)
- **Built-in**: Murmur3Partitioner (default, token-based), ByteOrderedPartitioner (order-preserving, legacy), RandomPartitioner (BigIntegerToken)
- **Interface**: `IPartitioner` — defines token space, range partitioning
- **Extension**: Class.forName (no parameters support, unlike ParameterizedClass)
- **Cluster scope**: **immutable after bootstrap**; cannot change without full data migration
- **Trunk-vs-5.0**: same interface; trunk has AccordSplitter supporting Accord consensus topology

---

### Back Pressure Strategy

**File**: `src/java/org/apache/cassandra/config/Config.java:183` (volatile ParameterizedClass)

- **Configuration**: `cassandra.yaml:back_pressure` (optional)
- **Interface**: `BackPressureStrategy` — applies to read/write load
- **Extension**: Class.forName + parameters
- **JMX mutable**: can change at runtime via JMX

---

### Seed Provider

**File**: `src/java/org/apache/cassandra/config/Config.java:157` (ParameterizedClass)

- **Configuration**: `cassandra.yaml:seed_provider`
- **Interface**: `SeedProvider`
- **Built-in**: SimpleSeedProvider (static seed list)
- **Extension**: Class.forName + parameters (e.g., seeds list)
- **Scope**: gossip bootstrap

---

### Other Knobs

| Knob | File:Line | Type | Scope | Pluggable? |
|------|-----------|------|-------|-----------|
| row_cache_class | Config.java:183 | String (bare class) | node | Yes, Class.forName |
| internode_authenticator | Config.java:159 | ParameterizedClass | inter-node auth | Yes, Class.forName + params |
| crypto_provider | Config.java:89 | ParameterizedClass | encryption | Yes, Class.forName + params |
| network_authorizer | Config.java:90 | ParameterizedClass | network auth | Yes, Class.forName + params |

---

## Extension Point Summary

### Registration Mechanisms

| Mechanism | Used By | Requires |
|-----------|---------|----------|
| **Java ServiceLoader** | SSTableFormat (CEP-17), CompressionProvider | `META-INF/services/org.apache.cassandra.io.sstable.format.SSTableFormat$Factory` |
| **Class.forName** (via config) | Memtable, Compactor, Compressor, Authenticator, Partitioner | YAML class_name + optional ParameterizedClass {parameters} |
| **Static FACTORY field** | Memtable (fallback if no factory() method) | `public static final Memtable.Factory FACTORY` in class |
| **factory() method** | Memtable | `public static Memtable.Factory factory(Map<String, String> params)` |
| **Reflection instantiation** | Compaction strategy | Constructor + setters for parameters |

### Configuration Layers

1. **Global (cassandra.yaml)**:
   - `authenticator`, `authorizer`, `partitioner`, `default_compaction`, `sstable.selected_format`, `memtable.configurations`
   - Applied at node startup

2. **Per-table (CREATE/ALTER TABLE)**:
   - `memtable`, `compaction`, `compression`, `caching`
   - Propagated to all nodes via schema change

3. **Runtime (JMX)**:
   - `compaction_throughput`, `permissions_validity`, `back_pressure_strategy`, `commitlog_sync`, etc. (volatile fields)

---

## Hard Couplings & Non-Pluggable Surfaces

### Q1-Critical: Memtable-Read Coordination

**Finding**: No extension point for read-before-flush visibility. The read path always retrieves `memtable.getPartition()` (in-memory), then reads disk SSTables. Analytics queries see only disk state.

- **Flush coordination** (Config.java:memtable_cleanup_threshold, memtable_flush_writers): tunable thresholds, not pluggable
- **Memtable-disk boundary**: hardcoded Flushing interface; flushes always produce uncompressed SSTables
- **Read merge**: hardcoded in SinglePartitionReadCommand / RangeCommand; no hook for "include pending flushes"

**Impact**: A freshness-aware analytical engine cannot read uncommitted memtable state without forking the read path or adding an interceptor (not pluggable).

### Q2-Critical: Storage Layout Assumptions

1. **Directory structure** (DataDirectory, CassandraRelevantProperties):
   - Hardcoded paths: `data_file_directories`, `commitlog_directory`, `saved_caches_directory`
   - No pluggable path provider

2. **Lifecycle assumptions**:
   - Flush → immediate SSTable visible → eligible for compaction → deletion
   - Compaction always reads disk SSTables, never re-serializes memtable state
   - CommitLog → replay → memtable → flush → SSTable (no alternative replay strategy)

3. **Java bootstrap**:
   - All pluggable components loaded via Class.forName or ServiceLoader (JVM-bound)
   - No native/FFI seam for out-of-JVM storage engines
   - DatabaseDescriptor.loadConfig() runs at startup; no runtime config hot-reload for most storage parameters

4. **Keyspace/table metadata**:
   - Cassandra metadata (replication, compaction, compression) embedded in SystemTables (system_schema.*)
   - No alternative metadata provider (e.g., external catalog)

5. **Token space / partitioning**:
   - Partitioner locked at cluster bootstrap (line 161 in Config.java)
   - Changing partitioner requires full rebuild

### Immutable / Cannot Extend

- **Replication strategy**: hardcoded SimpleStrategy, NetworkTopologyStrategy (no plugin interface)
- **Consistency levels**: enum, not pluggable
- **Mutation/Read semantics**: DELETE=tombstone, TTL=timestamp-based expiry (no pluggable expiry model)
- **Disk I/O patterns**: hardcoded BufferedReader / MappedByteBuffer; compaction_read_disk_access_mode (auto/mmap/direct) only a tuning knob, not a pluggable provider

---

## Q1 Freshness Relevance

**Question**: *When DataFusion reads a node via Arrow Flight, what must change so reads reflect memtable + SSTable state, not just flushed SSTables?*

**Answer**: 
1. **No current hook**: Cassandra provides no read-layer extension point to "freeze and serialize memtable state on demand."
2. **Required changes (not in scope)**:
   - Add `MemtableSnapshot` interface / provider
   - Memtable implementations export read-only snapshot (e.g., immutable BTree over skip-list)
   - Arrow Flight reader calls `ColumnFamilyStore.getMemtableSnapshot()` before/after disk SSTable reads
   - Merge logic unifies disk + memtable results
3. **Alternatively**: 
   - Flush before analytical read (loses write latency benefit)
   - Add CDC-style change log that includes memtable inserts (external freshness, not in-process)
   - External cache (Redis) mirrors memtable (operational complexity, no Cassandra hook)

---

## Trunk-vs-5.0 Deltas

| Feature | 5.0.x | Trunk | Impact |
|---------|-------|-------|--------|
| **CEP-11 Memtable API** | Single SkipListMemtable only | Multiple pluggable implementations (TrieMemtable, ShardedSkipListMemtable) | Trunk enables custom memtable strategies; 5.0 hardcoded |
| **CEP-17 SSTable Format API** | BigFormat only | Big + BTI discoverable via ServiceLoader; configurable | Trunk enables custom SSTable layouts; 5.0 hardcoded BIG format |
| **Compression Provider** | LZ4Compressor, Snappy, Deflate, Zstd (hardcoded) | Pluggable AbstractCompressionProvider, healthcheck + fallback | Trunk allows third-party compression; 5.0 limited |
| **Accord consensus** | Not shipped | AccordSplitter, FastPathStrategy in Config | Trunk supports Accord topology; 5.0 gossip-only |
| **Read repair policy** | Per-keyspace | ReadRepairStrategy enum + PercentileSpeculativeRetryPolicy (pluggable via TableParams) | Fine-grained read strategy control in trunk |

---

## Source Map (File Anchors)

- **Config hub**: `src/java/org/apache/cassandra/config/Config.java`
  - ParameterizedClass / InheritingClass: lines 86-92 (ParameterizedClass definition in same package)
  - Memtable options: 190-192
  - SSTable config: 157-159
  - Compressor providers: 92 (Map<String, ParameterizedClass>)
  - Commitlog compression: 172 (ParameterizedClass)

- **Memtable (CEP-11)**:
  - MemtableParams: `src/java/org/apache/cassandra/schema/MemtableParams.java:51-258` (parsing, Memtable.Factory lookup)
  - Memtable_API.md: `src/java/org/apache/cassandra/db/memtable/Memtable_API.md` (configuration format, examples)

- **SSTable (CEP-17)**:
  - SSTableFormat interface: `src/java/org/apache/cassandra/io/sstable/format/SSTableFormat.java:45+`
  - SSTable_API.md: `src/java/org/apache/cassandra/io/sstable/SSTable_API.md` (ServiceLoader, configuration)
  - BigFormat: `src/java/org/apache/cassandra/io/sstable/format/big/BigFormat.java:70+`

- **Compression**:
  - AbstractCompressionProvider: `src/java/org/apache/cassandra/io/compress/AbstractCompressionProvider.java:29-80`
  - CompressorRegistry: `src/java/org/apache/cassandra/io/compress/CompressorRegistry.java`

- **Compaction**:
  - CompactionParams: `src/java/org/apache/cassandra/schema/CompactionParams.java:49-150`

- **Cassandra.yaml**:
  - Authenticator: lines 198-206
  - Partitioner: line 400
  - Memtable: lines 818+
  - SSTable: lines 802+
  - Compaction: lines 691+ (read_disk_access_mode), 1228+ (default_compaction)

---

## Hypotheses for Q2 (Alternative Engine Feasibility)

1. **In-JVM plugin engine**: **Feasible** — implement SSTableFormat (CEP-17) + Memtable (CEP-11) + custom compaction strategy. Integration point is ServiceLoader discovery and Config YAML. Language: Java only.

2. **Adjacent OLAP engine**: **Partially feasible** — Arrow Flight reader can consume SSTables via CQLite-style external reader (CEP-17 doesn't mandate in-JVM), but **no memtable freshness without Cassandra patches**. Requires Q1 solution (memtable snapshot provider).

3. **Replacing storage engine entirely**: **Not feasible without forking** — lifecycle assumptions (flush→compact→delete) are wired in CompactionManager, ColumnFamilyStore.flush(), etc. No abstraction layer. Replication, consistency, lifecycle are hardcoded Java classes, not interfaces.

---

## Recommendations

- **For Q1 (freshness)**: File CEP for "Pluggable MemtableSnapshot Provider" + Arrow Flight integration
- **For Q2 (adjacent engine)**: Scope a CEP-17-compatible reader; separate issue for memtable freshness as blocker
- **For in-JVM plugin**: CEP-17 + CEP-11 already enable this; document SSTableFormat.Factory SPI registration path
