# Cassandra Streaming & Repair: Storage Engine Pluggability Index

## Summary

Cassandra's streaming subsystem (`streaming/`, `db/streaming/`, `repair/`) orchestrates zero-copy and partition-level SSTable transfer for bootstrap, repair, decommission, and range movement. It abstracts storage via `TableStreamManager` interface, with Cassandra's native engine (`CassandraStreamManager`) selecting and streaming SSTables. **Q1 answer**: By default, streaming sees only flushed SSTables (memtables flush before `createOutgoingStreams` via `writeAndAddMemtableRanges`). **Persistent memtables** can bypass flush via `Memtable.Factory.streamToMemtable()` hook (CEP-11, Cassandra 6.0 trunk). Hard couplings: `SSTableReader`, version/format metadata, `Component.Type.streamable` registry. Extension points: `TableStreamManager`, `SSTableFormat`, `Memtable.Factory` (CEP-11).

---

## Key Classes & Responsibilities

### High-Level Orchestration
| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| `TableStreamManager` (interface) | `streaming/TableStreamManager.java:37–59` | Abstract interface for storage engine; defines `createOutgoingStreams()`, `createStreamReceiver()`, `prepareIncomingStream()` |
| `CassandraStreamManager` | `db/streaming/CassandraStreamManager.java:67` | Cassandra's native implementation; selects SSTables via CFS view + repair metadata filter; calls `writeAndAddMemtableRanges()` before streaming |
| `StreamSession` | `streaming/StreamSession.java:98–150` | Orchestrates bidirectional streaming; manages session lifecycle (init → prepare → stream → complete); delegates to `TableStreamManager` per table |
| `StreamPlan` | `streaming/StreamPlan.java` | Builder API for streaming operations (bootstrap, repair, decommission); creates `StreamSession` instances |

### Outgoing (Sender) Path
| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| `CassandraOutgoingFile` | `db/streaming/CassandraOutgoingFile.java:43` | Outgoing stream for one SSTable section or whole file; wraps `Ref<SSTableReader>` + sections list; emits header + writer strategy (legacy partition-level or entire-sstable zero-copy) |
| `CassandraStreamWriter` | `db/streaming/CassandraStreamWriter.java` | Legacy partition-level streaming writer; deserializes uncompressed Data.db sections, re-encodes partitions |
| `CassandraCompressedStreamWriter` | `db/streaming/CassandraCompressedStreamWriter.java` | Partition-level streaming for compressed SSTables; decompresses Data.db on-the-fly, re-encodes partitions |
| `CassandraEntireSSTableStreamWriter` | `db/streaming/CassandraEntireSSTableStreamWriter.java:40` | Zero-copy entire-sstable streaming; streams component files verbatim via `FileChannel.transferTo()` + manifest |
| `CassandraStreamHeader` | `db/streaming/CassandraStreamHeader.java:46` | Metadata header sent before stream data: version, sections, estimated keys, compression info, serialization header, component manifest |
| `ComponentManifest` | `db/streaming/ComponentManifest.java:46` | Component list + size mapping for entire-sstable mode; encodes which .db/.idx/.etc files are streamed |
| `ComponentContext` | `db/streaming/ComponentContext.java` | Lock-held snapshot of sstable components during entire-sstable write; holds hard links to mutable components (stats, index summary) |

### Incoming (Receiver) Path
| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| `CassandraStreamReceiver` | `db/streaming/CassandraStreamReceiver.java` | Per-table receiver; creates `IStreamReader` instances and applies received SSTables to CFS |
| `CassandraIncomingFile` | `db/streaming/CassandraIncomingFile.java` | Incoming stream; chooses `IStreamReader` strategy (legacy `CassandraStreamReader` or entire-sstable `CassandraEntireSSTableStreamReader`) |
| `IStreamReader` (interface) | `db/streaming/IStreamReader.java:27` | Abstraction for reading a stream into `SSTableMultiWriter` |
| `CassandraStreamReader` | `db/streaming/CassandraStreamReader.java:79` | Legacy partition-level reader; decompresses (if needed), deserializes partitions, feeds into `SSTableMultiWriter` for flushing to new SSTable |
| `CassandraEntireSSTableStreamReader` | `db/streaming/CassandraEntireSSTableStreamReader.java:58` | Entire-sstable reader; writes component files directly via `SSTableTxnZeroCopyWriter`, mutates `Statistics.db` metadata, skips row-level deserialization |

### Format & Metadata
| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| `CompressionInfo` | `db/streaming/CompressionInfo.java` | Compression metadata (chunk offsets, lengths) sent for compressed streams; enables decompression on receiver side |
| `CassandraStreamHeader.Serializer` | `db/streaming/CassandraStreamHeader.java:158–` | Serializes/deserializes stream header: version string, format name, sections, compression, serialization header, component manifest |

### Repair Integration
| Class | File:Line | Responsibility |
|-------|-----------|-----------------|
| `CassandraValidationIterator` | `db/repair/CassandraValidationIterator.java:67` | Validation compaction for repair; also calls `writeAndAddMemtableRanges()` to flush memtable ranges into `Refs<SSTableReader>` |
| `CassandraTableRepairManager` | `db/repair/CassandraTableRepairManager.java` | Per-table repair entry point; delegates to repair task orchestrators |
| `LocalSyncTask` / `AsymmetricRemoteSyncTask` | `repair/LocalSyncTask.java`, `repair/AsymmetricRemoteSyncTask.java` | Repair sync tasks; invoke streaming via `StreamPlan` to send/receive differing sections |

---

## Extension Points & Pluggability Seams

### 1. **TableStreamManager Interface** (Primary Seam)
**Location**: `streaming/TableStreamManager.java:37–59`

A storage engine can implement `TableStreamManager` to:
- `createOutgoingStreams(session, replicas, pendingRepair, previewKind)` → list of `OutgoingStream` impls
- `createStreamReceiver(session, ranges, totalStreams)` → `StreamReceiver` impl
- `prepareIncomingStream(session, header)` → `IncomingStream` impl

**How it's used**: `ColumnFamilyStore.getStreamManager()` returns the instance; `StreamSession` calls it during prep phase to get streams. No hard-coded coupling to `CassandraStreamManager`.

**For CQLite**: An alternative engine would implement this interface, select its own data sources (e.g., external SSTables, Arrow files), and emit compatible `OutgoingStream`/`IncomingStream` impls.

### 2. **Memtable.Factory.streamToMemtable() / streamFromMemtable()** (CEP-11, Cassandra 6.0 trunk)
**Location**: `db/memtable/Memtable_API.md:154–157`

Persistent memtables can opt into streaming without flush:
- Return `true` from `Memtable.Factory.streamToMemtable()` → streaming retrieves live memtable data on send
- Return `true` from `Memtable.Factory.streamFromMemtable()` → incoming streams apply directly to memtable

**Implication for Q1**: Enables analytical reads to see memtable + SSTable state **without waiting for flush**. Requires custom memtable that can expose its data stream.

**Current Default**: `writeAndAddMemtableRanges()` (CassandraStreamManager.java:140, ColumnFamilyStore.java) flushes memtable → SSTable before `createOutgoingStreams()`, so default path sees only flushed state.

### 3. **SSTableFormat API** (Version/Format Pluggability)
**Location**: `io/sstable/format/SSTableFormat.java`

Defines writer/reader factories per format (BIG `na`/`nb`, BTI `da`, etc.). Streaming code queries `descriptor.version.format` to select serialization strategy. Alternative formats must implement:
- Reader/writer factories
- Component list via `SSTable.getStreamingComponents()` → filters by `Component.Type.streamable`
- `Version` with format-specific metadata

**Hard Coupling**: Entire-sstable streaming assumes components are bit-identical and byte-for-byte streamable. Any on-the-fly transformation (e.g., compression, reordering) breaks zero-copy promise.

### 4. **StreamingDataOutputPlus / StreamingDataInputPlus** (I/O Abstraction)
**Location**: `streaming/StreamingDataOutputPlus.java`, `streaming/StreamingDataInputPlus.java`

Abstracts the wire transport. Receiver can supply custom `DataInputPlus` wrapper to implement compression, encryption, or format translation.

---

## Hard Couplings (Format Assumptions & Invariants)

### A. **SSTable Format & Components**
- `Component.Type.streamable` flag determines which components stream (Data, Index, Summary, Filter, etc.; excludes CRC, TOC)
- Entire-sstable mode assumes components are **byte-identical** on receiver (no recompression, re-indexing, reordering)
- `CassandraOutgoingFile.computeShouldStreamEntireSSTables()` (line 189–199) disables zero-copy if:
  - `DatabaseDescriptor.streamEntireSSTables()` is false
  - SSTable has legacy counter shards (`hasLegacyCounterShards`)
  - Bloom filter format is pre-4.0 (incompatible)

### B. **Partition Encoding & Deserialization**
- Partition-level streaming relies on `SerializationHeader` (CassandraStreamHeader line 55, 175)
- Receiver expects rows encoded in format specified by `SerializationHeader.Component` (column, clustering, deletion encoding)
- Any schema evolution or type changes between sender/receiver can corrupt rows (no schema negotiation in stream header)

### C. **Version String & Format Name**
- Header carries `descriptor.version.toString()` and `version.format.name()` (CassandraStreamHeader.Serializer line 162–163)
- Receiver looks up serializers by format name: `ComponentManifest.serializers.get(header.version.format.name())`
- Format mismatch → deserialization exception

### D. **SSTableReader Metadata & Refs**
- Outgoing streams hold `Ref<SSTableReader>` for lifecycle management (prevents deletion during streaming)
- Receiver creates new `SSTableReader` after writing; no cross-engine compatibility
- Repair metadata (repairedAt, pendingRepair, sstableLevel) passed through header but tied to SSTableReader semantics

### E. **Compression Codec Registry**
- If compressed, `CassandraStreamHeader.compressionInfo` encodes chunk boundaries
- Receiver decompresses using codec from `CompressionInfo`, which references Java codec classes (LZ4, Snappy, etc.)
- Custom compression codecs must be registered in Cassandra's codec factory; no extensibility seam

### F. **CFS & Keyspace Lookup**
- Receiver looks up table schema via `TableId` and `ColumnFamilyStore.getIfExists()` (CassandraStreamReader line 124)
- If schema was dropped during streaming, stream aborts
- No alternative storage layer can intercept this; tight coupling to CFS lifecycle

---

## Q1 Relevance: Freshness & Memtable Visibility

**Q1: When DataFusion/Trino reads via Arrow Flight, how can the read see memtable + flushed SSTables?**

### Current State (5.0 & trunk default)
1. **Streaming layer**: `createOutgoingStreams()` is called **after** `writeAndAddMemtableRanges()` flushes memtables to SSTables (CassandraStreamManager line 140).
2. **Flight/analytical read**: Must independently fetch from both SSTables and live memtables **in the application** (CQLite, Arrow Flight server, etc.); streaming path is blind to memtables.
3. **Implication**: Q1 answer = **No automatic visibility**. The analytical reader (e.g., CQLite's Flight connector) must query CFS for both SSTables and memtables separately, then merge results.

### Trunk (CEP-11) Enhancement
- Persistent memtables with `streamToMemtable() = true` can **stream memtable data directly** without flush.
- If an alternative storage engine (e.g., CQLite-in-JVM) implements persistent memtables, streaming framework will call `memtable.partitionIterator()` to get live data.
- **For Q1**: A custom memtable factory that returns `streamToMemtable() = true` + exposes live data would enable streaming to see unflushed writes. Receiver must then choose to apply to memtable or SSTable.

### No Seam for Flight/Analytical Reads
- Flight server reads via CFS snapshot (immutable view of SSTables at a moment).
- Memtable reads are separate (via `memtable.partitionIterator()`).
- **Architectural gap**: No seam in streaming code for Flight to "prepare" a memtable snapshot before an analytical read starts.
- **Workaround**: Explicit flush before Flight query, or two-phase read (memtable + SSTables separately).

---

## Q2 Relevance: Alternative Storage Engine Feasibility

### **2a. CQLite as Alternative Engine (In-JVM or Embedded)**

**Feasibility**: **HIGH** (medium complexity).

**Seams**:
1. Implement `TableStreamManager` → produce `OutgoingStream` over CQLite's data format
2. Register as CFS stream manager via `ColumnFamilyStore` init or schema config (NOT yet exposed; would need custom CFS subclass or factory injection)
3. Write/read path: CQLite would emit partition iterators compatible with Cassandra's `UnfilteredRowIterator` interface

**Blockers**:
- No config/SPI to plug in alternative `TableStreamManager` per table; hardcoded to `CassandraStreamManager` in CFS constructor
- SerializationHeader coupling: CQLite format ≠ SSTable encoding; header negotiation needed
- Compaction tier/repair metadata: CQLite may not track repairedAt; requires adaptation

### **2b. CQLite as Adjacent OLAP Engine (Sidecar)**

**Feasibility**: **HIGH** (low complexity for read path).

**Seams**:
1. CQLite Flight server reads CFS independently (no streaming involved); streams Arrow IPC to consumer
2. Repair/bootstrap trigger streaming from Cassandra → external CQLite instance (requires custom `TableStreamManager` in Cassandra or external sync tool)
3. **For refresh in Q1**: CQLite's Flight connector can expose both Cassandra memtable + CQLite's SSTables via separate scans, then merge

**No Blockers**: CQLite is already external; no JVM coupling needed.

### **2c. Alternative Storage Format Inside Cassandra**

**Feasibility**: **MEDIUM** (high complexity).

**Seams**:
1. Implement `SSTableFormat` → custom reader/writer factories
2. Implement `TableStreamManager` → expose format-specific streams
3. Repair/compaction must handle format conversion (old SSTable → new format)

**Blockers**:
- Entire-sstable streaming assumes byte-identical components; any format change requires partition-level streaming (slow)
- Consistency checks (bloom filters, summaries) are format-specific
- Bootstrap would need sstabledump-like tool to convert existing SSTables

### **Storage Engine Seams Summary**

| Seam | Exposed? | Coupling | Effort |
|------|----------|----------|--------|
| `TableStreamManager` | ✅ Yes (interface) | Per-table via CFS | Low |
| Memtable CEP-11 hooks | ✅ Yes (trunk only) | Tight to Memtable.Factory | Medium |
| `SSTableFormat` | ✅ Yes (registry) | Format name string | High |
| Compaction/repair metadata | ❌ No | Baked into stream header | High |
| CFS constructor | ❌ No | Hardcoded `CassandraStreamManager` | High |
| Serialization header encoding | ❌ No | Per-format, no negotiation | High |

---

## Trunk vs. Cassandra 5.0 Deltas

| Area | 5.0 | Trunk (6.0) | Impact |
|------|-----|------------|--------|
| **CEP-11 Memtable API** | ❌ Not present | ✅ Full API (`streamToMemtable()`, factory params) | Q1: Enables persistent memtable streaming without flush |
| **SSTableFormat registry** | ✅ Present (BIG only) | ✅ BIG + BTI (`da`) | Trunk supports dual formats; 5.0 is BIG-only |
| **Entire-sstable streaming** | ✅ Yes | ✅ Yes | No change; both support zero-copy |
| **ComponentManifest** | ✅ Yes | ✅ Yes | No change |
| **Accord (Accord repair)** | ❌ Not present | ✅ New path via `AccordFetchCoordinator` | 6.0 adds alternative repair path; uses streaming similarly |
| **Stream format version** | Fixed at 5.0 wire proto | Wire proto versioning exists; can vary by cluster | Both allow version negotiation |

---

## Streaming Data Flow (Entire-sstable Zero-Copy)

```
Sender Side:
  StreamSession.startStreamingFiles()
    → CassandraOutgoingFile.write()
      → (if shouldStreamEntireSSTable) CassandraEntireSSTableStreamWriter.write()
        → for each Component:
          → ComponentContext.channel(component) → FileChannel
          → StreamingDataOutputPlus.writeFileToChannel(channel) [zero-copy via sendfile(2)]

Receiver Side:
  CassandraIncomingFile → IStreamReader
    → (if isEntireSSTable) CassandraEntireSSTableStreamReader.read()
      → SSTableTxnZeroCopyWriter.writeComponent()
        → DataInputPlus → FileChannel.write()
      → mutate Statistics.db (repairedAt, sstableLevel)
      → SSTableReader.open() on received files
      → CFS.addSSTable()
```

---

## Format Assumptions Leaking Into Streaming

| Assumption | Location | Consequence |
|-----------|----------|-------------|
| Partitions ordered by token | CassandraStreamWriter line ~180 | Receiver assumes sorted input; violates sorting → query corruption |
| Component files byte-identical | CassandraOutgoingFile line 209 | Entire-sstable mode assumes no lossy transformation |
| SerializationHeader stable | CassandraStreamReader line 110 | Column/deletion encoding mismatch → deserialization exception |
| Bloom filter format ≥ 4.0 | CassandraOutgoingFile line 195 | Pre-4.0 filters not streamable; fallback to partition-level (slow) |
| Version string parseable | CassandraStreamHeader.Serializer line 162 | Malformed version → version lookup fails |
| TableId exists in CFS | CassandraStreamReader line 124 | Schema drop during streaming → aborts stream |
| Compression codec available | CompressionInfo deserializer | Codec not registered → decompression fails |

---

## Recommendations for Q1 & Q2

### For Q1 (Freshness)
1. **Short term (5.0)**: Application layer (CQLite Flight) manually queries memtable + SSTables; merge in user space.
2. **Medium term (6.0+)**: Implement persistent memtable with `streamToMemtable() = true`; extend Flight/streaming to coordinate flush-free reads.
3. **Long term**: Seam for analytical read registration in CassandraStreamManager (not yet designed).

### For Q2 (Alternative Engine)
1. **CQLite as sidecar** (recommended): Implement external sync tool that calls `TableStreamManager.createOutgoingStreams()` directly; no CFS changes needed.
2. **CQLite in-JVM**: Requires CFS factory injection seam (design + code); medium effort, high coupling risk.
3. **Custom SSTableFormat**: High effort; only viable if format is Cassandra-compatible (binary identical components).

---

## Files & Line Anchors (Trunk @ HEAD)

- `streaming/TableStreamManager.java:37–59` — Interface
- `db/streaming/CassandraStreamManager.java:67–169` — Implementation
- `streaming/StreamSession.java:98–150` — Orchestration
- `db/streaming/CassandraOutgoingFile.java:43–239` — Sender
- `db/streaming/CassandraStreamWriter.java` — Partition-level send
- `db/streaming/CassandraEntireSSTableStreamWriter.java:40–119` — Zero-copy send
- `db/streaming/CassandraStreamReader.java:79–150` — Partition-level receive
- `db/streaming/CassandraEntireSSTableStreamReader.java:58–200+` — Zero-copy receive
- `db/streaming/CassandraStreamHeader.java:46–200+` — Metadata
- `db/streaming/ComponentManifest.java:46–180` — Component list
- `db/memtable/Memtable_API.md:154–157` — CEP-11 streaming hooks
- `io/sstable/format/SSTableFormat.java` — Format registry
- `db/ColumnFamilyStore.java` — `getStreamManager()`, `writeAndAddMemtableRanges()`
- `repair/LocalSyncTask.java` — Repair → streaming
