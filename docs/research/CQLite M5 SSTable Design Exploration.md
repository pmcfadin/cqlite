# **Architectural Analysis of Hybrid Storage Engines for Embedded CQL Compatibility: Bridging the Gap Between Log-Structured Ingestion and Immutable SSTables**

## **1\. Executive Introduction: The Embedded Storage Paradox**

The objective of designing "M5" within the cqlite ecosystem presents a distinct architectural paradox: the requirement to produce Apache Cassandra-compatible Sorted String Tables (SSTables) without the infrastructure of the Apache Cassandra server process. This challenge is not merely one of file format compliance but of fundamental database theory. Cassandra, and the LSM-tree (Log-Structured Merge-tree) paradigm it employs, is predicated on the "Staged Event-Driven Architecture" (SEDA). In this model, write operations are batched in volatile memory (Memtables) to amortize the cost of sorting, while persistent, immutable files (SSTables) are managed by autonomous background threads that perform compaction, flushing, and garbage collection. This architecture assumes a continuously running supervisor process with exclusive control over memory resources and I/O scheduling.1  
In contrast, cqlite operates as an embedded library. It is bound to the lifecycle of the host application, which may be ephemeral, resource-constrained, or single-threaded. There is no persistent daemon to manage the "care and feeding" of an LSM tree. If cqlite were to implement the standard Cassandra write path—logging to a CommitLog, buffering in RAM, and flushing only when memory is full—it would risk significant data loss upon host process termination and incur prohibitive startup recovery times.3 Furthermore, without a background compaction process, a naive implementation would result in an explosion of small data files, degrading read performance to unacceptable levels due to severe read amplification.5  
Therefore, the design of M5 necessitates a "Middle System"—a hybrid storage architecture that sits between the raw, high-throughput ingestion of a write-ahead log and the rigid, sorted structure of a BigTable-format SSTable. This report provides a comprehensive exploration of such architectures. We analyze mechanisms from SQLite’s Write-Ahead Log (WAL), Bitcask’s append-only model, WiscKey’s key-value separation, and modern succinct data structures like SuRF (Succinct Range Filters). The goal is to propose a storage discipline that provides immediate durability and acceptable read performance in a library context, while retaining the ability to eventually produce standard Cassandra artifacts.

## **2\. Deconstructing the Target: Apache Cassandra SSTable Internals**

To engineer a compatible hybrid system, one must first possess an exhaustive understanding of the target artifact. The Cassandra SSTable is not a single flat file but a complex composite of aligned components, each serving a critical role in the read path. The complexity of these components dictates why synchronous generation is computationally expensive and why a "Middle System" is required.

### **2.1 The BigTable Binary Format**

The target formats for compatibility are the legacy big format (Cassandra 3.0+) and the modern bti (Big Trie-Indexed) format (Cassandra 5.0+). A single SSTable logically represents a snapshot of the database at a point in time, organized into separate physical files on disk, typically identified by a generation identifier (e.g., nb-1-big).7

#### **2.1.1 The Data Component (Data.db)**

The Data.db file is the primary payload containing the actual rows and column values. Crucially, the data within this file is strictly ordered. The primary sort key is the hashed token of the **Partition Key**, and the secondary sort key (within a partition) is the **Clustering Key**.7 This strict ordering is the root cause of the "impedance mismatch" for an embedded library. To write a valid Data.db, the system must possess the complete set of rows for the flush interval and perform a global sort. One cannot simply append a new row to the end of a Data.db file if that row belongs to a partition that appears earlier in the file.  
The internal structure of the Data.db component is row-oriented. Each partition begins with a header containing the Partition Key and deletion timestamps (tombstones). This is followed by a sequence of rows. In newer formats, partition bodies may be compressed into blocks, requiring the writer to manage compression buffers and flush aligned chunks.7

#### **2.1.2 The Index Component (Index.db)**

Because the Data.db file is variable-width (rows have different sizes), random access is impossible without an index. The Index.db file maps Partition Keys to their byte offsets within the Data.db file.7 In the big format, this is a sequence of (Key, Offset) tuples. In the bti format, this is structured as a generic trie (prefix tree) to reduce space and improve lookup speed.9 For cqlite, this implies that the "Middle System" must maintain a mapping of keys to disk locations; otherwise, every read operation becomes a full table scan, which is non-performant for any dataset larger than a few megabytes.

#### **2.1.3 The Filter Component (Filter.db)**

To mitigate the cost of checking multiple SSTables during a read, Cassandra generates a Bloom Filter for every SSTable.7 This probabilistic data structure allows the system to determine, with certainty, if a key does *not* exist in a file, thereby saving a disk seek. In an embedded context where compaction is infrequent, the number of physical files may grow large (e.g., 100+ small files). In this scenario, the Bloom Filter becomes the single most critical component for read performance. However, standard Bloom Filters have a fatal flaw regarding CQL: they only support point lookups, not range scans.12 This limitation is a primary driver for investigating alternative structures like SuRF later in this report.

#### **2.1.4 Auxiliary Components**

* **CompressionInfo.db:** Contains metadata about the boundaries of compressed chunks in Data.db.7  
* **Statistics.db:** Stores metadata required for the query optimizer and compaction strategy, including the minimum and maximum timestamps, estimated row counts, and tombstone metrics.10  
* **Summary.db:** An in-memory sampling of the Index.db used to accelerate primary index lookups.7

### **2.2 The Write Path Divergence**

The standard Cassandra write path leverages the independence of the server process. Writes are appended to a CommitLog (for durability) and inserted into a Memtable (a concurrent skip-list in RAM).1 When the Memtable fills, a flush thread writes the sorted data to an SSTable.  
In cqlite, this model fails in two specific ways:

1. **The Concurrency Gap:** In a library, the "flush" must often happen in the foreground, blocking the write operation. If the Memtable is large (e.g., 512MB), the sort-and-write latency becomes perceptible to the user, causing "Stop-the-World" pauses.  
2. **The Compaction Void:** In Cassandra, compaction runs in the background to merge many small SSTables into fewer large ones.2 In cqlite, unless the host application explicitly calls a maintenance API, these files accumulate. A read operation might need to check hundreds of files, and without a running background process to merge them, the "Read Amplification" (RA) skyrockets.

Therefore, the "Middle System" must accept writes quickly (like a log) but structure them in a way that supports range queries (like an SSTable) without requiring immediate global sorting or background compaction.

## **3\. The Log-Structured Hybrid: Advanced WAL Architectures**

The most direct "middle system" is an enhanced Write-Ahead Log (WAL). In standard LSM designs, the WAL is a throw-away artifact used only for recovery. However, in an embedded system, the WAL can be elevated to a primary storage structure, provided it is indexed efficiently. This approach draws heavy inspiration from SQLite’s WAL mode and Bitcask’s append-only storage.

### **3.1 The SQLite WAL Precedent**

SQLite’s implementation of WAL provides a robust template for embedded concurrency. Unlike the traditional rollback journal, SQLite’s WAL allows simultaneous readers and a single writer.3 Crucially, it uses a separate \-shm (shared memory) file to index the WAL, allowing readers to find the latest version of a page in the WAL without accessing the main database file.3  
The mechanism involves three distinct files: the main database, the .wal file (containing new pages), and the .shm file (the WAL-index). When a writer appends a frame to the WAL, it updates the hash table in the shared memory file. Readers check this shared memory index to determine if they should read a page from the WAL or the main database file.15 This effectively allows the WAL to act as a "Persistent Memtable"—it is durable (on disk) but mutable (appended to) and queryable.  
**Relevance to Cqlite:** The SQLite model demonstrates that an "Uncompacted" log can serve as a primary read source if the indexing is fast. For cqlite, however, the unit of storage is not a "page" (as in SQLite's B-trees) but a "row" or "mutation." A direct port of SQLite’s paging WAL is insufficient because Cassandra tables are sparse and variable-width. We need a *Logical* WAL that stores CQL mutations, not physical disk pages. The critical takeaway from SQLite is the use of a **Shared Memory Index** (via mmap) to bridge the gap between the writer and concurrent readers.15

### **3.2 Bitcask and the Persistent Hash Map**

Bitcask offers a compelling alternative for Key-Value data. Developed for Riak, it writes data sequentially to an append-only log. An in-memory Hash Map (KeyDir) maps every key to its file\_id and offset.16  
**The Mechanism:**

1. **Write:** Append \`\` to the active log file. Update the in-memory Hash Map.  
2. **Read:** Look up Key in Hash Map $\\rightarrow$ Get Offset $\\rightarrow$ Single Disk Seek to read Value.  
3. **Recovery:** On startup, scan all log files to rebuild the Hash Map. To speed this up, Bitcask generates "Hint Files" during compaction, which contain only the keys and offsets, reducing the startup scan time significantly.18

**Critical Limitation for CQL:** Bitcask is fundamentally a Key-Value store. It supports GET(k) and PUT(k, v). It does *not* natively support Range Scans (SELECT \* FROM table WHERE k \> X). CQL heavily relies on range scans within partitions. In Bitcask, keys are in a Hash Map, which is unordered. A range query would require scanning the entire dataset or loading all keys into a sorted structure.16  
**The "Range-Bitcask" Hybrid for M5:**  
To adapt the Bitcask model for cqlite, the in-memory structure cannot be a Hash Map. It must be a **Persistent SkipList** or **B-tree**.

* **Write Path:** Append data to the Log (SSTable Data component equivalent). Insert the Key+Offset into an in-memory SkipList (SSTable Index component equivalent).  
* **Persistence:** Unlike a pure Memtable, the data is durable in the Log. The *Index* is volatile.  
* **Fast Recovery:** Periodically snapshot the SkipList to disk as a "Hint File" or a "Partial Index.db". On startup, mmap this index file instead of replaying the log.

This architecture effectively creates a "Persistent Memtable." The data on disk is unsorted (log order), but the index makes it appear sorted. This is a viable "Middle System" before conversion to a fully sorted SSTable.

## **4\. Key-Value Separation: The WiscKey Architecture**

One of the most expensive operations in LSM compaction (and thus, in SSTable generation) is the copying of value data. In typical workloads, keys are small, but values (rows) can be large. Standard compaction reads Key+Value from input files, sorts them, and writes Key+Value to output files. This results in high Write Amplification (WA).21 For an embedded library, this heavy I/O cost can freeze the application.

### **4.1 The WiscKey Concept**

WiscKey (used in BadgerDB and optimized RocksDB versions) separates keys from values to minimize I/O amplification.

* **vLog (Value Log):** All values are written to a purely append-only log file. They are never moved during compaction (unless garbage collected).  
* **LSM Tree:** The LSM tree stores only \<Key, Pointer\> pairs, where the pointer is the offset in the vLog.

### **4.2 The "Lightweight SSTable" Strategy for Cqlite**

For cqlite M5, this separation is a game-changer. It allows the system to produce valid SSTable structures without the cost of sorting the heavy payload data.

1. **Write Path:** Appending the full row to a Data.log (similar to the vLog). This provides sequential write performance.  
2. **Indexing:** The in-memory Memtable only stores \<PartitionKey, ClusteringKey, Offset\>. This structure is tiny compared to the full data.  
3. **Flush:** When memory is full, cqlite flushes *only* the Index to disk as a "Lightweight SSTable" (or Index.db). We do *not* rewrite the data.  
4. **Result:** The disk now contains a massive Data.log and several small, sorted Index.db files.

**Cassandra Compatibility Bridge:**  
This creates a divergence from the standard Cassandra format, which expects Data.db to be sorted. In this hybrid, Data.log is unsorted. However, this serves as an excellent intermediate state. To create a canonical SSTable, a foreground maintenance task (or a "Save As" operation) iterates through the sorted Index.db, reads the values from Data.log randomly, and writes a standard sorted Data.db.  
**Trade-off:** Range scans become slower because fetching values requires random seeks in the Data.log.21 However, modern SSDs handle parallel random reads effectively. To mitigate this, cqlite can implement **read-ahead buffering** or **prefetching** logic that detects a range scan and issues parallel I/O requests to the Data.log.23

## **5\. Write-Optimized Buffered Trees (B$^\\epsilon$-trees)**

If the primary constraint is the inability to perform background compaction, the storage structure must inherently tolerate fragmentation or buffer writes more effectively than a standard LSM. The B$^\\epsilon$-tree (or Fractal Tree) offers a mathematical compromise between the read-optimized B-tree and the write-optimized LSM.24

### **5.1 Theory of Operation**

In a standard B-tree, a write requires locating the leaf node and updating it, often incurring a random I/O. In an LSM, writes are buffered in RAM and flushed sequentially. A B$^\\epsilon$-tree adds a buffer to *every internal node* of the tree.25

* **Insertion:** When a key is inserted, it is placed into the root node's buffer. It is not immediately pushed down to the leaf.  
* **Flush:** When a node's buffer fills, a portion of the updates are batched and pushed down to the child nodes. This "trickles" data down the tree using sequential I/O operations rather than random seeks.

### **5.2 Application to Cqlite**

A B$^\\epsilon$-tree could serve as the primary storage engine for cqlite M5. It maintains sorted order (supporting CQL range queries) and provides excellent write performance without the "stop-the-world" compaction cycles of LSMs. It effectively spreads the cost of compaction across every write operation. However, implementation complexity is high, and the file format is fundamentally different from SSTables.  
**Hybrid Applicability:**  
While a full B$^\\epsilon$-tree might be overkill, the *concept* of buffering at the node level can be applied. We can treat the "Middle System" as a single, large root buffer. Writes accumulate in an append-only log (the buffer). When the log reaches a threshold, it is not just flushed, but *partitioned* into coarse-grained buckets (e.g., Token Ranges A-M, N-Z) and appended to separate log segments. This rough sorting facilitates future conversion to SSTables by ensuring that data is at least partially ordered, reducing the work required during the final sort.

## **6\. The Read Path Challenge: Succinct Range Filters (SuRF)**

A significant risk of using a "Middle System" (like a log or many small files) is **Read Amplification**. If cqlite accumulates 50 log files, a range query SELECT \* FROM t WHERE id \> 10 might have to check all 50 files. Standard Bloom Filters cannot help here, as they only support point lookups (id \= 10).12

### **6.1 SuRF Technology**

**SuRF (Succinct Range Filter)** is a data structure based on Fast Succinct Tries (FST) that serves as a range-aware Bloom filter.26 Unlike a Bloom filter, which hashes keys and loses all order information, SuRF stores a compressed representation of the key prefixes. This allows it to answer queries like "Are there any keys between 10 and 50 in this file?" with a high degree of accuracy.

### **6.2 The M5 Strategy**

For every "Middle System" file (e.g., segment-N.log), cqlite should generate a SuRF in memory (and persist it).

* **Query:** SELECT... WHERE id \> 100 AND id \< 200  
* **Execution:** cqlite checks the SuRF of every Log Segment.  
* **Result:** SuRF returns False for segments that have no keys in that range. The query engine skips those files entirely.  
* **Impact:** This dramatically reduces the read cost of keeping data in a "semi-sorted" or "log-structured" state, making the hybrid approach viable without aggressive background compaction.27

## **7\. Deep Dive: Durability and the mmap Protocol**

The "No Server" constraint implies that cqlite cannot rely on a daemon to manage page caches or write buffers. Instead, it must leverage the operating system's virtual memory subsystem via mmap.

### **7.1 The Mechanism of Persistence**

Using mmap (Memory-Mapped I/O) allows cqlite to treat a file on disk as an array in memory.

* **Read Path:** The OS manages paging. If cqlite reads an index that is hot, it stays in RAM. If memory pressure rises, the OS evicts it. This replaces the complex Buffer Pool Manager found in server databases.28  
* **Write Path:** Writes to the memory map are eventually flushed to disk by the kernel's pdflush threads. However, for ACID durability, cqlite must explicitly control this.

### **7.2 msync vs. fsync**

To ensure durability, cqlite must use synchronization primitives.

* **fsync(fd)**: Flushes all dirty pages associated with the file descriptor fd to disk. It is robust but can be slow if it flushes metadata.30  
* **msync(addr, len, MS\_SYNC)**: Flushes only the specified range of the memory map. This is potentially more efficient for creating a "Persistent Memtable" where only the tail of the log is dirty.31

**Recommendation:** cqlite should use mmap for the **Index** (Hint File) and **Filter** components, as these are read-heavy. For the **Data Log**, standard write() followed by fsync() (or opened with O\_DSYNC) is preferred for append-only integrity, as extending an mmap region usually requires expensive ftruncate and re-mapping operations.32

## **8\. Specific Recommendations for M5**

Based on the synthesis of the research materials, specifically the limitations of Bitcask (no ranges) and the overhead of standard LSMs (compaction), the following specification is recommended for M5:

### **Direction A: The Segmented Log with Persistent SkipList (The "Appended" SSTable)**

This approach focuses on fast writes and crash recovery, accepting that read performance degrades until a user-initiated "optimization" (compaction) occurs.  
**Architecture:**

1. **Storage Unit:** A "Segment". A Segment consists of an unsorted Log.db and a sorted Index.db.  
2. **Write Path:**  
   * Writes are appended to the active Log.db.  
   * Writes are inserted into an in-memory SkipList (Memtable).  
   * **Crucially:** The Log.db acts as the WAL. Durability is achieved via fsync on this log.  
3. **Flush (The Hybrid Step):**  
   * Instead of reading the Log.db and sorting it to create a standard Data.db, we simply **freeze** the Log.db.  
   * We flush the SkipList to disk as a dense Index.db and a SuRF Filter.db.  
   * **Outcome:** We have a valid SSTable *Index* pointing to an *unsorted* Data file.  
   * **Compatibility Hack:** Standard Cassandra readers expect Data.db to be sorted. cqlite's read path must be modified to tolerate unsorted data files *if* the Index allows random access.

**Pros:** Extremely fast flush (only writing keys). Zero write amplification during ingest.  
**Cons:** Range scans are effectively random I/O on the log. Not byte-compatible with standard Cassandra Data.db (requires a converter).

### **Direction B: The Memory-Mapped Coalescing Buffer (The "Mmap" Hybrid)**

This approach leverages the OS virtual memory system to manage the "Memtable" persistence, blurring the line between RAM and Disk.28  
**Architecture:**

1. **Storage Unit:** A large Memory-Mapped file (e.g., 1GB) acting as a persistent buffer.  
2. **Data Structure:** A B-tree or SkipList laid out directly in the memory-mapped region (using relative pointers, not absolute memory addresses).33  
3. **Write Path:**  
   * The application writes directly into the memory map.  
   * The OS manages dirty page writeback.  
   * msync() is called to enforce durability at transaction boundaries.30  
4. **Role in M5:** This persistent buffer acts as the "Middle System." It is effectively a sorted, persistent Memtable.  
5. **Transition:** When the map is full, a function ExportToSSTable() iterates over the sorted map and writes a standard Cassandra SSTable sequentially. The map is then cleared/recycled.

**Pros:** "Infinite" Memtable size (limited by disk). Sorted order is maintained at all times. Recovery is instant (just mmap the file). **Cons:** Random writes to mmap can cause write amplification at the OS page level (4KB). If the app crashes during a write, the data structure might be corrupted unless distinct "Commit" flags are used (Copy-on-Write).33

### **Direction C: The Log-Structured Merge with Foreground Merging (The "Lazy" LSM)**

This approach adheres closest to the LSM model but adapts the compaction strategy for a library environment.  
**Architecture:**

1. **Storage Unit:** Standard Cassandra SSTables (sorted Data.db, Index.db).  
2. **Write Path:**  
   * Writes go to a WAL and RAM Memtable.  
   * When Memtable is full (or at app exit), it is flushed to a **Tier-0 SSTable**.  
3. **The Twist (No Background Threads):**  
   * We use a **Tiered Compaction Strategy** (STCS). We allow many small SSTables to accumulate.  
   * To prevent read performance from collapsing, we use **SuRF (Succinct Range Filters)** 26 instead of standard Bloom filters.  
   * **Compaction on Read/Write:** We implement "Read-Repair" or "Write-Triggered" compaction. When a read touches \> 10 SSTables, we trigger a foreground merge of those specific tables. This amortizes the compaction cost into the read latency (which is acceptable for some embedded workloads).

**Pros:** Produces 100% compatible SSTables.  
**Cons:** Unpredictable latency spikes.

## **9\. Conclusion**

The design of M5 in cqlite requires deviating from the server-centric dogmas of Cassandra. By treating the SSTable generation as a two-stage process—first to a durable, indexed log (The Middle System), and optionally to a sorted standard SSTable—cqlite can achieve high write throughput and durability without the need for background threads. The integration of Key-Value separation principles and modern range filters (SuRF) provides the necessary read performance to make this hybrid approach viable for production workloads.  
The optimal direction is likely **Direction A (Segmented Log)**, as it leverages the "Append-Only" strength of logs for writes while using "Hint Files" and "SuRF" to simulate the read characteristics of an SSTable, bridging the gap between embedded library constraints and enterprise database features.  
---

**Table 1: Comparison of Proposed Hybrid Architectures**

| Feature | Direction A: Segmented Log | Direction B: Mmap Buffer | Direction C: Lazy LSM |
| :---- | :---- | :---- | :---- |
| **Write Latency** | **Lowest** (Append Only) | Medium (Random RAM/Disk Access) | Low (Sort in RAM, then Flush) |
| **Read Latency (Point)** | Fast (Index Lookup) | Fast (Sorted Tree) | Medium (Multiple Bloom Filters) |
| **Read Latency (Range)** | Medium (Random Disk I/O) | **Fastest** (Sorted Tree) | Slow (Merge Sort of Many Files) |
| **Crash Recovery** | Instant (Load Hint File) | Instant (Map File) | Instant (Load File List) |
| **Cassandra Compatibility** | Requires Conversion | Requires Conversion | **Native** |
| **Complexity** | Moderate | High (Custom Allocator) | Moderate |
| **Best For** | High Ingest, Crash Safety | Read-Heavy, Low Latency | Compatibility, Batch Writes |

### **Citations**

1

#### **Works cited**

1. Apache Cassandra — The minimum internals you need to know | by Alex Punnen \- Medium, accessed January 27, 2026, [https://medium.com/better-software/apache-cassandra-the-minimum-internals-you-need-to-know-89724603abb2](https://medium.com/better-software/apache-cassandra-the-minimum-internals-you-need-to-know-89724603abb2)  
2. Architecture in brief | Apache Cassandra 3.0 \- DataStax Docs, accessed January 27, 2026, [https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/architecture/archIntro.html](https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/architecture/archIntro.html)  
3. SQLite Write-Ahead Logging \- Anže's Blog, accessed January 27, 2026, [https://blog.pecar.me/sqlite-wal](https://blog.pecar.me/sqlite-wal)  
4. Serverless by Design: The Embedded Database Advantage | by Narendra reddy Sanikommu | Medium, accessed January 27, 2026, [https://medium.com/@narenreddy.sanikommu/serverless-by-design-the-embedded-database-advantage-9dbf1464e65a](https://medium.com/@narenreddy.sanikommu/serverless-by-design-the-embedded-database-advantage-9dbf1464e65a)  
5. An In-depth Discussion on the LSM Compaction Mechanism \- Alibaba Cloud Community, accessed January 27, 2026, [https://www.alibabacloud.com/blog/an-in-depth-discussion-on-the-lsm-compaction-mechanism\_596780](https://www.alibabacloud.com/blog/an-in-depth-discussion-on-the-lsm-compaction-mechanism_596780)  
6. An overview of Leveled Compaction in LSM-trees \- fjall-rs, accessed January 27, 2026, [https://fjall-rs.github.io/post/lsm-leveling/](https://fjall-rs.github.io/post/lsm-leveling/)  
7. Cassandra SSTables Overview \- Anant Corporation, accessed January 27, 2026, [https://anant.us/blog/modern-business/cassandra-sstables-overview/](https://anant.us/blog/modern-business/cassandra-sstables-overview/)  
8. Apache Cassandra 4.1: New SSTable Identifiers, accessed January 27, 2026, [https://cassandra.apache.org/\_/blog/Apache-Cassandra-4.1-New-SSTable-Identifiers.html](https://cassandra.apache.org/_/blog/Apache-Cassandra-4.1-New-SSTable-Identifiers.html)  
9. Storage Engine | Apache Cassandra Documentation, accessed January 27, 2026, [https://cassandra.apache.org/doc/latest/cassandra/architecture/storage-engine.html](https://cassandra.apache.org/doc/latest/cassandra/architecture/storage-engine.html)  
10. Is there a way to recreate lost SSTable index and other component files?, accessed January 27, 2026, [https://dba.stackexchange.com/questions/329084/is-there-a-way-to-recreate-lost-sstable-index-and-other-component-files](https://dba.stackexchange.com/questions/329084/is-there-a-way-to-recreate-lost-sstable-index-and-other-component-files)  
11. sstableupgrade \- AxonOps, accessed January 27, 2026, [https://axonops.com/docs/data-platforms/cassandra/operations/sstable-management/sstableupgrade/](https://axonops.com/docs/data-platforms/cassandra/operations/sstable-management/sstableupgrade/)  
12. Why Bloom filters cannot handle range queries? \- Stack Overflow, accessed January 27, 2026, [https://stackoverflow.com/questions/51153692/why-bloom-filters-cannot-handle-range-queries](https://stackoverflow.com/questions/51153692/why-bloom-filters-cannot-handle-range-queries)  
13. Understanding the Log-Structured Merge (LSM) Tree: A Deep Dive into Efficient Data Storage | by mandeep singh | Medium, accessed January 27, 2026, [https://medium.com/@mndpsngh21/understanding-the-log-structured-merge-lsm-tree-a-deep-dive-into-efficient-data-storage-d7ef3a7562ba](https://medium.com/@mndpsngh21/understanding-the-log-structured-merge-lsm-tree-a-deep-dive-into-efficient-data-storage-d7ef3a7562ba)  
14. WAL-mode File Format \- SQLite, accessed January 27, 2026, [https://sqlite.org/walformat.html](https://sqlite.org/walformat.html)  
15. Write-Ahead Logging, accessed January 27, 2026, [https://tool.oschina.net/uploads/apidocs/sqlite/wal.html](https://tool.oschina.net/uploads/apidocs/sqlite/wal.html)  
16. Log Databases Done Right, accessed January 27, 2026, [https://sathishsaravanan.com/blog/bitcask-log-databases/](https://sathishsaravanan.com/blog/bitcask-log-databases/)  
17. Bitcask \- Riak Documentation, accessed January 27, 2026, [https://docs.riak.com/riak/kv/2.2.3/setup/planning/backend/bitcask/index.html](https://docs.riak.com/riak/kv/2.2.3/setup/planning/backend/bitcask/index.html)  
18. Bitcask \- A Log-Structured fast KV store \- Arpit Bhayani, accessed January 27, 2026, [https://arpitbhayani.me/blogs/bitcask/](https://arpitbhayani.me/blogs/bitcask/)  
19. Build a BLAZINGLY FAST key-value store with Rust \- ltungv, accessed January 27, 2026, [https://www.tunglevo.com/note/build-a-blazingly-fast-key-value-store-with-rust/](https://www.tunglevo.com/note/build-a-blazingly-fast-key-value-store-with-rust/)  
20. bitcask package \- go.mills.io/bitcask/v2 \- Go Packages, accessed January 27, 2026, [https://pkg.go.dev/go.mills.io/bitcask/v2](https://pkg.go.dev/go.mills.io/bitcask/v2)  
21. WiscKey: Separating Keys from Values in SSD-conscious Storage \- USENIX, accessed January 27, 2026, [https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf)  
22. WiscKey: Separating Keys from Values in SSD-Conscious Storage \- tech-lessons.in, accessed January 27, 2026, [https://tech-lessons.in/en/blog/wisckey\_ssd\_conscious\_key\_value\_store/](https://tech-lessons.in/en/blog/wisckey_ssd_conscious_key_value_store/)  
23. Paper Notes: WiscKey – Separating Keys from Values in SSD-conscious Storage, accessed January 27, 2026, [https://distributed-computing-musings.com/2022/07/paper-notes-wisckey-separating-keys-from-values-in-ssd-conscious-storage/](https://distributed-computing-musings.com/2022/07/paper-notes-wisckey-separating-keys-from-values-in-ssd-conscious-storage/)  
24. An Introduction to Bε-trees and Write-Optimization \- Stony Brook Computer Science, accessed January 27, 2026, [https://www3.cs.stonybrook.edu/\~bender/newpub/2015-BenderFaJa-login-wods.pdf](https://www3.cs.stonybrook.edu/~bender/newpub/2015-BenderFaJa-login-wods.pdf)  
25. Bε-Tree \- B-Epsilon Tree \- Beta-Epsilon Tree \- Blocks and Files, accessed January 27, 2026, [https://blocksandfiles.com/2022/06/29/b%CE%B5-tree-b-epsilon-tree-beta-epsilon-tree/](https://blocksandfiles.com/2022/06/29/b%CE%B5-tree-b-epsilon-tree-beta-epsilon-tree/)  
26. SuRF: Practical Range Query Filtering with Fast Succinct Tries \- Parallel Data Lab, accessed January 27, 2026, [https://www.pdl.cmu.edu/PDL-FTP/Storage/surf\_sigmod18.pdf](https://www.pdl.cmu.edu/PDL-FTP/Storage/surf_sigmod18.pdf)  
27. Succinct Range Filters \- Communications of the ACM, accessed January 27, 2026, [https://cacm.acm.org/research/succinct-range-filters/](https://cacm.acm.org/research/succinct-range-filters/)  
28. Optimizing Performance and Storage of Memory-Mapped Persistent Data Structures \- VTechWorks, accessed January 27, 2026, [https://vtechworks.lib.vt.edu/bitstreams/b87d1696-381a-48fa-8168-e19b7e033ea0/download](https://vtechworks.lib.vt.edu/bitstreams/b87d1696-381a-48fa-8168-e19b7e033ea0/download)  
29. 3.4. Shared Memory With Memory-mapped Files \- Computer Science \- JMU, accessed January 27, 2026, [https://w3.cs.jmu.edu/kirkpams/OpenCSF/Books/csf/html/MMap.html](https://w3.cs.jmu.edu/kirkpams/OpenCSF/Books/csf/html/MMap.html)  
30. In-Depth Comparison of Linux System Calls: Msync vs. Fsync \- Oreate AI Blog, accessed January 27, 2026, [https://www.oreateai.com/blog/indepth-comparison-of-linux-system-calls-msync-vs-fsync/d8eabc0a7793345b0ab26c974303813e](https://www.oreateai.com/blog/indepth-comparison-of-linux-system-calls-msync-vs-fsync/d8eabc0a7793345b0ab26c974303813e)  
31. Does msync performance depend on the size of the provided range? \- Stack Overflow, accessed January 27, 2026, [https://stackoverflow.com/questions/68832263/does-msync-performance-depend-on-the-size-of-the-provided-range](https://stackoverflow.com/questions/68832263/does-msync-performance-depend-on-the-size-of-the-provided-range)  
32. Understanding when and how to use Memory Mapped Files | by Abhijit Mondal \- Medium, accessed January 27, 2026, [https://mecha-mind.medium.com/understanding-when-and-how-to-use-memory-mapped-files-b94707df30e9](https://mecha-mind.medium.com/understanding-when-and-how-to-use-memory-mapped-files-b94707df30e9)  
33. Lightning Memory-Mapped Database \- Wikipedia, accessed January 27, 2026, [https://en.wikipedia.org/wiki/Lightning\_Memory-Mapped\_Database](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database)  
34. luispedro/diskhash: Diskbased (persistent) hashtable \- GitHub, accessed January 27, 2026, [https://github.com/luispedro/diskhash](https://github.com/luispedro/diskhash)  
35. sstablepartitions | Apache Cassandra 3.0 \- DataStax Docs, accessed January 27, 2026, [https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/tools/toolsSSTablepartitions.html](https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/tools/toolsSSTablepartitions.html)  
36. CQLSSTableWriter (apache-cassandra API) \- Javadoc.io, accessed January 27, 2026, [https://javadoc.io/doc/org.apache.cassandra/cassandra-all/2.2.6/org/apache/cassandra/io/sstable/CQLSSTableWriter.html](https://javadoc.io/doc/org.apache.cassandra/cassandra-all/2.2.6/org/apache/cassandra/io/sstable/CQLSSTableWriter.html)  
37. SSTables : The secret sauce that behind Cassandra's write performance.. | by Abhinav Vinci, accessed January 27, 2026, [https://medium.com/@vinciabhinav7/cassandra-internals-sstables-the-secret-sauce-that-makes-cassandra-super-fast-3d5badac8eaf](https://medium.com/@vinciabhinav7/cassandra-internals-sstables-the-secret-sauce-that-makes-cassandra-super-fast-3d5badac8eaf)  
38. Universal Compaction · facebook/rocksdb Wiki \- GitHub, accessed January 27, 2026, [https://github.com/facebook/rocksdb/wiki/universal-compaction](https://github.com/facebook/rocksdb/wiki/universal-compaction)  
39. Universal Compaction in RocksDB and me \- Small Datum, accessed January 27, 2026, [http://smalldatum.blogspot.com/2023/06/universal-compaction-in-rocksdb-and-me.html](http://smalldatum.blogspot.com/2023/06/universal-compaction-in-rocksdb-and-me.html)