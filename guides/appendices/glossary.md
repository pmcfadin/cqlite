# CQLite Technical Glossary

## 🎯 Cassandra & SSTable Terms

### **SSTable (Sorted String Table)**
Immutable data file format used by Cassandra for persistent storage. Contains sorted key-value pairs with associated metadata.

### **Partition Key**
Primary key component that determines data distribution across nodes. Used to locate which SSTable contains specific data.

### **Clustering Key**
Secondary key component that determines sort order within a partition. Multiple rows can share same partition key but have different clustering keys.

### **Cell**
Smallest unit of data in Cassandra, representing a single column value with timestamp and optional TTL.

### **Tombstone**
Special marker indicating deleted data. Retained until compaction to ensure distributed delete consistency.

### **Memtable**
In-memory write buffer that gets flushed to SSTable when full. Not directly relevant to CQLite but understanding helps with SSTable structure.

### **CQL (Cassandra Query Language)**
SQL-like language for interacting with Cassandra. Version 3 is current standard.

### **UDT (User Defined Type)**
Custom composite data type defined by users, similar to structs. Serialized as nested structures in SSTables.

### **TTL (Time To Live)**
Expiration time for data. Cells with expired TTL become tombstones.

### **Token**
Hash value of partition key used for data distribution. Determines which node owns data in cluster.

## 📦 File Format Terms

### **BigFormat**
Standard SSTable format introduced in Cassandra 3.0, using file extensions like -big-Data.db.

### **Data.db**
Main SSTable component containing actual row data in sorted order by partition key.

### **Index.db**
Maps partition keys to their byte positions in Data.db file for efficient lookups.

### **Summary.db**
Sampling of Index.db entries kept in memory for faster partition location.

### **Statistics.db**
Metadata about SSTable contents: min/max timestamps, tombstone counts, column statistics.

### **CompressionInfo.db**
Metadata about compression chunks: offsets, lengths, and checksums.

### **Filter.db**
Bloom filter for fast negative lookups - probabilistically determines if partition exists.

### **TOC.txt**
Table of Contents - lists all component files belonging to an SSTable.

### **Digest.crc32**
CRC32 checksum of Data.db file for integrity verification.

## 🔧 Technical Terms

### **VInt (Variable Integer)**
Space-efficient integer encoding where small values use fewer bytes. First byte indicates total length.

### **Zero-Copy Deserialization**
Parsing technique that avoids copying data, instead providing direct references into memory-mapped files.

### **Bloom Filter**
Probabilistic data structure for set membership testing. Can have false positives but no false negatives.

### **Memory-Mapped I/O**
Technique mapping file contents directly into process memory space, enabling efficient random access.

### **SIMD (Single Instruction Multiple Data)**
CPU instructions that perform same operation on multiple data points simultaneously for performance.

### **Row Cache**
In-memory cache of frequently accessed rows. CQLite implements similar caching at parsing level.

### **Compaction**
Process merging multiple SSTables into one, removing tombstones and duplicate data. CQLite reads but doesn't compact.

### **Endianness**
Byte ordering in multi-byte values. Cassandra uses big-endian (network byte order) for compatibility.

## 🏗️ Rust-Specific Terms

### **nom**
Parser combinator library used for building composable parsers in Rust. Core of CQLite parsing logic.

### **serde**
Serialization framework for Rust. Used for converting between Rust types and CQL formats.

### **FFI (Foreign Function Interface)**
Mechanism for Rust code to be called from other languages like C, Python, or JavaScript.

### **WASM (WebAssembly)**
Portable binary format enabling Rust code to run in web browsers and other environments.

### **Cargo**
Rust's package manager and build system. Manages dependencies and compilation.

### **Crate**
Rust's compilation unit and package format. CQLite consists of multiple crates.

### **Trait**
Rust's interface mechanism defining shared behavior. Used extensively for type conversions.

### **Lifetime**
Rust's mechanism for tracking reference validity. Critical for zero-copy operations.

## 📊 Performance Terms

### **Throughput**
Amount of data processed per unit time. Key metric for SSTable parsing performance.

### **Latency**
Time to complete single operation. Critical for partition lookup performance.

### **Cache Line**
CPU cache unit (typically 64 bytes). Aligning data structures improves performance.

### **False Sharing**
Performance degradation when multiple threads access different data in same cache line.

### **Branch Prediction**
CPU optimization guessing which code path will execute. Parser design considers this.

### **Prefetching**
Loading data into cache before it's needed. Memory-mapped files enable OS-level prefetching.

## 🔄 Cassandra Compatibility Terms

### **Format Version**
SSTable format identifier. CQLite targets 'oa' (Cassandra 5.0+) format.

### **Gossip**
Cassandra's peer-to-peer communication protocol. Not relevant to CQLite but appears in metadata.

### **Snitch**
Cassandra's datacenter/rack awareness mechanism. May appear in SSTable metadata.

### **Consistency Level**
Distributed system concept affecting how data is written. Visible in SSTable timestamps.

### **Vector Clock**
Distributed timing mechanism. Understanding helps with timestamp interpretation.

## 🛠️ Development Terms

### **Property-Based Testing**
Testing approach generating random inputs to verify properties hold. Used extensively in CQLite.

### **Fuzzing**
Automated testing with random/malformed input to find crashes and vulnerabilities.

### **Benchmark**
Performance test measuring execution time and resource usage under controlled conditions.

### **CI/CD**
Continuous Integration/Deployment - automated testing and release pipeline.

### **MSRV (Minimum Supported Rust Version)**
Oldest Rust compiler version that can build the project. CQLite targets stable Rust.

---

*This glossary covers key terms needed to understand CQLite's implementation and Cassandra's SSTable format. Terms are grouped by category for easy reference.*