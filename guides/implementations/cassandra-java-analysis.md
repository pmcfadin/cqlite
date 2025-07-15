# Cassandra Java Implementation Analysis

## 🎯 BigFormat, SSTableReader, SSTableWriter Mastery

This analysis provides deep insights into Cassandra's Java implementation, extracted from comprehensive code analysis and optimized for Rust translation.

## 🏗️ BigFormat Class Hierarchy

### **Core Architecture**
```java
// org.apache.cassandra.io.sstable.format.big.BigFormat
public class BigFormat implements SSTableFormat<BigTableReader, BigTableWriter> {
    public static final BigFormat instance = new BigFormat();
    public static final Version latestVersion = new Version("oa");
    
    @Override
    public BigTableReader openReader(Descriptor descriptor, ...) {
        return BigTableReader.open(descriptor, components, metadata);
    }
    
    @Override  
    public BigTableWriter openWriter(Descriptor descriptor, ...) {
        return new BigTableWriter(descriptor, keyCount, repairedAt, metadata, ...);
    }
}
```

### **Key Design Patterns for Rust**
1. **Singleton Pattern** → Use `static` or `once_cell::sync::Lazy`
2. **Version Handling** → Enum with format detection
3. **Factory Methods** → Associated functions on structs
4. **Resource Management** → RAII with Drop trait

## 📖 SSTableReader Deep Dive

### **BigTableReader Implementation**
```java
public class BigTableReader extends SSTableReader {
    private final MappedByteBuffer dataBuffer;
    private final PartitionIndex partitionIndex;
    private final CompressionMetadata compressionMetadata;
    private final BloomFilter bloomFilter;
    
    public UnfilteredRowIterator iterator(DecoratedKey key, Slices slices, ...) {
        // 1. Check bloom filter first
        if (!bloomFilter.isPresent(key.getKey())) {
            return EmptyIterators.unfilteredRow(metadata());
        }
        
        // 2. Find partition in index
        RowIndexEntry indexEntry = getPosition(key, SSTableReader.Operator.EQ);
        if (indexEntry == null) {
            return EmptyIterators.unfilteredRow(metadata());
        }
        
        // 3. Seek to position and read data
        return createIterator(indexEntry, slices, ...);
    }
}
```

### **Critical Rust Translation Patterns**

#### **1. Memory-Mapped File Access**
```rust
// Rust equivalent using memmap2
use memmap2::Mmap;

pub struct BigTableReader {
    data_mmap: Mmap,
    partition_index: PartitionIndex,
    compression_metadata: Option<CompressionMetadata>,
    bloom_filter: Option<BloomFilter>,
}

impl BigTableReader {
    pub fn open(descriptor: &Descriptor) -> Result<Self> {
        let file = File::open(&descriptor.data_file)?;
        let data_mmap = unsafe { Mmap::map(&file)? };
        
        // Load auxiliary components
        let partition_index = PartitionIndex::load(&descriptor.index_file)?;
        let bloom_filter = BloomFilter::load(&descriptor.filter_file)?;
        
        Ok(Self { data_mmap, partition_index, bloom_filter })
    }
}
```

#### **2. Bloom Filter Integration**
```rust
impl BigTableReader {
    pub fn query_partition(&self, key: &PartitionKey) -> Result<Option<PartitionIterator>> {
        // Fast negative lookup via bloom filter
        if let Some(ref filter) = self.bloom_filter {
            if !filter.might_contain(key) {
                return Ok(None);
            }
        }
        
        // Index lookup for positive/unknown cases
        if let Some(index_entry) = self.partition_index.get(key) {
            Ok(Some(self.create_partition_iterator(index_entry)?))
        } else {
            Ok(None)
        }
    }
}
```

### **Performance Insights from Java Implementation**

#### **Caching Strategy**
```java
// Java caching patterns
private final ConcurrentHashMap<DecoratedKey, RowIndexEntry> keyCache;
private final ChunkCache chunkCache;

// Rust equivalent
use lru::LruCache;
use parking_lot::Mutex;

struct ReaderCaches {
    key_cache: Mutex<LruCache<PartitionKey, RowIndexEntry>>,
    chunk_cache: Mutex<LruCache<ChunkId, DecompressedChunk>>,
}
```

#### **I/O Optimization Patterns**
1. **Sequential reads preferred** over random access
2. **Batch decompression** of multiple chunks
3. **Index caching** for frequently accessed partitions
4. **Lazy loading** of auxiliary components

## ✍️ SSTableWriter Analysis

### **BigTableWriter Implementation**
```java
public class BigTableWriter extends SSTableWriter {
    private final SequentialWriter dataFile;
    private final IndexWriter indexWriter;
    private final BloomFilterWriter bloomWriter;
    private final CompressionWriter compressionWriter;
    
    @Override
    public void append(UnfilteredRowIterator iterator) {
        DecoratedKey key = iterator.partitionKey();
        
        // 1. Update bloom filter
        bloomWriter.add(key.getKey());
        
        // 2. Write partition data
        long startPosition = dataFile.position();
        writePartition(iterator);
        long endPosition = dataFile.position();
        
        // 3. Update index
        indexWriter.append(key, startPosition, endPosition - startPosition);
    }
}
```

### **Write Path Optimization for Rust**

#### **Streaming Writer Design**
```rust
pub struct BigTableWriter {
    data_writer: BufWriter<File>,
    index_builder: IndexBuilder,
    bloom_builder: BloomFilterBuilder,
    compression_writer: Option<CompressionWriter>,
    stats_collector: StatisticsCollector,
}

impl BigTableWriter {
    pub fn append_partition(&mut self, partition: PartitionData) -> Result<()> {
        let start_pos = self.data_writer.stream_position()?;
        
        // Add to bloom filter
        self.bloom_builder.add(&partition.key);
        
        // Write compressed data
        let compressed_size = if let Some(ref mut comp) = self.compression_writer {
            comp.write_partition(&mut self.data_writer, &partition)?
        } else {
            self.data_writer.write_all(&partition.data)?;
            partition.data.len()
        };
        
        // Update index
        self.index_builder.add_entry(
            partition.key,
            start_pos,
            compressed_size,
            partition.stats
        );
        
        // Collect statistics
        self.stats_collector.update(&partition);
        
        Ok(())
    }
}
```

#### **Atomic File Creation**
```rust
impl BigTableWriter {
    pub fn finish(mut self) -> Result<SSTableDescriptor> {
        // 1. Finalize data file
        self.data_writer.flush()?;
        let data_file = self.data_writer.into_inner()?;
        
        // 2. Write index file
        let index_file = self.index_builder.write_to_file(&self.descriptor.index_path)?;
        
        // 3. Write bloom filter
        let filter_file = self.bloom_builder.write_to_file(&self.descriptor.filter_path)?;
        
        // 4. Write statistics
        let stats_file = self.stats_collector.write_to_file(&self.descriptor.stats_path)?;
        
        // 5. Write TOC (table of contents)
        self.write_toc_file()?;
        
        // 6. Sync all files atomically
        sync_directory(&self.descriptor.directory)?;
        
        Ok(self.descriptor)
    }
}
```

## 🔧 Critical Implementation Details

### **Error Handling Patterns**
```java
// Java exception patterns
try {
    return reader.getPosition(key, SSTableReader.Operator.EQ);
} catch (IOException e) {
    throw new RuntimeException("Failed to read index", e);
}

// Rust equivalent with better error context
impl BigTableReader {
    fn get_position(&self, key: &PartitionKey) -> Result<Option<RowIndexEntry>> {
        self.partition_index
            .lookup(key)
            .with_context(|| format!("Failed to lookup partition key: {:?}", key))
    }
}
```

### **Resource Management**
```java
// Java try-with-resources
try (SSTableReader reader = SSTableReader.open(descriptor)) {
    return reader.iterator(key, slices);
}

// Rust RAII automatic cleanup
{
    let reader = BigTableReader::open(&descriptor)?;
    reader.partition_iterator(key, slices)
} // reader automatically closed via Drop
```

### **Serialization Compatibility**
```java
// Java serialization patterns
public void serialize(DataOutputPlus out) throws IOException {
    out.writeInt(magic);
    out.writeByte(version);
    out.writeUTF(keyspace);
    out.writeUTF(table);
}

// Rust equivalent with exact compatibility
impl CassandraSerialize for SSTableHeader {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_u32::<BigEndian>(self.magic)?;
        writer.write_u8(self.version)?;
        write_utf8_string(writer, &self.keyspace)?;
        write_utf8_string(writer, &self.table)?;
        Ok(())
    }
}
```

## 🎯 Key Takeaways for CQLite

### **1. Architecture Decisions**
- **Single format support** (BigFormat only) simplifies implementation
- **Memory-mapped access** crucial for performance
- **Lazy loading** of components enables fast startup
- **Caching strategy** essential for read performance

### **2. Performance Optimizations**
- **Bloom filter first** for negative lookups
- **Index caching** for hot partitions
- **Batch operations** where possible
- **Sequential I/O preferred** over random access

### **3. Error Handling**
- **Fail fast** on unsupported formats
- **Corruption detection** via checksums
- **Partial recovery** where possible
- **Clear error context** for debugging

### **4. Compatibility Requirements**
- **Exact serialization format** matching
- **Endianness handling** for cross-platform
- **Version detection** and validation
- **Component file relationships** maintained

---

*This analysis extracts the essential patterns from Cassandra's Java implementation that are directly applicable to building a high-performance Rust equivalent, focusing on compatibility and performance optimization.*