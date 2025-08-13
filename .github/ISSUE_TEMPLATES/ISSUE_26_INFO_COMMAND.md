# Issue #26: 📊 Implement proper SSTable info command

## 🎯 **Priority: HIGH** - Core User Feature

**Status**: Basic command exists but needs comprehensive implementation  
**Impact**: Users need SSTable inspection capabilities  
**Estimated Effort**: 2-3 days  
**Assigned**: TBD  

---

## 📋 **Problem Statement**

The `cqlite info` command currently has a basic implementation but needs to be enhanced to provide comprehensive SSTable file analysis. This command is critical for users to understand their data files before querying or processing them.

Current limitations:
- Limited metadata extraction
- No schema information display
- Missing compression and format details
- No statistics or health information
- Limited output formatting options

## ✅ **Acceptance Criteria**

### **Core Information Display**
- [ ] SSTable format version and Cassandra compatibility
- [ ] File size, creation date, and modification time
- [ ] Compression algorithm and compression ratio
- [ ] Row count estimation and actual statistics
- [ ] Keyspace and table name extraction
- [ ] Partition key and clustering key schema

### **Advanced Metadata**
- [ ] Column definitions with data types
- [ ] Index information (primary, secondary, bloom filter)
- [ ] TTL and tombstone statistics
- [ ] Generation number and level information  
- [ ] Repair and compaction metadata
- [ ] Component file listing (Data.db, Index.db, etc.)

### **Output Formats**
- [ ] Human-readable text format (default)
- [ ] JSON format for programmatic use
- [ ] CSV format for spreadsheet analysis
- [ ] YAML format for configuration files
- [ ] Summary mode for quick overview

### **Error Handling**
- [ ] Graceful handling of corrupted files
- [ ] Clear error messages for unsupported formats
- [ ] Warnings for unusual or problematic configurations
- [ ] Recovery suggestions for common issues

## 🔧 **Technical Requirements**

### **Command Interface**
```bash
# Basic usage
cqlite info /path/to/sstable-directory/
cqlite info /path/to/data.db

# With options
cqlite info --format json /path/to/sstable/
cqlite info --detailed /path/to/sstable/
cqlite info --summary /path/to/sstable/
cqlite info --validate /path/to/sstable/
```

### **Information Categories**

1. **File Information**
   ```rust
   pub struct FileInformation {
       pub path: PathBuf,
       pub size_bytes: u64,
       pub created: SystemTime,
       pub modified: SystemTime,
       pub permissions: String,
       pub checksum: Option<String>,
   }
   ```

2. **Format Information**
   ```rust
   pub struct FormatInformation {
       pub sstable_format: SSTableFormat,
       pub cassandra_version: CassandraVersion,
       pub format_version: String,
       pub big_format: bool,
       pub bti_format: bool,
   }
   ```

3. **Schema Information**
   ```rust
   pub struct SchemaInformation {
       pub keyspace: String,
       pub table: String,
       pub columns: Vec<ColumnDefinition>,
       pub partition_keys: Vec<String>,
       pub clustering_keys: Vec<ClusteringColumn>,
       pub indexes: Vec<IndexDefinition>,
   }
   ```

4. **Statistics Information**
   ```rust
   pub struct StatisticsInformation {
       pub estimated_row_count: u64,
       pub actual_row_count: Option<u64>,
       pub partition_count: u64,
       pub tombstone_count: u64,
       pub min_timestamp: Option<i64>,
       pub max_timestamp: Option<i64>,
       pub compression_ratio: Option<f64>,
   }
   ```

### **Implementation Structure**
```rust
pub struct InfoCommand {
    reader: SSTableReader,
    format: OutputFormat,
    detailed: bool,
    validate: bool,
}

impl InfoCommand {
    pub async fn execute(&self, path: &Path) -> Result<InfoResult> {
        let file_info = self.analyze_file(path).await?;
        let format_info = self.analyze_format(path).await?;
        let schema_info = self.analyze_schema(path).await?;
        let stats_info = self.analyze_statistics(path).await?;
        
        Ok(InfoResult {
            file: file_info,
            format: format_info, 
            schema: schema_info,
            statistics: stats_info,
        })
    }
}
```

## 🖥️ **Output Examples**

### **Text Format (Default)**
```
📄 SSTable Information
======================

📁 File Details:
   Path: /data/keyspace1/users-a1b2c3d4e5f6/mc-1-big-Data.db
   Size: 2.3 GB (2,456,789,123 bytes)
   Created: 2024-01-15 14:30:22 UTC
   Modified: 2024-01-15 14:35:45 UTC

🔧 Format Details:
   SSTable Format: Big Format (Cassandra 5.0+)
   Version: mc (5.0.0)
   BTI Format: Yes
   Compression: LZ4 (ratio: 3.2:1)

📋 Schema Information:
   Keyspace: ecommerce
   Table: users
   Partition Key: user_id (UUID)
   Clustering Keys: created_at (TIMESTAMP DESC)
   
   Columns:
   ├── user_id (UUID, partition key)
   ├── created_at (TIMESTAMP, clustering key)
   ├── name (TEXT)
   ├── email (TEXT) 
   ├── preferences (MAP<TEXT, TEXT>)
   └── last_login (TIMESTAMP)

📊 Statistics:
   Estimated Rows: 1,234,567
   Partitions: 987,654
   Tombstones: 1,234 (0.1%)
   Data Range: 2023-01-01 to 2024-01-15
   Average Row Size: 1.8 KB
   
✅ Health Status: Good
   - All components present
   - No corruption detected  
   - Compression working normally
```

### **JSON Format**
```json
{
  "file": {
    "path": "/data/keyspace1/users-a1b2c3d4e5f6/mc-1-big-Data.db",
    "size_bytes": 2456789123,
    "created": "2024-01-15T14:30:22Z",
    "modified": "2024-01-15T14:35:45Z"
  },
  "format": {
    "sstable_format": "BigFormat",
    "cassandra_version": "5.0.0",
    "format_version": "mc",
    "big_format": true,
    "bti_format": true,
    "compression": {
      "algorithm": "LZ4",
      "ratio": 3.2
    }
  },
  // ... more structured data
}
```

## 🧪 **Testing Requirements**

### **Unit Tests**
```rust
#[tokio::test]
async fn test_info_command_basic() {
    let test_sstable = create_test_sstable().await?;
    let info_cmd = InfoCommand::new(OutputFormat::Text, false);
    
    let result = info_cmd.execute(&test_sstable).await?;
    
    assert!(!result.file.path.as_os_str().is_empty());
    assert!(result.file.size_bytes > 0);
    assert_eq!(result.schema.keyspace, "test_keyspace");
    assert_eq!(result.schema.table, "test_table");
}

#[tokio::test]  
async fn test_info_command_json_output() {
    let test_sstable = create_test_sstable().await?;
    let info_cmd = InfoCommand::new(OutputFormat::Json, false);
    
    let result = info_cmd.execute(&test_sstable).await?;
    let json_output = result.to_json()?;
    
    // Validate JSON structure
    assert!(json_output.contains("\"format\""));
    assert!(json_output.contains("\"schema\"")); 
    assert!(json_output.contains("\"statistics\""));
}
```

### **Integration Tests**
```rust
#[tokio::test]
async fn test_info_command_with_real_data() {
    let test_files = generate_real_test_sstables().await?;
    
    for test_file in test_files {
        let output = Command::new("./target/debug/cqlite")
            .args(&["info", &test_file.path])
            .output()
            .await?;
            
        assert!(output.status.success());
        
        let stdout = String::from_utf8(output.stdout)?;
        assert!(stdout.contains("SSTable Information"));
        assert!(stdout.contains("File Details"));
        assert!(stdout.contains("Schema Information"));
    }
}
```

### **Error Handling Tests**
```rust
#[tokio::test]
async fn test_info_command_corrupted_file() {
    let corrupted_file = create_corrupted_sstable().await?;
    let info_cmd = InfoCommand::new(OutputFormat::Text, false);
    
    let result = info_cmd.execute(&corrupted_file).await;
    
    match result {
        Err(Error::CorruptedFile(msg)) => {
            assert!(msg.contains("corruption detected"));
            assert!(msg.contains("suggestions"));
        }
        _ => panic!("Expected corruption error"),
    }
}
```

## 📖 **User Experience Requirements**

### **Performance**
- [ ] Info display completes in < 2 seconds for typical files
- [ ] Large files (>1GB) analyzed in < 10 seconds
- [ ] Streaming analysis for very large files to avoid memory issues
- [ ] Responsive feedback for long-running operations

### **Usability**
- [ ] Clear, well-formatted output that's easy to read
- [ ] Helpful error messages with recovery suggestions
- [ ] Consistent terminology with Cassandra documentation
- [ ] Intuitive command-line options and help text

### **Reliability**
- [ ] Works with all supported SSTable formats
- [ ] Handles edge cases gracefully (empty files, unusual schemas)
- [ ] Provides partial information when some metadata is unavailable
- [ ] Never crashes or hangs on any input file

## 🚀 **Implementation Plan**

### **Phase 1: Core Implementation (Days 1-2)**
1. Implement basic file information extraction
2. Add format detection and version reporting
3. Create schema information extraction
4. Implement text output formatting

### **Phase 2: Advanced Features (Days 2-3)**
1. Add statistics calculation and reporting
2. Implement multiple output formats (JSON, CSV, YAML)
3. Add detailed and summary modes
4. Implement validation and health checking

### **Phase 3: Polish and Testing (Day 3)**
1. Comprehensive error handling and user experience
2. Performance optimization for large files  
3. Complete test suite and edge case handling
4. Documentation and help system updates

## 📊 **Success Metrics**

### **Functionality**
- [ ] Successfully analyzes 100% of valid SSTable files
- [ ] Provides accurate information for all supported formats
- [ ] Handles error cases gracefully with helpful messages
- [ ] All output formats produce valid, parseable results

### **Performance**
- [ ] Analysis completes in < 2s for files up to 100MB
- [ ] Memory usage stays under 64MB regardless of file size
- [ ] CPU usage remains reasonable during analysis

### **Quality**
- [ ] Unit test coverage > 95% for info command logic
- [ ] Integration test coverage > 90% for real file scenarios
- [ ] Zero crashes or hangs during extensive testing
- [ ] Consistent output format across different platforms

## 💡 **Dependencies**

- **Requires**: Core SSTable reading functionality (#25)
- **Enables**: REPL info command functionality (#24)
- **Related**: Query execution engine (#27)
- **Blocks**: Advanced CLI features

---

**Labels**: `high-priority`, `core`, `cli`, `user-experience`, `phase-1`  
**Milestone**: Core Functionality  
**Dependencies**: SSTable reading validation (#25)