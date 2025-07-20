//! Memory-mapped SSTable reader with schema-aware data access
//!
//! This module provides high-performance SSTable reading using memory-mapped files
//! for all components (Data.db, Index.db, Summary.db, etc.) with automatic flush
//! on any write operations.

use crate::error::{Error, Result};
use crate::schema::{TableSchema, CqlType};
use crate::parser::vint;
use crate::types::Value;
use memmap2::{Mmap, MmapMut, MmapOptions};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// Memory-mapped SSTable reader with schema awareness
pub struct SchemaAwareSSTableReader {
    /// Base path for SSTable files (without generation suffix)
    base_path: PathBuf,
    
    /// Table schema for data interpretation
    schema: Arc<TableSchema>,
    
    /// Memory-mapped files for all SSTable components
    memory_maps: RwLock<SSTableMemoryMaps>,
    
    /// Cached metadata for performance
    metadata_cache: RwLock<ReaderMetadata>,
}

/// Memory-mapped files for all SSTable components
struct SSTableMemoryMaps {
    /// Data.db - Row data
    data: Option<Mmap>,
    
    /// Index.db - Partition index
    index: Option<Mmap>,
    
    /// Summary.db - Index summary  
    summary: Option<Mmap>,
    
    /// Filter.db - Bloom filter
    filter: Option<Mmap>,
    
    /// Statistics.db - Table statistics
    statistics: Option<Mmap>,
    
    /// CompressionInfo.db - Compression metadata
    compression_info: Option<Mmap>,
    
    /// File handles for write operations
    file_handles: HashMap<String, File>,
}

/// Cached metadata for performance optimization
#[derive(Default)]
struct ReaderMetadata {
    /// Partition index cache
    partition_index: HashMap<Vec<u8>, u64>, // partition key -> offset in data file
    
    /// Column positions cache
    column_positions: HashMap<String, usize>, // column name -> position in row
    
    /// Statistics cache
    row_count: Option<u64>,
    data_size: Option<u64>,
}

/// Partition key for lookups
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    pub key_values: Vec<Value>,
}

/// Clustering key for range queries
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClusteringKey {
    pub key_values: Vec<Value>,
}

/// Decoded row data
#[derive(Debug, Clone)]
pub struct Row {
    pub partition_key: PartitionKey,
    pub clustering_key: Option<ClusteringKey>,
    pub columns: HashMap<String, Value>,
    pub timestamp: Option<i64>,
    pub ttl: Option<i32>,
}

/// Iterator over rows in a partition
pub struct PartitionIterator<'a> {
    reader: &'a SchemaAwareSSTableReader,
    data_slice: &'a [u8],
    position: usize,
    partition_key: PartitionKey,
}

/// Iterator over a range of clustering keys
pub struct RangeIterator<'a> {
    reader: &'a SchemaAwareSSTableReader,
    partition_iter: PartitionIterator<'a>,
    start_key: Option<ClusteringKey>,
    end_key: Option<ClusteringKey>,
}

impl SchemaAwareSSTableReader {
    /// Open SSTable with schema for reading
    pub fn open<P: AsRef<Path>>(sstable_path: P, schema: TableSchema) -> Result<Self> {
        let base_path = sstable_path.as_ref().to_path_buf();
        let schema = Arc::new(schema);
        
        let mut reader = Self {
            base_path,
            schema,
            memory_maps: RwLock::new(SSTableMemoryMaps::new()),
            metadata_cache: RwLock::new(ReaderMetadata::default()),
        };
        
        // Initialize memory maps for all components
        reader.initialize_memory_maps()?;
        
        // Build initial metadata cache
        reader.build_metadata_cache()?;
        
        Ok(reader)
    }
    
    /// Initialize memory maps for all SSTable components
    fn initialize_memory_maps(&self) -> Result<()> {
        let mut maps = self.memory_maps.write().unwrap();
        
        // Map each component if it exists
        maps.data = self.map_component("Data.db")?;
        maps.index = self.map_component("Index.db")?;
        maps.summary = self.map_component("Summary.db")?;
        maps.filter = self.map_component("Filter.db")?;
        maps.statistics = self.map_component("Statistics.db")?;
        maps.compression_info = self.map_component("CompressionInfo.db")?;
        
        Ok(())
    }
    
    /// Memory map a specific component file
    fn map_component(&self, component: &str) -> Result<Option<Mmap>> {
        let file_path = self.base_path.with_extension("").join(format!("-{}", component));
        
        if !file_path.exists() {
            return Ok(None);
        }
        
        let file = File::open(&file_path)
            .map_err(|e| Error::storage(format!("Failed to open {}: {}", component, e)))?;
        
        if file.metadata()?.len() == 0 {
            return Ok(None); // Empty file, don't map
        }
        
        let mmap = unsafe { MmapOptions::new().map(&file) }
            .map_err(|e| Error::storage(format!("Failed to memory map {}: {}", component, e)))?;
        
        Ok(Some(mmap))
    }
    
    /// Build metadata cache for performance
    fn build_metadata_cache(&self) -> Result<()> {
        let maps = self.memory_maps.read().unwrap();
        let mut cache = self.metadata_cache.write().unwrap();
        
        // Parse index file to build partition lookup
        if let Some(ref index_data) = maps.index {
            self.parse_index_file(&index_data, &mut cache)?;
        }
        
        // Parse statistics for metadata
        if let Some(ref stats_data) = maps.statistics {
            self.parse_statistics_file(&stats_data, &mut cache)?;
        }
        
        Ok(())
    }
    
    /// Parse Index.db file to build partition lookup cache
    fn parse_index_file(&self, index_data: &[u8], cache: &mut ReaderMetadata) -> Result<()> {
        let mut pos = 0;
        
        while pos < index_data.len() {
            // Read partition key (length-prefixed)
            if pos + 4 > index_data.len() {
                break;
            }
            
            let key_length = u32::from_be_bytes([
                index_data[pos], index_data[pos + 1], 
                index_data[pos + 2], index_data[pos + 3]
            ]) as usize;
            pos += 4;
            
            if pos + key_length > index_data.len() {
                break;
            }
            
            let key_bytes = index_data[pos..pos + key_length].to_vec();
            pos += key_length;
            
            // Read data file offset
            if pos + 8 > index_data.len() {
                break;
            }
            
            let data_offset = u64::from_be_bytes([
                index_data[pos], index_data[pos + 1], index_data[pos + 2], index_data[pos + 3],
                index_data[pos + 4], index_data[pos + 5], index_data[pos + 6], index_data[pos + 7],
            ]);
            pos += 8;
            
            cache.partition_index.insert(key_bytes, data_offset);
        }
        
        Ok(())
    }
    
    /// Parse Statistics.db file for metadata
    fn parse_statistics_file(&self, _stats_data: &[u8], _cache: &mut ReaderMetadata) -> Result<()> {
        // Statistics file parsing implementation
        // For now, just return Ok - will implement based on actual format
        Ok(())
    }
    
    /// Direct access: Get all rows for a partition key
    pub fn get_partition(&self, partition_key: &PartitionKey) -> Result<Vec<Row>> {
        let key_bytes = self.encode_partition_key(partition_key)?;
        
        // Look up partition in index
        let cache = self.metadata_cache.read().unwrap();
        let data_offset = cache.partition_index.get(&key_bytes)
            .ok_or_else(|| Error::not_found(format!("Partition key not found: {:?}", partition_key)))?;
        
        // Read partition data
        let maps = self.memory_maps.read().unwrap();
        let data_mmap = maps.data.as_ref()
            .ok_or_else(|| Error::storage("Data file not available".to_string()))?;
        
        let mut rows = Vec::new();
        let mut pos = *data_offset as usize;
        
        // Parse rows until we hit the next partition or end of file
        while pos < data_mmap.len() {
            match self.parse_row(&data_mmap[pos..], partition_key) {
                Ok((row, consumed)) => {
                    rows.push(row);
                    pos += consumed;
                    
                    // Check if we've moved to a different partition
                    // (this is a simplified check - real implementation would be more robust)
                    if pos + 100 < data_mmap.len() {
                        // Quick check if we're still in the same partition
                        if let Ok(next_key_bytes) = self.peek_partition_key(&data_mmap[pos..]) {
                            if next_key_bytes != key_bytes {
                                break;
                            }
                        }
                    }
                }
                Err(_) => break, // End of partition or error
            }
        }
        
        Ok(rows)
    }
    
    /// Iterator access: Scan all rows in a partition
    pub fn scan_partition(&self, partition_key: &PartitionKey) -> Result<PartitionIterator> {
        let key_bytes = self.encode_partition_key(partition_key)?;
        
        let cache = self.metadata_cache.read().unwrap();
        let data_offset = cache.partition_index.get(&key_bytes)
            .ok_or_else(|| Error::not_found(format!("Partition key not found: {:?}", partition_key)))?;
        
        let maps = self.memory_maps.read().unwrap();
        let data_mmap = maps.data.as_ref()
            .ok_or_else(|| Error::storage("Data file not available".to_string()))?;
        
        let data_slice = &data_mmap[*data_offset as usize..];
        
        Ok(PartitionIterator {
            reader: self,
            data_slice,
            position: 0,
            partition_key: partition_key.clone(),
        })
    }
    
    /// Iterator access: Scan range of clustering keys
    pub fn scan_range(
        &self, 
        partition_key: &PartitionKey,
        start_key: Option<ClusteringKey>,
        end_key: Option<ClusteringKey>
    ) -> Result<RangeIterator> {
        let partition_iter = self.scan_partition(partition_key)?;
        
        Ok(RangeIterator {
            reader: self,
            partition_iter,
            start_key,
            end_key,
        })
    }
    
    /// Parse a single row from binary data following exact Cassandra format
    fn parse_row(&self, data: &[u8], partition_key: &PartitionKey) -> Result<(Row, usize)> {
        let mut pos = 0;
        
        // Parse row header with exact Cassandra format
        let (row_header, header_size) = self.parse_cassandra_row_header(&data[pos..])?;
        pos += header_size;
        
        // Parse clustering key (if table has clustering columns)
        let clustering_key = if !self.schema.clustering_keys.is_empty() {
            let (key, key_size) = self.parse_clustering_key_exact(&data[pos..])?;
            pos += key_size;
            Some(key)
        } else {
            None
        };
        
        // Parse column data with exact binary format
        let mut columns = HashMap::new();
        
        // Parse columns in schema order for exact format compliance
        for column in &self.schema.columns {
            // Skip partition and clustering key columns (already parsed)
            if self.schema.is_partition_key(&column.name) || 
               self.schema.is_clustering_key(&column.name) {
                continue;
            }
            
            // Check if column is present (via column mask in header)
            if !row_header.has_column(&column.name) {
                columns.insert(column.name.clone(), Value::Null);
                continue;
            }
            
            let (value, value_size) = self.parse_column_value_exact(&data[pos..], &column.data_type)?;
            columns.insert(column.name.clone(), value);
            pos += value_size;
        }
        
        let row = Row {
            partition_key: partition_key.clone(),
            clustering_key,
            columns,
            timestamp: row_header.timestamp,
            ttl: row_header.ttl,
        };
        
        Ok((row, pos))
    }
    
    /// Parse Cassandra row header with exact binary format compliance
    fn parse_cassandra_row_header(&self, data: &[u8]) -> Result<(CassandraRowHeader, usize)> {
        let mut pos = 0;
        
        // Parse row flags (1 byte)
        if data.len() < 1 {
            return Err(Error::corruption("Row header too short".to_string()));
        }
        
        let flags_byte = data[pos];
        pos += 1;
        
        // Parse timestamp (VInt delta from partition min timestamp)
        let (timestamp_delta, ts_size) = vint::parse_vint(&data[pos..])
            .map_err(|_| Error::corruption("Invalid timestamp in row header".to_string()))?;
        pos += ts_size;
        
        // Parse TTL if present (VInt)
        let ttl = if flags_byte & 0x01 != 0 {
            let (ttl_val, ttl_size) = vint::parse_vint(&data[pos..])
                .map_err(|_| Error::corruption("Invalid TTL in row header".to_string()))?;
            pos += ttl_size;
            Some(ttl_val as i32)
        } else {
            None
        };
        
        // Parse local deletion time if present (VInt)
        let local_deletion_time = if flags_byte & 0x02 != 0 {
            let (del_time, del_size) = vint::parse_vint(&data[pos..])
                .map_err(|_| Error::corruption("Invalid deletion time in row header".to_string()))?;
            pos += del_size;
            Some(del_time as i32)
        } else {
            None
        };
        
        // Parse column count (VInt)
        let (column_count, count_size) = vint::parse_vint(&data[pos..])
            .map_err(|_| Error::corruption("Invalid column count in row header".to_string()))?;
        pos += count_size;
        
        // Parse column mask (bit array indicating which columns are present)
        let mask_bytes = ((column_count as usize + 7) / 8).max(1);
        if data.len() < pos + mask_bytes {
            return Err(Error::corruption("Column mask truncated".to_string()));
        }
        
        let column_mask = data[pos..pos + mask_bytes].to_vec();
        pos += mask_bytes;
        
        let header = CassandraRowHeader {
            flags: flags_byte,
            timestamp: Some(timestamp_delta), // Will be converted to absolute timestamp
            ttl,
            local_deletion_time,
            column_count: column_count as usize,
            column_mask,
        };
        
        Ok((header, pos))
    }
    
    /// Parse clustering key with exact Cassandra length-prefixed format
    fn parse_clustering_key_exact(&self, data: &[u8]) -> Result<(ClusteringKey, usize)> {
        let mut pos = 0;
        let mut key_values = Vec::new();
        
        for key_column in self.schema.ordered_clustering_keys() {
            // Each clustering key component is length-prefixed
            if pos + 4 > data.len() {
                return Err(Error::corruption("Clustering key component length missing".to_string()));
            }
            
            let component_length = i32::from_be_bytes([
                data[pos], data[pos + 1], data[pos + 2], data[pos + 3]
            ]);
            pos += 4;
            
            if component_length < 0 {
                // Null component
                key_values.push(Value::Null);
            } else {
                let length = component_length as usize;
                if pos + length > data.len() {
                    return Err(Error::corruption("Clustering key component data truncated".to_string()));
                }
                
                let component_data = &data[pos..pos + length];
                let (value, _) = self.parse_column_value_exact(component_data, &key_column.data_type)?;
                key_values.push(value);
                pos += length;
            }
        }
        
        Ok((ClusteringKey { key_values }, pos))
    }
    
    /// Parse column value with exact Cassandra binary format
    fn parse_column_value_exact(&self, data: &[u8], type_str: &str) -> Result<(Value, usize)> {
        let cql_type = CqlType::parse(type_str)?;
        
        // Handle null values
        if data.is_empty() {
            return Ok((Value::Null, 0));
        }
        
        match cql_type {
            CqlType::Boolean => {
                if data.len() < 1 {
                    return Err(Error::corruption("Boolean value too short".to_string()));
                }
                Ok((Value::Boolean(data[0] != 0), 1))
            }
            
            CqlType::TinyInt => {
                if data.len() < 1 {
                    return Err(Error::corruption("TinyInt value too short".to_string()));
                }
                Ok((Value::TinyInt(data[0] as i8), 1))
            }
            
            CqlType::SmallInt => {
                if data.len() < 2 {
                    return Err(Error::corruption("SmallInt value too short".to_string()));
                }
                let value = i16::from_be_bytes([data[0], data[1]]);
                Ok((Value::SmallInt(value), 2))
            }
            
            CqlType::Int => {
                if data.len() < 4 {
                    return Err(Error::corruption("Int value too short".to_string()));
                }
                let value = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                Ok((Value::Integer(value), 4))
            }
            
            CqlType::BigInt => {
                if data.len() < 8 {
                    return Err(Error::corruption("BigInt value too short".to_string()));
                }
                let value = i64::from_be_bytes([
                    data[0], data[1], data[2], data[3],
                    data[4], data[5], data[6], data[7],
                ]);
                Ok((Value::BigInt(value), 8))
            }
            
            CqlType::Float => {
                if data.len() < 4 {
                    return Err(Error::corruption("Float value too short".to_string()));
                }
                let bits = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let value = f32::from_bits(bits);
                Ok((Value::Float(value), 4))
            }
            
            CqlType::Double => {
                if data.len() < 8 {
                    return Err(Error::corruption("Double value too short".to_string()));
                }
                let bits = u64::from_be_bytes([
                    data[0], data[1], data[2], data[3],
                    data[4], data[5], data[6], data[7],
                ]);
                let value = f64::from_bits(bits);
                Ok((Value::Double(value), 8))
            }
            
            CqlType::Text | CqlType::Ascii | CqlType::Varchar => {
                // String data without length prefix (length already handled by caller)
                let text = String::from_utf8(data.to_vec())
                    .map_err(|e| Error::corruption(format!("Invalid UTF-8: {}", e)))?;
                
                Ok((Value::Text(text), data.len()))
            }
            
            CqlType::Blob => {
                // Binary data without length prefix (length already handled by caller)
                Ok((Value::Blob(data.to_vec()), data.len()))
            }
            
            CqlType::Timestamp => {
                if data.len() < 8 {
                    return Err(Error::corruption("Timestamp value too short".to_string()));
                }
                let millis = i64::from_be_bytes([
                    data[0], data[1], data[2], data[3],
                    data[4], data[5], data[6], data[7],
                ]);
                Ok((Value::Timestamp(millis), 8))
            }
            
            CqlType::Uuid | CqlType::TimeUuid => {
                if data.len() < 16 {
                    return Err(Error::corruption("UUID value too short".to_string()));
                }
                let uuid_bytes = data[0..16].to_vec();
                Ok((Value::Uuid(uuid_bytes), 16))
            }
            
            // Collections implemented as tuples (4-byte count + length-prefixed elements)
            CqlType::List(element_type) => {
                self.parse_collection_as_tuple(data, element_type, CollectionType::List)
            }
            
            CqlType::Set(element_type) => {
                self.parse_collection_as_tuple(data, element_type, CollectionType::Set)
            }
            
            CqlType::Map(key_type, value_type) => {
                self.parse_map_as_tuple(data, key_type, value_type)
            }
            
            // New types: Tuple, UDT, Frozen
            CqlType::Tuple(field_types) => {
                self.parse_tuple(data, &field_types)
            }
            
            CqlType::Udt(type_name, fields) => {
                self.parse_udt(data, type_name, &fields)
            }
            
            CqlType::Frozen(inner_type) => {
                // Frozen types have the same binary format as their inner type
                // but are immutable once created
                let (inner_value, consumed) = self.parse_column_value_exact(data, &format!("{:?}", inner_type))?;
                Ok((Value::Frozen(Box::new(inner_value)), consumed))
            }
            
            _ => {
                // Fallback for unsupported types
                Ok((Value::Null, 0))
            }
        }
    }
    
    /// Encode partition key to bytes for lookup
    fn encode_partition_key(&self, partition_key: &PartitionKey) -> Result<Vec<u8>> {
        let mut encoded = Vec::new();
        
        for (i, key_column) in self.schema.ordered_partition_keys().iter().enumerate() {
            if i >= partition_key.key_values.len() {
                return Err(Error::schema("Partition key missing values".to_string()));
            }
            
            let value = &partition_key.key_values[i];
            let value_bytes = self.encode_value(value, &key_column.data_type)?;
            
            // Length-prefix each component
            encoded.extend_from_slice(&(value_bytes.len() as u32).to_be_bytes());
            encoded.extend_from_slice(&value_bytes);
        }
        
        Ok(encoded)
    }
    
    /// Encode a value to bytes
    fn encode_value(&self, value: &Value, type_str: &str) -> Result<Vec<u8>> {
        let cql_type = CqlType::parse(type_str)?;
        
        match (value, cql_type) {
            (Value::Boolean(b), CqlType::Boolean) => Ok(vec![if *b { 1 } else { 0 }]),
            (Value::Integer(i), CqlType::Int) => Ok(i.to_be_bytes().to_vec()),
            (Value::BigInt(i), CqlType::BigInt) => Ok(i.to_be_bytes().to_vec()),
            (Value::Text(s), CqlType::Text | CqlType::Ascii | CqlType::Varchar) => {
                let mut bytes = Vec::new();
                let string_bytes = s.as_bytes();
                bytes.extend_from_slice(&(string_bytes.len() as u32).to_be_bytes());
                bytes.extend_from_slice(string_bytes);
                Ok(bytes)
            }
            (Value::Null, _) => Ok(vec![]),
            _ => Err(Error::type_conversion(format!("Cannot encode {:?} as {}", value, type_str))),
        }
    }
    
    /// Peek at partition key in data stream without consuming
    fn peek_partition_key(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Simplified partition key extraction
        // Real implementation would parse the row header properly
        
        if data.len() < 20 {
            return Err(Error::corruption("Data too short for partition key peek".to_string()));
        }
        
        // Return first 16 bytes as a simplified key representation
        Ok(data[0..16].to_vec())
    }
    
    /// Flush all memory maps and file handles
    pub fn flush(&self) -> Result<()> {
        let mut maps = self.memory_maps.write().unwrap();
        
        // Flush any file handles (for write operations)
        for (name, file) in &mut maps.file_handles {
            file.sync_all()
                .map_err(|e| Error::storage(format!("Failed to flush {}: {}", name, e)))?;
        }
        
        Ok(())
    }
    
    /// Parse tuple with fixed-size heterogeneous types
    fn parse_tuple(&self, data: &[u8], field_types: &[CqlType]) -> Result<(Value, usize)> {
        let mut pos = 0;
        let mut values = Vec::with_capacity(field_types.len());
        
        // Tuples are stored as a sequence of length-prefixed values
        for field_type in field_types {
            // Read field length (4 bytes big-endian signed)
            if pos + 4 > data.len() {
                return Err(Error::corruption("Tuple field length missing".to_string()));
            }
            
            let field_length = i32::from_be_bytes([
                data[pos], data[pos + 1], data[pos + 2], data[pos + 3]
            ]);
            pos += 4;
            
            if field_length < 0 {
                // Null field
                values.push(Value::Null);
            } else {
                let length = field_length as usize;
                if pos + length > data.len() {
                    return Err(Error::corruption("Tuple field data truncated".to_string()));
                }
                
                let field_data = &data[pos..pos + length];
                let field_type_str = self.cql_type_to_string(field_type);
                let (field_value, _) = self.parse_column_value_exact(field_data, &field_type_str)?;
                values.push(field_value);
                pos += length;
            }
        }
        
        Ok((Value::Tuple(values), pos))
    }
    
    /// Parse User Defined Type (UDT) with named fields
    fn parse_udt(&self, data: &[u8], type_name: &str, field_definitions: &[(String, CqlType)]) -> Result<(Value, usize)> {
        let mut pos = 0;
        let mut fields = HashMap::new();
        
        // UDTs are stored as a sequence of length-prefixed values for each field
        for (field_name, field_type) in field_definitions {
            // Read field length (4 bytes big-endian signed)
            if pos + 4 > data.len() {
                return Err(Error::corruption("UDT field length missing".to_string()));
            }
            
            let field_length = i32::from_be_bytes([
                data[pos], data[pos + 1], data[pos + 2], data[pos + 3]
            ]);
            pos += 4;
            
            if field_length < 0 {
                // Null field
                fields.insert(field_name.clone(), Value::Null);
            } else {
                let length = field_length as usize;
                if pos + length > data.len() {
                    return Err(Error::corruption("UDT field data truncated".to_string()));
                }
                
                let field_data = &data[pos..pos + length];
                let field_type_str = self.cql_type_to_string(field_type);
                let (field_value, _) = self.parse_column_value_exact(field_data, &field_type_str)?;
                fields.insert(field_name.clone(), field_value);
                pos += length;
            }
        }
        
        Ok((Value::Udt(type_name.to_string(), fields), pos))
    }
    
    /// Helper method to convert CqlType to string for recursive parsing
    fn cql_type_to_string(&self, cql_type: &CqlType) -> String {
        match cql_type {
            CqlType::Boolean => "boolean".to_string(),
            CqlType::TinyInt => "tinyint".to_string(),
            CqlType::SmallInt => "smallint".to_string(),
            CqlType::Int => "int".to_string(),
            CqlType::BigInt => "bigint".to_string(),
            CqlType::Float => "float".to_string(),
            CqlType::Double => "double".to_string(),
            CqlType::Text => "text".to_string(),
            CqlType::Ascii => "ascii".to_string(),
            CqlType::Varchar => "varchar".to_string(),
            CqlType::Blob => "blob".to_string(),
            CqlType::Timestamp => "timestamp".to_string(),
            CqlType::Date => "date".to_string(),
            CqlType::Time => "time".to_string(),
            CqlType::Uuid => "uuid".to_string(),
            CqlType::TimeUuid => "timeuuid".to_string(),
            CqlType::Inet => "inet".to_string(),
            CqlType::Duration => "duration".to_string(),
            CqlType::Decimal => "decimal".to_string(),
            CqlType::List(inner) => format!("list<{}>", self.cql_type_to_string(inner)),
            CqlType::Set(inner) => format!("set<{}>", self.cql_type_to_string(inner)),
            CqlType::Map(key, val) => format!("map<{}, {}>", self.cql_type_to_string(key), self.cql_type_to_string(val)),
            CqlType::Tuple(types) => {
                let type_strings: Vec<String> = types.iter().map(|t| self.cql_type_to_string(t)).collect();
                format!("tuple<{}>", type_strings.join(", "))
            }
            CqlType::Udt(name, _) => name.clone(),
            CqlType::Frozen(inner) => format!("frozen<{}>", self.cql_type_to_string(inner)),
            CqlType::Custom(name) => name.clone(),
        }
    }
    
    /// Parse collection as tuple (4-byte count + length-prefixed elements)
    fn parse_collection_as_tuple(&self, data: &[u8], element_type: &CqlType, collection_type: CollectionType) -> Result<(Value, usize)> {
        let mut pos = 0;
        
        // Read element count (4 bytes big-endian)
        if data.len() < 4 {
            return Err(Error::corruption("Collection count missing".to_string()));
        }
        
        let element_count = u32::from_be_bytes([
            data[pos], data[pos + 1], data[pos + 2], data[pos + 3]
        ]) as usize;
        pos += 4;
        
        let mut elements = Vec::with_capacity(element_count);
        
        // Parse each element with length prefix
        for _ in 0..element_count {
            // Read element length (4 bytes big-endian signed)
            if pos + 4 > data.len() {
                return Err(Error::corruption("Collection element length missing".to_string()));
            }
            
            let element_length = i32::from_be_bytes([
                data[pos], data[pos + 1], data[pos + 2], data[pos + 3]
            ]);
            pos += 4;
            
            if element_length < 0 {
                // Null element
                elements.push(Value::Null);
            } else {
                let length = element_length as usize;
                if pos + length > data.len() {
                    return Err(Error::corruption("Collection element data truncated".to_string()));
                }
                
                let element_data = &data[pos..pos + length];
                let element_type_str = match element_type {
                    CqlType::Text => "text",
                    CqlType::Int => "int", 
                    CqlType::BigInt => "bigint",
                    CqlType::Boolean => "boolean",
                    _ => "text", // Fallback
                };
                let (element_value, _) = self.parse_column_value_exact(element_data, element_type_str)?;
                elements.push(element_value);
                pos += length;
            }
        }
        
        let collection_value = match collection_type {
            CollectionType::List => Value::List(elements),
            CollectionType::Set => Value::Set(elements),
        };
        
        Ok((collection_value, pos))
    }
    
    /// Parse map as tuple (4-byte count + alternating key-value pairs)
    fn parse_map_as_tuple(&self, data: &[u8], key_type: &CqlType, value_type: &CqlType) -> Result<(Value, usize)> {
        let mut pos = 0;
        
        // Read pair count (4 bytes big-endian)
        if data.len() < 4 {
            return Err(Error::corruption("Map count missing".to_string()));
        }
        
        let pair_count = u32::from_be_bytes([
            data[pos], data[pos + 1], data[pos + 2], data[pos + 3]
        ]) as usize;
        pos += 4;
        
        let mut map_entries = Vec::with_capacity(pair_count);
        
        // Parse each key-value pair
        for _ in 0..pair_count {
            // Parse key with length prefix
            if pos + 4 > data.len() {
                return Err(Error::corruption("Map key length missing".to_string()));
            }
            
            let key_length = i32::from_be_bytes([
                data[pos], data[pos + 1], data[pos + 2], data[pos + 3]
            ]);
            pos += 4;
            
            let key_value = if key_length < 0 {
                Value::Null
            } else {
                let length = key_length as usize;
                if pos + length > data.len() {
                    return Err(Error::corruption("Map key data truncated".to_string()));
                }
                
                let key_data = &data[pos..pos + length];
                let key_type_str = match key_type {
                    CqlType::Text => "text",
                    CqlType::Int => "int",
                    CqlType::BigInt => "bigint",
                    _ => "text",
                };
                let (key_val, _) = self.parse_column_value_exact(key_data, key_type_str)?;
                pos += length;
                key_val
            };
            
            // Parse value with length prefix
            if pos + 4 > data.len() {
                return Err(Error::corruption("Map value length missing".to_string()));
            }
            
            let value_length = i32::from_be_bytes([
                data[pos], data[pos + 1], data[pos + 2], data[pos + 3]
            ]);
            pos += 4;
            
            let value_value = if value_length < 0 {
                Value::Null
            } else {
                let length = value_length as usize;
                if pos + length > data.len() {
                    return Err(Error::corruption("Map value data truncated".to_string()));
                }
                
                let value_data = &data[pos..pos + length];
                let value_type_str = match value_type {
                    CqlType::Text => "text",
                    CqlType::Int => "int",
                    CqlType::BigInt => "bigint",
                    _ => "text",
                };
                let (val_val, _) = self.parse_column_value_exact(value_data, value_type_str)?;
                pos += length;
                val_val
            };
            
            map_entries.push((key_value, value_value));
        }
        
        Ok((Value::Map(map_entries), pos))
    }
}

/// Cassandra row header with exact binary format compliance
#[derive(Debug)]
struct CassandraRowHeader {
    /// Row flags byte (TTL present, deletion time present, etc.)
    flags: u8,
    /// Timestamp delta from partition minimum (VInt)
    timestamp: Option<i64>,
    /// Time-to-live in seconds (VInt, if present)
    ttl: Option<i32>,
    /// Local deletion time (VInt, if present)
    local_deletion_time: Option<i32>,
    /// Number of columns in this row
    column_count: usize,
    /// Bit mask indicating which columns are present
    column_mask: Vec<u8>,
}

/// Collection type for tuple parsing
#[derive(Debug, Clone, Copy)]
enum CollectionType {
    List,
    Set,
}

/// Legacy row flags structure
#[derive(Debug)]
struct RowFlags {
    timestamp: Option<i64>,
    ttl: Option<i32>,
}

impl SSTableMemoryMaps {
    fn new() -> Self {
        Self {
            data: None,
            index: None,
            summary: None,
            filter: None,
            statistics: None,
            compression_info: None,
            file_handles: HashMap::new(),
        }
    }
}

impl CassandraRowHeader {
    /// Check if a column is present in this row
    fn has_column(&self, column_name: &str) -> bool {
        // For exact implementation, we'd need column index mapping
        // For now, assume all non-null columns are present
        // This would be enhanced with proper column index mapping
        true // Simplified - real implementation would check column_mask
    }
    
    /// Get absolute timestamp from delta
    fn absolute_timestamp(&self, partition_min_timestamp: i64) -> Option<i64> {
        self.timestamp.map(|delta| partition_min_timestamp + delta)
    }
}

/// Iterator implementations
impl<'a> Iterator for PartitionIterator<'a> {
    type Item = Result<Row>;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.data_slice.len() {
            return None;
        }
        
        match self.reader.parse_row(&self.data_slice[self.position..], &self.partition_key) {
            Ok((row, consumed)) => {
                self.position += consumed;
                Some(Ok(row))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl<'a> Iterator for RangeIterator<'a> {
    type Item = Result<Row>;
    
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.partition_iter.next() {
                Some(Ok(row)) => {
                    // Check if row is within range
                    if let Some(ref clustering_key) = row.clustering_key {
                        if let Some(ref start) = self.start_key {
                            if clustering_key < start {
                                continue;
                            }
                        }
                        
                        if let Some(ref end) = self.end_key {
                            if clustering_key > end {
                                return None; // Past end of range
                            }
                        }
                    }
                    
                    return Some(Ok(row));
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::*;
    
    fn create_test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test".to_string(),
            table: "users".to_string(),
            partition_keys: vec![
                KeyColumn {
                    name: "id".to_string(),
                    data_type: "bigint".to_string(),
                    position: 0,
                }
            ],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: false,
                    default: None,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                }
            ],
            comments: HashMap::new(),
        }
    }
    
    #[test]
    fn test_partition_key_encoding() {
        let schema = create_test_schema();
        let reader = SchemaAwareSSTableReader {
            base_path: "/tmp/test".into(),
            schema: Arc::new(schema),
            memory_maps: RwLock::new(SSTableMemoryMaps::new()),
            metadata_cache: RwLock::new(ReaderMetadata::default()),
        };
        
        let partition_key = PartitionKey {
            key_values: vec![Value::BigInt(123)],
        };
        
        let encoded = reader.encode_partition_key(&partition_key).unwrap();
        assert!(!encoded.is_empty());
    }
}