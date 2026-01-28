//! SSTable writer components for producing Cassandra 5.0-compatible SSTables
//!
//! This module coordinates the generation of all SSTable components:
//! - Data.db: Row data with partition and clustering ordering
//! - Index.db: Partition index for fast lookups
//! - Filter.db: Bloom filter for existence checks
//! - Statistics.db: Metadata for delta encoding
//! - Summary.db: Sampled index entries
//! - TOC.txt: Component manifest (publication barrier)
//!
//! Component generation order is critical (see M5 Council Recommendation):
//! 1. Statistics.db (provides delta encoding baseline)
//! 2. Data.db + Index.db (single pass, track offsets)
//! 3. Summary.db (sample Index.db entries)
//! 4. Filter.db (finalize Bloom filter)
//! 5. CompressionInfo.db (if compressed)
//! 6. Digest.crc32
//! 7. TOC.txt (makes SSTable visible)
//!
//! TODO: Implementation in M5.0-7 through M5.0-13

#[cfg(feature = "write-support")]
pub mod data_writer;
#[cfg(feature = "write-support")]
pub mod index_writer;
#[cfg(feature = "write-support")]
pub mod filter_writer;
#[cfg(feature = "write-support")]
pub mod stats_writer;
#[cfg(feature = "write-support")]
pub mod summary_writer;
#[cfg(feature = "write-support")]
pub mod toc_writer;
#[cfg(feature = "write-support")]
pub mod digest_writer;

#[cfg(feature = "write-support")]
pub use data_writer::DataWriter;
#[cfg(feature = "write-support")]
pub use index_writer::IndexWriter;
#[cfg(feature = "write-support")]
pub use filter_writer::FilterWriter;
#[cfg(feature = "write-support")]
pub use stats_writer::{StatisticsWriter, StatisticsMetadata};
#[cfg(feature = "write-support")]
pub use summary_writer::SummaryWriter;
#[cfg(feature = "write-support")]
pub use toc_writer::{TocWriter, ComponentEntry};
#[cfg(feature = "write-support")]
pub use digest_writer::DigestWriter;

use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::write_engine::mutation::{DecoratedKey, Mutation};
use std::path::{Path, PathBuf};

/// Information about a written SSTable
///
/// Returned by `SSTableWriter::finish()` after successfully writing all components.
#[cfg(feature = "write-support")]
#[derive(Debug, Clone)]
pub struct SSTableInfo {
    /// Path to the Data.db file
    pub data_path: PathBuf,
    /// Path to the Index.db file
    pub index_path: PathBuf,
    /// Path to the Filter.db file
    pub filter_path: PathBuf,
    /// Path to the Summary.db file
    pub summary_path: PathBuf,
    /// Path to the Statistics.db file
    pub stats_path: PathBuf,
    /// Path to the TOC.txt file
    pub toc_path: PathBuf,
    /// Path to the Digest.crc32 file
    pub digest_path: PathBuf,
    /// Number of partitions written
    pub partition_count: usize,
    /// Total size of Data.db file in bytes
    pub data_size: u64,
}

/// SSTable writer coordinator
///
/// Orchestrates the generation of all SSTable components in the correct order.
/// Produces valid Cassandra 5.0 BIG format SSTables.
///
/// # Write Order
///
/// Components are written in the following critical order:
/// 1. Statistics.db - Provides delta encoding baseline (FIRST)
/// 2. Data.db - Main partition/row data
/// 3. Index.db - Partition index (uses Data.db offsets)
/// 4. Filter.db - Bloom filter
/// 5. Summary.db - Sampled index entries
/// 6. Digest.crc32 - Data.db checksum
/// 7. TOC.txt - Table of contents (LAST, publication barrier)
///
/// # File Naming
///
/// All components follow the pattern: `nb-{generation}-big-{Component}.db`
/// Example: `nb-1-big-Data.db`, `nb-1-big-Index.db`
///
/// # Partition Ordering
///
/// Partitions MUST be written in Murmur3 token order (caller responsibility).
/// The writer validates token ordering on each `write_partition()` call.
///
/// # Example
///
/// ```rust,ignore
/// use cqlite_core::storage::sstable::writer::SSTableWriter;
/// use cqlite_core::storage::write_engine::mutation::{Mutation, DecoratedKey};
/// use cqlite_core::schema::TableSchema;
///
/// // Create schema
/// let schema = TableSchema::from_json("...")?;
///
/// // Create writer
/// let mut writer = SSTableWriter::new(
///     PathBuf::from("data/ks/table"),
///     1,  // generation
///     &schema
/// )?;
///
/// // Write partitions (MUST be in token order)
/// let key = DecoratedKey::new(token, key_bytes);
/// let mutations = vec![/* ... */];
/// writer.write_partition(key, mutations)?;
///
/// // Finish writing
/// let info = writer.finish().await?;
/// println!("Wrote SSTable with {} partitions", info.partition_count);
/// ```
#[cfg(feature = "write-support")]
#[derive(Debug)]
pub struct SSTableWriter {
    /// Output directory for SSTable files
    output_dir: PathBuf,
    /// SSTable generation number
    generation: u32,
    /// Table schema for column metadata
    schema: TableSchema,
    /// Statistics metadata (collected during writes)
    stats: StatisticsMetadata,
    /// Data.db writer
    data_writer: DataWriter,
    /// Index.db writer
    index_writer: IndexWriter,
    /// Filter.db writer
    filter_writer: Option<FilterWriter>,
    /// Summary.db writer
    summary_writer: SummaryWriter,
    /// Last token written (for ordering validation)
    last_token: Option<i64>,
    /// Number of partitions written
    partition_count: usize,
    /// Index.db offset tracking (for Summary.db sampling)
    index_offset: u64,
    /// Summary sampling counter (sample every N entries)
    summary_sample_counter: usize,
}

#[cfg(feature = "write-support")]
impl SSTableWriter {
    /// Create a new SSTable writer
    ///
    /// # Arguments
    ///
    /// * `output_dir` - Directory where SSTable files will be written
    /// * `generation` - SSTable generation number (e.g., 1, 2, 3...)
    /// * `schema` - Table schema for column metadata
    ///
    /// # Returns
    ///
    /// A new SSTableWriter ready to accept partitions.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let writer = SSTableWriter::new(
    ///     PathBuf::from("data/test_ks/users"),
    ///     1,
    ///     &schema
    /// )?;
    /// ```
    pub fn new(output_dir: PathBuf, generation: u32, schema: &TableSchema) -> Result<Self> {
        // Initialize statistics metadata with sentinel values
        let mut stats = StatisticsMetadata::new();
        // Pre-set min values to reasonable defaults (will be updated during writes)
        stats.min_timestamp = i64::MAX;
        stats.min_ttl = i32::MAX;
        stats.min_local_deletion_time = i32::MAX;

        // Create Data.db writer (needs stats for delta encoding)
        let data_writer = DataWriter::new(stats.clone());

        // Create Index.db writer
        let index_writer = IndexWriter::new();

        // Create Filter.db writer (1% false positive rate by default)
        // Start with capacity for 1 partition, will grow as needed
        let filter_path = Self::component_path(&output_dir, generation, "Filter.db");
        let filter_writer = Some(FilterWriter::new(filter_path, 1, 0.01)?);

        // Create Summary.db writer (sample every 128 entries per Cassandra default)
        let summary_writer = SummaryWriter::new(128);

        Ok(Self {
            output_dir,
            generation,
            schema: schema.clone(),
            stats,
            data_writer,
            index_writer,
            filter_writer,
            summary_writer,
            last_token: None,
            partition_count: 0,
            index_offset: 0,
            summary_sample_counter: 0,
        })
    }

    /// Write a partition (partition key + all mutations)
    ///
    /// # Arguments
    ///
    /// * `key` - DecoratedKey (token + raw partition key bytes)
    /// * `mutations` - All mutations for this partition (must be in clustering order)
    ///
    /// # Returns
    ///
    /// Ok(()) on success, or an error if:
    /// - Partitions are not in token order
    /// - Schema validation fails
    /// - I/O error occurs
    ///
    /// # Ordering Requirement
    ///
    /// Partitions MUST be written in ascending token order. This method validates
    /// ordering and returns an error if violated.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let key = DecoratedKey::new(12345, vec![0x00, 0x01, 0x02]);
    /// let mutations = vec![
    ///     Mutation::new(/* ... */)
    /// ];
    /// writer.write_partition(key, mutations)?;
    /// ```
    pub fn write_partition(&mut self, key: DecoratedKey, mutations: Vec<Mutation>) -> Result<()> {
        // Validate token ordering
        if let Some(last_token) = self.last_token {
            if key.token <= last_token {
                return Err(Error::InvalidInput(format!(
                    "Partitions must be written in token order: got token {} after {}",
                    key.token, last_token
                )));
            }
        }
        self.last_token = Some(key.token);

        // Update statistics from mutations
        for mutation in &mutations {
            self.stats.update_timestamp(mutation.timestamp_micros);
            if let Some(ttl) = mutation.ttl_seconds {
                self.stats.update_ttl(ttl as i32);
            }
            self.stats.increment_row_count();
            self.stats.add_column_count(mutation.operations.len() as u64);
        }

        // Write partition to Data.db and get offset
        let data_offset = self
            .data_writer
            .write_partition(&key, &mutations, &self.schema)?;

        // Add partition to Index.db
        self.index_writer.add_partition(&key, data_offset)?;

        // Add partition key to Filter.db
        if let Some(ref mut filter) = self.filter_writer {
            filter.add_key(&key);
        }

        // Sample for Summary.db (every 128th entry)
        if self.summary_sample_counter % 128 == 0 {
            self.summary_writer.add_entry(&key, self.index_offset)?;
        }

        // Track index offset for next summary sample
        // Each Index.db entry: 2 (marker) + 16 (digest) + VInt(position) + VInt(promoted_len=0)
        // Conservative estimate: 2 + 16 + 5 + 1 = 24 bytes per entry
        self.index_offset += 24;

        self.summary_sample_counter += 1;
        self.partition_count += 1;
        self.stats.increment_partition_count();

        Ok(())
    }

    /// Finish writing all components and return SSTable information
    ///
    /// This method:
    /// 1. Finalizes statistics metadata
    /// 2. Writes all component files in the correct order
    /// 3. Computes checksums
    /// 4. Writes TOC.txt (publication barrier)
    /// 5. Returns SSTableInfo with file paths and metadata
    ///
    /// # Returns
    ///
    /// SSTableInfo containing paths to all written files and metadata.
    ///
    /// # Errors
    ///
    /// Returns error if any component write fails.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let info = writer.finish().await?;
    /// println!("SSTable written to {}", info.data_path.display());
    /// ```
    pub async fn finish(mut self) -> Result<SSTableInfo> {
        // Finalize statistics metadata (normalize sentinel values)
        self.stats.finalize();

        // 1. Write Statistics.db (FIRST - provides delta baseline)
        let stats_path = Self::component_path(&self.output_dir, self.generation, "Statistics.db");
        let stats_writer = StatisticsWriter::new(stats_path.clone());
        stats_writer.write(&self.stats)?;

        // 2. Write Data.db
        let data_path = Self::component_path(&self.output_dir, self.generation, "Data.db");
        let data_bytes = self.data_writer.finish()?;
        tokio::fs::write(&data_path, &data_bytes).await?;
        let data_size = data_bytes.len() as u64;

        // 3. Write Index.db
        let index_path = Self::component_path(&self.output_dir, self.generation, "Index.db");
        let index_bytes = self.index_writer.finish()?;
        tokio::fs::write(&index_path, index_bytes).await?;

        // 4. Write Filter.db
        let filter_path = Self::component_path(&self.output_dir, self.generation, "Filter.db");
        if let Some(filter_writer) = self.filter_writer {
            filter_writer.finish().await?;
        }

        // 5. Write Summary.db
        let summary_path = Self::component_path(&self.output_dir, self.generation, "Summary.db");
        let summary_bytes = self.summary_writer.finish()?;
        tokio::fs::write(&summary_path, summary_bytes).await?;

        // 6. Write Digest.crc32 (compute CRC32 of Data.db)
        let digest_path = Self::component_path(&self.output_dir, self.generation, "Digest.crc32");
        let digest_writer = DigestWriter::new(digest_path.clone());
        let crc32_value = Self::compute_crc32(&data_path).await?;
        digest_writer.write(crc32_value)?;

        // 7. Write TOC.txt (LAST - publication barrier)
        let toc_path = Self::component_path(&self.output_dir, self.generation, "TOC.txt");
        let toc_writer = TocWriter::new(toc_path.clone());
        let components = vec![
            ComponentEntry::new(crate::storage::sstable::directory::types::SSTableComponent::Data),
            ComponentEntry::new(crate::storage::sstable::directory::types::SSTableComponent::Index),
            ComponentEntry::new(crate::storage::sstable::directory::types::SSTableComponent::Filter),
            ComponentEntry::new(crate::storage::sstable::directory::types::SSTableComponent::Summary),
            ComponentEntry::new(crate::storage::sstable::directory::types::SSTableComponent::Statistics),
            ComponentEntry::new(crate::storage::sstable::directory::types::SSTableComponent::Digest),
        ];
        toc_writer.write(&components)?;

        Ok(SSTableInfo {
            data_path,
            index_path,
            filter_path,
            summary_path,
            stats_path,
            toc_path,
            digest_path,
            partition_count: self.partition_count,
            data_size,
        })
    }

    /// Build component file path
    fn component_path(output_dir: &Path, generation: u32, component: &str) -> PathBuf {
        let filename = format!("nb-{}-big-{}", generation, component);
        output_dir.join(filename)
    }

    /// Compute CRC32 checksum of a file
    async fn compute_crc32(file_path: &PathBuf) -> Result<u32> {
        let data = tokio::fs::read(file_path).await?;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&data);
        Ok(hasher.finalize())
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;
    use crate::storage::write_engine::mutation::{CellOperation, PartitionKey, TableId};
    use crate::schema::{Column, KeyColumn};
    use crate::types::Value;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn create_test_schema() -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        }
    }

    fn create_test_mutation(
        keyspace: &str,
        table: &str,
        partition_id: i32,
        name: &str,
        timestamp: i64,
    ) -> Mutation {
        let table_id = TableId::new(keyspace, table);
        let pk = PartitionKey::single("id", Value::Integer(partition_id));

        Mutation::new(
            table_id,
            pk,
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text(name.to_string()),
            }],
            timestamp,
            None,
        )
    }

    #[tokio::test]
    async fn test_sstable_writer_single_partition() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // Create a partition
        let mutation = create_test_mutation("test_ks", "test_table", 1, "Alice", 1000000);
        let key = mutation.decorated_key(&schema).unwrap();

        writer.write_partition(key, vec![mutation]).unwrap();

        let info = writer.finish().await.unwrap();

        // Verify all files were created
        assert!(info.data_path.exists());
        assert!(info.index_path.exists());
        assert!(info.filter_path.exists());
        assert!(info.summary_path.exists());
        assert!(info.stats_path.exists());
        assert!(info.toc_path.exists());
        assert!(info.digest_path.exists());

        // Verify metadata
        assert_eq!(info.partition_count, 1);
        assert!(info.data_size > 0);

        // Verify file naming convention
        assert!(info
            .data_path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .contains("nb-1-big-Data.db"));
    }

    #[tokio::test]
    async fn test_sstable_writer_multiple_partitions() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // Write 3 partitions in token order
        let mutations = vec![
            create_test_mutation("test_ks", "test_table", 1, "Alice", 1000000),
            create_test_mutation("test_ks", "test_table", 2, "Bob", 1001000),
            create_test_mutation("test_ks", "test_table", 3, "Charlie", 1002000),
        ];

        // Sort by token
        let mut keyed_mutations: Vec<_> = mutations
            .into_iter()
            .map(|m| {
                let key = m.decorated_key(&schema).unwrap();
                (key, m)
            })
            .collect();
        keyed_mutations.sort_by_key(|(k, _)| k.token);

        for (key, mutation) in keyed_mutations {
            writer.write_partition(key, vec![mutation]).unwrap();
        }

        let info = writer.finish().await.unwrap();

        assert_eq!(info.partition_count, 3);
        assert!(info.data_size > 0);
    }

    #[tokio::test]
    async fn test_sstable_writer_token_ordering_validation() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // Write first partition
        let mutation1 = create_test_mutation("test_ks", "test_table", 1, "Alice", 1000000);
        let key1 = mutation1.decorated_key(&schema).unwrap();
        let token1 = key1.token;

        writer
            .write_partition(key1.clone(), vec![mutation1])
            .unwrap();

        // Try to write a partition with lower token (should fail)
        let key2 = DecoratedKey::new(token1 - 1, vec![0x00, 0x00, 0x00, 0x02]);
        let mutation2 = create_test_mutation("test_ks", "test_table", 2, "Bob", 1001000);

        let result = writer.write_partition(key2, vec![mutation2]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("token order"));
    }

    #[tokio::test]
    async fn test_sstable_writer_component_paths() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let _writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 42, &schema).unwrap();

        // Verify generation number is used in paths
        // (we don't actually write anything, just test path construction)

        let data_path = SSTableWriter::component_path(temp_dir.path(), 42, "Data.db");
        assert_eq!(
            data_path.file_name().unwrap().to_str().unwrap(),
            "nb-42-big-Data.db"
        );
    }

    #[tokio::test]
    async fn test_sstable_writer_toc_contents() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        let mutation = create_test_mutation("test_ks", "test_table", 1, "Alice", 1000000);
        let key = mutation.decorated_key(&schema).unwrap();

        writer.write_partition(key, vec![mutation]).unwrap();
        let info = writer.finish().await.unwrap();

        // Read TOC.txt and verify contents
        let toc_contents = std::fs::read_to_string(&info.toc_path).unwrap();
        assert!(toc_contents.contains("Data.db"));
        assert!(toc_contents.contains("Index.db"));
        assert!(toc_contents.contains("Filter.db"));
        assert!(toc_contents.contains("Summary.db"));
        assert!(toc_contents.contains("Statistics.db"));
        assert!(toc_contents.contains("Digest.crc32"));
        assert!(toc_contents.contains("TOC.txt"));
    }

    #[tokio::test]
    async fn test_sstable_writer_statistics_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // Write partitions with varying timestamps and TTLs
        let mutations = vec![
            {
                let mut m = create_test_mutation("test_ks", "test_table", 1, "Alice", 1000000);
                m.ttl_seconds = Some(3600);
                m
            },
            create_test_mutation("test_ks", "test_table", 2, "Bob", 2000000),
            {
                let mut m = create_test_mutation("test_ks", "test_table", 3, "Charlie", 1500000);
                m.ttl_seconds = Some(7200);
                m
            },
        ];

        for mutation in mutations {
            let key = mutation.decorated_key(&schema).unwrap();
            writer.write_partition(key, vec![mutation]).unwrap();
        }

        // Check statistics were updated
        assert_eq!(writer.stats.min_timestamp, 1000000);
        assert_eq!(writer.stats.max_timestamp, 2000000);
        assert_eq!(writer.stats.min_ttl, 3600);
        assert_eq!(writer.stats.max_ttl, 7200);
        assert_eq!(writer.stats.partition_count, 3);

        let _info = writer.finish().await.unwrap();
    }

    #[tokio::test]
    async fn test_sstable_writer_digest_crc32() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let mut writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        let mutation = create_test_mutation("test_ks", "test_table", 1, "Alice", 1000000);
        let key = mutation.decorated_key(&schema).unwrap();

        writer.write_partition(key, vec![mutation]).unwrap();
        let info = writer.finish().await.unwrap();

        // Verify Digest.crc32 was created and contains a number
        let digest_contents = std::fs::read_to_string(&info.digest_path).unwrap();
        assert!(!digest_contents.is_empty());
        assert!(digest_contents.parse::<u32>().is_ok());
    }

    #[tokio::test]
    async fn test_sstable_writer_empty_sstable() {
        let temp_dir = TempDir::new().unwrap();
        let schema = create_test_schema();

        let writer = SSTableWriter::new(temp_dir.path().to_path_buf(), 1, &schema).unwrap();

        // Finish without writing any partitions
        let info = writer.finish().await.unwrap();

        assert_eq!(info.partition_count, 0);
        assert!(info.data_path.exists());
        assert!(info.toc_path.exists());
    }
}
