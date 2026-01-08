//! CQL CREATE Statement Generation
//!
//! This module provides comprehensive CQL CREATE TABLE statement generation
//! from discovered schema information, supporting all Cassandra features
//! including UDTs, collections, indexes, and table options.

use std::collections::{HashMap, BTreeMap};
use std::fmt::Write;

use crate::{
    Error, Result,
    schema::{
        TableSchema, Column, KeyColumn, ClusteringColumn, CqlType,
        discovery::{
            SchemaInfo, ColumnDefinition, UDTDefinition, IndexDefinition, 
            TableOptions, CompactionStrategy, CompressionOptions, CachingOptions,
            IndexType, CollectionType, CollectionKind,
        },
    },
};

/// CQL generation configuration
#[derive(Debug, Clone)]
pub struct CqlGeneratorConfig {
    /// Include IF NOT EXISTS clause
    pub include_if_not_exists: bool,
    /// Include table options (WITH clause)
    pub include_table_options: bool,
    /// Include index definitions
    pub include_indexes: bool,
    /// Include UDT definitions
    pub include_udt_definitions: bool,
    /// Format output for readability
    pub format_output: bool,
    /// Include comments
    pub include_comments: bool,
    /// Target Cassandra version for compatibility
    pub target_version: CassandraVersion,
    /// Indentation style
    pub indent_style: IndentStyle,
}

/// Supported Cassandra versions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CassandraVersion {
    V3_11,
    V4_0,
    V4_1,
    V5_0,
}

/// Indentation style for formatted output
#[derive(Debug, Clone)]
pub enum IndentStyle {
    Spaces(usize),
    Tabs,
}

impl Default for CqlGeneratorConfig {
    fn default() -> Self {
        Self {
            include_if_not_exists: false,
            include_table_options: true,
            include_indexes: true,
            include_udt_definitions: true,
            format_output: true,
            include_comments: false,
            target_version: CassandraVersion::V5_0,
            indent_style: IndentStyle::Spaces(2),
        }
    }
}

/// CQL statement generator
pub struct CqlGenerator {
    config: CqlGeneratorConfig,
}

/// Generated CQL output
#[derive(Debug, Clone)]
pub struct GeneratedCql {
    /// UDT definitions (ordered by dependencies)
    pub udt_definitions: Vec<String>,
    /// Main CREATE TABLE statement
    pub create_table: String,
    /// Index definitions
    pub index_definitions: Vec<String>,
    /// Complete CQL script
    pub complete_script: String,
    /// Generation metadata
    pub metadata: GenerationMetadata,
}

/// CQL generation metadata
#[derive(Debug, Clone)]
pub struct GenerationMetadata {
    /// Number of UDTs generated
    pub udt_count: usize,
    /// Number of indexes generated
    pub index_count: usize,
    /// Target Cassandra version
    pub target_version: CassandraVersion,
    /// Generation timestamp
    pub generated_at: std::time::SystemTime,
    /// Total lines of CQL
    pub total_lines: usize,
    /// Features used
    pub features_used: Vec<String>,
}

impl CqlGenerator {
    /// Create a new CQL generator with default configuration
    pub fn new() -> Self {
        Self {
            config: CqlGeneratorConfig::default(),
        }
    }

    /// Create generator with custom configuration
    pub fn with_config(config: CqlGeneratorConfig) -> Self {
        Self { config }
    }

    /// Generate CQL from schema information
    pub fn generate_from_schema_info(&self, schema_info: &SchemaInfo) -> Result<GeneratedCql> {
        let mut features_used = Vec::new();
        let start_time = std::time::SystemTime::now();

        // Generate UDT definitions first (with dependency ordering)
        let udt_definitions = if self.config.include_udt_definitions {
            let udts = self.generate_udt_definitions(&schema_info.user_defined_types)?;
            if !udts.is_empty() {
                features_used.push("UDT".to_string());
            }
            udts
        } else {
            Vec::new()
        };

        // Generate main CREATE TABLE statement
        let create_table = self.generate_create_table_from_schema_info(schema_info)?;

        // Check for advanced features
        if !schema_info.clustering_keys.is_empty() {
            features_used.push("Clustering Keys".to_string());
        }
        if !schema_info.collection_types.is_empty() {
            features_used.push("Collections".to_string());
        }
        if !schema_info.static_columns.is_empty() {
            features_used.push("Static Columns".to_string());
        }

        // Generate index definitions
        let index_definitions = if self.config.include_indexes {
            let indexes = self.generate_index_definitions(&schema_info.indexes, &schema_info.keyspace, &schema_info.table)?;
            if !indexes.is_empty() {
                features_used.push("Secondary Indexes".to_string());
            }
            indexes
        } else {
            Vec::new()
        };

        // Combine into complete script
        let complete_script = self.build_complete_script(&udt_definitions, &create_table, &index_definitions)?;
        let total_lines = complete_script.lines().count();

        let metadata = GenerationMetadata {
            udt_count: udt_definitions.len(),
            index_count: index_definitions.len(),
            target_version: self.config.target_version.clone(),
            generated_at: start_time,
            total_lines,
            features_used,
        };

        Ok(GeneratedCql {
            udt_definitions,
            create_table,
            index_definitions,
            complete_script,
            metadata,
        })
    }

    /// Generate CQL from basic table schema
    pub fn generate_from_table_schema(&self, schema: &TableSchema) -> Result<GeneratedCql> {
        let start_time = std::time::SystemTime::now();
        let mut features_used = Vec::new();

        // Generate main CREATE TABLE statement
        let create_table = self.generate_create_table_from_table_schema(schema)?;

        // Check for features
        if !schema.clustering_keys.is_empty() {
            features_used.push("Clustering Keys".to_string());
        }

        // For basic schema, no UDTs or indexes
        let udt_definitions = Vec::new();
        let index_definitions = Vec::new();
        let complete_script = create_table.clone();
        let total_lines = complete_script.lines().count();

        let metadata = GenerationMetadata {
            udt_count: 0,
            index_count: 0,
            target_version: self.config.target_version.clone(),
            generated_at: start_time,
            total_lines,
            features_used,
        };

        Ok(GeneratedCql {
            udt_definitions,
            create_table,
            index_definitions,
            complete_script,
            metadata,
        })
    }

    /// Generate just the CREATE TABLE statement from schema info
    pub fn generate_create_table_statement(&self, schema_info: &SchemaInfo) -> Result<String> {
        self.generate_create_table_from_schema_info(schema_info)
    }

    // Private implementation methods

    fn generate_udt_definitions(&self, udts: &[UDTDefinition]) -> Result<Vec<String>> {
        // Order UDTs by dependencies (simple approach - can be enhanced)
        let ordered_udts = self.order_udts_by_dependencies(udts);
        let mut definitions = Vec::new();

        for udt in ordered_udts {
            let definition = self.generate_single_udt_definition(&udt)?;
            definitions.push(definition);
        }

        Ok(definitions)
    }

    fn order_udts_by_dependencies(&self, udts: &[UDTDefinition]) -> Vec<&UDTDefinition> {
        // Simple dependency ordering - can be enhanced with proper topological sort
        let mut ordered = Vec::new();
        let mut remaining: Vec<_> = udts.iter().collect();

        // First pass: UDTs with no UDT dependencies
        let mut i = 0;
        while i < remaining.len() {
            let udt = remaining[i];
            if !self.udt_has_udt_dependencies(udt, udts) {
                ordered.push(udt);
                remaining.remove(i);
            } else {
                i += 1;
            }
        }

        // Add remaining UDTs (may have circular dependencies)
        ordered.extend(remaining);
        ordered
    }

    fn udt_has_udt_dependencies(&self, udt: &UDTDefinition, all_udts: &[UDTDefinition]) -> bool {
        for field in &udt.fields {
            if self.field_type_references_udt(&field.field_type, all_udts) {
                return true;
            }
        }
        false
    }

    fn field_type_references_udt(&self, field_type: &str, all_udts: &[UDTDefinition]) -> bool {
        // Simple check - can be enhanced with proper type parsing
        for udt in all_udts {
            if field_type.contains(&udt.name) {
                return true;
            }
        }
        false
    }

    fn generate_single_udt_definition(&self, udt: &UDTDefinition) -> Result<String> {
        let mut cql = String::new();

        if self.config.include_comments {
            writeln!(cql, "-- User-Defined Type: {}", udt.name)?;
        }

        // CREATE TYPE statement
        write!(cql, "CREATE TYPE")?;
        if self.config.include_if_not_exists {
            write!(cql, " IF NOT EXISTS")?;
        }
        writeln!(cql, " {}.{} (", udt.keyspace, udt.name)?;

        // Fields
        for (i, field) in udt.fields.iter().enumerate() {
            if i > 0 {
                writeln!(cql, ",")?;
            }

            if self.config.format_output {
                write!(cql, "{}", self.get_indent())?;
            }
            write!(cql, "{} {}", field.name, field.field_type)?;
        }

        if self.config.format_output {
            writeln!(cql)?;
        }
        writeln!(cql, ");")?;

        Ok(cql)
    }

    fn generate_create_table_from_schema_info(&self, schema_info: &SchemaInfo) -> Result<String> {
        let mut cql = String::new();

        if self.config.include_comments {
            writeln!(cql, "-- Table: {}.{}", schema_info.keyspace, schema_info.table)?;
            if !schema_info.metadata.source_files.is_empty() {
                writeln!(cql, "-- Discovered from {} SSTable files", schema_info.metadata.source_files.len())?;
            }
        }

        // CREATE TABLE statement
        write!(cql, "CREATE TABLE")?;
        if self.config.include_if_not_exists {
            write!(cql, " IF NOT EXISTS")?;
        }
        writeln!(cql, " {}.{} (", schema_info.keyspace, schema_info.table)?;

        // Collect all columns for formatting
        let mut all_columns = Vec::new();

        // Regular columns
        for col in &schema_info.regular_columns {
            all_columns.push((col.name.clone(), col.data_type.clone(), false));
        }

        // Static columns
        for col in &schema_info.static_columns {
            all_columns.push((col.name.clone(), col.data_type.clone(), true));
        }

        // Write columns
        for (i, (name, data_type, is_static)) in all_columns.iter().enumerate() {
            if i > 0 {
                writeln!(cql, ",")?;
            }

            if self.config.format_output {
                write!(cql, "{}", self.get_indent())?;
            }

            write!(cql, "{} {}", name, data_type)?;
            if *is_static {
                write!(cql, " STATIC")?;
            }
        }

        // Primary key
        if !schema_info.partition_key.is_empty() {
            writeln!(cql, ",")?;
            if self.config.format_output {
                write!(cql, "{}", self.get_indent())?;
            }

            write!(cql, "PRIMARY KEY ")?;
            self.write_primary_key_definition(&mut cql, &schema_info.partition_key, &schema_info.clustering_keys)?;
        }

        if self.config.format_output {
            writeln!(cql)?;
        }
        write!(cql, ")")?;

        // Table options
        if self.config.include_table_options {
            self.write_table_options(&mut cql, &schema_info.table_options)?;
        }

        writeln!(cql, ";")?;

        Ok(cql)
    }

    fn generate_create_table_from_table_schema(&self, schema: &TableSchema) -> Result<String> {
        let mut cql = String::new();

        if self.config.include_comments {
            writeln!(cql, "-- Table: {}.{}", schema.keyspace, schema.table)?;
        }

        // CREATE TABLE statement
        write!(cql, "CREATE TABLE")?;
        if self.config.include_if_not_exists {
            write!(cql, " IF NOT EXISTS")?;
        }
        writeln!(cql, " {}.{} (", schema.keyspace, schema.table)?;

        // Columns
        for (i, column) in schema.columns.iter().enumerate() {
            if i > 0 {
                writeln!(cql, ",")?;
            }

            if self.config.format_output {
                write!(cql, "{}", self.get_indent())?;
            }

            write!(cql, "{} {}", column.name, column.data_type)?;
        }

        // Primary key
        if !schema.partition_keys.is_empty() {
            writeln!(cql, ",")?;
            if self.config.format_output {
                write!(cql, "{}", self.get_indent())?;
            }

            write!(cql, "PRIMARY KEY ")?;
            self.write_primary_key_from_table_schema(&mut cql, schema)?;
        }

        if self.config.format_output {
            writeln!(cql)?;
        }
        writeln!(cql, ");")?;

        Ok(cql)
    }

    fn write_primary_key_definition(
        &self,
        cql: &mut String,
        partition_keys: &[ColumnDefinition],
        clustering_keys: &[ClusteringColumn],
    ) -> Result<()> {
        if partition_keys.len() == 1 && clustering_keys.is_empty() {
            // Simple primary key
            write!(cql, "({})", partition_keys[0].name)?;
        } else {
            // Composite primary key
            write!(cql, "(")?;

            // Partition key
            if partition_keys.len() == 1 {
                write!(cql, "{}", partition_keys[0].name)?;
            } else {
                write!(cql, "(")?;
                for (i, pk) in partition_keys.iter().enumerate() {
                    if i > 0 {
                        write!(cql, ", ")?;
                    }
                    write!(cql, "{}", pk.name)?;
                }
                write!(cql, ")")?;
            }

            // Clustering keys
            for ck in clustering_keys {
                write!(cql, ", {}", ck.name)?;
            }

            write!(cql, ")")?;
        }

        Ok(())
    }

    fn write_primary_key_from_table_schema(&self, cql: &mut String, schema: &TableSchema) -> Result<()> {
        if schema.partition_keys.len() == 1 && schema.clustering_keys.is_empty() {
            // Simple primary key
            write!(cql, "({})", schema.partition_keys[0].name)?;
        } else {
            // Composite primary key
            write!(cql, "(")?;

            // Partition key
            if schema.partition_keys.len() == 1 {
                write!(cql, "{}", schema.partition_keys[0].name)?;
            } else {
                write!(cql, "(")?;
                for (i, pk) in schema.partition_keys.iter().enumerate() {
                    if i > 0 {
                        write!(cql, ", ")?;
                    }
                    write!(cql, "{}", pk.name)?;
                }
                write!(cql, ")")?;
            }

            // Clustering keys
            for ck in &schema.clustering_keys {
                write!(cql, ", {}", ck.name)?;
            }

            write!(cql, ")")?;
        }

        Ok(())
    }

    fn write_table_options(&self, cql: &mut String, options: &TableOptions) -> Result<()> {
        let mut with_clauses = Vec::new();

        // Compaction
        if let Some(ref compaction) = options.compaction {
            let mut compaction_map = vec![format!("'class': '{}'", compaction.class)];
            for (key, value) in &compaction.options {
                compaction_map.push(format!("'{}': '{}'", key, value));
            }
            with_clauses.push(format!("compaction = {{{}}}", compaction_map.join(", ")));
        }

        // Compression
        if let Some(ref compression) = options.compression {
            let mut compression_map = vec![format!("'algorithm': '{}'", compression.algorithm)];
            if let Some(chunk_length) = compression.chunk_length_kb {
                compression_map.push(format!("'chunk_length_in_kb': {}", chunk_length));
            }
            if let Some(crc_chance) = compression.crc_check_chance {
                compression_map.push(format!("'crc_check_chance': {}", crc_chance));
            }
            with_clauses.push(format!("compression = {{{}}}", compression_map.join(", ")));
        }

        // Caching
        if let Some(ref caching) = options.caching {
            with_clauses.push(format!(
                "caching = {{'keys': '{}', 'rows_per_partition': '{}'}}",
                caching.keys, caching.rows_per_partition
            ));
        }

        // Simple options
        if let Some(bloom_fp_chance) = options.bloom_filter_fp_chance {
            with_clauses.push(format!("bloom_filter_fp_chance = {}", bloom_fp_chance));
        }

        if let Some(gc_grace) = options.gc_grace_seconds {
            with_clauses.push(format!("gc_grace_seconds = {}", gc_grace));
        }

        if let Some(ttl) = options.default_time_to_live {
            with_clauses.push(format!("default_time_to_live = {}", ttl));
        }

        if let Some(flush_period) = options.memtable_flush_period_in_ms {
            with_clauses.push(format!("memtable_flush_period_in_ms = {}", flush_period));
        }

        // Additional properties
        for (key, value) in &options.additional_properties {
            with_clauses.push(format!("{} = {}", key, value));
        }

        if !with_clauses.is_empty() {
            if self.config.format_output {
                writeln!(cql)?;
                write!(cql, "WITH ")?;
                for (i, clause) in with_clauses.iter().enumerate() {
                    if i > 0 {
                        writeln!(cql)?;
                        write!(cql, "{}AND ", self.get_indent())?;
                    }
                    write!(cql, "{}", clause)?;
                }
            } else {
                write!(cql, " WITH {}", with_clauses.join(" AND "))?;
            }
        }

        Ok(())
    }

    fn generate_index_definitions(
        &self,
        indexes: &[IndexDefinition],
        keyspace: &str,
        table: &str,
    ) -> Result<Vec<String>> {
        let mut definitions = Vec::new();

        for index in indexes {
            let definition = self.generate_single_index_definition(index, keyspace, table)?;
            definitions.push(definition);
        }

        Ok(definitions)
    }

    fn generate_single_index_definition(
        &self,
        index: &IndexDefinition,
        keyspace: &str,
        table: &str,
    ) -> Result<String> {
        let mut cql = String::new();

        if self.config.include_comments {
            writeln!(cql, "-- Index: {}", index.name)?;
        }

        write!(cql, "CREATE INDEX")?;
        if self.config.include_if_not_exists {
            write!(cql, " IF NOT EXISTS")?;
        }
        write!(cql, " {} ON {}.{}", index.name, keyspace, table)?;

        match &index.index_type {
            IndexType::Secondary => {
                write!(cql, " ({})", index.target_column)?;
            }
            IndexType::Composite => {
                write!(cql, " ({})", index.target_column)?; // Simplified
            }
            IndexType::Custom(class) => {
                write!(cql, " ({}) USING '{}'", index.target_column, class)?;
            }
        }

        // Index options
        if !index.options.is_empty() {
            write!(cql, " WITH OPTIONS = {{")?;
            let mut first = true;
            for (key, value) in &index.options {
                if !first {
                    write!(cql, ", ")?;
                }
                write!(cql, "'{}': '{}'", key, value)?;
                first = false;
            }
            write!(cql, "}}")?;
        }

        writeln!(cql, ";")?;

        Ok(cql)
    }

    fn build_complete_script(
        &self,
        udt_definitions: &[String],
        create_table: &str,
        index_definitions: &[String],
    ) -> Result<String> {
        use std::fmt::Write;
        let mut script = String::new();

        if self.config.include_comments {
            writeln!(script, "-- CQL Schema Generated by CQLite")
                .map_err(|e| Error::internal(format!("Failed to write to script: {}", e)))?;
            writeln!(script, "-- Target Version: {:?}", self.config.target_version)
                .map_err(|e| Error::internal(format!("Failed to write to script: {}", e)))?;
            writeln!(script, "-- Generated at: {:?}", std::time::SystemTime::now())
                .map_err(|e| Error::internal(format!("Failed to write to script: {}", e)))?;
            writeln!(script)
                .map_err(|e| Error::internal(format!("Failed to write to script: {}", e)))?;
        }

        // UDT definitions first
        if !udt_definitions.is_empty() {
            if self.config.include_comments {
                writeln!(script, "-- User-Defined Types")
                    .map_err(|e| Error::internal(format!("Failed to write to script: {}", e)))?;
            }
            for udt in udt_definitions {
                writeln!(script, "{}", udt)
                    .map_err(|e| Error::internal(format!("Failed to write to script: {}", e)))?;
            }
            writeln!(script)
                .map_err(|e| Error::internal(format!("Failed to write to script: {}", e)))?;
        }

        // Main table
        if self.config.include_comments {
            writeln!(script, "-- Main Table")
                .map_err(|e| Error::internal(format!("Failed to write to script: {}", e)))?;
        }
        writeln!(script, "{}", create_table)
            .map_err(|e| Error::internal(format!("Failed to write to script: {}", e)))?;

        // Indexes
        if !index_definitions.is_empty() {
            writeln!(script)
                .map_err(|e| Error::internal(format!("Failed to write to script: {}", e)))?;
            if self.config.include_comments {
                writeln!(script, "-- Secondary Indexes")
                    .map_err(|e| Error::internal(format!("Failed to write to script: {}", e)))?;
            }
            for index in index_definitions {
                writeln!(script, "{}", index)
                    .map_err(|e| Error::internal(format!("Failed to write to script: {}", e)))?;
            }
        }

        Ok(script)
    }

    fn get_indent(&self) -> String {
        match &self.config.indent_style {
            IndentStyle::Spaces(count) => " ".repeat(*count),
            IndentStyle::Tabs => "\t".to_string(),
        }
    }
}

impl Default for CqlGenerator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::discovery::*;
    use std::time::SystemTime;

    #[test]
    fn test_simple_table_generation() {
        let generator = CqlGenerator::new();
        
        let schema = TableSchema {
            keyspace: "test".to_string(),
            table: "users".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
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
        };

        let result = generator.generate_from_table_schema(&schema).unwrap();
        
        assert!(result.create_table.contains("CREATE TABLE test.users"));
        assert!(result.create_table.contains("id uuid"));
        assert!(result.create_table.contains("name text"));
        assert!(result.create_table.contains("PRIMARY KEY (id)"));
        assert_eq!(result.metadata.udt_count, 0);
        assert_eq!(result.metadata.index_count, 0);
    }

    #[test]
    fn test_composite_primary_key() {
        let generator = CqlGenerator::new();
        
        let schema = TableSchema {
            keyspace: "test".to_string(),
            table: "events".to_string(),
            partition_keys: vec![KeyColumn {
                name: "user_id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "timestamp".to_string(),
                data_type: "timestamp".to_string(),
                position: 0,
                order: "DESC".to_string(),
            }],
            columns: vec![
                Column {
                    name: "user_id".to_string(),
                    data_type: "uuid".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "timestamp".to_string(),
                    data_type: "timestamp".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "event_data".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        };

        let result = generator.generate_from_table_schema(&schema).unwrap();
        
        assert!(result.create_table.contains("PRIMARY KEY (user_id, timestamp)"));
        assert!(result.metadata.features_used.contains(&"Clustering Keys".to_string()));
    }

    #[test]
    fn test_config_options() {
        let config = CqlGeneratorConfig {
            include_if_not_exists: true,
            include_comments: true,
            format_output: true,
            ..Default::default()
        };
        
        let generator = CqlGenerator::with_config(config);
        
        let schema = TableSchema {
            keyspace: "test".to_string(),
            table: "users".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        };

        let result = generator.generate_from_table_schema(&schema).unwrap();
        
        assert!(result.create_table.contains("IF NOT EXISTS"));
        assert!(result.create_table.contains("-- Table:"));
    }
}