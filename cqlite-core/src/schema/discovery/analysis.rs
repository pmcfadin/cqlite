//! Data-driven schema analysis phases for the discovery engine.
//!
//! These [`SchemaDiscoveryEngine`] methods implement the per-phase analysis
//! that turns SSTable headers and sampled values into a [`SchemaInfo`]: header
//! metadata extraction, data sampling, UDT discovery, collection-type analysis,
//! index discovery, and final schema assembly.

use std::collections::HashMap;

use crate::{parser::header::SSTableHeader, schema::ClusteringColumn, types::Value, Result};

use super::model::{
    CollectionKind, CollectionType, ColumnDefinition, ConsistencyResults, DiscoveryMethod,
    DiscoveryMetrics, IndexDefinition, IndexType, SchemaInfo, SchemaMetadata, TypeInfo,
    UDTDefinition, UdtFieldDefinition, ValidationResults,
};
use super::{DiscoveryContext, SchemaDiscoveryEngine};

// Additional implementation methods for SchemaDiscoveryEngine
impl SchemaDiscoveryEngine {
    pub(super) async fn extract_header_metadata(
        &self,
        context: &mut DiscoveryContext,
    ) -> Result<()> {
        // Parse headers from each source file
        for file_path in &context.source_files.clone() {
            match self.parse_sstable_header(file_path).await {
                Ok(header) => {
                    context.headers.push(header.clone());

                    // Update Cassandra version if not set
                    if context.cassandra_version.is_none() {
                        context.cassandra_version = Some(header.cassandra_version);
                    }
                }
                Err(e) => {
                    // Log error but continue with other files
                    log::warn!("Failed to parse header from {:?}: {}", file_path, e);
                }
            }
        }

        Ok(())
    }

    /// Parse SSTable header from file
    async fn parse_sstable_header(&self, file_path: &std::path::Path) -> Result<SSTableHeader> {
        use crate::storage::sstable::reader::SSTableReader;

        let reader =
            SSTableReader::open(file_path, &self.core_config, self.platform.clone()).await?;
        Ok(reader.header().clone())
    }

    pub(super) async fn sample_data_for_inference(
        &self,
        context: &mut DiscoveryContext,
    ) -> Result<()> {
        let mut total_sampled = 0;

        for file_path in &context.source_files.clone() {
            if total_sampled >= self.config.max_sample_rows {
                break;
            }

            match self
                .sample_sstable_data(file_path, self.config.max_sample_rows - total_sampled)
                .await
            {
                Ok(samples) => {
                    total_sampled += samples.len();

                    // Organize samples by column
                    for row in samples {
                        for (column_name, value) in row {
                            context
                                .column_samples
                                .entry(column_name)
                                .or_default()
                                .push(value);
                        }
                    }
                }
                Err(e) => {
                    log::warn!("Failed to sample data from {:?}: {}", file_path, e);
                }
            }
        }

        context.total_rows_sampled = total_sampled;
        Ok(())
    }

    /// Sample data from an SSTable file
    async fn sample_sstable_data(
        &self,
        file_path: &std::path::Path,
        max_rows: usize,
    ) -> Result<Vec<HashMap<String, Value>>> {
        use crate::storage::sstable::reader::SSTableReader;

        let reader =
            SSTableReader::open(file_path, &self.core_config, self.platform.clone()).await?;

        // Get all entries and sample up to max_rows
        let all_entries = reader.get_all_entries().await?;

        // Issue #1334: each entry carries a `ScanRow` row. Disassemble a live row's
        // interned cells into a name→value map for type inference; suppress markers
        // (row tombstone / null row) which carry no columns. Column names come from
        // the decoded cells themselves rather than the header column list.
        let samples: Vec<HashMap<String, Value>> = all_entries
            .into_iter()
            .take(max_rows)
            .filter_map(|(_table_id, _row_key, row)| {
                let cells = row.into_cells()?;
                if cells.is_empty() {
                    return None;
                }
                let row_data: HashMap<String, Value> = cells
                    .into_iter()
                    .map(|(name, v)| (name.to_string(), v))
                    .collect();
                Some(row_data)
            })
            .collect();

        Ok(samples)
    }

    pub(super) async fn discover_udts(&self, context: &mut DiscoveryContext) -> Result<()> {
        // UDT discovery from headers and data samples
        for header in &context.headers {
            for column_def in &header.columns {
                // Check if column type indicates a UDT
                if self.is_udt_type(&column_def.column_type) {
                    let udt_name = self.extract_udt_name(&column_def.column_type);

                    if !context.discovered_udts.contains_key(&udt_name) {
                        // Create basic UDT definition from type info
                        let udt_def = UDTDefinition {
                            name: udt_name.clone(),
                            keyspace: context.keyspace.clone(),
                            fields: self.parse_udt_fields(&column_def.column_type),
                            version: Some(1),
                        };

                        context.discovered_udts.insert(udt_name, udt_def);
                    }
                }
            }
        }

        // Analyze UDT usage in sample data
        for (column_name, samples) in &context.column_samples {
            for sample in samples {
                if let Value::Udt(udt_map) = sample {
                    // Infer UDT structure from data
                    let udt_name = format!("{}_udt", column_name); // Generate name if not available

                    if !context.discovered_udts.contains_key(&udt_name) {
                        let mut fields = Vec::new();

                        for (position, field) in udt_map.fields.iter().enumerate() {
                            let field_def = UdtFieldDefinition {
                                name: field.name.clone(),
                                field_type: "text".to_string(), // Would need type inference here
                                position,
                                nullable: true,
                            };
                            fields.push(field_def);
                        }

                        let udt_def = UDTDefinition {
                            name: udt_name.clone(),
                            keyspace: context.keyspace.clone(),
                            fields,
                            version: Some(1),
                        };

                        context.discovered_udts.insert(udt_name, udt_def);
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if a type string indicates a UDT
    fn is_udt_type(&self, type_str: &str) -> bool {
        // UDTs are typically not standard CQL types
        !matches!(
            type_str.to_lowercase().as_str(),
            "text"
                | "varchar"
                | "ascii"
                | "int"
                | "bigint"
                | "smallint"
                | "tinyint"
                | "float"
                | "double"
                | "boolean"
                | "timestamp"
                | "date"
                | "time"
                | "uuid"
                | "timeuuid"
                | "blob"
                | "varint"
                | "decimal"
                | "duration"
                | "inet"
                | "counter"
        ) && !type_str.starts_with("list<")
            && !type_str.starts_with("set<")
            && !type_str.starts_with("map<")
            && !type_str.starts_with("tuple<")
            && !type_str.starts_with("frozen<")
    }

    /// Extract UDT name from type string
    fn extract_udt_name(&self, type_str: &str) -> String {
        // Remove any qualifiers and get base type name
        type_str.split('<').next().unwrap_or(type_str).to_string()
    }

    /// Parse UDT field definitions from type string
    fn parse_udt_fields(&self, _type_str: &str) -> Vec<UdtFieldDefinition> {
        // This is a simplified implementation
        // Real UDT parsing would require more sophisticated type parsing
        vec![UdtFieldDefinition {
            name: "field".to_string(),
            field_type: "text".to_string(),
            position: 0,
            nullable: true,
        }]
    }

    pub(super) async fn analyze_collection_types(
        &self,
        context: &mut DiscoveryContext,
    ) -> Result<()> {
        // Analyze collection types from headers
        for header in &context.headers {
            for column_def in &header.columns {
                if let Some(collection_type) = self.parse_collection_type(&column_def.column_type) {
                    context
                        .collection_types
                        .insert(column_def.name.clone(), collection_type);
                }
            }
        }

        // Analyze collection types from sample data
        for (column_name, samples) in &context.column_samples {
            let mut detected_collections = Vec::new();

            for sample in samples {
                match sample {
                    Value::List(elements) => {
                        let element_type = if !elements.is_empty() {
                            self.infer_element_type(elements)
                        } else {
                            "text".to_string()
                        };

                        detected_collections.push(CollectionType {
                            kind: CollectionKind::List,
                            element_type: Some(element_type),
                            key_type: None,
                            value_type: None,
                            is_frozen: false,
                        });
                    }
                    Value::Set(elements) => {
                        let element_type = if !elements.is_empty() {
                            self.infer_element_type(elements)
                        } else {
                            "text".to_string()
                        };

                        detected_collections.push(CollectionType {
                            kind: CollectionKind::Set,
                            element_type: Some(element_type),
                            key_type: None,
                            value_type: None,
                            is_frozen: false,
                        });
                    }
                    Value::Map(map) => {
                        let (key_type, value_type) = if !map.is_empty() {
                            let keys: Vec<Value> = map.iter().map(|(k, _)| k.clone()).collect();
                            let values: Vec<Value> = map.iter().map(|(_, v)| v.clone()).collect();
                            (
                                self.infer_element_type(&keys),
                                self.infer_element_type(&values),
                            )
                        } else {
                            ("text".to_string(), "text".to_string())
                        };

                        detected_collections.push(CollectionType {
                            kind: CollectionKind::Map,
                            element_type: None,
                            key_type: Some(key_type),
                            value_type: Some(value_type),
                            is_frozen: false,
                        });
                    }
                    Value::Tuple(_) => {
                        detected_collections.push(CollectionType {
                            kind: CollectionKind::Tuple,
                            element_type: None,
                            key_type: None,
                            value_type: None,
                            is_frozen: false,
                        });
                    }
                    _ => {}
                }
            }

            // Use the most common collection type for this column
            if let Some(collection_type) =
                self.select_most_common_collection_type(detected_collections)
            {
                context
                    .collection_types
                    .insert(column_name.clone(), collection_type);
            }
        }

        Ok(())
    }

    /// Parse collection type from type string
    fn parse_collection_type(&self, type_str: &str) -> Option<CollectionType> {
        let lower_type = type_str.to_lowercase();

        if lower_type.starts_with("list<") {
            let element_type = self.extract_inner_type(type_str, "list<");
            Some(CollectionType {
                kind: CollectionKind::List,
                element_type: Some(element_type),
                key_type: None,
                value_type: None,
                is_frozen: false,
            })
        } else if lower_type.starts_with("set<") {
            let element_type = self.extract_inner_type(type_str, "set<");
            Some(CollectionType {
                kind: CollectionKind::Set,
                element_type: Some(element_type),
                key_type: None,
                value_type: None,
                is_frozen: false,
            })
        } else if lower_type.starts_with("map<") {
            let (key_type, value_type) = self.extract_map_types(type_str);
            Some(CollectionType {
                kind: CollectionKind::Map,
                element_type: None,
                key_type: Some(key_type),
                value_type: Some(value_type),
                is_frozen: false,
            })
        } else if lower_type.starts_with("tuple<") {
            Some(CollectionType {
                kind: CollectionKind::Tuple,
                element_type: None,
                key_type: None,
                value_type: None,
                is_frozen: false,
            })
        } else if lower_type.starts_with("frozen<") {
            // Parse the inner type and mark as frozen
            if let Some(mut inner_collection) =
                self.parse_collection_type(&self.extract_inner_type(type_str, "frozen<"))
            {
                inner_collection.is_frozen = true;
                Some(inner_collection)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Extract inner type from generic type string
    fn extract_inner_type(&self, type_str: &str, _prefix: &str) -> String {
        if let Some(start) = type_str.find('<') {
            if let Some(end) = type_str.rfind('>') {
                return type_str[start + 1..end].to_string();
            }
        }
        "text".to_string()
    }

    /// Extract key and value types from map type string
    fn extract_map_types(&self, type_str: &str) -> (String, String) {
        if let Some(start) = type_str.find('<') {
            if let Some(end) = type_str.rfind('>') {
                let inner = &type_str[start + 1..end];
                if let Some(comma_pos) = inner.find(',') {
                    let key_type = inner[..comma_pos].trim().to_string();
                    let value_type = inner[comma_pos + 1..].trim().to_string();
                    return (key_type, value_type);
                }
            }
        }
        ("text".to_string(), "text".to_string())
    }

    /// Infer element type from a collection of values
    fn infer_element_type(&self, elements: &[Value]) -> String {
        if elements.is_empty() {
            return "text".to_string();
        }

        // Count type occurrences
        let mut type_counts = HashMap::new();
        for element in elements {
            let type_name = match element {
                Value::Text(_) => "text",
                Value::Integer(_) => "int",
                Value::BigInt(_) => "bigint",
                Value::Float(_) => "double",
                Value::Boolean(_) => "boolean",
                Value::Uuid(_) => "uuid",
                Value::Timestamp(_) => "timestamp",
                Value::Blob(_) => "blob",
                _ => "text",
            };
            *type_counts.entry(type_name).or_insert(0) += 1;
        }

        // Return most common type
        type_counts
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(type_name, _)| type_name.to_string())
            .unwrap_or_else(|| "text".to_string())
    }

    /// Select the most common collection type from detected types
    fn select_most_common_collection_type(
        &self,
        mut types: Vec<CollectionType>,
    ) -> Option<CollectionType> {
        if types.is_empty() {
            return None;
        }

        // For simplicity, just return the first type
        // In a real implementation, you'd want to find the most common type
        types.sort_by(|a, b| format!("{:?}", a.kind).cmp(&format!("{:?}", b.kind)));
        types.into_iter().next()
    }

    pub(super) async fn discover_indexes(&self, context: &mut DiscoveryContext) -> Result<()> {
        // Index discovery from SSTable metadata and headers
        for header in &context.headers {
            // Check for index-related metadata in the header
            if let Some(index_info) = self.extract_index_info_from_header(header) {
                context.indexes.extend(index_info);
            }
        }

        // Infer potential indexes from column usage patterns
        for (column_name, samples) in &context.column_samples {
            // Check if column has characteristics that suggest indexing
            if self.should_suggest_index(column_name, samples) {
                let index_def = IndexDefinition {
                    name: format!("{}_idx", column_name),
                    target_column: column_name.clone(),
                    index_type: IndexType::Secondary,
                    options: HashMap::new(),
                };

                // Only add if not already present
                if !context
                    .indexes
                    .iter()
                    .any(|idx| idx.target_column == *column_name)
                {
                    context.indexes.push(index_def);
                }
            }
        }

        Ok(())
    }

    /// Extract index information from SSTable header
    fn extract_index_info_from_header(
        &self,
        _header: &SSTableHeader,
    ) -> Option<Vec<IndexDefinition>> {
        // SSTable headers don't typically contain direct index information
        // This would need to be enhanced with actual index discovery logic
        // from Cassandra's system tables or index files
        None
    }

    /// Determine if a column should have an index based on usage patterns
    fn should_suggest_index(&self, column_name: &str, samples: &[Value]) -> bool {
        // Simple heuristics for index suggestion

        // 1. Columns with "id" in the name (common lookup pattern)
        if column_name.to_lowercase().contains("id") {
            return true;
        }

        // 2. Columns with high cardinality (many unique values)
        let unique_values: std::collections::HashSet<_> = samples.iter().collect();
        let cardinality_ratio = unique_values.len() as f64 / samples.len() as f64;
        if cardinality_ratio > 0.7 && samples.len() > 10 {
            return true;
        }

        // 3. Columns that look like foreign keys or references
        if column_name.to_lowercase().ends_with("_id")
            || column_name.to_lowercase().ends_with("_ref")
            || column_name.to_lowercase().contains("email")
            || column_name.to_lowercase().contains("username")
        {
            return true;
        }

        // 4. UUID columns (often used for lookups)
        if samples.iter().any(|v| matches!(v, Value::Uuid(_))) {
            return true;
        }

        false
    }

    pub(super) async fn build_schema_info(
        &self,
        context: &mut DiscoveryContext,
    ) -> Result<SchemaInfo> {
        let mut partition_key = Vec::new();
        let mut clustering_keys = Vec::new();
        let mut regular_columns = Vec::new();
        let mut static_columns = Vec::new();

        // Extract column information from headers
        let mut position = 0;
        for header in &context.headers {
            for column_def in &header.columns {
                // Infer type from header and samples
                let samples = context
                    .column_samples
                    .get(&column_def.name)
                    .cloned()
                    .unwrap_or_default();
                let type_info = self
                    .type_inference
                    .infer_column_type(&samples)
                    .await
                    .unwrap_or_else(|_| TypeInfo {
                        type_id: column_def.column_type.clone(),
                        type_params: vec![],
                        is_frozen: false,
                        element_type: None,
                        key_type: None,
                        value_type: None,
                        udt_fields: None,
                        tuple_elements: None,
                    });

                let confidence = self.calculate_type_confidence(&samples, &type_info);

                let column = ColumnDefinition {
                    name: column_def.name.clone(),
                    data_type: column_def.column_type.clone(),
                    type_info,
                    nullable: true,   // Assume nullable unless proven otherwise
                    is_static: false, // Would need additional logic to detect static columns
                    default_value: None,
                    position,
                    confidence,
                };

                // Classify column based on position and naming patterns
                if self.is_partition_key_column(&column_def.name, position) {
                    partition_key.push(column);
                } else if self.is_clustering_column(&column_def.name, position) {
                    clustering_keys.push(ClusteringColumn {
                        name: column_def.name.clone(),
                        data_type: column_def.column_type.clone(),
                        position: clustering_keys.len(), // Use current size as position
                        order: crate::schema::ClusteringOrder::Asc, // Default to ascending
                    });
                } else if column.is_static {
                    static_columns.push(column);
                } else {
                    regular_columns.push(column);
                }

                position += 1;
            }
        }

        // If no partition key was found, assume first column is partition key
        if partition_key.is_empty() && !regular_columns.is_empty() {
            let first_column = regular_columns.remove(0);
            partition_key.push(first_column);
        }

        // Build validation results
        let validation_results = ValidationResults {
            status: self.determine_validation_status(&partition_key, &regular_columns),
            errors: Vec::new(), // Would be populated by actual validation
            warnings: self.generate_validation_warnings(&partition_key, &regular_columns),
            consistency_results: ConsistencyResults {
                files_analyzed: context.source_files.len(),
                schema_mismatches: 0,
                type_inconsistencies: Vec::new(),
                udt_conflicts: Vec::new(),
            },
        };

        Ok(SchemaInfo {
            keyspace: context.keyspace.clone(),
            table: context.table.clone(),
            partition_key,
            clustering_keys,
            regular_columns,
            static_columns,
            collection_types: context.collection_types.clone(),
            user_defined_types: context.discovered_udts.values().cloned().collect(),
            indexes: context.indexes.clone(),
            table_options: context.table_options.clone(),
            metadata: SchemaMetadata {
                discovered_at: std::time::SystemTime::now(),
                source_files: context.source_files.clone(),
                total_rows_sampled: context.total_rows_sampled,
                cassandra_version: context.cassandra_version,
                discovery_method: DiscoveryMethod::Hybrid,
                version: 1,
                validation_results,
                performance_metrics: DiscoveryMetrics {
                    total_time_ms: 0, // Will be filled in later
                    header_parsing_time_ms: 0,
                    data_sampling_time_ms: 0,
                    type_inference_time_ms: 0,
                    validation_time_ms: 0,
                    peak_memory_usage_bytes: 0,
                },
            },
        })
    }

    /// Heuristic to determine if a column is a partition key
    fn is_partition_key_column(&self, column_name: &str, position: usize) -> bool {
        // Simple heuristics - in practice this would be more sophisticated
        position == 0
            || column_name.to_lowercase().contains("key")
            || column_name.to_lowercase() == "id"
            || column_name.to_lowercase().ends_with("_id")
    }

    /// Heuristic to determine if a column is a clustering column
    fn is_clustering_column(&self, column_name: &str, position: usize) -> bool {
        // Simple heuristics
        (position == 1 && !self.is_partition_key_column(column_name, position))
            || column_name.to_lowercase().contains("time")
            || column_name.to_lowercase().contains("date")
            || column_name.to_lowercase().contains("order")
    }
}
