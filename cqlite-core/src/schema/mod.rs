//! Schema definition and parsing for CQLite
//!
//! This module handles schema definitions that describe the structure of
//! Cassandra tables for schema-aware SSTable reading. It supports both
//! JSON-based schema definitions and CQL CREATE TABLE statement parsing.

pub mod aggregator;
pub mod cql_parser;
pub mod discovery;
pub mod parser;
pub mod registry;

// Concern submodules split out of this file (issue #1134, source-split
// doctrine). They add inherent impls on the types defined here and own the
// `UdtRegistry` type; the `pub use` below preserves the `schema::UdtRegistry`
// public path.
pub(crate) mod cql_type_parser;
mod key_ordering;
mod schema_comparator;
mod udt_registry;

pub use udt_registry::{split_qualified_udt, udt_registry_from_cql, UdtRegistry};

/// Test-only work counters for the derived-comparator caching path (issue #1709).
///
/// Compiled out entirely in non-test builds (`#[cfg(test)]`), so they impose
/// zero cost on production code. They let the registry unit tests assert that,
/// after a schema is registered, [`registry::SchemaRegistry::get_parsing_context`]
/// performs ZERO `CqlType::parse` calls and ZERO `TableSchema` deep clones on the
/// request path (both were `O(columns)` / `4` per call before caching).
///
/// The counters are **thread-local**, not global atomics: with the default
/// `#[tokio::test]` current-thread runtime, a test's awaited async work runs on
/// the test's own OS thread, so each test observes only its own parse/clone work
/// and is immune to pollution from tests running concurrently on other threads.
#[cfg(test)]
pub(crate) mod work_counters {
    use std::cell::Cell;

    thread_local! {
        /// Incremented once per [`super::CqlType::parse`] invocation.
        static PARSE_CALLS: Cell<usize> = const { Cell::new(0) };
        /// Incremented once per `TableSchema` deep clone performed by
        /// [`super::registry::SchemaRegistry::get_schema`].
        static SCHEMA_CLONES: Cell<usize> = const { Cell::new(0) };
    }

    pub(crate) fn record_parse_call() {
        PARSE_CALLS.with(|c| c.set(c.get() + 1));
    }

    pub(crate) fn record_schema_clone() {
        SCHEMA_CLONES.with(|c| c.set(c.get() + 1));
    }

    /// Reset both counters to zero (call before the measured request path).
    pub(crate) fn reset() {
        PARSE_CALLS.with(|c| c.set(0));
        SCHEMA_CLONES.with(|c| c.set(0));
    }

    pub(crate) fn parse_calls() -> usize {
        PARSE_CALLS.with(|c| c.get())
    }

    pub(crate) fn schema_clones() -> usize {
        SCHEMA_CLONES.with(|c| c.get())
    }
}

// Re-export aggregator components
pub use aggregator::{
    AggregatorConfig, LoadErrorType, LoadResult, SchemaAggregator, SchemaLoadError,
    SchemaLoadWarning,
};

// Re-export CQL parsing functions
pub use cql_parser::{
    cql_type_to_type_id, extract_table_name, parse_cql_schema, parse_cql_schema_with_visitor,
    parse_create_table, table_name_matches,
};

// Re-export discovery and registry components
pub use discovery::{
    ColumnDefinition, DiscoveryMethod, IndexDefinition, SchemaDiscoveryConfig,
    SchemaDiscoveryEngine, SchemaInfo, SchemaMetadata, TableOptions, TypeInfo, UDTDefinition,
    ValidationError, ValidationResults, ValidationStatus, ValidationWarning,
};

pub use registry::{
    ParsingContext, RegistryStatistics, SchemaChange, SchemaChangeType, SchemaQuery,
    SchemaRegistry, SchemaRegistryConfig, SchemaSource, SchemaValidationStatus, SchemaValidator,
    SchemaVersion, ValidationReport,
};

// Test-only counter for the number of times `find_schema_by_table` deep-clones a
// `TableSchema` out of the registry (issue #1587, E5). A query must resolve its
// schema ONCE and share it by `Arc`, so this stays `== 1` per query (it was 2–4
// when each planning/execution step re-resolved). Same thread-local rationale as
// the query executor's other work counters.
#[cfg(test)]
thread_local! {
    pub(crate) static TABLE_SCHEMA_CLONES: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

pub use parser::SchemaParser;

// Type alias for backward compatibility
pub type ColumnSpec = Column;

use crate::error::{Error, Result};
use crate::parser::header::SSTableHeader;
use crate::parser::types::CqlTypeId;
use crate::storage::StorageEngine;
use crate::types::UdtTypeDef;
use crate::Config;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Table schema definition loaded from JSON
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// Keyspace name
    pub keyspace: String,

    /// Table name
    pub table: String,

    /// Partition key columns (ordered)
    pub partition_keys: Vec<KeyColumn>,

    /// Clustering key columns (ordered)  
    pub clustering_keys: Vec<ClusteringColumn>,

    /// All columns in the table
    pub columns: Vec<Column>,

    /// Optional metadata
    #[serde(default)]
    pub comments: HashMap<String, String>,

    /// Dropped-column drop times in microseconds (column name → drop_time_micros).
    ///
    /// Populated from the schema-loading surface (JSON `dropped_columns`, or set
    /// programmatically) since drop times are assigned at DDL-execution by the
    /// cluster catalog (`system_schema.dropped_columns`) and are not recorded in
    /// local SSTable files or the CQL `DROP COLUMN` text. Used during compaction
    /// to discard cells of a dropped column whose timestamp ≤ the drop time
    /// (Cassandra `cb34ad47`). See issues #904 (this plumbing) and #847 (the
    /// merge-side filter).
    ///
    /// Scope (#847): this map carries only the drop time, so the dropped column's
    /// pre-drop cells are decoded using its CURRENT type in [`Self::columns`] (the
    /// decode contract enforced by [`Self::validate_dropped_columns`]). That is
    /// byte-correct when the column's type is unchanged — the common case. A
    /// column dropped and later RE-ADDED with a DIFFERENT type is out of scope:
    /// the historical cells would be decoded with the new type and could
    /// misparse. Supporting per-version types requires carrying type metadata
    /// here (or decoding from the SSTable serialization-header type) and is
    /// follow-up work alongside the element-level representation in #899.
    ///
    /// Filtering is also at row-timestamp granularity (the merge stream surfaces
    /// only the row write-time per cell); exact per-cell purging is tracked as
    /// follow-up #922.
    #[serde(default)]
    pub dropped_columns: HashMap<String, i64>,
}

/// Partition key column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyColumn {
    /// Column name
    pub name: String,

    /// CQL data type
    #[serde(rename = "type")]
    pub data_type: String,

    /// Position in composite key (0-based)
    pub position: usize,
}

/// Clustering key column with ordering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusteringColumn {
    /// Column name
    pub name: String,

    /// CQL data type
    #[serde(rename = "type")]
    pub data_type: String,

    /// Position in clustering key (0-based)
    pub position: usize,

    /// Sort order (ASC or DESC)
    #[serde(default)]
    pub order: ClusteringOrder,
}

/// Clustering order enum for sorting
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ClusteringOrder {
    /// Ascending order
    #[default]
    Asc,
    /// Descending order
    Desc,
}

impl From<&str> for ClusteringOrder {
    fn from(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "DESC" => ClusteringOrder::Desc,
            _ => ClusteringOrder::Asc,
        }
    }
}

impl std::fmt::Display for ClusteringOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusteringOrder::Asc => write!(f, "ASC"),
            ClusteringOrder::Desc => write!(f, "DESC"),
        }
    }
}

/// Regular column definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    /// Column name
    pub name: String,

    /// CQL data type (e.g., "text", "bigint", "list<int>")
    #[serde(rename = "type")]
    pub data_type: String,

    /// Whether column can be null
    #[serde(default)]
    pub nullable: bool,

    /// Default value (if any)
    #[serde(default)]
    pub default: Option<serde_json::Value>,

    /// Whether this is a STATIC column
    #[serde(default)]
    pub is_static: bool,
}

/// Parsed CQL data type
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum CqlType {
    // Primitive types
    Boolean,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Counter,
    Float,
    Double,
    Decimal,
    Text,
    Ascii,
    Varchar,
    Blob,
    Timestamp,
    Date,
    Time,
    Uuid,
    TimeUuid,
    Inet,
    Duration,
    Varint,

    // Collection types (implemented as tuples)
    List(Box<CqlType>),
    Set(Box<CqlType>),
    Map(Box<CqlType>, Box<CqlType>),

    // Complex types
    Tuple(Vec<CqlType>),
    Udt(String, Vec<(String, CqlType)>), // name, fields
    Frozen(Box<CqlType>),

    // Custom/Unknown
    Custom(String),
}

/// Whether a de-prefixed `Custom` type name is a plausible UDT reference.
///
/// `CqlType::parse` returns `Custom(..)` both for real UDT names and for type
/// strings it cannot structurally parse (e.g. an uppercase `SET<TEXT>` whose
/// collection prefix it doesn't recognize). Only simple identifiers
/// (alphanumeric / `_` / `.`) can name a UDT, so structural fragments
/// containing `<`, `>`, `,` or whitespace are excluded from UDT validation.
pub(crate) fn is_udt_identifier(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '.')
}

impl TableSchema {
    /// Extract schema from SSTable header column metadata
    ///
    /// This method constructs a TableSchema from the column information
    /// embedded in the SSTable header's SerializationHeader.
    pub fn from_sstable_header(header: &SSTableHeader) -> Result<Self> {
        // Separate columns by role
        let mut partition_keys = Vec::new();
        let mut clustering_keys = Vec::new();
        let mut regular_columns = Vec::new();

        for col_info in &header.columns {
            if col_info.is_primary_key {
                if col_info.is_clustering {
                    clustering_keys.push(col_info);
                } else {
                    partition_keys.push(col_info);
                }
            } else {
                regular_columns.push(col_info);
            }
        }

        // Validate all partition keys have positions
        for col_info in &partition_keys {
            if col_info.key_position.is_none() {
                return Err(Error::schema(format!(
                    "Partition key column '{}' missing key_position in SSTable header",
                    col_info.name
                )));
            }
        }

        // Validate all clustering keys have positions
        for col_info in &clustering_keys {
            if col_info.key_position.is_none() {
                return Err(Error::schema(format!(
                    "Clustering key column '{}' missing key_position in SSTable header",
                    col_info.name
                )));
            }
        }

        // Sort by header's key_position to establish canonical ordering
        partition_keys.sort_by_key(|c| c.key_position.unwrap());
        clustering_keys.sort_by_key(|c| c.key_position.unwrap());

        // Build KeyColumn with contiguous 0-based positions for CQLite's internal representation
        // (SSTable key_position values may have gaps; we normalize to [0,1,2,...])
        let partition_keys: Vec<KeyColumn> = partition_keys
            .iter()
            .enumerate()
            .map(|(pos, col)| KeyColumn {
                name: col.name.clone(),
                data_type: col.column_type.clone(),
                position: pos, // Contiguous internal position, not header key_position
            })
            .collect();

        // Build ClusteringColumn with contiguous positions
        let clustering_keys: Vec<ClusteringColumn> = clustering_keys
            .iter()
            .enumerate()
            .map(|(pos, col)| ClusteringColumn {
                name: col.name.clone(),
                data_type: col.column_type.clone(),
                position: pos, // Contiguous internal position, not header key_position
                // Issue #759: the serialization header wraps a DESC clustering
                // column's comparator in `ReversedType(...)`. That authoritative
                // signal is captured in `ColumnInfo::clustering_reversed` during
                // Statistics.db parsing (no heuristics). `data_type` already holds
                // the unwrapped inner CQL type, so deserialization is undisturbed.
                order: if col.clustering_reversed {
                    ClusteringOrder::Desc
                } else {
                    ClusteringOrder::Asc
                },
            })
            .collect();

        // All columns including keys
        let columns: Vec<Column> = header
            .columns
            .iter()
            .map(|col| Column {
                name: col.name.clone(),
                data_type: col.column_type.clone(),
                nullable: !col.is_primary_key, // Primary keys are non-nullable
                default: None,
                // Static-column classification is authoritative metadata from the
                // Statistics.db SerializationHeader (definitive guide Ch.7 / Appendix B),
                // surfaced on ColumnInfo.is_static. Issue #758 / Epic #756.
                is_static: col.is_static,
            })
            .collect();

        if partition_keys.is_empty() {
            return Err(Error::schema(
                "No partition keys found in SSTable header".to_string(),
            ));
        }

        let schema = TableSchema {
            keyspace: header.keyspace.clone(),
            table: header.table_name.clone(),
            partition_keys,
            clustering_keys,
            columns,
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        schema.validate()?;
        Ok(schema)
    }

    /// Load schema from JSON file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)
            .map_err(|e| Error::schema(format!("Failed to read schema file: {}", e)))?;

        Self::from_json(&content)
    }

    /// Parse schema from JSON string
    pub fn from_json(json: &str) -> Result<Self> {
        let schema: TableSchema = serde_json::from_str(json)
            .map_err(|e| Error::schema(format!("Invalid JSON schema: {}", e)))?;

        schema.validate()?;
        Ok(schema)
    }

    /// Save schema to JSON file
    pub fn to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| Error::serialization(format!("Failed to serialize schema: {}", e)))?;

        fs::write(path, json)
            .map_err(|e| Error::schema(format!("Failed to write schema file: {}", e)))?;

        Ok(())
    }

    /// Validate schema consistency
    pub fn validate(&self) -> Result<()> {
        // Validate keyspace and table names
        if self.keyspace.is_empty() {
            return Err(Error::schema("Keyspace name cannot be empty".to_string()));
        }

        if self.table.is_empty() {
            return Err(Error::schema("Table name cannot be empty".to_string()));
        }

        // Must have at least one partition key
        if self.partition_keys.is_empty() {
            return Err(Error::schema(
                "Table must have at least one partition key".to_string(),
            ));
        }

        // Validate partition key positions are contiguous
        let mut positions: Vec<_> = self.partition_keys.iter().map(|k| k.position).collect();
        positions.sort();
        for (i, &pos) in positions.iter().enumerate() {
            if pos != i {
                return Err(Error::schema(format!(
                    "Partition key positions must be contiguous starting from 0, found gap at position {}",
                    i
                )));
            }
        }

        // Validate clustering key positions (if any)
        if !self.clustering_keys.is_empty() {
            let mut positions: Vec<_> = self.clustering_keys.iter().map(|k| k.position).collect();
            positions.sort();
            for (i, &pos) in positions.iter().enumerate() {
                if pos != i {
                    return Err(Error::schema(format!(
                        "Clustering key positions must be contiguous starting from 0, found gap at position {}",
                        i
                    )));
                }
            }
        }

        // Validate data types
        for column in &self.columns {
            CqlType::parse(&column.data_type).map_err(|e| {
                Error::schema(format!(
                    "Invalid data type '{}' for column '{}': {}",
                    column.data_type, column.name, e
                ))
            })?;
        }

        // NOTE: UDT-reference validation requires a registry and is not done here
        // (a TableSchema is self-contained). At schema-load time, call
        // `validate_udt_references(&registry)` to fail fast on undefined UDTs.

        // Validate all key columns exist in columns list
        for key in &self.partition_keys {
            if !self.columns.iter().any(|c| c.name == key.name) {
                return Err(Error::schema(format!(
                    "Partition key '{}' not found in columns list",
                    key.name
                )));
            }
        }

        for key in &self.clustering_keys {
            if !self.columns.iter().any(|c| c.name == key.name) {
                return Err(Error::schema(format!(
                    "Clustering key '{}' not found in columns list",
                    key.name
                )));
            }
        }

        self.validate_dropped_columns()?;

        Ok(())
    }

    /// The **post-drop** schema that compaction uses to *write* its output.
    ///
    /// The decode schema retains dropped columns (carrying their type) so input
    /// cells can be parsed and then purged by the merge filter (see
    /// [`Self::validate_dropped_columns`]). The compaction *output* must keep its
    /// serialization header / row column bitmap consistent with the cells that
    /// actually survive the merge:
    ///
    /// - A dropped column with **no surviving cells** (all cells were at or
    ///   before its drop time) is removed from `columns` so it does not appear in
    ///   the output header. This lets a natural post-drop reader schema (which
    ///   omits the column) read the output without the header-column /
    ///   bitmap-index misalignment that retaining it would cause (roborev #847).
    /// - A dropped column with **surviving cells** (re-added: cells written after
    ///   `drop_time`) is RETAINED in `columns`, because the merge still emits
    ///   those cells and the writer needs a matching header column — otherwise
    ///   the cell would be serialized with no header entry and corrupt the row.
    ///
    /// `retained` is the set of dropped-column names that had surviving cells
    /// (computed by `compact_sstables` from a merge pre-pass). The returned
    /// schema carries an empty `dropped_columns` map: the purge has already
    /// happened, so the output must not re-purge the surviving cells on a later
    /// compaction.
    pub fn for_compaction_output(
        &self,
        retained: &std::collections::HashSet<String>,
    ) -> TableSchema {
        TableSchema {
            keyspace: self.keyspace.clone(),
            table: self.table.clone(),
            partition_keys: self.partition_keys.clone(),
            clustering_keys: self.clustering_keys.clone(),
            columns: self
                .columns
                .iter()
                .filter(|c| {
                    // Keep a column unless it is a dropped column with no
                    // surviving cells.
                    !self.dropped_columns.contains_key(&c.name) || retained.contains(&c.name)
                })
                .cloned()
                .collect(),
            comments: self.comments.clone(),
            dropped_columns: HashMap::new(),
        }
    }

    /// Validate the dropped-column decode contract (#904/#847).
    ///
    /// Dropped-column filtering during compaction discards a dropped column's
    /// cells *after they are decoded*. The schema-driven reader only decodes a
    /// column whose name is present in [`Self::columns`] (it intersects the
    /// on-disk serialization-header columns with the schema); a column absent
    /// from `columns` is skipped without consuming its bytes, so its cells would
    /// never reach the filter and surrounding columns could misalign.
    ///
    /// Therefore every column named in `dropped_columns` MUST remain declared in
    /// `columns` (carrying its type) so its cells decode and can be purged. This
    /// mirrors Cassandra retaining a dropped column's type in
    /// `system_schema.dropped_columns`. Decoding a dropped column that is absent
    /// from `columns` (purely from header type metadata) is follow-up work
    /// related to #899 and intentionally out of scope here.
    pub fn validate_dropped_columns(&self) -> Result<()> {
        for name in self.dropped_columns.keys() {
            if !self.columns.iter().any(|c| &c.name == name) {
                return Err(Error::schema(format!(
                    "dropped column '{}' must remain declared in `columns` (with its type) so \
                     its cells can be decoded and purged during compaction; a dropped column \
                     present only in `dropped_columns` cannot be decoded (see #904/#847)",
                    name
                )));
            }
        }
        Ok(())
    }

    /// Validate that every UDT referenced by a column exists in the registry.
    ///
    /// This is a schema-load-time pass that fails fast with a schema-category
    /// error naming the missing UDT, instead of surfacing the problem later as a
    /// confusing parse/deserialization error (issue #761). Nested references are
    /// validated recursively: a UDT inside a collection, `frozen<>`, a tuple, or
    /// another UDT is checked just like a top-level reference.
    ///
    /// UDTs are looked up in the schema's own keyspace; the `system` keyspace is
    /// also consulted so built-in/system UDTs resolve regardless of the table's
    /// keyspace.
    pub fn validate_udt_references(&self, registry: &UdtRegistry) -> Result<()> {
        for column in &self.columns {
            // Reuse the same parse the rest of validation uses; parse errors are
            // reported by `validate()`, so ignore them here.
            if let Ok(cql_type) = CqlType::parse(&column.data_type) {
                self.check_type_udt_references(&cql_type, &column.name, registry)?;
            }
        }
        Ok(())
    }

    /// Recursively check a single CQL type for UDT references that are not
    /// present in the registry.
    fn check_type_udt_references(
        &self,
        cql_type: &CqlType,
        column_name: &str,
        registry: &UdtRegistry,
    ) -> Result<()> {
        match cql_type {
            CqlType::Udt(name, _) => {
                self.ensure_udt_exists(name, column_name, registry)?;
            }
            // `CqlType::parse` represents UDT references as `Custom("udt:<name>")`
            // for names with mixed case / underscores / digits, but as a bare
            // `Custom("<name>")` for purely-lowercase names (e.g. `address`).
            // Validate the de-prefixed name *only* when it is a simple type
            // identifier: `parse` also yields a bare `Custom` for type strings it
            // can't structurally parse (e.g. an uppercase `SET<TEXT>` collection),
            // which must NOT be mistaken for a UDT (roborev job 39 + the
            // collections-fixture regression).
            CqlType::Custom(name) => {
                let udt_name = name.strip_prefix("udt:").unwrap_or(name);
                if is_udt_identifier(udt_name) {
                    self.ensure_udt_exists(udt_name, column_name, registry)?;
                }
            }
            CqlType::List(inner) | CqlType::Set(inner) | CqlType::Frozen(inner) => {
                self.check_type_udt_references(inner, column_name, registry)?;
            }
            CqlType::Map(key_type, value_type) => {
                self.check_type_udt_references(key_type, column_name, registry)?;
                self.check_type_udt_references(value_type, column_name, registry)?;
            }
            CqlType::Tuple(field_types) => {
                for field_type in field_types {
                    self.check_type_udt_references(field_type, column_name, registry)?;
                }
            }
            _ => {} // Primitive types reference no UDTs.
        }
        Ok(())
    }

    /// Confirm a referenced UDT exists in the table's keyspace (or the `system`
    /// keyspace), returning a schema-category error naming the missing UDT.
    fn ensure_udt_exists(
        &self,
        udt_name: &str,
        column_name: &str,
        registry: &UdtRegistry,
    ) -> Result<()> {
        // A reference may be qualified as `keyspace.udt`; honor an explicit
        // keyspace, otherwise resolve against the table's keyspace. Routes through
        // the single shared splitter (issue #2807).
        let (lookup_keyspace, bare_name) =
            crate::schema::split_qualified_udt(udt_name, self.keyspace.as_str());

        if registry.contains_udt(lookup_keyspace, bare_name)
            || registry.contains_udt("system", bare_name)
        {
            return Ok(());
        }

        Err(Error::schema(format!(
            "Column '{}' references undefined UDT '{}' in keyspace '{}'",
            column_name, udt_name, lookup_keyspace
        )))
    }

    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Check if column is a partition key
    pub fn is_partition_key(&self, name: &str) -> bool {
        self.partition_keys.iter().any(|k| k.name == name)
    }

    /// Check if column is a clustering key
    pub fn is_clustering_key(&self, name: &str) -> bool {
        self.clustering_keys.iter().any(|k| k.name == name)
    }

    // `ordered_partition_keys` / `ordered_clustering_keys` live in the
    // `key_ordering` submodule (issue #1677): they memoize-avoid the per-call
    // re-sort via a "sort only if not already ordered" fast path.

    /// Create a minimal test schema (for testing only)
    #[cfg(test)]
    pub fn new_for_testing(keyspace: &str, table: &str) -> Self {
        Self {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            }],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }
}

/// Schema management service for handling table schemas and UDT definitions.
///
/// Issue #1708 (one schema source of truth): the manager holds an
/// `Arc<RwLock<SchemaRegistry>>` and RESOLVES every schema/UDT lookup THROUGH
/// that registry (and REGISTERS manager-side loads INTO it). It keeps NO
/// by-value schema/UDT copy of its own, so registry-side TTL-refresh /
/// auto-discovery and manager-side loads are always mutually visible — the two
/// can never silently diverge. The registry owns freshness (TTL/refresh); the
/// manager delegates.
#[derive(Debug)]
pub struct SchemaManager {
    #[allow(dead_code)]
    storage: Arc<StorageEngine>,
    /// The single source of truth for table schemas and UDTs.
    registry: Arc<RwLock<registry::SchemaRegistry>>,
}

impl SchemaManager {
    /// Build a default schema registry sharing the given platform/config.
    async fn default_registry(
        platform: Arc<crate::platform::Platform>,
        config: &Config,
    ) -> Result<Arc<RwLock<registry::SchemaRegistry>>> {
        let registry = registry::SchemaRegistry::new(
            registry::SchemaRegistryConfig::default(),
            platform,
            config.clone(),
        )
        .await?;
        Ok(Arc::new(RwLock::new(registry)))
    }

    /// Create a new schema manager from a path
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        // Create temporary storage engine (not actually used in this context)
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await?);
        let storage = Arc::new(
            StorageEngine::open(
                path.as_ref(),
                &config,
                platform.clone(),
                #[cfg(feature = "state_machine")]
                None,
            )
            .await?,
        );

        let registry = Self::default_registry(platform, &config).await?;
        Ok(Self { storage, registry })
    }

    /// Create a new schema manager with storage
    pub async fn new_with_storage(storage: Arc<StorageEngine>, config: &Config) -> Result<Self> {
        let platform = Arc::new(crate::platform::Platform::new(config).await?);
        let registry = Self::default_registry(platform, config).await?;
        let manager = Self { storage, registry };

        // Load built-in UDT definitions for Cassandra 5.0 compatibility
        manager.load_default_udts().await;

        Ok(manager)
    }

    /// Create a new schema manager with a pre-loaded SchemaRegistry
    ///
    /// This constructor is used when schemas are loaded from external .cql files
    /// during ingestion, allowing the pre-loaded schemas to be used by the query engine.
    ///
    /// Issue #1708: the registry is SHARED by reference (not snapshotted). Every
    /// subsequent registry-side update is immediately visible to this manager,
    /// and every manager-side load is registered back into this same registry.
    ///
    /// # Arguments
    ///
    /// * `storage` - The storage engine instance
    /// * `registry` - Pre-loaded schema registry from ingestion
    /// * `_config` - Database configuration (currently unused)
    pub async fn new_with_registry(
        storage: Arc<StorageEngine>,
        registry: Arc<tokio::sync::RwLock<registry::SchemaRegistry>>,
        _config: &Config,
    ) -> Result<Self> {
        Ok(Self { storage, registry })
    }

    /// Access the shared schema registry (test-only).
    ///
    /// Lets tests register schemas/UDTs into the single source of truth the
    /// manager resolves through, to assert cross-visibility (issue #1708).
    #[cfg(test)]
    pub(crate) fn registry(&self) -> Arc<RwLock<registry::SchemaRegistry>> {
        self.registry.clone()
    }

    /// Load default UDT definitions that are commonly used in Cassandra
    async fn load_default_udts(&self) {
        // Common address UDT used in many Cassandra schemas
        let address_udt = UdtTypeDef::new("test_keyspace".to_string(), "address".to_string())
            .with_field("street".to_string(), CqlType::Text, true)
            .with_field("city".to_string(), CqlType::Text, true)
            .with_field("state".to_string(), CqlType::Text, true)
            .with_field("zip_code".to_string(), CqlType::Text, true)
            .with_field("country".to_string(), CqlType::Text, true);

        self.register_udt(address_udt).await;

        // Enhanced person UDT with nested address
        let person_udt = UdtTypeDef::new("test_keyspace".to_string(), "person".to_string())
            .with_field("name".to_string(), CqlType::Text, true)
            .with_field("age".to_string(), CqlType::Int, true)
            .with_field("email".to_string(), CqlType::Text, true)
            .with_field(
                "addresses".to_string(),
                CqlType::List(Box::new(CqlType::Udt(
                    "address".to_string(),
                    vec![
                        ("street".to_string(), CqlType::Text),
                        ("city".to_string(), CqlType::Text),
                        ("state".to_string(), CqlType::Text),
                        ("zip_code".to_string(), CqlType::Text),
                        ("country".to_string(), CqlType::Text),
                    ],
                ))),
                true,
            )
            .with_field(
                "contact_info".to_string(),
                CqlType::Map(Box::new(CqlType::Text), Box::new(CqlType::Text)),
                true,
            );

        self.register_udt(person_udt).await;

        // Company UDT with nested person and address relationships
        let company_udt = UdtTypeDef::new("test_keyspace".to_string(), "company".to_string())
            .with_field("name".to_string(), CqlType::Text, false)
            .with_field(
                "headquarters".to_string(),
                CqlType::Udt(
                    "address".to_string(),
                    vec![
                        ("street".to_string(), CqlType::Text),
                        ("city".to_string(), CqlType::Text),
                        ("state".to_string(), CqlType::Text),
                        ("zip_code".to_string(), CqlType::Text),
                        ("country".to_string(), CqlType::Text),
                    ],
                ),
                true,
            )
            .with_field(
                "employees".to_string(),
                CqlType::Set(Box::new(CqlType::Udt("person".to_string(), vec![]))),
                true,
            )
            .with_field("founded_year".to_string(), CqlType::Int, true);

        self.register_udt(company_udt).await;
    }

    /// Register a new UDT type definition into the shared registry (issue #1708).
    pub async fn register_udt(&self, udt_def: UdtTypeDef) {
        // `SchemaRegistry::register_udt` is infallible in practice (it only
        // inserts into the in-memory UDT registry); ignore the `Ok(())`.
        let _ = self.registry.read().await.register_udt(udt_def).await;
    }

    /// Get a UDT definition from the shared registry (returns a cloned UdtTypeDef).
    pub async fn get_udt(&self, keyspace: &str, name: &str) -> Option<UdtTypeDef> {
        self.registry
            .read()
            .await
            .get_udt(keyspace, name)
            .await
            .ok()
            .flatten()
    }

    /// Load schema for a table.
    ///
    /// Returns `Err(Error::Schema(..))` for an unknown table. CQLite never
    /// fabricates a schema for a table nobody defined — doing so would return
    /// fabricated-shape rows for undefined tables, violating the no-heuristics
    /// mandate. This mirrors the I3 (#1626) hard-fail precedent (issue #1710).
    ///
    /// This resolves THROUGH the shared registry (issue #1708), so a
    /// registry-side TTL-refresh / auto-discovery is always observed and a
    /// stale by-value snapshot can never be served. No write happens on the
    /// unknown-table path.
    pub async fn load_schema(&self, table_name: &str) -> Result<TableSchema> {
        // A `keyspace.table` name resolves as an exact scoped lookup; a bare
        // name matches by table name across keyspaces.
        let (keyspace, table) = match table_name.split_once('.') {
            Some((ks, tbl)) => (Some(ks.to_string()), tbl),
            None => (None, table_name),
        };
        self.find_schema_by_table(&keyspace, table)
            .await?
            .ok_or_else(|| {
                Error::schema(format!(
                    "unknown table {}; no schema registered or discovered",
                    table_name
                ))
            })
    }

    /// Parse and register a schema from a CQL CREATE TABLE statement.
    ///
    /// Registers INTO the shared registry (issue #1708) so the parsed schema is
    /// immediately visible to every other holder of the registry (parsing paths,
    /// other managers), not stored in a private manager-only map.
    pub async fn parse_and_register_cql_schema(&self, cql: &str) -> Result<TableSchema> {
        let schema = cql_parser::parse_cql_schema(cql)?;
        self.registry
            .read()
            .await
            .register_schema(schema.clone(), registry::SchemaSource::Cql(cql.to_string()))
            .await?;
        Ok(schema)
    }

    /// Find schema by table name with optional keyspace matching.
    ///
    /// Resolves THROUGH the shared registry (issue #1708); the registry owns
    /// freshness (issue #1708 roborev Medium): a fresh matched entry is cloned
    /// once (issue #1587), an EXPIRED entry is refreshed rather than served
    /// stale, and an unregistered table yields `Ok(None)` with NO fabrication
    /// (issue #1710). The registry's refresh error (if any) propagates as `Err`.
    pub async fn find_schema_by_table(
        &self,
        keyspace: &Option<String>,
        table: &str,
    ) -> Result<Option<TableSchema>> {
        let found = self
            .registry
            .read()
            .await
            .find_schema_by_table(keyspace, table)
            .await?;
        // Preserve the issue #1587 (E5) once-per-query deep-clone accounting: the
        // registry performs exactly one `TableSchema` clone on a hit.
        #[cfg(test)]
        if found.is_some() {
            TABLE_SCHEMA_CLONES.with(|c| c.set(c.get() + 1));
        }
        Ok(found)
    }

    /// Extract table information from CQL without full parsing
    pub fn extract_table_info(&self, cql: &str) -> Result<(Option<String>, String)> {
        cql_parser::extract_table_name(cql)
    }

    /// Convert CQL type string to internal type ID
    pub fn cql_type_to_internal(&self, cql_type: &str) -> Result<CqlTypeId> {
        cql_parser::cql_type_to_type_id(cql_type)
    }

    /// Get table schema by name (async for compatibility)
    pub async fn get_table_schema(&self, table_name: &str) -> Result<TableSchema> {
        // Try to find schema by table name
        if let Some(schema) = self.find_schema_by_table(&None, table_name).await? {
            Ok(schema)
        } else {
            Err(Error::Schema(format!(
                "Table schema not found: {}",
                table_name
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_validation() {
        let schema_json = r#"
        {
            "keyspace": "test",
            "table": "users",
            "partition_keys": [
                {"name": "id", "type": "bigint", "position": 0}
            ],
            "clustering_keys": [],
            "columns": [
                {"name": "id", "type": "bigint", "nullable": false},
                {"name": "name", "type": "text", "nullable": true}
            ]
        }
        "#;

        let schema = TableSchema::from_json(schema_json).unwrap();
        assert_eq!(schema.keyspace, "test");
        assert_eq!(schema.table, "users");
        assert_eq!(schema.partition_keys.len(), 1);
        assert_eq!(schema.columns.len(), 2);
    }

    #[test]
    fn test_schema_validation_failures() {
        // Missing partition key
        let invalid_schema = r#"
        {
            "keyspace": "test",
            "table": "users", 
            "partition_keys": [],
            "clustering_keys": [],
            "columns": []
        }
        "#;

        assert!(TableSchema::from_json(invalid_schema).is_err());

        // Invalid type
        let invalid_type = r#"
        {
            "keyspace": "test",
            "table": "users",
            "partition_keys": [
                {"name": "id", "type": "invalid_type", "position": 0}
            ],
            "clustering_keys": [],
            "columns": [
                {"name": "id", "type": "invalid_type", "nullable": false}
            ]
        }
        "#;

        // This should succeed as we allow custom types
        assert!(TableSchema::from_json(invalid_type).is_ok());
    }

    fn udt_schema(column_type: &str) -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "t".to_string(),
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
                    name: "value".to_string(),
                    data_type: column_type.to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    #[test]
    fn test_udt_reference_undefined_top_level_errors() {
        let registry = UdtRegistry::new();
        let schema = udt_schema("MyMissingType");

        let err = schema
            .validate_udt_references(&registry)
            .expect_err("undefined UDT reference must fail validation");
        let msg = err.to_string();
        assert!(
            matches!(err, Error::Schema(_)),
            "expected schema-category error, got {err:?}"
        );
        assert!(
            msg.contains("MyMissingType"),
            "error must name the missing UDT, got: {msg}"
        );
    }

    #[test]
    fn test_udt_reference_undefined_lowercase_errors() {
        // Regression (roborev job 39): a UDT name that is purely lowercase
        // letters (no underscore/digit) parses to a bare `Custom("<name>")`
        // with no `udt:` prefix, so validation must still catch it —
        // top-level and nested.
        let registry = UdtRegistry::new();
        for col_type in ["address", "list<frozen<address>>"] {
            let schema = udt_schema(col_type);
            let err = schema
                .validate_udt_references(&registry)
                .expect_err("undefined lowercase UDT must fail validation");
            assert!(matches!(err, Error::Schema(_)), "got {err:?}");
            assert!(
                err.to_string().contains("address"),
                "error must name the missing UDT, got: {err}"
            );
        }
    }

    #[test]
    fn test_uppercase_collection_of_primitives_is_not_a_udt() {
        // Regression (collections fixture): uppercase collections of primitives
        // must parse and not be mistaken for a UDT reference.
        let registry = UdtRegistry::new();
        for col_type in [
            "SET<TEXT>",
            "LIST<INT>",
            "MAP<TEXT, TEXT>",
            "FROZEN<LIST<INT>>",
        ] {
            let schema = udt_schema(col_type);
            schema
                .validate_udt_references(&registry)
                .unwrap_or_else(|e| panic!("'{col_type}' must not be flagged as a UDT: {e}"));
        }
    }

    #[test]
    fn test_uppercase_collection_with_undefined_udt_errors() {
        // Regression (roborev job 51): an undefined UDT nested inside an
        // UPPERCASE collection must still be detected — case-insensitive parsing
        // means the nested reference is validated, not skipped.
        let registry = UdtRegistry::new();
        for col_type in [
            "LIST<MissingType>",
            "MAP<TEXT, MissingType>",
            "FROZEN<SET<MissingType>>",
        ] {
            let schema = udt_schema(col_type);
            let err = schema
                .validate_udt_references(&registry)
                .expect_err("undefined UDT in uppercase collection must fail");
            assert!(matches!(err, Error::Schema(_)), "got {err:?}");
            assert!(
                err.to_string().contains("MissingType"),
                "error must name the missing UDT, got: {err}"
            );
        }
    }

    #[test]
    fn test_udt_reference_undefined_nested_in_collection_errors() {
        let registry = UdtRegistry::new();
        let schema = udt_schema("list<frozen<NestedMissing>>");

        let err = schema
            .validate_udt_references(&registry)
            .expect_err("nested undefined UDT reference must fail validation");
        let msg = err.to_string();
        assert!(matches!(err, Error::Schema(_)));
        assert!(
            msg.contains("NestedMissing"),
            "error must name the nested missing UDT, got: {msg}"
        );
    }

    #[test]
    fn test_udt_reference_undefined_nested_in_map_errors() {
        let registry = UdtRegistry::new();
        let schema = udt_schema("map<text, MapValueMissing>");

        let err = schema
            .validate_udt_references(&registry)
            .expect_err("undefined UDT in map value must fail validation");
        assert!(matches!(err, Error::Schema(_)));
        assert!(err.to_string().contains("MapValueMissing"));
    }

    #[test]
    fn test_udt_reference_defined_top_level_ok() {
        let mut registry = UdtRegistry::new();
        registry.register_udt(
            UdtTypeDef::new("test_ks".to_string(), "MyType".to_string()).with_field(
                "a".to_string(),
                CqlType::Text,
                true,
            ),
        );
        let schema = udt_schema("MyType");
        schema
            .validate_udt_references(&registry)
            .expect("defined UDT should validate");
    }

    #[test]
    fn test_udt_reference_defined_nested_ok() {
        let mut registry = UdtRegistry::new();
        registry.register_udt(
            UdtTypeDef::new("test_ks".to_string(), "MyType".to_string()).with_field(
                "a".to_string(),
                CqlType::Text,
                true,
            ),
        );
        let schema = udt_schema("list<frozen<MyType>>");
        schema
            .validate_udt_references(&registry)
            .expect("defined nested UDT should validate");
    }

    #[test]
    fn test_validate_udt_references_no_udts_ok() {
        // Schemas without any UDT columns must validate against an empty registry.
        let registry = UdtRegistry::new();
        let schema = udt_schema("map<text, list<int>>");
        schema
            .validate_udt_references(&registry)
            .expect("primitive/collection-only schema should validate");
    }

    async fn build_storage() -> Arc<StorageEngine> {
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());
        let temp_dir = tempfile::tempdir().unwrap();
        Arc::new(
            StorageEngine::open(
                temp_dir.path(),
                &config,
                platform,
                #[cfg(feature = "state_machine")]
                None,
            )
            .await
            .unwrap(),
        )
    }

    async fn build_shared_registry() -> Arc<RwLock<registry::SchemaRegistry>> {
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());
        Arc::new(RwLock::new(
            registry::SchemaRegistry::new(
                registry::SchemaRegistryConfig::default(),
                platform,
                config,
            )
            .await
            .unwrap(),
        ))
    }

    async fn test_manager() -> Arc<SchemaManager> {
        let config = Config::default();
        let storage = build_storage().await;
        Arc::new(
            SchemaManager::new_with_storage(storage, &config)
                .await
                .unwrap(),
        )
    }

    fn table_schema_named(table_name: &str) -> TableSchema {
        let mut schema = udt_schema("text");
        schema.table = table_name.to_string();
        schema
    }

    /// Issue #1710: `load_schema` for a table nobody defined must return `Err`,
    /// never a fabricated `uuid id` schema (no-heuristics mandate; I3 #1626
    /// hard-fail precedent). RED on main, which fabricated a default schema.
    #[tokio::test]
    async fn test_load_schema_unknown_table_errs() {
        let manager = test_manager().await;

        let err = manager
            .load_schema("never_defined_table")
            .await
            .expect_err("unknown table must error, not fabricate a schema");
        assert!(matches!(err, Error::Schema(_)), "got {err:?}");
        assert!(
            err.to_string().contains("never_defined_table"),
            "error must name the unknown table, got: {err}"
        );

        // And the failed lookup must NOT register a fabricated entry.
        let stats = manager
            .registry()
            .read()
            .await
            .get_statistics()
            .await
            .unwrap();
        assert_eq!(
            stats.total_schemas, 0,
            "unknown-table lookup must not mutate the registry"
        );
    }

    /// Issue #1710: concurrent `load_schema` of a registered table returns the
    /// real schema from every task, and never mutates the registry.
    #[tokio::test]
    async fn test_concurrent_schema_access() {
        let manager = test_manager().await;

        // Register 3 real schemas up front THROUGH the shared registry (issue
        // #1708 single source of truth; authoritative metadata only).
        for i in 0..3 {
            let name = format!("table_{}", i);
            manager
                .registry()
                .read()
                .await
                .register_schema(table_schema_named(&name), registry::SchemaSource::Manual)
                .await
                .unwrap();
        }

        // Spawn 10 concurrent tasks loading the 3 tables.
        let mut handles = vec![];
        for i in 0..10 {
            let m = Arc::clone(&manager);
            let handle = tokio::spawn(async move {
                let table = format!("table_{}", i % 3);
                let schema = m.load_schema(&table).await.expect("registered table loads");
                assert_eq!(schema.table, table);
            });
            handles.push(handle);
        }
        for handle in handles {
            handle.await.unwrap();
        }

        // Registry is unchanged: exactly the 3 registered entries, no fabrication.
        let stats = manager
            .registry()
            .read()
            .await
            .get_statistics()
            .await
            .unwrap();
        assert_eq!(stats.total_schemas, 3);
        for i in 0..3 {
            assert!(manager.load_schema(&format!("table_{}", i)).await.is_ok());
        }
    }

    /// Issue #1708: the manager must resolve THROUGH the shared registry, never a
    /// stale by-value snapshot captured at construction. Register v1, build the
    /// manager, then update the schema THROUGH the registry (simulating a
    /// TTL-refresh / auto-discovery re-registration) and confirm the manager
    /// observes v2. RED on main (returned the snapshotted v1).
    #[tokio::test]
    async fn test_manager_reflects_registry_updates_no_stale_snapshot() {
        let registry = build_shared_registry().await;

        // v1: two columns (id uuid, value text).
        let v1 = table_schema_named("evolving");
        assert_eq!(v1.columns.len(), 2);
        registry
            .read()
            .await
            .register_schema(v1.clone(), registry::SchemaSource::Manual)
            .await
            .unwrap();

        let storage = build_storage().await;
        let config = Config::default();
        let manager = SchemaManager::new_with_registry(storage, registry.clone(), &config)
            .await
            .unwrap();

        let got_v1 = manager.get_table_schema("evolving").await.unwrap();
        assert_eq!(got_v1.columns.len(), 2, "manager must see v1");

        // Update THROUGH the registry: v2 adds a column.
        let mut v2 = v1.clone();
        v2.columns.push(Column {
            name: "extra".to_string(),
            data_type: "int".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        });
        registry
            .read()
            .await
            .register_schema(v2, registry::SchemaSource::Manual)
            .await
            .unwrap();

        let got_v2 = manager.get_table_schema("evolving").await.unwrap();
        assert_eq!(
            got_v2.columns.len(),
            3,
            "manager must resolve THROUGH the registry, not a stale by-value snapshot"
        );
    }

    /// Issue #1708 (roborev Medium): the registry OWNS freshness and the manager
    /// merely delegates — an entry EXPIRED in the registry must NOT be served
    /// stale through the manager. With `cache_ttl_seconds = 0` the registered
    /// schema expires, so `load_schema`/`get_table_schema` must return the
    /// registry's refresh outcome (an `Err` here — no SSTables back a
    /// manually-registered schema), NOT `Ok(stale schema)`. RED on HEAD, which
    /// read the registry map directly and served the expired entry forever.
    #[tokio::test]
    async fn test_manager_does_not_serve_ttl_expired_schema() {
        let config = Config::default();
        let platform = Arc::new(crate::platform::Platform::new(&config).await.unwrap());
        let mut reg_config = registry::SchemaRegistryConfig::default();
        reg_config.cache_ttl_seconds = 0; // every registered entry expires immediately
        let registry = Arc::new(RwLock::new(
            registry::SchemaRegistry::new(reg_config, platform, config.clone())
                .await
                .unwrap(),
        ));

        // Register a real schema THROUGH the shared registry.
        registry
            .read()
            .await
            .register_schema(
                table_schema_named("stale_tbl"),
                registry::SchemaSource::Manual,
            )
            .await
            .unwrap();

        let storage = build_storage().await;
        let manager = SchemaManager::new_with_registry(storage, registry.clone(), &config)
            .await
            .unwrap();

        // Let the zero-TTL entry expire.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // The manager must delegate to the registry's refresh path (which errors
        // for a manually-registered schema with no backing SSTables) rather than
        // serve the stale entry. On HEAD this returned `Ok(stale)`.
        let loaded = manager.load_schema("stale_tbl").await;
        assert!(
            loaded.is_err(),
            "manager must delegate TTL freshness to the registry, not serve a stale schema; got {loaded:?}"
        );
        let got = manager.get_table_schema("stale_tbl").await;
        assert!(
            got.is_err(),
            "get_table_schema must also honor registry expiry; got {got:?}"
        );

        // The unknown-table no-fabrication contract (#1710) still holds.
        assert!(
            manager.load_schema("never_defined_here").await.is_err(),
            "unknown table still errors (no fabrication)"
        );
    }

    /// Issue #1708: UDT-registry sharing must be constructor-INDEPENDENT. A UDT
    /// registered into the shared registry is visible via the manager's `get_udt`
    /// for EVERY constructor path, and a UDT registered via the manager is
    /// visible via the shared registry. RED on main for `new`/`new_with_storage`
    /// (they built an unshared private UDT-registry copy).
    #[tokio::test]
    async fn test_all_constructors_share_udt_registry() {
        // Manager built via `new`.
        let temp = tempfile::tempdir().unwrap();
        let via_new = SchemaManager::new(temp.path()).await.unwrap();

        // Manager built via `new_with_storage`.
        let config = Config::default();
        let via_storage = {
            let storage = build_storage().await;
            SchemaManager::new_with_storage(storage, &config)
                .await
                .unwrap()
        };

        // Manager built via `new_with_registry`.
        let via_registry = {
            let registry = build_shared_registry().await;
            let storage = build_storage().await;
            SchemaManager::new_with_registry(storage, registry, &config)
                .await
                .unwrap()
        };

        for (label, manager) in [
            ("new", &via_new),
            ("new_with_storage", &via_storage),
            ("new_with_registry", &via_registry),
        ] {
            // registry -> manager visibility
            let point = UdtTypeDef::new("ks_share".to_string(), "point".to_string()).with_field(
                "x".to_string(),
                CqlType::Int,
                true,
            );
            manager
                .registry()
                .read()
                .await
                .register_udt(point)
                .await
                .unwrap();
            assert!(
                manager.get_udt("ks_share", "point").await.is_some(),
                "[{label}] UDT registered in the shared registry must be visible via the manager"
            );

            // manager -> registry visibility
            let line = UdtTypeDef::new("ks_share".to_string(), "line".to_string()).with_field(
                "len".to_string(),
                CqlType::Int,
                true,
            );
            manager.register_udt(line).await;
            let seen = manager
                .registry()
                .read()
                .await
                .get_udt("ks_share", "line")
                .await
                .unwrap();
            assert!(
                seen.is_some(),
                "[{label}] UDT registered via the manager must be visible in the shared registry"
            );
        }
    }

    #[test]
    fn test_schema_from_sstable_header() {
        use crate::parser::header::{
            CassandraVersion, ColumnInfo, CompressionInfo, SSTableHeader, SSTableStats,
        };
        use std::collections::HashMap;

        let columns = vec![
            ColumnInfo {
                name: "id".to_string(),
                column_type: "int".to_string(),
                is_primary_key: true,
                key_position: Some(0),
                is_static: false,
                is_clustering: false,
                clustering_reversed: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                column_type: "text".to_string(),
                is_primary_key: false,
                key_position: None,
                is_static: false,
                is_clustering: false,
                clustering_reversed: false,
            },
        ];

        let header = SSTableHeader {
            cassandra_version: CassandraVersion::V5_0Bti,
            version: 1,
            table_id: [0; 16],
            keyspace: "test_ks".to_string(),
            table_name: "test_table".to_string(),
            generation: 1,
            compression: CompressionInfo {
                algorithm: "NONE".to_string(),
                chunk_size: 0,
                parameters: HashMap::new(),
            },
            stats: SSTableStats::default(),
            columns,
            properties: HashMap::new(),
        };

        let schema = TableSchema::from_sstable_header(&header).unwrap();

        assert_eq!(schema.keyspace, "test_ks");
        assert_eq!(schema.table, "test_table");
        assert_eq!(schema.partition_keys.len(), 1);
        assert_eq!(schema.partition_keys[0].name, "id");
        assert_eq!(schema.columns.len(), 2);
    }
}
