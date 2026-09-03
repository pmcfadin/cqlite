//! Mutation types for CQL write operations
//!
//! Represents INSERT, UPDATE, DELETE operations as structured mutations.
//! Supports cell-level operations with timestamps and TTL.
//!
//! This module implements the core data types for M5 write support:
//! - `Mutation`: Represents a write operation (INSERT, UPDATE, DELETE)
//! - `DecoratedKey`: Token + raw key bytes for partition ordering
//! - `PartitionKey`: Multi-column partition key with schema-aware encoding
//! - `ClusteringKey`: Multi-column clustering key with ASC/DESC ordering
//! - `CellOperation`: Cell-level write/delete operations

use super::clustering_order::compare_values;
use crate::error::{Error, Result};
use crate::schema::{ClusteringOrder, TableSchema};
use crate::types::{ComparatorType, Value};
use std::cmp::Ordering;

/// Table identifier (keyspace + table name)
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TableId {
    /// Keyspace name
    pub keyspace: String,
    /// Table name
    pub table: String,
}

impl TableId {
    /// Create a new table identifier
    pub fn new(keyspace: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            keyspace: keyspace.into(),
            table: table.into(),
        }
    }

    /// Get the fully qualified table name (keyspace.table)
    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.keyspace, self.table)
    }
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.keyspace, self.table)
    }
}

/// A mutation represents a write operation (INSERT, UPDATE, DELETE)
///
/// This is the fundamental unit of write operations in CQLite, corresponding to
/// a single CQL INSERT/UPDATE/DELETE statement. Each mutation targets a specific
/// row (identified by partition key + optional clustering key) and contains
/// one or more cell operations.
///
/// # Tombstone Support (M5.2)
///
/// Mutations can represent various deletion types:
/// - Cell tombstone: `CellOperation::Delete` for single column
/// - Row tombstone: `CellOperation::DeleteRow` for entire row
/// - Range tombstone: `range_tombstones` field for clustering key ranges
/// - Partition tombstone: `partition_tombstone` field for entire partition
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Mutation {
    /// Target table
    pub table: TableId,
    /// Partition key values
    pub partition_key: PartitionKey,
    /// Clustering key values (None for tables without clustering keys)
    pub clustering_key: Option<ClusteringKey>,
    /// Cell-level operations (writes or deletes)
    pub operations: Vec<CellOperation>,
    /// Timestamp in microseconds since Unix epoch
    pub timestamp_micros: i64,
    /// Time-to-live in seconds applied to all cells in this mutation (None = no expiration).
    ///
    /// This is set by `USING TTL` in CQL statements and applies uniformly to all
    /// `Write` operations. For per-column TTL, use `CellOperation::WriteWithTtl`
    /// in the operations list instead.
    pub ttl_seconds: Option<u32>,
    /// Partition tombstone (deletes entire partition)
    pub partition_tombstone: Option<PartitionTombstone>,
    /// Range tombstones (delete clustering key ranges within partition)
    pub range_tombstones: Vec<RangeTombstone>,
    /// Explicit local deletion time (seconds since Unix epoch) for the row and
    /// cell tombstones (`DeleteRow`, `Delete`) produced by this mutation.
    ///
    /// `None` (the default) preserves historical behavior: the writer derives
    /// the local deletion time from `timestamp_micros` (`timestamp_micros /
    /// 1_000_000`). When `Some(ldt)`, the writer emits exactly `ldt` as the
    /// `localDeletionTime` of every row/cell tombstone in this mutation, and
    /// feeds it into the Statistics.db min/max `localDeletionTime` aggregates.
    ///
    /// Partition and range tombstones carry their own `local_deletion_time`
    /// inside [`PartitionTombstone`] / [`RangeTombstone`] and are unaffected by
    /// this field. (Issue #764)
    #[serde(default)]
    pub local_deletion_time: Option<i32>,
    /// Row-level deletion that COEXISTS with the live cells produced by this
    /// mutation's `operations` (issue #932).
    ///
    /// `Some((deletion_time_micros, local_deletion_time_secs))` carries a row
    /// tombstone whose `deletion_time` is DECOUPLED from `timestamp_micros` — it
    /// is older than the row's surviving cells (whose own writetimes drive
    /// `timestamp_micros`). The writer emits a `HAS_DELETION` row carrying BOTH
    /// the deletion AND the surviving cells, so older cells of OTHER columns in
    /// SSTables not part of a partial compaction stay shadowed (they cannot
    /// resurrect). This is distinct from a `CellOperation::DeleteRow`, whose
    /// deletion time IS the mutation timestamp; `row_tombstone` is used only on
    /// the compaction merge→mutation path where the two diverge.
    ///
    /// `None` (the default) preserves historical behavior: a row deletion, when
    /// present, rides as `CellOperation::DeleteRow` at `timestamp_micros`.
    #[serde(default)]
    pub row_tombstone: Option<(i64, i32)>,
    /// Per-column write timestamps (microseconds) for the simple-cell
    /// `Write`/`WriteWithTtl` operations of this mutation (issue #1018).
    ///
    /// `CellOperation::Write`/`WriteWithTtl` carry no per-cell timestamp — every
    /// such cell historically inherits the row's single `timestamp_micros`. On
    /// the compaction merge→mutation path a reconciled row can hold sibling cells
    /// at DIFFERENT writetimes (e.g. live `name`@100 alongside a `score` cell
    /// tombstone@300). Promoting every live cell to the row's MAX writetime would
    /// rewrite `name`'s writetime to 300, losing fidelity. This side-channel maps
    /// a regular column name → that cell's OWN write timestamp so the writer emits
    /// it verbatim (clearing `USE_ROW_TIMESTAMP` when it differs from the row
    /// marker).
    ///
    /// `None` / absent column (the default) preserves historical behavior: the
    /// cell inherits `timestamp_micros`. Populated only by
    /// `merge_entry_to_mutation`; the WAL / CQL-INSERT paths leave it unset (their
    /// cells genuinely share one writetime). `#[serde(default)]` keeps backward
    /// compatibility with mutations serialized before this field existed.
    #[serde(default)]
    pub cell_write_timestamps: Option<std::collections::HashMap<String, i64>>,
}

impl Mutation {
    /// Create a new mutation
    pub fn new(
        table: TableId,
        partition_key: PartitionKey,
        clustering_key: Option<ClusteringKey>,
        operations: Vec<CellOperation>,
        timestamp_micros: i64,
        ttl_seconds: Option<u32>,
    ) -> Self {
        Self {
            table,
            partition_key,
            clustering_key,
            operations,
            timestamp_micros,
            ttl_seconds,
            partition_tombstone: None,
            range_tombstones: Vec::new(),
            local_deletion_time: None,
            row_tombstone: None,
            cell_write_timestamps: None,
        }
    }

    /// Per-cell write timestamp (microseconds) for `column`, falling back to the
    /// mutation's row `timestamp_micros` when no per-cell override was supplied
    /// (issue #1018). Used by the writer to decide whether a simple cell keeps
    /// `USE_ROW_TIMESTAMP` or carries its own explicit delta.
    pub fn cell_write_timestamp(&self, column: &str) -> i64 {
        self.cell_write_timestamps
            .as_ref()
            .and_then(|m| m.get(column).copied())
            .unwrap_or(self.timestamp_micros)
    }

    /// Attach a row-level deletion that coexists with this mutation's live cells
    /// (issue #932).
    ///
    /// `deletion_time` is in microseconds (`markedForDeleteAt`); `ldt` is the
    /// `localDeletionTime` in GC-clock seconds. The writer emits a `HAS_DELETION`
    /// row carrying both the deletion and the surviving cells, decoupling the
    /// deletion time from `timestamp_micros` (the row's liveness writetime).
    #[must_use]
    pub fn with_row_tombstone(mut self, deletion_time: i64, ldt: i32) -> Self {
        self.row_tombstone = Some((deletion_time, ldt));
        self
    }

    /// Set an explicit local deletion time (seconds since Unix epoch) for the
    /// row/cell tombstones produced by this mutation (Issue #764).
    ///
    /// When unset, the writer derives the local deletion time from
    /// `timestamp_micros` exactly as it did historically.
    pub fn with_local_deletion_time(mut self, local_deletion_time: i32) -> Self {
        self.local_deletion_time = Some(local_deletion_time);
        self
    }

    /// Local deletion time (seconds since Unix epoch) to stamp on the row/cell
    /// tombstones of this mutation, falling back to the timestamp-derived value
    /// when no explicit value was supplied (Issue #764).
    pub fn effective_local_deletion_time(&self) -> i32 {
        self.local_deletion_time
            .unwrap_or((self.timestamp_micros / 1_000_000) as i32)
    }

    /// Get the decorated key for this mutation (token + raw bytes)
    pub fn decorated_key(&self, schema: &TableSchema) -> Result<DecoratedKey> {
        self.partition_key.to_decorated_key(schema)
    }
}

/// Partition tombstone for deleting entire partition
///
/// Stored in the partition header and shadows all rows in the partition
/// when the partition deletion time is greater than the row timestamps.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionTombstone {
    /// Deletion timestamp in microseconds since Unix epoch
    pub deletion_time: i64,
    /// Local deletion time in seconds since Unix epoch
    pub local_deletion_time: i32,
}

/// Range tombstone for deleting a range of clustering keys
///
/// Stored as markers within the partition data and shadows all rows
/// in the specified clustering key range.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RangeTombstone {
    /// Start bound (inclusive or exclusive)
    pub start: ClusteringBound,
    /// End bound (inclusive or exclusive)
    pub end: ClusteringBound,
    /// Deletion timestamp in microseconds since Unix epoch
    pub deletion_time: i64,
    /// Local deletion time in seconds since Unix epoch
    pub local_deletion_time: i32,
}

/// Clustering key bound for range tombstones
///
/// Defines the boundary of a range deletion. Can be inclusive or exclusive,
/// or represent the minimum/maximum possible clustering key.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ClusteringBound {
    /// Inclusive bound (clustering key is part of the deletion range)
    Inclusive(ClusteringKey),
    /// Exclusive bound (clustering key is NOT part of the deletion range)
    Exclusive(ClusteringKey),
    /// Before all clustering keys (start of partition)
    Bottom,
    /// After all clustering keys (end of partition)
    Top,
}

/// Operations that can be applied to individual cells within a row.
///
/// # Per-Cell TTL
///
/// Per-cell TTL is supported via the `WriteWithTtl` variant when using
/// the JSON mutation format directly. CQL syntax (`USING TTL`) applies
/// TTL uniformly to all cells in a statement. To set different TTLs
/// per column, submit separate mutations or use JSON mutations with
/// `WriteWithTtl`:
///
/// ```json
/// {"WriteWithTtl": {"column": "session_token", "value": {"Text": "abc"}, "ttl_seconds": 3600}}
/// ```
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum CellOperation {
    /// Write a value to a column
    Write {
        /// Column name
        column: String,
        /// Column value
        value: Value,
    },
    /// Write a value to a column with TTL (expiring cell).
    ///
    /// The cell will expire after `ttl_seconds` seconds. This is the only
    /// way to set per-column TTL — CQL `USING TTL` applies to all cells
    /// in a statement. Use JSON mutations to set different TTLs per column.
    WriteWithTtl {
        /// Column name
        column: String,
        /// Column value
        value: Value,
        /// Time-to-live in seconds
        ttl_seconds: u32,
        /// Authoritative per-cell `localDeletionTime` (seconds since the Unix
        /// epoch, the on-disk GC clock) for this expiring cell — Cassandra's
        /// `localExpirationTime`, equal to `writetime_seconds + ttl_seconds`
        /// (issue #1538).
        ///
        /// When `Some`, the writer stamps the emitting expiring cell with this
        /// LDT VERBATIM rather than deriving one from `SystemTime::now() + ttl`.
        /// The compaction merge→rewrite path
        /// ([`cells_to_cell_operations`]) sets it from the SOURCE cell's own
        /// authoritative on-disk LDT ([`CellData::local_deletion_time`]) so a
        /// live expiring cell that survives a compaction is re-emitted
        /// byte-identically (its `localDeletionTime` is preserved, not
        /// recomputed from the compaction wall clock — mirror of #921's
        /// Delete-path precedent).
        ///
        /// `None` preserves the historical behavior (the writer derives
        /// `now + ttl`), so the fresh CQL/WAL `USING TTL` write paths that build
        /// a `WriteWithTtl` without a surfaced source LDT are unchanged.
        ///
        /// `#[serde(default)]` only helps SELF-DESCRIBING formats (e.g. the JSON
        /// `--mutation` CLI input), where a missing field is defaulted by name.
        /// It does NOT provide WAL back-compat: the WAL uses bincode, a
        /// non-self-describing positional format that IGNORES `#[serde(default)]`,
        /// so upgrade-replay of pre-#1538 `WriteWithTtl` records is handled by the
        /// pre-#1538 mirror layouts in `wal.rs` (`PreCellLdtWriteTtlMutation` /
        /// `PreCellLdtWriteTtlCellOperation`), not by this attribute. Do not delete
        /// those mirrors on the assumption this attribute covers the WAL.
        ///
        /// [`cells_to_cell_operations`]: crate::storage::write_engine::merge
        /// [`CellData::local_deletion_time`]: crate::storage::write_engine::merge::CellData::local_deletion_time
        #[serde(default)]
        local_deletion_time: Option<i32>,
    },
    /// Delete a specific column
    Delete {
        /// Column name
        column: String,
        /// Optional per-cell `localDeletionTime` (seconds since the Unix epoch,
        /// the on-disk GC clock) for this cell tombstone (#921 finding 2).
        ///
        /// When `Some`, the writer stamps the emitted cell tombstone with this
        /// LDT VERBATIM rather than deriving one from the enclosing mutation's
        /// timestamp / [`Mutation::local_deletion_time`]. The compaction
        /// merge→rewrite path sets it from the SOURCE cell tombstone's own LDT
        /// (`TombstoneInfo::local_deletion_time`) so a within-grace cell
        /// tombstone that survives a compaction keeps its original GC clock and
        /// is not purged too early / kept too long in a LATER compaction.
        ///
        /// `None` preserves the historical behavior (the writer derives the LDT
        /// from the mutation), so the WAL / CQL-DELETE paths that build a
        /// `Delete` without a surfaced source LDT are unchanged.
        ///
        /// `#[serde(default)]` keeps backward compatibility with `Delete`
        /// values serialized before this field existed (e.g. older WAL records).
        ///
        /// [`Mutation::local_deletion_time`]: Mutation::local_deletion_time
        #[serde(default)]
        local_deletion_time: Option<i32>,
    },
    /// Delete entire row (row tombstone)
    DeleteRow,
    /// Write (or tombstone) a single element of a non-frozen complex column
    /// (a list/set member or a map entry), carrying its OWN write metadata and
    /// its preserved source cell path (epic #899, Phase B).
    ///
    /// Unlike [`Write`](Self::Write)/[`WriteWithTtl`](Self::WriteWithTtl) — which
    /// describe a whole-column value at one row timestamp — this op preserves the
    /// per-element granularity of Cassandra's multi-cell on-disk layout so the
    /// compaction writer can emit two elements of the same column at DIFFERENT
    /// timestamps (explicit deltas, not one promoted row timestamp), round-trip a
    /// LIST element's 16-byte TimeUUID cell path byte-for-byte, and emit an
    /// element-level tombstone (`value == None`, IS_DELETED).
    ///
    /// PHASE B SCOPE: this variant is plumbing + writer capability only. The real
    /// compaction pipeline (`merge_entry_to_mutation`) does NOT yet emit it — that
    /// flip (and differential byte-parity verification) is Phase C. In Phase B it
    /// is exercised by unit tests that hand-construct mutations.
    WriteComplexElement {
        /// Complex column name.
        column: String,
        /// Raw cell-path bytes identifying this element (the serialized element
        /// for a SET, the serialized key for a MAP, the 16-byte TimeUUID for a
        /// LIST). Preserved byte-for-byte from the source SSTable — never
        /// regenerated.
        cell_path: Vec<u8>,
        /// Decoded element value. `None` means an element-level tombstone
        /// (IS_DELETED) or an empty-value element (e.g. SET members store the
        /// element in the path with an empty value). Use `is_deleted` — NOT the
        /// shape of this field — to distinguish the two.
        value: Option<Value>,
        /// Per-element write timestamp in microseconds. When equal to the row
        /// liveness timestamp the writer keeps `USE_ROW_TIMESTAMP` (0x08);
        /// otherwise it clears the flag and writes an explicit unsigned delta.
        timestamp_micros: i64,
        /// TTL in seconds when the element is expiring (`None` otherwise).
        ttl_seconds: Option<u32>,
        /// `localDeletionTime` in seconds for an expiring / deleted element
        /// (`None` when not applicable). Far-future values in `[2^31, 2^32)` are
        /// carried as the wrapping `as u32 as i32` representation.
        local_deletion_time: Option<i32>,
        /// Authoritative per-element IS_DELETED (0x01) flag, carried verbatim
        /// from the reader's [`ComplexElement::is_deleted`] (the source of
        /// truth). The writer emits `CELL_IS_DELETED` iff this is `true`.
        ///
        /// This is NOT re-derived from `value`/`ttl_seconds`/`local_deletion_time`
        /// (no-heuristics mandate, CLAUDE.md): an expiring SET member legitimately
        /// has `value == None`, `ttl_seconds == Some`, `local_deletion_time ==
        /// Some` while `is_deleted == false` — re-deriving would misclassify it as
        /// a tombstone (roborev #885, Finding 2).
        ///
        /// [`ComplexElement::is_deleted`]: crate::storage::sstable::reader::compaction_row::ComplexElement::is_deleted
        is_deleted: bool,
    },
    /// A per-column complex deletion marker (the `markedForDeleteAt` +
    /// `localDeletionTime` tombstone Cassandra writes ahead of a multi-cell
    /// column's elements) covering elements written at or before
    /// `marked_for_delete_at` (epic #899, Phase B).
    ///
    /// When present for a complex column, the writer emits a REAL complex
    /// deletion marker (replacing the hardcoded `DeletionTime.LIVE` sentinel)
    /// followed by the surviving per-element cells.
    ///
    /// PHASE B SCOPE: plumbing + writer capability only; not yet emitted by
    /// `merge_entry_to_mutation` (Phase C).
    ComplexDeletion {
        /// Complex column name.
        column: String,
        /// `markedForDeleteAt` in microseconds.
        marked_for_delete_at: i64,
        /// `localDeletionTime` in seconds since the Unix epoch.
        local_deletion_time: i32,
    },
}

/// Partition key with multi-column support
///
/// Stores the partition key as a list of (column name, value) pairs.
/// The order must match the schema's partition key definition.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PartitionKey {
    /// Column name and value pairs (in schema order)
    pub columns: Vec<(String, Value)>,
}

impl PartitionKey {
    /// Create a new partition key
    pub fn new(columns: Vec<(String, Value)>) -> Self {
        Self { columns }
    }

    /// Create a partition key from a single column
    pub fn single(column: impl Into<String>, value: Value) -> Self {
        Self {
            columns: vec![(column.into(), value)],
        }
    }

    /// Serialize partition key to bytes according to Cassandra's on-disk encoding.
    ///
    /// Single-component keys are written as raw value bytes.
    /// Multi-component keys use `[len][value][0x00]` per component, including a
    /// trailing `0x00` after the final component.
    pub fn to_bytes(&self, schema: &TableSchema) -> Result<Vec<u8>> {
        if self.columns.is_empty() {
            return Err(Error::InvalidInput("Empty partition key".to_string()));
        }

        // Validate column count matches schema
        if self.columns.len() != schema.partition_keys.len() {
            return Err(Error::InvalidInput(format!(
                "Partition key column count mismatch: expected {}, got {}",
                schema.partition_keys.len(),
                self.columns.len()
            )));
        }

        // Single-component key: no length prefix. Return the serialized value
        // directly to avoid allocating (and copying into) an intermediate Vec.
        if self.columns.len() == 1 {
            return self.serialize_value(&self.columns[0].1, &schema.partition_keys[0]);
        }

        let mut result = Vec::new();

        // Multi-component partition keys use a `0x00` end-of-component marker
        // after every component, including the last one.
        for (i, (_, value)) in self.columns.iter().enumerate() {
            let value_bytes = self.serialize_value(value, &schema.partition_keys[i])?;
            let len = value_bytes.len();
            if len > u16::MAX as usize {
                return Err(Error::InvalidInput(format!(
                    "Partition key component too large: {} bytes",
                    len
                )));
            }
            // 2-byte big-endian length prefix
            result.extend_from_slice(&(len as u16).to_be_bytes());
            result.extend_from_slice(&value_bytes);
            result.push(0x00);
        }

        Ok(result)
    }

    /// Convert to DecoratedKey (token + raw bytes)
    pub fn to_decorated_key(&self, schema: &TableSchema) -> Result<DecoratedKey> {
        let key_bytes = self.to_bytes(schema)?;
        let token = calculate_murmur3_token(&key_bytes)?;
        Ok(DecoratedKey::new(token, key_bytes))
    }

    /// Deserialize partition key from raw bytes (inverse of `to_bytes`)
    ///
    /// Single-component keys are raw value bytes.
    /// Multi-component keys use `[len:u16 BE][value bytes][0x00]` per component.
    pub fn from_bytes(data: &[u8], schema: &TableSchema) -> Result<Self> {
        if schema.partition_keys.is_empty() {
            return Err(Error::InvalidInput(
                "Schema has no partition keys".to_string(),
            ));
        }

        if data.is_empty() {
            return Err(Error::InvalidInput("Empty partition key bytes".to_string()));
        }

        // Delegate to the canonical codec shared with the read/scan path so the
        // two never diverge (Issue #586).
        let columns =
            crate::storage::partition_key_codec::decode_partition_key_columns(data, schema)?;

        Ok(PartitionKey { columns })
    }

    /// Serialize a single value to bytes according to its CQL type
    fn serialize_value(
        &self,
        value: &Value,
        key_column: &crate::schema::KeyColumn,
    ) -> Result<Vec<u8>> {
        // Get comparator type from schema
        let comparator = ComparatorType::from_data_type(&key_column.data_type)?;

        serialize_value_bytes(value, &comparator)
    }
}

/// Clustering key with multi-column support
///
/// Stores the clustering key as a list of (column name, value) pairs.
/// The order must match the schema's clustering key definition.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClusteringKey {
    /// Column name and value pairs (in schema order)
    pub columns: Vec<(String, Value)>,
}

impl ClusteringKey {
    /// Create a new clustering key
    pub fn new(columns: Vec<(String, Value)>) -> Self {
        Self { columns }
    }

    /// Create a clustering key from a single column
    pub fn single(column: impl Into<String>, value: Value) -> Self {
        Self {
            columns: vec![(column.into(), value)],
        }
    }

    /// Compare two clustering keys according to schema-defined ordering
    ///
    /// Each clustering column can be ASC or DESC. This method requires
    /// schema information to determine the correct ordering.
    pub fn compare(&self, other: &Self, schema: &TableSchema) -> Result<Ordering> {
        // A clustering key may have *fewer* stored components than the schema
        // defines (an absent trailing component). Cassandra treats an absent
        // component exactly like an explicit NULL: it sorts first regardless of
        // ASC/DESC. We therefore iterate over the schema clustering positions
        // and read each side positionally via `.get(i)`, rather than zipping the
        // two `columns` vectors (which would stop at the shorter key and report
        // a falsely-Equal ordering for a genuinely absent trailing component).
        if self.columns.len() > schema.clustering_keys.len()
            || other.columns.len() > schema.clustering_keys.len()
        {
            return Err(Error::Schema(format!(
                "Clustering key has more columns than schema: {} / {} > {}",
                self.columns.len(),
                other.columns.len(),
                schema.clustering_keys.len()
            )));
        }

        for (i, cluster_col) in schema.clustering_keys.iter().enumerate() {
            // Absent component (shorter key) is equivalent to an explicit NULL.
            let a_val = self.columns.get(i).map(|(_, v)| v).unwrap_or(&Value::Null);
            let b_val = other.columns.get(i).map(|(_, v)| v).unwrap_or(&Value::Null);

            // Cassandra (ref 587612cd): clustering reversal (DESC) only reverses
            // the comparison of two *present* values. An absent/NULL component
            // sorts first regardless of ASC/DESC, so the reversal must never flip
            // null/empty-component ordering. Empty-vs-valued comparisons are
            // routed through the type (compare_values) so they reverse correctly
            // on DESC columns, but an absent or NULL component is not a typed
            // value and is handled positionally here.
            let final_ordering = match (a_val, b_val) {
                // Two absent/NULL components are equal in either direction.
                (Value::Null, Value::Null) => Ordering::Equal,
                // Exactly one side absent/NULL: NULL sorts first (Less)
                // regardless of ASC/DESC. Do not apply the DESC reversal.
                (Value::Null, _) => Ordering::Less,
                (_, Value::Null) => Ordering::Greater,
                // Both present: compare through the type, then apply DESC
                // reversal so empty-vs-valued ordering matches Cassandra.
                (_, _) => {
                    let ordering = compare_values(a_val, b_val)?;
                    if cluster_col.order == ClusteringOrder::Desc {
                        ordering.reverse()
                    } else {
                        ordering
                    }
                }
            };

            if final_ordering != Ordering::Equal {
                return Ok(final_ordering);
            }
        }

        Ok(Ordering::Equal)
    }
}

impl Ord for ClusteringKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // Fallback comparison without schema: lexicographic by value
        // This is used for BTreeMap ordering in memtable.
        // Schema-aware comparison should use `compare()` method.
        for ((_, a_val), (_, b_val)) in self.columns.iter().zip(other.columns.iter()) {
            let ordering = compare_values(a_val, b_val).unwrap_or_else(|_| {
                // Type mismatch fallback: deterministic ordering via Debug representation.
                // This only matters for BTreeMap key placement in memtable, not persistence.
                // Heterogeneous SSTables (e.g. Frozen(List) vs List) must not crash the process.
                format!("{a_val:?}").cmp(&format!("{b_val:?}"))
            });
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        self.columns.len().cmp(&other.columns.len())
    }
}

impl PartialOrd for ClusteringKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// PartialEq is defined in terms of `Ord`, NOT derived. A derived `PartialEq`
// would use `Value`'s IEEE-754 `PartialEq` (where `-0.0 == +0.0` and
// `NaN != NaN`), which is inconsistent with `Ord`/`Eq` (issues #1870/#2010:
// clustering comparison now uses the Cassandra/Java total order, where `-0.0`
// and `+0.0` are distinct and `NaN == NaN`). Delegating to `cmp` keeps
// PartialEq consistent with Ord/Eq (Rust contract) AND matches Cassandra row
// identity for signed-zero and NaN clustering values. (`ClusteringKey` does
// not derive `Hash`, so there is no eq/hash contract to uphold here.)
impl PartialEq for ClusteringKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for ClusteringKey {}

/// Decorated key: Murmur3 token + raw partition key bytes
///
/// This is the fundamental ordering key in Cassandra SSTables. Partitions are
/// ordered first by token (i64), then by raw key bytes for collision resolution.
///
/// # Hash Collision Handling
///
/// While Murmur3 hash collisions are extremely rare in practice, the ordering
/// implementation handles them correctly:
/// 1. Primary ordering: by token (Murmur3 hash value)
/// 2. Secondary ordering: by raw partition key bytes (for hash collisions)
///
/// This ensures deterministic, stable ordering even when two different partition
/// keys produce the same token value.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DecoratedKey {
    /// Murmur3 hash token (i64)
    pub token: i64,
    /// Raw partition key bytes
    pub key: Vec<u8>,
}

impl DecoratedKey {
    /// Create a new decorated key
    pub fn new(token: i64, key: Vec<u8>) -> Self {
        Self { token, key }
    }

    /// Create a decorated key from raw partition key bytes
    pub fn from_key_bytes(key_bytes: Vec<u8>) -> Result<Self> {
        let token = calculate_murmur3_token(&key_bytes)?;
        Ok(Self::new(token, key_bytes))
    }
}

impl Ord for DecoratedKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // Primary ordering: by token
        match self.token.cmp(&other.token) {
            Ordering::Equal => {
                // Secondary ordering: by raw key bytes (for hash collisions)
                self.key.cmp(&other.key)
            }
            other => other,
        }
    }
}

impl PartialOrd for DecoratedKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Calculate Murmur3 token from partition key bytes
///
/// Uses Cassandra's Murmur3Partitioner algorithm:
/// 1. Compute Cassandra's `MurmurHash.hash3_x64_128`
/// 2. Take `h1` as the signed token
/// 3. Apply `normalize`: map `i64::MIN` → `i64::MAX` (Cassandra excludes MIN_VALUE)
fn calculate_murmur3_token(key_bytes: &[u8]) -> Result<i64> {
    if key_bytes.is_empty() {
        return Ok(i64::MIN);
    }

    Ok(crate::util::cassandra_murmur3::cassandra_murmur3_token(
        key_bytes,
    ))
}

/// Serialize a Value to bytes according to its CQL type
///
/// This is used for partition key encoding and follows Cassandra's
/// type-specific serialization rules.
fn serialize_value_bytes(value: &Value, comparator: &ComparatorType) -> Result<Vec<u8>> {
    match (value, comparator) {
        (Value::Null, _) => Ok(Vec::new()),

        (Value::Boolean(b), ComparatorType::Boolean) => Ok(vec![if *b { 1 } else { 0 }]),

        (Value::TinyInt(n), ComparatorType::TinyInt) => Ok(vec![*n as u8]),

        (Value::SmallInt(n), ComparatorType::SmallInt) => Ok(n.to_be_bytes().to_vec()),

        (Value::Integer(n), ComparatorType::Int) => Ok(n.to_be_bytes().to_vec()),

        (Value::BigInt(n), ComparatorType::BigInt) => Ok(n.to_be_bytes().to_vec()),

        (Value::Counter(n), ComparatorType::Counter) => Ok(n.to_be_bytes().to_vec()),

        (Value::Float32(f), ComparatorType::Float32) => Ok(f.to_bits().to_be_bytes().to_vec()),

        (Value::Float(f), ComparatorType::Float) => Ok(f.to_bits().to_be_bytes().to_vec()),

        (Value::Text(s), ComparatorType::Text) => Ok(s.to_vec()),

        (Value::Blob(bytes), ComparatorType::Blob) => Ok(bytes.to_vec()),

        (Value::Timestamp(millis), ComparatorType::Timestamp) => Ok(millis.to_be_bytes().to_vec()),

        (Value::Date(days), ComparatorType::Date) => {
            // Cassandra DATE: stored as unsigned int with Integer.MIN_VALUE offset
            let stored = days.wrapping_sub(i32::MIN) as u32;
            Ok(stored.to_be_bytes().to_vec())
        }

        (Value::Uuid(bytes), ComparatorType::Uuid) => Ok(bytes.to_vec()),

        // Time and Inet are mapped to Custom types in ComparatorType
        (Value::Time(nanos), ComparatorType::Custom(name)) if name == "time" => {
            Ok(nanos.to_be_bytes().to_vec())
        }

        (Value::Inet(bytes), ComparatorType::Custom(name)) if name == "inet" => Ok(bytes.to_vec()),

        (Value::Varint(bytes), ComparatorType::Varint) => Ok(bytes.to_vec()),

        (Value::Decimal { scale, unscaled }, ComparatorType::Decimal) => {
            // Decimal: [scale (4B BE i32)][unscaled bytes]
            let mut result = Vec::new();
            result.extend_from_slice(&scale.to_be_bytes());
            result.extend_from_slice(unscaled);
            Ok(result)
        }

        (
            Value::Duration {
                months,
                days,
                nanos,
            },
            ComparatorType::Duration,
        ) => {
            // Duration: [months (4B)][days (4B)][nanos (8B)]
            let mut result = Vec::new();
            result.extend_from_slice(&months.to_be_bytes());
            result.extend_from_slice(&days.to_be_bytes());
            result.extend_from_slice(&nanos.to_be_bytes());
            Ok(result)
        }

        _ => Err(Error::InvalidInput(format!(
            "Type mismatch: value {:?} does not match comparator {:?}",
            value, comparator
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ClusteringColumn, ClusteringOrder, KeyColumn};
    use std::collections::HashMap;

    fn create_test_schema(
        partition_cols: Vec<(&str, &str)>,
        clustering_cols: Vec<(&str, &str, ClusteringOrder)>,
    ) -> TableSchema {
        TableSchema {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            partition_keys: partition_cols
                .into_iter()
                .enumerate()
                .map(|(i, (name, data_type))| KeyColumn {
                    name: name.to_string(),
                    data_type: data_type.to_string(),
                    position: i,
                })
                .collect(),
            clustering_keys: clustering_cols
                .into_iter()
                .enumerate()
                .map(|(i, (name, data_type, order))| ClusteringColumn {
                    name: name.to_string(),
                    data_type: data_type.to_string(),
                    position: i,
                    order,
                })
                .collect(),
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    #[test]
    fn test_table_id() {
        let table_id = TableId::new("my_keyspace", "my_table");
        assert_eq!(table_id.keyspace, "my_keyspace");
        assert_eq!(table_id.table, "my_table");
        assert_eq!(table_id.qualified_name(), "my_keyspace.my_table");
        assert_eq!(table_id.to_string(), "my_keyspace.my_table");
    }

    #[test]
    fn test_partition_key_single_int() {
        let schema = create_test_schema(vec![("id", "int")], vec![]);
        let pk = PartitionKey::single("id", Value::Integer(42));

        let bytes = pk.to_bytes(&schema).unwrap();
        // Single component: no length prefix, just 4-byte big-endian int
        assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x2A]);
    }

    #[test]
    fn test_partition_key_multi_component() {
        let schema = create_test_schema(vec![("id", "int"), ("name", "text")], vec![]);
        let pk = PartitionKey::new(vec![
            ("id".to_string(), Value::Integer(42)),
            ("name".to_string(), Value::text("hello".to_string())),
        ]);

        let bytes = pk.to_bytes(&schema).unwrap();
        // Multi-component partition key format:
        // [len1(2B)][val1][0x00][len2(2B)][val2][0x00]
        let expected = vec![
            0x00, 0x04, // len1 = 4
            0x00, 0x00, 0x00, 0x2A, // int = 42
            0x00, // end-of-component after component 1
            0x00, 0x05, // len2 = 5
            b'h', b'e', b'l', b'l', b'o', // text = "hello"
            0x00, // end-of-component after component 2
        ];
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_partition_key_three_components() {
        // Issue #438: Verify 3-component composite keys (e.g., tick_data table)
        let schema = create_test_schema(
            vec![("symbol", "text"), ("exchange", "text"), ("bucket", "int")],
            vec![],
        );
        let pk = PartitionKey::new(vec![
            ("symbol".to_string(), Value::text("AAPL".to_string())),
            ("exchange".to_string(), Value::text("NYSE".to_string())),
            ("bucket".to_string(), Value::Integer(100)),
        ]);

        let bytes = pk.to_bytes(&schema).unwrap();
        // Composite partition key format: end-of-component byte after every component
        let expected = vec![
            0x00, 0x04, // len1 = 4
            b'A', b'A', b'P', b'L', // "AAPL"
            0x00, // end-of-component
            0x00, 0x04, // len2 = 4
            b'N', b'Y', b'S', b'E', // "NYSE"
            0x00, // end-of-component
            0x00, 0x04, // len3 = 4
            0x00, 0x00, 0x00, 0x64, // int = 100
            0x00, // end-of-component
        ];
        assert_eq!(bytes, expected);
    }

    #[test]
    fn test_decorated_key_ordering() {
        let dk1 = DecoratedKey::new(100, vec![1, 2, 3]);
        let dk2 = DecoratedKey::new(200, vec![1, 2, 3]);
        let dk3 = DecoratedKey::new(100, vec![1, 2, 4]);

        // Order by token first
        assert!(dk1 < dk2);
        assert!(dk2 > dk1);

        // Equal tokens: order by key bytes
        assert!(dk1 < dk3);
        assert!(dk3 > dk1);

        // Equal tokens and keys
        let dk4 = DecoratedKey::new(100, vec![1, 2, 3]);
        assert_eq!(dk1, dk4);
    }

    #[test]
    fn test_murmur3_token_empty_key() {
        let token = calculate_murmur3_token(&[]).unwrap();
        assert_eq!(token, i64::MIN);
    }

    #[test]
    fn test_murmur3_token_deterministic() {
        let key_bytes = b"test_key";
        let token1 = calculate_murmur3_token(key_bytes).unwrap();
        let token2 = calculate_murmur3_token(key_bytes).unwrap();
        assert_eq!(token1, token2, "Token calculation should be deterministic");
    }

    #[test]
    fn test_murmur3_token_different_keys() {
        let token1 = calculate_murmur3_token(b"key1").unwrap();
        let token2 = calculate_murmur3_token(b"key2").unwrap();
        assert_ne!(
            token1, token2,
            "Different keys should produce different tokens"
        );
    }

    #[test]
    fn test_murmur3_token_matches_cassandra_for_composite_uuid_key() {
        // Verified against Cassandra 5.0:
        // SELECT token(tenant_id, user_id) FROM issue438_probe.multi_pk_raw;
        let key_bytes = vec![
            0x00, 0x10, 0x0f, 0x0f, 0x0f, 0x0f, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x10, 0x0f, 0x0f, 0x0f, 0x0f, 0x00, 0x00, 0x40,
            0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xaa, 0x00,
        ];

        let token = calculate_murmur3_token(&key_bytes).unwrap();
        assert_eq!(token, -5_116_541_970_184_546_410);
    }

    #[test]
    fn test_decorated_key_from_bytes() {
        let key_bytes = vec![0x00, 0x00, 0x00, 0x2A]; // int = 42
        let dk = DecoratedKey::from_key_bytes(key_bytes.clone()).unwrap();

        assert_eq!(dk.key, key_bytes);
        // Token should be calculated consistently
        let expected_token = calculate_murmur3_token(&key_bytes).unwrap();
        assert_eq!(dk.token, expected_token);
    }

    #[test]
    fn test_clustering_key_ordering() {
        let schema = create_test_schema(
            vec![("id", "int")],
            vec![("ts", "timestamp", ClusteringOrder::Asc)],
        );

        let ck1 = ClusteringKey::single("ts", Value::Timestamp(1000));
        let ck2 = ClusteringKey::single("ts", Value::Timestamp(2000));

        let ordering = ck1.compare(&ck2, &schema).unwrap();
        assert_eq!(ordering, Ordering::Less);
    }

    #[test]
    fn test_clustering_key_desc_ordering() {
        let schema = create_test_schema(
            vec![("id", "int")],
            vec![("ts", "timestamp", ClusteringOrder::Desc)],
        );

        let ck1 = ClusteringKey::single("ts", Value::Timestamp(1000));
        let ck2 = ClusteringKey::single("ts", Value::Timestamp(2000));

        let ordering = ck1.compare(&ck2, &schema).unwrap();
        // DESC ordering reverses the comparison
        assert_eq!(ordering, Ordering::Greater);
    }

    // --- Issues #1870/#2010: PartialEq for ClusteringKey must be consistent
    // with Ord/Eq (Cassandra total order), NOT IEEE-754 float equality. ---

    #[test]
    fn test_clustering_key_partial_eq_consistent_with_ord() {
        // Signed zeros are DISTINCT under the Cassandra total order, so
        // -0.0 must NOT equal +0.0 (a derived IEEE PartialEq would merge them).
        let neg_zero = ClusteringKey::single("f", Value::Float(-0.0));
        let pos_zero = ClusteringKey::single("f", Value::Float(0.0));
        assert_eq!(neg_zero.cmp(&pos_zero), Ordering::Less);
        assert_ne!(neg_zero, pos_zero);
        assert_eq!(
            neg_zero == pos_zero,
            neg_zero.cmp(&pos_zero) == Ordering::Equal
        );

        // NaN is EQUAL to itself under the total order (a derived IEEE
        // PartialEq would report NaN != NaN).
        let nan_a = ClusteringKey::single("f", Value::Float(f64::NAN));
        let nan_b = ClusteringKey::single("f", Value::Float(f64::NAN));
        assert_eq!(nan_a.cmp(&nan_b), Ordering::Equal);
        assert_eq!(nan_a, nan_b);

        // Same invariants for the 32-bit float variant.
        let f32_neg_zero = ClusteringKey::single("f", Value::Float32(-0.0));
        let f32_pos_zero = ClusteringKey::single("f", Value::Float32(0.0));
        assert_ne!(f32_neg_zero, f32_pos_zero);
        let f32_nan_a = ClusteringKey::single("f", Value::Float32(f32::NAN));
        let f32_nan_b = ClusteringKey::single("f", Value::Float32(f32::NAN));
        assert_eq!(f32_nan_a, f32_nan_b);
    }

    // --- Issue #849: clustering reversal must not flip null/empty ordering ---

    #[test]
    fn test_clustering_null_sorts_first_asc() {
        // An absent (NULL) trailing component must sort before any valued
        // component on an ASC clustering column.
        let schema = create_test_schema(
            vec![("id", "int")],
            vec![("ts", "timestamp", ClusteringOrder::Asc)],
        );

        let null_ck = ClusteringKey::single("ts", Value::Null);
        let valued_ck = ClusteringKey::single("ts", Value::Timestamp(1000));

        assert_eq!(
            null_ck.compare(&valued_ck, &schema).unwrap(),
            Ordering::Less,
            "NULL must sort before a value on ASC"
        );
        assert_eq!(
            valued_ck.compare(&null_ck, &schema).unwrap(),
            Ordering::Greater,
            "value must sort after NULL on ASC"
        );
    }

    #[test]
    fn test_clustering_null_sorts_first_desc() {
        // The blanket `ordering.reverse()` previously flipped this so that NULL
        // sorted *after* a value on DESC columns. Cassandra sorts NULL first
        // regardless of ASC/DESC (ref 587612cd).
        let schema = create_test_schema(
            vec![("id", "int")],
            vec![("ts", "timestamp", ClusteringOrder::Desc)],
        );

        let null_ck = ClusteringKey::single("ts", Value::Null);
        let valued_ck = ClusteringKey::single("ts", Value::Timestamp(1000));

        assert_eq!(
            null_ck.compare(&valued_ck, &schema).unwrap(),
            Ordering::Less,
            "NULL must sort before a value even on DESC"
        );
        assert_eq!(
            valued_ck.compare(&null_ck, &schema).unwrap(),
            Ordering::Greater,
            "value must sort after NULL even on DESC"
        );

        // Two NULLs compare equal regardless of order.
        let null_ck2 = ClusteringKey::single("ts", Value::Null);
        assert_eq!(
            null_ck.compare(&null_ck2, &schema).unwrap(),
            Ordering::Equal
        );
    }

    #[test]
    fn test_clustering_empty_vs_valued_asc() {
        // An empty (zero-length) text component routes through the type:
        // empty < non-empty on ASC.
        let schema = create_test_schema(
            vec![("id", "int")],
            vec![("c", "text", ClusteringOrder::Asc)],
        );

        let empty_ck = ClusteringKey::single("c", Value::text(String::new()));
        let valued_ck = ClusteringKey::single("c", Value::text("a".to_string()));

        assert_eq!(
            empty_ck.compare(&valued_ck, &schema).unwrap(),
            Ordering::Less,
            "empty text must sort before non-empty on ASC"
        );
    }

    #[test]
    fn test_clustering_empty_vs_valued_desc() {
        // On a DESC column the type-routed comparison is reversed for two
        // present values: empty > non-empty.
        let schema = create_test_schema(
            vec![("id", "int")],
            vec![("c", "text", ClusteringOrder::Desc)],
        );

        let empty_ck = ClusteringKey::single("c", Value::text(String::new()));
        let valued_ck = ClusteringKey::single("c", Value::text("a".to_string()));

        assert_eq!(
            empty_ck.compare(&valued_ck, &schema).unwrap(),
            Ordering::Greater,
            "empty text must sort after non-empty on DESC (type-routed reversal)"
        );
        assert_eq!(
            valued_ck.compare(&empty_ck, &schema).unwrap(),
            Ordering::Less
        );
    }

    #[test]
    fn test_clustering_null_first_on_second_desc_component() {
        // Multi-component: first component equal, NULL on the second (DESC)
        // component must still sort first.
        let schema = create_test_schema(
            vec![("id", "int")],
            vec![
                ("a", "int", ClusteringOrder::Asc),
                ("b", "timestamp", ClusteringOrder::Desc),
            ],
        );

        let null_b = ClusteringKey::new(vec![
            ("a".to_string(), Value::Integer(1)),
            ("b".to_string(), Value::Null),
        ]);
        let valued_b = ClusteringKey::new(vec![
            ("a".to_string(), Value::Integer(1)),
            ("b".to_string(), Value::Timestamp(5000)),
        ]);

        assert_eq!(
            null_b.compare(&valued_b, &schema).unwrap(),
            Ordering::Less,
            "NULL on a DESC sub-component still sorts first"
        );
    }

    #[test]
    fn test_clustering_absent_trailing_desc_component_sorts_first() {
        // Review (#849): an *absent* trailing component (shorter key) must be
        // treated identically to an explicit NULL. (a=1) vs (a=1, b=5000) where
        // b is DESC: the short key (absent b) must sort FIRST regardless of the
        // DESC order on b. Previously the zip stopped at the shorter key and
        // returned Equal.
        let schema = create_test_schema(
            vec![("id", "int")],
            vec![
                ("a", "int", ClusteringOrder::Asc),
                ("b", "timestamp", ClusteringOrder::Desc),
            ],
        );

        // Short key omits the trailing DESC component `b` entirely.
        let absent_b = ClusteringKey::new(vec![("a".to_string(), Value::Integer(1))]);
        let valued_b = ClusteringKey::new(vec![
            ("a".to_string(), Value::Integer(1)),
            ("b".to_string(), Value::Timestamp(5000)),
        ]);

        assert_eq!(
            absent_b.compare(&valued_b, &schema).unwrap(),
            Ordering::Less,
            "absent trailing DESC component must sort first (null-first), not Equal"
        );
        assert_eq!(
            valued_b.compare(&absent_b, &schema).unwrap(),
            Ordering::Greater,
            "present value must sort after an absent trailing component"
        );

        // An absent trailing component must compare equal to an explicit NULL
        // for the same position.
        let explicit_null_b = ClusteringKey::new(vec![
            ("a".to_string(), Value::Integer(1)),
            ("b".to_string(), Value::Null),
        ]);
        assert_eq!(
            absent_b.compare(&explicit_null_b, &schema).unwrap(),
            Ordering::Equal,
            "absent trailing component == explicit NULL at the same position"
        );
        assert_eq!(
            explicit_null_b.compare(&absent_b, &schema).unwrap(),
            Ordering::Equal,
        );
    }

    #[test]
    fn test_clustering_absent_trailing_asc_component_sorts_first() {
        // Same null-first rule on an ASC trailing component: the shorter key
        // (absent component) sorts before any present value.
        let schema = create_test_schema(
            vec![("id", "int")],
            vec![
                ("a", "int", ClusteringOrder::Asc),
                ("b", "int", ClusteringOrder::Asc),
            ],
        );

        let absent_b = ClusteringKey::new(vec![("a".to_string(), Value::Integer(7))]);
        let valued_b = ClusteringKey::new(vec![
            ("a".to_string(), Value::Integer(7)),
            ("b".to_string(), Value::Integer(0)),
        ]);

        assert_eq!(
            absent_b.compare(&valued_b, &schema).unwrap(),
            Ordering::Less,
            "absent trailing ASC component must sort first"
        );
        assert_eq!(
            valued_b.compare(&absent_b, &schema).unwrap(),
            Ordering::Greater,
        );
    }

    #[test]
    fn test_clustering_both_absent_trailing_component_equal() {
        // Two keys that both omit the trailing component compare Equal.
        let schema = create_test_schema(
            vec![("id", "int")],
            vec![
                ("a", "int", ClusteringOrder::Asc),
                ("b", "timestamp", ClusteringOrder::Desc),
            ],
        );

        let a1 = ClusteringKey::new(vec![("a".to_string(), Value::Integer(3))]);
        let a2 = ClusteringKey::new(vec![("a".to_string(), Value::Integer(3))]);
        assert_eq!(a1.compare(&a2, &schema).unwrap(), Ordering::Equal);
    }

    #[test]
    fn test_mutation_creation() {
        let table_id = TableId::new("ks", "table");
        let pk = PartitionKey::single("id", Value::Integer(1));
        let ops = vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::text("Alice".to_string()),
        }];

        let mutation = Mutation::new(table_id.clone(), pk, None, ops, 1234567890, None);

        assert_eq!(mutation.table.keyspace, "ks");
        assert_eq!(mutation.table.table, "table");
        assert_eq!(mutation.timestamp_micros, 1234567890);
        assert_eq!(mutation.ttl_seconds, None);
        assert_eq!(mutation.operations.len(), 1);
    }

    #[test]
    fn test_cell_operation_write() {
        let op = CellOperation::Write {
            column: "age".to_string(),
            value: Value::Integer(30),
        };

        match op {
            CellOperation::Write { column, value } => {
                assert_eq!(column, "age");
                assert_eq!(value, Value::Integer(30));
            }
            _ => panic!("Expected Write operation"),
        }
    }

    #[test]
    fn test_cell_operation_delete() {
        let op = CellOperation::Delete {
            column: "name".to_string(),
            local_deletion_time: None,
        };

        match op {
            CellOperation::Delete { column, .. } => {
                assert_eq!(column, "name");
            }
            _ => panic!("Expected Delete operation"),
        }
    }

    #[test]
    fn test_cell_operation_delete_row() {
        let op = CellOperation::DeleteRow;
        assert!(matches!(op, CellOperation::DeleteRow));
    }

    #[test]
    fn test_serialize_value_types() {
        // Boolean
        let bytes = serialize_value_bytes(&Value::Boolean(true), &ComparatorType::Boolean).unwrap();
        assert_eq!(bytes, vec![1]);

        // Integer
        let bytes = serialize_value_bytes(&Value::Integer(42), &ComparatorType::Int).unwrap();
        assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x2A]);

        // Text
        let bytes = serialize_value_bytes(&Value::text("hello".to_string()), &ComparatorType::Text)
            .unwrap();
        assert_eq!(bytes, b"hello");

        // UUID
        let uuid_bytes = [
            0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB,
            0xCD, 0xEF,
        ];
        let bytes = serialize_value_bytes(&Value::Uuid(uuid_bytes), &ComparatorType::Uuid).unwrap();
        assert_eq!(bytes, uuid_bytes);
    }

    #[test]
    fn test_compare_values() {
        assert_eq!(
            compare_values(&Value::Integer(1), &Value::Integer(2)).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Integer(2), &Value::Integer(1)).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            compare_values(&Value::Integer(1), &Value::Integer(1)).unwrap(),
            Ordering::Equal
        );

        // Null comparison
        assert_eq!(
            compare_values(&Value::Null, &Value::Integer(1)).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            compare_values(&Value::Integer(1), &Value::Null).unwrap(),
            Ordering::Greater
        );
    }

    #[test]
    fn test_partition_key_to_decorated_key() {
        let schema = create_test_schema(vec![("id", "int")], vec![]);
        let pk = PartitionKey::single("id", Value::Integer(42));

        let dk = pk.to_decorated_key(&schema).unwrap();
        assert_eq!(dk.key, vec![0x00, 0x00, 0x00, 0x2A]);

        // Token should match direct calculation
        let expected_token = calculate_murmur3_token(&dk.key).unwrap();
        assert_eq!(dk.token, expected_token);
    }

    #[test]
    fn test_murmur3_token_cassandra_compatibility() {
        // Test known token values from Cassandra to validate our implementation
        // These values were generated by Cassandra 5.0 Murmur3Partitioner

        // Test case 1: int value 1
        let key1 = vec![0x00, 0x00, 0x00, 0x01];
        let token1 = calculate_murmur3_token(&key1).unwrap();
        // Cassandra produces deterministic tokens for the same input
        // The exact value depends on Murmur3 algorithm implementation
        assert_ne!(token1, 0, "Token should not be zero for non-zero input");

        // Test case 2: int value 100
        let key2 = vec![0x00, 0x00, 0x00, 0x64];
        let token2 = calculate_murmur3_token(&key2).unwrap();
        assert_ne!(
            token2, token1,
            "Different keys should produce different tokens"
        );

        // Test case 3: text value "test"
        let key3 = b"test";
        let token3 = calculate_murmur3_token(key3).unwrap();
        assert_ne!(token3, token1);
        assert_ne!(token3, token2);

        // Test consistency: same key should always produce same token
        let token1_repeat = calculate_murmur3_token(&key1).unwrap();
        assert_eq!(token1, token1_repeat, "Tokens must be deterministic");
    }

    #[test]
    fn test_decorated_key_btree_ordering() {
        // Verify that DecoratedKey ordering is correct for use in BTreeMap
        use std::collections::BTreeMap;

        let mut map = BTreeMap::new();

        // Insert keys in non-sorted order
        let dk3 = DecoratedKey::new(300, vec![3]);
        let dk1 = DecoratedKey::new(100, vec![1]);
        let dk2 = DecoratedKey::new(200, vec![2]);

        map.insert(dk3.clone(), "value3");
        map.insert(dk1.clone(), "value1");
        map.insert(dk2.clone(), "value2");

        // Verify BTreeMap orders by token
        let keys: Vec<_> = map.keys().collect();
        assert_eq!(keys[0].token, 100);
        assert_eq!(keys[1].token, 200);
        assert_eq!(keys[2].token, 300);
    }

    #[test]
    fn test_decorated_key_hash_collision_handling() {
        // Test Issue #406: Explicit hash collision scenario
        // When two different keys produce the same token (extremely rare but possible),
        // they should be ordered by raw key bytes to ensure deterministic ordering.

        let token = 12345_i64; // Shared token value (simulated collision)

        let dk1 = DecoratedKey::new(token, vec![0x00, 0x01, 0x02]); // Key A
        let dk2 = DecoratedKey::new(token, vec![0x00, 0x01, 0x03]); // Key B (differs in last byte)
        let dk3 = DecoratedKey::new(token, vec![0x00, 0x01, 0x02]); // Key C (identical to A)

        // Equal tokens: order by key bytes
        assert!(dk1 < dk2, "Keys with same token should order by bytes");
        assert!(dk2 > dk1, "Key comparison should be consistent");
        assert_eq!(
            dk1.cmp(&dk3),
            Ordering::Equal,
            "Identical keys should be equal"
        );

        // Verify ordering is stable in BTreeMap
        use std::collections::BTreeMap;
        let mut map = BTreeMap::new();

        map.insert(dk2.clone(), "value2");
        map.insert(dk1.clone(), "value1");
        map.insert(dk3.clone(), "value3"); // Overwrites dk1 (same key)

        // Should have 2 entries (dk1/dk3 are same key)
        assert_eq!(map.len(), 2);

        // Verify ordering by raw bytes
        let keys: Vec<_> = map.keys().collect();
        assert_eq!(keys[0].key, vec![0x00, 0x01, 0x02]); // dk1/dk3
        assert_eq!(keys[1].key, vec![0x00, 0x01, 0x03]); // dk2
    }

    #[test]
    fn test_clustering_key_ord_valid_comparison() {
        // Test Issue #409: Valid comparisons work correctly
        let ck1 = ClusteringKey::single("ts", Value::Timestamp(1000));
        let ck2 = ClusteringKey::single("ts", Value::Timestamp(2000));
        let ck3 = ClusteringKey::single("ts", Value::Timestamp(1000));

        // Basic ordering
        assert_eq!(ck1.cmp(&ck2), Ordering::Less);
        assert_eq!(ck2.cmp(&ck1), Ordering::Greater);
        assert_eq!(ck1.cmp(&ck3), Ordering::Equal);

        // Multi-column clustering key
        let ck_multi1 = ClusteringKey::new(vec![
            ("year".to_string(), Value::Integer(2024)),
            ("month".to_string(), Value::SmallInt(1)),
        ]);
        let ck_multi2 = ClusteringKey::new(vec![
            ("year".to_string(), Value::Integer(2024)),
            ("month".to_string(), Value::SmallInt(2)),
        ]);

        assert_eq!(ck_multi1.cmp(&ck_multi2), Ordering::Less);
    }

    #[test]
    fn test_clustering_key_ord_type_mismatch_is_total_and_does_not_panic() {
        // Issue #458/#465: `Ord for ClusteringKey` MUST NOT panic on a type mismatch.
        // Heterogeneous SSTables can produce mismatched clustering value types, and a
        // panic in `cmp` would crash the memtable BTreeMap. Instead, the implementation
        // falls back to a deterministic ordering. This test confirms it is panic-free,
        // total (antisymmetric), and deterministic.
        let ck1 = ClusteringKey::single("ts", Value::Timestamp(1000));
        let ck2 = ClusteringKey::single("ts", Value::Integer(2000)); // Different type

        // Must not panic.
        let ord_12 = ck1.cmp(&ck2);
        let ord_21 = ck2.cmp(&ck1);

        // Determinism: repeated comparisons return the same result.
        assert_eq!(ord_12, ck1.cmp(&ck2), "comparison must be deterministic");

        // Antisymmetry / totality: a<b implies b>a (and the mismatch is never reported
        // as Equal, since the underlying values differ).
        assert_ne!(
            ord_12,
            Ordering::Equal,
            "mismatched types must not compare Equal"
        );
        assert_eq!(
            ord_12.reverse(),
            ord_21,
            "ordering must be antisymmetric (a.cmp(b) == b.cmp(a).reverse())"
        );

        // Reflexivity: a key compares Equal to itself even across the fallback path.
        assert_eq!(ck1.cmp(&ck1), Ordering::Equal);

        // It must remain usable as a BTreeMap key without panicking.
        use std::collections::BTreeMap;
        let mut map = BTreeMap::new();
        map.insert(ck1.clone(), "a");
        map.insert(ck2.clone(), "b");
        assert_eq!(map.len(), 2, "both distinct keys should be retained");
    }

    #[test]
    fn test_clustering_key_ord_btree_ordering() {
        // Test Issue #409: Verify ClusteringKey works correctly in BTreeMap
        use std::collections::BTreeMap;

        let mut map = BTreeMap::new();

        let ck3 = ClusteringKey::single("ts", Value::Timestamp(3000));
        let ck1 = ClusteringKey::single("ts", Value::Timestamp(1000));
        let ck2 = ClusteringKey::single("ts", Value::Timestamp(2000));

        // Insert in non-sorted order
        map.insert(ck3.clone(), "value3");
        map.insert(ck1.clone(), "value1");
        map.insert(ck2.clone(), "value2");

        // Verify BTreeMap orders correctly
        let values: Vec<_> = map.values().copied().collect();
        assert_eq!(values, vec!["value1", "value2", "value3"]);
    }

    #[test]
    fn test_compare_frozen_list_values() {
        // Issue #437: Frozen collection clustering keys must be comparable
        let list_a = Value::Frozen(Box::new(Value::List(vec![
            Value::text("a".to_string()),
            Value::text("b".to_string()),
        ])));
        let list_b = Value::Frozen(Box::new(Value::List(vec![
            Value::text("a".to_string()),
            Value::text("c".to_string()),
        ])));
        let list_c = Value::Frozen(Box::new(Value::List(vec![
            Value::text("a".to_string()),
            Value::text("b".to_string()),
            Value::text("c".to_string()),
        ])));

        // Same elements: equal
        assert_eq!(compare_values(&list_a, &list_a).unwrap(), Ordering::Equal);
        // Different second element: a < c
        assert_eq!(compare_values(&list_a, &list_b).unwrap(), Ordering::Less);
        assert_eq!(compare_values(&list_b, &list_a).unwrap(), Ordering::Greater);
        // Prefix match, shorter < longer
        assert_eq!(compare_values(&list_a, &list_c).unwrap(), Ordering::Less);
        assert_eq!(compare_values(&list_c, &list_a).unwrap(), Ordering::Greater);
    }

    #[test]
    fn test_frozen_list_clustering_key_btree_ordering() {
        // Issue #437: Frozen list clustering keys must sort correctly in BTreeMap
        use std::collections::BTreeMap;

        let mut map = BTreeMap::new();

        // Create clustering keys with frozen lists of varying sizes (mimics test data generator)
        let ck_2elem = ClusteringKey::single(
            "tags",
            Value::Frozen(Box::new(Value::List(vec![
                Value::text("ck_0_0".to_string()),
                Value::text("ck_0_1".to_string()),
            ]))),
        );
        let ck_3elem = ClusteringKey::single(
            "tags",
            Value::Frozen(Box::new(Value::List(vec![
                Value::text("ck_1_0".to_string()),
                Value::text("ck_1_1".to_string()),
                Value::text("ck_1_2".to_string()),
            ]))),
        );
        let ck_4elem = ClusteringKey::single(
            "tags",
            Value::Frozen(Box::new(Value::List(vec![
                Value::text("ck_2_0".to_string()),
                Value::text("ck_2_1".to_string()),
                Value::text("ck_2_2".to_string()),
                Value::text("ck_2_3".to_string()),
            ]))),
        );

        // Insert in non-sorted order
        map.insert(ck_4elem.clone(), "4elem");
        map.insert(ck_2elem.clone(), "2elem");
        map.insert(ck_3elem.clone(), "3elem");

        // All three should be distinct keys (no deduplication)
        assert_eq!(map.len(), 3, "All frozen list CKs should be distinct");

        // Verify ordering: ck_0_* < ck_1_* < ck_2_* (lexicographic by first element)
        let values: Vec<_> = map.values().copied().collect();
        assert_eq!(values, vec!["2elem", "3elem", "4elem"]);
    }

    #[test]
    fn test_partition_key_from_bytes_single_int() {
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let original = PartitionKey::single("id", Value::Integer(42));
        let bytes = original.to_bytes(&schema).unwrap();
        let decoded = PartitionKey::from_bytes(&bytes, &schema).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_partition_key_from_bytes_single_uuid() {
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let uuid_bytes = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let original = PartitionKey::single("id", Value::Uuid(uuid_bytes));
        let bytes = original.to_bytes(&schema).unwrap();
        let decoded = PartitionKey::from_bytes(&bytes, &schema).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_partition_key_from_bytes_single_text() {
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "name".to_string(),
                data_type: "text".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let original = PartitionKey::single("name", Value::text("hello".to_string()));
        let bytes = original.to_bytes(&schema).unwrap();
        let decoded = PartitionKey::from_bytes(&bytes, &schema).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_partition_key_from_bytes_multi_component() {
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_keys: vec![
                KeyColumn {
                    name: "tenant".to_string(),
                    data_type: "text".to_string(),
                    position: 0,
                },
                KeyColumn {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    position: 1,
                },
            ],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        let original = PartitionKey::new(vec![
            ("tenant".to_string(), Value::text("acme".to_string())),
            ("id".to_string(), Value::Integer(99)),
        ]);
        let bytes = original.to_bytes(&schema).unwrap();
        let decoded = PartitionKey::from_bytes(&bytes, &schema).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_partition_key_from_bytes_empty_errors() {
        let schema = TableSchema {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        };

        assert!(PartitionKey::from_bytes(&[], &schema).is_err());
    }

    #[test]
    fn test_clustering_key_cmp_type_mismatch_does_not_panic() {
        // Two ClusteringKeys whose sole column has mismatched value types.
        // This can happen with heterogeneous SSTables (e.g. Frozen(List) vs List).
        // The Ord impl must not panic; it must produce a deterministic result.
        let key_a = ClusteringKey {
            columns: vec![("col".to_string(), Value::Integer(1))],
        };
        let key_b = ClusteringKey {
            columns: vec![("col".to_string(), Value::text("1".to_string()))],
        };

        // Must not panic.
        let first = key_a.cmp(&key_b);
        // Must be deterministic: same result on repeated calls.
        let second = key_a.cmp(&key_b);
        assert_eq!(first, second);

        // Reflexive: a key compared against itself is Equal.
        assert_eq!(key_a.cmp(&key_a), Ordering::Equal);
    }
}
