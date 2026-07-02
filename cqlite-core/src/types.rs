//! Core data types for CQLite

pub mod comparator;

#[cfg(test)]
mod comparator_test;

pub use comparator::ComparatorType;

use crate::schema::CqlType;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

// Size constants for fixed-size types
const BOOL_SIZE: usize = 1;
const TINYINT_SIZE: usize = 1;
const SMALLINT_SIZE: usize = 2;
const INT_SIZE: usize = 4;
const BIGINT_SIZE: usize = 8;
const FLOAT32_SIZE: usize = 4;
const FLOAT64_SIZE: usize = 8;
const UUID_SIZE: usize = 16;
const DURATION_SIZE: usize = 12; // 3 * 4 bytes (months, days, nanos)
const TOMBSTONE_SIZE: usize = 16;
const VINT_LENGTH_PREFIX: usize = 4;

/// Database value type that can hold any supported data type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// Null value
    Null,
    /// Boolean value
    Boolean(bool),
    /// 32-bit signed integer
    Integer(i32),
    /// 64-bit signed integer
    BigInt(i64),
    /// Cassandra Counter type (distributed increment-only counter with CRDT semantics)
    Counter(i64),
    /// 64-bit floating point number
    Float(f64),
    /// UTF-8 string
    Text(String),
    /// Binary data
    Blob(Vec<u8>),
    /// Timestamp (milliseconds since Unix epoch)
    Timestamp(i64),
    /// Date (days since Unix epoch: 1970-01-01)
    Date(i32),
    /// Time (nanoseconds since midnight)
    Time(i64),
    /// UUID as 16 bytes
    Uuid([u8; 16]),
    /// Variable-length integer
    Varint(Vec<u8>),
    /// Decimal value with scale and unscaled value
    Decimal { scale: i32, unscaled: Vec<u8> },
    /// Duration value with months, days, and nanoseconds
    Duration { months: i32, days: i32, nanos: i64 },
    /// JSON value
    Json(serde_json::Value),
    /// 8-bit signed integer (for exact Cassandra compatibility)
    TinyInt(i8),
    /// 16-bit signed integer (for exact Cassandra compatibility)
    SmallInt(i16),
    /// 32-bit floating point (for exact Cassandra compatibility)
    Float32(f32),
    /// List of values
    List(Vec<Value>),
    /// Set of values (implemented as Vec for ordering preservation)
    Set(Vec<Value>),
    /// Map of key-value pairs (Vec of tuples for exact Cassandra format)
    Map(Vec<(Value, Value)>),
    /// Tuple with fixed-size heterogeneous types
    Tuple(Vec<Value>),
    /// User defined type with structured fields
    Udt(UdtValue),
    /// Frozen wrapper for collections (immutable)
    Frozen(Box<Value>),
    /// Tombstone marker indicating deleted data
    Tombstone(TombstoneInfo),
    /// IP address (4 bytes for IPv4, 16 bytes for IPv6)
    Inet(Vec<u8>),
}

/// Ordered interned cells of a single decoded row (issue #1334).
///
/// Each entry is `(column_name, value)` where the name is a shared `Arc<str>`
/// handle interned once by the row decoder. Carrying a cell's name into
/// `QueryRow.values` is therefore a reference-count bump — NOT a per-cell heap
/// `String` allocation. Entries are ordered exactly as the producer emits them
/// (emit-time alphabetical by column name).
pub type RowCells = Vec<(Arc<str>, Value)>;

/// Payload of a single scanned row as it crosses the storage → query boundary
/// (issue #1334).
///
/// This is a **dedicated internal carrier — deliberately NOT a variant of the
/// public [`Value`] enum**. Keeping it off `Value` keeps the public value type
/// closed (no breaking variant, no defensive match arms) and lets the interned
/// [`RowCells`] names reach `QueryRow.values` without ever round-tripping
/// through a `String`.
///
/// There is exactly ONE row-carrier path: every scan/compaction producer builds
/// a `ScanRow` and every consumer disassembles a `ScanRow`, so a live row's
/// column values can never silently fall through to a non-row fallback.
#[derive(Debug, Clone, PartialEq)]
pub enum ScanRow {
    /// A live row: its interned cells, ordered exactly as emitted (emit-time
    /// alphabetical by column name). These cells are ALREADY DECODED — a
    /// consumer surfaces them as-is (never re-decoding a cell value).
    Row(RowCells),
    /// A RAW, UNDECODED whole-row value carried with explicit provenance
    /// (issue #1334). Emitted only by the fallback producers — the `data_access`
    /// offset-read placeholder and the legacy `parse_block_entries*` fallback —
    /// which pre-#1334 returned a bare `Value::Blob` of a row's raw value bytes.
    ///
    /// This is DELIBERATELY distinct from [`ScanRow::Row`]: it tells a consumer,
    /// by provenance (not by inspecting cell name/shape), that the bytes still
    /// need schema-decoding. A schema-aware consumer schema-decodes these bytes
    /// into the table's columns; a no-schema consumer surfaces them as the exact
    /// pre-#1334 shape (a single `"data"` [`Value::Blob`] column). An already
    /// decoded row — even one whose only non-key column is a blob column named
    /// `"data"` — is a [`ScanRow::Row`] and is NEVER treated as raw.
    RawRow(Vec<u8>),
    /// A non-row scan marker carried on the same channel — a row tombstone
    /// (`Value::Tombstone`), an absent/null row (`Value::Null`), or a synthetic
    /// compaction carrier value. Suppressed from user-visible output but
    /// preserved so the compaction merge can reconcile deletions.
    Marker(Value),
}

impl ScanRow {
    /// Cells surfaced to a no-schema consumer: a live row's interned cells, the
    /// single synthetic `"data"` blob for a raw fallback row, or `None` for a
    /// marker (tombstone/null).
    ///
    /// A [`ScanRow::RawRow`] surfaces as one `("data", Value::Blob(bytes))` cell —
    /// the exact pre-#1334 bare-`Value::Blob` shape a no-schema consumer produced.
    pub fn into_cells(self) -> Option<RowCells> {
        match self {
            ScanRow::Row(cells) => Some(cells),
            ScanRow::RawRow(bytes) => {
                Some(vec![(std::sync::Arc::from("data"), Value::Blob(bytes))])
            }
            ScanRow::Marker(_) => None,
        }
    }

    /// True when this is a suppressed marker (row tombstone or absent/null row).
    pub fn is_marker(&self) -> bool {
        matches!(self, ScanRow::Marker(_))
    }

    /// Number of cells in a live row, or the byte length of a raw/marker value.
    ///
    /// Inspection convenience: a live [`ScanRow::Row`] reports its cell count; a
    /// [`ScanRow::RawRow`] reports its undecoded byte length; a
    /// [`ScanRow::Marker`] delegates to the wrapped value's [`Value::len`].
    pub fn len(&self) -> usize {
        match self {
            ScanRow::Row(cells) => cells.len(),
            ScanRow::RawRow(bytes) => bytes.len(),
            ScanRow::Marker(v) => v.len(),
        }
    }

    /// True when a live row has no cells, a raw row has no bytes, or the marker
    /// value is empty/null.
    pub fn is_empty(&self) -> bool {
        match self {
            ScanRow::Row(cells) => cells.is_empty(),
            ScanRow::RawRow(bytes) => bytes.is_empty(),
            ScanRow::Marker(v) => v.is_empty(),
        }
    }

    /// Byte view of a raw fallback row's undecoded bytes, or a marker's wrapped
    /// value bytes; `None` for a decoded live row.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            ScanRow::RawRow(bytes) => Some(bytes),
            ScanRow::Marker(v) => v.as_bytes(),
            ScanRow::Row(_) => None,
        }
    }

    /// Classifier that maps a single ALREADY-DECODED named cell onto the
    /// scan → query row carrier (issue #1334). Used by the block-emit producers
    /// for genuine static/clustering cells so a live value can never be
    /// mis-wrapped as a suppressed marker.
    ///
    /// A genuinely absent cell ([`Value::Null`]) or a tombstone
    /// ([`Value::Tombstone`]) stays a suppressible [`ScanRow::Marker`] —
    /// `build_row_from_scan`/`into_cells()` drop those from user-visible output.
    /// ANY other (decoded, live) value becomes a live single-cell
    /// [`ScanRow::Row`] under `column_name` (interned `Arc<str>`) so it surfaces
    /// in SELECT/export/schema-discovery. This is for DECODED cells only; a raw
    /// undecoded whole-row value uses [`ScanRow::RawRow`] instead.
    pub fn classify_cell(column_name: &str, value: Value) -> ScanRow {
        match value {
            Value::Null | Value::Tombstone(_) => ScanRow::Marker(value),
            _ => ScanRow::Row(vec![(std::sync::Arc::from(column_name), value)]),
        }
    }
}

/// User Defined Type value with structured field access
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UdtValue {
    /// UDT type name
    pub type_name: String,
    /// Keyspace where the UDT is defined
    pub keyspace: String,
    /// Ordered list of fields (matches schema definition order)
    pub fields: Vec<UdtField>,
}

/// UDT field with name and optional value
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UdtField {
    /// Field name
    pub name: String,
    /// Field value (None represents null)
    pub value: Option<Value>,
}

/// UDT type definition for schema management
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UdtTypeDef {
    /// Keyspace name
    pub keyspace: String,
    /// UDT type name
    pub name: String,
    /// Field definitions in schema order
    pub fields: Vec<UdtFieldDef>,
}

/// UDT field definition in schema
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UdtFieldDef {
    /// Field name
    pub name: String,
    /// Field data type
    pub field_type: CqlType,
    /// Whether the field can be null (default: true)
    #[serde(default = "default_nullable")]
    pub nullable: bool,
}

/// Tuple value with positional fields
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TupleValue {
    /// Positional fields (None represents null)
    pub fields: Vec<Option<Value>>,
}

fn default_nullable() -> bool {
    true
}

/// Tombstone information for tracking deletions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TombstoneInfo {
    /// Deletion timestamp in microseconds since Unix epoch
    pub deletion_time: i64,
    /// Type of tombstone
    pub tombstone_type: TombstoneType,
    /// Local deletion time in **seconds** since the Unix epoch (the on-disk
    /// `localDeletionTime`, GC clock), as opposed to `deletion_time` which is the
    /// reconciliation `markedForDeleteAt` in microseconds.
    ///
    /// Carried so the compaction merge→rewrite path can preserve a tombstone's
    /// source LDT instead of re-deriving it from the deletion timestamp (#873),
    /// which keeps gc_grace semantics faithful and avoids underflowing the
    /// unsigned row-deletion LDT delta in the writer. `0` when unknown.
    ///
    /// `#[serde(default)]` keeps backward compatibility with serialized values
    /// written before this field existed.
    #[serde(default)]
    pub local_deletion_time: i64,
    /// TTL if applicable (for TTL-based expiration)
    pub ttl: Option<i64>,
    /// Range start key for range tombstones
    pub range_start: Option<RowKey>,
    /// Range end key for range tombstones
    pub range_end: Option<RowKey>,
}

/// Per-cell write metadata surfaced when `WRITETIME(col)` or `TTL(col)` is in the SELECT.
///
/// Moved here from `crate::query::result` so the storage layer can populate it
/// without creating a cyclic dependency (storage → query).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CellWriteMetadata {
    /// Write timestamp of the cell in **microseconds since Unix epoch**.
    ///
    /// For cells that inherit the row-level liveness timestamp
    /// (`USE_ROW_TIMESTAMP` flag) this is the row timestamp.
    /// Matches `WRITETIME(col)` semantics exactly.
    pub write_timestamp_micros: i64,

    /// Expiration info when the cell was written with a TTL.
    ///
    /// `None` when the cell has no TTL (it does not expire).
    pub expiration: Option<CellExpiration>,
}

/// TTL / expiration info for a cell that was written with a TTL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CellExpiration {
    /// TTL in **seconds** as written by the client.
    pub ttl_seconds: i32,

    /// Epoch-seconds at which the cell expires (local deletion time).
    ///
    /// When `now_seconds > expires_at`, the cell is expired.
    pub expires_at_seconds: i64,
}

/// Types of tombstones in Cassandra
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TombstoneType {
    /// Row-level deletion (entire row is deleted)
    RowTombstone,
    /// Cell-level deletion (specific column is deleted)
    CellTombstone,
    /// Range tombstone (a range of clustering rows is deleted)
    RangeTombstone,
    /// TTL expiration (data expired due to TTL)
    TtlExpiration,
    /// Partition-level deletion (`DELETE FROM t WHERE pk = ?`).
    ///
    /// Distinct from `RowTombstone` (which targets a single clustered row).
    /// A partition tombstone is stored in the partition header's
    /// `deletionInfo` field (`markedForDeleteAt` / `localDeletionTime`) and
    /// supersedes every row and cell within the partition whose writetime is
    /// older than the tombstone timestamp.
    ///
    /// In the delta-scan layer (`feature = "delta-scan"`) this maps directly
    /// to `DeltaRecord::PartitionDelete`.  In the normal read path the reader
    /// already parses the deletion-time bytes from the partition header (see
    /// `version_gate::has_partition_level_deletion_presence_marker`); this
    /// variant makes that information expressible in the shared type system.
    PartitionTombstone,
}

impl UdtValue {
    /// Create a new UDT value
    pub fn new(type_name: String, keyspace: String) -> Self {
        Self {
            type_name,
            keyspace,
            fields: Vec::new(),
        }
    }

    /// Add a field to the UDT
    pub fn with_field(mut self, name: String, value: Option<Value>) -> Self {
        self.fields.push(UdtField { name, value });
        self
    }

    /// Get a field value by name
    pub fn get_field(&self, name: &str) -> Option<&Value> {
        self.fields
            .iter()
            .find(|f| f.name == name)
            .and_then(|f| f.value.as_ref())
    }

    /// Set a field value
    pub fn set_field(&mut self, name: String, value: Option<Value>) {
        if let Some(field) = self.fields.iter_mut().find(|f| f.name == name) {
            field.value = value;
        } else {
            self.fields.push(UdtField { name, value });
        }
    }

    /// Get all field names
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }

    /// Get number of fields
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
}

impl UdtTypeDef {
    /// Create a new UDT type definition
    pub fn new(keyspace: String, name: String) -> Self {
        Self {
            keyspace,
            name,
            fields: Vec::new(),
        }
    }

    /// Add a field definition
    pub fn with_field(mut self, name: String, field_type: CqlType, nullable: bool) -> Self {
        self.fields.push(UdtFieldDef {
            name,
            field_type,
            nullable,
        });
        self
    }

    /// Get field definition by name
    pub fn get_field(&self, name: &str) -> Option<&UdtFieldDef> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Validate UDT value against this type definition
    pub fn validate_value(&self, value: &UdtValue) -> crate::Result<()> {
        // Check that type names match
        if value.type_name != self.name {
            return Err(crate::Error::schema(format!(
                "UDT type name mismatch: expected '{}', found '{}'",
                self.name, value.type_name
            )));
        }

        // Check that keyspace matches
        if value.keyspace != self.keyspace {
            return Err(crate::Error::schema(format!(
                "UDT keyspace mismatch: expected '{}', found '{}'",
                self.keyspace, value.keyspace
            )));
        }

        // Validate each field
        for field_def in &self.fields {
            if let Some(field_value) = value.get_field(&field_def.name) {
                // Field is present, check type compatibility
                if !Self::is_compatible_type(&field_value.data_type(), &field_def.field_type) {
                    return Err(crate::Error::schema(format!(
                        "Field '{}' type mismatch: expected {:?}, found {:?}",
                        field_def.name,
                        field_def.field_type,
                        field_value.data_type()
                    )));
                }
            } else if !field_def.nullable {
                // Field is missing but not nullable
                return Err(crate::Error::schema(format!(
                    "Non-nullable field '{}' is missing",
                    field_def.name
                )));
            }
        }

        Ok(())
    }

    fn is_compatible_type(value_type: &CqlType, expected_type: &CqlType) -> bool {
        // For now, require exact match - could be extended for type coercion
        value_type == expected_type
    }
}

impl TupleValue {
    /// Create a new tuple value
    pub fn new(fields: Vec<Option<Value>>) -> Self {
        Self { fields }
    }

    /// Get field by position
    pub fn get_field(&self, index: usize) -> Option<&Value> {
        self.fields.get(index).and_then(|f| f.as_ref())
    }

    /// Set field by position
    pub fn set_field(&mut self, index: usize, value: Option<Value>) {
        if index < self.fields.len() {
            self.fields[index] = value;
        }
    }

    /// Get field count
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
}

impl Value {
    /// Get the data type of this value
    pub fn data_type(&self) -> CqlType {
        match self {
            Value::Null => CqlType::Text, // Default type for null
            Value::Boolean(_) => CqlType::Boolean,
            Value::Integer(_) => CqlType::Int,
            Value::BigInt(_) => CqlType::BigInt,
            Value::Counter(_) => CqlType::Counter, // Counter type (will be added to CqlType)
            Value::Float(_) => CqlType::Double,
            Value::Text(_) => CqlType::Text,
            Value::Blob(_) => CqlType::Blob,
            Value::Timestamp(_) => CqlType::Timestamp,
            Value::Time(_) => CqlType::Time,
            Value::Date(_) => CqlType::Date,
            Value::Uuid(_) => CqlType::Uuid,
            Value::Json(_) => CqlType::Text, // JSON stored as text
            Value::TinyInt(_) => CqlType::TinyInt,
            Value::SmallInt(_) => CqlType::SmallInt,
            Value::Float32(_) => CqlType::Float,
            Value::List(elements) => {
                let element_type = if elements.is_empty() {
                    CqlType::Text
                } else {
                    elements[0].data_type()
                };
                CqlType::List(Box::new(element_type))
            }
            Value::Set(elements) => {
                let element_type = if elements.is_empty() {
                    CqlType::Text
                } else {
                    elements[0].data_type()
                };
                CqlType::Set(Box::new(element_type))
            }
            Value::Map(pairs) => {
                let (key_type, value_type) = if pairs.is_empty() {
                    (CqlType::Text, CqlType::Text)
                } else {
                    (pairs[0].0.data_type(), pairs[0].1.data_type())
                };
                CqlType::Map(Box::new(key_type), Box::new(value_type))
            }
            Value::Tuple(fields) => {
                let field_types = fields.iter().map(|f| f.data_type()).collect();
                CqlType::Tuple(field_types)
            }
            Value::Udt(udt) => {
                let fields = udt
                    .fields
                    .iter()
                    .map(|f| {
                        let field_type = if let Some(ref value) = f.value {
                            value.data_type()
                        } else {
                            CqlType::Text // Default for null fields
                        };
                        (f.name.clone(), field_type)
                    })
                    .collect();
                CqlType::Udt(udt.type_name.clone(), fields)
            }
            Value::Frozen(inner) => CqlType::Frozen(Box::new(inner.data_type())),
            Value::Varint(_) => CqlType::Varint,
            Value::Decimal { .. } => CqlType::Decimal,
            Value::Duration { .. } => CqlType::Duration,
            Value::Tombstone(_) => CqlType::Text, // Tombstones don't have a specific type
            Value::Inet(_) => CqlType::Inet,
        }
    }

    /// Check if this value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Check if this value is a tombstone (deleted)
    pub fn is_tombstone(&self) -> bool {
        matches!(self, Value::Tombstone(_))
    }

    /// Check if this value is deleted (null or tombstone)
    pub fn is_deleted(&self) -> bool {
        self.is_null() || self.is_tombstone()
    }

    /// Check if this value has expired based on TTL
    pub fn is_expired(&self, current_time: i64) -> bool {
        match self {
            Value::Tombstone(info) => {
                // Check if TTL has expired
                if let Some(ttl) = info.ttl {
                    current_time > info.deletion_time + ttl
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    /// Get the deletion timestamp if this is a tombstone
    pub fn deletion_time(&self) -> Option<i64> {
        match self {
            Value::Tombstone(info) => Some(info.deletion_time),
            _ => None,
        }
    }

    /// Create a tombstone with the given type and timestamp
    fn create_tombstone(
        tombstone_type: TombstoneType,
        deletion_time: i64,
        ttl: Option<i64>,
        range_start: Option<RowKey>,
        range_end: Option<RowKey>,
    ) -> Self {
        Value::Tombstone(TombstoneInfo {
            deletion_time,
            tombstone_type,
            // No GC-clock LDT is supplied by this constructor; default to 0.
            local_deletion_time: 0,
            ttl,
            range_start,
            range_end,
        })
    }

    /// Create a row tombstone with the given timestamp
    pub fn row_tombstone(deletion_time: i64) -> Self {
        Self::create_tombstone(TombstoneType::RowTombstone, deletion_time, None, None, None)
    }

    /// Create a cell tombstone with the given timestamp
    pub fn cell_tombstone(deletion_time: i64) -> Self {
        Self::create_tombstone(
            TombstoneType::CellTombstone,
            deletion_time,
            None,
            None,
            None,
        )
    }

    /// Create a TTL expiration tombstone
    pub fn ttl_tombstone(deletion_time: i64, ttl: i64) -> Self {
        Self::create_tombstone(
            TombstoneType::TtlExpiration,
            deletion_time,
            Some(ttl),
            None,
            None,
        )
    }

    /// Create a range tombstone for clustering key ranges
    pub fn range_tombstone(deletion_time: i64, start_key: RowKey, end_key: RowKey) -> Self {
        Self::create_tombstone(
            TombstoneType::RangeTombstone,
            deletion_time,
            None,
            Some(start_key),
            Some(end_key),
        )
    }

    /// Create a range tombstone with TTL for clustering key ranges
    pub fn range_tombstone_with_ttl(
        deletion_time: i64,
        start_key: RowKey,
        end_key: RowKey,
        ttl: i64,
    ) -> Self {
        Self::create_tombstone(
            TombstoneType::RangeTombstone,
            deletion_time,
            Some(ttl),
            Some(start_key),
            Some(end_key),
        )
    }

    /// Get the tombstone type if this is a tombstone
    pub fn tombstone_type(&self) -> Option<TombstoneType> {
        match self {
            Value::Tombstone(info) => Some(info.tombstone_type),
            _ => None,
        }
    }

    /// Check if this tombstone covers a specific key (for range tombstones)
    pub fn tombstone_covers_key(&self, key: &RowKey) -> bool {
        match self {
            Value::Tombstone(info) if info.tombstone_type == TombstoneType::RangeTombstone => {
                match (&info.range_start, &info.range_end) {
                    (Some(start), Some(end)) => key >= start && key <= end,
                    (Some(start), None) => key >= start,
                    (None, Some(end)) => key <= end,
                    (None, None) => false,
                }
            }
            Value::Tombstone(_) => true, // Row and cell tombstones cover their specific key
            _ => false,
        }
    }

    /// Get TTL information from tombstone
    pub fn tombstone_ttl(&self) -> Option<i64> {
        match self {
            Value::Tombstone(info) => info.ttl,
            _ => None,
        }
    }

    /// Check if this is a specific type of tombstone
    pub fn is_tombstone_type(&self, tombstone_type: TombstoneType) -> bool {
        match self {
            Value::Tombstone(info) => info.tombstone_type == tombstone_type,
            _ => false,
        }
    }

    /// Get range information for range tombstones
    pub fn tombstone_range(&self) -> Option<(Option<&RowKey>, Option<&RowKey>)> {
        match self {
            Value::Tombstone(info) if info.tombstone_type == TombstoneType::RangeTombstone => {
                Some((info.range_start.as_ref(), info.range_end.as_ref()))
            }
            _ => None,
        }
    }

    /// Try to convert this value to a boolean
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to convert this value to an integer
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::TinyInt(i) => Some(*i as i32),
            Value::SmallInt(i) => Some(*i as i32),
            _ => None,
        }
    }

    /// Try to convert this value to a big integer
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::BigInt(i) => Some(*i),
            Value::Counter(i) => Some(*i),
            Value::Integer(i) => Some(*i as i64),
            Value::TinyInt(i) => Some(*i as i64),
            Value::SmallInt(i) => Some(*i as i64),
            _ => None,
        }
    }

    /// Try to convert this value to a date (days since epoch)
    pub fn as_date(&self) -> Option<i32> {
        match self {
            Value::Date(d) => Some(*d),
            _ => None,
        }
    }

    /// Try to convert this value to a time (nanoseconds since midnight)
    pub fn as_time(&self) -> Option<i64> {
        match self {
            Value::Time(t) => Some(*t),
            _ => None,
        }
    }

    /// Try to convert this value to a float
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Float32(f) => Some(*f as f64),
            Value::Integer(i) => Some(*i as f64),
            Value::BigInt(i) => Some(*i as f64),
            Value::Counter(i) => Some(*i as f64),
            Value::TinyInt(i) => Some(*i as f64),
            Value::SmallInt(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to convert this value to a string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Try to convert this value to bytes
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(b) => Some(b),
            Value::Text(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    /// Try to convert this value to IP address bytes
    pub fn as_inet_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Inet(bytes) => Some(bytes),
            _ => None,
        }
    }

    /// Get the length of the byte representation for this value
    /// Returns the length of the underlying bytes for Blob/Text, or 0 for other types
    pub fn len(&self) -> usize {
        match self {
            Value::Blob(b) => b.len(),
            Value::Text(s) => s.len(),
            _ => 0,
        }
    }

    /// Check if the value is empty (for Blob/Text) or is Null
    pub fn is_empty(&self) -> bool {
        match self {
            Value::Null => true,
            Value::Blob(b) => b.is_empty(),
            Value::Text(s) => s.is_empty(),
            _ => false,
        }
    }

    /// Get the size in bytes for this value when serialized
    pub fn size_estimate(&self) -> usize {
        match self {
            Value::Null => BOOL_SIZE,
            Value::Boolean(_) => BOOL_SIZE,
            Value::TinyInt(_) => TINYINT_SIZE,
            Value::SmallInt(_) => SMALLINT_SIZE,
            Value::Integer(_) => INT_SIZE,
            Value::BigInt(_) | Value::Counter(_) => BIGINT_SIZE,
            Value::Float32(_) => FLOAT32_SIZE,
            Value::Float(_) => FLOAT64_SIZE,
            Value::Timestamp(_) | Value::Time(_) => BIGINT_SIZE,
            Value::Date(_) => INT_SIZE,
            Value::Uuid(_) => UUID_SIZE,
            Value::Duration { .. } => DURATION_SIZE,
            Value::Tombstone(_) => TOMBSTONE_SIZE,

            // Variable-length types with prefix
            Value::Text(s) => VINT_LENGTH_PREFIX + s.len(),
            Value::Blob(b) => VINT_LENGTH_PREFIX + b.len(),
            Value::Inet(addr) => VINT_LENGTH_PREFIX + addr.len(),
            Value::Varint(data) => VINT_LENGTH_PREFIX + data.len(),
            Value::Decimal { unscaled, .. } => INT_SIZE + VINT_LENGTH_PREFIX + unscaled.len(),
            Value::Json(j) => VINT_LENGTH_PREFIX + j.to_string().len(),

            // Collections
            Value::List(items) | Value::Set(items) => {
                Self::collection_size(items.iter(), VINT_LENGTH_PREFIX + BOOL_SIZE)
            }
            Value::Map(pairs) => {
                let overhead = VINT_LENGTH_PREFIX + 2 * BOOL_SIZE; // count + key_type + value_type
                pairs.iter().fold(overhead, |acc, (k, v)| {
                    acc + k.size_estimate() + v.size_estimate()
                })
            }
            Value::Tuple(items) => Self::collection_size(items.iter(), VINT_LENGTH_PREFIX),
            Value::Udt(udt_value) => {
                let mut size = VINT_LENGTH_PREFIX
                    + udt_value.type_name.len()
                    + VINT_LENGTH_PREFIX
                    + udt_value.keyspace.len()
                    + VINT_LENGTH_PREFIX; // field count
                for field in &udt_value.fields {
                    size += VINT_LENGTH_PREFIX + field.name.len();
                    size += match &field.value {
                        Some(val) => BOOL_SIZE + val.size_estimate(),
                        None => BOOL_SIZE,
                    };
                }
                size
            }
            Value::Frozen(inner) => inner.size_estimate(),
        }
    }

    /// Helper to calculate collection size
    fn collection_size<'a, I>(items: I, overhead: usize) -> usize
    where
        I: Iterator<Item = &'a Value>,
    {
        items.fold(overhead, |acc, item| acc + item.size_estimate())
    }

    /// Check if this value represents an empty collection
    pub fn is_empty_collection(&self) -> bool {
        match self {
            Value::List(list) => list.is_empty(),
            Value::Set(set) => set.is_empty(),
            Value::Map(map) => map.is_empty(),
            Value::Tuple(tuple) => tuple.is_empty(),
            _ => false,
        }
    }

    /// Get the element count for collections
    pub fn collection_len(&self) -> Option<usize> {
        match self {
            Value::List(list) => Some(list.len()),
            Value::Set(set) => Some(set.len()),
            Value::Map(map) => Some(map.len()),
            Value::Tuple(tuple) => Some(tuple.len()),
            _ => None,
        }
    }

    /// Check if this value can be used as a collection element
    pub fn is_valid_collection_element(&self) -> bool {
        match self {
            Value::Null => false, // Null elements typically not allowed in collections
            _ => true,
        }
    }

    /// Validate collection type consistency
    pub fn validate_collection_types(&self) -> crate::Result<()> {
        match self {
            Value::List(list) => {
                Self::validate_homogeneous_collection(list.iter(), "List")?;
                Ok(())
            }
            Value::Set(set) => {
                Self::validate_homogeneous_collection(set.iter(), "Set")?;
                Self::check_unique_items(set.iter(), "Set")?;
                Ok(())
            }
            Value::Map(map) => {
                if !map.is_empty() {
                    let (first_key, first_value) = &map[0];
                    let key_type = first_key.data_type();
                    let value_type = first_value.data_type();

                    for (key, value) in map.iter().skip(1) {
                        if key.data_type() != key_type {
                            return Err(crate::Error::schema(format!(
                                "Map contains mixed key types: {:?} and {:?}",
                                key_type,
                                key.data_type()
                            )));
                        }
                        if value.data_type() != value_type {
                            return Err(crate::Error::schema(format!(
                                "Map contains mixed value types: {:?} and {:?}",
                                value_type,
                                value.data_type()
                            )));
                        }
                    }

                    Self::check_unique_items(map.iter().map(|(k, _)| k), "Map keys")?;
                }
                Ok(())
            }
            _ => Ok(()), // Non-collections are always valid
        }
    }

    /// Helper to validate homogeneous collection types
    fn validate_homogeneous_collection<'a, I>(
        mut items: I,
        collection_name: &str,
    ) -> crate::Result<()>
    where
        I: Iterator<Item = &'a Value>,
    {
        if let Some(first) = items.next() {
            let first_type = first.data_type();
            for item in items {
                if item.data_type() != first_type {
                    return Err(crate::Error::schema(format!(
                        "{} contains mixed types: {:?} and {:?}",
                        collection_name,
                        first_type,
                        item.data_type()
                    )));
                }
            }
        }
        Ok(())
    }

    /// Helper to check for duplicate items
    fn check_unique_items<'a, I>(items: I, collection_name: &str) -> crate::Result<()>
    where
        I: Iterator<Item = &'a Value>,
    {
        let mut seen = std::collections::HashSet::new();
        for item in items {
            let item_str = format!("{}", item);
            if !seen.insert(item_str.clone()) {
                return Err(crate::Error::schema(format!(
                    "{} contains duplicate: {}",
                    collection_name, item_str
                )));
            }
        }
        Ok(())
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),

            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.partial_cmp(b),
            (Value::Counter(a), Value::Counter(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.partial_cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
            (Value::Time(a), Value::Time(b)) => a.partial_cmp(b),
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.partial_cmp(b),
            (Value::TinyInt(a), Value::TinyInt(b)) => a.partial_cmp(b),
            (Value::SmallInt(a), Value::SmallInt(b)) => a.partial_cmp(b),
            (Value::Float32(a), Value::Float32(b)) => a.partial_cmp(b),

            (Value::Inet(a), Value::Inet(b)) => a.partial_cmp(b),

            // For complex types, compare by string representation
            (a, b) => a.to_string().partial_cmp(&b.to_string()),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Integer(i) => write!(f, "{}", i),
            Value::BigInt(i) => write!(f, "{}", i),
            Value::TinyInt(i) => write!(f, "{}", i),
            Value::SmallInt(i) => write!(f, "{}", i),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Float32(fl) => write!(f, "{}", fl),
            Value::Counter(i) => write!(f, "counter:{}", i),
            Value::Text(s) => write!(f, "'{}'", s),
            Value::Blob(b) => write!(f, "BLOB({} bytes)", b.len()),
            Value::Timestamp(ts) => Self::fmt_typed(f, "TIMESTAMP", ts),
            Value::Date(days) => Self::fmt_typed(f, "DATE", days),
            Value::Time(nanos) => Self::fmt_time(f, *nanos),
            Value::Uuid(uuid) => Self::fmt_typed(f, "UUID", hex::encode(uuid)),
            Value::Json(json) => Self::fmt_typed(f, "JSON", json),
            Value::Inet(bytes) => Self::fmt_inet(f, bytes),
            Value::Varint(data) => write!(f, "VARINT(0x{})", hex::encode(data)),
            Value::Decimal { scale, unscaled } => {
                write!(f, "DECIMAL(scale={}, unscaled={:?})", scale, unscaled)
            }
            Value::Duration {
                months,
                days,
                nanos,
            } => {
                write!(f, "DURATION({}M {}D {}ns)", months, days, nanos)
            }
            Value::Set(items) => Self::fmt_collection(f, items.iter(), '{', '}'),
            Value::List(items) => Self::fmt_collection(f, items.iter(), '[', ']'),
            Value::Tuple(items) => Self::fmt_collection(f, items.iter(), '(', ')'),
            Value::Map(pairs) => Self::fmt_map(f, pairs),
            Value::Udt(udt) => Self::fmt_udt(f, udt),
            Value::Frozen(inner) => Self::fmt_typed(f, "FROZEN", &**inner),
            Value::Tombstone(info) => Self::fmt_tombstone(f, info),
        }
    }
}

impl Value {
    /// Format a typed value wrapper like TIMESTAMP(value)
    fn fmt_typed(
        f: &mut fmt::Formatter<'_>,
        type_name: &str,
        value: impl fmt::Display,
    ) -> fmt::Result {
        write!(f, "{}({})", type_name, value)
    }

    /// Format a time value as HH:MM:SS.nnnnnnnnn
    fn fmt_time(f: &mut fmt::Formatter<'_>, nanos: i64) -> fmt::Result {
        let total_seconds = nanos / 1_000_000_000;
        let hours = total_seconds / 3600;
        let minutes = (total_seconds % 3600) / 60;
        let seconds = total_seconds % 60;
        let remaining_nanos = nanos % 1_000_000_000;
        write!(
            f,
            "TIME({:02}:{:02}:{:02}.{:09})",
            hours, minutes, seconds, remaining_nanos
        )
    }

    /// Format an IP address (IPv4 or IPv6)
    fn fmt_inet(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
        match bytes.len() {
            4 => write!(f, "{}.{}.{}.{}", bytes[0], bytes[1], bytes[2], bytes[3]),
            16 => write!(
                f,
                "{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}:{:02x}{:02x}",
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
            ),
            _ => write!(f, "INET({} bytes)", bytes.len()),
        }
    }

    /// Format a tombstone with type and timestamp
    fn fmt_tombstone(f: &mut fmt::Formatter<'_>, info: &TombstoneInfo) -> fmt::Result {
        let type_name = match info.tombstone_type {
            TombstoneType::RowTombstone => "ROW",
            TombstoneType::CellTombstone => "CELL",
            TombstoneType::RangeTombstone => "RANGE",
            TombstoneType::PartitionTombstone => "PARTITION",
            TombstoneType::TtlExpiration => {
                return match info.ttl {
                    Some(ttl) => write!(f, "TOMBSTONE(TTL@{}+{})", info.deletion_time, ttl),
                    None => write!(f, "TOMBSTONE(TTL@{})", info.deletion_time),
                };
            }
        };
        write!(f, "TOMBSTONE({}@{})", type_name, info.deletion_time)
    }

    /// Format a collection with delimiters
    fn fmt_collection<'a, I>(
        f: &mut fmt::Formatter<'_>,
        items: I,
        open: char,
        close: char,
    ) -> fmt::Result
    where
        I: Iterator<Item = &'a Value>,
    {
        write!(f, "{}", open)?;
        for (i, item) in items.enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", item)?;
        }
        write!(f, "{}", close)
    }

    /// Format a map
    fn fmt_map(f: &mut fmt::Formatter<'_>, pairs: &[(Value, Value)]) -> fmt::Result {
        write!(f, "{{")?;
        for (i, (key, value)) in pairs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}: {}", key, value)?;
        }
        write!(f, "}}")
    }

    /// Format a UDT
    fn fmt_udt(f: &mut fmt::Formatter<'_>, udt: &UdtValue) -> fmt::Result {
        write!(f, "{}{{", udt.type_name)?;
        // Sort fields by name for deterministic output
        let mut sorted_fields: Vec<_> = udt.fields.iter().collect();
        sorted_fields.sort_by_key(|field| &field.name);

        for (i, field) in sorted_fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            match &field.value {
                Some(value) => write!(f, "'{}': {}", field.name, value)?,
                None => write!(f, "'{}': NULL", field.name)?,
            }
        }
        write!(f, "}}")
    }
}

/// Data type enumeration
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    /// Null type
    Null,
    /// Boolean type
    Boolean,
    /// 8-bit signed integer
    TinyInt,
    /// 16-bit signed integer
    SmallInt,
    /// 32-bit signed integer
    Integer,
    /// 64-bit signed integer
    BigInt,
    /// 32-bit floating point
    Float32,
    /// 64-bit floating point
    Float,
    /// Variable-length text
    Text,
    /// Variable-length binary data
    Blob,
    /// Timestamp with microsecond precision
    Timestamp,
    /// UUID type
    Uuid,
    /// JSON document
    Json,
    /// List of values
    List,
    /// Set of values
    Set,
    /// Map of key-value pairs
    Map,
    /// Tuple type with heterogeneous fields
    Tuple,
    /// User defined type
    Udt,
    /// Frozen type wrapper
    Frozen,
    /// Tombstone marker
    Tombstone,
}

impl DataType {
    /// Check if this type is numeric
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::TinyInt
                | DataType::SmallInt
                | DataType::Integer
                | DataType::BigInt
                | DataType::Float32
                | DataType::Float
        )
    }

    /// Check if this type is textual
    pub fn is_textual(&self) -> bool {
        matches!(self, DataType::Text)
    }

    /// Check if this type is binary
    pub fn is_binary(&self) -> bool {
        matches!(self, DataType::Blob)
    }

    /// Get the default value for this type
    pub fn default_value(&self) -> Value {
        match self {
            DataType::Null => Value::Null,
            DataType::Boolean => Value::Boolean(false),
            DataType::TinyInt => Value::TinyInt(0),
            DataType::SmallInt => Value::SmallInt(0),
            DataType::Integer => Value::Integer(0),
            DataType::BigInt => Value::BigInt(0),
            DataType::Float32 => Value::Float32(0.0),
            DataType::Float => Value::Float(0.0),
            DataType::Text => Value::Text(String::new()),
            DataType::Blob => Value::Blob(Vec::new()),
            DataType::Timestamp => Value::Timestamp(0),
            DataType::Uuid => Value::Uuid([0; 16]),
            DataType::Json => Value::Json(serde_json::Value::Null),
            DataType::List => Value::List(Vec::new()),
            DataType::Set => Value::Set(Vec::new()),
            DataType::Map => Value::Map(Vec::new()),
            DataType::Tuple => Value::Tuple(Vec::new()),
            DataType::Udt => Value::Udt(UdtValue {
                type_name: String::new(),
                keyspace: String::new(),
                fields: Vec::new(),
            }),
            DataType::Frozen => Value::Frozen(Box::new(Value::Null)),
            DataType::Tombstone => Value::Tombstone(TombstoneInfo {
                deletion_time: 0,
                tombstone_type: TombstoneType::RowTombstone,
                local_deletion_time: 0,
                ttl: None,
                range_start: None,
                range_end: None,
            }),
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            DataType::Null => "NULL",
            DataType::Boolean => "BOOLEAN",
            DataType::TinyInt => "TINYINT",
            DataType::SmallInt => "SMALLINT",
            DataType::Integer => "INTEGER",
            DataType::BigInt => "BIGINT",
            DataType::Float32 => "FLOAT32",
            DataType::Float => "FLOAT",
            DataType::Text => "TEXT",
            DataType::Blob => "BLOB",
            DataType::Timestamp => "TIMESTAMP",
            DataType::Uuid => "UUID",
            DataType::Json => "JSON",
            DataType::List => "LIST",
            DataType::Set => "SET",
            DataType::Map => "MAP",
            DataType::Tuple => "TUPLE",
            DataType::Udt => "UDT",
            DataType::Frozen => "FROZEN",
            DataType::Tombstone => "TOMBSTONE",
        };
        write!(f, "{}", name)
    }
}

/// Row key type - used for indexing and sorting
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RowKey(pub Vec<u8>);

impl RowKey {
    /// Create a new row key from bytes
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Create a row key from a value
    pub fn from_value(value: &Value) -> crate::Result<Self> {
        let bytes =
            bincode::serialize(value).map_err(|e| crate::Error::serialization(e.to_string()))?;
        Ok(Self(bytes))
    }

    /// Get the byte representation
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Get the length in bytes
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the key is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<u8>> for RowKey {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<&[u8]> for RowKey {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
}

impl From<String> for RowKey {
    fn from(s: String) -> Self {
        Self(s.into_bytes())
    }
}

impl From<&str> for RowKey {
    fn from(s: &str) -> Self {
        Self(s.as_bytes().to_vec())
    }
}

/// Table identifier
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableId(pub String);

impl TableId {
    /// Create a new table ID
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Get the table name
    pub fn name(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for TableId {
    fn from(name: String) -> Self {
        Self(name)
    }
}

impl From<&str> for TableId {
    fn from(name: &str) -> Self {
        Self(name.to_string())
    }
}

/// Column identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnId(pub String);

impl ColumnId {
    /// Create a new column ID
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Get the column name
    pub fn name(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ColumnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for ColumnId {
    fn from(name: String) -> Self {
        Self(name)
    }
}

impl From<&str> for ColumnId {
    fn from(name: &str) -> Self {
        Self(name.to_string())
    }
}

// Custom Eq implementation for Value to handle floating-point values
impl Eq for Value {}

// Custom Hash implementation for Value to handle floating-point values
impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Boolean(b) => b.hash(state),
            Value::Integer(i) => i.hash(state),
            Value::BigInt(i) => i.hash(state),
            Value::Counter(i) => i.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::Text(s) => s.hash(state),
            Value::Blob(b) => b.hash(state),
            Value::Timestamp(t) => t.hash(state),
            Value::Time(t) => t.hash(state),
            Value::Date(d) => d.hash(state),
            Value::Uuid(u) => u.hash(state),
            Value::Varint(v) => v.hash(state),
            Value::Decimal { scale, unscaled } => {
                scale.hash(state);
                unscaled.hash(state);
            }
            Value::Duration {
                months,
                days,
                nanos,
            } => {
                months.hash(state);
                days.hash(state);
                nanos.hash(state);
            }
            Value::Json(j) => j.to_string().hash(state),
            Value::TinyInt(i) => i.hash(state),
            Value::SmallInt(i) => i.hash(state),
            Value::Float32(f) => f.to_bits().hash(state),
            Value::List(l) => l.hash(state),
            Value::Set(s) => s.hash(state),
            Value::Map(m) => m.hash(state),
            Value::Tuple(t) => t.hash(state),
            Value::Udt(u) => u.hash(state),
            Value::Frozen(f) => f.hash(state),
            Value::Tombstone(t) => t.hash(state),
            Value::Inet(i) => i.hash(state),
        }
    }
}

// Also need to add Hash to these related types
impl std::hash::Hash for UdtValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.type_name.hash(state);
        self.keyspace.hash(state);
        self.fields.hash(state);
    }
}

impl std::hash::Hash for UdtField {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.value.hash(state);
    }
}

impl std::hash::Hash for TombstoneInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.deletion_time.hash(state);
        self.tombstone_type.hash(state);
        self.local_deletion_time.hash(state);
        self.ttl.hash(state);
        self.range_start.hash(state);
        self.range_end.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_types() {
        assert_eq!(Value::Integer(42).data_type(), CqlType::Int);
        assert_eq!(Value::Text("hello".to_string()).data_type(), CqlType::Text);
        assert_eq!(Value::Boolean(true).data_type(), CqlType::Boolean);
    }

    #[test]
    fn test_value_conversions() {
        let val = Value::Integer(42);
        assert_eq!(val.as_i32(), Some(42));
        assert_eq!(val.as_i64(), Some(42));
        assert_eq!(val.as_f64(), Some(42.0));
        assert_eq!(val.as_bool(), None);
    }

    #[test]
    fn test_row_key_creation() {
        let key1 = RowKey::from("test");
        let key2 = RowKey::from(b"test".to_vec());
        assert_eq!(key1.as_bytes(), key2.as_bytes());
    }

    #[test]
    fn test_value_display() {
        assert_eq!(Value::Null.to_string(), "NULL");
        assert_eq!(Value::Integer(42).to_string(), "42");
        assert_eq!(Value::Text("hello".to_string()).to_string(), "'hello'");
    }

    #[test]
    fn test_new_value_types() {
        // Test Tuple
        let tuple = Value::Tuple(vec![
            Value::Integer(42),
            Value::Text("hello".to_string()),
            Value::Boolean(true),
        ]);
        assert!(matches!(tuple.data_type(), CqlType::Tuple(_)));
        assert_eq!(tuple.to_string(), "(42, 'hello', true)");

        // Test UDT
        let udt = Value::Udt(UdtValue {
            type_name: "Person".to_string(),
            keyspace: "test".to_string(),
            fields: vec![
                UdtField {
                    name: "name".to_string(),
                    value: Some(Value::Text("John".to_string())),
                },
                UdtField {
                    name: "age".to_string(),
                    value: Some(Value::Integer(30)),
                },
            ],
        });
        assert!(matches!(udt.data_type(), CqlType::Udt(_, _)));
        assert!(udt.to_string().contains("Person{"));

        // Test Frozen
        let frozen_list = Value::Frozen(Box::new(Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ])));
        assert!(matches!(frozen_list.data_type(), CqlType::Frozen(..)));
        assert_eq!(frozen_list.to_string(), "FROZEN([1, 2, 3])");
    }

    #[test]
    fn test_new_data_types() {
        assert_eq!(DataType::Tuple.to_string(), "TUPLE");
        assert_eq!(DataType::Udt.to_string(), "UDT");
        assert_eq!(DataType::Frozen.to_string(), "FROZEN");
        assert_eq!(DataType::Tombstone.to_string(), "TOMBSTONE");

        // Test default values
        assert_eq!(DataType::Tuple.default_value(), Value::Tuple(Vec::new()));
        assert_eq!(
            DataType::Udt.default_value(),
            Value::Udt(UdtValue::new(String::new(), String::new()))
        );
        assert_eq!(
            DataType::Frozen.default_value(),
            Value::Frozen(Box::new(Value::Null))
        );
        assert!(matches!(
            DataType::Tombstone.default_value(),
            Value::Tombstone(_)
        ));
    }

    #[test]
    fn test_all_value_variants() {
        // Test all Value enum variants for coverage
        let values = vec![
            Value::Null,
            Value::Boolean(true),
            Value::Integer(42),
            Value::BigInt(9223372036854775807i64),
            Value::Counter(1000000i64),
            Value::Float(std::f64::consts::PI),
            Value::Text("test string".to_string()),
            Value::Blob(vec![1, 2, 3, 4]),
            Value::Timestamp(1234567890),
            Value::Uuid([0u8; 16]),
            Value::Varint(vec![1, 2, 3]),
            Value::Decimal {
                scale: 2,
                unscaled: vec![1, 2, 3],
            },
            Value::Duration {
                months: 1,
                days: 30,
                nanos: 1000000000,
            },
            Value::Json(serde_json::Value::String("test".to_string())),
            Value::TinyInt(127i8),
            Value::SmallInt(32767i16),
            Value::Float32(std::f32::consts::PI),
            Value::List(vec![Value::Integer(1), Value::Integer(2)]),
            Value::Set(vec![
                Value::Text("a".to_string()),
                Value::Text("b".to_string()),
            ]),
            Value::Map(vec![(Value::Text("key".to_string()), Value::Integer(42))]),
            Value::Tuple(vec![Value::Integer(1), Value::Text("test".to_string())]),
            Value::Udt(UdtValue::new("TestType".to_string(), "test_ks".to_string())),
            Value::Frozen(Box::new(Value::Integer(42))),
            Value::Tombstone(TombstoneInfo {
                deletion_time: 1000,
                tombstone_type: TombstoneType::RowTombstone,
                local_deletion_time: 0,
                ttl: None,
                range_start: None,
                range_end: None,
            }),
        ];

        // Test data_type() for all variants
        for value in &values {
            let _ = value.data_type();
        }

        // Test conversion methods
        assert_eq!(values[1].as_bool(), Some(true));
        assert_eq!(values[2].as_i32(), Some(42));
        assert_eq!(values[3].as_i64(), Some(9223372036854775807i64));
        assert_eq!(values[4].as_i64(), Some(1000000i64)); // Counter
        assert_eq!(values[5].as_f64(), Some(std::f64::consts::PI));
        assert_eq!(values[6].as_str(), Some("test string"));
        assert_eq!(values[7].as_bytes(), Some([1u8, 2, 3, 4].as_slice()));

        // Test is_null, is_tombstone, is_deleted
        assert!(values[0].is_null());
        assert!(values[23].is_tombstone());
        assert!(values[0].is_deleted());
        assert!(values[23].is_deleted());

        // Test size_estimate for all variants
        for value in &values {
            assert!(value.size_estimate() > 0);
        }
    }

    #[test]
    fn test_all_tombstone_types() {
        let _current_time = 2000;
        let start_key = RowKey::new(vec![1, 2, 3]);
        let end_key = RowKey::new(vec![4, 5, 6]);

        // Test all tombstone creation methods
        let row_tombstone = Value::row_tombstone(1000);
        let cell_tombstone = Value::cell_tombstone(1000);
        let ttl_tombstone = Value::ttl_tombstone(1000, 500);
        let range_tombstone = Value::range_tombstone(1000, start_key.clone(), end_key.clone());
        let range_tombstone_ttl =
            Value::range_tombstone_with_ttl(1000, start_key.clone(), end_key.clone(), 500);

        // Test tombstone type checking
        assert!(row_tombstone.is_tombstone_type(TombstoneType::RowTombstone));
        assert!(cell_tombstone.is_tombstone_type(TombstoneType::CellTombstone));
        assert!(ttl_tombstone.is_tombstone_type(TombstoneType::TtlExpiration));
        assert!(range_tombstone.is_tombstone_type(TombstoneType::RangeTombstone));
        assert!(range_tombstone_ttl.is_tombstone_type(TombstoneType::RangeTombstone));

        // Test deletion time retrieval
        assert_eq!(row_tombstone.deletion_time(), Some(1000));
        assert_eq!(cell_tombstone.deletion_time(), Some(1000));

        // Test TTL functionality
        assert_eq!(ttl_tombstone.tombstone_ttl(), Some(500));
        assert!(ttl_tombstone.is_expired(2000));
        assert!(!ttl_tombstone.is_expired(1200));

        // Test range tombstone functionality
        let test_key = RowKey::new(vec![3, 3, 3]);
        assert!(range_tombstone.tombstone_covers_key(&test_key));

        let range_info = range_tombstone.tombstone_range();
        assert!(range_info.is_some());
    }

    #[test]
    fn test_collection_validation() {
        // Test list validation
        let valid_list = Value::List(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]);
        assert!(valid_list.validate_collection_types().is_ok());
        assert_eq!(valid_list.collection_len(), Some(3));
        assert!(!valid_list.is_empty_collection());

        let mixed_list = Value::List(vec![Value::Integer(1), Value::Text("two".to_string())]);
        assert!(mixed_list.validate_collection_types().is_err());

        // Test set validation
        let valid_set = Value::Set(vec![
            Value::Text("a".to_string()),
            Value::Text("b".to_string()),
        ]);
        assert!(valid_set.validate_collection_types().is_ok());

        let duplicate_set = Value::Set(vec![
            Value::Text("a".to_string()),
            Value::Text("a".to_string()),
        ]);
        assert!(duplicate_set.validate_collection_types().is_err());

        // Test map validation
        let valid_map = Value::Map(vec![
            (Value::Text("key1".to_string()), Value::Integer(1)),
            (Value::Text("key2".to_string()), Value::Integer(2)),
        ]);
        assert!(valid_map.validate_collection_types().is_ok());

        let duplicate_key_map = Value::Map(vec![
            (Value::Text("key1".to_string()), Value::Integer(1)),
            (Value::Text("key1".to_string()), Value::Integer(2)),
        ]);
        assert!(duplicate_key_map.validate_collection_types().is_err());

        // Test empty collections
        let empty_list = Value::List(vec![]);
        assert!(empty_list.is_empty_collection());
        assert_eq!(empty_list.collection_len(), Some(0));

        // Test is_valid_collection_element
        assert!(!Value::Null.is_valid_collection_element());
        assert!(Value::Integer(42).is_valid_collection_element());
    }

    #[test]
    fn test_udt_functionality() {
        let mut udt = UdtValue::new("Person".to_string(), "test_ks".to_string());

        // Test field operations
        udt = udt.with_field("name".to_string(), Some(Value::Text("John".to_string())));
        udt = udt.with_field("age".to_string(), Some(Value::Integer(30)));

        assert_eq!(udt.field_count(), 2);
        assert_eq!(
            udt.get_field("name"),
            Some(&Value::Text("John".to_string()))
        );
        assert_eq!(udt.get_field("age"), Some(&Value::Integer(30)));
        assert_eq!(udt.get_field("nonexistent"), None);

        let field_names = udt.field_names();
        assert!(field_names.contains(&"name"));
        assert!(field_names.contains(&"age"));

        // Test field modification
        udt.set_field("age".to_string(), Some(Value::Integer(31)));
        assert_eq!(udt.get_field("age"), Some(&Value::Integer(31)));

        // Test new field addition via set_field
        udt.set_field(
            "email".to_string(),
            Some(Value::Text("john@example.com".to_string())),
        );
        assert_eq!(udt.field_count(), 3);
    }

    #[test]
    fn test_udt_type_def_functionality() {
        let type_def = UdtTypeDef::new("test_ks".to_string(), "Person".to_string())
            .with_field("name".to_string(), CqlType::Text, false)
            .with_field("age".to_string(), CqlType::Int, true);

        // Test field retrieval
        let name_field = type_def.get_field("name");
        assert!(name_field.is_some());
        assert_eq!(name_field.unwrap().field_type, CqlType::Text);
        assert!(!name_field.unwrap().nullable);

        // Test validation of matching UDT value
        let valid_udt = UdtValue::new("Person".to_string(), "test_ks".to_string())
            .with_field("name".to_string(), Some(Value::Text("John".to_string())))
            .with_field("age".to_string(), Some(Value::Integer(30)));

        assert!(type_def.validate_value(&valid_udt).is_ok());

        // Test validation failures
        let wrong_type_udt = UdtValue::new("Wrong".to_string(), "test_ks".to_string());
        assert!(type_def.validate_value(&wrong_type_udt).is_err());

        let wrong_keyspace_udt = UdtValue::new("Person".to_string(), "wrong_ks".to_string());
        assert!(type_def.validate_value(&wrong_keyspace_udt).is_err());
    }

    #[test]
    fn test_tuple_functionality() {
        let mut tuple = TupleValue::new(vec![
            Some(Value::Integer(1)),
            Some(Value::Text("test".to_string())),
            None,
        ]);

        assert_eq!(tuple.field_count(), 3);
        assert_eq!(tuple.get_field(0), Some(&Value::Integer(1)));
        assert_eq!(tuple.get_field(1), Some(&Value::Text("test".to_string())));
        assert_eq!(tuple.get_field(2), None);
        assert_eq!(tuple.get_field(3), None);

        // Test field modification
        tuple.set_field(2, Some(Value::Boolean(true)));
        assert_eq!(tuple.get_field(2), Some(&Value::Boolean(true)));

        // Test out-of-bounds modification (should be no-op)
        tuple.set_field(10, Some(Value::Integer(42)));
        assert_eq!(tuple.field_count(), 3);
    }

    #[test]
    fn test_data_type_functionality() {
        // Test is_numeric for all types
        assert!(DataType::TinyInt.is_numeric());
        assert!(DataType::SmallInt.is_numeric());
        assert!(DataType::Integer.is_numeric());
        assert!(DataType::BigInt.is_numeric());
        assert!(DataType::Float32.is_numeric());
        assert!(DataType::Float.is_numeric());
        assert!(!DataType::Text.is_numeric());
        assert!(!DataType::Boolean.is_numeric());

        // Test default values for all types
        assert_eq!(DataType::Null.default_value(), Value::Null);
        assert_eq!(DataType::Boolean.default_value(), Value::Boolean(false));
        assert_eq!(DataType::Integer.default_value(), Value::Integer(0));
        assert_eq!(DataType::Text.default_value(), Value::Text(String::new()));

        // Test string representation
        assert_eq!(DataType::Boolean.to_string(), "BOOLEAN");
        assert_eq!(DataType::Integer.to_string(), "INTEGER");
        assert_eq!(DataType::Text.to_string(), "TEXT");
        assert_eq!(DataType::Tuple.to_string(), "TUPLE");
        assert_eq!(DataType::Udt.to_string(), "UDT");
        assert_eq!(DataType::Frozen.to_string(), "FROZEN");
        assert_eq!(DataType::Tombstone.to_string(), "TOMBSTONE");
    }

    #[test]
    fn test_row_key_functionality() {
        // Test creation from bytes
        let key1 = RowKey::new(vec![1, 2, 3, 4]);
        assert_eq!(key1.len(), 4);
        assert!(!key1.is_empty());
        assert_eq!(key1.as_bytes(), &[1, 2, 3, 4]);

        // Test creation from value
        let value = Value::Text("test".to_string());
        let key2 = RowKey::from_value(&value).unwrap();
        assert!(!key2.is_empty());

        // Test empty key
        let empty_key = RowKey::new(vec![]);
        assert!(empty_key.is_empty());
        assert_eq!(empty_key.len(), 0);

        // Test From implementations
        let key3 = RowKey::from("test");
        let key4 = RowKey::from(b"test".to_vec());
        assert_eq!(key3.as_bytes(), key4.as_bytes());
    }

    #[test]
    fn test_value_comparison() {
        // Test null comparisons
        assert!(Value::Null < Value::Integer(42));
        assert!(Value::Integer(42) > Value::Null);
        assert_eq!(
            Value::Null.partial_cmp(&Value::Null),
            Some(std::cmp::Ordering::Equal)
        );

        // Test same-type comparisons
        assert!(Value::Integer(1) < Value::Integer(2));
        assert!(Value::Text("a".to_string()) < Value::Text("b".to_string()));
        assert!(Value::Boolean(false) < Value::Boolean(true));

        // Test mixed-type comparisons (fall back to string comparison)
        let int_val = Value::Integer(42);
        let text_val = Value::Text("hello".to_string());
        assert!(int_val.partial_cmp(&text_val).is_some());
    }

    #[test]
    fn test_tombstone_functionality() {
        // Test row tombstone creation
        let row_tombstone = Value::row_tombstone(1000);
        assert!(row_tombstone.is_tombstone());
        assert!(row_tombstone.is_deleted());
        assert_eq!(row_tombstone.deletion_time(), Some(1000));
        assert!(!row_tombstone.is_expired(500)); // before deletion
        assert!(!row_tombstone.is_expired(1500)); // TTL tombstones only expire

        // Test cell tombstone creation
        let cell_tombstone = Value::cell_tombstone(2000);
        assert!(cell_tombstone.is_tombstone());
        assert_eq!(cell_tombstone.deletion_time(), Some(2000));

        // Test TTL tombstone creation
        let ttl_tombstone = Value::ttl_tombstone(3000, 1000);
        assert!(ttl_tombstone.is_tombstone());
        assert_eq!(ttl_tombstone.deletion_time(), Some(3000));
        assert!(!ttl_tombstone.is_expired(3500)); // within TTL
        assert!(ttl_tombstone.is_expired(5000)); // past TTL

        // Test regular values
        let regular_value = Value::Integer(42);
        assert!(!regular_value.is_tombstone());
        assert!(!regular_value.is_deleted());
        assert_eq!(regular_value.deletion_time(), None);
        assert!(!regular_value.is_expired(1000));

        // Test null values
        let null_value = Value::Null;
        assert!(!null_value.is_tombstone());
        assert!(null_value.is_deleted()); // null is considered deleted
    }

    #[test]
    fn test_tombstone_display() {
        let row_tombstone = Value::row_tombstone(1000);
        assert_eq!(row_tombstone.to_string(), "TOMBSTONE(ROW@1000)");

        let cell_tombstone = Value::cell_tombstone(2000);
        assert_eq!(cell_tombstone.to_string(), "TOMBSTONE(CELL@2000)");

        let ttl_tombstone = Value::ttl_tombstone(3000, 1000);
        assert_eq!(ttl_tombstone.to_string(), "TOMBSTONE(TTL@3000+1000)");
    }

    // =========================================================================
    // Type Invariant Tests (Issue #267)
    // =========================================================================

    /// Test: Empty list data_type() returns List<Text> (documents current behavior)
    ///
    /// Issue #267: This test documents that empty lists default to Text element type
    /// because value-driven type inference cannot determine the actual type.
    #[test]
    fn test_data_type_empty_list_defaults_to_text() {
        use crate::schema::CqlType;

        let empty_list = Value::List(vec![]);
        let data_type = empty_list.data_type();

        // Documented behavior: empty lists return List<Text>
        assert_eq!(
            data_type,
            CqlType::List(Box::new(CqlType::Text)),
            "Empty list should return List<Text> (value-driven inference limitation)"
        );
    }

    /// Test: Null UDT field data_type() returns Text (documents current behavior)
    ///
    /// Issue #267: This test documents that null UDT fields default to Text type
    /// because value-driven type inference cannot determine the schema type.
    #[test]
    fn test_data_type_null_udt_field_defaults_to_text() {
        use crate::schema::CqlType;

        let udt = UdtValue {
            type_name: "test_type".to_string(),
            keyspace: "test_keyspace".to_string(),
            fields: vec![
                UdtField {
                    name: "present_field".to_string(),
                    value: Some(Value::Integer(42)),
                },
                UdtField {
                    name: "null_field".to_string(),
                    value: None, // Null value
                },
            ],
        };

        let data_type = Value::Udt(udt).data_type();

        match data_type {
            CqlType::Udt(name, fields) => {
                assert_eq!(name, "test_type");

                let null_field = fields.iter().find(|(n, _)| n == "null_field");
                assert!(null_field.is_some());

                // Documented behavior: null fields return Text
                assert_eq!(
                    null_field.unwrap().1,
                    CqlType::Text,
                    "Null UDT field should return Text (value-driven inference limitation)"
                );

                let present_field = fields.iter().find(|(n, _)| n == "present_field");
                assert_eq!(
                    present_field.unwrap().1,
                    CqlType::Int,
                    "Present field should preserve actual type"
                );
            }
            _ => panic!("Expected CqlType::Udt"),
        }
    }

    /// Test: Frozen data_type() preserves inner collection type
    ///
    /// Issue #267: Validates that Frozen wrapper correctly delegates to inner type.
    #[test]
    fn test_frozen_data_type_preserves_inner() {
        use crate::schema::CqlType;

        // Non-empty frozen list should preserve element type
        let list = Value::List(vec![Value::BigInt(100)]);
        let frozen = Value::Frozen(Box::new(list));
        let data_type = frozen.data_type();

        match data_type {
            CqlType::Frozen(inner) => match *inner {
                CqlType::List(element) => {
                    assert_eq!(
                        *element,
                        CqlType::BigInt,
                        "Should preserve BigInt element type"
                    );
                }
                other => panic!("Expected List, got {:?}", other),
            },
            other => panic!("Expected Frozen, got {:?}", other),
        }

        // Empty frozen list still defaults to Text (documented limitation)
        let empty_frozen = Value::Frozen(Box::new(Value::List(vec![])));
        match empty_frozen.data_type() {
            CqlType::Frozen(inner) => match *inner {
                CqlType::List(element) => {
                    assert_eq!(
                        *element,
                        CqlType::Text,
                        "Empty frozen list defaults to Text"
                    );
                }
                other => panic!("Expected List, got {:?}", other),
            },
            other => panic!("Expected Frozen, got {:?}", other),
        }
    }
}
