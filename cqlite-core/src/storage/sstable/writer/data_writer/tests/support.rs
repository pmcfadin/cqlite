//! Shared test fixtures for the `data_writer` tests (issue #1118 split).
//! Relocated verbatim from the original inline `mod tests`; only helper
//! visibility was widened to `pub(super)` so the per-scenario test modules
//! can reach them. No test logic changed.
#![allow(dead_code)]
#![allow(unused_imports)]

use super::super::*;
use crate::schema::{ClusteringColumn, ClusteringOrder, Column, CqlType, KeyColumn, TableSchema};
use crate::storage::serialization::types::TypeSerializer;
use crate::storage::write_engine::mutation::{CellOperation, ClusteringKey, PartitionKey, TableId};
use crate::types::UdtValue;
use std::collections::HashMap;

/// Whether a simple (non-complex) cell value has a fixed byte size or a variable
/// length encoded by a preceding unsigned VInt.
///
/// Mirrors [`cell_value_uses_length_prefix`]: Boolean, Integer, BigInt, Float32,
/// Float, Timestamp, and Uuid are fixed-size; everything else (Text, Blob, …) is
/// variable and prefixed with a VUInt length.
#[derive(Clone, Copy)]
pub(super) enum CellValueSizing {
    /// The value is exactly this many bytes (no length prefix).
    Fixed(usize),
    /// The value is prefixed by an unsigned VInt length.
    Variable,
}

/// One fully-decoded complex cell from a `write_complex_column_per_element`
/// output buffer, for byte-level assertions.
#[derive(Debug)]
pub(super) struct DecodedComplexCell {
    pub(super) flags: u8,
    /// Absolute timestamp delta from `min_timestamp` (only when an explicit
    /// timestamp was written, i.e. NOT USE_ROW_TIMESTAMP).
    pub(super) ts_delta: Option<u64>,
    /// LDT delta from `min_local_deletion_time` (only when deleted/expiring
    /// and not USE_ROW_TTL).
    pub(super) ldt_delta: Option<u64>,
    /// TTL delta from `min_ttl` (only when expiring and not USE_ROW_TTL).
    pub(super) ttl_delta: Option<u64>,
    pub(super) cell_path: Vec<u8>,
    pub(super) value: Option<Vec<u8>>,
}

pub(super) fn create_test_schema() -> TableSchema {
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
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "age".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

pub(super) fn create_test_stats() -> StatisticsMetadata {
    let mut stats = StatisticsMetadata::new();
    stats.min_timestamp = 1000000;
    stats.min_ttl = 0;
    stats.min_local_deletion_time = 0;
    stats
}

/// Schema with a clustering column: id (pk) / ck (clustering) / v (regular).
pub(super) fn clustering_test_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "v".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

pub(super) fn op_columns(row: &RowWrite<'_>) -> Vec<String> {
    row.ops
        .iter()
        .filter_map(|m| match m.op {
            CellOperation::Write { column, .. }
            | CellOperation::WriteWithTtl { column, .. }
            | CellOperation::Delete { column, .. }
            | CellOperation::WriteComplexElement { column, .. }
            | CellOperation::ComplexDeletion { column, .. } => Some(column.clone()),
            CellOperation::DeleteRow => None,
        })
        .collect()
}

pub(super) fn phase3_address_schema() -> UdtTypeDef {
    UdtTypeDef::new("test_ks".to_string(), "address".to_string())
        .with_field("street".to_string(), CqlType::Text, true)
        .with_field("city".to_string(), CqlType::Text, true)
}

pub(super) fn phase3_person_schema() -> UdtTypeDef {
    UdtTypeDef::new("test_ks".to_string(), "person".to_string())
        .with_field("name".to_string(), CqlType::Text, true)
        .with_field(
            "phone_numbers".to_string(),
            CqlType::List(Box::new(CqlType::Frozen(Box::new(CqlType::Udt(
                "phone_number".to_string(),
                vec![],
            ))))),
            true,
        )
        .with_field(
            "home_address".to_string(),
            CqlType::Frozen(Box::new(CqlType::Udt("address".to_string(), vec![]))),
            true,
        )
}

pub(super) fn phase3_company_schema() -> UdtTypeDef {
    UdtTypeDef::new("test_ks".to_string(), "company".to_string())
        .with_field("name".to_string(), CqlType::Text, true)
        .with_field(
            "employees".to_string(),
            CqlType::List(Box::new(CqlType::Frozen(Box::new(CqlType::Udt(
                "person".to_string(),
                vec![],
            ))))),
            true,
        )
        .with_field(
            "departments".to_string(),
            CqlType::Map(
                Box::new(CqlType::Text),
                Box::new(CqlType::Frozen(Box::new(CqlType::List(Box::new(
                    CqlType::Frozen(Box::new(CqlType::Udt("person".to_string(), vec![]))),
                ))))),
            ),
            true,
        )
}

pub(super) fn phase3_address_value() -> UdtValue {
    UdtValue::new("address".to_string(), "test_ks".to_string())
        .with_field(
            "street".to_string(),
            Some(Value::Text("Main St".to_string())),
        )
        .with_field("city".to_string(), Some(Value::Text("Seattle".to_string())))
}

pub(super) fn phase3_phone_value() -> UdtValue {
    UdtValue::new("phone_number".to_string(), "test_ks".to_string())
        .with_field("label".to_string(), Some(Value::Text("mobile".to_string())))
        .with_field(
            "number".to_string(),
            Some(Value::Text("+1-555-0101".to_string())),
        )
}

pub(super) fn phase3_person_value(name: &str) -> UdtValue {
    UdtValue::new("person".to_string(), "test_ks".to_string())
        .with_field("name".to_string(), Some(Value::Text(name.to_string())))
        .with_field(
            "phone_numbers".to_string(),
            Some(Value::List(vec![Value::Frozen(Box::new(Value::Udt(
                Box::new(phase3_phone_value()),
            )))])),
        )
        .with_field(
            "home_address".to_string(),
            Some(Value::Frozen(Box::new(Value::Udt(Box::new(
                phase3_address_value(),
            ))))),
        )
}

pub(super) fn phase3_company_value() -> UdtValue {
    let person = phase3_person_value("Alice");
    UdtValue::new("company".to_string(), "test_ks".to_string())
        .with_field("name".to_string(), Some(Value::Text("Acme".to_string())))
        .with_field(
            "employees".to_string(),
            Some(Value::List(vec![Value::Frozen(Box::new(Value::Udt(
                Box::new(person.clone()),
            )))])),
        )
        .with_field(
            "departments".to_string(),
            Some(Value::Map(vec![(
                Value::Text("platform".to_string()),
                Value::Frozen(Box::new(Value::List(vec![Value::Frozen(Box::new(
                    Value::Udt(Box::new(person)),
                ))]))),
            )])),
        )
}

pub(super) fn create_static_test_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "static_val".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
            },
            Column {
                name: "regular_val".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Parse the output of `write_row` / `writer.finish()` and return the flags byte
/// for each simple (non-complex) cell, in schema column order.
///
/// This walks the deterministic row-header structure so wall-clock-derived bytes
/// inside the TTL/LDT delta fields cannot be misidentified as cell-flag bytes:
///
/// ```text
/// [row_flags: u8]                        ← byte 0; no clustering prefix here
/// [row_body_size: unsigned VInt]
/// [prev_size: unsigned VInt]
/// [timestamp_delta: unsigned VInt]       ← present when ROW_HAS_TIMESTAMP
/// [ttl_delta: unsigned VInt]             ← present when ROW_HAS_TTL
/// [ldt_delta: unsigned VInt]             ← present when ROW_HAS_TTL (wall-clock!)
/// [column_bitmap: unsigned VInt]         ← present when NOT ROW_HAS_ALL_COLUMNS
/// per cell (one per `column_sizings` entry):
///   [flags: u8]                          ← captured here
///   if NOT CELL_USE_ROW_TIMESTAMP:
///     [timestamp_delta: unsigned VInt]
///   if CELL_IS_DELETED:
///     [ldt_delta: unsigned VInt]
///   if NOT CELL_HAS_EMPTY_VALUE:
///     match sizing:
///       Variable  → [value_len: unsigned VInt] + [value_len bytes]
///       Fixed(n)  → [n bytes]
/// ```
///
/// `column_sizings` must list one entry per regular column in schema order.
pub(super) fn parse_simple_row_cell_flags(
    buf: &[u8],
    column_sizings: &[CellValueSizing],
) -> Vec<u8> {
    fn read_uvint(buf: &[u8], pos: &mut usize) -> u64 {
        let first = buf[*pos];
        *pos += 1;
        if first == 0xFF {
            let mut v = 0u64;
            for _ in 0..8 {
                v = (v << 8) | buf[*pos] as u64;
                *pos += 1;
            }
            return v;
        }
        let extra = first.leading_ones() as usize;
        let mask = 0xFF_u8.wrapping_shr((extra + 1) as u32);
        let mut v = (first & mask) as u64;
        for _ in 0..extra {
            v = (v << 8) | buf[*pos] as u64;
            *pos += 1;
        }
        v
    }

    let mut pos = 0usize;

    // Row flags — byte 0, no clustering prefix for the test cases using this helper.
    let row_flags = buf[pos];
    pos += 1;

    // row_body_size + prev_size (two VInts we skip)
    read_uvint(buf, &mut pos); // row_body_size
    read_uvint(buf, &mut pos); // prev_size

    // Liveness timestamp delta
    if (row_flags & ROW_HAS_TIMESTAMP) != 0 {
        read_uvint(buf, &mut pos);
    }
    // TTL delta + LDT delta (LDT is wall-clock-derived — the source of flakiness)
    if (row_flags & ROW_HAS_TTL) != 0 {
        read_uvint(buf, &mut pos); // ttl_delta
        read_uvint(buf, &mut pos); // ldt_delta
    }
    // Deletion time (2 VInts)
    if (row_flags & ROW_HAS_DELETION) != 0 {
        read_uvint(buf, &mut pos);
        read_uvint(buf, &mut pos);
    }
    // Column bitmap (1 VInt; present when NOT ROW_HAS_ALL_COLUMNS)
    if (row_flags & ROW_HAS_ALL_COLUMNS) == 0 {
        read_uvint(buf, &mut pos);
    }

    // Now read one flags byte per column.
    let mut flags_out = Vec::with_capacity(column_sizings.len());
    for &sizing in column_sizings {
        let cell_flags = buf[pos];
        pos += 1;
        flags_out.push(cell_flags);

        // Skip timestamp delta when the cell carries its own timestamp.
        if (cell_flags & CELL_USE_ROW_TIMESTAMP) == 0 {
            read_uvint(buf, &mut pos);
        }
        // Tombstone cells carry an LDT delta.
        if (cell_flags & CELL_IS_DELETED) != 0 {
            read_uvint(buf, &mut pos);
        }
        // Skip value (absent when HAS_EMPTY_VALUE is set).
        if (cell_flags & CELL_HAS_EMPTY_VALUE) == 0 {
            match sizing {
                CellValueSizing::Variable => {
                    let value_len = read_uvint(buf, &mut pos) as usize;
                    pos += value_len;
                }
                CellValueSizing::Fixed(n) => {
                    pos += n;
                }
            }
        }
    }

    flags_out
}

/// Parse a `write_complex_column` output buffer and return the flag byte for every cell.
///
/// The buffer has this deterministic structure:
/// ```text
/// [complex_deletion_ts_delta:  unsigned VInt]  ← 2 VInts, time-derived but fixed per stats
/// [complex_deletion_ldt_delta: unsigned VInt]
/// [cell_count: unsigned VInt]
/// per cell:
///   [flags: u8]
///   if IS_EXPIRING (0x02 set):
///     [ts_delta:  unsigned VInt]
///     [ldt_delta: unsigned VInt]   ← wall-clock-derived
///     [ttl_delta: unsigned VInt]
///   [path_len:  unsigned VInt]
///   [path_bytes: path_len]
///   if !HAS_EMPTY_VALUE (0x04 NOT set):
///     [value_len: unsigned VInt]
///     [value_bytes: value_len]
/// ```
///
/// Scanning the raw buffer for a flag byte value is fragile because
/// wall-clock-derived LDT bytes can coincidentally equal the flag byte (~1-2% of CI runs).
/// This helper walks the structure deterministically so each flag byte is read at
/// its exact position.
pub(super) fn parse_complex_cell_flags(buf: &[u8]) -> Vec<u8> {
    /// Read one unsigned VInt from `buf` starting at `*pos`; advance `*pos`.
    fn read_uvint(buf: &[u8], pos: &mut usize) -> u64 {
        let first = buf[*pos];
        *pos += 1;
        if first == 0xFF {
            // 9-byte form: 0xFF + 8 big-endian bytes
            let mut v = 0u64;
            for _ in 0..8 {
                v = (v << 8) | buf[*pos] as u64;
                *pos += 1;
            }
            return v;
        }
        // Count leading 1-bits in `first` to determine extra bytes
        let extra = first.leading_ones() as usize;
        // Data bits in first byte: mask off the leading 1s and the 0 separator
        let mask = 0xFF_u8.wrapping_shr((extra + 1) as u32);
        let mut v = (first & mask) as u64;
        for _ in 0..extra {
            v = (v << 8) | buf[*pos] as u64;
            *pos += 1;
        }
        v
    }

    let mut pos = 0usize;
    // Skip complex deletion header: 2 unsigned VInts
    read_uvint(buf, &mut pos);
    read_uvint(buf, &mut pos);

    // Cell count
    let cell_count = read_uvint(buf, &mut pos) as usize;

    let mut flags_out = Vec::with_capacity(cell_count);
    for _ in 0..cell_count {
        let flags = buf[pos];
        pos += 1;
        flags_out.push(flags);

        if (flags & CELL_IS_EXPIRING) != 0 {
            // IS_EXPIRING: ts_delta + ldt_delta + ttl_delta (3 unsigned VInts)
            read_uvint(buf, &mut pos);
            read_uvint(buf, &mut pos);
            read_uvint(buf, &mut pos);
        }
        // USE_ROW_TIMESTAMP / non-expiring cells: no extra fields before path

        // Cell path: path_len VInt + path_len bytes
        let path_len = read_uvint(buf, &mut pos) as usize;
        pos += path_len;

        // Cell value: only present when HAS_EMPTY_VALUE is NOT set
        if (flags & CELL_HAS_EMPTY_VALUE) == 0 {
            let value_len = read_uvint(buf, &mut pos) as usize;
            pos += value_len;
        }
    }

    flags_out
}

/// Build a deterministic set of partitions used by the streaming tests.
pub(super) fn streaming_test_partitions() -> Vec<(DecoratedKey, Vec<Mutation>)> {
    let table_id = TableId::new("test_ks", "test_table");
    (0..16u32)
        .map(|i| {
            let key = DecoratedKey::new(i as i64, i.to_be_bytes().to_vec());
            let pk = PartitionKey::single("id", Value::Integer(i as i32));
            let mutation = Mutation::new(
                table_id.clone(),
                pk,
                None,
                vec![CellOperation::Write {
                    column: "name".to_string(),
                    value: Value::Text(format!("partition-{i}")),
                }],
                1_001_000 + i as i64,
                None,
            );
            (key, vec![mutation])
        })
        .collect()
}

/// Decode `(complex_deletion_ts_delta, complex_deletion_ldt_delta, cells)`
/// from a per-element complex-column buffer, walking the exact wire format
/// the reader (`parse_complex_cell_value`) parses.
pub(super) fn decode_complex_column(buf: &[u8]) -> (u64, u64, Vec<DecodedComplexCell>) {
    fn read_uvint(buf: &[u8], pos: &mut usize) -> u64 {
        let first = buf[*pos];
        *pos += 1;
        if first == 0xFF {
            let mut v = 0u64;
            for _ in 0..8 {
                v = (v << 8) | buf[*pos] as u64;
                *pos += 1;
            }
            return v;
        }
        let extra = first.leading_ones() as usize;
        let mask = 0xFF_u8.wrapping_shr((extra + 1) as u32);
        let mut v = (first & mask) as u64;
        for _ in 0..extra {
            v = (v << 8) | buf[*pos] as u64;
            *pos += 1;
        }
        v
    }

    let mut pos = 0usize;
    let del_ts = read_uvint(buf, &mut pos);
    let del_ldt = read_uvint(buf, &mut pos);
    let cell_count = read_uvint(buf, &mut pos) as usize;

    let mut cells = Vec::with_capacity(cell_count);
    for _ in 0..cell_count {
        let flags = buf[pos];
        pos += 1;
        let is_deleted = (flags & CELL_IS_DELETED) != 0;
        let is_expiring = (flags & CELL_IS_EXPIRING) != 0;
        let has_empty_value = (flags & CELL_HAS_EMPTY_VALUE) != 0;
        let use_row_ts = (flags & CELL_USE_ROW_TIMESTAMP) != 0;
        let use_row_ttl = (flags & CELL_USE_ROW_TTL) != 0;

        let ts_delta = if !use_row_ts {
            Some(read_uvint(buf, &mut pos))
        } else {
            None
        };
        let ldt_delta = if !use_row_ttl && (is_deleted || is_expiring) {
            Some(read_uvint(buf, &mut pos))
        } else {
            None
        };
        let ttl_delta = if !use_row_ttl && is_expiring {
            Some(read_uvint(buf, &mut pos))
        } else {
            None
        };

        let path_len = read_uvint(buf, &mut pos) as usize;
        let cell_path = buf[pos..pos + path_len].to_vec();
        pos += path_len;

        let value = if is_deleted || has_empty_value {
            None
        } else {
            let value_len = read_uvint(buf, &mut pos) as usize;
            let v = buf[pos..pos + value_len].to_vec();
            pos += value_len;
            Some(v)
        };

        cells.push(DecodedComplexCell {
            flags,
            ts_delta,
            ldt_delta,
            ttl_delta,
            cell_path,
            value,
        });
    }
    (del_ts, del_ldt, cells)
}

pub(super) fn set_column(name: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: "set<int>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

pub(super) fn list_column(name: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: "list<int>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

/// Schema: id (pk int) / tags (regular non-frozen set<int>). The single
/// regular column is complex, so the bitmap is a 1-column subset.
pub(super) fn complex_only_schema() -> TableSchema {
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
                name: "tags".to_string(),
                data_type: "set<int>".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Walk a serialized merged-row body for `complex_only_schema` (one regular
/// complex column, no clustering) far enough to assert Finding 1 + 2. Returns
/// `(row_flags, column_present, complex_deletion_decoded, cells)`.
pub(super) fn parse_complex_only_row(
    body: &[u8],
    flags: u8,
    stats: &StatisticsMetadata,
) -> (bool, Option<(u64, u64)>, Vec<DecodedComplexCell>) {
    // The merged-row body layout written by `build_merged_row_body` (after the
    // outer row_size + prev_size, which we strip in the caller):
    //   [timestamp delta]  if HAS_TIMESTAMP
    //   [ttl + ldt deltas] if HAS_TTL
    //   [deletion 2 vints] if HAS_DELETION
    //   [column bitmap]    if NOT HAS_ALL_COLUMNS
    //   [complex column: complex_deletion(2 vints) + cell_count + cells...]
    fn read_uvint(buf: &[u8], pos: &mut usize) -> u64 {
        let first = buf[*pos];
        *pos += 1;
        if first == 0xFF {
            let mut v = 0u64;
            for _ in 0..8 {
                v = (v << 8) | buf[*pos] as u64;
                *pos += 1;
            }
            return v;
        }
        let extra = first.leading_ones() as usize;
        let mask = 0xFF_u8.wrapping_shr((extra + 1) as u32);
        let mut v = (first & mask) as u64;
        for _ in 0..extra {
            v = (v << 8) | buf[*pos] as u64;
            *pos += 1;
        }
        v
    }

    let _ = stats;
    let mut pos = 0usize;

    if (flags & ROW_HAS_TIMESTAMP) != 0 {
        let _ts = read_uvint(body, &mut pos);
    }
    if (flags & ROW_HAS_TTL) != 0 {
        let _ttl = read_uvint(body, &mut pos);
        let _ldt = read_uvint(body, &mut pos);
    }
    if (flags & ROW_HAS_DELETION) != 0 {
        let _dts = read_uvint(body, &mut pos);
        let _dldt = read_uvint(body, &mut pos);
    }

    // Column bitmap: for the single regular complex column, bit 0 == 1 means
    // the column is MISSING. HAS_ALL_COLUMNS (which we never expect for a
    // single complex column) would skip the bitmap entirely.
    let column_present = if (flags & ROW_HAS_ALL_COLUMNS) == 0 {
        let bitmap = read_uvint(body, &mut pos);
        (bitmap & 0x1) == 0
    } else {
        true
    };

    // Complex column: deletion header then cells (reusing the fragment walk).
    let del_ts = read_uvint(body, &mut pos);
    let del_ldt = read_uvint(body, &mut pos);
    let cell_count = read_uvint(body, &mut pos) as usize;

    let mut cells = Vec::with_capacity(cell_count);
    for _ in 0..cell_count {
        let cell_flags = body[pos];
        pos += 1;
        let is_deleted = (cell_flags & CELL_IS_DELETED) != 0;
        let is_expiring = (cell_flags & CELL_IS_EXPIRING) != 0;
        let has_empty_value = (cell_flags & CELL_HAS_EMPTY_VALUE) != 0;
        let use_row_ts = (cell_flags & CELL_USE_ROW_TIMESTAMP) != 0;
        let use_row_ttl = (cell_flags & CELL_USE_ROW_TTL) != 0;

        let ts_delta = if !use_row_ts {
            Some(read_uvint(body, &mut pos))
        } else {
            None
        };
        let ldt_delta = if !use_row_ttl && (is_deleted || is_expiring) {
            Some(read_uvint(body, &mut pos))
        } else {
            None
        };
        let ttl_delta = if !use_row_ttl && is_expiring {
            Some(read_uvint(body, &mut pos))
        } else {
            None
        };

        let path_len = read_uvint(body, &mut pos) as usize;
        let cell_path = body[pos..pos + path_len].to_vec();
        pos += path_len;

        let value = if is_deleted || has_empty_value {
            None
        } else {
            let value_len = read_uvint(body, &mut pos) as usize;
            let v = body[pos..pos + value_len].to_vec();
            pos += value_len;
            Some(v)
        };

        cells.push(DecodedComplexCell {
            flags: cell_flags,
            ts_delta,
            ldt_delta,
            ttl_delta,
            cell_path,
            value,
        });
    }

    (column_present, Some((del_ts, del_ldt)), cells)
}

/// Schema: id (pk int) + two non-frozen complex columns. `aaa_set` sorts
/// before `zzz_set`, so they straddle the whole-column / per-element split.
pub(super) fn two_complex_columns_schema() -> TableSchema {
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
            set_column("aaa_set"),
            set_column("zzz_set"),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// `person { name text, age int, email text }` as a top-level non-frozen UDT
/// marshal string.
pub(super) fn person_udt_marshal() -> String {
    "org.apache.cassandra.db.marshal.UserType(\
         test_ks,706572736f6e,\
         6e616d65:org.apache.cassandra.db.marshal.UTF8Type,\
         616765:org.apache.cassandra.db.marshal.Int32Type,\
         656d61696c:org.apache.cassandra.db.marshal.UTF8Type)"
        .to_string()
}

pub(super) fn udt_column(name: &str, data_type: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: data_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

pub(super) fn udt_field(name: &str, value: Option<Value>) -> crate::types::UdtField {
    crate::types::UdtField {
        name: name.to_string(),
        value,
    }
}

pub(super) fn person_reader() -> crate::storage::sstable::reader::V5CompressedLegacyParser {
    crate::storage::sstable::reader::V5CompressedLegacyParser::new(
        "test_ks".to_string(),
        "test_table".to_string(),
        1_000_000, // min_timestamp (matches create_test_stats)
        0,         // min_local_deletion_time
        Some(0),   // min_ttl
    )
}

/// Schema with a non-frozen complex column `tags` (`list<text>`).
pub(super) fn complex_column_schema() -> TableSchema {
    TableSchema {
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![Column {
            name: "tags".to_string(),
            data_type: "list<text>".to_string(),
            nullable: true,
            default: None,
            is_static: false,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Schema with TWO non-frozen complex columns (`tags` and `notes`, both
/// `list<text>`) for mixed-stream / multi-column reconcile tests (issue #921).
pub(super) fn two_complex_column_schema() -> TableSchema {
    let mut schema = complex_column_schema();
    schema.columns.push(Column {
        name: "notes".to_string(),
        data_type: "list<text>".to_string(),
        nullable: true,
        default: None,
        is_static: false,
    });
    schema
}

/// Build the `person { name text, age int, email text }` registry def used
/// by the bare-name tests, in declared field order.
pub(super) fn person_udt_def() -> UdtTypeDef {
    UdtTypeDef::new("test_ks".to_string(), "person".to_string())
        .with_field("name".to_string(), CqlType::Text, true)
        .with_field("age".to_string(), CqlType::Int, true)
        .with_field("email".to_string(), CqlType::Text, true)
}

/// A registry containing only the `person` UDT.
pub(super) fn person_registry() -> UdtRegistry {
    let mut reg = UdtRegistry::new();
    reg.register_udt(person_udt_def());
    reg
}
