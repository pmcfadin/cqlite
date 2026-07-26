//! Deterministic, self-contained synthetic row shapes for the byte-cap suite
//! (issue #2825).
//!
//! # Why these are synthetic
//!
//! A byte-cap test backed by the fetched `test_wide_rows` corpus would pass
//! **vacuously** in a checkout that never ran `fetch-datasets.sh`: the repo ships
//! only JSONL references, so the table directory exists but has no `Data.db`, the
//! scan yields zero rows, and "every non-final batch has fewer than `batch_size`
//! rows" is trivially true over zero batches. The testing doctrine forbids that,
//! so the wide-row coverage is generated **in process** from the shapes here:
//! every byte-cap test runs with real rows regardless of the dataset state.
//!
//! Only the drift-sensitive *shape* (keyspace/table/DDL/columns and the mutation
//! set) lives here. The temp-dir flush plumbing stays with each caller because it
//! depends on `tempfile`, a dev-dependency — the same split
//! [`crate::test_fixtures`] uses.
//!
//! Both callers — the in-crate `batch_bytes_tests` unit suite and the
//! `tests/issue_2825_max_batch_bytes_e2e.rs` integration binary — build from
//! these definitions, so the unit and end-to-end evidence describe the same rows.

use std::collections::HashMap;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{CellOperation, Mutation, PartitionKey, TableId};
use cqlite_core::types::Value;

/// Keyspace of both synthetic byte-cap shapes.
pub const BYTECAP_KS: &str = "bytecap_ks";

/// Wide-row table: one `blob` payload column wide enough that a full
/// `batch_size`-row batch would blow any sane byte-cap.
pub const WIDE_TBL: &str = "wide_rows";

/// DDL for [`wide_row_schema`], as a ticket carries it.
pub const WIDE_DDL: &str =
    "CREATE TABLE bytecap_ks.wide_rows (id int PRIMARY KEY, payload blob, label text)";

/// Narrow table: the no-regression shape — a full `batch_size`-row batch of these
/// sits far under [`crate::batch_bytes::DEFAULT_MAX_BATCH_BYTES`], so the row-cap
/// must remain the binding boundary.
pub const NARROW_TBL: &str = "narrow_rows";

/// DDL for [`narrow_row_schema`].
pub const NARROW_DDL: &str =
    "CREATE TABLE bytecap_ks.narrow_rows (id int PRIMARY KEY, name text, score int)";

/// Write timestamp for every fixture row — a fixed, deterministic value, never
/// wall-clock, so the fixture is reproducible run to run.
pub const FIXTURE_TIMESTAMP: i64 = 100;

fn col(name: &str, ty: &str, nullable: bool) -> Column {
    Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    }
}

/// `id int PRIMARY KEY, payload blob, label text` — the wide shape.
pub fn wide_row_schema() -> TableSchema {
    TableSchema {
        keyspace: BYTECAP_KS.into(),
        table: WIDE_TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            col("id", "int", false),
            col("payload", "blob", true),
            col("label", "text", true),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// `id int PRIMARY KEY, name text, score int` — the narrow shape.
pub fn narrow_row_schema() -> TableSchema {
    TableSchema {
        keyspace: BYTECAP_KS.into(),
        table: NARROW_TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            col("id", "int", false),
            col("name", "text", true),
            col("score", "int", true),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// Deterministic payload bytes for row `id`: a fixed-length, id-derived fill, so
/// two runs produce byte-identical SSTables and every row has the SAME width
/// (which makes "rows per batch" an exact, assertable function of the cap).
pub fn wide_payload(id: i32, payload_len: usize) -> Vec<u8> {
    let fill = (id % 251) as u8;
    vec![fill; payload_len]
}

/// `n_rows` single-row partitions, each carrying a `payload_len`-byte blob.
pub fn wide_row_mutations(n_rows: i32, payload_len: usize) -> Vec<Mutation> {
    (0..n_rows)
        .map(|id| {
            Mutation::new(
                TableId::new(BYTECAP_KS, WIDE_TBL),
                PartitionKey::single("id", Value::Integer(id)),
                None,
                vec![
                    CellOperation::Write {
                        column: "payload".into(),
                        value: Value::Blob(wide_payload(id, payload_len).into()),
                    },
                    CellOperation::Write {
                        column: "label".into(),
                        value: Value::text(format!("row-{id}")),
                    },
                ],
                FIXTURE_TIMESTAMP,
                None,
            )
        })
        .collect()
}

/// `n_rows` narrow single-row partitions (`name` is one char, `score` an int).
pub fn narrow_row_mutations(n_rows: i32) -> Vec<Mutation> {
    (0..n_rows)
        .map(|id| {
            Mutation::new(
                TableId::new(BYTECAP_KS, NARROW_TBL),
                PartitionKey::single("id", Value::Integer(id)),
                None,
                vec![
                    CellOperation::Write {
                        column: "name".into(),
                        value: Value::text("n"),
                    },
                    CellOperation::Write {
                        column: "score".into(),
                        value: Value::Integer(id),
                    },
                ],
                FIXTURE_TIMESTAMP,
                None,
            )
        })
        .collect()
}
