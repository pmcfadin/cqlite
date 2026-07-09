//! Shared field-shape fixture — the single source of truth (issue #2283).
//!
//! The `cassandra_easy_stress.keyvalue` shape (issue #2193) is exercised by TWO
//! independent callers that MUST stay byte-for-byte in lockstep:
//!
//! * `examples/emit_arrow_golden.rs` — emits the committed `keyvalue.flightdata`
//!   golden the Java `FlightDataGoldenDecodeTest` decodes, and
//! * `tests/do_get_transport_test.rs` — whose
//!   `do_get_over_transport_matches_committed_golden` byte-compares the LIVE wire
//!   bytes against that same golden.
//!
//! When these two definitions live apart, drift in either silently breaks (or
//! silently "fixes") the byte-identical pin, defeating its purpose. Both callers
//! are separate crates linked against this library, so the shared fixture has to
//! be reachable through the public surface — a `pub(crate)` module or a
//! `tests/common` module would NOT reach the example. It is `#[doc(hidden)]` so
//! it stays out of the documented API while remaining callable.
//!
//! Only the drift-sensitive *field shape* (keyspace/table/columns, the canonical
//! rows, timestamp, batch size, and the mutation set) lives here. The temp-dir
//! flush plumbing stays local to each caller because it depends on `tempfile`
//! (a dev-dependency), and is generic (schema-in, data-dir-out) rather than
//! shape-specific.

use std::collections::HashMap;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{CellOperation, Mutation, PartitionKey, TableId};
use cqlite_core::types::Value;

/// Keyspace of the field-shape fixture (the round-3 cassandra-easy-stress run).
pub const KEYVALUE_KS: &str = "cassandra_easy_stress";
/// Table of the field-shape fixture.
pub const KEYVALUE_TBL: &str = "keyvalue";
/// The DDL the connector ticket carries for the field-shape fixture: a text
/// partition key + a single text value column, no clustering key.
pub const KEYVALUE_DDL: &str =
    "CREATE TABLE cassandra_easy_stress.keyvalue (key text PRIMARY KEY, value text)";
/// The 3 canonical `(key, value)` rows. Pinned so the Java decode assertion can
/// hard-code them; row order in the output is the server's token order, so the
/// Java side asserts the value SET rather than positional order.
pub const KEYVALUE_ROWS: [(&str, &str); 3] = [("k1", "1"), ("k2", "2"), ("k3", "3")];
/// Write timestamp for every fixture row (a fixed, deterministic value — NOT
/// wall-clock — so the golden stays byte-stable).
pub const KEYVALUE_TIMESTAMP: i64 = 100;
/// Producer/service batch size. `8192` matches the field flight image so all 3
/// rows land in one final-flush batch — the exact shape that failed in the
/// round-3 run — and keeps the golden and the live-transport pin aligned.
pub const KEYVALUE_BATCH_SIZE: usize = 8192;

/// The field-shape `keyvalue` schema: `key text` partition key + `value text`
/// regular column, no clustering key.
pub fn keyvalue_schema() -> TableSchema {
    let col = |name: &str, nullable: bool| Column {
        name: name.into(),
        data_type: "text".into(),
        nullable,
        default: None,
        is_static: false,
    };
    TableSchema {
        keyspace: KEYVALUE_KS.into(),
        table: KEYVALUE_TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "key".into(),
            data_type: "text".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![col("key", false), col("value", true)],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// One `Write` mutation for `(key, value)` at [`KEYVALUE_TIMESTAMP`]: partition
/// key `key`, regular column `value`.
pub fn keyvalue_write(key: &str, value: &str) -> Mutation {
    Mutation::new(
        TableId::new(KEYVALUE_KS, KEYVALUE_TBL),
        PartitionKey::single("key", Value::Text(key.into())),
        None,
        vec![CellOperation::Write {
            column: "value".into(),
            value: Value::Text(value.into()),
        }],
        KEYVALUE_TIMESTAMP,
        None,
    )
}

/// One `Write` mutation per [`KEYVALUE_ROWS`] entry.
pub fn keyvalue_mutations() -> Vec<Mutation> {
    KEYVALUE_ROWS
        .iter()
        .map(|(key, value)| keyvalue_write(key, value))
        .collect()
}
