//! Shared byte-pin infrastructure — the single source of truth (issues
//! #2283/#2285).
//!
//! Two responsibilities, both scoped to byte-pinned / golden-comparison
//! contexts: (1) the drift-sensitive `keyvalue` field shape used by both byte-pin
//! callers (below), and (2) [`assert_wire_deterministic_metadata`] — the guard
//! that fails LOUDLY if a schema about to be byte-pinned carries a field whose
//! on-wire metadata order is not deterministic across process runs (#2285).
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

use arrow::datatypes::Schema as ArrowSchema;
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

// ---- Wire-metadata-order guard (issue #2285) -----------------------------------

/// The maximum number of metadata entries a `Field` may carry while its on-wire
/// order stays deterministic across process runs.
///
/// **Why 1 (and not more):** arrow-ipc's schema serialiser
/// (`arrow-ipc/src/convert.rs::metadata_to_fb`, confirmed against 53.4.1) builds
/// the `custom_metadata` flatbuffer vector by iterating `Field::metadata()`
/// UNSORTED, and `arrow_schema::Field` stores metadata ONLY as a
/// `std::collections::HashMap<String, String>` (settable solely via
/// `with_metadata`/`set_metadata`, both `HashMap`-typed) — there is NO public
/// hook to control `custom_metadata` ordering, so no re-ordering pass at the
/// schema level survives to the wire. Rust's default `HashMap` hasher
/// (`RandomState`) is randomly seeded per process, so a field with >= 2 metadata
/// entries serialises them in a process-random order. A field with 0 or 1 entries
/// is order-trivial and therefore byte-stable across runs.
///
/// This is a **fundamental arrow-rs limitation**: making the wire order
/// deterministic would require either a manual flatbuffer re-encode of the
/// post-encoded IPC schema message (effectively re-implementing the encoder,
/// far out of scope) or an upstream arrow-rs change to sort in `metadata_to_fb`.
/// Rather than a fix that does not actually work, [`assert_wire_deterministic_metadata`]
/// DETECTS-and-REJECTS: it fails loudly so no one can silently add a flaky
/// byte-compared golden for a field carrying >= 2 metadata keys.
pub const MAX_WIRE_DETERMINISTIC_FIELD_METADATA: usize = 1;

/// A field carries more metadata entries than have a deterministic on-wire order
/// (see [`MAX_WIRE_DETERMINISTIC_FIELD_METADATA`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonDeterministicWireMetadata {
    /// Name of the offending field.
    pub field: String,
    /// Its metadata keys, sorted for a stable, readable message.
    pub keys: Vec<String>,
}

impl std::fmt::Display for NonDeterministicWireMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "field {:?} carries {} metadata entries {:?}, whose on-wire order is \
             NOT deterministic across process runs (arrow-ipc iterates Field \
             metadata as an unsorted HashMap — see issue #2285); such a field \
             must not back a byte-compared golden",
            self.field,
            self.keys.len(),
            self.keys,
        )
    }
}

impl std::error::Error for NonDeterministicWireMetadata {}

/// Assert `schema` is safe to use in a BYTE-PINNED / golden-comparison context:
/// every field carries at most [`MAX_WIRE_DETERMINISTIC_FIELD_METADATA`] metadata
/// entries, so its `custom_metadata` wire order is deterministic across process
/// runs (issue #2285).
///
/// Call this at every point that GENERATES or byte-compares a golden (the golden
/// emitter, the transport byte-pin test). It is intentionally NOT called on the
/// general `arrow_schema()` path: a live `do_get` response legitimately carries
/// fields with two or more metadata entries (e.g. uuid columns with both the
/// arrow extension name and `cqlite:pushdown`), which are fine for the SEMANTIC
/// decode that the `all_scalars.arrows` golden uses — only BYTE comparison is
/// order-sensitive.
pub fn assert_wire_deterministic_metadata(
    schema: &ArrowSchema,
) -> Result<(), NonDeterministicWireMetadata> {
    for field in schema.fields() {
        if field.metadata().len() > MAX_WIRE_DETERMINISTIC_FIELD_METADATA {
            let mut keys: Vec<String> = field.metadata().keys().cloned().collect();
            keys.sort();
            return Err(NonDeterministicWireMetadata {
                field: field.name().clone(),
                keys,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    /// The fail-fast fires for a field with >= 2 metadata entries — the exact
    /// shape (uuid: `ARROW:extension:name` + `cqlite:pushdown`) whose wire order
    /// is hash-seed-dependent (#2285). This is the guard's whole reason to exist.
    #[test]
    fn rejects_field_with_two_metadata_entries() {
        let mut md = HashMap::new();
        md.insert("ARROW:extension:name".to_string(), "arrow.uuid".to_string());
        md.insert("cqlite:pushdown".to_string(), "equality".to_string());
        let field = Field::new("id", DataType::FixedSizeBinary(16), true).with_metadata(md);
        let schema = ArrowSchema::new(vec![field]);

        let err = assert_wire_deterministic_metadata(&schema)
            .expect_err("a field with 2 metadata entries must be rejected");
        assert_eq!(err.field, "id");
        assert_eq!(
            err.keys,
            vec![
                "ARROW:extension:name".to_string(),
                "cqlite:pushdown".to_string()
            ],
            "keys are reported sorted for a stable message"
        );
    }

    /// A field with a single metadata entry (order-trivial) is accepted — this is
    /// the `keyvalue` byte-pin golden's shape (`cqlite:pushdown` only).
    #[test]
    fn accepts_field_with_single_metadata_entry() {
        let mut md = HashMap::new();
        md.insert("cqlite:pushdown".to_string(), "full".to_string());
        let field = Field::new("value", DataType::Utf8, true).with_metadata(md);
        let schema = ArrowSchema::new(vec![field]);
        assert!(
            assert_wire_deterministic_metadata(&schema).is_ok(),
            "a 1-metadata-entry field is order-trivial and byte-stable"
        );
    }

    /// The actual `keyvalue` wire schema the byte-pin golden is built from must be
    /// guard-clean — proving the guard does not regress the committed golden and
    /// that the `keyvalue` case really is the safe single-entry shape.
    #[test]
    fn keyvalue_wire_schema_is_byte_pin_safe() {
        let producer =
            crate::producer::MergeProducer::new(keyvalue_schema(), KEYVALUE_BATCH_SIZE).unwrap();
        let schema = producer.arrow_schema().unwrap();
        assert!(
            assert_wire_deterministic_metadata(&schema).is_ok(),
            "the keyvalue byte-pin golden schema must have <= 1 metadata entry per field"
        );
    }
}
