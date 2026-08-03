//! The PINNED `ws0.events` schema (issue #3096).
//!
//! # Where the pin comes from, and how the two committed copies were reconciled
//!
//! Two committed artifacts describe this table:
//!
//! * `docs/reports/ws0-3026-artifacts/ws0-corpus/schema-as-created.cql` — the
//!   `DESCRIBE TABLE` of the Cassandra-created #3026 corpus, and
//!   `docs/reports/ws0-3100-artifacts/ws0-h2h/schemas/ws0-events.cql` — the bare
//!   DDL used by the #3100 head-to-head.
//!
//! They AGREE exactly on what this generator needs: the twelve columns, their
//! types, `PRIMARY KEY (part_id, seq, event_time)`, and
//! `CLUSTERING ORDER BY (seq ASC, event_time ASC)`. They differ ONLY in table
//! OPTIONS, which the `DESCRIBE` form carries and the bare form omits — most
//! materially `compression = {LZ4Compressor, chunk_length_in_kb: 16}`.
//!
//! This generator follows the `ws0-events.cql` form (the task's tie-breaker), and
//! that is also the only form CQLite can honor: the production write surface
//! emits UNCOMPRESSED SSTables and never a `CompressionInfo.db` (issue #1406), so
//! the `DESCRIBE` form's LZ4 clause is unreproducible here by construction. That
//! is a recorded difference from the #3026/#3100 corpora, not a defect — and it
//! is precisely why this change re-baselines rather than restating those
//! corpora's absolute rows/s.
//!
//! No-heuristics (issue #28): every column name and type below comes from that
//! DDL. Nothing is inferred from bytes, from a file name, or from a value.

use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};

/// Keyspace of the pinned fixture table.
pub const KEYSPACE: &str = "ws0";
/// Table of the pinned fixture table.
pub const TABLE: &str = "events";

/// The pinned DDL, byte-identical to
/// `docs/reports/ws0-3100-artifacts/ws0-h2h/schemas/ws0-events.cql` (asserted by
/// [`tests::ddl_matches_the_committed_pin`]). Emitted next to the corpus so both
/// measurement arms read the SAME DDL the corpus was written from.
pub const DDL: &str = "CREATE TABLE ws0.events (part_id text, seq int, event_time timestamp, blob_a blob, blob_b blob, device_id uuid, metric_a int, metric_b bigint, metric_c double, payload text, region text, status text, PRIMARY KEY (part_id, seq, event_time)) WITH CLUSTERING ORDER BY (seq ASC, event_time ASC);";

/// The twelve `(name, cql_type)` pairs in DDL declaration order. `cells_per_row`
/// in the recorded corpus identity is derived from this length, never assumed.
pub const COLUMNS: [(&str, &str); 12] = [
    ("part_id", "text"),
    ("seq", "int"),
    ("event_time", "timestamp"),
    ("blob_a", "blob"),
    ("blob_b", "blob"),
    ("device_id", "uuid"),
    ("metric_a", "int"),
    ("metric_b", "bigint"),
    ("metric_c", "double"),
    ("payload", "text"),
    ("region", "text"),
    ("status", "text"),
];

/// The three PRIMARY KEY column names: partition key first, then the two
/// clustering columns in order.
const PK_COLUMNS: [&str; 3] = ["part_id", "seq", "event_time"];

/// The pinned `ws0.events` [`TableSchema`], built from [`COLUMNS`] so the schema
/// and the emitted DDL can never drift.
pub fn ws0_events_schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "part_id".to_string(),
            data_type: "text".to_string(),
            position: 0,
        }],
        clustering_keys: vec![
            ClusteringColumn {
                name: "seq".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: Default::default(),
            },
            ClusteringColumn {
                name: "event_time".to_string(),
                data_type: "timestamp".to_string(),
                position: 1,
                order: Default::default(),
            },
        ],
        columns: COLUMNS
            .iter()
            .map(|(name, ty)| Column {
                name: (*name).to_string(),
                data_type: (*ty).to_string(),
                nullable: !PK_COLUMNS.contains(name),
                default: None,
                is_static: false,
            })
            .collect(),
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn repo_root() -> PathBuf {
        // `tools/ws0-corpus-gen` -> `tools` -> repo root.
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("tools/<crate> has a grandparent")
            .to_path_buf()
    }

    /// The in-code [`DDL`] must stay byte-identical (modulo trailing whitespace)
    /// to the committed pin, so a corpus can never be generated from a DDL that
    /// silently drifted from the artifact the measurement method cites.
    #[test]
    fn ddl_matches_the_committed_pin() {
        let pin =
            repo_root().join("docs/reports/ws0-3100-artifacts/ws0-h2h/schemas/ws0-events.cql");
        let committed = std::fs::read_to_string(&pin)
            .unwrap_or_else(|e| panic!("read committed DDL pin {}: {e}", pin.display()));
        assert_eq!(
            committed.trim(),
            DDL.trim(),
            "tools/ws0-corpus-gen's pinned DDL diverged from {}",
            pin.display()
        );
    }

    /// The `DESCRIBE`-form artifact and the bare-DDL artifact must still agree on
    /// the twelve columns and the primary key — the reconciliation this module's
    /// docs record is ASSERTED, not just asserted in prose.
    #[test]
    fn the_two_committed_schema_artifacts_agree_on_columns_and_key() {
        let describe =
            repo_root().join("docs/reports/ws0-3026-artifacts/ws0-corpus/schema-as-created.cql");
        let text = std::fs::read_to_string(&describe)
            .unwrap_or_else(|e| panic!("read {}: {e}", describe.display()));
        for (name, ty) in COLUMNS {
            assert!(
                text.contains(&format!("{name} {ty},")),
                "{} must declare `{name} {ty}`",
                describe.display()
            );
        }
        assert!(
            text.contains("PRIMARY KEY (part_id, seq, event_time)"),
            "{} must declare the pinned primary key",
            describe.display()
        );
    }

    /// The schema is built from [`COLUMNS`]: the PK columns are non-nullable, the
    /// rest nullable, and none is static.
    #[test]
    fn schema_shape_matches_the_ddl() {
        let s = ws0_events_schema();
        assert_eq!(s.columns.len(), 12);
        assert_eq!(s.partition_keys.len(), 1);
        assert_eq!(s.clustering_keys.len(), 2);
        for c in &s.columns {
            assert!(!c.is_static, "{} must not be static", c.name);
            assert_eq!(
                c.nullable,
                !PK_COLUMNS.contains(&c.name.as_str()),
                "{} nullability must follow its PK membership",
                c.name
            );
        }
    }
}
