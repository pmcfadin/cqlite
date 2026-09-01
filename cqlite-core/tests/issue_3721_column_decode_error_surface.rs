//! Issue #3721 — a per-column decode failure must be OBSERVABLE, never a
//! successful `SELECT` with the failing column *and every later on-disk column*
//! silently missing.
//!
//! # What this pins
//!
//! Row assembly (`row_decoder/row_data.rs`) had exactly ONE error handler per
//! column path, and both were a bare `break`:
//!
//! * the COMPLEX arm (shared by `parse_complex_column_inner` — the compaction /
//!   elements-out read — and `parse_complex_column`, the user-facing read), and
//! * the SIMPLE arm (`// CRITICAL FIX: Stop parsing remaining columns…`).
//!
//! `break` exits the per-column loop, after which the row is assembled from the
//! cells collected SO FAR and returned as `Ok`. The failing column and every
//! later on-disk column therefore vanish while the read reports SUCCESS —
//! indistinguishable from "those columns are genuinely null". Cassandra does not
//! do this: a cell it cannot read raises out of `UnfilteredSerializer` and the
//! read fails (`CorruptSSTableException`); it never serves a short row.
//!
//! Every case below asserts at the PUBLIC surface (`Database::execute`), because
//! the whole defect is that the unit level looks fine.

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::{Config, Database};

#[path = "support/datasets_root.rs"]
mod datasets_root;

use datasets_root::{describe_search, sstables_root_for_table};

/// COMMITTED BIG (`nb`) fixture: `id int PRIMARY KEY, s set<int>, m map<int,text>`,
/// both collections NON-FROZEN (multicell), so `m` is decoded by the COMPLEX arm.
/// On-disk regular-column order is alphabetical (`m` then `s`), so `s` is the
/// "later on-disk column" a swallow also drops.
const COMPLEX_KEYSPACE: &str = "test_signed_coll";
const COMPLEX_TABLE: &str = "signed_int_collections";

/// COMMITTED BTI (`da`) fixture: `id uuid PRIMARY KEY, name text, age int,
/// salary bigint, active boolean, created timestamp`. On-disk regular-column
/// order is alphabetical, so `salary` follows `name`.
const SIMPLE_KEYSPACE: &str = "test_da";
const SIMPLE_TABLE: &str = "simple_table";

/// Resolve a fixture root, or FAIL — never skip (issue #3220). Both fixtures are
/// force-added to git, so absence is a resolution defect, not a legitimate state.
fn fixture_root(keyspace: &str, table: &str) -> PathBuf {
    sstables_root_for_table(keyspace, table).unwrap_or_else(|| {
        panic!(
            "{keyspace}.{table} is COMMITTED to git and must resolve in every checkout, \
             unconditionally (issue #3220) — {}.\n  remedy: git restore --source=HEAD -- \
             test-data/datasets/sstables",
            describe_search(keyspace, table)
        )
    })
}

/// Write a CQL schema to a scratch file and return its path.
///
/// The DATA is always real Cassandra-written bytes; only the DECLARED type is
/// varied, which is how a per-column value decode failure is provoked without
/// hand-forging an SSTable (a CQLite-written fixture would be invariant to a
/// uniform framing error and could not be an oracle here).
fn write_schema(dir: &Path, body: &str) -> PathBuf {
    let path = dir.join("schema.cql");
    std::fs::write(&path, body).expect("write scratch schema");
    path
}

async fn open_db(root: &Path, schema: &Path, keyspace: &str) -> Database {
    let cfg = IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: root.to_path_buf(),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    let result = ingest(cfg).await.expect("ingestion succeeds");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "schema must load"
    );
    result.database
}

const CORRECT_COMPLEX_SCHEMA: &str = "\
CREATE KEYSPACE IF NOT EXISTS test_signed_coll WITH replication = \
{'class': 'SimpleStrategy', 'replication_factor': 1};
USE test_signed_coll;
CREATE TABLE IF NOT EXISTS signed_int_collections (
    id INT PRIMARY KEY,
    s  SET<INT>,
    m  MAP<INT, TEXT>
);
";

/// Identical to [`CORRECT_COMPLEX_SCHEMA`] except that `m`'s KEY type is declared
/// `BIGINT` (8 bytes) while the on-disk cell paths are 4-byte `int` keys, so
/// `parse_cell_path_key` returns `Err` for the FIRST element of `m`.
const WRONG_KEY_COMPLEX_SCHEMA: &str = "\
CREATE KEYSPACE IF NOT EXISTS test_signed_coll WITH replication = \
{'class': 'SimpleStrategy', 'replication_factor': 1};
USE test_signed_coll;
CREATE TABLE IF NOT EXISTS signed_int_collections (
    id INT PRIMARY KEY,
    s  SET<INT>,
    m  MAP<BIGINT, TEXT>
);
";

#[tokio::test]
async fn probe_complex_swallow_today() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = fixture_root(COMPLEX_KEYSPACE, COMPLEX_TABLE);

    let good = write_schema(dir.path(), CORRECT_COMPLEX_SCHEMA);
    let db = open_db(&root, &good, COMPLEX_KEYSPACE).await;
    let ok = db
        .execute(&format!(
            "SELECT * FROM {COMPLEX_KEYSPACE}.{COMPLEX_TABLE}"
        ))
        .await
        .expect("correct-schema SELECT");
    eprintln!("PROBE correct-schema rows={}", ok.rows.len());
    for r in ok.rows.iter().take(3) {
        eprintln!("  {:?}", r.values);
    }

    let bad_dir = tempfile::tempdir().expect("tempdir");
    let bad = write_schema(bad_dir.path(), WRONG_KEY_COMPLEX_SCHEMA);
    let db2 = open_db(&root, &bad, COMPLEX_KEYSPACE).await;
    let res = db2
        .execute(&format!(
            "SELECT * FROM {COMPLEX_KEYSPACE}.{COMPLEX_TABLE}"
        ))
        .await;
    match res {
        Ok(r) => {
            eprintln!("PROBE wrong-key SELECT returned Ok rows={}", r.rows.len());
            for row in r.rows.iter().take(3) {
                eprintln!("  {:?}", row.values);
            }
        }
        Err(e) => eprintln!("PROBE wrong-key SELECT returned Err: {e}"),
    }
}

const CORRECT_DA_COLL: &str = "\
CREATE KEYSPACE IF NOT EXISTS test_da WITH replication = \
{'class': 'SimpleStrategy', 'replication_factor': 1};
USE test_da;
CREATE TABLE IF NOT EXISTS collection_table (
    id UUID PRIMARY KEY,
    tags SET<TEXT>,
    scores LIST<INT>,
    properties MAP<TEXT, TEXT>
);
";

const WRONG_DA_COLL: &str = "\
CREATE KEYSPACE IF NOT EXISTS test_da WITH replication = \
{'class': 'SimpleStrategy', 'replication_factor': 1};
USE test_da;
CREATE TABLE IF NOT EXISTS collection_table (
    id UUID PRIMARY KEY,
    tags SET<TEXT>,
    scores LIST<INT>,
    properties MAP<INT, TEXT>
);
";

const CORRECT_DA_SIMPLE: &str = "\
CREATE KEYSPACE IF NOT EXISTS test_da WITH replication = \
{'class': 'SimpleStrategy', 'replication_factor': 1};
USE test_da;
CREATE TABLE IF NOT EXISTS simple_table (
    id UUID PRIMARY KEY,
    name TEXT,
    age INT,
    salary BIGINT,
    active BOOLEAN,
    created TIMESTAMP
);
";

const WRONG_DA_SIMPLE: &str = "\
CREATE KEYSPACE IF NOT EXISTS test_da WITH replication = \
{'class': 'SimpleStrategy', 'replication_factor': 1};
USE test_da;
CREATE TABLE IF NOT EXISTS simple_table (
    id UUID PRIMARY KEY,
    name INET,
    age INT,
    salary BIGINT,
    active BOOLEAN,
    created TIMESTAMP
);
";

async fn probe(label: &str, ks: &str, table: &str, schema_body: &str) {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = fixture_root(ks, table);
    let schema = write_schema(dir.path(), schema_body);
    let db = open_db(&root, &schema, ks).await;
    match db.execute(&format!("SELECT * FROM {ks}.{table}")).await {
        Ok(r) => {
            eprintln!("PROBE {label}: Ok rows={}", r.rows.len());
            for row in r.rows.iter().take(2) {
                eprintln!("   {:?}", row.values);
            }
        }
        Err(e) => eprintln!("PROBE {label}: Err: {e}"),
    }
}

#[tokio::test]
async fn probe_da_cases() {
    probe("da-coll-correct", "test_da", "collection_table", CORRECT_DA_COLL).await;
    probe("da-coll-wrong", "test_da", "collection_table", WRONG_DA_COLL).await;
    probe("da-simple-correct", "test_da", "simple_table", CORRECT_DA_SIMPLE).await;
    probe("da-simple-wrong", "test_da", "simple_table", WRONG_DA_SIMPLE).await;
}
