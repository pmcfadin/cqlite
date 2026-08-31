//! Issue #3612 (review round 1) — WHAT A CELL-PATH-KEY ERROR DOES AT THE PUBLIC
//! SURFACE, measured rather than reasoned about.
//!
//! Two reviewers disagreed about this and the disagreement decided a design
//! choice, so it is pinned here as an executable fact rather than as prose.
//!
//! ## The claim under test
//! `V5CompressedLegacyParser::parse_cell_path_key` can return `Err`. The `?` at
//! its call site DOES propagate out of `parse_complex_column` — but row assembly
//! then SWALLOWS it: the complex-column `match` in
//! `row_decoder/row_data.rs` has one and only one error handler,
//! `Err(e) => { tracing::debug!(…); break; }`, and that handler is shared by BOTH
//! reads, because the user-facing read and the compaction/elements-out read are
//! merely the two arms of the `if compaction_complex_out.is_some()` expression
//! that produces `parse_result`. `break` leaves the column loop, so the failing
//! column AND EVERY LATER ON-DISK COLUMN silently vanish from the row.
//!
//! ## Why it matters (the design consequence)
//! A silently truncated row is more destructive than one wrongly-typed value, so
//! the cell-path key decoder must NOT invent error classes: it returns `Err` only
//! where Cassandra's own `validate`/`split` throws (a wrong fixed width, a
//! non-4/16-byte `inet`, trailing bytes after a composite's components), and
//! returns the opaque `Value::Blob` plus a `warn!` when the only problem is that
//! CQLite cannot model the declared type. See the module header of
//! `row_decoder/complex_column/cell_path_key.rs`.
//!
//! ## How the error is provoked, and why this is honest
//! The committed Cassandra fixture `test_udt_collision.udt_collide` stores `cm`'s
//! key as a 26-byte serialized `collide` UDT. This test hands the reader a schema
//! that declares that same column `map<int, int>` — ONE substitution, applied to
//! the committed `.cql` at run time so the mutation is visible and cannot drift —
//! so the exact-width check refuses the key. That is a genuine, reachable
//! cell-path-key error over real Cassandra-written bytes.
//!
//! NOT covered here, and stated rather than implied: the *unmodellable type*
//! class cannot be reached through a schema-provided public read at all. The CQL
//! layer rejects an undefined UDT (`references undefined UDT`) and rejects a
//! quoted-custom or `vector<…>` map key outright, and the no-schema path resolves
//! the key type from the on-disk marshal form, which decodes. It is covered at
//! unit level in `cell_path_key_tests.rs`.
//!
//! The truncation itself is a PRE-EXISTING defect of row assembly (it swallows
//! every complex-column error, including the width errors that predate #3612),
//! filed as its own follow-up. This test pins the CURRENT behaviour so the
//! follow-up has a red-to-green target and so nobody re-derives the reviewers'
//! disagreement from source.
#![cfg(feature = "cli-helpers")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::platform::Platform;
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::sstable::reader::compaction_row::CompactionRowData;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::types::Value;
use cqlite_core::Config;

const KEYSPACE: &str = "test_udt_collision";
const TABLE: &str = "udt_collide";
const QUERY: &str = "SELECT * FROM test_udt_collision.udt_collide";
const SUBJECT_ROW_ID: i32 = 1;

/// The declared type of `cm` in the committed schema, and the mismatching type
/// this test substitutes for it. `map<int, int>` makes the 26-byte on-disk UDT
/// key fail the exact-width check (`int` is exactly 4 bytes).
const DECLARED_CM: &str = "cm  map<frozen<collide>, int>,";
const MISMATCHED_CM: &str = "cm  map<int, int>,";

/// Columns that appear in the fixture's on-disk cell order AFTER `cm`. The
/// `break` drops these too, which is the property that makes the swallow worse
/// than an opaque value — it is not confined to the offending column.
const COLUMNS_AFTER_CM_ON_DISK: [&str; 1] = ["tm"];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a workspace parent")
        .to_path_buf()
}

fn fixture_root() -> PathBuf {
    workspace_root()
        .join("test-data")
        .join("fixtures")
        .join("issue_3504")
}

fn committed_schema() -> PathBuf {
    workspace_root()
        .join("test-data")
        .join("schemas")
        .join("issue-3504-udt-collision.cql")
}

fn table_dir() -> PathBuf {
    let ks_dir = fixture_root().join(KEYSPACE);
    let mut hits: Vec<PathBuf> = std::fs::read_dir(&ks_dir)
        .unwrap_or_else(|e| panic!("committed fixture keyspace dir unreadable {ks_dir:?}: {e}"))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&format!("{TABLE}-")))
        })
        .collect();
    hits.sort();
    assert_eq!(hits.len(), 1, "expected one {TABLE}-* dir, got {hits:?}");
    hits.remove(0)
}

/// The committed schema with `cm`'s key type replaced, written into `dir`.
///
/// Asserts the substitution actually happened, so a rename in the committed
/// `.cql` makes this test FAIL rather than silently running against the unmodified schema and
/// pass for the wrong reason.
fn mismatched_schema_in(dir: &Path) -> PathBuf {
    let src = committed_schema();
    let text = std::fs::read_to_string(&src)
        .unwrap_or_else(|e| panic!("committed schema unreadable {src:?}: {e}"));
    assert!(
        text.contains(DECLARED_CM),
        "the committed schema no longer declares `{DECLARED_CM}` — update this test \
         rather than letting it run against an unmodified schema"
    );
    let mutated = text.replace(DECLARED_CM, MISMATCHED_CM);
    assert!(
        mutated.contains(MISMATCHED_CM) && !mutated.contains(DECLARED_CM),
        "substitution did not apply"
    );
    let out = dir.join("cm-width-mismatch.cql");
    std::fs::write(&out, mutated).expect("write the mutated schema");
    out
}

async fn select_subject_row(schema: PathBuf) -> BTreeMap<String, Value> {
    let res = ingest(IngestionConfig {
        schema_paths: vec![schema],
        data_dir: fixture_root(),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some(KEYSPACE.to_string()),
    })
    .await
    .expect("ingest must succeed: the mutated schema is still valid CQL");
    let result = res
        .database
        .execute(QUERY)
        .await
        .expect("the SELECT itself must not error — that IS the finding under test");
    assert!(
        !result.rows.is_empty(),
        "zero rows from a PRESENT fixture is a decode failure, never a skip"
    );
    for row in &result.rows {
        if row.values.get("id") == Some(&Value::Integer(SUBJECT_ROW_ID)) {
            return row
                .values
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();
        }
    }
    panic!("fixture row id={SUBJECT_ROW_ID} not found");
}

fn is_absent(v: Option<&Value>) -> bool {
    matches!(v, None | Some(Value::Null))
}

/// RECORDED DEFECT (pre-existing, follow-up filed): a cell-path-key error does
/// NOT fail the `SELECT`. It silently drops the offending column AND every later
/// on-disk column, and the query reports success.
#[tokio::test]
async fn a_cell_path_key_error_silently_truncates_the_row_it_does_not_fail_the_select() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let row = select_subject_row(mismatched_schema_in(tmp.path())).await;

    // (1) The SELECT succeeded — asserted by `select_subject_row`'s `expect`.
    // (2) The offending column is GONE, not wrong.
    assert!(
        is_absent(row.get("cm")),
        "cm should be absent/null after its key failed to decode, got {:?}",
        row.get("cm")
    );
    // (3) THE DAMAGE: later on-disk columns are gone too. This is what makes an
    //     `Err` from the key decoder worse than an opaque value, and it is the
    //     measurement behind the module header's error-budget rule.
    for col in COLUMNS_AFTER_CM_ON_DISK {
        assert!(
            is_absent(row.get(col)),
            "'{col}' follows cm on disk and is dropped by the same `break`, got {:?}",
            row.get(col)
        );
    }
    // (4) The CONTROL: columns decoded BEFORE cm survive, so this really is a
    //     positional truncation and not a whole-row failure.
    for col in ["id", "c", "p", "fcm", "ftm", "fs"] {
        assert!(
            !is_absent(row.get(col)),
            "'{col}' precedes cm on disk and must survive, got {:?}",
            row.get(col)
        );
    }
}

/// The SAME swallow on the COMPACTION read, which is the other arm feeding the
/// one `Err` handler. Asserted through the public
/// `SSTableReader::iterate_all_partitions_for_compaction`.
#[tokio::test]
async fn the_compaction_read_swallows_the_same_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let schema_path = mismatched_schema_in(tmp.path());
    let res = ingest(IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: fixture_root(),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some(KEYSPACE.to_string()),
    })
    .await
    .expect("ingest must succeed");
    let schemas: Vec<TableSchema> = res
        .schema_registry
        .read()
        .await
        .list_schemas(None)
        .await
        .expect("list_schemas");
    let schema = schemas
        .into_iter()
        .find(|s| s.table == TABLE)
        .expect("the mutated schema registers udt_collide");

    let data_path = std::fs::read_dir(table_dir())
        .expect("fixture dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .expect("committed Data.db");
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let reader = SSTableReader::open(&data_path, &config, platform)
        .await
        .expect("open the committed fixture");

    let rows = reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
        .expect("the compaction read must not error either — that is the finding");
    assert!(!rows.is_empty(), "the fixture has partitions");

    // The compaction read reports the row LIVE and simply lacks the dropped
    // columns; it does not surface the decode failure to its caller.
    let mut saw_live = false;
    for row in &rows {
        if let CompactionRowData::Live { simple, .. } = &row.row_data {
            saw_live = true;
            for cell in simple {
                assert_ne!(
                    cell.column, "cm",
                    "cm is dropped by the swallow, so it must not appear as a cell"
                );
            }
        }
    }
    assert!(saw_live, "at least one live row must be produced");
}
