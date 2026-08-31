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
//! The Cassandra fixture `test_types.cx_nested_frozen_collections` stores
//! `m_list_vals map<text, frozen<list<int>>>` with the TEXT cell-path keys
//! `evens` (5 bytes) and `odds` (4). This test hands the reader a schema
//! declaring that key `int` — ONE substitution, applied to the committed `.cql` at
//! run time so the mutation is visible and cannot drift — so `evens` fails the
//! exact-width check. That is a genuine, reachable cell-path-key error over real
//! Cassandra-written bytes.
//!
//! THE SUBJECT MOVED, AND WHY IT HAD TO (issue #3612, R7). This test originally
//! provoked the error on `test_udt_collision.udt_collide`'s `cm` by declaring its
//! `frozen<collide>` key `int`. R7 made the multicell branch prefer the
//! AUTHORITATIVE MARSHAL key type over the schema, so for a UDT-keyed map a
//! mismatched schema is now correctly IGNORED and the key decodes fine —
//! desirable (authoritative metadata wins, issue #28), and it silently DISARMED
//! this test: both cases went green while asserting nothing, caught only because
//! the gate ran them. The provocation therefore has to be a column whose marshal
//! key type is NOT UDT-bearing, so `prefer_udt_marshal_element` keeps the schema
//! form. `m_list_vals`'s marshal key is `UTF8Type`, which qualifies.
//!
//! Its on-disk cell order — measured from the golden: `l_set_vals`,
//! `m_list_vals`, `s_map_vals` — also gives all three roles in ONE row, so the
//! control that a column decoded BEFORE the subject SURVIVES is now an in-vector
//! assertion on `complex` rather than an indirect one on `simple`.
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

const KEYSPACE: &str = "test_types";
const TABLE: &str = "cx_nested_frozen_collections";
const QUERY: &str = "SELECT * FROM test_types.cx_nested_frozen_collections";
const SUBJECT_ROW_ID: i32 = 1;

/// The declared type of the subject column, and the mismatching type this test
/// substitutes for it. The on-disk cell-path keys are the TEXT strings `evens`
/// (5 bytes) and `odds` (4), so declaring the key `int` makes `evens` fail the
/// exact-width check.
///
/// WHY THIS COLUMN AND NOT `cm` (issue #3612, R7): the provocation must be a
/// column whose AUTHORITATIVE MARSHAL key type is not UDT-bearing. R7 made the
/// multicell branch prefer the marshal spelling over the schema, so for a
/// UDT-keyed map like `cm` a mismatched schema is now correctly IGNORED and the
/// key decodes fine — a desirable consequence (authoritative metadata wins,
/// issue #28) that silently disarmed this test's original provocation. This
/// column's marshal key is `UTF8Type`, so `prefer_udt_marshal_element` keeps the
/// schema form and the mismatch still bites.
const DECLARED_CM: &str = "m_list_vals map<text, frozen<list<int>>>,";
const MISMATCHED_CM: &str = "m_list_vals map<int, frozen<list<int>>>,";

/// Columns in the fixture's on-disk cell order AFTER the subject. The `break`
/// drops these too — the property that makes the swallow worse than an opaque
/// value, since it is not confined to the offending column. Measured from the
/// golden: `l_set_vals`, `m_list_vals`, `s_map_vals`.
const COLUMNS_AFTER_CM_ON_DISK: [&str; 1] = ["s_map_vals"];
/// Decoded BEFORE the subject, so it must SURVIVE — the control proving this is a
/// positional truncation and not a whole-row failure.
const COLUMNS_BEFORE_CM_ON_DISK: [&str; 1] = ["l_set_vals"];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a workspace parent")
        .to_path_buf()
}

/// Resolved PER TABLE (issue #3220): neither the env root nor the checkout is a
/// superset of the other, so a fixed root can silently yield zero rows.
fn fixture_root() -> PathBuf {
    datasets_root::sstables_root_for_table(KEYSPACE, TABLE).unwrap_or_else(|| {
        panic!("no candidate root holds {KEYSPACE}.{TABLE}; fetch the datasets corpus")
    })
}

#[path = "support/datasets_root.rs"]
mod datasets_root;

fn committed_schema() -> PathBuf {
    workspace_root()
        .join("test-data")
        .join("schemas")
        .join("cql-type-parity.cql")
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
        if row.values.get("pk") == Some(&Value::Integer(SUBJECT_ROW_ID)) {
            return row
                .values
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();
        }
    }
    panic!("fixture row pk={SUBJECT_ROW_ID} not found");
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
        is_absent(row.get("m_list_vals")),
        "cm should be absent/null after its key failed to decode, got {:?}",
        row.get("m_list_vals")
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
    for col in COLUMNS_BEFORE_CM_ON_DISK {
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

    // The property, asserted where it is OBSERVABLE. `m_list_vals` is a COMPLEX
    // column, so it can only ever appear in `complex` — asserting its absence from
    // `simple` would hold no matter what the decoder did (a vacuous assertion this
    // test shipped with for one round, caught by a mutant check).
    //
    // Here the IN-VECTOR control is available and is used: `l_set_vals` is decoded
    // BEFORE the subject on disk, so it must be PRESENT in `complex` while the
    // subject and `s_map_vals` after it are gone. That is a strictly better control
    // than the previous fixture allowed, where every non-subject collection column
    // was frozen and so landed in `simple`.
    let mut checked_rows = 0usize;
    for row in &rows {
        if let CompactionRowData::Live {
            simple, complex, ..
        } = &row.row_data
        {
            let _ = simple;
            let complex_pre: Vec<&str> = complex.iter().map(|c| c.column.as_str()).collect();
            if !complex_pre.contains(&COLUMNS_BEFORE_CM_ON_DISK[0]) {
                // Not a row carrying the subject's neighbours.
                continue;
            }
            checked_rows += 1;
            // CONTROL: the column decoded BEFORE the subject is present, so the
            // read really happened and the row is not simply empty.
            let complex_names = complex_pre;
            assert!(
                !complex_names.contains(&"m_list_vals"),
                "m_list_vals's key failed to decode, so the swallow drops it from \
                 the compaction read's complex columns; got {complex_names:?}"
            );
            for later in COLUMNS_AFTER_CM_ON_DISK {
                assert!(
                    !complex_names.contains(&later),
                    "'{later}' follows the subject on disk and is dropped by the \
                     same `break`; got {complex_names:?}"
                );
            }
        }
    }
    assert_eq!(
        checked_rows, 1,
        "exactly one fixture row carries the subject's neighbours; if this is 0 \
         the assertions above ran against nothing"
    );
}
