//! Issues #3612 + #3721 — WHAT A CELL-PATH-KEY ERROR DOES AT THE PUBLIC SURFACE,
//! measured rather than reasoned about.
//!
//! Two reviewers disagreed about this and the disagreement decided a design
//! choice, so it is pinned here as an executable fact rather than as prose.
//!
//! ## What this file pinned BEFORE, and why it was inverted
//!
//! #3612 added the corruption checks Cassandra makes when decoding a multicell
//! map's cell-path key (a wrong fixed width, a non-4/16-byte `inet`, trailing
//! bytes after a composite's components). This file was written — mutant-verified
//! — to record that those checks were then UNOBSERVABLE: `parse_cell_path_key`
//! returned `Err`, the `?` propagated it out of `parse_complex_column`, and row
//! assembly SWALLOWED it. The complex-column `match` in `row_decoder/row_data.rs`
//! had one and only one error handler, `Err(e) => { tracing::debug!(…); break; }`,
//! shared by BOTH reads (the user-facing read and the compaction/elements-out read
//! are merely the two arms of the `if compaction_complex_out.is_some()` expression
//! producing `parse_result`). `break` left the column loop, so the failing column
//! AND EVERY LATER ON-DISK COLUMN silently vanished while the `SELECT` reported
//! success. That measurement is #3612's own argument for why #3721 is its
//! prerequisite: a corruption check whose only effect is a silently shorter row is
//! not a corruption check.
//!
//! **#3721 removed the swallow, so this file now pins the OTHER side of the same
//! measurement**: the identical provocation, over the identical Cassandra-written
//! bytes, must now surface as [`cqlite_core::Error::ColumnDecode`] naming the
//! column — on the user-facing read AND on the compaction read. The file is kept,
//! not deleted, because it is the regression oracle for the prerequisite
//! relationship: reintroduce the `break` and these tests go red at the exact
//! assertion that used to be their subject.
//!
//! ## Why it matters (the design consequence, unchanged by the flip)
//! A silently truncated row is more destructive than one wrongly-typed value, so
//! the cell-path key decoder must NOT invent error classes: it returns `Err` only
//! where Cassandra's own `validate`/`split` throws (a wrong fixed width, a
//! non-4/16-byte `inet`, trailing bytes after a composite's components), and
//! returns the opaque `Value::Blob` plus a `warn!` when the only problem is that
//! CQLite cannot model the declared type. See the module header of
//! `row_decoder/complex_column/cell_path_key.rs`. That budget still holds — it now
//! decides which conditions FAIL A READ rather than which ones truncate a row,
//! which raises the stakes on it rather than lowering them.
//!
//! ## How the error is provoked, and why this is honest
//! The Cassandra fixture `test_types.cx_nested_frozen_collections` stores
//! `m_list_vals map<text, frozen<list<int>>>` with the TEXT cell-path keys
//! `evens` (5 bytes) and `odds` (4). This test hands the reader a schema
//! declaring that key `int` — ONE substitution, applied to the committed `.cql` at
//! run time so the mutation is visible and cannot drift — so `evens` fails the
//! exact-width check (`Int32Serializer.validate`: 4 bytes or empty). That is a
//! genuine, reachable cell-path-key error over real Cassandra-written bytes.
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
//! ## What keeps the flipped test mutant-verified
//! An `is_err()` assertion is satisfied by a mutation that breaks every read, so
//! three properties are asserted instead of one, and a CONTROL runs beside them:
//!
//! 1. the error is the dedicated MATCHABLE variant `Error::ColumnDecode`, naming
//!    `m_list_vals` — not any error, and not a message-text test (issue #28);
//! 2. its `column_type` names BOTH the dispatch type the failed decode ran on
//!    (`map<int, …>`, the mis-declared schema) and the on-disk header marshal type
//!    (`map<text, …>`), so the report identifies the declaration that is wrong;
//! 3. its `source` is #3612's own width check, preserved rather than flattened;
//! 4. the CONTROL — the SAME read against the UNMUTATED committed schema returns
//!    every column of the row, `m_list_vals` included. Without it, "the read
//!    fails" would be satisfiable by a decoder that fails unconditionally.
//!
//! Its on-disk cell order — measured from the golden: `l_set_vals`,
//! `m_list_vals`, `s_map_vals` — is what made the OLD assertions positional (a
//! surviving predecessor, vanished successors). Post-fix there is no partial row
//! to inspect, so the predecessor/successor split survives only in the control,
//! where all three must be present.
//!
//! NOT covered here, and stated rather than implied: the *unmodellable type*
//! class cannot be reached through a schema-provided public read at all. The CQL
//! layer rejects an undefined UDT (`references undefined UDT`) and rejects a
//! quoted-custom or `vector<…>` map key outright, and the no-schema path resolves
//! the key type from the on-disk marshal form, which decodes. It is covered at
//! unit level in `cell_path_key_tests.rs`.
#![cfg(feature = "cli-helpers")]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::error::ErrorCategory;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::platform::Platform;
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::sstable::reader::compaction_row::CompactionRowData;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::types::Value;
use cqlite_core::{Config, Error};

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

/// The fixture's complex columns in on-disk cell order, measured from the golden.
/// The `break` used to drop the subject and everything after it; the CONTROL now
/// requires ALL THREE to be present when the schema is correct, which is what
/// keeps "the read fails" from being satisfiable by a decoder that always fails.
const COMPLEX_COLUMNS_ON_DISK: [&str; 3] = ["l_set_vals", "m_list_vals", "s_map_vals"];
/// Decoded BEFORE the subject on disk. Under the swallow it was the surviving
/// predecessor; it is retained as the compaction control's anchor for finding the
/// row that carries the subject.
const COLUMN_BEFORE_SUBJECT_ON_DISK: &str = "l_set_vals";
/// The subject column, whose mis-declared key type provokes #3612's width check.
const SUBJECT_COLUMN: &str = "m_list_vals";
/// A stable fragment of the check #3612 added, read from
/// `cell_path_key.rs`'s fixed-width table (authority
/// `git show cassandra-5.0.8:src/java/org/apache/cassandra/serializers/Int32Serializer.java`,
/// `validate`: 4 bytes or empty). Asserted on the `source`, never on the
/// top-level message, so the CAUSE is shown to survive rather than be flattened.
const WIDTH_CHECK_FRAGMENT: &str = "requires exactly";

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

async fn read_row(schema: PathBuf) -> Result<BTreeMap<String, Value>, Error> {
    let res = ingest(IngestionConfig {
        schema_paths: vec![schema],
        data_dir: fixture_root(),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some(KEYSPACE.to_string()),
    })
    .await
    .expect("ingest must succeed: the mutated schema is still valid CQL");
    let result = res.database.execute(QUERY).await?;
    assert!(
        !result.rows.is_empty(),
        "zero rows from a PRESENT fixture is a decode failure, never a skip"
    );
    for row in &result.rows {
        if row.values.get("pk") == Some(&Value::Integer(SUBJECT_ROW_ID)) {
            return Ok(row
                .values
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect());
        }
    }
    panic!("fixture row pk={SUBJECT_ROW_ID} not found");
}

/// Assert `err` is the dedicated, MATCHABLE per-column variant #3721 introduced,
/// carrying every field the report needs. A message-text test would be exactly the
/// string-pattern inference issue #28 forbids, so the variant is matched and its
/// fields are read structurally.
///
/// The `on_disk` half is what identifies the MIS-DECLARATION as the cause: the
/// dispatch ran on the supplied `map<int, …>` while the header describes the real
/// `map<text, …>` bytes, and only naming both points a reader at the declaration.
fn assert_is_the_cell_path_key_failure(err: &Error) {
    let Error::ColumnDecode {
        column,
        column_type,
        source,
        ..
    } = err
    else {
        panic!(
            "the cell-path-key width check must surface as Error::ColumnDecode, not \
             be swallowed into a short row (issues #3612/#3721); got: {err:?}"
        );
    };
    assert_eq!(
        column, SUBJECT_COLUMN,
        "the error must NAME the failing column"
    );
    for expected in ["map<int", "map<text"] {
        assert!(
            column_type.contains(expected),
            "column_type must name BOTH the dispatch type the decode ran on and the \
             on-disk header marshal type (missing `{expected}`); got `{column_type}`"
        );
    }
    let cause = source.to_string();
    assert!(
        cause.contains(WIDTH_CHECK_FRAGMENT),
        "the preserved `source` must be #3612's own fixed-width check, so the cause \
         is not flattened away; got `{cause}`"
    );
    assert_eq!(err.category(), ErrorCategory::Data);
    assert!(!err.is_recoverable());
}

fn is_absent(v: Option<&Value>) -> bool {
    matches!(v, None | Some(Value::Null))
}

/// THE FLIP (issues #3612 + #3721). A cell-path-key error now FAILS the `SELECT`
/// with a named [`Error::ColumnDecode`]. It used to drop the offending column and
/// every later on-disk column while reporting success.
#[tokio::test]
async fn a_cell_path_key_error_fails_the_select_it_does_not_truncate_the_row() {
    let tmp = tempfile::tempdir().expect("tempdir");
    match read_row(mismatched_schema_in(tmp.path())).await {
        Err(e) => assert_is_the_cell_path_key_failure(&e),
        Ok(row) => {
            // Diagnose the swallow explicitly rather than reporting a bare
            // "expected Err": the shape below IS the defect #3721 removed, and a
            // reader of a future failure should not have to re-derive it.
            let present: Vec<&str> = COMPLEX_COLUMNS_ON_DISK
                .iter()
                .copied()
                .filter(|c| !is_absent(row.get(*c)))
                .collect();
            panic!(
                "the SELECT reported success after the cell-path key failed its width \
                 check — the swallow is back (issue #3721). Complex columns present: \
                 {present:?}; expected an Error::ColumnDecode naming '{SUBJECT_COLUMN}'"
            );
        }
    }
}

/// THE CONTROL. The identical read against the UNMUTATED committed schema returns
/// every complex column of the row — so the failure above is attributable to the
/// ONE substituted key type, and not to a decoder that fails unconditionally.
#[tokio::test]
async fn the_unmutated_schema_reads_every_complex_column_of_the_same_row() {
    let row = read_row(committed_schema())
        .await
        .expect("the committed schema decodes the fixture");
    for col in COMPLEX_COLUMNS_ON_DISK {
        assert!(
            !is_absent(row.get(col)),
            "'{col}' must be present under the correct schema, got {:?}",
            row.get(col)
        );
    }
}

/// The SAME error on the COMPACTION read, which is the other arm feeding the one
/// `Err` handler `row_data.rs` used to share. Asserted through the public
/// `SSTableReader::iterate_all_partitions_for_compaction`, which now returns the
/// failure to its caller instead of yielding partition rows with columns missing.
#[tokio::test]
async fn the_compaction_read_surfaces_the_same_error() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let schema = registered_schema(mismatched_schema_in(tmp.path())).await;
    let reader = open_fixture_reader().await;

    match reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
    {
        Err(e) => assert_is_the_cell_path_key_failure(&e),
        Ok(rows) => panic!(
            "the compaction read reported success over {} partition rows after the \
             cell-path key failed its width check — the swallow is back (issue #3721)",
            rows.len()
        ),
    }
}

/// The compaction CONTROL, matching the read one: under the UNMUTATED schema the
/// same call yields the row with all three complex columns, so the failure above
/// is attributable to the substituted key type alone.
#[tokio::test]
async fn the_compaction_read_returns_every_complex_column_under_the_correct_schema() {
    let schema = registered_schema(committed_schema()).await;
    let reader = open_fixture_reader().await;

    let rows = reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
        .expect("the committed schema decodes the fixture");
    assert!(!rows.is_empty(), "the fixture has partitions");

    let mut checked_rows = 0usize;
    for row in &rows {
        if let CompactionRowData::Live { complex, .. } = &row.row_data {
            let names: Vec<&str> = complex.iter().map(|c| c.column.as_str()).collect();
            if !names.contains(&COLUMN_BEFORE_SUBJECT_ON_DISK) {
                continue;
            }
            checked_rows += 1;
            for col in COMPLEX_COLUMNS_ON_DISK {
                assert!(
                    names.contains(&col),
                    "'{col}' must be present under the correct schema; got {names:?}"
                );
            }
        }
    }
    assert_eq!(
        checked_rows, 1,
        "exactly one fixture row carries the subject's neighbours; if this is 0 the \
         assertions above ran against nothing"
    );
}

/// Ingest `schema_path` and hand back the registered `TableSchema` for the fixture
/// table — the shape `iterate_all_partitions_for_compaction` takes.
async fn registered_schema(schema_path: PathBuf) -> TableSchema {
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
    schemas
        .into_iter()
        .find(|s| s.table == TABLE)
        .expect("the schema registers the fixture table")
}

async fn open_fixture_reader() -> SSTableReader {
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
    SSTableReader::open(&data_path, &config, platform)
        .await
        .expect("open the committed fixture")
}
