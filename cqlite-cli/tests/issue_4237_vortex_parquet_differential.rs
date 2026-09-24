//! Cross-format differential: Vortex vs Parquet (issue #4237, R9 — the PRIMARY oracle).
//!
//! For each of the 33 committed fixture tables (`test_basic` 8, `test_collections` 8,
//! `test_timeseries` 9, `test_wide_rows` 8), the SAME `SELECT *` is exported to both formats
//! from the compiled CLI, both are read back to Arrow with their OWN readers (`parquet` crate /
//! `vortex` crate), and compared on schema (full column set, both directions — #3890) and every
//! value (order-sensitive — design.md's row/field-order contract, R3 in `export-vortex`).
//!
//! Parquet is already golden-proven against `sstabledump` JSONL elsewhere
//! (`parquet_dataset_roundtrip_tests.rs`, `issue_1490_parquet_jsonl_parity.rs`); this test anchors
//! Vortex to that same Cassandra-written truth TRANSITIVELY, never by a Vortex-write-then-read
//! round-trip alone (#3042).
//!
//! `required-features = ["state_machine", "vortex"]` (Cargo.toml) — `state_machine` already
//! forwards `cqlite-core/parquet`, so this pair gives both writers. No other gate component
//! enables that combination today, so a NEW component runs this file (design.md D3,
//! `vortex-parquet-differential`, owner Seam-1 ruling 2026-09-23: slim gate wiring).
//! `CQLITE_REQUIRE_FIXTURES=1` turns a missing dataset root / table into a hard failure; without
//! it, a missing corpus is a clean skip (ordinary local dev).
//!
//! Two tables are QUARANTINED (`QUARANTINED_TABLES`), both the SAME pre-existing,
//! format-independent row-decoder gap this test discovered (issue #4279) — see that constant's
//! doc comment. 31 of the 33 committed fixture tables are swept unconditionally.

#![cfg(all(feature = "state_machine", feature = "vortex"))]

use arrow::array::Array;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::BTreeSet;
use std::error::Error as StdError;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

// ============================================================================
// Fixture discovery + fail-closed dataset-root resolution
// ============================================================================

/// `CQLITE_REQUIRE_FIXTURES=1` turns a missing dataset root / table into a hard failure (the
/// strict lane that runs this differential as the merge-gating oracle); otherwise a missing
/// corpus is a clean skip (ordinary local dev without the gitignored fixtures). Mirrors the
/// convention in `cqlite-core/tests/issue_1104_compaction_incompressible_chunks.rs` and siblings.
fn require_fixtures_strict() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn datasets_root() -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("test-data/datasets")
        });
    if root.join("sstables").exists() {
        Some(root)
    } else {
        None
    }
}

fn schemas_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("test-data/schemas")
}

/// The 4 fixture keyspaces and the committed schema file each resolves through (#3131/#3148: the
/// schema fixtures are committed source, resolved checkout-relative, never derived from
/// `CQLITE_DATASETS_ROOT`). Table names are discovered by walking each keyspace directory rather
/// than hand-typed (mirrors `test-data/scripts/smoke-test-all-tables.sh`'s discovery convention),
/// so a newly-added table in one of these 4 keyspaces is automatically swept.
const KEYSPACE_SCHEMAS: &[(&str, &str)] = &[
    ("test_basic", "basic-types.cql"),
    ("test_collections", "collections.cql"),
    ("test_timeseries", "time-series.cql"),
    ("test_wide_rows", "wide-rows.cql"),
];

struct FixtureTable {
    keyspace: &'static str,
    table: String,
    schema: PathBuf,
}

/// QUARANTINE (issue #4279, discovered by this test): the `V5CompressedLegacy` row decoder
/// (`row_decoder/row_framing.rs`) has explicit fixed-width clustering-column decode arms only
/// for `int`/`uuid`/`bigint`/`counter`, falling through to a generic "treat as blob" branch for
/// EVERY other CQL type. Both tables below trip that same fallback, on two different clustering
/// types (`DATE`, `FROZEN<LIST<TEXT>>`): `test_wide_rows.wide_partition_table`
/// (`clustering_col5 DATE`) and `test_collections.collection_clustering_table`
/// (`clustering_key FROZEN<LIST<TEXT>>`).
///
/// This is FORMAT-INDEPENDENT (verified against plain `--out json`) and pre-existing — NOT a
/// Vortex-specific defect: the Parquet writer hits the identical `expected <T> value, got
/// Blob(...)` failure at the SAME shared `rows_to_record_batch_with_schema` step Vortex uses
/// (design.md D1), before any Vortex-specific code runs. Quarantined here (not silently dropped —
/// named, with the citing issue) until #4279 lands; every OTHER fixture table is swept normally.
const QUARANTINED_TABLES: &[(&str, &str)] = &[
    ("test_wide_rows", "wide_partition_table"),
    ("test_collections", "collection_clustering_table"),
];

fn is_quarantined(table: &FixtureTable) -> bool {
    QUARANTINED_TABLES
        .iter()
        .any(|(ks, t)| *ks == table.keyspace && *t == table.table)
}

/// Discover every fixture table (issue #3220: resolve fixture roots per TABLE, assert per CASE —
/// a keyspace-level "the root has SOME tables" check cannot see one table's Data.db missing
/// behind its siblings).
fn discover_fixture_tables(datasets_root: &Path) -> Vec<FixtureTable> {
    let mut tables = Vec::new();
    for (keyspace, schema_file) in KEYSPACE_SCHEMAS {
        let schema = schemas_dir().join(schema_file);
        let keyspace_dir = datasets_root.join("sstables").join(keyspace);
        let Ok(entries) = std::fs::read_dir(&keyspace_dir) else {
            continue;
        };
        let mut dirs: Vec<_> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .collect();
        dirs.sort_by_key(|e| e.file_name());
        for entry in dirs {
            let dir_name = entry.file_name().to_string_lossy().into_owned();
            // Directory shape is `<table>-<32-hex-char generation id>`; table names in these
            // fixture schemas are snake_case with no dashes, so splitting on the FIRST '-'
            // recovers the table name exactly (verified against `basic-types.cql` et al.).
            let table = dir_name
                .split_once('-')
                .map(|(t, _)| t.to_string())
                .unwrap_or(dir_name);
            tables.push(FixtureTable {
                keyspace,
                table,
                schema: schema.clone(),
            });
        }
    }
    tables
}

/// A table dir with at least one `*-Data.db` file — a discovered-but-Data.db-absent table is a
/// SEPARATE, fail-closed-under-strict case (see `differential_all_fixture_tables_agree`), never
/// silently dropped from the discovered set.
fn table_dir_has_data_db(datasets_root: &Path, table: &FixtureTable) -> Option<PathBuf> {
    let keyspace_dir = datasets_root.join("sstables").join(table.keyspace);
    let entries = std::fs::read_dir(&keyspace_dir).ok()?;
    for entry in entries.filter_map(|e| e.ok()) {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(&format!("{}-", table.table)) && entry.path().is_dir() {
            let has_data_db = std::fs::read_dir(entry.path())
                .ok()?
                .filter_map(|e| e.ok())
                .any(|e| e.file_name().to_string_lossy().ends_with("-Data.db"));
            if has_data_db {
                return Some(entry.path());
            }
        }
    }
    None
}

// ============================================================================
// CLI invocation + readers
// ============================================================================

fn run_cli(args: &[&str]) -> (String, String, bool) {
    let output = Command::new(env!("CARGO_BIN_EXE_cqlite"))
        .args(args)
        .output()
        .expect("failed to spawn cqlite");
    (
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
        output.status.success(),
    )
}

/// Returns the file-level schema (from the Parquet FOOTER, always present regardless of row
/// count — issue #4237 test-harness fix: deriving the column set from `batches.first()` reads
/// `None` on a genuinely 0-row table and misreports an empty column set as a real divergence)
/// plus every row-group batch.
fn read_parquet_file(
    path: &Path,
) -> Result<(std::sync::Arc<ArrowSchema>, Vec<RecordBatch>), Box<dyn StdError>> {
    let bytes = std::fs::read(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
    let schema = builder.schema().clone();
    let reader = builder.build()?;
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Box::new(e) as Box<dyn StdError>)?;
    Ok((schema, batches))
}

/// Read a `.vortex` file back to ONE Arrow `RecordBatch` via Vortex's own reader — the format's
/// own official Arrow bridge, exactly as `read_parquet_file` above uses the `parquet` crate's own
/// reader. `vortex::file::Writer`/`Reader` are natively async, so this helper is too; the caller
/// drives it on a throwaway current-thread runtime (test code only — the production writer in
/// `cqlite-core::export::vortex` uses `tokio` because it is called from the CLI's own async
/// context; this test binary has no ambient runtime, so it builds its own).
async fn read_vortex_file_async(path: &Path) -> Result<RecordBatch, Box<dyn StdError>> {
    use vortex::array::stream::ArrayStreamExt;
    use vortex::array::VortexSessionExecute;
    use vortex::arrow::ArrowSessionExt;
    use vortex::file::OpenOptionsSessionExt;
    use vortex::session::VortexSession;
    use vortex::VortexSessionDefault;

    let session = VortexSession::default();
    let file = session.open_options().open_path(path.to_path_buf()).await?;
    let array = file.scan()?.into_array_stream()?.read_all().await?;

    let mut ctx = session.create_execution_ctx();
    let arrow_array = session.arrow().execute_arrow(array, None, &mut ctx)?;
    let struct_array = arrow_array
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .ok_or("Vortex read-back did not yield a top-level struct array")?;
    Ok(RecordBatch::from(struct_array.clone()))
}

fn read_vortex_file(path: &Path) -> Result<RecordBatch, Box<dyn StdError>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(read_vortex_file_async(path))
}

// ============================================================================
// Comparison — full column set both directions (#3890), order-sensitive values
// ============================================================================

fn column_name_set(schema: &ArrowSchema) -> BTreeSet<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

/// Every value in `column`, across all `batches`, as its `array_value_to_string` rendering —
/// value-level (not physical-encoding-level) comparison, so a schema-legal re-encoding (e.g.
/// dictionary vs plain, or a different Vortex/Parquet chunk boundary — design.md D2) between the
/// two formats' read-backs is not mistaken for a value divergence.
fn column_values_as_strings(batches: &[RecordBatch], column: &str) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let Ok(idx) = batch.schema().index_of(column) else {
            continue;
        };
        let col = batch.column(idx);
        for row in 0..batch.num_rows() {
            out.push(
                array_value_to_string(col.as_ref(), row)
                    .unwrap_or_else(|e| format!("<display-error: {e}>")),
            );
        }
    }
    out
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Run the differential for one table. Returns `Ok(row_count)` or a descriptive `Err` naming the
/// table and the exact divergence (column-set direction, row count, or the first mismatching
/// column + row).
fn diff_one_table(datasets_root: &Path, table: &FixtureTable) -> Result<usize, Box<dyn StdError>> {
    let sstables = datasets_root.join("sstables");
    let qualified = format!("{}.{}", table.keyspace, table.table);
    let tmp = TempDir::new()?;
    let parquet_path = tmp.path().join("out.parquet");
    let vortex_path = tmp.path().join("out.vortex");

    let (_, stderr, ok) = run_cli(&[
        "--schema",
        table.schema.to_str().unwrap(),
        "--data-dir",
        sstables.to_str().unwrap(),
        "export",
        parquet_path.to_str().unwrap(),
        "--format",
        "parquet",
        "--table",
        &qualified,
    ]);
    if !ok {
        return Err(format!("{qualified}: parquet export failed: {stderr}").into());
    }

    let (_, stderr, ok) = run_cli(&[
        "--schema",
        table.schema.to_str().unwrap(),
        "--data-dir",
        sstables.to_str().unwrap(),
        "export",
        vortex_path.to_str().unwrap(),
        "--format",
        "vortex",
        "--table",
        &qualified,
    ]);
    if !ok {
        return Err(format!("{qualified}: vortex export failed: {stderr}").into());
    }

    let (parquet_schema, parquet_batches) = read_parquet_file(&parquet_path)
        .map_err(|e| format!("{qualified}: failed to read back parquet: {e}"))?;
    let vortex_batch = read_vortex_file(&vortex_path)
        .map_err(|e| format!("{qualified}: failed to read back vortex: {e}"))?;
    let vortex_schema = vortex_batch.schema();
    let vortex_batches = [vortex_batch];

    let parquet_row_count = total_rows(&parquet_batches);
    let vortex_row_count = total_rows(&vortex_batches);
    if parquet_row_count != vortex_row_count {
        return Err(format!(
            "{qualified}: row count diverges — parquet {parquet_row_count}, vortex {vortex_row_count}"
        )
        .into());
    }

    // Derived from the FILE-LEVEL schema (present even for a genuinely 0-row table), not
    // `batches.first()` — see `read_parquet_file`'s doc comment.
    let parquet_cols = column_name_set(&parquet_schema);
    let vortex_cols = column_name_set(&vortex_schema);

    let parquet_only: Vec<_> = parquet_cols.difference(&vortex_cols).cloned().collect();
    let vortex_only: Vec<_> = vortex_cols.difference(&parquet_cols).cloned().collect();
    if !parquet_only.is_empty() || !vortex_only.is_empty() {
        return Err(format!(
            "{qualified}: column set diverges — only in parquet: {parquet_only:?}, only in vortex: {vortex_only:?}"
        )
        .into());
    }

    for column in &parquet_cols {
        let p_values = column_values_as_strings(&parquet_batches, column);
        let v_values = column_values_as_strings(&vortex_batches, column);
        if p_values != v_values {
            let first_mismatch = p_values
                .iter()
                .zip(v_values.iter())
                .position(|(p, v)| p != v);
            return Err(format!(
                "{qualified}.{column}: value diverges at row {:?} (parquet={:?}, vortex={:?})",
                first_mismatch,
                first_mismatch.map(|i| &p_values[i]),
                first_mismatch.map(|i| &v_values[i]),
            )
            .into());
        }
    }

    Ok(parquet_row_count)
}

// ============================================================================
// Tests
// ============================================================================

/// R9.1 — every committed fixture table agrees, full column set both directions, no per-case
/// skip. Fails closed on a missing dataset root / table under `CQLITE_REQUIRE_FIXTURES=1`.
#[test]
fn differential_all_fixture_tables_agree() {
    let strict = require_fixtures_strict();

    let Some(root) = datasets_root() else {
        assert!(
            !strict,
            "CQLITE_REQUIRE_FIXTURES=1 but CQLITE_DATASETS_ROOT is unset — \
             fetch with bash test-data/scripts/fetch-datasets.sh"
        );
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping differential");
        return;
    };

    let tables = discover_fixture_tables(&root);
    assert!(
        !tables.is_empty(),
        "discovered zero fixture tables under {} — keyspace directories missing?",
        root.join("sstables").display()
    );

    let mut swept = 0usize;
    let mut total_rows_compared = 0usize;
    let mut failures: Vec<String> = Vec::new();

    let mut quarantined = 0usize;
    for table in &tables {
        let qualified = format!("{}.{}", table.keyspace, table.table);
        if is_quarantined(table) {
            quarantined += 1;
            eprintln!("{qualified}: QUARANTINED (issue #4279), skipping");
            continue;
        }
        let Some(_data_db_dir) = table_dir_has_data_db(&root, table) else {
            assert!(
                !strict,
                "CQLITE_REQUIRE_FIXTURES=1 but {qualified} has no Data.db under {} — \
                 dropped table or partial dataset? fetch with bash test-data/scripts/fetch-datasets.sh",
                root.join("sstables").join(table.keyspace).display()
            );
            eprintln!("{qualified}: no Data.db present, skipping (non-strict)");
            continue;
        };

        match diff_one_table(&root, table) {
            Ok(rows) => {
                swept += 1;
                total_rows_compared += rows;
                eprintln!("{qualified}: OK ({rows} rows, both formats agree)");
            }
            Err(e) => {
                failures.push(e.to_string());
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} fixture tables diverged between Parquet and Vortex:\n{}",
        failures.len(),
        tables.len(),
        failures.join("\n")
    );
    assert!(
        swept > 0,
        "zero fixture tables were actually swept (all had missing Data.db) — \
         CQLITE_DATASETS_ROOT points at a corpus-less root"
    );
    assert_eq!(
        quarantined,
        QUARANTINED_TABLES.len(),
        "quarantined-table count drifted from QUARANTINED_TABLES — a table was added/removed \
         from the corpus without updating this list"
    );
    eprintln!(
        "vortex-parquet differential: {swept}/{} tables swept ({quarantined} quarantined, issue #4279), \
         {total_rows_compared} total rows compared, all agree",
        tables.len()
    );
}

/// R2.1 (CLI-level confirmation): a UUID column's Vortex read-back matches Parquet's exactly —
/// `test_basic.simple_table` carries a `uuid` primary-key column (`basic-types.cql`).
#[test]
fn differential_uuid_column_matches() {
    let strict = require_fixtures_strict();
    let Some(root) = datasets_root() else {
        assert!(
            !strict,
            "CQLITE_REQUIRE_FIXTURES=1 but CQLITE_DATASETS_ROOT is unset"
        );
        eprintln!("CQLITE_DATASETS_ROOT not set, skipping");
        return;
    };

    let table = FixtureTable {
        keyspace: "test_basic",
        table: "simple_table".to_string(),
        schema: schemas_dir().join("basic-types.cql"),
    };
    if table_dir_has_data_db(&root, &table).is_none() {
        assert!(
            !strict,
            "CQLITE_REQUIRE_FIXTURES=1 but test_basic.simple_table has no Data.db"
        );
        eprintln!("test_basic.simple_table: no Data.db present, skipping");
        return;
    }

    match diff_one_table(&root, &table) {
        Ok(rows) => assert!(rows > 0, "simple_table exported 0 rows"),
        Err(e) => panic!("{e}"),
    }
}
