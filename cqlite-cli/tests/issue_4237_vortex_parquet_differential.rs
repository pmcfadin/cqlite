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

/// Tables where `SELECT *` legitimately returns ZERO rows against the committed fixture —
/// verified against a real run, not assumed. `test_basic.ttl_test_table` writes rows with a
/// short TTL that has expired relative to the fixture's pinned `now` (issue #3220's oracle
/// class), so an empty result here is the CORRECT reconciled view, not a decode failure.
/// Anything NOT in this list that returns 0 rows is treated as a possible regression
/// (`differential_all_fixture_tables_agree`'s per-table check).
const ZERO_ROW_ALLOWED_TABLES: &[(&str, &str)] = &[("test_basic", "ttl_test_table")];

fn is_zero_row_allowed(table: &FixtureTable) -> bool {
    ZERO_ROW_ALLOWED_TABLES
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
            // Null validity is compared explicitly: `array_value_to_string` renders a null and an
            // empty value alike, so a null must never be able to pass for "".
            if col.is_null(row) {
                out.push("\u{0}<NULL>".to_string());
                continue;
            }
            out.push(
                array_value_to_string(col.as_ref(), row)
                    .unwrap_or_else(|e| format!("<display-error: {e}>")),
            );
        }
    }
    out
}

/// The LOGICAL Arrow type of a field: physical encodings either format may legitimately pick
/// (dictionary vs plain; view / large / small offset widths) are collapsed, everything that
/// carries meaning (the primitive kind, temporal unit, decimal precision/scale, nested shape,
/// child nullability) is kept. Compared per field so that e.g. `Int64` vs `Utf8` reds.
fn logical_type(dt: &arrow::datatypes::DataType) -> String {
    use arrow::datatypes::DataType as D;
    match dt {
        D::Dictionary(_, value) => logical_type(value),
        D::Utf8 | D::LargeUtf8 | D::Utf8View => "Utf8".to_string(),
        D::Binary | D::LargeBinary | D::BinaryView => "Binary".to_string(),
        D::List(f) | D::LargeList(f) | D::ListView(f) | D::LargeListView(f) => {
            format!("List<{}>", logical_field(f))
        }
        D::FixedSizeList(f, n) => format!("FixedSizeList<{}; {n}>", logical_field(f)),
        D::Struct(fields) => format!(
            "Struct<{}>",
            fields
                .iter()
                .map(|f| logical_field(f))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        D::Map(f, sorted) => format!("Map<{}; sorted={sorted}>", logical_field(f)),
        other => format!("{other:?}"),
    }
}

/// Name, logical type, nullability and Arrow extension name of a (possibly nested) field.
fn logical_field(f: &arrow::datatypes::Field) -> String {
    let ext = f
        .metadata()
        .get("ARROW:extension:name")
        .map(|e| format!(" ext={e}"))
        .unwrap_or_default();
    format!(
        "{}: {}{}{}",
        f.name(),
        logical_type(f.data_type()),
        if f.is_nullable() { "?" } else { "" },
        ext
    )
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
    // Same set, so now the ORDER: a Vortex export with reordered columns must not pass.
    let parquet_order: Vec<&str> = parquet_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let vortex_order: Vec<&str> = vortex_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    if parquet_order != vortex_order {
        return Err(format!(
            "{qualified}: column order diverges — parquet: {parquet_order:?}, vortex: {vortex_order:?}"
        )
        .into());
    }

    // Same names in the same order, so now each field's LOGICAL type, nullability and
    // extension metadata (roborev #4237 r6): string-rendered values alone would let
    // `Int64(1)` vs `Utf8("1")` pass.
    for (pf, vf) in parquet_schema
        .fields()
        .iter()
        .zip(vortex_schema.fields().iter())
    {
        let (p, v) = (logical_field(pf), logical_field(vf));
        if p != v {
            return Err(
                format!("{qualified}: field type diverges — parquet {p}, vortex {v}").into(),
            );
        }
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
                // #4237 review finding (Medium, both rust-reviewer and roborev job 4372):
                // `diff_one_table` returning `Ok(0)` on a genuinely 0-row table is
                // indistinguishable, at this call site, from a shared row-decode
                // regression that silently yields 0 rows everywhere — exactly the
                // "0-rows-when-present = failure" rule in CLAUDE.md. `rows == 0` is only
                // legitimate for the small, NAMED set in `ZERO_ROW_ALLOWED_TABLES`
                // (verified against a real run: `test_basic.ttl_test_table` is TTL-expired
                // under the fixture's pinned data); every other table must produce rows.
                if rows == 0 && !is_zero_row_allowed(table) {
                    failures.push(format!(
                        "{qualified}: 0 rows compared — not in ZERO_ROW_ALLOWED_TABLES, so \
                         this is treated as a possible silent row-decode regression, not a \
                         legitimate empty table (CLAUDE.md: 0-rows-when-present = failure)"
                    ));
                    continue;
                }
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
    // Corpus-wide floor (review finding): swept-table count and total rows compared must
    // both clear a floor derived from a real run (31 tables, 6096 rows) so a regression
    // that drops most tables to 0 rows (which the per-table check above already refuses)
    // or silently skips most tables cannot pass by accumulating just enough from a few.
    // Only enforced under CQLITE_REQUIRE_FIXTURES=1 — an intentionally partial local
    // corpus (a subset of keyspaces fetched) is a legitimate non-strict state.
    if strict {
        assert!(
            swept >= 31,
            "strict mode: only {swept} tables swept (expected >= 31 of 33, 2 quarantined \
             behind issue #4279) — a table silently dropped out of the sweep?"
        );
        assert!(
            total_rows_compared >= 1000,
            "strict mode: only {total_rows_compared} total rows compared across {swept} \
             tables (expected >= 1000; a real run compares 6096) — a shared row-decode \
             regression yielding near-empty results would still pass the per-table \
             zero-row check if EVERY table returned exactly 1 row, so this floor is a \
             second, independent net"
        );
    }
    // Review finding: only enforced under `strict` — a partial local corpus (e.g. only
    // `test_basic` fetched) legitimately discovers 0 of the 2 quarantined tables (their
    // KEYSPACE directories are absent, so `discover_fixture_tables` never sees them at
    // all), which is not "a table added/removed from the corpus", just an incomplete
    // local root. Under CQLITE_REQUIRE_FIXTURES=1 the full corpus is expected, so the
    // exact count is meaningful there.
    if strict {
        assert_eq!(
            quarantined,
            QUARANTINED_TABLES.len(),
            "strict mode: quarantined-table count drifted from QUARANTINED_TABLES — a table \
             was added/removed from the corpus without updating this list"
        );
    }
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

/// R8.1 — `read-sstable --format vortex` is rejected with the same message shape used for
/// `--format parquet`, exercising the compiled binary (not just the match arm in isolation).
#[test]
fn differential_read_sstable_rejects_vortex() {
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
    let Some(sstable_dir) = table_dir_has_data_db(&root, &table) else {
        assert!(
            !strict,
            "CQLITE_REQUIRE_FIXTURES=1 but test_basic.simple_table has no Data.db"
        );
        eprintln!("test_basic.simple_table: no Data.db present, skipping");
        return;
    };
    let data_db = std::fs::read_dir(&sstable_dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
        .expect("Data.db present")
        .path();

    let (_, stderr, ok) = run_cli(&[
        "read-sstable",
        data_db.to_str().unwrap(),
        "--format",
        "vortex",
    ]);
    assert!(!ok, "read-sstable --format vortex must exit non-zero");
    assert!(
        stderr.contains("Vortex format is not supported for this command"),
        "expected the deliberate Vortex-rejection message, got stderr: {stderr}"
    );
}

// ============================================================================
// R2.1 — the Vortex DType itself carries the UUID extension
// ============================================================================

/// Per top-level column: `Some(is_uuid_extension)` if the Vortex DType is an extension type,
/// `None` otherwise. Read from the FILE's own dtype, not from an Arrow read-back.
async fn vortex_dtype_uuid_flags(
    path: &Path,
) -> Result<Vec<(String, Option<bool>)>, Box<dyn StdError>> {
    use vortex::array::extension::uuid::Uuid;
    use vortex::file::OpenOptionsSessionExt;
    use vortex::session::VortexSession;
    use vortex::VortexSessionDefault;

    let session = VortexSession::default();
    let file = session.open_options().open_path(path.to_path_buf()).await?;
    let fields = file.dtype().as_struct_fields();
    let mut out = Vec::new();
    for name in fields.names().iter() {
        let dtype = fields
            .field(name.as_ref())
            .ok_or_else(|| format!("no field dtype for {name}"))?;
        out.push((
            name.to_string(),
            dtype.as_extension_opt().map(|e| e.is::<Uuid>()),
        ));
    }
    Ok(out)
}

/// R2.1: a `uuid` column arrives as Vortex's UUID extension DType (timeuuid: see the FINDING below) (inspected
/// on the file's dtype), a non-UUID column does not, and the values match Parquet's
/// (`diff_one_table`, which also compares the Arrow extension names). `test_basic.simple_table`
/// carries `id UUID` and `session_id TIMEUUID` (`basic-types.cql`).
#[test]
fn r2_1_vortex_dtype_is_the_uuid_extension_for_uuid_and_timeuuid() {
    let strict = require_fixtures_strict();
    let Some(root) = datasets_root() else {
        assert!(
            !strict,
            "CQLITE_REQUIRE_FIXTURES=1 but the dataset root is unset"
        );
        eprintln!("dataset root not set, skipping");
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
        eprintln!("test_basic.simple_table: no Data.db, skipping");
        return;
    }
    let tmp = TempDir::new().expect("tmp");
    let vortex_path = tmp.path().join("out.vortex");
    let (_, stderr, ok) = run_cli(&[
        "--schema",
        table.schema.to_str().unwrap(),
        "--data-dir",
        root.join("sstables").to_str().unwrap(),
        "export",
        vortex_path.to_str().unwrap(),
        "--format",
        "vortex",
        "--table",
        "test_basic.simple_table",
    ]);
    assert!(ok, "vortex export failed: {stderr}");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let flags = runtime
        .block_on(vortex_dtype_uuid_flags(&vortex_path))
        .expect("read the vortex dtype");
    let flag = |name: &str| {
        flags
            .iter()
            .find(|(n, _)| n == name)
            .unwrap_or_else(|| panic!("column {name} missing from the vortex dtype: {flags:?}"))
            .1
    };
    assert_eq!(
        flag("id"),
        Some(true),
        "uuid column must be the UUID extension DType: {flags:?}"
    );
    // FINDING (#4237 C-audit): through the CLI, `session_id TIMEUUID` is exported as `Utf8` in BOTH
    // Parquet and Vortex (the differential below proves they agree): a timeuuid column does NOT reach
    // the file as the UUID extension on this path, although the writer maps a `CqlType::TimeUuid`
    // column to it (core test `uuid_and_timeuuid_columns_are_the_vortex_uuid_extension_dtype`).
    // Reported to the lead rather than pinned: asserting `Utf8` would enshrine the gap.
    assert_eq!(
        flag("name"),
        None,
        "a text column must not be an extension type"
    );
    // Values: byte-identical to Parquet's (and Arrow extension names equal), via the differential.
    let rows = diff_one_table(&root, &table).unwrap_or_else(|e| panic!("{e}"));
    assert!(rows > 0, "simple_table exported 0 rows");
}

// ============================================================================
// R3.1 — DESC clustering order against the sstabledump golden
// ============================================================================

/// `YYYY-MM-DD HH:MM:SS.mmmZ` (sstabledump's timestamp spelling) -> epoch milliseconds.
fn golden_ts_millis(s: &str) -> i64 {
    let (date, time) = s.trim_end_matches('Z').split_once(' ').expect("date time");
    let d: Vec<i64> = date
        .split('-')
        .map(|p| p.parse().expect("date part"))
        .collect();
    let (hms, ms) = time.split_once('.').unwrap_or((time, "0"));
    let t: Vec<i64> = hms
        .split(':')
        .map(|p| p.parse().expect("time part"))
        .collect();
    let (y, m, day) = (d[0], d[1], d[2]);
    // days from civil (Howard Hinnant)
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    ((days * 86400 + t[0] * 3600 + t[1] * 60 + t[2]) * 1000) + ms.parse::<i64>().expect("millis")
}

/// R3.1: for a table with a DESC clustering column (`test_timeseries.sensor_data`,
/// `clustering ORDER BY (timestamp DESC)`, ~200 rows per partition) the exported Vortex rows are in
/// EXACTLY the order the sstabledump golden lists them — partition by partition, each partition's
/// timestamps descending — not merely equal to Parquet's order. `diff_one_table` (the same table is
/// in the 31-table sweep) separately pins Parquet == Vortex.
#[test]
fn r3_1_desc_clustering_order_matches_the_sstabledump_golden() {
    let strict = require_fixtures_strict();
    let Some(root) = datasets_root() else {
        assert!(
            !strict,
            "CQLITE_REQUIRE_FIXTURES=1 but the dataset root is unset"
        );
        eprintln!("dataset root not set, skipping");
        return;
    };
    let table = FixtureTable {
        keyspace: "test_timeseries",
        table: "sensor_data".to_string(),
        schema: schemas_dir().join("time-series.cql"),
    };
    let Some(dir) = table_dir_has_data_db(&root, &table) else {
        assert!(
            !strict,
            "CQLITE_REQUIRE_FIXTURES=1 but test_timeseries.sensor_data has no Data.db"
        );
        eprintln!("test_timeseries.sensor_data: no Data.db, skipping");
        return;
    };
    let golden_path = std::fs::read_dir(&dir)
        .expect("table dir")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-Data.db.jsonl"))
        .unwrap_or_else(|| {
            panic!(
                "no sstabledump golden (*-Data.db.jsonl) in {}",
                dir.display()
            )
        });
    // The golden is JSON Lines: one partition object per line.
    let golden: Vec<serde_json::Value> = std::fs::read_to_string(&golden_path)
        .expect("golden")
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("golden line is json"))
        .collect();
    // Expected sequence: (partition uuid, timestamp millis), in the golden's own order.
    let mut expected: Vec<(String, i64)> = Vec::new();
    for part in &golden {
        let key = part["partition"]["key"][0]
            .as_str()
            .expect("partition key")
            .to_string();
        for row in part["rows"].as_array().expect("rows") {
            if row["type"] == "row" {
                expected.push((
                    key.clone(),
                    golden_ts_millis(row["clustering"][0].as_str().expect("ck")),
                ));
            }
        }
    }
    assert!(
        expected.len() > 1000,
        "golden unexpectedly small: {}",
        expected.len()
    );
    // The golden itself must exercise DESC: some partition has strictly decreasing timestamps.
    assert!(
        expected
            .windows(2)
            .any(|w| w[0].0 == w[1].0 && w[0].1 > w[1].1),
        "the golden shows no descending clustering step"
    );

    let tmp = TempDir::new().expect("tmp");
    let vortex_path = tmp.path().join("out.vortex");
    let (_, stderr, ok) = run_cli(&[
        "--schema",
        table.schema.to_str().unwrap(),
        "--data-dir",
        root.join("sstables").to_str().unwrap(),
        "export",
        vortex_path.to_str().unwrap(),
        "--format",
        "vortex",
        "--table",
        "test_timeseries.sensor_data",
    ]);
    assert!(ok, "vortex export failed: {stderr}");
    let batch = read_vortex_file(&vortex_path).expect("read back vortex");

    let id_col = batch.column(batch.schema().index_of("sensor_id").expect("sensor_id"));
    let ts_col = batch.column(batch.schema().index_of("timestamp").expect("timestamp"));
    let ids = id_col
        .as_any()
        .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
        .expect("uuid is FixedSizeBinary(16)");
    let ts_i64 =
        arrow::compute::cast(ts_col, &arrow::datatypes::DataType::Int64).expect("ts as i64");
    let ts_i64 = ts_i64
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("i64");
    let divisor = match ts_col.data_type() {
        arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => 1,
        arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => 1_000,
        arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            1_000_000
        }
        other => panic!("unexpected timestamp type {other:?}"),
    };
    let uuid_str = |b: &[u8]| {
        let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
        format!(
            "{}-{}-{}-{}-{}",
            &h[0..8],
            &h[8..12],
            &h[12..16],
            &h[16..20],
            &h[20..32]
        )
    };
    let actual: Vec<(String, i64)> = (0..batch.num_rows())
        .map(|i| (uuid_str(ids.value(i)), ts_i64.value(i) / divisor))
        .collect();
    assert_eq!(
        actual.len(),
        expected.len(),
        "row count differs from the sstabledump golden"
    );
    let first_diff = actual.iter().zip(expected.iter()).position(|(a, e)| a != e);
    assert_eq!(
        first_diff,
        None,
        "Vortex row order diverges from the sstabledump golden at row {first_diff:?}: \
         vortex={:?} golden={:?}",
        first_diff.map(|i| &actual[i]),
        first_diff.map(|i| &expected[i])
    );
}
