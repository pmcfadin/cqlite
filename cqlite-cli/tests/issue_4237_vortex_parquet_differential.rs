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
//! All 33 committed fixture tables are swept (R9.1). Two tables were quarantined while the
//! clustering-column decoder gap this test discovered (issue #4279) was open; #4279 fixed it and
//! they are back in the sweep.

#![cfg(all(feature = "state_machine", feature = "vortex"))]

use arrow::array::Array;
use std::error::Error as StdError;
use std::path::{Path, PathBuf};
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

#[path = "support/vortex_differential.rs"]
mod vortex_differential;
use vortex_differential::*;

/// `diff_table` over a [`FixtureTable`] of the 33-table sweep (`datasets_root` holds `sstables/`).
fn diff_one_table(datasets_root: &Path, table: &FixtureTable) -> Result<usize, Box<dyn StdError>> {
    diff_table(
        &datasets_root.join("sstables"),
        table.keyspace,
        &table.table,
        &table.schema,
    )
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

    for table in &tables {
        let qualified = format!("{}.{}", table.keyspace, table.table);
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
    // both clear a floor derived from a real run (33 tables) so a regression
    // that drops most tables to 0 rows (which the per-table check above already refuses)
    // or silently skips most tables cannot pass by accumulating just enough from a few.
    // Only enforced under CQLITE_REQUIRE_FIXTURES=1 — an intentionally partial local
    // corpus (a subset of keyspaces fetched) is a legitimate non-strict state.
    if strict {
        assert!(
            swept >= 33,
            "strict mode: only {swept} tables swept (expected all 33, R9.1) — a table \
             silently dropped out of the sweep?"
        );
        assert!(
            total_rows_compared >= 1000,
            "strict mode: only {total_rows_compared} total rows compared across {swept} \
             tables (expected >= 1000; a real run compares thousands) — a shared row-decode \
             regression yielding near-empty results would still pass the per-table \
             zero-row check if EVERY table returned exactly 1 row, so this floor is a \
             second, independent net"
        );
    }
    eprintln!(
        "vortex-parquet differential: {swept}/{} tables swept \
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

/// R2.1: a `uuid` AND a `timeuuid` column arrive as Vortex's UUID extension DType (inspected
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
    assert_eq!(
        flag("session_id"),
        Some(true),
        "timeuuid column must be the UUID extension DType: {flags:?}"
    );
    assert_eq!(
        flag("name"),
        None,
        "a text column must not be an extension type"
    );
    // Parquet carries the same extension on both columns (read from its Arrow schema).
    let parquet_path = tmp.path().join("out.parquet");
    let (_, stderr, ok) = run_cli(&[
        "--schema",
        table.schema.to_str().unwrap(),
        "--data-dir",
        root.join("sstables").to_str().unwrap(),
        "export",
        parquet_path.to_str().unwrap(),
        "--format",
        "parquet",
        "--table",
        "test_basic.simple_table",
    ]);
    assert!(ok, "parquet export failed: {stderr}");
    let (parquet_schema, _) = read_parquet_file(&parquet_path).expect("read parquet back");
    for col in ["id", "session_id"] {
        let f = parquet_schema.field_with_name(col).expect("parquet field");
        assert_eq!(
            f.metadata().get("ARROW:extension:name").map(String::as_str),
            Some("arrow.uuid"),
            "parquet column {col} must carry the Arrow UUID extension: {f:?}"
        );
    }
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
/// in the 33-table sweep) separately pins Parquet == Vortex.
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
