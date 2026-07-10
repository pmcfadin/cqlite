//! CSV export-writer throughput micro-bench (issue #1494, finding AD5).
//!
//! Companion to `cqlite-core/benches/export_throughput.rs`. The CSV export
//! writer's REAL public surface is `cqlite_cli::output::CSVWriter` — the CLI
//! crate, not `cqlite-core` (core carries no `csv` dependency). The spec
//! (openspec/changes/flight-throughput-benches) task 2.2 names a CSV export
//! bench alongside json/parquet/delta; to bench the real writer instead of a
//! faked non-public surface, this bench is HOSTED in `cqlite-cli` and drives the
//! same `CSVWriter::write(&QueryResult, &OutputConfig)` call the CLI's `--format
//! csv` output uses. (Lead resolution for the spec's crate-host conflict; flagged
//! for the C / spec-audit pass.)
//!
//! Gate id: `export/csv` (criterion group `export`, bench `csv`), registered
//! STRICT in `cqlite-core/benches/perf-gate.json` with the same 10% ratio
//! threshold as the other `export/*` writers. The perf-regression workflow runs
//! it via `cargo bench -p cqlite-cli --features cli-helpers --bench export_csv`
//! on both the PR and `main`, so the policy entry is actually executed (not a
//! decorative knob).
//!
//! Fixture shape / row count match the core export benches: a `SELECT *` over the
//! type-heavy `test_collections.collection_table` (500 rows × 7 cols). The
//! fixture-open logic is duplicated here (~30 lines) rather than sharing the
//! `cqlite-core/benches/fixtures` module across crates — a deliberate,
//! spec-blessed duplication (no new shared crate).
//!
//! Wiring evidence / non-vacuity: the bench runs the real `SELECT *`, **panics**
//! at setup if it returns zero rows, and asserts the CSV writer produced a real
//! header + body before benching the steady state — so a broken fixture can never
//! record a fake measurement. When the canonical dataset is absent the bench
//! skip-registers (creates no group) rather than benching nothing.
//!
//! Reproduce (baseline record — see `cqlite-core/benches/README.md`):
//! ```text
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo bench -p cqlite-cli --features cli-helpers --bench export_csv \
//!   -- --sample-size 20
//! ```

use criterion::{criterion_group, criterion_main, Criterion};

// ---------------------------------------------------------------------------
// CSV export writer over a single loaded type-heavy fixture (cli-helpers).
// ---------------------------------------------------------------------------

/// Locate the `test-data/datasets` root (mirrors the core bench fixture loader).
/// Prefers `CQLITE_DATASETS_ROOT`; else the workspace-relative fallback (the
/// crate dir is `<workspace>/cqlite-cli`, so datasets live at
/// `<workspace>/test-data/datasets`).
#[cfg(feature = "cli-helpers")]
fn datasets_root() -> std::path::PathBuf {
    match std::env::var("CQLITE_DATASETS_ROOT") {
        Ok(root) => std::path::PathBuf::from(root),
        Err(_) => {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../test-data/datasets")
        }
    }
}

/// Resolve the CFID-suffixed `<keyspace>/<table>-<hash>` SSTable directory, or
/// `None` when the fixture (or a real `-Data.db` component) is absent.
#[cfg(feature = "cli-helpers")]
fn table_dir(keyspace: &str, table: &str) -> Option<std::path::PathBuf> {
    let parent = datasets_root().join("sstables").join(keyspace);
    let prefix = format!("{table}-");
    let dir = std::fs::read_dir(&parent)
        .ok()?
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().starts_with(&prefix))?;
    let path = dir.path();
    // Require a real data component (the corpus ships `.jsonl` sidecars even when
    // the binary `-Data.db` is not fetched); `-Data.db` excludes `-Data.db.jsonl`.
    let has_data = std::fs::read_dir(&path)
        .ok()?
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_string_lossy().ends_with("-Data.db"));
    has_data.then_some(path)
}

/// Recursively copy a flat SSTable directory tree into an isolated temp dir.
#[cfg(feature = "cli-helpers")]
fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) {
    std::fs::create_dir_all(dst).expect("create dst dir");
    for entry in std::fs::read_dir(src).expect("read src dir") {
        let entry = entry.expect("dir entry");
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            copy_dir_recursive(&from, &to);
        } else {
            std::fs::copy(&from, &to).expect("copy fixture file");
        }
    }
}

#[cfg(feature = "cli-helpers")]
fn bench_csv_export(c: &mut Criterion) {
    use cqlite_cli::config::OutputConfig;
    use cqlite_cli::output::CSVWriter;
    use cqlite_core::ingestion::{ingest, IngestionConfig};
    use criterion::{black_box, Throughput};

    const KEYSPACE: &str = "test_collections";
    const TABLE: &str = "collection_table";
    const SCHEMA_FILE: &str = "collections.cql";

    // Skip-register (no group) when the canonical dataset is absent.
    let Some(src) = table_dir(KEYSPACE, TABLE) else {
        eprintln!(
            "export_csv: skipping — {KEYSPACE}.{TABLE} absent \
             (run fetch-datasets.sh + set CQLITE_DATASETS_ROOT)"
        );
        return;
    };

    // Copy the fixture into an isolated temp dir so the bench never mutates the
    // shared corpus (mirrors cqlite-core's open_read_db).
    let tmp = tempfile::TempDir::new().expect("create temp dir for csv fixture");
    let dst = tmp
        .path()
        .join(KEYSPACE)
        .join(src.file_name().expect("fixture dir has a final component"));
    copy_dir_recursive(&src, &dst);

    let schema_path = datasets_root().join("../schemas").join(SCHEMA_FILE);
    let cfg = IngestionConfig {
        schema_paths: vec![schema_path],
        data_dir: tmp.path().to_path_buf(),
        version_hint: Some("5.0".to_string()),
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{KEYSPACE}/{TABLE}")),
    };

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let db = rt
        .block_on(ingest(cfg))
        .expect("ingest csv fixture")
        .database;
    let sql = format!("SELECT * FROM {KEYSPACE}.{TABLE}");
    let result = rt
        .block_on(db.execute(&sql))
        .expect("fixture scan must succeed");

    // Non-vacuity: a present fixture that yields zero rows is a setup failure.
    assert!(
        !result.rows.is_empty(),
        "export_csv: fixture {KEYSPACE}.{TABLE} returned 0 rows — refusing to \
         record a vacuous CSV measurement (0-rows-when-present = failure)"
    );
    let row_count = result.rows.len() as u64;

    // Prove the real CSV writer produced a header + body before benching the
    // steady state (wiring evidence for the public CSVWriter surface).
    let output_cfg = OutputConfig::default();
    let probe = CSVWriter::write(&result, &output_cfg).expect("csv write must succeed");
    assert!(
        probe.lines().count() as u64 > row_count,
        "export_csv: CSVWriter produced no header+body — refusing a vacuous measurement"
    );
    eprintln!(
        "export_csv: {KEYSPACE}.{TABLE} scan -> {row_count} rows, {} columns, {} CSV bytes",
        result.metadata.columns.len(),
        probe.len()
    );

    let mut group = c.benchmark_group("export");
    group.throughput(Throughput::Elements(row_count));
    group.bench_function("csv", |b| {
        b.iter(|| {
            let csv =
                CSVWriter::write(black_box(&result), black_box(&output_cfg)).expect("csv write");
            black_box(csv.len())
        });
    });
    group.finish();
}

fn export_csv(c: &mut Criterion) {
    #[cfg(feature = "cli-helpers")]
    bench_csv_export(c);
    // Reference the parameter so `-D warnings` is clean under default features
    // (no cli-helpers → an empty, valid run, matching the core export bench).
    let _ = c;
}

criterion_group!(export_csv_benches, export_csv);
criterion_main!(export_csv_benches);
