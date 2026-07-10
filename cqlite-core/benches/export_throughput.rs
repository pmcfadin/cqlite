//! Export / conversion throughput micro-benches (issue #1494, finding AD5).
//!
//! Tier-1 of the export/Flight perf net (epic #1469). These measure the
//! CPU-bound data plane the Flight `do_get` path and the Parquet/JSON exporters
//! all share, over the pinned canonical datasets. They are the STRICT,
//! ratio-gated entries in `benches/perf-gate.json` — a real regression on the
//! converter or a per-format writer clears the same-runner PR-vs-`main` median
//! threshold in `.github/workflows/perf-regression.yml`.
//!
//! Benches (all `export/*`, so the perf-gate ids are stable):
//!   - `export/rows_to_record_batch` — CQL→Arrow conversion (the per-cell data
//!     plane shared by Flight + Parquet); the primary STRICT signal.
//!   - `export/json`   — `QueryResult::to_json` serialization to bytes.
//!   - `export/parquet`— `ParquetWriter::write` (feature `parquet`).
//!   - `export/delta`  — `write_delta_records_to_bytes` over synthetic upsert
//!     records (features `delta-scan` + `parquet`); the delta Parquet writer's
//!     public contract takes `DeltaRecord`s, so this drives the real writer.
//!
//! Wiring evidence / non-vacuity: the fixture-backed benches run a real
//! `SELECT *` over a type-heavy fixture and **panic** at setup if it returns
//! zero rows, so a broken fixture can never record a fake measurement. When the
//! canonical dataset is absent the fixture-backed group skip-registers (creates
//! no group) rather than benching nothing.
//!
//! Reproduce (baseline record — see `benches/README.md`):
//! ```text
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo bench -p cqlite-core --features cli-helpers,write-support,parquet,delta-scan \
//!   --bench export_throughput
//! ```

use criterion::{criterion_group, criterion_main, Criterion};

#[path = "fixtures/mod.rs"]
mod fixtures;

#[path = "profiling/mod.rs"]
mod profiling;

// ---------------------------------------------------------------------------
// Tier-1a: fixture-backed conversion + per-format writers (cli-helpers + arrow)
// ---------------------------------------------------------------------------

/// Converter + JSON (+ Parquet when the feature is on) over a single loaded
/// type-heavy fixture. The fixture is scanned ONCE; all format benches convert
/// the same in-memory `QueryResult`, so the measurement isolates conversion /
/// serialization cost from read-path cost.
#[cfg(all(feature = "cli-helpers", feature = "arrow"))]
fn bench_fixture_export(c: &mut Criterion) {
    use criterion::{black_box, Throughput};
    use cqlite_core::export::rows_to_record_batch;

    // TYPE_HEAVY exercises the per-cell conversion path across many CQL types
    // (the cost AE1–AE5 tighten). Skip-register (no group) when absent.
    let fx = fixtures::ReadFixture::TYPE_HEAVY;
    if !fixtures::fixture_present(&fx) {
        eprintln!(
            "export_throughput: skipping fixture-backed benches — {} absent \
             (run fetch-datasets.sh + set CQLITE_DATASETS_ROOT)",
            fx.qualified()
        );
        return;
    }

    let loaded = fixtures::open_read_db(&fx);
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let sql = format!("SELECT * FROM {}", fx.qualified());
    let result = rt
        .block_on(loaded.db.execute(&sql))
        .expect("fixture scan must succeed");

    // Non-vacuity: a present fixture that yields zero rows is a setup failure,
    // never a 0-row measurement.
    assert!(
        !result.rows.is_empty(),
        "export_throughput: fixture {} returned 0 rows — refusing to record a \
         vacuous conversion measurement (0-rows-when-present = failure)",
        fx.qualified()
    );
    let row_count = result.rows.len() as u64;
    eprintln!(
        "export_throughput: {} scan -> {} rows, {} columns",
        fx.qualified(),
        row_count,
        result.metadata.columns.len()
    );

    let mut group = c.benchmark_group("export");
    group.throughput(Throughput::Elements(row_count));

    group.bench_function("rows_to_record_batch", |b| {
        b.iter(|| {
            let batch =
                rows_to_record_batch(black_box(&result.metadata.columns), black_box(&result.rows))
                    .expect("conversion must succeed");
            black_box(batch.num_rows())
        });
    });

    group.bench_function("json", |b| {
        b.iter(|| {
            let json = black_box(&result).to_json();
            let bytes = serde_json::to_vec(&json).expect("json serialize");
            black_box(bytes.len())
        });
    });

    #[cfg(feature = "parquet")]
    {
        use cqlite_core::export::parquet::{ParquetExportOptions, ParquetWriter};
        let opts = ParquetExportOptions::default();
        group.bench_function("parquet", |b| {
            b.iter(|| {
                let bytes =
                    ParquetWriter::write(black_box(&result), black_box(&opts)).expect("parquet");
                black_box(bytes.len())
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Tier-1b: delta Parquet writer (delta-scan + parquet), self-contained
// ---------------------------------------------------------------------------

/// The delta Parquet writer over a fixed set of synthetic `Upsert` records.
/// Self-contained (no dataset): the writer's public contract IS a stream of
/// `DeltaRecord`s, so feeding it synthetic upserts drives the real writer.
#[cfg(all(feature = "delta-scan", feature = "parquet"))]
fn bench_delta_export(c: &mut Criterion) {
    use criterion::{black_box, Throughput};
    use std::collections::HashMap;

    use cqlite_core::export::{write_delta_records_to_bytes, DeltaParquetOptions};
    use cqlite_core::schema::{
        ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema,
    };
    use cqlite_core::storage::sstable::reader::delta_scan::{CellDelta, DeltaRecord, RowKeys};
    use cqlite_core::types::{ColumnId, Value};

    const N_RECORDS: usize = 5_000;

    let schema = TableSchema {
        keyspace: "bench_ks".into(),
        table: "delta_t".into(),
        partition_keys: vec![KeyColumn {
            name: "pk".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".into(),
            data_type: "text".into(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![Column {
            name: "val".into(),
            data_type: "text".into(),
            is_static: false,
            nullable: true,
            default: None,
        }],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    };

    let records: Vec<DeltaRecord> = (0..N_RECORDS)
        .map(|i| DeltaRecord::Upsert {
            keys: RowKeys::new(
                vec![Value::Integer(i as i32)],
                vec![Value::Text(format!("ck{i}"))],
            ),
            liveness: None,
            cells: vec![(
                ColumnId::new("val"),
                CellDelta::value(Value::Text(format!("value-{i}")), 1_000_000 + i as i64),
            )],
        })
        .collect();

    // Non-vacuity: prove the writer actually produced a Parquet file for the
    // records before benching the steady state.
    let probe = write_delta_records_to_bytes(
        records.iter().cloned(),
        &schema,
        DeltaParquetOptions::default(),
    )
    .expect("delta write must succeed");
    assert!(
        probe.len() > 4 && &probe[0..4] == b"PAR1",
        "export_throughput: delta writer produced no Parquet output — refusing a vacuous measurement"
    );

    let mut group = c.benchmark_group("export");
    group.throughput(Throughput::Elements(N_RECORDS as u64));
    group.bench_function("delta", |b| {
        b.iter(|| {
            let bytes = write_delta_records_to_bytes(
                black_box(&records).iter().cloned(),
                black_box(&schema),
                DeltaParquetOptions::default(),
            )
            .expect("delta write");
            black_box(bytes.len())
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// Dispatcher — each sub-bench is independently feature-gated so the bench
// binary compiles (and reports an empty-but-valid run) under any feature set.
// ---------------------------------------------------------------------------

fn export_throughput(c: &mut Criterion) {
    #[cfg(all(feature = "cli-helpers", feature = "arrow"))]
    bench_fixture_export(c);
    #[cfg(all(feature = "delta-scan", feature = "parquet"))]
    bench_delta_export(c);
    // Reference the parameter so `-D warnings` is clean when no feature enables
    // a sub-bench (the default-feature build compiles an empty, valid run).
    let _ = c;
}

criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = export_throughput
);
criterion_main!(benches);
