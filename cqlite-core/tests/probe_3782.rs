//! Issue #3782 measurement probe — **`#[ignore]`d, run by hand**.
//!
//! Records what a SINGLE-BYTE corruption of a `text` CLUSTERING-key value does
//! to the read and compaction paths on a REAL Cassandra 5.0 fixture
//! (`test_basic.composite_key_table`, `nb`/BIG, LZ4, `clustering_key2 TEXT`).
//!
//! The corruption is applied to the LZ4 **literal** carrying the value, then the
//! chunk's trailing CRC32 is recomputed — so the change is length-preserving,
//! provably a single decompressed byte (asserted), and invisible to integrity
//! checks. No CQLite-written bytes are involved (#3042).
//!
//! Measured on `main` @ 1023095ee (2026-09-02), `CQLITE_DATASETS_ROOT=/data/datasets`,
//! i.e. BEFORE the #3782 fix. The committed regression lane that pins the fixed
//! behaviour is `issue_3782_corrupt_row_refusal.rs`; both stage the fixture through
//! the SAME harness (`support/corrupt_clustering_fixture.rs`) so they can never
//! measure different mutations. This file stays `#[ignore]`d: it REPORTS numbers,
//! it asserts almost nothing, and re-running it is how the table above is refreshed.
//!
//! | surface                                       | control | mutated | note |
//! |-----------------------------------------------|---------|---------|------|
//! | `Database::execute` (materializing)           | 100     | **23**  | `Ok`, no error |
//! | `Database::execute_streaming`                 | 100     | **23**  | `Ok`, 0 err items |
//! | `iterate_all_partitions_for_compaction`       | 100     | **102** | 2 keys LOST, 3 FABRICATED |
//! | `stream_all_partitions_for_compaction`        | 100     | **102** | same |
//! | `iterate_all_partitions` (#2302 index path)   | 100     | **23**  | emits the #2302 fallback WARN, then returns short |
//!
//! Run:
//! ```text
//! CQLITE_DATASETS_ROOT=/data/datasets \
//!   cargo test -p cqlite-core --features cli-helpers --test probe_3782 \
//!   -- --ignored --nocapture --test-threads=1
//! ```
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::platform::Platform;
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::Database;

#[path = "support/corrupt_clustering_fixture.rs"]
mod fixture;

use fixture::{comp_file, FIX_KS, FIX_TABLE, SCHEMA_FILE};

/// Collect WARN/ERROR tracing output into a shared buffer.
#[derive(Clone, Default)]
struct LogSink(Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for LogSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogSink {
    type Writer = LogSink;
    fn make_writer(&'a self) -> LogSink {
        self.clone()
    }
}
impl LogSink {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap().clone()).to_string()
    }
}

fn datasets_root() -> PathBuf {
    PathBuf::from(std::env::var("CQLITE_DATASETS_ROOT").expect("CQLITE_DATASETS_ROOT"))
}

fn schemas_dir() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root")
        .join("test-data")
        .join("schemas")
}

fn fixture_dir() -> PathBuf {
    let root = datasets_root().join("sstables").join(FIX_KS);
    for e in std::fs::read_dir(&root)
        .expect("read keyspace dir")
        .flatten()
    {
        let n = e.file_name().to_string_lossy().to_string();
        if n.starts_with(&format!("{FIX_TABLE}-")) && e.path().is_dir() {
            return e.path();
        }
    }
    panic!("fixture {FIX_KS}.{FIX_TABLE} not found under {root:?}");
}

fn table_schema() -> cqlite_core::schema::TableSchema {
    let cql = std::fs::read_to_string(schemas_dir().join(SCHEMA_FILE)).unwrap();
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {FIX_TABLE}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = FIX_KS.to_string();
    t
}

async fn open_db(data_dir: PathBuf) -> Database {
    ingest(IngestionConfig {
        schema_paths: vec![schemas_dir().join(SCHEMA_FILE)],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{FIX_KS}/")),
    })
    .await
    .expect("ingest")
    .database
}

/// Stage a pristine copy and a single-byte-mutated copy of the fixture, via the
/// shared harness the committed regression lane uses
/// (`support/corrupt_clustering_fixture.rs`), so probe and regression can never
/// measure different mutations. Returns the two TABLE directories.
fn stage() -> (PathBuf, PathBuf) {
    let staged = fixture::stage_control_and_mutated(&fixture_dir(), "probe");
    eprintln!(
        "PROBE3782 mutated decompressed_off={} (chunk CRC32 recomputed)",
        staged.mutated_offset
    );
    (staged.control_dir, staged.mutated_dir)
}

/// Q1 — the READ path. Control vs mutated row counts through the public
/// materializing and streaming surfaces, plus captured WARN/ERROR output.
#[tokio::test]
#[ignore = "measurement probe for issue #3782; run by hand with --ignored"]
async fn probe_3782_q1_read_path() {
    let sink = LogSink::default();
    let _ = tracing_subscriber::fmt()
        .with_writer(sink.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .try_init();

    let (ctl, mutated) = stage();
    let ctl_root = ctl.parent().unwrap().parent().unwrap().to_path_buf();
    let mut_root = mutated.parent().unwrap().parent().unwrap().to_path_buf();
    let sql = format!("SELECT * FROM {FIX_KS}.{FIX_TABLE}");

    let control = open_db(ctl_root)
        .await
        .execute(&sql)
        .await
        .expect("control read")
        .rows
        .len();
    eprintln!("PROBE3782 Q1 CONTROL execute -> Ok rows={control}");
    assert!(
        control > 0,
        "0-rows-when-present: the control read must return rows"
    );
    let after_control = sink.text().lines().count();
    eprintln!("PROBE3782 Q1 CONTROL warn/error lines={after_control}");

    let db = open_db(mut_root).await;
    match db.execute(&sql).await {
        Ok(r) => eprintln!(
            "PROBE3782 Q1 MUTATED execute -> Ok rows={} (control={control}, LOST={})",
            r.rows.len(),
            control.saturating_sub(r.rows.len())
        ),
        Err(e) => eprintln!("PROBE3782 Q1 MUTATED execute -> Err {e}"),
    }

    let cfg = StreamingConfig {
        buffer_size: 8,
        ..Default::default()
    };
    match db.execute_streaming(&sql, cfg).await {
        Ok(mut it) => {
            let (mut ok, mut err) = (0usize, 0usize);
            while let Some(item) = it.next_async().await {
                if item.is_ok() {
                    ok += 1
                } else {
                    err += 1
                }
            }
            eprintln!("PROBE3782 Q1 MUTATED streaming -> ok_rows={ok} err_items={err}");
        }
        Err(e) => eprintln!("PROBE3782 Q1 MUTATED streaming -> Err {e}"),
    }

    let logs = sink.text();
    eprintln!(
        "PROBE3782 Q1 warn/error lines total={} index-fallback-warns={}",
        logs.lines().count(),
        logs.matches("falling back to a full sequential scan")
            .count()
    );
}

/// Q2 — the COMPACTION path and the #2302 index-random-read path.
#[tokio::test]
#[ignore = "measurement probe for issue #3782; run by hand with --ignored"]
async fn probe_3782_q2_compaction_and_index_paths() {
    let sink = LogSink::default();
    let _ = tracing_subscriber::fmt()
        .with_writer(sink.clone())
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .try_init();

    let (ctl, mutated) = stage();
    let schema = table_schema();
    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let mut keys: std::collections::BTreeMap<&str, Vec<Vec<u8>>> = Default::default();

    for (label, dir) in [("CONTROL", &ctl), ("MUTATED", &mutated)] {
        let reader = SSTableReader::open(&comp_file(dir, "-Data.db"), &config, platform.clone())
            .await
            .expect("open SSTableReader");

        let before = sink.text().lines().count();
        match reader
            .iterate_all_partitions_for_compaction(Some(&schema))
            .await
        {
            Ok(rows) => {
                eprintln!("PROBE3782 Q2 {label} compaction -> Ok rows={}", rows.len());
                keys.insert(
                    label,
                    rows.iter().map(|r| r.key.as_bytes().to_vec()).collect(),
                );
            }
            Err(e) => eprintln!("PROBE3782 Q2 {label} compaction -> Err {e}"),
        }
        eprintln!(
            "PROBE3782 Q2 {label} compaction warn/error lines={}",
            sink.text().lines().count() - before
        );

        let cancel = cqlite_core::storage::scan_cancel::ScanCancel::new();
        let mut n = 0usize;
        let r = reader
            .stream_all_partitions_for_compaction(Some(&schema), &cancel, |_row| {
                n += 1;
                Ok(std::ops::ControlFlow::Continue(()))
            })
            .await;
        match r {
            Ok(()) => eprintln!("PROBE3782 Q2 {label} stream_compaction -> Ok rows={n}"),
            Err(e) => {
                eprintln!("PROBE3782 Q2 {label} stream_compaction -> Err after {n} rows: {e}")
            }
        }

        let before = sink.text().lines().count();
        match reader.iterate_all_partitions().await {
            Ok(rows) => eprintln!(
                "PROBE3782 Q2 {label} iterate_all_partitions -> Ok partitions={}",
                rows.len()
            ),
            Err(e) => eprintln!("PROBE3782 Q2 {label} iterate_all_partitions -> Err {e}"),
        }
        let logs = sink.text();
        eprintln!(
            "PROBE3782 Q2 {label} iterate warn/error lines={} index-fallback-warns-cumulative={}",
            logs.lines().count() - before,
            logs.matches("falling back to a full sequential scan")
                .count()
        );
    }

    if let (Some(c), Some(m)) = (keys.get("CONTROL"), keys.get("MUTATED")) {
        assert!(
            !c.is_empty(),
            "0-rows-when-present: the control compaction must yield rows"
        );
        let cs: std::collections::BTreeSet<_> = c.iter().cloned().collect();
        let ms: std::collections::BTreeSet<_> = m.iter().cloned().collect();
        eprintln!(
            "PROBE3782 Q2 KEY DIFF control_rows={} mutated_rows={} LOST_KEYS={} FABRICATED_KEYS={}",
            c.len(),
            m.len(),
            cs.difference(&ms).count(),
            ms.difference(&cs).count()
        );
    }
}
