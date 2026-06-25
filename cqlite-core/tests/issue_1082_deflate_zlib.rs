//! Issue #1082 (Epic #970): Cassandra Deflate is ZLIB-wrapped across ALL chunk
//! decode paths — including the full-scan `stitch_all_chunks` path.
//!
//! Cassandra's `DeflateCompressor` uses `java.util.zip.Deflater`/`Inflater`, which
//! emit a ZLIB-wrapped stream: a 2-byte header (`0x78 0x9c`) + DEFLATE body +
//! 4-byte Adler-32 trailer. There is NO 4-byte uncompressed-size prefix (that is an
//! LZ4/Zstd convention). The prior full-scan path mis-read the zlib header
//! (`0x78 0x9c ..`) as a ~2 GB big-endian "size", tripping the decompression-bomb
//! guard (`size 2023550423 exceeds limit`) and failing every `SELECT * /
//! COUNT(*)` over a deflate table.
//!
//! This is an END-TO-END regression test: it drives the SAME `Database::execute`
//! query path the CLI uses, which for an `nb` (V5CompressedLegacy) table funnels
//! through `stitch_all_chunks` -> `Compression::decompress`. It asserts the
//! full-scan row count for `test_comp.deflate_table` matches the committed
//! sstabledump JSONL golden, and (as a regression guard for the same `decompress`
//! function) that LZ4 / Snappy / Zstd full scans return the golden count too.
//!
//! Fixtures resolve via `CQLITE_DATASETS_ROOT`; the table-dir UUID is never
//! hardcoded (globbed by `<table>-` prefix). When the dataset (or its gitignored
//! `*.db` binaries) is absent, every test SKIPs cleanly. When the fixture IS
//! present, a zero-row or mismatched result FAILS loudly.
//!
//! Requires the `cli-helpers` feature (the `ingestion` module that builds a
//! queryable `Database`) and `state_machine` (the query engine); without them the
//! whole file compiles out, matching the other end-to-end SELECT integration tests.
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::Database;

const KEYSPACE_FILTER: &str = "/test_comp/";

fn datasets_root() -> Option<PathBuf> {
    std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.exists())
}

fn schema_path() -> Option<PathBuf> {
    if let Some(root) = datasets_root() {
        let p = root
            .parent()?
            .join("schemas")
            .join("compression-parity.cql");
        if p.exists() {
            return Some(p);
        }
    }
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let p = manifest
        .parent()?
        .join("test-data")
        .join("schemas")
        .join("compression-parity.cql");
    p.exists().then_some(p)
}

/// Resolve `<root>/sstables/test_comp/<table>-<uuid>/`, globbing by prefix.
fn fixture_dir(table: &str) -> Option<PathBuf> {
    let root = datasets_root()?;
    let comp_dir = root.join("sstables").join("test_comp");
    if !comp_dir.is_dir() {
        return None;
    }
    let prefix = format!("{table}-");
    std::fs::read_dir(&comp_dir)
        .ok()?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(&prefix))
                    .unwrap_or(false)
        })
}

/// Count the total CQL rows in the committed sstabledump JSONL golden (sum of the
/// `rows` arrays across every partition line, counting only `type == "row"`).
/// Returns `None` if the golden file is absent.
fn golden_row_count(table: &str) -> Option<usize> {
    let dir = fixture_dir(table)?;
    let jsonl = dir.join("nb-1-big-Data.db.jsonl");
    let text = std::fs::read_to_string(&jsonl).ok()?;
    let mut total = 0usize;
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("{table}: golden JSONL parse failed: {e}\nline: {line}"));
        if let Some(rows) = v.get("rows").and_then(|r| r.as_array()) {
            total += rows
                .iter()
                .filter(|r| r.get("type").and_then(|t| t.as_str()) == Some("row"))
                .count();
        }
    }
    Some(total)
}

/// Ingest the `test_comp` keyspace and return a queryable `Database`, or a skip
/// reason. The Data.db binaries are gitignored, so a missing fixture SKIPs.
async fn setup_db() -> Result<Database, String> {
    let root = datasets_root().ok_or("CQLITE_DATASETS_ROOT unset or path missing")?;
    let schema = schema_path().ok_or("compression-parity.cql schema not found")?;
    let data_dir = root.join("sstables");
    if !data_dir.exists() {
        return Err(format!("sstables dir not found at {data_dir:?}"));
    }
    // Require at least one real Data.db so we never silently pass on an un-fetched
    // fixture corpus.
    let deflate = fixture_dir("deflate_table").ok_or("test_comp/deflate_table-* dir absent")?;
    if !deflate.join("nb-1-big-Data.db").exists() {
        return Err(format!(
            "deflate_table Data.db missing (binary not fetched) at {deflate:?}"
        ));
    }

    let cfg = IngestionConfig {
        schema_paths: vec![schema],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(KEYSPACE_FILTER.to_string()),
    };
    let result = ingest(cfg).await.map_err(|e| format!("ingestion: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".into());
    }
    Ok(result.database)
}

/// Run a full-table `SELECT *` and assert the returned row count equals the JSONL
/// golden. This is the END-TO-END acceptance check for the table's chunk decode
/// path (`stitch_all_chunks` -> `Compression::decompress`).
async fn assert_full_scan_matches_golden(db: &Database, table: &str) {
    let Some(expected) = golden_row_count(table) else {
        eprintln!("SKIP {table}: golden JSONL absent");
        return;
    };
    assert!(
        expected > 0,
        "{table}: golden present but reports zero rows (corrupt golden)"
    );

    let query = format!("SELECT * FROM test_comp.{table}");
    let result = db
        .execute(&query)
        .await
        .unwrap_or_else(|e| panic!("{table}: full-scan SELECT * failed: {e}"));

    assert_eq!(
        result.rows.len(),
        expected,
        "{table}: full-scan row count {} != golden {}",
        result.rows.len(),
        expected
    );
    eprintln!(
        "{table}: full-scan rows={} == golden={expected}",
        result.rows.len()
    );
}

/// THE fix: a deflate full scan (stitch_all_chunks path) succeeds and matches the
/// golden row count. Previously failed with the 2 GB decompression-bomb error.
#[tokio::test]
async fn deflate_full_scan_matches_golden() {
    let db = match setup_db().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("SKIP deflate_full_scan_matches_golden: {e}");
            return;
        }
    };
    assert_full_scan_matches_golden(&db, "deflate_table").await;
}

/// Regression guard for the OTHER algorithms that share `Compression::decompress`
/// via the same `stitch_all_chunks` full-scan path — the deflate fix must not
/// disturb LZ4 / Snappy / Zstd framing.
#[tokio::test]
async fn lz4_snappy_zstd_full_scan_unbroken() {
    let db = match setup_db().await {
        Ok(db) => db,
        Err(e) => {
            eprintln!("SKIP lz4_snappy_zstd_full_scan_unbroken: {e}");
            return;
        }
    };
    for table in ["lz4_table", "snappy_table", "zstd_table"] {
        assert_full_scan_matches_golden(&db, table).await;
    }
}
