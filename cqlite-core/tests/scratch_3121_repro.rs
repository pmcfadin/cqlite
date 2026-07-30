//! SCRATCH triage repro for issue #3121 (row-deleted clustering row resurfacing
//! as a phantom null row in a static-bearing partition). NOT commit-quality;
//! prints the observed row set for `test_tomb.static_with_tombstones` pk=1.

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::types::Value;
use cqlite_core::Database;

const TTL_NOW_OVERRIDE_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";
const PINNED_NOW: i64 = 1_800_000_000;

fn datasets_root() -> PathBuf {
    PathBuf::from(
        std::env::var("CQLITE_DATASETS_ROOT").unwrap_or_else(|_| "test-data/datasets".into()),
    )
}

async fn open_db(sstables_dir: &Path, schema: &Path, keyspace: &str) -> Result<Database, String> {
    let cfg = IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: sstables_dir.to_path_buf(),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{keyspace}/")),
    };
    let result = ingest(cfg).await.map_err(|e| format!("ingestion: {e}"))?;
    if result.schema_load_result.schemas_loaded == 0 {
        return Err("no schemas loaded".into());
    }
    Ok(result.database)
}

fn render(v: &Value) -> String {
    match v {
        Value::Null => "<null>".to_string(),
        other => format!("{other:?}"),
    }
}

async fn run_query(root: &Path, schema: &Path, q: &str, label: &str) {
    std::env::set_var(TTL_NOW_OVERRIDE_ENV, PINNED_NOW.to_string());
    let db = open_db(root, schema, "test_tomb")
        .await
        .expect("open test_tomb");
    let res = db.execute(q).await;
    std::env::remove_var(TTL_NOW_OVERRIDE_ENV);
    let res = res.unwrap_or_else(|e| panic!("{label}: SELECT failed: {e}"));
    eprintln!(
        "=== {label} :: {q}\n    projected: {:?}\n    ROWS: {}",
        res.metadata
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        res.rows.len()
    );
    for row in &res.rows {
        let mut parts: Vec<String> = Vec::new();
        for c in &res.metadata.columns {
            let v = row.values.get(c.name.as_str()).unwrap_or(&Value::Null);
            parts.push(format!("{}={}", c.name, render(v)));
        }
        eprintln!("    ROW  {}", parts.join(" | "));
    }
}

#[tokio::test]
async fn scratch_3121_static_with_tombstones_pk1() {
    let root = datasets_root().join("sstables");
    let schema = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../test-data/schemas/tombstone-parity.cql")
        .canonicalize()
        .expect("schema path");
    assert!(
        root.join("test_tomb/static_with_tombstones-4cdb9780702011f1b8f419c9a388d558/nb-1-big-Data.db").exists()
            || root.join("test_tomb").exists(),
        "fixture root missing: {}",
        root.display()
    );

    run_query(
        &root,
        &schema,
        "SELECT pk, ck, stat_col, row_col FROM test_tomb.static_with_tombstones WHERE pk = 1",
        "point pk=1",
    )
    .await;

    run_query(
        &root,
        &schema,
        "SELECT pk, ck, stat_col, row_col FROM test_tomb.static_with_tombstones",
        "full scan",
    )
    .await;
}
