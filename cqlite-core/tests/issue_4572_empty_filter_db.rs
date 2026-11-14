//! Issue #4572 — a missing / empty / undeserializable `Filter.db` must load as an ALWAYS-PRESENT
//! filter (cost: a probe), never as an all-negative one that drops a generation's live keys.
//!
//! Cassandra 5.0.8 oracle (`io/sstable/format/SSTableReader.java`, `FilterComponent`,
//! `utils/FilterFactory.java`): a Filter.db that is absent or fails to load is replaced by
//! `FilterFactory.AlwaysPresent` — the filter then says "maybe" for every key, so reads fall
//! through to the index. Never "definitely absent".
//!
//! Inputs are REAL Cassandra-written generations (datasets-v4: BIG `test_basic/simple_table`, 1000
//! partitions; BTI `test_da/simple_table`) copied into a tempdir with ONLY `Filter.db` replaced — a
//! documented edit of real bytes. Expected rows come from the generation's own `sstabledump` golden
//! (`*-Data.db.jsonl`), never from CQLite output. Every point read must return exactly the row the
//! golden holds, with the real filter AND with each damaged one.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::path::Path;

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::{Config, Database};

#[path = "support/datasets_root.rs"]
mod datasets_root;

fn strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// Every partition key literal of the generation's golden, in file order.
fn golden_keys(dir: &Path) -> Vec<String> {
    let jsonl = std::fs::read_dir(dir)
        .expect("fixture dir")
        .flatten()
        .map(|e| e.path())
        .find(|p| p.to_string_lossy().ends_with("-Data.db.jsonl"))
        .expect("committed sstabledump golden");
    let text = std::fs::read_to_string(jsonl).expect("golden");
    let keys: Vec<String> = text
        .lines()
        .filter_map(|l| {
            // Tolerates both `"key":["x"]` (compact) and `"key": ["x"]` (pretty-printed goldens).
            let after = l.split("\"key\"").nth(1)?;
            let after = after.split_once('[')?.1.trim_start().strip_prefix('"')?;
            Some(after.split('"').next()?.to_string())
        })
        .collect();
    assert!(!keys.is_empty(), "golden holds no partitions");
    keys
}

/// A copy of the generation with `Filter.db` replaced by `filter` (`None` = leave the real one,
/// `Some(None)` is not used: removal is its own case).
fn damaged_copy(src: &Path, ks: &str, filter: Option<Option<&[u8]>>) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dst = tmp
        .path()
        .join(ks)
        .join(src.file_name().expect("table dir"));
    std::fs::create_dir_all(&dst).expect("mkdir");
    for e in std::fs::read_dir(src).expect("dir").flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if !e.path().is_file() || name.ends_with(".jsonl") {
            continue;
        }
        if name.ends_with("-Filter.db") {
            match filter {
                None => {
                    std::fs::copy(e.path(), dst.join(&name)).expect("copy");
                }
                Some(Some(bytes)) => std::fs::write(dst.join(&name), bytes).expect("write"),
                Some(None) => {} // absent
            }
            continue;
        }
        std::fs::copy(e.path(), dst.join(&name)).expect("copy");
    }
    tmp
}

async fn open(root: &Path, schema_file: &str, ks: &str, mode: ReadPathMode) -> Database {
    let schema = datasets_root::schema_path(schema_file).expect("committed schema");
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    ingest(IngestionConfig {
        schema_paths: vec![schema],
        data_dir: root.to_path_buf(),
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{ks}/")),
    })
    .await
    .expect("ingest")
    .database
}

async fn check(ks: &str, table: &str, schema_file: &str, max_keys: usize) {
    let src = match datasets_root::resolve_table_generation_dir(ks, table) {
        Ok(d) => d,
        Err(e) if strict() => {
            panic!("{ks}.{table}: fixture missing under CQLITE_REQUIRE_FIXTURES: {e}")
        }
        Err(e) => {
            eprintln!("SKIP {ks}.{table}: {e}");
            return;
        }
    };
    let keys: Vec<String> = golden_keys(&src).into_iter().take(max_keys).collect();
    let real_filter = std::fs::read_dir(&src)
        .expect("dir")
        .flatten()
        .find(|e| e.file_name().to_string_lossy().ends_with("-Filter.db"))
        .map(|e| std::fs::read(e.path()).expect("Filter.db"))
        .expect("the fixture has a Filter.db");
    // [name, replacement]: the real filter, then every way a Filter.db can be unusable.
    let variants: Vec<(&str, Option<Option<&[u8]>>)> = vec![
        ("real", None),
        ("empty", Some(Some(&[]))),
        ("truncated-5-bytes", Some(Some(&real_filter[..5]))),
        ("zeroed-header", Some(Some(&[0u8; 16]))),
        ("absent", Some(None)),
    ];
    for (label, filter) in variants {
        let tmp = damaged_copy(&src, ks, filter);
        for mode in [ReadPathMode::Point, ReadPathMode::Auto] {
            let db = open(tmp.path(), schema_file, ks, mode).await;
            for k in &keys {
                let r = db
                    .execute(&format!("SELECT id FROM {ks}.{table} WHERE id = {k}"))
                    .await
                    .unwrap_or_else(|e| panic!("{ks}.{table} {label} [{mode:?}] id={k}: {e}"));
                assert_eq!(
                    r.rows.len(),
                    1,
                    "{ks}.{table} Filter.db={label} [{mode:?}]: the point read of the PRESENT key {k} \
                     returned {} row(s), the golden holds exactly 1",
                    r.rows.len()
                );
            }
        }
    }
}

/// BIG (`nb`): 1000 partitions; the bloom decides whether a generation is read at all.
#[tokio::test]
async fn big_point_reads_survive_every_unusable_filter_db() {
    check("test_basic", "simple_table", "basic-types.cql", 40).await;
}

/// BTI (`da`): the same property on a trie-indexed generation.
#[tokio::test]
async fn bti_point_reads_survive_every_unusable_filter_db() {
    check("test_da", "simple_table", "da-test.cql", 3).await;
}
