//! Integration test for the one-shot `compact_sstables` entry point (Issue #842).
//!
//! `compact_sstables` is the engine entry point behind the `cqlite compact` CLI
//! command and the compaction-parity harness. Unlike `WriteEngine::maintenance_step`,
//! it compacts an *explicit* set of input SSTables into an *explicit* output
//! directory, with an explicit `gc_before` / `now_sec`.
//!
//! This test builds two overlapping SSTables via the public WriteEngine API, then
//! drives `compact_sstables` over exactly those files (newest-generation first) and
//! asserts the merged output is a valid, readable SSTable with last-write-wins
//! semantics applied.
//!
//! NOTE: `gc_before` is passed but purging is not yet applied during the merge
//! (issues #845/#848); this test therefore asserts merge/LWW correctness, not
//! tombstone purging.

#![cfg(feature = "write-support")]

use cqlite_core::platform::Platform;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder};
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::merge::compact_sstables;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::TableId as CqlTableId;
use cqlite_core::types::Value;
use cqlite_core::Config;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: "compact_ks".to_string(),
        table: "items".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn write_row(id: i32, name: &str, timestamp: i64) -> Mutation {
    let table_id = TableId::new("compact_ks", "items");
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "name".to_string(),
        value: Value::Text(name.to_string()),
    }];
    Mutation::new(table_id, pk, None, ops, timestamp, None)
}

/// Discover published `nb-*-big-Data.db` files under `dir`, newest-generation first.
///
/// Mirrors the CLI's `discover_input_sstables` so this test exercises the same
/// input-ordering contract `compact_sstables` relies on (run index 0 = newest).
fn discover_inputs(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = Vec::new();
    collect(dir, &mut found, 8);
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

fn collect(dir: &std::path::Path, out: &mut Vec<(u64, PathBuf)>, depth: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
            let base = name.trim_end_matches("-Data.db");
            if !path.with_file_name(format!("{base}-TOC.txt")).exists() {
                continue;
            }
            let generation = name
                .strip_prefix("nb-")
                .and_then(|s| s.split("-big-").next())
                .and_then(|g| g.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((generation, path));
        } else if depth > 0 && path.is_dir() {
            collect(&path, out, depth - 1);
        }
    }
}

/// Build two overlapping SSTables and compact them via `compact_sstables`.
///
/// - SSTable A (ts=100): ids 1..=10, names `a-name-{id}`
/// - SSTable B (ts=200): ids 6..=15, names `b-name-{id}` — overrides A on 6..=10
///
/// Expected merged output: 15 partitions; ids 6..=10 carry B's `b-name-*` value
/// (newest timestamp wins).
#[test]
fn compact_sstables_merges_explicit_inputs_with_lww() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let output_dir = temp.path().join("out");
    let schema = make_schema();

    // ── Build two input SSTables via the public WriteEngine API ──
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    for id in 1_i32..=10 {
        engine
            .write(write_row(id, &format!("a-name-{id}"), 100))
            .expect("write A");
    }
    let info_a = rt
        .block_on(engine.flush())
        .expect("flush A")
        .expect("info A");
    assert_eq!(info_a.partition_count, 10, "SSTable A: 10 partitions");

    for id in 6_i32..=15 {
        engine
            .write(write_row(id, &format!("b-name-{id}"), 200))
            .expect("write B");
    }
    let info_b = rt
        .block_on(engine.flush())
        .expect("flush B")
        .expect("info B");
    assert_eq!(info_b.partition_count, 10, "SSTable B: 10 partitions");

    drop(engine); // release the write-dir before re-reading inputs

    // ── Compact exactly those inputs into an explicit output dir ──
    let inputs = discover_inputs(&data_dir);
    assert_eq!(inputs.len(), 2, "expected 2 input SSTables, got {inputs:?}");

    let report = rt
        .block_on(compact_sstables(
            inputs,
            &output_dir,
            &schema,
            9,                   // output generation
            Some(1_700_000_000), // gc_before (threaded but not yet applied)
            None,                // now_sec
        ))
        .expect("compaction must succeed");

    assert_eq!(report.stats.input_files, 2, "merged 2 inputs");
    assert_eq!(
        report.stats.output_partitions, 15,
        "union of ids 1..=15 = 15 partitions"
    );

    // ── Every output component is published (TOC.txt is the barrier) ──
    let out = &report.output;
    for (label, path) in [
        ("Data.db", &out.data_path),
        ("Index.db", out.index_path.as_ref().unwrap()),
        ("Filter.db", out.filter_path.as_ref().unwrap()),
        ("Summary.db", out.summary_path.as_ref().unwrap()),
        ("Statistics.db", &out.stats_path),
        ("Digest.crc32", &out.digest_path),
        ("TOC.txt", &out.toc_path),
    ] {
        assert!(
            path.exists(),
            "output component {label} must exist at {path:?}"
        );
    }
    // Filter.db is optional (disabled bloom filter omits it, Issue #852).
    if let Some(filter) = &out.filter_path {
        assert!(
            filter.exists(),
            "output component Filter.db must exist at {filter:?}"
        );
    }
    assert!(
        out.data_path.to_string_lossy().contains("nb-9-big-"),
        "output should use the requested generation 9: {:?}",
        out.data_path
    );

    // ── Re-open the output and assert read-back + last-write-wins ──
    let cqlite_config = Config::default();
    let manager = rt.block_on(async {
        let platform = Arc::new(Platform::new(&cqlite_config).await.expect("platform"));
        SSTableManager::new(
            &output_dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager opens the compacted output")
    });

    let table_id = CqlTableId::from("compact_ks.items");
    let results = rt
        .block_on(manager.scan(&table_id, None, None, None, Some(&schema)))
        .expect("post-compaction scan");

    assert_eq!(
        results.len(),
        15,
        "merged output must contain the union of both inputs (15 rows)"
    );

    // ids 6..=10 overlap; SSTable B (ts=200) wins over A (ts=100).
    let by_pk: HashMap<Vec<u8>, Value> = results.into_iter().map(|(k, v)| (k.0, v)).collect();
    for id in 6_i32..=10 {
        let key: Vec<u8> = id.to_be_bytes().into();
        let row = by_pk
            .get(&key)
            .unwrap_or_else(|| panic!("PK {id} must be present in merged output"));
        let rendered = format!("{row:?}");
        assert!(
            rendered.contains(&format!("b-name-{id}")),
            "PK {id}: newest write (b-name-{id}) must win LWW, got {rendered}"
        );
    }
}

// ── Disabled bloom filter (#852 review finding 1) ───────────────────────────

/// Same shape as `make_schema` but with `bloom_filter_fp_chance = 1.0`, which
/// disables the bloom filter (Cassandra's AlwaysPresentFilter): the writer emits
/// NO Filter.db component.
fn make_disabled_filter_schema() -> TableSchema {
    let mut schema = make_schema();
    schema
        .comments
        .insert("bloom_filter_fp_chance".to_string(), "1.0".to_string());
    schema
}

/// Regression for Issue #852 review finding 1: compacting a table whose bloom
/// filter is disabled must succeed end to end. The compaction publish step
/// previously included a mandatory `filter_path` in its rename list, so it tried
/// to rename a non-existent Filter.db and failed. With `filter_path: Option`,
/// the publish must skip the absent component and emit no Filter.db.
#[test]
fn compact_disabled_filter_table_succeeds_without_filter_db() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let output_dir = temp.path().join("out");
    let schema = make_disabled_filter_schema();

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    for id in 1_i32..=10 {
        engine
            .write(write_row(id, &format!("a-name-{id}"), 100))
            .expect("write A");
    }
    let info_a = rt
        .block_on(engine.flush())
        .expect("flush A")
        .expect("info A");
    // The flushed input itself must carry no Filter.db (sanity).
    assert!(
        info_a.filter_path.is_none(),
        "disabled-filter flush must not emit Filter.db"
    );

    for id in 6_i32..=15 {
        engine
            .write(write_row(id, &format!("b-name-{id}"), 200))
            .expect("write B");
    }
    rt.block_on(engine.flush())
        .expect("flush B")
        .expect("info B");

    drop(engine);

    let inputs = discover_inputs(&data_dir);
    assert_eq!(inputs.len(), 2, "expected 2 input SSTables, got {inputs:?}");

    let report = rt
        .block_on(compact_sstables(
            inputs,
            &output_dir,
            &schema,
            9,
            Some(1_700_000_000),
            None,
        ))
        .expect("compaction of a disabled-filter table must succeed");

    assert_eq!(report.stats.output_partitions, 15, "union of ids 1..=15");

    let out = &report.output;
    // The merged output reports no Filter.db, none was written, and the TOC
    // omits it.
    assert!(
        out.filter_path.is_none(),
        "compacted disabled-filter output must not report a Filter.db path"
    );
    let toc = std::fs::read_to_string(&out.toc_path).expect("read TOC");
    assert!(
        !toc.contains("Filter.db"),
        "compacted output TOC must omit Filter.db, got: {toc}"
    );
    // No Filter.db file should exist anywhere in the output directory.
    let filter_present = std::fs::read_dir(&output_dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| e.file_name().to_string_lossy().ends_with("Filter.db"));
    assert!(
        !filter_present,
        "no Filter.db file may be published for a disabled filter"
    );

    // The merged output must still be readable.
    let cqlite_config = Config::default();
    let manager = rt.block_on(async {
        let platform = Arc::new(Platform::new(&cqlite_config).await.expect("platform"));
        SSTableManager::new(
            &output_dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager opens the compacted output")
    });
    let table_id = CqlTableId::from("compact_ks.items");
    let results = rt
        .block_on(manager.scan(&table_id, None, None, None, Some(&schema)))
        .expect("post-compaction scan");
    assert_eq!(results.len(), 15, "merged output must contain 15 rows");
}

// ── Clustering-key regression (#857) ────────────────────────────────────────

fn make_clustering_schema() -> TableSchema {
    TableSchema {
        keyspace: "compact_ks".to_string(),
        table: "items".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "v".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn write_clustered_row(id: i32, ck: i32, v: &str, timestamp: i64) -> Mutation {
    Mutation::new(
        TableId::new("compact_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "v".to_string(),
            value: Value::Text(v.to_string()),
        }],
        timestamp,
        None,
    )
}

/// Regression for #857: compacting a table WITH clustering columns must produce a
/// valid SSTable. cqlite previously left the clustering column inside the row's
/// cells, so the writer emitted it a second time as a phantom regular cell — which
/// corrupted the row body (Cassandra's sstabledump and cqlite's own reader both
/// failed; the read-back returned 0 rows). Multiple clustering rows in one
/// partition exercise the wide-row path.
#[test]
fn compact_clustering_table_preserves_rows_and_lww() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let temp = TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let output_dir = temp.path().join("out");
    let schema = make_clustering_schema();

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine creation");

    // SSTable A (ts=1000): partition id=1, clustering rows ck=0,1,2.
    for ck in 0_i32..=2 {
        engine
            .write(write_clustered_row(1, ck, &format!("a{ck}"), 1000))
            .expect("write A");
    }
    rt.block_on(engine.flush())
        .expect("flush A")
        .expect("info A");

    // SSTable B (ts=2000): overrides ck=1,2 and adds ck=3.
    for ck in 1_i32..=3 {
        engine
            .write(write_clustered_row(1, ck, &format!("b{ck}"), 2000))
            .expect("write B");
    }
    rt.block_on(engine.flush())
        .expect("flush B")
        .expect("info B");

    drop(engine);

    let inputs = discover_inputs(&data_dir);
    assert_eq!(inputs.len(), 2, "expected 2 input SSTables, got {inputs:?}");

    let report = rt
        .block_on(compact_sstables(
            inputs,
            &output_dir,
            &schema,
            9,
            None,
            None,
        ))
        .expect("compaction must succeed");
    assert_eq!(report.stats.output_partitions, 1, "single partition id=1");

    // Re-open and scan: the merged partition must have 4 clustering rows with LWW.
    let cqlite_config = Config::default();
    let manager = rt.block_on(async {
        let platform = Arc::new(Platform::new(&cqlite_config).await.expect("platform"));
        SSTableManager::new(
            &output_dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager opens the compacted clustering output")
    });

    let table_id = CqlTableId::from("compact_ks.items");
    let results = rt
        .block_on(manager.scan(&table_id, None, None, None, Some(&schema)))
        .expect("post-compaction scan");

    assert_eq!(
        results.len(),
        4,
        "merged partition must have 4 clustering rows (ck=0..=3), got {}",
        results.len()
    );

    // Newest write wins per clustering row: ck0=a0, ck1=b1, ck2=b2, ck3=b3.
    let rendered = format!("{:?}", results.iter().map(|(_, v)| v).collect::<Vec<_>>());
    for expected in ["a0", "b1", "b2", "b3"] {
        assert!(
            rendered.contains(expected),
            "merged output must contain {expected}; shadowed a1/a2 must not win. got {rendered}"
        );
    }
    for shadowed in ["a1", "a2"] {
        assert!(
            !rendered.contains(shadowed),
            "{shadowed} was overwritten at ts=2000 and must not appear; got {rendered}"
        );
    }
}
