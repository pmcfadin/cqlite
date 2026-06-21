//! Integration test: static-row presence is read from the input SSTable headers,
//! not the current schema only (Issue #850, Cassandra ref cb34ad47).
//!
//! Scenario (the #850 data-loss bug):
//!   1. Two SSTables are written while the table still HAS a static column `s`.
//!      Their Statistics.db SerializationHeaders record `s` as static.
//!   2. The static column is later DROPPED from the schema.
//!   3. Compaction runs under the dropped schema.
//!
//! Pre-#850 the writer derived static-column presence from the current schema
//! only (`data_writer.rs`: `schema_has_static`), so the compacted output dropped
//! the static-row prelude entirely — divergence / data loss versus Cassandra,
//! which reads static presence from the input headers.
//!
//! The fix builds an *effective compaction schema* by unioning the input
//! SSTable headers' static columns back onto the current schema, so the merger
//! decodes the static cells and the writer emits the static prelude. This test
//! drives the full WriteEngine compaction path and asserts that the compacted
//! output's SerializationHeader still declares `s` as static (header-driven
//! presence) and that the partitions read back without corruption.

#![cfg(feature = "write-support")]

use std::sync::Arc;

use cqlite_core::config::Config;
use cqlite_core::platform::Platform;
use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::SSTableManager;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, STCSPolicy, TableId, WriteEngine,
    WriteEngineConfig,
};
use cqlite_core::types::{TableId as CqlTableId, Value};
use std::collections::HashMap;
use tempfile::TempDir;

const KS: &str = "static850";
const TBL: &str = "t";

fn schema_with_static() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: Default::default(),
        }],
        columns: vec![
            Column {
                name: "s".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: true,
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

fn schema_dropped_static() -> TableSchema {
    let mut s = schema_with_static();
    s.columns.retain(|c| c.name != "s");
    s
}

/// A mutation that writes the static column `s` and a clustering row cell `v`.
fn write_static_and_row(pk: i32, s_val: &str, v_val: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("pk", Value::Integer(pk)),
        Some(ClusteringKey::single("ck", Value::Integer(1))),
        vec![
            CellOperation::Write {
                column: "s".to_string(),
                value: Value::Text(s_val.to_string()),
            },
            CellOperation::Write {
                column: "v".to_string(),
                value: Value::Text(v_val.to_string()),
            },
        ],
        ts,
        None,
    )
}

#[test]
fn static_prelude_survives_compaction_after_static_column_dropped() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let temp_dir = TempDir::new().expect("tempdir");
    let data_dir = temp_dir.path().join("data");
    let wal_dir = temp_dir.path().join("wal");

    // ── Phase 1: write two SSTables WHILE the table still has the static column.
    {
        let config =
            WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema_with_static());
        let mut engine = WriteEngine::new(config).expect("engine (with static)");

        engine
            .write(write_static_and_row(1, "static-1", "row-1", 100))
            .expect("write pk=1");
        let info1 = rt
            .block_on(engine.flush())
            .expect("flush 1")
            .expect("info 1");
        assert!(info1.partition_count > 0, "SSTable 1 must be non-empty");

        engine
            .write(write_static_and_row(2, "static-2", "row-2", 200))
            .expect("write pk=2");
        let info2 = rt
            .block_on(engine.flush())
            .expect("flush 2")
            .expect("info 2");
        assert!(info2.partition_count > 0, "SSTable 2 must be non-empty");

        rt.block_on(engine.close()).expect("close engine 1");
    }

    // Sanity: the input SSTables' headers must declare `s` as static, otherwise
    // the rest of the test would be vacuous.
    let input_static_cols = collect_header_static_columns(&data_dir);
    assert!(
        input_static_cols.contains(&"s".to_string()),
        "input SSTable headers must record `s` as a static column (got {:?})",
        input_static_cols
    );

    // ── Phase 2: compact under the schema with the static column DROPPED.
    {
        let config =
            WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema_dropped_static());
        let mut engine = WriteEngine::new(config).expect("engine (dropped static)");

        // Permissive STCS so the two tiny SSTables group into one bucket.
        let policy = STCSPolicy::new(2, 32, 0.01, 100.0, 0).expect("valid STCS params");
        engine
            .set_merge_policy(Box::new(policy))
            .expect("set policy");

        let budget = std::time::Duration::from_secs(30);
        let mut compacted = false;
        for _ in 0..6 {
            let report = engine.maintenance_step(budget).expect("maintenance_step");
            if !report.completed_merges.is_empty() {
                compacted = true;
                break;
            }
            if !report.pending_compaction {
                break;
            }
        }
        assert!(compacted, "compaction must complete");
        rt.block_on(engine.close()).expect("close engine 2");
    }

    // ── Assert: the compacted output's header still declares `s` as static.
    // This is the header-driven static presence the fix restores: the effective
    // compaction schema re-added `s` from the input headers and threaded it to
    // the output writer.
    let output_static_cols = collect_header_static_columns(&data_dir);
    assert!(
        output_static_cols.contains(&"s".to_string()),
        "#850 regression: after compacting under the dropped-static schema, the \
         output SSTable header dropped the static column `s` (got {:?}). Static-row \
         presence must be read from the input headers, not the current schema only.",
        output_static_cols
    );

    // ── Assert: the compacted partitions read back without corruption.
    let cqlite_config = Config::default();
    let manager = rt.block_on(async {
        let platform = Arc::new(Platform::new(&cqlite_config).await.expect("platform"));
        SSTableManager::new(
            &data_dir,
            &cqlite_config,
            platform,
            #[cfg(feature = "state_machine")]
            None,
        )
        .await
        .expect("SSTableManager open")
    });

    let table_id = CqlTableId::from(format!("{}.{}", KS, TBL).as_str());
    let results = rt
        .block_on(manager.scan(&table_id, None, None, None, Some(&schema_with_static())))
        .expect("post-compaction scan");
    assert!(
        !results.is_empty(),
        "compacted output must still return rows after a scan"
    );
}

/// Collect the names of static columns declared in every `*-Statistics.db`
/// SerializationHeader under `data_dir/<ks>/<tbl>/`.
fn collect_header_static_columns(data_dir: &std::path::Path) -> Vec<String> {
    let table_dir = data_dir.join(KS).join(TBL);
    let mut names: Vec<String> = Vec::new();
    let read_dir = match std::fs::read_dir(&table_dir) {
        Ok(rd) => rd,
        Err(_) => return names,
    };
    for entry in read_dir.flatten() {
        let path = entry.path();
        let is_stats = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.ends_with("Statistics.db"))
            .unwrap_or(false);
        if !is_stats {
            continue;
        }
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if let Ok((_, stats)) =
            cqlite_core::parser::enhanced_statistics_parser::parse_statistics_with_fallback(
                &bytes, None,
            )
        {
            for col in &stats.serialization_header_columns {
                if col.is_static && !names.contains(&col.name) {
                    names.push(col.name.clone());
                }
            }
        }
    }
    names
}
