//! Issue #4193 R6 (roborev, Medium): the traced point-read builder must carry the same UDT
//! registry a schema-aware read uses, so `explain` and `SELECT` agree on a `frozen<UDT>` inside
//! a collection. Real Cassandra-written data: `test_collections.collections_with_udts`.

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use std::path::Path;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::schema::cql_parser::{
    classify_statement, parse_create_table, split_cql_statements, StatementType,
};
use cqlite_core::schema::{udt_registry_from_cql, TableSchema};
use cqlite_core::storage::partition_key_codec::encode_partition_key_columns;
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::write_engine::merge::trace::{RecordingSink, Verdict};
use cqlite_core::storage::write_engine::merge::{
    build_traced_merger_with_registry, effective_compaction_schema, MergeStep,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};

#[path = "support/datasets_root.rs"]
mod datasets_root;

const KS: &str = "test_collections";
const TABLE: &str = "collections_with_udts";

fn schema_from(cql: &str) -> TableSchema {
    for statement in split_cql_statements(cql) {
        if !matches!(classify_statement(&statement), StatementType::CreateTable) {
            continue;
        }
        let (_, mut schema) = parse_create_table(&statement).expect("parse CREATE TABLE");
        if schema.table.eq_ignore_ascii_case(TABLE) {
            schema.keyspace = KS.to_string();
            return schema;
        }
    }
    panic!("{KS}.{TABLE} not declared in collections.cql");
}

async fn select_all(
    root: &Path,
    schema_path: &Path,
) -> Vec<std::collections::BTreeMap<String, Value>> {
    let cfg = IngestionConfig {
        schema_paths: vec![schema_path.to_path_buf()],
        data_dir: root.to_path_buf(),
        version_hint: None,
        core_config: Config::default(),
        table_directory_filter: Some(format!("/{KS}/")),
    };
    let db: Database = ingest(cfg).await.expect("ingest").database;
    let result = db
        .execute(&format!("SELECT * FROM {KS}.{TABLE}"))
        .await
        .expect("SELECT");
    result
        .rows
        .iter()
        .map(|row| {
            row.values
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn explain_matches_select_for_frozen_udt_inside_collections() {
    let root = datasets_root::sstables_root_for_table(KS, TABLE)
        .unwrap_or_else(|| panic!("{}", datasets_root::describe_search(KS, TABLE)));
    let schema_path = datasets_root::schema_path("collections.cql").expect("collections.cql");
    let cql = std::fs::read_to_string(&schema_path).expect("read schema");
    let schema = schema_from(&cql);
    let registry = udt_registry_from_cql(&cql, KS);
    let generations = datasets_root::table_generation_dirs(&root, KS, TABLE);
    let dir = generations.first().expect("a generation dir");
    let paths: Vec<_> = std::fs::read_dir(dir)
        .expect("read dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.to_string_lossy().ends_with("-Data.db"))
        .collect();
    assert!(!paths.is_empty(), "fixture must hold a Data.db");
    let effective = effective_compaction_schema(&schema, &paths);

    let rows = select_all(&root, &schema_path).await;
    assert!(
        !rows.is_empty(),
        "SELECT returned 0 rows: fixture not present"
    );

    let mut compared_udt_values = 0usize;
    for row in &rows {
        let key = encode_partition_key_columns(&[row["user_id"].clone()], &effective)
            .expect("encode key");
        let run = |reg: Option<&cqlite_core::schema::UdtRegistry>| {
            let mut merger = build_traced_merger_with_registry(
                paths.clone(),
                &[key.clone()],
                &effective,
                reg,
                ScanCancel::default(),
                RecordingSink::new(),
            )
            .expect("build")
            .expect("partition present");
            loop {
                match merger.step().expect("step") {
                    MergeStep::Complete => break,
                    MergeStep::Partition { .. } => {}
                }
            }
            merger.into_trace_sink().into_parts().0
        };
        let with = run(Some(&registry));
        let without = run(None);
        for column in [
            "addresses",
            "contacts",
            "locations_visited",
            "emergency_contacts",
        ] {
            let expected: Vec<Value> = match row.get(column) {
                Some(Value::List(v)) | Some(Value::Set(v)) => v.clone(),
                Some(Value::Map(pairs)) => pairs.iter().map(|(_, v)| v.clone()).collect(),
                _ => continue,
            };
            let winners =
                |cells: &[cqlite_core::storage::write_engine::merge::trace::CellDecision]| {
                    cells
                        .iter()
                        .filter(|c| c.column == column && c.verdict == Verdict::Winner)
                        .filter_map(|c| c.value.clone())
                        .collect::<Vec<Value>>()
                };
            let with_winners = winners(&with);
            assert_eq!(with_winners.len(), expected.len(), "{column}: winner count");
            for want in &expected {
                assert!(
                    with_winners.contains(want),
                    "{column}: SELECT element {want:?} has no matching explain winner in {with_winners:?}"
                );
            }
            // Tripwire that the registry is what makes the difference on this data.
            assert!(
                winners(&without).iter().all(|v| !expected.contains(v)),
                "{column}: registry-free decode unexpectedly equals SELECT; the test would not detect a dropped registry"
            );
            compared_udt_values += expected.len();
        }
    }
    assert!(
        compared_udt_values > 0,
        "no frozen-UDT collection element was compared"
    );
}
