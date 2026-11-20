//! Issue #4193 independent-review fixes, proven on Cassandra-written fixtures:
//!
//! * **M3** — clustering-key pseudo-cells never appear in the decision trail.
//! * **L7** — a partition well PAST `gc_grace` (expired tombstones, `test_tomb`): explain's
//!   compaction-posture merge equals what a read returns, both directions (#3890).
//!
//! (M1's untraced-allocation proof lives beside `ReconcileState` — it needs `pub(crate)` access —
//! and L5's probe mapping next to `point_read.rs`.)

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::schema::cql_parser::{
    classify_statement, parse_create_table, split_cql_statements, StatementType,
};
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::partition_key_codec::encode_partition_key_columns;
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::write_engine::merge::trace::{
    CellDecision, RecordingSink, TombstoneRecord, Verdict,
};
use cqlite_core::storage::write_engine::merge::{
    build_single_partition_merger_with_trace, effective_compaction_schema, MergeStep,
};
use cqlite_core::types::Value;
use cqlite_core::Config;

#[path = "support/datasets_root.rs"]
mod datasets_root;

fn load_table_schema(cql_path: &Path, keyspace: &str, table: &str) -> TableSchema {
    let content = std::fs::read_to_string(cql_path).expect("read schema");
    for statement in split_cql_statements(&content) {
        if !matches!(classify_statement(&statement), StatementType::CreateTable) {
            continue;
        }
        let (_, mut schema) = parse_create_table(&statement).expect("parse CREATE TABLE");
        if schema.table.eq_ignore_ascii_case(table) {
            schema.keyspace = keyspace.to_string();
            return schema;
        }
    }
    panic!(
        "table {keyspace}.{table} not found in {}",
        cql_path.display()
    );
}

/// Newest-to-oldest `nb-N-big-Data.db` under `dir` (run_index 0 = newest).
fn generations_newest_first(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = std::fs::read_dir(dir)
        .expect("read_dir")
        .flatten()
        .filter_map(|e| {
            let path = e.path();
            let name = path.file_name()?.to_str()?.to_string();
            let generation = name
                .strip_prefix("nb-")?
                .strip_suffix("-big-Data.db")?
                .parse::<u64>()
                .ok()?;
            Some((generation, path))
        })
        .collect();
    found.sort_by_key(|(g, _)| std::cmp::Reverse(*g));
    found.into_iter().map(|(_, p)| p).collect()
}

struct Fixture {
    root: PathBuf,
    dir: PathBuf,
    schema_file: &'static str,
    keyspace: &'static str,
    table: &'static str,
    gc_grace: i64,
}

impl Fixture {
    fn new(
        keyspace: &'static str,
        table: &'static str,
        schema_file: &'static str,
        gc: i64,
    ) -> Self {
        let root = datasets_root::sstables_root_for_table(keyspace, table)
            .unwrap_or_else(|| panic!("{}", datasets_root::describe_search(keyspace, table)));
        let dir = datasets_root::table_generation_dirs(&root, keyspace, table)
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("no usable {keyspace}.{table} generation dir"));
        Self {
            root,
            dir,
            schema_file,
            keyspace,
            table,
            gc_grace: gc,
        }
    }

    fn schema(&self) -> TableSchema {
        let path = datasets_root::schema_path(self.schema_file).expect("committed schema");
        load_table_schema(&path, self.keyspace, self.table)
    }

    /// Full-compaction-posture explain (every generation, purge-safe) for partition `id`.
    fn explain(&self, id: i32, now: i64) -> (Vec<CellDecision>, Vec<TombstoneRecord>) {
        let schema = self.schema();
        let paths = generations_newest_first(&self.dir);
        let eff = effective_compaction_schema(&schema, &paths);
        let key = encode_partition_key_columns(&[Value::Integer(id)], &eff).expect("key");
        let mut merger = build_single_partition_merger_with_trace(
            paths,
            &[key],
            &eff,
            ScanCancel::default(),
            RecordingSink::new(),
        )
        .expect("build traced merger")
        .unwrap_or_else(|| panic!("partition {id} must be held by a generation"))
        .with_now_secs(Some(now))
        .with_gc_before_secs(Some(now - self.gc_grace))
        .with_purge_safe(true);
        while !matches!(merger.step().expect("step"), MergeStep::Complete) {}
        let (cells, tombs, _probes) = merger.into_trace_sink().into_parts();
        (cells, tombs)
    }
}

fn ck_of(c: &CellDecision) -> Option<i64> {
    match c.clustering.as_ref()?.columns.first()?.1 {
        Value::Integer(v) => Some(i64::from(v)),
        ref other => panic!("unexpected clustering value {other:?}"),
    }
}

/// M3: across every `test_explain.trace_decisions` partition (a clustered table written by
/// Cassandra), no trace cell is a key column — the clustering column `ck` and the partition key
/// `id` are retained in the cell list only for read-back and are not data.
#[test]
fn key_pseudo_cells_never_appear_in_the_trail() {
    let fx = Fixture::new(
        "test_explain",
        "trace_decisions",
        "explain-trace.cql",
        864_000,
    );
    let mut traced_cells = 0;
    for id in 1..=8 {
        let (cells, _) = fx.explain(id, 1_794_535_358);
        for c in &cells {
            assert!(
                c.column != "ck" && c.column != "id",
                "key column `{}` leaked into the trail for partition {id}: {c:?}",
                c.column
            );
        }
        traced_cells += cells.len();
    }
    assert!(
        traced_cells > 8,
        "non-vacuous: real data cells were traced ({traced_cells})"
    );
}

async fn select_rows(fx: &Fixture, id: i32, mode: ReadPathMode) -> BTreeMap<(i64, String), Value> {
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    let result = ingest(IngestionConfig {
        schema_paths: vec![datasets_root::schema_path(fx.schema_file).expect("schema")],
        data_dir: fx.root.clone(),
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{}/{}", fx.keyspace, fx.table)),
    })
    .await
    .expect("ingest");
    let rows = result
        .database
        .execute(&format!(
            "SELECT * FROM {}.{} WHERE pk = {id}",
            fx.keyspace, fx.table
        ))
        .await
        .expect("SELECT")
        .rows;
    let mut out = BTreeMap::new();
    for row in rows {
        let ck = match row.values.get("ck") {
            Some(Value::Integer(v)) => i64::from(*v),
            other => panic!("row without an integer ck: {other:?}"),
        };
        for (col, v) in &row.values {
            if matches!(col.as_ref(), "pk" | "ck") || matches!(v, Value::Null) {
                continue;
            }
            out.insert((ck, col.to_string()), v.clone());
        }
    }
    out
}

/// L7: `test_tomb.resurrection_gc_positive` (Cassandra-written, 2 generations; gen 2 carries a
/// row tombstone + a cell tombstone for pk 1 and a partition tombstone for pk 2, all with
/// localDeletionTime 2026-11-11T00:53:44Z = 1794358424, gc_grace 864000). Explained 30 days past
/// gc_grace — every tombstone expired — the compaction-posture winners must equal what a read
/// returns for the partition, in both directions, on both read paths (auto, full).
#[tokio::test]
async fn expired_tombstones_explain_equals_read() {
    const LDT: i64 = 1_794_358_424;
    let now = LDT + 864_000 + 30 * 86_400;
    let fx = Fixture::new(
        "test_tomb",
        "resurrection_gc_positive",
        "tombstone-parity.cql",
        864_000,
    );
    for pk in [1, 2] {
        let (cells, tombs) = fx.explain(pk, now);
        assert!(
            tombs.iter().all(|t| t.droppable_at_now),
            "pk {pk}: every tombstone is past gc_grace, so droppable: {tombs:?}"
        );
        assert!(
            !tombs.is_empty(),
            "pk {pk}: the fixture's tombstones are traced"
        );
        let mut winners: BTreeMap<(i64, String), Value> = BTreeMap::new();
        for c in cells.iter().filter(|c| c.verdict == Verdict::Winner) {
            let v = c.value.clone().expect("a winner carries a value");
            winners.insert((ck_of(c).expect("clustered"), c.column.clone()), v);
        }
        if pk == 1 {
            // ck 1, 4, 5 survive (ck 2 row-deleted, ck 3 `val` cell-deleted): non-vacuous.
            assert!(winners.keys().any(|(ck, _)| *ck == 1), "{winners:?}");
        } else {
            assert!(winners.is_empty(), "pk 2 is wholly deleted: {winners:?}");
        }
        for (mode, name) in [
            // Auto, not forced Point: a table with tombstones fails a forced Point closed
            // (`tombstones_build_no_prune`); Auto is the path users actually get.
            (ReadPathMode::Auto, "auto"),
            (ReadPathMode::Full, "full"),
        ] {
            let read = select_rows(&fx, pk, mode).await;
            assert_eq!(
                winners, read,
                "pk {pk} ({name}): explain winners (left) must equal the read row set (right)"
            );
        }
    }
}
