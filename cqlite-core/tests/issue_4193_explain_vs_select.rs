//! Issue #4193, requirement R6 — the trail agrees with the answer, on both
//! read paths, in both directions (spec `cli-explain` R6.1).
//!
//! `explain`'s `winner` set must equal the row `SELECT *` returns, under
//! BOTH `CQLITE_READ_PATH=point` and `=full`, at the SAME pinned `now`. Per
//! issue #3890 ("a point-read test that compares a SUBSET of columns... cannot
//! see a truncated point row"), the comparison runs in BOTH directions: no
//! `SELECT` column may be missing a matching `winner`, and no `winner` may be
//! missing from the `SELECT` row — each failure names the column AND the
//! direction.
//!
//! Scope: the partition KEY column(s) are excluded from the comparison by
//! design, not by omission — `explain`'s `CellDecision` trail records
//! reconciled CELL versions (regular/complex columns and clustering-key
//! pseudo-cells), never the partition key itself (design.md §D2's
//! `CellDecision` has no partition-key field), while `SELECT *` always
//! projects it. Comparing it would therefore report a structural,
//! never-fixable "column-without-winner" on every case and mask a real
//! divergence in the columns that actually flow through the trace.

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use std::path::{Path, PathBuf};

use serial_test::serial;

use cqlite_core::config::ReadPathMode;
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::schema::cql_parser::{
    classify_statement, parse_create_table, split_cql_statements, StatementType,
};
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::partition_key_codec::encode_partition_key_columns;
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::write_engine::merge::trace::{RecordingSink, Verdict};
use cqlite_core::storage::write_engine::merge::{
    build_single_partition_merger_with_trace, effective_compaction_schema, MergeStep,
};
use cqlite_core::types::Value;
use cqlite_core::{Config, Database};

#[path = "support/datasets_root.rs"]
mod datasets_root;

/// Debug-only reader seam pinning read-time TTL "now" so the `SELECT` side's
/// TTL expiry matches `explain`'s pinned `now` exactly (`now_clock.rs`).
const TTL_NOW_OVERRIDE_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";
const GC_GRACE_SECONDS: i64 = 864_000;

/// RAII guard for the process-global TTL-now env var (mirrors
/// `point_vs_full_differential.rs`'s `EnvVarGuard`) — restores the previous
/// value (or unsets) on drop so this test cannot leak a pin into a sibling.
struct TtlNowGuard {
    previous: Option<std::ffi::OsString>,
}

impl TtlNowGuard {
    #[must_use = "the clock stays pinned only while the returned guard is alive"]
    fn pin(now: i64) -> Self {
        let previous = std::env::var_os(TTL_NOW_OVERRIDE_ENV);
        std::env::set_var(TTL_NOW_OVERRIDE_ENV, now.to_string());
        Self { previous }
    }
}

impl Drop for TtlNowGuard {
    fn drop(&mut self) {
        match self.previous.take() {
            Some(previous) => std::env::set_var(TTL_NOW_OVERRIDE_ENV, previous),
            None => std::env::remove_var(TTL_NOW_OVERRIDE_ENV),
        }
    }
}

fn load_table_schema(cql_path: &Path, keyspace: &str, table: &str) -> TableSchema {
    let content = std::fs::read_to_string(cql_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", cql_path.display()));
    for statement in split_cql_statements(&content) {
        if !matches!(classify_statement(&statement), StatementType::CreateTable) {
            continue;
        }
        let (_, mut schema) = parse_create_table(&statement).unwrap_or_else(|error| {
            panic!("parse CREATE TABLE in {}: {error:?}", cql_path.display())
        });
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

fn discover_generations_newest_first(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<(u64, PathBuf)> = std::fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("read_dir {}: {error}", dir.display()))
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let name = path.file_name()?.to_str()?.to_string();
            if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
                let generation = name
                    .strip_prefix("nb-")?
                    .split("-big-")
                    .next()?
                    .parse::<u64>()
                    .ok()?;
                Some((generation, path))
            } else {
                None
            }
        })
        .collect();
    found.sort_by_key(|(generation, _)| std::cmp::Reverse(*generation));
    found.into_iter().map(|(_, path)| path).collect()
}

fn trace_decisions_generation_dir() -> PathBuf {
    let root = datasets_root::sstables_root_for_table("test_explain", "trace_decisions")
        .unwrap_or_else(|| {
            panic!(
                "{}",
                datasets_root::describe_search("test_explain", "trace_decisions")
            )
        });
    datasets_root::table_generation_dirs(&root, "test_explain", "trace_decisions")
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("no usable test_explain.trace_decisions generation dir"))
}

fn trace_decisions_schema() -> TableSchema {
    let schema_path = datasets_root::schema_path("explain-trace.cql")
        .expect("committed test-data/schemas/explain-trace.cql fixture");
    load_table_schema(&schema_path, "test_explain", "trace_decisions")
}

/// Library entry point mirroring `cqlite-cli`'s `explain` command (design.md
/// §D3): full-compaction posture over EVERY generation of the table, driven
/// with a `RecordingSink`. Returns the `winner` cells' `(column, value)`
/// pairs for the row matching `clustering_ck` (or, when `None`, the whole
/// partition's winners — this fixture is single-clustering-row per id except
/// id=3, which every case below avoids).
fn explain_winners(id: i32, now: i64) -> Vec<(String, Value)> {
    let schema = trace_decisions_schema();
    let dir = trace_decisions_generation_dir();
    let paths = discover_generations_newest_first(&dir);
    assert_eq!(paths.len(), 2, "expected both generations under {dir:?}");
    let effective_schema = effective_compaction_schema(&schema, &paths);
    let key = encode_partition_key_columns(&[Value::Integer(id)], &effective_schema)
        .expect("encode partition key");
    let merger = build_single_partition_merger_with_trace(
        paths,
        &[key],
        &effective_schema,
        ScanCancel::default(),
        RecordingSink::new(),
    )
    .expect("build traced merger")
    .unwrap_or_else(|| panic!("partition id={id} must be held by at least one generation"));
    let mut merger = merger
        .with_now_secs(Some(now))
        .with_gc_before_secs(Some(now - GC_GRACE_SECONDS))
        .with_purge_safe(true);
    loop {
        match merger.step().expect("merge step") {
            MergeStep::Complete => break,
            MergeStep::Partition { .. } => {}
        }
    }
    let (cells, _tombstones, _probes) = merger.into_trace_sink().into_parts();
    cells
        .into_iter()
        .filter(|cell| cell.verdict == Verdict::Winner && cell.column != "ck")
        .map(|cell| {
            (
                cell.column,
                cell.value
                    .unwrap_or_else(|| panic!("a Winner CellDecision must carry a value")),
            )
        })
        .collect()
}

async fn open_db(root: &Path, schema: &Path, mode: ReadPathMode) -> Database {
    let mut core_config = Config::default();
    core_config.query.forced_read_path = Some(mode);
    let cfg = IngestionConfig {
        schema_paths: vec![schema.to_path_buf()],
        data_dir: root.to_path_buf(),
        version_hint: None,
        core_config,
        table_directory_filter: Some("/test_explain/".to_string()),
    };
    let result = ingest(cfg).await.expect("ingestion succeeds");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "expected at least one schema loaded"
    );
    result.database
}

/// Assert `explain`'s winners equal `SELECT *`'s row for `id` at `now`, under
/// BOTH read paths, in BOTH directions (#3890).
async fn assert_explain_matches_select(id: i32, now: i64) {
    let _clock = TtlNowGuard::pin(now);
    let root = datasets_root::sstables_root_for_table("test_explain", "trace_decisions")
        .unwrap_or_else(|| {
            panic!(
                "{}",
                datasets_root::describe_search("test_explain", "trace_decisions")
            )
        });
    let schema_path = datasets_root::schema_path("explain-trace.cql")
        .expect("committed test-data/schemas/explain-trace.cql fixture");

    let winners = explain_winners(id, now);

    for (mode, mode_name) in [(ReadPathMode::Point, "point"), (ReadPathMode::Full, "full")] {
        let db = open_db(&root, &schema_path, mode).await;
        let result = db
            .execute(&format!(
                "SELECT * FROM test_explain.trace_decisions WHERE id = {id}"
            ))
            .await
            .unwrap_or_else(|error| panic!("SELECT ({mode_name}) failed: {error}"));

        if winners.is_empty() {
            // Every non-key cell was shadowed/expired. A row with a genuine
            // clustering row marker (distinct from the expired cell's own
            // liveness) legitimately still exists — CQL returns it with the
            // non-key column(s) NULL, not absent entirely — so the correct
            // agreement is "no non-key column present", not "zero rows".
            for row in &result.rows {
                for (column, value) in &row.values {
                    assert!(
                        column.as_ref() == "id" || column.as_ref() == "ck",
                        "column-without-winner: id={id} ({mode_name}) explain has no \
                         winners but SELECT's row carries non-key column `{column}` = \
                         {value:?}"
                    );
                }
            }
            continue;
        }
        assert_eq!(
            result.rows.len(),
            1,
            "expected exactly one row for id={id} under {mode_name}, got {:?}",
            result.rows
        );
        let row = &result.rows[0];

        // Direction 1: every winner must have a matching SELECT column+value.
        for (column, value) in &winners {
            match row.values.get(column.as_str()) {
                Some(select_value) => assert_eq!(
                    select_value, value,
                    "winner-without-column: column `{column}` (id={id}, {mode_name}) has \
                     winner value {value:?} but SELECT returned {select_value:?}"
                ),
                None => panic!(
                    "winner-without-column: column `{column}` (id={id}, {mode_name}) has a \
                     winner ({value:?}) but is ABSENT from the SELECT row: {:?}",
                    row.values
                ),
            }
        }

        // Direction 2: every non-key SELECT column must have a matching winner.
        for (column, select_value) in &row.values {
            if column.as_ref() == "id" || column.as_ref() == "ck" {
                continue; // partition/clustering key columns are out of scope (see module doc)
            }
            match winners.iter().find(|(c, _)| c == column.as_ref()) {
                Some((_, winner_value)) => assert_eq!(
                    winner_value, select_value,
                    "column-without-winner: column `{column}` (id={id}, {mode_name}) SELECT \
                     value {select_value:?} disagrees with winner {winner_value:?}"
                ),
                None => panic!(
                    "column-without-winner: column `{column}` (id={id}, {mode_name}) is in the \
                     SELECT row ({select_value:?}) but has no matching winner in {winners:?}"
                ),
            }
        }
    }
}

/// Partition 1: shadowed-by-timestamp, the simplest case (no tombstones, no
/// TTL) — proves basic agreement on both read paths.
#[tokio::test]
#[serial]
async fn winners_match_select_shadowed_by_timestamp() {
    assert_explain_matches_select(1, 1_789_963_136).await;
}

/// Partition 6: a full-map complex-column overwrite, exercising the ONE case
/// where `explain`'s winner set spans a MULTI-CELL column (the map's
/// surviving element) rather than a single scalar cell.
#[tokio::test]
#[serial]
async fn winners_match_select_collection_overwrite() {
    assert_explain_matches_select(6, 1_789_963_136).await;
}

/// Partition 7: TTL expiry. Evaluated one second past the on-disk expiry
/// instant, proving the SAME pinned `now` (via `CQLITE_TTL_NOW_OVERRIDE_SECS`
/// on the SELECT side, `with_now_secs` on the explain side) makes an expired
/// cell agree as ABSENT on both surfaces — an expired winner column must
/// simply not appear in either.
#[tokio::test]
#[serial]
async fn winners_match_select_expired_cell_is_absent_on_both() {
    const EXPIRES_AT: i64 = 1_789_966_734;
    assert_explain_matches_select(7, EXPIRES_AT + 1).await;
}
