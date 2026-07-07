//! `sstableloader` integration tests for Issues #396 and #432.
//!
//! These tests validate the full write path:
//! 1. Create schema in a real Cassandra 5.0 node
//! 2. Write mutations with CQLite
//! 3. Flush portable SSTables
//! 4. Validate the flushed artifact with Cassandra tooling
//! 5. Package the flushed SSTables into a loader-friendly directory
//! 6. Load them with `sstableloader`
//! 7. Verify results through CQL queries
//!
//! The product contract is that flush already produces portable Cassandra
//! SSTable components. The packaging step used here is only a loader/import
//! convenience for the current test harness.

#![cfg(all(feature = "write-support", feature = "docker-integration"))]

#[path = "../../tests/helpers/docker.rs"]
mod docker_helpers;

use chrono::NaiveDate;
use cqlite_core::{
    error::{Error, Result as CqliteResult},
    schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema},
    storage::sstable::writer::SSTableInfo,
    storage::write_engine::{
        CellOperation, ClusteringKey, ExportOptions, ExportReport, Mutation, PartitionKey, TableId,
        WriteEngine, WriteEngineConfig,
    },
    types::Value,
};
use docker_helpers::{CassandraContainer, CqlshOutput};
use num_bigint::BigInt;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::Mutex;
use uuid::Uuid;

const KEYSPACE: &str = "sstableloader_test";

struct LoaderHarness {
    _work_dir: TempDir,
    package_dir: TempDir,
    engine: Arc<Mutex<WriteEngine>>,
    cassandra: CassandraContainer,
    keyspace: String,
    table: String,
}

impl LoaderHarness {
    async fn new(schema: TableSchema, create_table_cql: &str) -> CqliteResult<Option<Self>> {
        let Some(cassandra) = maybe_start_cassandra()? else {
            return Ok(None);
        };

        ensure_keyspace(&cassandra, &schema.keyspace)?;
        recreate_table(
            &cassandra,
            &schema.keyspace,
            &schema.table,
            create_table_cql,
        )?;

        let work_dir = TempDir::new().map_err(io_to_cqlite)?;
        let package_dir = TempDir::new().map_err(io_to_cqlite)?;
        let config = WriteEngineConfig::new(
            work_dir.path().join("data"),
            work_dir.path().join("wal"),
            schema.clone(),
        );

        let engine = WriteEngine::new(config)?;

        Ok(Some(Self {
            _work_dir: work_dir,
            package_dir,
            engine: Arc::new(Mutex::new(engine)),
            cassandra,
            keyspace: schema.keyspace,
            table: schema.table,
        }))
    }

    async fn write(&self, mutation: Mutation) -> CqliteResult<()> {
        let mut engine = self.engine.lock().await;
        engine.write_async(mutation).await
    }

    async fn flush_portable_sstable(&self) -> CqliteResult<SSTableInfo> {
        let info = {
            let mut engine = self.engine.lock().await;
            engine.flush().await?
        }
        .ok_or_else(|| {
            Error::Storage(format!(
                "Flush produced no SSTable for {}",
                self.fully_qualified_table()
            ))
        })?;

        validate_flushed_components(&info)?;
        Ok(info)
    }

    fn sstabledump_flushed_artifact(&self, info: &SSTableInfo) -> CqliteResult<JsonValue> {
        let local_dir = info.data_path.parent().ok_or_else(|| {
            Error::Storage(format!(
                "Flushed Data.db for {} has no parent directory",
                self.fully_qualified_table()
            ))
        })?;
        let data_file_name = info
            .data_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                Error::Storage(format!(
                    "Flushed Data.db path is not valid UTF-8: {}",
                    info.data_path.display()
                ))
            })?;

        let result = self
            .cassandra
            .run_sstabledump(local_dir, data_file_name)
            .map_err(io_to_cqlite)?;
        if !result.is_successful() {
            return Err(Error::Storage(format!(
                "Flushed artifact validation failed for {}: {}",
                self.fully_qualified_table(),
                result.summary()
            )));
        }

        serde_json::from_str::<JsonValue>(&result.stdout).map_err(|err| {
            Error::Storage(format!(
                "Failed to parse sstabledump JSON for {}: {err}\nOutput:\n{}",
                self.fully_qualified_table(),
                result.stdout
            ))
        })
    }

    async fn package_flushed_sstable(&self) -> CqliteResult<ExportReport> {
        let report = {
            let mut engine = self.engine.lock().await;
            engine
                .export_sstable(
                    self.package_dir.path(),
                    ExportOptions::new(&self.keyspace, &self.table, 1),
                )
                .await?
        };

        report.validate_components()?;
        Ok(report)
    }

    fn import_packaged_sstable(&self, report: &ExportReport) -> CqliteResult<()> {
        let result = self
            .cassandra
            .run_sstableloader(&report.output_path, &self.keyspace, &self.table)
            .map_err(io_to_cqlite)?;

        assert!(
            result.is_successful(),
            "sstableloader failed for {}.{}: {}",
            self.keyspace,
            self.table,
            result.summary()
        );

        Ok(())
    }

    async fn package_for_loader_import(&self) -> CqliteResult<()> {
        let info = self.flush_portable_sstable().await?;
        let _artifact = self.sstabledump_flushed_artifact(&info)?;
        let report = self.package_flushed_sstable().await?;
        self.import_packaged_sstable(&report)
    }

    fn query(&self, query: &str) -> CqliteResult<CqlshOutput> {
        self.cassandra.execute_cql(query).map_err(io_to_cqlite)
    }

    fn query_until<F>(
        &self,
        query: &str,
        timeout: Duration,
        predicate: F,
    ) -> CqliteResult<CqlshOutput>
    where
        F: Fn(&CqlshOutput) -> bool,
    {
        let start = Instant::now();
        let mut last_output = None;

        let mut last_error = None;

        while start.elapsed() < timeout {
            match self.query(query) {
                Ok(output) => {
                    if predicate(&output) {
                        return Ok(output);
                    }
                    last_output = Some(output);
                }
                Err(err) if is_retryable_query_error(&err) => {
                    last_error = Some(err.to_string());
                }
                Err(err) => {
                    return Err(self.enrich_query_failure(query, err));
                }
            }
            std::thread::sleep(Duration::from_millis(250));
        }

        Err(Error::Storage(format!(
            "Timed out waiting for query to satisfy predicate: `{}`. Last output: {:?}. Last retryable error: {:?}",
            query, last_output, last_error
        )))
    }

    fn fully_qualified_table(&self) -> String {
        format!("{}.{}", self.keyspace, self.table)
    }

    fn enrich_query_failure(&self, query: &str, err: Error) -> Error {
        let mut message = format!(
            "Query `{query}` failed against imported table {}: {}",
            self.fully_qualified_table(),
            err
        );

        if let Ok(components) = self
            .cassandra
            .list_table_components(&self.keyspace, &self.table)
        {
            let trimmed = components.trim();
            if !trimmed.is_empty() {
                message.push_str("\nImported table components:\n");
                message.push_str(trimmed);
            }
        }

        if let Ok(log_tail) = self.cassandra.tail_system_log(80) {
            let trimmed = log_tail.trim();
            if !trimmed.is_empty() {
                message.push_str("\nRecent Cassandra system.log:\n");
                message.push_str(trimmed);
            }
        }

        Error::Storage(message)
    }
}

fn validate_flushed_components(info: &SSTableInfo) -> CqliteResult<()> {
    let required = [
        &info.data_path,
        info.index_path.as_ref().unwrap(),
        info.summary_path.as_ref().unwrap(),
        &info.stats_path,
        &info.toc_path,
        &info.digest_path,
    ];

    for path in required {
        if !path.exists() {
            return Err(Error::Storage(format!(
                "Missing flushed SSTable component: {}",
                path.display()
            )));
        }
    }

    // Filter.db is optional (disabled bloom filter omits it, Issue #852).
    if let Some(path) = &info.filter_path {
        if !path.exists() {
            return Err(Error::Storage(format!(
                "Missing flushed Filter.db: {}",
                path.display()
            )));
        }
    }

    if let Some(path) = &info.compression_info_path {
        if !path.exists() {
            return Err(Error::Storage(format!(
                "Missing flushed CompressionInfo.db: {}",
                path.display()
            )));
        }
    }

    Ok(())
}

// =============================================================================
// Tier 1: sstableloader Acceptance
// =============================================================================

#[tokio::test]
async fn test_sstableloader_simple_table() -> CqliteResult<()> {
    let Some(harness) = LoaderHarness::new(
        create_simple_schema("loader_simple"),
        &create_simple_table_cql("loader_simple"),
    )
    .await?
    else {
        return Ok(());
    };

    let rows = [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 300)];
    for (id, name, value) in rows {
        harness
            .write(simple_mutation(
                "loader_simple",
                id,
                name,
                value,
                1_704_067_200_000_000,
            ))
            .await?;
    }

    harness.package_for_loader_import().await?;

    let count_query = format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table());
    let output = harness.query_until(&count_query, Duration::from_secs(15), |out| {
        parse_count(out).ok() == Some(3)
    })?;
    assert_eq!(parse_count(&output)?, 3);

    Ok(())
}

#[tokio::test]
async fn test_sstableloader_clustering_table() -> CqliteResult<()> {
    let Some(harness) = LoaderHarness::new(
        create_clustered_schema("loader_clustered"),
        &create_clustered_table_cql("loader_clustered"),
    )
    .await?
    else {
        return Ok(());
    };

    for index in 0..5 {
        harness
            .write(clustered_mutation(
                "loader_clustered",
                7,
                &format!("row_{index:03}"),
                &format!("value_{index}"),
                1_704_067_200_000_000 + index as i64,
            ))
            .await?;
    }

    harness.package_for_loader_import().await?;

    let query = format!(
        "SELECT COUNT(*) FROM {} WHERE pk = 7",
        harness.fully_qualified_table()
    );
    let output = harness.query_until(&query, Duration::from_secs(15), |out| {
        parse_count(out).ok() == Some(5)
    })?;
    assert_eq!(parse_count(&output)?, 5);

    Ok(())
}

#[tokio::test]
async fn test_sstableloader_multiple_partitions() -> CqliteResult<()> {
    let Some(harness) = LoaderHarness::new(
        create_simple_schema("loader_multi_partition"),
        &create_simple_table_cql("loader_multi_partition"),
    )
    .await?
    else {
        return Ok(());
    };

    let rows = [
        (42, "Zoe", 4200),
        (1, "Alice", 100),
        (100, "Max", 1000),
        (7, "Drew", 700),
    ];
    for (id, name, value) in rows {
        harness
            .write(simple_mutation(
                "loader_multi_partition",
                id,
                name,
                value,
                1_704_067_200_000_000 + id as i64,
            ))
            .await?;
    }

    harness.package_for_loader_import().await?;

    let query = format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table());
    let output = harness.query_until(&query, Duration::from_secs(15), |out| {
        parse_count(out).ok() == Some(4)
    })?;
    assert_eq!(parse_count(&output)?, 4);

    Ok(())
}

#[tokio::test]
async fn test_sstableloader_all_stage0_types() -> CqliteResult<()> {
    let Some(harness) = LoaderHarness::new(
        create_types_schema("loader_all_types"),
        &create_types_table_cql("loader_all_types"),
    )
    .await?
    else {
        return Ok(());
    };

    harness
        .write(types_mutation(
            "loader_all_types",
            1,
            "all-types-row",
            1_704_067_200_000_000,
        ))
        .await?;

    harness.package_for_loader_import().await?;

    let query = format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table());
    let output = harness.query_until(&query, Duration::from_secs(15), |out| {
        parse_count(out).ok() == Some(1)
    })?;
    assert_eq!(parse_count(&output)?, 1);

    Ok(())
}

// =============================================================================
// Tier 2: CQL Query Verification
// =============================================================================

#[tokio::test]
async fn test_sstableloader_select_all_returns_written_rows() -> CqliteResult<()> {
    let Some(harness) = LoaderHarness::new(
        create_simple_schema("loader_select_all"),
        &create_simple_table_cql("loader_select_all"),
    )
    .await?
    else {
        return Ok(());
    };

    let expected = vec![
        vec!["1".to_string(), "Alice".to_string(), "100".to_string()],
        vec!["2".to_string(), "Bob".to_string(), "200".to_string()],
        vec!["3".to_string(), "Charlie".to_string(), "300".to_string()],
    ];

    for row in &expected {
        harness
            .write(simple_mutation(
                "loader_select_all",
                row[0].parse().unwrap(),
                &row[1],
                row[2].parse().unwrap(),
                1_704_067_200_000_000,
            ))
            .await?;
    }

    harness.package_for_loader_import().await?;

    let query = format!(
        "SELECT id, name, value FROM {}",
        harness.fully_qualified_table()
    );
    let output = harness.query_until(&query, Duration::from_secs(15), |out| out.rows.len() == 3)?;
    assert_rows_unordered_eq(output.rows, expected);

    Ok(())
}

#[tokio::test]
async fn test_sstableloader_where_on_partition_key() -> CqliteResult<()> {
    let Some(harness) = LoaderHarness::new(
        create_simple_schema("loader_where_partition"),
        &create_simple_table_cql("loader_where_partition"),
    )
    .await?
    else {
        return Ok(());
    };

    for (id, name, value) in [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 300)] {
        harness
            .write(simple_mutation(
                "loader_where_partition",
                id,
                name,
                value,
                1_704_067_200_000_000 + id as i64,
            ))
            .await?;
    }

    harness.package_for_loader_import().await?;

    let query = format!(
        "SELECT id, name, value FROM {} WHERE id = 2",
        harness.fully_qualified_table()
    );
    let output = harness.query_until(&query, Duration::from_secs(15), |out| out.rows.len() == 1)?;
    assert_eq!(
        output.rows,
        vec![vec!["2".to_string(), "Bob".to_string(), "200".to_string()]]
    );

    Ok(())
}

#[tokio::test]
async fn test_sstableloader_where_on_clustering_key() -> CqliteResult<()> {
    let Some(harness) = LoaderHarness::new(
        create_clustered_schema("loader_where_clustering"),
        &create_clustered_table_cql("loader_where_clustering"),
    )
    .await?
    else {
        return Ok(());
    };

    for index in 0..4 {
        harness
            .write(clustered_mutation(
                "loader_where_clustering",
                11,
                &format!("row_{index:03}"),
                &format!("value_{index}"),
                1_704_067_200_000_000 + index as i64,
            ))
            .await?;
    }

    harness.package_for_loader_import().await?;

    let query = format!(
        "SELECT pk, ck, data FROM {} WHERE pk = 11 AND ck = 'row_002'",
        harness.fully_qualified_table()
    );
    let output = harness.query_until(&query, Duration::from_secs(15), |out| out.rows.len() == 1)?;
    assert_eq!(
        output.rows,
        vec![vec![
            "11".to_string(),
            "row_002".to_string(),
            "value_2".to_string()
        ]]
    );

    Ok(())
}

#[tokio::test]
async fn test_sstableloader_stage0_types_round_trip_values() -> CqliteResult<()> {
    let Some(harness) = LoaderHarness::new(
        create_types_schema("loader_types_round_trip"),
        &create_types_table_cql("loader_types_round_trip"),
    )
    .await?
    else {
        return Ok(());
    };

    harness
        .write(types_mutation(
            "loader_types_round_trip",
            1,
            "all-types-row",
            1_704_067_200_000_000,
        ))
        .await?;

    harness.package_for_loader_import().await?;

    let query = format!(
        "SELECT pk, ck, text_col, int_col, bigint_col, boolean_col, toUnixTimestamp(timestamp_col) AS ts_ms, uuid_col FROM {} WHERE pk = 1 AND ck = 'all-types-row'",
        harness.fully_qualified_table()
    );
    let output = harness.query_until(&query, Duration::from_secs(15), |out| out.rows.len() == 1)?;
    assert_eq!(
        output.rows,
        vec![vec![
            "1".to_string(),
            "all-types-row".to_string(),
            "stage0".to_string(),
            "42".to_string(),
            "9223372036".to_string(),
            "True".to_string(),
            "1704067200000".to_string(),
            "12345678-9abc-4def-8123-456789abcdef".to_string(),
        ]]
    );

    Ok(())
}

// =============================================================================
// Tier 3: Stress Cases
// =============================================================================

#[tokio::test]
async fn test_sstableloader_large_partition_1000_rows() -> CqliteResult<()> {
    let Some(harness) = LoaderHarness::new(
        create_clustered_schema("loader_large_partition"),
        &create_clustered_table_cql("loader_large_partition"),
    )
    .await?
    else {
        return Ok(());
    };

    for index in 0..1000 {
        harness
            .write(clustered_mutation(
                "loader_large_partition",
                1,
                &format!("row_{index:04}"),
                &format!("value_{index}"),
                1_704_067_200_000_000 + index as i64,
            ))
            .await?;
    }

    harness.package_for_loader_import().await?;

    let query = format!(
        "SELECT COUNT(*) FROM {} WHERE pk = 1",
        harness.fully_qualified_table()
    );
    let output = harness.query_until(&query, Duration::from_secs(20), |out| {
        parse_count(out).ok() == Some(1000)
    })?;
    assert_eq!(parse_count(&output)?, 1000);

    Ok(())
}

#[tokio::test]
async fn test_sstableloader_many_partitions_100_distinct() -> CqliteResult<()> {
    let Some(harness) = LoaderHarness::new(
        create_simple_schema("loader_many_partitions"),
        &create_simple_table_cql("loader_many_partitions"),
    )
    .await?
    else {
        return Ok(());
    };

    for id in 0..100 {
        harness
            .write(simple_mutation(
                "loader_many_partitions",
                id,
                &format!("user_{id}"),
                id,
                1_704_067_200_000_000 + id as i64,
            ))
            .await?;
    }

    harness.package_for_loader_import().await?;

    let query = format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table());
    let output = harness.query_until(&query, Duration::from_secs(20), |out| {
        parse_count(out).ok() == Some(100)
    })?;
    assert_eq!(parse_count(&output)?, 100);

    Ok(())
}

#[tokio::test]
async fn test_sstableloader_concurrent_writes_followed_by_load() -> CqliteResult<()> {
    let Some(harness) = LoaderHarness::new(
        create_simple_schema("loader_concurrent"),
        &create_simple_table_cql("loader_concurrent"),
    )
    .await?
    else {
        return Ok(());
    };

    let mut tasks = Vec::new();
    for worker in 0..4 {
        let engine = Arc::clone(&harness.engine);
        tasks.push(tokio::spawn(async move {
            for offset in 0..25 {
                let id = worker * 25 + offset;
                let mutation = simple_mutation(
                    "loader_concurrent",
                    id,
                    &format!("worker_{worker}_user_{offset}"),
                    id,
                    1_704_067_200_000_000 + id as i64,
                );
                let mut guard = engine.lock().await;
                guard.write_async(mutation).await?;
            }
            Ok::<(), Error>(())
        }));
    }

    for task in tasks {
        task.await
            .map_err(|err| Error::Storage(format!("concurrent write task failed: {err}")))??;
    }

    harness.package_for_loader_import().await?;

    let query = format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table());
    let output = harness.query_until(&query, Duration::from_secs(20), |out| {
        parse_count(out).ok() == Some(100)
    })?;
    assert_eq!(parse_count(&output)?, 100);

    Ok(())
}

// =============================================================================
// Issue #432: Broad Primitive-Type Write Compatibility Coverage
// =============================================================================

async fn run_issue_432_scenario<ArtifactCheck, QueryCheck>(
    schema: TableSchema,
    create_table_cql: &str,
    mutations: Vec<Mutation>,
    artifact_check: ArtifactCheck,
    query_check: QueryCheck,
) -> CqliteResult<()>
where
    ArtifactCheck: Fn(&JsonValue) -> CqliteResult<()>,
    QueryCheck: Fn(&LoaderHarness) -> CqliteResult<()>,
{
    let Some(harness) = LoaderHarness::new(schema, create_table_cql).await? else {
        return Ok(());
    };

    for mutation in mutations {
        harness
            .write(mutation)
            .await
            .map_err(|err| stage_error("write", &harness.fully_qualified_table(), err))?;
    }

    let info = harness
        .flush_portable_sstable()
        .await
        .map_err(|err| stage_error("flush", &harness.fully_qualified_table(), err))?;
    let dump = harness
        .sstabledump_flushed_artifact(&info)
        .map_err(|err| stage_error("artifact", &harness.fully_qualified_table(), err))?;
    artifact_check(&dump)
        .map_err(|err| stage_error("artifact", &harness.fully_qualified_table(), err))?;

    let report = harness
        .package_flushed_sstable()
        .await
        .map_err(|err| stage_error("packaging", &harness.fully_qualified_table(), err))?;
    harness
        .import_packaged_sstable(&report)
        .map_err(|err| stage_error("import", &harness.fully_qualified_table(), err))?;
    query_check(&harness).map_err(|err| stage_error("query", &harness.fully_qualified_table(), err))
}

#[tokio::test]
async fn test_issue_432_simple_table_portable_sstable_round_trip() -> CqliteResult<()> {
    let row1_id = "11111111-1111-4111-8111-111111111111";
    let row2_id = "22222222-2222-4222-8222-222222222222";
    let row3_id = "33333333-3333-4333-8333-333333333333";

    run_issue_432_scenario(
        schema_simple_table(),
        &create_simple_table_issue_432_cql(),
        vec![
            simple_table_mutation(
                row1_id,
                "Alice Primitive",
                31,
                123_456,
                1.75,
                68.5,
                true,
                "2024-01-15T12:34:56.789Z",
                "2024-01-15",
                "09:15:30.123456789",
                &[0x00, 0x01, 0x02, 0xFF, 0x10],
                decimal_from_i64(1_234_567, 2),
                "b40eb2a0-1b7f-11ef-8000-000000000001",
                &[192, 168, 1, 44],
                -5,
                1200,
                duration_value(2, 3, 4_001),
                "varchar-alpha",
                "ASCII-ALPHA",
                1_704_072_896_789_000,
            ),
            simple_table_mutation(
                row2_id,
                "Bob Primitive",
                0,
                -9_999,
                1.5,
                82.25,
                false,
                "2024-02-20T08:00:00.000Z",
                "2024-02-20",
                "08:00:00.000000001",
                &[0x10, 0x20, 0x30],
                decimal_from_i64(-765_432, 3),
                "b40eb2a1-1b7f-11ef-8000-000000000002",
                &[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                12,
                -321,
                duration_value(0, 1, 999),
                "varchar-beta",
                "ASCII-BETA",
                1_708_416_000_000_000,
            ),
            simple_table_mutation(
                row3_id,
                "Carol Primitive",
                54,
                88_888,
                1.95,
                91.0,
                true,
                "2024-03-10T18:45:30.250Z",
                "2024-03-10",
                "18:45:30.250000000",
                &[0xAA, 0xBB, 0xCC, 0xDD],
                decimal_from_i64(42, 0),
                "b40eb2a2-1b7f-11ef-8000-000000000003",
                &[10, 0, 0, 9],
                7,
                42,
                duration_value(5, 0, 0),
                "varchar-gamma",
                "ASCII-GAMMA",
                1_710_096_330_250_000,
            ),
        ],
        |dump| {
            assert_sstabledump_counts(dump, 3, 3)?;
            assert_sstabledump_contains(
                dump,
                &[
                    "\"name\":\"account_balance\",\"value\":12345.67",
                    "\"name\":\"birth_date\",\"value\":\"2024-01-15\"",
                    "\"name\":\"duration_val\",\"value\":\"2mo3d4us1ns\"",
                    "\"name\":\"ip_address\",\"value\":\"192.168.1.44\"",
                    "\"name\":\"session_id\",\"value\":\"b40eb2a0-1b7f-11ef-8000-000000000001\"",
                    "\"name\":\"description\",\"value\":\"0x000102ff10\"",
                ],
            )
        },
        move |harness| {
            assert_query_count(
                harness,
                &format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table()),
                3,
            )?;

            let query = format!(
                "SELECT name, age, salary, height, weight, active, \
                 toUnixTimestamp(created), birth_date, work_time, description, \
                 account_balance, session_id, ip_address, small_number, medium_number, \
                 duration_val, varchar_field, ascii_field \
                 FROM {} WHERE id = {}",
                harness.fully_qualified_table(),
                cql_uuid(row1_id)
            );
            let output =
                harness.query_until(&query, Duration::from_secs(20), |out| out.rows.len() == 1)?;
            assert_eq!(
                output.rows,
                vec![vec![
                    "Alice Primitive".to_string(),
                    "31".to_string(),
                    "123456".to_string(),
                    "1.75".to_string(),
                    "68.5".to_string(),
                    "True".to_string(),
                    "1705322096789".to_string(),
                    "2024-01-15".to_string(),
                    "09:15:30.123456789".to_string(),
                    "0x000102ff10".to_string(),
                    "12345.67".to_string(),
                    "b40eb2a0-1b7f-11ef-8000-000000000001".to_string(),
                    "192.168.1.44".to_string(),
                    "-5".to_string(),
                    "1200".to_string(),
                    "2mo3d4us1ns".to_string(),
                    "varchar-alpha".to_string(),
                    "ASCII-ALPHA".to_string(),
                ]]
            );

            let timeuuid_query = format!(
                "SELECT session_id FROM {} WHERE id = {}",
                harness.fully_qualified_table(),
                cql_uuid(row2_id)
            );
            let output = harness.query_until(&timeuuid_query, Duration::from_secs(20), |out| {
                out.rows.len() == 1
            })?;
            assert_timeuuid_value(&output.rows[0][0])?;
            assert_eq!(output.rows[0][0], "b40eb2a1-1b7f-11ef-8000-000000000002");

            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn test_issue_432_composite_key_table_portable_sstable_round_trip() -> CqliteResult<()> {
    let partition_key = "aaaaaaaa-0000-4000-8000-000000000001";

    run_issue_432_scenario(
        schema_composite_key_table(),
        &create_composite_key_table_issue_432_cql(),
        vec![
            composite_key_table_mutation(
                partition_key,
                "2024-04-10T10:00:00.000Z",
                "alpha",
                "latest alpha",
                10,
                1_712_743_200_000_000,
            ),
            composite_key_table_mutation(
                partition_key,
                "2024-04-10T10:00:00.000Z",
                "beta",
                "latest beta",
                11,
                1_712_743_200_001_000,
            ),
            composite_key_table_mutation(
                partition_key,
                "2024-04-09T10:00:00.000Z",
                "gamma",
                "older gamma",
                9,
                1_712_656_800_000_000,
            ),
        ],
        |dump| {
            assert_sstabledump_counts(dump, 1, 3)?;
            assert_sstabledump_contains(
                dump,
                &[
                    "\"clustering\":[\"2024-04-10 10:00:00.000Z\",\"alpha\"]",
                    "\"clustering\":[\"2024-04-10 10:00:00.000Z\",\"beta\"]",
                    "\"name\":\"value\",\"value\":9",
                ],
            )
        },
        move |harness| {
            assert_query_count(
                harness,
                &format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table()),
                3,
            )?;

            let query = format!(
                "SELECT toUnixTimestamp(clustering_key1), clustering_key2, data, value \
                 FROM {} WHERE partition_key = {}",
                harness.fully_qualified_table(),
                cql_uuid(partition_key)
            );
            let output =
                harness.query_until(&query, Duration::from_secs(20), |out| out.rows.len() == 3)?;
            assert_eq!(
                output.rows,
                vec![
                    vec![
                        "1712743200000".to_string(),
                        "alpha".to_string(),
                        "latest alpha".to_string(),
                        "10".to_string(),
                    ],
                    vec![
                        "1712743200000".to_string(),
                        "beta".to_string(),
                        "latest beta".to_string(),
                        "11".to_string(),
                    ],
                    vec![
                        "1712656800000".to_string(),
                        "gamma".to_string(),
                        "older gamma".to_string(),
                        "9".to_string(),
                    ],
                ]
            );

            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn test_issue_432_multi_partition_table_portable_sstable_round_trip() -> CqliteResult<()> {
    let tenant_a = "0f0f0f0f-0000-4000-8000-000000000001";
    let user_a = "0f0f0f0f-0000-4000-8000-0000000000aa";
    let tenant_b = "0f0f0f0f-0000-4000-8000-000000000002";
    let user_b = "0f0f0f0f-0000-4000-8000-0000000000bb";

    run_issue_432_scenario(
        schema_multi_partition_table(),
        &create_multi_partition_table_issue_432_cql(),
        vec![
            multi_partition_table_mutation(
                tenant_a,
                user_a,
                "analytics",
                "b40eb2b0-1b7f-11ef-8000-000000000010",
                "tenant-a-analytics-0",
                500,
                "meta-a0",
                1_715_011_200_000_000,
            ),
            multi_partition_table_mutation(
                tenant_a,
                user_a,
                "analytics",
                "b40eb2b1-1b7f-11ef-8000-000000000011",
                "tenant-a-analytics-1",
                650,
                "meta-a1",
                1_715_011_200_001_000,
            ),
            multi_partition_table_mutation(
                tenant_b,
                user_b,
                "billing",
                "b40eb2b2-1b7f-11ef-8000-000000000012",
                "tenant-b-billing-0",
                900,
                "meta-b0",
                1_715_097_600_000_000,
            ),
        ],
        |dump| {
            assert_sstabledump_counts(dump, 2, 3)?;
            assert_sstabledump_contains(
                dump,
                &[
                    "\"clustering\":[\"analytics\",\"b40eb2b0-1b7f-11ef-8000-000000000010\"]",
                    "\"name\":\"metadata\",\"value\":\"meta-b0\"",
                ],
            )
        },
        move |harness| {
            assert_query_count(
                harness,
                &format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table()),
                3,
            )?;

            let query = format!(
                "SELECT category, item_id, name, value, metadata \
                 FROM {} WHERE tenant_id = {} AND user_id = {}",
                harness.fully_qualified_table(),
                cql_uuid(tenant_a),
                cql_uuid(user_a)
            );
            let output =
                harness.query_until(&query, Duration::from_secs(20), |out| out.rows.len() == 2)?;
            assert_eq!(
                output.rows,
                vec![
                    vec![
                        "analytics".to_string(),
                        "b40eb2b0-1b7f-11ef-8000-000000000010".to_string(),
                        "tenant-a-analytics-0".to_string(),
                        "500".to_string(),
                        "meta-a0".to_string(),
                    ],
                    vec![
                        "analytics".to_string(),
                        "b40eb2b1-1b7f-11ef-8000-000000000011".to_string(),
                        "tenant-a-analytics-1".to_string(),
                        "650".to_string(),
                        "meta-a1".to_string(),
                    ],
                ]
            );
            for row in &output.rows {
                assert_timeuuid_value(&row[1])?;
            }

            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn test_issue_432_static_columns_table_portable_sstable_round_trip() -> CqliteResult<()> {
    let partition_a = "12345678-0000-4000-8000-000000000001";
    let partition_b = "12345678-0000-4000-8000-000000000002";

    run_issue_432_scenario(
        schema_static_columns_table(),
        &create_static_columns_table_issue_432_cql(),
        vec![
            static_only_mutation(
                "test_basic",
                "static_columns_table",
                partition_a,
                "static-a",
                1_716_537_600_000_000,
            ),
            static_row_mutation(
                partition_a,
                "2024-05-01T00:00:00.000Z",
                "row-a0",
                1,
                1_716_537_600_001_000,
            ),
            static_row_mutation(
                partition_a,
                "2024-05-01T01:00:00.000Z",
                "row-a1",
                2,
                1_716_541_200_000_000,
            ),
            static_only_mutation(
                "test_basic",
                "static_columns_table",
                partition_b,
                "static-b",
                1_716_624_000_000_000,
            ),
            static_row_mutation(
                partition_b,
                "2024-05-02T00:00:00.000Z",
                "row-b0",
                3,
                1_716_624_000_001_000,
            ),
        ],
        |dump| {
            assert_sstabledump_counts(dump, 2, 5)?;
            assert_sstabledump_contains(
                dump,
                &[
                    "\"name\":\"static_data\",\"value\":\"static-a\"",
                    "\"name\":\"row_value\",\"value\":3",
                ],
            )
        },
        move |harness| {
            assert_query_count(
                harness,
                &format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table()),
                3,
            )?;

            let query = format!(
                "SELECT static_data, toUnixTimestamp(clustering_key), row_data, row_value \
                 FROM {} WHERE partition_key = {}",
                harness.fully_qualified_table(),
                cql_uuid(partition_a)
            );
            let output =
                harness.query_until(&query, Duration::from_secs(20), |out| out.rows.len() == 2)?;
            assert_eq!(
                output.rows,
                vec![
                    vec![
                        "static-a".to_string(),
                        "1714521600000".to_string(),
                        "row-a0".to_string(),
                        "1".to_string(),
                    ],
                    vec![
                        "static-a".to_string(),
                        "1714525200000".to_string(),
                        "row-a1".to_string(),
                        "2".to_string(),
                    ],
                ]
            );

            Ok(())
        },
    )
    .await
}

/// Issue #507 — gen_static mutation shape: clustering rows whose operations include
/// both a static column and regular columns must produce a valid SSTable that
/// Cassandra accepts via sstabledump and sstableloader.
///
/// Previously CQLite emitted no static-row prelude when every mutation had a
/// clustering key, causing Cassandra to fire `UnfilteredSerializer` assertion 4.
#[tokio::test]
async fn test_issue_507_gen_static_shape_cassandra_readback() -> CqliteResult<()> {
    let partition_hex = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
    let partition_uuid = format!(
        "{}-{}-{}-{}-{}",
        &partition_hex[0..8],
        &partition_hex[8..12],
        &partition_hex[12..16],
        &partition_hex[16..20],
        &partition_hex[20..32]
    );

    // Build mutations identical to what gen_static() produces:
    //   Two clustering rows in the same partition, each with clustering_key set,
    //   and operations carrying static_data (STATIC) + row_data + row_value.
    let ts = 1_704_067_200_000_000i64;
    let cluster_ts_base_ms: i64 = 1_704_067_200_000;

    let pk_uuid = uuid_value(&partition_uuid);

    run_issue_432_scenario(
        schema_static_columns_table(),
        &create_static_columns_table_issue_432_cql(),
        vec![
            // Mutation 1: clustering_key = cluster_ts_base_ms + 1000, row_data = "alpha"
            Mutation::new(
                TableId::new("test_basic", "static_columns_table"),
                PartitionKey::single("partition_key", pk_uuid.clone()),
                Some(ClusteringKey::single(
                    "clustering_key",
                    Value::Timestamp(cluster_ts_base_ms + 1000),
                )),
                vec![
                    CellOperation::Write {
                        column: "static_data".to_string(),
                        value: Value::Text("shared-static-text".to_string()),
                    },
                    CellOperation::Write {
                        column: "row_data".to_string(),
                        value: Value::Text("alpha".to_string()),
                    },
                    CellOperation::Write {
                        column: "row_value".to_string(),
                        value: Value::Integer(11),
                    },
                ],
                ts,
                None,
            ),
            // Mutation 2: clustering_key = cluster_ts_base_ms + 2000, row_data = "beta"
            Mutation::new(
                TableId::new("test_basic", "static_columns_table"),
                PartitionKey::single("partition_key", pk_uuid.clone()),
                Some(ClusteringKey::single(
                    "clustering_key",
                    Value::Timestamp(cluster_ts_base_ms + 2000),
                )),
                vec![
                    CellOperation::Write {
                        column: "static_data".to_string(),
                        value: Value::Text("shared-static-text".to_string()),
                    },
                    CellOperation::Write {
                        column: "row_data".to_string(),
                        value: Value::Text("beta".to_string()),
                    },
                    CellOperation::Write {
                        column: "row_value".to_string(),
                        value: Value::Integer(22),
                    },
                ],
                ts,
                None,
            ),
        ],
        |dump| {
            // sstabledump must succeed (exit 0) — that alone proves the prelude is valid.
            // The dump should show one partition with a staticRow and two clustering rows.
            assert_sstabledump_counts(dump, 1, 2)?;
            let dump_str = serde_json::to_string(dump)
                .map_err(|e| Error::Storage(format!("JSON serialize: {e}")))?;
            assert!(
                dump_str.contains("staticRow") || dump_str.contains("static_data"),
                "sstabledump output should contain a staticRow or static_data column; \
                 got: {dump_str}"
            );
            assert_sstabledump_contains(
                dump,
                &[
                    "\"name\":\"static_data\",\"value\":\"shared-static-text\"",
                    "\"name\":\"row_data\",\"value\":\"alpha\"",
                    "\"name\":\"row_data\",\"value\":\"beta\"",
                    "\"name\":\"row_value\",\"value\":11",
                    "\"name\":\"row_value\",\"value\":22",
                ],
            )
        },
        move |harness| {
            assert_query_count(
                harness,
                &format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table()),
                2,
            )?;

            let query = format!(
                "SELECT static_data, row_data, row_value \
                 FROM {} WHERE partition_key = {}",
                harness.fully_qualified_table(),
                cql_uuid(&partition_uuid)
            );
            let output =
                harness.query_until(&query, Duration::from_secs(20), |out| out.rows.len() == 2)?;
            // Both rows should see the same static_data value.
            assert_eq!(output.rows[0][0], "shared-static-text");
            assert_eq!(output.rows[1][0], "shared-static-text");
            // row_data / row_value differ per clustering row.
            let row_data_values: Vec<&str> = output.rows.iter().map(|r| r[1].as_str()).collect();
            assert!(
                row_data_values.contains(&"alpha") && row_data_values.contains(&"beta"),
                "Expected rows with row_data 'alpha' and 'beta'; got {:?}",
                row_data_values
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn test_issue_432_ttl_test_table_portable_sstable_round_trip() -> CqliteResult<()> {
    let ttl_id = "44444444-4444-4444-8444-444444444444";

    run_issue_432_scenario(
        schema_ttl_test_table(),
        &create_ttl_test_table_issue_432_cql(),
        vec![ttl_test_table_mutation(
            ttl_id,
            "ttl-row",
            9001,
            "session-ttl",
            7200,
            1_717_401_600_000_000,
        )],
        |dump| {
            assert_sstabledump_counts(dump, 1, 1)?;
            assert_sstabledump_contains(
                dump,
                &[
                    "\"name\":\"temporary_data\",\"value\":\"ttl-row\"",
                    "\"name\":\"expiring_value\",\"value\":9001",
                ],
            )
        },
        move |harness| {
            assert_query_count(
                harness,
                &format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table()),
                1,
            )?;

            let query = format!(
                "SELECT temporary_data, expiring_value, session_info, TTL(temporary_data), TTL(expiring_value) \
                 FROM {} WHERE id = {}",
                harness.fully_qualified_table(),
                cql_uuid(ttl_id)
            );
            let output = harness.query_until(&query, Duration::from_secs(20), |out| out.rows.len() == 1)?;
            let row = output.rows.first().cloned().ok_or_else(|| {
                Error::Storage("TTL query returned no row".to_string())
            })?;
            assert_eq!(row[0], "ttl-row");
            assert_eq!(row[1], "9001");
            assert_eq!(row[2], "session-ttl");
            assert_ttl_between(&row[3], 7100, 7200)?;
            assert_ttl_between(&row[4], 7100, 7200)?;

            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn test_issue_432_sensor_data_portable_sstable_round_trip() -> CqliteResult<()> {
    let sensor_id = "55555555-5555-4555-8555-555555555555";

    run_issue_432_scenario(
        schema_sensor_data(),
        &create_sensor_data_issue_432_cql(),
        vec![
            sensor_data_mutation(
                sensor_id,
                "2024-06-01T12:00:00.000Z",
                21.5,
                45.0,
                1001.25,
                99,
                "lab-a",
                "ok",
                1_717_243_200_000_000,
            ),
            sensor_data_mutation(
                sensor_id,
                "2024-06-01T12:05:00.000Z",
                22.0,
                44.5,
                1001.5,
                98,
                "lab-a",
                "warn",
                1_717_243_500_000_000,
            ),
            sensor_data_mutation(
                sensor_id,
                "2024-06-01T12:10:00.000Z",
                22.25,
                44.0,
                1001.75,
                97,
                "lab-a",
                "ok",
                1_717_243_800_000_000,
            ),
        ],
        |dump| {
            assert_sstabledump_counts(dump, 1, 3)?;
            assert_sstabledump_contains(
                dump,
                &[
                    "\"name\":\"temperature\",\"value\":22.25",
                    "\"name\":\"battery_level\",\"value\":97",
                    "\"name\":\"status\",\"value\":\"warn\"",
                ],
            )
        },
        move |harness| {
            assert_query_count(
                harness,
                &format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table()),
                3,
            )?;

            let query = format!(
                "SELECT toUnixTimestamp(timestamp), temperature, humidity, pressure, battery_level, location, status \
                 FROM {} WHERE sensor_id = {}",
                harness.fully_qualified_table(),
                cql_uuid(sensor_id)
            );
            let output = harness.query_until(&query, Duration::from_secs(20), |out| out.rows.len() == 3)?;
            assert_eq!(
                output.rows,
                vec![
                    vec![
                        "1717243800000".to_string(),
                        "22.25".to_string(),
                        "44".to_string(),
                        "1001.75".to_string(),
                        "97".to_string(),
                        "lab-a".to_string(),
                        "ok".to_string(),
                    ],
                    vec![
                        "1717243500000".to_string(),
                        "22".to_string(),
                        "44.5".to_string(),
                        "1001.5".to_string(),
                        "98".to_string(),
                        "lab-a".to_string(),
                        "warn".to_string(),
                    ],
                    vec![
                        "1717243200000".to_string(),
                        "21.5".to_string(),
                        "45".to_string(),
                        "1001.25".to_string(),
                        "99".to_string(),
                        "lab-a".to_string(),
                        "ok".to_string(),
                    ],
                ]
            );

            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn test_issue_432_stock_prices_portable_sstable_round_trip() -> CqliteResult<()> {
    run_issue_432_scenario(
        schema_stock_prices(),
        &create_stock_prices_issue_432_cql(),
        vec![
            stock_prices_mutation(
                "ACME",
                "2024-07-01",
                "2024-07-01T09:30:00.000Z",
                decimal_from_i64(12_510, 2),
                decimal_from_i64(12_655, 2),
                decimal_from_i64(12_475, 2),
                decimal_from_i64(12_625, 2),
                10_000,
                decimal_from_i64(12_625, 2),
                1_719_825_000_000_000,
            ),
            stock_prices_mutation(
                "ACME",
                "2024-07-01",
                "2024-07-01T09:31:00.000Z",
                decimal_from_i64(12_625, 2),
                decimal_from_i64(12_700, 2),
                decimal_from_i64(12_610, 2),
                decimal_from_i64(12_680, 2),
                12_500,
                decimal_from_i64(12_680, 2),
                1_719_825_060_000_000,
            ),
            stock_prices_mutation(
                "ACME",
                "2024-07-01",
                "2024-07-01T09:32:00.000Z",
                decimal_from_i64(12_680, 2),
                decimal_from_i64(12_745, 2),
                decimal_from_i64(12_650, 2),
                decimal_from_i64(12_715, 2),
                15_000,
                decimal_from_i64(12_715, 2),
                1_719_825_120_000_000,
            ),
        ],
        |dump| {
            assert_sstabledump_counts(dump, 1, 3)?;
            assert_sstabledump_contains(
                dump,
                &[
                    "\"name\":\"open_price\",\"value\":125.1",
                    "\"name\":\"adjusted_close\",\"value\":127.15",
                    "\"key\":[\"ACME\",\"2024-07-01\"]",
                ],
            )
        },
        |harness| {
            assert_query_count(
                harness,
                &format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table()),
                3,
            )?;

            let query = format!(
                "SELECT toUnixTimestamp(timestamp), open_price, high_price, low_price, close_price, volume, adjusted_close \
                 FROM {} WHERE symbol = 'ACME' AND trading_day = '2024-07-01'",
                harness.fully_qualified_table()
            );
            let output = harness.query_until(&query, Duration::from_secs(20), |out| out.rows.len() == 3)?;
            assert_eq!(
                output.rows,
                vec![
                    vec![
                        "1719826200000".to_string(),
                        "125.10".to_string(),
                        "126.55".to_string(),
                        "124.75".to_string(),
                        "126.25".to_string(),
                        "10000".to_string(),
                        "126.25".to_string(),
                    ],
                    vec![
                        "1719826260000".to_string(),
                        "126.25".to_string(),
                        "127.00".to_string(),
                        "126.10".to_string(),
                        "126.80".to_string(),
                        "12500".to_string(),
                        "126.80".to_string(),
                    ],
                    vec![
                        "1719826320000".to_string(),
                        "126.80".to_string(),
                        "127.45".to_string(),
                        "126.50".to_string(),
                        "127.15".to_string(),
                        "15000".to_string(),
                        "127.15".to_string(),
                    ],
                ]
            );

            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn test_issue_432_wide_partition_table_portable_sstable_round_trip() -> CqliteResult<()> {
    let partition_key = "66666666-6666-4666-8666-666666666666";

    run_issue_432_scenario(
        schema_wide_partition_table(),
        &create_wide_partition_table_issue_432_cql(),
        vec![
            wide_partition_table_mutation(
                partition_key,
                "2024-08-01T10:00:00.000Z",
                "alpha",
                10,
                "77777777-0000-4000-8000-000000000001",
                "2024-08-10",
                "wide-row-a",
                1000,
                &[0xDE, 0xAD, 0xBE, 0xEF],
                "{\"slot\":0}",
                1_722_506_400_000_000,
            ),
            wide_partition_table_mutation(
                partition_key,
                "2024-08-01T10:00:00.000Z",
                "alpha",
                9,
                "77777777-0000-4000-8000-000000000002",
                "2024-08-11",
                "wide-row-b",
                1001,
                &[0xDE, 0xAD, 0xBE, 0xF0],
                "{\"slot\":1}",
                1_722_506_401_000_000,
            ),
            wide_partition_table_mutation(
                partition_key,
                "2024-07-31T10:00:00.000Z",
                "beta",
                11,
                "77777777-0000-4000-8000-000000000003",
                "2024-08-12",
                "wide-row-c",
                1002,
                &[0xDE, 0xAD, 0xBE, 0xF1],
                "{\"slot\":2}",
                1_722_420_000_000_000,
            ),
        ],
        |dump| {
            assert_sstabledump_counts(dump, 1, 3)?;
            assert_sstabledump_contains(
                dump,
                &[
                    "\"name\":\"data_column\",\"value\":\"wide-row-a\"",
                    "\"name\":\"blob_column\",\"value\":\"0xdeadbeef\"",
                    "\"name\":\"json_column\",\"value\":\"{\\\"slot\\\":2}\"",
                ],
            )
        },
        move |harness| {
            assert_query_count(
                harness,
                &format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table()),
                3,
            )?;

            let query = format!(
                "SELECT toUnixTimestamp(clustering_col1), clustering_col2, clustering_col3, \
                 clustering_col4, clustering_col5, data_column, value_column, blob_column, json_column \
                 FROM {} WHERE partition_key = {}",
                harness.fully_qualified_table(),
                cql_uuid(partition_key)
            );
            let output = harness.query_until(&query, Duration::from_secs(20), |out| out.rows.len() == 3)?;
            assert_eq!(
                output.rows,
                vec![
                    vec![
                        "1722506400000".to_string(),
                        "alpha".to_string(),
                        "10".to_string(),
                        "77777777-0000-4000-8000-000000000001".to_string(),
                        "2024-08-10".to_string(),
                        "wide-row-a".to_string(),
                        "1000".to_string(),
                        "0xdeadbeef".to_string(),
                        "{\"slot\":0}".to_string(),
                    ],
                    vec![
                        "1722506400000".to_string(),
                        "alpha".to_string(),
                        "9".to_string(),
                        "77777777-0000-4000-8000-000000000002".to_string(),
                        "2024-08-11".to_string(),
                        "wide-row-b".to_string(),
                        "1001".to_string(),
                        "0xdeadbef0".to_string(),
                        "{\"slot\":1}".to_string(),
                    ],
                    vec![
                        "1722420000000".to_string(),
                        "beta".to_string(),
                        "11".to_string(),
                        "77777777-0000-4000-8000-000000000003".to_string(),
                        "2024-08-12".to_string(),
                        "wide-row-c".to_string(),
                        "1002".to_string(),
                        "0xdeadbef1".to_string(),
                        "{\"slot\":2}".to_string(),
                    ],
                ]
            );

            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn test_issue_432_large_blob_table_portable_sstable_round_trip() -> CqliteResult<()> {
    let file_id = "88888888-8888-4888-8888-888888888888";
    let chunk_0 = deterministic_blob(64, 0x20);
    let chunk_1 = deterministic_blob(64, 0x40);
    let chunk_2 = deterministic_blob(64, 0x60);
    let chunk_0_hex = blob_hex(&chunk_0);
    let chunk_1_hex = blob_hex(&chunk_1);
    let chunk_2_hex = blob_hex(&chunk_2);
    let chunk_0_artifact = format!("\"name\":\"chunk_data\",\"value\":\"{}\"", chunk_0_hex);
    let chunk_2_artifact = format!("\"name\":\"chunk_data\",\"value\":\"{}\"", chunk_2_hex);

    run_issue_432_scenario(
        schema_large_blob_table(),
        &create_large_blob_table_issue_432_cql(),
        vec![
            large_blob_table_mutation(
                file_id,
                0,
                "artifact.bin",
                "application/octet-stream",
                &chunk_0,
                64,
                3,
                "checksum-0",
                1_723_536_000_000_000,
            ),
            large_blob_table_mutation(
                file_id,
                1,
                "artifact.bin",
                "application/octet-stream",
                &chunk_1,
                64,
                3,
                "checksum-1",
                1_723_536_001_000_000,
            ),
            large_blob_table_mutation(
                file_id,
                2,
                "artifact.bin",
                "application/octet-stream",
                &chunk_2,
                64,
                3,
                "checksum-2",
                1_723_536_002_000_000,
            ),
        ],
        |dump| {
            assert_sstabledump_counts(dump, 1, 3)?;
            assert_sstabledump_contains(
                dump,
                &[
                    &chunk_0_artifact,
                    &chunk_2_artifact,
                ],
            )
        },
        move |harness| {
            assert_query_count(
                harness,
                &format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table()),
                3,
            )?;

            let query = format!(
                "SELECT chunk_id, file_name, mime_type, chunk_data, chunk_size, total_chunks, checksum \
                 FROM {} WHERE file_id = {}",
                harness.fully_qualified_table(),
                cql_uuid(file_id)
            );
            let output = harness.query_until(&query, Duration::from_secs(20), |out| out.rows.len() == 3)?;
            assert_eq!(
                output.rows,
                vec![
                    vec![
                        "0".to_string(),
                        "artifact.bin".to_string(),
                        "application/octet-stream".to_string(),
                        chunk_0_hex.clone(),
                        "64".to_string(),
                        "3".to_string(),
                        "checksum-0".to_string(),
                    ],
                    vec![
                        "1".to_string(),
                        "artifact.bin".to_string(),
                        "application/octet-stream".to_string(),
                        chunk_1_hex.clone(),
                        "64".to_string(),
                        "3".to_string(),
                        "checksum-1".to_string(),
                    ],
                    vec![
                        "2".to_string(),
                        "artifact.bin".to_string(),
                        "application/octet-stream".to_string(),
                        chunk_2_hex.clone(),
                        "64".to_string(),
                        "3".to_string(),
                        "checksum-2".to_string(),
                    ],
                ]
            );

            Ok(())
        },
    )
    .await
}

// =============================================================================
// Issue #439: Wide-row write path portability
// =============================================================================

#[tokio::test]
async fn test_issue_439_mixed_simple_and_complex_columns_round_trip() -> CqliteResult<()> {
    let row_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";

    run_issue_432_scenario(
        schema_issue_439_mixed_columns_table(),
        &create_issue_439_mixed_columns_table_cql(),
        vec![issue_439_mixed_columns_mutation(
            row_id,
            "portable mixed row",
            &["alpha", "beta"],
            1_710_096_000_000_000,
        )],
        |dump| {
            assert_sstabledump_counts(dump, 1, 1)?;
            assert_sstabledump_contains(
                dump,
                &[
                    "\"name\":\"z_simple\",\"value\":\"portable mixed row\"",
                    "\"name\":\"a_complex\"",
                    "\"path\":[\"alpha\"]",
                    "\"path\":[\"beta\"]",
                ],
            )
        },
        move |harness| {
            assert_query_count(
                harness,
                &format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table()),
                1,
            )?;

            let query = format!(
                "SELECT z_simple FROM {} WHERE id = {}",
                harness.fully_qualified_table(),
                cql_uuid(row_id)
            );
            let output =
                harness.query_until(&query, Duration::from_secs(20), |out| out.rows.len() == 1)?;
            assert_eq!(output.rows, vec![vec!["portable mixed row".to_string()]]);
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn test_issue_439_product_catalog_portable_sstable_round_trip() -> CqliteResult<()> {
    let category_a = "10000000-0000-4000-8000-000000000001";
    let category_b = "20000000-0000-4000-8000-000000000002";
    let product_a = "30000000-0000-4000-8000-000000000003";
    let product_b = "40000000-0000-4000-8000-000000000004";

    run_issue_432_scenario(
        schema_issue_439_product_catalog(),
        &create_issue_439_product_catalog_cql(),
        vec![
            issue_439_product_catalog_mutation(
                category_a,
                product_a,
                "Trail Shoes",
                "Off-road running shoe",
                12,
                1_710_120_000_000_000,
            ),
            issue_439_product_catalog_mutation(
                category_b,
                product_b,
                "Climbing Pack",
                "Alpine day pack",
                7,
                1_710_120_100_000_000,
            ),
        ],
        |dump| {
            assert_sstabledump_counts(dump, 2, 2)?;
            assert_sstabledump_contains(
                dump,
                &[
                    "\"name\":\"product_name\",\"value\":\"Trail Shoes\"",
                    "\"name\":\"product_name\",\"value\":\"Climbing Pack\"",
                    "\"name\":\"tags\"",
                    "\"name\":\"specifications\"",
                    "\"name\":\"attributes\"",
                ],
            )
        },
        move |harness| {
            assert_query_count(
                harness,
                &format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table()),
                2,
            )?;

            let query = format!(
                "SELECT product_name, availability_count FROM {} WHERE category_id = {} AND product_id = {}",
                harness.fully_qualified_table(),
                cql_uuid(category_a),
                cql_uuid(product_a)
            );
            let output =
                harness.query_until(&query, Duration::from_secs(20), |out| out.rows.len() == 1)?;
            assert_eq!(
                output.rows,
                vec![vec!["Trail Shoes".to_string(), "12".to_string()]]
            );
            Ok(())
        },
    )
    .await
}

#[tokio::test]
async fn test_issue_434_phase3_complex_type_round_trip() -> CqliteResult<()> {
    let row_id = "70000000-0000-4000-8000-000000000007";

    run_issue_432_scenario(
        schema_issue_434_phase3_complex_table(),
        &create_issue_434_phase3_complex_table_cql(),
        vec![issue_434_phase3_complex_mutation(
            row_id,
            1_710_130_000_000_000,
        )],
        |dump| {
            assert_sstabledump_counts(dump, 1, 1)?;
            assert_sstabledump_contains(
                dump,
                &[
                    "\"name\":\"nested_map_list\"",
                    "\"name\":\"company_map\"",
                    "\"name\":\"tuple_with_list\"",
                    "\"name\":\"complex_tuple\"",
                    "こんにちは",
                ],
            )?;

            let compact = serde_json::to_string(dump).map_err(|err| {
                Error::Storage(format!("Failed to serialize sstabledump JSON: {err}"))
            })?;
            assert!(
                !compact.contains("\"name\":\"nullable_map\""),
                "NULL collection column should be absent from sstabledump output"
            );

            Ok(())
        },
        move |harness| {
            assert_query_count(
                harness,
                &format!("SELECT COUNT(*) FROM {}", harness.fully_qualified_table()),
                1,
            )?;

            let query = format!(
                "SELECT tuple_with_list, company_map, unicode_map, empty_list FROM {} WHERE id = {}",
                harness.fully_qualified_table(),
                cql_uuid(row_id)
            );
            let output = harness.query_until(&query, Duration::from_secs(20), |out| {
                out.raw_output.contains("phase3")
                    && out.raw_output.contains("Acme")
                    && out.raw_output.contains("platform")
                    && out.raw_output.contains("こんにちは")
            })?;

            assert!(output.raw_output.contains("phase3"));
            assert!(output.raw_output.contains("Acme"));
            assert!(output.raw_output.contains("platform"));
            assert!(output.raw_output.contains("こんにちは"));
            assert!(output.raw_output.contains("null"));

            Ok(())
        },
    )
    .await
}

// =============================================================================
// Helpers
// =============================================================================

fn maybe_start_cassandra() -> CqliteResult<Option<CassandraContainer>> {
    let explicit_container = match std::env::var("CQLITE_CASSANDRA_CONTAINER") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            eprintln!(
                "Skipping sstableloader integration test: set CQLITE_CASSANDRA_CONTAINER to a running Cassandra 5.0 container ID"
            );
            return Ok(None);
        }
    };

    match CassandraContainer::start() {
        Ok(container) => {
            container.wait_until_ready(300).map_err(io_to_cqlite)?;
            Ok(Some(container))
        }
        Err(err) => Err(Error::Storage(format!(
            "Failed to connect to Cassandra container `{explicit_container}`: {err}"
        ))),
    }
}

fn ensure_keyspace(cassandra: &CassandraContainer, keyspace: &str) -> CqliteResult<()> {
    let cql = format!(
        "CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}};"
    );
    cassandra.execute_cql(&cql).map_err(io_to_cqlite)?;
    Ok(())
}

fn recreate_table(
    cassandra: &CassandraContainer,
    keyspace: &str,
    table: &str,
    create_table_cql: &str,
) -> CqliteResult<()> {
    let drop_cql = format!("DROP TABLE IF EXISTS {keyspace}.{table};");
    cassandra.execute_cql(&drop_cql).map_err(io_to_cqlite)?;
    cassandra
        .execute_cql(create_table_cql)
        .map_err(io_to_cqlite)?;
    Ok(())
}

fn parse_count(output: &CqlshOutput) -> CqliteResult<usize> {
    let value = output
        .rows
        .first()
        .and_then(|row| row.first())
        .ok_or_else(|| Error::Storage(format!("COUNT query returned no rows: {:?}", output)))?;

    value
        .parse::<usize>()
        .map_err(|err| Error::Storage(format!("Failed to parse COUNT value `{value}`: {err}")))
}

fn assert_rows_unordered_eq(mut actual: Vec<Vec<String>>, mut expected: Vec<Vec<String>>) {
    actual.sort();
    expected.sort();
    assert_eq!(actual, expected);
}

fn io_to_cqlite(err: std::io::Error) -> Error {
    Error::Storage(err.to_string())
}

fn is_retryable_query_error(err: &Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    [
        "connection error",
        "connectionrefusederror",
        "failed to connect",
        "no host available",
        "unable to connect",
        "operationtimedout",
        "temporarily unavailable",
    ]
    .iter()
    .any(|pattern| message.contains(pattern))
}

fn create_simple_schema(table_name: &str) -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: table_name.to_string(),
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
            Column {
                name: "value".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn create_clustered_schema(table_name: &str) -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: table_name.to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "text".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "text".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "data".to_string(),
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

fn create_types_schema(table_name: &str) -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: table_name.to_string(),
        partition_keys: vec![KeyColumn {
            name: "pk".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "text".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            Column {
                name: "pk".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "text".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "text_col".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "int_col".to_string(),
                data_type: "int".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "bigint_col".to_string(),
                data_type: "bigint".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "boolean_col".to_string(),
                data_type: "boolean".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "timestamp_col".to_string(),
                data_type: "timestamp".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
            Column {
                name: "uuid_col".to_string(),
                data_type: "uuid".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn create_simple_table_cql(table_name: &str) -> String {
    format!("CREATE TABLE {KEYSPACE}.{table_name} (id int PRIMARY KEY, name text, value int);")
}

fn create_clustered_table_cql(table_name: &str) -> String {
    format!(
        "CREATE TABLE {KEYSPACE}.{table_name} (pk int, ck text, data text, PRIMARY KEY (pk, ck));"
    )
}

fn create_types_table_cql(table_name: &str) -> String {
    format!(
        "CREATE TABLE {KEYSPACE}.{table_name} (pk int, ck text, text_col text, int_col int, bigint_col bigint, boolean_col boolean, timestamp_col timestamp, uuid_col uuid, PRIMARY KEY (pk, ck));"
    )
}

fn simple_mutation(table_name: &str, id: i32, name: &str, value: i32, timestamp: i64) -> Mutation {
    let table_id = TableId::new(KEYSPACE, table_name);
    let partition_key = PartitionKey::single("id", Value::Integer(id));
    let operations = vec![
        CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        },
        CellOperation::Write {
            column: "value".to_string(),
            value: Value::Integer(value),
        },
    ];

    Mutation::new(table_id, partition_key, None, operations, timestamp, None)
}

fn clustered_mutation(table_name: &str, pk: i32, ck: &str, data: &str, timestamp: i64) -> Mutation {
    let table_id = TableId::new(KEYSPACE, table_name);
    let partition_key = PartitionKey::single("pk", Value::Integer(pk));
    let clustering_key = Some(ClusteringKey::single("ck", Value::Text(ck.to_string())));
    let operations = vec![CellOperation::Write {
        column: "data".to_string(),
        value: Value::Text(data.to_string()),
    }];

    Mutation::new(
        table_id,
        partition_key,
        clustering_key,
        operations,
        timestamp,
        None,
    )
}

fn types_mutation(table_name: &str, pk: i32, ck: &str, mutation_timestamp: i64) -> Mutation {
    let table_id = TableId::new(KEYSPACE, table_name);
    let partition_key = PartitionKey::single("pk", Value::Integer(pk));
    let clustering_key = Some(ClusteringKey::single("ck", Value::Text(ck.to_string())));
    let operations = vec![
        CellOperation::Write {
            column: "text_col".to_string(),
            value: Value::Text("stage0".to_string()),
        },
        CellOperation::Write {
            column: "int_col".to_string(),
            value: Value::Integer(42),
        },
        CellOperation::Write {
            column: "bigint_col".to_string(),
            value: Value::BigInt(9_223_372_036),
        },
        CellOperation::Write {
            column: "boolean_col".to_string(),
            value: Value::Boolean(true),
        },
        CellOperation::Write {
            column: "timestamp_col".to_string(),
            value: Value::Timestamp(1_704_067_200_000),
        },
        CellOperation::Write {
            column: "uuid_col".to_string(),
            value: Value::Uuid([
                0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0x4D, 0xEF, 0x81, 0x23, 0x45, 0x67, 0x89, 0xAB,
                0xCD, 0xEF,
            ]),
        },
    ];

    Mutation::new(
        table_id,
        partition_key,
        clustering_key,
        operations,
        mutation_timestamp,
        None,
    )
}

fn stage_error(stage: &str, table: &str, err: Error) -> Error {
    Error::Storage(format!("{stage} validation failed for {table}: {err}"))
}

fn assert_query_count(harness: &LoaderHarness, query: &str, expected: usize) -> CqliteResult<()> {
    let output = harness.query_until(query, Duration::from_secs(20), |out| {
        parse_count(out).ok() == Some(expected)
    })?;
    assert_eq!(parse_count(&output)?, expected);
    Ok(())
}

fn assert_sstabledump_counts(
    dump: &JsonValue,
    expected_partitions: usize,
    expected_rows: usize,
) -> CqliteResult<()> {
    let partitions = dump.as_array().ok_or_else(|| {
        Error::Storage(format!("sstabledump output is not a JSON array: {}", dump))
    })?;

    let row_count = partitions
        .iter()
        .map(|partition| {
            partition
                .get("rows")
                .and_then(JsonValue::as_array)
                .map(|rows| rows.len())
                .unwrap_or(0)
        })
        .sum::<usize>();

    assert_eq!(partitions.len(), expected_partitions);
    assert_eq!(row_count, expected_rows);
    Ok(())
}

fn assert_sstabledump_contains(dump: &JsonValue, snippets: &[&str]) -> CqliteResult<()> {
    let compact = serde_json::to_string(dump)
        .map_err(|err| Error::Storage(format!("Failed to serialize sstabledump JSON: {err}")))?;
    for snippet in snippets {
        assert!(
            compact.contains(snippet),
            "sstabledump output missing snippet `{snippet}`\nOutput: {compact}"
        );
    }
    Ok(())
}

fn assert_ttl_between(value: &str, min: usize, max: usize) -> CqliteResult<()> {
    let ttl = value
        .parse::<usize>()
        .map_err(|err| Error::Storage(format!("Failed to parse TTL value `{value}`: {err}")))?;
    assert!(
        (min..=max).contains(&ttl),
        "TTL {ttl} not in expected range [{min}, {max}]"
    );
    Ok(())
}

fn assert_timeuuid_value(value: &str) -> CqliteResult<()> {
    let uuid = Uuid::parse_str(value)
        .map_err(|err| Error::Storage(format!("Invalid UUID string `{value}`: {err}")))?;
    if uuid.get_version_num() != 1 {
        return Err(Error::Storage(format!(
            "Expected timeuuid version 1, got version {} for `{value}`",
            uuid.get_version_num()
        )));
    }
    Ok(())
}

fn cql_uuid(uuid: &str) -> String {
    uuid.to_string()
}

fn uuid_value(uuid: &str) -> Value {
    Value::Uuid(
        Uuid::parse_str(uuid)
            .expect("invalid UUID literal")
            .into_bytes(),
    )
}

fn decimal_from_i64(unscaled: i64, scale: i32) -> Value {
    Value::Decimal {
        scale,
        unscaled: BigInt::from(unscaled).to_signed_bytes_be(),
    }
}

fn duration_value(months: i32, days: i32, nanos: i64) -> Value {
    Value::Duration {
        months,
        days,
        nanos,
    }
}

fn date_days(date: &str) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch");
    NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .expect("invalid date literal")
        .signed_duration_since(epoch)
        .num_days() as i32
}

fn timestamp_millis(timestamp: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .expect("invalid timestamp literal")
        .timestamp_millis()
}

fn time_nanos(time: &str) -> i64 {
    let (clock, nanos_str) = time.split_once('.').expect("time must have nanos");
    let mut parts = clock.split(':');
    let hour = parts.next().and_then(|v| v.parse::<i64>().ok()).unwrap();
    let minute = parts.next().and_then(|v| v.parse::<i64>().ok()).unwrap();
    let second = parts.next().and_then(|v| v.parse::<i64>().ok()).unwrap();
    let nanos = nanos_str.parse::<i64>().expect("invalid nanos");
    (((hour * 60 + minute) * 60) + second) * 1_000_000_000 + nanos
}

fn blob_hex(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn deterministic_blob(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|index| seed.wrapping_add(index as u8))
        .collect()
}

fn key_column(name: &str, data_type: &str, position: usize) -> KeyColumn {
    KeyColumn {
        name: name.to_string(),
        data_type: data_type.to_string(),
        position,
    }
}

fn clustering_column(
    name: &str,
    data_type: &str,
    position: usize,
    order: ClusteringOrder,
) -> ClusteringColumn {
    ClusteringColumn {
        name: name.to_string(),
        data_type: data_type.to_string(),
        position,
        order,
    }
}

fn regular_column(name: &str, data_type: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: data_type.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

fn static_column(name: &str, data_type: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: data_type.to_string(),
        nullable: true,
        default: None,
        is_static: true,
    }
}

fn build_schema(
    keyspace: &str,
    table: &str,
    partition_keys: Vec<KeyColumn>,
    clustering_keys: Vec<ClusteringColumn>,
    columns: Vec<Column>,
) -> TableSchema {
    TableSchema {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        partition_keys,
        clustering_keys,
        columns,
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn base_mutation(
    keyspace: &str,
    table: &str,
    partition_key: PartitionKey,
    clustering_key: Option<ClusteringKey>,
    operations: Vec<CellOperation>,
    timestamp_micros: i64,
    ttl_seconds: Option<u32>,
) -> Mutation {
    Mutation::new(
        TableId::new(keyspace, table),
        partition_key,
        clustering_key,
        operations,
        timestamp_micros,
        ttl_seconds,
    )
}

fn schema_simple_table() -> TableSchema {
    build_schema(
        "test_basic",
        "simple_table",
        vec![key_column("id", "uuid", 0)],
        vec![],
        vec![
            regular_column("id", "uuid"),
            regular_column("name", "text"),
            regular_column("age", "int"),
            regular_column("salary", "bigint"),
            regular_column("height", "float"),
            regular_column("weight", "double"),
            regular_column("active", "boolean"),
            regular_column("created", "timestamp"),
            regular_column("birth_date", "date"),
            regular_column("work_time", "time"),
            regular_column("description", "blob"),
            regular_column("account_balance", "decimal"),
            regular_column("session_id", "timeuuid"),
            regular_column("ip_address", "inet"),
            regular_column("small_number", "tinyint"),
            regular_column("medium_number", "smallint"),
            regular_column("duration_val", "duration"),
            regular_column("varchar_field", "varchar"),
            regular_column("ascii_field", "ascii"),
        ],
    )
}

fn create_simple_table_issue_432_cql() -> String {
    r#"CREATE TABLE test_basic.simple_table (
id UUID PRIMARY KEY,
name TEXT,
age INT,
salary BIGINT,
height FLOAT,
weight DOUBLE,
active BOOLEAN,
created TIMESTAMP,
birth_date DATE,
work_time TIME,
description BLOB,
account_balance DECIMAL,
session_id TIMEUUID,
ip_address INET,
small_number TINYINT,
medium_number SMALLINT,
duration_val DURATION,
varchar_field VARCHAR,
ascii_field ASCII
);"#
    .to_string()
}

fn simple_table_mutation(
    id: &str,
    name: &str,
    age: i32,
    salary: i64,
    height: f32,
    weight: f64,
    active: bool,
    created: &str,
    birth_date: &str,
    work_time: &str,
    description: &[u8],
    account_balance: Value,
    session_id: &str,
    ip_address: &[u8],
    small_number: i8,
    medium_number: i16,
    duration_val: Value,
    varchar_field: &str,
    ascii_field: &str,
    timestamp_micros: i64,
) -> Mutation {
    base_mutation(
        "test_basic",
        "simple_table",
        PartitionKey::single("id", uuid_value(id)),
        None,
        vec![
            CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text(name.to_string()),
            },
            CellOperation::Write {
                column: "age".to_string(),
                value: Value::Integer(age),
            },
            CellOperation::Write {
                column: "salary".to_string(),
                value: Value::BigInt(salary),
            },
            CellOperation::Write {
                column: "height".to_string(),
                value: Value::Float32(height),
            },
            CellOperation::Write {
                column: "weight".to_string(),
                value: Value::Float(weight),
            },
            CellOperation::Write {
                column: "active".to_string(),
                value: Value::Boolean(active),
            },
            CellOperation::Write {
                column: "created".to_string(),
                value: Value::Timestamp(timestamp_millis(created)),
            },
            CellOperation::Write {
                column: "birth_date".to_string(),
                value: Value::Date(date_days(birth_date)),
            },
            CellOperation::Write {
                column: "work_time".to_string(),
                value: Value::Time(time_nanos(work_time)),
            },
            CellOperation::Write {
                column: "description".to_string(),
                value: Value::Blob(description.to_vec()),
            },
            CellOperation::Write {
                column: "account_balance".to_string(),
                value: account_balance,
            },
            CellOperation::Write {
                column: "session_id".to_string(),
                value: uuid_value(session_id),
            },
            CellOperation::Write {
                column: "ip_address".to_string(),
                value: Value::Inet(ip_address.to_vec()),
            },
            CellOperation::Write {
                column: "small_number".to_string(),
                value: Value::TinyInt(small_number),
            },
            CellOperation::Write {
                column: "medium_number".to_string(),
                value: Value::SmallInt(medium_number),
            },
            CellOperation::Write {
                column: "duration_val".to_string(),
                value: duration_val,
            },
            CellOperation::Write {
                column: "varchar_field".to_string(),
                value: Value::Text(varchar_field.to_string()),
            },
            CellOperation::Write {
                column: "ascii_field".to_string(),
                value: Value::Text(ascii_field.to_string()),
            },
        ],
        timestamp_micros,
        None,
    )
}

fn schema_composite_key_table() -> TableSchema {
    build_schema(
        "test_basic",
        "composite_key_table",
        vec![key_column("partition_key", "uuid", 0)],
        vec![
            clustering_column("clustering_key1", "timestamp", 0, ClusteringOrder::Desc),
            clustering_column("clustering_key2", "text", 1, ClusteringOrder::Asc),
        ],
        vec![
            regular_column("partition_key", "uuid"),
            regular_column("clustering_key1", "timestamp"),
            regular_column("clustering_key2", "text"),
            regular_column("data", "text"),
            regular_column("value", "int"),
        ],
    )
}

fn create_composite_key_table_issue_432_cql() -> String {
    r#"CREATE TABLE test_basic.composite_key_table (
partition_key UUID,
clustering_key1 TIMESTAMP,
clustering_key2 TEXT,
data TEXT,
value INT,
PRIMARY KEY (partition_key, clustering_key1, clustering_key2)
) WITH CLUSTERING ORDER BY (clustering_key1 DESC, clustering_key2 ASC);"#
        .to_string()
}

fn composite_key_table_mutation(
    partition_key: &str,
    clustering_key1: &str,
    clustering_key2: &str,
    data: &str,
    value: i32,
    timestamp_micros: i64,
) -> Mutation {
    base_mutation(
        "test_basic",
        "composite_key_table",
        PartitionKey::single("partition_key", uuid_value(partition_key)),
        Some(ClusteringKey::new(vec![
            (
                "clustering_key1".to_string(),
                Value::Timestamp(timestamp_millis(clustering_key1)),
            ),
            (
                "clustering_key2".to_string(),
                Value::Text(clustering_key2.to_string()),
            ),
        ])),
        vec![
            CellOperation::Write {
                column: "data".to_string(),
                value: Value::Text(data.to_string()),
            },
            CellOperation::Write {
                column: "value".to_string(),
                value: Value::Integer(value),
            },
        ],
        timestamp_micros,
        None,
    )
}

fn schema_multi_partition_table() -> TableSchema {
    build_schema(
        "test_basic",
        "multi_partition_table",
        vec![
            key_column("tenant_id", "uuid", 0),
            key_column("user_id", "uuid", 1),
        ],
        vec![
            clustering_column("category", "text", 0, ClusteringOrder::Asc),
            clustering_column("item_id", "timeuuid", 1, ClusteringOrder::Asc),
        ],
        vec![
            regular_column("tenant_id", "uuid"),
            regular_column("user_id", "uuid"),
            regular_column("category", "text"),
            regular_column("item_id", "timeuuid"),
            regular_column("name", "text"),
            regular_column("value", "bigint"),
            regular_column("metadata", "text"),
        ],
    )
}

fn create_multi_partition_table_issue_432_cql() -> String {
    r#"CREATE TABLE test_basic.multi_partition_table (
tenant_id UUID,
user_id UUID,
category TEXT,
item_id TIMEUUID,
name TEXT,
value BIGINT,
metadata TEXT,
PRIMARY KEY ((tenant_id, user_id), category, item_id)
);"#
    .to_string()
}

fn multi_partition_table_mutation(
    tenant_id: &str,
    user_id: &str,
    category: &str,
    item_id: &str,
    name: &str,
    value: i64,
    metadata: &str,
    timestamp_micros: i64,
) -> Mutation {
    base_mutation(
        "test_basic",
        "multi_partition_table",
        PartitionKey::new(vec![
            ("tenant_id".to_string(), uuid_value(tenant_id)),
            ("user_id".to_string(), uuid_value(user_id)),
        ]),
        Some(ClusteringKey::new(vec![
            ("category".to_string(), Value::Text(category.to_string())),
            ("item_id".to_string(), uuid_value(item_id)),
        ])),
        vec![
            CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text(name.to_string()),
            },
            CellOperation::Write {
                column: "value".to_string(),
                value: Value::BigInt(value),
            },
            CellOperation::Write {
                column: "metadata".to_string(),
                value: Value::Text(metadata.to_string()),
            },
        ],
        timestamp_micros,
        None,
    )
}

fn schema_static_columns_table() -> TableSchema {
    build_schema(
        "test_basic",
        "static_columns_table",
        vec![key_column("partition_key", "uuid", 0)],
        vec![clustering_column(
            "clustering_key",
            "timestamp",
            0,
            ClusteringOrder::Asc,
        )],
        vec![
            regular_column("partition_key", "uuid"),
            regular_column("clustering_key", "timestamp"),
            static_column("static_data", "text"),
            regular_column("row_data", "text"),
            regular_column("row_value", "int"),
        ],
    )
}

fn create_static_columns_table_issue_432_cql() -> String {
    r#"CREATE TABLE test_basic.static_columns_table (
partition_key UUID,
clustering_key TIMESTAMP,
static_data TEXT STATIC,
row_data TEXT,
row_value INT,
PRIMARY KEY (partition_key, clustering_key)
);"#
    .to_string()
}

fn static_only_mutation(
    keyspace: &str,
    table: &str,
    partition_key: &str,
    static_data: &str,
    timestamp_micros: i64,
) -> Mutation {
    base_mutation(
        keyspace,
        table,
        PartitionKey::single("partition_key", uuid_value(partition_key)),
        None,
        vec![CellOperation::Write {
            column: "static_data".to_string(),
            value: Value::Text(static_data.to_string()),
        }],
        timestamp_micros,
        None,
    )
}

fn static_row_mutation(
    partition_key: &str,
    clustering_key: &str,
    row_data: &str,
    row_value: i32,
    timestamp_micros: i64,
) -> Mutation {
    base_mutation(
        "test_basic",
        "static_columns_table",
        PartitionKey::single("partition_key", uuid_value(partition_key)),
        Some(ClusteringKey::single(
            "clustering_key",
            Value::Timestamp(timestamp_millis(clustering_key)),
        )),
        vec![
            CellOperation::Write {
                column: "row_data".to_string(),
                value: Value::Text(row_data.to_string()),
            },
            CellOperation::Write {
                column: "row_value".to_string(),
                value: Value::Integer(row_value),
            },
        ],
        timestamp_micros,
        None,
    )
}

fn schema_ttl_test_table() -> TableSchema {
    build_schema(
        "test_basic",
        "ttl_test_table",
        vec![key_column("id", "uuid", 0)],
        vec![],
        vec![
            regular_column("id", "uuid"),
            regular_column("temporary_data", "text"),
            regular_column("expiring_value", "int"),
            regular_column("session_info", "text"),
        ],
    )
}

fn create_ttl_test_table_issue_432_cql() -> String {
    r#"CREATE TABLE test_basic.ttl_test_table (
id UUID PRIMARY KEY,
temporary_data TEXT,
expiring_value INT,
session_info TEXT
);"#
    .to_string()
}

fn ttl_test_table_mutation(
    id: &str,
    temporary_data: &str,
    expiring_value: i32,
    session_info: &str,
    ttl_seconds: u32,
    timestamp_micros: i64,
) -> Mutation {
    base_mutation(
        "test_basic",
        "ttl_test_table",
        PartitionKey::single("id", uuid_value(id)),
        None,
        vec![
            CellOperation::Write {
                column: "temporary_data".to_string(),
                value: Value::Text(temporary_data.to_string()),
            },
            CellOperation::Write {
                column: "expiring_value".to_string(),
                value: Value::Integer(expiring_value),
            },
            CellOperation::Write {
                column: "session_info".to_string(),
                value: Value::Text(session_info.to_string()),
            },
        ],
        timestamp_micros,
        Some(ttl_seconds),
    )
}

fn schema_sensor_data() -> TableSchema {
    build_schema(
        "test_timeseries",
        "sensor_data",
        vec![key_column("sensor_id", "uuid", 0)],
        vec![clustering_column(
            "timestamp",
            "timestamp",
            0,
            ClusteringOrder::Desc,
        )],
        vec![
            regular_column("sensor_id", "uuid"),
            regular_column("timestamp", "timestamp"),
            regular_column("temperature", "float"),
            regular_column("humidity", "float"),
            regular_column("pressure", "double"),
            regular_column("battery_level", "tinyint"),
            regular_column("location", "text"),
            regular_column("status", "text"),
        ],
    )
}

fn create_sensor_data_issue_432_cql() -> String {
    r#"CREATE TABLE test_timeseries.sensor_data (
sensor_id UUID,
timestamp TIMESTAMP,
temperature FLOAT,
humidity FLOAT,
pressure DOUBLE,
battery_level TINYINT,
location TEXT,
status TEXT,
PRIMARY KEY (sensor_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);"#
        .to_string()
}

fn sensor_data_mutation(
    sensor_id: &str,
    timestamp: &str,
    temperature: f32,
    humidity: f32,
    pressure: f64,
    battery_level: i8,
    location: &str,
    status: &str,
    timestamp_micros: i64,
) -> Mutation {
    base_mutation(
        "test_timeseries",
        "sensor_data",
        PartitionKey::single("sensor_id", uuid_value(sensor_id)),
        Some(ClusteringKey::single(
            "timestamp",
            Value::Timestamp(timestamp_millis(timestamp)),
        )),
        vec![
            CellOperation::Write {
                column: "temperature".to_string(),
                value: Value::Float32(temperature),
            },
            CellOperation::Write {
                column: "humidity".to_string(),
                value: Value::Float32(humidity),
            },
            CellOperation::Write {
                column: "pressure".to_string(),
                value: Value::Float(pressure),
            },
            CellOperation::Write {
                column: "battery_level".to_string(),
                value: Value::TinyInt(battery_level),
            },
            CellOperation::Write {
                column: "location".to_string(),
                value: Value::Text(location.to_string()),
            },
            CellOperation::Write {
                column: "status".to_string(),
                value: Value::Text(status.to_string()),
            },
        ],
        timestamp_micros,
        None,
    )
}

fn schema_stock_prices() -> TableSchema {
    build_schema(
        "test_timeseries",
        "stock_prices",
        vec![
            key_column("symbol", "text", 0),
            key_column("trading_day", "date", 1),
        ],
        vec![clustering_column(
            "timestamp",
            "timestamp",
            0,
            ClusteringOrder::Asc,
        )],
        vec![
            regular_column("symbol", "text"),
            regular_column("trading_day", "date"),
            regular_column("timestamp", "timestamp"),
            regular_column("open_price", "decimal"),
            regular_column("high_price", "decimal"),
            regular_column("low_price", "decimal"),
            regular_column("close_price", "decimal"),
            regular_column("volume", "bigint"),
            regular_column("adjusted_close", "decimal"),
        ],
    )
}

fn create_stock_prices_issue_432_cql() -> String {
    r#"CREATE TABLE test_timeseries.stock_prices (
symbol TEXT,
trading_day DATE,
timestamp TIMESTAMP,
open_price DECIMAL,
high_price DECIMAL,
low_price DECIMAL,
close_price DECIMAL,
volume BIGINT,
adjusted_close DECIMAL,
PRIMARY KEY ((symbol, trading_day), timestamp)
) WITH CLUSTERING ORDER BY (timestamp ASC);"#
        .to_string()
}

fn stock_prices_mutation(
    symbol: &str,
    trading_day: &str,
    timestamp: &str,
    open_price: Value,
    high_price: Value,
    low_price: Value,
    close_price: Value,
    volume: i64,
    adjusted_close: Value,
    timestamp_micros: i64,
) -> Mutation {
    base_mutation(
        "test_timeseries",
        "stock_prices",
        PartitionKey::new(vec![
            ("symbol".to_string(), Value::Text(symbol.to_string())),
            (
                "trading_day".to_string(),
                Value::Date(date_days(trading_day)),
            ),
        ]),
        Some(ClusteringKey::single(
            "timestamp",
            Value::Timestamp(timestamp_millis(timestamp)),
        )),
        vec![
            CellOperation::Write {
                column: "open_price".to_string(),
                value: open_price,
            },
            CellOperation::Write {
                column: "high_price".to_string(),
                value: high_price,
            },
            CellOperation::Write {
                column: "low_price".to_string(),
                value: low_price,
            },
            CellOperation::Write {
                column: "close_price".to_string(),
                value: close_price,
            },
            CellOperation::Write {
                column: "volume".to_string(),
                value: Value::BigInt(volume),
            },
            CellOperation::Write {
                column: "adjusted_close".to_string(),
                value: adjusted_close,
            },
        ],
        timestamp_micros,
        None,
    )
}

fn schema_wide_partition_table() -> TableSchema {
    build_schema(
        "test_wide_rows",
        "wide_partition_table",
        vec![key_column("partition_key", "uuid", 0)],
        vec![
            clustering_column("clustering_col1", "timestamp", 0, ClusteringOrder::Desc),
            clustering_column("clustering_col2", "text", 1, ClusteringOrder::Asc),
            clustering_column("clustering_col3", "int", 2, ClusteringOrder::Desc),
            clustering_column("clustering_col4", "uuid", 3, ClusteringOrder::Asc),
            clustering_column("clustering_col5", "date", 4, ClusteringOrder::Desc),
        ],
        vec![
            regular_column("partition_key", "uuid"),
            regular_column("clustering_col1", "timestamp"),
            regular_column("clustering_col2", "text"),
            regular_column("clustering_col3", "int"),
            regular_column("clustering_col4", "uuid"),
            regular_column("clustering_col5", "date"),
            regular_column("data_column", "text"),
            regular_column("value_column", "bigint"),
            regular_column("blob_column", "blob"),
            regular_column("json_column", "text"),
        ],
    )
}

fn create_wide_partition_table_issue_432_cql() -> String {
    r#"CREATE TABLE test_wide_rows.wide_partition_table (
partition_key UUID,
clustering_col1 TIMESTAMP,
clustering_col2 TEXT,
clustering_col3 INT,
clustering_col4 UUID,
clustering_col5 DATE,
data_column TEXT,
value_column BIGINT,
blob_column BLOB,
json_column TEXT,
PRIMARY KEY (partition_key, clustering_col1, clustering_col2, clustering_col3, clustering_col4, clustering_col5)
) WITH CLUSTERING ORDER BY (clustering_col1 DESC, clustering_col2 ASC, clustering_col3 DESC, clustering_col4 ASC, clustering_col5 DESC);"#
        .to_string()
}

fn wide_partition_table_mutation(
    partition_key: &str,
    clustering_col1: &str,
    clustering_col2: &str,
    clustering_col3: i32,
    clustering_col4: &str,
    clustering_col5: &str,
    data_column: &str,
    value_column: i64,
    blob_column: &[u8],
    json_column: &str,
    timestamp_micros: i64,
) -> Mutation {
    base_mutation(
        "test_wide_rows",
        "wide_partition_table",
        PartitionKey::single("partition_key", uuid_value(partition_key)),
        Some(ClusteringKey::new(vec![
            (
                "clustering_col1".to_string(),
                Value::Timestamp(timestamp_millis(clustering_col1)),
            ),
            (
                "clustering_col2".to_string(),
                Value::Text(clustering_col2.to_string()),
            ),
            (
                "clustering_col3".to_string(),
                Value::Integer(clustering_col3),
            ),
            ("clustering_col4".to_string(), uuid_value(clustering_col4)),
            (
                "clustering_col5".to_string(),
                Value::Date(date_days(clustering_col5)),
            ),
        ])),
        vec![
            CellOperation::Write {
                column: "data_column".to_string(),
                value: Value::Text(data_column.to_string()),
            },
            CellOperation::Write {
                column: "value_column".to_string(),
                value: Value::BigInt(value_column),
            },
            CellOperation::Write {
                column: "blob_column".to_string(),
                value: Value::Blob(blob_column.to_vec()),
            },
            CellOperation::Write {
                column: "json_column".to_string(),
                value: Value::Text(json_column.to_string()),
            },
        ],
        timestamp_micros,
        None,
    )
}

fn schema_issue_439_mixed_columns_table() -> TableSchema {
    build_schema(
        "test_wide_rows",
        "issue_439_mixed_columns",
        vec![key_column("id", "uuid", 0)],
        vec![],
        vec![
            regular_column("id", "uuid"),
            regular_column("z_simple", "text"),
            regular_column("a_complex", "set<text>"),
        ],
    )
}

fn create_issue_439_mixed_columns_table_cql() -> String {
    r#"CREATE TABLE test_wide_rows.issue_439_mixed_columns (
id UUID PRIMARY KEY,
z_simple TEXT,
a_complex SET<TEXT>
);"#
    .to_string()
}

fn issue_439_mixed_columns_mutation(
    id: &str,
    z_simple: &str,
    a_complex: &[&str],
    timestamp_micros: i64,
) -> Mutation {
    base_mutation(
        "test_wide_rows",
        "issue_439_mixed_columns",
        PartitionKey::single("id", uuid_value(id)),
        None,
        vec![
            CellOperation::Write {
                column: "z_simple".to_string(),
                value: Value::Text(z_simple.to_string()),
            },
            CellOperation::Write {
                column: "a_complex".to_string(),
                value: Value::Set(
                    a_complex
                        .iter()
                        .map(|value| Value::Text((*value).to_string()))
                        .collect(),
                ),
            },
        ],
        timestamp_micros,
        None,
    )
}

fn schema_issue_439_product_catalog() -> TableSchema {
    build_schema(
        "test_wide_rows",
        "product_catalog_issue_439",
        vec![key_column("category_id", "uuid", 0)],
        vec![clustering_column(
            "product_id",
            "uuid",
            0,
            ClusteringOrder::Asc,
        )],
        vec![
            regular_column("category_id", "uuid"),
            regular_column("product_id", "uuid"),
            regular_column("product_name", "text"),
            regular_column("description", "text"),
            regular_column("long_description", "text"),
            regular_column("specifications", "map<text, text>"),
            regular_column("images", "list<text>"),
            regular_column("tags", "set<text>"),
            regular_column("price", "decimal"),
            regular_column("currency", "text"),
            regular_column("availability_count", "int"),
            regular_column("weight", "float"),
            regular_column("dimensions", "map<text, float>"),
            regular_column("reviews_summary", "map<text, double>"),
            regular_column("attributes", "map<text, frozen<set<text>>>"),
            regular_column("related_products", "set<uuid>"),
            regular_column("created_at", "timestamp"),
            regular_column("updated_at", "timestamp"),
        ],
    )
}

fn create_issue_439_product_catalog_cql() -> String {
    r#"CREATE TABLE test_wide_rows.product_catalog_issue_439 (
category_id UUID,
product_id UUID,
product_name TEXT,
description TEXT,
long_description TEXT,
specifications MAP<TEXT, TEXT>,
images LIST<TEXT>,
tags SET<TEXT>,
price DECIMAL,
currency TEXT,
availability_count INT,
weight FLOAT,
dimensions MAP<TEXT, FLOAT>,
reviews_summary MAP<TEXT, DOUBLE>,
attributes MAP<TEXT, FROZEN<SET<TEXT>>>,
related_products SET<UUID>,
created_at TIMESTAMP,
updated_at TIMESTAMP,
PRIMARY KEY (category_id, product_id)
);"#
    .to_string()
}

fn issue_439_product_catalog_mutation(
    category_id: &str,
    product_id: &str,
    product_name: &str,
    description: &str,
    availability_count: i32,
    timestamp_micros: i64,
) -> Mutation {
    base_mutation(
        "test_wide_rows",
        "product_catalog_issue_439",
        PartitionKey::single("category_id", uuid_value(category_id)),
        Some(ClusteringKey::single("product_id", uuid_value(product_id))),
        vec![
            CellOperation::Write {
                column: "product_name".to_string(),
                value: Value::Text(product_name.to_string()),
            },
            CellOperation::Write {
                column: "description".to_string(),
                value: Value::Text(description.to_string()),
            },
            CellOperation::Write {
                column: "long_description".to_string(),
                value: Value::Text(format!("{description} with extended specs")),
            },
            CellOperation::Write {
                column: "specifications".to_string(),
                value: Value::Map(vec![
                    (
                        Value::Text("material".to_string()),
                        Value::Text("nylon".to_string()),
                    ),
                    (
                        Value::Text("origin".to_string()),
                        Value::Text("USA".to_string()),
                    ),
                ]),
            },
            CellOperation::Write {
                column: "images".to_string(),
                value: Value::List(vec![
                    Value::Text("front.jpg".to_string()),
                    Value::Text("detail.jpg".to_string()),
                ]),
            },
            CellOperation::Write {
                column: "tags".to_string(),
                value: Value::Set(vec![
                    Value::Text("featured".to_string()),
                    Value::Text("seasonal".to_string()),
                ]),
            },
            CellOperation::Write {
                column: "price".to_string(),
                value: decimal_from_i64(12_999, 2),
            },
            CellOperation::Write {
                column: "currency".to_string(),
                value: Value::Text("USD".to_string()),
            },
            CellOperation::Write {
                column: "availability_count".to_string(),
                value: Value::Integer(availability_count),
            },
            CellOperation::Write {
                column: "weight".to_string(),
                value: Value::Float32(1.25),
            },
            CellOperation::Write {
                column: "dimensions".to_string(),
                value: Value::Map(vec![
                    (Value::Text("height".to_string()), Value::Float32(10.0)),
                    (Value::Text("width".to_string()), Value::Float32(20.0)),
                ]),
            },
            CellOperation::Write {
                column: "reviews_summary".to_string(),
                value: Value::Map(vec![
                    (Value::Text("average".to_string()), Value::Float(4.8)),
                    (Value::Text("count".to_string()), Value::Float(128.0)),
                ]),
            },
            CellOperation::Write {
                column: "attributes".to_string(),
                value: Value::Map(vec![
                    (
                        Value::Text("terrain".to_string()),
                        Value::Frozen(Box::new(Value::Set(vec![
                            Value::Text("trail".to_string()),
                            Value::Text("rock".to_string()),
                        ]))),
                    ),
                    (
                        Value::Text("season".to_string()),
                        Value::Frozen(Box::new(Value::Set(vec![
                            Value::Text("spring".to_string()),
                            Value::Text("fall".to_string()),
                        ]))),
                    ),
                ]),
            },
            CellOperation::Write {
                column: "related_products".to_string(),
                value: Value::Set(vec![
                    uuid_value("50000000-0000-4000-8000-000000000005"),
                    uuid_value("60000000-0000-4000-8000-000000000006"),
                ]),
            },
            CellOperation::Write {
                column: "created_at".to_string(),
                value: Value::Timestamp(timestamp_millis("2024-03-10T18:45:30.000Z")),
            },
            CellOperation::Write {
                column: "updated_at".to_string(),
                value: Value::Timestamp(timestamp_millis("2024-03-10T18:46:30.000Z")),
            },
        ],
        timestamp_micros,
        None,
    )
}

fn schema_issue_434_phase3_complex_table() -> TableSchema {
    build_schema(
        "test_phase3",
        "issue_434_complex_types",
        vec![key_column("id", "uuid", 0)],
        vec![],
        vec![
            regular_column("id", "uuid"),
            regular_column(
                "nested_map_list",
                "map<text, frozen<list<frozen<map<text, int>>>>>",
            ),
            regular_column("company_map", "map<text, frozen<company>>"),
            regular_column("tuple_with_list", "tuple<text, list<int>>"),
            regular_column(
                "complex_tuple",
                "tuple<text, frozen<list<int>>, frozen<map<text, frozen<address>>>, frozen<person>>",
            ),
            regular_column("unicode_map", "map<text, text>"),
            regular_column("empty_list", "list<text>"),
            regular_column("nullable_map", "map<text, text>"),
        ],
    )
}

fn create_issue_434_phase3_complex_table_cql() -> String {
    r#"CREATE TYPE IF NOT EXISTS test_phase3.address (
    street TEXT,
    city TEXT
);
CREATE TYPE IF NOT EXISTS test_phase3.phone_number (
    label TEXT,
    number TEXT
);
CREATE TYPE IF NOT EXISTS test_phase3.person (
    name TEXT,
    phone_numbers LIST<FROZEN<phone_number>>,
    home_address FROZEN<address>
);
CREATE TYPE IF NOT EXISTS test_phase3.company (
    name TEXT,
    employees LIST<FROZEN<person>>,
    departments MAP<TEXT, FROZEN<LIST<FROZEN<person>>>>
);
CREATE TABLE test_phase3.issue_434_complex_types (
    id UUID PRIMARY KEY,
    nested_map_list MAP<TEXT, FROZEN<LIST<FROZEN<MAP<TEXT, INT>>>>>,
    company_map MAP<TEXT, FROZEN<company>>,
    tuple_with_list TUPLE<TEXT, LIST<INT>>,
    complex_tuple TUPLE<TEXT, FROZEN<LIST<INT>>, FROZEN<MAP<TEXT, FROZEN<address>>>, FROZEN<person>>,
    unicode_map MAP<TEXT, TEXT>,
    empty_list LIST<TEXT>,
    nullable_map MAP<TEXT, TEXT>
);"#
        .to_string()
}

fn issue_434_address_value(street: &str, city: &str) -> Value {
    Value::Udt(Box::new(cqlite_core::types::UdtValue::new("address".to_string(), "test_phase3".to_string())
        .with_field("street".to_string(), Some(Value::Text(street.to_string())))
        .with_field("city".to_string(), Some(Value::Text(city.to_string())))))
}

fn issue_434_phone_value(label: &str, number: &str) -> Value {
    Value::Udt(Box::new(cqlite_core::types::UdtValue::new("phone_number".to_string(), "test_phase3".to_string())
        .with_field("label".to_string(), Some(Value::Text(label.to_string())))
        .with_field("number".to_string(), Some(Value::Text(number.to_string())))))
}

fn issue_434_person_value(name: &str) -> Value {
    Value::Udt(Box::new(cqlite_core::types::UdtValue::new("person".to_string(), "test_phase3".to_string())
        .with_field("name".to_string(), Some(Value::Text(name.to_string())))
        .with_field(
            "phone_numbers".to_string(),
            Some(Value::List(vec![Value::Frozen(Box::new(
                issue_434_phone_value("mobile", "+1-555-0101"),
            ))])),
        )
        .with_field(
            "home_address".to_string(),
            Some(Value::Frozen(Box::new(issue_434_address_value(
                "Main St", "Seattle",
            )))),
        )))
}

fn issue_434_company_value() -> Value {
    let person = issue_434_person_value("Alice");
    Value::Udt(Box::new(cqlite_core::types::UdtValue::new("company".to_string(), "test_phase3".to_string())
        .with_field("name".to_string(), Some(Value::Text("Acme".to_string())))
        .with_field(
            "employees".to_string(),
            Some(Value::List(vec![Value::Frozen(Box::new(person.clone()))])),
        )
        .with_field(
            "departments".to_string(),
            Some(Value::Map(vec![(
                Value::Text("platform".to_string()),
                Value::Frozen(Box::new(Value::List(vec![Value::Frozen(Box::new(person))]))),
            )])),
        )))
}

fn issue_434_phase3_complex_mutation(id: &str, timestamp_micros: i64) -> Mutation {
    base_mutation(
        "test_phase3",
        "issue_434_complex_types",
        PartitionKey::single("id", uuid_value(id)),
        None,
        vec![
            CellOperation::Write {
                column: "nested_map_list".to_string(),
                value: Value::Map(vec![(
                    Value::Text("outer".to_string()),
                    Value::Frozen(Box::new(Value::List(vec![Value::Frozen(Box::new(
                        Value::Map(vec![(Value::Text("inner".to_string()), Value::Integer(7))]),
                    ))]))),
                )]),
            },
            CellOperation::Write {
                column: "company_map".to_string(),
                value: Value::Map(vec![(
                    Value::Text("primary".to_string()),
                    Value::Frozen(Box::new(issue_434_company_value())),
                )]),
            },
            CellOperation::Write {
                column: "tuple_with_list".to_string(),
                value: Value::Tuple(vec![
                    Value::Text("phase3".to_string()),
                    Value::List(vec![
                        Value::Integer(1),
                        Value::Integer(2),
                        Value::Integer(3),
                    ]),
                ]),
            },
            CellOperation::Write {
                column: "complex_tuple".to_string(),
                value: Value::Tuple(vec![
                    Value::Text("tuple".to_string()),
                    Value::Frozen(Box::new(Value::List(vec![
                        Value::Integer(3),
                        Value::Integer(5),
                        Value::Integer(8),
                    ]))),
                    Value::Frozen(Box::new(Value::Map(vec![(
                        Value::Text("home".to_string()),
                        Value::Frozen(Box::new(issue_434_address_value("Main St", "Seattle"))),
                    )]))),
                    Value::Frozen(Box::new(issue_434_person_value("Tuple User"))),
                ]),
            },
            CellOperation::Write {
                column: "unicode_map".to_string(),
                value: Value::Map(vec![(
                    Value::Text("挨拶".to_string()),
                    Value::Text("こんにちは".to_string()),
                )]),
            },
            CellOperation::Write {
                column: "empty_list".to_string(),
                value: Value::List(vec![]),
            },
        ],
        timestamp_micros,
        None,
    )
}

fn schema_large_blob_table() -> TableSchema {
    build_schema(
        "test_wide_rows",
        "large_blob_table",
        vec![key_column("file_id", "uuid", 0)],
        vec![clustering_column(
            "chunk_id",
            "int",
            0,
            ClusteringOrder::Asc,
        )],
        vec![
            regular_column("file_id", "uuid"),
            regular_column("chunk_id", "int"),
            regular_column("file_name", "text"),
            regular_column("mime_type", "text"),
            regular_column("chunk_data", "blob"),
            regular_column("chunk_size", "int"),
            regular_column("total_chunks", "int"),
            regular_column("checksum", "text"),
        ],
    )
}

fn create_large_blob_table_issue_432_cql() -> String {
    r#"CREATE TABLE test_wide_rows.large_blob_table (
file_id UUID,
chunk_id INT,
file_name TEXT,
mime_type TEXT,
chunk_data BLOB,
chunk_size INT,
total_chunks INT,
checksum TEXT,
PRIMARY KEY (file_id, chunk_id)
) WITH CLUSTERING ORDER BY (chunk_id ASC);"#
        .to_string()
}

fn large_blob_table_mutation(
    file_id: &str,
    chunk_id: i32,
    file_name: &str,
    mime_type: &str,
    chunk_data: &[u8],
    chunk_size: i32,
    total_chunks: i32,
    checksum: &str,
    timestamp_micros: i64,
) -> Mutation {
    base_mutation(
        "test_wide_rows",
        "large_blob_table",
        PartitionKey::single("file_id", uuid_value(file_id)),
        Some(ClusteringKey::single("chunk_id", Value::Integer(chunk_id))),
        vec![
            CellOperation::Write {
                column: "file_name".to_string(),
                value: Value::Text(file_name.to_string()),
            },
            CellOperation::Write {
                column: "mime_type".to_string(),
                value: Value::Text(mime_type.to_string()),
            },
            CellOperation::Write {
                column: "chunk_data".to_string(),
                value: Value::Blob(chunk_data.to_vec()),
            },
            CellOperation::Write {
                column: "chunk_size".to_string(),
                value: Value::Integer(chunk_size),
            },
            CellOperation::Write {
                column: "total_chunks".to_string(),
                value: Value::Integer(total_chunks),
            },
            CellOperation::Write {
                column: "checksum".to_string(),
                value: Value::Text(checksum.to_string()),
            },
        ],
        timestamp_micros,
        None,
    )
}
