//! `sstableloader` integration tests for issue #396.
//!
//! These tests validate the full write path:
//! 1. Create schema in a real Cassandra 5.0 node
//! 2. Write mutations with CQLite
//! 3. Package the flushed SSTables into a loader-friendly directory
//! 4. Load them with `sstableloader`
//! 5. Verify results through CQL queries
//!
//! The product contract is that flush already produces portable Cassandra
//! SSTable components. The packaging step used here is only a loader/import
//! convenience for the current test harness.

#![cfg(all(feature = "write-support", feature = "docker-integration"))]

#[path = "../../tests/helpers/docker.rs"]
mod docker_helpers;

use cqlite_core::{
    error::{Error, Result as CqliteResult},
    schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema},
    storage::write_engine::{
        CellOperation, ClusteringKey, ExportOptions, Mutation, PartitionKey, TableId, WriteEngine,
        WriteEngineConfig,
    },
    types::Value,
};
use docker_helpers::{CassandraContainer, CqlshOutput};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::sync::Mutex;

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
        recreate_table(&cassandra, &schema.keyspace, &schema.table, create_table_cql)?;

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

    async fn package_for_loader_import(&self) -> CqliteResult<()> {
        let report = {
            let mut engine = self.engine.lock().await;
            engine
                .export_sstable(
                    self.package_dir.path(),
                    ExportOptions::new(&self.keyspace, &self.table, 1),
                )
                .await?
        };

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
            .write(simple_mutation("loader_simple", id, name, value, 1_704_067_200_000_000))
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

    let rows = [(42, "Zoe", 4200), (1, "Alice", 100), (100, "Max", 1000), (7, "Drew", 700)];
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
    assert_eq!(output.rows, vec![vec!["2".to_string(), "Bob".to_string(), "200".to_string()]]);

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
    cassandra.execute_cql(create_table_cql).map_err(io_to_cqlite)?;
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
    }
}

fn create_simple_table_cql(table_name: &str) -> String {
    format!(
        "CREATE TABLE {KEYSPACE}.{table_name} (id int PRIMARY KEY, name text, value int);"
    )
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

fn simple_mutation(
    table_name: &str,
    id: i32,
    name: &str,
    value: i32,
    timestamp: i64,
) -> Mutation {
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

fn clustered_mutation(
    table_name: &str,
    pk: i32,
    ck: &str,
    data: &str,
    timestamp: i64,
) -> Mutation {
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
                0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0x4D, 0xEF, 0x81, 0x23, 0x45, 0x67, 0x89,
                0xAB, 0xCD, 0xEF,
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
