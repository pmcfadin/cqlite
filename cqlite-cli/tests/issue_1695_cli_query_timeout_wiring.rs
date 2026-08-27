//! Issue #1695: END-TO-END wiring evidence for `performance.query_timeout_ms`.
//!
//! # What this lane proves
//!
//! The CLI knob is not decoration: it reaches the field the query engine
//! enforces, and a query that exceeds it fails with the CLI's query-execution
//! exit code. The chain asserted here, in order:
//!
//! ```text
//! cqlite_cli::config::PerformanceConfig::query_timeout_ms   (the operator knob)
//!   -> cqlite_cli::core_config::to_core_config              (the mapping, #1695)
//!   -> cqlite_core::Config.query.max_execution_time         (the enforced field)
//!   -> Database::execute -> QueryEngine::execute            (the chokepoint bound)
//!   -> cqlite_core::Error::QueryTimeout                     (the typed outcome)
//!   -> cqlite_cli::error::classify_error -> exit code 5     (the operator contract)
//! ```
//!
//! `main.rs`'s `create_core_config` is a one-line delegation to
//! `to_core_config`, so the binary and this test exercise the same mapping.
//!
//! # Fixtures
//!
//! The knob's unit is MILLISECONDS, so demonstrating an elapse needs a scan that
//! reliably takes longer than 1ms: the corpus's largest FETCHED table (~650 KiB /
//! 1000 partitions). Its binaries are gitignored, so that case skips clean on a
//! minimal checkout (and fails closed under `CQLITE_REQUIRE_FIXTURES=1`). The
//! `0` = unbounded direction needs no timing and uses a COMMITTED fixture, so it
//! always runs. No wall-clock threshold is asserted anywhere (issue #2642).

#![cfg(feature = "cli-helpers")]

// The shared TABLE-granular fixture resolver (issue #3220). Included by path
// because it lives in cqlite-core's test tree; its own nested `#[path]` to
// `test-data/support/fixture_roots.rs` resolves relative to THAT file, and its
// repo-root anchor is the workspace `Cargo.toml`, so both work from this crate.
#[path = "../../cqlite-core/tests/support/datasets_root.rs"]
mod datasets_root;

use std::time::Duration;

use cqlite_cli::config::Config as CliConfig;
use cqlite_cli::core_config::to_core_config;
use cqlite_cli::error::{classify_error, CliExitCode};
use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::{Database, Error};

use datasets_root::{describe_search, schema_path, sstables_root_for_table};

/// `true` when the dataset-dependent lanes must fail rather than skip.
fn require_fixtures() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

/// Open a fixture `Database` through the CLI's OWN configuration mapping, with
/// `query_timeout_ms` as the only knob varied. Returns `None` (skip-clean) when a
/// non-committed fixture is absent.
async fn open_via_cli_config(
    keyspace: &str,
    table: &str,
    schema_file: &str,
    committed: bool,
    query_timeout_ms: u64,
) -> Option<Database> {
    let Some(root) = sstables_root_for_table(keyspace, table) else {
        assert!(
            !committed && !require_fixtures(),
            "fixture must resolve (fail-closed): {}",
            describe_search(keyspace, table)
        );
        eprintln!(
            "SKIP: fetched fixture absent ({keyspace}.{table}); {} — set \
             CQLITE_REQUIRE_FIXTURES=1 to enforce.",
            describe_search(keyspace, table)
        );
        return None;
    };
    let schema = schema_path(schema_file).expect("committed schema must be readable (#3148)");

    // THE WIRING UNDER TEST: the operator sets milliseconds in the CLI config,
    // and nothing else in this test touches `max_execution_time`.
    let mut cli_config = CliConfig::default();
    cli_config.performance.query_timeout_ms = query_timeout_ms;
    let core_config =
        to_core_config(&cli_config).expect("CLI config must map to a valid core config");
    assert_eq!(
        core_config.query.max_execution_time,
        Duration::from_millis(query_timeout_ms),
        "the CLI knob must land on the field the engine enforces"
    );

    let result = ingest(IngestionConfig {
        schema_paths: vec![schema],
        data_dir: root,
        version_hint: None,
        core_config,
        table_directory_filter: Some(format!("/{keyspace}/")),
    })
    .await
    .expect("ingestion of the fixture");
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "schema must load"
    );
    Some(result.database)
}

/// `query_timeout_ms = 1` + a multi-millisecond real scan ⇒ the query fails with
/// the timeout error, and the CLI classifies it as a query-execution failure
/// (exit code 5) rather than as a data/corruption or data-dir problem.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_timeout_ms_bounds_a_cli_query_and_maps_to_exit_code_5() {
    let Some(db) =
        open_via_cli_config("test_basic", "simple_table", "basic-types.cql", false, 1).await
    else {
        return;
    };

    let outcome = db.execute("SELECT * FROM test_basic.simple_table").await;
    let err = match outcome {
        Err(e) => e,
        Ok(result) => panic!(
            "REGRESSION (issue #1695): the CLI's performance.query_timeout_ms = 1 was IGNORED — \
             the scan completed with {} rows. The knob is a placebo again.",
            result.rows.len()
        ),
    };
    assert!(
        matches!(err, Error::QueryTimeout { .. }),
        "the budget must surface as the distinct timeout error, not as an unrelated \
         failure: {err}"
    );

    // The operator contract: a timed-out query exits 5 (query execution), never 3
    // (schema) or 4 (data dir).
    let exit = classify_error(&anyhow::Error::new(err));
    assert_eq!(
        exit,
        CliExitCode::QueryExecutionError,
        "a query-budget elapse must map to the query-execution exit code"
    );
}

/// `query_timeout_ms = 0` is the documented "no timeout" spelling: it reaches the
/// core as `Duration::ZERO` and the query runs unbounded (non-empty result, so
/// this cannot pass on an empty fixture). Uses a COMMITTED fixture, so it always
/// runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_query_timeout_ms_leaves_a_cli_query_unbounded() {
    let db = open_via_cli_config(
        "test_comp",
        "incompressible_uncompressed_chunk",
        "compression-parity.cql",
        true,
        0,
    )
    .await
    .expect("the COMMITTED fixture can never skip");

    let result = db
        .execute("SELECT * FROM test_comp.incompressible_uncompressed_chunk")
        .await
        .expect("query_timeout_ms = 0 must mean UNBOUNDED, not an instant elapse");
    assert!(
        !result.rows.is_empty(),
        "the committed fixture must yield rows — a 0-row pass would make this vacuous"
    );
}
