//! Query execution command handler.
//!
//! Extracted verbatim from `commands/mod.rs` during the module split (issue #1126).

#![allow(dead_code)]

use crate::cli::OutputFormat;
#[cfg(feature = "state_machine")]
use anyhow::Context;
use anyhow::Result;
#[cfg(feature = "state_machine")]
use cqlite_core::Database;
use std::path::Path;

#[cfg(feature = "state_machine")]
pub async fn execute_query(
    database: &Database,
    query: &str,
    explain: bool,
    timing: bool,
    format: OutputFormat,
    config: &crate::config::OutputConfig,
) -> Result<()> {
    use crate::output::{write_to_target, OutputTarget};
    use std::time::Instant;

    let start_time = Instant::now();

    // Handle explain queries (always to stdout, not affected by --output)
    if explain {
        let explain_result = database
            .explain(query)
            .await
            .with_context(|| "Failed to explain query")?;

        println!("Query Explanation");
        println!("================");
        println!("Query Type: {}", explain_result.query_type);
        println!("Plan Type: {}", explain_result.plan_type);
        println!("Estimated Cost: {:.2}", explain_result.estimated_cost);
        println!("Estimated Rows: {}", explain_result.estimated_rows);

        if !explain_result.selected_indexes.is_empty() {
            println!("\nSelected Indexes:");
            for index in &explain_result.selected_indexes {
                println!("  - {index}");
            }
        }

        if !explain_result.execution_steps.is_empty() {
            println!("\nExecution Steps:");
            for (i, step) in explain_result.execution_steps.iter().enumerate() {
                println!("  {}. {}", i + 1, step);
            }
        }

        if !explain_result.parallelization_info.is_empty() {
            println!("\nParallelization:");
            for info in &explain_result.parallelization_info {
                println!("  - {info}");
            }
        }

        if timing {
            let elapsed = start_time.elapsed();
            println!("\nTiming: {:.2}ms", elapsed.as_millis());
        }

        return Ok(());
    }

    // Execute the query
    let result = database
        .execute(query)
        .await
        .with_context(|| "Failed to execute query")?;

    // Generate output bytes based on format (Issue #279)
    let output_bytes: Vec<u8> = match format {
        OutputFormat::Table => {
            use crate::output::table::TableWriter;
            let table_output = TableWriter::write(&result, config)
                .map_err(|e| anyhow::anyhow!("Failed to format table output: {}", e))?;
            table_output.into_bytes()
        }
        OutputFormat::Json => {
            use crate::output::json::JSONWriter;
            let json_output = JSONWriter::write(&result, config)
                .map_err(|e| anyhow::anyhow!("Failed to format JSON output: {}", e))?;
            json_output.into_bytes()
        }
        OutputFormat::Csv => {
            use crate::output::CSVWriter;
            let csv_output = CSVWriter::write(&result, config)
                .map_err(|e| anyhow::anyhow!("Failed to format CSV output: {}", e))?;
            csv_output.into_bytes()
        }
        OutputFormat::Parquet => {
            use crate::output::ParquetWriter;
            ParquetWriter::write(&result, config)
                .map_err(|e| anyhow::anyhow!("Failed to format Parquet output: {}", e))?
        }
    };

    // Write to target (stdout or file)
    write_to_target(&output_bytes, &config.target, config.overwrite)
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    // Add newline for text formats written to stdout (not for binary/file output)
    if matches!(config.target, OutputTarget::Stdout) && !matches!(format, OutputFormat::Parquet) {
        // Text writers don't include trailing newline, so add one for stdout
        // (CSV already ends with newline from csv crate)
        if !matches!(format, OutputFormat::Csv) {
            println!();
        }
    }

    // Show success message for file output (to stderr so it doesn't mix with output)
    if let OutputTarget::File(path) = &config.target {
        eprintln!("Output written to: {}", path.display());
    }

    // Show timing information if requested (to stderr when writing to file)
    if timing {
        let elapsed = start_time.elapsed();
        eprintln!("\nQuery executed in {:.2}ms", elapsed.as_millis());

        let performance = result.performance();
        if performance.total_time_us > 0 {
            eprintln!(
                "Parse time: {:.2}ms",
                performance.parse_time_us as f64 / 1000.0
            );
            eprintln!(
                "Planning time: {:.2}ms",
                performance.planning_time_us as f64 / 1000.0
            );
            eprintln!(
                "Execution time: {:.2}ms",
                performance.execution_time_us as f64 / 1000.0
            );
            eprintln!("Memory usage: {} bytes", performance.memory_usage_bytes);
            eprintln!("I/O operations: {}", performance.io_operations);
            if performance.cache_hits + performance.cache_misses > 0 {
                eprintln!(
                    "Cache hit ratio: {:.1}%",
                    performance.cache_hit_ratio() * 100.0
                );
            }
        }
    }

    // Show warnings if any (to stderr)
    let warnings = result.warnings();
    if !warnings.is_empty() {
        eprintln!("\nWarnings:");
        for warning in warnings {
            eprintln!("  ⚠️  {warning}");
        }
    }

    Ok(())
}

#[cfg(not(feature = "state_machine"))]
pub async fn execute_query(
    _database: &cqlite_core::Database,
    _query: &str,
    _explain: bool,
    _timing: bool,
    _format: OutputFormat,
    _config: &crate::config::OutputConfig,
) -> Result<()> {
    Err(anyhow::anyhow!(
        "Query execution is not available in M1.\n\
         Build with --features state_machine to enable this feature.\n\
         See CLAUDE.md for M1 API examples."
    ))
}

/// Print results in CSV format
#[cfg(feature = "state_machine")]
fn print_csv_format(
    result: &cqlite_core::query::result::QueryResult,
    config: &crate::config::OutputConfig,
) -> Result<()> {
    use crate::output::CSVWriter;

    // CSVWriter handles limit internally via config
    let csv_output = CSVWriter::write(result, config)
        .map_err(|e| anyhow::anyhow!("Failed to format CSV output: {}", e))?;

    print!("{}", csv_output);
    Ok(())
}

/// Execute a CQL SELECT query against SSTable data (live data, no mocking!)
pub async fn execute_select_query(
    sstable_path: &Path,
    schema_path: &Path,
    query: &str,
    format: OutputFormat,
    auto_detect: bool,
    cassandra_version: Option<String>,
) -> Result<()> {
    use super::schema_load::load_schema_file;
    use super::support::{resolve_sstable_path, QueryExecutor, QueryExecutorConfig};

    // Load schema from file (supports both .cql and .json)
    let _schema = load_schema_file(schema_path, auto_detect, cassandra_version.as_deref())?;

    println!("🚀 Executing CQL query against LIVE SSTable data!");
    println!("📂 SSTable: {}", sstable_path.display());
    println!("📋 Schema: {}", schema_path.display());
    println!("🔍 Query: {query}");

    // Smart path resolution: if directory, find the Data.db file
    let actual_sstable_path = resolve_sstable_path(sstable_path)?;
    println!("📄 Data file: {}", actual_sstable_path.display());

    // Create query executor
    let executor = QueryExecutor::new(QueryExecutorConfig);

    // Execute the query
    let result = executor.execute_select(query).await?;

    // Display results
    match format {
        OutputFormat::Table => result.display_table(),
        OutputFormat::Json => result.display_json()?,
        OutputFormat::Csv => result.display_csv()?,
        OutputFormat::Parquet => {
            return Err(anyhow::anyhow!("Parquet format is not supported for this command. Use --out json or --out csv instead."));
        }
    }

    Ok(())
}
