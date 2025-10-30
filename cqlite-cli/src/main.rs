// EMERGENCY M1 FIX: Allow clippy warnings
#![allow(clippy::all)]

use anyhow::Result;
use clap::Parser;
use cqlite_core::{Config as CoreConfig, Database};
use std::path::PathBuf;
use tracing::info;

#[cfg(feature = "state_machine")]
use cqlite_core::ingestion::{ingest, IngestionConfig};

mod cli;
mod cli_types;
mod commands;
mod config;
mod error;
mod formatter;
mod output;
mod script_executor;

use cli_types::{AdminCommands, Cli, Commands};
use commands::info::execute_info_command;
// mod data_parser;
// mod formatter; // New cqlsh-compatible formatter
// mod interactive;
// mod pagination;
// mod query_executor;
mod repl; // Core REPL engine
          // mod repl_data_integration; // REPL data integration
          // mod table_scanner;
          // mod tui;

#[tokio::main]
async fn main() {
    // Run main logic and handle exit codes
    if let Err(e) = run_main().await {
        let exit_code = error::classify_error(&e);
        error::print_error(&e, exit_code);
        std::process::exit(exit_code.as_i32());
    }
}

/// Main CLI logic that returns Result for proper error handling
async fn run_main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging based on verbosity
    let log_level = match (cli.quiet, cli.verbose) {
        (true, _) => "error",
        (false, 0) => "info",
        (false, 1) => "debug",
        (false, _) => "trace",
    };

    // Configure logging to stderr only (Issue #129)
    // This prevents debug/warn logs from contaminating stdout JSON/CSV output
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(log_level))
        .target(env_logger::Target::Stderr)
        .init();

    info!("Starting CQLite CLI v{}", env!("CARGO_PKG_VERSION"));

    // Load configuration
    let config = config::Config::load(cli.config.clone(), &cli)?;

    // Initialize database connection
    let db_path = cli
        .database
        .or(config.default_database.clone())
        .unwrap_or_else(|| PathBuf::from("cqlite.db"));

    // Initialize the database engine - check for ingestion path first
    #[cfg(feature = "state_machine")]
    let database = if cli.schema.is_some() && (cli.data_dir.is_some() || cli.dataset.is_some()) {
        // One-shot ingestion path: load schema and discover SSTables
        info!("Using one-shot ingestion mode");

        if let Some(dataset_name) = &cli.dataset {
            // SECURITY: Validate dataset name to prevent directory traversal attacks
            if dataset_name.contains("..")
                || dataset_name.contains('/')
                || dataset_name.contains('\\')
                || dataset_name.starts_with('.')
            {
                return Err(anyhow::anyhow!(
                    "Invalid dataset name '{}': must not contain '..', '/', '\\', or start with '.'",
                    dataset_name
                ));
            }

            // Dataset mode: use sstables directory path directly
            // The dataset structure is: datasets_root/sstables/{dataset_name}/
            // which is flat (not production keyspace/table-uuid layout)
            info!("Dataset mode: using dataset '{}'", dataset_name);

            // Get datasets root from environment variable or use default
            let datasets_root = std::env::var("CQLITE_DATASETS_ROOT")
                .ok()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("test-data/datasets"));

            info!("Using datasets root: {}", datasets_root.display());

            // For dataset mode, use the sstables/{dataset_name} directory as the data_dir
            // The DiscoveryService will scan this flat structure
            let dataset_data_dir = datasets_root.join("sstables").join(dataset_name);

            // SECURITY: Canonicalize and verify the path stays within datasets_root
            let canonical_dir = dataset_data_dir.canonicalize().map_err(|e| {
                anyhow::anyhow!(
                    "Dataset '{}' not found or inaccessible: {}. Check CQLITE_DATASETS_ROOT={}",
                    dataset_name,
                    e,
                    datasets_root.display()
                )
            })?;

            let canonical_root = datasets_root.canonicalize().map_err(|e| {
                anyhow::anyhow!(
                    "Datasets root directory not found: {}. Check CQLITE_DATASETS_ROOT={}",
                    e,
                    datasets_root.display()
                )
            })?;

            if !canonical_dir.starts_with(&canonical_root) {
                return Err(anyhow::anyhow!(
                    "Security violation: dataset path '{}' escaped datasets root directory",
                    dataset_name
                ));
            }

            info!("Dataset data directory: {}", dataset_data_dir.display());

            // DATASET MODE FIX: The dataset directory IS the keyspace directory with table subdirectories
            // Use DiscoveryService, but pass the parent (sstables/) so scanner finds dataset as keyspace
            let dataset_parent = datasets_root.join("sstables");

            // Use standard ingestion with the PARENT directory and filter by dataset name
            let dataset_segment = format!("/{}/", dataset_name);
            let ingestion_config = IngestionConfig {
                schema_paths: vec![cli.schema.clone().unwrap()],
                data_dir: dataset_parent, // Scanner will find all datasets as keyspaces
                version_hint: cli.cassandra_version.clone(),
                core_config: create_core_config(&config)?,
                table_directory_filter: Some(dataset_segment), // Filter to this dataset only
            };

            // Ingest - filtering happens inside ingestion module
            match ingest(ingestion_config).await {
                Ok(result) => {
                    info!(
                        "Dataset ingestion complete: {} schemas loaded, {} table directories from '{}'",
                        result.schema_load_result.schemas_loaded,
                        result.discovery_summary.table_directories.len(),
                        dataset_name
                    );
                    result.database
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Dataset ingestion failed: {}", e));
                }
            }
        } else {
            // Production mode: use standard ingestion with DiscoveryService (no filter)
            let ingestion_config = IngestionConfig {
                schema_paths: vec![cli.schema.clone().unwrap()],
                data_dir: cli.data_dir.clone().unwrap(),
                version_hint: cli.cassandra_version.clone(),
                core_config: create_core_config(&config)?,
                table_directory_filter: None,
            };

            match ingest(ingestion_config).await {
                Ok(result) => {
                    info!(
                        "Ingestion complete: {} schemas loaded, {} SSTables found",
                        result.schema_load_result.schemas_loaded,
                        result.discovery_summary.sstables_found
                    );
                    result.database
                }
                Err(e) => {
                    // Error will be classified by error.rs for proper exit codes
                    return Err(anyhow::anyhow!("Ingestion failed: {}", e));
                }
            }
        }
    } else {
        // Original Database::open() path for backward compatibility
        initialize_database(&db_path, &config).await?
    };

    #[cfg(not(feature = "state_machine"))]
    let database = initialize_database(&db_path, &config).await?;

    // Create output config for query execution
    let output_config =
        config::OutputConfig::from_cli(&config, cli.no_color, cli.limit, cli.page_size);

    // Handle --file flag (script execution) - takes precedence over subcommands
    if let Some(file_path) = cli.file {
        return script_executor::execute_script_file(
            &file_path,
            &database,
            &output_config,
            cli.format,
        )
        .await;
    }

    // Handle --execute flag (single statement execution) - takes precedence over subcommands
    if let Some(query) = cli.execute {
        // Issue #142: Experimental fallback to read-sstable for SELECT when ingestion unavailable
        // This is a temporary feature (disabled by default) that will be removed in M3
        // Check this FIRST before schema validation to avoid false negatives
        let will_use_fallback = if cli.enable_select_fallback {
            // Check if ingestion is unavailable (no schema or no data source)
            let ingestion_unavailable =
                cli.schema.is_none() || (cli.data_dir.is_none() && cli.dataset.is_none());

            // Check if query is a SELECT statement
            let is_select_query = query.trim().to_uppercase().starts_with("SELECT");

            ingestion_unavailable && is_select_query
        } else {
            false
        };

        // Issue #199: Pre-flight schema validation to fail-fast on schema/data mismatch
        // Only validate if NOT using fallback (fallback doesn't use schema)
        #[cfg(feature = "state_machine")]
        if !will_use_fallback {
            // Extract table name from query for validation
            // This uses a simple pattern match - for full parsing see query planner
            let table_name_result = extract_table_name_from_query(&query);

            if let Ok(table_name) = table_name_result {
                // Check schema availability before query execution
                if !database.has_schema_for_table(&table_name).await {
                    // Get detailed status for error message
                    let status = database.schema_status(&table_name).await;

                    match status {
                        cqlite_core::SchemaStatus::Missing { reason, .. } => {
                            return Err(anyhow::anyhow!(
                                "Schema not found for table '{}'\n\n\
                                 Cause: {}\n\n\
                                 Troubleshooting:\n\
                                 1. Verify table name matches schema definition\n\
                                 2. Check that schema file was loaded correctly\n\
                                 3. Use 'read-sstable' command to inspect SSTable contents directly",
                                table_name,
                                reason
                            ));
                        }
                        cqlite_core::SchemaStatus::ExtractionFailed {
                            cause, suggestion, ..
                        } => {
                            return Err(anyhow::anyhow!(
                                "Schema extraction failed for table '{}'\n\n\
                                 Cause: {}\n\n\
                                 Troubleshooting:\n\
                                 1. {}\n\
                                 2. Verify SSTable files are valid Cassandra 5.0 format\n\
                                 3. Check that Statistics.db contains SerializationHeader\n\
                                 4. Try regenerating SSTables from CQL schema",
                                table_name,
                                cause,
                                suggestion
                            ));
                        }
                        _ => {} // Schema available, continue to query execution
                    }
                }
            }
            // If table name extraction fails, let query planner handle it
        }

        // Execute the fallback if conditions are met
        if will_use_fallback {
            eprintln!("⚠️  Using experimental read-sstable fallback (temporary feature, disabled by default)");

            // Extract table path from SELECT query using simple regex
            // Supports patterns like: SELECT * FROM /path/to/table or SELECT * FROM path/to/table
            let table_path_result = extract_table_path_from_select(&query);

            match table_path_result {
                Ok(table_path) => {
                    eprintln!("📂 Extracted table path: {}", table_path.display());

                    // Call read-sstable command with extracted path
                    return commands::read_sstable::execute_read_sstable_command(
                        &table_path,
                        cli.format,
                        cli.limit,
                        0,     // skip
                        false, // keys_only
                        false, // raw
                        cli.verbose > 0,
                    )
                    .await;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "SELECT fallback failed: {}. \
                             Provide schema and data-dir for full query engine support.",
                        e
                    ));
                }
            }
        }

        return commands::execute_query(
            &database,
            &query,
            false, // explain
            false, // timing
            cli.format,
            &output_config,
        )
        .await;
    }

    match cli.command {
        Some(Commands::Repl { tui }) => {
            // Check if we need to run ingestion from config file
            #[cfg(feature = "state_machine")]
            let database = if !config.schema_paths.is_empty() && config.data_directory.is_some() {
                info!("REPL: Running ingestion from config file");
                info!(
                    "REPL: Loading {} schema file(s) from config",
                    config.schema_paths.len()
                );
                info!(
                    "REPL: Discovering SSTables in: {}",
                    config.data_directory.as_ref().unwrap().display()
                );

                let ingestion_config = IngestionConfig {
                    schema_paths: config.schema_paths.clone(),
                    data_dir: config.data_directory.clone().unwrap(),
                    version_hint: config.cassandra_version.clone(),
                    core_config: create_core_config(&config)?,
                    table_directory_filter: None, // REPL doesn't filter tables
                };

                match ingest(ingestion_config).await {
                    Ok(result) => {
                        info!(
                            "REPL ingestion complete: {} schema(s) loaded, {} SSTable(s) discovered, {} keyspace(s) found",
                            result.schema_load_result.schemas_loaded,
                            result.discovery_summary.sstables_found,
                            result.discovery_summary.keyspaces.len()
                        );
                        result.database
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!(
                            "REPL ingestion failed: {}. Check schema paths and data directory in config file.",
                            e
                        ));
                    }
                }
            } else {
                // No config-based ingestion, use existing database
                database
            };

            #[cfg(not(feature = "state_machine"))]
            let database = database; // Just use existing database if state_machine feature disabled

            // Create REPL configuration from loaded config (not hardcoded!)
            let repl_config = repl::ReplConfig {
                mode: if tui {
                    repl::ReplMode::Tui
                } else {
                    repl::ReplMode::Basic
                },
                // Use config.repl settings with CLI flag overrides
                enable_history: config.repl.enable_history,
                enable_completion: config.repl.enable_completion,
                enable_colors: if cli.no_color {
                    false
                } else {
                    config.repl.enable_colors
                },
                output_format: repl::OutputFormat::Table,
                max_history_size: config.repl.max_history_size,
                page_size: cli.page_size.unwrap_or(config.repl.page_size),
                show_timing: config.repl.show_timing,
                enable_paging: config.repl.enable_paging,
                prompt: config.repl.prompt.clone(),
                prompt_continuation: config.repl.prompt_continuation.clone(),
            };

            // Initialize and run REPL engine
            let mut engine = repl::ReplEngine::new(repl_config, &db_path, config, database)
                .map_err(|e| anyhow::anyhow!("Failed to initialize REPL: {}", e))?;

            // Run REPL and convert ReplError to proper exit codes
            engine.run().await.map_err(|e| {
                // Convert ReplError to anyhow::Error while preserving exit code information
                match &e {
                    repl::ReplError::SchemaError(msg) => {
                        anyhow::anyhow!("Schema error: {}", msg)
                    }
                    repl::ReplError::DataDirectoryError(msg) => {
                        anyhow::anyhow!("Data directory error: {}", msg)
                    }
                    repl::ReplError::UnsupportedFeature(msg) => {
                        anyhow::anyhow!("Unsupported feature: {}", msg)
                    }
                    _ => anyhow::anyhow!("REPL error: {}", e),
                }
            })
        }
        Some(Commands::Query {
            query,
            explain,
            timing,
        }) => {
            let output_config =
                config::OutputConfig::from_cli(&config, cli.no_color, cli.limit, cli.page_size);
            commands::execute_query(
                &database,
                &query,
                explain,
                timing,
                cli.format,
                &output_config,
            )
            .await
        }
        Some(Commands::Import {
            file,
            format,
            table,
            mapping: _,
            batch_size: _,
        }) => commands::import_data(&database, &file, format, Some(&table)).await,
        Some(Commands::Export {
            file,
            format,
            table,
            query: _,
        }) => commands::export_data(&database, &table, &file, format).await,
        Some(Commands::Admin { command }) => {
            commands::admin::handle_admin_command(&database, command).await
        }
        Some(Commands::Schema { command }) => {
            commands::schema::handle_schema_command(&database, command).await
        }
        Some(Commands::Bench { command }) => {
            commands::bench::handle_bench_command(&database, command).await
        }
        Some(Commands::ReadSstable {
            file,
            format,
            limit,
            skip,
            keys_only,
            raw,
            verbose,
        }) => {
            commands::read_sstable::execute_read_sstable_command(
                &file, format, limit, skip, keys_only, raw, verbose,
            )
            .await
        }
        Some(Commands::Info {
            path,
            format,
            detailed,
        }) => {
            match path {
                Some(path) => {
                    execute_info_command(
                        &path,
                        detailed,
                        format,
                        false, // validate - default to false
                        cli.schema.as_deref(),
                        cli.auto_detect,
                        cli.cassandra_version.clone(),
                    )
                    .await
                }
                None => {
                    println!("📋 Displaying database information");
                    commands::admin::handle_admin_command(&database, AdminCommands::Info).await
                }
            }
        }
        None => {
            // Default to help message for now
            println!("CQLite CLI v{}", env!("CARGO_PKG_VERSION"));
            println!("Use --help for available commands");
            Ok(())
        }
    }
}

/// Initialize the database engine with proper configuration
async fn initialize_database(db_path: &PathBuf, config: &config::Config) -> Result<Database> {
    // Create the database directory if it doesn't exist
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Convert CLI config to core config
    let core_config = create_core_config(config)?;

    info!("Initializing database at: {}", db_path.display());

    // Open the database with the core configuration
    let database = Database::open(db_path, core_config)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to initialize database: {}", e))?;

    info!("Database initialized successfully");

    Ok(database)
}

/// Convert CLI configuration to core database configuration
fn create_core_config(cli_config: &config::Config) -> Result<CoreConfig> {
    let mut core_config = CoreConfig::default();

    // Apply CLI configuration settings to core config
    if let Some(memory_limit_mb) = cli_config.performance.memory_limit_mb {
        core_config.memory.max_memory = memory_limit_mb * 1024 * 1024; // Convert MB to bytes
    }

    // Set cache size from CLI config
    core_config.memory.block_cache.max_size = cli_config.performance.cache_size_mb * 1024 * 1024; // Convert MB to bytes

    // Set query timeout
    core_config.query.max_execution_time =
        std::time::Duration::from_millis(cli_config.performance.query_timeout_ms);

    // Enable optimizations for better performance
    core_config.query.enable_optimization = true;
    core_config.storage.enable_bloom_filters = true;

    // Validate the configuration
    core_config
        .validate()
        .map_err(|e| anyhow::anyhow!("Invalid database configuration: {}", e))?;

    Ok(core_config)
}

/// Extract table name from query for schema validation (Issue #199)
///
/// This is a simple pattern match for pre-flight validation.
/// The query planner will do full parsing during execution.
fn extract_table_name_from_query(query: &str) -> Result<String> {
    let normalized = query.trim().to_uppercase();

    // Handle SELECT statements
    if let Some(from_pos) = normalized.find("FROM") {
        let after_from = query[from_pos + 4..].trim();
        let table_name = after_from
            .split_whitespace()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No table name found after FROM clause"))?;

        // Remove trailing semicolon or WHERE clause
        let cleaned = table_name
            .trim_end_matches(';')
            .split_whitespace()
            .next()
            .unwrap_or(table_name);

        // Handle qualified table names (keyspace.table) - extract just the table part
        let table_only = cleaned.split('.').last().unwrap_or(cleaned);

        return Ok(table_only.to_string());
    }

    // Handle INSERT statements
    if let Some(into_pos) = normalized.find("INTO") {
        let after_into = query[into_pos + 4..].trim();
        let table_name = after_into
            .split_whitespace()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No table name found after INTO clause"))?;

        // Handle qualified table names (keyspace.table)
        let cleaned = table_name.trim_end_matches(';');
        let table_only = cleaned.split('.').last().unwrap_or(cleaned);
        return Ok(table_only.to_string());
    }

    // Handle UPDATE statements
    if let Some(update_pos) = normalized.find("UPDATE") {
        let after_update = query[update_pos + 6..].trim();
        let table_name = after_update
            .split_whitespace()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No table name found after UPDATE clause"))?;

        // Handle qualified table names (keyspace.table)
        let cleaned = table_name.trim_end_matches(';');
        let table_only = cleaned.split('.').last().unwrap_or(cleaned);
        return Ok(table_only.to_string());
    }

    // Handle DELETE statements
    if normalized.find("DELETE").is_some() {
        if let Some(from_pos) = normalized.find("FROM") {
            let after_from = query[from_pos + 4..].trim();
            let table_name = after_from
                .split_whitespace()
                .next()
                .ok_or_else(|| anyhow::anyhow!("No table name found after FROM clause"))?;

            // Handle qualified table names (keyspace.table)
            let cleaned = table_name.trim_end_matches(';');
            let table_only = cleaned.split('.').last().unwrap_or(cleaned);
            return Ok(table_only.to_string());
        }
    }

    Err(anyhow::anyhow!("Unable to extract table name from query"))
}

/// Extract table path from SELECT query for fallback routing (Issue #142)
/// Supports simple patterns: SELECT * FROM /path/to/table
fn extract_table_path_from_select(query: &str) -> Result<PathBuf> {
    // Remove extra whitespace and normalize
    let normalized_query = query
        .trim()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let uppercase_query = normalized_query.to_uppercase();

    // Find FROM clause
    if let Some(from_pos) = uppercase_query.find("FROM") {
        // Extract text after FROM
        let after_from = &normalized_query[from_pos + 4..].trim();

        // Find first token after FROM (this should be the path)
        let path_token = after_from
            .split_whitespace()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No table path found after FROM clause"))?;

        // Remove trailing semicolon if present
        let cleaned_path = path_token.trim_end_matches(';');

        // SECURITY: Check for directory traversal attempts before canonicalization
        // Issue #142: Defense-in-depth for temporary fallback feature
        if cleaned_path.contains("..") {
            return Err(anyhow::anyhow!(
                "Security violation: path contains '..' which could indicate directory traversal attempt: {}",
                cleaned_path
            ));
        }

        let path = PathBuf::from(cleaned_path);

        // SECURITY: Canonicalize to resolve symlinks and validate path exists
        let canonical_path = path.canonicalize().map_err(|e| {
            anyhow::anyhow!(
                "Table path does not exist or is inaccessible: {}. Ensure the path points to a valid SSTable file or directory. Error: {}",
                path.display(),
                e
            )
        })?;

        Ok(canonical_path)
    } else {
        Err(anyhow::anyhow!(
            "Invalid SELECT query: missing FROM clause. Use format: SELECT * FROM /path/to/table"
        ))
    }
}
