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
