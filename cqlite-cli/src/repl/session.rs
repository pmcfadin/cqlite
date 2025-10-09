// REPL Session Management
//
// Manages the state and context of a REPL session, including database connections,
// current keyspace, configuration, and session persistence.

use super::{ReplError, ReplResult};
use crate::config::Config;
use anyhow::Result;
use cqlite_core::{Database, QueryResult};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Current state of the REPL session
#[derive(Debug, Clone, PartialEq)]
pub enum SessionState {
    /// Session is initializing
    Initializing,
    /// Session is ready for commands
    Ready,
    /// Session is executing a command
    Executing,
    /// Session is in error state
    Error(String),
    /// Session is shutting down
    Shutdown,
}

/// REPL session context and state
pub struct ReplSession {
    /// Database connection
    database: Arc<Database>,
    /// Session configuration
    config: Config,
    /// Database file path
    db_path: PathBuf,
    /// Current session state
    state: SessionState,
    /// Current keyspace (if any)
    current_keyspace: Option<String>,
    /// Data directory for SSTable files
    data_dir: Option<PathBuf>,
    /// Session variables
    variables: HashMap<String, String>,
    /// Connection metadata
    connection_info: ConnectionInfo,
    /// Performance metrics
    metrics: SessionMetrics,
}

/// Connection information
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// Database version
    pub version: String,
    /// Connection timestamp
    pub connected_at: std::time::SystemTime,
    /// Last activity timestamp
    pub last_activity: std::time::SystemTime,
    /// Total queries executed
    pub queries_executed: u64,
    /// Total errors encountered
    pub errors_count: u64,
}

/// Session performance metrics
#[derive(Debug, Clone, Default)]
pub struct SessionMetrics {
    /// Total execution time (microseconds)
    pub total_execution_time_us: u64,
    /// Query count by type
    pub query_counts: HashMap<String, u64>,
    /// Average query time (microseconds)
    pub avg_query_time_us: f64,
    /// Memory usage (bytes)
    pub memory_usage_bytes: u64,
    /// Cache statistics
    pub cache_hits: u64,
    pub cache_misses: u64,
}

impl ReplSession {
    /// Create a new REPL session
    pub fn new(db_path: &Path, config: Config, database: Database) -> ReplResult<Self> {
        let connection_info = ConnectionInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            connected_at: std::time::SystemTime::now(),
            last_activity: std::time::SystemTime::now(),
            queries_executed: 0,
            errors_count: 0,
        };

        Ok(Self {
            database: Arc::new(database),
            config,
            db_path: db_path.to_path_buf(),
            state: SessionState::Initializing,
            current_keyspace: None,
            data_dir: None,
            variables: HashMap::new(),
            connection_info,
            metrics: SessionMetrics::default(),
        })
    }

    /// Initialize the session
    pub async fn initialize(&mut self) -> ReplResult<()> {
        self.state = SessionState::Ready;
        self.connection_info.last_activity = std::time::SystemTime::now();

        // Try to load default keyspace from config
        if let Some(default_keyspace) = self.config.default_keyspace.clone() {
            if !default_keyspace.is_empty() {
                let _ = self.use_keyspace(&default_keyspace).await;
            }
        }

        // Set data directory if configured
        if let Some(ref data_dir) = self.config.data_directory {
            if !data_dir.as_os_str().is_empty() {
                self.data_dir = Some(data_dir.clone());
            }
        }

        Ok(())
    }

    /// Get current session state
    pub fn state(&self) -> &SessionState {
        &self.state
    }

    /// Get current keyspace
    pub fn current_keyspace(&self) -> Option<&String> {
        self.current_keyspace.as_ref()
    }

    /// Get database path
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Get data directory
    pub fn data_dir(&self) -> Option<&Path> {
        self.data_dir.as_deref()
    }

    /// Set data directory
    pub fn set_data_dir(&mut self, path: Option<PathBuf>) {
        self.data_dir = path;
    }

    /// Switch to a keyspace
    pub async fn use_keyspace(&mut self, keyspace: &str) -> ReplResult<()> {
        self.state = SessionState::Executing;
        self.connection_info.last_activity = std::time::SystemTime::now();

        // Validate keyspace exists by querying system tables
        let query = format!(
            "SELECT keyspace_name FROM system.keyspaces WHERE keyspace_name = '{}'",
            keyspace
        );

        match self.database.execute(&query).await {
            Ok(result) => {
                if result.rows.is_empty() {
                    self.state = SessionState::Ready;
                    return Err(ReplError::Session(format!(
                        "Keyspace '{}' not found",
                        keyspace
                    )));
                }

                self.current_keyspace = Some(keyspace.to_string());
                self.state = SessionState::Ready;
                Ok(())
            }
            Err(e) => {
                // If system query fails, allow setting anyway (might be valid in data directory)
                self.current_keyspace = Some(keyspace.to_string());
                self.state = SessionState::Ready;
                self.connection_info.errors_count += 1;

                // Log warning but don't fail
                log::warn!("Could not verify keyspace '{}': {}", keyspace, e);
                Ok(())
            }
        }
    }

    /// Execute a query
    pub async fn execute_query(&mut self, query: &str) -> ReplResult<QueryResult> {
        self.state = SessionState::Executing;
        self.connection_info.last_activity = std::time::SystemTime::now();

        let start_time = std::time::Instant::now();

        match self.database.execute(query).await {
            Ok(result) => {
                let elapsed = start_time.elapsed();
                self.update_metrics(query, elapsed, true);
                self.connection_info.queries_executed += 1;
                self.state = SessionState::Ready;
                Ok(result)
            }
            Err(e) => {
                let elapsed = start_time.elapsed();
                self.update_metrics(query, elapsed, false);
                self.connection_info.errors_count += 1;
                self.state = SessionState::Ready;
                Err(ReplError::Database(e.into()))
            }
        }
    }

    /// List available tables
    pub async fn list_tables(&mut self) -> ReplResult<Vec<String>> {
        self.state = SessionState::Executing;

        let query = if let Some(ref keyspace) = self.current_keyspace {
            format!(
                "SELECT table_name FROM system.tables WHERE keyspace_name = '{}'",
                keyspace
            )
        } else {
            "SELECT keyspace_name, table_name FROM system.tables WHERE keyspace_name != 'system'"
                .to_string()
        };

        match self.database.execute(&query).await {
            Ok(result) => {
                self.state = SessionState::Ready;
                let mut tables = Vec::new();

                for row in &result.rows {
                    if let Some(ref _keyspace) = self.current_keyspace {
                        // Just table names for current keyspace
                        if let Some(table_name) = row.get("table_name") {
                            tables.push(table_name.to_string());
                        }
                    } else {
                        // Qualified names for all keyspaces
                        if let (Some(keyspace_name), Some(table_name)) =
                            (row.get("keyspace_name"), row.get("table_name"))
                        {
                            tables.push(format!("{}.{}", keyspace_name, table_name));
                        }
                    }
                }

                Ok(tables)
            }
            Err(e) => {
                self.state = SessionState::Ready;

                // Fallback to data directory scanning if system query fails
                if let Some(ref data_dir) = self.data_dir {
                    match self.scan_data_directory_tables(data_dir).await {
                        Ok(tables) => Ok(tables),
                        Err(_) => Err(ReplError::Database(e.into())),
                    }
                } else {
                    Err(ReplError::Database(e.into()))
                }
            }
        }
    }

    /// List available keyspaces
    pub async fn list_keyspaces(&mut self) -> ReplResult<Vec<String>> {
        self.state = SessionState::Executing;

        let query = "SELECT keyspace_name FROM system.keyspaces";

        match self.database.execute(query).await {
            Ok(result) => {
                self.state = SessionState::Ready;
                let mut keyspaces = Vec::new();

                for row in &result.rows {
                    if let Some(keyspace_name) = row.get("keyspace_name") {
                        keyspaces.push(keyspace_name.to_string());
                    }
                }

                Ok(keyspaces)
            }
            Err(e) => {
                self.state = SessionState::Ready;

                // Fallback to data directory scanning
                if let Some(ref data_dir) = self.data_dir {
                    match self.scan_data_directory_keyspaces(data_dir).await {
                        Ok(keyspaces) => Ok(keyspaces),
                        Err(_) => Err(ReplError::Database(e.into())),
                    }
                } else {
                    Err(ReplError::Database(e.into()))
                }
            }
        }
    }

    /// Describe an object (table, keyspace, etc.)
    pub async fn describe_object(&mut self, object_name: &str) -> ReplResult<String> {
        self.state = SessionState::Executing;

        // Parse object name (could be keyspace.table or just table)
        let (keyspace, table) = if object_name.contains('.') {
            let parts: Vec<&str> = object_name.split('.').collect();
            if parts.len() == 2 {
                (Some(parts[0]), parts[1])
            } else {
                (self.current_keyspace.as_deref(), object_name)
            }
        } else {
            (self.current_keyspace.as_deref(), object_name)
        };

        if let Some(ks) = keyspace {
            match self.describe_table(ks, table).await {
                Ok(description) => {
                    self.state = SessionState::Ready;
                    Ok(description)
                }
                Err(e) => {
                    self.state = SessionState::Ready;
                    Err(e)
                }
            }
        } else {
            self.state = SessionState::Ready;
            Err(ReplError::Session(
                "No keyspace specified and no current keyspace set".to_string(),
            ))
        }
    }

    /// Describe a specific table
    async fn describe_table(&self, keyspace: &str, table: &str) -> ReplResult<String> {
        let query = format!(
            "SELECT column_name, type, kind FROM system.columns WHERE keyspace_name = '{}' AND table_name = '{}' ORDER BY position",
            keyspace, table
        );

        match self.database.execute(&query).await {
            Ok(result) => {
                if result.rows.is_empty() {
                    return Err(ReplError::Session(format!(
                        "Table '{}.{}' not found",
                        keyspace, table
                    )));
                }

                let mut description = String::new();
                description.push_str(&format!("Table: {}.{}\n", keyspace, table));
                description.push_str("Columns:\n");

                for row in &result.rows {
                    if let (Some(col_name), Some(col_type), Some(col_kind)) =
                        (row.get("column_name"), row.get("type"), row.get("kind"))
                    {
                        let kind_desc = match col_kind.to_string().as_str() {
                            "partition_key" => " (PARTITION KEY)",
                            "clustering" => " (CLUSTERING KEY)",
                            "regular" => "",
                            _ => "",
                        };
                        description
                            .push_str(&format!("  {} {}{}\n", col_name, col_type, kind_desc));
                    }
                }

                Ok(description)
            }
            Err(e) => Err(ReplError::Database(e.into())),
        }
    }

    /// Get session variable
    pub fn get_variable(&self, name: &str) -> Option<&String> {
        self.variables.get(name)
    }

    /// Set session variable
    pub fn set_variable(&mut self, name: String, value: String) {
        self.variables.insert(name, value);
    }

    /// Get connection information
    pub fn connection_info(&self) -> &ConnectionInfo {
        &self.connection_info
    }

    /// Get session metrics
    pub fn metrics(&self) -> &SessionMetrics {
        &self.metrics
    }

    /// Update session metrics
    fn update_metrics(&mut self, query: &str, elapsed: std::time::Duration, success: bool) {
        let elapsed_us = elapsed.as_micros() as u64;
        self.metrics.total_execution_time_us += elapsed_us;

        // Update average
        let total_queries = self.connection_info.queries_executed + if success { 1 } else { 0 };
        if total_queries > 0 {
            self.metrics.avg_query_time_us =
                self.metrics.total_execution_time_us as f64 / total_queries as f64;
        }

        // Categorize query type
        let query_type = self.categorize_query(query);
        *self.metrics.query_counts.entry(query_type).or_insert(0) += 1;
    }

    /// Categorize query for metrics
    fn categorize_query(&self, query: &str) -> String {
        let upper = query.to_uppercase();
        let trimmed = upper.trim();

        if trimmed.starts_with("SELECT") {
            "SELECT".to_string()
        } else if trimmed.starts_with("INSERT") {
            "INSERT".to_string()
        } else if trimmed.starts_with("UPDATE") {
            "UPDATE".to_string()
        } else if trimmed.starts_with("DELETE") {
            "DELETE".to_string()
        } else if trimmed.starts_with("CREATE") {
            "CREATE".to_string()
        } else if trimmed.starts_with("ALTER") {
            "ALTER".to_string()
        } else if trimmed.starts_with("DROP") {
            "DROP".to_string()
        } else if trimmed.starts_with("DESCRIBE") {
            "DESCRIBE".to_string()
        } else {
            "OTHER".to_string()
        }
    }

    /// Scan data directory for tables (fallback when system queries fail)
    async fn scan_data_directory_tables(&self, data_dir: &Path) -> Result<Vec<String>> {
        use std::fs;

        let mut tables = Vec::new();

        if let Some(ref keyspace) = self.current_keyspace {
            // Scan specific keyspace directory
            let keyspace_dir = data_dir.join(keyspace);
            if keyspace_dir.exists() {
                for entry in fs::read_dir(&keyspace_dir)? {
                    let entry = entry?;
                    if entry.path().is_dir() {
                        if let Some(dir_name) = entry.file_name().to_str() {
                            if let Some(table_name) = self.extract_table_name(dir_name) {
                                tables.push(table_name);
                            }
                        }
                    }
                }
            }
        } else {
            // Scan all keyspace directories
            for entry in fs::read_dir(data_dir)? {
                let entry = entry?;
                if entry.path().is_dir() {
                    if let Some(keyspace_name) = entry.file_name().to_str() {
                        if keyspace_name.starts_with('.') || keyspace_name == "system" {
                            continue;
                        }

                        let keyspace_dir = entry.path();
                        for table_entry in fs::read_dir(&keyspace_dir)? {
                            let table_entry = table_entry?;
                            if table_entry.path().is_dir() {
                                if let Some(dir_name) = table_entry.file_name().to_str() {
                                    if let Some(table_name) = self.extract_table_name(dir_name) {
                                        tables.push(format!("{}.{}", keyspace_name, table_name));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(tables)
    }

    /// Scan data directory for keyspaces
    async fn scan_data_directory_keyspaces(&self, data_dir: &Path) -> Result<Vec<String>> {
        use std::fs;

        let mut keyspaces = Vec::new();

        for entry in fs::read_dir(data_dir)? {
            let entry = entry?;
            if entry.path().is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    if !name.starts_with('.') && name != "system" {
                        keyspaces.push(name.to_string());
                    }
                }
            }
        }

        keyspaces.sort();
        Ok(keyspaces)
    }

    /// Extract table name from SSTable directory name
    fn extract_table_name(&self, dir_name: &str) -> Option<String> {
        // Expected format: tablename-uuid
        if let Some(dash_pos) = dir_name.find('-') {
            let table_part = &dir_name[..dash_pos];
            if !table_part.is_empty() && table_part.chars().all(|c| c.is_alphanumeric() || c == '_')
            {
                return Some(table_part.to_string());
            }
        }
        None
    }

    /// Shutdown the session
    pub async fn shutdown(&mut self) -> ReplResult<()> {
        self.state = SessionState::Shutdown;

        // Perform cleanup tasks
        self.save_session_state().await?;

        Ok(())
    }

    /// Save session state for persistence
    async fn save_session_state(&self) -> ReplResult<()> {
        // This would save session state to a file or database
        // For now, just log some statistics
        log::info!(
            "Session ending. Queries executed: {}, Errors: {}",
            self.connection_info.queries_executed,
            self.connection_info.errors_count
        );
        Ok(())
    }

    /// Export session metrics as a report
    pub fn export_metrics(&self) -> String {
        let mut report = String::new();

        report.push_str("=== CQLite Session Report ===\n");
        report.push_str(&format!("Database: {}\n", self.db_path.display()));
        report.push_str(&format!(
            "Session Duration: {:?}\n",
            self.connection_info
                .last_activity
                .duration_since(self.connection_info.connected_at)
                .unwrap_or_default()
        ));
        report.push_str(&format!(
            "Queries Executed: {}\n",
            self.connection_info.queries_executed
        ));
        report.push_str(&format!("Errors: {}\n", self.connection_info.errors_count));
        report.push_str(&format!(
            "Average Query Time: {:.2}ms\n",
            self.metrics.avg_query_time_us / 1000.0
        ));

        if !self.metrics.query_counts.is_empty() {
            report.push_str("\nQuery Types:\n");
            for (query_type, count) in &self.metrics.query_counts {
                report.push_str(&format!("  {}: {}\n", query_type, count));
            }
        }

        if let Some(ref keyspace) = self.current_keyspace {
            report.push_str(&format!("Current Keyspace: {}\n", keyspace));
        }

        report
    }

    /// Replace the Database instance with a new one
    ///
    /// Used when rebuilding the database after ingestion changes.
    /// The old Database will be dropped when the Arc refcount reaches zero.
    pub fn replace_database(&mut self, new_database: Database) -> ReplResult<()> {
        self.database = Arc::new(new_database);
        Ok(())
    }

    /// Get reference to the session configuration
    pub fn config(&self) -> &Config {
        &self.config
    }
}
