//! REPL Integration Module
//!
//! Integrates the CQL Query Processor with the existing REPL system,
//! providing enhanced query execution capabilities for interactive use.

use crate::query_processor::{CQLQueryProcessor, QueryExecutionContext, REPLQueryResult};
use crate::interactive::ReplSession;
use anyhow::Result;
use colored::*;
use cqlite_core::{Database, query::QueryEngine, schema::SchemaManager, storage::StorageEngine};
use std::sync::Arc;
use std::time::Instant;

/// Enhanced REPL query executor with CQL processing capabilities
pub struct REPLQueryExecutor {
    /// CQL query processor
    query_processor: CQLQueryProcessor,
    /// Current execution context
    context: QueryExecutionContext,
}

impl REPLQueryExecutor {
    /// Create a new REPL query executor
    pub fn new(
        database: Arc<Database>,
        query_engine: Arc<QueryEngine>,
        schema_manager: Arc<SchemaManager>,
        storage_engine: Arc<StorageEngine>,
        session: &ReplSession,
    ) -> Result<Self> {
        let query_processor = CQLQueryProcessor::new(
            database,
            query_engine,
            schema_manager,
            storage_engine,
        )?;

        let context = QueryExecutionContext {
            current_keyspace: session.current_keyspace.clone(),
            page_size: session.page_size,
            timing_enabled: session.timing_enabled,
            query_history: session.command_history.clone(),
            interactive_mode: true,
        };

        Ok(Self {
            query_processor,
            context,
        })
    }

    /// Execute a CQL query with enhanced REPL features
    pub async fn execute_query(&mut self, query: &str) -> Result<()> {
        println!("{} {}", "🔍 Executing:".blue().bold(), query.yellow());
        
        let result = self.query_processor.execute_query(query, &self.context).await?;
        
        self.display_enhanced_result(&result).await?;
        
        Ok(())
    }

    /// Display enhanced query results with REPL-specific formatting
    async fn display_enhanced_result(&self, result: &REPLQueryResult) -> Result<()> {
        println!();

        // Display main results
        if result.result.rows.is_empty() && result.result.rows_affected > 0 {
            // DML query
            println!(
                "{} {} rows affected",
                "✅".green().bold(),
                result.result.rows_affected
            );
        } else if !result.result.rows.is_empty() {
            // SELECT query with results
            self.display_formatted_results(&result.result).await?;
            
            println!();
            println!(
                "{} Returned {} row{}",
                "📊 Results:".cyan().bold(),
                result.result.rows.len(),
                if result.result.rows.len() == 1 { "" } else { "s" }
            );
        } else {
            // DDL query or empty result
            println!("{} Query executed successfully", "✅".green().bold());
        }

        // Display timing information if enabled
        if self.context.timing_enabled {
            self.display_performance_metrics(result);
        }

        // Display warnings if any
        if !result.warnings.is_empty() {
            println!();
            println!("{} Warnings:", "⚠️ ".yellow().bold());
            for warning in &result.warnings {
                println!("  ⚠️  {}", warning.yellow());
            }
        }

        // Display optimization hints
        if !result.optimization_hints.is_empty() {
            println!();
            println!("{} Optimization Hints:", "💡".cyan().bold());
            for hint in &result.optimization_hints {
                println!("  💡 {}", hint.cyan());
            }
        }

        Ok(())
    }

    /// Display performance metrics
    fn display_performance_metrics(&self, result: &REPLQueryResult) {
        println!();
        println!(
            "{} Execution time: {:.2}ms",
            "⏱️  Query completed:".green(),
            result.execution_time_ms
        );

        let metrics = &result.performance_metrics;
        if metrics.parse_time_us > 0 || metrics.planning_time_us > 0 || metrics.execution_time_us > 0 {
            println!(
                "{} Parse: {:.2}ms | Planning: {:.2}ms | Execution: {:.2}ms",
                "🔧 Timing breakdown:".dimmed(),
                metrics.parse_time_us as f64 / 1000.0,
                metrics.planning_time_us as f64 / 1000.0,
                metrics.execution_time_us as f64 / 1000.0
            );
        }

        if metrics.memory_usage_bytes > 0 {
            println!(
                "{} Memory used: {:.2} KB",
                "💾 Resources:".dimmed(),
                metrics.memory_usage_bytes as f64 / 1024.0
            );
        }

        if metrics.sstables_scanned > 0 {
            println!(
                "{} SSTable files scanned: {}",
                "📂 Storage:".dimmed(),
                metrics.sstables_scanned
            );
        }

        if metrics.rows_examined > 0 {
            println!(
                "{} Rows examined: {} | Rows returned: {} | Efficiency: {:.1}%",
                "📊 Data:".dimmed(),
                metrics.rows_examined,
                metrics.rows_returned,
                if metrics.rows_examined > 0 {
                    (metrics.rows_returned as f64 / metrics.rows_examined as f64) * 100.0
                } else {
                    0.0
                }
            );
        }

        let total_cache_ops = metrics.cache_hits + metrics.cache_misses;
        if total_cache_ops > 0 {
            let hit_ratio = (metrics.cache_hits as f64 / total_cache_ops as f64) * 100.0;
            println!(
                "{} Cache hits: {} | Cache misses: {} | Hit ratio: {:.1}%",
                "🎯 Cache:".dimmed(),
                metrics.cache_hits,
                metrics.cache_misses,
                hit_ratio
            );
        }
    }

    /// Display formatted query results
    async fn display_formatted_results(&self, result: &cqlite_core::QueryResult) -> Result<()> {
        if result.rows.is_empty() {
            println!("{}", "📭 No rows returned".yellow());
            return Ok(());
        }

        // Get column names
        let column_names = result.column_names();
        if column_names.is_empty() {
            println!("{}", "❓ No columns in result".yellow());
            return Ok(());
        }

        let total_rows = result.rows.len();
        let should_page = self.context.page_size > 0 && total_rows > self.context.page_size;

        if should_page {
            self.display_paged_results(result, &column_names).await?;
        } else {
            self.display_table_results(result, &column_names)?;
        }

        Ok(())
    }

    /// Display results in table format
    fn display_table_results(
        &self,
        result: &cqlite_core::QueryResult,
        column_names: &[String],
    ) -> Result<()> {
        // Calculate optimal column widths
        let mut col_widths = Vec::new();
        for col_name in column_names {
            let mut max_width = col_name.len();
            for row in result.iter() {
                if let Some(value) = row.get(col_name) {
                    max_width = max_width.max(format!("{}", value).len());
                }
            }
            col_widths.push(max_width.max(8).min(50)); // minimum 8, maximum 50
        }

        println!("{}", "📊 Results:".green().bold());

        // Print top border
        print!("┌");
        for (i, width) in col_widths.iter().enumerate() {
            print!("{}", "─".repeat(width + 2));
            if i < col_widths.len() - 1 {
                print!("┬");
            }
        }
        println!("┐");

        // Print header
        print!("│");
        for (i, (col_name, width)) in column_names.iter().zip(col_widths.iter()).enumerate() {
            print!(" {:width$} ", col_name.bold().cyan(), width = width);
            if i < column_names.len() - 1 {
                print!("│");
            }
        }
        println!("│");

        // Print header separator
        print!("├");
        for (i, width) in col_widths.iter().enumerate() {
            print!("{}", "─".repeat(width + 2));
            if i < col_widths.len() - 1 {
                print!("┼");
            }
        }
        println!("┤");

        // Print rows
        for row in result.iter() {
            print!("│");
            for (i, (col_name, width)) in column_names.iter().zip(col_widths.iter()).enumerate() {
                let value = row.get(col_name)
                    .map(|v| {
                        let s = format!("{}", v);
                        if s.len() > *width {
                            format!("{}...", &s[..width.saturating_sub(3)])
                        } else {
                            s
                        }
                    })
                    .unwrap_or_else(|| "NULL".dimmed().to_string());
                print!(" {:width$} ", value, width = width);
                if i < column_names.len() - 1 {
                    print!("│");
                }
            }
            println!("│");
        }

        // Print bottom border
        print!("└");
        for (i, width) in col_widths.iter().enumerate() {
            print!("{}", "─".repeat(width + 2));
            if i < col_widths.len() - 1 {
                print!("┴");
            }
        }
        println!("┘");

        Ok(())
    }

    /// Display results with pagination
    async fn display_paged_results(
        &self,
        result: &cqlite_core::QueryResult,
        column_names: &[String],
    ) -> Result<()> {
        use std::io::{self, Write};

        let total_rows = result.rows.len();
        let page_size = self.context.page_size;
        let total_pages = (total_rows + page_size - 1) / page_size;

        println!(
            "{} {} rows total, showing {} per page",
            "📊 Paged Results:".green().bold(),
            total_rows,
            page_size
        );

        for page in 0..total_pages {
            let start_idx = page * page_size;
            let end_idx = ((page + 1) * page_size).min(total_rows);

            println!();
            println!(
                "{} Page {} of {} (rows {}-{})",
                "📄".cyan(),
                page + 1,
                total_pages,
                start_idx + 1,
                end_idx
            );

            // Create subset for this page
            let page_rows: Vec<_> = result.rows.iter().skip(start_idx).take(end_idx - start_idx).collect();

            if !page_rows.is_empty() {
                self.display_page_table(&page_rows, column_names)?;
            }

            // Ask user if they want to continue (except for last page)
            if page < total_pages - 1 {
                print!("\n{} Press Enter for next page, 'q' to quit: ", "❓".cyan());
                io::stdout().flush()?;

                let mut input = String::new();
                io::stdin().read_line(&mut input)?;

                if input.trim().to_lowercase() == "q" {
                    println!("📄 Stopped at page {} of {}", page + 1, total_pages);
                    break;
                }
            }
        }

        Ok(())
    }

    /// Display a page of results in table format
    fn display_page_table(
        &self,
        rows: &[&cqlite_core::query::result::QueryRow],
        column_names: &[String],
    ) -> Result<()> {
        // Similar to display_table_results but for a subset of rows
        let mut col_widths = Vec::new();
        for col_name in column_names {
            let mut max_width = col_name.len();
            for row in rows {
                if let Some(value) = row.get(col_name) {
                    max_width = max_width.max(format!("{}", value).len());
                }
            }
            col_widths.push(max_width.max(8).min(50));
        }

        // Print table structure
        print!("┌");
        for (i, width) in col_widths.iter().enumerate() {
            print!("{}", "─".repeat(width + 2));
            if i < col_widths.len() - 1 { print!("┬"); }
        }
        println!("┐");

        print!("│");
        for (i, (col_name, width)) in column_names.iter().zip(col_widths.iter()).enumerate() {
            print!(" {:width$} ", col_name.bold().cyan(), width = width);
            if i < column_names.len() - 1 { print!("│"); }
        }
        println!("│");

        print!("├");
        for (i, width) in col_widths.iter().enumerate() {
            print!("{}", "─".repeat(width + 2));
            if i < col_widths.len() - 1 { print!("┼"); }
        }
        println!("┤");

        for row in rows {
            print!("│");
            for (i, (col_name, width)) in column_names.iter().zip(col_widths.iter()).enumerate() {
                let value = row.get(col_name)
                    .map(|v| {
                        let s = format!("{}", v);
                        if s.len() > *width {
                            format!("{}...", &s[..width.saturating_sub(3)])
                        } else {
                            s
                        }
                    })
                    .unwrap_or_else(|| "NULL".dimmed().to_string());
                print!(" {:width$} ", value, width = width);
                if i < column_names.len() - 1 { print!("│"); }
            }
            println!("│");
        }

        print!("└");
        for (i, width) in col_widths.iter().enumerate() {
            print!("{}", "─".repeat(width + 2));
            if i < col_widths.len() - 1 { print!("┴"); }
        }
        println!("┘");

        Ok(())
    }

    /// Update current keyspace in context
    pub fn set_current_keyspace(&mut self, keyspace: Option<String>) {
        self.context.current_keyspace = keyspace;
    }

    /// Update pagination settings
    pub fn set_page_size(&mut self, page_size: usize) {
        self.context.page_size = page_size;
    }

    /// Toggle timing display
    pub fn set_timing_enabled(&mut self, enabled: bool) {
        self.context.timing_enabled = enabled;
    }

    /// Add query to history
    pub fn add_to_history(&mut self, query: String) {
        self.context.query_history.push(query);
        
        // Limit history size
        if self.context.query_history.len() > 1000 {
            self.context.query_history.remove(0);
        }
    }

    /// Get query suggestions for auto-completion
    pub async fn get_query_suggestions(&self, partial_query: &str, cursor_position: usize) -> Vec<String> {
        // This would integrate with the QueryCompletion from query_processor
        let mut suggestions = Vec::new();
        let partial = partial_query[..cursor_position].to_uppercase();

        // Basic keyword suggestions
        let keywords = vec![
            "SELECT", "FROM", "WHERE", "ORDER BY", "LIMIT", "INSERT", "UPDATE",
            "DELETE", "CREATE", "DROP", "DESCRIBE", "USE", "AND", "OR", "ASC", "DESC"
        ];

        for keyword in keywords {
            if let Some(last_word) = partial.split_whitespace().last() {
                if keyword.starts_with(last_word) {
                    suggestions.push(keyword.to_string());
                }
            }
        }

        // Table name suggestions
        if partial.ends_with("FROM ") || partial.ends_with("INTO ") || partial.ends_with("UPDATE ") {
            // Would query available tables from schema manager
            suggestions.push("table_name".to_string());
        }

        suggestions
    }

    /// Provide context-aware error help
    pub fn provide_error_help(&self, error: &str, query: &str) {
        println!();
        
        if error.contains("table") && error.contains("not found") {
            println!("{} Table Not Found Help:", "💡 Hint:".cyan().bold());
            println!("  • Use {} to list available tables", ":tables".green());
            println!("  • Check table name spelling and case sensitivity");
            println!("  • Use {} to see table schema", ":describe <table>".green());
            if self.context.current_keyspace.is_none() {
                println!("  • Set keyspace with {} or use fully qualified name", ":use <keyspace>".green());
            }
        } else if error.contains("keyspace") && error.contains("not found") {
            println!("{} Keyspace Not Found Help:", "💡 Hint:".cyan().bold());
            println!("  • Use {} to list available keyspaces", ":keyspaces".green());
            println!("  • Check keyspace name spelling");
            println!("  • Configure data directory if needed");
        } else if error.contains("syntax") || error.contains("parse") {
            println!("{} CQL Syntax Help:", "💡 Hint:".cyan().bold());
            println!("  • Check CQL reference: {}", ":help cql".green());
            println!("  • Example queries: {}", ":help examples".green());
            
            if query.to_uppercase().contains("SELECT") {
                println!("  • Basic SELECT: {}", "SELECT * FROM table_name LIMIT 10;".yellow());
                println!("  • With WHERE: {}", "SELECT * FROM table WHERE column = 'value';".yellow());
            }
        } else if error.contains("column") {
            println!("{} Column Help:", "💡 Hint:".cyan().bold());
            println!("  • Use {} to see table columns", ":describe <table>".green());
            println!("  • Check column name spelling and case sensitivity");
            println!("  • Verify the table has the expected schema");
        }
    }
}

/// Schema exploration utilities for REPL
pub struct REPLSchemaExplorer {
    schema_manager: Arc<SchemaManager>,
    database: Arc<Database>,
}

impl REPLSchemaExplorer {
    pub fn new(schema_manager: Arc<SchemaManager>, database: Arc<Database>) -> Self {
        Self {
            schema_manager,
            database,
        }
    }

    /// Get list of available keyspaces
    pub async fn get_keyspaces(&self) -> Result<Vec<String>> {
        let result = self.database.execute("SELECT keyspace_name FROM system.keyspaces").await?;
        let mut keyspaces = Vec::new();
        
        for row in result.rows {
            if let Some(ks_name) = row.get("keyspace_name") {
                keyspaces.push(ks_name.to_string());
            }
        }
        
        Ok(keyspaces)
    }

    /// Get list of tables in a keyspace
    pub async fn get_tables(&self, keyspace: Option<&str>) -> Result<Vec<String>> {
        let query = if let Some(ks) = keyspace {
            format!(
                "SELECT table_name FROM system.tables WHERE keyspace_name = '{}' AND keyspace_name != 'system'",
                ks
            )
        } else {
            "SELECT keyspace_name, table_name FROM system.tables WHERE keyspace_name != 'system'".to_string()
        };

        let result = self.database.execute(&query).await?;
        let mut tables = Vec::new();

        for row in result.rows {
            if let Some(ks) = keyspace {
                if let Some(table_name) = row.get("table_name") {
                    tables.push(table_name.to_string());
                }
            } else {
                if let (Some(ks_name), Some(table_name)) = (row.get("keyspace_name"), row.get("table_name")) {
                    tables.push(format!("{}.{}", ks_name, table_name));
                }
            }
        }

        Ok(tables)
    }

    /// Get detailed table schema
    pub async fn get_table_schema(&self, keyspace: &str, table: &str) -> Result<TableSchemaDetail> {
        let query = format!(
            "SELECT column_name, type, kind, clustering_order, position FROM system.columns WHERE keyspace_name = '{}' AND table_name = '{}' ORDER BY position",
            keyspace, table
        );

        let result = self.database.execute(&query).await?;
        let mut columns = Vec::new();
        let mut partition_keys = Vec::new();
        let mut clustering_keys = Vec::new();

        for row in result.rows {
            if let (Some(col_name), Some(col_type), Some(col_kind)) = (
                row.get("column_name"),
                row.get("type"),
                row.get("kind")
            ) {
                let column = SchemaColumn {
                    name: col_name.to_string(),
                    data_type: col_type.to_string(),
                    kind: col_kind.to_string(),
                };

                match col_kind.to_string().as_str() {
                    "partition_key" => partition_keys.push(column.clone()),
                    "clustering" => clustering_keys.push(column.clone()),
                    _ => {}
                }

                columns.push(column);
            }
        }

        Ok(TableSchemaDetail {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            columns,
            partition_keys,
            clustering_keys,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SchemaColumn {
    pub name: String,
    pub data_type: String,
    pub kind: String,
}

#[derive(Debug)]
pub struct TableSchemaDetail {
    pub keyspace: String,
    pub table: String,
    pub columns: Vec<SchemaColumn>,
    pub partition_keys: Vec<SchemaColumn>,
    pub clustering_keys: Vec<SchemaColumn>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_repl_query_executor_creation() {
        // Test creation of REPL query executor
        // Would need proper setup with real components
    }

    #[tokio::test]
    async fn test_enhanced_result_display() {
        // Test result formatting and display
    }

    #[tokio::test]
    async fn test_schema_exploration() {
        // Test schema discovery and exploration
    }
}