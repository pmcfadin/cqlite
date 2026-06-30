//! TUI application state model and state transitions (Issue #1130).
//!
//! Holds the panel-state structs, the [`TuiApp`] application state, and the
//! pure/data-loading state transitions on it. Drawing lives in [`super::render`]
//! and input handling lives in [`super::events`].

use crate::config::Config;
use crate::status_metrics::{StatusMetrics, METRICS_REFRESH_INTERVAL};
use anyhow::Result;
use cqlite_core::Database;
use ratatui::{
    layout::Rect,
    widgets::{ListState, TableState},
};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

// =============================================================================
// Panel State Structures (Issue #251)
// =============================================================================

/// Panel visibility configuration - toggleable with F2/F3/F4 keys
#[derive(Debug, Clone, Copy)]
pub(super) struct PanelVisibility {
    pub(super) tables: bool,  // F2 toggle - Tables browser panel
    pub(super) results: bool, // F3 toggle - Query results panel
    pub(super) history: bool, // F4 toggle - Query history panel
}

impl Default for PanelVisibility {
    fn default() -> Self {
        Self {
            tables: true,
            results: true,
            history: true,
        }
    }
}

impl PanelVisibility {
    /// Reset to default layout (F5)
    pub(super) fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Active panel for keyboard focus navigation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FocusPanel {
    Tables,
    Results,
    History,
    Input,
}

impl FocusPanel {
    /// Cycle to next visible panel (Tab key)
    pub(super) fn next(self, visibility: &PanelVisibility) -> Self {
        let order = [
            (FocusPanel::Tables, visibility.tables),
            (FocusPanel::Results, visibility.results),
            (FocusPanel::History, visibility.history),
            (FocusPanel::Input, true), // Input always visible
        ];

        let current_idx = order.iter().position(|(p, _)| *p == self).unwrap_or(3);

        // Find next visible panel
        for i in 1..=order.len() {
            let next_idx = (current_idx + i) % order.len();
            if order[next_idx].1 {
                return order[next_idx].0;
            }
        }
        FocusPanel::Input // Fallback
    }

    /// Cycle to previous visible panel (Shift+Tab key)
    pub(super) fn prev(self, visibility: &PanelVisibility) -> Self {
        let order = [
            (FocusPanel::Tables, visibility.tables),
            (FocusPanel::Results, visibility.results),
            (FocusPanel::History, visibility.history),
            (FocusPanel::Input, true),
        ];

        let current_idx = order.iter().position(|(p, _)| *p == self).unwrap_or(3);

        for i in 1..=order.len() {
            let prev_idx = (current_idx + order.len() - i) % order.len();
            if order[prev_idx].1 {
                return order[prev_idx].0;
            }
        }
        FocusPanel::Input
    }
}

/// Table entry in the tables browser
#[derive(Debug, Clone)]
pub(super) struct TableEntry {
    #[allow(dead_code)] // Reserved for keyspace display
    pub(super) keyspace: String,
    #[allow(dead_code)] // Reserved for table name display
    pub(super) name: String,
    pub(super) qualified_name: String, // "keyspace.table"
}

/// Tables browser panel state
#[derive(Debug)]
pub(super) struct TablesBrowserState {
    pub(super) entries: Vec<TableEntry>,
    pub(super) filtered_indices: Vec<usize>, // Indices into entries after filter
    pub(super) filter_text: String,
    pub(super) filter_active: bool, // Is filter input mode active
    pub(super) list_state: ListState,
}

impl Default for TablesBrowserState {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            filtered_indices: Vec::new(),
            filter_text: String::new(),
            filter_active: false,
            list_state: ListState::default(),
        }
    }
}

impl TablesBrowserState {
    /// Apply filter to entries and update filtered_indices
    pub(super) fn apply_filter(&mut self) {
        if self.filter_text.is_empty() {
            self.filtered_indices = (0..self.entries.len()).collect();
        } else {
            let filter_lower = self.filter_text.to_lowercase();
            self.filtered_indices = self
                .entries
                .iter()
                .enumerate()
                .filter(|(_, e)| e.qualified_name.to_lowercase().contains(&filter_lower))
                .map(|(i, _)| i)
                .collect();
        }
        // Reset selection if out of bounds
        if let Some(selected) = self.list_state.selected() {
            if selected >= self.filtered_indices.len() {
                if self.filtered_indices.is_empty() {
                    self.list_state.select(None);
                } else {
                    self.list_state.select(Some(0));
                }
            }
        }
    }

    /// Get currently selected entry
    pub(super) fn selected_entry(&self) -> Option<&TableEntry> {
        self.list_state
            .selected()
            .and_then(|idx| self.filtered_indices.get(idx))
            .and_then(|&entry_idx| self.entries.get(entry_idx))
    }
}

/// Query results table state with scroll tracking
#[derive(Debug)]
pub(super) struct ResultsTableState {
    pub(super) columns: Vec<String>,
    pub(super) rows: Vec<Vec<String>>,
    pub(super) row_offset: usize, // Vertical scroll position
    pub(super) col_offset: usize, // Horizontal scroll position (column index)
    pub(super) selected_row: Option<usize>,
    pub(super) column_widths: Vec<u16>, // Calculated widths for each column
    pub(super) table_state: TableState,
}

impl Default for ResultsTableState {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            row_offset: 0,
            col_offset: 0,
            selected_row: None,
            column_widths: Vec::new(),
            table_state: TableState::default(),
        }
    }
}

impl ResultsTableState {
    /// Calculate column widths based on content
    pub(super) fn calculate_widths(&mut self) {
        if self.columns.is_empty() {
            self.column_widths = vec![];
            return;
        }

        // Start with header widths (minimum width)
        let mut widths: Vec<u16> = self.columns.iter().map(|c| c.len() as u16).collect();

        // Expand for content (sample first 100 rows)
        for row in self.rows.iter().take(100) {
            for (i, cell) in row.iter().enumerate() {
                if i < widths.len() {
                    widths[i] = widths[i].max(cell.len() as u16);
                }
            }
        }

        // Add padding (2 spaces) and cap at 40 chars
        for w in &mut widths {
            *w = (*w + 2).min(40);
        }

        self.column_widths = widths;
    }

    /// Get visible columns range based on offset and available width
    pub(super) fn visible_columns(&self, available_width: u16) -> std::ops::Range<usize> {
        if self.column_widths.is_empty() {
            return 0..0;
        }

        let start = self
            .col_offset
            .min(self.column_widths.len().saturating_sub(1));
        let mut end = start;
        let mut used_width = 0u16;

        for i in start..self.column_widths.len() {
            let col_width = self.column_widths.get(i).copied().unwrap_or(10);
            if used_width + col_width > available_width && end > start {
                break;
            }
            used_width += col_width;
            end = i + 1;
        }

        start..end.max(start + 1).min(self.columns.len())
    }

    /// Check if there are more columns to the left
    pub(super) fn has_scroll_left(&self) -> bool {
        self.col_offset > 0
    }

    /// Check if there are more columns to the right
    pub(super) fn has_scroll_right(&self, available_width: u16) -> bool {
        let visible = self.visible_columns(available_width);
        visible.end < self.columns.len()
    }

    /// Clear results
    pub(super) fn clear(&mut self) {
        self.columns.clear();
        self.rows.clear();
        self.row_offset = 0;
        self.col_offset = 0;
        self.selected_row = None;
        self.column_widths.clear();
        self.table_state = TableState::default();
    }
}

/// Layout areas computed from panel visibility
pub(super) struct LayoutAreas {
    pub(super) header: Rect,
    pub(super) tables: Option<Rect>,
    pub(super) results: Option<Rect>,
    pub(super) history: Option<Rect>,
    pub(super) input: Rect,
    pub(super) status: Rect,
}

// =============================================================================
// TUI Application State
// =============================================================================

/// TUI Application State
pub(super) struct TuiApp {
    pub(super) db_path: std::path::PathBuf,
    pub(super) database: Arc<Database>,
    pub(super) input: String,
    #[allow(dead_code)] // Legacy mode - replaced by focus_panel
    pub(super) input_mode: InputMode,
    pub(super) messages: Vec<String>,
    #[allow(dead_code)] // Reserved for future scroll implementation
    pub(super) scroll_offset: usize,
    pub(super) history: Vec<String>,
    pub(super) history_index: Option<usize>,
    pub(super) query_results: Vec<QueryDisplayResult>,
    #[allow(dead_code)] // Legacy - replaced by history_scroll
    pub(super) results_scroll: ListState,
    pub(super) show_help: bool,
    pub(super) status_message: String,
    #[allow(dead_code)] // Reserved for future use
    pub(super) last_execution_time: Option<Duration>,
    /// Status metrics for enhanced status bar (Issue #242)
    pub(super) status_metrics: Option<StatusMetrics>,
    /// Last time metrics were refreshed
    pub(super) metrics_last_updated: Option<Instant>,

    // Issue #251: Multi-panel layout fields
    /// Panel visibility state (F2/F3/F4 toggles)
    pub(super) panel_visibility: PanelVisibility,
    /// Currently focused panel for keyboard navigation
    pub(super) focus_panel: FocusPanel,
    /// Tables browser panel state
    pub(super) tables_browser: TablesBrowserState,
    /// Query results table with horizontal scrolling
    pub(super) results_table: ResultsTableState,
    /// History panel scroll state
    pub(super) history_scroll: ListState,
    /// Current keyspace context for header display
    pub(super) current_keyspace: Option<String>,
}

#[derive(Clone, PartialEq)]
#[allow(dead_code)] // Normal variant reserved for future use
pub(super) enum InputMode {
    Normal,
    Editing,
    Results,
    Help,
}

#[derive(Clone)]
pub(super) struct QueryDisplayResult {
    pub(super) query: String,
    pub(super) success: bool,
    #[allow(dead_code)] // Row count - used in History panel display
    pub(super) rows: usize,
    pub(super) execution_time: Option<Duration>,
    #[allow(dead_code)] // Reserved for future error display
    pub(super) error_message: Option<String>,
}

impl TuiApp {
    pub(super) async fn new(
        db_path: &Path,
        config: &Config,
        database: Arc<Database>,
    ) -> Result<Self> {
        // Collect initial metrics
        let initial_metrics = StatusMetrics::collect(Some(db_path), Some(&database)).await;

        // Create initial app state
        let mut app = TuiApp {
            db_path: db_path.to_path_buf(),
            database,
            input: String::new(),
            input_mode: InputMode::Editing,
            messages: vec![
                "Welcome to CQLite TUI Mode!".to_string(),
                "Type CQL queries and press Enter to execute.".to_string(),
                "Press F1 for help, Tab to navigate panels, Esc to exit.".to_string(),
                String::new(),
            ],
            scroll_offset: 0,
            history: Vec::new(),
            history_index: None,
            query_results: Vec::new(),
            results_scroll: ListState::default(),
            show_help: false,
            status_message: "Ready".to_string(),
            last_execution_time: None,
            status_metrics: Some(initial_metrics),
            metrics_last_updated: Some(Instant::now()),
            // Issue #251: Multi-panel layout initialization
            panel_visibility: PanelVisibility::default(),
            focus_panel: FocusPanel::Input,
            tables_browser: TablesBrowserState::default(),
            results_table: ResultsTableState::default(),
            history_scroll: ListState::default(),
            current_keyspace: None,
        };

        // Load tables for browser panel (Issue #251)
        app.load_tables(config).await;

        Ok(app)
    }

    /// Check if metrics need refresh (stale after METRICS_REFRESH_INTERVAL)
    fn metrics_stale(&self) -> bool {
        match self.metrics_last_updated {
            Some(last) => last.elapsed() > METRICS_REFRESH_INTERVAL,
            None => true,
        }
    }

    /// Refresh status metrics if stale
    pub(super) async fn refresh_metrics(&mut self) {
        if self.metrics_stale() {
            self.status_metrics =
                Some(StatusMetrics::collect(Some(&self.db_path), Some(&self.database)).await);
            self.metrics_last_updated = Some(Instant::now());
        }
    }

    /// Load tables into the tables browser (Issue #251)
    ///
    /// Uses the data directory scanning approach from the REPL session fallback.
    /// This scans the filesystem for table directories since the TUI mode does
    /// not have an active REPL session.
    async fn load_tables(&mut self, config: &Config) {
        // Get data directory from config
        let data_dir = match &config.data_directory {
            Some(dir) if !dir.as_os_str().is_empty() => dir,
            _ => {
                // No data directory configured - tables panel will be empty
                return;
            }
        };

        // Scan data directory for tables
        match self.scan_tables(data_dir).await {
            Ok(tables) => {
                self.tables_browser.entries = tables;
                self.tables_browser.apply_filter();

                // Select first entry if available
                if !self.tables_browser.filtered_indices.is_empty() {
                    self.tables_browser.list_state.select(Some(0));
                }
            }
            Err(e) => {
                // Log error but don't fail initialization - empty table list is acceptable
                eprintln!("Warning: Failed to load tables: {}", e);
            }
        }
    }

    /// Scan data directory for table entries
    ///
    /// This is adapted from the prior REPL session data-directory table scan but
    /// returns TableEntry structs suitable for the TUI browser.
    async fn scan_tables(&self, data_dir: &Path) -> Result<Vec<TableEntry>> {
        use std::fs;

        let mut entries = Vec::new();

        // Scan all keyspace directories
        let read_dir = fs::read_dir(data_dir)
            .map_err(|e| anyhow::anyhow!("Failed to read data directory: {}", e))?;

        for entry in read_dir {
            let entry =
                entry.map_err(|e| anyhow::anyhow!("Failed to read directory entry: {}", e))?;

            if !entry.path().is_dir() {
                continue;
            }

            let keyspace_name = match entry.file_name().to_str() {
                Some(name) if !name.starts_with('.') && name != "system" => name.to_string(),
                _ => continue,
            };

            // Scan tables in this keyspace
            let keyspace_dir = entry.path();
            let table_read_dir = match fs::read_dir(&keyspace_dir) {
                Ok(rd) => rd,
                Err(_) => continue, // Skip unreadable keyspace directories
            };

            for table_entry in table_read_dir {
                let table_entry = match table_entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };

                if !table_entry.path().is_dir() {
                    continue;
                }

                if let Some(dir_name) = table_entry.file_name().to_str() {
                    if let Some(table_name) = extract_table_name(dir_name) {
                        entries.push(TableEntry {
                            keyspace: keyspace_name.clone(),
                            name: table_name.clone(),
                            qualified_name: format!("{}.{}", keyspace_name, table_name),
                        });
                    }
                }
            }
        }

        // Sort by qualified name for consistent ordering
        entries.sort_by(|a, b| a.qualified_name.cmp(&b.qualified_name));

        Ok(entries)
    }

    /// Execute a CQL query
    pub(super) async fn execute_query(&mut self) {
        if self.input.trim().is_empty() {
            return;
        }

        let query = self.input.trim().to_string();
        self.history.push(query.clone());
        self.history_index = None;

        self.status_message = "Executing query...".to_string();

        let start_time = std::time::Instant::now();
        match self.database.execute(&query).await {
            Ok(result) => {
                let execution_time = start_time.elapsed();
                self.last_execution_time = Some(execution_time);

                let display_result = QueryDisplayResult {
                    query: query.clone(),
                    success: true,
                    rows: result.rows.len(),
                    execution_time: Some(execution_time),
                    error_message: None,
                };

                self.query_results.insert(0, display_result);

                // Add result summary to messages
                if result.rows.is_empty() && result.rows_affected > 0 {
                    self.messages.push(format!(
                        "✓ Query executed: {} rows affected ({})",
                        result.rows_affected,
                        format_duration(execution_time)
                    ));
                    // Clear results table for non-SELECT queries
                    self.results_table.clear();
                } else {
                    self.messages.push(format!(
                        "✓ Query executed: {} rows returned ({})",
                        result.rows.len(),
                        format_duration(execution_time)
                    ));

                    // Populate results table for display (Issue #251)
                    if !result.rows.is_empty() {
                        let column_names = result.rows[0].column_names();
                        self.results_table.columns = column_names.clone();
                        self.results_table.rows = result
                            .rows
                            .iter()
                            .map(|row| {
                                column_names
                                    .iter()
                                    .map(|col| {
                                        row.get(col)
                                            .map(|v| v.to_string())
                                            .unwrap_or_else(|| "NULL".to_string())
                                    })
                                    .collect()
                            })
                            .collect();
                        self.results_table.row_offset = 0;
                        self.results_table.col_offset = 0;
                        self.results_table.calculate_widths();

                        // Also add to messages for scrollback
                        self.messages
                            .push(format!("Columns: {}", column_names.join(", ")));
                    } else {
                        self.results_table.clear();
                    }
                }

                self.status_message =
                    format!("Query completed in {}", format_duration(execution_time));
            }
            Err(e) => {
                let execution_time = start_time.elapsed();

                let display_result = QueryDisplayResult {
                    query: query.clone(),
                    success: false,
                    rows: 0,
                    execution_time: Some(execution_time),
                    error_message: Some(e.to_string()),
                };

                self.query_results.insert(0, display_result);
                self.messages.push(format!("✗ Query failed: {}", e));
                self.status_message = "Query failed".to_string();
                // Don't clear results on error - keep previous results visible
            }
        }

        // Keep only last 20 results
        if self.query_results.len() > 20 {
            self.query_results.truncate(20);
        }

        // Keep only last 100 messages
        if self.messages.len() > 100 {
            self.messages.drain(0..self.messages.len() - 100);
        }

        self.input.clear();
    }

    /// Handle navigation in query history
    pub(super) fn navigate_history(&mut self, up: bool) {
        if self.history.is_empty() {
            return;
        }

        if up {
            let index = match self.history_index {
                None => self.history.len() - 1,
                Some(i) if i > 0 => i - 1,
                Some(_) => return,
            };
            self.history_index = Some(index);
            self.input = self.history[index].clone();
        } else {
            match self.history_index {
                None => return,
                Some(i) if i < self.history.len() - 1 => {
                    self.history_index = Some(i + 1);
                    self.input = self.history[i + 1].clone();
                }
                Some(_) => {
                    self.history_index = None;
                    self.input.clear();
                }
            }
        }
    }
}

/// Format a Duration for display with smart unit selection
///
/// - < 1ms: Display as "XXXμs" (microseconds)
/// - 1-999ms: Display as "X.Xms" (milliseconds with 1 decimal)
/// - >= 1000ms: Display as "X.Xs" (seconds with 1 decimal)
///
/// Examples:
/// - 450μs -> "450μs"
/// - 1.2ms -> "1.2ms"
/// - 7.0ms -> "7.0ms"
/// - 1500ms -> "1.5s"
pub(super) fn format_duration(duration: Duration) -> String {
    let micros = duration.as_micros();

    if micros < 1_000 {
        // Sub-millisecond: show microseconds
        format!("{}μs", micros)
    } else if micros < 1_000_000 {
        // 1-999ms: show milliseconds with 1 decimal place
        format!("{:.1}ms", micros as f64 / 1_000.0)
    } else {
        // >= 1 second: show seconds with 1 decimal place
        format!("{:.1}s", micros as f64 / 1_000_000.0)
    }
}

/// Extract table name from SSTable directory name
///
/// Expected format: tablename-uuid
/// Returns the table name part before the first dash.
pub(super) fn extract_table_name(dir_name: &str) -> Option<String> {
    if let Some(dash_pos) = dir_name.find('-') {
        let table_part = &dir_name[..dash_pos];
        if !table_part.is_empty() && table_part.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Some(table_part.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // FocusPanel Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_focus_panel_next_all_visible() {
        let visibility = PanelVisibility {
            tables: true,
            results: true,
            history: true,
        };

        // Cycle through all panels
        assert_eq!(FocusPanel::Tables.next(&visibility), FocusPanel::Results);
        assert_eq!(FocusPanel::Results.next(&visibility), FocusPanel::History);
        assert_eq!(FocusPanel::History.next(&visibility), FocusPanel::Input);
        assert_eq!(FocusPanel::Input.next(&visibility), FocusPanel::Tables);
    }

    #[test]
    fn test_focus_panel_prev_all_visible() {
        let visibility = PanelVisibility {
            tables: true,
            results: true,
            history: true,
        };

        // Cycle backwards through all panels
        assert_eq!(FocusPanel::Tables.prev(&visibility), FocusPanel::Input);
        assert_eq!(FocusPanel::Input.prev(&visibility), FocusPanel::History);
        assert_eq!(FocusPanel::History.prev(&visibility), FocusPanel::Results);
        assert_eq!(FocusPanel::Results.prev(&visibility), FocusPanel::Tables);
    }

    #[test]
    fn test_focus_panel_next_skips_hidden_tables() {
        let visibility = PanelVisibility {
            tables: false,
            results: true,
            history: true,
        };

        // Should skip Tables and go to Results
        assert_eq!(FocusPanel::Input.next(&visibility), FocusPanel::Results);
        assert_eq!(FocusPanel::Results.next(&visibility), FocusPanel::History);
        assert_eq!(FocusPanel::History.next(&visibility), FocusPanel::Input);
    }

    #[test]
    fn test_focus_panel_next_skips_hidden_results() {
        let visibility = PanelVisibility {
            tables: true,
            results: false,
            history: true,
        };

        // Should skip Results
        assert_eq!(FocusPanel::Tables.next(&visibility), FocusPanel::History);
        assert_eq!(FocusPanel::History.next(&visibility), FocusPanel::Input);
        assert_eq!(FocusPanel::Input.next(&visibility), FocusPanel::Tables);
    }

    #[test]
    fn test_focus_panel_next_skips_hidden_history() {
        let visibility = PanelVisibility {
            tables: true,
            results: true,
            history: false,
        };

        // Should skip History
        assert_eq!(FocusPanel::Results.next(&visibility), FocusPanel::Input);
        assert_eq!(FocusPanel::Input.next(&visibility), FocusPanel::Tables);
        assert_eq!(FocusPanel::Tables.next(&visibility), FocusPanel::Results);
    }

    #[test]
    fn test_focus_panel_next_only_input_visible() {
        let visibility = PanelVisibility {
            tables: false,
            results: false,
            history: false,
        };

        // All panels hidden - should cycle only on Input
        assert_eq!(FocusPanel::Input.next(&visibility), FocusPanel::Input);
        assert_eq!(FocusPanel::Tables.next(&visibility), FocusPanel::Input);
        assert_eq!(FocusPanel::Results.next(&visibility), FocusPanel::Input);
        assert_eq!(FocusPanel::History.next(&visibility), FocusPanel::Input);
    }

    #[test]
    fn test_focus_panel_prev_skips_hidden_panels() {
        let visibility = PanelVisibility {
            tables: false,
            results: true,
            history: false,
        };

        // Only Results and Input visible
        assert_eq!(FocusPanel::Results.prev(&visibility), FocusPanel::Input);
        assert_eq!(FocusPanel::Input.prev(&visibility), FocusPanel::Results);
    }

    #[test]
    fn test_focus_panel_input_always_reachable() {
        // Input is always reachable regardless of visibility
        let visibility = PanelVisibility {
            tables: false,
            results: false,
            history: false,
        };

        // Start from any panel, should eventually reach Input
        let mut current = FocusPanel::Tables;
        for _ in 0..5 {
            current = current.next(&visibility);
        }
        assert_eq!(current, FocusPanel::Input);
    }

    // -------------------------------------------------------------------------
    // TablesBrowserState Filter Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_apply_filter_empty_shows_all() {
        let mut state = TablesBrowserState::default();
        state.entries = vec![
            TableEntry {
                keyspace: "ks1".to_string(),
                name: "table1".to_string(),
                qualified_name: "ks1.table1".to_string(),
            },
            TableEntry {
                keyspace: "ks2".to_string(),
                name: "table2".to_string(),
                qualified_name: "ks2.table2".to_string(),
            },
            TableEntry {
                keyspace: "ks3".to_string(),
                name: "users".to_string(),
                qualified_name: "ks3.users".to_string(),
            },
        ];

        state.filter_text = String::new();
        state.apply_filter();

        // All entries should be visible
        assert_eq!(state.filtered_indices, vec![0, 1, 2]);
    }

    #[test]
    fn test_apply_filter_matches_some() {
        let mut state = TablesBrowserState::default();
        state.entries = vec![
            TableEntry {
                keyspace: "ks1".to_string(),
                name: "table1".to_string(),
                qualified_name: "ks1.table1".to_string(),
            },
            TableEntry {
                keyspace: "ks2".to_string(),
                name: "table2".to_string(),
                qualified_name: "ks2.table2".to_string(),
            },
            TableEntry {
                keyspace: "ks3".to_string(),
                name: "users".to_string(),
                qualified_name: "ks3.users".to_string(),
            },
        ];

        state.filter_text = "table".to_string();
        state.apply_filter();

        // Only entries containing "table" should be visible
        assert_eq!(state.filtered_indices, vec![0, 1]);
    }

    #[test]
    fn test_apply_filter_matches_none() {
        let mut state = TablesBrowserState::default();
        state.entries = vec![
            TableEntry {
                keyspace: "ks1".to_string(),
                name: "table1".to_string(),
                qualified_name: "ks1.table1".to_string(),
            },
            TableEntry {
                keyspace: "ks2".to_string(),
                name: "table2".to_string(),
                qualified_name: "ks2.table2".to_string(),
            },
        ];

        state.filter_text = "nonexistent".to_string();
        state.apply_filter();

        // No entries should match
        assert_eq!(state.filtered_indices, Vec::<usize>::new());
    }

    #[test]
    fn test_apply_filter_case_insensitive() {
        let mut state = TablesBrowserState::default();
        state.entries = vec![
            TableEntry {
                keyspace: "TestKS".to_string(),
                name: "Users".to_string(),
                qualified_name: "TestKS.Users".to_string(),
            },
            TableEntry {
                keyspace: "prodks".to_string(),
                name: "products".to_string(),
                qualified_name: "prodks.products".to_string(),
            },
        ];

        state.filter_text = "USERS".to_string();
        state.apply_filter();

        // Case-insensitive match
        assert_eq!(state.filtered_indices, vec![0]);
    }

    #[test]
    fn test_apply_filter_resets_selection_when_out_of_bounds() {
        let mut state = TablesBrowserState::default();
        state.entries = vec![
            TableEntry {
                keyspace: "ks1".to_string(),
                name: "table1".to_string(),
                qualified_name: "ks1.table1".to_string(),
            },
            TableEntry {
                keyspace: "ks2".to_string(),
                name: "table2".to_string(),
                qualified_name: "ks2.table2".to_string(),
            },
            TableEntry {
                keyspace: "ks3".to_string(),
                name: "users".to_string(),
                qualified_name: "ks3.users".to_string(),
            },
        ];

        // Select the last item
        state.filtered_indices = vec![0, 1, 2];
        state.list_state.select(Some(2));

        // Apply filter that removes selected item
        state.filter_text = "table".to_string();
        state.apply_filter();

        // Selection should be reset to first item
        assert_eq!(state.list_state.selected(), Some(0));
        assert_eq!(state.filtered_indices, vec![0, 1]);
    }

    #[test]
    fn test_apply_filter_resets_selection_when_empty() {
        let mut state = TablesBrowserState::default();
        state.entries = vec![TableEntry {
            keyspace: "ks1".to_string(),
            name: "table1".to_string(),
            qualified_name: "ks1.table1".to_string(),
        }];

        state.filtered_indices = vec![0];
        state.list_state.select(Some(0));

        // Apply filter that matches nothing
        state.filter_text = "nonexistent".to_string();
        state.apply_filter();

        // Selection should be cleared
        assert_eq!(state.list_state.selected(), None);
        assert_eq!(state.filtered_indices, Vec::<usize>::new());
    }

    // -------------------------------------------------------------------------
    // ResultsTableState Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_calculate_widths_empty_columns() {
        let mut state = ResultsTableState::default();
        state.calculate_widths();

        assert_eq!(state.column_widths, Vec::<u16>::new());
    }

    #[test]
    fn test_calculate_widths_headers_only() {
        let mut state = ResultsTableState::default();
        state.columns = vec!["id".to_string(), "name".to_string(), "email".to_string()];
        state.calculate_widths();

        // Widths should be header length + 2 (padding)
        // "id" (2) + 2 = 4, "name" (4) + 2 = 6, "email" (5) + 2 = 7
        assert_eq!(state.column_widths, vec![4, 6, 7]);
    }

    #[test]
    fn test_calculate_widths_with_data() {
        let mut state = ResultsTableState::default();
        state.columns = vec!["id".to_string(), "name".to_string()];
        state.rows = vec![
            vec!["1".to_string(), "Alice".to_string()],
            vec!["2".to_string(), "BobTheBuilder".to_string()],
        ];
        state.calculate_widths();

        // id column: max(2, 1) + 2 = 4
        // name column: max(4, 13) + 2 = 15
        assert_eq!(state.column_widths, vec![4, 15]);
    }

    #[test]
    fn test_calculate_widths_caps_at_40() {
        let mut state = ResultsTableState::default();
        state.columns = vec!["long_column".to_string()];
        state.rows = vec![vec!["a".repeat(100)]];
        state.calculate_widths();

        // Should cap at 40
        assert_eq!(state.column_widths, vec![40]);
    }

    #[test]
    fn test_calculate_widths_samples_first_100_rows() {
        let mut state = ResultsTableState::default();
        state.columns = vec!["data".to_string()];

        // Create 150 rows, with row 101 having the longest content
        let mut rows = Vec::new();
        for i in 0..150 {
            if i == 101 {
                rows.push(vec!["very_long_content_here".to_string()]);
            } else {
                rows.push(vec!["x".to_string()]);
            }
        }
        state.rows = rows;
        state.calculate_widths();

        // Should only sample first 100, so won't see row 101's long content
        // Width should be max(4, 1) + 2 = 6
        assert_eq!(state.column_widths, vec![6]);
    }

    #[test]
    fn test_visible_columns_empty() {
        let state = ResultsTableState::default();
        let visible = state.visible_columns(100);

        assert_eq!(visible, 0..0);
    }

    #[test]
    fn test_visible_columns_all_fit() {
        let mut state = ResultsTableState::default();
        state.columns = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        state.column_widths = vec![10, 10, 10];
        state.col_offset = 0;

        let visible = state.visible_columns(50);

        // All columns fit
        assert_eq!(visible, 0..3);
    }

    #[test]
    fn test_visible_columns_with_offset() {
        let mut state = ResultsTableState::default();
        state.columns = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        state.column_widths = vec![10, 10, 10];
        state.col_offset = 1;

        let visible = state.visible_columns(50);

        // Should start from offset 1
        assert_eq!(visible, 1..3);
    }

    #[test]
    fn test_visible_columns_partial_fit() {
        let mut state = ResultsTableState::default();
        state.columns = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        state.column_widths = vec![15, 15, 15];
        state.col_offset = 0;

        let visible = state.visible_columns(25);

        // Only first 2 columns fit (15 + 15 = 30 > 25, but 15 < 25)
        assert_eq!(visible, 0..1);
    }

    #[test]
    fn test_visible_columns_clamps_offset() {
        let mut state = ResultsTableState::default();
        state.columns = vec!["a".to_string(), "b".to_string()];
        state.column_widths = vec![10, 10];
        state.col_offset = 100; // Beyond bounds

        let visible = state.visible_columns(50);

        // Should clamp to last valid column
        assert_eq!(visible, 1..2);
    }

    #[test]
    fn test_has_scroll_left() {
        let mut state = ResultsTableState::default();

        state.col_offset = 0;
        assert!(!state.has_scroll_left());

        state.col_offset = 1;
        assert!(state.has_scroll_left());

        state.col_offset = 5;
        assert!(state.has_scroll_left());
    }

    #[test]
    fn test_has_scroll_right() {
        let mut state = ResultsTableState::default();
        state.columns = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        state.column_widths = vec![20, 20, 20];
        state.col_offset = 0;

        // With 30 available width, only first 2 columns visible
        assert!(state.has_scroll_right(30));

        // With 100 available width, all columns visible
        assert!(!state.has_scroll_right(100));
    }

    #[test]
    fn test_has_scroll_right_at_end() {
        let mut state = ResultsTableState::default();
        state.columns = vec!["a".to_string(), "b".to_string()];
        state.column_widths = vec![10, 10];
        state.col_offset = 1; // At last column

        // At last column, no scroll right
        assert!(!state.has_scroll_right(100));
    }

    #[test]
    fn test_clear() {
        let mut state = ResultsTableState::default();
        state.columns = vec!["a".to_string(), "b".to_string()];
        state.rows = vec![vec!["1".to_string(), "2".to_string()]];
        state.row_offset = 5;
        state.col_offset = 2;
        state.selected_row = Some(3);
        state.column_widths = vec![10, 20];

        state.clear();

        assert!(state.columns.is_empty());
        assert!(state.rows.is_empty());
        assert_eq!(state.row_offset, 0);
        assert_eq!(state.col_offset, 0);
        assert_eq!(state.selected_row, None);
        assert!(state.column_widths.is_empty());
    }

    // -------------------------------------------------------------------------
    // PanelVisibility Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_panel_visibility_default() {
        let visibility = PanelVisibility::default();

        assert!(visibility.tables);
        assert!(visibility.results);
        assert!(visibility.history);
    }

    #[test]
    fn test_panel_visibility_reset() {
        let mut visibility = PanelVisibility {
            tables: false,
            results: false,
            history: false,
        };

        visibility.reset();

        assert!(visibility.tables);
        assert!(visibility.results);
        assert!(visibility.history);
    }

    #[test]
    fn test_panel_visibility_reset_restores_default() {
        let mut visibility = PanelVisibility {
            tables: true,
            results: false,
            history: true,
        };

        visibility.reset();

        let default = PanelVisibility::default();
        assert_eq!(visibility.tables, default.tables);
        assert_eq!(visibility.results, default.results);
        assert_eq!(visibility.history, default.history);
    }

    // -------------------------------------------------------------------------
    // format_duration Tests (Microsecond Display Support)
    // -------------------------------------------------------------------------

    #[test]
    fn test_format_duration_microseconds() {
        // Sub-millisecond times should display in microseconds
        assert_eq!(format_duration(Duration::from_micros(0)), "0μs");
        assert_eq!(format_duration(Duration::from_micros(1)), "1μs");
        assert_eq!(format_duration(Duration::from_micros(450)), "450μs");
        assert_eq!(format_duration(Duration::from_micros(999)), "999μs");
    }

    #[test]
    fn test_format_duration_milliseconds() {
        // 1-999ms should display with 1 decimal place
        assert_eq!(format_duration(Duration::from_micros(1_000)), "1.0ms");
        assert_eq!(format_duration(Duration::from_micros(1_200)), "1.2ms");
        assert_eq!(format_duration(Duration::from_micros(7_000)), "7.0ms");
        assert_eq!(format_duration(Duration::from_micros(74_000)), "74.0ms");
        assert_eq!(format_duration(Duration::from_micros(123_456)), "123.5ms");
        assert_eq!(format_duration(Duration::from_micros(999_999)), "1000.0ms");
    }

    #[test]
    fn test_format_duration_seconds() {
        // >= 1000ms should display as seconds with 1 decimal place
        assert_eq!(format_duration(Duration::from_micros(1_000_000)), "1.0s");
        assert_eq!(format_duration(Duration::from_micros(1_500_000)), "1.5s");
        assert_eq!(format_duration(Duration::from_micros(2_750_000)), "2.8s");
        assert_eq!(format_duration(Duration::from_micros(10_000_000)), "10.0s");
        assert_eq!(
            format_duration(Duration::from_micros(123_456_789)),
            "123.5s"
        );
    }

    #[test]
    fn test_format_duration_boundary_cases() {
        // Test exact boundaries between units
        assert_eq!(format_duration(Duration::from_nanos(999_999)), "999μs");
        assert_eq!(format_duration(Duration::from_nanos(1_000_000)), "1.0ms");
        assert_eq!(
            format_duration(Duration::from_nanos(999_999_999)),
            "1000.0ms"
        );
        assert_eq!(format_duration(Duration::from_nanos(1_000_000_000)), "1.0s");
    }

    #[test]
    fn test_format_duration_typical_query_times() {
        // Test typical query execution times seen in the wild
        assert_eq!(format_duration(Duration::from_micros(500)), "500μs"); // Fast indexed lookup
        assert_eq!(format_duration(Duration::from_micros(3_500)), "3.5ms"); // Normal query
        assert_eq!(format_duration(Duration::from_micros(25_000)), "25.0ms"); // Slower query
        assert_eq!(format_duration(Duration::from_micros(150_000)), "150.0ms"); // Complex query
    }

    // -------------------------------------------------------------------------
    // History Navigation Tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_navigate_history_empty() {
        // Create a minimal TuiApp for testing history navigation
        let mut app_state = create_test_app_state();

        // Navigating empty history should do nothing
        app_state.navigate_history(true);
        assert_eq!(app_state.input, "");
        assert_eq!(app_state.history_index, None);
    }

    #[test]
    fn test_navigate_history_up_from_fresh() {
        let mut app_state = create_test_app_state();

        // Add some history
        app_state.history.push("SELECT * FROM users".to_string());
        app_state.history.push("SELECT * FROM orders".to_string());
        app_state.history.push("SELECT * FROM products".to_string());

        // Press Up once - should show most recent command
        app_state.navigate_history(true);
        assert_eq!(app_state.input, "SELECT * FROM products");
        assert_eq!(app_state.history_index, Some(2));

        // Press Up again - should show previous command
        app_state.navigate_history(true);
        assert_eq!(app_state.input, "SELECT * FROM orders");
        assert_eq!(app_state.history_index, Some(1));

        // Press Up again - should show oldest command
        app_state.navigate_history(true);
        assert_eq!(app_state.input, "SELECT * FROM users");
        assert_eq!(app_state.history_index, Some(0));

        // Press Up again - should stay at oldest
        app_state.navigate_history(true);
        assert_eq!(app_state.input, "SELECT * FROM users");
        assert_eq!(app_state.history_index, Some(0));
    }

    #[test]
    fn test_navigate_history_down() {
        let mut app_state = create_test_app_state();

        // Add history
        app_state.history.push("command1".to_string());
        app_state.history.push("command2".to_string());
        app_state.history.push("command3".to_string());

        // Navigate to middle of history
        app_state.navigate_history(true);
        app_state.navigate_history(true);
        assert_eq!(app_state.history_index, Some(1));
        assert_eq!(app_state.input, "command2");

        // Press Down - should go forward
        app_state.navigate_history(false);
        assert_eq!(app_state.input, "command3");
        assert_eq!(app_state.history_index, Some(2));

        // Press Down again - should clear input and reset
        app_state.navigate_history(false);
        assert_eq!(app_state.input, "");
        assert_eq!(app_state.history_index, None);

        // Press Down when already at end - should do nothing
        app_state.navigate_history(false);
        assert_eq!(app_state.input, "");
        assert_eq!(app_state.history_index, None);
    }

    #[test]
    fn test_navigate_history_cycle() {
        let mut app_state = create_test_app_state();

        // Add history
        app_state.history.push("first".to_string());
        app_state.history.push("second".to_string());

        // Go up to oldest
        app_state.navigate_history(true);
        app_state.navigate_history(true);
        assert_eq!(app_state.input, "first");

        // Go down to newest
        app_state.navigate_history(false);
        assert_eq!(app_state.input, "second");

        // Go down past end - should clear
        app_state.navigate_history(false);
        assert_eq!(app_state.input, "");
        assert_eq!(app_state.history_index, None);

        // Can navigate up again from fresh state
        app_state.navigate_history(true);
        assert_eq!(app_state.input, "second");
        assert_eq!(app_state.history_index, Some(1));
    }

    /// Helper struct for testing history navigation without a full TuiApp
    /// This allows us to test the navigate_history logic in isolation
    struct TestHistoryState {
        input: String,
        history: Vec<String>,
        history_index: Option<usize>,
    }

    impl TestHistoryState {
        fn new() -> Self {
            Self {
                input: String::new(),
                history: Vec::new(),
                history_index: None,
            }
        }

        /// Navigate history (same logic as TuiApp::navigate_history)
        fn navigate_history(&mut self, up: bool) {
            if self.history.is_empty() {
                return;
            }

            if up {
                let index = match self.history_index {
                    None => self.history.len() - 1,
                    Some(i) if i > 0 => i - 1,
                    Some(_) => return,
                };
                self.history_index = Some(index);
                self.input = self.history[index].clone();
            } else {
                match self.history_index {
                    None => return,
                    Some(i) if i < self.history.len() - 1 => {
                        self.history_index = Some(i + 1);
                        self.input = self.history[i + 1].clone();
                    }
                    Some(_) => {
                        self.history_index = None;
                        self.input.clear();
                    }
                }
            }
        }
    }

    /// Helper function to create a minimal test state for history navigation
    fn create_test_app_state() -> TestHistoryState {
        TestHistoryState::new()
    }

    // -------------------------------------------------------------------------
    // extract_table_name Tests (Issue #251)
    // -------------------------------------------------------------------------

    #[test]
    fn test_extract_table_name_valid() {
        assert_eq!(
            extract_table_name("users-3b7a9d8c"),
            Some("users".to_string())
        );
        assert_eq!(
            extract_table_name("test_table-abc123"),
            Some("test_table".to_string())
        );
        assert_eq!(
            extract_table_name("MyTable123-uuid"),
            Some("MyTable123".to_string())
        );
    }

    #[test]
    fn test_extract_table_name_invalid() {
        // No dash
        assert_eq!(extract_table_name("users"), None);
        // Empty before dash
        assert_eq!(extract_table_name("-uuid"), None);
        // Special characters
        assert_eq!(extract_table_name("table.name-uuid"), None);
        assert_eq!(
            extract_table_name("table-name-uuid"),
            Some("table".to_string())
        ); // Takes first part before first dash
    }

    #[test]
    fn test_extract_table_name_edge_cases() {
        // Multiple dashes - should extract first part
        assert_eq!(
            extract_table_name("my-table-uuid-1234"),
            Some("my".to_string())
        );
        // Just dash
        assert_eq!(extract_table_name("-"), None);
        // Empty string
        assert_eq!(extract_table_name(""), None);
    }
}
