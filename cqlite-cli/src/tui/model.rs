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

    /// Refresh status metrics if stale.
    ///
    /// Returns `true` only when the staleness branch actually fired (i.e. the
    /// status bar's values were re-collected), so the event loop can repaint on
    /// exactly those cycles (issue #1718).
    pub(super) async fn refresh_metrics(&mut self) -> bool {
        if self.metrics_stale() {
            self.status_metrics =
                Some(StatusMetrics::collect(Some(&self.db_path), Some(&self.database)).await);
            self.metrics_last_updated = Some(Instant::now());
            return true;
        }
        false
    }

    /// Test seam (issue #1718): force the next [`Self::refresh_metrics`] call to
    /// take the staleness branch, so the draw-gating tests are deterministic and
    /// never depend on the wall clock.
    #[cfg(test)]
    pub(super) fn mark_metrics_stale(&mut self) {
        self.metrics_last_updated = None;
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
#[path = "model_tests.rs"]
mod model_tests;
