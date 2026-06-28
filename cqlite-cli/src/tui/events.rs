//! TUI input/event handling and the main event loop (Issue #1130).
//!
//! Owns the `crossterm` event loop ([`run_tui`]) and the per-panel key
//! handlers. State lives in [`super::model::TuiApp`]; drawing lives in
//! [`super::render`].

use super::model::{FocusPanel, TuiApp};
use super::render::ui;
use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use ratatui::{backend::Backend, Terminal};
use std::time::Duration;

/// Main TUI event loop
pub(super) async fn run_tui<B: Backend>(
    terminal: &mut Terminal<B>,
    app: &mut TuiApp,
) -> Result<()> {
    loop {
        // Refresh metrics if stale (every 5 seconds)
        app.refresh_metrics().await;

        terminal.draw(|f| ui(f, app))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                // Handle key events
                if handle_key_event(app, key).await {
                    return Ok(()); // Exit requested
                }
            }
        }
    }
}

/// Handle key events - returns true if should exit
async fn handle_key_event(app: &mut TuiApp, key: event::KeyEvent) -> bool {
    // Help mode - any key closes it
    if app.show_help {
        app.show_help = false;
        return false;
    }

    // Filter input mode in tables panel - handle specially
    if app.tables_browser.filter_active {
        return handle_filter_key(app, key);
    }

    // Global keybindings (always active)
    match key.code {
        KeyCode::F(1) => {
            app.show_help = true;
            return false;
        }
        KeyCode::F(2) => {
            app.panel_visibility.tables = !app.panel_visibility.tables;
            // Adjust focus if hiding current panel
            if !app.panel_visibility.tables && app.focus_panel == FocusPanel::Tables {
                app.focus_panel = app.focus_panel.next(&app.panel_visibility);
            }
            return false;
        }
        KeyCode::F(3) => {
            app.panel_visibility.results = !app.panel_visibility.results;
            if !app.panel_visibility.results && app.focus_panel == FocusPanel::Results {
                app.focus_panel = app.focus_panel.next(&app.panel_visibility);
            }
            return false;
        }
        KeyCode::F(4) => {
            app.panel_visibility.history = !app.panel_visibility.history;
            if !app.panel_visibility.history && app.focus_panel == FocusPanel::History {
                app.focus_panel = app.focus_panel.next(&app.panel_visibility);
            }
            return false;
        }
        KeyCode::F(5) => {
            app.panel_visibility.reset();
            return false;
        }
        KeyCode::Esc => {
            return true; // Exit
        }
        KeyCode::Tab => {
            app.focus_panel = app.focus_panel.next(&app.panel_visibility);
            return false;
        }
        KeyCode::BackTab => {
            app.focus_panel = app.focus_panel.prev(&app.panel_visibility);
            return false;
        }
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            return true; // Ctrl+C exits
        }
        KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.messages.clear();
            app.query_results.clear();
            app.results_table.clear();
            app.status_message = "Screen cleared".to_string();
            return false;
        }
        // Number keys for direct panel focus (only when NOT in Input panel)
        KeyCode::Char('1')
            if key.modifiers.is_empty()
                && app.panel_visibility.tables
                && app.focus_panel != FocusPanel::Input =>
        {
            app.focus_panel = FocusPanel::Tables;
            return false;
        }
        KeyCode::Char('2')
            if key.modifiers.is_empty()
                && app.panel_visibility.results
                && app.focus_panel != FocusPanel::Input =>
        {
            app.focus_panel = FocusPanel::Results;
            return false;
        }
        KeyCode::Char('3')
            if key.modifiers.is_empty()
                && app.panel_visibility.history
                && app.focus_panel != FocusPanel::Input =>
        {
            app.focus_panel = FocusPanel::History;
            return false;
        }
        _ => {}
    }

    // Panel-specific keybindings
    match app.focus_panel {
        FocusPanel::Tables => handle_tables_key(app, key).await,
        FocusPanel::Results => handle_results_key(app, key),
        FocusPanel::History => handle_history_key(app, key),
        FocusPanel::Input => handle_input_key(app, key).await,
    }

    false
}

/// Handle keys when filter input is active
fn handle_filter_key(app: &mut TuiApp, key: event::KeyEvent) -> bool {
    match key.code {
        KeyCode::Enter | KeyCode::Esc => {
            app.tables_browser.filter_active = false;
        }
        KeyCode::Char(c) => {
            app.tables_browser.filter_text.push(c);
            app.tables_browser.apply_filter();
        }
        KeyCode::Backspace => {
            app.tables_browser.filter_text.pop();
            app.tables_browser.apply_filter();
        }
        _ => {}
    }
    false
}

/// Handle keys in Tables panel
async fn handle_tables_key(app: &mut TuiApp, key: event::KeyEvent) {
    let browser = &mut app.tables_browser;

    match key.code {
        KeyCode::Char('j') | KeyCode::Down => {
            let selected = browser.list_state.selected().unwrap_or(0);
            if selected < browser.filtered_indices.len().saturating_sub(1) {
                browser.list_state.select(Some(selected + 1));
            }
        }
        KeyCode::Char('k') | KeyCode::Up => {
            let selected = browser.list_state.selected().unwrap_or(0);
            if selected > 0 {
                browser.list_state.select(Some(selected - 1));
            }
        }
        KeyCode::Char('/') => {
            browser.filter_active = true;
        }
        KeyCode::Enter => {
            // Query selected table
            if let Some(entry) = browser.selected_entry().cloned() {
                app.input = format!("SELECT * FROM {} LIMIT 100", entry.qualified_name);
                app.focus_panel = FocusPanel::Input;
            }
        }
        KeyCode::Char('d') => {
            // Describe selected table
            if let Some(entry) = browser.selected_entry().cloned() {
                app.input = format!("DESCRIBE {}", entry.qualified_name);
                app.focus_panel = FocusPanel::Input;
            }
        }
        KeyCode::Char('g') => {
            browser.list_state.select(Some(0));
        }
        KeyCode::Char('G') => {
            let last = browser.filtered_indices.len().saturating_sub(1);
            browser.list_state.select(Some(last));
        }
        _ => {}
    }
}

/// Handle keys in Results panel
fn handle_results_key(app: &mut TuiApp, key: event::KeyEvent) {
    let results = &mut app.results_table;

    match key.code {
        KeyCode::Char('j') | KeyCode::Down => {
            if results.row_offset < results.rows.len().saturating_sub(1) {
                results.row_offset += 1;
            }
        }
        KeyCode::Char('k') | KeyCode::Up => {
            if results.row_offset > 0 {
                results.row_offset -= 1;
            }
        }
        KeyCode::Char('h') | KeyCode::Left => {
            if results.col_offset > 0 {
                results.col_offset -= 1;
            }
        }
        KeyCode::Char('l') | KeyCode::Right => {
            if results.col_offset < results.columns.len().saturating_sub(1) {
                results.col_offset += 1;
            }
        }
        KeyCode::Char('g') => {
            results.row_offset = 0;
            results.col_offset = 0;
        }
        KeyCode::Char('G') => {
            results.row_offset = results.rows.len().saturating_sub(10);
        }
        KeyCode::PageUp => {
            results.row_offset = results.row_offset.saturating_sub(20);
        }
        KeyCode::PageDown => {
            let max_offset = results.rows.len().saturating_sub(10);
            results.row_offset = (results.row_offset + 20).min(max_offset);
        }
        _ => {}
    }
}

/// Handle keys in History panel
fn handle_history_key(app: &mut TuiApp, key: event::KeyEvent) {
    match key.code {
        KeyCode::Char('j') | KeyCode::Down => {
            let selected = app.history_scroll.selected().unwrap_or(0);
            if selected < app.query_results.len().saturating_sub(1) {
                app.history_scroll.select(Some(selected + 1));
            }
        }
        KeyCode::Char('k') | KeyCode::Up => {
            let selected = app.history_scroll.selected().unwrap_or(0);
            if selected > 0 {
                app.history_scroll.select(Some(selected - 1));
            }
        }
        KeyCode::Enter => {
            // Copy selected query to input
            if let Some(selected) = app.history_scroll.selected() {
                if let Some(result) = app.query_results.get(selected) {
                    app.input = result.query.clone();
                    app.focus_panel = FocusPanel::Input;
                }
            }
        }
        KeyCode::Char('g') => {
            app.history_scroll.select(Some(0));
        }
        KeyCode::Char('G') => {
            let last = app.query_results.len().saturating_sub(1);
            app.history_scroll.select(Some(last));
        }
        _ => {}
    }
}

/// Handle keys in Input panel
async fn handle_input_key(app: &mut TuiApp, key: event::KeyEvent) {
    match key.code {
        KeyCode::Enter => {
            app.execute_query().await;
        }
        KeyCode::Char(c) => {
            app.input.push(c);
            // Reset history navigation when user starts typing
            app.history_index = None;
        }
        KeyCode::Backspace => {
            app.input.pop();
            // Reset history navigation when user edits input
            app.history_index = None;
        }
        KeyCode::Up => {
            app.navigate_history(true);
        }
        KeyCode::Down => {
            app.navigate_history(false);
        }
        _ => {}
    }
}
