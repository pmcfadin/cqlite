//! TUI drawing, layout, and widget rendering (Issue #1130).
//!
//! All `ratatui` drawing for the TUI lives here: the layout calculator, the
//! per-panel render functions, and the top-level [`ui`] entry point used by
//! the event loop in [`super::events`]. State is read from
//! [`super::model::TuiApp`].

use super::model::{format_duration, FocusPanel, LayoutAreas, PanelVisibility, TuiApp};
use crate::status_metrics::HealthIndicator;
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table, Wrap},
    Frame,
};

// =============================================================================
// Layout Calculation (Issue #251)
// =============================================================================

/// Build dynamic layout based on panel visibility
fn build_layout(area: Rect, visibility: &PanelVisibility) -> LayoutAreas {
    // Vertical layout: Header | Main | Input | Status
    let vertical_chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(0)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(10),   // Main content
            Constraint::Length(3), // Input
            Constraint::Length(3), // Status
        ])
        .split(area);

    let main_area = vertical_chunks[1];

    // Horizontal split: Tables panel (left) | Right side
    let (tables_area, right_area) = if visibility.tables {
        let h_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(25), // Tables panel
                Constraint::Percentage(75), // Right side
            ])
            .split(main_area);
        (Some(h_chunks[0]), h_chunks[1])
    } else {
        (None, main_area)
    };

    // Right side vertical split: Results (top) | History (bottom)
    let (results_area, history_area) = match (visibility.results, visibility.history) {
        (true, true) => {
            let v_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Percentage(65), // Results
                    Constraint::Percentage(35), // History
                ])
                .split(right_area);
            (Some(v_chunks[0]), Some(v_chunks[1]))
        }
        (true, false) => (Some(right_area), None),
        (false, true) => (None, Some(right_area)),
        (false, false) => (None, None),
    };

    LayoutAreas {
        header: vertical_chunks[0],
        tables: tables_area,
        results: results_area,
        history: history_area,
        input: vertical_chunks[2],
        status: vertical_chunks[3],
    }
}

// =============================================================================
// Panel Rendering Functions (Issue #251)
// =============================================================================

/// Render the Tables browser panel
fn render_tables_panel(f: &mut Frame, area: Rect, app: &mut TuiApp) {
    let is_focused = app.focus_panel == FocusPanel::Tables;
    let border_style = if is_focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    // Split for filter input (if active or has text)
    let (filter_area, list_area) =
        if app.tables_browser.filter_active || !app.tables_browser.filter_text.is_empty() {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(1)])
                .split(area);
            (Some(chunks[0]), chunks[1])
        } else {
            (None, area)
        };

    // Render filter input if visible
    if let Some(filter_rect) = filter_area {
        let filter_border = if app.tables_browser.filter_active {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let filter = Paragraph::new(app.tables_browser.filter_text.as_str())
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Filter (/)")
                    .border_style(filter_border),
            )
            .style(if app.tables_browser.filter_active {
                Style::default().fg(Color::Yellow)
            } else {
                Style::default()
            });
        f.render_widget(filter, filter_rect);

        // Set cursor in filter input mode
        if app.tables_browser.filter_active {
            f.set_cursor(
                filter_rect.x + app.tables_browser.filter_text.len() as u16 + 1,
                filter_rect.y + 1,
            );
        }
    }

    // Render table list - collect items before borrowing list_state mutably
    let items: Vec<ListItem> = app
        .tables_browser
        .filtered_indices
        .iter()
        .map(|&idx| {
            if let Some(entry) = app.tables_browser.entries.get(idx) {
                ListItem::new(Line::from(vec![
                    Span::styled("+ ", Style::default().fg(Color::Green)),
                    Span::raw(entry.qualified_name.clone()),
                ]))
            } else {
                ListItem::new(Line::from(""))
            }
        })
        .collect();

    let title = format!(
        "Tables [1] ({}/{})",
        app.tables_browser.filtered_indices.len(),
        app.tables_browser.entries.len()
    );

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title)
                .border_style(border_style),
        )
        .highlight_style(
            Style::default()
                .add_modifier(Modifier::REVERSED)
                .fg(Color::Cyan),
        )
        .highlight_symbol("> ");

    f.render_stateful_widget(list, list_area, &mut app.tables_browser.list_state);
}

/// Render the Query Results panel with Table widget
fn render_results_panel(f: &mut Frame, area: Rect, app: &mut TuiApp) {
    let is_focused = app.focus_panel == FocusPanel::Results;
    let border_style = if is_focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    // If no results, show empty state with messages
    if app.results_table.columns.is_empty() {
        // Show messages instead when no query results
        let messages: Vec<ListItem> = app
            .messages
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let content = Line::from(Span::raw(format!("{}: {}", i + 1, m)));
                ListItem::new(content)
            })
            .collect();

        let empty_widget = List::new(messages).block(
            Block::default()
                .borders(Borders::ALL)
                .title("Query Results [2]")
                .border_style(border_style),
        );
        f.render_widget(empty_widget, area);
        return;
    }

    // Calculate visible columns based on available width
    let inner_width = area.width.saturating_sub(4); // Account for borders and highlight symbol
    let visible_cols = app.results_table.visible_columns(inner_width);

    // Build column widths for visible columns - clone to avoid borrow issues
    let column_widths: Vec<u16> = app.results_table.column_widths.clone();

    // Build header row - clone columns to avoid borrow issues and truncate to column width
    let header_cells: Vec<Cell> = app.results_table.columns[visible_cols.clone()]
        .iter()
        .enumerate()
        .map(|(idx, h)| {
            // Get column index in full list for width lookup
            let col_idx = visible_cols.start + idx;
            let col_width = column_widths.get(col_idx).copied().unwrap_or(10) as usize;
            let max_chars = col_width.saturating_sub(2);

            // Truncate header to fit column width
            let truncated = if h.len() > max_chars {
                format!("{}…", &h[..max_chars.saturating_sub(1)])
            } else {
                h.clone()
            };

            Cell::from(truncated).style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )
        })
        .collect();
    let header = Row::new(header_cells).height(1);

    // Build data rows with vertical scrolling - clone row data to avoid borrow issues
    let visible_height = area.height.saturating_sub(4) as usize; // Account for borders and header
    let row_offset = app.results_table.row_offset;
    let rows: Vec<Row> = app
        .results_table
        .rows
        .iter()
        .skip(row_offset)
        .take(visible_height)
        .map(|row| {
            let cells: Vec<Cell> = visible_cols
                .clone()
                .enumerate()
                .filter_map(|(_idx, i)| {
                    row.get(i).map(|cell_content| {
                        // Get the column width for truncation
                        let col_width = column_widths.get(i).copied().unwrap_or(10) as usize;
                        // Truncate cell content to fit column width (account for padding)
                        let max_chars = col_width.saturating_sub(2);
                        let truncated = if cell_content.len() > max_chars {
                            format!("{}…", &cell_content[..max_chars.saturating_sub(1)])
                        } else {
                            cell_content.clone()
                        };
                        Cell::from(truncated)
                    })
                })
                .collect();
            Row::new(cells)
        })
        .collect();
    let widths: Vec<Constraint> = visible_cols
        .clone()
        .filter_map(|i| column_widths.get(i).map(|&w| Constraint::Length(w)))
        .collect();

    // Build title with scroll indicators
    let has_left = app.results_table.has_scroll_left();
    let has_right = app.results_table.has_scroll_right(inner_width);
    let num_cols = app.results_table.columns.len();
    let num_rows = app.results_table.rows.len();
    let scroll_hint = if has_left || has_right {
        format!(
            " (cols {}-{}/{}) ",
            visible_cols.start + 1,
            visible_cols.end,
            num_cols
        )
    } else {
        String::new()
    };
    let row_hint = if num_rows > visible_height {
        format!(
            " rows {}-{}/{}",
            row_offset + 1,
            (row_offset + visible_height).min(num_rows),
            num_rows
        )
    } else {
        format!(" {} rows", num_rows)
    };
    let title = format!("Query Results [2]{}{}", scroll_hint, row_hint);

    let table = Table::new(rows)
        .header(header)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title)
                .border_style(border_style),
        )
        .widths(&widths)
        .column_spacing(1) // Add 1 space between columns to prevent overlap
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED))
        .highlight_symbol(">> ");

    f.render_stateful_widget(table, area, &mut app.results_table.table_state);
}

/// Render the Query History panel
fn render_history_panel(f: &mut Frame, area: Rect, app: &mut TuiApp) {
    let is_focused = app.focus_panel == FocusPanel::History;
    let border_style = if is_focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let items: Vec<ListItem> = app
        .query_results
        .iter()
        .map(|result| {
            let status = if result.success { "✓" } else { "✗" };
            let time_str = result
                .execution_time
                .map(|t| format_duration(t))
                .unwrap_or_else(|| "--".to_string());

            let status_style = if result.success {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(Color::Red)
            };

            // Truncate query to fit available width (estimate ~60 chars for query text)
            let query_text = if result.query.len() > 60 {
                format!("{}…", &result.query[..59])
            } else {
                result.query.clone()
            };

            // CRITICAL: Build the entire line content as a single Line to prevent wrapping
            // Format: "✓ 7ms SELECT * FROM test_basic.composite_key_table"
            let line = Line::from(vec![
                Span::styled(status, status_style),
                Span::raw(" "),
                Span::styled(time_str, Style::default().fg(Color::Cyan)),
                Span::raw(" "),
                Span::raw(query_text),
            ]);

            ListItem::new(line)
        })
        .collect();

    let title = format!("Query History [3] ({})", app.query_results.len());

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title)
                .border_style(border_style),
        )
        .highlight_style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .bg(Color::DarkGray),
        )
        .highlight_symbol("> ");

    f.render_stateful_widget(list, area, &mut app.history_scroll);
}

/// Render the header bar
fn render_header(f: &mut Frame, area: Rect, app: &TuiApp) {
    let keyspace_text = app
        .current_keyspace
        .as_ref()
        .map(|ks| format!("[{}]", ks))
        .unwrap_or_else(|| "[no keyspace]".to_string());

    let header = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(
                "CQLite TUI v0.1.0",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(keyspace_text, Style::default().fg(Color::Yellow)),
            Span::raw("  "),
            Span::styled("F1:Help", Style::default().fg(Color::DarkGray)),
            Span::raw(" "),
            Span::styled("F2-F4:Toggle", Style::default().fg(Color::DarkGray)),
            Span::raw(" "),
            Span::styled("Esc:Exit", Style::default().fg(Color::DarkGray)),
        ]),
        Line::from(vec![
            Span::raw("Database: "),
            Span::styled(
                app.db_path.display().to_string(),
                Style::default().fg(Color::Green),
            ),
        ]),
    ])
    .block(Block::default().borders(Borders::ALL));
    f.render_widget(header, area);
}

/// Render the input area
fn render_input(f: &mut Frame, area: Rect, app: &TuiApp) {
    let is_focused = app.focus_panel == FocusPanel::Input;
    let border_style = if is_focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    };

    let input = Paragraph::new(app.input.as_str())
        .style(if is_focused {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
        })
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("CQL> ")
                .border_style(border_style),
        );
    f.render_widget(input, area);

    // Set cursor position when input is focused
    if is_focused && !app.tables_browser.filter_active {
        f.set_cursor(area.x + app.input.len() as u16 + 1, area.y + 1);
    }
}

/// Render the status bar
fn render_status(f: &mut Frame, area: Rect, app: &TuiApp) {
    let (health_text, health_color) = match app.status_metrics.as_ref() {
        Some(metrics) => match metrics.health {
            HealthIndicator::Ok => ("OK", Color::Green),
            HealthIndicator::Warning => ("WARN", Color::Yellow),
            HealthIndicator::Error => ("ERR", Color::Red),
        },
        None => ("--", Color::DarkGray),
    };

    let memory_text = app
        .status_metrics
        .as_ref()
        .map(|m| m.format_memory())
        .unwrap_or_else(|| "--".to_string());

    let data_text = app
        .status_metrics
        .as_ref()
        .map(|m| m.format_data())
        .unwrap_or_else(|| "--".to_string());

    // Show focused panel in mode
    let mode_text = match app.focus_panel {
        FocusPanel::Tables => "TABLES",
        FocusPanel::Results => "RESULTS",
        FocusPanel::History => "HISTORY",
        FocusPanel::Input => "INPUT",
    };

    let status_line = Line::from(vec![
        Span::raw("Health: "),
        Span::styled(health_text, Style::default().fg(health_color)),
        Span::raw(" | Mem: "),
        Span::styled(&memory_text, Style::default().fg(Color::Cyan)),
        Span::raw(" | Data: "),
        Span::styled(&data_text, Style::default().fg(Color::Cyan)),
        Span::raw(" | Status: "),
        Span::styled(&app.status_message, Style::default().fg(Color::Green)),
        Span::raw(" | Mode: "),
        Span::styled(mode_text, Style::default().fg(Color::Cyan)),
    ]);

    let status = Paragraph::new(status_line).block(Block::default().borders(Borders::ALL));
    f.render_widget(status, area);
}

/// Draw the TUI interface
pub(super) fn ui(f: &mut Frame, app: &mut TuiApp) {
    if app.show_help {
        draw_help(f);
        return;
    }

    // Build dynamic layout based on panel visibility
    let layout = build_layout(f.size(), &app.panel_visibility);

    // Render header
    render_header(f, layout.header, app);

    // Render visible panels
    if let Some(tables_area) = layout.tables {
        render_tables_panel(f, tables_area, app);
    }

    if let Some(results_area) = layout.results {
        render_results_panel(f, results_area, app);
    }

    if let Some(history_area) = layout.history {
        render_history_panel(f, history_area, app);
    }

    // If no panels are visible in main area, show a message
    if layout.tables.is_none() && layout.results.is_none() && layout.history.is_none() {
        // This shouldn't happen normally, but handle gracefully
        let main_area = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(1),
                Constraint::Length(3),
                Constraint::Length(3),
            ])
            .split(f.size())[1];

        let msg = Paragraph::new("Press F2/F3/F4 to show panels, or F5 to reset layout")
            .block(Block::default().borders(Borders::ALL).title("No Panels"))
            .style(Style::default().fg(Color::DarkGray));
        f.render_widget(msg, main_area);
    }

    // Render input area
    render_input(f, layout.input, app);

    // Render status bar
    render_status(f, layout.status, app);
}

/// Draw the help screen
fn draw_help(f: &mut Frame) {
    let help_text = vec![
        Line::from(Span::styled(
            "CQLite TUI Help (Issue #251)",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "Global Commands:",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from("  F1          Toggle this help screen"),
        Line::from("  F2          Toggle Tables panel"),
        Line::from("  F3          Toggle Results panel"),
        Line::from("  F4          Toggle History panel"),
        Line::from("  F5          Reset layout (show all panels)"),
        Line::from("  Tab         Cycle focus to next panel"),
        Line::from("  Shift+Tab   Cycle focus to previous panel"),
        Line::from("  1/2/3       Jump directly to panel"),
        Line::from("  Esc         Exit application"),
        Line::from("  Ctrl+C      Quit immediately"),
        Line::from("  Ctrl+L      Clear screen and history"),
        Line::from(""),
        Line::from(Span::styled(
            "Tables Panel [1]:",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from("  j/k, Up/Down  Navigate tables"),
        Line::from("  /             Open filter input"),
        Line::from("  Enter         Query selected table"),
        Line::from("  d             Describe selected table"),
        Line::from("  g/G           Jump to first/last table"),
        Line::from(""),
        Line::from(Span::styled(
            "Results Panel [2]:",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from("  j/k, Up/Down  Scroll rows"),
        Line::from("  h/l, Left/Right  Scroll columns (horizontal)"),
        Line::from("  g/G           Jump to first/last row"),
        Line::from("  PgUp/PgDn     Page up/down"),
        Line::from(""),
        Line::from(Span::styled(
            "History Panel [3]:",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from("  j/k, Up/Down  Navigate history"),
        Line::from("  Enter         Copy query to input"),
        Line::from("  g/G           Jump to first/last entry"),
        Line::from(""),
        Line::from(Span::styled(
            "Input Panel:",
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from("  Enter         Execute current query"),
        Line::from("  Up/Down       Navigate command history"),
        Line::from("  Backspace     Delete character"),
        Line::from(""),
        Line::from(Span::styled(
            "Press any key to close this help",
            Style::default().fg(Color::DarkGray),
        )),
    ];

    let help_paragraph = Paragraph::new(help_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Help - Press any key to close")
                .border_style(Style::default().fg(Color::Yellow)),
        )
        .wrap(Wrap { trim: true });

    let area = centered_rect(85, 95, f.size());
    f.render_widget(help_paragraph, area);
}

/// Create a centered rectangle
fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
