use crate::config::Config;
use anyhow::Result;
use cqlite_core::Database;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
    Frame, Terminal,
};
use tokio::sync::Mutex;

pub async fn start_tui_mode(db_path: &Path, config: &Config, database: Database) -> Result<()> {
    // Initialize the database
    let db = Arc::new(database);
    
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create app state
    let mut app = TuiApp::new(db_path, config, db).await?;
    let res = run_tui(&mut terminal, &mut app).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("{:?}", err)
    }

    Ok(())
}

/// TUI Application State
struct TuiApp {
    db_path: std::path::PathBuf,
    database: Arc<Database>,
    input: String,
    input_mode: InputMode,
    messages: Vec<String>,
    scroll_offset: usize,
    history: Vec<String>,
    history_index: Option<usize>,
    query_results: Vec<QueryDisplayResult>,
    results_scroll: ListState,
    show_help: bool,
    status_message: String,
    last_execution_time: Option<Duration>,
}

#[derive(Clone, PartialEq)]
enum InputMode {
    Normal,
    Editing,
    Results,
    Help,
}

#[derive(Clone)]
struct QueryDisplayResult {
    query: String,
    success: bool,
    rows: usize,
    execution_time: Option<Duration>,
    error_message: Option<String>,
}

impl TuiApp {
    async fn new(db_path: &Path, _config: &Config, database: Arc<Database>) -> Result<Self> {
        Ok(TuiApp {
            db_path: db_path.to_path_buf(),
            database,
            input: String::new(),
            input_mode: InputMode::Editing,
            messages: vec![
                "Welcome to CQLite TUI Mode!".to_string(),
                "Type your CQL queries and press Enter to execute.".to_string(),
                "Press F1 for help, Esc to exit.".to_string(),
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
        })
    }

    /// Execute a CQL query
    async fn execute_query(&mut self) {
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
                        "✓ Query executed: {} rows affected ({:.2}ms)",
                        result.rows_affected,
                        execution_time.as_millis()
                    ));
                } else {
                    self.messages.push(format!(
                        "✓ Query executed: {} rows returned ({:.2}ms)",
                        result.rows.len(),
                        execution_time.as_millis()
                    ));
                    
                    // Show first few rows in messages
                    if !result.rows.is_empty() {
                        let column_names = result.rows[0].column_names();
                        self.messages.push(format!("Columns: {}", column_names.join(", ")));
                        
                        for (i, row) in result.rows.iter().take(3).enumerate() {
                            let values: Vec<String> = column_names.iter()
                                .map(|col| {
                                    row.get(col)
                                        .map(|v| v.to_string())
                                        .unwrap_or_else(|| "NULL".to_string())
                                })
                                .collect();
                            self.messages.push(format!("  Row {}: {}", i + 1, values.join(" | ")));
                        }
                        
                        if result.rows.len() > 3 {
                            self.messages.push(format!("  ... and {} more rows", result.rows.len() - 3));
                        }
                    }
                }
                
                self.status_message = format!("Query completed in {:.2}ms", execution_time.as_millis());
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

/// Main TUI event loop
async fn run_tui<B: Backend>(terminal: &mut Terminal<B>, app: &mut TuiApp) -> Result<()> {
    loop {
        terminal.draw(|f| ui(f, app))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                match (app.input_mode.clone(), key.code) {
                    // Global keybindings
                    (_, KeyCode::F(1)) => {
                        app.show_help = !app.show_help;
                        app.input_mode = if app.show_help { InputMode::Help } else { InputMode::Editing };
                    }
                    (_, KeyCode::Esc) => {
                        if app.show_help {
                            app.show_help = false;
                            app.input_mode = InputMode::Editing;
                        } else {
                            return Ok(());
                        }
                    }
                    
                    // Help mode
                    (InputMode::Help, _) => {
                        // Any key in help mode returns to editing
                        app.show_help = false;
                        app.input_mode = InputMode::Editing;
                    }
                    
                    // Query input mode
                    (InputMode::Editing, KeyCode::Enter) => {
                        app.execute_query().await;
                    }
                    (InputMode::Editing, KeyCode::Char(c)) => {
                        if key.modifiers.contains(KeyModifiers::CONTROL) {
                            match c {
                                'c' => return Ok(()), // Ctrl+C to exit
                                'l' => {
                                    app.messages.clear();
                                    app.query_results.clear();
                                    app.status_message = "Screen cleared".to_string();
                                }
                                _ => {}
                            }
                        } else {
                            app.input.push(c);
                        }
                    }
                    (InputMode::Editing, KeyCode::Backspace) => {
                        app.input.pop();
                    }
                    (InputMode::Editing, KeyCode::Up) => {
                        app.navigate_history(true);
                    }
                    (InputMode::Editing, KeyCode::Down) => {
                        app.navigate_history(false);
                    }
                    (InputMode::Editing, KeyCode::Tab) => {
                        app.input_mode = InputMode::Results;
                        app.results_scroll.select(Some(0));
                    }
                    
                    // Results navigation mode
                    (InputMode::Results, KeyCode::Tab) => {
                        app.input_mode = InputMode::Editing;
                        app.results_scroll.select(None);
                    }
                    (InputMode::Results, KeyCode::Up) => {
                        let selected = app.results_scroll.selected().unwrap_or(0);
                        if selected > 0 {
                            app.results_scroll.select(Some(selected - 1));
                        }
                    }
                    (InputMode::Results, KeyCode::Down) => {
                        let selected = app.results_scroll.selected().unwrap_or(0);
                        if selected < app.query_results.len().saturating_sub(1) {
                            app.results_scroll.select(Some(selected + 1));
                        }
                    }
                    (InputMode::Results, KeyCode::Enter) => {
                        if let Some(selected) = app.results_scroll.selected() {
                            if let Some(result) = app.query_results.get(selected) {
                                app.input = result.query.clone();
                                app.input_mode = InputMode::Editing;
                                app.results_scroll.select(None);
                            }
                        }
                    }
                    
                    _ => {}
                }
            }
        }
    }
}

/// Draw the TUI interface
fn ui(f: &mut Frame, app: &TuiApp) {
    if app.show_help {
        draw_help(f);
        return;
    }

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .margin(1)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(1),    // Main content
            Constraint::Length(3), // Input
            Constraint::Length(3), // Status
        ])
        .split(f.size());

    // Header
    let header = Paragraph::new(vec![
        Line::from(vec![
            Span::styled("CQLite TUI", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::raw(" - Database: "),
            Span::styled(app.db_path.display().to_string(), Style::default().fg(Color::Yellow)),
        ]),
        Line::from(vec![
            Span::raw("F1: Help | Tab: Navigate | Esc: Exit | Ctrl+C: Quit | Ctrl+L: Clear")
        ]),
    ])
    .block(Block::default().borders(Borders::ALL).title("Header"));
    f.render_widget(header, chunks[0]);

    // Main content area - split between messages and results
    let main_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(chunks[1]);

    // Messages/Output area
    let messages: Vec<ListItem> = app
        .messages
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let content = Line::from(Span::raw(format!("{}: {}", i + 1, m)));
            ListItem::new(content)
        })
        .collect();
    
    let messages_widget = List::new(messages)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Query Output")
                .border_style(if app.input_mode == InputMode::Editing {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default()
                })
        )
        .highlight_style(Style::default().add_modifier(Modifier::BOLD))
        .highlight_symbol("> ");
    f.render_widget(messages_widget, main_chunks[0]);

    // Query results history
    let results: Vec<ListItem> = app
        .query_results
        .iter()
        .map(|result| {
            let status = if result.success { "✓" } else { "✗" };
            let time_str = result.execution_time
                .map(|t| format!("{:.2}ms", t.as_millis()))
                .unwrap_or_else(|| "--".to_string());
            
            let line = if result.success {
                Line::from(vec![
                    Span::styled(status, Style::default().fg(Color::Green)),
                    Span::raw(format!(" {} rows ({})", result.rows, time_str)),
                    Span::raw(format!(" | {}", result.query.chars().take(30).collect::<String>())),
                ])
            } else {
                Line::from(vec![
                    Span::styled(status, Style::default().fg(Color::Red)),
                    Span::raw(format!(" Error ({})", time_str)),
                    Span::raw(format!(" | {}", result.query.chars().take(30).collect::<String>())),
                ])
            };
            
            ListItem::new(line)
        })
        .collect();
    
    let results_widget = List::new(results)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Query History")
                .border_style(if app.input_mode == InputMode::Results {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default()
                })
        )
        .highlight_style(Style::default().add_modifier(Modifier::BOLD).bg(Color::DarkGray))
        .highlight_symbol("> ");
    f.render_stateful_widget(results_widget, main_chunks[1], &mut app.results_scroll.clone());

    // Input area
    let input = Paragraph::new(app.input.as_str())
        .style(match app.input_mode {
            InputMode::Editing => Style::default().fg(Color::Yellow),
            _ => Style::default(),
        })
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("CQL Query Input")
                .border_style(if app.input_mode == InputMode::Editing {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default()
                })
        );
    f.render_widget(input, chunks[2]);

    // Status bar
    let status_text = vec![
        Line::from(vec![
            Span::raw("Status: "),
            Span::styled(&app.status_message, Style::default().fg(Color::Green)),
            Span::raw(" | Mode: "),
            Span::styled(
                match app.input_mode {
                    InputMode::Editing => "EDIT",
                    InputMode::Results => "RESULTS",
                    InputMode::Help => "HELP",
                    _ => "NORMAL",
                },
                Style::default().fg(Color::Cyan)
            ),
        ])
    ];
    
    let status = Paragraph::new(status_text)
        .block(Block::default().borders(Borders::ALL).title("Status"));
    f.render_widget(status, chunks[3]);

    // Set cursor position in input mode
    if app.input_mode == InputMode::Editing {
        f.set_cursor(
            chunks[2].x + app.input.len() as u16 + 1,
            chunks[2].y + 1,
        );
    }
}

/// Draw the help screen
fn draw_help(f: &mut Frame) {
    let help_text = vec![
        Line::from(Span::styled("CQLite TUI Help", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))),
        Line::from(""),
        Line::from("🔧 Global Commands:"),
        Line::from("  F1         - Toggle this help screen"),
        Line::from("  Esc        - Exit application"),
        Line::from("  Ctrl+C     - Quit immediately"),
        Line::from("  Ctrl+L     - Clear screen and history"),
        Line::from(""),
        Line::from("📝 Query Input Mode (default):"),
        Line::from("  Enter      - Execute current query"),
        Line::from("  Up/Down    - Navigate query history"),
        Line::from("  Tab        - Switch to results navigation"),
        Line::from("  Backspace  - Delete character"),
        Line::from(""),
        Line::from("📊 Results Navigation Mode:"),
        Line::from("  Tab        - Switch back to query input"),
        Line::from("  Up/Down    - Navigate query history"),
        Line::from("  Enter      - Copy selected query to input"),
        Line::from(""),
        Line::from("💡 Tips:"),
        Line::from("  • Type CQL queries like: SELECT * FROM table_name"),
        Line::from("  • Use CREATE TABLE, INSERT, UPDATE, DELETE commands"),
        Line::from("  • Query results appear in the left panel"),
        Line::from("  • Query history is shown in the right panel"),
        Line::from("  • Press any key to close this help"),
    ];

    let help_paragraph = Paragraph::new(help_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Help - Press any key to close")
                .border_style(Style::default().fg(Color::Yellow))
        )
        .wrap(Wrap { trim: true });

    let area = centered_rect(80, 90, f.size());
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
