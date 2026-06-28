//! Terminal UI (ratatui) for CQLite — interactive multi-panel CQL browser.
//!
//! Split by responsibility (Issue #1130, epic #1116):
//! - [`model`]: application state and state transitions ([`model::TuiApp`]).
//! - [`events`]: the `crossterm` event loop and per-panel key handlers.
//! - [`render`]: `ratatui` layout and widget drawing.
//!
//! The sole public entry point is [`start_tui_mode`].

mod events;
mod model;
mod render;

use crate::config::Config;
use anyhow::Result;
use cqlite_core::Database;
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::path::Path;
use std::sync::Arc;

use events::run_tui;
use model::TuiApp;

pub async fn start_tui_mode(db_path: &Path, config: &Config, database: Database) -> Result<()> {
    // CRITICAL: Disable log output to prevent messages from bleeding into the
    // TUI display. The unified tracing subscriber is already installed in
    // main.rs (Issue #1033) and cannot be replaced, so we suppress at the `log`
    // facade level — this silences the `log::*` call sites bridged into tracing.
    log::set_max_level(log::LevelFilter::Off);

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
