use crate::config::Config;
use anyhow::Result;
use std::path::Path;

pub async fn start_tui_mode(_db_path: &Path, _config: &Config) -> Result<()> {
    // TODO: Implement full TUI mode with ratatui
    println!("TUI mode is not yet implemented.");
    println!("Falling back to interactive REPL mode...");

    // For now, fall back to REPL
    crate::interactive::start_repl_mode(_db_path, _config).await
}
