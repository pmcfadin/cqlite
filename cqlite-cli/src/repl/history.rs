// Command History Management
//
// Manages command history for the REPL, including persistence, search,
// and navigation through previous commands.

use super::{ReplError, ReplResult};
use std::collections::VecDeque;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Command history entry
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    /// The command text
    pub command: String,
    /// Timestamp when command was executed
    pub timestamp: std::time::SystemTime,
    /// Execution duration (if available)
    pub duration: Option<std::time::Duration>,
    /// Whether the command succeeded
    pub success: Option<bool>,
    /// Command category for filtering
    pub category: HistoryCategory,
}

/// Categories of commands for history filtering
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HistoryCategory {
    Query,      // CQL queries
    Meta,       // Meta-commands (:help, :quit, etc.)
    Config,     // Configuration changes
    Navigation, // :use, :tables, etc.
    System,     // :clear, :history, etc.
    Unknown,
}

impl std::fmt::Display for HistoryCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HistoryCategory::Query => write!(f, "query"),
            HistoryCategory::Meta => write!(f, "meta"),
            HistoryCategory::Config => write!(f, "config"),
            HistoryCategory::Navigation => write!(f, "navigation"),
            HistoryCategory::System => write!(f, "system"),
            HistoryCategory::Unknown => write!(f, "unknown"),
        }
    }
}

/// History search filter
#[derive(Debug, Clone)]
pub struct HistoryFilter {
    /// Text pattern to match
    pub pattern: Option<String>,
    /// Category filter
    pub category: Option<HistoryCategory>,
    /// Only successful commands
    pub success_only: bool,
    /// Time range filter
    pub since: Option<std::time::SystemTime>,
    /// Maximum number of results
    pub limit: Option<usize>,
}

impl Default for HistoryFilter {
    fn default() -> Self {
        Self {
            pattern: None,
            category: None,
            success_only: false,
            since: None,
            limit: Some(50),
        }
    }
}

/// Command history manager
pub struct HistoryManager {
    /// Command history entries
    history: VecDeque<HistoryEntry>,
    /// Maximum number of entries to keep
    max_size: usize,
    /// Current position for navigation
    current_position: Option<usize>,
    /// History file path
    history_file: Option<PathBuf>,
    /// Whether to persist history to file
    persistent: bool,
    /// Statistics
    stats: HistoryStats,
}

/// History statistics
#[derive(Debug, Default)]
pub struct HistoryStats {
    pub total_commands: u64,
    pub successful_commands: u64,
    pub failed_commands: u64,
    pub by_category: std::collections::HashMap<HistoryCategory, u64>,
    pub avg_duration_ms: f64,
}

impl HistoryManager {
    /// Create a new history manager
    pub fn new(max_size: usize) -> ReplResult<Self> {
        Ok(Self {
            history: VecDeque::with_capacity(max_size),
            max_size,
            current_position: None,
            history_file: None,
            persistent: false,
            stats: HistoryStats::default(),
        })
    }

    /// Create a new persistent history manager
    pub fn new_persistent(max_size: usize, history_dir: &Path) -> ReplResult<Self> {
        let history_file = history_dir.join("cqlite_history.txt");

        // Create history directory if it doesn't exist
        if let Some(parent) = history_file.parent() {
            fs::create_dir_all(parent).map_err(ReplError::Io)?;
        }

        let mut manager = Self {
            history: VecDeque::with_capacity(max_size),
            max_size,
            current_position: None,
            history_file: Some(history_file),
            persistent: true,
            stats: HistoryStats::default(),
        };

        // Load existing history
        manager.load_history()?;

        Ok(manager)
    }

    /// Add a command to history
    pub fn add_command(&mut self, command: &str) -> ReplResult<()> {
        // Skip empty commands and duplicates
        if command.trim().is_empty() {
            return Ok(());
        }

        // Skip consecutive duplicates
        if let Some(last_entry) = self.history.back() {
            if last_entry.command.trim() == command.trim() {
                return Ok(());
            }
        }

        let entry = HistoryEntry {
            command: command.to_string(),
            timestamp: std::time::SystemTime::now(),
            duration: None,
            success: None,
            category: self.categorize_command(command),
        };

        self.add_entry(entry)?;
        Ok(())
    }

    /// Add a command with execution details
    pub fn add_command_with_result(
        &mut self,
        command: &str,
        duration: std::time::Duration,
        success: bool,
    ) -> ReplResult<()> {
        if command.trim().is_empty() {
            return Ok(());
        }

        let entry = HistoryEntry {
            command: command.to_string(),
            timestamp: std::time::SystemTime::now(),
            duration: Some(duration),
            success: Some(success),
            category: self.categorize_command(command),
        };

        self.add_entry(entry.clone())?;
        self.update_stats(&entry);

        Ok(())
    }

    /// Add an entry to history
    fn add_entry(&mut self, entry: HistoryEntry) -> ReplResult<()> {
        // Remove oldest entries if at capacity
        while self.history.len() >= self.max_size {
            self.history.pop_front();
        }

        self.history.push_back(entry.clone());
        self.current_position = None;

        // Persist if enabled
        if self.persistent {
            self.persist_entry(&entry)?;
        }

        Ok(())
    }

    /// Categorize a command
    fn categorize_command(&self, command: &str) -> HistoryCategory {
        let trimmed = command.trim();

        if trimmed.starts_with(':') || trimmed.starts_with('.') || trimmed.starts_with('\\') {
            if trimmed.contains("config") || trimmed.contains("set") {
                HistoryCategory::Config
            } else if trimmed.contains("use")
                || trimmed.contains("tables")
                || trimmed.contains("keyspaces")
                || trimmed.contains("describe")
            {
                HistoryCategory::Navigation
            } else if trimmed.contains("clear")
                || trimmed.contains("history")
                || trimmed.contains("source")
            {
                HistoryCategory::System
            } else {
                HistoryCategory::Meta
            }
        } else {
            let upper = trimmed.to_uppercase();
            if upper.starts_with("SELECT")
                || upper.starts_with("INSERT")
                || upper.starts_with("UPDATE")
                || upper.starts_with("DELETE")
                || upper.starts_with("CREATE")
                || upper.starts_with("ALTER")
                || upper.starts_with("DROP")
            {
                HistoryCategory::Query
            } else {
                HistoryCategory::Unknown
            }
        }
    }

    /// Get recent commands
    pub fn recent_commands(&self, limit: usize) -> Vec<String> {
        self.history
            .iter()
            .rev()
            .take(limit)
            .map(|entry| entry.command.clone())
            .collect()
    }

    /// Search history with filter
    pub fn search(&self, filter: &HistoryFilter) -> Vec<&HistoryEntry> {
        let mut results: Vec<&HistoryEntry> = self
            .history
            .iter()
            .filter(|entry| self.matches_filter(entry, filter))
            .collect();

        // Sort by timestamp (newest first)
        results.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        // Apply limit
        if let Some(limit) = filter.limit {
            results.truncate(limit);
        }

        results
    }

    /// Check if entry matches filter
    fn matches_filter(&self, entry: &HistoryEntry, filter: &HistoryFilter) -> bool {
        // Pattern matching
        if let Some(ref pattern) = filter.pattern {
            if !entry
                .command
                .to_lowercase()
                .contains(&pattern.to_lowercase())
            {
                return false;
            }
        }

        // Category matching
        if let Some(ref category) = filter.category {
            if entry.category != *category {
                return false;
            }
        }

        // Success filter
        if filter.success_only {
            if let Some(success) = entry.success {
                if !success {
                    return false;
                }
            } else {
                return false; // No success info available
            }
        }

        // Time range filter
        if let Some(since) = filter.since {
            if entry.timestamp < since {
                return false;
            }
        }

        true
    }

    /// Navigate to previous command
    pub fn previous(&mut self) -> Option<String> {
        if self.history.is_empty() {
            return None;
        }

        let new_position = match self.current_position {
            None => self.history.len() - 1,
            Some(pos) => {
                if pos > 0 {
                    pos - 1
                } else {
                    return None; // Already at oldest
                }
            }
        };

        self.current_position = Some(new_position);
        Some(self.history[new_position].command.clone())
    }

    /// Navigate to next command
    pub fn next(&mut self) -> Option<String> {
        if let Some(pos) = self.current_position {
            if pos < self.history.len() - 1 {
                self.current_position = Some(pos + 1);
                Some(self.history[pos + 1].command.clone())
            } else {
                self.current_position = None;
                None // Return to current input
            }
        } else {
            None
        }
    }

    /// Reset navigation position
    pub fn reset_position(&mut self) {
        self.current_position = None;
    }

    /// Get history statistics
    pub fn stats(&self) -> &HistoryStats {
        &self.stats
    }

    /// Update statistics
    fn update_stats(&mut self, entry: &HistoryEntry) {
        self.stats.total_commands += 1;

        if let Some(success) = entry.success {
            if success {
                self.stats.successful_commands += 1;
            } else {
                self.stats.failed_commands += 1;
            }
        }

        // Update category stats
        *self
            .stats
            .by_category
            .entry(entry.category.clone())
            .or_insert(0) += 1;

        // Update average duration
        if let Some(duration) = entry.duration {
            let duration_ms = duration.as_millis() as f64;
            let total_duration =
                self.stats.avg_duration_ms * (self.stats.total_commands - 1) as f64;
            self.stats.avg_duration_ms =
                (total_duration + duration_ms) / self.stats.total_commands as f64;
        }
    }

    /// Load history from file
    fn load_history(&mut self) -> ReplResult<()> {
        if let Some(ref path) = self.history_file {
            if path.exists() {
                let content = fs::read_to_string(path).map_err(ReplError::Io)?;

                for line in content.lines() {
                    if !line.trim().is_empty() {
                        // Parse history entry (simple format for now)
                        if let Some(command) = self.parse_history_line(line) {
                            let entry = HistoryEntry {
                                command,
                                timestamp: std::time::SystemTime::now(),
                                duration: None,
                                success: None,
                                category: self.categorize_command(&line),
                            };

                            if self.history.len() >= self.max_size {
                                self.history.pop_front();
                            }
                            self.history.push_back(entry);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Parse a history line (simple format)
    fn parse_history_line(&self, line: &str) -> Option<String> {
        // For now, just return the line as-is
        // In a more sophisticated implementation, this would parse
        // timestamp and other metadata
        if line.trim().is_empty() {
            None
        } else {
            Some(line.trim().to_string())
        }
    }

    /// Persist a single entry
    fn persist_entry(&self, entry: &HistoryEntry) -> ReplResult<()> {
        if let Some(ref path) = self.history_file {
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(ReplError::Io)?;

            // Simple format: just the command for now
            writeln!(file, "{}", entry.command).map_err(ReplError::Io)?;
        }

        Ok(())
    }

    /// Save all history to file
    pub fn save_history(&self) -> ReplResult<()> {
        if let Some(ref path) = self.history_file {
            let mut file = fs::File::create(path).map_err(ReplError::Io)?;

            for entry in &self.history {
                writeln!(file, "{}", entry.command).map_err(ReplError::Io)?;
            }
        }

        Ok(())
    }

    /// Clear all history
    pub fn clear(&mut self) -> ReplResult<()> {
        self.history.clear();
        self.current_position = None;
        self.stats = HistoryStats::default();

        // Clear file if persistent
        if let Some(ref path) = self.history_file {
            if path.exists() {
                fs::remove_file(path).map_err(ReplError::Io)?;
            }
        }

        Ok(())
    }

    /// Get history size
    pub fn len(&self) -> usize {
        self.history.len()
    }

    /// Check if history is empty
    pub fn is_empty(&self) -> bool {
        self.history.is_empty()
    }

    /// Export history as text
    pub fn export_text(&self) -> String {
        let mut output = String::new();

        output.push_str(&format!(
            "# CQLite Command History ({})\n",
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
        ));
        output.push_str(&format!("# Total commands: {}\n", self.history.len()));
        output.push_str("# Format: command\n\n");

        for entry in &self.history {
            output.push_str(&entry.command);
            output.push('\n');
        }

        output
    }

    /// Export history with metadata
    pub fn export_detailed(&self) -> String {
        let mut output = String::new();

        output.push_str(&format!(
            "# CQLite Detailed Command History ({})\n",
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
        ));
        output.push_str(&format!(
            "# Total commands: {}\n",
            self.stats.total_commands
        ));
        output.push_str(&format!(
            "# Successful: {}\n",
            self.stats.successful_commands
        ));
        output.push_str(&format!("# Failed: {}\n", self.stats.failed_commands));
        output.push_str(&format!(
            "# Average duration: {:.2}ms\n",
            self.stats.avg_duration_ms
        ));
        output.push_str("# Format: timestamp | category | duration | success | command\n\n");

        for entry in &self.history {
            let timestamp = entry
                .timestamp
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let duration_str = entry
                .duration
                .map(|d| format!("{:.2}ms", d.as_millis()))
                .unwrap_or_else(|| "N/A".to_string());

            let success_str = entry
                .success
                .map(|s| if s { "OK" } else { "ERR" })
                .unwrap_or("N/A");

            output.push_str(&format!(
                "{} | {} | {} | {} | {}\n",
                timestamp, entry.category, duration_str, success_str, entry.command
            ));
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_basic_history() {
        let mut history = HistoryManager::new(10).unwrap();

        history.add_command("SELECT * FROM users").unwrap();
        history.add_command("SELECT count(*) FROM orders").unwrap();

        assert_eq!(history.len(), 2);

        let recent = history.recent_commands(5);
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0], "SELECT count(*) FROM orders");
        assert_eq!(recent[1], "SELECT * FROM users");
    }

    #[test]
    fn test_navigation() {
        let mut history = HistoryManager::new(10).unwrap();

        history.add_command("command1").unwrap();
        history.add_command("command2").unwrap();
        history.add_command("command3").unwrap();

        // Navigate backwards
        assert_eq!(history.previous(), Some("command3".to_string()));
        assert_eq!(history.previous(), Some("command2".to_string()));
        assert_eq!(history.previous(), Some("command1".to_string()));
        assert_eq!(history.previous(), None); // At beginning

        // Navigate forwards
        assert_eq!(history.next(), Some("command2".to_string()));
        assert_eq!(history.next(), Some("command3".to_string()));
        assert_eq!(history.next(), None); // Back to current
    }

    #[test]
    fn test_search_filter() {
        let mut history = HistoryManager::new(10).unwrap();

        history.add_command("SELECT * FROM users").unwrap();
        history.add_command(":tables").unwrap();
        history.add_command("SELECT * FROM orders").unwrap();

        let filter = HistoryFilter {
            pattern: Some("SELECT".to_string()),
            ..Default::default()
        };

        let results = history.search(&filter);
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.command.contains("SELECT")));
    }

    #[test]
    fn test_categorization() {
        let history = HistoryManager::new(10).unwrap();

        assert_eq!(
            history.categorize_command("SELECT * FROM users"),
            HistoryCategory::Query
        );
        assert_eq!(history.categorize_command(":help"), HistoryCategory::Meta);
        assert_eq!(
            history.categorize_command(":config show"),
            HistoryCategory::Config
        );
        assert_eq!(
            history.categorize_command(":tables"),
            HistoryCategory::Navigation
        );
        assert_eq!(
            history.categorize_command(":clear"),
            HistoryCategory::System
        );
    }

    #[test]
    fn test_persistent_history() {
        let temp_dir = tempdir().unwrap();
        let temp_path = temp_dir.path();

        {
            let mut history = HistoryManager::new_persistent(10, temp_path).unwrap();
            history.add_command("SELECT 1").unwrap();
            history.add_command("SELECT 2").unwrap();
        }

        // Load in new instance
        let history = HistoryManager::new_persistent(10, temp_path).unwrap();
        assert_eq!(history.len(), 2);

        let recent = history.recent_commands(5);
        assert!(recent.contains(&"SELECT 1".to_string()));
        assert!(recent.contains(&"SELECT 2".to_string()));
    }
}
