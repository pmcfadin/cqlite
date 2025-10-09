// Core REPL Engine Implementation
//
// The main REPL engine that coordinates command parsing, execution, and output formatting.
// This is the central component that integrates with the existing CLI infrastructure.

use super::{
    commands, CommandParser, CommandType, CompletionEngine, ExecutionResult, HistoryManager,
    OutputFormat, ParsedCommand, ReplError, ReplMode, ReplResult, ReplSession,
};
use crate::config::Config;
use colored::Colorize;
use cqlite_core::{Database, QueryResult};
use std::io::{self, Write};
use std::path::Path;

/// Core REPL engine configuration
#[derive(Debug, Clone)]
pub struct ReplConfig {
    /// REPL mode (basic, tui, interactive)
    pub mode: ReplMode,
    /// Enable command history
    pub enable_history: bool,
    /// Enable command completion
    pub enable_completion: bool,
    /// Enable colored output
    pub enable_colors: bool,
    /// Default output format
    pub output_format: OutputFormat,
    /// Maximum history size
    pub max_history_size: usize,
    /// Page size for results
    pub page_size: usize,
    /// Enable timing display
    pub show_timing: bool,
    /// Enable paging for large results
    pub enable_paging: bool,
    /// Prompt customization
    pub prompt: String,
    /// Secondary prompt for multi-line commands
    pub prompt_continuation: String,
}

impl Default for ReplConfig {
    fn default() -> Self {
        Self {
            mode: ReplMode::Interactive,
            enable_history: true,
            enable_completion: true,
            enable_colors: true,
            output_format: OutputFormat::Table,
            max_history_size: 1000,
            page_size: 50,
            show_timing: false,
            enable_paging: true,
            prompt: "cqlite> ".to_string(),
            prompt_continuation: "    -> ".to_string(),
        }
    }
}

/// Main REPL engine
pub struct ReplEngine {
    /// REPL configuration
    config: ReplConfig,
    /// Command parser
    parser: CommandParser,
    /// Session state
    session: ReplSession,
    /// Command history manager
    history: Option<HistoryManager>,
    /// Completion engine
    completion: Option<CompletionEngine>,
    /// Current multi-line command buffer
    command_buffer: String,
    /// Whether we're in multi-line mode
    in_multiline: bool,
}

impl ReplEngine {
    /// Create a new REPL engine
    pub fn new(
        config: ReplConfig,
        db_path: &Path,
        app_config: Config,
        database: Database,
    ) -> ReplResult<Self> {
        let session = ReplSession::new(db_path, app_config, database)?;
        let parser = CommandParser::new();

        let history = if config.enable_history {
            Some(HistoryManager::new(config.max_history_size)?)
        } else {
            None
        };

        let completion = if config.enable_completion {
            Some(CompletionEngine::new())
        } else {
            None
        };

        Ok(Self {
            config,
            parser,
            session,
            history,
            completion,
            command_buffer: String::new(),
            in_multiline: false,
        })
    }

    /// Start the REPL loop
    pub async fn run(&mut self) -> ReplResult<()> {
        self.display_startup_banner().await?;

        match self.config.mode {
            ReplMode::Basic => self.run_basic_repl().await,
            ReplMode::Interactive => self.run_interactive_repl().await,
            ReplMode::Tui => self.run_tui_repl().await,
        }
    }

    /// Run basic REPL mode
    async fn run_basic_repl(&mut self) -> ReplResult<()> {
        let stdin = io::stdin();
        let mut input = String::new();

        loop {
            // Display prompt
            self.display_prompt()?;

            // Read input
            input.clear();
            match stdin.read_line(&mut input) {
                Ok(0) => break, // EOF
                Ok(_) => {
                    let trimmed = input.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    match self.process_input(trimmed).await? {
                        ExecutionResult::Continue => continue,
                        ExecutionResult::Exit => break,
                        ExecutionResult::ExitWithCode(_) => break,
                    }
                }
                Err(e) => {
                    eprintln!("{} Input error: {}", "Error:".red().bold(), e);
                    break;
                }
            }
        }

        self.display_goodbye().await?;
        Ok(())
    }

    /// Run interactive REPL mode (with enhanced features)
    async fn run_interactive_repl(&mut self) -> ReplResult<()> {
        // For now, fall back to basic REPL
        // In the future, this would integrate with rustyline or similar
        // for advanced line editing, history, and completion
        self.run_basic_repl().await
    }

    /// Run TUI REPL mode
    async fn run_tui_repl(&mut self) -> ReplResult<()> {
        // Placeholder for TUI integration
        // This would integrate with the existing tui.rs module
        println!(
            "{} TUI mode not yet implemented in core engine",
            "Info:".cyan().bold()
        );
        self.run_interactive_repl().await
    }

    /// Process a line of input
    pub fn process_input<'a>(
        &'a mut self,
        input: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ReplResult<ExecutionResult>> + 'a>>
    {
        Box::pin(async move { self.process_input_impl(input).await })
    }

    /// Internal implementation of process_input
    async fn process_input_impl(&mut self, input: &str) -> ReplResult<ExecutionResult> {
        // Handle multi-line commands
        if self.should_continue_multiline(input) {
            self.add_to_command_buffer(input);
            return Ok(ExecutionResult::Continue);
        }

        // Complete command (either single line or end of multi-line)
        let command = if self.in_multiline {
            self.add_to_command_buffer(input);
            let full_command = self.command_buffer.clone();
            self.reset_command_buffer();
            full_command
        } else {
            input.to_string()
        };

        // Add to history
        if let Some(ref mut history) = self.history {
            history.add_command(&command)?;
        }

        // Parse and execute command
        match self.parser.parse(&command) {
            Ok(parsed_command) => self.execute_command(parsed_command).await,
            Err(e) => {
                eprintln!("{} Command parsing error: {}", "Error:".red().bold(), e);
                Ok(ExecutionResult::Continue)
            }
        }
    }

    /// Execute a parsed command
    async fn execute_command(&mut self, command: ParsedCommand) -> ReplResult<ExecutionResult> {
        match command.command_type {
            CommandType::Exit => Ok(ExecutionResult::Exit),
            CommandType::Help { topic } => {
                self.execute_help_command(topic.as_deref()).await?;
                Ok(ExecutionResult::Continue)
            }
            CommandType::Config { operation } => {
                self.execute_config_command(operation).await?;
                Ok(ExecutionResult::Continue)
            }
            CommandType::Tables => {
                self.execute_tables_command().await?;
                Ok(ExecutionResult::Continue)
            }
            CommandType::Describe { object_name } => {
                self.execute_describe_command(&object_name).await?;
                Ok(ExecutionResult::Continue)
            }
            CommandType::Use { keyspace } => {
                self.execute_use_command(&keyspace).await?;
                Ok(ExecutionResult::Continue)
            }
            CommandType::CqlQuery { query } => {
                self.execute_cql_query(&query).await?;
                Ok(ExecutionResult::Continue)
            }
            CommandType::Clear => {
                self.execute_clear_command().await?;
                Ok(ExecutionResult::Continue)
            }
            CommandType::History => {
                self.execute_history_command().await?;
                Ok(ExecutionResult::Continue)
            }
            CommandType::Source { file_path } => {
                self.execute_source_command(&file_path).await?;
                Ok(ExecutionResult::Continue)
            }
            CommandType::Status => {
                commands::execute_status(self.session.data_dir()).await?;
                Ok(ExecutionResult::Continue)
            }
            CommandType::Unknown { input } => {
                eprintln!("{} Unknown command: {}", "Error:".red().bold(), input);
                println!("Type {} for help", ":help".green());
                Ok(ExecutionResult::Continue)
            }
        }
    }

    /// Check if input should continue multi-line command
    fn should_continue_multiline(&self, input: &str) -> bool {
        // Continue if we're already in multi-line mode and line doesn't end with semicolon
        if self.in_multiline {
            return !input.trim_end().ends_with(';');
        }

        // Start multi-line mode for certain SQL keywords without semicolon
        let trimmed = input.trim();
        let sql_keywords = [
            "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP",
        ];

        sql_keywords
            .iter()
            .any(|keyword| trimmed.to_uppercase().starts_with(keyword) && !trimmed.ends_with(';'))
    }

    /// Add input to command buffer
    fn add_to_command_buffer(&mut self, input: &str) {
        if !self.in_multiline {
            self.in_multiline = true;
            self.command_buffer.clear();
        }

        if !self.command_buffer.is_empty() {
            self.command_buffer.push(' ');
        }
        self.command_buffer.push_str(input);
    }

    /// Reset command buffer
    fn reset_command_buffer(&mut self) {
        self.command_buffer.clear();
        self.in_multiline = false;
    }

    /// Display REPL prompt
    fn display_prompt(&self) -> ReplResult<()> {
        let prompt = if self.in_multiline {
            self.config.prompt_continuation.clone()
        } else {
            self.format_prompt()
        };

        print!("{}", prompt);
        io::stdout().flush().map_err(ReplError::Io)?;
        Ok(())
    }

    /// Format the main prompt with context
    fn format_prompt(&self) -> String {
        let mut prompt = String::new();

        // Add keyspace if set
        if let Some(ref keyspace) = self.session.current_keyspace() {
            prompt.push_str(&format!("{}@", keyspace.cyan()));
        }

        // Add base prompt
        prompt.push_str("cqlite");

        // Add mode indicator for non-basic modes
        match self.config.mode {
            ReplMode::Tui => prompt.push_str("[tui]"),
            ReplMode::Interactive => prompt.push_str("[i]"),
            ReplMode::Basic => {}
        }

        prompt.push_str(&"> ".blue().bold().to_string());
        prompt
    }

    /// Display startup banner
    async fn display_startup_banner(&self) -> ReplResult<()> {
        if !self.config.enable_colors {
            println!("CQLite Interactive Shell");
            println!("Type :help for help, :quit to exit");
            return Ok(());
        }

        println!(
            "{}",
            "╔═══════════════════════════════════════════════╗".cyan()
        );
        println!(
            "{}",
            "║           CQLite REPL Engine v2.0            ║"
                .cyan()
                .bold()
        );
        println!(
            "{}",
            "║      High-Performance Cassandra Reader       ║".cyan()
        );
        println!(
            "{}",
            "╚═══════════════════════════════════════════════╝".cyan()
        );
        println!();

        println!(
            "🗄️  Database: {}",
            self.session.db_path().display().to_string().yellow()
        );
        println!("🔧 Mode: {}", format!("{:?}", self.config.mode).green());
        println!("📊 Engine: {}", "CQLite Core v0.1.0".green());

        if let Some(ref keyspace) = self.session.current_keyspace() {
            println!("📦 Keyspace: {}", keyspace.yellow());
        }

        println!();
        println!("{}", "Quick Commands:".cyan().bold());
        println!("  • {} - Show help", ":help".green());
        println!("  • {} - List tables", ":tables".green());
        println!("  • {} - Execute CQL", "SELECT * FROM table;".yellow());
        println!("  • {} - Exit", ":quit".red());
        println!();

        Ok(())
    }

    /// Display goodbye message
    async fn display_goodbye(&self) -> ReplResult<()> {
        if self.config.enable_colors {
            println!();
            println!("{}", "Goodbye! Thank you for using CQLite.".cyan().bold());
        } else {
            println!("Goodbye!");
        }
        Ok(())
    }

    /// Execute help command
    async fn execute_help_command(&self, topic: Option<&str>) -> ReplResult<()> {
        match topic {
            Some("commands") => self.show_commands_help(),
            Some("config") => self.show_config_help(),
            Some("cql") => self.show_cql_help(),
            Some("examples") => self.show_examples_help(),
            None => self.show_general_help(),
            Some(unknown) => {
                println!("{} Unknown help topic: {}", "Error:".red().bold(), unknown);
                println!("Available topics: commands, config, cql, examples");
            }
        }
        Ok(())
    }

    /// Execute config command
    async fn execute_config_command(&mut self, operation: String) -> ReplResult<()> {
        // Parse config operation (show, set key=value, etc.)
        if operation.is_empty() || operation == "show" {
            self.show_current_config();
        } else if operation.contains('=') {
            // Set configuration
            let parts: Vec<&str> = operation.splitn(2, '=').collect();
            if parts.len() == 2 {
                self.set_config_value(parts[0].trim(), parts[1].trim())?;
            } else {
                println!(
                    "{} Invalid config format. Use: :config key=value",
                    "Error:".red().bold()
                );
            }
        } else {
            println!(
                "{} Unknown config operation: {}",
                "Error:".red().bold(),
                operation
            );
            println!("Usage: :config [show] or :config key=value");
        }
        Ok(())
    }

    /// Execute tables command
    async fn execute_tables_command(&mut self) -> ReplResult<()> {
        println!("{}", "📋 Listing tables...".cyan().bold());

        match self.session.list_tables().await {
            Ok(tables) => {
                if tables.is_empty() {
                    println!("📭 No tables found");
                    println!("💡 Use CREATE TABLE or configure data directory");
                } else {
                    for table in tables {
                        println!("  📄 {}", table.green());
                    }
                }
            }
            Err(e) => {
                eprintln!("{} Failed to list tables: {}", "Error:".red().bold(), e);
            }
        }

        Ok(())
    }

    /// Execute describe command
    async fn execute_describe_command(&mut self, object_name: &str) -> ReplResult<()> {
        println!(
            "{} {}",
            "🔍 Describing:".cyan().bold(),
            object_name.yellow()
        );

        match self.session.describe_object(object_name).await {
            Ok(description) => {
                println!("{}", description);
            }
            Err(e) => {
                eprintln!(
                    "{} Failed to describe {}: {}",
                    "Error:".red().bold(),
                    object_name,
                    e
                );
                println!("💡 Try :tables to list available objects");
            }
        }

        Ok(())
    }

    /// Execute use command
    async fn execute_use_command(&mut self, keyspace: &str) -> ReplResult<()> {
        match self.session.use_keyspace(keyspace).await {
            Ok(()) => {
                println!(
                    "{} Now using keyspace: {}",
                    "✅".green(),
                    keyspace.yellow().bold()
                );
            }
            Err(e) => {
                eprintln!(
                    "{} Failed to use keyspace {}: {}",
                    "Error:".red().bold(),
                    keyspace,
                    e
                );
            }
        }
        Ok(())
    }

    /// Execute CQL query
    async fn execute_cql_query(&mut self, query: &str) -> ReplResult<()> {
        let start_time = std::time::Instant::now();

        println!("{} {}", "🔍 Executing:".blue().bold(), query.yellow());

        match self.session.execute_query(query).await {
            Ok(result) => {
                let elapsed = start_time.elapsed();
                self.display_query_result(&result)?;

                if self.config.show_timing {
                    println!();
                    println!(
                        "{} {:.2}ms",
                        "⏱️  Execution time:".green(),
                        elapsed.as_millis()
                    );
                }
            }
            Err(e) => {
                let elapsed = start_time.elapsed();
                eprintln!(
                    "{} Query failed after {:.2}ms",
                    "❌ Error:".red().bold(),
                    elapsed.as_millis()
                );
                eprintln!("  {}", e.to_string().red());
                self.provide_query_hints(query, &e);
            }
        }

        Ok(())
    }

    /// Execute clear command
    async fn execute_clear_command(&self) -> ReplResult<()> {
        print!("\\x1B[2J\\x1B[1;1H");
        io::stdout().flush().map_err(ReplError::Io)?;
        Ok(())
    }

    /// Execute history command
    async fn execute_history_command(&self) -> ReplResult<()> {
        if let Some(ref history) = self.history {
            println!("{}", "📜 Command History".cyan().bold());
            println!("{}", "═".repeat(20).cyan());

            let commands = history.recent_commands(20);
            if commands.is_empty() {
                println!("📭 No commands in history");
            } else {
                for (i, cmd) in commands.iter().enumerate() {
                    println!("  {:3}. {}", i + 1, cmd);
                }
            }
        } else {
            println!("{} History is disabled", "Info:".cyan().bold());
        }
        Ok(())
    }

    /// Execute source command
    async fn execute_source_command(&mut self, file_path: &str) -> ReplResult<()> {
        println!(
            "{} Executing commands from: {}",
            "📂".cyan(),
            file_path.yellow()
        );

        let path = std::path::Path::new(file_path);
        if !path.exists() {
            eprintln!("{} File not found: {}", "Error:".red().bold(), file_path);
            return Ok(());
        }

        let content = std::fs::read_to_string(path).map_err(|e| ReplError::Io(e))?;

        let mut executed = 0;
        let errors = 0;

        for (line_num, line) in content.lines().enumerate() {
            let trimmed = line.trim();

            // Skip empty lines and comments
            if trimmed.is_empty() || trimmed.starts_with("--") || trimmed.starts_with("#") {
                continue;
            }

            println!(
                "{}:{} {}",
                file_path,
                line_num + 1,
                trimmed.to_string().dimmed()
            );

            match self.process_input(trimmed).await? {
                ExecutionResult::Continue => executed += 1,
                ExecutionResult::Exit => {
                    println!("🛑 Execution stopped due to exit command");
                    break;
                }
                ExecutionResult::ExitWithCode(_) => {
                    println!("🛑 Execution stopped");
                    break;
                }
            }
        }

        println!();
        println!(
            "📊 File execution completed: {} commands executed, {} errors",
            executed, errors
        );
        Ok(())
    }

    /// Display query result
    fn display_query_result(&self, result: &QueryResult) -> ReplResult<()> {
        match self.config.output_format {
            OutputFormat::Table => self.display_table_result(result),
            OutputFormat::Csv => self.display_csv_result(result),
            OutputFormat::Json => self.display_json_result(result),
            OutputFormat::Raw => self.display_raw_result(result),
        }
    }

    /// Display result in table format
    fn display_table_result(&self, result: &QueryResult) -> ReplResult<()> {
        if result.rows.is_empty() {
            if result.rows_affected > 0 {
                println!(
                    "{} {} rows affected",
                    "✅".green().bold(),
                    result.rows_affected
                );
            } else {
                println!("{} No rows returned", "📭".yellow());
            }
            return Ok(());
        }

        println!();
        println!(
            "{} {} rows returned",
            "📊 Results:".green().bold(),
            result.rows.len()
        );

        // For now, delegate to the existing display logic
        // In a full implementation, this would be integrated with the REPL's table formatter
        println!("(Table formatting would be implemented here)");

        Ok(())
    }

    /// Display result in CSV format
    fn display_csv_result(&self, _result: &QueryResult) -> ReplResult<()> {
        println!("CSV output not yet implemented");
        Ok(())
    }

    /// Display result in JSON format
    fn display_json_result(&self, _result: &QueryResult) -> ReplResult<()> {
        println!("JSON output not yet implemented");
        Ok(())
    }

    /// Display result in raw format
    fn display_raw_result(&self, _result: &QueryResult) -> ReplResult<()> {
        println!("Raw output not yet implemented");
        Ok(())
    }

    /// Provide helpful hints for query errors
    fn provide_query_hints(&self, _query: &str, error: &ReplError) {
        let error_msg = error.to_string();

        println!();
        if error_msg.contains("table") && error_msg.contains("not found") {
            println!("{} Table not found. Try:", "💡 Hint:".cyan().bold());
            println!("  • {} to list tables", ":tables".green());
            println!("  • Check table name spelling");
        } else if error_msg.contains("syntax") {
            println!("{} Syntax error. Try:", "💡 Hint:".cyan().bold());
            println!("  • {} for CQL help", ":help cql".green());
            println!("  • Check query syntax");
        } else {
            println!(
                "{} For general help: {}",
                "💡 Hint:".cyan().bold(),
                ":help".green()
            );
        }
    }

    /// Show general help
    fn show_general_help(&self) {
        println!("{}", "CQLite REPL Help".cyan().bold());
        println!("{}", "═".repeat(20).cyan());
        println!();
        println!("Commands:");
        println!("  :help [topic]    Show help (topics: commands, config, cql, examples)");
        println!("  :quit, :exit     Exit the REPL");
        println!("  :tables          List all tables");
        println!("  :describe <obj>  Describe object");
        println!("  :use <keyspace>  Switch keyspace");
        println!("  :config [op]     Show/set configuration");
        println!("  :status          Show discovery and schema coverage status");
        println!("  :clear           Clear screen");
        println!("  :history         Show command history");
        println!("  :source <file>   Execute commands from file");
        println!();
        println!("CQL queries can be executed directly (end with semicolon for multi-line)");
    }

    /// Show commands help
    fn show_commands_help(&self) {
        println!("{}", "Available Commands".cyan().bold());
        println!("{}", "═".repeat(20).cyan());
        println!();
        println!("Meta Commands:");
        println!("  :help            Show this help");
        println!("  :quit, :exit     Exit REPL");
        println!("  :clear           Clear screen");
        println!("  :history         Show recent commands");
        println!();
        println!("Database Commands:");
        println!("  :tables          List all tables");
        println!("  :describe <obj>  Show object schema");
        println!("  :use <keyspace>  Switch to keyspace");
        println!("  :status          Show discovery and schema coverage status");
        println!();
        println!("File Commands:");
        println!("  :source <file>   Execute SQL file");
        println!();
        println!("Configuration:");
        println!("  :config          Show current settings");
        println!("  :config key=val  Set configuration");
    }

    /// Show config help
    fn show_config_help(&self) {
        println!("{}", "Configuration Help".cyan().bold());
        println!("{}", "═".repeat(20).cyan());
        println!();
        println!("Available settings:");
        println!("  output_format    Output format (table, csv, json, raw)");
        println!("  page_size        Result page size");
        println!("  show_timing      Show query timing (true/false)");
        println!("  enable_paging    Enable result paging (true/false)");
        println!();
        println!("Usage:");
        println!("  :config                    Show all settings");
        println!("  :config output_format=json Set output format");
        println!("  :config show_timing=true   Enable timing");
    }

    /// Show CQL help
    fn show_cql_help(&self) {
        println!("{}", "CQL Query Help".cyan().bold());
        println!("{}", "═".repeat(20).cyan());
        println!();
        println!("Supported CQL:");
        println!("  SELECT * FROM table;");
        println!("  SELECT col1, col2 FROM table WHERE condition;");
        println!("  DESCRIBE TABLE table_name;");
        println!();
        println!("Multi-line queries:");
        println!("  Start typing a query and press Enter");
        println!("  Continue on next lines");
        println!("  End with semicolon (;) to execute");
    }

    /// Show examples help
    fn show_examples_help(&self) {
        println!("{}", "Usage Examples".cyan().bold());
        println!("{}", "═".repeat(20).cyan());
        println!();
        println!("Basic workflow:");
        println!("  :tables");
        println!("  :describe users");
        println!("  SELECT * FROM users LIMIT 5;");
        println!();
        println!("Configuration:");
        println!("  :config output_format=json");
        println!("  :config show_timing=true");
        println!();
        println!("File execution:");
        println!("  :source /path/to/queries.sql");
    }

    /// Show current configuration
    fn show_current_config(&self) {
        println!("{}", "Current Configuration".cyan().bold());
        println!("{}", "═".repeat(25).cyan());
        println!();
        println!("REPL Settings:");
        println!("  Mode: {:?}", self.config.mode);
        println!("  Output Format: {:?}", self.config.output_format);
        println!("  Page Size: {}", self.config.page_size);
        println!("  Show Timing: {}", self.config.show_timing);
        println!("  Enable Paging: {}", self.config.enable_paging);
        println!("  Enable Colors: {}", self.config.enable_colors);
        println!(
            "  History: {}",
            if self.history.is_some() {
                "enabled"
            } else {
                "disabled"
            }
        );
        println!(
            "  Completion: {}",
            if self.completion.is_some() {
                "enabled"
            } else {
                "disabled"
            }
        );
    }

    /// Set configuration value
    fn set_config_value(&mut self, key: &str, value: &str) -> ReplResult<()> {
        match key {
            "output_format" => {
                self.config.output_format = match value.to_lowercase().as_str() {
                    "table" => OutputFormat::Table,
                    "csv" => OutputFormat::Csv,
                    "json" => OutputFormat::Json,
                    "raw" => OutputFormat::Raw,
                    _ => {
                        println!(
                            "{} Invalid output format. Use: table, csv, json, raw",
                            "Error:".red().bold()
                        );
                        return Ok(());
                    }
                };
                println!(
                    "{} Output format set to: {:?}",
                    "✅".green(),
                    self.config.output_format
                );
            }
            "page_size" => match value.parse::<usize>() {
                Ok(size) if size > 0 => {
                    self.config.page_size = size;
                    println!("{} Page size set to: {}", "✅".green(), size);
                }
                _ => {
                    println!(
                        "{} Invalid page size. Must be positive number",
                        "Error:".red().bold()
                    );
                }
            },
            "show_timing" => match value.to_lowercase().as_str() {
                "true" | "on" | "1" | "yes" => {
                    self.config.show_timing = true;
                    println!("{} Timing enabled", "✅".green());
                }
                "false" | "off" | "0" | "no" => {
                    self.config.show_timing = false;
                    println!("{} Timing disabled", "✅".green());
                }
                _ => {
                    println!(
                        "{} Invalid boolean value. Use: true/false",
                        "Error:".red().bold()
                    );
                }
            },
            "enable_paging" => match value.to_lowercase().as_str() {
                "true" | "on" | "1" | "yes" => {
                    self.config.enable_paging = true;
                    println!("{} Paging enabled", "✅".green());
                }
                "false" | "off" | "0" | "no" => {
                    self.config.enable_paging = false;
                    println!("{} Paging disabled", "✅".green());
                }
                _ => {
                    println!(
                        "{} Invalid boolean value. Use: true/false",
                        "Error:".red().bold()
                    );
                }
            },
            _ => {
                println!(
                    "{} Unknown configuration key: {}",
                    "Error:".red().bold(),
                    key
                );
                println!("Available keys: output_format, page_size, show_timing, enable_paging");
            }
        }

        Ok(())
    }

    /// Get reference to session
    pub fn session(&self) -> &ReplSession {
        &self.session
    }

    /// Get mutable reference to session
    pub fn session_mut(&mut self) -> &mut ReplSession {
        &mut self.session
    }

    /// Get current configuration
    pub fn config(&self) -> &ReplConfig {
        &self.config
    }
}
