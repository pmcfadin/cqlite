// Command Parser for REPL Engine
//
// Parses user input and converts it into structured commands that can be executed
// by the REPL engine. Handles both meta-commands (:help, :quit) and CQL queries.

use super::{ReplError, ReplResult};
#[allow(unused_imports)]
use std::fmt;

/// Types of commands that can be executed in the REPL
#[derive(Debug, Clone, PartialEq)]
pub enum CommandType {
    /// Exit the REPL
    Exit,
    /// Show help information
    Help { topic: Option<String> },
    /// Configuration operations
    Config { operation: String },
    /// List tables
    Tables,
    /// Describe an object (table, keyspace, etc.)
    Describe { object_name: String },
    /// Switch to a keyspace
    Use { keyspace: String },
    /// Execute a CQL query
    CqlQuery { query: String },
    /// Clear the screen
    Clear,
    /// Show command history
    History,
    /// Execute commands from a file
    Source { file_path: String },
    /// Show discovery and schema coverage status
    Status,
    /// List keyspaces
    Keyspaces,
    /// Schema management commands
    Schema { operation: SchemaOperation },
    /// Show health diagnostics
    Health,
    /// Unknown command
    Unknown { input: String },
    /// Flush memtable to SSTable (Issue #392)
    Flush,
    /// Show write engine statistics (Issue #392)
    WriteStats,
    /// Run maintenance/compaction (Issue #392)
    Maintenance { budget_ms: Option<u64> },
}

/// Schema operation types
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaOperation {
    /// Load schema from file(s)
    Load { paths: Vec<String> },
    /// Refresh currently loaded schemas
    Refresh,
    /// Unload all schemas
    Unload,
    /// Show schema status
    Show,
    /// List loaded schemas with coverage info
    List,
}

/// A parsed command with metadata
#[derive(Debug, Clone)]
pub struct ParsedCommand {
    /// The type of command
    pub command_type: CommandType,
    /// Original input text
    pub original_input: String,
    /// Whether this is a multi-line command
    pub is_multiline: bool,
    /// Command metadata
    pub metadata: CommandMetadata,
}

/// Additional metadata about the command
#[derive(Debug, Clone, Default)]
pub struct CommandMetadata {
    /// Estimated execution complexity (0-10)
    pub complexity: u8,
    /// Whether this command modifies state
    pub modifies_state: bool,
    /// Whether this command requires database access
    pub requires_database: bool,
    /// Command category
    pub category: CommandCategory,
}

/// Categories of commands
#[derive(Debug, Clone, PartialEq, Default)]
pub enum CommandCategory {
    #[default]
    Unknown,
    Meta,       // :help, :quit, etc.
    Query,      // SELECT, INSERT, etc.
    Schema,     // DESCRIBE, CREATE TABLE, etc.
    Navigation, // USE, :tables, etc.
    System,     // :clear, :history, etc.
}

impl fmt::Display for CommandCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandCategory::Unknown => write!(f, "unknown"),
            CommandCategory::Meta => write!(f, "meta"),
            CommandCategory::Query => write!(f, "query"),
            CommandCategory::Schema => write!(f, "schema"),
            CommandCategory::Navigation => write!(f, "navigation"),
            CommandCategory::System => write!(f, "system"),
        }
    }
}

/// Command parser
pub struct CommandParser {
    /// Enable strict parsing (reject ambiguous commands)
    strict_mode: bool,
}

impl CommandParser {
    /// Create a new command parser
    pub fn new() -> Self {
        Self { strict_mode: false }
    }

    /// Create a new command parser in strict mode
    pub fn new_strict() -> Self {
        Self { strict_mode: true }
    }

    /// Parse a command string into a structured command
    pub fn parse(&self, input: &str) -> ReplResult<ParsedCommand> {
        let trimmed = input.trim();

        if trimmed.is_empty() {
            return Err(ReplError::CommandParsing("Empty command".to_string()));
        }

        let command_type = self.parse_command_type(trimmed)?;
        let metadata = self.generate_metadata(&command_type, trimmed);

        Ok(ParsedCommand {
            command_type,
            original_input: input.to_string(),
            is_multiline: self.is_multiline_command(trimmed),
            metadata,
        })
    }

    /// Parse the command type from input
    fn parse_command_type(&self, input: &str) -> ReplResult<CommandType> {
        // Handle meta-commands (starting with : or .)
        if let Some(meta_cmd) = self.try_parse_meta_command(input) {
            return Ok(meta_cmd);
        }

        // Handle CQL commands
        if let Some(cql_cmd) = self.try_parse_cql_command(input) {
            return Ok(cql_cmd);
        }

        // Unknown command
        Ok(CommandType::Unknown {
            input: input.to_string(),
        })
    }

    /// Try to parse as a meta-command
    fn try_parse_meta_command(&self, input: &str) -> Option<CommandType> {
        if !input.starts_with(':') && !input.starts_with('.') && !input.starts_with('\\') {
            return None;
        }

        // Remove the prefix
        let cmd = if input.starts_with(':') {
            &input[1..]
        } else if input.starts_with('.') {
            &input[1..]
        } else if input.starts_with('\\') {
            &input[1..]
        } else {
            input
        };

        let parts: Vec<&str> = cmd.split_whitespace().collect();
        if parts.is_empty() {
            return None;
        }

        match parts[0].to_lowercase().as_str() {
            // Exit commands
            "quit" | "exit" | "q" => Some(CommandType::Exit),

            // Help commands
            "help" | "h" | "?" => {
                let topic = if parts.len() > 1 {
                    Some(parts[1..].join(" "))
                } else {
                    None
                };
                Some(CommandType::Help { topic })
            }

            // Configuration commands
            "config" | "set" | "show" => {
                let operation = if parts.len() > 1 {
                    parts[1..].join(" ")
                } else {
                    "show".to_string()
                };
                Some(CommandType::Config { operation })
            }

            // Navigation commands
            "tables" | "list" => Some(CommandType::Tables),
            "describe" | "desc" | "d" => {
                if parts.len() > 1 {
                    Some(CommandType::Describe {
                        object_name: parts[1..].join(" "),
                    })
                } else if self.strict_mode {
                    None // Require object name in strict mode
                } else {
                    Some(CommandType::Describe {
                        object_name: "".to_string(),
                    })
                }
            }
            "use" => {
                if parts.len() > 1 {
                    Some(CommandType::Use {
                        keyspace: parts[1].to_string(),
                    })
                } else {
                    None // USE requires a keyspace
                }
            }

            // System commands
            "clear" | "cls" => Some(CommandType::Clear),
            "history" | "hist" => Some(CommandType::History),
            "source" | "load" => {
                if parts.len() > 1 {
                    Some(CommandType::Source {
                        file_path: parts[1..].join(" "),
                    })
                } else {
                    None // SOURCE requires a file path
                }
            }
            "status" => Some(CommandType::Status),
            "keyspaces" => Some(CommandType::Keyspaces),
            "health" => Some(CommandType::Health),

            // Write commands (Issue #392)
            "flush" => Some(CommandType::Flush),
            "write-stats" | "writestats" | "wstats" | "stats" => Some(CommandType::WriteStats),
            "maintenance" | "maint" | "compact" => {
                let budget_ms = if parts.len() > 1 {
                    parts[1].parse().ok()
                } else {
                    None
                };
                Some(CommandType::Maintenance { budget_ms })
            }

            // Schema commands
            "schema" => {
                if parts.len() > 1 {
                    let schema_op = self.parse_schema_operation(&parts[1..])?;
                    Some(CommandType::Schema {
                        operation: schema_op,
                    })
                } else {
                    // Default to show if no operation specified
                    Some(CommandType::Schema {
                        operation: SchemaOperation::Show,
                    })
                }
            }

            _ => None,
        }
    }

    /// Parse schema operation subcommands
    fn parse_schema_operation(&self, parts: &[&str]) -> Option<SchemaOperation> {
        if parts.is_empty() {
            return Some(SchemaOperation::Show);
        }

        match parts[0].to_lowercase().as_str() {
            "load" => {
                if parts.len() > 1 {
                    // Collect all remaining parts as file paths
                    let paths: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();
                    Some(SchemaOperation::Load { paths })
                } else {
                    None // LOAD requires at least one path
                }
            }
            "refresh" => Some(SchemaOperation::Refresh),
            "unload" => Some(SchemaOperation::Unload),
            "show" | "status" => Some(SchemaOperation::Show),
            "list" => Some(SchemaOperation::List),
            _ => None,
        }
    }

    /// Try to parse as a CQL command
    fn try_parse_cql_command(&self, input: &str) -> Option<CommandType> {
        let upper_input = input.to_uppercase();
        let trimmed = upper_input.trim();

        // Check for common CQL keywords
        let cql_keywords = [
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "ALTER",
            "DROP",
            "TRUNCATE",
            "DESCRIBE",
            "USE",
            "GRANT",
            "REVOKE",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "COPY",
            "EXPLAIN",
            "CONSISTENCY",
        ];

        for keyword in &cql_keywords {
            if trimmed.starts_with(keyword) {
                // Ensure it's actually a word boundary
                let rest = &trimmed[keyword.len()..];
                if rest.is_empty() || rest.starts_with(' ') || rest.starts_with('\t') {
                    return Some(CommandType::CqlQuery {
                        query: input.to_string(),
                    });
                }
            }
        }

        // Check for CQLSH-style describe commands
        if trimmed.starts_with("DESC ") {
            return Some(CommandType::CqlQuery {
                query: input.to_string(),
            });
        }

        // If it contains SQL-like patterns, assume it's CQL
        if self.looks_like_sql(input) {
            return Some(CommandType::CqlQuery {
                query: input.to_string(),
            });
        }

        None
    }

    /// Check if input looks like SQL/CQL
    fn looks_like_sql(&self, input: &str) -> bool {
        let upper = input.to_uppercase();

        // Contains SQL-like keywords
        let sql_patterns = [
            " FROM ",
            " WHERE ",
            " GROUP BY ",
            " ORDER BY ",
            " HAVING ",
            " LIMIT ",
            " OFFSET ",
            " JOIN ",
            " INNER ",
            " LEFT ",
            " RIGHT ",
            " FULL ",
            " ON ",
            " AS ",
            " INTO ",
            " VALUES ",
            " SET ",
            " AND ",
            " OR ",
            " NOT ",
        ];

        sql_patterns.iter().any(|pattern| upper.contains(pattern)) ||
        // Contains parentheses (function calls, subqueries)
        (input.contains('(') && input.contains(')')) ||
        // Contains semicolon (statement terminator)
        input.contains(';') ||
        // Contains common operators
        input.contains('=') || input.contains('<') || input.contains('>') ||
        // Contains string literals
        (input.contains('\'') && input.matches('\'').count() >= 2)
    }

    /// Check if this is a multi-line command
    fn is_multiline_command(&self, input: &str) -> bool {
        input.contains('\n') || input.contains('\r')
    }

    /// Generate metadata for a command
    fn generate_metadata(&self, command_type: &CommandType, _input: &str) -> CommandMetadata {
        let mut metadata = CommandMetadata::default();

        match command_type {
            CommandType::Exit => {
                metadata.category = CommandCategory::Meta;
                metadata.complexity = 1;
                metadata.modifies_state = false;
                metadata.requires_database = false;
            }
            CommandType::Help { .. } => {
                metadata.category = CommandCategory::Meta;
                metadata.complexity = 1;
                metadata.modifies_state = false;
                metadata.requires_database = false;
            }
            CommandType::Config { .. } => {
                metadata.category = CommandCategory::Meta;
                metadata.complexity = 2;
                metadata.modifies_state = true;
                metadata.requires_database = false;
            }
            CommandType::Tables => {
                metadata.category = CommandCategory::Navigation;
                metadata.complexity = 3;
                metadata.modifies_state = false;
                metadata.requires_database = true;
            }
            CommandType::Describe { .. } => {
                metadata.category = CommandCategory::Schema;
                metadata.complexity = 4;
                metadata.modifies_state = false;
                metadata.requires_database = true;
            }
            CommandType::Use { .. } => {
                metadata.category = CommandCategory::Navigation;
                metadata.complexity = 2;
                metadata.modifies_state = true;
                metadata.requires_database = true;
            }
            CommandType::CqlQuery { query } => {
                metadata.category = self.categorize_cql_query(query);
                metadata.complexity = self.estimate_query_complexity(query);
                metadata.modifies_state = self.query_modifies_state(query);
                metadata.requires_database = true;
            }
            CommandType::Clear => {
                metadata.category = CommandCategory::System;
                metadata.complexity = 1;
                metadata.modifies_state = false;
                metadata.requires_database = false;
            }
            CommandType::History => {
                metadata.category = CommandCategory::System;
                metadata.complexity = 1;
                metadata.modifies_state = false;
                metadata.requires_database = false;
            }
            CommandType::Source { .. } => {
                metadata.category = CommandCategory::System;
                metadata.complexity = 5;
                metadata.modifies_state = true;
                metadata.requires_database = true;
            }
            CommandType::Status => {
                metadata.category = CommandCategory::System;
                metadata.complexity = 3;
                metadata.modifies_state = false;
                metadata.requires_database = false;
            }
            CommandType::Keyspaces => {
                metadata.category = CommandCategory::Navigation;
                metadata.complexity = 2;
                metadata.modifies_state = false;
                metadata.requires_database = false;
            }
            CommandType::Health => {
                metadata.category = CommandCategory::System;
                metadata.complexity = 2;
                metadata.modifies_state = false;
                metadata.requires_database = false;
            }
            CommandType::Schema { operation } => {
                metadata.category = CommandCategory::Schema;
                match operation {
                    SchemaOperation::Load { .. } => {
                        metadata.complexity = 6;
                        metadata.modifies_state = true;
                        metadata.requires_database = true;
                    }
                    SchemaOperation::Refresh => {
                        metadata.complexity = 5;
                        metadata.modifies_state = true;
                        metadata.requires_database = true;
                    }
                    SchemaOperation::Unload => {
                        metadata.complexity = 4;
                        metadata.modifies_state = true;
                        metadata.requires_database = true;
                    }
                    SchemaOperation::Show => {
                        metadata.complexity = 2;
                        metadata.modifies_state = false;
                        metadata.requires_database = false;
                    }
                    SchemaOperation::List => {
                        metadata.complexity = 2;
                        metadata.modifies_state = false;
                        metadata.requires_database = false;
                    }
                }
            }
            CommandType::Unknown { .. } => {
                metadata.category = CommandCategory::Unknown;
                metadata.complexity = 0;
                metadata.modifies_state = false;
                metadata.requires_database = false;
            }
            // Issue #392: Write commands
            CommandType::Flush => {
                metadata.category = CommandCategory::System;
                metadata.complexity = 5;
                metadata.modifies_state = true;
                metadata.requires_database = true;
            }
            CommandType::WriteStats => {
                metadata.category = CommandCategory::System;
                metadata.complexity = 2;
                metadata.modifies_state = false;
                metadata.requires_database = true;
            }
            CommandType::Maintenance { .. } => {
                metadata.category = CommandCategory::System;
                metadata.complexity = 6;
                metadata.modifies_state = true;
                metadata.requires_database = true;
            }
        }

        metadata
    }

    /// Categorize a CQL query
    fn categorize_cql_query(&self, query: &str) -> CommandCategory {
        let upper = query.to_uppercase();
        let trimmed = upper.trim();

        if trimmed.starts_with("SELECT") || trimmed.starts_with("EXPLAIN") {
            CommandCategory::Query
        } else if trimmed.starts_with("CREATE")
            || trimmed.starts_with("ALTER")
            || trimmed.starts_with("DROP")
            || trimmed.starts_with("DESCRIBE")
        {
            CommandCategory::Schema
        } else if trimmed.starts_with("USE") {
            CommandCategory::Navigation
        } else {
            CommandCategory::Query
        }
    }

    /// Estimate query complexity (0-10 scale)
    fn estimate_query_complexity(&self, query: &str) -> u8 {
        let upper = query.to_uppercase();
        let mut complexity = 1;

        // Basic operations
        if upper.contains("SELECT") {
            complexity += 1;
        }
        if upper.contains("INSERT") || upper.contains("UPDATE") || upper.contains("DELETE") {
            complexity += 2;
        }

        // Joins increase complexity
        if upper.contains(" JOIN ") {
            complexity += 2;
        }
        if upper.contains(" LEFT ") || upper.contains(" RIGHT ") || upper.contains(" FULL ") {
            complexity += 1;
        }

        // Subqueries
        let paren_count = query.matches('(').count();
        if paren_count > 1 {
            complexity += paren_count.min(3) as u8;
        }

        // Aggregations
        if upper.contains("GROUP BY") {
            complexity += 1;
        }
        if upper.contains("ORDER BY") {
            complexity += 1;
        }
        if upper.contains("HAVING") {
            complexity += 1;
        }

        // Complex functions
        if upper.contains("COUNT(")
            || upper.contains("SUM(")
            || upper.contains("AVG(")
            || upper.contains("MAX(")
            || upper.contains("MIN(")
        {
            complexity += 1;
        }

        // DDL operations
        if upper.contains("CREATE TABLE") || upper.contains("ALTER TABLE") {
            complexity += 2;
        }

        complexity.min(10)
    }

    /// Check if query modifies database state
    fn query_modifies_state(&self, query: &str) -> bool {
        let upper = query.to_uppercase();
        let modifying_keywords = [
            "INSERT", "UPDATE", "DELETE", "TRUNCATE", "CREATE", "ALTER", "DROP", "GRANT", "REVOKE",
        ];

        modifying_keywords.iter().any(|keyword| {
            let pattern = format!(" {} ", keyword);
            format!(" {} ", upper).contains(&pattern) || upper.starts_with(keyword)
        })
    }

    /// Validate a parsed command
    pub fn validate(&self, command: &ParsedCommand) -> ReplResult<()> {
        match &command.command_type {
            CommandType::Describe { object_name } => {
                if object_name.is_empty() {
                    return Err(ReplError::CommandParsing(
                        "DESCRIBE command requires an object name".to_string(),
                    ));
                }
            }
            CommandType::Use { keyspace } => {
                if keyspace.is_empty() {
                    return Err(ReplError::CommandParsing(
                        "USE command requires a keyspace name".to_string(),
                    ));
                }
                // Validate keyspace name format
                if !self.is_valid_identifier(keyspace) {
                    return Err(ReplError::CommandParsing(format!(
                        "Invalid keyspace name: {}",
                        keyspace
                    )));
                }
            }
            CommandType::Source { file_path } => {
                if file_path.is_empty() {
                    return Err(ReplError::CommandParsing(
                        "SOURCE command requires a file path".to_string(),
                    ));
                }
            }
            CommandType::CqlQuery { query } => {
                // Basic CQL validation
                if query.trim().is_empty() {
                    return Err(ReplError::CommandParsing("Empty CQL query".to_string()));
                }

                // Check for balanced parentheses
                if !self.has_balanced_parentheses(query) {
                    return Err(ReplError::CommandParsing(
                        "Unbalanced parentheses in query".to_string(),
                    ));
                }

                // Check for balanced quotes
                if !self.has_balanced_quotes(query) {
                    return Err(ReplError::CommandParsing(
                        "Unbalanced quotes in query".to_string(),
                    ));
                }
            }
            _ => {
                // Other commands don't need special validation
            }
        }

        Ok(())
    }

    /// Check if identifier is valid
    fn is_valid_identifier(&self, name: &str) -> bool {
        if name.is_empty() {
            return false;
        }

        // Allow quoted identifiers
        if name.starts_with('"') && name.ends_with('"') && name.len() > 2 {
            return true;
        }

        // Check unquoted identifier rules
        let first_char = name.chars().next().unwrap();
        if !first_char.is_ascii_alphabetic() && first_char != '_' {
            return false;
        }

        name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    /// Check for balanced parentheses
    fn has_balanced_parentheses(&self, text: &str) -> bool {
        let mut depth = 0;
        let mut in_string = false;
        let mut escape_next = false;

        for ch in text.chars() {
            if escape_next {
                escape_next = false;
                continue;
            }

            match ch {
                '\\' if in_string => escape_next = true,
                '\'' => in_string = !in_string,
                '(' if !in_string => depth += 1,
                ')' if !in_string => {
                    depth -= 1;
                    if depth < 0 {
                        return false;
                    }
                }
                _ => {}
            }
        }

        depth == 0
    }

    /// Check for balanced quotes
    fn has_balanced_quotes(&self, text: &str) -> bool {
        let mut single_quote_count = 0;
        let mut escape_next = false;

        for ch in text.chars() {
            if escape_next {
                escape_next = false;
                continue;
            }

            match ch {
                '\\' => escape_next = true,
                '\'' => single_quote_count += 1,
                _ => {}
            }
        }

        single_quote_count % 2 == 0
    }
}

impl Default for CommandParser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_exit_commands() {
        let parser = CommandParser::new();

        let cases = [":quit", ":exit", ":q", ".quit", "\\q"];
        for case in &cases {
            let result = parser.parse(case).unwrap();
            assert_eq!(result.command_type, CommandType::Exit);
        }
    }

    #[test]
    fn test_parse_help_commands() {
        let parser = CommandParser::new();

        let result = parser.parse(":help").unwrap();
        assert_eq!(result.command_type, CommandType::Help { topic: None });

        let result = parser.parse(":help config").unwrap();
        assert_eq!(
            result.command_type,
            CommandType::Help {
                topic: Some("config".to_string())
            }
        );
    }

    #[test]
    fn test_parse_cql_queries() {
        let parser = CommandParser::new();

        let cases = [
            "SELECT * FROM users",
            "INSERT INTO users (id, name) VALUES (1, 'test')",
            "CREATE TABLE test (id int PRIMARY KEY)",
        ];

        for case in &cases {
            let result = parser.parse(case).unwrap();
            if let CommandType::CqlQuery { query } = result.command_type {
                assert_eq!(query, case.to_string());
            } else {
                panic!("Expected CqlQuery, got {:?}", result.command_type);
            }
        }
    }

    #[test]
    fn test_command_validation() {
        let parser = CommandParser::new();

        // Valid commands
        let result = parser.parse(":describe users").unwrap();
        assert!(parser.validate(&result).is_ok());

        // Invalid commands
        let result = parser.parse(":describe").unwrap();
        assert!(parser.validate(&result).is_err());
    }

    #[test]
    fn test_complexity_estimation() {
        let parser = CommandParser::new();

        let simple_query = "SELECT * FROM users";
        let result = parser.parse(simple_query).unwrap();
        assert!(result.metadata.complexity <= 3);

        let complex_query = "SELECT u.*, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id ORDER BY u.name";
        let result = parser.parse(complex_query).unwrap();
        assert!(result.metadata.complexity >= 5);
    }
}
