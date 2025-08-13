// Command Line Completion Engine
//
// Provides intelligent auto-completion for CQL commands, table names, column names,
// and meta-commands in the REPL environment.

use super::{ReplResult, ReplError, CompletionContext};
use std::collections::HashSet;

/// Completion suggestion with metadata
#[derive(Debug, Clone, PartialEq)]
pub struct CompletionSuggestion {
    /// The completion text
    pub text: String,
    /// Display text (may be different from completion text)
    pub display: String,
    /// Description of the suggestion
    pub description: Option<String>,
    /// Type of completion
    pub completion_type: CompletionType,
    /// Priority (higher = more relevant)
    pub priority: u8,
}

/// Types of completions
#[derive(Debug, Clone, PartialEq)]
pub enum CompletionType {
    /// CQL keyword (SELECT, FROM, etc.)
    Keyword,
    /// Meta-command (:help, :quit, etc.)
    MetaCommand,
    /// Table name
    Table,
    /// Column name
    Column,
    /// Keyspace name
    Keyspace,
    /// Function name
    Function,
    /// Data type
    DataType,
    /// File path
    FilePath,
    /// Configuration option
    ConfigOption,
}

/// Completion engine
pub struct CompletionEngine {
    /// CQL keywords cache
    cql_keywords: HashSet<String>,
    /// Meta-commands cache
    meta_commands: HashSet<String>,
    /// CQL functions cache
    cql_functions: HashSet<String>,
    /// Data types cache
    data_types: HashSet<String>,
    /// Configuration options cache
    config_options: HashSet<String>,
}

impl CompletionEngine {
    /// Create a new completion engine
    pub fn new() -> Self {
        let mut engine = Self {
            cql_keywords: HashSet::new(),
            meta_commands: HashSet::new(),
            cql_functions: HashSet::new(),
            data_types: HashSet::new(),
            config_options: HashSet::new(),
        };
        
        engine.initialize_static_completions();
        engine
    }
    
    /// Initialize static completion data
    fn initialize_static_completions(&mut self) {
        // CQL Keywords
        let keywords = [
            "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE",
            "CREATE", "ALTER", "DROP", "USE", "DESCRIBE",
            "FROM", "WHERE", "ORDER", "GROUP", "HAVING",
            "BY", "ASC", "DESC", "LIMIT", "OFFSET",
            "AND", "OR", "NOT", "IN", "LIKE", "IS", "NULL",
            "PRIMARY", "KEY", "CLUSTERING", "INDEX",
            "TABLE", "KEYSPACE", "TYPE", "FUNCTION",
            "IF", "EXISTS", "WITH", "OPTIONS",
            "ALLOW", "FILTERING", "TOKEN", "COUNT",
            "SUM", "AVG", "MIN", "MAX", "DISTINCT",
            "AS", "CAST", "TTL", "WRITETIME",
            "BATCH", "BEGIN", "UNLOGGED", "COUNTER",
            "STATIC", "FROZEN", "TUPLE", "MAP", "SET", "LIST",
        ];
        
        for keyword in &keywords {
            self.cql_keywords.insert(keyword.to_string());
        }
        
        // Meta-commands
        let meta_commands = [
            ":help", ":quit", ":exit", ":q", ":clear", ":cls",
            ":tables", ":list", ":describe", ":desc", ":use",
            ":config", ":show", ":set", ":history", ":timing",
            ":source", ":load", ":keyspaces", ":info", ":schema",
        ];
        
        for cmd in &meta_commands {
            self.meta_commands.insert(cmd.to_string());
        }
        
        // CQL Functions
        let functions = [
            "now", "uuid", "timeuuid", "dateof", "unixTimestampOf",
            "toDate", "toTimestamp", "toUnixTimestamp",
            "minTimeuuid", "maxTimeuuid",
            "count", "sum", "avg", "min", "max",
            "writetime", "ttl", "token",
            "cast", "toJson", "fromJson",
            "blobasbigint", "blobAsInt", "blobAsText",
            "bigintAsBlob", "intAsBlob", "textAsBlob",
        ];
        
        for func in &functions {
            self.cql_functions.insert(func.to_string());
        }
        
        // Data types
        let data_types = [
            "ascii", "bigint", "blob", "boolean", "counter",
            "date", "decimal", "double", "duration", "float",
            "inet", "int", "smallint", "text", "time",
            "timestamp", "timeuuid", "tinyint", "uuid", "varchar",
            "varint", "map", "set", "list", "tuple", "frozen",
        ];
        
        for dtype in &data_types {
            self.data_types.insert(dtype.to_string());
        }
        
        // Configuration options
        let config_options = [
            "output_format", "page_size", "show_timing", "enable_paging",
            "enable_colors", "data_dir", "keyspace", "prompt",
            "prompt_continuation", "max_history_size",
        ];
        
        for option in &config_options {
            self.config_options.insert(option.to_string());
        }
    }
    
    /// Get completions for the given context
    pub fn get_completions(&self, context: &CompletionContext) -> ReplResult<Vec<CompletionSuggestion>> {
        let mut suggestions = Vec::new();
        
        // Parse the current input to understand context
        let analysis = self.analyze_input(&context.line, context.pos);
        
        match analysis.completion_context {
            InputContext::MetaCommand => {
                suggestions.extend(self.complete_meta_commands(&analysis.current_word));
            }
            InputContext::CqlKeyword => {
                suggestions.extend(self.complete_cql_keywords(&analysis.current_word));
                suggestions.extend(self.complete_functions(&analysis.current_word));
            }
            InputContext::TableName => {
                suggestions.extend(self.complete_table_names(&analysis.current_word, &context.tables));
            }
            InputContext::ColumnName => {
                suggestions.extend(self.complete_column_names(&analysis.current_word, &context.tables));
            }
            InputContext::KeyspaceName => {
                suggestions.extend(self.complete_keyspace_names(&analysis.current_word, &context.keyspaces));
            }
            InputContext::DataType => {
                suggestions.extend(self.complete_data_types(&analysis.current_word));
            }
            InputContext::ConfigOption => {
                suggestions.extend(self.complete_config_options(&analysis.current_word));
            }
            InputContext::FilePath => {
                suggestions.extend(self.complete_file_paths(&analysis.current_word));
            }
            InputContext::Unknown => {
                // Provide general completions
                suggestions.extend(self.complete_cql_keywords(&analysis.current_word));
                suggestions.extend(self.complete_meta_commands(&analysis.current_word));
            }
        }
        
        // Sort by priority and relevance
        suggestions.sort_by(|a, b| {
            b.priority.cmp(&a.priority)
                .then_with(|| a.text.len().cmp(&b.text.len()))
                .then_with(|| a.text.cmp(&b.text))
        });
        
        // Limit number of suggestions
        suggestions.truncate(50);
        
        Ok(suggestions)
    }
    
    /// Analyze input to determine completion context
    fn analyze_input(&self, line: &str, pos: usize) -> InputAnalysis {
        let safe_pos = pos.min(line.len());
        let text_before_cursor = &line[..safe_pos];
        
        // Find the current word being typed
        let current_word = self.extract_current_word(text_before_cursor);
        
        // Determine context based on input structure
        let context = if text_before_cursor.trim_start().starts_with(':') {
            if text_before_cursor.contains(" config ") {
                InputContext::ConfigOption
            } else if text_before_cursor.contains(" use ") {
                InputContext::KeyspaceName
            } else if text_before_cursor.contains(" describe ") || text_before_cursor.contains(" desc ") {
                InputContext::TableName
            } else if text_before_cursor.contains(" source ") || text_before_cursor.contains(" load ") {
                InputContext::FilePath
            } else {
                InputContext::MetaCommand
            }
        } else {
            self.analyze_cql_context(text_before_cursor)
        };
        
        InputAnalysis {
            current_word,
            completion_context: context,
            line_before_cursor: text_before_cursor.to_string(),
        }
    }
    
    /// Analyze CQL context
    fn analyze_cql_context(&self, text: &str) -> InputContext {
        let upper_text = text.to_uppercase();
        let words: Vec<&str> = upper_text.split_whitespace().collect();
        
        if words.is_empty() {
            return InputContext::CqlKeyword;
        }
        
        // Look for context clues
        if let Some(last_word) = words.last() {
            if words.len() >= 2 {
                let prev_word = words[words.len() - 2];
                
                // After FROM, join with, etc. - expect table names
                if prev_word == "FROM" || prev_word == "JOIN" || 
                   prev_word == "UPDATE" || prev_word == "INTO" {
                    return InputContext::TableName;
                }
                
                // After USE - expect keyspace names
                if prev_word == "USE" {
                    return InputContext::KeyspaceName;
                }
                
                // After data type keywords - expect data types
                if words.len() >= 3 {
                    let context_phrase = format!("{} {}", words[words.len() - 3], prev_word);
                    if context_phrase.contains("PRIMARY KEY") || 
                       context_phrase.contains("CLUSTERING KEY") ||
                       prev_word == "TYPE" {
                        return InputContext::DataType;
                    }
                }
            }
            
            // If current word looks like a function call
            if last_word.ends_with('(') || text.contains('(') {
                return InputContext::CqlKeyword; // Functions are included in keyword completion
            }
        }
        
        // Check if we're in a SELECT list (after SELECT but before FROM)
        if upper_text.contains("SELECT") && !upper_text.contains("FROM") {
            return InputContext::ColumnName;
        }
        
        // Default to keyword completion
        InputContext::CqlKeyword
    }
    
    /// Extract the current word being typed
    fn extract_current_word(&self, text: &str) -> String {
        let chars: Vec<char> = text.chars().collect();
        let mut start = chars.len();
        let mut end = chars.len();
        
        // Find the start of the current word
        for i in (0..chars.len()).rev() {
            let ch = chars[i];
            if ch.is_whitespace() || ch == '(' || ch == ')' || ch == ',' || ch == ';' {
                start = i + 1;
                break;
            }
            if i == 0 {
                start = 0;
                break;
            }
        }
        
        // Extract the word
        if start < end {
            chars[start..end].iter().collect()
        } else {
            String::new()
        }
    }
    
    /// Complete meta-commands
    fn complete_meta_commands(&self, prefix: &str) -> Vec<CompletionSuggestion> {
        let mut suggestions = Vec::new();
        let lower_prefix = prefix.to_lowercase();
        
        for cmd in &self.meta_commands {
            if cmd.to_lowercase().starts_with(&lower_prefix) {
                let description = self.get_meta_command_description(cmd);
                suggestions.push(CompletionSuggestion {
                    text: cmd.clone(),
                    display: cmd.clone(),
                    description: Some(description),
                    completion_type: CompletionType::MetaCommand,
                    priority: self.calculate_priority(cmd, prefix, 8),
                });
            }
        }
        
        suggestions
    }
    
    /// Complete CQL keywords
    fn complete_cql_keywords(&self, prefix: &str) -> Vec<CompletionSuggestion> {
        let mut suggestions = Vec::new();
        let upper_prefix = prefix.to_uppercase();
        
        for keyword in &self.cql_keywords {
            if keyword.starts_with(&upper_prefix) {
                suggestions.push(CompletionSuggestion {
                    text: keyword.clone(),
                    display: keyword.clone(),
                    description: None,
                    completion_type: CompletionType::Keyword,
                    priority: self.calculate_priority(keyword, prefix, 7),
                });
            }
        }
        
        suggestions
    }
    
    /// Complete function names
    fn complete_functions(&self, prefix: &str) -> Vec<CompletionSuggestion> {
        let mut suggestions = Vec::new();
        let lower_prefix = prefix.to_lowercase();
        
        for func in &self.cql_functions {
            if func.to_lowercase().starts_with(&lower_prefix) {
                suggestions.push(CompletionSuggestion {
                    text: format!("{}()", func),
                    display: format!("{}()", func),
                    description: Some(format!("CQL function: {}", func)),
                    completion_type: CompletionType::Function,
                    priority: self.calculate_priority(func, prefix, 6),
                });
            }
        }
        
        suggestions
    }
    
    /// Complete table names
    fn complete_table_names(&self, prefix: &str, tables: &[String]) -> Vec<CompletionSuggestion> {
        let mut suggestions = Vec::new();
        let lower_prefix = prefix.to_lowercase();
        
        for table in tables {
            if table.to_lowercase().starts_with(&lower_prefix) {
                suggestions.push(CompletionSuggestion {
                    text: table.clone(),
                    display: table.clone(),
                    description: Some("Table".to_string()),
                    completion_type: CompletionType::Table,
                    priority: self.calculate_priority(table, prefix, 9),
                });
            }
        }
        
        suggestions
    }
    
    /// Complete column names (simplified - would need schema info)
    fn complete_column_names(&self, prefix: &str, _tables: &[String]) -> Vec<CompletionSuggestion> {
        let mut suggestions = Vec::new();
        let lower_prefix = prefix.to_lowercase();
        
        // Common column names as fallback
        let common_columns = [
            "id", "name", "email", "created_at", "updated_at",
            "user_id", "timestamp", "value", "data", "type",
            "status", "description", "title", "content",
        ];
        
        for col in &common_columns {
            if col.starts_with(&lower_prefix) {
                suggestions.push(CompletionSuggestion {
                    text: col.to_string(),
                    display: col.to_string(),
                    description: Some("Column".to_string()),
                    completion_type: CompletionType::Column,
                    priority: self.calculate_priority(col, prefix, 5),
                });
            }
        }
        
        suggestions
    }
    
    /// Complete keyspace names
    fn complete_keyspace_names(&self, prefix: &str, keyspaces: &[String]) -> Vec<CompletionSuggestion> {
        let mut suggestions = Vec::new();
        let lower_prefix = prefix.to_lowercase();
        
        for keyspace in keyspaces {
            if keyspace.to_lowercase().starts_with(&lower_prefix) {
                suggestions.push(CompletionSuggestion {
                    text: keyspace.clone(),
                    display: keyspace.clone(),
                    description: Some("Keyspace".to_string()),
                    completion_type: CompletionType::Keyspace,
                    priority: self.calculate_priority(keyspace, prefix, 8),
                });
            }
        }
        
        suggestions
    }
    
    /// Complete data types
    fn complete_data_types(&self, prefix: &str) -> Vec<CompletionSuggestion> {
        let mut suggestions = Vec::new();
        let lower_prefix = prefix.to_lowercase();
        
        for dtype in &self.data_types {
            if dtype.starts_with(&lower_prefix) {
                suggestions.push(CompletionSuggestion {
                    text: dtype.clone(),
                    display: dtype.clone(),
                    description: Some("Data type".to_string()),
                    completion_type: CompletionType::DataType,
                    priority: self.calculate_priority(dtype, prefix, 6),
                });
            }
        }
        
        suggestions
    }
    
    /// Complete configuration options
    fn complete_config_options(&self, prefix: &str) -> Vec<CompletionSuggestion> {
        let mut suggestions = Vec::new();
        let lower_prefix = prefix.to_lowercase();
        
        for option in &self.config_options {
            if option.starts_with(&lower_prefix) {
                let description = self.get_config_option_description(option);
                suggestions.push(CompletionSuggestion {
                    text: option.clone(),
                    display: option.clone(),
                    description: Some(description),
                    completion_type: CompletionType::ConfigOption,
                    priority: self.calculate_priority(option, prefix, 7),
                });
            }
        }
        
        suggestions
    }
    
    /// Complete file paths (basic implementation)
    fn complete_file_paths(&self, prefix: &str) -> Vec<CompletionSuggestion> {
        let mut suggestions = Vec::new();
        
        // Basic file path completion
        if prefix.ends_with('/') || prefix.is_empty() {
            // Directory completion would go here
            suggestions.push(CompletionSuggestion {
                text: "./".to_string(),
                display: "./".to_string(),
                description: Some("Current directory".to_string()),
                completion_type: CompletionType::FilePath,
                priority: 5,
            });
        }
        
        suggestions
    }
    
    /// Calculate completion priority based on relevance
    fn calculate_priority(&self, suggestion: &str, prefix: &str, base_priority: u8) -> u8 {
        if prefix.is_empty() {
            return base_priority;
        }
        
        let lower_suggestion = suggestion.to_lowercase();
        let lower_prefix = prefix.to_lowercase();
        
        // Exact match gets highest priority
        if lower_suggestion == lower_prefix {
            return base_priority + 3;
        }
        
        // Starts with prefix gets high priority
        if lower_suggestion.starts_with(&lower_prefix) {
            return base_priority + 2;
        }
        
        // Contains prefix gets medium priority
        if lower_suggestion.contains(&lower_prefix) {
            return base_priority + 1;
        }
        
        base_priority
    }
    
    /// Get description for meta-commands
    fn get_meta_command_description(&self, cmd: &str) -> String {
        match cmd {
            ":help" => "Show help information".to_string(),
            ":quit" | ":exit" | ":q" => "Exit the REPL".to_string(),
            ":clear" | ":cls" => "Clear the screen".to_string(),
            ":tables" | ":list" => "List all tables".to_string(),
            ":describe" | ":desc" => "Describe an object".to_string(),
            ":use" => "Switch to a keyspace".to_string(),
            ":config" => "Show/set configuration".to_string(),
            ":history" => "Show command history".to_string(),
            ":timing" => "Toggle timing display".to_string(),
            ":source" | ":load" => "Execute commands from file".to_string(),
            ":keyspaces" => "List all keyspaces".to_string(),
            ":info" => "Show object information".to_string(),
            ":schema" => "Show schema information".to_string(),
            _ => "Meta-command".to_string(),
        }
    }
    
    /// Get description for configuration options
    fn get_config_option_description(&self, option: &str) -> String {
        match option {
            "output_format" => "Output format (table, csv, json, raw)".to_string(),
            "page_size" => "Number of rows per page".to_string(),
            "show_timing" => "Show query execution timing".to_string(),
            "enable_paging" => "Enable result paging".to_string(),
            "enable_colors" => "Enable colored output".to_string(),
            "data_dir" => "Cassandra data directory path".to_string(),
            "keyspace" => "Default keyspace".to_string(),
            "prompt" => "REPL prompt string".to_string(),
            "prompt_continuation" => "Multi-line prompt string".to_string(),
            "max_history_size" => "Maximum history entries".to_string(),
            _ => "Configuration option".to_string(),
        }
    }
}

/// Input analysis result
#[derive(Debug)]
struct InputAnalysis {
    /// The current word being typed
    current_word: String,
    /// The completion context
    completion_context: InputContext,
    /// Text before cursor
    line_before_cursor: String,
}

/// Context for completion
#[derive(Debug, PartialEq)]
enum InputContext {
    MetaCommand,
    CqlKeyword,
    TableName,
    ColumnName,
    KeyspaceName,
    DataType,
    ConfigOption,
    FilePath,
    Unknown,
}

impl Default for CompletionEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_meta_command_completion() {
        let engine = CompletionEngine::new();
        let context = CompletionContext {
            line: ":he".to_string(),
            pos: 3,
            session_state: super::SessionState::Ready,
            tables: vec![],
            keyspaces: vec![],
        };
        
        let completions = engine.get_completions(&context).unwrap();
        assert!(!completions.is_empty());
        assert!(completions.iter().any(|c| c.text == ":help"));
    }
    
    #[test]
    fn test_keyword_completion() {
        let engine = CompletionEngine::new();
        let context = CompletionContext {
            line: "SEL".to_string(),
            pos: 3,
            session_state: super::SessionState::Ready,
            tables: vec![],
            keyspaces: vec![],
        };
        
        let completions = engine.get_completions(&context).unwrap();
        assert!(!completions.is_empty());
        assert!(completions.iter().any(|c| c.text == "SELECT"));
    }
    
    #[test]
    fn test_table_completion() {
        let engine = CompletionEngine::new();
        let tables = vec!["users".to_string(), "orders".to_string()];
        let context = CompletionContext {
            line: "SELECT * FROM us".to_string(),
            pos: 16,
            session_state: super::SessionState::Ready,
            tables,
            keyspaces: vec![],
        };
        
        let completions = engine.get_completions(&context).unwrap();
        assert!(completions.iter().any(|c| c.text == "users"));
    }
    
    #[test]
    fn test_current_word_extraction() {
        let engine = CompletionEngine::new();
        
        assert_eq!(engine.extract_current_word("SELECT * FROM us"), "us");
        assert_eq!(engine.extract_current_word(":he"), ":he");
        assert_eq!(engine.extract_current_word("SELECT count("), "count(");
        assert_eq!(engine.extract_current_word("SELECT * FROM users WHERE name = 'jo"), "'jo");
    }
}