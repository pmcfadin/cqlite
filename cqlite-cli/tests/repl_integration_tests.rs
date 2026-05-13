//! Integration tests for REPL functionality
//!
//! These tests validate the REPL engine initialization, configuration,
//! and basic command parsing without requiring actual query execution.

use anyhow::Result;
use cqlite_cli::repl::{CommandParser, CommandType, OutputFormat, ReplConfig, ReplMode};
use tempfile::TempDir;

// ============================================================================
// REPL Configuration Tests
// ============================================================================

#[test]
fn test_repl_config_default() -> Result<()> {
    let config = ReplConfig::default();

    assert_eq!(config.mode, ReplMode::Interactive);
    assert!(config.enable_history);
    assert!(config.enable_completion);
    assert!(config.enable_colors);
    assert_eq!(config.output_format, OutputFormat::Table);
    assert_eq!(config.max_history_size, 1000);
    assert_eq!(config.page_size, 50);
    assert!(!config.show_timing);
    assert!(config.enable_paging);
    assert_eq!(config.prompt, "cqlite> ");
    assert_eq!(config.prompt_continuation, "    -> ");

    Ok(())
}

#[test]
fn test_repl_config_validation() -> Result<()> {
    // Test that ReplConfig can be created with valid settings

    // Test mode variations
    let config = ReplConfig {
        mode: ReplMode::Basic,
        ..Default::default()
    };
    assert_eq!(config.mode, ReplMode::Basic);

    let config = ReplConfig {
        mode: ReplMode::Tui,
        ..Default::default()
    };
    assert_eq!(config.mode, ReplMode::Tui);

    // Test output format variations
    let config = ReplConfig {
        output_format: OutputFormat::Json,
        ..Default::default()
    };
    assert_eq!(config.output_format, OutputFormat::Json);

    let config = ReplConfig {
        output_format: OutputFormat::Csv,
        ..Default::default()
    };
    assert_eq!(config.output_format, OutputFormat::Csv);

    // Test numeric settings
    let config = ReplConfig {
        page_size: 100,
        ..Default::default()
    };
    assert_eq!(config.page_size, 100);

    let config = ReplConfig {
        max_history_size: 5000,
        ..Default::default()
    };
    assert_eq!(config.max_history_size, 5000);

    // Test boolean flags
    let config = ReplConfig {
        enable_history: false,
        ..Default::default()
    };
    assert!(!config.enable_history);

    let config = ReplConfig {
        show_timing: true,
        ..Default::default()
    };
    assert!(config.show_timing);

    Ok(())
}

#[test]
fn test_repl_mode_variants() -> Result<()> {
    // Ensure all ReplMode variants are available
    let modes = vec![ReplMode::Basic, ReplMode::Tui, ReplMode::Interactive];

    for mode in modes {
        let config = ReplConfig {
            mode: mode.clone(),
            ..ReplConfig::default()
        };
        assert_eq!(config.mode, mode);
    }

    Ok(())
}

#[test]
fn test_output_format_variants() -> Result<()> {
    // Ensure all OutputFormat variants are available
    let formats = vec![
        OutputFormat::Table,
        OutputFormat::Csv,
        OutputFormat::Json,
        OutputFormat::Raw,
    ];

    for format in formats {
        let config = ReplConfig {
            output_format: format.clone(),
            ..ReplConfig::default()
        };
        assert_eq!(config.output_format, format);
    }

    Ok(())
}

// ============================================================================
// Command Parser Tests
// ============================================================================

#[test]
fn test_command_parser_initialization() -> Result<()> {
    // Test that CommandParser can be created
    let _parser = CommandParser::new();
    let _parser_strict = CommandParser::new_strict();

    // Parser initialization successful if we get here without panic
    Ok(())
}

#[test]
fn test_parse_exit_commands() -> Result<()> {
    let parser = CommandParser::new();

    let exit_commands = vec![":quit", ":exit", ":q", ".quit", "\\q"];

    for cmd in exit_commands {
        let result = parser.parse(cmd)?;
        assert_eq!(result.command_type, CommandType::Exit);
    }

    Ok(())
}

#[test]
fn test_parse_help_commands() -> Result<()> {
    let parser = CommandParser::new();

    // Test basic help
    let result = parser.parse(":help")?;
    assert_eq!(result.command_type, CommandType::Help { topic: None });

    // Test help with topic
    let result = parser.parse(":help config")?;
    assert_eq!(
        result.command_type,
        CommandType::Help {
            topic: Some("config".to_string())
        }
    );

    // Test help with multiple word topic
    let result = parser.parse(":help show config")?;
    assert_eq!(
        result.command_type,
        CommandType::Help {
            topic: Some("show config".to_string())
        }
    );

    Ok(())
}

#[test]
fn test_parse_config_commands() -> Result<()> {
    let parser = CommandParser::new();

    // Test config show (default)
    let result = parser.parse(":config")?;
    assert!(matches!(result.command_type, CommandType::Config { .. }));

    // Test config set
    let result = parser.parse(":config output_format=json")?;
    match result.command_type {
        CommandType::Config { operation } => {
            assert!(operation.contains("output_format=json"));
        }
        _ => panic!("Expected Config command"),
    }

    Ok(())
}

#[test]
fn test_parse_navigation_commands() -> Result<()> {
    let parser = CommandParser::new();

    // Test tables command
    let result = parser.parse(":tables")?;
    assert_eq!(result.command_type, CommandType::Tables);

    // Test describe command
    let result = parser.parse(":describe users")?;
    match result.command_type {
        CommandType::Describe { object_name } => {
            assert_eq!(object_name, "users");
        }
        _ => panic!("Expected Describe command"),
    }

    // Test use command
    let result = parser.parse(":use test_keyspace")?;
    match result.command_type {
        CommandType::Use { keyspace } => {
            assert_eq!(keyspace, "test_keyspace");
        }
        _ => panic!("Expected Use command"),
    }

    Ok(())
}

#[test]
fn test_parse_system_commands() -> Result<()> {
    let parser = CommandParser::new();

    // Test clear command
    let result = parser.parse(":clear")?;
    assert_eq!(result.command_type, CommandType::Clear);

    // Test history command
    let result = parser.parse(":history")?;
    assert_eq!(result.command_type, CommandType::History);

    // Test source command
    let result = parser.parse(":source /path/to/file.sql")?;
    match result.command_type {
        CommandType::Source { file_path } => {
            assert_eq!(file_path, "/path/to/file.sql");
        }
        _ => panic!("Expected Source command"),
    }

    // Test status command
    let result = parser.parse(":status")?;
    assert_eq!(result.command_type, CommandType::Status);

    Ok(())
}

#[test]
fn test_parse_cql_queries() -> Result<()> {
    let parser = CommandParser::new();

    let queries = vec![
        "SELECT * FROM users",
        "INSERT INTO users (id, name) VALUES (1, 'test')",
        "UPDATE users SET name = 'updated' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
        "CREATE TABLE test (id int PRIMARY KEY)",
        "ALTER TABLE users ADD email text",
        "DROP TABLE users",
    ];

    for query in queries {
        let result = parser.parse(query)?;
        match result.command_type {
            CommandType::CqlQuery {
                query: parsed_query,
            } => {
                assert_eq!(parsed_query, query);
            }
            _ => panic!("Expected CqlQuery for: {query}"),
        }
    }

    Ok(())
}

#[test]
fn test_parse_unknown_commands() -> Result<()> {
    let parser = CommandParser::new();

    // Test unknown meta-command
    let result = parser.parse(":unknown")?;
    assert!(matches!(result.command_type, CommandType::Unknown { .. }));

    // Test random text (not a valid CQL pattern)
    let result = parser.parse("random text without cql keywords")?;
    assert!(matches!(result.command_type, CommandType::Unknown { .. }));

    Ok(())
}

#[test]
fn test_command_validation() -> Result<()> {
    let parser = CommandParser::new();

    // Valid commands should pass validation
    let valid_commands = vec![
        ":help",
        ":quit",
        ":describe users",
        ":use keyspace",
        "SELECT * FROM users",
    ];

    for cmd in valid_commands {
        let parsed = parser.parse(cmd)?;
        assert!(
            parser.validate(&parsed).is_ok(),
            "Failed to validate: {cmd}"
        );
    }

    // Invalid commands should fail validation
    let invalid_commands = vec![
        ":describe", // Missing object name
    ];

    for cmd in invalid_commands {
        let parsed = parser.parse(cmd)?;
        assert!(
            parser.validate(&parsed).is_err(),
            "Should have failed validation: {cmd}"
        );
    }

    // Note: Commands without required arguments are parsed as Unknown, not as their intended type
    let use_no_keyspace = parser.parse(":use")?;
    assert!(matches!(
        use_no_keyspace.command_type,
        CommandType::Unknown { .. }
    ));

    let source_no_file = parser.parse(":source")?;
    assert!(matches!(
        source_no_file.command_type,
        CommandType::Unknown { .. }
    ));

    Ok(())
}

#[test]
fn test_command_metadata_generation() -> Result<()> {
    let parser = CommandParser::new();

    // Test that metadata is generated for commands
    let result = parser.parse(":help")?;
    assert_eq!(result.metadata.complexity, 1);
    assert!(!result.metadata.modifies_state);
    assert!(!result.metadata.requires_database);

    let result = parser.parse("SELECT * FROM users")?;
    assert!(result.metadata.complexity > 0);
    assert!(!result.metadata.modifies_state);
    assert!(result.metadata.requires_database);

    let result = parser.parse("INSERT INTO users (id) VALUES (1)")?;
    assert!(result.metadata.complexity > 1);
    assert!(result.metadata.modifies_state);
    assert!(result.metadata.requires_database);

    Ok(())
}

#[test]
fn test_balanced_parentheses_validation() -> Result<()> {
    let parser = CommandParser::new();

    // Valid queries with balanced parentheses
    let valid = vec![
        "SELECT COUNT(*) FROM users",
        "SELECT * FROM users WHERE id IN (1, 2, 3)",
        "INSERT INTO users (id, name) VALUES (1, 'test')",
    ];

    for query in valid {
        let parsed = parser.parse(query)?;
        assert!(
            parser.validate(&parsed).is_ok(),
            "Should pass validation: {query}"
        );
    }

    // Invalid queries with unbalanced parentheses
    let invalid = vec![
        "SELECT COUNT( FROM users",
        "SELECT * FROM users WHERE id IN (1, 2, 3",
        "INSERT INTO users (id, name) VALUES 1, 'test')",
    ];

    for query in invalid {
        let parsed = parser.parse(query)?;
        assert!(
            parser.validate(&parsed).is_err(),
            "Should fail validation: {query}"
        );
    }

    Ok(())
}

#[test]
fn test_balanced_quotes_validation() -> Result<()> {
    let parser = CommandParser::new();

    // Valid queries with balanced quotes
    let valid = vec![
        "SELECT * FROM users WHERE name = 'test'",
        "INSERT INTO users (id, name) VALUES (1, 'John O\\'Brien')",
        "SELECT 'hello' AS greeting",
    ];

    for query in valid {
        let parsed = parser.parse(query)?;
        assert!(
            parser.validate(&parsed).is_ok(),
            "Should pass validation: {query}"
        );
    }

    // Invalid queries with unbalanced quotes
    let invalid = vec![
        "SELECT * FROM users WHERE name = 'test",
        "SELECT 'hello AS greeting",
    ];

    for query in invalid {
        let parsed = parser.parse(query)?;
        assert!(
            parser.validate(&parsed).is_err(),
            "Should fail validation: {query}"
        );
    }

    Ok(())
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_parse_empty_command() -> Result<()> {
    let parser = CommandParser::new();

    let result = parser.parse("");
    assert!(result.is_err(), "Empty command should fail parsing");

    let result = parser.parse("   ");
    assert!(
        result.is_err(),
        "Whitespace-only command should fail parsing"
    );

    Ok(())
}

#[test]
fn test_parse_multiline_detection() -> Result<()> {
    let parser = CommandParser::new();

    // Single-line command
    let result = parser.parse("SELECT * FROM users")?;
    assert!(!result.is_multiline);

    // Multi-line command
    let result = parser.parse("SELECT *\nFROM users\nWHERE id = 1")?;
    assert!(result.is_multiline);

    Ok(())
}

// ============================================================================
// File System Integration Tests
// ============================================================================

#[test]
fn test_temp_directory_creation() -> Result<()> {
    // Test that we can create temporary directories for REPL history/state
    let temp_dir = TempDir::new()?;
    assert!(temp_dir.path().exists());

    // Test creating subdirectories
    let history_dir = temp_dir.path().join("history");
    std::fs::create_dir(&history_dir)?;
    assert!(history_dir.exists());

    Ok(())
}

#[test]
fn test_config_file_operations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let config_path = temp_dir.path().join("repl_config.toml");

    // Test that we can write a basic config structure
    let config_content = r#"
[repl]
enable_history = true
enable_completion = true
enable_colors = true
show_timing = false
page_size = 50
enable_paging = true
max_history_size = 1000
prompt = "cqlite> "
prompt_continuation = "    -> "
"#;

    std::fs::write(&config_path, config_content)?;
    assert!(config_path.exists());

    // Test that we can read it back
    let content = std::fs::read_to_string(&config_path)?;
    assert!(content.contains("enable_history"));
    assert!(content.contains("prompt"));

    Ok(())
}

// ============================================================================
// Command Prefix Tests
// ============================================================================

#[test]
fn test_command_prefix_variations() -> Result<()> {
    let parser = CommandParser::new();

    // Test : prefix
    let result = parser.parse(":help")?;
    assert!(matches!(result.command_type, CommandType::Help { .. }));

    // Test . prefix
    let result = parser.parse(".help")?;
    assert!(matches!(result.command_type, CommandType::Help { .. }));

    // Test \ prefix
    let result = parser.parse("\\help")?;
    assert!(matches!(result.command_type, CommandType::Help { .. }));

    Ok(())
}

#[test]
fn test_case_insensitive_commands() -> Result<()> {
    let parser = CommandParser::new();

    // Meta-commands should be case-insensitive
    let variations = vec![":HELP", ":Help", ":help", ":HeLp"];

    for cmd in variations {
        let result = parser.parse(cmd)?;
        assert!(matches!(result.command_type, CommandType::Help { .. }));
    }

    Ok(())
}

// ============================================================================
// Complexity Estimation Tests
// ============================================================================

#[test]
fn test_query_complexity_estimation() -> Result<()> {
    let parser = CommandParser::new();

    // Simple query - low complexity
    let simple = parser.parse("SELECT * FROM users")?;
    assert!(simple.metadata.complexity <= 3);

    // Complex query - high complexity
    let complex = parser.parse(
        "SELECT u.*, COUNT(o.id) FROM users u
         LEFT JOIN orders o ON u.id = o.user_id
         GROUP BY u.id
         ORDER BY u.name",
    )?;
    assert!(complex.metadata.complexity >= 5);

    Ok(())
}

// ============================================================================
// Identifier Validation Tests
// ============================================================================

#[test]
fn test_keyspace_identifier_validation() -> Result<()> {
    let parser = CommandParser::new();

    // Valid identifiers (simple alphanumeric and underscore)
    let valid = vec![":use keyspace1", ":use _underscore"];

    for cmd in valid {
        let parsed = parser.parse(cmd)?;
        assert!(parser.validate(&parsed).is_ok(), "Should validate: {cmd}");
    }

    // Quoted identifiers - test that they parse correctly
    // Note: The validator may have specific rules about quoted identifiers
    let quoted = parser.parse(":use \"quoted identifier\"")?;
    // Just ensure it parses without panic
    match quoted.command_type {
        CommandType::Use { keyspace } => {
            assert!(keyspace.contains("quoted"));
        }
        _ => panic!("Expected Use command"),
    }

    Ok(())
}
