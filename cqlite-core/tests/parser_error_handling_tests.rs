use cqlite_core::parser::error::{ErrorCategory, ErrorContext, ErrorSeverity, ParserError};
use cqlite_core::parser::traits::SourcePosition;

#[test]
fn test_parser_error_creation_all_types() {
    let pos = SourcePosition::new(10, 5, 100, 20);

    // Test syntax error creation
    let syntax_err = ParserError::syntax("Expected ';'", pos.clone());
    assert!(matches!(syntax_err, ParserError::SyntaxError { .. }));
    assert_eq!(syntax_err.position(), Some(&pos));
    assert!(!syntax_err.is_recoverable());
    assert_eq!(syntax_err.category(), &ErrorCategory::Syntax);
    assert_eq!(syntax_err.severity(), &ErrorSeverity::Error);

    // Test syntax error with expected tokens
    let syntax_expected = ParserError::syntax_with_expected(
        "Unexpected token",
        pos.clone(),
        vec!["SELECT".to_string(), "INSERT".to_string()],
    );
    assert!(matches!(
        syntax_expected,
        ParserError::SyntaxError {
            expected: Some(_),
            ..
        }
    ));

    // Test semantic error
    let semantic_err = ParserError::semantic("Table does not exist");
    assert!(matches!(semantic_err, ParserError::SemanticError { .. }));
    assert_eq!(semantic_err.position(), None);
    assert!(!semantic_err.is_recoverable());
    assert_eq!(semantic_err.category(), &ErrorCategory::Semantic);

    // Test semantic error with position
    let semantic_pos = ParserError::semantic_at("Column not found", pos.clone());
    assert!(matches!(
        semantic_pos,
        ParserError::SemanticError {
            position: Some(_),
            ..
        }
    ));

    // Test lexical error
    let lexical_err = ParserError::lexical("Invalid character", pos.clone());
    assert!(matches!(lexical_err, ParserError::LexicalError { .. }));
    assert!(!lexical_err.is_recoverable());

    // Test backend error
    let backend_err = ParserError::backend("nom", "Parse failed");
    assert!(matches!(backend_err, ParserError::BackendError { .. }));
    assert!(backend_err.is_recoverable()); // Backend errors are recoverable
    assert_eq!(backend_err.category(), &ErrorCategory::Backend);

    // Test backend error with position
    let backend_pos = ParserError::backend_at("antlr", "Grammar error", pos.clone());
    assert!(matches!(
        backend_pos,
        ParserError::BackendError {
            position: Some(_),
            ..
        }
    ));

    // Test type error
    let type_err = ParserError::type_error("Invalid type conversion");
    assert!(matches!(type_err, ParserError::TypeError { .. }));
    assert!(!type_err.is_recoverable());
    assert_eq!(type_err.category(), &ErrorCategory::Type);

    // Test type mismatch
    let type_mismatch = ParserError::type_mismatch("Integer", "String", Some(pos.clone()));
    assert!(matches!(
        type_mismatch,
        ParserError::TypeError {
            expected_type: Some(_),
            actual_type: Some(_),
            ..
        }
    ));

    // Test configuration error
    let config_err = ParserError::configuration("Invalid timeout value");
    assert!(matches!(config_err, ParserError::ConfigurationError { .. }));
    assert!(config_err.is_recoverable());
    assert_eq!(config_err.category(), &ErrorCategory::Configuration);

    // Test unsupported feature error
    let feature_err = ParserError::unsupported_feature("nom", "streaming");
    assert!(matches!(
        feature_err,
        ParserError::UnsupportedFeature { .. }
    ));
    assert!(feature_err.is_recoverable());

    // Test timeout error
    let timeout_err = ParserError::timeout(5000);
    assert!(matches!(timeout_err, ParserError::Timeout { .. }));
    assert!(timeout_err.is_recoverable());

    // Test resource limit error
    let limit_err = ParserError::resource_limit("memory", 1000, 1500);
    assert!(matches!(limit_err, ParserError::ResourceLimit { .. }));
    assert!(limit_err.is_recoverable());

    // Test internal error
    let internal_err = ParserError::internal("Unexpected null pointer");
    assert!(matches!(internal_err, ParserError::InternalError { .. }));
    assert!(!internal_err.is_recoverable());
    assert_eq!(internal_err.severity(), &ErrorSeverity::Fatal);

    // Test internal error with cause
    let internal_cause = ParserError::internal_with_cause("Parser crash", "Stack overflow");
    assert!(matches!(
        internal_cause,
        ParserError::InternalError { cause: Some(_), .. }
    ));
}

#[test]
fn test_error_recovery_suggestions() {
    // Test timeout error suggestions
    let timeout_err = ParserError::timeout(5000);
    let suggestions = timeout_err.recovery_suggestions();
    assert!(!suggestions.is_empty());
    assert!(suggestions[0].contains("timeout"));
    assert!(suggestions[1].contains("complexity"));

    // Test feature error suggestions
    let feature_err = ParserError::unsupported_feature("nom", "streaming");
    let suggestions = feature_err.recovery_suggestions();
    assert!(!suggestions.is_empty());
    assert!(suggestions[0].contains("backend"));
    assert!(suggestions[1].contains("feature"));

    // Test backend error suggestions
    let backend_err = ParserError::backend("antlr", "Grammar not loaded");
    let suggestions = backend_err.recovery_suggestions();
    assert!(!suggestions.is_empty());
    assert!(suggestions[0].contains("backend"));

    // Test resource limit suggestions
    let limit_err = ParserError::resource_limit("memory", 1000, 1500);
    let suggestions = limit_err.recovery_suggestions();
    assert!(!suggestions.is_empty());
    assert!(suggestions[0].contains("limit"));
    assert!(suggestions[1].contains("usage"));

    // Test configuration error suggestions
    let config_err = ParserError::configuration("Invalid parser settings");
    let suggestions = config_err.recovery_suggestions();
    assert!(!suggestions.is_empty());
    assert!(suggestions[0].contains("configuration"));

    // Test error with no suggestions
    let syntax_err = ParserError::syntax("Missing semicolon", SourcePosition::start());
    let suggestions = syntax_err.recovery_suggestions();
    assert!(suggestions.is_empty());
}

#[test]
fn test_error_message_and_position_extraction() {
    let pos = SourcePosition::new(5, 10, 50, 10);

    // Test message extraction for all error types
    let syntax_err = ParserError::syntax("Expected token", pos.clone());
    assert_eq!(syntax_err.message(), "Expected token");

    let semantic_err = ParserError::semantic("Undefined table");
    assert_eq!(semantic_err.message(), "Undefined table");

    let lexical_err = ParserError::lexical("Invalid character", pos.clone());
    assert_eq!(lexical_err.message(), "Invalid character");

    let backend_err = ParserError::backend("nom", "Parse error");
    assert_eq!(backend_err.message(), "Parse error");

    let type_err = ParserError::type_error("Type mismatch");
    assert_eq!(type_err.message(), "Type mismatch");

    let config_err = ParserError::configuration("Bad config");
    assert_eq!(config_err.message(), "Bad config");

    let feature_err = ParserError::unsupported_feature("nom", "feature");
    assert!(feature_err.message().contains("feature"));
    assert!(feature_err.message().contains("nom"));

    let internal_err = ParserError::internal("Internal failure");
    assert_eq!(internal_err.message(), "Internal failure");

    let timeout_err = ParserError::timeout(3000);
    assert!(timeout_err.message().contains("3000ms"));

    let limit_err = ParserError::resource_limit("stack", 100, 150);
    assert!(limit_err.message().contains("stack"));

    // Test position extraction
    assert_eq!(syntax_err.position(), Some(&pos));
    assert_eq!(lexical_err.position(), Some(&pos));
    assert_eq!(semantic_err.position(), None);
    assert_eq!(backend_err.position(), None);
    assert_eq!(config_err.position(), None);
}

#[test]
fn test_error_context_and_snippets() {
    let input = "SELECT * FROM users\nWHERE id = 'unclosed\nORDER BY name".to_string();
    let context = ErrorContext::new(input.clone(), "nom".to_string());

    // Test basic context creation
    assert_eq!(context.input, input);
    assert_eq!(context.backend, "nom");
    assert_eq!(context.config, None);
    assert_eq!(context.stack_trace, None);

    // Test context with config
    let context_with_config = context.clone().with_config("timeout=5000".to_string());
    assert_eq!(context_with_config.config, Some("timeout=5000".to_string()));

    // Test context with stack trace
    let stack_trace = vec!["parse_select".to_string(), "parse_statement".to_string()];
    let context_with_stack = context.clone().with_stack_trace(stack_trace.clone());
    assert_eq!(context_with_stack.stack_trace, Some(stack_trace));

    // Test error snippet generation
    let pos = SourcePosition::new(2, 15, 30, 8); // Line 2, column 15
    let snippet = context.get_error_snippet(&pos, 1);
    assert!(snippet.contains("WHERE"));
    assert!(snippet.contains(">>>"));
    assert!(snippet.contains("^")); // Error indicator

    // Test snippet with edge cases
    let pos_first_line = SourcePosition::new(1, 8, 8, 1);
    let snippet_first = context.get_error_snippet(&pos_first_line, 2);
    assert!(snippet_first.contains("SELECT"));

    let pos_last_line = SourcePosition::new(3, 5, 50, 1);
    let snippet_last = context.get_error_snippet(&pos_last_line, 1);
    assert!(snippet_last.contains("ORDER"));

    // Test out-of-bounds positions
    let pos_invalid = SourcePosition::new(10, 1, 100, 1);
    let snippet_invalid = context.get_error_snippet(&pos_invalid, 1);
    assert_eq!(snippet_invalid, input);
}

#[test]
fn test_error_conversion_to_core_error() {
    let pos = SourcePosition::start();

    // Test syntax error conversion
    let syntax_err = ParserError::syntax("Parse error", pos.clone());
    let core_err: cqlite_core::error::Error = syntax_err.into();
    // Should convert to SQL parse error - verify error message contains expected text
    assert!(
        core_err.to_string().contains("Parse error")
            || matches!(core_err, cqlite_core::error::Error::SqlParse(_))
    );

    // Test semantic error conversion
    let semantic_err = ParserError::semantic("Semantic issue");
    let core_err: cqlite_core::error::Error = semantic_err.into();
    assert!(
        core_err.to_string().contains("Semantic issue")
            || matches!(core_err, cqlite_core::error::Error::SqlParse(_))
    );

    // Test lexical error conversion
    let lexical_err = ParserError::lexical("Lexical problem", pos.clone());
    let core_err: cqlite_core::error::Error = lexical_err.into();
    assert!(
        core_err.to_string().contains("Lexical problem")
            || matches!(core_err, cqlite_core::error::Error::SqlParse(_))
    );

    // Test backend error conversion
    let backend_err = ParserError::backend("nom", "Backend failure");
    let core_err: cqlite_core::error::Error = backend_err.into();
    assert!(
        core_err.to_string().contains("Backend failure")
            || matches!(core_err, cqlite_core::error::Error::Internal(_))
    );

    // Test type error conversion
    let type_err = ParserError::type_error("Type conversion failed");
    let core_err: cqlite_core::error::Error = type_err.into();
    assert!(
        core_err.to_string().contains("Type conversion failed")
            || matches!(core_err, cqlite_core::error::Error::TypeConversion(_))
    );

    // Test configuration error conversion
    let config_err = ParserError::configuration("Bad configuration");
    let core_err: cqlite_core::error::Error = config_err.into();
    assert!(
        core_err.to_string().contains("Bad configuration")
            || matches!(core_err, cqlite_core::error::Error::Configuration(_))
    );

    // Test unsupported feature error conversion
    let feature_err = ParserError::unsupported_feature("nom", "streaming");
    let core_err: cqlite_core::error::Error = feature_err.into();
    assert!(core_err.to_string().contains("streaming") && core_err.to_string().contains("nom"));

    // Test timeout error conversion
    let timeout_err = ParserError::timeout(5000);
    let core_err: cqlite_core::error::Error = timeout_err.into();
    assert!(core_err.to_string().contains("5000ms"));

    // Test resource limit error conversion
    let limit_err = ParserError::resource_limit("memory", 1000, 1500);
    let core_err: cqlite_core::error::Error = limit_err.into();
    assert!(
        core_err.to_string().contains("memory")
            && core_err.to_string().contains("1500")
            && core_err.to_string().contains("1000")
    );
}

#[test]
fn test_error_utils_nom_conversion() {
    use cqlite_core::parser::error::utils::from_nom_error;
    use nom::error::Error as NomError;

    // Test nom error conversion
    let nom_err = nom::Err::Error(NomError::new("test input", nom::error::ErrorKind::Tag));
    let parser_err = from_nom_error(nom_err, "test input");
    assert!(matches!(parser_err, ParserError::BackendError { .. }));
    assert!(parser_err.message().contains("Parse error"));

    // Test nom failure conversion
    let nom_failure = nom::Err::Failure(NomError::new("test input", nom::error::ErrorKind::Alt));
    let parser_err = from_nom_error(nom_failure, "test input");
    assert!(matches!(parser_err, ParserError::BackendError { .. }));

    // Test nom incomplete conversion
    let nom_incomplete: nom::Err<nom::error::Error<&str>> =
        nom::Err::Incomplete(nom::Needed::Size(std::num::NonZeroUsize::new(5).unwrap()));
    let parser_err = from_nom_error(nom_incomplete, "test input");
    assert!(matches!(parser_err, ParserError::BackendError { .. }));
    assert!(parser_err.message().contains("Incomplete"));
}

#[test]
fn test_error_utils_contextual_error_creation() {
    use cqlite_core::parser::error::utils::create_contextual_error;

    let input = "SELECT * FROM users\nWHERE id = ?".to_string();
    let context =
        ErrorContext::new(input, "nom".to_string()).with_config("timeout=5000".to_string());

    let pos = SourcePosition::new(2, 10, 25, 1);
    let error = ParserError::syntax("Unexpected token", pos);

    let contextual_message = create_contextual_error(error, &context);

    assert!(contextual_message.contains("Parser Error"));
    assert!(contextual_message.contains("Unexpected token"));
    assert!(contextual_message.contains("line 2, column 10"));
    assert!(contextual_message.contains("Backend: nom"));
    assert!(contextual_message.contains("Configuration: timeout=5000"));
    assert!(contextual_message.contains("WHERE"));
    assert!(contextual_message.contains(">>>"));
}

#[test]
fn test_error_utils_error_chaining() {
    use cqlite_core::parser::error::utils::chain_errors;

    // Test empty error list
    let empty_errors = vec![];
    let chained = chain_errors(empty_errors);
    assert!(matches!(chained, ParserError::InternalError { .. }));
    assert!(chained.message().contains("No errors"));

    // Test single error
    let single_error = vec![ParserError::syntax("Test error", SourcePosition::start())];
    let chained = chain_errors(single_error);
    assert!(matches!(chained, ParserError::SyntaxError { .. }));

    // Test multiple errors
    let multiple_errors = vec![
        ParserError::syntax("First error", SourcePosition::start()),
        ParserError::semantic("Second error"),
        ParserError::lexical("Third error", SourcePosition::start()),
    ];
    let chained = chain_errors(multiple_errors);
    assert!(matches!(chained, ParserError::InternalError { .. }));
    assert!(chained.message().contains("Multiple errors"));
    assert!(chained.message().contains("First error"));
    assert!(chained.message().contains("Second error"));
    assert!(chained.message().contains("Third error"));
}

#[test]
fn test_parser_warning_creation() {
    use cqlite_core::parser::error::ParserWarning;

    // Test basic warning creation
    let warning = ParserWarning::new("Deprecated syntax".to_string(), ErrorCategory::Syntax);
    assert_eq!(warning.message, "Deprecated syntax");
    assert_eq!(warning.category, ErrorCategory::Syntax);
    assert_eq!(warning.position, None);

    // Test warning with position
    let pos = SourcePosition::new(5, 10, 50, 5);
    let warning_with_pos = ParserWarning::with_position(
        "Unused variable".to_string(),
        ErrorCategory::Semantic,
        pos.clone(),
    );
    assert_eq!(warning_with_pos.message, "Unused variable");
    assert_eq!(warning_with_pos.category, ErrorCategory::Semantic);
    assert_eq!(warning_with_pos.position, Some(pos));
}

#[test]
fn test_error_severity_and_category_enums() {
    // Test error severity levels
    let info = ErrorSeverity::Info;
    let warning = ErrorSeverity::Warning;
    let error = ErrorSeverity::Error;
    let fatal = ErrorSeverity::Fatal;

    assert_ne!(info, warning);
    assert_ne!(warning, error);
    assert_ne!(error, fatal);

    // Test error categories
    let syntax = ErrorCategory::Syntax;
    let semantic = ErrorCategory::Semantic;
    let type_cat = ErrorCategory::Type;
    let config = ErrorCategory::Configuration;
    let backend = ErrorCategory::Backend;
    let internal = ErrorCategory::Internal;

    assert_ne!(syntax, semantic);
    assert_ne!(semantic, type_cat);
    assert_ne!(type_cat, config);
    assert_ne!(config, backend);
    assert_ne!(backend, internal);
}

#[test]
fn test_complex_error_scenarios() {
    // Test complex error scenario with multiple components
    let pos = SourcePosition::new(15, 25, 200, 10);

    // Create a complex syntax error with expected tokens
    let complex_syntax = ParserError::syntax_with_expected(
        "Expected SELECT, INSERT, UPDATE, or DELETE",
        pos.clone(),
        vec![
            "SELECT".to_string(),
            "INSERT".to_string(),
            "UPDATE".to_string(),
            "DELETE".to_string(),
        ],
    );

    assert!(matches!(complex_syntax, ParserError::SyntaxError {
        expected: Some(ref tokens),
        ..
    } if tokens.len() == 4));

    // Test type mismatch with detailed information
    let detailed_type_error =
        ParserError::type_mismatch("INTEGER", "VARCHAR(255)", Some(pos.clone()));

    assert!(matches!(detailed_type_error, ParserError::TypeError {
        expected_type: Some(ref expected),
        actual_type: Some(ref actual),
        position: Some(_),
        ..
    } if expected == "INTEGER" && actual == "VARCHAR(255)"));

    // Test resource limit with specific values
    let memory_limit = ParserError::resource_limit("parse_stack_depth", 1000, 1024);
    assert!(matches!(
        memory_limit,
        ParserError::ResourceLimit {
            current_value: 1024,
            max_value: 1000,
            ..
        }
    ));

    // Test backend error with position
    let positioned_backend = ParserError::backend_at(
        "antlr4",
        "Rule 'selectStatement' failed at alternative 3",
        pos.clone(),
    );
    assert!(matches!(positioned_backend, ParserError::BackendError {
        backend: ref b,
        position: Some(_),
        ..
    } if b == "antlr4"));

    // Test internal error with cause chain
    let caused_internal = ParserError::internal_with_cause(
        "Parser state machine corrupted",
        std::io::Error::new(std::io::ErrorKind::InvalidData, "Bad state"),
    );
    assert!(matches!(
        caused_internal,
        ParserError::InternalError { cause: Some(_), .. }
    ));
    assert!(caused_internal.message().contains("corrupted"));
}
