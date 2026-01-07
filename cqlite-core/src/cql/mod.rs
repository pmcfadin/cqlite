//! # CQL Text Parsing Module
//!
//! This module provides CQL (Cassandra Query Language) text parsing functionality.
//! It parses CQL query strings into Abstract Syntax Trees (AST) for further processing.
//!
//! ## Architecture Overview
//!
//! This is one of four parsing subsystems in cqlite-core:
//!
//! | Module | Purpose |
//! |--------|---------|
//! | **`cql/`** | Full CQL text → AST parsing (this module) |
//! | `parser/` | SSTable binary format parsing |
//! | `schema/cql_parser.rs` | CREATE TABLE → TableSchema |
//! | `query/parser.rs` | Lightweight DML → ParsedQuery |
//!
//! See `docs/architecture/parser-overview.md` for the complete architecture overview.
//!
//! ## Key Distinction
//!
//! - **cql/** = CQL text parsing (query strings → AST)
//! - **parser/** = SSTable binary format parsing (binary data → structured values)
//!
//! ## Available Backends
//!
//! - **Nom**: High-performance parser combinator implementation (recommended)
//! - **ANTLR**: Grammar-based parsing (placeholder for future development)
//!
//! ## Usage
//!
//! ```rust,ignore
//! use cqlite_core::cql::{create_default_parser, CqlStatement};
//!
//! let parser = create_default_parser()?;
//! let statement = parser.parse("SELECT * FROM users").await?;
//! ```

// Core trait definitions
pub mod ast;
pub mod config;
pub mod error;
pub mod traits;
pub mod visitor;

// Parser implementations
pub mod antlr_backend;
pub mod nom_backend;

// Factory and configuration
pub mod factory;

// Schema integration with parser layer
pub mod schema_integration;

// Re-export core trait abstractions
pub use traits::{
    CqlParser, CqlParserFactory, CqlVisitor, FactoryInfo, ParserBackendInfo, ParserFeature,
    PerformanceCharacteristics, SourcePosition,
};

// Re-export AST types for convenience
pub use ast::{
    CqlAssignment, CqlAssignmentOperator, CqlBinaryOperator, CqlColumnDef, CqlCreateTable,
    CqlDataType, CqlDelete, CqlDropTable, CqlExpression, CqlIdentifier, CqlInsert, CqlInsertValues,
    CqlLiteral, CqlOrderBy, CqlPrimaryKey, CqlSelect, CqlSelectItem, CqlSortDirection,
    CqlStatement, CqlTable, CqlTableOptions, CqlUnaryOperator, CqlUpdate, CqlUsing,
};

// Re-export visitor pattern
pub use visitor::{
    DefaultVisitor, IdentifierCollector, SchemaBuilderVisitor, SemanticValidator,
    TypeCollectorVisitor, ValidationVisitor,
};

// Re-export error types
pub use error::{ErrorCategory as ParserErrorCategory, ErrorSeverity, ParserError, ParserWarning};

// Re-export configuration
pub use config::{
    MemorySettings, ParserBackend, ParserConfig, ParserFeature as ConfigFeature,
    PerformanceSettings, SecuritySettings,
};

// Re-export factory
pub use factory::{register_global_factory, ParserFactory, ParserRegistry, UseCase};

// Re-export schema integration functions
pub use schema_integration::{
    extract_table_name_enhanced, parse_cql_schema_enhanced, parse_cql_schema_fast,
    parse_cql_schema_simple, parse_cql_schema_strict, parse_cql_schemas_batch,
    table_name_matches_enhanced, validate_cql_schema_syntax, SchemaParserConfig,
};

// Re-export deprecated functions with explicit warning suppression
#[allow(deprecated)]
pub use schema_integration::parse_cql_schema_compat;

// Re-export parser implementations
pub use antlr_backend::AntlrParser;
pub use nom_backend::NomParser;

use crate::error::Result;
use std::sync::Arc;

/// Re-export common result types
pub use crate::error::Result as CqlResult;

/// Convenience function to create a default parser
pub fn create_default_parser() -> Result<Arc<dyn CqlParser + Send + Sync>> {
    ParserFactory::create_default()
}
