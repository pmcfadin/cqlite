//! # CQL Text Parsing Module
//!
//! Parses CQL (Cassandra Query Language) query strings into Abstract Syntax Trees.
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

pub mod ast;
pub mod config;
pub mod error;
pub mod traits;
pub mod visitor;

pub mod antlr_backend;
pub mod nom_backend;

#[cfg(feature = "write-support")]
pub mod mutation_parser;

pub mod factory;
pub mod schema_integration;

pub use ast::{
    CqlAssignment, CqlAssignmentOperator, CqlBinaryOperator, CqlColumnDef, CqlCreateTable,
    CqlDataType, CqlDelete, CqlDropTable, CqlExpression, CqlIdentifier, CqlInsert, CqlInsertValues,
    CqlLiteral, CqlOrderBy, CqlPrimaryKey, CqlSelect, CqlSelectItem, CqlSortDirection,
    CqlStatement, CqlTable, CqlTableOptions, CqlUnaryOperator, CqlUpdate, CqlUsing,
};
pub use config::{
    MemorySettings, ParserBackend, ParserConfig, ParserFeature as ConfigFeature,
    PerformanceSettings, SecuritySettings,
};
pub use error::{ErrorCategory as ParserErrorCategory, ErrorSeverity, ParserError, ParserWarning};
pub use factory::{register_global_factory, ParserFactory, ParserRegistry, UseCase};
pub use schema_integration::{
    extract_table_name_enhanced, parse_cql_schema_enhanced, parse_cql_schema_fast,
    parse_cql_schema_simple, parse_cql_schema_strict, parse_cql_schemas_batch,
    table_name_matches_enhanced, validate_cql_schema_syntax, SchemaParserConfig,
};
pub use traits::{
    CqlParser, CqlParserFactory, CqlVisitor, FactoryInfo, ParserBackendInfo, ParserFeature,
    PerformanceCharacteristics, SourcePosition,
};
pub use visitor::{
    DefaultVisitor, IdentifierCollector, SchemaBuilderVisitor, SemanticValidator,
    TypeCollectorVisitor, ValidationVisitor,
};

#[allow(deprecated)]
pub use schema_integration::parse_cql_schema_compat;

pub use antlr_backend::AntlrParser;
pub use nom_backend::NomParser;

pub use crate::error::Result as CqlResult;

use crate::error::Result;
use std::sync::Arc;

/// Convenience function to create a default parser.
pub fn create_default_parser() -> Result<Arc<dyn CqlParser + Send + Sync>> {
    ParserFactory::create_default()
}
