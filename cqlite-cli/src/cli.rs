use clap::{Subcommand, ValueEnum};
use std::path::PathBuf;

pub use crate::{AdminCommands, BenchCommands, Commands, SchemaCommands};

#[derive(ValueEnum, Clone, Debug)]
pub enum OutputFormat {
    Table,
    Json,
    Csv,
    Yaml,
}

#[derive(ValueEnum, Clone, Debug)]
pub enum ImportFormat {
    Csv,
    Json,
    Parquet,
}

#[derive(ValueEnum, Clone, Debug)]
pub enum ExportFormat {
    Csv,
    Json,
    Parquet,
    Sql,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Table => write!(f, "table"),
            OutputFormat::Json => write!(f, "json"),
            OutputFormat::Csv => write!(f, "csv"),
            OutputFormat::Yaml => write!(f, "yaml"),
        }
    }
}

impl std::fmt::Display for ImportFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImportFormat::Csv => write!(f, "csv"),
            ImportFormat::Json => write!(f, "json"),
            ImportFormat::Parquet => write!(f, "parquet"),
        }
    }
}

impl std::fmt::Display for ExportFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExportFormat::Csv => write!(f, "csv"),
            ExportFormat::Json => write!(f, "json"),
            ExportFormat::Parquet => write!(f, "parquet"),
            ExportFormat::Sql => write!(f, "sql"),
        }
    }
}
