use clap::ValueEnum;
use std::path::PathBuf;
use anyhow::{Context, Result, anyhow};

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

/// Supported Cassandra versions for compatibility
#[derive(Debug, Clone, PartialEq)]
pub enum CassandraVersion {
    V311,
    V400,
    V500,
    Unknown(String),
}

impl CassandraVersion {
    pub fn from_string(version: &str) -> Self {
        match version {
            "3.11" | "3.11.0" => CassandraVersion::V311,
            "4.0" | "4.0.0" => CassandraVersion::V400,
            "5.0" | "5.0.0" => CassandraVersion::V500,
            _ => CassandraVersion::Unknown(version.to_string()),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            CassandraVersion::V311 => "3.11".to_string(),
            CassandraVersion::V400 => "4.0".to_string(),
            CassandraVersion::V500 => "5.0".to_string(),
            CassandraVersion::Unknown(v) => v.clone(),
        }
    }

    pub fn is_supported(&self) -> bool {
        matches!(self, CassandraVersion::V311 | CassandraVersion::V400 | CassandraVersion::V500)
    }
}

/// SSTable format version detection
pub fn detect_sstable_version(sstable_path: &PathBuf) -> Result<String> {
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};

    let mut file = File::open(sstable_path)
        .with_context(|| format!("Failed to open SSTable file: {}", sstable_path.display()))?;

    // Read first few bytes to detect format
    let mut header = vec![0u8; 16];
    file.read_exact(&mut header)
        .with_context(|| "Failed to read SSTable header for version detection")?;

    // Basic version detection based on file header patterns
    // This is a simplified implementation - real detection would be more sophisticated
    if header.starts_with(b"SSTA") {
        // Check for specific version markers
        file.seek(SeekFrom::Start(4))?;
        let mut version_bytes = vec![0u8; 4];
        file.read_exact(&mut version_bytes)?;
        
        let version = u32::from_be_bytes([version_bytes[0], version_bytes[1], version_bytes[2], version_bytes[3]]);
        
        match version {
            0x0001 => Ok("3.11".to_string()),
            0x0002 => Ok("4.0".to_string()),
            0x0003 => Ok("5.0".to_string()),
            _ => Ok("unknown".to_string()),
        }
    } else {
        // Try to detect based on file structure patterns
        file.seek(SeekFrom::End(-512))?;
        let mut footer = vec![0u8; 512];
        file.read(&mut footer)?;
        
        if footer.iter().any(|&b| b != 0) {
            // Has footer - likely newer format
            Ok("4.0".to_string())
        } else {
            // No footer - likely older format
            Ok("3.11".to_string())
        }
    }
}

/// Validate Cassandra version string
pub fn validate_cassandra_version(version: &str) -> Result<CassandraVersion> {
    let cassandra_version = CassandraVersion::from_string(version);
    
    if !cassandra_version.is_supported() {
        return Err(anyhow!(
            "Unsupported Cassandra version: {}. Supported versions: 3.11, 4.0, 5.0", 
            version
        ));
    }
    
    Ok(cassandra_version)
}

/// Create enhanced error message with version context
pub fn create_version_error(base_error: &str, detected_version: Option<&str>, provided_version: Option<&str>) -> String {
    let mut error_msg = base_error.to_string();
    
    if let Some(detected) = detected_version {
        error_msg.push_str(&format!("\nDetected SSTable version: {}", detected));
    }
    
    if let Some(provided) = provided_version {
        error_msg.push_str(&format!("\nProvided Cassandra version: {}", provided));
    }
    
    error_msg.push_str("\nHint: Try using --auto-detect flag for automatic version detection");
    error_msg.push_str("\n      or specify --cassandra-version to override (supported: 3.11, 4.0, 5.0)");
    
    error_msg
}
