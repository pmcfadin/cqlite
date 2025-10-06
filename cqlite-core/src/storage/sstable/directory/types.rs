use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Represents a single SSTable generation with all its component files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableGeneration {
    /// Generation number (e.g., 1 for "nb-1-big")
    pub generation: u32,
    /// Format type (e.g., "big", "da" for BTI)
    pub format: String,
    /// Table name
    pub table_name: String,
    /// Component files mapped by component type
    pub components: HashMap<SSTableComponent, PathBuf>,
    /// Base directory path
    pub base_path: PathBuf,
}

/// SSTable component types found in Cassandra 5
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[allow(clippy::upper_case_acronyms)]
pub enum SSTableComponent {
    /// Main data file containing row data
    Data,
    /// Index file for partition/row lookups (BIG format)
    Index,
    /// Statistics metadata
    Statistics,
    /// Bloom filter for negative lookups
    Filter,
    /// Index summary (BIG format)
    Summary,
    /// Compression metadata and block info
    CompressionInfo,
    /// CRC32 checksum
    Digest,
    /// Table of contents listing all components
    TOC,
    /// BTI Partitions index (BTI format only)
    Partitions,
    /// BTI Rows index (BTI format only)
    Rows,
}

impl FromStr for SSTableComponent {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "Data.db" => Ok(SSTableComponent::Data),
            "Index.db" => Ok(SSTableComponent::Index),
            "Statistics.db" => Ok(SSTableComponent::Statistics),
            "Filter.db" => Ok(SSTableComponent::Filter),
            "Summary.db" => Ok(SSTableComponent::Summary),
            "CompressionInfo.db" => Ok(SSTableComponent::CompressionInfo),
            "Digest.crc32" => Ok(SSTableComponent::Digest),
            "TOC.txt" => Ok(SSTableComponent::TOC),
            "Partitions.db" => Ok(SSTableComponent::Partitions),
            "Rows.db" => Ok(SSTableComponent::Rows),
            _ => Err(Error::invalid_format(format!(
                "Unknown SSTable component: {}",
                s
            ))),
        }
    }
}

impl SSTableComponent {
    /// Returns the file extension for this component
    pub fn file_extension(&self) -> &'static str {
        match self {
            SSTableComponent::Data => "Data.db",
            SSTableComponent::Index => "Index.db",
            SSTableComponent::Statistics => "Statistics.db",
            SSTableComponent::Filter => "Filter.db",
            SSTableComponent::Summary => "Summary.db",
            SSTableComponent::CompressionInfo => "CompressionInfo.db",
            SSTableComponent::Digest => "Digest.crc32",
            SSTableComponent::TOC => "TOC.txt",
            SSTableComponent::Partitions => "Partitions.db",
            SSTableComponent::Rows => "Rows.db",
        }
    }

    /// Returns whether this component is required for reading data
    pub fn is_required(&self) -> bool {
        matches!(self, SSTableComponent::Data | SSTableComponent::Statistics)
    }

    /// Returns whether this component is BTI-specific
    pub fn is_bti_specific(&self) -> bool {
        matches!(self, SSTableComponent::Partitions | SSTableComponent::Rows)
    }

    /// Returns whether this component is BIG-specific
    pub fn is_big_specific(&self) -> bool {
        matches!(self, SSTableComponent::Index | SSTableComponent::Summary)
    }
}

/// Represents a secondary index with its own SSTable files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecondaryIndex {
    /// Index name (e.g., "metadata_idx")
    pub index_name: String,
    /// Index directory path
    pub index_path: PathBuf,
    /// SSTable generations for this index
    pub generations: Vec<SSTableGeneration>,
}
