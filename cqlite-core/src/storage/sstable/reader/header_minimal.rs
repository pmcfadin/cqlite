//! The HEADERLESS minimal-header builders and their `CompressionInfo.db` probe
//! (split out of `header.rs` per the campsite rule, epic #1116).
//!
//! `nb`/`oa` BIG and `da` BTI `Data.db` are headerless: the file begins directly
//! with compressed-chunk data and every piece of metadata lives in the companion
//! `Statistics.db` / `CompressionInfo.db`. `header.rs` dispatches to the builder
//! here once it has established (from the version gates plus the magic-number
//! probe) that there is no embedded header to parse. Grouping the two builders with
//! the ONE probe they share keeps the absent-vs-unreadable rule below in one place.

use std::path::Path;

use super::{
    extract_generation_from_path, extract_keyspace_from_path, extract_table_name_from_path,
};
use crate::parser::header::{CassandraVersion, CompressionInfo, SSTableHeader, SSTableStats};
use crate::storage::sstable::reader::compression::extract_sstable_base_name;
use crate::{Error, Result};

/// Create minimal header for headerless NB format files
pub(super) async fn create_minimal_nb_header(path: &Path) -> Result<SSTableHeader> {
    // #4159: absent ⇒ uncompressed; present-but-unreadable PROPAGATES.
    let compression_algorithm = match load_nb_compression_info(path).await? {
        Some(info) => {
            tracing::info!(
                "Loaded CompressionInfo.db for NB format: algorithm={}, chunk_length={}, chunks={}",
                info.algorithm,
                info.chunk_length,
                info.chunk_offsets.len()
            );
            info.algorithm
        }
        None => {
            tracing::debug!(
                "No CompressionInfo.db beside NB format file '{}' — an uncompressed SSTable",
                path.display()
            );
            "NONE".to_string()
        }
    };

    // Create a minimal header for NB format with compression info
    Ok(SSTableHeader {
        cassandra_version: CassandraVersion::V5_0NewBig, // NB format maps to NewBig
        version: 0,        // NB format doesn't have version in Data.db
        table_id: [0; 16], // Table ID is in other components
        keyspace: extract_keyspace_from_path(path),
        table_name: extract_table_name_from_path(path),
        generation: extract_generation_from_path(path),
        compression: CompressionInfo {
            algorithm: compression_algorithm,
            chunk_size: 16384, // Default chunk size
            parameters: std::collections::HashMap::new(),
        },
        stats: SSTableStats {
            row_count: 0,
            min_timestamp: 0,
            max_timestamp: 0,
            max_deletion_time: 0,
            compression_ratio: 1.0,
            row_size_histogram: vec![],
        },
        columns: vec![],
        properties: std::collections::HashMap::new(),
    })
}

/// Build a minimal headerless header for BTI ("da") format Data.db (issue #831).
///
/// BTI Data.db is headerless and chunk-compressed, just like nb/oa BIG format,
/// so this mirrors [`create_minimal_nb_header`] but sets `cassandra_version` to
/// [`CassandraVersion::V5_0Bti`] — which is what makes the reader engage schema
/// extraction (mod.rs schema-eligible match) and schema-aware V5 row parsing for
/// the BTI partition decode path. Compression metadata is loaded from the sibling
/// CompressionInfo.db (BTI Data.db is LZ4-chunk-compressed).
pub(super) async fn create_minimal_bti_header(path: &Path) -> Result<SSTableHeader> {
    // Same absent-vs-unreadable split as `create_minimal_nb_header` (#4159).
    let compression_algorithm = match load_nb_compression_info(path).await? {
        Some(info) => {
            tracing::info!(
                "Loaded CompressionInfo.db for BTI format: algorithm={}, chunk_length={}, chunks={}",
                info.algorithm,
                info.chunk_length,
                info.chunk_offsets.len()
            );
            info.algorithm
        }
        None => {
            tracing::debug!(
                "No CompressionInfo.db beside BTI format file '{}' — an uncompressed SSTable",
                path.display()
            );
            "NONE".to_string()
        }
    };

    Ok(SSTableHeader {
        cassandra_version: CassandraVersion::V5_0Bti, // da format maps to BTI
        version: 0,                                   // headerless: no version in Data.db
        table_id: [0; 16],                            // Table ID is in other components
        keyspace: extract_keyspace_from_path(path),
        table_name: extract_table_name_from_path(path),
        generation: extract_generation_from_path(path),
        compression: CompressionInfo {
            algorithm: compression_algorithm,
            chunk_size: 16384, // Default chunk size
            parameters: std::collections::HashMap::new(),
        },
        stats: SSTableStats {
            row_count: 0,
            min_timestamp: 0,
            max_timestamp: 0,
            max_deletion_time: 0,
            compression_ratio: 1.0,
            row_size_histogram: vec![],
        },
        columns: vec![],
        properties: std::collections::HashMap::new(),
    })
}

/// Load the sibling `CompressionInfo.db`, distinguishing ABSENT from UNREADABLE
/// (issue #4159; the split is pinned by `header_compression_info_tests`).
///
/// `Ok(None)` = no such component, the ordinary shape of an UNCOMPRESSED SSTable
/// (#1406), so the caller reports `algorithm = "NONE"`. `Err` = the component IS
/// there and did not open/read/parse; `"NONE"` there would read COMPRESSED chunks as
/// raw bytes and decode to nothing. Both used to take one `Err` arm commented
/// "Assuming no compression".
pub(super) async fn load_nb_compression_info(
    data_db_path: &Path,
) -> Result<Option<crate::storage::sstable::compression_info::CompressionInfo>> {
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    // An unparseable descriptor is NOT evidence of an uncompressed SSTable — we
    // cannot tell where its sidecar would be — so this refuses, not reports absence.
    let base_name = extract_sstable_base_name(data_db_path).ok_or_else(|| {
        Error::InvalidFormat(format!(
            "Cannot derive the SSTable base name from {:?}, so its CompressionInfo.db \
             cannot be located; refusing rather than assuming the data is uncompressed",
            data_db_path
        ))
    })?;

    // Build CompressionInfo.db path
    let parent_dir = data_db_path.parent().unwrap_or(Path::new("."));
    let compression_info_path = parent_dir.join(format!("{}-CompressionInfo.db", base_name));

    // Read and parse CompressionInfo.db
    let mut file = match File::open(&compression_info_path).await {
        Ok(f) => f,
        // ABSENT: an uncompressed SSTable has no CompressionInfo.db at all.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(Error::InvalidFormat(format!(
                "CompressionInfo.db at {:?} is PRESENT but could not be opened: {}. \
                 Refusing rather than reading its compressed Data.db as uncompressed",
                compression_info_path, e
            )))
        }
    };

    let mut data = Vec::new();
    file.read_to_end(&mut data).await.map_err(|e| {
        Error::InvalidFormat(format!(
            "Failed to read CompressionInfo.db at {:?}: {}",
            compression_info_path, e
        ))
    })?;

    crate::storage::sstable::compression_info::CompressionInfo::parse(&data).map(Some)
}
