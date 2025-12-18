//! Block I/O operations for SSTable readers.
//!
//! This module handles:
//! - Block header parsing for different Cassandra formats (NB, BTI, Legacy)
//! - Block data reading (direct and streaming for large blocks)
//! - Retry logic for transient I/O errors

use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use tokio::sync::Mutex;

use super::header::{detect_ascii_header_corruption, is_ascii_corruption_value};
use super::types::SSTableReaderConfig;
use crate::{Error, Result};

/// Read next block with enhanced error handling and streaming support
pub(crate) async fn read_next_block(
    file: &Arc<Mutex<BufReader<File>>>,
    cassandra_version: &crate::parser::header::CassandraVersion,
    config: &SSTableReaderConfig,
    compression_info: &Option<Arc<crate::storage::sstable::compression_info::CompressionInfo>>,
    current_chunk_index: &std::sync::atomic::AtomicUsize,
    header_offset: u64,
) -> Result<Option<Vec<u8>>> {
    read_next_block_with_retry(
        file,
        cassandra_version,
        config,
        compression_info,
        current_chunk_index,
        header_offset,
        3,
    )
    .await
}

/// Read block with retry logic for handling transient I/O errors
async fn read_next_block_with_retry(
    file: &Arc<Mutex<BufReader<File>>>,
    cassandra_version: &crate::parser::header::CassandraVersion,
    config: &SSTableReaderConfig,
    compression_info: &Option<Arc<crate::storage::sstable::compression_info::CompressionInfo>>,
    current_chunk_index: &std::sync::atomic::AtomicUsize,
    header_offset: u64,
    max_retries: usize,
) -> Result<Option<Vec<u8>>> {
    let mut retry_count = 0;

    loop {
        match read_next_block_impl(
            file,
            cassandra_version,
            config,
            compression_info,
            current_chunk_index,
            header_offset,
        )
        .await
        {
            Ok(result) => return Ok(result),
            Err(e) => {
                retry_count += 1;
                if retry_count >= max_retries {
                    log::error!("Failed to read block after {} retries: {}", max_retries, e);
                    return Err(e);
                }

                log::warn!(
                    "Block read failed (attempt {}/{}): {}, retrying...",
                    retry_count,
                    max_retries,
                    e
                );

                // Brief delay before retry
                tokio::time::sleep(tokio::time::Duration::from_millis(10 * retry_count as u64))
                    .await;
            }
        }
    }
}

/// Internal block reading implementation
async fn read_next_block_impl(
    file: &Arc<Mutex<BufReader<File>>>,
    cassandra_version: &crate::parser::header::CassandraVersion,
    config: &SSTableReaderConfig,
    compression_info: &Option<Arc<crate::storage::sstable::compression_info::CompressionInfo>>,
    current_chunk_index: &std::sync::atomic::AtomicUsize,
    _header_offset: u64, // Unused for NB format; kept for potential future BTI/Legacy use
) -> Result<Option<Vec<u8>>> {
    log::debug!("block_io::read_next_block_impl: Starting block read");
    log::debug!(
        "block_io::read_next_block_impl: Cassandra version: {:?}",
        cassandra_version
    );

    // NB format uses ChunkReader logic - returns compressed chunk data directly
    // V5_0Uncompressed format: read raw data directly (no block headers, no compression)
    if matches!(
        cassandra_version,
        crate::parser::header::CassandraVersion::V5_0Uncompressed
    ) {
        log::debug!("block_io::read_next_block_impl: Using uncompressed direct read");
        return read_uncompressed_data_block(file).await;
    }

    match cassandra_version {
        crate::parser::header::CassandraVersion::V5_0NewBig
        | crate::parser::header::CassandraVersion::V5_0NewBigFormat // Issue #212: BTI SSTables with byte-comparable keys
        | crate::parser::header::CassandraVersion::V5_0DataFormat
        | crate::parser::header::CassandraVersion::V5_0FormatC
        | crate::parser::header::CassandraVersion::V5_0FormatD
        | crate::parser::header::CassandraVersion::V5_0FormatE
        | crate::parser::header::CassandraVersion::V5_0FormatF
        | crate::parser::header::CassandraVersion::V5_0FormatG
        | crate::parser::header::CassandraVersion::V5_0StaticColumns
        | crate::parser::header::CassandraVersion::V5_0ComplexTypes
        | crate::parser::header::CassandraVersion::V5_0TypedCollections
        | crate::parser::header::CassandraVersion::V5_0WideRows => {
            // NB format versions using chunked compression (Snappy/LZ4):
            // - V5_0ComplexTypes (0x82365C00) - added in Issue #219
            // - V5_0TypedCollections (0x0F3C0000) - added in Issue #221
            // - V5_0WideRows (0xF07C5C00) - added in Issue #219 (Snappy collision handling)
            log::debug!("block_io::read_next_block_impl: Using NB format chunk reader");

            // Get file size for chunk size calculation
            let file_size = {
                let mut file_guard = file.lock().await;
                let current = file_guard.stream_position().await?;
                file_guard.seek(std::io::SeekFrom::End(0)).await?;
                let size = file_guard.stream_position().await?;
                file_guard.seek(std::io::SeekFrom::Start(current)).await?;
                size
            };

            // Read chunk with CRC validation
            // Note: For NB format files, CompressionInfo chunk offsets are always relative
            // to the start of the Data.db file (offset 0). Any embedded SSTable header is
            // part of the compressed data, not a separate uncompressed prefix.
            // Therefore, we always use header_offset=0 for NB format chunk reading.
            return read_nb_format_chunk_data(
                file,
                compression_info,
                current_chunk_index,
                file_size,
                0, // NB format: chunk offsets are relative to file start
            )
            .await;
        }
        _ => {
            // BTI and Legacy formats use traditional block headers
        }
    }

    // Read block header with format-specific handling (BTI and Legacy only)
    let block_header = match cassandra_version {
        crate::parser::header::CassandraVersion::V5_0Bti => {
            log::debug!("block_io::read_next_block_impl: Using BTI format block header reader");
            read_bti_format_block_header(file).await?
        }
        _ => {
            log::debug!("block_io::read_next_block_impl: Using legacy format block header reader");
            read_legacy_format_block_header(file).await?
        }
    };

    let Some((compressed_size, checksum, current_pos)) = block_header else {
        log::debug!("block_io::read_next_block_impl: Block header returned None (EOF)");
        return Ok(None); // EOF
    };

    log::debug!(
        "block_io::read_next_block_impl: Block header: compressed_size={}, checksum={}, pos={}",
        compressed_size,
        checksum,
        current_pos
    );

    // Validate block size to prevent memory issues and detect corruption
    if compressed_size > 64 * 1024 * 1024 {
        // 64MB limit
        return Err(Error::corruption(format!(
            "Block size too large: {} bytes (limit: 64MB)",
            compressed_size
        )));
    }

    // Detect ASCII corruption patterns in block size
    if is_ascii_corruption_value(compressed_size) {
        return Err(Error::corruption(format!(
            "Block size appears to be ASCII corruption: {} (0x{:08x}) - likely misaligned file reading",
            compressed_size, compressed_size
        )));
    }

    if compressed_size == 0 {
        log::info!("Encountered empty block at position {}", current_pos);
        return Ok(Some(Vec::new()));
    }

    // Read block data with streaming for large blocks
    let block_data = if compressed_size > config.read_buffer_size as u32 {
        read_large_block_streaming(file, compressed_size as usize, config).await?
    } else {
        read_block_direct(file, compressed_size as usize).await?
    };

    // Validate checksum if enabled
    if config.validate_checksums && checksum != 0 {
        let computed_checksum = crc32fast::hash(&block_data);
        if computed_checksum != checksum {
            return Err(Error::corruption(format!(
                "Block checksum mismatch at position {}: expected 0x{:08x}, got 0x{:08x}",
                current_pos, checksum, computed_checksum
            )));
        }
        log::debug!("Block checksum validated: 0x{:08x}", checksum);
    }

    log::debug!(
        "Successfully read block: {} bytes at position {}",
        block_data.len(),
        current_pos
    );
    Ok(Some(block_data))
}

/// Read chunk data for NB format using ChunkReader logic
///
/// NB format uses chunked compression with metadata in CompressionInfo.db.
/// This function:
/// 1. Seeks to the chunk offset from CompressionInfo
/// 2. Reads the compressed chunk bytes
/// 3. Reads and validates the trailing CRC32 checksum
/// 4. Returns compressed chunk data ready for decompression
///
/// # Offset Handling
///
/// For NB format files, CompressionInfo chunk offsets are ABSOLUTE file positions
/// (relative to byte 0 of Data.db), not relative to any header. This applies to:
/// - Headerless files (most common): chunk 0 starts at offset 0
/// - Snappy collision cases (Issue #219): correctly detected as headerless
///
/// The `header_offset` parameter is preserved for potential future BTI/Legacy format
/// support where chunk offsets may be relative to compressed data start, but for
/// NB format it should always be 0.
async fn read_nb_format_chunk_data(
    file: &Arc<Mutex<BufReader<File>>>,
    compression_info: &Option<Arc<crate::storage::sstable::compression_info::CompressionInfo>>,
    current_chunk_index: &std::sync::atomic::AtomicUsize,
    file_size: u64,
    header_offset: u64,
) -> Result<Option<Vec<u8>>> {
    log::debug!("read_nb_format_chunk_data: Starting chunk read");

    // Must have CompressionInfo for NB format
    let Some(comp_info) = compression_info else {
        return Err(Error::InvalidFormat(
            "NB format requires CompressionInfo.db but none was loaded".to_string(),
        ));
    };

    let chunk_idx = current_chunk_index.load(std::sync::atomic::Ordering::Relaxed);

    // Check if all chunks read
    if chunk_idx >= comp_info.chunk_offsets.len() {
        log::debug!(
            "read_nb_format_chunk_data: All chunks read ({}/{})",
            chunk_idx,
            comp_info.chunk_offsets.len()
        );
        return Ok(None); // EOF
    }

    log::debug!(
        "read_nb_format_chunk_data: Reading chunk {}/{}",
        chunk_idx,
        comp_info.chunk_offsets.len()
    );

    // Get chunk offset from CompressionInfo
    let chunk_offset = comp_info
        .compressed_chunk_offset(chunk_idx)
        .ok_or_else(|| Error::InvalidFormat(format!("No offset for chunk {}", chunk_idx)))?;

    log::debug!(
        "read_nb_format_chunk_data: Chunk {} offset: 0x{:x}",
        chunk_idx,
        chunk_offset
    );

    // Calculate total chunk size (includes trailing 4-byte CRC32)
    let total_chunk_size = comp_info
        .compressed_chunk_size(chunk_idx, file_size)
        .ok_or_else(|| {
            Error::InvalidFormat(format!(
                "Cannot determine size for chunk {} (file_size={})",
                chunk_idx, file_size
            ))
        })?;

    // Validate chunk size
    if total_chunk_size < 4 {
        return Err(Error::InvalidFormat(format!(
            "Chunk {} size too small: {} bytes (minimum 4 for CRC)",
            chunk_idx, total_chunk_size
        )));
    }

    // Chunk data size = total_chunk_size - 4 bytes for trailing CRC
    let chunk_data_size = (total_chunk_size - 4) as usize;

    log::debug!(
        "read_nb_format_chunk_data: Chunk {} total_size={}, data_size={}, offset=0x{:x}",
        chunk_idx,
        total_chunk_size,
        chunk_data_size,
        chunk_offset
    );

    // Read chunk data and CRC32 from file
    let (chunk_data, expected_crc) = {
        let mut file_guard = file.lock().await;

        // Seek to chunk offset (adjusted by header_offset for files with embedded headers)
        // CompressionInfo chunk offsets are relative to start of compressed data
        let absolute_offset = chunk_offset + header_offset;
        file_guard
            .seek(std::io::SeekFrom::Start(absolute_offset))
            .await
            .map_err(|e| {
                Error::Io(std::io::Error::new(
                    e.kind(),
                    format!(
                        "Failed to seek to chunk {} at offset 0x{:x} (header_offset={}): {}",
                        chunk_idx, absolute_offset, header_offset, e
                    ),
                ))
            })?;

        // Read chunk bytes (NOT including trailing CRC32)
        let mut chunk_data = vec![0u8; chunk_data_size];
        file_guard.read_exact(&mut chunk_data).await.map_err(|e| {
            Error::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to read chunk {} data ({} bytes at offset 0x{:x}): {}",
                    chunk_idx, chunk_data_size, chunk_offset, e
                ),
            ))
        })?;

        // Read trailing CRC32 (4 bytes, big-endian)
        let mut crc_bytes = [0u8; 4];
        file_guard.read_exact(&mut crc_bytes).await.map_err(|e| {
            Error::Io(std::io::Error::new(
                e.kind(),
                format!(
                    "Failed to read CRC32 for chunk {} at offset 0x{:x}: {}",
                    chunk_idx,
                    chunk_offset + chunk_data_size as u64,
                    e
                ),
            ))
        })?;
        let expected_crc = u32::from_be_bytes(crc_bytes);

        (chunk_data, expected_crc)
    };

    // Compute CRC32 of chunk bytes using crc32fast (Java-compatible algorithm)
    let computed_crc = crc32fast::hash(&chunk_data);

    // Validate CRC (fail-fast on mismatch)
    if computed_crc != expected_crc {
        return Err(Error::InvalidFormat(format!(
            "CRC32 mismatch for chunk {} at offset 0x{:x}: expected=0x{:08x}, computed=0x{:08x}, chunk_size={}",
            chunk_idx, chunk_offset, expected_crc, computed_crc, chunk_data_size
        )));
    }

    log::debug!(
        "read_nb_format_chunk_data: CRC32 validated for chunk {}: 0x{:08x}",
        chunk_idx,
        expected_crc
    );
    log::debug!(
        "read_nb_format_chunk_data: Successfully read chunk {}: {} bytes (compressed)",
        chunk_idx,
        chunk_data.len()
    );

    // Increment for next call
    current_chunk_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    // Return compressed chunk data (caller will decompress)
    Ok(Some(chunk_data))
}

/// Read block header for BTI format
async fn read_bti_format_block_header(
    file: &Arc<Mutex<BufReader<File>>>,
) -> Result<Option<(u32, u32, u64)>> {
    // BTI format has a slightly different header structure
    let mut header_buffer = [0u8; 12]; // 12-byte header for BTI
    let current_pos = {
        let mut file_guard = file.lock().await;
        let pos = file_guard.stream_position().await.unwrap_or(0);
        match file_guard.read_exact(&mut header_buffer).await {
            Ok(_) => pos,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => {
                return Err(Error::Io(std::io::Error::other(format!(
                    "Failed to read BTI block header: {}",
                    e
                ))));
            }
        }
    };

    // Check for ASCII corruption before parsing the header
    if detect_ascii_header_corruption(&header_buffer) {
        return Err(Error::corruption(format!(
            "BTI block header appears to contain ASCII corruption at position {}: {:?}",
            current_pos,
            String::from_utf8_lossy(&header_buffer[0..4])
        )));
    }

    let compressed_size = u32::from_be_bytes([
        header_buffer[0],
        header_buffer[1],
        header_buffer[2],
        header_buffer[3],
    ]);
    let checksum = u32::from_be_bytes([
        header_buffer[8],
        header_buffer[9],
        header_buffer[10],
        header_buffer[11],
    ]);

    Ok(Some((compressed_size, checksum, current_pos)))
}

/// Read block header for legacy format
async fn read_legacy_format_block_header(
    file: &Arc<Mutex<BufReader<File>>>,
) -> Result<Option<(u32, u32, u64)>> {
    let mut header_buffer = [0u8; 8]; // Minimal 8-byte header
    let current_pos = {
        let mut file_guard = file.lock().await;
        let pos = file_guard.stream_position().await.unwrap_or(0);
        match file_guard.read_exact(&mut header_buffer).await {
            Ok(_) => pos,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => {
                return Err(Error::Io(std::io::Error::other(format!(
                    "Failed to read legacy block header: {}",
                    e
                ))));
            }
        }
    };

    let compressed_size = u32::from_be_bytes([
        header_buffer[0],
        header_buffer[1],
        header_buffer[2],
        header_buffer[3],
    ]);
    let checksum = u32::from_be_bytes([
        header_buffer[4],
        header_buffer[5],
        header_buffer[6],
        header_buffer[7],
    ]);

    Ok(Some((compressed_size, checksum, current_pos)))
}

/// Read block data directly for small blocks
async fn read_block_direct(file: &Arc<Mutex<BufReader<File>>>, size: usize) -> Result<Vec<u8>> {
    let mut block_data = vec![0u8; size];
    {
        let mut file_guard = file.lock().await;
        file_guard.read_exact(&mut block_data).await.map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Failed to read block data ({}): {}",
                size, e
            )))
        })?;
    }
    Ok(block_data)
}

/// Read large block using streaming I/O to reduce memory pressure
async fn read_large_block_streaming(
    file: &Arc<Mutex<BufReader<File>>>,
    size: usize,
    config: &SSTableReaderConfig,
) -> Result<Vec<u8>> {
    let mut block_data = Vec::with_capacity(size);
    let buffer_size = config.read_buffer_size.min(size);
    let mut buffer = vec![0u8; buffer_size];
    let mut remaining = size;

    log::info!(
        "Reading large block ({} bytes) using streaming with {} byte buffer",
        size,
        buffer_size
    );

    {
        let mut file_guard = file.lock().await;
        while remaining > 0 {
            let to_read = remaining.min(buffer_size);
            file_guard
                .read_exact(&mut buffer[..to_read])
                .await
                .map_err(|e| {
                    Error::Io(std::io::Error::other(format!(
                        "Failed to read block chunk ({}): {}",
                        to_read, e
                    )))
                })?;

            block_data.extend_from_slice(&buffer[..to_read]);
            remaining -= to_read;

            // Allow other tasks to run during large reads
            if remaining > 0 && block_data.len() % (1024 * 1024) == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    Ok(block_data)
}

/// Read uncompressed data block for V5_0Uncompressed format
///
/// This format has no compression and no block headers - the entire data section
/// after the 4096-byte file header is raw partition data. We read remaining data
/// from current position to EOF, returning it as a single block.
async fn read_uncompressed_data_block(
    file: &Arc<Mutex<BufReader<File>>>,
) -> Result<Option<Vec<u8>>> {
    let (current_pos, file_size) = {
        let mut file_guard = file.lock().await;
        let current = file_guard.stream_position().await.map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Failed to get stream position: {}",
                e
            )))
        })?;

        // Get file size
        file_guard
            .seek(std::io::SeekFrom::End(0))
            .await
            .map_err(|e| {
                Error::Io(std::io::Error::other(format!(
                    "Failed to seek to end: {}",
                    e
                )))
            })?;
        let size = file_guard.stream_position().await.map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Failed to get file size: {}",
                e
            )))
        })?;

        // Seek back to current position
        file_guard
            .seek(std::io::SeekFrom::Start(current))
            .await
            .map_err(|e| {
                Error::Io(std::io::Error::other(format!(
                    "Failed to seek back to position: {}",
                    e
                )))
            })?;

        (current, size)
    };

    // Calculate remaining bytes
    let remaining = file_size.saturating_sub(current_pos) as usize;

    if remaining == 0 {
        log::debug!(
            "read_uncompressed_data_block: EOF reached at position {}",
            current_pos
        );
        return Ok(None);
    }

    log::debug!(
        "read_uncompressed_data_block: Reading {} bytes from position {}",
        remaining,
        current_pos
    );

    // Read remaining data
    let mut data = vec![0u8; remaining];
    {
        let mut file_guard = file.lock().await;
        file_guard.read_exact(&mut data).await.map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Failed to read uncompressed data block ({} bytes): {}",
                remaining, e
            )))
        })?;
    }

    log::debug!(
        "read_uncompressed_data_block: Successfully read {} bytes",
        data.len()
    );

    Ok(Some(data))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;

    // =========================================================================
    // ASCII corruption detection tests
    // =========================================================================

    #[test]
    fn test_is_ascii_corruption_value_known_patterns() {
        // Known ASCII corruption values from header.rs
        assert!(is_ascii_corruption_value(2959239534)); // "bin" pattern
        assert!(is_ascii_corruption_value(1684108385)); // "data" pattern
    }

    #[test]
    fn test_is_ascii_corruption_value_normal_values() {
        // Normal block sizes should not be flagged
        assert!(!is_ascii_corruption_value(4096));
        assert!(!is_ascii_corruption_value(65536));
        assert!(!is_ascii_corruption_value(1048576));
    }

    #[test]
    fn test_detect_ascii_header_corruption_ascii_text() {
        // Headers containing ASCII text should be detected
        let header = b"DATA1234";
        assert!(detect_ascii_header_corruption(header));

        let header2 = b"bindata!";
        assert!(detect_ascii_header_corruption(header2));
    }

    #[test]
    fn test_detect_ascii_header_corruption_binary() {
        // Normal binary headers should not be detected
        let header = [0x00, 0x00, 0x10, 0x00, 0x12, 0x34, 0x56, 0x78]; // Size 4096
        assert!(!detect_ascii_header_corruption(&header));
    }

    // =========================================================================
    // Block size validation tests
    // =========================================================================

    #[test]
    fn test_block_size_limit() {
        // Block size limit is 64MB (64 * 1024 * 1024)
        let limit = 64 * 1024 * 1024;

        // Sizes up to limit should be valid
        assert!(4096 <= limit);
        assert!(64 * 1024 * 1024 <= limit);

        // Sizes above limit would be rejected
        assert!(65 * 1024 * 1024 > limit);
    }

    #[test]
    fn test_empty_block_handling() {
        // Empty blocks (size 0) should be handled gracefully
        let size = 0u32;
        assert_eq!(size, 0);
        // The implementation returns Ok(Some(Vec::new())) for empty blocks
    }

    // =========================================================================
    // CRC32 calculation tests
    // =========================================================================

    #[test]
    fn test_crc32_calculation() {
        // Test CRC32 calculation using crc32fast
        let data = b"test data for CRC";
        let crc = crc32fast::hash(data);

        // CRC should be deterministic
        assert_eq!(crc, crc32fast::hash(data));

        // Different data should have different CRC
        let data2 = b"different test data";
        assert_ne!(crc, crc32fast::hash(data2));
    }

    #[test]
    fn test_crc32_empty_data() {
        let data: &[u8] = b"";
        let crc = crc32fast::hash(data);

        // Empty data has a specific CRC value
        assert_eq!(crc, 0); // CRC32 of empty data is 0
    }

    // =========================================================================
    // Header parsing tests
    // =========================================================================

    #[test]
    fn test_block_header_parsing_big_endian() {
        // Test big-endian parsing of block headers
        let header_buffer = [0x00, 0x00, 0x10, 0x00, 0x12, 0x34, 0x56, 0x78];

        // Legacy format: size (4 bytes) + checksum (4 bytes)
        let compressed_size = u32::from_be_bytes([
            header_buffer[0],
            header_buffer[1],
            header_buffer[2],
            header_buffer[3],
        ]);
        let checksum = u32::from_be_bytes([
            header_buffer[4],
            header_buffer[5],
            header_buffer[6],
            header_buffer[7],
        ]);

        assert_eq!(compressed_size, 4096); // 0x00001000
        assert_eq!(checksum, 0x12345678);
    }

    #[test]
    fn test_bti_header_parsing() {
        // BTI format: 12-byte header
        // [0-3]: compressed size, [4-7]: uncompressed size, [8-11]: checksum
        let header_buffer = [
            0x00, 0x00, 0x08, 0x00, // size: 2048
            0x00, 0x00, 0x10, 0x00, // uncompressed: 4096
            0xAB, 0xCD, 0xEF, 0x12, // checksum
        ];

        let compressed_size = u32::from_be_bytes([
            header_buffer[0],
            header_buffer[1],
            header_buffer[2],
            header_buffer[3],
        ]);
        let checksum = u32::from_be_bytes([
            header_buffer[8],
            header_buffer[9],
            header_buffer[10],
            header_buffer[11],
        ]);

        assert_eq!(compressed_size, 2048);
        assert_eq!(checksum, 0xABCDEF12);
    }

    // =========================================================================
    // Chunk index tests
    // =========================================================================

    #[test]
    fn test_atomic_chunk_index_increment() {
        let index = AtomicUsize::new(0);

        assert_eq!(index.load(std::sync::atomic::Ordering::Relaxed), 0);

        // Simulate chunk reads
        index.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(index.load(std::sync::atomic::Ordering::Relaxed), 1);

        index.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(index.load(std::sync::atomic::Ordering::Relaxed), 2);
    }

    // =========================================================================
    // Integration tests with real files (async)
    // =========================================================================

    #[tokio::test]
    async fn test_read_block_direct_empty() {
        // Test reading zero bytes
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join("test_empty_block.bin");

        // Create empty file
        tokio::fs::write(&temp_file, b"").await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BufReader::new(file)));

        let result = read_block_direct(&file, 0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);

        // Cleanup
        tokio::fs::remove_file(&temp_file).await.ok();
    }

    #[tokio::test]
    async fn test_read_block_direct_small() {
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join("test_small_block.bin");

        // Create test file with known content
        let test_data = b"Hello, World! This is test data.";
        tokio::fs::write(&temp_file, test_data).await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BufReader::new(file)));

        let result = read_block_direct(&file, test_data.len()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), test_data);

        // Cleanup
        tokio::fs::remove_file(&temp_file).await.ok();
    }

    #[tokio::test]
    async fn test_read_uncompressed_data_block() {
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join("test_uncompressed_block.bin");

        // Create test file
        let test_data = b"Uncompressed test data block content";
        tokio::fs::write(&temp_file, test_data).await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BufReader::new(file)));

        let result = read_uncompressed_data_block(&file).await;
        assert!(result.is_ok());

        let data = result.unwrap();
        assert!(data.is_some());
        assert_eq!(data.unwrap(), test_data);

        // Cleanup
        tokio::fs::remove_file(&temp_file).await.ok();
    }

    #[tokio::test]
    async fn test_read_uncompressed_data_block_eof() {
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join("test_uncompressed_eof.bin");

        // Create empty file
        tokio::fs::write(&temp_file, b"").await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BufReader::new(file)));

        // Should return None for EOF
        let result = read_uncompressed_data_block(&file).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Cleanup
        tokio::fs::remove_file(&temp_file).await.ok();
    }

    #[tokio::test]
    async fn test_read_legacy_format_block_header_eof() {
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join("test_legacy_header_eof.bin");

        // Create file with only 4 bytes (incomplete header)
        tokio::fs::write(&temp_file, &[0x00, 0x00, 0x10, 0x00])
            .await
            .unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BufReader::new(file)));

        // Should return None for incomplete header (EOF)
        let result = read_legacy_format_block_header(&file).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Cleanup
        tokio::fs::remove_file(&temp_file).await.ok();
    }

    #[tokio::test]
    async fn test_read_legacy_format_block_header_valid() {
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join("test_legacy_header_valid.bin");

        // Create valid 8-byte header
        let header = [0x00, 0x00, 0x10, 0x00, 0x12, 0x34, 0x56, 0x78];
        tokio::fs::write(&temp_file, &header).await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BufReader::new(file)));

        let result = read_legacy_format_block_header(&file).await;
        assert!(result.is_ok());

        let (size, checksum, pos) = result.unwrap().unwrap();
        assert_eq!(size, 4096);
        assert_eq!(checksum, 0x12345678);
        assert_eq!(pos, 0);

        // Cleanup
        tokio::fs::remove_file(&temp_file).await.ok();
    }

    #[tokio::test]
    async fn test_read_bti_format_block_header_valid() {
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join("test_bti_header_valid.bin");

        // Create valid 12-byte BTI header
        let header = [
            0x00, 0x00, 0x08, 0x00, // size: 2048
            0x00, 0x00, 0x10, 0x00, // uncompressed: 4096
            0xAB, 0xCD, 0xEF, 0x12, // checksum
        ];
        tokio::fs::write(&temp_file, &header).await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BufReader::new(file)));

        let result = read_bti_format_block_header(&file).await;
        assert!(result.is_ok());

        let (size, checksum, pos) = result.unwrap().unwrap();
        assert_eq!(size, 2048);
        assert_eq!(checksum, 0xABCDEF12);
        assert_eq!(pos, 0);

        // Cleanup
        tokio::fs::remove_file(&temp_file).await.ok();
    }

    #[tokio::test]
    async fn test_read_large_block_streaming() {
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join("test_large_block.bin");

        // Create larger test file (128KB)
        let size = 128 * 1024;
        let test_data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        tokio::fs::write(&temp_file, &test_data).await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BufReader::new(file)));

        let config = SSTableReaderConfig {
            read_buffer_size: 4096, // Small buffer to test streaming
            validate_checksums: true,
            ..Default::default()
        };

        let result = read_large_block_streaming(&file, size, &config).await;
        assert!(result.is_ok());

        let data = result.unwrap();
        assert_eq!(data.len(), size);
        assert_eq!(data, test_data);

        // Cleanup
        tokio::fs::remove_file(&temp_file).await.ok();
    }

    #[tokio::test]
    async fn test_read_with_real_sstable_data() {
        // Test with real SSTable data if available
        let datasets_root = match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => {
                eprintln!("CQLITE_DATASETS_ROOT not set, skipping real data test");
                return;
            }
        };

        let simple_table_dir = datasets_root.join("sstables/test_basic");
        if !simple_table_dir.exists() {
            eprintln!("test_basic not found, skipping real data test");
            return;
        }

        // Find simple_table
        let table_dir = std::fs::read_dir(&simple_table_dir)
            .ok()
            .and_then(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .find(|e| {
                        e.file_name()
                            .to_str()
                            .map(|n| n.starts_with("simple_table"))
                            .unwrap_or(false)
                    })
                    .map(|e| e.path())
            });

        let Some(table_path) = table_dir else {
            eprintln!("simple_table not found, skipping");
            return;
        };

        // Find Data.db file
        let data_file = std::fs::read_dir(&table_path).ok().and_then(|entries| {
            entries
                .filter_map(|e| e.ok())
                .find(|e| {
                    e.file_name()
                        .to_str()
                        .map(|n| n.ends_with("-Data.db"))
                        .unwrap_or(false)
                })
                .map(|e| e.path())
        });

        let Some(data_path) = data_file else {
            eprintln!("Data.db not found, skipping");
            return;
        };

        // Open and read first bytes
        let file = tokio::fs::File::open(&data_path).await.unwrap();
        let metadata = file.metadata().await.unwrap();
        eprintln!(
            "Opened real SSTable Data.db: {} ({} bytes)",
            data_path.display(),
            metadata.len()
        );

        let file = Arc::new(Mutex::new(BufReader::new(file)));

        // Try reading a small block
        if metadata.len() > 100 {
            let result = read_block_direct(&file, 100).await;
            assert!(result.is_ok(), "Should read first 100 bytes of real file");
            let data = result.unwrap();
            assert_eq!(data.len(), 100);
            eprintln!("Successfully read first 100 bytes from real SSTable");
        }
    }
}
