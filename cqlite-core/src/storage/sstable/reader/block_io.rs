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
) -> Result<Option<Vec<u8>>> {
    read_next_block_with_retry(
        file,
        cassandra_version,
        config,
        compression_info,
        current_chunk_index,
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
        )
        .await
        {
            Ok(result) => return Ok(result),
            Err(e) => {
                retry_count += 1;
                if retry_count >= max_retries {
                    eprintln!("Failed to read block after {} retries: {}", max_retries, e);
                    return Err(e);
                }

                eprintln!(
                    "Block read failed (attempt {}/{}): {}, retrying...",
                    retry_count, max_retries, e
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
) -> Result<Option<Vec<u8>>> {
    eprintln!("[DEBUG block_io::read_next_block_impl] Starting block read");
    eprintln!(
        "[DEBUG block_io::read_next_block_impl] Cassandra version: {:?}",
        cassandra_version
    );

    // NB format uses ChunkReader logic - returns compressed chunk data directly
    match cassandra_version {
        crate::parser::header::CassandraVersion::V5_0NewBig
        | crate::parser::header::CassandraVersion::V5_0DataFormat
        | crate::parser::header::CassandraVersion::V5_0FormatC
        | crate::parser::header::CassandraVersion::V5_0FormatD
        | crate::parser::header::CassandraVersion::V5_0FormatE
        | crate::parser::header::CassandraVersion::V5_0FormatF
        | crate::parser::header::CassandraVersion::V5_0FormatG => {
            eprintln!("[DEBUG block_io::read_next_block_impl] Using NB format chunk reader");

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
            return read_nb_format_chunk_data(
                file,
                compression_info,
                current_chunk_index,
                file_size,
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
            eprintln!(
                "[DEBUG block_io::read_next_block_impl] Using BTI format block header reader"
            );
            read_bti_format_block_header(file).await?
        }
        _ => {
            eprintln!(
                "[DEBUG block_io::read_next_block_impl] Using legacy format block header reader"
            );
            read_legacy_format_block_header(file).await?
        }
    };

    let Some((compressed_size, checksum, current_pos)) = block_header else {
        eprintln!("[DEBUG block_io::read_next_block_impl] Block header returned None (EOF)");
        return Ok(None); // EOF
    };

    eprintln!("[DEBUG block_io::read_next_block_impl] Block header: compressed_size={}, checksum={}, pos={}",
              compressed_size, checksum, current_pos);

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
        println!("Encountered empty block at position {}", current_pos);
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
        println!("Block checksum validated: 0x{:08x}", checksum);
    }

    println!(
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
async fn read_nb_format_chunk_data(
    file: &Arc<Mutex<BufReader<File>>>,
    compression_info: &Option<Arc<crate::storage::sstable::compression_info::CompressionInfo>>,
    current_chunk_index: &std::sync::atomic::AtomicUsize,
    file_size: u64,
) -> Result<Option<Vec<u8>>> {
    eprintln!("[DEBUG read_nb_format_chunk_data] Starting chunk read");

    // Must have CompressionInfo for NB format
    let Some(comp_info) = compression_info else {
        return Err(Error::InvalidFormat(
            "NB format requires CompressionInfo.db but none was loaded".to_string(),
        ));
    };

    let chunk_idx = current_chunk_index.load(std::sync::atomic::Ordering::Relaxed);

    // Check if all chunks read
    if chunk_idx >= comp_info.chunk_offsets.len() {
        eprintln!(
            "[DEBUG read_nb_format_chunk_data] All chunks read ({}/{})",
            chunk_idx,
            comp_info.chunk_offsets.len()
        );
        return Ok(None); // EOF
    }

    eprintln!(
        "[DEBUG read_nb_format_chunk_data] Reading chunk {}/{}",
        chunk_idx,
        comp_info.chunk_offsets.len()
    );

    // Get chunk offset from CompressionInfo
    let chunk_offset = comp_info
        .compressed_chunk_offset(chunk_idx)
        .ok_or_else(|| Error::InvalidFormat(format!("No offset for chunk {}", chunk_idx)))?;

    eprintln!(
        "[DEBUG read_nb_format_chunk_data] Chunk {} offset: 0x{:x}",
        chunk_idx, chunk_offset
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

    eprintln!(
        "[DEBUG read_nb_format_chunk_data] Chunk {} total_size={}, data_size={}, offset=0x{:x}",
        chunk_idx, total_chunk_size, chunk_data_size, chunk_offset
    );

    // Read chunk data and CRC32 from file
    let (chunk_data, expected_crc) = {
        let mut file_guard = file.lock().await;

        // Seek to chunk offset
        file_guard
            .seek(std::io::SeekFrom::Start(chunk_offset))
            .await
            .map_err(|e| {
                Error::Io(std::io::Error::new(
                    e.kind(),
                    format!(
                        "Failed to seek to chunk {} at offset 0x{:x}: {}",
                        chunk_idx, chunk_offset, e
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

    eprintln!(
        "[DEBUG read_nb_format_chunk_data] CRC32 validated for chunk {}: 0x{:08x}",
        chunk_idx, expected_crc
    );
    eprintln!(
        "[DEBUG read_nb_format_chunk_data] Successfully read chunk {}: {} bytes (compressed)",
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

    println!(
        "Reading large block ({} bytes) using streaming with {} byte buffer",
        size, buffer_size
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
