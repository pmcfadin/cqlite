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

    // Read block header with format-specific handling
    let block_header = match cassandra_version {
        crate::parser::header::CassandraVersion::V5_0NewBig
        | crate::parser::header::CassandraVersion::V5_0DataFormat => {
            eprintln!("[DEBUG block_io::read_next_block_impl] Using NB format block header reader");
            read_nb_format_block_header(file, compression_info, current_chunk_index).await?
        }
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

/// Read block header for NB format (Cassandra 5.0 new big format)
///
/// Cassandra 5.0 "nb" format uses chunked compression with metadata in CompressionInfo.db.
/// This function reads one chunk at a time using the chunk boundaries from CompressionInfo.
/// For uncompressed files, it reads in fixed-size blocks to avoid the 64MB limit.
async fn read_nb_format_block_header(
    file: &Arc<Mutex<BufReader<File>>>,
    compression_info: &Option<Arc<crate::storage::sstable::compression_info::CompressionInfo>>,
    current_chunk_index: &std::sync::atomic::AtomicUsize,
) -> Result<Option<(u32, u32, u64)>> {
    let current_pos = {
        let mut file_guard = file.lock().await;
        file_guard.stream_position().await.unwrap_or(0)
    };

    eprintln!(
        "[DEBUG read_nb_format_block_header] Current file position: {}",
        current_pos
    );

    // Check file size for EOF detection
    let file_size = {
        let mut file_guard = file.lock().await;
        file_guard.seek(std::io::SeekFrom::End(0)).await?;
        let size = file_guard.stream_position().await?;
        file_guard
            .seek(std::io::SeekFrom::Start(current_pos))
            .await?;
        size
    };

    if current_pos >= file_size {
        eprintln!("[DEBUG read_nb_format_block_header] At EOF");
        return Ok(None);
    }

    // Use CompressionInfo for chunked reading if available
    if let Some(comp_info) = compression_info {
        let chunk_idx = current_chunk_index.load(std::sync::atomic::Ordering::Relaxed);

        eprintln!(
            "[DEBUG read_nb_format_block_header] Using chunked reading: chunk_index={}",
            chunk_idx
        );

        // Check if we've read all chunks
        if chunk_idx >= comp_info.chunk_offsets.len() {
            eprintln!(
                "[DEBUG read_nb_format_block_header] All chunks read ({}/{})",
                chunk_idx,
                comp_info.chunk_offsets.len()
            );
            return Ok(None);
        }

        // Get chunk size from CompressionInfo
        let chunk_size = comp_info
            .compressed_chunk_size(chunk_idx, file_size)
            .ok_or_else(|| {
                Error::corruption(format!(
                    "Failed to get compressed chunk size for chunk {} (total chunks: {})",
                    chunk_idx,
                    comp_info.chunk_offsets.len()
                ))
            })?;

        eprintln!(
            "[DEBUG read_nb_format_block_header] Chunk {}: size={} bytes",
            chunk_idx, chunk_size
        );

        // Increment chunk index for next call
        current_chunk_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Return chunk size (checksum=0 for chunk-level reading)
        Ok(Some((chunk_size as u32, 0, current_pos)))
    } else {
        // Uncompressed: read in fixed-size blocks (64KB max to stay under limit)
        let remaining_size = (file_size - current_pos) as u32;
        let block_size = std::cmp::min(remaining_size, 65536); // 64KB blocks

        if block_size == 0 {
            return Ok(None);
        }

        eprintln!(
            "[DEBUG read_nb_format_block_header] Uncompressed: reading {} byte block",
            block_size
        );

        Ok(Some((block_size, 0, current_pos)))
    }
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
