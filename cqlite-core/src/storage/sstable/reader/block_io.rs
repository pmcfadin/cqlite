//! Block I/O operations for SSTable readers.
//!
//! This module handles:
//! - Block header parsing for different Cassandra formats (NB, BTI, Legacy)
//! - Block data reading (direct and streaming for large blocks)
//! - Retry logic for transient I/O errors

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Mutex;

use super::crc::CrcDb;
use super::header::{detect_ascii_header_corruption, is_ascii_corruption_value};
use super::source::BlockSource;
use super::types::SSTableReaderConfig;
use crate::{Error, Result};

/// Maximum bytes returned by a single *piecewise* `read_uncompressed_data_block`
/// call.
///
/// An uncompressed NB SSTable (CQLite's own write output has no CompressionInfo.db)
/// has no chunk boundaries to read against. Returning the WHOLE data section in
/// one `Vec` makes every stitching consumer's working set scale with the file
/// size — defeating the bounded sliding-window compaction read (issue #827).
///
/// So for the **stitching** consumers (NB-without-CompressionInfo:
/// `stitch_all_chunks`, `stream_all_partitions_for_compaction`) this path yields
/// the data section in fixed-size pieces across successive `read_next_block`
/// calls (advancing the file's stream position). Those consumers concatenate
/// pieces and drain whole partitions out of the front, so a partition straddling
/// a piece boundary is handled by the same NeedMore refill logic as a real
/// compression chunk. The value mirrors Cassandra's default 64 KiB compression
/// chunk so behaviour is uniform across compressed and uncompressed inputs.
///
/// CRITICAL (issue #827 Finding 2): the piecewise split is applied ONLY to those
/// stitching consumers. The `V5_0Uncompressed` format is NOT stitched — its
/// callers (`iterate_all_partitions`, `sequential_scan`) parse each returned
/// block as a SELF-CONTAINED unit. Handing them a 64 KiB piece would truncate any
/// partition/row crossing a piece boundary (silent drop/corruption). Those
/// callers therefore receive the ENTIRE data section as one CONTIGUOUS buffer
/// (`piecewise = false`), exactly as before the #827 change.
const UNCOMPRESSED_READ_PIECE_BYTES: usize = 64 * 1024;

/// Read next block with enhanced error handling and streaming support
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_next_block(
    file: &Arc<Mutex<BlockSource>>,
    cassandra_version: &crate::parser::header::CassandraVersion,
    config: &SSTableReaderConfig,
    compression_info: &Option<Arc<crate::storage::sstable::compression_info::CompressionInfo>>,
    crc_reader: Option<&CrcDb>,
    current_chunk_index: &std::sync::atomic::AtomicUsize,
    header_offset: u64,
) -> Result<Option<Vec<u8>>> {
    read_next_block_with_retry(
        file,
        cassandra_version,
        config,
        compression_info,
        crc_reader,
        current_chunk_index,
        header_offset,
        3,
    )
    .await
}

/// Read block with retry logic for handling transient I/O errors
#[allow(clippy::too_many_arguments)]
async fn read_next_block_with_retry(
    file: &Arc<Mutex<BlockSource>>,
    cassandra_version: &crate::parser::header::CassandraVersion,
    config: &SSTableReaderConfig,
    compression_info: &Option<Arc<crate::storage::sstable::compression_info::CompressionInfo>>,
    crc_reader: Option<&CrcDb>,
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
            crc_reader,
            current_chunk_index,
            header_offset,
        )
        .await
        {
            Ok(result) => return Ok(result),
            Err(e) => {
                // Never retry a non-recoverable error (issue #1396). Retries exist
                // for TRANSIENT I/O faults; a CRC/corruption error is deterministic
                // and non-recoverable. Critically, the uncompressed piecewise read
                // advances the stream position BEFORE verifying, so retrying a
                // failed CRC would silently skip the corrupt piece and return the
                // NEXT one — turning fail-fast corruption into a silent truncation.
                // Surface it immediately instead.
                if !e.is_recoverable() {
                    return Err(e);
                }
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
#[allow(clippy::too_many_arguments)]
async fn read_next_block_impl(
    file: &Arc<Mutex<BlockSource>>,
    cassandra_version: &crate::parser::header::CassandraVersion,
    config: &SSTableReaderConfig,
    compression_info: &Option<Arc<crate::storage::sstable::compression_info::CompressionInfo>>,
    crc_reader: Option<&CrcDb>,
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
        // V5_0Uncompressed is NOT stitched: its callers parse each returned block
        // as a self-contained unit, so return the whole data section contiguously
        // (issue #827 Finding 2). Piecewise here would silently truncate any
        // partition/row crossing a 64 KiB boundary.
        //
        // Read-time CRC verification (issue #1396): every returned chunk is
        // verified against CRC.db (default-on) when a CRC.db is present.
        return read_uncompressed_data_block(file, config, false, crc_reader).await;
    }

    // Issue #831: BTI ("da") Data.db is chunk-compressed exactly like NB — the
    // chunk offsets live in CompressionInfo.db and the file is a stream of
    // LZ4-compressed chunks (each followed by a 4-byte CRC32), NOT a sequence of
    // self-describing 12-byte block headers. When CompressionInfo is present,
    // route BTI through the same CompressionInfo-driven chunk reader as NB rather
    // than the (incorrect) block-header reader below. Without CompressionInfo, an
    // uncompressed BTI Data.db is read directly.
    let is_bti = matches!(
        cassandra_version,
        crate::parser::header::CassandraVersion::V5_0Bti
    );
    if is_bti && compression_info.is_none() {
        log::debug!("block_io::read_next_block_impl: BTI without CompressionInfo, direct read");
        // BTI direct read is parsed as a self-contained unit (like V5_0Uncompressed
        // above), so return the whole data section contiguously (issue #827 Finding 2):
        // piecewise here would truncate any partition/row crossing a 64 KiB boundary.
        // BTI ships no CRC.db, so `crc_reader` is `None` here (issue #1396).
        return read_uncompressed_data_block(file, config, false, crc_reader).await;
    }

    if cassandra_version.is_nb_format() || is_bti {
        log::debug!("block_io::read_next_block_impl: Using NB/BTI format chunk reader");

        // File size for chunk-size calculation. The SSTable is immutable, so this
        // reads the cached length instead of re-deriving it with a seek(End)/back
        // probe on every chunk (issue #1586).
        let file_size = {
            let mut file_guard = file.lock().await;
            file_guard.len().await?
        };

        // Read chunk with CRC validation
        // Note: For NB format files, CompressionInfo chunk offsets are always relative
        // to the start of the Data.db file (offset 0). Any embedded SSTable header is
        // part of the compressed data, not a separate uncompressed prefix.
        // Therefore, we always use header_offset=0 for NB format chunk reading.
        return read_nb_format_chunk_data(
            file,
            config,
            compression_info,
            crc_reader,
            current_chunk_index,
            file_size,
            0, // NB format: chunk offsets are relative to file start
        )
        .await;
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
#[allow(clippy::too_many_arguments)]
async fn read_nb_format_chunk_data(
    file: &Arc<Mutex<BlockSource>>,
    config: &SSTableReaderConfig,
    compression_info: &Option<Arc<crate::storage::sstable::compression_info::CompressionInfo>>,
    crc_reader: Option<&CrcDb>,
    current_chunk_index: &std::sync::atomic::AtomicUsize,
    file_size: u64,
    header_offset: u64,
) -> Result<Option<Vec<u8>>> {
    log::debug!("read_nb_format_chunk_data: Starting chunk read");

    // If no CompressionInfo.db, the NB format SSTable is uncompressed.
    // Fall back to reading raw data directly (same as V5_0Uncompressed).
    let Some(comp_info) = compression_info else {
        log::debug!(
            "read_nb_format_chunk_data: No CompressionInfo.db, falling back to raw data read"
        );
        // NB-without-CompressionInfo IS stitched (requires_chunk_stitching() is
        // true for NB format): the sliding-window stitchers reassemble pieces and
        // handle NeedMore across boundaries, so piecewise reads keep their working
        // set bounded (issue #827) without truncating partitions.
        //
        // Read-time CRC verification (issue #1396): CQLite's own uncompressed `nb`
        // write output ships a CRC.db (#1197); when present, `crc_reader` is `Some`
        // and each piece is verified on chunk_size-aligned boundaries.
        return read_uncompressed_data_block(file, config, true, crc_reader).await;
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

    // Bounds-check the chunk against the actual Data.db length BEFORE allocating.
    // A corrupt CompressionInfo.db offset (e.g. an MSB-set value that survived
    // the ascending check) makes `compressed_chunk_size` derive a multi-exabyte
    // length from adjacent offsets; `vec![0u8; chunk_data_size]` below would then
    // panic/OOM. Reject instead, so a corrupt offset surfaces as a recoverable
    // error rather than crashing the reader/verifier (roborev #970).
    let chunk_end = chunk_offset
        .checked_add(header_offset)
        .and_then(|abs| abs.checked_add(total_chunk_size));
    match chunk_end {
        Some(end) if end <= file_size => {}
        _ => {
            return Err(Error::InvalidFormat(format!(
                "Chunk {} at offset 0x{:x} with size {} exceeds Data.db length {} \
                 — corrupt CompressionInfo.db chunk offset",
                chunk_idx, chunk_offset, total_chunk_size, file_size
            )));
        }
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
        // A5 read-work counter (SEEK_CALLS; consumer E4): one per block-read seek in
        // the production compressed-chunk read path. No-op in release (design.md
        // Decision 1/2).
        crate::storage::sstable::read_work_counters::record_seek();
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
    file: &Arc<Mutex<BlockSource>>,
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
    file: &Arc<Mutex<BlockSource>>,
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
async fn read_block_direct(file: &Arc<Mutex<BlockSource>>, size: usize) -> Result<Vec<u8>> {
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

/// Read exactly `size` bytes from `reader` into a freshly allocated `Vec`, using
/// a reusable scratch buffer capped at `buffer_size`.
///
/// The point of this helper is the *allocation shape* (Issue #592): the only
/// allocation that scales with `size` is the returned buffer the caller asked
/// for. The transient read scratch is bounded to `buffer_size` regardless of how
/// large `size` is, so reading a large block never requires a second
/// file-sized working buffer (and we avoid the redundant zero-initialization of
/// a `vec![0u8; size]`). The loop yields periodically so a large read does not
/// starve other tasks on the runtime.
async fn read_into_vec_capped<R>(
    reader: &mut R,
    size: usize,
    buffer_size: usize,
) -> std::io::Result<Vec<u8>>
where
    R: AsyncReadExt + Unpin,
{
    let mut out = Vec::with_capacity(size);
    if size == 0 {
        return Ok(out);
    }
    // Cap the scratch buffer to `buffer_size` but never exceed `size` (no point
    // allocating a buffer larger than the data) and never below 1 byte.
    let cap = buffer_size.clamp(1, size);
    let mut scratch = vec![0u8; cap];
    let mut remaining = size;

    while remaining > 0 {
        let to_read = remaining.min(cap);
        reader.read_exact(&mut scratch[..to_read]).await?;
        out.extend_from_slice(&scratch[..to_read]);
        remaining -= to_read;

        // Allow other tasks to run during large reads.
        if remaining > 0 && out.len() % (1024 * 1024) == 0 {
            tokio::task::yield_now().await;
        }
    }

    Ok(out)
}

/// Read large block using streaming I/O to reduce memory pressure
async fn read_large_block_streaming(
    file: &Arc<Mutex<BlockSource>>,
    size: usize,
    config: &SSTableReaderConfig,
) -> Result<Vec<u8>> {
    let buffer_size = config.read_buffer_size.min(size.max(1));
    log::info!(
        "Reading large block ({} bytes) using streaming with {} byte buffer",
        size,
        buffer_size
    );

    let mut file_guard = file.lock().await;
    read_into_vec_capped(&mut *file_guard, size, config.read_buffer_size)
        .await
        .map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Failed to read block chunk: {}",
                e
            )))
        })
}

/// Read uncompressed data block (no compression, no block headers): the data
/// section after the file header is raw partition data.
///
/// `piecewise` selects the return contract (issue #827 Finding 2):
///
/// - `false` (DEFAULT for `V5_0Uncompressed`): return the ENTIRE remaining data
///   section as one CONTIGUOUS buffer. Non-stitching callers
///   (`iterate_all_partitions`, `sequential_scan`) parse each returned block as a
///   self-contained unit, so they MUST receive a complete unit — a partition or
///   row crossing a 64 KiB boundary would otherwise be parsed as truncated and
///   silently dropped/corrupted.
/// - `true` (for NB-without-CompressionInfo stitching callers): return at most
///   one [`UNCOMPRESSED_READ_PIECE_BYTES`] piece per call, advancing the file's
///   stream position so successive calls walk the section. Only the sliding-
///   window stitchers (which reassemble across pieces and handle `NeedMore`) use
///   this, keeping their working set bounded regardless of file size. When a
///   `crc_reader` is present the piece is instead sized to end on a CRC-chunk
///   boundary (see below) so every full CRC chunk is verified exactly once; that
///   can enlarge the piece by at most one (bounded) CRC chunk.
///
/// In BOTH modes the *read itself* streams through a capped scratch buffer
/// (`config.read_buffer_size`) rather than allocating and zeroing a second
/// file-sized buffer up front. See [`read_into_vec_capped`] and Issue #592.
async fn read_uncompressed_data_block(
    file: &Arc<Mutex<BlockSource>>,
    config: &SSTableReaderConfig,
    piecewise: bool,
    crc_reader: Option<&CrcDb>,
) -> Result<Option<Vec<u8>>> {
    let (current_pos, file_size) = {
        let mut file_guard = file.lock().await;
        let current = file_guard.stream_position().await.map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Failed to get stream position: {}",
                e
            )))
        })?;

        // File size from the cached immutable length — no seek(End)/back probe on
        // every piece read (issue #1586).
        let size = file_guard.len().await.map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "Failed to get file size: {}",
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

    // Piecewise (stitching callers): yield at most one fixed-size piece per call
    // so the sliding-window stitch buffer stays bounded regardless of file size
    // (issue #827). The file's stream position advances by the bytes read, so the
    // next call returns the next piece and EOF is reached naturally.
    //
    // Contiguous (V5_0Uncompressed non-stitching callers, Finding 2): return the
    // WHOLE remaining section so the block is a complete, self-contained parse
    // unit and no partition/row is truncated at a piece boundary.
    let to_read = if piecewise {
        match crc_reader {
            // With a CRC.db present (issue #1396) the piece MUST end on a CRC-chunk
            // boundary (or EOF) so every full CRC chunk lands entirely inside
            // exactly one returned piece and is verified before its bytes are
            // emitted. A fixed 64 KiB piece would straddle any chunk larger than
            // 64 KiB, and `verify_uncompressed_chunks` only checks chunks FULLY
            // contained in the buffer — so such a chunk would be silently skipped
            // and corruption returned unverified. We read at least one full chunk
            // (or the ~64 KiB target, whichever is larger) and round the piece end
            // UP to the next chunk boundary; successive pieces then start
            // chunk-aligned. Alignment enlarges the piece by at most one CRC chunk,
            // which `MAX_CRC_CHUNK_SIZE` (Fix 2) bounds, keeping memory bounded.
            Some(crc) => {
                let cs = crc.chunk_size() as u64; // > 0 and bounded (CrcDb::parse validates)
                let want = (UNCOMPRESSED_READ_PIECE_BYTES as u64).max(cs);
                let target_end = current_pos.saturating_add(want);
                let aligned_end = target_end.div_ceil(cs).saturating_mul(cs).min(file_size);
                // aligned_end > current_pos (want >= one chunk), capped at file_size,
                // so this never exceeds `remaining` and is always >= 1.
                (aligned_end - current_pos) as usize
            }
            None => remaining.min(UNCOMPRESSED_READ_PIECE_BYTES),
        }
    } else {
        remaining
    };

    log::debug!(
        "read_uncompressed_data_block: Reading {} of {} remaining bytes from position {}",
        to_read,
        remaining,
        current_pos
    );

    // Read the piece through a capped scratch buffer so the transient working
    // set does not scale with the file size (Issue #592).
    let data = {
        let mut file_guard = file.lock().await;
        read_into_vec_capped(&mut *file_guard, to_read, config.read_buffer_size)
            .await
            .map_err(|e| {
                Error::Io(std::io::Error::other(format!(
                    "Failed to read uncompressed data block ({} bytes): {}",
                    to_read, e
                )))
            })?
    };

    log::debug!(
        "read_uncompressed_data_block: Successfully read {} bytes",
        data.len()
    );

    // Read-time CRC verification (issue #1396), default-on and unconditional when
    // a CRC.db is present (Cassandra writes one for every uncompressed BIG
    // SSTable). Verify every fully-covered chunk_size-aligned block of the returned
    // bytes against the authoritative stored CRC32. A mismatch is a typed,
    // non-recoverable corruption error naming the chunk index + Data.db offset —
    // never returns the corrupt bytes / wrong values / a silent empty result. The
    // compressed path is unaffected (it uses its own inline per-chunk CRC).
    if let Some(crc) = crc_reader {
        verify_uncompressed_chunks(file, crc, &data, current_pos, file_size).await?;
    }

    Ok(Some(data))
}

/// Verify EVERY `CRC.db` chunk that overlaps a just-read uncompressed Data.db
/// range `[start_offset, start_offset + data.len())` against the authoritative
/// `CRC.db` (issue #1396).
///
/// Verification is done on `chunk_size` boundaries independent of the read-piece
/// size (`UNCOMPRESSED_READ_PIECE_BYTES` and `CRC_CHUNK_SIZE` may differ). A
/// `CRC.db` chunk covers the WHOLE Data.db byte range `[c*cs, min((c+1)*cs,
/// file_size))` indexed from Data.db offset 0. Because sequential reads begin at
/// `actual_header_size` (NOT necessarily a chunk boundary), the FIRST overlapping
/// chunk can start before `start_offset`: its prefix bytes (the header region)
/// are not in the returned buffer. A previous version verified only chunks
/// *fully contained* in the buffer and therefore SKIPPED that first chunk,
/// returning corruption in its data bytes UNVERIFIED (soundness bug, Fix 1).
///
/// To close that gap this now verifies every overlapping chunk regardless of the
/// start offset: for each chunk it assembles the full `[lo, hi)` block from the
/// resident `data` (the overlapping middle) plus any missing prefix `[lo,
/// start_offset)` or suffix `[end, hi)` READ from `file`, then checks the CRC32
/// over the complete chunk. In the common sequential case only the first chunk's
/// header prefix is ever read from disk; the file position is restored to `end`
/// afterwards so the caller's subsequent piecewise reads continue unaffected.
///
/// The final short chunk is verified once its end reaches `file_size`. A chunk
/// whose CRC entry is missing from a truncated `CRC.db` is a typed error (via
/// [`CrcDb::crc_for_chunk`]); the harmless trailing compaction empty-final-chunk
/// entry (issue #1222) maps beyond `file_size` and is never queried.
///
/// Memory: at most one `chunk_size` block is materialised at a time (bounded by
/// `MAX_CRC_CHUNK_SIZE`) — no new Data.db-file-sized allocation (issue #1396
/// memory budget).
async fn verify_uncompressed_chunks(
    file: &Arc<Mutex<BlockSource>>,
    crc: &CrcDb,
    data: &[u8],
    start_offset: u64,
    file_size: u64,
) -> Result<()> {
    let cs = crc.chunk_size() as u64;
    if cs == 0 {
        return Err(Error::corruption(
            "CRC.db chunk size is zero; cannot verify uncompressed chunks",
        ));
    }
    if data.is_empty() {
        return Ok(());
    }
    // `data.len()` is bounded by the read-piece cap; `start_offset` is a real
    // file position — overflow is implausible, but saturate to stay panic-free.
    let end = start_offset.saturating_add(data.len() as u64);
    let first = start_offset / cs;
    // `end > start_offset` (data non-empty), so `end - 1` never underflows.
    let last = (end - 1) / cs;

    let mut did_seek = false;
    for chunk in first..=last {
        let lo = chunk.saturating_mul(cs);
        // True Data.db byte range of this chunk (final chunk is short).
        let hi = ((chunk + 1).saturating_mul(cs)).min(file_size);
        if hi <= lo {
            break; // chunk begins at/after EOF; nothing real to verify
        }

        // Assemble the WHOLE chunk [lo, hi): the part inside [start_offset, end)
        // comes from the resident `data`; any missing prefix/suffix is read from
        // the file so this chunk is fully verified regardless of where the read
        // began (Fix 1: the first chunk was previously skipped).
        let mut whole = Vec::with_capacity((hi - lo) as usize);
        let pre_hi = start_offset.min(hi);
        if lo < pre_hi {
            read_range_into(file, lo, pre_hi, &mut whole).await?;
            did_seek = true;
        }
        let mid_lo = lo.max(start_offset);
        let mid_hi = hi.min(end);
        if mid_lo < mid_hi {
            whole.extend_from_slice(
                &data[(mid_lo - start_offset) as usize..(mid_hi - start_offset) as usize],
            );
        }
        let suf_lo = end.max(lo);
        if suf_lo < hi {
            read_range_into(file, suf_lo, hi, &mut whole).await?;
            did_seek = true;
        }
        debug_assert_eq!(whole.len() as u64, hi - lo);

        let computed = crc32fast::hash(&whole);
        let expected = crc.crc_for_chunk(chunk as usize)?;
        if computed != expected {
            return Err(Error::corruption(format!(
                "uncompressed CRC32 mismatch for chunk {} at Data.db offset 0x{:x} \
                 ({} bytes): expected=0x{:08x}, computed=0x{:08x} (CRC.db)",
                chunk,
                lo,
                hi - lo,
                expected,
                computed
            )));
        }
    }

    // Restore the file position to `end` (where the caller's main read left it)
    // if any completing read moved it, so sequential piecewise reads continue.
    if did_seek {
        let mut guard = file.lock().await;
        guard
            .seek(std::io::SeekFrom::Start(end))
            .await
            .map_err(|e| {
                Error::Io(std::io::Error::other(format!(
                    "failed to restore Data.db position after CRC verification: {e}"
                )))
            })?;
    }
    Ok(())
}

/// Read the Data.db byte range `[lo, hi)` from `file` and append it to `out`.
///
/// Used by [`verify_uncompressed_chunks`] to complete a CRC chunk whose prefix
/// (header region) or suffix is not present in the just-read buffer. The
/// allocation is bounded by one `chunk_size` block (`MAX_CRC_CHUNK_SIZE`).
async fn read_range_into(
    file: &Arc<Mutex<BlockSource>>,
    lo: u64,
    hi: u64,
    out: &mut Vec<u8>,
) -> Result<()> {
    let mut buf = vec![0u8; (hi - lo) as usize];
    let mut guard = file.lock().await;
    guard
        .seek(std::io::SeekFrom::Start(lo))
        .await
        .map_err(|e| {
            Error::Io(std::io::Error::other(format!(
                "failed to seek Data.db to 0x{lo:x} for CRC chunk completion: {e}"
            )))
        })?;
    guard.read_exact(&mut buf).await.map_err(|e| {
        Error::corruption(format!(
            "failed to read Data.db bytes [0x{lo:x}, 0x{hi:x}) for CRC verification: {e}"
        ))
    })?;
    out.extend_from_slice(&buf);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;
    use tempfile::TempDir;

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
        let temp_dir = TempDir::new().expect("create temp dir");
        let temp_file = temp_dir.path().join("test_empty_block.bin");

        // Create empty file
        tokio::fs::write(&temp_file, b"").await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BlockSource::buffered(file)));

        let result = read_block_direct(&file, 0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_read_block_direct_small() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let temp_file = temp_dir.path().join("test_small_block.bin");

        // Create test file with known content
        let test_data = b"Hello, World! This is test data.";
        tokio::fs::write(&temp_file, test_data).await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BlockSource::buffered(file)));

        let result = read_block_direct(&file, test_data.len()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), test_data);
    }

    #[tokio::test]
    async fn test_read_uncompressed_data_block() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let temp_file = temp_dir.path().join("test_uncompressed_block.bin");

        // Create test file
        let test_data = b"Uncompressed test data block content";
        tokio::fs::write(&temp_file, test_data).await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BlockSource::buffered(file)));

        let config = SSTableReaderConfig::default();
        // Contiguous (V5_0Uncompressed non-stitching) read.
        let result = read_uncompressed_data_block(&file, &config, false, None).await;
        assert!(result.is_ok());

        let data = result.unwrap();
        assert!(data.is_some());
        assert_eq!(data.unwrap(), test_data);
    }

    #[tokio::test]
    async fn test_read_uncompressed_data_block_eof() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let temp_file = temp_dir.path().join("test_uncompressed_eof.bin");

        // Create empty file
        tokio::fs::write(&temp_file, b"").await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BlockSource::buffered(file)));

        // Should return None for EOF
        let config = SSTableReaderConfig::default();
        let result = read_uncompressed_data_block(&file, &config, false, None).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    // ========================================================================
    // Uncompressed read-time CRC verification (issue #1396)
    // ========================================================================

    fn synth_crc_db(chunk_size: u32, crcs: &[u32]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&(chunk_size as i32).to_be_bytes());
        for c in crcs {
            v.extend_from_slice(&c.to_be_bytes());
        }
        v
    }

    /// Build an `Arc<Mutex<BlockSource>>` over `bytes` for the verifier tests.
    /// The returned `TempDir` MUST be held for the source's lifetime.
    async fn blocksource_from(bytes: &[u8]) -> (TempDir, Arc<Mutex<BlockSource>>) {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("data.bin");
        tokio::fs::write(&path, bytes).await.expect("write data");
        let file = tokio::fs::File::open(&path).await.expect("open data");
        (dir, Arc::new(Mutex::new(BlockSource::buffered(file))))
    }

    #[tokio::test]
    async fn verify_uncompressed_chunks_clean_multichunk_passes() {
        // Chunk size must be >= MIN_CRC_CHUNK_SIZE (4096, issue #1396 floor) so
        // the synthetic CRC.db parses. 2.5 chunks -> 3 CRC entries.
        let cs = 4096u32;
        let csz = cs as usize;
        let size = csz * 2 + csz / 2; // chunks [0,cs),[cs,2cs),[2cs,size)
        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let crcs = [
            crc32fast::hash(&data[0..csz]),
            crc32fast::hash(&data[csz..2 * csz]),
            crc32fast::hash(&data[2 * csz..size]),
        ];
        let crc = CrcDb::parse(&synth_crc_db(cs, &crcs)).expect("parse");
        let (_dir, file) = blocksource_from(&data).await;
        // Whole-file contiguous read starting at offset 0.
        verify_uncompressed_chunks(&file, &crc, &data, 0, data.len() as u64)
            .await
            .expect("clean data verifies");
    }

    #[tokio::test]
    async fn verify_uncompressed_chunks_flip_in_later_chunk_attributed_to_that_chunk() {
        // >= MIN_CRC_CHUNK_SIZE (4096, issue #1396 floor); 3 chunks.
        let cs = 4096u32;
        let csz = cs as usize;
        let size = csz * 2 + csz / 2;
        let mut data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let crcs = [
            crc32fast::hash(&data[0..csz]),
            crc32fast::hash(&data[csz..2 * csz]),
            crc32fast::hash(&data[2 * csz..size]),
        ];
        let crc = CrcDb::parse(&synth_crc_db(cs, &crcs)).expect("parse");
        // Flip a byte inside chunk 1 ([cs, 2cs)).
        data[csz + 100] ^= 0xFF;
        let (_dir, file) = blocksource_from(&data).await;
        let err = verify_uncompressed_chunks(&file, &crc, &data, 0, data.len() as u64)
            .await
            .expect_err("corrupt chunk must error");
        let msg = err.to_string();
        assert!(
            matches!(err, Error::Corruption(_)),
            "typed corruption: {msg}"
        );
        assert!(msg.contains("chunk 1"), "must name chunk 1: {msg}");
        // chunk 1 starts at Data.db offset 4096 == 0x1000.
        assert!(
            msg.contains("0x1000"),
            "must name the Data.db offset 0x1000: {msg}"
        );
    }

    #[tokio::test]
    async fn verify_uncompressed_chunks_truncated_crc_db_is_typed_error() {
        // >= MIN_CRC_CHUNK_SIZE (4096, issue #1396 floor); needs 3 CRC entries.
        let cs = 4096u32;
        let csz = cs as usize;
        let size = csz * 2 + csz / 2;
        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        // Only provide 1 entry -> chunk 1/2 have no CRC -> truncation error.
        let crc =
            CrcDb::parse(&synth_crc_db(cs, &[crc32fast::hash(&data[0..csz])])).expect("parse");
        let (_dir, file) = blocksource_from(&data).await;
        let err = verify_uncompressed_chunks(&file, &crc, &data, 0, data.len() as u64)
            .await
            .expect_err("truncated CRC.db must error");
        assert!(matches!(err, Error::Corruption(_)), "typed: {err}");
    }

    /// Fix 1 (issue #1396, SOUNDNESS): a sequential uncompressed read that begins
    /// after `actual_header_size` (a non-chunk-boundary offset) must STILL fully
    /// verify CHUNK 0 — the chunk that spans the header region. A byte corrupted
    /// in chunk 0's header prefix `[0, start_offset)` — the bytes that are NOT in
    /// the returned buffer and were previously skipped — must be caught as typed
    /// corruption naming chunk 0. This drives the actual reader wiring
    /// (`read_uncompressed_data_block` with the file pre-seeked to the header
    /// offset), not the verify helper in isolation.
    #[tokio::test]
    async fn header_offset_read_still_verifies_chunk_0_prefix() {
        // >= MIN_CRC_CHUNK_SIZE (4096, issue #1396 floor); 3 chunks.
        let cs = 4096u32;
        let csz = cs as usize;
        let size = csz * 2 + csz / 2;
        let clean: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let crcs = [
            crc32fast::hash(&clean[0..csz]),
            crc32fast::hash(&clean[csz..2 * csz]),
            crc32fast::hash(&clean[2 * csz..size]),
        ];
        let crc = CrcDb::parse(&synth_crc_db(cs, &crcs)).expect("parse");
        let config = SSTableReaderConfig::default();
        let header_size = 3u64; // simulate actual_header_size: read starts mid-chunk-0

        // 1) Clean: a read starting at the header offset verifies chunk 0 (its
        //    prefix [0,3) is read from disk) and every later chunk, returning
        //    [3,20) byte-identical.
        let (dir, file) = blocksource_from(&clean).await;
        {
            let mut g = file.lock().await;
            g.seek(std::io::SeekFrom::Start(header_size)).await.unwrap();
        }
        let piece = read_uncompressed_data_block(&file, &config, false, Some(&crc))
            .await
            .expect("clean header-offset read verifies")
            .expect("non-empty section");
        assert_eq!(piece, clean[3..], "returned bytes are the post-header data");
        drop(dir);

        // 2) Corrupt a byte in chunk 0's HEADER PREFIX [0,3) — a byte the read
        //    buffer never contains. The OLD verifier skipped chunk 0 entirely for
        //    a header-offset read, so this flip was returned UNVERIFIED. It must
        //    now be caught as typed corruption naming chunk 0 (offset 0x0).
        let mut corrupt = clean.clone();
        corrupt[1] ^= 0xFF; // inside [0, header_size)
        let (dir, file) = blocksource_from(&corrupt).await;
        {
            let mut g = file.lock().await;
            g.seek(std::io::SeekFrom::Start(header_size)).await.unwrap();
        }
        let err = read_uncompressed_data_block(&file, &config, false, Some(&crc))
            .await
            .expect_err("corruption in chunk 0's header prefix must be caught, not returned");
        let msg = err.to_string();
        assert!(matches!(err, Error::Corruption(_)), "typed: {msg}");
        assert!(
            msg.contains("chunk 0"),
            "must name chunk 0 (proving it is no longer skipped): {msg}"
        );
        drop(dir);
    }

    #[tokio::test]
    async fn test_read_legacy_format_block_header_eof() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let temp_file = temp_dir.path().join("test_legacy_header_eof.bin");

        // Create file with only 4 bytes (incomplete header)
        tokio::fs::write(&temp_file, &[0x00, 0x00, 0x10, 0x00])
            .await
            .unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BlockSource::buffered(file)));

        // Should return None for incomplete header (EOF)
        let result = read_legacy_format_block_header(&file).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_read_legacy_format_block_header_valid() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let temp_file = temp_dir.path().join("test_legacy_header_valid.bin");

        // Create valid 8-byte header
        let header = [0x00, 0x00, 0x10, 0x00, 0x12, 0x34, 0x56, 0x78];
        tokio::fs::write(&temp_file, &header).await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BlockSource::buffered(file)));

        let result = read_legacy_format_block_header(&file).await;
        assert!(result.is_ok());

        let (size, checksum, pos) = result.unwrap().unwrap();
        assert_eq!(size, 4096);
        assert_eq!(checksum, 0x12345678);
        assert_eq!(pos, 0);
    }

    #[tokio::test]
    async fn test_read_bti_format_block_header_valid() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let temp_file = temp_dir.path().join("test_bti_header_valid.bin");

        // Create valid 12-byte BTI header
        let header = [
            0x00, 0x00, 0x08, 0x00, // size: 2048
            0x00, 0x00, 0x10, 0x00, // uncompressed: 4096
            0xAB, 0xCD, 0xEF, 0x12, // checksum
        ];
        tokio::fs::write(&temp_file, &header).await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BlockSource::buffered(file)));

        let result = read_bti_format_block_header(&file).await;
        assert!(result.is_ok());

        let (size, checksum, pos) = result.unwrap().unwrap();
        assert_eq!(size, 2048);
        assert_eq!(checksum, 0xABCDEF12);
        assert_eq!(pos, 0);
    }

    #[tokio::test]
    async fn test_read_large_block_streaming() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let temp_file = temp_dir.path().join("test_large_block.bin");

        // Create larger test file (128KB)
        let size = 128 * 1024;
        let test_data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        tokio::fs::write(&temp_file, &test_data).await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BlockSource::buffered(file)));

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
    }

    /// Issue #592: the transient read scratch buffer must stay capped at
    /// `buffer_size` no matter how large the block is, so a position-to-EOF read
    /// of a huge uncompressed SSTable never allocates a second file-sized working
    /// buffer (which would blow the <128MB memory target). A regression to
    /// `vec![0u8; size]` + a single `read_exact` would hand the reader a
    /// `size`-sized `ReadBuf` and trip this assertion.
    #[tokio::test]
    async fn read_into_vec_capped_bounds_scratch_buffer() {
        use std::pin::Pin;
        use std::sync::atomic::Ordering;
        use std::task::{Context, Poll};
        use tokio::io::ReadBuf;

        /// A reader that serves `data` and records the largest single read
        /// request (the capacity of the `ReadBuf` handed to each `poll_read`).
        struct MaxReadRecorder {
            data: std::io::Cursor<Vec<u8>>,
            max_request: Arc<AtomicUsize>,
        }

        impl tokio::io::AsyncRead for MaxReadRecorder {
            fn poll_read(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                self.max_request
                    .fetch_max(buf.remaining(), Ordering::Relaxed);
                let pos = self.data.position() as usize;
                let inner = self.data.get_ref();
                let avail = &inner[pos.min(inner.len())..];
                let n = avail.len().min(buf.remaining());
                buf.put_slice(&avail[..n]);
                self.data.set_position((pos + n) as u64);
                Poll::Ready(Ok(()))
            }
        }

        let size = 4 * 1024 * 1024; // 4 MiB block
        let buffer_size = 64 * 1024; // 64 KiB cap (block is 64x larger)
        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let max_request = Arc::new(AtomicUsize::new(0));
        let mut reader = MaxReadRecorder {
            data: std::io::Cursor::new(data.clone()),
            max_request: Arc::clone(&max_request),
        };

        let out = read_into_vec_capped(&mut reader, size, buffer_size)
            .await
            .expect("capped read should succeed");

        // Byte-identical output: only the allocation shape changed.
        assert_eq!(out.len(), size);
        assert_eq!(out, data);

        let observed = max_request.load(Ordering::Relaxed);
        assert!(
            observed <= buffer_size,
            "scratch read request {} exceeded cap {} — allocation is scaling with block size",
            observed,
            buffer_size
        );
    }

    /// Issue #592 + #827: the PIECEWISE `read_uncompressed_data_block` (stitching
    /// callers: NB-without-CompressionInfo) must stream a data section far larger
    /// than both `read_buffer_size` and the per-call piece cap, returning
    /// byte-identical data when the pieces are concatenated, and bounding each
    /// returned piece to `UNCOMPRESSED_READ_PIECE_BYTES` so the sliding-window
    /// compaction read stays memory-bounded.
    #[tokio::test]
    async fn uncompressed_data_block_streams_large_block_byte_identical() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let temp_file = temp_dir.path().join("issue_592_uncompressed_large.bin");

        // 3.5 piece-caps so several pieces plus a short tail are returned.
        let size = UNCOMPRESSED_READ_PIECE_BYTES * 3 + UNCOMPRESSED_READ_PIECE_BYTES / 2;
        let test_data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
        tokio::fs::write(&temp_file, &test_data).await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BlockSource::buffered(file)));

        let config = SSTableReaderConfig {
            read_buffer_size: 8 * 1024, // small buffer forces capped scratch reads
            ..Default::default()
        };

        // piecewise = true: each call returns at most one piece; concatenating all
        // pieces must reproduce the section byte-for-byte. EOF is Ok(None).
        let mut assembled = Vec::new();
        let mut pieces = 0;
        while let Some(piece) = read_uncompressed_data_block(&file, &config, true, None)
            .await
            .expect("read should succeed")
        {
            assert!(
                piece.len() <= UNCOMPRESSED_READ_PIECE_BYTES,
                "piece {} bytes exceeds the {} byte cap — read is not bounded",
                piece.len(),
                UNCOMPRESSED_READ_PIECE_BYTES
            );
            assembled.extend_from_slice(&piece);
            pieces += 1;
        }
        assert_eq!(assembled.len(), size);
        assert_eq!(assembled, test_data);
        assert!(
            pieces >= 4,
            "expected the section to be split into multiple bounded pieces, got {pieces}"
        );
    }

    /// Issue #1396 (soundness / verification-bypass): the PIECEWISE
    /// `read_uncompressed_data_block` must, when a `CRC.db` is present, size each
    /// returned piece so every full CRC chunk lands entirely inside exactly one
    /// piece — even when the CRC chunk size EXCEEDS the 64 KiB read-piece target.
    /// Otherwise a chunk larger than 64 KiB straddles two fixed pieces and
    /// `verify_uncompressed_chunks` (which only checks chunks fully contained in a
    /// single buffer) NEVER verifies it, silently returning corrupt bytes. Here a
    /// synthetic 128 KiB-chunk `CRC.db` (paired with a synthetic Data.db) drives
    /// the piecewise scan surface directly: a clean scan passes, and a single
    /// flipped byte in a >64 KiB chunk is caught as typed corruption naming that
    /// chunk. (A real Cassandra CRC.db always uses 64 KiB, so a >64 KiB fixture
    /// must be synthetic; the assertion is on the actual reader wiring, not the
    /// verify helper in isolation.)
    #[tokio::test]
    async fn piecewise_uncompressed_read_verifies_chunks_larger_than_piece_size() {
        let cs: usize = 128 * 1024; // 2x UNCOMPRESSED_READ_PIECE_BYTES -> every chunk spans >1 piece
        assert!(cs > UNCOMPRESSED_READ_PIECE_BYTES);
        // 2.5 chunks: two full + a short final chunk.
        let size = cs * 2 + cs / 2;
        let clean: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let crcs = [
            crc32fast::hash(&clean[0..cs]),
            crc32fast::hash(&clean[cs..2 * cs]),
            crc32fast::hash(&clean[2 * cs..size]),
        ];
        let crc = CrcDb::parse(&synth_crc_db(cs as u32, &crcs)).expect("parse synthetic CRC.db");

        let config = SSTableReaderConfig::default();
        let temp_dir = TempDir::new().expect("create temp dir");

        // 1) Clean data verifies across all pieces and reassembles byte-identical.
        let clean_path = temp_dir.path().join("issue_1396_clean.bin");
        tokio::fs::write(&clean_path, &clean).await.unwrap();
        let file = tokio::fs::File::open(&clean_path).await.unwrap();
        let file = Arc::new(Mutex::new(BlockSource::buffered(file)));
        let mut assembled = Vec::new();
        while let Some(piece) = read_uncompressed_data_block(&file, &config, true, Some(&crc))
            .await
            .expect("clean piecewise read verifies")
        {
            // Every full chunk in this piece must have been verified, so a piece
            // must be a whole number of chunks (except a final short tail at EOF).
            assembled.extend_from_slice(&piece);
        }
        assert_eq!(
            assembled, clean,
            "clean data must reassemble byte-identical"
        );

        // 2) Flip one byte inside chunk 2 (a >64 KiB chunk). The CRC entries are
        //    the ORIGINAL values, so the piece covering chunk 2 must fail.
        let mut corrupt = clean.clone();
        corrupt[2 * cs + 5] ^= 0xFF;
        let corrupt_path = temp_dir.path().join("issue_1396_corrupt.bin");
        tokio::fs::write(&corrupt_path, &corrupt).await.unwrap();
        let file = tokio::fs::File::open(&corrupt_path).await.unwrap();
        let file = Arc::new(Mutex::new(BlockSource::buffered(file)));
        let mut caught: Option<Error> = None;
        loop {
            match read_uncompressed_data_block(&file, &config, true, Some(&crc)).await {
                Ok(Some(_)) => continue,
                Ok(None) => break,
                Err(e) => {
                    caught = Some(e);
                    break;
                }
            }
        }
        let err = caught.expect("flipped byte in a >64 KiB chunk must be caught, not returned");
        assert!(
            matches!(err, Error::Corruption(_)),
            "typed corruption: {err}"
        );
        assert!(
            err.to_string().contains("chunk 2"),
            "must name the corrupt chunk 2: {err}"
        );
    }

    /// Issue #827 Finding 2: the CONTIGUOUS `read_uncompressed_data_block`
    /// (`piecewise = false`, the `V5_0Uncompressed` non-stitching path) must
    /// return the ENTIRE data section in ONE call even when it far exceeds
    /// `UNCOMPRESSED_READ_PIECE_BYTES`. Non-stitching callers parse each returned
    /// block as a self-contained unit, so a piecewise split here would truncate
    /// any partition/row crossing a 64 KiB boundary (silent drop/corruption).
    /// A regression to unconditional piecewise reads trips the single-call
    /// assertion below.
    #[tokio::test]
    async fn uncompressed_data_block_contiguous_returns_whole_section_in_one_call() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let temp_file = temp_dir
            .path()
            .join("issue_827_uncompressed_contiguous.bin");

        // Larger than several piece-caps — a single partition this size would be
        // shredded if the read split it.
        let size = UNCOMPRESSED_READ_PIECE_BYTES * 3 + 7;
        let test_data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        tokio::fs::write(&temp_file, &test_data).await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BlockSource::buffered(file)));

        let config = SSTableReaderConfig {
            read_buffer_size: 8 * 1024, // small scratch buffer (#592) must NOT cause splitting
            ..Default::default()
        };

        // piecewise = false: the FIRST call must return the whole section.
        let first = read_uncompressed_data_block(&file, &config, false, None)
            .await
            .expect("read should succeed")
            .expect("a non-empty section");
        assert_eq!(
            first.len(),
            size,
            "Finding 2: contiguous read must return the whole {size}-byte section \
             in one call, got {} bytes (it was split into pieces)",
            first.len()
        );
        assert_eq!(first, test_data, "contiguous read must be byte-identical");

        // And the next call is EOF (the section was fully consumed).
        let next = read_uncompressed_data_block(&file, &config, false, None)
            .await
            .expect("read should succeed");
        assert!(
            next.is_none(),
            "Finding 2: after a contiguous full-section read the next call must be EOF"
        );
    }

    /// Issue #827 Finding 2 (dispatch-level): `read_next_block` for the
    /// `V5_0Uncompressed` format must return the whole data section as ONE
    /// contiguous block (no chunk stitching is applied to this format, so each
    /// returned block is a complete parse unit). This exercises the exact
    /// `read_next_block_impl` dispatch a NORMAL (non-compaction) scan takes for a
    /// V5_0Uncompressed SSTable whose data section exceeds 64 KiB.
    #[tokio::test]
    async fn read_next_block_v5_0_uncompressed_returns_contiguous_section() {
        use crate::parser::header::CassandraVersion;

        let temp_dir = TempDir::new().expect("create temp dir");
        let temp_file = temp_dir.path().join("issue_827_v5_uncompressed_block.bin");

        // A >64 KiB "partition" body. We position the reader at offset 0 (the
        // dispatch reads from the current stream position to EOF).
        let size = UNCOMPRESSED_READ_PIECE_BYTES * 2 + 123;
        let test_data: Vec<u8> = (0..size).map(|i| (i % 199) as u8).collect();
        tokio::fs::write(&temp_file, &test_data).await.unwrap();

        let file = tokio::fs::File::open(&temp_file).await.unwrap();
        let file = Arc::new(Mutex::new(BlockSource::buffered(file)));

        let config = SSTableReaderConfig {
            read_buffer_size: 8 * 1024,
            ..Default::default()
        };
        let chunk_index = AtomicUsize::new(0);

        // V5_0Uncompressed dispatch: contiguous whole-section read.
        let block = read_next_block(
            &file,
            &CassandraVersion::V5_0Uncompressed,
            &config,
            &None, // no CompressionInfo
            None,  // no CRC.db in this unit test
            &chunk_index,
            0,
        )
        .await
        .expect("read_next_block should succeed")
        .expect("a non-empty block");

        assert_eq!(
            block.len(),
            size,
            "Finding 2: a normal V5_0Uncompressed read must return the whole \
             {size}-byte section as one block, got {} (truncated to a piece)",
            block.len()
        );
        assert_eq!(
            block, test_data,
            "block must be byte-identical to the section"
        );

        // Next dispatch is EOF.
        let next = read_next_block(
            &file,
            &CassandraVersion::V5_0Uncompressed,
            &config,
            &None,
            None,
            &chunk_index,
            0,
        )
        .await
        .expect("read_next_block should succeed");
        assert!(
            next.is_none(),
            "Finding 2: second V5_0Uncompressed read is EOF"
        );
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

        let file = Arc::new(Mutex::new(BlockSource::buffered(file)));

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
