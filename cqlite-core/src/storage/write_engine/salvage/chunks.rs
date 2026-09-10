//! Chunk-CRC pre-flight for salvage (design D1): validate every `Data.db`
//! chunk BEFORE the recovery loop and record the bad-chunk set, so a
//! partition whose byte range intersects a CRC-failed chunk is a loss with
//! class `chunk-crc` EVEN IF its bytes happened to still parse (#3782: a
//! flipped byte that still decodes). Cassandra's chunk CRC is the authority
//! on what is trustworthy, not whether the parser tolerated the bytes.
//!
//! Unlike `verify::check_inline_chunk_crc` / `check_uncompressed_crc_db`
//! (which report the FIRST bad chunk and stop, matching the read path's
//! fail-fast posture), this module walks EVERY chunk and collects the WHOLE
//! bad set — salvage needs to know which PARTITIONS are affected, not just
//! that the file has a problem.

use super::ComponentFinding;
use crate::storage::sstable::chunk_reader::ChunkReader;
use crate::storage::sstable::compression_info::CompressionInfo;
use std::collections::BTreeSet;
use std::path::Path;

/// Bad chunk indices plus a summary finding, when any chunk failed.
pub(super) struct ChunkPreflight {
    pub(super) bad_chunks: BTreeSet<u64>,
    pub(super) finding: Option<ComponentFinding>,
    /// The uncompressed chunk size that bounds this file's chunking — the
    /// same domain `Index.db`/`Partitions.db` offsets live in, so a
    /// partition's `[offset, end)` maps to chunk indices via
    /// [`chunks_for_range`] using this value.
    pub(super) chunk_size: u64,
}

/// Which chunk indices (inclusive) a partition's decompressed-domain byte
/// range `[offset, end)` intersects, given `chunk_size`. `end` is exclusive;
/// an empty/zero-length range still names the ONE chunk holding `offset`.
pub(super) fn chunks_for_range(offset: u64, end: u64, chunk_size: u64) -> Vec<u64> {
    if chunk_size == 0 {
        return Vec::new();
    }
    let start_chunk = offset / chunk_size;
    let last_byte = end.saturating_sub(1).max(offset);
    let end_chunk = last_byte / chunk_size;
    (start_chunk..=end_chunk).collect()
}

/// Validate every inline chunk CRC32 of a compressed `Data.db` (#998),
/// without decompressing (matches `verify::check_inline_chunk_crc`'s
/// posture) — collecting every failing chunk index instead of stopping at
/// the first.
pub(super) fn compressed_chunk_preflight(
    data_path: &Path,
    compression_info: &CompressionInfo,
) -> std::io::Result<ChunkPreflight> {
    use std::fs::File;

    let file = File::open(data_path)?;
    let total_size = file.metadata()?.len();
    let reader = std::io::BufReader::new(file);
    let mut chunk_reader = ChunkReader::new(reader, compression_info.clone(), total_size);

    let mut bad_chunks = BTreeSet::new();
    let mut first_detail: Option<String> = None;
    for i in 0..chunk_reader.chunk_count() {
        if let Err(e) = chunk_reader.read_chunk(i) {
            bad_chunks.insert(i as u64);
            if first_detail.is_none() {
                first_detail = Some(format!("chunk {i}: {e}"));
            }
        }
    }

    let finding = first_detail.map(|detail| ComponentFinding {
        class: "ChunkDecompressionError".to_string(),
        component: "Data.db".to_string(),
        detail: format!(
            "{} of {} chunk(s) failed inline CRC32 validation; first: {detail}",
            bad_chunks.len(),
            chunk_reader.chunk_count()
        ),
    });

    Ok(ChunkPreflight {
        bad_chunks,
        finding,
        chunk_size: compression_info.chunk_length as u64,
    })
}

/// Validate every uncompressed `Data.db` chunk against `CRC.db` (issue
/// #1396), collecting every failing chunk index. Absent `CRC.db` is
/// warn-and-proceed (design D4, matching `verify`): an empty bad-chunk set,
/// no finding — the chunk-CRC signal simply is not available.
pub(super) async fn uncompressed_chunk_preflight(
    data_path: &Path,
    crc_path: &Path,
) -> crate::Result<ChunkPreflight> {
    use crate::storage::sstable::reader::crc::CrcDb;
    use tokio::io::AsyncReadExt;

    if !crc_path.exists() {
        return Ok(ChunkPreflight {
            bad_chunks: BTreeSet::new(),
            finding: None,
            // No CRC.db to derive a chunk size from; 0 disables chunk-range
            // mapping (callers treat an empty bad-chunk set as "nothing to
            // map" regardless).
            chunk_size: 0,
        });
    }

    let data_len = tokio::fs::metadata(data_path)
        .await
        .map(|m| m.len())
        .unwrap_or(0);
    let crc = CrcDb::open(crc_path, data_len).await?;
    let chunk_size = crc.chunk_size() as u64;

    let mut file = tokio::fs::File::open(data_path).await?;
    let mut bad_chunks = BTreeSet::new();
    let mut first_detail: Option<String> = None;
    let mut chunk_index: u64 = 0;
    let mut buf = vec![0u8; chunk_size.max(1) as usize];
    loop {
        let mut filled = 0usize;
        loop {
            match file.read(&mut buf[filled..]).await? {
                0 => break,
                n => {
                    filled += n;
                    if filled == buf.len() {
                        break;
                    }
                }
            }
        }
        if filled == 0 {
            break;
        }
        let computed = crc32fast::hash(&buf[..filled]);
        match crc.crc_for_chunk(chunk_index as usize) {
            Ok(expected) if expected == computed => {}
            Ok(_) => {
                bad_chunks.insert(chunk_index);
                if first_detail.is_none() {
                    first_detail = Some(format!("chunk {chunk_index}: CRC32 mismatch"));
                }
            }
            Err(e) => {
                bad_chunks.insert(chunk_index);
                if first_detail.is_none() {
                    first_detail = Some(format!("chunk {chunk_index}: CRC.db has no entry: {e}"));
                }
            }
        }
        chunk_index += 1;
        if filled < buf.len() {
            break;
        }
    }

    let finding = first_detail.map(|detail| ComponentFinding {
        class: "UncompressedChunkCrcMismatch".to_string(),
        component: "Data.db".to_string(),
        detail: format!(
            "{} of {} chunk(s) failed CRC.db validation; first: {detail}",
            bad_chunks.len(),
            chunk_index
        ),
    });

    Ok(ChunkPreflight {
        bad_chunks,
        finding,
        chunk_size,
    })
}
