//! Synchronous positional chunk reader for the windowed streaming scan's IO half
//! (issue #1940 restructure).
//!
//! # Why this exists
//!
//! The windowed feed runs inside a single `spawn_blocking` task (issues #1593,
//! #1940): both the raw chunk read AND its decompression must stay OFF the async
//! worker pool. The former implementation drove the read with
//! `futures::executor::block_on(read_next_block_parts(..))` — an async read over a
//! `tokio::fs::File`. Even though it ran on a blocking thread, `tokio::fs`'s
//! `poll_read` RE-DISPATCHES the actual `read(2)` to tokio's blocking pool, so each
//! chunk read amplified into a second blocking-pool thread. rust-reviewer proved it
//! bounded, but roborev flagged it as a latent hang under a small custom
//! `max_blocking_threads`. Owner decision (issue #1940): REMOVE the hazard class —
//! read SYNCHRONOUSLY on the `spawn_blocking` thread, with zero nested async and
//! zero blocking-pool amplification.
//!
//! These helpers read via the reader's `point_source` — the positional
//! [`ReadAt`](super::read_at::ReadAt) plane built once at open for EVERY backend
//! (buffered = `pread` on a dedicated `std::fs::File`; mmap = a resident slice;
//! `O_DIRECT` = an aligned positioned read). A positional read carries its offset
//! as a parameter and touches no tokio reactor/timer, so it completes fully on the
//! calling (blocking) thread — no `tokio::fs`, no `block_on`, no second blocking
//! thread. `read_at.rs`'s module docs anticipated exactly this adoption ("shaped so
//! the windowed streaming scan can adopt it later"). Decompression continues to run
//! in `decode_scan_chunk` on the same blocking thread (the D2 substrate placement,
//! unchanged).
//!
//! Byte-parity + integrity are preserved verbatim from the former cursor path:
//! - **compressed NB** (`CompressionInfo` present): the same per-chunk record
//!   geometry (`compressed_chunk_offset`/`compressed_chunk_size`, incl. the +4
//!   trailing CRC), the same corrupt-offset bounds check (roborev #970), the same
//!   degenerate empty-trailing-chunk EOF bound (issue #2225), and the same
//!   CRC32-BEFORE-decompress order (the CRC is verified here; the payload is
//!   returned only after it matches, so a corrupt chunk never reaches the
//!   decompressor). The compressed read reuses the caller's per-loop scratch (≤1
//!   alloc/chunk, D2) and records a scratch-regrowth as a copy-chain alloc so the
//!   `issue_1940_scan_window_substrate_allocs` guard stays honest.
//! - **uncompressed NB** (no `CompressionInfo`; CQLite's own write surface): the
//!   data section is fed in bounded pieces, and every `CRC.db` chunk covering a
//!   piece is verified BEFORE the piece is handed on — computing the CRC from the
//!   resident piece bytes when a chunk is fully contained, else reading the
//!   straddling prefix/suffix positionally (mirrors
//!   `verify_uncompressed_section_in_buffer`). Each chunk is verified at most once
//!   per reader lifetime via the shared `verified_uncompressed_chunks` memo.
//!
//! Kept in a sibling file (campsite rule, epic #1116); included via
//! `#[path = "scan_stream_windowed_read.rs"] impl`-carrying module in the parent.

use std::sync::Arc;

use tokio::sync::mpsc;

use super::SSTableReader;
use crate::storage::sstable::read_work_counters as rwc;
use crate::{Error, Result};

/// Bytes handed to the parse half per uncompressed-NB read. Mirrors Cassandra's
/// default 64 KiB compression chunk so the piece size is uniform with the
/// compressed path; the sliding window stitches across pieces, so the boundary is
/// invisible to the parse output. CRC verification is done on `CRC.db` chunk
/// boundaries independently of this piece size.
const UNCOMPRESSED_READ_PIECE_BYTES: usize = 64 * 1024;

impl SSTableReader {
    /// Read compressed chunk `chunk_index` SYNCHRONOUSLY via `point_source`,
    /// verifying its trailing CRC32 before returning the compressed payload (issue
    /// #1940). Returns `Ok(None)` at EOF (past the last chunk, or the degenerate
    /// empty trailing chunk, issue #2225). Mirrors `read_nb_format_chunk_data`'s
    /// geometry + integrity checks exactly, but positional (no cursor mutex, no
    /// `tokio::fs`) and reusing the caller's `scratch` (≤1 alloc/chunk, D2).
    pub(super) fn read_compressed_chunk_sync(
        &self,
        chunk_index: usize,
        scratch: &mut Vec<u8>,
    ) -> Result<Option<Vec<u8>>> {
        let comp_info = match self.compression_info.as_ref() {
            Some(ci) => ci,
            // Callers gate on `compression_info.is_some()`; a None here is a bug.
            None => return Ok(None),
        };
        let file_size = self.point_source.len();

        if chunk_index >= comp_info.chunk_offsets.len() {
            return Ok(None); // EOF
        }

        // Degenerate empty trailing chunk (issue #2225): a chunk whose logical
        // start is at/after `data_length` carries 0 real bytes; Cassandra's reader
        // never touches it (every logical position < data_length maps earlier), and
        // handing 0 bytes to a decompressor fails. Treat it (and anything beyond,
        // chunk starts being monotonic) as EOF — identical to the cursor path.
        let chunk_length = comp_info.chunk_length as u64;
        if chunk_length > 0 {
            let logical_start = (chunk_index as u64).saturating_mul(chunk_length);
            if logical_start >= comp_info.data_length {
                return Ok(None);
            }
        }

        let chunk_offset = comp_info
            .compressed_chunk_offset(chunk_index)
            .ok_or_else(|| Error::InvalidFormat(format!("No offset for chunk {chunk_index}")))?;
        let total_chunk_size = comp_info
            .compressed_chunk_size(chunk_index, file_size)
            .ok_or_else(|| {
                Error::InvalidFormat(format!(
                    "Cannot determine size for chunk {chunk_index} (file_size={file_size})"
                ))
            })?;
        if total_chunk_size < 4 {
            return Err(Error::InvalidFormat(format!(
                "Chunk {chunk_index} size too small: {total_chunk_size} bytes (minimum 4 for CRC)"
            )));
        }
        // Bounds-check against the real Data.db length BEFORE allocating — a corrupt
        // CompressionInfo offset must surface as a recoverable error, never a
        // multi-exabyte allocation (roborev #970). NB chunk offsets are absolute
        // (header_offset = 0).
        let chunk_end = chunk_offset.checked_add(total_chunk_size);
        match chunk_end {
            Some(end) if end <= file_size => {}
            _ => {
                return Err(Error::InvalidFormat(format!(
                    "Chunk {chunk_index} at offset 0x{chunk_offset:x} with size {total_chunk_size} \
                     exceeds Data.db length {file_size} — corrupt CompressionInfo.db chunk offset"
                )));
            }
        }
        let chunk_data_size = (total_chunk_size - 4) as usize;

        // A5 counters (E4 SEEK_CALLS + E3 READ_CALLS): the positioned read stands in
        // for the cursor path's per-chunk seek + read, so record one of each per
        // chunk to keep the `reads == decompresses` substrate invariant intact.
        rwc::record_seek();
        rwc::record_read();

        // ONE positioned read for payload + trailing CRC into the REUSED scratch
        // (D2). `clear()`+`resize` reuses the backing store when large enough, so a
        // steady-state scan performs no per-chunk compressed-side allocation; a
        // regrowth is recorded as a copy-chain alloc so the ≤1-alloc/chunk guard
        // stays honest (mirrors `read_nb_format_chunk_data`).
        let mut buf = std::mem::take(scratch);
        buf.clear();
        let cap_before = buf.capacity();
        buf.resize(total_chunk_size as usize, 0u8);
        if buf.capacity() > cap_before {
            rwc::record_chunk_path_alloc();
        }
        if let Err(e) = self.point_source.read_exact_at(chunk_offset, &mut buf) {
            // Return the scratch's capacity to the caller so the next read reuses it.
            buf.clear();
            *scratch = buf;
            return Err(Error::Io(std::io::Error::other(format!(
                "Failed to read chunk {chunk_index} data+CRC ({total_chunk_size} bytes at \
                 offset 0x{chunk_offset:x}): {e}"
            ))));
        }

        // Split the trailing 4-byte big-endian CRC32 off the payload and verify it
        // BEFORE the payload is trusted (guardrail #1411): a mismatch is a typed
        // error and the payload never reaches the decompressor.
        let expected_crc = u32::from_be_bytes([
            buf[chunk_data_size],
            buf[chunk_data_size + 1],
            buf[chunk_data_size + 2],
            buf[chunk_data_size + 3],
        ]);
        buf.truncate(chunk_data_size);
        let computed_crc = crc32fast::hash(&buf);
        if computed_crc != expected_crc {
            return Err(Error::InvalidFormat(format!(
                "CRC32 mismatch for chunk {chunk_index} at offset 0x{chunk_offset:x}: \
                 expected=0x{expected_crc:08x}, computed=0x{computed_crc:08x}, \
                 chunk_size={chunk_data_size}"
            )));
        }
        Ok(Some(buf))
    }

    /// Read the next bounded piece of an UNCOMPRESSED-NB data section
    /// SYNCHRONOUSLY via `point_source`, starting at byte `pos` (an absolute
    /// Data.db offset). Returns `Ok(None)` at EOF, else `Ok(Some((piece, next_pos)))`.
    /// Every `CRC.db` chunk covering the piece is verified (once per reader
    /// lifetime) BEFORE the piece is returned — from the resident bytes when a
    /// chunk is fully contained, else by reading the straddling remainder
    /// positionally (issue #1396; mirrors `verify_uncompressed_section_in_buffer`).
    pub(super) fn read_uncompressed_piece_sync(&self, pos: u64) -> Result<Option<(Vec<u8>, u64)>> {
        let file_size = self.point_source.len();
        let remaining = file_size.saturating_sub(pos);
        if remaining == 0 {
            return Ok(None); // EOF
        }
        let to_read = remaining.min(UNCOMPRESSED_READ_PIECE_BYTES as u64) as usize;
        let mut buf = vec![0u8; to_read];
        self.point_source
            .read_exact_at(pos, &mut buf)
            .map_err(|e| {
                Error::Io(std::io::Error::other(format!(
                    "Failed to read uncompressed piece ({to_read} bytes at 0x{pos:x}): {e}"
                )))
            })?;
        let end = pos + to_read as u64;

        // CRC verification (no-op when this reader has no CRC.db). Compute each
        // covering chunk's CRC from the resident piece when fully contained, else
        // read the straddling prefix/suffix positionally. `verify_covering_chunks`
        // is synchronous and memoizes each chunk so it is checked at most once.
        if let Some(crc) = self.crc_reader.as_deref() {
            let piece = &buf;
            self.verify_covering_chunks(crc, pos, end, |lo, hi, chunk| {
                if lo >= pos && hi <= end {
                    let s = (lo - pos) as usize;
                    let e = (hi - pos) as usize;
                    Ok(crc32fast::hash(&piece[s..e]))
                } else {
                    let mut cbuf = vec![0u8; (hi - lo) as usize];
                    self.point_source
                        .read_exact_at(lo, &mut cbuf)
                        .map_err(|e| {
                            Error::corruption(format!(
                                "failed to read uncompressed chunk {chunk} at Data.db offset \
                             0x{lo:x} for CRC verification: {e}"
                            ))
                        })?;
                    Ok(crc32fast::hash(&cbuf))
                }
            })?;
        }
        Ok(Some((buf, end)))
    }

    /// Synchronous compressed-NB feed body (issue #1940): read each compressed
    /// chunk positionally (CRC verified in the read), decode it (D2), and ship the
    /// decompressed `Bytes` on `raw_tx`. `Ok(())` on clean EOF or
    /// consumer-ended-early; `Err` on a read/decode failure. Runs on the feed's
    /// `spawn_blocking` thread — no `tokio::fs`, no `block_on`, no blocking-pool
    /// amplification.
    pub(super) fn feed_compressed_chunks(
        reader: &Arc<Self>,
        raw_tx: &mpsc::Sender<bytes::Bytes>,
        max_compressed_length: usize,
    ) -> Result<()> {
        // Reused compressed-read scratch (issue #1940, D2): read fills it, decode
        // borrows it, recycle for the next read — no per-chunk compressed alloc.
        // Pre-reserve ONCE to the max stored compressed-chunk RECORD size from
        // authoritative CompressionInfo metadata: `max_compressed_length + 4`
        // (largest compressed payload any chunk can occupy + trailing 4-byte CRC).
        // The read path `resize`s the scratch to each chunk's ACTUAL record size, up
        // to that bound; the true high-water mark means the per-chunk
        // `clear()`+`resize` never grows in steady state, keeping the ≤1-alloc/chunk
        // guard green. Metadata-driven, no heuristic.
        let mut scratch: Vec<u8> = Vec::new();
        if let Some(ci) = reader.compression_info.as_ref() {
            scratch.reserve((ci.max_compressed_length as usize).saturating_add(4));
        }
        let mut chunk_index = 0usize;
        loop {
            // SYNCHRONOUS positional read + CRC (issue #1940): CRC is verified inside
            // the read, BEFORE the payload is trusted (guardrail #1411), exactly as
            // the former cursor path did.
            match reader.read_compressed_chunk_sync(chunk_index, &mut scratch)? {
                Some(compressed) => {
                    // Decompression stays HERE, on this blocking thread (D2), off the
                    // async reactor for every backend.
                    let (decoded, recycled) =
                        reader.decode_scan_chunk(chunk_index, max_compressed_length, compressed)?;
                    scratch = recycled;
                    chunk_index += 1;
                    if raw_tx.blocking_send(decoded).is_err() {
                        return Ok(()); // consumer/parse ended early
                    }
                }
                None => return Ok(()), // EOF
            }
        }
    }

    /// Synchronous uncompressed-NB feed body (issue #1940): read the data section
    /// in bounded positional pieces (each `CRC.db`-verified before it is shipped),
    /// decode (zero-copy `Vec`→`Bytes`, no decompressor), and ship on `raw_tx`.
    /// `Ok(())` on clean EOF or consumer-ended-early; `Err` on a read/CRC failure.
    pub(super) fn feed_uncompressed_pieces(
        reader: &Arc<Self>,
        raw_tx: &mpsc::Sender<bytes::Bytes>,
    ) -> Result<()> {
        let mut pos = reader.calculate_header_size() as u64;
        let mut chunk_index = 0usize;
        loop {
            match reader.read_uncompressed_piece_sync(pos)? {
                Some((piece, next_pos)) => {
                    // No compressor: `decode_scan_chunk` moves the piece into the B1
                    // cache zero-copy (`Bytes::from(Vec)`) — no per-piece copy — and
                    // returns the refcounted substrate the window borrows.
                    let (decoded, _recycled) =
                        reader.decode_scan_chunk(chunk_index, usize::MAX, piece)?;
                    pos = next_pos;
                    chunk_index += 1;
                    if raw_tx.blocking_send(decoded).is_err() {
                        return Ok(()); // consumer/parse ended early
                    }
                }
                None => return Ok(()), // EOF
            }
        }
    }
}
