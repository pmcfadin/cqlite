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
//! These helpers read via the reader's `scan_positional_source` (issue #2876; was
//! `point_source` under the original #1940 restructure) — the positional
//! [`ReadAt`](super::read_at::ReadAt) plane built once at open for EVERY backend
//! (buffered = `pread` on a dedicated `std::fs::File`; mmap = a resident slice over
//! the SAME unadvised mapping the scan backend uses; `O_DIRECT` = an aligned
//! positioned read). A positional read carries its offset as a parameter and
//! touches no tokio reactor/timer, so it completes fully on the calling (blocking)
//! thread — no `tokio::fs`, no `block_on`, no second blocking thread. `read_at.rs`'s
//! module docs anticipated exactly this adoption ("shaped so the windowed streaming
//! scan can adopt it later"). Decompression continues to run in `decode_scan_chunk`
//! on the same blocking thread (the D2 substrate placement, unchanged).
//!
//! This whole feed is scan-only (`feed_compressed_chunks` / `feed_uncompressed_pieces`
//! are driven exclusively by the windowed streaming scan, never a point lookup), so
//! EVERY read here uses `scan_positional_source`, never the reader's dedicated
//! `MADV_RANDOM` point-read mapping (`point_source`, issue #2210) — that advice
//! suppresses kernel readahead, which is exactly backwards for this mostly-
//! sequential feed (issue #2876, the #2210 × #1940 cross-path regression this file
//! was the other half of).
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

use crate::observability::read_phase::ReadPhase;
use std::sync::Arc;

use tokio::sync::mpsc;

use super::super::read_at::DirectScratch;
use super::SSTableReader;
use crate::storage::sstable::read_work_counters as rwc;
use crate::{Error, Result};

/// Bytes handed to the parse half per uncompressed-NB read. Mirrors Cassandra's
/// default 64 KiB compression chunk so the piece size is uniform with the
/// compressed path; the sliding window stitches across pieces, so the boundary is
/// invisible to the parse output. CRC verification is done on `CRC.db` chunk
/// boundaries independently of this piece size.
const UNCOMPRESSED_READ_PIECE_BYTES: usize = 64 * 1024;

/// Wrap a positional-read failure with human-readable `context` WITHOUT
/// discarding its [`ErrorKind`](std::io::ErrorKind) (issue #1940 BLOCKER-2).
///
/// The synchronous positional read path used to relabel every read failure as
/// `std::io::Error::other(..)` (kind `Other`), which erased the source kind — so
/// a genuine transient fault (`Interrupted`/`WouldBlock`/`TimedOut`) surfacing
/// through the feed could no longer be recognised by [`is_transient_io`] and was
/// silently NOT retried. Rebuilding the wrapper with the SAME kind keeps the
/// classifier honest and lets callers up the stack still see the true io kind,
/// mirroring `block_io::io_error_with_context`. Non-`Io` variants
/// (corruption/format) are already typed and pass through unchanged.
fn io_error_with_context(context: impl std::fmt::Display, source: Error) -> Error {
    match source {
        Error::Io(io) => {
            let kind = io.kind();
            Error::Io(std::io::Error::new(kind, format!("{context}: {io}")))
        }
        other => other,
    }
}

impl SSTableReader {
    /// Positional `read_exact_at` that applies the SAME one-shot transient-I/O
    /// retry the former async feed path had (issue #1588 / #1940 BLOCKER-2), and
    /// records the read thread at the REAL read site (issue #1940 guard
    /// integrity).
    ///
    /// A transient kernel fault (`Interrupted` EINTR / `WouldBlock` EAGAIN /
    /// `TimedOut`) is retried EXACTLY once by re-reading at the SAME captured
    /// `offset` (a positional read carries its offset as a parameter, so the retry
    /// needs no re-seek); every other error — deterministic corruption/format, or
    /// a non-transient io kind — fails fast. The returned error is the backend's
    /// `Error::Io`, so its `ErrorKind` is PRESERVED for the caller to annotate via
    /// [`io_error_with_context`] (never collapsed to `Error::other`).
    ///
    /// The `record_io_read_thread` probe fires HERE, at the actual
    /// `scan_positional_source.read_exact_at` call site (not at the top of the feed closure),
    /// so the #1940 no-nesting guard records the thread that performs the real read
    /// — making its `io_read_thread == decode_thread` equality a true detector of a
    /// read dispatched off the feed thread. Compiled only under `scan-offload-probe`.
    fn positional_read_exact_retry_once(
        &self,
        offset: u64,
        buf: &mut [u8],
        scratch: &mut DirectScratch,
    ) -> Result<()> {
        #[cfg(feature = "scan-offload-probe")]
        super::probe::record_io_read_thread();
        let mut attempt = 0u8;
        loop {
            // `read_exact_at_reusing` reuses `scratch`'s aligned bounce buffer on
            // the Direct-I/O backend (issue #2319 — no per-chunk aligned alloc); for
            // mmap/plain-file backends it defers to `read_exact_at` and `scratch`
            // stays empty, so those paths are byte-for-byte unchanged (#1940).
            match self
                .scan_positional_source
                .read_exact_at_reusing(offset, buf, scratch)
            {
                Ok(()) => return Ok(()),
                Err(e)
                    if attempt == 0
                        && crate::storage::sstable::reader::block_io::is_transient_io(&e) =>
                {
                    tracing::warn!(
                        "transient I/O fault ({e}) on positional read at offset 0x{offset:x}; \
                         re-reading once"
                    );
                    attempt = 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Read compressed chunk `chunk_index` SYNCHRONOUSLY via `scan_positional_source`,
    /// verifying its trailing CRC32 before returning the compressed payload (issue
    /// #1940). Returns `Ok(None)` at EOF (past the last chunk, or the degenerate
    /// empty trailing chunk, issue #2225). Mirrors `read_nb_format_chunk_data`'s
    /// geometry + integrity checks exactly, but positional (no cursor mutex, no
    /// `tokio::fs`) and reusing the caller's `scratch` (≤1 alloc/chunk, D2).
    pub(super) fn read_compressed_chunk_sync(
        &self,
        chunk_index: usize,
        scratch: &mut Vec<u8>,
        direct_scratch: &mut DirectScratch,
    ) -> Result<Option<Vec<u8>>> {
        // NO-READ EXITS COME FIRST, BEFORE THE io TIMER EXISTS (issue #1707,
        // roborev job 133). Every exit below — no `CompressionInfo`, past the last
        // chunk (EOF), the degenerate empty trailing chunk (#2225) — performs NO
        // read at all. Timing them would charge function-call and EOF-check time to
        // `read.phase.io` and emit an io SAMPLE for a scan that read nothing. The
        // emitter treats a recorded phase as EVIDENCE THAT THE PHASE RAN (entry is
        // tracked separately from duration, #1707), so constructing a timer here
        // would state that this scan performed `Data.db` reads when it performed
        // none — an absent io series is how a caller learns the opposite.
        let comp_info = match self.compression_info.as_ref() {
            Some(ci) => ci,
            // Callers gate on `compression_info.is_some()`; a None here is a bug.
            None => return Ok(None),
        };

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

        // GEOMETRY AND BUFFER PREPARATION RUN UNTIMED (issue #1707, roborev job
        // 145). Everything from here to the timer below is arithmetic over already-
        // resident `CompressionInfo` metadata plus a `Vec` reserve — no byte of
        // `Data.db` is touched. Timing it charged invalid-offset errors, undersized-
        // chunk errors, bounds failures and ordinary allocation to `read.phase.io`,
        // whose catalogued meaning is time spent in STORAGE. An allocator stall
        // reported as storage latency sends the operator to the disk; a geometry
        // error reported as io emits an io SAMPLE for a call that read nothing.
        let file_size = self.scan_positional_source.len();
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
        // (D2). Grow-on-demand to the CHECKED `total_chunk_size` for THIS chunk —
        // never a giant upfront reserve to the metadata `max_compressed_length`
        // (which Cassandra commonly stamps as `i32::MAX`, an OOM/DoS on ordinary
        // files, issue #1940 BLOCKER-1). `try_reserve_exact` returns a typed error
        // on allocation failure instead of panicking/aborting. After the first few
        // chunks the scratch has reached the working-set high-water mark, so
        // subsequent chunks hit neither the reserve nor the resize regrowth —
        // preserving the ≤1-alloc/chunk STEADY STATE (the `record_chunk_path_alloc`
        // regrowth counter proves it). `clear()`+`resize` reuses the backing store
        // when large enough (mirrors `read_nb_format_chunk_data`).
        let mut buf = std::mem::take(scratch);
        buf.clear();
        let cap_before = buf.capacity();
        let need = total_chunk_size as usize;
        if need > buf.capacity() {
            if let Err(e) = buf.try_reserve_exact(need) {
                // Return the scratch to the caller so a retry could reuse it.
                *scratch = buf;
                return Err(Error::memory(format!(
                    "failed to reserve {need} bytes for chunk {chunk_index} \
                     (data+CRC at offset 0x{chunk_offset:x}): {e}"
                )));
            }
        }
        buf.resize(need, 0u8);
        if buf.capacity() > cap_before {
            rwc::record_chunk_path_alloc();
        }
        // io PHASE (issue #1707): the timer opens IMMEDIATELY BEFORE the positional
        // read and spans the rest of the function — the read itself, its one-shot
        // transient retry, and the trailing CRC verify, which is part of SERVICING
        // the read (the payload is not deliverable until it is verified) and is
        // catalogued inside the phase ("CRC verify included"). A FAILED read stays
        // charged: it issued a real `pread` and waited for the kernel to answer, so
        // a slow failing read is io time and must not vanish from the phase exactly
        // when the disk is sick. Only work that touches no device — geometry above,
        // buffer allocation above — was moved out. Zero `Instant::now()` when the
        // scan is unmetered.
        //
        // The injected test delay is armed HERE and nowhere earlier, and must MOVE
        // WITH this timer if it is ever relocated: it stands in for real read
        // latency, so a call that performs no read must not sleep it, and it must
        // stay inside the timed region so an armed delay is charged to io exactly as
        // real latency is.
        let _io_phase = crate::observability::read_phase::scoped(ReadPhase::Io);
        crate::observability::read_phase::io_delay::sleep_if_armed();
        if let Err(e) =
            self.positional_read_exact_retry_once(chunk_offset, &mut buf, direct_scratch)
        {
            // Return the scratch's capacity to the caller so the next read reuses it.
            buf.clear();
            *scratch = buf;
            // Preserve the source io kind (issue #1940 BLOCKER-2) — never collapse
            // a transient fault to `Error::other`.
            return Err(io_error_with_context(
                format!(
                    "Failed to read chunk {chunk_index} data+CRC ({total_chunk_size} bytes at \
                     offset 0x{chunk_offset:x})"
                ),
                e,
            ));
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
    /// SYNCHRONOUSLY via `scan_positional_source`, starting at byte `pos` (an absolute
    /// Data.db offset). Returns `Ok(None)` at EOF, else `Ok(Some((piece, next_pos)))`.
    /// Every `CRC.db` chunk covering the piece is verified (once per reader
    /// lifetime) BEFORE the piece is returned — from the resident bytes when a
    /// chunk is fully contained, else by reading the straddling remainder
    /// positionally (issue #1396; mirrors `verify_uncompressed_section_in_buffer`).
    pub(super) fn read_uncompressed_piece_sync(
        &self,
        pos: u64,
        direct_scratch: &mut DirectScratch,
    ) -> Result<Option<(Vec<u8>, u64)>> {
        // The EOF exit comes FIRST, before the io timer exists (issue #1707, roborev
        // job 133) — same rule as the compressed sibling above: a call that reads
        // nothing must construct no timer, sleep no injected delay, and contribute
        // no io sample.
        let file_size = self.scan_positional_source.len();
        let remaining = file_size.saturating_sub(pos);
        if remaining == 0 {
            return Ok(None); // EOF
        }
        // Piece geometry and the destination buffer are prepared UNTIMED (issue
        // #1707, roborev job 145) — same rule as the compressed sibling above: a
        // `min` and a `vec![0u8; n]` touch no device, so charging them to
        // `read.phase.io` would report allocator time as storage latency.
        let to_read = remaining.min(UNCOMPRESSED_READ_PIECE_BYTES as u64) as usize;
        let mut buf = vec![0u8; to_read];
        // io PHASE (issue #1707): opens immediately before the positional piece read
        // and spans every covering-chunk CRC verify (including the positional reads a
        // straddling chunk needs). Failed reads stay charged, for the reason given at
        // the compressed sibling. Injected delay inside, as there.
        let _io_phase = crate::observability::read_phase::scoped(ReadPhase::Io);
        crate::observability::read_phase::io_delay::sleep_if_armed();
        // Same one-shot transient-retry + kind-preserving wrap as the compressed
        // path (issue #1940 BLOCKER-2): a transient fault is re-read once, and the
        // source io kind is preserved (never collapsed to `Error::other`).
        self.positional_read_exact_retry_once(pos, &mut buf, direct_scratch)
            .map_err(|e| {
                io_error_with_context(
                    format!("Failed to read uncompressed piece ({to_read} bytes at 0x{pos:x})"),
                    e,
                )
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
                    // A chunk straddling the piece boundary must be read off disk.
                    // Route it through `read_exact_at_reusing` with the SAME per-scan
                    // `direct_scratch` the main piece read uses (issue #2319), so the
                    // Direct-I/O backend reuses the one aligned bounce buffer instead
                    // of allocating a fresh one per straddling chunk. For mmap/plain
                    // backends this defers to `read_exact_at` (scratch stays empty),
                    // so those paths are unchanged.
                    let mut cbuf = vec![0u8; (hi - lo) as usize];
                    self.scan_positional_source
                        .read_exact_at_reusing(lo, &mut cbuf, direct_scratch)
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
        //
        // Reserve ONCE to the ACTUAL working-set high-water mark — the largest
        // real compressed-chunk RECORD size, computed from the authoritative
        // `chunk_offsets` deltas (issue #1940 BLOCKER-1). This is NOT the metadata
        // `max_compressed_length`: Cassandra COMMONLY stamps that field as
        // `i32::MAX` (it equals `i32::MAX` whenever `minCompressRatio == 0`, the
        // DEFAULT — CompressionParams.java:186-189), so a `max_compressed_length`-
        // driven reserve would attempt a multi-GB allocation on opening an ORDINARY
        // compressed scan (an OOM/DoS before any real chunk size is known). The
        // offset-delta high-water mark is bounded by the real per-chunk record size
        // (a small multiple of `chunk_length`, e.g. 16 KiB), never the file size or
        // the i32::MAX bound. Reserving to it once means the per-chunk
        // `clear()`+`resize` never regrows in steady state — preserving the
        // ≤1-alloc/chunk property the `issue_1940_scan_window_substrate_allocs`
        // guard measures (the read path's per-chunk `try_reserve_exact` remains a
        // bounds-checked safety net that records a regrowth only if a chunk ever
        // exceeds this mark). `try_reserve_exact` returns a typed error on
        // allocation failure instead of aborting; a corrupt oversized delta surfaces
        // as `Error::memory`, never a panic. Authoritative metadata, no heuristic.
        let mut scratch: Vec<u8> = Vec::new();
        if let Some(ci) = reader.compression_info.as_ref() {
            let file_size = reader.scan_positional_source.len();
            let max_record = (0..ci.chunk_offsets.len())
                .filter_map(|i| ci.compressed_chunk_size(i, file_size))
                .max()
                .unwrap_or(0);
            let want = usize::try_from(max_record).unwrap_or(usize::MAX);
            if want > 0 {
                scratch.try_reserve_exact(want).map_err(|e| {
                    Error::memory(format!(
                        "failed to reserve {want} bytes for the windowed compressed-read scratch \
                         (largest actual chunk record): {e}"
                    ))
                })?;
            }
        }
        // ONE reusable aligned bounce buffer for the WHOLE scan (issue #2319): the
        // Direct-I/O backend reuses it across every chunk read instead of allocating
        // a fresh ~chunk-sized aligned buffer per chunk (the #1940 regression). No-op
        // (stays empty) for the mmap/plain-file backends.
        let mut direct_scratch = DirectScratch::new();
        let mut chunk_index = 0usize;
        loop {
            // SYNCHRONOUS positional read + CRC (issue #1940): CRC is verified inside
            // the read, BEFORE the payload is trusted (guardrail #1411), exactly as
            // the former cursor path did.
            match reader.read_compressed_chunk_sync(
                chunk_index,
                &mut scratch,
                &mut direct_scratch,
            )? {
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
        // ONE reusable aligned bounce buffer for the whole scan (issue #2319), same
        // as the compressed feed; no-op for non-Direct backends.
        let mut direct_scratch = DirectScratch::new();
        let mut pos = reader.calculate_header_size() as u64;
        let mut chunk_index = 0usize;
        loop {
            match reader.read_uncompressed_piece_sync(pos, &mut direct_scratch)? {
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
