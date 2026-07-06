//! Direct-I/O read-ahead window sizing (issue #1596, Epic F / F6).
//!
//! The direct-I/O scan cursor ([`DirectCursor`](super::source::DirectCursor))
//! refills an *aligned* read-ahead window with one `pread` per refill, then hands
//! sub-ranges of that window to the chunk reader. Each on-disk compression chunk
//! is `chunk_size` payload bytes + a trailing 4-byte CRC32, so the record the
//! chunk reader consumes in one shot is `chunk_size + 4` bytes.
//!
//! If the configured window (`direct_io_prefetch_bytes`) is SMALLER than one
//! chunk record, a single chunk read straddles multiple window refills — read
//! amplification: `ceil(record / window)` `pread`s for what should be one. Worse,
//! a window that is not a small multiple of the record makes the straddle count
//! fluctuate as chunks drift across window boundaries.
//!
//! [`clamp_direct_prefetch_window`] removes that pathology at *source
//! construction* (the audit F6 fix): it floors the window at `2 × (chunk_size +
//! 4)` and keeps 4K alignment. With a window ≥ 2 records, any one chunk record
//! spans at most two aligned windows, so per-chunk refills are bounded to
//! `ceil(record / window) + 1 = 2` regardless of where the record falls. The
//! default window (1 MiB) already exceeds this for the default 64 KiB chunk, so
//! the clamp only bites when a caller sets `direct_io_prefetch_bytes` below a
//! chunk — turning a silent amplification into the intended single-window read.

/// I/O alignment (bytes) for direct reads — mirrors `source::DIRECT_IO_ALIGN` and
/// `read_at::DIRECT_IO_ALIGN`. Kept here (non-`cfg`) so window sizing is available
/// on every platform (the direct backend degrades to buffered off-Unix, but the
/// window value is still computed at the shared call site).
pub(crate) const DIRECT_IO_ALIGN: usize = 4096;

/// Cassandra's default compression chunk length (`chunk_length_in_kb: 64`). Used
/// as the chunk-size basis for the window floor at reader open, where the actual
/// per-SSTable `CompressionInfo.chunk_length` is not yet parsed. This fully
/// protects the common (default-chunk) case; a caller that both picks a larger
/// custom chunk AND a deliberately tiny window retains that explicit choice.
pub(crate) const DEFAULT_COMPRESSION_CHUNK_BYTES: usize = 64 * 1024;

/// Clamp/round a requested direct-I/O prefetch window so a single compression
/// chunk record never straddles more than two aligned refills.
///
/// Returns `max(prefetch_bytes, 2 × (chunk_size + 4))`, never below one alignment
/// unit, rounded UP to [`DIRECT_IO_ALIGN`]. All arithmetic saturates (a
/// near-`usize::MAX` `chunk_size`/`prefetch_bytes` yields the largest aligned
/// value rather than overflowing), so the caller's downstream aligned allocation
/// simply fails and falls back to buffered I/O instead of panicking.
pub(crate) fn clamp_direct_prefetch_window(prefetch_bytes: usize, chunk_size: usize) -> usize {
    // On-disk record = payload + trailing 4-byte CRC32.
    let record = chunk_size.saturating_add(4);
    // Two records so any one record spans at most two aligned windows.
    let floor = record.saturating_mul(2);
    let want = prefetch_bytes.max(floor).max(DIRECT_IO_ALIGN);
    want.checked_next_multiple_of(DIRECT_IO_ALIGN)
        .unwrap_or(usize::MAX & !(DIRECT_IO_ALIGN - 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Model the direct cursor's per-chunk refill count: reading a record of
    /// `record` bytes through an aligned `window`, worst-case starting offset,
    /// costs `ceil(record / window) + 1` refills (the `+1` for a record that
    /// straddles a window boundary). This mirrors `DirectCursor::ensure_buffered`
    /// without needing `O_DIRECT`, so the amplification invariant is testable on
    /// every platform.
    fn worst_case_refills(window: usize, record: usize) -> usize {
        record.div_ceil(window) + 1
    }

    /// RED-first invariant (issue #1596 TDD): a prefetch window SMALLER than one
    /// chunk record amplifies reads (this is the "fails today" state the audit
    /// calls out), and the F6 clamp restores the `≤ ceil(chunk/window)+1 = 2`
    /// bound. Both halves are asserted against the same `worst_case_refills`
    /// model so the clamp's effect is falsifiable, not asserted by fiat.
    #[test]
    fn clamp_bounds_per_chunk_refills_to_two() {
        let chunk = DEFAULT_COMPRESSION_CHUNK_BYTES;
        let record = chunk + 4;

        // A deliberately tiny window (below one chunk) — the misconfiguration the
        // audit targets. Worst case it straddles many refills (> 2): read amp.
        let tiny = 8 * 1024;
        assert!(
            worst_case_refills(tiny, record) > 2,
            "an unclamped sub-chunk window must amplify (>2 refills/chunk); \
             got {} for window={} record={}",
            worst_case_refills(tiny, record),
            tiny,
            record
        );

        // After the clamp the window holds ≥ 2 records, so any one record spans
        // at most two aligned windows: refills bounded to exactly 2.
        let clamped = clamp_direct_prefetch_window(tiny, chunk);
        assert!(
            clamped >= 2 * record,
            "clamped window must hold >= 2 records: {clamped} < {}",
            2 * record
        );
        assert_eq!(
            worst_case_refills(clamped, record),
            2,
            "clamped window must bound per-chunk refills to ceil(chunk/window)+1 = 2"
        );
    }

    #[test]
    fn clamp_result_is_4k_aligned_and_at_least_floor() {
        for &(pf, chunk) in &[
            (0usize, 64 * 1024usize),
            (1, 64 * 1024),
            (8 * 1024, 64 * 1024),
            (4095, 4096),
            (1024 * 1024, 64 * 1024), // default window already above floor
            (128 * 1024, 256 * 1024), // larger custom chunk
        ] {
            let w = clamp_direct_prefetch_window(pf, chunk);
            assert_eq!(w % DIRECT_IO_ALIGN, 0, "window {w} not 4K-aligned");
            assert!(
                w >= 2 * (chunk + 4),
                "window {w} below floor 2*(chunk+4)={} (chunk={chunk})",
                2 * (chunk + 4)
            );
            assert!(w >= pf, "window {w} dropped below requested {pf}");
            assert!(w >= DIRECT_IO_ALIGN, "window {w} below one alignment unit");
        }
    }

    /// A window already larger than the floor is preserved (only rounded up to
    /// alignment) — the clamp never SHRINKS a well-sized window.
    #[test]
    fn clamp_preserves_large_windows() {
        let big = 4 * 1024 * 1024;
        assert_eq!(
            clamp_direct_prefetch_window(big, DEFAULT_COMPRESSION_CHUNK_BYTES),
            big,
            "an already-aligned window above the floor is returned unchanged"
        );
    }

    /// Saturating arithmetic: a pathological `chunk_size`/`prefetch_bytes` near
    /// `usize::MAX` yields the largest aligned value, never a panic/overflow.
    #[test]
    fn clamp_saturates_on_overflow() {
        let w = clamp_direct_prefetch_window(usize::MAX, usize::MAX);
        assert_eq!(w % DIRECT_IO_ALIGN, 0, "saturated window must stay aligned");
        assert_eq!(w, usize::MAX & !(DIRECT_IO_ALIGN - 1));
    }
}
