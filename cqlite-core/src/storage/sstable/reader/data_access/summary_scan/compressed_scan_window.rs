//! Chunk-aligned coalesced read window for the Summary-guided compressed scan
//! (issue #2877), extracted from the parent
//! [`summary_scan`](super) module (campsite rule, epic #1116).
//!
//! The parent module owns the WALK (`walk_in_range_partition_slices`); this
//! module owns the one piece of state that walk carries for a COMPRESSED
//! `Data.db`: [`CompressedScanWindow`], the sliding window of decompressed bytes
//! that serves consecutive in-range partition slices from ONE coalesced,
//! chunk-aligned `read_compressed_offset_window` call instead of one call per
//! partition.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::storage::sstable::compression_info::CompressionInfo;
use crate::storage::sstable::reader::read_at::ReadAt;
use crate::storage::sstable::reader::SSTableReader;
use crate::{Error, Result};

/// Steady-state target size (bytes) of the coalesced, chunk-aligned
/// compressed-scan read window (issue #2877), mirroring the uncompressed
/// non-stitching walk's `SEQUENTIAL_WINDOW_TARGET_BYTES` (`full_index_stream.rs`,
/// 4 MiB) precedent. See [`CompressedScanWindow`] for why the window's end is
/// always rounded UP to a `CompressionInfo.chunk_length` boundary rather than cut
/// at the target byte count exactly, and why a window only REACHES this size
/// after ramping up to it.
const COMPRESSED_SCAN_WINDOW_TARGET_BYTES: u64 = 4 * 1024 * 1024;

/// Hard ceiling on the ramped greedy FLOOR (issue #2877 roborev, blocker A): the
/// ramp doubles `chunk_length` per refill but can never ask for more than the
/// steady-state target, so `chunk_length << ramp` is clamped here and the ramp
/// counter stops advancing once the clamp binds. Equal to
/// [`COMPRESSED_SCAN_WINDOW_TARGET_BYTES`] by construction — named separately
/// because it is the ramp's *invariant*, asserted directly by the unit tests
/// below.
///
/// Scope, precisely (it bounds the FLOOR, not the WINDOW): a refill's length is
/// `max(minimal, floor)` rounded UP to a `chunk_length` boundary, so the window
/// can legitimately exceed this cap by (a) up to one `chunk_length` of alignment
/// rounding and (b) however much a SINGLE partition larger than the floor needs
/// (`minimal`, deliberately never `min`-ed — the large-partition case must not
/// regress). Concretely, `CompressionInfo::parse` admits a `chunk_length` up to
/// 256 MiB, and an 8 MiB `chunk_length` therefore yields an 8 MiB window even
/// though the floor clamps at 4 MiB. What the cap DOES guarantee is that
/// read-ahead — bytes fetched beyond what the current partition needs — is
/// bounded by the target plus one chunk, which is what the memory derivation
/// under "Preserved invariants" below spends (`+ one chunk_length`).
const COMPRESSED_SCAN_WINDOW_RAMP_MAX_BYTES: u64 = COMPRESSED_SCAN_WINDOW_TARGET_BYTES;

/// Process-global count of coalesced window REFILLS — one per
/// `read_compressed_offset_window` call the Summary-guided compressed scan makes
/// (issue #2877). Mirrors
/// [`DECOMPRESS_CALLS`](crate::storage::sstable::reader::data_access::DECOMPRESS_CALLS)'s
/// always-on `Relaxed`-add shape (integration tests compile the lib without its
/// `test` cfg, and one atomic add per multi-MiB read is free).
///
/// This is the observable proof of the RAMP: covering an N-chunk data section in
/// O(log(target/chunk_length)) + N*chunk_length/target refills — rather than one
/// refill per chunk boundary crossed — is only possible if the window actually
/// grows to its steady-state target. Read/reset via
/// [`SSTableReader::scan_window_refill_count`] /
/// [`SSTableReader::reset_scan_window_counters`].
static SCAN_WINDOW_REFILLS: AtomicU64 = AtomicU64::new(0);

/// Process-global count of dead-prefix COMPACTIONS (the `Vec::drain` memmove that
/// reclaims already-served bytes) performed by the coalesced window (issue #2877).
///
/// The oracle for roborev blocker B: compaction must be O(refills), NOT
/// O(partitions). Draining the dead prefix for every partition served shifts the
/// whole remaining window each time — quadratic byte copying across the thousands
/// of narrow partitions one 4 MiB window holds, which is exactly the coalescing
/// win this issue exists to deliver. Read/reset via
/// [`SSTableReader::scan_window_prefix_compaction_count`] /
/// [`SSTableReader::reset_scan_window_counters`].
static SCAN_WINDOW_PREFIX_COMPACTIONS: AtomicU64 = AtomicU64::new(0);

impl SSTableReader {
    /// Coalesced compressed-scan window refills since the last
    /// [`reset_scan_window_counters`](Self::reset_scan_window_counters) (issue
    /// #2877). See [`SCAN_WINDOW_REFILLS`].
    pub fn scan_window_refill_count() -> u64 {
        SCAN_WINDOW_REFILLS.load(Ordering::Relaxed)
    }

    /// Coalesced compressed-scan window dead-prefix compactions (memmoves) since
    /// the last [`reset_scan_window_counters`](Self::reset_scan_window_counters)
    /// (issue #2877). See [`SCAN_WINDOW_PREFIX_COMPACTIONS`].
    pub fn scan_window_prefix_compaction_count() -> u64 {
        SCAN_WINDOW_PREFIX_COMPACTIONS.load(Ordering::Relaxed)
    }

    /// Reset both coalesced-window work counters (test/instrumentation harness).
    pub fn reset_scan_window_counters() {
        SCAN_WINDOW_REFILLS.store(0, Ordering::Relaxed);
        SCAN_WINDOW_PREFIX_COMPACTIONS.store(0, Ordering::Relaxed);
    }
}

/// A chunk-aligned sliding window over the DECOMPRESSED bytes of a compressed
/// scan, keyed to the UNCOMPRESSED data-section offset domain (issue #2877).
///
/// `walk_in_range_partition_slices` used to call
/// [`read_compressed_offset_window`](SSTableReader::read_compressed_offset_window)
/// once PER PARTITION. That helper maps `[start, start+size)` onto
/// `CompressionInfo.db` chunks and decompresses every chunk it touches with no
/// cross-call memoisation — so a 16-64 KiB chunk holding many narrow partitions
/// was read + decompressed once per partition it contains, entirely bypassing
/// the Epic B decompressed-chunk cache on this hot path (the issue's root
/// cause). This window instead accumulates consecutive in-range partitions and
/// refills in ONE coalesced call whenever the current partition's span is not
/// already covered.
///
/// # Why chunk-ALIGNED, not just chunk-sized
///
/// A window boundary that falls in the MIDDLE of a chunk would make that chunk
/// get decompressed twice: once as the tail of window N, once again as the
/// head of window N+1 (both calls independently map their own `[start, end)`
/// onto `CompressionInfo.chunk_length`-sized chunks and decompress whichever
/// they touch). Rounding every refill's end UP to the next
/// `chunk_length` boundary — and always resuming the NEXT window exactly at the
/// previous window's end — means windows tile the byte space with NO gaps and
/// NO overlaps at chunk granularity, so each chunk is decompressed by EXACTLY
/// ONE window's read call across the whole scan (the acceptance criterion this
/// window exists to satisfy).
///
/// # Why the size RAMPS instead of jumping straight to the target
///
/// (Issue #2877 roborev, blocker A.) A flat "every fresh fill reads at least 4
/// MiB" defeats the walk's own token pushdown and early termination: a `decode`
/// closure that returns `ControlFlow::Break` after one row, or a narrow token
/// range holding a handful of partitions, would still read AND LZ4-decompress 4
/// MiB of mostly out-of-range partition bodies. So the greedy floor starts at ONE
/// `chunk_length` and DOUBLES per refill ([`Self::ramped_floor`]), clamped at
/// [`COMPRESSED_SCAN_WINDOW_RAMP_MAX_BYTES`]:
///
/// - an early-terminating callback pays ~one chunk, not 4 MiB;
/// - a narrow token range never matures the ramp (few refills ⇒ small floor), and
///   its total over-read is bounded by what it has already legitimately read
///   (each refill is at most double the previous floor);
/// - a long full scan reaches the 4 MiB steady state after ~`log2(target /
///   chunk_length)` refills — a few hundred KiB of ramp-up for a 16 KiB chunk —
///   and is asymptotically identical to a flat 4 MiB window.
///
/// The floor is only ever a FLOOR: `minimal.max(floor)` keeps a single partition
/// LARGER than the ramped size served in one go, so the large-partition case
/// never regresses.
///
/// # Preserved invariants
///
/// - **CRC-before-decompress ordering** (guardrail #1411/#1773): unchanged —
///   every chunk still goes through `read_compressed_offset_window`'s
///   CRC-validated chunk reader, just fewer times.
/// - **SCAN-plane reads** (issue #2876, and the reason the two fixes are
///   inseparable): every refill is issued on the positional plane the WALK hands
///   in — the reader's UNADVISED `scan_positional_source` — never the
///   `MADV_RANDOM` `point_source`. Coalescing and the read-intent split are
///   complementary halves of one mechanism: bigger sequential reads only pay off
///   on a mapping that reads ahead, and CASSANDRA-15452 is the upstream precedent
///   for the failure mode (their userspace scan buffer was defeated by the layer
///   underneath it). Pinned end-to-end by
///   `cqlite-core/tests/issue_2877_scan_chunk_coalescing.rs`'s combined test,
///   which counts reads PER PLANE and requires zero on the point plane.
/// - **`partition_slice_fully_consumed`** (Signal B): unchanged — still checked
///   per partition against the slice this window serves.
/// - **Dead-prefix reclamation is O(refills), not O(partitions)** (issue #2877
///   roborev, blocker B): the already-served prefix is dropped at REFILL time
///   ([`Self::compact_prefix`]) rather than after every partition, so serving
///   thousands of narrow partitions out of one window costs zero byte copying.
///   Mechanism, precisely: a per-refill `Vec::drain` of the dead prefix plus an
///   advance of [`Self::start`](Self#structfield.start). There is deliberately NO
///   separate logical-`head` offset field — draining once per refill is already
///   O(refills), and one field fewer means one fewer invariant to keep.
///   Compaction drops only from the FRONT, so the chunk-aligned TAIL boundary
///   (`self.start + bytes.len()`) is invariant under it and a chunk already paid
///   for can never be re-decompressed.
/// - **Memory bound** (updated for the lazy reclamation above): `largest
///   partition span + max(COMPRESSED_SCAN_WINDOW_TARGET_BYTES, largest partition
///   span) + one chunk_length`. Derivation: compaction runs BEFORE every append,
///   and a straddling partition's live remainder is `have_end - start < end -
///   start` — i.e. under its own span; the append itself adds at most
///   `max(minimal, ramped floor)` rounded up to a chunk, and both `minimal` and
///   the floor are bounded by `max(largest span, target)`. A gap-realign replaces
///   the buffer outright, so it is bounded by the second term alone. Deferring
///   reclamation to refill time therefore costs at most ONE extra partition span
///   of residency versus draining per partition — never unbounded growth, and
///   comfortably within the `<128MB` target.
/// - **The ramp counts CONSECUTIVE partitions only**: a gap-realign (the walk
///   skipped an out-of-range run — e.g. a wraparound scan's jump to its second
///   segment) RESETS the ramp to one chunk. Sequential locality is exactly what
///   the ramp is spending read-ahead on, and a jump proves it was just broken; the
///   new region re-earns its window. So a sparse/narrow range that keeps jumping
///   can never accumulate its way to a 4 MiB over-read, while a dense scan (one
///   initial gap fill, then straddle-appends) ramps uninterrupted.
pub(super) struct CompressedScanWindow {
    /// Decompressed bytes of `[start, start + bytes.len())` in the UNCOMPRESSED
    /// data-section offset domain. Empty until the first refill.
    bytes: Vec<u8>,
    /// Start offset of `bytes`, in the same domain. Meaningless while `filled`
    /// is `false`. Advances only at refill time (see [`Self::compact_prefix`]),
    /// so between refills it lags behind the partition being served — which is
    /// precisely what makes per-partition serving copy-free.
    start: u64,
    /// Whether `bytes`/`start` hold a real window yet.
    filled: bool,
    /// Doubling shift count for the greedy floor (`chunk_length << ramp`, clamped
    /// at [`COMPRESSED_SCAN_WINDOW_RAMP_MAX_BYTES`]). Starts at 0 (one chunk),
    /// advances one step per refill until the clamp binds, and RESETS to 0 on a
    /// gap-realign (the consecutive run it was earning read-ahead for is broken).
    ramp: u32,
}

impl CompressedScanWindow {
    pub(super) fn new() -> Self {
        Self {
            bytes: Vec::new(),
            start: 0,
            filled: false,
            ramp: 0,
        }
    }

    /// Current greedy FLOOR for a refill: `chunk_length << ramp`, clamped at
    /// [`COMPRESSED_SCAN_WINDOW_RAMP_MAX_BYTES`].
    ///
    /// Overflow-safe by construction — `chunk_length` comes from
    /// `CompressionInfo.db` and may be any `u32` (including a hostile 4 GiB-1):
    /// the shift is applied to `1u64` under `checked_shl` (so a ramp ≥ 64 cannot
    /// UB) and multiplied in with `saturating_mul`, then clamped. No arithmetic
    /// path can panic, wrap, or exceed the cap.
    fn ramped_floor(&self, chunk_length: u64) -> u64 {
        let factor = 1u64.checked_shl(self.ramp).unwrap_or(u64::MAX);
        chunk_length
            .saturating_mul(factor)
            .min(COMPRESSED_SCAN_WINDOW_RAMP_MAX_BYTES)
    }

    /// Advance the ramp one doubling step, unless the clamp already binds (so the
    /// counter itself stays small and `ramped_floor` is a fixed point at the cap).
    fn advance_ramp(&mut self, chunk_length: u64) {
        if self.ramped_floor(chunk_length) < COMPRESSED_SCAN_WINDOW_RAMP_MAX_BYTES {
            self.ramp = self.ramp.saturating_add(1);
        }
    }

    /// Drop the dead prefix `[self.start, live_from)` — bytes the strictly
    /// ascending walk can never ask for again — and advance `self.start` to
    /// match. Called ONLY from a refill (issue #2877 roborev, blocker B), so the
    /// per-partition serving path performs zero copying.
    ///
    /// Front-only: `self.start + bytes.len()` (the chunk-aligned tail boundary a
    /// straddle-append resumes from) is invariant under this operation.
    fn compact_prefix(&mut self, live_from: u64) {
        if !self.filled || live_from <= self.start {
            return;
        }
        let drop_n = usize::try_from(live_from - self.start)
            .unwrap_or(usize::MAX)
            .min(self.bytes.len());
        if drop_n == 0 {
            return;
        }
        self.bytes.drain(0..drop_n);
        self.start += drop_n as u64;
        SCAN_WINDOW_PREFIX_COMPACTIONS.fetch_add(1, Ordering::Relaxed);
    }

    /// Fetch chunk-aligned decompressed bytes starting at `aligned_from` via the
    /// reader's existing `read_compressed_offset_window`, sized to cover at least
    /// up to `need_until`, padded out to `floor` (the ramped greedy floor), and
    /// rounded UP to a `chunk_length` boundary — never past `data_section_end`.
    ///
    /// `minimal.max(floor)` (never `min`): a single partition LARGER than the
    /// ramped floor is still served by one call, so the large-partition case
    /// cannot regress.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_aligned(
        reader: &SSTableReader,
        source: &dyn ReadAt,
        ci: &CompressionInfo,
        chunk_length: u64,
        aligned_from: u64,
        need_until: u64,
        data_section_end: u64,
        floor: u64,
    ) -> Result<Vec<u8>> {
        let remaining = data_section_end.saturating_sub(aligned_from);
        let minimal = need_until.saturating_sub(aligned_from);
        let want = minimal.max(floor).min(remaining);
        let raw_end = aligned_from.saturating_add(want);
        let rounded_end = raw_end.div_ceil(chunk_length).saturating_mul(chunk_length);
        let window_end = rounded_end.min(aligned_from.saturating_add(remaining));
        let window_len = window_end.saturating_sub(aligned_from);
        // Fail CLOSED on a zero-length request: `read_compressed_offset_window`
        // documents `size >= 1` (it derives `last_chunk` from `end - 1`). A valid
        // walk never asks for this (`need_until > aligned_from` and
        // `need_until <= data_section_end`), so reaching it means the caller's
        // offsets contradict `CompressionInfo.data_length`.
        if window_len == 0 {
            return Err(Error::corruption(format!(
                "walk_in_range_partition_slices: coalesced compressed window at \
                 {aligned_from} would be empty (need_until {need_until}, \
                 data_section_end {data_section_end}, issue #2877)"
            )));
        }
        let Ok(window_len_u32) = u32::try_from(window_len) else {
            return Err(Error::corruption(format!(
                "walk_in_range_partition_slices: coalesced compressed window length \
                 {window_len} overflows u32 (issue #2877)"
            )));
        };
        SCAN_WINDOW_REFILLS.fetch_add(1, Ordering::Relaxed);
        // `source` is the walk's SCAN-intent plane (issue #2876), threaded through
        // rather than read off the reader: the coalescing window must never widen
        // reads onto the `MADV_RANDOM` point mapping, or the readahead the larger
        // reads exist to earn is suppressed again (CASSANDRA-15452's lesson).
        reader
            .read_compressed_offset_window(source, ci, aligned_from, window_len_u32)
            .await
    }

    /// Serve `[start, end)` from this window, refilling iff not already
    /// covered. `data_section_end` bounds the last window so it never reads
    /// past the data section.
    ///
    /// Two distinct refill shapes (issue #2877 roborev finding, High —
    /// correctness): a naive "always resume exactly where the previous window
    /// ended" is WRONG whenever the current partition does not start exactly
    /// there.
    /// - **Straddle** (`start` is still inside the buffered window but `end`
    ///   runs past it — the common case once many small partitions have been
    ///   served from one big window): APPEND new chunk-aligned bytes onto the
    ///   tail (relative to the TRUE buffered end, computed before any
    ///   compaction), preserving the already-decompressed live prefix, so a
    ///   chunk already paid for is never re-decompressed.
    /// - **Gap** (`start` is beyond the buffered window entirely — e.g. the
    ///   out-of-range run SKIPPED between a compressed wraparound scan's two
    ///   segments, which are never read so the window never advances for
    ///   them; or the very first fill): REALIGN directly to the chunk
    ///   containing `start`, discarding whatever was buffered. Blindly
    ///   "continuing" from the stale tail here either UNDERFLOWS (`start -
    ///   self.start` when the stale tail is ahead of `start`) or leaves the
    ///   window short of `start` entirely (a false corruption error) —
    ///   exactly the bug this fixes.
    pub(super) async fn slice(
        &mut self,
        reader: &SSTableReader,
        source: &dyn ReadAt,
        ci: &CompressionInfo,
        start: u64,
        end: u64,
        data_section_end: u64,
    ) -> Result<&[u8]> {
        let chunk_length = ci.chunk_length as u64;
        if chunk_length == 0 {
            return Err(Error::corruption(
                "walk_in_range_partition_slices: CompressionInfo chunk_length is zero \
                 (issue #2877)"
                    .to_string(),
            ));
        }
        if end <= start {
            return Err(Error::corruption(format!(
                "walk_in_range_partition_slices: empty/reversed partition slice \
                 [{start}, {end}) (issue #2877)"
            )));
        }
        // The TRUE buffered end — captured BEFORE any compaction, which only ever
        // drops from the front and therefore leaves this value unchanged.
        let have_end = if self.filled {
            self.start + self.bytes.len() as u64
        } else {
            0
        };

        if !self.filled || start < self.start || end > have_end {
            if self.filled && start >= self.start && start <= have_end {
                // Straddle: CONSECUTIVE with what we already served, so the ramp
                // carries over. Reclaim the dead prefix (O(refills), blocker B),
                // then APPEND from the true buffered end.
                let floor = self.ramped_floor(chunk_length);
                self.compact_prefix(start);
                let extra = Self::fetch_aligned(
                    reader,
                    source,
                    ci,
                    chunk_length,
                    have_end,
                    end,
                    data_section_end,
                    floor,
                )
                .await?;
                self.bytes.extend_from_slice(&extra);
            } else {
                // Gap (or the very first fill, or a defensive `start <
                // self.start`): realign fresh, discarding the stale buffer. The
                // sequential run the ramp was paying for is BROKEN here, so the
                // ramp resets — the new region re-earns its read-ahead one chunk
                // at a time (issue #2877 roborev blocker A: a sparse range that
                // keeps jumping must never accumulate into a 4 MiB over-read).
                self.ramp = 0;
                let floor = self.ramped_floor(chunk_length);
                let aligned_start = (start / chunk_length) * chunk_length;
                self.bytes = Self::fetch_aligned(
                    reader,
                    source,
                    ci,
                    chunk_length,
                    aligned_start,
                    end,
                    data_section_end,
                    floor,
                )
                .await?;
                self.start = aligned_start;
            }
            self.filled = true;
            self.advance_ramp(chunk_length);
        }

        // Post-refill invariant: `self.start <= start` (a straddle compaction
        // lands exactly on `start`; a gap realign lands on the chunk BELOW it).
        // Assert it rather than papering over a violation with a saturating
        // subtraction, which would silently serve the WRONG bytes.
        if start < self.start {
            return Err(Error::corruption(format!(
                "walk_in_range_partition_slices: coalesced compressed window starts at \
                 {} but partition slice starts at {start} (issue #2877)",
                self.start
            )));
        }
        let lo = usize::try_from(start - self.start).unwrap_or(usize::MAX);
        let hi = usize::try_from(end - self.start).unwrap_or(usize::MAX);
        if hi > self.bytes.len() {
            return Err(Error::corruption(format!(
                "walk_in_range_partition_slices: coalesced compressed window short by \
                 {} bytes (issue #2877)",
                hi - self.bytes.len()
            )));
        }
        Ok(&self.bytes[lo..hi])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ramp must be overflow-safe and cap-bounded for EVERY
    /// `CompressionInfo.chunk_length` a (possibly hostile) sidecar can encode and
    /// every ramp value reachable by saturation — no panic, no wrap, never above
    /// the cap (issue #2877 roborev, blocker A).
    #[test]
    fn ramped_floor_is_overflow_safe_and_capped() {
        for chunk_length in [1u64, 4096, 16 * 1024, 64 * 1024, u32::MAX as u64] {
            for ramp in [0u32, 1, 10, 22, 23, 63, 64, 200, u32::MAX] {
                let w = CompressedScanWindow {
                    bytes: Vec::new(),
                    start: 0,
                    filled: false,
                    ramp,
                };
                let floor = w.ramped_floor(chunk_length);
                assert!(
                    floor <= COMPRESSED_SCAN_WINDOW_RAMP_MAX_BYTES,
                    "floor {floor} exceeded the cap for chunk_length {chunk_length} \
                     at ramp {ramp}"
                );
                assert!(
                    floor >= chunk_length.min(COMPRESSED_SCAN_WINDOW_RAMP_MAX_BYTES),
                    "floor {floor} fell below one chunk ({chunk_length}) at ramp {ramp}"
                );
            }
        }
    }

    /// The floor DOUBLES per refill from one chunk up to the cap, then holds
    /// steady — the ramp semantics an early-terminating scan and a long full scan
    /// respectively rely on (issue #2877 roborev, blocker A).
    #[test]
    fn ramp_doubles_from_one_chunk_then_holds_at_cap() {
        let chunk_length = 64 * 1024u64;
        let mut w = CompressedScanWindow::new();
        assert_eq!(
            w.ramped_floor(chunk_length),
            chunk_length,
            "a fresh window's first refill must ask for ONE chunk, not the target"
        );
        let mut seen = Vec::new();
        for _ in 0..12 {
            seen.push(w.ramped_floor(chunk_length));
            w.advance_ramp(chunk_length);
        }
        assert_eq!(
            &seen[..7],
            &[
                64 * 1024,
                128 * 1024,
                256 * 1024,
                512 * 1024,
                1024 * 1024,
                2048 * 1024,
                COMPRESSED_SCAN_WINDOW_RAMP_MAX_BYTES,
            ],
            "the floor must double per refill up to the cap: {seen:?}"
        );
        assert!(
            seen[7..]
                .iter()
                .all(|f| *f == COMPRESSED_SCAN_WINDOW_RAMP_MAX_BYTES),
            "past the cap the floor must hold steady: {seen:?}"
        );
        // The COUNTER itself must stop, not merely the floor it computes: 64 KiB
        // << 6 == 4 MiB == COMPRESSED_SCAN_WINDOW_RAMP_MAX_BYTES, so `advance_ramp`
        // takes exactly 6 steps and then becomes a no-op — even though the loop
        // above called it 12 times. An upper-bound assert (`<= 32`) would pass with
        // `advance_ramp`'s clamp guard DELETED (12 calls ⇒ ramp 12), pinning
        // nothing; `assert_eq!` is what makes "the counter stops at the cap" a
        // real claim.
        const EXPECTED_RAMP_AT_CAP: u32 = 6;
        assert_eq!(
            w.ramp,
            EXPECTED_RAMP_AT_CAP,
            "the ramp COUNTER must stop advancing at the cap after exactly \
             {EXPECTED_RAMP_AT_CAP} steps (chunk_length {chunk_length} << \
             {EXPECTED_RAMP_AT_CAP} == {} == the cap \
             {COMPRESSED_SCAN_WINDOW_RAMP_MAX_BYTES}), got {}",
            chunk_length << EXPECTED_RAMP_AT_CAP,
            w.ramp
        );
    }

    /// A chunk_length at or above the cap must clamp the ramped FLOOR to the cap
    /// immediately and never advance the ramp (nothing to ramp toward).
    ///
    /// Note what this does NOT claim: the cap bounds the FLOOR, not the window. An
    /// 8 MiB `chunk_length` (`CompressionInfo::parse` admits up to 256 MiB) still
    /// yields an 8 MiB window, because every refill rounds UP to a `chunk_length`
    /// boundary — asserted directly below so the distinction stays documented in
    /// executable form.
    #[test]
    fn oversized_chunk_length_clamps_the_floor_immediately() {
        let mut w = CompressedScanWindow::new();
        let chunk_length = 8 * 1024 * 1024u64;
        assert_eq!(
            w.ramped_floor(chunk_length),
            COMPRESSED_SCAN_WINDOW_RAMP_MAX_BYTES,
            "the FLOOR clamps at the cap"
        );
        w.advance_ramp(chunk_length);
        assert_eq!(w.ramp, 0, "the ramp must not advance once the clamp binds");
        // The resulting WINDOW is one whole chunk, i.e. ABOVE the cap: a refill's
        // length is the floor rounded up to a chunk boundary.
        let floor = w.ramped_floor(chunk_length);
        let window_len = floor.div_ceil(chunk_length) * chunk_length;
        assert_eq!(
            window_len, chunk_length,
            "an oversized chunk_length yields a window of one chunk ({chunk_length} \
             bytes), which EXCEEDS the floor cap \
             {COMPRESSED_SCAN_WINDOW_RAMP_MAX_BYTES} — the cap bounds read-AHEAD, \
             not the window"
        );
    }

    /// Compaction drops only from the FRONT: the chunk-aligned tail boundary a
    /// straddle-append resumes from must be invariant (issue #2877 blocker B).
    #[test]
    fn compact_prefix_preserves_the_tail_boundary() {
        let mut w = CompressedScanWindow {
            bytes: (0u8..64).collect(),
            start: 1000,
            filled: true,
            ramp: 0,
        };
        let tail_before = w.start + w.bytes.len() as u64;
        w.compact_prefix(1040);
        // There is NO separate logical-head field: reclamation is a per-refill
        // `Vec::drain` of the dead prefix plus an advance of `self.start` (equally
        // O(refills), one field fewer).
        assert_eq!(
            w.start, 1040,
            "`self.start` must advance to the requested live-from offset"
        );
        assert_eq!(
            w.start + w.bytes.len() as u64,
            tail_before,
            "the tail boundary must NOT move (a paid-for chunk must never be \
             re-decompressed)"
        );
        assert_eq!(
            w.bytes[0], 40,
            "the surviving bytes must be the live suffix"
        );
        // Idempotent / no-op for a `live_from` at or below the current `start`.
        // Asserted on
        // this window's OWN state rather than on the process-global
        // `scan_window_prefix_compaction_count()`: that counter is shared by every
        // test in the lib binary, so snapshotting it here would go non-deterministic
        // the moment any sibling test drives the compressed walk under cargo's
        // parallel intra-binary threads (the recurring flake class of
        // `big_locate_b4_repeat_zero_reprobe`). "Must not copy" IS local
        // invariance — `bytes.len()` and `start` unchanged.
        let len_before = w.bytes.len();
        let start_before = w.start;
        let first_byte_before = w.bytes[0];
        w.compact_prefix(1000); // strictly below `start` ⇒ nothing to drop
        w.compact_prefix(1040); // exactly `start` ⇒ nothing to drop
        assert_eq!(
            (w.bytes.len(), w.start, w.bytes[0]),
            (len_before, start_before, first_byte_before),
            "a no-op compaction must not copy: buffer length, start offset, and the \
             first live byte must all be unchanged"
        );
    }
}
