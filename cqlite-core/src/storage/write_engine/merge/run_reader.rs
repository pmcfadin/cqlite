//! `RunReader`: the buffered reader over one SSTable run (split out of `merge/mod.rs`, #4658).

use super::*;

/// Buffered reader for a single SSTable run
///
/// Maintains a peek buffer for efficient lookahead without repeated I/O.
/// Buffer size is fixed at 8KB worth of entries for predictable memory usage.
#[cfg(feature = "write-support")]
pub(super) struct RunReader {
    /// Abstract SSTable row iterator (boxed, not Debug)
    pub(super) reader: Box<dyn SSTableRowIterator>,
    /// Peek buffer (FIFO)
    buffer: VecDeque<MergeEntry>,
    /// Target buffer size in bytes (~8KB)
    buffer_size: usize,
    /// Whether this run is exhausted
    exhausted: bool,
}

#[cfg(feature = "write-support")]
impl std::fmt::Debug for RunReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunReader")
            .field("buffer_len", &self.buffer.len())
            .field("buffer_size", &self.buffer_size)
            .field("exhausted", &self.exhausted)
            .finish()
    }
}

#[cfg(feature = "write-support")]
impl RunReader {
    /// Default buffer size (8KB worth of entries)
    pub(super) const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

    /// Create a new run reader
    pub(super) fn new(reader: Box<dyn SSTableRowIterator>) -> Self {
        Self {
            reader,
            buffer: VecDeque::new(),
            buffer_size: Self::DEFAULT_BUFFER_SIZE,
            exhausted: false,
        }
    }

    /// Peek at the next entry without consuming it
    ///
    /// Returns None if this run is exhausted.
    ///
    /// Test-only since issue #1664: `refill_heap` was the sole production
    /// caller and now moves the owned entry via [`Self::advance`] instead of
    /// peek+clone. Kept for the `RunReader` API test that exercises lazy refill.
    #[cfg(test)]
    pub(super) fn peek(&mut self) -> Result<Option<&MergeEntry>> {
        // Refill buffer if empty and not exhausted
        if self.buffer.is_empty() && !self.exhausted {
            self.refill_buffer()?;
        }

        Ok(self.buffer.front())
    }

    /// Advance to the next entry
    ///
    /// Consumes the front entry and returns it.
    pub(super) fn advance(&mut self) -> Result<Option<MergeEntry>> {
        if let Some(entry) = self.buffer.pop_front() {
            return Ok(Some(entry));
        }

        // Buffer empty, try to refill
        if !self.exhausted {
            self.refill_buffer()?;
            Ok(self.buffer.pop_front())
        } else {
            Ok(None)
        }
    }

    /// Check if this run is exhausted
    pub(super) fn is_exhausted(&self) -> bool {
        self.exhausted && self.buffer.is_empty()
    }

    /// Refill the peek buffer from the underlying reader
    fn refill_buffer(&mut self) -> Result<()> {
        let mut bytes_buffered = 0;

        while bytes_buffered < self.buffer_size {
            // Issue #2819 (B2): time the BLOCKING recv into the pull-wait
            // accumulator so the row-drive loop excludes it from `stream_merge`
            // (zero cost when no flight sink is installed).
            let next = crate::observability::stream_subphase::time_recv(|| self.reader.next());
            match next {
                Some(Ok(entry)) => {
                    // Estimate entry size for buffer management. `saturating_add`:
                    // the estimator fails CLOSED at `usize::MAX` for a
                    // pathologically deep/wide value (#2820), which must stop
                    // read-ahead, never overflow this accumulator.
                    bytes_buffered =
                        bytes_buffered.saturating_add(Self::estimate_entry_size(&entry));
                    self.buffer.push_back(entry);
                }
                Some(Err(e)) => return Err(e),
                None => {
                    self.exhausted = true;
                    break;
                }
            }
        }

        Ok(())
    }

    /// Estimate the memory size of an entry
    ///
    /// Delegates to [`entry_size::estimate_entry_size`], a
    /// sibling module (#1116 campsite rule) that walks nested values with a
    /// bounded iterative traversal and an EXHAUSTIVE `Value` match. The previous
    /// inline version ended in `_ => 32` for every complex variant, which made
    /// both byte budgets denominated in this figure — this read-ahead buffer and
    /// the #2820 egress batch budget — bypassable by large nested payloads.
    pub(super) fn estimate_entry_size(entry: &MergeEntry) -> usize {
        entry_size::estimate_entry_size(entry)
    }
}

/// Abstract iterator trait for SSTable rows
///
/// This allows the K-way merger to work with different SSTable reader
/// implementations without coupling to specific reader types.
#[cfg(feature = "write-support")]
pub trait SSTableRowIterator: Send {
    /// Get the next row from this SSTable
    fn next(&mut self) -> Option<Result<MergeEntry>>;

    /// Test-only observation hook (issue #2765): the bounded egress
    /// `sync_channel` capacity this run's producer→consumer channel was
    /// constructed with — i.e. the EXACT argument passed to `sync_channel`, so a
    /// wiring test can prove the adaptive per-channel capacity actually reaches
    /// the construction site. `None` for runs that have no such channel
    /// (synthetic/seeked `Vec`-backed iterators).
    ///
    /// Issue #2820: that argument is in MESSAGES (the channel carries BATCHES),
    /// so the hook reports BOTH halves of the conversion — the message capacity
    /// AND the `egress_budget` ROW snapshot it was derived from — and a wiring
    /// test asserts the equivalence. Reporting only one would let a channel
    /// silently built with the ROW budget as its message capacity (a 256x
    /// resident-row blow-up) read as correct.
    #[cfg(test)]
    fn egress_channel_capacity(&self) -> Option<usize> {
        None
    }

    /// Test-only sibling of [`Self::egress_channel_capacity`] (issue #2820): the
    /// ROW capacity that message capacity was derived from.
    #[cfg(test)]
    fn egress_rows_capacity(&self) -> Option<usize> {
        None
    }
}
