//! The `SSTableReader` OPEN constructors and the single place a failed open is
//! recorded (issues #1037, #1704).
//!
//! Split out of `reader/mod.rs` per the campsite rule (#1116): that file is 1721
//! lines against the ~800-line target, and issue #1704 needed to add a constructor
//! here. Everything below is a VERBATIM move except [`SSTableReader::open_unrecorded`],
//! which is new.
//!
//! # One recording boundary per failed operation
//!
//! Every public constructor funnels into [`SSTableReader::open_with_cache_cancellable`],
//! which records a failed open into `cqlite.errors.total{category, subsystem="reader"}`.
//! That is correct when the open IS the operation. It is WRONG when the open is an
//! inner step of an operation whose own seam also records — the failure is then
//! counted twice for one user-visible failure. This is the same single-boundary rule
//! `compaction::finalize_merge_async` already states for its own inner helpers
//! ("this is an *unrecorded* inner helper ... recording here too would double-count").
//!
//! [`SSTableReader::open_unrecorded`] is the entry point for those inner steps.

use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::sstables_open_count_for;
use super::SSTableReader;
use crate::platform::Platform;
use crate::storage::scan_cancel::ScanCancel;
use crate::{Config, Result};

impl SSTableReader {
    /// Open an SSTable file for reading.
    ///
    /// Instrumented (epic #1031 / #1034): wraps the open in a
    /// `sstable.reader.open` span, increments the [`SSTABLES_OPEN`] gauge on
    /// success, and records an error on the `reader` subsystem when open fails.
    ///
    /// [`SSTABLES_OPEN`]: crate::observability::catalog::SSTABLES_OPEN
    pub async fn open(path: &Path, config: &Config, platform: Arc<Platform>) -> Result<Self> {
        // Back-compat: existing callers and sibling crates get a FRESH per-reader
        // decompressed-chunk cache sized from config (issue #1567). Production
        // reads route through `SSTableManager`, which calls `open_with_cache` with
        // its SHARED instance so all readers of a dataset share one cache.
        //
        // Honor the `config.memory.block_cache.enabled` toggle (issue #1568): when
        // disabled this yields a genuine no-op cache so the direct reader path
        // bypasses caching identically to the manager path, instead of the toggle
        // being decorative here. Reuses the shared `build_chunk_cache` helper.
        let cache = super::super::build_chunk_cache(config);
        Self::open_with_cache(path, config, platform, cache).await
    }

    /// Cancel-aware [`open`](Self::open) (issue #2383 fix C).
    ///
    /// Threads a synchronous [`ScanCancel`](crate::storage::scan_cancel::ScanCancel)
    /// into the Index.db partition-index parse so a client-disconnect cancel
    /// aborts a 1.58M-entry parse within one poll interval instead of pinning a
    /// tokio worker to completion. Non-cancellable callers use [`open`](Self::open)
    /// (a default never-cancel flag). Returns [`Error::Cancelled`] on a mid-parse
    /// trip.
    pub async fn open_cancellable(
        path: &Path,
        config: &Config,
        platform: Arc<Platform>,
        cancel: crate::storage::scan_cancel::ScanCancel,
    ) -> Result<Self> {
        let cache = super::super::build_chunk_cache(config);
        Self::open_with_cache_cancellable(path, config, platform, cache, cancel).await
    }

    /// Open an SSTable file for reading, sharing the provided
    /// [`DecompressedChunkCache`](crate::storage::cache::DecompressedChunkCache).
    ///
    /// Identical to [`open`](Self::open) except the reader stores `cache` (an
    /// `Arc` clone) instead of minting its own, so every reader a manager opens
    /// for one dataset consults the same bytes-bounded chunk cache (issue #1567).
    pub async fn open_with_cache(
        path: &Path,
        config: &Config,
        platform: Arc<Platform>,
        cache: Arc<crate::storage::cache::DecompressedChunkCache>,
    ) -> Result<Self> {
        Self::open_with_cache_cancellable(
            path,
            config,
            platform,
            cache,
            crate::storage::scan_cancel::ScanCancel::default(),
        )
        .await
    }

    /// Cancel-aware [`open_with_cache`](Self::open_with_cache) (issue #2383 fix C):
    /// same as it but threads `cancel` into the Index.db parse. See
    /// [`open_cancellable`](Self::open_cancellable).
    pub async fn open_with_cache_cancellable(
        path: &Path,
        config: &Config,
        platform: Arc<Platform>,
        cache: Arc<crate::storage::cache::DecompressedChunkCache>,
        cancel: crate::storage::scan_cancel::ScanCancel,
    ) -> Result<Self> {
        Self::open_instrumented(
            path,
            config,
            platform,
            cache,
            cancel,
            OpenErrorReporting::SelfReported,
        )
        .await
    }

    /// [`open`](Self::open) with the failed-open reporting mode stated BY THE CALLER
    /// (issue #1704).
    ///
    /// # Why this is a parameter and not a second named function
    ///
    /// Both mistakes are silent. [`OpenErrorReporting::SelfReported`] under a caller
    /// that also records counts one failure TWICE;
    /// [`OpenErrorReporting::DeferredToCaller`] under a caller with no seam records it
    /// ZERO times, which is worse and is exactly the regression a universally
    /// non-recording open introduced. Only the caller knows which it is, so it must
    /// say — an unparameterised `open_unrecorded` encoded a precondition that nothing
    /// at the call site could check.
    ///
    /// # Behavioural identity, which is now real rather than claimed
    ///
    /// Exactly ONE action differs from [`open`](Self::open): the `record_error` call on
    /// the `Err` arm. Both routes go through [`open_instrumented`](Self::open_instrumented),
    /// so the `sstable.reader.open` span, its `file_size`/`sstable_format` fields, the
    /// parenting of the nested open-phase spans, and the `SSTABLES_OPEN` gauge are
    /// literally the same code. The first version of this function called `open_inner`
    /// directly and silently dropped the span while its doc claimed identity — the
    /// shared helper is what makes the claim checkable instead of aspirational.
    pub(crate) async fn open_with_reporting(
        path: &Path,
        config: &Config,
        platform: Arc<Platform>,
        reporting: OpenErrorReporting,
    ) -> Result<Self> {
        let cache = super::super::build_chunk_cache(config);
        Self::open_instrumented(
            path,
            config,
            platform,
            cache,
            ScanCancel::default(),
            reporting,
        )
        .await
    }

    /// The ONE open implementation: span, success gauge, and — for
    /// [`OpenErrorReporting::SelfReported`] only — the failed-open increment.
    async fn open_instrumented(
        path: &Path,
        config: &Config,
        platform: Arc<Platform>,
        cache: Arc<crate::storage::cache::DecompressedChunkCache>,
        cancel: crate::storage::scan_cancel::ScanCancel,
        reporting: OpenErrorReporting,
    ) -> Result<Self> {
        use crate::observability::{self as obs, catalog};
        use tracing::Instrument as _;

        let span = tracing::debug_span!(
            "sstable.reader.open",
            file_size = tracing::field::Empty,
            sstable_format = tracing::field::Empty,
        );

        // Instrument the future rather than holding an entered guard across the
        // `.await`: entering a span guard and then awaiting can attach unrelated
        // async work scheduled on this task to the span. `Instrument` enters the
        // span only while this specific future is polled.
        let result = Self::open_inner(path, config, platform, cache, cancel)
            .instrument(span.clone())
            .await;
        match &result {
            Ok(reader) => {
                let format = reader.sstable_format_label();
                span.record("file_size", reader.stats.file_size);
                span.record("sstable_format", format);
                // SSTABLES_OPEN is a snapshot gauge of the live reader count;
                // record the current PER-FORMAT count after this open succeeds so
                // the format-attributed gauge series stays correct under mixed
                // BIG/BTI readers. A SUCCESSFUL open is identical in both modes:
                // the reporting mode is only ever about who counts a FAILURE.
                let now = sstables_open_count_for(format).fetch_add(1, Ordering::Relaxed) + 1;
                obs::record_gauge(
                    catalog::SSTABLES_OPEN,
                    now,
                    &[(catalog::attr::SSTABLE_FORMAT, format.into())],
                );
            }
            Err(e) => {
                if matches!(reporting, OpenErrorReporting::SelfReported) {
                    // Record the error WHILE the open span is current so
                    // `mark_span_error` marks THIS `sstable.reader.open` span. The
                    // instrumented future has already completed here, so the span is
                    // no longer entered; `in_scope` re-enters it for the duration of
                    // the error-recording call.
                    span.in_scope(|| obs::record_error(e, "reader"));
                }
            }
        }
        result
    }
}

/// Who counts a failed open into `cqlite.errors.total{subsystem="reader"}`
/// (issue #1704). The ONLY thing that varies between the two open routes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OpenErrorReporting {
    /// This open IS the operation: it records its own failure. The mode every
    /// PUBLIC entry point uses, so a caller who does not think about it is never
    /// silently un-instrumented.
    SelfReported,
    /// This open is an inner step; the caller's operation seam records instead.
    ///
    /// Constructed by `scan_delta` (feature `delta-scan`) and by the cross-generation
    /// streaming merge (`not(feature = "tombstones")`). A build with `tombstones` on
    /// and `delta-scan` off therefore has no constructor for it — the variant is still
    /// the correct half of the distinction, so it is allowed rather than cfg'd away
    /// into a shape that would differ per feature set.
    #[allow(dead_code)]
    DeferredToCaller,
}
