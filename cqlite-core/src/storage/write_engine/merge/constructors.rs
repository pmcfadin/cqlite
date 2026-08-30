//! `KWayMerger`'s opening constructors, and the ONE place a caller states who counts
//! a failed input open (issues #1037, #1704).
//!
//! Split out of `merge/mod.rs` per the campsite rule (#1116): that file is 12,655
//! lines against the ~800-line target, so the parameter issue #1704 needed could not
//! be added there. The public constructor below is a VERBATIM move; the
//! reporting-aware one it delegates to is new.
//!
//! # Why the caller states it, rather than the callee assuming it
//!
//! Every input is REOPENED inside its producer thread. A recording open
//! double-counts when the caller's own seam also records (issue #1704, roborev C on
//! the streaming read path); a NON-recording open loses the signal entirely when the
//! caller has no seam — and `KWayMerger` is externally reachable public API
//! (`cqlite_core::storage::write_engine::merge::KWayMerger::new`), so "wrap every
//! caller" has no complete form: there is always one more call site outside this
//! crate. The callee therefore cannot assume either way, and the mode is an explicit
//! parameter of the internal constructor. Everything public keeps
//! [`OpenErrorReporting::SelfReported`] — today's behaviour — so a new caller that
//! does not think about it gets a visible extra increment, never silence.

use std::collections::BinaryHeap;
use std::path::PathBuf;

use super::{egress_budget, KWayMerger, RunReader, SSTableRowIteratorAdapter};
use crate::schema::TableSchema;
use crate::storage::sstable::reader::OpenErrorReporting;
use crate::{Error, Result};

impl KWayMerger {
    /// K-way merge constructor that opens the input SSTables under a cooperative
    /// [`ScanCancel`](crate::storage::scan_cancel::ScanCancel) (issue #2264).
    ///
    /// The token is wired onto every per-run reader so the compaction scan each
    /// run's producer thread drives — which, for an index-less (Summary.db
    /// absent) SSTable, otherwise fully materialises the whole Data.db in one
    /// uninterruptible pass — polls it at a bounded interval and abandons the walk
    /// promptly when a driving Flight `do_get` is cancelled. `new`/`new_with_gc*`
    /// delegate here with a never-cancelled default token, so non-Flight callers
    /// are unaffected.
    ///
    /// Failed input opens are [`OpenErrorReporting::SelfReported`]: this is public API
    /// and its callers are not required to have a recording seam (issue #1704).
    pub fn new_with_gc_and_registry_cancellable(
        input_paths: Vec<PathBuf>,
        schema: &TableSchema,
        gc_before_secs: Option<i64>,
        now_secs: Option<i64>,
        udt_registry: Option<crate::schema::UdtRegistry>,
        scan_cancel: crate::storage::scan_cancel::ScanCancel,
    ) -> Result<Self> {
        Self::new_with_open_reporting(
            input_paths,
            schema,
            gc_before_secs,
            now_secs,
            udt_registry,
            scan_cancel,
            OpenErrorReporting::SelfReported,
        )
    }

    /// [`new_with_gc_and_registry_cancellable`](Self::new_with_gc_and_registry_cancellable)
    /// with the failed-input-open reporting mode stated by the CALLER (issue #1704).
    ///
    /// Pass [`OpenErrorReporting::DeferredToCaller`] only from a call site that can
    /// name the seam which will count the failure instead. See the module doc.
    pub(crate) fn new_with_open_reporting(
        input_paths: Vec<PathBuf>,
        schema: &TableSchema,
        gc_before_secs: Option<i64>,
        now_secs: Option<i64>,
        udt_registry: Option<crate::schema::UdtRegistry>,
        scan_cancel: crate::storage::scan_cancel::ScanCancel,
        reporting: OpenErrorReporting,
    ) -> Result<Self> {
        if input_paths.is_empty() {
            return Err(Error::InvalidInput(
                "K-way merge requires at least one input file".to_string(),
            ));
        }

        // Enforce the dropped-column decode contract (#904/#847): every column
        // named in `dropped_columns` must still be declared in `columns` so its
        // cells decode and can be purged. Guard here (the authoritative compaction
        // entry) for programmatically-built schemas that bypass `validate()`.
        schema.validate_dropped_columns()?;

        // Create run readers for each input SSTable (ordered newest to oldest).
        // The registry (when supplied) is cloned onto each reader so a frozen UDT
        // value decodes structurally instead of erroring out and dropping the row
        // (issue #1234). No producer-side `LIMIT` push-down (issue #2361, roborev
        // round 2): every producer scans until genuinely cancelled — `LIMIT` is
        // enforced purely downstream (consumer post-reconciliation break +
        // cancel-aware Drop teardown). See
        // [`SSTableReader::stream_all_partitions_cancellable`](crate::storage::sstable::reader::SSTableReader::stream_all_partitions_cancellable)'s
        // doc for why a partition-granular producer budget cannot be correct.
        // Issue #2765: register this k-way merge ONCE and snapshot the adaptive
        // per-channel egress capacity; ALL source channels below share it, so a
        // solo compaction gets 256 per source regardless of input count.
        let (channel_capacity, egress_slot) = egress_budget::begin_merge();

        let mut runs = Vec::with_capacity(input_paths.len());
        for (run_index, path) in input_paths.iter().enumerate() {
            let adapter = SSTableRowIteratorAdapter::open(
                path,
                run_index,
                schema,
                udt_registry.clone(),
                scan_cancel.clone(),
                channel_capacity,
                reporting,
            )?;
            runs.push(RunReader::new(Box::new(adapter)));
        }

        // Initialize heap (will be populated on first step)
        let heap = BinaryHeap::new();

        Ok(Self {
            runs,
            heap,
            current_partition: None,
            schema: schema.clone(),
            // Issue #1668, stage 5c-i: `Arc`-wrapped clone of `schema`, used
            // only by the heap's schema-aware comparator (see the field doc).
            schema_arc: std::sync::Arc::new(schema.clone()),
            gc_before_secs,
            now_secs,
            // Conservatively unsafe by default (#921 finding 1): purging is
            // dormant until a caller proves the compaction spans all of the
            // table's SSTables via `with_purge_safe`.
            purge_safe: false,
            // No overlap bound by default (#935): a partial compaction stays
            // conservative (no purging) until a caller supplies the min outside
            // timestamp via `with_max_purgeable_timestamp`.
            max_purgeable_timestamp: None,
            _egress_slot: Some(egress_slot),
        })
    }
}

/// Build the cross-generation STREAMING read merge, whose failed input opens are
/// counted by the measured `JoinedStream` the rows are delivered through — so the
/// reopen inside each producer thread must NOT count them again (issue #1704).
///
/// A named helper rather than an inline call so the one site that defers can state
/// WHY in one place, and so `generation_merge`'s call stays a single expression.
pub(crate) fn new_merger_deferring_open_errors(
    input_paths: Vec<PathBuf>,
    schema: &TableSchema,
) -> Result<KWayMerger> {
    KWayMerger::new_with_open_reporting(
        input_paths,
        schema,
        None,
        None,
        None,
        crate::storage::scan_cancel::ScanCancel::default(),
        OpenErrorReporting::DeferredToCaller,
    )
}
