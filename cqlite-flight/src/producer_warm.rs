//! Warm reader-set merge routing on the producer (issue #2310, WS3 #2342).
//!
//! The cold `do_get` path opens a fresh [`SSTableReader`] per input on every
//! request ([`MergeProducer::produce_streaming`] → `KWayMerger::new_cancellable`,
//! paying the Index/Summary/Statistics/bloom parse each time). This module adds
//! the WARM analogue: the [`crate::warm::WarmTableRegistry`] hands the producer
//! already-open, possibly-SHARED `Arc<SSTableReader>`s (kept parsed across
//! requests) and the merge is driven via the #2346 reader-based seams
//! (`KWayMerger::new_from_readers` / `build_single_partition_merger_from_readers`).
//! Everything downstream — the point-read route selection, LIMIT/budget pushdown,
//! tombstone reconciliation, Arrow conversion — is byte-identical to the cold
//! path; only WHO opened the reader differs.
//!
//! Lives in its own module (not `producer.rs`) because that file is over the
//! campsite file-size threshold (epic #1116).

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use cqlite_core::query::AccessPath;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::write_engine::{build_single_partition_merger_from_readers, KWayMerger};

use crate::cancel::CancelFlag;
use crate::producer::{BatchSink, CollectSink, MergeProducer, ProducerError};
use crate::scan_progress::ScanProgress;

impl MergeProducer {
    /// Stream the row-merge of a WARM reader set (issue #2310), mirroring
    /// [`MergeProducer::produce_streaming`] but over already-open
    /// `Arc<SSTableReader>`s. `readers` MUST be ordered newest-generation-first
    /// (the warm registry guarantees this) — run index = LWW tie-break rank.
    ///
    /// Token pruning uses each reader's ALREADY-PARSED endpoint tokens
    /// (`SSTableReader::endpoint_tokens`, zero extra I/O), so a warm hit re-reads
    /// no `Summary.db` even for a token-filtered scan — preserving the
    /// "zero Index/Summary/Statistics/bloom parse" property (spec Requirement 2).
    pub(crate) fn produce_streaming_from_readers(
        &self,
        readers: Vec<Arc<SSTableReader>>,
        cancel: &CancelFlag,
        sink: &mut dyn BatchSink,
        progress: &ScanProgress,
        on_merger_built: impl FnOnce(),
    ) -> Result<(), ProducerError> {
        if self.is_aggregating() || readers.is_empty() {
            return Ok(());
        }
        // Token-prune the warm reader set with zero extra I/O.
        let readers = self.prune_readers(readers);
        if readers.is_empty() {
            on_merger_built();
            return Ok(());
        }

        // Issue #2207: a pushed full-PK-equality predicate routes to the partition
        // point-read path over the SAME warm readers, exactly as the cold path.
        if let Some(plan) = self.point_read_keys() {
            let key_bytes: Vec<Vec<u8>> = plan.keys.into_iter().map(|(k, _)| k).collect();
            let label = plan.access_path.label();
            if key_bytes.is_empty() {
                on_merger_built();
                return Ok(());
            }
            let built = build_single_partition_merger_from_readers(
                readers,
                &key_bytes,
                &self.schema,
                cancel.scan_cancel(),
            )
            .map_err(|e| match e {
                cqlite_core::Error::Cancelled => ProducerError::Cancelled,
                other => ProducerError::Merge(other),
            })?;
            on_merger_built();
            return match built {
                None => Ok(()),
                Some(mut merger) => self.drive_merge(&mut merger, cancel, sink, progress, label),
            };
        }

        let mut merger = KWayMerger::new_from_readers(readers, &self.schema, cancel.scan_cancel())
            .map_err(ProducerError::Merge)?;
        on_merger_built();
        self.drive_merge(
            &mut merger,
            cancel,
            sink,
            progress,
            AccessPath::FullScan.label(),
        )
    }

    /// Token-prune a warm reader set by each reader's already-parsed endpoint
    /// tokens (zero I/O). Returns `readers` unchanged when there is no token
    /// filter. A reader whose endpoint tokens are unknown (Summary.db was absent)
    /// is KEPT (fail open) — pruning must never drop an SSTable that might contain
    /// matching partitions, exactly like the path-based `prune_paths`.
    fn prune_readers(&self, readers: Vec<Arc<SSTableReader>>) -> Vec<Arc<SSTableReader>> {
        let Some(token) = &self.spec.token else {
            return readers;
        };
        readers
            .into_iter()
            .filter(|r| match r.endpoint_tokens() {
                Some((min_token, max_token)) => token.overlaps(min_token, max_token),
                None => true,
            })
            .collect()
    }

    /// Collect the warm reader-set streaming merge into a `Vec` (issue #2310
    /// wiring evidence + dual-path parity): the SAME path `do_get` streams over
    /// warm readers, so a test can compare its batches byte-for-byte against the
    /// cold path.
    pub fn produce_streaming_from_readers_to_vec(
        &self,
        readers: Vec<Arc<SSTableReader>>,
        cancel: &CancelFlag,
    ) -> Result<Vec<RecordBatch>, ProducerError> {
        let mut batches = Vec::new();
        {
            let mut sink = CollectSink(&mut batches);
            self.produce_streaming_from_readers(
                readers,
                cancel,
                &mut sink,
                &ScanProgress::default(),
                || {},
            )?;
        }
        Ok(batches)
    }
}
