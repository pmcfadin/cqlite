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
//! path; only WHO opened the reader differs. Crucially, decode posture matches
//! too (issue #2349): the cold path
//! (`KWayMerger::new_with_gc_and_registry_cancellable`) and the warm registry BOTH
//! open readers WITH the SAME resolved UDT registry (from the ticket DDL's
//! `CREATE TYPE` statements), so a `frozen<UDT>`-in-collection cell decodes
//! structurally on both — parity guaranteed by matching the SAME authority, and
//! both flip together when the DDL declares no UDTs (registry-free).
//!
//! Lives in its own module (not `producer.rs`) because that file is over the
//! campsite file-size threshold (epic #1116).

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use cqlite_core::query::AccessPath;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::write_engine::{
    build_single_partition_merger_from_readers, KWayMerger, PointAccessRecording,
};

use crate::bypass::{bypass_reason_with_udts, ForcedMergePath, ScanRowSource};
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
            // Cold-path parity (issue #2310, roborev 1641): the cold
            // `produce_streaming` prunes paths in the CALLER before ever being
            // invoked, so an empty post-prune set never reaches a merger-built
            // call — its top-level `paths.is_empty()` check returns `Ok(())`
            // WITHOUT firing `on_merger_built` (no merger is built when there is
            // nothing to merge). The warm path prunes INTERNALLY (readers arrive
            // unpruned), so it must mirror that exact "nothing to merge → no
            // phase-boundary fire" behavior here, not treat an empty PRUNED set
            // like the point-read route's deliberate "still fire the boundary"
            // case (which fires because a merger-build attempt was genuinely
            // dispatched and came back empty, not because pruning skipped it).
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
            // Issue #2827: THIS is the Flight warm point path's logical point-read
            // boundary — one access per key, recorded here because no enclosing
            // layer records it (the cold path's builder records for itself, and the
            // core executor records at its own storage boundary). Never per-SSTable.
            let built = build_single_partition_merger_from_readers(
                readers,
                &key_bytes,
                &self.schema,
                cancel.scan_cancel(),
                PointAccessRecording::Record,
            )
            .map_err(|e| match e {
                cqlite_core::Error::Cancelled => ProducerError::Cancelled,
                other => ProducerError::Merge(other),
            })?;
            on_merger_built();
            return match built {
                None => Ok(()),
                // Issue #2423: row-granular streaming drive (see `produce_point`) —
                // bounds a warm wide-partition point read to one clustering group +
                // batch and makes cancellation mid-partition, byte-identically.
                Some(merger) => {
                    // Issue #2374/#2789: thread the read-time reconciliation clock
                    // so the warm point-read merge expires TTL cells with parity.
                    let mut merger = merger.with_now_secs(Some(self.now_secs));
                    self.drive_merge_over(&mut merger, cancel, sink, progress, label)
                }
            };
        }

        // Issue #2412 §C / #2413 Option A: push the split's token range INTO the
        // per-SSTable Summary-guided walk so out-of-range partition bodies are
        // never read (the token filter still runs downstream at `drive_merge` as a
        // backstop). A full scan (no token filter) passes `None` → full-ring walk.
        // BOTH arms below push the same bound.
        let token_bound = self.spec.token.as_ref().map(|t| t.to_scan_bound());

        // Issue #3058: with exactly ONE post-prune source there is nothing to
        // reconcile ACROSS generations — read-time SELECT semantics are applied
        // inside the single-generation decoder (`read_shadowing = true`,
        // `PartitionShadow`, issue #1741). Take the fast path only when the
        // conjunctive, fail-closed predicate holds (see `crate::bypass`); the
        // aggregation precondition is guaranteed by the `is_aggregating()` early
        // return above and the point-read precondition by the route returned
        // above, so both are ASSERTED here rather than re-derived.
        debug_assert!(
            !self.is_aggregating(),
            "the aggregate route returns above; the bypass site is unreachable for it"
        );
        let reason = bypass_reason_with_udts(
            &readers,
            &self.schema,
            ForcedMergePath::from_env(),
            self.is_aggregating(),
            // Issue #2339: whether the MERGE arm can decode a composite collection
            // element depends on this registry AND the keyspace it is keyed by, so
            // the divergence predicate gets the SAME scope the reassembler will.
            self.udt_scope(),
        );
        if reason.is_selected() {
            if let Some(reader) = readers.first().cloned() {
                // `open` PRIMES the walk: a reader the single-generation stream
                // cannot serve reports that BEFORE emitting anything, so this
                // falls through to the merge arm with no partial output.
                if let Some(mut source) = ScanRowSource::open(
                    reader,
                    self.schema.clone(),
                    token_bound,
                    // Issue #2374/#2789 + #3058: the SAME request-scoped
                    // reconciliation clock the merge arm threads via
                    // `with_now_secs` below, so TTL expiry is decided at one
                    // pinned instant on BOTH arms.
                    self.now_secs,
                    cancel.scan_cancel(),
                )? {
                    // Same phase boundary the merge arm fires: the inputs are
                    // open and rows are about to stream.
                    on_merger_built();
                    return self.drive_row_source(
                        &mut source,
                        cancel,
                        sink,
                        progress,
                        AccessPath::FullScan.label(),
                    );
                }
            }
        }

        let mut merger =
            KWayMerger::new_from_readers(readers, &self.schema, cancel.scan_cancel(), token_bound)
                .map_err(ProducerError::Merge)?
                // Issue #2374/#2789: thread the read-time reconciliation clock so
                // the warm full-scan merge expires TTL cells with parity to core.
                .with_now_secs(Some(self.now_secs));
        on_merger_built();
        // Issue #2423: the warm full-scan branch streams row-granularly too, matching
        // the cold full-scan path (#2230) — bounded memory + mid-partition cancel.
        self.drive_merge_over(
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use cqlite_core::{Config, Platform};

    use crate::filter::ScanSpec;
    use crate::producer::CollectSink;
    use crate::scan_progress::ScanProgress;
    use crate::testutil::{build_sstables, simple_schema, write_row};
    use crate::ticket::FlightTicket;

    use super::*;

    /// Finding 2 (#2310, roborev 1641): when token pruning empties the warm
    /// reader set inside `produce_streaming_from_readers`, the call must return
    /// WITHOUT firing `on_merger_built` — mirroring the cold path's "nothing to
    /// merge → no phase-boundary fire" posture (`produce_streaming`'s top-level
    /// `paths.is_empty()` check never fires it either, since the cold path prunes
    /// in the CALLER before the merge-driving call is ever invoked). Red on
    /// pre-fix code: the pruned-to-empty branch fired the callback
    /// unconditionally before returning.
    #[test]
    fn empty_pruned_warm_set_fires_no_merger_built_callback() {
        let schema = simple_schema();
        let (_temp, _data, table_dir) =
            build_sstables(&schema, vec![vec![write_row(1, "a", 10, 100)]]);
        let data_db = std::fs::read_dir(&table_dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.ends_with("-Data.db"))
            })
            .expect("a Data.db exists");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let reader = rt.block_on(async {
            let config = Config::default();
            let platform = Arc::new(Platform::new(&config).await.unwrap());
            Arc::new(
                SSTableReader::open(&data_db, &config, platform)
                    .await
                    .unwrap(),
            )
        });
        let (min_tok, max_tok) = reader
            .endpoint_tokens()
            .expect("Summary.db parsed, endpoint tokens known");

        // A token range strictly beyond the reader's own span, guaranteed NOT to
        // overlap it — pruning must drop this single reader entirely.
        let spec = ScanSpec::from_ticket(
            &FlightTicket {
                token_start: Some(max_tok.saturating_add(1)),
                token_end: Some(max_tok.saturating_add(2)),
                ..Default::default()
            },
            &schema,
        )
        .unwrap();
        assert!(
            !spec.token.as_ref().unwrap().overlaps(min_tok, max_tok),
            "the chosen range must genuinely exclude the reader's span"
        );

        let producer = MergeProducer::with_spec(schema, 1024, spec).unwrap();
        let fired = Arc::new(AtomicBool::new(false));
        let fired_probe = Arc::clone(&fired);
        let mut batches = Vec::new();
        let mut sink = CollectSink(&mut batches);
        producer
            .produce_streaming_from_readers(
                vec![reader],
                &CancelFlag::new(),
                &mut sink,
                &ScanProgress::default(),
                move || fired_probe.store(true, Ordering::SeqCst),
            )
            .expect("empty-pruned warm set is a clean no-op, not an error");

        assert!(
            !fired.load(Ordering::SeqCst),
            "on_merger_built must NOT fire when token pruning empties the warm \
             reader set (cold-path phase-accounting parity, roborev 1641)"
        );
        assert!(batches.is_empty(), "nothing was merged, nothing streamed");
    }
}
