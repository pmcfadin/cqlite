//! Partition point-read routing on the producer (issue #2207).
//!
//! Extends [`MergeProducer`] with the `do_get` point path: detect a full-PK
//! equality route, resolve concrete partition keys (token-excluded before any
//! seek), build a k-way merger over ONLY the target partition(s), then reconcile
//! and stream through the SAME [`MergeProducer::drive_merge`] loop the scan path
//! uses — reporting the access-path label of the ROUTE VARIANT the router chose
//! (`streaming_partition_lookup` for a single-PK route, `multi_partition_lookup`
//! for an `IN`/`Or` list, independent of the surviving-key count).
//!
//! Lives in its own module (not `producer.rs`) because that file is over the
//! campsite file-size threshold (epic #1116).

use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use cqlite_core::query::AccessPath;
use cqlite_core::storage::write_engine::{build_single_partition_merger, PartitionKey};
use cqlite_core::types::Value;

use crate::cancel::CancelFlag;
use crate::point_read::{detect_route, PointReadRoute};
use crate::producer::{BatchSink, CollectSink, MergeProducer, ProducerError};
use crate::scan_progress::ScanProgress;

/// A resolved partition point-read plan: the access-path label fixed by the
/// router's ROUTE VARIANT, plus the token-pruned target keys to seek.
///
/// `access_path` is decided by [`detect_route`]'s variant BEFORE any
/// dedup/token-pruning, so it never flip-flops on the surviving-key count
/// (roborev job 1623): a `MultiPartitionPointRead` route stays
/// `multi_partition_lookup` even when its keys prune to one survivor.
pub(crate) struct PointReadPlan {
    /// Label = router's decision (route variant), NOT surviving key count.
    pub(crate) access_path: AccessPath,
    /// `(raw_key_bytes, token)` per surviving target key, in the route's order.
    pub(crate) keys: Vec<(Vec<u8>, i64)>,
}

impl MergeProducer {
    /// Run the streaming merge over already-resolved `paths` and collect the
    /// batches into a `Vec` (issue #2207 wiring evidence + dual-path parity).
    ///
    /// This is the SAME path `do_get` streams — including the point-read route
    /// selection — so a test can drive the point path end-to-end and compare its
    /// batches byte-for-byte against the full-scan collect path
    /// ([`MergeProducer::produce_from_paths`]). `paths` MUST be token-pruned /
    /// resolved (as [`MergeProducer::resolve_paths`] returns).
    pub fn produce_streaming_to_vec(
        &self,
        paths: Vec<PathBuf>,
        cancel: &CancelFlag,
    ) -> Result<Vec<RecordBatch>, ProducerError> {
        let mut batches = Vec::new();
        {
            let mut sink = CollectSink(&mut batches);
            self.produce_streaming(paths, cancel, &mut sink, &ScanProgress::default(), || {})?;
        }
        Ok(batches)
    }
    /// Resolve the point-read route into concrete raw partition keys, or `None`
    /// when the pushed predicate is not a full-PK equality (the scan path).
    ///
    /// Returns the access-path label the read will report ALONGSIDE the keys. The
    /// label reflects the ROUTE VARIANT `detect_route` decided — independent of
    /// how many keys survive later dedup/token pruning: a `MultiPartitionPointRead`
    /// (a full-PK `IN`/`Or` list, even one that prunes to a single surviving key)
    /// stays `multi_partition_lookup`; a plain `PartitionPointRead` is
    /// `streaming_partition_lookup`. This keeps the field-run evidence label stable
    /// per the router's decision (roborev job 1623) instead of flip-flopping on the
    /// surviving-key count.
    ///
    /// Each returned key is `(raw_key_bytes, token)`, in the route's order. Keys
    /// whose Murmur3 token falls OUTSIDE the split's token range are dropped here
    /// (before any seek) — the point read stays within the split's range exactly
    /// as the scan path's per-partition token guard does. A key whose typed values
    /// cannot be serialized to partition-key bytes (should not happen for a
    /// schema-typed predicate) drops the whole route to the scan path (returns
    /// `None`) rather than risk a wrong answer.
    pub(crate) fn point_read_keys(&self) -> Option<PointReadPlan> {
        // Label = router's decision (route variant), NOT surviving key count —
        // single-element IN is still a multi-partition ROUTE (roborev job 1623).
        let (access_path, component_keys): (AccessPath, Vec<Vec<Value>>) =
            match detect_route(self.spec.filter.as_ref(), &self.schema) {
                PointReadRoute::Scan => return None,
                PointReadRoute::PartitionPointRead(values) => {
                    (AccessPath::StreamingPartitionLookup, vec![values])
                }
                PointReadRoute::MultiPartitionPointRead(keys) => {
                    (AccessPath::MultiPartitionLookup, keys)
                }
            };

        let pk_names: Vec<&str> = self
            .schema
            .partition_keys
            .iter()
            .map(|c| c.name.as_str())
            .collect();

        let mut out: Vec<(Vec<u8>, i64)> = Vec::with_capacity(component_keys.len());
        for values in component_keys {
            // Defensive guard: `detect_route` now guarantees every routed key binds
            // EVERY partition-key component (a no-/partial-binding filter routes
            // Scan, roborev job 1616), so this length mismatch should be
            // unreachable. Keep it as a fail-safe — a wrong-arity key would be a
            // routing logic bug, and dropping to the scan path is always correct.
            if values.len() != pk_names.len() {
                debug_assert_eq!(
                    values.len(),
                    pk_names.len(),
                    "detect_route must only yield full-arity partition keys"
                );
                return None;
            }
            let columns: Vec<(String, Value)> = pk_names
                .iter()
                .zip(values.into_iter())
                .map(|(name, v)| ((*name).to_string(), v))
                .collect();
            let decorated = match PartitionKey::new(columns).to_decorated_key(&self.schema) {
                Ok(d) => d,
                // A predicate that cannot serialize to key bytes is not a route we
                // can honour safely — fall back to the scan path.
                Err(_) => return None,
            };
            // Token-range exclusion BEFORE any seek: drop keys outside the split.
            if let Some(token) = &self.spec.token {
                if !token.contains(decorated.token) {
                    continue;
                }
            }
            out.push((decorated.key, decorated.token));
        }
        Some(PointReadPlan {
            access_path,
            keys: out,
        })
    }

    /// Drive the partition point-read path (issue #2207): build a k-way merger over
    /// ONLY the target partition(s) across the candidate SSTables (seek where the
    /// index resolves, scan-fallback where it does not, prune on a definite bloom
    /// negative), then reconcile + stream through the SAME
    /// [`MergeProducer::drive_merge`] loop the scan path uses — reporting the
    /// `access_path` label the router already fixed (route variant, not the
    /// surviving-key count; roborev job 1623).
    pub(crate) fn produce_point(
        &self,
        plan: PointReadPlan,
        paths: Vec<PathBuf>,
        cancel: &CancelFlag,
        sink: &mut dyn BatchSink,
        progress: &ScanProgress,
        on_merger_built: impl FnOnce(),
    ) -> Result<(), ProducerError> {
        let PointReadPlan { access_path, keys } = plan;
        // Every key was token-excluded → nothing to read. Still fire the phase
        // boundary so the caller's `merge_setup → stream` accounting stays honest.
        let key_bytes: Vec<Vec<u8>> = keys.into_iter().map(|(k, _)| k).collect();
        if key_bytes.is_empty() {
            on_merger_built();
            return Ok(());
        }

        // Map a cooperative cancellation to the distinct `Cancelled` variant, never
        // masking a real I/O/corruption error as a clean cancel (issue #2264,
        // mirroring `drive_merge`).
        let built =
            build_single_partition_merger(paths, &key_bytes, &self.schema, cancel.scan_cancel())
                .map_err(|e| match e {
                    cqlite_core::Error::Cancelled => ProducerError::Cancelled,
                    other => ProducerError::Merge(other),
                })?;

        on_merger_built();
        // Label = router's decision (route variant), NOT surviving key count —
        // single-element IN is still a multi-partition ROUTE (roborev job 1623).
        // `access_path` is fixed by `detect_route`'s variant in `point_read_keys`
        // BEFORE any dedup/token pruning, so a `MultiPartitionPointRead` that prunes
        // to one surviving key still reports `multi_partition_lookup` (never
        // flip-flopping to `streaming_partition_lookup` on the survivor count). This
        // is the field-run evidence that pushdown engaged on the routed shape.
        let label = access_path.label();
        match built {
            // No candidate SSTable holds any target key → zero rows examined, so
            // nothing to stream (and no rows-scanned emission either — no work).
            None => Ok(()),
            Some(mut merger) => self.drive_merge(&mut merger, cancel, sink, progress, label),
        }
    }
}
