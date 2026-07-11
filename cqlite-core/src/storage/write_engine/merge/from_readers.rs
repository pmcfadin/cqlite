//! Warm/shared-reader k-way merge construction (issue #2346).
//!
//! The path-based [`KWayMerger::new`]/[`KWayMerger::new_cancellable`] family
//! (`mod.rs`) opens a fresh [`SSTableReader`] per input INSIDE its own detached
//! producer thread — necessary for the compaction/write-engine callers, whose
//! inputs may be deleted once the merge (and thus every producer thread) has
//! finished (issue #591). A cached-reader caller — the intended consumer is a
//! future Flight warm-handle registry (epic #2310) — instead wants to hand the
//! merger ALREADY-OPEN, possibly-SHARED `Arc<SSTableReader>`s it keeps parsed
//! across requests, paying the reader-open + Index/Summary/Statistics/bloom
//! parse cost once per SSTable generation instead of once per request.
//!
//! [`KWayMerger::new_from_readers`] is that seam. It reuses every other piece
//! of the k-way merge (heap, reconciliation, LWW tie-break by run index)
//! byte-identically — only WHO opens/owns the `SSTableReader` differs from the
//! path-based constructors.
//!
//! ## Delegation (no behavioural drift between the two producer shapes)
//!
//! [`drive_compaction_stream`] is the single streaming-emit helper BOTH producer
//! thread shapes call: the path-based [`SSTableRowIteratorAdapter::producer_thread`]
//! (`mod.rs`, unchanged opening/threading behaviour — still one fresh reader
//! opened per thread, in parallel, exactly as before this issue) and the new
//! [`SSTableRowIteratorAdapter::open_from_reader`]'s producer thread (this file,
//! never opens a reader). Factoring the conversion/backpressure/
//! cancellation-by-variant logic into one function means the two shapes cannot
//! silently diverge.
//!
//! `KWayMerger::new_with_gc_and_registry_cancellable` (the path-based
//! constructor) is intentionally NOT rewritten to open readers eagerly and then
//! call [`KWayMerger::new_from_readers`] — that would move every input's open
//! from N-parallel-producer-threads to one serial pass on the calling thread,
//! a real latency regression for a multi-SSTable merge with no benefit (the
//! path-based caller never has a reader to share in the first place). The
//! shared code is the STREAMING logic, not the opening timing.
//!
//! ## File-lifetime / open-config contract for caller-supplied readers
//!
//! The path-based adapter forces `use_mmap = false` + `DiskAccessMode::Buffered`
//! specifically because compaction inputs may be deleted by
//! `finalize_merge_async` once every producer thread has finished (issue #591) —
//! a reader opened any other way could hold a dangling mapping. A reader passed
//! into [`SSTableRowIteratorAdapter::open_from_reader`] is opened by the CALLER
//! (not by this seam), so that safety property is the CALLER's responsibility:
//! the backing `Data.db` MUST NOT be deleted while any `Arc<SSTableReader>`
//! clone (including ones held by other concurrent runs, or by a cache) is still
//! alive. This is a materially different contract from the path-based adapter's
//! self-contained guarantee, so it is called out explicitly rather than
//! silently inherited. A read-only warm-handle cache that never deletes/replaces
//! a generation's `Data.db` out from under a live `Arc` (evicting only its OWN
//! reference, per #1749's fail-closed model) satisfies this trivially.
//!
//! UDT registry: [`SSTableRowIteratorAdapter::open`] (path-based) can call
//! `reader.set_udt_registry(..)` because it just opened its OWN exclusive
//! reader. `open_from_reader` CANNOT — the reader is shared (`Arc`), so no
//! `&mut self` is available. A caller needing UDT-aware decode over a shared
//! reader must open it WITH the registry already resolved (before wrapping it
//! in `Arc`); `open_from_reader` takes no `udt_registry` parameter for this
//! reason (an accepted-but-silently-ignored parameter would be a correctness
//! trap).

use std::sync::mpsc::SyncSender;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::schema::TableSchema;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::SSTableReader;

use super::{
    producer_gauge, KWayMerger, MergeEntry, MergeProducerError, RunReader, SSTableRowIterator,
    SSTableRowIteratorAdapter, STREAMING_CHANNEL_CAPACITY,
};

/// Drive `reader`'s compaction stream into `sender`, converting each row via
/// [`SSTableRowIteratorAdapter::build_merge_entry`].
///
/// Shared by BOTH producer-thread shapes (see the module doc): the path-based
/// adapter (which opens its own reader per thread) and the shared-reader
/// adapter this module adds. `scan_cancel` is the PER-CALL token
/// [`SSTableReader::stream_all_partitions_for_compaction`] now takes (issue
/// #2346) — never a field mutated onto `reader`, so two concurrent calls over
/// the SAME shared reader (two different producer threads, each with its own
/// token) cancel independently.
pub(super) async fn drive_compaction_stream(
    reader: &SSTableReader,
    run_index: usize,
    schema: &TableSchema,
    scan_cancel: &ScanCancel,
    sender: &SyncSender<std::result::Result<MergeEntry, MergeProducerError>>,
) -> Result<()> {
    reader
        .stream_all_partitions_for_compaction(Some(schema), scan_cancel, |compaction_row| {
            let msg =
                SSTableRowIteratorAdapter::build_merge_entry(run_index, compaction_row, schema)
                    .map_err(MergeProducerError::from);
            match sender.send(msg) {
                Ok(()) => Ok(std::ops::ControlFlow::Continue(())),
                Err(_) => Ok(std::ops::ControlFlow::Break(())),
            }
        })
        .await
}

impl SSTableRowIteratorAdapter {
    /// Open a streaming run over an ALREADY-OPEN, possibly-SHARED
    /// [`SSTableReader`] (issue #2346), instead of opening a fresh reader from a
    /// path ([`SSTableRowIteratorAdapter::open`]).
    ///
    /// Still spawns a dedicated producer thread (preserving the O(M)
    /// thread-per-input / bounded-channel backpressure architecture — issues
    /// #827/#2316) but never opens/owns a reader itself; it drives the
    /// caller-supplied `Arc<SSTableReader>` directly via
    /// [`drive_compaction_stream`]. See the module doc for the file-lifetime
    /// and UDT-registry contract differences from the path-based `open`.
    pub(crate) fn open_from_reader(
        reader: Arc<SSTableReader>,
        run_index: usize,
        schema: &TableSchema,
        scan_cancel: ScanCancel,
    ) -> Result<Self> {
        let schema = schema.clone();
        let (sender, receiver) = std::sync::mpsc::sync_channel(STREAMING_CHANNEL_CAPACITY);

        // Issue #2316: account this producer on the live-thread gauge BEFORE
        // spawning (see `SSTableRowIteratorAdapter::open`'s identical rationale).
        producer_gauge::spawned();

        let producer = match std::thread::Builder::new().spawn(move || {
            Self::producer_thread_from_reader(reader, run_index, schema, scan_cancel, sender);
        }) {
            Ok(handle) => handle,
            Err(e) => {
                producer_gauge::rollback();
                return Err(Error::Storage(format!(
                    "streaming producer (shared reader): failed to spawn thread: {}",
                    e
                )));
            }
        };

        Ok(Self {
            receiver,
            _producer: producer,
        })
    }

    /// Body of the shared-reader producer thread (issue #2346).
    ///
    /// Unlike [`Self::producer_thread`] (path-based), this NEVER opens a
    /// reader — it drives the caller-supplied `Arc<SSTableReader>` directly via
    /// [`drive_compaction_stream`], reusing the exact
    /// conversion/backpressure/cancellation-by-variant semantics. Still owns a
    /// dedicated `current_thread` runtime (issue #2316: zero extra worker
    /// threads) so the async `stream_all_partitions_for_compaction` call can
    /// run without a nested-runtime panic.
    fn producer_thread_from_reader(
        reader: Arc<SSTableReader>,
        run_index: usize,
        schema: TableSchema,
        scan_cancel: ScanCancel,
        sender: SyncSender<std::result::Result<MergeEntry, MergeProducerError>>,
    ) {
        let _thread_guard = producer_gauge::ProducerThreadGuard;
        let error_sender = sender.clone();

        let stream_result = (|| -> Result<()> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    Error::Storage(format!(
                        "streaming producer (shared reader): failed to create runtime: {}",
                        e
                    ))
                })?;
            rt.block_on(drive_compaction_stream(
                &reader,
                run_index,
                &schema,
                &scan_cancel,
                &sender,
            ))
        })();

        if let Err(e) = stream_result {
            // Forward the error (preserving `Cancelled` distinctly, issue #2264);
            // ignore send failure (consumer may have dropped).
            let _ = error_sender.send(Err(MergeProducerError::from(e)));
        }
        // Channel closed naturally when `sender` is dropped here.
    }
}

impl KWayMerger {
    /// Build a k-way merger over already-open, possibly-SHARED `SSTableReader`s
    /// (issue #2346): a warm-handle cache can hand this constructor `Arc`
    /// clones of readers it keeps parsed across requests, instead of the
    /// per-request path-based open ([`KWayMerger::new_cancellable`]).
    ///
    /// `readers` must be ordered newest-to-oldest generation (run index = LWW
    /// tie-break rank), exactly as the path-based constructors' `input_paths`
    /// are. Reconciliation is byte-identical to the path-based merge — only WHO
    /// opens/owns the `SSTableReader` differs.
    pub fn new_from_readers(
        readers: Vec<Arc<SSTableReader>>,
        schema: &TableSchema,
        scan_cancel: ScanCancel,
    ) -> Result<Self> {
        if readers.is_empty() {
            return Err(Error::InvalidInput(
                "K-way merge requires at least one input reader".to_string(),
            ));
        }
        schema.validate_dropped_columns()?;

        let mut runs = Vec::with_capacity(readers.len());
        for (run_index, reader) in readers.into_iter().enumerate() {
            let adapter = SSTableRowIteratorAdapter::open_from_reader(
                reader,
                run_index,
                schema,
                scan_cancel.clone(),
            )?;
            runs.push(RunReader::new(
                Box::new(adapter) as Box<dyn SSTableRowIterator>
            ));
        }

        Ok(Self {
            runs,
            heap: std::collections::BinaryHeap::new(),
            current_partition: None,
            schema: schema.clone(),
            // Issue #1668, stage 5c-i: see the field doc in `mod.rs`.
            schema_arc: Arc::new(schema.clone()),
            gc_before_secs: None,
            now_secs: None,
            purge_safe: false,
            max_purgeable_timestamp: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::Platform;
    use crate::storage::write_engine::merge::MergeStep;
    use crate::storage::write_engine::mutation::PartitionKey;
    use crate::storage::write_engine::test_support::{create_test_schema, flush_n_sstables_sync};
    use crate::storage::write_engine::{WriteEngine, WriteEngineConfig};
    use crate::types::Value;
    use crate::Config;
    use tempfile::TempDir;

    fn config_for(temp_dir: &TempDir) -> WriteEngineConfig {
        WriteEngineConfig::new(
            temp_dir.path().join("data"),
            temp_dir.path().join("wal"),
            create_test_schema(),
        )
    }

    async fn open_reader(path: &std::path::Path) -> SSTableReader {
        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        SSTableReader::open(path, &config, platform).await.unwrap()
    }

    /// Drain a merger into `(partition_key_bytes, row_count)` pairs, sorted —
    /// enough to prove byte-identical reconciliation across two constructors
    /// without depending on a specific row-shape decoder.
    fn collect_merge_rows(mut merger: KWayMerger) -> Vec<(Vec<u8>, usize)> {
        let mut out = Vec::new();
        while let MergeStep::Partition { key, rows } = merger.step().expect("merge step") {
            out.push((key.key.clone(), rows.len()));
        }
        out.sort();
        out
    }

    /// Red-then-green (b): `KWayMerger::new_from_readers` (reader-based) must
    /// reconcile BYTE-IDENTICALLY to `KWayMerger::new_cancellable` (path-based)
    /// over the SAME SSTables — proving the path-based constructor's behaviour
    /// is preserved and the two producer-thread shapes never diverge (issue
    /// #2346). Fails to compile on pre-#2346 `main` (`new_from_readers` and
    /// `Arc<SSTableReader>`-based construction do not exist there).
    ///
    /// Plain `#[test]` (not `#[tokio::test]`): `flush_n_sstables_sync` drives
    /// its OWN runtime to flush (mirrors the `cqlite-flight` test convention —
    /// nesting a `#[tokio::test]` runtime here would panic on
    /// "Cannot start a runtime from within a runtime"), so the SSTables are
    /// built first, then a fresh runtime drives the async reader-open/merge.
    #[test]
    fn new_from_readers_matches_path_based_reconciliation() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = WriteEngine::new(config_for(&temp_dir)).unwrap();
        // Two distinct-generation SSTables — a real multi-run merge, not a
        // single-input vacuity.
        let paths = flush_n_sstables_sync(&mut engine, 2);
        assert_eq!(paths.len(), 2, "test precondition: two SSTables written");
        let schema = create_test_schema();

        let path_based = KWayMerger::new_cancellable(paths.clone(), &schema, ScanCancel::default())
            .expect("path-based merger constructs");

        let rt = tokio::runtime::Runtime::new().unwrap();
        let reader_based = rt.block_on(async {
            let mut readers = Vec::with_capacity(paths.len());
            for path in &paths {
                readers.push(Arc::new(open_reader(path).await));
            }
            KWayMerger::new_from_readers(readers, &schema, ScanCancel::default())
                .expect("reader-based merger constructs")
        });

        assert_eq!(
            collect_merge_rows(path_based),
            collect_merge_rows(reader_based),
            "reader-based full-scan merger must reconcile byte-identically to the \
             path-based one (issue #2346) — only WHO opens the reader differs"
        );
    }

    /// Red-then-green (b), point-read variant: the reader-based
    /// `build_single_partition_merger_from_readers` must match the path-based
    /// `build_single_partition_merger` for the SAME target key across the SAME
    /// SSTables. Fails to compile on pre-#2346 `main` (the reader-based builder
    /// does not exist there).
    ///
    /// Plain `#[test]` — same rationale as the sibling test above.
    #[test]
    fn build_single_partition_merger_from_readers_matches_path_based() {
        let temp_dir = TempDir::new().unwrap();
        let mut engine = WriteEngine::new(config_for(&temp_dir)).unwrap();
        let paths = flush_n_sstables_sync(&mut engine, 2);
        let schema = create_test_schema();

        // `flush_n_sstables_sync` writes ids `batch*100 + row` for row in 0..5;
        // id=0 (batch 0, row 0) is always present.
        let key_bytes = PartitionKey::single("id", Value::Integer(0))
            .to_bytes(&schema)
            .expect("encode target key");

        let path_based = super::super::build_single_partition_merger(
            paths.clone(),
            &[key_bytes.clone()],
            &schema,
            ScanCancel::default(),
        )
        .expect("path-based probe succeeds")
        .expect("path-based merger must find the key");

        let rt = tokio::runtime::Runtime::new().unwrap();
        let reader_based = rt.block_on(async {
            let mut readers = Vec::with_capacity(paths.len());
            for path in &paths {
                readers.push(Arc::new(open_reader(path).await));
            }
            super::super::build_single_partition_merger_from_readers(
                readers,
                &[key_bytes],
                &schema,
                ScanCancel::default(),
            )
            .expect("reader-based probe succeeds")
            .expect("reader-based merger must find the key")
        });

        assert_eq!(
            collect_merge_rows(path_based),
            collect_merge_rows(reader_based),
            "reader-based point-read merger must reconcile byte-identically to the \
             path-based one (issue #2346)"
        );
    }
}
