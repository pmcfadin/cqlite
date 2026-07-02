//! Unit + dataset-dependent guards for the windowed streaming scan
//! ([`super`] = `scan_stream_windowed`). Split out of the parent module to
//! keep the source file under the campsite-rule size limit (issue #1143).
//!
//! Included via `#[cfg(test)] #[path = "scan_stream_windowed_tests.rs"] mod tests;`
//! in the parent, so `use super::*` resolves to the windowed-scan module's
//! private items (which these guards drive directly).

use super::*;
// `ScanRow` comes via `super::*`; `Value` is needed for the marker payloads in
// the batch fixtures below (issue #1334).
use crate::types::Value;

/// Issue #1143 finding 2 — pure decision guard. The blocking parse half runs
/// its terminal (`at_final_chunk = true`) drain IFF the I/O half did NOT fail
/// mid-stream. This pins the truth table the call site in
/// [`SSTableReader::drain_scan_window_blocking`] depends on; a refactor that
/// drops the flag, flips the load, or inverts the condition flips one of these
/// assertions and fails the build.
#[test]
fn terminal_drain_skipped_iff_io_failed() {
    // Clean EOF (io_failed == false): run the terminal drain so a trailing
    // partition with no END_OF_PARTITION marker is emitted (Done, not a
    // refill request that will never come).
    assert!(
        should_run_terminal_drain(false),
        "clean EOF must run the terminal drain (else the last partition is dropped)"
    );
    // Mid-stream read error (io_failed == true): SKIP the terminal drain so
    // the truncated trailing window cannot surface a spurious/garbage
    // partition before the caller returns the I/O Err.
    assert!(
        !should_run_terminal_drain(true),
        "Issue #1143: a mid-stream I/O failure MUST skip the terminal drain so a \
             truncated window emits no spurious trailing partition"
    );
}

/// Issue #1143 finding 1 (roborev) — the batching in-flight bound is a single
/// documented CONSTANT, derived purely from the batch sizing knobs, and is
/// independent of the caller's `buffer_size`. This pins the algebra so a future
/// tweak to `BATCH_EMIT_ROWS` / `BATCH_CHANNEL_CAP` that changes the worst case
/// must also update the documented [`MAX_INFLIGHT_BATCH_ROWS`] constant (and the
/// `scan_stream` doc that quotes it), rather than silently widening the window in
/// which parsing can run ahead of a stalled consumer.
#[test]
fn max_inflight_batch_rows_matches_sizing_knobs() {
    // Against a stalled consumer THREE full batches coexist: the channel
    // (BATCH_CHANNEL_CAP items) + the one batch the forwarder has recv()'d and is
    // flattening + the one batch the producer is parked-in-blocking_send holding
    // = (CAP + 2) batches.
    assert_eq!(
        MAX_INFLIGHT_BATCH_ROWS,
        (BATCH_CHANNEL_CAP + 2) * BATCH_EMIT_ROWS,
        "MAX_INFLIGHT_BATCH_ROWS must equal (BATCH_CHANNEL_CAP + 2) * BATCH_EMIT_ROWS; \
         update the constant AND the scan_stream doc if the sizing knobs change"
    );
    // The bound is a constant, not a function of buffer_size: nothing in its
    // definition references the caller's channel size. (Compile-time fact; the
    // explicit assertion documents the intent for readers.)
    assert!(
        MAX_INFLIGHT_BATCH_ROWS > 0 && BATCH_EMIT_ROWS > 0,
        "the batching bound must be a positive constant"
    );
}

/// Issue #1143 finding 2 (roborev) — NON-VACUOUS error-flush guard. The previous
/// end-to-end test appended a corrupt chunk AFTER full clean chunks, but the
/// in-stream per-chunk-boundary flush already delivered those rows at the chunk
/// boundary BEFORE the corrupt chunk errored — so it passed even with the
/// error-path `flush_pending` deleted. This drives the extracted finish seam
/// [`finish_blocking_drain`] DIRECTLY with a genuinely NON-EMPTY pending `batch`
/// and an `Err`, the precise state the per-chunk flush cannot reach: confirmed
/// rows still buffered when the error fires.
///
/// PROOF it is non-vacuous: delete the `flush_pending(batch, tx)` call in the
/// `Err` arm of `finish_blocking_drain` and this test FAILS (the receiver gets
/// zero rows, so `received == 0 != 2`). With the flush it PASSES (both pending
/// rows arrive ahead of the terminal `Err`). Verified locally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finish_blocking_drain_flushes_pending_before_error() {
    let (tx, mut rx) = mpsc::channel::<Result<Vec<(RowKey, ScanRow)>>>(8);

    // Two confirmed rows sitting in the pending batch (fewer than BATCH_EMIT_ROWS,
    // so they were NOT yet flushed as a full batch) when a mid-stream error fires.
    let mut batch: Vec<(RowKey, ScanRow)> = vec![
        (
            RowKey::from(b"k1".to_vec()),
            ScanRow::Marker(Value::Text("v1".into())),
        ),
        (
            RowKey::from(b"k2".to_vec()),
            ScanRow::Marker(Value::Text("v2".into())),
        ),
    ];
    let drained: Result<()> = Err(Error::corruption("mid-stream decompress failure"));

    // Run the seam on a blocking thread (it uses blocking_send) and capture both
    // the propagated error and the rows delivered ahead of it.
    let handle = tokio::task::spawn_blocking(move || {
        let r = finish_blocking_drain(drained, &mut batch, /* broke */ false, &tx);
        // `batch` must have been drained by the flush, not left holding rows.
        (r, batch)
    });
    let (result, leftover_batch) = handle.await.expect("finish task");

    let mut received = 0usize;
    let mut got_err = false;
    while let Some(item) = rx.recv().await {
        match item {
            Ok(rows) => received += rows.len(),
            Err(_) => got_err = true,
        }
    }

    assert!(
        result.is_err(),
        "finish_blocking_drain must propagate the mid-stream Err"
    );
    assert!(
        !got_err,
        "finish_blocking_drain forwards ONLY the pending rows; the terminal Err is \
         surfaced via its return value (the forwarder/caller emits it), not the channel"
    );
    assert_eq!(
        received, 2,
        "Issue #1143 REGRESSION: the {} pending rows confirmed before the mid-stream \
         error were DROPPED. The error path must flush_pending(batch) BEFORE \
         propagating the Err. (Deleting that flush makes this 0.)",
        2
    );
    assert!(
        leftover_batch.is_empty(),
        "the pending batch must be drained by the error flush, not left buffered"
    );
}

/// Issue #1143 finding 2 companion — the success path flushes the trailing partial
/// batch when the consumer is still attached, and drops it when the consumer has
/// gone (`broke`). Pins the other two arms of [`finish_blocking_drain`].
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finish_blocking_drain_flushes_trailing_batch_on_success() {
    // Consumer attached, clean finish: the trailing partial batch is delivered.
    let (tx, mut rx) = mpsc::channel::<Result<Vec<(RowKey, ScanRow)>>>(8);
    let mut batch: Vec<(RowKey, ScanRow)> = vec![(
        RowKey::from(b"k".to_vec()),
        ScanRow::Marker(Value::Text("v".into())),
    )];
    let handle =
        tokio::task::spawn_blocking(move || finish_blocking_drain(Ok(()), &mut batch, false, &tx));
    handle.await.expect("task").expect("clean finish");
    let mut received = 0usize;
    while let Some(Ok(rows)) = rx.recv().await {
        received += rows.len();
    }
    assert_eq!(
        received, 1,
        "clean finish must flush the trailing partial batch"
    );

    // Consumer dropped (`broke`): nothing is flushed.
    let (tx2, mut rx2) = mpsc::channel::<Result<Vec<(RowKey, ScanRow)>>>(8);
    let mut batch2: Vec<(RowKey, ScanRow)> = vec![(
        RowKey::from(b"k".to_vec()),
        ScanRow::Marker(Value::Text("v".into())),
    )];
    let handle2 =
        tokio::task::spawn_blocking(move || finish_blocking_drain(Ok(()), &mut batch2, true, &tx2));
    handle2.await.expect("task2").expect("clean finish (broke)");
    let mut received2 = 0usize;
    while let Some(Ok(rows)) = rx2.recv().await {
        received2 += rows.len();
    }
    assert_eq!(
        received2, 0,
        "a finish with broke=true must NOT flush (consumer already dropped)"
    );
}

// Real-behavior guard (issue #1143 finding 2): drive the private blocking
// parse half (`drain_scan_window_blocking`) directly over a genuinely
// multi-chunk fixture, with the SAME truncated input, toggling ONLY
// `io_failed`. With `io_failed = false` the terminal drain parses the
// truncated trailing window and emits an extra (final) partition; with
// `io_failed = true` that terminal drain is skipped, so it emits strictly
// fewer rows. This proves the skip is the `io_failed` gate's doing, not a
// side effect — and it fails if the gate is removed (both runs would then
// emit the same trailing partition).
//
// Dataset-dependent: skips when the fixture's Data.db is absent (matches the
// other #1143 integration guards). The pure `terminal_drain_skipped_iff_io_failed`
// test above runs dataset-independently in every gate, so the gate is never
// vacuous even without fetched data.
mod fixture_drain {
    use super::*;
    use crate::storage::sstable::SSTableReader;
    use std::path::PathBuf;
    use tokio::io::AsyncSeekExt;

    const KEYSPACE: &str = "test_wide_rows";
    const TABLE: &str = "wide_partition_table";

    fn datasets_root() -> Option<PathBuf> {
        std::env::var("CQLITE_DATASETS_ROOT")
            .ok()
            .map(PathBuf::from)
            .filter(|p| p.exists())
    }

    fn fixture_data_db() -> Option<PathBuf> {
        fixture_data_db_for(KEYSPACE, TABLE)
    }

    fn fixture_data_db_for(keyspace: &str, table: &str) -> Option<PathBuf> {
        let table_root = datasets_root()?.join("sstables").join(keyspace);
        for entry in std::fs::read_dir(&table_root).ok()?.flatten() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with(&format!("{table}-")) && entry.path().is_dir() {
                for f in std::fs::read_dir(entry.path()).ok()?.flatten() {
                    if f.file_name()
                        .to_str()
                        .is_some_and(|n| n.ends_with("-Data.db"))
                    {
                        return Some(f.path());
                    }
                }
            }
        }
        None
    }

    // A deliberately WIDE compressed fixture (999 rows) so the parse half confirms
    // strictly more rows than the batch channel can hold, forcing the producer to
    // PARK on `blocking_send` and exercising the in-flight bound (issue #1143
    // finding 1). `wide_partition_table` only confirms ~100 rows, below the
    // 512-row channel capacity, so it cannot force the park.
    const WIDE_KEYSPACE: &str = "test_basic";
    const WIDE_TABLE: &str = "simple_table";

    /// Collect every raw compressed chunk of the data section, in order, the
    /// way `run_scan_stream_windowed`'s I/O half would feed them.
    async fn collect_raw_chunks(reader: &SSTableReader) -> Vec<Vec<u8>> {
        let cursor = reader.new_scan_cursor().await.expect("scan cursor");
        let header_size = reader.calculate_header_size();
        {
            let mut g = cursor.file.lock().await;
            g.seek(std::io::SeekFrom::Start(header_size as u64))
                .await
                .expect("seek to data section");
        }
        let mut chunks = Vec::new();
        while let Some(c) = reader.read_next_block(&cursor).await.expect("read chunk") {
            chunks.push(c);
        }
        chunks
    }

    /// Run the blocking parse half over `chunks` with the given `io_failed`,
    /// returning the number of `(RowKey, Value)` entries it emitted. Runs on
    /// the current thread (the function is synchronous); the bounded channel
    /// is pre-filled and its sender dropped so `blocking_recv` never blocks.
    fn drain_count(reader: &SSTableReader, chunks: &[Vec<u8>], io_failed: bool) -> usize {
        let ctx = WindowParseCtx {
            table_id: TableId::new(format!(
                "{}.{}",
                reader.header.keyspace, reader.header.table_name
            )),
            start_key: None,
            end_key: None,
            schema: reader.get_table_schema(None),
            max_compressed_length: reader
                .compression_info
                .as_ref()
                .map(|ci| ci.max_compressed_length as usize)
                .unwrap_or(usize::MAX),
        };
        // Feed raw chunks through an unbounded->bounded-shaped channel large
        // enough to hold them all, then drop the sender so the parse half sees
        // a CLEAN close; the `io_failed` flag (not the close reason) drives the
        // terminal-drain decision under test.
        let (raw_tx, raw_rx) = mpsc::channel::<Vec<u8>>(chunks.len().max(1));
        for c in chunks {
            raw_tx.try_send(c.clone()).expect("prefill raw chunk");
        }
        drop(raw_tx);
        // Output channel now carries batched rows (issue #1143). Big enough
        // that `blocking_send` never blocks here; count rows ACROSS batches.
        let (out_tx, mut out_rx) = mpsc::channel::<Result<Vec<(RowKey, ScanRow)>>>(4096);
        let flag = Arc::new(AtomicBool::new(io_failed));
        reader
            .drain_scan_window_blocking(ctx, raw_rx, out_tx, flag)
            .expect("drain_scan_window_blocking");
        let mut n = 0usize;
        while let Ok(item) = out_rx.try_recv() {
            if let Ok(rows) = item {
                n += rows.len();
            }
        }
        n
    }

    /// Build the same `WindowParseCtx` the I/O half resolves for this fixture.
    fn ctx_for(reader: &SSTableReader) -> WindowParseCtx {
        WindowParseCtx {
            table_id: TableId::new(format!(
                "{}.{}",
                reader.header.keyspace, reader.header.table_name
            )),
            start_key: None,
            end_key: None,
            schema: reader.get_table_schema(None),
            max_compressed_length: reader
                .compression_info
                .as_ref()
                .map(|ci| ci.max_compressed_length as usize)
                .unwrap_or(usize::MAX),
        }
    }

    /// Drain `chunks` and return the number of `(RowKey, Value)` rows confirmed
    /// WITHOUT the terminal (`at_final_chunk = true`) drain — i.e. only the rows
    /// the parse half would have flushed by the time MORE input is required
    /// (NeedMore at a chunk boundary). Implemented by reusing `drain_count` with
    /// `io_failed = true`, which is the documented "skip terminal drain" gate
    /// (`should_run_terminal_drain(true) == false`).
    ///
    /// Why not the CLEAN (`io_failed = false`) count: a clean drain of a prefix
    /// runs the terminal drain and so counts the prefix's trailing partition,
    /// which in `drain_with_trailing_corrupt` is NOT confirmed before the corrupt
    /// chunk arrives (that partition is held at NeedMore awaiting bytes that turn
    /// out to be the corrupt chunk). Using the clean count as `expected_pending`
    /// therefore over-counts for some fixtures and makes the assertion flaky
    /// (roborev finding, issue #1143). The skip-terminal count matches exactly the
    /// rows the real flow confirms before the error.
    fn pending_before_more_input(reader: &SSTableReader, chunks: &[Vec<u8>]) -> usize {
        drain_count(reader, chunks, true)
    }

    /// Run the blocking parse half over `chunks` followed by ONE deliberately
    /// corrupt compressed chunk that fails to decompress mid-stream, returning
    /// `(result, rows_received_before_error)`. The corrupt chunk is short enough
    /// (`< max_compressed_length`) that the parse half routes it through
    /// `Compression::decompress`, which errors — exercising the mid-stream
    /// decompress-error path AFTER `chunks` already produced confirmed rows.
    fn drain_with_trailing_corrupt(
        reader: &SSTableReader,
        chunks: &[Vec<u8>],
    ) -> (Result<()>, usize) {
        let ctx = ctx_for(reader);
        // Capacity for the real chunks + the corrupt one.
        let (raw_tx, raw_rx) = mpsc::channel::<Vec<u8>>(chunks.len() + 1);
        for c in chunks {
            raw_tx.try_send(c.clone()).expect("prefill raw chunk");
        }
        // A short, non-raw, non-decompressible chunk: 8 bytes that no supported
        // codec accepts. Strictly shorter than any real `max_compressed_length`,
        // so it is NOT treated as an incompressible-raw chunk and goes through
        // `decompress`, which returns Err.
        raw_tx
            .try_send(vec![0xFFu8; 8])
            .expect("prefill corrupt chunk");
        drop(raw_tx);

        // Large output channel so `blocking_send` never blocks; we count rows
        // delivered ACROSS batches BEFORE the terminal error.
        let (out_tx, mut out_rx) = mpsc::channel::<Result<Vec<(RowKey, ScanRow)>>>(4096);
        let flag = Arc::new(AtomicBool::new(false));
        let result = reader.drain_scan_window_blocking(ctx, raw_rx, out_tx, flag);

        let mut received = 0usize;
        while let Ok(item) = out_rx.try_recv() {
            if let Ok(rows) = item {
                received += rows.len();
            }
        }
        (result, received)
    }

    /// Issue #1143 — END-TO-END delivery + error-surfacing guard on the real
    /// `drain_scan_window_blocking` flow: a real-chunk prefix followed by a corrupt
    /// trailing chunk must deliver every confirmed row AND return `Err`.
    ///
    /// NOTE (roborev finding, issue #1143): this is NOT the error-FLUSH guard. With
    /// a corrupt chunk appended AFTER clean chunks, the in-stream per-chunk-boundary
    /// flush already delivers the prefix's confirmed rows at the chunk boundary
    /// BEFORE the corrupt chunk errors — so this test still passes even if the
    /// error-path `flush_pending` is deleted, i.e. it does NOT guard that path.
    /// The non-vacuous error-flush guard is the dataset-independent unit test
    /// [`super::finish_blocking_drain_flushes_pending_before_error`], which drives
    /// the finish seam with a genuinely non-empty pending batch + an `Err` and
    /// FAILS if the flush is removed. This end-to-end test is retained as a
    /// real-data smoke that the corrupt-chunk path delivers rows and surfaces the
    /// error together.
    ///
    /// Dataset-dependent: skips when the fixture is absent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mid_stream_error_still_delivers_confirmed_pending_rows() {
        let Some(data_db) = fixture_data_db() else {
            eprintln!("Skipping {KEYSPACE}.{TABLE}: no Data.db present (run fetch-datasets.sh).");
            return;
        };

        let cfg = crate::Config::default();
        let platform = Arc::new(
            crate::platform::Platform::new(&cfg)
                .await
                .expect("platform"),
        );
        let reader = SSTableReader::open(&data_db, &cfg, platform)
            .await
            .expect("open reader");
        assert!(
            reader.compression_info.is_some(),
            "fixture must be compressed so the corrupt chunk hits the decompress-error path"
        );

        let chunks = collect_raw_chunks(&reader).await;
        assert!(
            chunks.len() > 1,
            "Issue #1143: need a multi-chunk fixture ({} chunk(s))",
            chunks.len()
        );

        // Choose the smallest real-chunk prefix that confirms at least one row
        // but fewer than BATCH_EMIT_ROWS BEFORE more input is required, so those
        // rows sit in the pending batch (not an already-flushed full batch) when
        // the corrupt chunk errors. We count with the SKIP-terminal-drain path
        // (`pending_before_more_input`), not a clean terminal drain: in the
        // corrupt-trailing flow the prefix's last partition is held at NeedMore
        // awaiting the next chunk (which turns out corrupt), so it is NOT
        // confirmed; counting it via a clean terminal drain would over-count and
        // make the assertion flaky (roborev finding, issue #1143). This makes the
        // assertion specifically about the flush-on-error of the *pending* batch
        // (the regression), not earlier full batches or the unconfirmed tail.
        let reader = Arc::new(reader);
        let mut chosen: Option<(Vec<Vec<u8>>, usize)> = None;
        for n in 1..chunks.len() {
            let prefix = chunks[..n].to_vec();
            let r = Arc::clone(&reader);
            let p = prefix.clone();
            let cnt = tokio::task::spawn_blocking(move || pending_before_more_input(&r, &p))
                .await
                .expect("pending count task");
            if (1..BATCH_EMIT_ROWS).contains(&cnt) {
                chosen = Some((prefix, cnt));
                break;
            }
        }
        let Some((prefix, expected_pending)) = chosen else {
            eprintln!(
                "Skipping: no chunk prefix of {KEYSPACE}.{TABLE} yields 1..{BATCH_EMIT_ROWS} \
                 confirmed rows; cannot stage a partial pending batch for this fixture."
            );
            return;
        };

        let r = Arc::clone(&reader);
        let (result, received) =
            tokio::task::spawn_blocking(move || drain_with_trailing_corrupt(&r, &prefix))
                .await
                .expect("corrupt drain task");

        eprintln!(
            "Issue #1143 flush-on-error guard: expected_pending={expected_pending}, \
             received_before_error={received}, result_is_err={}",
            result.is_err()
        );

        assert!(
            result.is_err(),
            "the trailing corrupt chunk must make the parse half return Err"
        );
        assert_eq!(
            received, expected_pending,
            "Issue #1143 REGRESSION: confirmed rows produced before the mid-stream \
             decompress error were DROPPED. Expected the consumer to receive all \
             {expected_pending} pending rows before the terminal Err, got {received}. \
             The batching change must flush the pending batch before propagating an error."
        );
    }

    /// Issue #1143 finding 1 (roborev) — real-behavior bound check. Drive the
    /// private blocking parse half against a genuinely large multi-chunk fixture
    /// with a STALLED consumer (a batch channel of the production [`BATCH_CHANNEL_CAP`]
    /// that is NEVER drained), and prove parsing cannot run unboundedly ahead: the
    /// producer blocks on `blocking_send` once the channel fills, having emitted at
    /// most `BATCH_CHANNEL_CAP` channel items. Because nothing here drains the
    /// channel, no forwarder batch and no further pending tail can escape, so the
    /// channel-resident worst case is `BATCH_CHANNEL_CAP * BATCH_EMIT_ROWS` rows —
    /// strictly within the documented [`MAX_INFLIGHT_BATCH_ROWS`]. This is the
    /// invariant the documented bound rests on, exercised on the real parse loop
    /// (not a re-derivation): if a future change let the producer keep parsing past
    /// a full channel (e.g. an unbounded channel, or dropping `blocking_send`'s
    /// backpressure), this test would see MORE than `BATCH_CHANNEL_CAP` items or a
    /// completed (non-blocked) drain and fail.
    ///
    /// Scope: this bounds the BATCHING subsystem (channel-resident batches) ONLY —
    /// the name's "independent of `buffer_size`" means the batching pool is NOT
    /// sized by the caller's channel, not that it is the whole pipeline's resident
    /// bound. The full resident-row worst case ALSO includes the inherent
    /// `max_partition_size` term (the one confirmed partition `drain_scan_window`
    /// materializes in `surviving` before batching — pre-existing #1156 behavior);
    /// that term is documented on the constant and NOT asserted here.
    ///
    /// Dataset-dependent: skips when the fixture is absent. The pure
    /// `max_inflight_batch_rows_matches_sizing_knobs` test above runs in every gate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_inflight_rows_are_bounded_independent_of_buffer_size() {
        let Some(data_db) = fixture_data_db_for(WIDE_KEYSPACE, WIDE_TABLE) else {
            eprintln!(
                "Skipping {WIDE_KEYSPACE}.{WIDE_TABLE}: no Data.db present (run fetch-datasets.sh). \
                 The pure max_inflight_batch_rows_matches_sizing_knobs guard still runs."
            );
            return;
        };

        let cfg = crate::Config::default();
        let platform = Arc::new(
            crate::platform::Platform::new(&cfg)
                .await
                .expect("platform"),
        );
        let reader = SSTableReader::open(&data_db, &cfg, platform)
            .await
            .expect("open reader");
        assert!(
            reader.compression_info.is_some(),
            "fixture must be compressed to exercise the windowed chunk-stitching path"
        );

        // The bound check only needs ENOUGH confirmed rows to overflow the batch
        // channel (asserted below via `confirmed_total`); it does NOT require
        // multiple chunks — a single wide chunk drains many full batches into the
        // bounded channel just the same.
        let chunks = collect_raw_chunks(&reader).await;
        assert!(
            !chunks.is_empty(),
            "Issue #1143: fixture produced no raw chunks"
        );

        let reader = Arc::new(reader);

        // First, how many rows does the whole fixture confirm? (skip-terminal path)
        let all_chunks = chunks.clone();
        let r0 = Arc::clone(&reader);
        let confirmed_total =
            tokio::task::spawn_blocking(move || pending_before_more_input(&r0, &all_chunks))
                .await
                .expect("count task");
        let channel_capacity_rows = BATCH_CHANNEL_CAP * BATCH_EMIT_ROWS;
        if confirmed_total <= channel_capacity_rows {
            eprintln!(
                "Skipping bound check: fixture confirms only {confirmed_total} rows \
                 (<= channel capacity {channel_capacity_rows}); cannot force the \
                 producer to block. Need a wider fixture."
            );
            return;
        }

        // Drive the real blocking drain with a batch channel of the PRODUCTION
        // capacity that we NEVER drain, on a dedicated thread. The producer will
        // fill the channel and then park in `blocking_send`. Count how many rows
        // escaped the producer (i.e. are resident in the channel) and assert it
        // never exceeds the channel-resident bound.
        let ctx = ctx_for(&reader);
        let (raw_tx, raw_rx) = mpsc::channel::<Vec<u8>>(chunks.len().max(1));
        for c in &chunks {
            raw_tx.try_send(c.clone()).expect("prefill raw chunk");
        }
        drop(raw_tx);
        // PRODUCTION-sized batch channel, deliberately left undrained.
        let (out_tx, mut out_rx) =
            mpsc::channel::<Result<Vec<(RowKey, ScanRow)>>>(BATCH_CHANNEL_CAP);
        let flag = Arc::new(AtomicBool::new(false));
        let r = Arc::clone(&reader);
        let drain_handle =
            std::thread::spawn(move || r.drain_scan_window_blocking(ctx, raw_rx, out_tx, flag));

        // Give the producer time to fill the channel and PARK on blocking_send.
        // It cannot finish: confirmed_total > channel_capacity_rows and we never
        // drain, so it must block (proving the bound is enforced by backpressure,
        // not by the fixture being small).
        std::thread::sleep(std::time::Duration::from_millis(500));
        assert!(
            !drain_handle.is_finished(),
            "Issue #1143 REGRESSION: the parse half RAN TO COMPLETION against an \
             undrained, bounded batch channel — backpressure is broken and parsing \
             ran arbitrarily far ahead of the (stalled) consumer. Confirmed rows \
             {confirmed_total} exceed channel capacity {channel_capacity_rows}, so a \
             correct producer MUST be parked in blocking_send here."
        );

        // The producer being PARKED (asserted above) while confirmed_total exceeds
        // the channel capacity is the load-bearing proof: with the channel full and
        // never drained, the producer cannot push a (CAP+1)-th item, so at most
        // `BATCH_CHANNEL_CAP` batches are resident ahead of the stalled consumer.
        // Each batch carries at most `BATCH_EMIT_ROWS` rows (the producer flushes the
        // moment a batch REACHES that size), so the resident worst case is
        // `BATCH_CHANNEL_CAP * BATCH_EMIT_ROWS` rows, within MAX_INFLIGHT_BATCH_ROWS.
        //
        // Now drain item-by-item to release the producer, asserting EACH received
        // batch respects the per-batch size cap. (We cannot snapshot the parked
        // channel without freeing slots — every recv lets the producer refill — so
        // the channel-resident COUNT is proven structurally by the park + the
        // per-batch cap, not by a racy drain count.)
        let mut max_batch_rows = 0usize;
        let mut total_received = 0usize;
        while let Some(item) = out_rx.recv().await {
            if let Ok(rows) = item {
                assert!(
                    rows.len() <= BATCH_EMIT_ROWS,
                    "Issue #1143 REGRESSION: a batch carried {} rows, exceeding \
                     BATCH_EMIT_ROWS={BATCH_EMIT_ROWS}; per-batch sizing is unbounded.",
                    rows.len()
                );
                max_batch_rows = max_batch_rows.max(rows.len());
                total_received += rows.len();
            }
        }
        let resident_worst_case = BATCH_CHANNEL_CAP * BATCH_EMIT_ROWS;
        assert!(
            resident_worst_case <= MAX_INFLIGHT_BATCH_ROWS,
            "Issue #1143: channel-resident worst case {resident_worst_case} must be \
             within the documented MAX_INFLIGHT_BATCH_ROWS={MAX_INFLIGHT_BATCH_ROWS}"
        );
        assert_eq!(
            total_received, confirmed_total,
            "Issue #1143: draining the stalled-then-released scan must yield every \
             confirmed row exactly once (no loss/dup from backpressure)"
        );
        eprintln!(
            "Issue #1143 in-flight bound guard: producer parked with a full \
             (cap {BATCH_CHANNEL_CAP}) batch channel; resident worst case \
             {resident_worst_case} rows <= bound {MAX_INFLIGHT_BATCH_ROWS}; \
             max single batch {max_batch_rows} <= {BATCH_EMIT_ROWS}; \
             total confirmed {confirmed_total}."
        );
        let _ = drain_handle.join().expect("join drain thread");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn io_failed_skips_terminal_drain_on_truncated_window() {
        let Some(data_db) = fixture_data_db() else {
            eprintln!(
                "Skipping {KEYSPACE}.{TABLE}: no Data.db present (run fetch-datasets.sh). \
                     The pure terminal_drain_skipped_iff_io_failed guard still runs."
            );
            return;
        };

        let cfg = crate::Config::default();
        let platform = Arc::new(
            crate::platform::Platform::new(&cfg)
                .await
                .expect("platform"),
        );
        let reader = SSTableReader::open(&data_db, &cfg, platform)
            .await
            .expect("open reader");
        // The fixture must be chunk-compressed (the windowed path's
        // precondition); `collect_raw_chunks` below reads >1 raw chunk only
        // for a multi-chunk compressed Data.db, which the assert enforces.
        assert!(
            reader.compression_info.is_some(),
            "fixture must be compressed to exercise the windowed chunk-stitching path"
        );

        let chunks = collect_raw_chunks(&reader).await;
        assert!(
            chunks.len() > 1,
            "Issue #1143: need a multi-chunk fixture so dropping the last chunk \
                 leaves a non-empty truncated trailing window ({} chunk(s))",
            chunks.len()
        );

        // Drop the LAST raw chunk to simulate a mid-stream truncation: the
        // trailing window now holds a PARTIAL final partition that the
        // terminal drain would otherwise parse and emit.
        let truncated = &chunks[..chunks.len() - 1];

        // The blocking parse half is synchronous; run it off the async runtime
        // via spawn_blocking (it uses blocking_recv/blocking_send).
        let reader = Arc::new(reader);
        let r1 = Arc::clone(&reader);
        let t1 = truncated.to_vec();
        let clean = tokio::task::spawn_blocking(move || drain_count(&r1, &t1, false))
            .await
            .expect("clean drain task");
        let r2 = Arc::clone(&reader);
        let t2 = truncated.to_vec();
        let failed = tokio::task::spawn_blocking(move || drain_count(&r2, &t2, true))
            .await
            .expect("failed drain task");

        eprintln!(
                "Issue #1143 terminal-drain guard: truncated window emitted clean(io_failed=false)={clean} \
                 rows vs failed(io_failed=true)={failed} rows"
            );

        // The clean run MUST have something to lose: its terminal drain parses
        // the truncated trailing window into at least one extra partition.
        // (If this fails the fixture's last chunk ended exactly on a partition
        // boundary — pick a fixture whose tail straddles a chunk.)
        assert!(
            clean > failed,
            "Issue #1143 REGRESSION: io_failed did NOT skip the terminal drain — \
                 truncated window emitted the SAME {failed} rows with and without the \
                 io_failed gate. A mid-stream read error must NOT surface the partial \
                 trailing partition (clean={clean}, failed={failed})."
        );
    }
}
