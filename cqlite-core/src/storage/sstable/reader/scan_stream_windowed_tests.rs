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

/// Issue #1593 (roborev) — the [`FeedFailureGuard`] semantics the panic-path fix
/// rests on. An ARMED guard flips `io_failed` to `true` when dropped; a DISARMED
/// guard leaves it untouched. A refactor that inverts the arm/disarm sense, drops
/// the `Drop` impl, or forgets to disarm the clean-EOF path flips one of these.
#[test]
fn feed_failure_guard_fires_on_drop_iff_armed() {
    // Armed guard dropped (the panic / early-exit case): io_failed becomes true.
    let armed_flag = AtomicBool::new(false);
    {
        let _g = FeedFailureGuard::armed(&armed_flag);
        // dropped at end of scope, still armed
    }
    assert!(
        armed_flag.load(Ordering::SeqCst),
        "Issue #1593: an ARMED FeedFailureGuard must set io_failed=true on drop \
         (the panic/early-exit path), else the parse half would spuriously \
         terminal-drain a truncated window"
    );

    // Disarmed guard dropped (the clean-EOF path): io_failed stays false so the
    // terminal drain runs exactly as before.
    let disarmed_flag = AtomicBool::new(false);
    {
        let mut g = FeedFailureGuard::armed(&disarmed_flag);
        g.disarm();
    }
    assert!(
        !disarmed_flag.load(Ordering::SeqCst),
        "Issue #1593: a DISARMED FeedFailureGuard must leave io_failed false so the \
         clean-EOF terminal drain still runs (happy path byte-identical)"
    );
}

/// Issue #1593 (roborev) — the ORDERING the fix depends on: on a panic the
/// body-local guard must flip `io_failed` to `true` BEFORE the feed closure's
/// captured `raw_tx` drops during unwind (body locals drop before a `move`
/// closure's captured environment). We cannot inject a panic into the real
/// `read_next_block_parts` feed loop without a production test hook, so this test
/// reproduces the exact drop-order scenario: a `move` closure that OWNS a sender
/// analog (which, on drop, records the `io_failed` value it observes) and creates
/// a body-local armed guard, then panics. If the guard fires before the captured
/// sender drops — the property the real fix relies on — the sender observes
/// `io_failed == true`, exactly what the parse half needs to skip the terminal
/// drain.
///
/// INTEGRATION LIMITATION (documented): an end-to-end panic inside the live
/// `feed_raw_chunks_blocking` `spawn_blocking` closure is not cleanly injectable
/// without adding a production-only fault hook, so this proves the drop-order
/// contract on a faithful stand-in rather than the live feed loop. The live wiring
/// is a single `FeedFailureGuard::armed(&io_failed_feed)` body-local at the top of
/// that closure (disarmed only on the clean-EOF exit), whose ordering guarantee is
/// exactly what this test pins.
#[test]
fn feed_failure_guard_fires_before_captured_tx_drops_on_panic() {
    use std::panic::AssertUnwindSafe;

    // Sender analog: on drop, snapshots the io_failed value it can observe. In the
    // real feed closure this is `raw_tx`, whose close is what the parse half reads.
    struct RecordingTx<'a> {
        io_failed: &'a AtomicBool,
        observed_on_drop: &'a AtomicBool,
    }
    impl Drop for RecordingTx<'_> {
        fn drop(&mut self) {
            self.observed_on_drop
                .store(self.io_failed.load(Ordering::SeqCst), Ordering::SeqCst);
        }
    }

    let io_failed = AtomicBool::new(false);
    let observed_on_drop = AtomicBool::new(false);
    // The atomics stay OWNED by this frame so we can read them after the unwind.
    // `&AtomicBool` is `Copy`, so the `move` closure captures these shared refs by
    // copy (not the atomics), while `tx` is captured by move so it drops DURING
    // the closure's unwind.
    let io_failed_ref: &AtomicBool = &io_failed;
    let tx = RecordingTx {
        io_failed: io_failed_ref,
        observed_on_drop: &observed_on_drop,
    };

    // A `move` closure capturing `tx` (like the feed closure captures `raw_tx`)
    // with a BODY-LOCAL armed guard, that panics. AssertUnwindSafe because the
    // shared &AtomicBool references are not UnwindSafe by default; the test does
    // not observe any poisoned/half-updated state, only the post-unwind snapshot.
    let result = std::panic::catch_unwind(AssertUnwindSafe(move || {
        let _guard = FeedFailureGuard::armed(io_failed_ref);
        // `tx` is captured by move; keep it live until the panic so its drop
        // happens during unwind AFTER the body-local guard's drop.
        let _keep = &tx;
        panic!("simulated feed-closure panic");
    }));

    assert!(result.is_err(), "the closure must have panicked");
    assert!(
        io_failed.load(Ordering::SeqCst),
        "Issue #1593: the guard must set io_failed=true during unwind"
    );
    assert!(
        observed_on_drop.load(Ordering::SeqCst),
        "Issue #1593 REGRESSION: the captured sender (raw_tx analog) observed \
         io_failed=false when it dropped during unwind — the guard did NOT fire \
         first. The parse half would then read a spurious CLEAN EOF and emit a \
         truncated trailing partition. The body-local guard must drop (and set the \
         flag) BEFORE the captured sender."
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

    /// Decode raw compressed `chunks` into the decompressed `Bytes` the IO half
    /// now ships on the channel (issue #1940, D2). The parse half no longer
    /// decodes, so these helpers must pre-decode exactly as `decode_scan_chunk`
    /// does (CRC was verified during the read that produced `chunks`).
    fn decode_chunks(reader: &SSTableReader, chunks: &[Vec<u8>]) -> Result<Vec<bytes::Bytes>> {
        let max_cl = reader
            .compression_info
            .as_ref()
            .map(|ci| ci.max_compressed_length as usize)
            .unwrap_or(usize::MAX);
        let mut out = Vec::with_capacity(chunks.len());
        for (i, c) in chunks.iter().enumerate() {
            let (decoded, _recycled) = reader.decode_scan_chunk(i, max_cl, c.clone())?;
            out.push(decoded);
        }
        Ok(out)
    }

    /// Run the blocking parse half over `chunks` with the given `io_failed`,
    /// returning the number of `(RowKey, Value)` entries it emitted. Runs on
    /// the current thread (the function is synchronous); the bounded channel
    /// is pre-filled and its sender dropped so `blocking_recv` never blocks.
    fn drain_count(reader: &SSTableReader, chunks: &[Vec<u8>], io_failed: bool) -> usize {
        let (res, n) = drain_result(reader, chunks, io_failed);
        res.expect("drain_scan_window_blocking");
        n
    }

    /// As [`drain_count`], but hands back the drain's own `Result` ALONGSIDE the
    /// number of rows that reached the output channel before it returned.
    ///
    /// Issue #3721 needs both halves: a per-column decode failure now PROPAGATES
    /// out of the parse half instead of being swallowed as an end-of-partition
    /// signal, so a caller must be able to observe the `Err` — and, separately, to
    /// measure whether the streaming path had already emitted rows to its consumer
    /// when it did.
    ///
    /// Issue #3782 needs the same helper to assert a REFUSAL (a terminal drain over a
    /// TRUNCATED window returns the decode error instead of emitting a partial
    /// trailing partition). The tuple serves both, where its `Result<usize>` could
    /// not: on `Err` a `Result<usize>` carries NO count, and the rows-already-emitted
    /// figure is exactly what distinguishes "refused before emitting" from "refused
    /// after handing rows to the consumer".
    fn drain_result(
        reader: &SSTableReader,
        chunks: &[Vec<u8>],
        io_failed: bool,
    ) -> (Result<()>, usize) {
        let ctx = WindowParseCtx {
            now_secs: None,
            start_key: None,
            end_key: None,
            schema: reader.get_table_schema(None),
            max_compressed_length: reader
                .compression_info
                .as_ref()
                .map(|ci| ci.max_compressed_length as usize)
                .unwrap_or(usize::MAX),
        };
        // Feed DECODED chunks (issue #1940) through an unbounded->bounded-shaped
        // channel large enough to hold them all, then drop the sender so the parse
        // half sees a CLEAN close; the `io_failed` flag (not the close reason)
        // drives the terminal-drain decision under test.
        let decoded = decode_chunks(reader, chunks).expect("decode chunks");
        let (raw_tx, raw_rx) = mpsc::channel::<bytes::Bytes>(decoded.len().max(1));
        for c in decoded {
            raw_tx.try_send(c).expect("prefill decoded chunk");
        }
        drop(raw_tx);
        // Output channel now carries batched rows (issue #1143). Big enough
        // that `blocking_send` never blocks here; count rows ACROSS batches.
        let (out_tx, mut out_rx) = mpsc::channel::<Result<Vec<(RowKey, ScanRow)>>>(4096);
        let flag = Arc::new(AtomicBool::new(io_failed));
        let res = reader.drain_scan_window_blocking(ctx, raw_rx, out_tx, flag);
        let mut n = 0usize;
        while let Ok(item) = out_rx.try_recv() {
            if let Ok(rows) = item {
                n += rows.len();
            }
        }
        (res, n)
    }

    /// Build the same `WindowParseCtx` the I/O half resolves for this fixture.
    fn ctx_for(reader: &SSTableReader) -> WindowParseCtx {
        WindowParseCtx {
            now_secs: None,
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

    /// Reproduce the production sequence when a trailing chunk fails to decode
    /// (issue #1940, D2): the IO half DECODES each chunk before shipping it, so a
    /// corrupt chunk errors in the IO half — its bytes NEVER reach the parse half.
    /// Returns `(decode_result_of_corrupt_chunk, rows_delivered_from_good_prefix)`:
    /// the good prefix is decoded and fed to the parse half with `io_failed = true`
    /// (mirroring the IO half having set the flag and dropped the sender after the
    /// decode error), so the parse half delivers the prefix's confirmed rows and
    /// SKIPS the terminal drain, and the corrupt chunk's decode Err is what the IO
    /// half surfaces as the scan result.
    fn drain_with_trailing_corrupt(
        reader: &SSTableReader,
        chunks: &[Vec<u8>],
    ) -> (Result<()>, usize) {
        let ctx = ctx_for(reader);
        // A short, non-raw, non-decompressible chunk: 8 bytes that no supported
        // codec accepts. Strictly shorter than any real `max_compressed_length`,
        // so it is NOT an incompressible-raw chunk and goes through `decompress`,
        // which returns Err — at DECODE time, in the IO half.
        let max_cl = reader
            .compression_info
            .as_ref()
            .map(|ci| ci.max_compressed_length as usize)
            .unwrap_or(usize::MAX);
        let corrupt_result = reader
            .decode_scan_chunk(chunks.len(), max_cl, vec![0xFFu8; 8])
            .map(|_| ());

        // Feed the DECODED good prefix to the parse half with io_failed = true.
        let decoded = decode_chunks(reader, chunks).expect("decode good prefix");
        let (raw_tx, raw_rx) = mpsc::channel::<bytes::Bytes>(decoded.len().max(1));
        for c in decoded {
            raw_tx.try_send(c).expect("prefill decoded chunk");
        }
        drop(raw_tx);

        // Large output channel so `blocking_send` never blocks; we count rows
        // delivered ACROSS batches BEFORE the (IO-half) error.
        let (out_tx, mut out_rx) = mpsc::channel::<Result<Vec<(RowKey, ScanRow)>>>(4096);
        let flag = Arc::new(AtomicBool::new(true));
        reader
            .drain_scan_window_blocking(ctx, raw_rx, out_tx, flag)
            .expect("parse half over the clean prefix must not itself error");

        let mut received = 0usize;
        while let Ok(item) = out_rx.try_recv() {
            if let Ok(rows) = item {
                received += rows.len();
            }
        }
        (corrupt_result, received)
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
    /// materializes in `scratch` before batching — pre-existing #1156 behavior);
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
        let decoded = decode_chunks(&reader, &chunks).expect("decode chunks");
        let (raw_tx, raw_rx) = mpsc::channel::<bytes::Bytes>(decoded.len().max(1));
        for c in decoded {
            raw_tx.try_send(c).expect("prefill decoded chunk");
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
        let clean = tokio::task::spawn_blocking(move || drain_result(&r1, &t1, false))
            .await
            .expect("clean drain task");
        let r2 = Arc::clone(&reader);
        let t2 = truncated.to_vec();
        let failed = tokio::task::spawn_blocking(move || drain_result(&r2, &t2, true))
            .await
            .expect("failed drain task");

        eprintln!(
            "Issue #1143 terminal-drain guard: truncated window, \
             clean(io_failed=false) -> {:?} after {} rows; \
             failed(io_failed=true) -> {:?} after {} rows",
            clean.0.as_ref().err().map(|e| e.to_string()),
            clean.1,
            failed.0.as_ref().err().map(|e| e.to_string()),
            failed.1
        );

        // Issue #3782 sharpened the same discriminator from its side: the clean run's
        // terminal drive is `at_final_chunk = true` over bytes whose last row is cut, so
        // it REFUSES rather than emitting partial rows. Before that it emitted them and
        // this guard compared row COUNTS — i.e. the fixture built to expose silent
        // truncation was asserted from the wrong side. Both issues reached that
        // conclusion independently.
        //
        // THE PROPERTY (issue #1143), unchanged: `io_failed` SKIPS the terminal
        // drain, so a mid-stream read error never surfaces the partial trailing
        // partition. The gated run must therefore complete cleanly, having parsed
        // only whole partitions confirmed before the truncation.
        assert!(
            failed.0.is_ok(),
            "Issue #1143 REGRESSION: with io_failed set, the terminal drain must be \
             SKIPPED, so the truncated trailing fragment is never parsed and the \
             drain returns Ok; got {:?}",
            failed.0
        );

        // HOW THE PROPERTY IS MEASURED CHANGED WITH ISSUE #3721, and this is an
        // INVERSION of the old measurement, not a relaxation of it.
        //
        // The old assertion was `clean > failed`: the ungated run parsed the
        // truncated fragment and emitted MORE rows, and that surplus was the
        // evidence its terminal drain had run. Those surplus rows existed only
        // because row assembly SWALLOWED the decode failure the truncation causes
        // and returned the partial partition as a successful read — the defect
        // issue #3721 removes. With the swallow gone the ungated run reports the
        // truncation instead of serving it, so the evidence that its terminal
        // drain RAN is now the ERROR rather than the surplus.
        //
        // This discriminates STRICTLY MORE than `clean > failed` did: it names the
        // mechanism (`Error::ColumnDecode`, matched on the VARIANT — never on
        // message text, issue #28) instead of comparing two row counts that could
        // coincide for unrelated reasons.
        let Err(e) = clean.0 else {
            panic!(
                "Issue #1143/#3721 REGRESSION: the ungated (io_failed=false) drain \
                 must RUN its terminal drain over the truncated trailing fragment \
                 and REPORT the resulting decode failure. It returned Ok after {} \
                 rows instead — either the terminal drain did not run (the #1143 \
                 property is broken in the other direction) or the failure was \
                 swallowed into a partial partition (the #3721 defect is back).",
                clean.1
            );
        };
        assert!(
            matches!(e, Error::ColumnDecode { .. }),
            "the truncated fragment's failure must surface as the dedicated \
             per-column variant (issue #3721), not as some other error; got {e:?}"
        );
    }
}

/// Issue #1707 (roborev job 133) — a windowed-feed read helper that reads NOTHING
/// must record NO io time and must not sleep the injected test delay.
///
/// # Why this is a distinct property, and why absence is the assertion
///
/// The read-phase design's whole contract is that a phase which never ran emits NO
/// sample: `read_metrics` skips a zero-nanos phase deliberately, because a `0.0`
/// asserts a measurement that was never taken, and `observability::read_phase`'s
/// coverage boundary tells operators that an absent `read.phase.*` series means NOT
/// MEASURED — never "fast". The io seam is the one place that rule can be broken in
/// the OTHER direction: both feed helpers are called once more than they read (the
/// terminal call that returns `Ok(None)` at EOF), and one that starts its timer
/// before its no-data checks charges function-call and EOF-check time to
/// `read.phase.io` — a FABRICATED measurement for a read that never happened, and
/// on a scan that reads nothing at all, an io SAMPLE with no read behind it.
///
/// So the assertion is `nanos(Io) == 0`, not "small": zero is a fact about a call
/// that did no work, and any timer at all makes it non-zero. It is deliberately NOT
/// a wall-clock threshold (#2642) — nothing here compares an elapsed time against a
/// budget, so no host timing can change the verdict.
///
/// The injected `io_delay` is ARMED across the no-read calls on purpose: it is armed
/// immediately inside the timed region at both seams, so if it is ever hoisted back
/// ahead of the EOF checks alongside the timer, this test fails LOUDLY (milliseconds
/// charged to io for a call that read nothing) instead of marginally. The positive
/// controls in the same test are what stop the whole case passing vacuously — they
/// prove the sink really is installed and really does receive a REAL read's io time.
///
/// Fixtures are COMMITTED to git, so absence is a resolution defect, never a
/// legitimate skip (#3220): both cases fail closed.
mod io_phase_no_read_absence {
    use super::*;
    use crate::observability::read_phase::{self, io_delay, ReadPhase, ReadPhaseTimings};
    use crate::storage::sstable::reader::read_at::DirectScratch;
    use crate::storage::sstable::SSTableReader;
    use std::path::PathBuf;
    use std::time::Duration;

    /// A COMMITTED compressed BIG (`nb`) fixture — it has a `CompressionInfo.db`, so
    /// `read_compressed_chunk_sync` is the helper under test.
    const COMPRESSED: (&str, &str) = ("test_big", "wide_partition");

    /// A COMMITTED **uncompressed** BIG (`nb`) fixture — no `CompressionInfo.db`, so
    /// the feed drives `read_uncompressed_piece_sync`.
    const UNCOMPRESSED: (&str, &str) = ("test_comp", "uncompressed_table");

    /// Every candidate corpus root, in the order they are probed. Neither root is a
    /// superset of the other (#3220), so resolution walks BOTH and picks by
    /// EVIDENCE — the root that actually holds this table's `*-Data.db`.
    fn candidate_roots() -> Vec<PathBuf> {
        let mut roots = Vec::new();
        if let Ok(env_root) = std::env::var("CQLITE_DATASETS_ROOT") {
            roots.push(PathBuf::from(env_root));
        }
        // Checkout-relative: `cqlite-core/..` is the workspace root.
        roots.push(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("test-data")
                .join("datasets"),
        );
        roots
    }

    /// Resolve a COMMITTED fixture's `Data.db`, FAIL-CLOSED (#3220).
    fn committed_data_db((keyspace, table): (&str, &str)) -> PathBuf {
        for root in candidate_roots() {
            let ks_dir = root.join("sstables").join(keyspace);
            let Ok(entries) = std::fs::read_dir(&ks_dir) else {
                continue;
            };
            let prefix = format!("{table}-");
            for gen_dir in entries.flatten().map(|e| e.path()) {
                if !gen_dir.is_dir()
                    || !gen_dir
                        .file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with(&prefix))
                {
                    continue;
                }
                let Ok(files) = std::fs::read_dir(&gen_dir) else {
                    continue;
                };
                if let Some(data) = files
                    .flatten()
                    .map(|f| f.path())
                    .find(|p| p.to_string_lossy().ends_with("-Data.db"))
                {
                    return data;
                }
            }
        }
        panic!(
            "{keyspace}.{table} is COMMITTED to git and must resolve in every checkout, \
             unconditionally (#3220) — searched {:?}",
            candidate_roots()
        )
    }

    async fn open_reader(fixture: (&str, &str)) -> Arc<SSTableReader> {
        let data_db = committed_data_db(fixture);
        let cfg = crate::Config::default();
        let platform = Arc::new(
            crate::platform::Platform::new(&cfg)
                .await
                .expect("platform"),
        );
        Arc::new(
            SSTableReader::open(&data_db, &cfg, platform)
                .await
                .expect("open the committed fixture"),
        )
    }

    /// The COMPRESSED feed's terminal call — `chunk_index` past the last chunk —
    /// reads nothing, so it must contribute no io time at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(read_phase_io_delay)]
    async fn a_past_eof_compressed_chunk_read_records_no_io_time() {
        let reader = open_reader(COMPRESSED).await;
        let past_eof = reader
            .compression_info
            .as_ref()
            .expect("the committed fixture is compressed")
            .chunk_offsets
            .len();

        // Armed across BOTH calls: milliseconds land in io only if a timed region is
        // entered, so a resurrected pre-check timer/delay fails this loudly.
        let _armed = io_delay::arm(Duration::from_millis(5));

        let eof_sink = Arc::new(ReadPhaseTimings::default());
        let mut scratch: Vec<u8> = Vec::new();
        let mut direct = DirectScratch::new();
        {
            let _installed = read_phase::install(Some(Arc::clone(&eof_sink)));
            let out = reader
                .read_compressed_chunk_sync(past_eof, &mut scratch, &mut direct)
                .expect("a past-EOF chunk index is clean EOF, not an error");
            assert!(
                out.is_none(),
                "chunk {past_eof} is past the last chunk, so the helper must report EOF"
            );
        }
        assert_eq!(
            eof_sink.nanos(ReadPhase::Io),
            0,
            "issue #1707: a past-EOF chunk read performs NO read, so it must charge \
             NOTHING to read.phase.io — an io sample behind a call that read nothing \
             is a fabricated measurement, exactly what the absent-phase rule exists \
             to prevent (with a 5ms delay armed, a non-zero value here also means the \
             injected delay slept on a path that never reads)"
        );

        // Positive control — without it the case could pass because the sink was
        // never installed, or because the seam records nothing at all.
        let read_sink = Arc::new(ReadPhaseTimings::default());
        {
            let _installed = read_phase::install(Some(Arc::clone(&read_sink)));
            let out = reader
                .read_compressed_chunk_sync(0, &mut scratch, &mut direct)
                .expect("chunk 0 of a committed fixture reads")
                .expect("chunk 0 is real data, not EOF");
            assert!(!out.is_empty(), "chunk 0 carries compressed bytes");
        }
        assert!(
            read_sink.nanos(ReadPhase::Io) > 0,
            "positive control: a REAL chunk read must charge io time (else the \
             absence assertion above proves nothing about the seam)"
        );
    }

    /// The UNCOMPRESSED feed's terminal call — `pos` at the end of the file — reads
    /// nothing, so it must contribute no io time at all.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(read_phase_io_delay)]
    async fn an_at_eof_uncompressed_piece_read_records_no_io_time() {
        let reader = open_reader(UNCOMPRESSED).await;
        assert!(
            reader.compression_info.is_none(),
            "the committed fixture must be UNCOMPRESSED so the feed drives the \
             uncompressed piece helper"
        );
        let file_size = reader.scan_positional_source.len();
        assert!(file_size > 0, "the committed fixture has a data section");

        let _armed = io_delay::arm(Duration::from_millis(5));

        let eof_sink = Arc::new(ReadPhaseTimings::default());
        let mut direct = DirectScratch::new();
        {
            let _installed = read_phase::install(Some(Arc::clone(&eof_sink)));
            let out = reader
                .read_uncompressed_piece_sync(file_size, &mut direct)
                .expect("a position at end-of-file is clean EOF, not an error");
            assert!(
                out.is_none(),
                "there is nothing left to read at offset {file_size} (= file length)"
            );
        }
        assert_eq!(
            eof_sink.nanos(ReadPhase::Io),
            0,
            "issue #1707: an at-EOF piece read performs NO read, so it must charge \
             NOTHING to read.phase.io (same fabricated-measurement rule as the \
             compressed sibling)"
        );

        // Positive control, as above: the FIRST piece of the data section is a real
        // read and must be charged.
        let read_sink = Arc::new(ReadPhaseTimings::default());
        let header = reader.calculate_header_size() as u64;
        {
            let _installed = read_phase::install(Some(Arc::clone(&read_sink)));
            let (piece, next) = reader
                .read_uncompressed_piece_sync(header, &mut direct)
                .expect("the first data-section piece reads")
                .expect("the first piece is real data, not EOF");
            assert!(!piece.is_empty(), "the first piece carries bytes");
            assert!(next > header, "the read advanced the cursor");
        }
        assert!(
            read_sink.nanos(ReadPhase::Io) > 0,
            "positive control: a REAL piece read must charge io time"
        );
    }
}
