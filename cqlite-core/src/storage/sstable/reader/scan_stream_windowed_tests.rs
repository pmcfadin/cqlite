//! Unit + dataset-dependent guards for the windowed streaming scan
//! ([`super`] = `scan_stream_windowed`). Split out of the parent module to
//! keep the source file under the campsite-rule size limit (issue #1143).
//!
//! Included via `#[cfg(test)] #[path = "scan_stream_windowed_tests.rs"] mod tests;`
//! in the parent, so `use super::*` resolves to the windowed-scan module's
//! private items (which these guards drive directly).

use super::*;

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
        let table_root = datasets_root()?.join("sstables").join(KEYSPACE);
        for entry in std::fs::read_dir(&table_root).ok()?.flatten() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with(&format!("{TABLE}-")) && entry.path().is_dir() {
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
        let (out_tx, mut out_rx) = mpsc::channel::<Result<Vec<(RowKey, Value)>>>(4096);
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
