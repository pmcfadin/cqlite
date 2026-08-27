//! Read-metric accounting at the streaming-scan boundary (issue #1701, roborev F2).
//!
//! Gated on `observability-testing` because the assertions read back the emitted
//! series through the in-memory capture harness. Deterministic and hermetic: the
//! channel is hand-built, so "the producer already enqueued N rows the consumer never
//! polled" is a fact of the fixture rather than a timing outcome — the shape a real
//! `LIMIT` consumer produces, without the race an integration test would need.
//!
//! Included from [`super`] via `#[path = "joined_scan_stream_read_metric_tests.rs"]`,
//! so `use super::*` reaches `JoinedStream` and its constructors.

use crate::observability::testing;
use crate::observability::{catalog, testing::CapturedMetrics};
use tokio::sync::mpsc;

use super::*;

fn row(byte: u8) -> (RowKey, ScanRow) {
    (RowKey::new(vec![byte]), ScanRow::Row(Vec::new()))
}

fn duration_recordings(metrics: &CapturedMetrics, name: &str) -> u64 {
    metrics
        .find(name)
        .map(|m| m.points.iter().filter_map(|p| p.count).sum())
        .unwrap_or(0)
}

/// A consumer that stops polling early (the `LIMIT` shape) and drops the stream
/// must still report the rows the producer had ALREADY materialised and enqueued.
///
/// Before the drain in [`JoinedStream::drop`], this reported ONE row of the five
/// the producer sent, while `cqlite.read.bytes` still counted the chunks all five
/// were decoded from — so a read-amplification ratio computed from the pair was
/// wrong in exactly the case that matters.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(read_metrics)]
async fn dropping_a_buffered_stream_reports_the_rows_the_producer_materialised() {
    let mc = testing::metrics_capture();
    mc.reset();

    const SENT: usize = 5;
    let (tx, rx) = mpsc::channel(SENT + 1);
    for i in 0..SENT {
        // Two DISTINCT partitions (keys 0,0,0 then 1,1) so the partition counter
        // is not trivially equal to the row counter.
        tx.send(Ok(row(if i < 3 { 0 } else { 1 })))
            .await
            .expect("send");
    }
    drop(tx); // the producer finished: every row is enqueued, none is lost
    let task = tokio::spawn(async {});

    {
        let mut stream = RowScanStream::new_measured_rows(rx, task, None);
        // Poll exactly ONE row, then abandon the stream with four rows buffered.
        let first = stream.recv().await.expect("one item").expect("ok item");
        assert_eq!(first.0.as_bytes(), &[0u8]);
    }

    let metrics = mc.flush_and_collect();
    assert_eq!(
        metrics.counter_sum(catalog::READ_ROWS),
        SENT as f64,
        "an abandoned stream must report all {SENT} rows the producer enqueued, \
         not just the one that was polled; points: {:?}",
        metrics.find(catalog::READ_ROWS).map(|m| &m.points)
    );
    assert_eq!(
        metrics.counter_sum(catalog::READ_PARTITIONS),
        2.0,
        "both distinct partitions the producer emitted; points: {:?}",
        metrics.find(catalog::READ_PARTITIONS).map(|m| &m.points)
    );
    assert_eq!(
        duration_recordings(&metrics, catalog::READ_DURATION),
        1,
        "the abandoned scan is still ONE read operation"
    );
}

/// The other half of the same property: a stream drained to its end of stream
/// reports each row EXACTLY once — the drain-on-drop must not re-count rows the
/// consumer already polled.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(read_metrics)]
async fn a_fully_consumed_stream_counts_each_row_exactly_once() {
    let mc = testing::metrics_capture();
    mc.reset();

    const SENT: usize = 4;
    let (tx, rx) = mpsc::channel(SENT + 1);
    for i in 0..SENT {
        tx.send(Ok(row(i as u8))).await.expect("send");
    }
    drop(tx);
    let task = tokio::spawn(async {});

    {
        let mut stream = RowScanStream::new_measured_rows(rx, task, None);
        let mut seen = 0usize;
        while let Some(item) = stream.recv().await {
            item.expect("ok item");
            seen += 1;
        }
        assert_eq!(seen, SENT);
    }

    let metrics = mc.flush_and_collect();
    assert_eq!(
        metrics.counter_sum(catalog::READ_ROWS),
        SENT as f64,
        "each polled row counts ONCE — the drop-drain must not double count; \
         points: {:?}",
        metrics.find(catalog::READ_ROWS).map(|m| &m.points)
    );
    assert_eq!(
        duration_recordings(&metrics, catalog::READ_DURATION),
        1,
        "one operation, one duration sample (recv end-of-stream then Drop)"
    );
}
