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

/// A LIVE, BLOCKED producer cannot lose an enqueued row to the drop-drain race
/// (issue #1701, roborev R2).
///
/// # The race, and why the sibling cases cannot see it
///
/// The other two cases pre-fill an ALREADY-CLOSED channel, so no send can land during
/// the drain. With a producer still running, `Drop` used to `try_recv()` until `Empty`
/// and only then destroy the receiver — and the drain itself FREES capacity, so a
/// blocked `send` could complete inside that window. That row was decoded, accepted by
/// the channel, and then discarded AFTER the meter had finished: abandoned-stream
/// totals that depend on thread timing, which is worse than no metric at all.
///
/// # What is deterministic here, and what is deliberately NOT asserted
///
/// The fixture forces the exact pre-race state without a timer: capacity 1, one
/// buffered row, and the producer PROVABLY parked in its second `send` (a handshake,
/// not a sleep). It then asserts the two properties that hold on every schedule — the
/// parked send is rejected, and the reported total equals what the channel actually
/// held.
///
/// It does NOT try to reproduce the LOSS itself. That needs one specific interleaving
/// (the parked send completing after the drain's final `try_recv` and before the
/// receiver's destruction) which no test can force from outside the drop path, and
/// chasing it with a sleep is the wall-clock flake the doctrine forbids. The fix makes
/// the loss unreachable by CONSTRUCTION — the send set is frozen before the drain
/// starts — so the ordering itself is pinned structurally by
/// [`the_drop_closes_the_channel_before_draining`], which REDs if the `close()` is
/// removed or moved after the drain.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(read_metrics)]
async fn a_blocked_producer_cannot_enqueue_a_row_the_drain_will_discard() {
    let mc = testing::metrics_capture();
    mc.reset();

    // Capacity ONE: the producer's first send occupies the whole channel, so its
    // second send must park until capacity frees.
    let (tx, rx) = mpsc::channel::<Result<(RowKey, ScanRow)>>(1);
    let (first_sent_tx, first_sent_rx) = tokio::sync::oneshot::channel::<()>();
    let (verdict_tx, verdict_rx) = tokio::sync::oneshot::channel::<bool>();

    let producer = tokio::spawn(async move {
        tx.send(Ok(row(7)))
            .await
            .expect("first send fits capacity 1");
        let _ = first_sent_tx.send(());
        // Parks here until the receiver frees a slot (the OLD drain) or is closed
        // (the fix). `is_err()` is the discriminator, reported back rather than
        // asserted on this task so a failure is a test assertion, not a task panic.
        let second = tx.send(Ok(row(8))).await;
        let _ = verdict_tx.send(second.is_err());
    });

    // The producer is now PROVABLY in the parked state: row 7 is buffered and row 8's
    // send has been entered. No timer involved.
    first_sent_rx
        .await
        .expect("producer signalled its first send");

    {
        let _stream = RowScanStream::new_measured_rows(rx, task_noop(), None);
        // Abandon WITHOUT polling: the LIMIT-consumer shape, with a producer still live.
    }

    let second_send_failed = verdict_rx.await.expect("producer reported its verdict");
    assert!(
        second_send_failed,
        "the parked send must FAIL — Drop closes the channel BEFORE draining, so a row \
         cannot be enqueued into the window between the last try_recv and the \
         receiver's destruction and then be discarded uncounted"
    );
    producer.await.expect("producer task must not panic");

    let metrics = mc.flush_and_collect();
    // Exactly the ONE row that was really enqueued: the buffered row is accounted by
    // the drain, and the rejected row is not counted because it never entered the
    // channel. Either way the total matches what the channel actually held — the
    // property the race destroyed.
    assert_eq!(
        metrics.counter_sum(catalog::READ_ROWS),
        1.0,
        "the abandoned stream reports exactly the rows the channel held; points: {:?}",
        metrics.find(catalog::READ_ROWS).map(|m| &m.points)
    );
    assert_eq!(
        duration_recordings(&metrics, catalog::READ_DURATION),
        1,
        "still ONE read operation"
    );
}

/// A producer handle for a case whose producer task is separate from the stream's own
/// (the stream only needs SOME finished task to join).
fn task_noop() -> tokio::task::JoinHandle<()> {
    tokio::spawn(async {})
}

/// STRUCTURAL pin for the ordering the race fix depends on (issue #1701, roborev R2).
///
/// `Drop` must `close()` the channel BEFORE it drains: closing freezes the set of
/// enqueued rows, so no send can land in the window between the drain's last
/// `try_recv()` and the receiver's destruction — the row that would then be discarded
/// after the meter had already finished. A behavioural test cannot force that
/// interleaving deterministically (see the sibling case), so the ORDER is asserted
/// here, on the source itself: `include_str!` embeds the module at COMPILE time, so
/// there is no path resolution, no I/O, and nothing to skip.
#[test]
fn the_drop_closes_the_channel_before_draining() {
    const SRC: &str = include_str!("joined_scan_stream.rs");

    let drop_impl = SRC
        .split_once("impl<T: ScanStreamItem> Drop for JoinedStream<T> {")
        .map(|(_, rest)| rest)
        .expect("the Drop impl must exist — it is what emits an abandoned scan's totals");
    let body = drop_impl
        .split_once("\n}")
        .map(|(body, _)| body)
        .unwrap_or(drop_impl);

    let close_at = body.find("self.rx.close();").expect(
        "Drop must close the channel before draining: without it a live producer can \
         enqueue a row after the drain's last try_recv and it is discarded UNCOUNTED, \
         making abandoned-stream totals depend on thread timing (issue #1701 R2)",
    );
    let drain_at = body
        .find("self.rx.try_recv()")
        .expect("Drop must drain the buffered rows the consumer never polled");
    assert!(
        close_at < drain_at,
        "self.rx.close() must come BEFORE the drain loop; closing after it leaves the \
         same race open (close_at={close_at}, drain_at={drain_at})"
    );
}
