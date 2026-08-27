//! The multi-generation merge ABANDONS its detached blocking work when the
//! caller's future is dropped (issue #1695, roborev blocker).
//!
//! # Why this is a hard case to test honestly
//!
//! The defect is a RACE by nature — a timer fires while a blocking thread is
//! mid-merge — and "the merge stopped early" is invisible from the result (the
//! caller is gone; there IS no result). Asserting it by elapsed time would be the
//! wall-clock-threshold anti-pattern (#2642), and asserting it by "wait and hope
//! the merge did not finish first" would be a flake generator: pre-fix the merge
//! ALSO terminates — just later.
//!
//! # The construction that removes the race entirely
//!
//! The runtime is built with `max_blocking_threads(1)` and the test OCCUPIES that
//! one thread with a closure parked on a channel it controls. The merge's
//! `spawn_blocking` closure is therefore QUEUED and cannot have started. Only then
//! is the scan future dropped — tripping the per-call flag — and only then is the
//! occupier released. The merge closure thus begins with its flag ALREADY set, so
//! its first per-partition check (which sits BEFORE the first `step()`) decides
//! the outcome deterministically, on every host and under any scheduler.
//!
//! `probe::abandoned()` is the observable, and `probe::armed()` is the
//! anti-vacuity guard: `armed == 1` proves the scan really reached the merge's
//! spawn point (a fixture or routing mistake that never got there would otherwise
//! leave both counters at zero and "prove" nothing). Pre-fix — with the closure
//! running to completion on a dropped `JoinHandle` — `abandoned()` stays `0`.
//!
//! Surface: `SSTableManager::scan`, the public API the query engine's bounded
//! `execute` reaches for a multi-generation table. The engine half of the chain
//! (an elapsed budget DROPS the inner future) is pinned separately by
//! `query::engine::deadline::tests::elapse_drops_the_inner_future` and
//! `tests/issue_1695_query_timeout.rs`.

use std::sync::mpsc;

use serial_test::serial;
use tempfile::TempDir;

use super::probe;
use crate::storage::sstable::generation_merge::multi_gen_fixture as fixture;
use crate::storage::write_engine::test_support::create_test_schema;

/// Bound on how many yields we give the released merge closure to record its
/// abandonment. NOT a timing assertion: the outcome is already decided by
/// construction, this only stops the test hanging if the mechanism is absent.
const MAX_POLLS: usize = 2_000;

#[test]
#[serial]
fn a_dropped_multi_generation_read_abandons_its_blocking_merge() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        // THE construction: exactly one blocking thread, which the test holds.
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .expect("runtime");

    rt.block_on(async {
        let temp = TempDir::new().expect("temp dir");
        let data_dir = fixture::flush_overlapping_generations(&temp).await;
        let manager = fixture::open_manager(&data_dir).await;
        let schema = create_test_schema();
        let table = fixture::table_id();

        // Precondition: this fixture really does route through the reconciling
        // multi-generation merge (a single-generation table would never spawn the
        // blocking merge at all, making everything below vacuous). Also warms every
        // reader, so the dropped scan below needs no blocking thread before the
        // merge's own `spawn_blocking`.
        let warm = manager
            .scan(&table, None, None, None, Some(&schema))
            .await
            .expect("warm-up scan");
        assert_eq!(
            warm.len(),
            fixture::reconciled_rows(),
            "precondition: the fixture must reconcile across generations, else no \
             cross-generation merge runs and this case proves nothing"
        );

        probe::reset();

        // Occupy the single blocking thread so the merge closure can only QUEUE.
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (running_tx, running_rx) = tokio::sync::oneshot::channel::<()>();
        let occupier = tokio::task::spawn_blocking(move || {
            let _ = running_tx.send(());
            let _ = release_rx.recv();
        });
        running_rx.await.expect("occupier must be on the thread");

        // Drive the scan to its first suspension. With the pool occupied that is the
        // `spawn_blocking` join inside the merge, so the closure is queued, unstarted.
        let mut scan = Box::pin(manager.scan(&table, None, None, None, Some(&schema)));
        let mut armed = false;
        for _ in 0..MAX_POLLS {
            if poll_once(&mut scan).is_some() {
                panic!(
                    "the scan must not COMPLETE: its merge cannot run, the one blocking \
                     thread is held by the occupier"
                );
            }
            if probe::armed() > 0 {
                armed = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            armed,
            "precondition: the scan must reach the merge's spawn point (armed guard); \
             without it nothing was queued and the case would be vacuous"
        );
        assert_eq!(
            probe::abandoned(),
            0,
            "precondition: nothing may be abandoned yet — the closure has not started"
        );

        // THE EVENT: the caller's future goes away. Nothing else changes.
        drop(scan);

        // Let the queued merge closure run. It starts with its flag already tripped.
        drop(release_tx);
        occupier.await.expect("occupier join");

        for _ in 0..MAX_POLLS {
            if probe::abandoned() >= 1 {
                return;
            }
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        panic!(
            "the blocking merge did NOT abandon after its caller's future was dropped: it \
             ran to completion building a result nobody could receive (armed={}, abandoned={})",
            probe::armed(),
            probe::abandoned()
        );
    });
}

/// Poll `fut` exactly once with a no-op waker: `None` when it is still pending.
fn poll_once<F: std::future::Future>(fut: &mut std::pin::Pin<Box<F>>) -> Option<F::Output> {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    match fut.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => Some(v),
        std::task::Poll::Pending => None,
    }
}
