//! Issue #2370 — mid-flight read-back of the #2361 concurrency gauges through
//! the REAL `do_get` handler path (the first such validation).
//!
//! The `cqlite.rpc.phase.active` gauge (#2361) and `cqlite.rpc.in_flight` gauge
//! (#2264) were only ever validated by SYNCHRONOUS unit arithmetic (two guards on
//! one thread) or single-stream settle checks — never read back while N real
//! `do_get` RPCs overlap in flight. This test holds N streams provably open (a
//! barrier + slow consumer under channel backpressure) and asserts BOTH gauges
//! read the TRUE concurrent count == N mid-flight, then settle to baseline after
//! the streams are released (a midstream drop) AND after N streams complete
//! normally.
//!
//! ## Isolation requirement
//!
//! The gauges are PROCESS-GLOBAL atomics. An exact `== N` read-back is only
//! meaningful if no sibling test has a `do_get` in flight concurrently, so this
//! file holds EXACTLY ONE `#[test]` — one file = one binary = one process, with
//! no sibling threads under plain `cargo test`, and per-test process isolation
//! under nextest (the gate default). Do not add a second `#[test]` here; add a
//! sibling FILE instead.
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --test issue_2370_gauge_readback_test
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::Ticket;
use futures::StreamExt;
use tokio::sync::{Barrier, Notify};

use cqlite_flight::service::CqliteFlightService;

mod concurrent_support;
use concurrent_support as support;

/// Number of simultaneous streams held open. Field runs use 8.
const N: usize = 8;

/// Generous fail-loud ceiling for a gauge to reach its expected level or settle.
/// The gauge moves the instant the handler runs; this is a hang detector, never a
/// tight wall-clock margin.
const GAUGE_TIMEOUT: Duration = Duration::from_secs(30);

/// Poll `read()` until it returns `>= target` or the timeout elapses; returns the
/// final observed value.
async fn poll_until_at_least(target: i64, timeout: Duration, read: impl Fn() -> i64) -> i64 {
    let deadline = Instant::now() + timeout;
    loop {
        let v = read();
        if v >= target || Instant::now() >= deadline {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Poll `read()` until it returns `<= target` or the timeout elapses; returns the
/// final observed value.
async fn poll_until_at_most(target: i64, timeout: Duration, read: impl Fn() -> i64) -> i64 {
    let deadline = Instant::now() + timeout;
    loop {
        let v = read();
        if v <= target || Instant::now() >= deadline {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[test]
fn phase_active_and_in_flight_read_true_concurrent_count_then_settle() {
    // Big multi-SSTable fixture + batch_size 1 so each producer fills the 4-slot
    // do_get channel and PARKS while its slow consumer holds — keeping the RPC in
    // flight without draining.
    let total = 200usize;
    let (_temp, data_dir) = support::build_multi_sstable_fixture(total);
    let svc = CqliteFlightService::new(data_dir, 1);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let baseline_if = cqlite_flight::obs::in_flight_level("do_get");
        let baseline_pa = cqlite_flight::obs::phase_active_level("do_get");
        // In an isolated binary the baseline is 0, but read it explicitly so the
        // assertions are baseline-relative and never assume a clean slate.
        let running = support::start_server(svc).await;
        let addr = running.addr;

        // ---- Phase 1: hold N streams open, read the gauges mid-flight ----------
        let barrier = Arc::new(Barrier::new(N + 1));
        let release = Arc::new(Notify::new());
        let mut holders = Vec::new();
        for _ in 0..N {
            let barrier = barrier.clone();
            let release = release.clone();
            holders.push(tokio::spawn(async move {
                let mut client = support::connect(addr).await;
                let resp = client
                    .do_get(Ticket::new(support::scan_ticket()))
                    .await
                    .expect("do_get rpc");
                let stream = resp.into_inner().map(|r| r.map_err(FlightError::Tonic));
                let mut rb = FlightRecordBatchStream::new_from_flight_data(stream);
                // Read ONE batch so the RPC is provably in flight and streaming.
                let first = rb.next().await;
                assert!(
                    first.is_some(),
                    "each held stream must yield at least one batch before parking"
                );
                // Signal "I am in flight and holding", then wait for release while
                // keeping the stream (and thus the RPC accounting) alive.
                barrier.wait().await;
                release.notified().await;
                // Release: drop the stream + client WITHOUT draining (a midstream
                // drop under backpressure — the #2264 shape).
                drop(rb);
                drop(client);
            }));
        }

        // All N have read a batch and are holding → every stream is in flight.
        barrier.wait().await;

        // THE read-back: both gauges must reflect the true concurrent count N.
        let if_level = poll_until_at_least(baseline_if + N as i64, GAUGE_TIMEOUT, || {
            cqlite_flight::obs::in_flight_level("do_get")
        })
        .await;
        assert_eq!(
            if_level,
            baseline_if + N as i64,
            "cqlite.rpc.in_flight{{do_get}} must read the true {N} concurrent streams mid-flight"
        );
        let pa_level = poll_until_at_least(baseline_pa + N as i64, GAUGE_TIMEOUT, || {
            cqlite_flight::obs::phase_active_level("do_get")
        })
        .await;
        assert_eq!(
            pa_level,
            baseline_pa + N as i64,
            "cqlite.rpc.phase.active{{do_get}} must read the true {N} concurrent streams mid-flight \
             (the #2361 gauge as a LEVEL, not a 0/1 flag) — first real-handler validation"
        );

        // Release all holders → they drop their streams midstream.
        release.notify_waiters();
        for h in holders {
            h.await.expect("holder task panicked");
        }

        // Settle after the midstream drop: both gauges return to baseline. On the
        // pre-#2264 code the parked producers never release and this never settles.
        let settled_if = poll_until_at_most(baseline_if, GAUGE_TIMEOUT, || {
            cqlite_flight::obs::in_flight_level("do_get")
        })
        .await;
        assert!(
            settled_if <= baseline_if,
            "in_flight must settle to its {baseline_if} baseline after the midstream drop, got {settled_if}"
        );
        let settled_pa = poll_until_at_most(baseline_pa, GAUGE_TIMEOUT, || {
            cqlite_flight::obs::phase_active_level("do_get")
        })
        .await;
        assert!(
            settled_pa <= baseline_pa,
            "phase.active must settle to its {baseline_pa} baseline after the midstream drop, got {settled_pa}"
        );

        // ---- Phase 2: N streams that COMPLETE normally also settle to baseline --
        let mut drains = Vec::new();
        for _ in 0..N {
            drains.push(tokio::spawn(async move {
                let mut client = support::connect(addr).await;
                let batches = support::do_get_batches(&mut client, support::scan_ticket()).await;
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            }));
        }
        for h in drains {
            let rows = h.await.expect("drain task panicked");
            assert_eq!(rows, total, "each fully-drained concurrent scan returns all rows");
        }
        let after_complete = poll_until_at_most(baseline_if, GAUGE_TIMEOUT, || {
            cqlite_flight::obs::in_flight_level("do_get")
        })
        .await;
        assert!(
            after_complete <= baseline_if,
            "in_flight must settle to its {baseline_if} baseline after N streams COMPLETE, got {after_complete}"
        );

        running.server.abort();
    });
}
