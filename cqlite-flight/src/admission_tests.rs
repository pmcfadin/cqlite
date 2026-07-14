//! Deterministic `do_get` admission-control tests (issue #2420, WS4).
//!
//! Every scenario is injected, never timed: concurrency is held via pre-acquired
//! permits / single-poll `futures::poll!` (which never idles the runtime, so the
//! paused clock cannot auto-advance), and the permit-wait timeout fires via the
//! paused/auto-advancing Tokio clock (`tokio::time::pause`) — never a wall-clock
//! sleep. The end-to-end tests build real in-process SSTables (via the write
//! engine, so no external fixtures) and drive the public `FlightService::do_get`
//! surface, proving the ceiling engages on the real path.
//!
//! Red-on-main proof: `excess_do_gets_never_reach_setup` asserts the excess
//! requests never run `do_get_resolve` (the `resolves` work counter, bumped only
//! by the filesystem-touching `resolve_dir` call inside it, stays flat) while all
//! permits are held — with the acquire-before-resolve wiring removed, every
//! offered request runs the filesystem resolve and the counter jumps, reding the
//! test. Ticket VALIDATION (parse + schema/predicate build, roborev-1698) is
//! deliberately excluded from this gate: it is cheap and filesystem-free, so it
//! runs BEFORE admission and must not wait behind the semaphore — see
//! `req_malformed_ticket_bypasses_admission_entirely` below.

use std::task::Poll;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::{Code, Request};

use crate::admission::{Admission, AdmissionConfig};
use crate::service::CqliteFlightService;
use crate::testutil::{build_sstables, simple_schema, total_rows, write_row, KS, SIMPLE_DDL, TBL};
use crate::ticket::FlightTicket;
use cqlite_core::observability::catalog;

/// A small ceiling with a generous, effectively-non-firing wait timeout (the
/// tests that need a timeout to fire configure their own tiny value).
fn cfg(k: usize, timeout: Duration) -> AdmissionConfig {
    AdmissionConfig {
        max_concurrent_scans: k,
        wait_timeout: timeout,
    }
}

/// A wait timeout large enough that it never auto-advances during a test that
/// wakes its waiters explicitly (by releasing a held permit / aborting).
const BIG_TIMEOUT: Duration = Duration::from_secs(3600);

/// The `do_get` ticket bytes for the shared in-process table.
fn ticket_bytes() -> Vec<u8> {
    FlightTicket {
        keyspace: KS.into(),
        table: TBL.into(),
        ddl: SIMPLE_DDL.into(),
        ..Default::default()
    }
    .to_bytes()
    .unwrap()
}

fn do_get_request() -> Request<Ticket> {
    Request::new(Ticket::new(ticket_bytes()))
}

/// Build a service over `n` in-process rows sharing `admission`. Runs OUTSIDE any
/// async runtime (`build_sstables` drives its own), so callers invoke this before
/// entering [`block_on_paused`].
fn build_service(admission: Admission, n: i32) -> (tempfile::TempDir, CqliteFlightService) {
    let schema = simple_schema();
    let rows: Vec<_> = (0..n)
        .map(|i| write_row(i, &format!("n{i}"), i * 10, 100))
        .collect();
    let (temp, data_dir, _dir) = build_sstables(&schema, vec![rows]);
    (
        temp,
        CqliteFlightService::with_admission(data_dir, 1024, admission),
    )
}

/// Drive `fut` on a fresh current-thread runtime with the Tokio clock PAUSED, so
/// the permit-wait timeout auto-advances deterministically (no real sleep). Must
/// be called with `fut` constructed OUTSIDE any other runtime.
fn block_on_paused<F: std::future::Future>(fut: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        tokio::time::pause();
        fut.await
    })
}

/// Let every currently-spawned task run up to its first `.await` park. `yield_now`
/// keeps the yielding task ready, so the runtime never idles onto a timer — the
/// paused clock cannot auto-advance during this loop.
async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

async fn decode(stream: <CqliteFlightService as FlightService>::DoGetStream) -> Vec<RecordBatch> {
    let mapped = stream.map(|r| r.map_err(|s| FlightError::ExternalError(Box::new(s))));
    let mut rb = FlightRecordBatchStream::new_from_flight_data(mapped);
    let mut out = Vec::new();
    while let Some(batch) = rb.next().await {
        out.push(batch.unwrap());
    }
    out
}

// ---- Requirement 1: do_get concurrency is bounded by the configured limit ----

/// roborev-1696: an UNCONTENDED acquire (a permit is immediately available) must
/// NOT be counted as waiting even transiently — the `waiting` gauge is a genuine
/// backpressure signal (requests parked in the wait queue), not an artifact of
/// how `acquire` is implemented. Also asserts an instant admit records no
/// permit-wait histogram sample (the fast path is a distinct, zero-noise path
/// from the slow/contended one).
#[test]
fn req1_uncontended_acquire_never_touches_waiting_gauge() {
    block_on_paused(async {
        let adm = Admission::new(cfg(4, BIG_TIMEOUT));
        let before = adm.snapshot();
        assert_eq!(before.waiting, 0);
        assert_eq!(before.in_use, 0);

        // Four uncontended acquires — the ceiling is never saturated.
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(adm.acquire().await.unwrap());
        }

        let after = adm.snapshot();
        assert_eq!(after.in_use, 4, "all four admitted");
        assert_eq!(
            after.waiting, 0,
            "an uncontended acquire must never register as waiting, even transiently"
        );
        assert_eq!(
            after.wait_samples, before.wait_samples,
            "an instant admit records no permit-wait histogram sample"
        );
        drop(held);
    });
}

/// roborev-1696: the CONTENDED path is unchanged — a request that genuinely waits
/// (the ceiling is saturated) still shows up in the `waiting` gauge and still
/// records a permit-wait sample once admitted.
#[test]
fn req1_contended_acquire_path_unchanged() {
    block_on_paused(async {
        let adm = Admission::new(cfg(1, BIG_TIMEOUT));
        let held = adm.acquire().await.unwrap(); // uncontended — fast path
        assert_eq!(adm.snapshot().waiting, 0, "the first acquire never waits");

        let before = adm.snapshot();
        let mut waiter = Box::pin(adm.acquire());
        assert!(matches!(futures::poll!(waiter.as_mut()), Poll::Pending));
        assert_eq!(
            adm.snapshot().waiting,
            1,
            "the contended acquire IS counted as waiting"
        );

        drop(held); // free the only permit — admits the waiter
        let admitted = match futures::poll!(waiter.as_mut()) {
            Poll::Ready(Ok(permit)) => permit,
            other => panic!("the waiter should be admitted once the permit frees, got {other:?}"),
        };
        let after = adm.snapshot();
        assert_eq!(after.waiting, 0, "admission clears the waiting gauge");
        assert!(
            after.wait_samples > before.wait_samples,
            "a genuinely-contended acquire still records a permit-wait sample"
        );
        drop(admitted);
    });
}

/// Scenario: offering more than `K` concurrent acquires holds in-flight at `K`.
/// Primitive-level: all `K` permits held, `M` excess futures polled once each stay
/// Pending; the in-use gauge never exceeds `K` and waiting reads `M`.
#[test]
fn req1_bounded_admission_holds_in_flight_at_k() {
    block_on_paused(async {
        let adm = Admission::new(cfg(2, BIG_TIMEOUT));
        let p1 = adm.acquire().await.unwrap();
        let p2 = adm.acquire().await.unwrap();
        assert_eq!(adm.snapshot().in_use, 2, "K permits admitted");

        // M = 3 excess acquires; poll each once — they park, never admitted.
        let mut excess: Vec<_> = (0..3).map(|_| Box::pin(adm.acquire())).collect();
        for f in excess.iter_mut() {
            assert!(matches!(futures::poll!(f.as_mut()), Poll::Pending));
        }
        let s = adm.snapshot();
        assert_eq!(
            s.in_use, 2,
            "in-use never exceeds K while the barrier is held"
        );
        assert_eq!(s.waiting, 3, "all M excess are parked waiting");

        drop(excess);
        assert_eq!(
            adm.snapshot().waiting,
            0,
            "dropping waiters clears the gauge"
        );
        drop((p1, p2));
    });
}

/// Scenario: releasing exactly one permit admits exactly one waiter — no
/// over-admission, no lost wakeup.
#[test]
fn req1_releasing_one_permit_admits_exactly_one_waiter() {
    block_on_paused(async {
        let adm = Admission::new(cfg(1, BIG_TIMEOUT));
        let held = adm.acquire().await.unwrap();
        let mut a = Box::pin(adm.acquire());
        let mut b = Box::pin(adm.acquire());
        assert!(matches!(futures::poll!(a.as_mut()), Poll::Pending));
        assert!(matches!(futures::poll!(b.as_mut()), Poll::Pending));
        assert_eq!(adm.snapshot().waiting, 2);

        drop(held); // free exactly one permit

        // FIFO: the first waiter is admitted; the second stays parked.
        let first = futures::poll!(a.as_mut());
        let second = futures::poll!(b.as_mut());
        let admitted = match first {
            Poll::Ready(Ok(permit)) => permit,
            Poll::Ready(Err(_)) => panic!("first waiter errored unexpectedly"),
            Poll::Pending => panic!("first waiter should be admitted after a permit freed"),
        };
        assert!(
            matches!(second, Poll::Pending),
            "the second waiter must NOT be admitted (no over-admission)"
        );
        let s = adm.snapshot();
        assert_eq!(s.in_use, 1, "exactly one admitted");
        assert_eq!(s.waiting, 1, "one still waiting");
        drop((admitted, b));
    });
}

/// Red-on-main proof (Scenario 1's do_get clause): with all `K` permits held, the
/// `K + M` offered `do_get`s never reach the filesystem resolve (`do_get_resolve`)
/// — the `resolves` work counter, bumped only inside `resolve_dir`, stays flat.
/// Remove the acquire-before-resolve wiring and every request runs the resolve,
/// jumping the counter: the test reds. (Ticket validation itself — parse +
/// schema/predicate build — deliberately runs BEFORE this gate; see
/// `req_malformed_ticket_bypasses_admission_entirely`.)
#[test]
fn req1_excess_do_gets_never_reach_setup() {
    let adm = Admission::new(cfg(2, BIG_TIMEOUT));
    let (_temp, svc) = build_service(adm.clone(), 8);
    block_on_paused(async move {
        // Hold ALL K permits via the barrier.
        let h1 = adm.acquire().await.unwrap();
        let h2 = adm.acquire().await.unwrap();
        let baseline = svc.setup_work().resolves;

        // Offer K + M = 4 do_gets; each parks on acquire before opening anything.
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let svc = svc.clone();
            tasks.push(tokio::spawn(
                async move { svc.do_get(do_get_request()).await },
            ));
        }
        settle().await;

        assert_eq!(
            svc.setup_work().resolves,
            baseline,
            "no excess request reached the filesystem resolve while all K permits are held"
        );
        let s = adm.snapshot();
        assert_eq!(s.in_use, 2, "only the K barrier permits are held");
        assert_eq!(
            s.waiting, 4,
            "all K + M offered requests are parked in admission"
        );

        for t in tasks {
            t.abort();
        }
        drop((h1, h2));
    });
}

/// roborev-1698: a MALFORMED ticket (fails validation with no filesystem access)
/// must bypass admission entirely — it can never succeed no matter how many times
/// it is retried, so it must fail with its OWN status (`INVALID_ARGUMENT`)
/// immediately: never wait behind the admission semaphore (even with every permit
/// held), never `UNAVAILABLE` (which would make the connector failover-retry an
/// unsatisfiable request into a poison retry storm), and never consume/contend
/// for a permit (`rejected_total` and the waiting gauge stay exactly where they
/// started).
#[test]
fn req_malformed_ticket_bypasses_admission_entirely() {
    let adm = Admission::new(cfg(1, BIG_TIMEOUT));
    let (_temp, svc) = build_service(adm.clone(), 8);
    block_on_paused(async move {
        // Hold the ONLY permit — every real do_get would have to wait for it.
        let held = adm.acquire().await.unwrap();
        let before = adm.snapshot();

        // Malformed ticket bytes: not even valid JSON, so `FlightTicket::from_bytes`
        // fails before admission is ever consulted.
        let malformed = Request::new(Ticket::new(b"not a valid flight ticket".to_vec()));
        let err = svc
            .do_get(malformed)
            .await
            .err()
            .expect("a malformed ticket must error");

        assert_eq!(
            err.code(),
            Code::InvalidArgument,
            "a malformed ticket fails validation, never waits for admission, got: {err:?}"
        );
        assert_ne!(
            err.code(),
            Code::Unavailable,
            "validation failures are not shed as UNAVAILABLE — they can never succeed on retry"
        );

        let after = adm.snapshot();
        assert_eq!(
            after.waiting, before.waiting,
            "the malformed ticket never registered as waiting"
        );
        assert_eq!(
            after.rejected_total, before.rejected_total,
            "the malformed ticket never contended for (or was shed by) admission"
        );
        assert_eq!(after.in_use, before.in_use, "the held permit is untouched");
        drop(held);
    });
}

// ---- Requirement 2: sustained overload sheds with UNAVAILABLE ----

/// Scenario: a request that cannot get a permit within the timeout is rejected
/// `UNAVAILABLE` (never `RESOURCE_EXHAUSTED`, never `OK`), zero batches delivered,
/// `rejected_total += 1`.
#[test]
fn req2_overload_rejects_unavailable_after_timeout() {
    let adm = Admission::new(cfg(1, Duration::from_secs(30)));
    let (_temp, svc) = build_service(adm.clone(), 8);
    block_on_paused(async move {
        let held = adm.acquire().await.unwrap(); // take the only permit
        let before = adm.snapshot().rejected_total;

        // The paused clock auto-advances the 30s wait deadline while the barrier
        // is held → the do_get is shed before any batch.
        let err = svc
            .do_get(do_get_request())
            .await
            .err()
            .expect("saturated do_get must be rejected, not admitted");
        assert_eq!(
            err.code(),
            Code::Unavailable,
            "reject status must be UNAVAILABLE"
        );
        assert_ne!(
            err.code(),
            Code::ResourceExhausted,
            "RESOURCE_EXHAUSTED would defeat the connector's #2241 failover"
        );
        assert_eq!(
            adm.snapshot().rejected_total,
            before + 1,
            "rejected_total increments by exactly one"
        );
        drop(held);
    });
}

/// Scenario: a short burst is absorbed by the wait — a permit frees before the
/// timeout, so the request is admitted with `OK` and `rejected_total` is unchanged.
#[test]
fn req2_short_burst_absorbed_without_rejection() {
    let adm = Admission::new(cfg(1, BIG_TIMEOUT));
    let (_temp, svc) = build_service(adm.clone(), 8);
    block_on_paused(async move {
        let held = adm.acquire().await.unwrap();
        let before_reject = adm.snapshot().rejected_total;

        let svc2 = svc.clone();
        let handle = tokio::spawn(async move { svc2.do_get(do_get_request()).await });
        settle().await;
        assert_eq!(adm.snapshot().waiting, 1, "the excess request is waiting");

        drop(held); // free a permit BEFORE the timeout — the burst is absorbed
        let resp = handle
            .await
            .unwrap()
            .expect("the waiter must be admitted, not rejected");
        let batches = decode(resp.into_inner()).await;
        assert!(total_rows(&batches) > 0, "admitted scan returns its rows");
        assert_eq!(
            adm.snapshot().rejected_total,
            before_reject,
            "no rejection on an absorbed burst"
        );
    });
}

// ---- Requirement 3: a cancelled/disconnected do_get releases its permit ----

/// Scenario: dropping every admitted stream returns the admission gauge to
/// baseline (zero leaked permits) and a subsequently offered scan is admitted
/// immediately.
#[test]
fn req3_dropping_admitted_streams_returns_gauge_to_baseline() {
    let adm = Admission::new(cfg(2, BIG_TIMEOUT));
    let (_temp, svc) = build_service(adm.clone(), 8);
    block_on_paused(async move {
        let baseline = adm.snapshot().in_use;
        let mut streams = Vec::new();
        for _ in 0..2 {
            let resp = svc.do_get(do_get_request()).await.unwrap();
            let mut stream = resp.into_inner();
            let _first = stream.next().await; // consume the first message
            streams.push(stream);
        }
        assert_eq!(adm.snapshot().in_use, 2, "K admitted, in-flight");

        drop(streams); // client disconnect on all K
        assert_eq!(
            adm.snapshot().in_use,
            baseline,
            "gauge returns to baseline — zero leaked permits"
        );

        // A new scan is admitted immediately (a permit is free again).
        let resp = svc.do_get(do_get_request()).await.unwrap();
        assert_eq!(adm.snapshot().in_use, 1, "the freed permit re-admits");
        drop(resp);
    });
}

/// Scenario: a request cancelled while waiting for a permit never holds one — the
/// waiting gauge returns to zero and the in-use gauge is unchanged.
#[test]
fn req3_cancel_while_waiting_never_acquires() {
    block_on_paused(async {
        let adm = Admission::new(cfg(1, BIG_TIMEOUT));
        let held = adm.acquire().await.unwrap();
        let mut waiter = Box::pin(adm.acquire());
        assert!(matches!(futures::poll!(waiter.as_mut()), Poll::Pending));
        assert_eq!(adm.snapshot().waiting, 1);

        drop(waiter); // cancelled while waiting (client disconnect before admission)

        let s = adm.snapshot();
        assert_eq!(s.waiting, 0, "the cancelled waiter released its wait slot");
        assert_eq!(s.in_use, 1, "it never acquired — in-use unchanged");
        drop(held);
    });
}

// ---- Requirement 4: the admission limit is a real, wired configuration knob ----

/// Scenario: a configured limit `K` bounds admitted concurrency to `K`,
/// demonstrated for two distinct `K` so the value provably flows to the ceiling.
#[test]
fn req4_configured_k_bounds_admitted_concurrency() {
    for k in [1usize, 3usize] {
        let adm = Admission::new(cfg(k, BIG_TIMEOUT));
        let (_temp, svc) = build_service(adm.clone(), 8);
        block_on_paused(async move {
            assert_eq!(adm.limit(), k, "configured K flows to the ceiling");

            // Admit K do_gets; hold their (unconsumed) response streams so the
            // permits stay in use.
            let mut held = Vec::new();
            for _ in 0..k {
                held.push(svc.do_get(do_get_request()).await.unwrap());
            }
            assert_eq!(adm.snapshot().in_use as usize, k, "in-use == configured K");

            // One more request cannot be admitted — it waits.
            let svc2 = svc.clone();
            let excess = tokio::spawn(async move { svc2.do_get(do_get_request()).await });
            settle().await;
            let s = adm.snapshot();
            assert_eq!(
                s.in_use as usize, k,
                "in-use bounded by exactly the configured K={k}, not a constant"
            );
            assert!(s.waiting >= 1, "the excess request waits");

            excess.abort();
            drop(held);
        });
    }
}

/// roborev-1697: a `--max-concurrent-scans` value ABOVE `Semaphore::MAX_PERMITS`
/// must construct cleanly with a clamped ceiling, never panic (`Semaphore::new`
/// panics above that bound) — a bad operator-supplied config must fail
/// gracefully, never crash startup.
#[test]
fn req4_absurd_configured_k_clamps_instead_of_panicking() {
    let absurd = tokio::sync::Semaphore::MAX_PERMITS + 1_000_000;
    let adm = Admission::new(cfg(absurd, BIG_TIMEOUT));
    assert_eq!(
        adm.limit(),
        tokio::sync::Semaphore::MAX_PERMITS,
        "an out-of-range K is clamped to Semaphore::MAX_PERMITS, not honoured verbatim"
    );
}

/// Scenario: the permit-wait timeout is honoured as configured — a different
/// budget yields a correspondingly different reject point (logical time under the
/// paused clock; no real sleep).
#[test]
fn req4_permit_wait_timeout_is_wired() {
    fn reject_after(budget: Duration) -> Duration {
        block_on_paused(async move {
            let adm = Admission::new(cfg(1, budget));
            let _held = adm.acquire().await.unwrap(); // occupy the only permit
            let start = tokio::time::Instant::now();
            let err = adm.acquire().await.unwrap_err();
            assert_eq!(err.code(), Code::Unavailable);
            start.elapsed()
        })
    }

    // The paused clock advances to the deadline (tokio's timer granularity rounds
    // up to the next millisecond), so assert a tight window at the configured
    // budget rather than exact equality — and that a longer budget provably waits
    // longer (the knob is wired, not decorative).
    let slack = Duration::from_millis(50);
    let short = reject_after(Duration::from_secs(5));
    let long = reject_after(Duration::from_secs(30));
    assert!(
        short >= Duration::from_secs(5) && short < Duration::from_secs(5) + slack,
        "reject point tracks the configured 5s budget, got {short:?}"
    );
    assert!(
        long >= Duration::from_secs(30) && long < Duration::from_secs(30) + slack,
        "reject point tracks the configured 30s budget, got {long:?}"
    );
    assert!(
        long > short,
        "a longer configured budget yields a later reject point"
    );
}

// ---- Requirement 5: admission state is exported as observability instruments ----

/// Scenario: admission gauges/counters track engagement deterministically, and are
/// distinct names from `cqlite.rpc.in_flight`.
#[test]
fn req5_admission_instruments_track_engagement() {
    // Distinct instrument names from the WS2 RPC gauge (no double counting).
    for name in [
        catalog::FLIGHT_ADMISSION_LIMIT,
        catalog::FLIGHT_ADMISSION_IN_USE,
        catalog::FLIGHT_ADMISSION_WAITING,
        catalog::FLIGHT_ADMISSION_REJECTED_TOTAL,
        catalog::FLIGHT_ADMISSION_WAIT_SECONDS,
    ] {
        assert_ne!(
            name,
            catalog::RPC_IN_FLIGHT,
            "distinct from the RPC in-flight gauge"
        );
        assert!(
            catalog::ALL_METRICS.contains(&name),
            "{name} registered in the catalog"
        );
    }

    block_on_paused(async {
        let adm = Admission::new(cfg(2, Duration::from_secs(10)));
        let p1 = adm.acquire().await.unwrap();
        let p2 = adm.acquire().await.unwrap();
        assert_eq!(adm.snapshot().in_use, 2, "in-use reads K");

        // A waiting reading (M made to wait), then release it.
        let mut waiter = Box::pin(adm.acquire());
        assert!(matches!(futures::poll!(waiter.as_mut()), Poll::Pending));
        assert_eq!(
            adm.snapshot().waiting,
            1,
            "waiting reads the current wait count"
        );
        drop(waiter);

        // One wait times out: rejection counter increments by one and a
        // permit-wait sample is recorded.
        let before = adm.snapshot();
        let err = adm.acquire().await.unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);
        let after = adm.snapshot();
        assert_eq!(
            after.in_use, 2,
            "the timeout admitted nothing — in-use still K"
        );
        assert_eq!(after.waiting, 0, "the timed-out waiter released its slot");
        assert_eq!(
            after.rejected_total,
            before.rejected_total + 1,
            "one rejection"
        );
        assert!(
            after.wait_samples > before.wait_samples,
            "the permit-wait histogram recorded a sample"
        );
        drop((p1, p2));
    });
}

// ---- Requirement 6: admission does not alter the result of an admitted scan ----

/// Scenario: an admitted-after-wait result equals the unbounded result — identical
/// rows, order, schema, and batch boundaries.
#[test]
fn req6_admitted_after_wait_equals_unbounded_result() {
    // Unbounded path: a generous ceiling, admitted immediately.
    let adm_unbounded = Admission::new(cfg(64, BIG_TIMEOUT));
    let (_t1, svc_unbounded) = build_service(adm_unbounded, 8);
    let unbounded = block_on_paused(async move {
        let resp = svc_unbounded.do_get(do_get_request()).await.unwrap();
        decode(resp.into_inner()).await
    });

    // Admission path: K = 1, admitted only after waiting for a permit to free.
    let adm = Admission::new(cfg(1, BIG_TIMEOUT));
    let (_t2, svc) = build_service(adm.clone(), 8);
    let admitted = block_on_paused(async move {
        let held = adm.acquire().await.unwrap();
        let svc2 = svc.clone();
        let handle = tokio::spawn(async move { svc2.do_get(do_get_request()).await });
        settle().await;
        assert_eq!(adm.snapshot().waiting, 1, "the scan waits for a permit");
        drop(held); // admit it after the wait
        let resp = handle.await.unwrap().expect("admitted after waiting");
        decode(resp.into_inner()).await
    });

    assert!(!unbounded.is_empty(), "the fixture returns rows");
    assert_eq!(
        unbounded, admitted,
        "admission is transparent: identical rows, order, schema, and batch boundaries"
    );
}
