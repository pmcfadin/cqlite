//! Issue #2370 — N concurrent COLD `do_get`s must NOT amplify reader opens with
//! N (the #2383 single-flight pin extended through the rpc layer under
//! concurrency).
//!
//! The #2383 resolve-phase spin (a `do_get` re-parsing the same generation's full
//! `Index.db` repeatedly) was pinned SEQUENTIALLY. The field trigger is N
//! CONCURRENT queries against one server: if the warm registry's single-flight
//! open fails to coalesce concurrent cold opens, each racing request re-opens
//! (and, pre-#2412, re-parsed) every generation, so the work-done total climbs
//! toward `#generations × N` (the 8× amplifier the field saw). This test fires N
//! concurrent cold `do_get`s at ONE service over the SAME table and reads back
//! the warm registry's `reader_opens` counter, pinning that the total is bounded
//! by a small per-generation CONSTANT — INDEPENDENT of N — proving the concurrent
//! opens single-flighted.
//!
//! ## Re-anchored for issue #2412 (lazy Summary-guided BIG open)
//!
//! This test originally read back `cqlite.sstable.index_parses_total` (one
//! increment per full `Index.db` parse) as BOTH the "cold real work happened"
//! lower bound AND the "single-flighted, not ×N" upper bound. Since #2412 Stage
//! 2, a cold open over a usable `Summary.db` (this fixture's writer-produced
//! shape) performs ZERO full `Index.db` parses at open (deferred, lazy) — so
//! `index_parses_total` reads `0` for BOTH a correctly single-flighted run AND a
//! broken (un-coalesced) one, and can no longer discriminate them. The
//! single-flight property is proven instead via `reader_opens` (the warm
//! registry's real-open work-done counter, `WarmMetricsSnapshot`): a
//! single-flighted cold run opens ONE reader PER generation (never ×N); a broken
//! one opens up to N × generations. `index_parses_total == 0` is retained as a
//! documentary assertion of the #2412 improvement, not the discriminating pin.
//!
//! ## Separate integration-test process
//!
//! The OTel capture harness installs a PROCESS-GLOBAL meter provider, so this file
//! holds exactly one `#[test]` in its own binary (matching the #2383/#2163
//! precedent) and never shares cqlite-flight's parallel `--lib` unit-test binary.
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --features observability-testing \
//!   --test issue_2370_single_flight_test
//! ```

#![cfg(feature = "observability-testing")]

use std::sync::Arc;

use tokio::sync::Barrier;

use cqlite_core::observability::{catalog, testing};
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::test_fixtures as fx;

mod concurrent_support;
use concurrent_support as support;

/// Number of simultaneous cold requests. Field runs use 8.
const N: usize = 8;

/// Count the `nb-*-big-Data.db` generations under the fixture's table dir.
fn generation_count(data_dir: &std::path::Path) -> usize {
    let table_dir = data_dir.join(fx::KEYVALUE_KS).join(fx::KEYVALUE_TBL);
    std::fs::read_dir(&table_dir)
        .expect("table dir")
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .count()
}

#[test]
fn n_concurrent_cold_do_gets_do_not_amplify_index_parses_with_n() {
    // Install the process-global in-memory meter BEFORE any parse in this process.
    let mc = testing::metrics_capture();

    let total = 40usize;
    let (_temp, data_dir) = support::build_multi_sstable_fixture(total);
    let generations = generation_count(&data_dir);
    // `>= 2` (not `== 2`) is DELIBERATE (roborev job 1657 LOW, declined with
    // evidence): the bound below is generation-RELATIVE, and the single-flight
    // property under test is N-independence — the fail signal is
    // `generations × N`, which stays far above the bound for any N>2 REGARDLESS
    // of the exact generation count. An extra generation widens the
    // per-generation-constant bound proportionally but never weakens the
    // N-scaling rejection, so pinning the count exactly would only couple this
    // test to `build_multi_sstable_fixture`'s internals for no gain in
    // discrimination. The `>= 2` floor is the meaningful precondition (≥2
    // generations is what makes cold-open coalescing observable at all).
    assert!(
        generations >= 2,
        "fixture must hold ≥2 generations to make single-flight meaningful, got {generations}"
    );

    let svc = CqliteFlightService::new(data_dir, 8192);
    // Issue #2412 re-anchor: `CqliteFlightService` is cheaply `Clone` (an `Arc`
    // handle over the shared warm registry), so a clone kept OUTSIDE the copy
    // moved into the server lets this test read `warm_metrics()` after the
    // concurrent run — both clones drive the SAME underlying registry.
    let svc_handle = svc.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let parses = rt.block_on(async move {
        // Serve the ONE cold service over real loopback gRPC (roborev job 1659
        // MEDIUM): all N concurrent cold requests traverse the actual tonic
        // transport + server-side handler before we sample the work-done counters,
        // so the single-flight coalescing is proven through the REAL rpc path (a
        // per-connection registry regression would amplify here but be invisible to
        // a direct in-process `svc.do_get()` that hand-shares one `Arc`). The one
        // `FlightServiceServer` instance shares its reader registry across every
        // connection, exactly as production does.
        let running = support::start_server(svc).await;
        let addr = running.addr;

        // Reset AFTER the server binds but BEFORE any request: `start_server` opens
        // no readers (they open lazily on first `do_get`) and `connect` performs no
        // parse, so only the cold-open work from the N concurrent do_gets below is
        // counted. metrics_capture is process-global, so the server-side parses
        // (same process, spawned task) are captured.
        mc.reset();

        // Shared start barrier (roborev job 1656 MEDIUM): without a rendezvous the
        // first request can warm the registry before the rest overlap, so a broken
        // (un-coalesced) implementation could still pass. Every client connects,
        // then blocks here until all N are ready, then they issue their cold
        // `do_get` together — making the concurrent cold-open coalescing genuine.
        let start = Arc::new(Barrier::new(N));
        let mut handles = Vec::new();
        for _ in 0..N {
            let start = start.clone();
            handles.push(tokio::spawn(async move {
                let mut client = support::connect(addr).await;
                start.wait().await;
                let batches = support::do_get_batches(&mut client, support::scan_ticket()).await;
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            }));
        }
        for h in handles {
            let rows = h.await.expect("concurrent do_get task panicked");
            assert!(
                rows > 0,
                "each concurrent cold do_get must stream its rows over transport"
            );
        }

        running.server.abort();

        // Sample the parse counter INSIDE the block (metrics_capture is moved in so
        // the moved-in `svc` and the meter share one process): the total cold-open
        // parses across all N concurrent requests over real transport.
        mc.flush_and_collect()
            .counter_sum(catalog::INDEX_PARSES_TOTAL)
    });

    // Documentary assertion of the #2412 improvement (see the module doc): a
    // usable Summary.db means cold opens never full-parse Index.db at all.
    assert_eq!(
        parses, 0.0,
        "cold opens over a usable Summary.db must full-parse Index.db ZERO times \
         (issue #2412 lazy open); got {parses}"
    );

    // THE single-flight pin, re-anchored on `reader_opens` (see module doc):
    // `index_parses_total` reads 0 regardless of coalescing for this fixture
    // shape, so the discriminating counter is the warm registry's real-open
    // work-done metric instead.
    let reader_opens = svc_handle.warm_metrics().reader_opens;

    // Lower bound: a cold read really opened every generation's reader at least
    // once (never a 0-open skip that would mean a generation went unread).
    assert!(
        reader_opens >= generations as u64,
        "N={N} concurrent COLD do_gets over {generations} generations must open each \
         generation's reader at least once (cold real work), got {reader_opens}"
    );

    // The total is a small per-generation CONSTANT, INDEPENDENT of N. A
    // single-flight failure (the #2383 ×N amplifier through the rpc layer) makes
    // each of the N requests open every generation itself, so the total climbs
    // toward `#generations × N` (here up to 16) — far past this bound.
    let bound = generations as u64;
    assert!(
        reader_opens <= bound,
        "N={N} concurrent COLD do_gets over {generations} generations must NOT amplify reader \
         opens with N: expected ≤ {bound} (a per-generation constant), got {reader_opens}. A \
         value scaling toward {} means the concurrent opens did not single-flight (the #2383 \
         amplifier through the rpc layer).",
        generations * N
    );
}
