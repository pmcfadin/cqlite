//! Issue #2370 — N concurrent COLD `do_get`s must NOT amplify Index.db parses
//! with N (the #2383 single-flight pin extended through the rpc layer under
//! concurrency).
//!
//! The #2383 resolve-phase spin (a `do_get` re-parsing the same generation's full
//! `Index.db` repeatedly) was pinned SEQUENTIALLY. The field trigger is N
//! CONCURRENT queries against one server: if the warm registry's single-flight
//! open fails to coalesce concurrent cold opens, each racing request re-parses
//! every generation, so `index_parses_total` climbs toward `#generations × N` (the
//! 8× amplifier the field saw). This test fires N concurrent cold `do_get`s at ONE
//! service over the SAME table and reads back the authoritative
//! `cqlite.sstable.index_parses_total` counter, pinning that the total is bounded
//! by a small per-generation CONSTANT — INDEPENDENT of N — proving the concurrent
//! opens single-flighted.
//!
//! ## Why the bound is `PER_OPEN_PARSES × #generations`, not `#generations`
//!
//! The issue's ideal is `index_parses_total == #generations` (each generation's
//! Index.db parsed once per query — the counter's documented contract in
//! `catalog::INDEX_PARSES_TOTAL`). On current main it is `2 × #generations`:
//! `SSTableReader::open` parses the full `Index.db` TWICE per reader open —
//! `reader/mod.rs` calls BOTH `load_index` (→ legacy `SSTableIndex`) and
//! `load_index_reader` (→ spec `IndexReader`), each running the O(entries)
//! `parse_all_partition_keys_*` loop `index_parses_total` counts. That per-OPEN
//! double-parse is a SEPARATE redundancy (a real finding reported with this suite,
//! independent of concurrency and of the warm registry) — the single-flight this
//! test pins is that the total does NOT scale with N. `PER_OPEN_PARSES` documents
//! the current per-open multiplicity; tighten it to `1` when the reader-open
//! double-parse is fixed (this test stays green through that fix — the bound only
//! rejects a per-REQUEST, N-scaling amplification, never a smaller constant).
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

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::observability::{catalog, testing};
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::test_fixtures as fx;

mod concurrent_support;
use concurrent_support as support;

/// Number of simultaneous cold requests. Field runs use 8.
const N: usize = 8;

/// Current number of full `Index.db` parses `SSTableReader::open` performs PER
/// reader open (see the module doc: `load_index` + `load_index_reader`). The
/// ideal is 1; `2` documents the reported per-open double-parse finding. The pin
/// below is that the TOTAL is `PER_OPEN_PARSES × #generations` — a per-generation
/// constant that does NOT grow with N. Reduce to 1 when the double-parse is fixed.
const PER_OPEN_PARSES: f64 = 2.0;

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

/// Drive one in-process `do_get` and fully drain its stream so every parse fires.
async fn do_get_drain(svc: &CqliteFlightService, ticket: Vec<u8>) -> usize {
    let resp = svc
        .do_get(Request::new(Ticket::new(ticket)))
        .await
        .expect("do_get");
    let mut stream = resp.into_inner();
    let mut msgs = 0usize;
    while let Some(item) = stream.next().await {
        item.expect("stream item ok");
        msgs += 1;
    }
    msgs
}

#[test]
fn n_concurrent_cold_do_gets_do_not_amplify_index_parses_with_n() {
    // Install the process-global in-memory meter BEFORE any parse in this process.
    let mc = testing::metrics_capture();

    let total = 40usize;
    let (_temp, data_dir) = support::build_multi_sstable_fixture(total);
    let generations = generation_count(&data_dir);
    assert!(
        generations >= 2,
        "fixture must hold ≥2 generations to make single-flight meaningful, got {generations}"
    );

    let svc = CqliteFlightService::new(data_dir, 8192);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    mc.reset();
    rt.block_on(async {
        // Fire N COLD do_gets concurrently at the SAME (cold) service. Cloning the
        // service shares the same warm registry via its `Arc`, so a working
        // single-flight coalesces the concurrent opens per generation.
        let mut handles = Vec::new();
        for _ in 0..N {
            let svc = svc.clone();
            handles.push(tokio::spawn(async move {
                do_get_drain(&svc, support::scan_ticket()).await
            }));
        }
        for h in handles {
            let msgs = h.await.expect("concurrent do_get task panicked");
            assert!(
                msgs > 0,
                "each concurrent cold do_get must stream at least a schema message"
            );
        }
    });

    let parses = mc
        .flush_and_collect()
        .counter_sum(catalog::INDEX_PARSES_TOTAL);

    // Lower bound: a cold read really parses every generation at least once (never
    // a 0-parse skip that would mean a generation went unread).
    assert!(
        parses >= generations as f64,
        "N={N} concurrent COLD do_gets over {generations} generations must parse each generation's \
         Index.db at least once (cold real work), got {parses}"
    );

    // THE single-flight pin: the total is a small per-generation CONSTANT,
    // INDEPENDENT of N. A single-flight failure (the #2383 ×N amplifier through the
    // rpc layer) makes each of the N requests re-parse every generation, so the
    // total climbs toward `#generations × N` (here up to 16–32) — far past this
    // bound. `PER_OPEN_PARSES` (currently 2, the reported reader-open double-parse
    // finding) is a per-generation constant; the bound rejects only a per-REQUEST,
    // N-scaling amplification, and stays green if the double-parse is fixed to 1.
    let bound = PER_OPEN_PARSES * generations as f64;
    assert!(
        parses <= bound,
        "N={N} concurrent COLD do_gets over {generations} generations must NOT amplify Index.db \
         parses with N: expected ≤ {bound} (= {PER_OPEN_PARSES} × {generations}, a per-generation \
         constant), got {parses}. A value scaling toward {} means the concurrent opens did not \
         single-flight (the #2383 amplifier through the rpc layer).",
        generations * N
    );
}
