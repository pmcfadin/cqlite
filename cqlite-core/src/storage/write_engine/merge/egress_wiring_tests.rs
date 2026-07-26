//! End-to-end wiring evidence for the adaptive egress budget (issue #2765).
//!
//! These tests build REAL `KWayMerger`s over REAL flushed SSTables and observe,
//! per source channel, the exact `sync_channel` capacity the production
//! constructor threaded in (via the `#[cfg(test)]`
//! [`SSTableRowIterator::egress_channel_capacity`](super::SSTableRowIterator::egress_channel_capacity)
//! hook — the exact argument passed to `sync_channel`). They prove:
//!
//! 1. The capacity is keyed per k-way MERGE, not per source: ALL `K` source
//!    channels of one merge share ONE snapshot, so their observed capacities are
//!    IDENTICAL. The pre-rework per-SOURCE counting made the 9th/10th source of
//!    a single merge see a higher active count and receive a SMALLER cap — the
//!    equality assertion below FAILS on that design. (Immune to ambient
//!    concurrency: a concurrent merge shifts the shared common value but can
//!    never break the per-merge equality.)
//! 2. The snapshot reaches BOTH construction sites — `open` (path-based
//!    compaction/full-scan) AND `open_from_reader` (shared-reader warm scan).
//!    Un-wiring either call site back to a per-source `begin_merge` breaks the
//!    equality and fails these tests.
//!
//! Why equality (not `== 256`): asserting an exact value would race any
//! concurrent test that legitimately drives the process-global active-merge
//! count — the #2451 flake class. Equality here + the shrink-under-concurrency
//! property (proven end-to-end on the REAL global by
//! [`concurrency_drives_real_per_channel_cap_below_max`] below, and
//! deterministically against a PRIVATE atomic by
//! `egress_budget::tests::concurrent_begin_shrinks_per_channel_capacity`)
//! together establish the "solo K-way merge = 256 per source" contract (AC#1)
//! without a wall-clock/shared-global race.

use super::{build_single_partition_merger, egress_budget, KWayMerger};
use crate::platform::Platform;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::write_engine::mutation::PartitionKey;
use crate::storage::write_engine::test_support::{
    create_test_mutation, create_test_schema, flush_n_sstables_sync,
};
use crate::storage::write_engine::{WriteEngine, WriteEngineConfig};
use crate::types::Value;
use crate::Config;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Number of source SSTables per merge. `>= 9` is required to DISTINGUISH the
/// per-merge design from the old per-source one: with per-source counting the
/// 9th source (active == 9) would drop to `capacity_for(9) == 227 < 256`, so its
/// cap would DIFFER from source 0's — breaking the equality assertion. At `K <
/// 9` both designs keep every source at 256 (indistinguishable), so K=10 here.
const K: usize = 10;

fn config_for(temp_dir: &TempDir) -> WriteEngineConfig {
    WriteEngineConfig::new(
        temp_dir.path().join("data"),
        temp_dir.path().join("wal"),
        create_test_schema(),
    )
}

async fn open_reader(path: &std::path::Path) -> SSTableReader {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    SSTableReader::open(path, &config, platform)
        .await
        .expect("open reader")
}

/// The observed per-source egress `sync_channel` capacities of a built merger,
/// one entry per channel-backed run (reaches into the merge module's private
/// `runs`/`reader` — legal from this descendant module).
fn source_caps(merger: &KWayMerger) -> Vec<usize> {
    merger
        .runs
        .iter()
        .filter_map(|r| r.reader.egress_channel_capacity())
        .collect()
}

fn assert_single_shared_snapshot(caps: &[usize], site: &str) {
    assert_eq!(
        caps.len(),
        K,
        "{site}: every one of the {K} source channels must expose its capacity \
         (got {})",
        caps.len()
    );
    let first = caps[0];
    assert!(
        caps.iter().all(|&c| c == first),
        "{site}: all {K} source channels of ONE merge must share ONE capacity \
         snapshot (per-merge keying) — got {caps:?}. A per-SOURCE count would \
         shrink the later sources below the first."
    );
    assert!(
        (egress_budget::min_cap()..=egress_budget::MAX_CAP).contains(&first),
        "{site}: shared cap {first} must be within [{}, {}]",
        egress_budget::min_cap(),
        egress_budget::MAX_CAP
    );
}

/// `open` (path-based) call site: a `K`-way path-based merge shares one adaptive
/// capacity snapshot across all `K` source channels.
#[test]
fn path_based_merge_shares_one_capacity_snapshot() {
    let temp = TempDir::new().expect("temp dir");
    let mut engine = WriteEngine::new(config_for(&temp)).expect("engine");
    let paths = flush_n_sstables_sync(&mut engine, K);
    assert_eq!(paths.len(), K, "precondition: {K} SSTables flushed");
    let schema = create_test_schema();

    let merger =
        KWayMerger::new_cancellable(paths, &schema, ScanCancel::default()).expect("merger builds");
    assert_single_shared_snapshot(&source_caps(&merger), "open (path-based)");
}

/// `open_from_reader` (shared-reader) call site: the reader-based `K`-way merge
/// shares one adaptive capacity snapshot across all `K` source channels.
#[test]
fn reader_based_merge_shares_one_capacity_snapshot() {
    let temp = TempDir::new().expect("temp dir");
    let mut engine = WriteEngine::new(config_for(&temp)).expect("engine");
    let paths = flush_n_sstables_sync(&mut engine, K);
    assert_eq!(paths.len(), K, "precondition: {K} SSTables flushed");
    let schema = create_test_schema();

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let merger = rt.block_on(async {
        let mut readers = Vec::with_capacity(paths.len());
        for path in &paths {
            readers.push(Arc::new(open_reader(path).await));
        }
        KWayMerger::new_from_readers(readers, &schema, ScanCancel::default(), None)
            .expect("reader-based merger builds")
    });
    assert_single_shared_snapshot(&source_caps(&merger), "open_from_reader (shared)");
}

/// Raw partition-key bytes for `id = <id>` under the test schema.
fn key_bytes(id: i32) -> Vec<u8> {
    PartitionKey::single("id", Value::Integer(id))
        .to_bytes(&create_test_schema())
        .expect("encode partition key")
}

/// Flush TWO generations that BOTH contain `id = 0` (newer overwrite), returning
/// them newest-first. Both candidates therefore hold the queried key, so with
/// their index stripped BOTH fall back to `NeedsScan` (neither can prune via a
/// token range) — giving two egress channels that must share ONE snapshot.
fn flush_two_generations_same_key(temp: &TempDir) -> Vec<std::path::PathBuf> {
    let mut engine = WriteEngine::new(config_for(temp)).expect("engine");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    engine
        .write(create_test_mutation(0, "gen1", 1000))
        .expect("write gen1");
    let p1 = rt
        .block_on(engine.flush())
        .expect("flush gen1")
        .expect("gen1 sstable")
        .data_path;
    engine
        .write(create_test_mutation(0, "gen2", 2000))
        .expect("write gen2");
    let p2 = rt
        .block_on(engine.flush())
        .expect("flush gen2")
        .expect("gen2 sstable")
        .data_path;
    vec![p2, p1]
}

/// Delete the `Index.db` + `Summary.db` siblings of a flushed `Data.db` so the
/// point-read probe can no longer seek and must fall back to `NeedsScan` — the
/// only path that opens an egress channel (`SinglePartitionFilterRun`).
fn strip_index_siblings(data_path: &std::path::Path) {
    let name = data_path
        .file_name()
        .and_then(|n| n.to_str())
        .expect("data filename");
    let dir = data_path.parent().expect("data parent");
    for comp in ["Index.db", "Summary.db"] {
        let sib = dir.join(name.replace("Data.db", comp));
        // Best-effort: a component the writer did not emit is simply absent.
        let _ = std::fs::remove_file(sib);
    }
}

/// Baseline-return (Low #3): building then dropping a REAL `KWayMerger` must
/// leave the process-global active-merge count exactly where it started — proof
/// the RAII guard's decrement fires on a real merger drop.
///
/// ONE baseline `before0` is pinned BEFORE the loop and NEVER re-sampled: we run
/// `ATTEMPTS` build/drop cycles, then assert the count has returned to
/// `<= before0`. Every build increments and every drop must decrement, so the
/// NET effect of the whole run is zero — but a PROBABILISTIC leak (say one build
/// in ten whose guard fails to decrement) leaves the count PERMANENTLY elevated
/// at `before0 + leaked`, regardless of WHICH cycle leaked. A prior "re-sample
/// `before` each attempt + count quiet windows" shape absorbed a leaked slot into
/// the next attempt's baseline, so a 1-in-10 leak still passed — this catches it
/// because the single pinned baseline cannot absorb an unpaired decrement.
///
/// Ambient churn from parallel tests is tolerated by a BOUNDED POLL of the final
/// read (a transiently-elevated ambient merge drains within the deadline); our
/// OWN leaked slots never drain, so a leak stays > `before0` and fails. A
/// separate check that some build raised the count above `before0` keeps the
/// registration side honest (guards against a "stopped incrementing" regression
/// that would make build+drop net-zero vacuously).
#[test]
fn real_merger_drop_returns_its_active_merge_slot() {
    let temp = TempDir::new().expect("temp dir");
    let mut engine = WriteEngine::new(config_for(&temp)).expect("engine");
    let paths = flush_n_sstables_sync(&mut engine, 1);
    let schema = create_test_schema();

    const ATTEMPTS: usize = 20;
    let before0 = egress_budget::active_count();
    let mut observed_increment = false;
    for _ in 0..ATTEMPTS {
        let merger = KWayMerger::new_cancellable(paths.clone(), &schema, ScanCancel::default())
            .expect("merger builds");
        // While held, our merger's slot must lift the count above the pinned
        // baseline at least once across the run (registration fires).
        if egress_budget::active_count() > before0 {
            observed_increment = true;
        }
        drop(merger); // channel close + producer join; guard MUST decrement here
    }
    assert!(
        observed_increment,
        "no build ever raised the active-merge count above the baseline \
         ({before0}) — registration (begin_merge increment) appears unwired"
    );

    // Bounded-poll the FINAL count back to the pinned baseline: a transient
    // ambient merge drains within the deadline; an unpaired OUR decrement (a
    // leak on ANY of the {ATTEMPTS} cycles) stays permanently elevated and fails.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut returned = egress_budget::active_count() <= before0;
    while !returned && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
        returned = egress_budget::active_count() <= before0;
    }
    assert!(
        returned,
        "active-merge slot LEAKED: after {ATTEMPTS} build/drop cycles the count \
         ({}) stayed above the pinned pre-loop baseline ({before0}) — a guard's \
         decrement did not fire on at least one drop",
        egress_budget::active_count()
    );
}

/// Lazy registration (blocker): an all-`Seeked` point read builds only in-memory
/// `VecRun`s — ZERO egress channels — so it must occupy NO active-merge slot.
/// Default (`not(tombstones)`) build only: there the probe genuinely seeks; the
/// `tombstones` build always reports `NeedsScan` (a channel), covered below.
#[cfg(not(feature = "tombstones"))]
#[test]
fn seek_only_point_read_registers_no_slot() {
    let temp = TempDir::new().expect("temp dir");
    let mut engine = WriteEngine::new(config_for(&temp)).expect("engine");
    let paths = flush_n_sstables_sync(&mut engine, 1);
    let schema = create_test_schema();
    // id=0 is present in the first flushed batch, so the candidate SEEKS.
    let merger =
        build_single_partition_merger(paths, &[key_bytes(0)], &schema, ScanCancel::default())
            .expect("point-read builder")
            .expect("a present key yields a merger");

    assert!(
        source_caps(&merger).is_empty(),
        "an all-seek point read has NO egress channels"
    );
    assert!(
        merger._egress_slot.is_none(),
        "a channel-less point read must occupy NO active-merge slot (lazy \
         registration): high-QPS seek-only point reads must not throttle a \
         concurrent channel-backed merge"
    );
}

/// Point-read wiring (Low #2): a `NeedsScan` fail-safe point read DOES open
/// egress channels (`SinglePartitionFilterRun`), which must all share ONE
/// snapshot AND register exactly one slot. Forces `NeedsScan` by stripping the
/// index siblings so the probe cannot seek. Exercises the point-read seam's
/// snapshot-outside-constructor + `with_egress_slot` re-attach, and the
/// `SinglePartitionFilterRun::egress_channel_capacity` delegate.
#[test]
fn needs_scan_point_read_shares_snapshot_and_registers_one_slot() {
    let temp = TempDir::new().expect("temp dir");
    // Two candidates that BOTH hold id=0, both forced to NeedsScan (index
    // stripped) → two egress channels that must share ONE snapshot.
    let paths = flush_two_generations_same_key(&temp);
    assert_eq!(paths.len(), 2);
    for p in &paths {
        strip_index_siblings(p);
    }
    let schema = create_test_schema();
    let merger =
        build_single_partition_merger(paths, &[key_bytes(0)], &schema, ScanCancel::default())
            .expect("point-read builder")
            .expect("index-stripped candidates fall back to a scan merger");

    let caps = source_caps(&merger);
    assert_eq!(
        caps.len(),
        2,
        "both NeedsScan candidates must open an egress channel (got {caps:?})"
    );
    let first = caps[0];
    assert!(
        caps.iter().all(|&c| c == first),
        "both point-read fail-safe channels must share ONE snapshot: {caps:?}"
    );
    assert!(
        (egress_budget::min_cap()..=egress_budget::MAX_CAP).contains(&first),
        "shared cap {first} within clamp"
    );
    assert!(
        merger._egress_slot.is_some(),
        "a channel-backed point read must occupy exactly one active-merge slot"
    );
}

/// Low #3 — end-to-end proof the adaptive throttle actually FIRES in production
/// (not just against the private atomic): build and HOLD `budget/MAX_CAP + 1`
/// real `KWayMerger`s so the next merge registers past the 256 clamp, then build
/// one more and assert EVERY one of its source channels got a capacity strictly
/// below `MAX_CAP`. A refactor that made `begin_merge` return `MAX_CAP` while
/// still incrementing `ACTIVE` (silently dead throttle) fails HERE.
///
/// `hold_count` is sized from the RESOLVED (env-overridable) budget — NOT the
/// compile-time constant — because the production path (`capacity_for` →
/// `resolved()`) uses the resolved budget too; a compile-time size would fail
/// under `CQLITE_EGRESS_ROW_BUDGET=8192` (still 9 holds → extra at active=10 →
/// `8192/10` clamps back to 256). With `hold_count = budget/MAX_CAP + 1` OUR own
/// mergers force `active ≥ hold_count`, so the extra's snapshot
/// `capacity_for(≥ hold_count+1) = budget/(budget/MAX_CAP + 2) < MAX_CAP` holds
/// regardless of the resolved budget; ambient merges only raise the count →
/// lower the cap, so `< MAX_CAP` is monotone-safe (cannot flake high).
#[test]
fn concurrency_drives_real_per_channel_cap_below_max() {
    let temp = TempDir::new().expect("temp dir");
    let mut engine = WriteEngine::new(config_for(&temp)).expect("engine");
    let paths = flush_n_sstables_sync(&mut engine, 1);
    let schema = create_test_schema();

    // Sized from the RESOLVED budget, but CLAMPED to 64 so a pathological
    // `CQLITE_EGRESS_ROW_BUDGET` can't spawn thousands of real mergers/FDs. If the
    // clamp defeats the shrink precondition (a huge budget where even a 65th merge
    // stays at the 256 clamp), skip: the property is unobservable within a bounded
    // workload, not violated.
    let hold_count = (egress_budget::budget() / egress_budget::MAX_CAP + 1).min(64);
    if egress_budget::capacity_for(hold_count + 1) >= egress_budget::MAX_CAP {
        return;
    }
    let mut held = Vec::with_capacity(hold_count);
    for _ in 0..hold_count {
        held.push(
            KWayMerger::new_cancellable(paths.clone(), &schema, ScanCancel::default())
                .expect("held merger"),
        );
    }
    // Built while all `hold_count` mergers are still alive (owned by `held`).
    let extra = KWayMerger::new_cancellable(paths.clone(), &schema, ScanCancel::default())
        .expect("extra merger");
    let caps = source_caps(&extra);
    assert!(!caps.is_empty(), "the extra merger has an egress channel");
    assert!(
        caps.iter().all(|&c| c < egress_budget::MAX_CAP),
        "with {hold_count}+ concurrent merges the new merge's per-channel caps \
         must fall below {} — the adaptive throttle firing end-to-end (got \
         {caps:?})",
        egress_budget::MAX_CAP
    );
    drop(held); // keep the held mergers alive until AFTER the assertion above
}
