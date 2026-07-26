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
//! count (e.g. `egress_budget::tests::concurrent_begin_merge_shrinks…`) — the
//! #2451 flake class. Equality + the [`egress_budget`](super::egress_budget)
//! unit tests (`capacity_for(1) == 256`, shrink-under-concurrency) together
//! establish the "solo K-way merge = 256 per source" contract (AC#1) without a
//! wall-clock/shared-global race.

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
        (egress_budget::MIN_CAP..=egress_budget::MAX_CAP).contains(&first),
        "{site}: shared cap {first} must be within [{}, {}]",
        egress_budget::MIN_CAP,
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
/// the RAII guard's decrement fires on a real merger drop (not just the
/// private-atomic pairing test). Retried to tolerate transient ambient merges
/// from parallel tests: a genuine missing-decrement leak elevates the count
/// PERMANENTLY, so it would fail EVERY attempt, whereas ambient noise clears in
/// some quiet window.
#[test]
fn real_merger_drop_returns_its_active_merge_slot() {
    let temp = TempDir::new().expect("temp dir");
    let mut engine = WriteEngine::new(config_for(&temp)).expect("engine");
    let paths = flush_n_sstables_sync(&mut engine, 1);
    let schema = create_test_schema();

    let returned = (0..64).any(|_| {
        let before = egress_budget::active_count();
        {
            let _m = KWayMerger::new_cancellable(paths.clone(), &schema, ScanCancel::default())
                .expect("merger builds");
            // While alive the merger holds its slot (our +1, plus any ambient).
            assert!(egress_budget::active_count() > before || before > 0);
        }
        // After drop our contribution is gone; in a quiet window the count is
        // back at (or below) the pre-build baseline.
        egress_budget::active_count() <= before
    });
    assert!(
        returned,
        "a real KWayMerger drop must return its active-merge slot (no leak)"
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
        (egress_budget::MIN_CAP..=egress_budget::MAX_CAP).contains(&first),
        "shared cap {first} within clamp"
    );
    assert!(
        merger._egress_slot.is_some(),
        "a channel-backed point read must occupy exactly one active-merge slot"
    );
}
