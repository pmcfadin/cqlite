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

use super::{egress_budget, KWayMerger};
use crate::platform::Platform;
use crate::storage::scan_cancel::ScanCancel;
use crate::storage::sstable::reader::SSTableReader;
use crate::storage::write_engine::test_support::{create_test_schema, flush_n_sstables_sync};
use crate::storage::write_engine::{WriteEngine, WriteEngineConfig};
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
