//! In-crate concurrency scenarios for the `ReadAt` point-read migration
//! (issue #1573, Epic C / C2). These live in-crate (not in `tests/`) because they
//! inject a `pub(crate)` [`ReadAt`](super::read_at::ReadAt) test double into the
//! reader's `point_source` — a surface no external crate can reach.
//!
//! Requires `CQLITE_DATASETS_ROOT` + fetched binaries; every test skips (never
//! fails) when its fixture is absent, and never treats 0 rows as a skip.

#![cfg(test)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::read_at::{SerializingReadAt, SleepingReadAt};
use super::SSTableReader;
use crate::{Config, Platform};

/// Locate a `*-Data.db` under `<datasets>/sstables/<keyspace>/<table>-*/`.
/// Returns `None` (skip) when the datasets root or fixture is absent.
fn find_data_db(keyspace: &str, table: &str) -> Option<PathBuf> {
    let root = std::env::var("CQLITE_DATASETS_ROOT").ok()?;
    let ks_dir = PathBuf::from(root).join("sstables").join(keyspace);
    let prefix = format!("{table}-");
    for entry in std::fs::read_dir(&ks_dir).ok()?.flatten() {
        if !entry.file_name().to_string_lossy().starts_with(&prefix) {
            continue;
        }
        for f in std::fs::read_dir(entry.path()).ok()?.flatten() {
            if f.file_name().to_string_lossy().ends_with("-Data.db") {
                return Some(f.path());
            }
        }
    }
    None
}

async fn open_reader(path: &std::path::Path) -> Option<SSTableReader> {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.ok()?);
    SSTableReader::open(path, &config, platform).await.ok()
}

/// Issue every offset concurrently through `read_value_at_offset` on a shared
/// reader and return the wall time. Each offset is distinct so the shared chunk
/// cache does not collapse the reads into one.
async fn concurrent_point_reads(
    reader: Arc<SSTableReader>,
    offsets: &[u64],
    size: u32,
) -> Duration {
    let start = Instant::now();
    let mut handles = Vec::new();
    for &off in offsets {
        let r = Arc::clone(&reader);
        handles.push(tokio::spawn(async move {
            // Result is intentionally ignored: the scenario measures whether the
            // reads SERIALIZE, not their payload (payload correctness is the
            // parity test's job). Every call still traverses the point source.
            let _ = r.read_value_at_offset(off, size).await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    start.elapsed()
}

/// Scenario: 8 concurrent point reads do NOT convoy.
///
/// The reader's `point_source` is wrapped in a 12ms-sleeping [`SleepingReadAt`]
/// (no lock) for the treatment, and in a lock-holding [`SerializingReadAt`] for
/// the control. The control proves the harness CAN serialize (the pre-#1573
/// `Arc<Mutex<BlockSource>>` convoy: ~N×delay); the treatment proves the migrated
/// positional path does NOT (well under 8×delay), because positioned reads on a
/// shared source are independent (`&self`, no cursor mutex).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn eight_concurrent_point_reads_do_not_convoy() {
    let Some(path) = find_data_db("test_basic", "uncompressed_table") else {
        eprintln!("SKIP (#1573 convoy): test_basic/uncompressed_table Data.db absent");
        return;
    };
    let delay = Duration::from_millis(12);
    // 8 distinct offsets (distinct cache keys) so each read reaches the source.
    let offsets: Vec<u64> = (0..8u64).map(|i| i * 96).collect();
    let size = 48u32;

    // --- Control: a lock-holding source serializes (reproduces the convoy). ---
    let control = {
        let mut reader = open_reader(&path).await.expect("open control reader");
        let real = reader.clone_point_source();
        reader.set_point_source(Arc::new(SerializingReadAt::new(real, delay)));
        let reader = Arc::new(reader);
        concurrent_point_reads(reader, &offsets, size).await
    };

    // --- Treatment: the migrated positional source runs the 8 reads in parallel. ---
    let calls = Arc::new(AtomicUsize::new(0));
    let treatment = {
        let mut reader = open_reader(&path).await.expect("open treatment reader");
        let real = reader.clone_point_source();
        reader.set_point_source(Arc::new(SleepingReadAt::new(real, delay, calls.clone())));
        let reader = Arc::new(reader);
        concurrent_point_reads(reader, &offsets, size).await
    };

    // Routing proof (deterministic; this is what is RED on `main`, where
    // get_cached_data/verify_uncompressed_range still lock `self.file` and never
    // touch `point_source`): the point path must reach the injected source.
    assert!(
        calls.load(Ordering::Relaxed) >= offsets.len(),
        "point path must route through `point_source` (>= {} reads); got {} \
         — a 0 here means the point path no longer uses point_source",
        offsets.len(),
        calls.load(Ordering::Relaxed)
    );

    // Parallelism proof, expressed RELATIVE to the measured control (no absolute
    // wall-clock threshold, so it does not flake on a slow/loaded CI host): the
    // serialized control does ~8× the sleeping work, so the parallel treatment
    // must be at least ~2× faster. On `main` (mutex convoy) treatment ≈ control.
    assert!(
        treatment.saturating_mul(2) < control,
        "migrated positional point reads must NOT convoy: parallel treatment {treatment:?} \
         should be far faster than the serialized control {control:?} (>= 2×); \
         near-equal means the reads still serialize"
    );
}

/// Scenario: two concurrent interleaved point reads at different offsets each
/// return their own bytes (no cross-contamination), driven end-to-end through the
/// reader's `read_value_at_offset` on the shared positional source.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_point_reads_return_correct_bytes() {
    let Some(path) = find_data_db("test_basic", "uncompressed_table") else {
        eprintln!("SKIP (#1573 interleave): test_basic/uncompressed_table Data.db absent");
        return;
    };
    let reader = Arc::new(open_reader(&path).await.expect("open reader"));

    // Two disjoint ranges read concurrently many times; each must be internally
    // consistent across repetitions (a shared cursor would interleave them).
    let (off_a, off_b, size) = (0u64, 256u64, 32u32);
    let first_a = reader
        .read_value_at_offset(off_a, size)
        .await
        .expect("read A");
    let first_b = reader
        .read_value_at_offset(off_b, size)
        .await
        .expect("read B");

    let mut handles = Vec::new();
    for _ in 0..50 {
        let (ra, rb) = (Arc::clone(&reader), Arc::clone(&reader));
        let (ea, eb) = (first_a.clone(), first_b.clone());
        handles.push(tokio::spawn(async move {
            let a = ra.read_value_at_offset(off_a, size).await.expect("A");
            let b = rb.read_value_at_offset(off_b, size).await.expect("B");
            assert_eq!(a, ea, "offset A read drifted under concurrency");
            assert_eq!(b, eb, "offset B read drifted under concurrency");
        }));
    }
    for h in handles {
        h.await.expect("task");
    }
}
