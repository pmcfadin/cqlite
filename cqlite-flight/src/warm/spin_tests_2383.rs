//! Issue #2383 RED repros — resolve-phase CPU spin (round-8 field failure).
//!
//! The field failure: Arrow Flight `do_get` against a snapshot with 2 BIG-nb
//! SSTables (~1.58M partitions each) re-parses the FULL `Index.db` for the SAME
//! generation repeatedly (8× "Parsed 1586932 partition entries" for one logical
//! query), pinning tokio workers in the O(entries) `memcmp`/vint parse loop. LIMIT
//! 5, `count(*)`, and PK point-reads all hang; cancellation doesn't stop the spin.
//!
//! These registry-level repros pin the three confirmed mechanisms sharply and
//! scale-free using the warm registry's `reader_opens` work-done probe (each
//! reader open is exactly one full `Index.db` parse):
//!
//! 1. **Rebind across snapshot teardown** — a per-query snapshot dir is cleared
//!    and a NEW dir with the SAME inodes is staged; the warm set must REBIND to
//!    the live path with ZERO further parses, not fully re-open every generation
//!    (fix B; direction named in #2356 "rebind-by-inode").
//! 2. **Single-flight rebuild** — M concurrent misses for ONE fresh key must
//!    coalesce onto ONE rebuild (total opens == #generations), not open ×M
//!    (fix A).
//! 3. **Cancel granularity inside the parse** — cancelling DURING a large
//!    `Index.db` parse must abort promptly, not run the whole parse to completion
//!    (fix C).
//!
//! Loaded via `#[path]` from `registry.rs` (campsite rule) so it can drive the
//! crate-internal test hooks (`set_open_barrier`, `debug_*`, `metrics`).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use arrow::array::{Array, StringArray};
use cqlite_core::schema::TableSchema;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::write_engine::{Durability, WriteEngine, WriteEngineConfig};

use crate::cancel::CancelFlag;
use crate::producer::MergeProducer;
use crate::testutil::{
    build_sstables, make_snapshot, simple_schema, write_row, KS, SIMPLE_DDL, TBL,
};
use crate::warm::{TableKey, WarmError, WarmTableRegistry};

fn key() -> TableKey {
    TableKey::new(KS, TBL)
}

fn ddl() -> u64 {
    crate::warm::ddl_hash(SIMPLE_DDL)
}

/// Decode a warm reader set into its sorted `name` column values through the SAME
/// streaming-merge path `do_get` drives — proves the rebound/rebuilt set reads the
/// LIVE inodes correctly (no ENOENT), not merely that an open succeeded.
fn decode_names(schema: &TableSchema, readers: Vec<Arc<SSTableReader>>) -> Vec<String> {
    let producer = MergeProducer::new(schema.clone(), 1024).expect("producer");
    let batches = producer
        .produce_streaming_from_readers_to_vec(readers, &CancelFlag::new())
        .expect("decode readers");
    let mut out = Vec::new();
    for b in &batches {
        let names = b
            .column_by_name("name")
            .expect("name column")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name is text");
        for i in 0..names.len() {
            out.push(names.value(i).to_string());
        }
    }
    out.sort();
    out
}

/// Build ONE SSTable generation with `n` distinct int-PK partitions so its
/// `Index.db` is large enough that a full parse dominates wall time (the spin
/// loop). A large flush threshold keeps all `n` rows in ONE generation — the sync
/// `write()` path auto-flushes over the default threshold when no runtime is
/// active, which would otherwise split them across many SSTables.
fn build_big_single_gen(schema: &TableSchema, n: i32) -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone())
        .with_flush_threshold(1usize << 30) // 1 GiB: no mid-write auto-flush.
        .with_durability(Durability::Disabled); // bulk-load: skip per-write WAL fsync.
    let mut engine = WriteEngine::new(config).expect("engine");
    for id in 0..n {
        engine.write(write_row(id, "n", id, 100)).expect("write");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("info");
    let table_dir = data_dir.join(&schema.keyspace).join(&schema.table);
    // Prove it really is a SINGLE generation (one -Data.db) so the cancel repro
    // trips DURING one parse, not at a coarse between-generation boundary.
    let data_files = std::fs::read_dir(&table_dir)
        .expect("table dir")
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|f| f.ends_with("-Data.db"))
        })
        .count();
    assert_eq!(
        data_files, 1,
        "the cancel repro needs exactly ONE big generation (found {data_files})"
    );
    (temp, table_dir)
}

/// **Fix B — rebind across snapshot teardown WITHOUT re-parse.** Warm from a
/// per-query snapshot dir; the connector clears it (`clearSnapshot`) and stages a
/// FRESH dir over the SAME underlying inodes (hardlinks). Because the cached
/// readers' paths are now dead, the warm lookup must REBIND them to the live path
/// — zero further `Index.db` parses — instead of fully re-opening every
/// generation. The rebound readers must back LIVE paths (#2352 ENOENT protection)
/// and decode correctly.
///
/// RED on current main: `cached_paths_all_present` returns false for the dead
/// snap1 paths, so `rebuild` lands both generations in `added` and RE-OPENS them
/// from the live dir → `reader_opens` climbs by #generations (measured: +2). The
/// fix rebinds by inode identity → +0. This IS the field spin: every per-query
/// snapshot swap re-parses the whole index.
#[test]
fn rebind_across_snapshot_teardown_does_not_reparse() {
    let schema = simple_schema();
    let (_temp, _data, table_dir) = build_sstables(
        &schema,
        vec![
            vec![write_row(1, "a", 1, 100)],
            vec![write_row(2, "b", 2, 100)],
        ],
    );

    let reg = WarmTableRegistry::new();
    let cancel = CancelFlag::new();

    // Query N: warm from snap1 (readers open with file_path inside snap1).
    let snap1 = make_snapshot(&table_dir, "snap1");
    reg.warm_readers(&key(), ddl(), &schema, &snap1, Some("snap1"), &cancel)
        .expect("first snapshot warms");
    let opens_after_first = reg.metrics().snapshot().reader_opens;
    assert!(opens_after_first >= 2, "both generations parsed cold");

    // Connector clears query N's snapshot; the live inodes in table_dir persist.
    std::fs::remove_dir_all(&snap1).expect("clear query-N snapshot dir");

    // Query N+1: a NEW snapshot dir over the SAME inodes (hardlinks).
    let snap2 = make_snapshot(&table_dir, "snap2");
    let w2 = reg
        .warm_readers(&key(), ddl(), &schema, &snap2, Some("snap2"), &cancel)
        .expect("second snapshot request after teardown");

    // Every returned reader must back a LIVE path (#2352 ENOENT protection kept).
    for r in &w2.readers {
        assert!(
            std::fs::metadata(r.file_path()).is_ok(),
            "rebound reader must back a live path, got dead {}",
            r.file_path().display()
        );
    }
    // Row correctness through the same merge path do_get drives.
    assert_eq!(
        decode_names(&schema, w2.readers),
        vec!["a".to_string(), "b".to_string()],
        "rebound warm set reads the live inodes correctly (no ENOENT)"
    );

    // THE anti-spin assertion: the teardown+restage cost ZERO further full parses.
    // RED today (a full rebuild re-opens both generations → +2).
    assert_eq!(
        reg.metrics().snapshot().reader_opens,
        opens_after_first,
        "a snapshot teardown + same-inode restage must REBIND (zero further \
         Index.db parses), not re-open every generation (issue #2383 / #2356)"
    );
}

/// **Fix A — single-flight rebuild.** M concurrent misses for ONE fresh table key
/// must coalesce onto a single rebuild: total `reader_opens` == #generations, not
/// ×M. A start barrier launches all M threads together and a per-open sleep hook
/// widens the open window so, on the unfixed code, every thread misses and opens
/// its OWN copy of both generations before any swap lands.
///
/// RED on current main: `open_added` has no coalescing and the epoch guard
/// discards losers only AFTER they have already parsed, so M threads × 2
/// generations = up to 16 full `Index.db` parses (measured ≫ 2). The swap-time
/// dedup keeps the CACHE correct (2 readers) but does nothing about the redundant
/// PARSE work — exactly the concurrent-splits arm of the field spin.
#[test]
fn concurrent_misses_single_flight_one_parse_per_generation() {
    const M: usize = 8;
    let schema = simple_schema();
    let (temp, _data, table_dir) = build_sstables(
        &schema,
        vec![
            vec![write_row(1, "a", 1, 100)],
            vec![write_row(2, "b", 2, 100)],
        ],
    );

    let reg = Arc::new(WarmTableRegistry::new());
    // Widen the per-open window so all M threads are concurrently mid-open on the
    // unfixed path (fires only on threads that actually open — no deadlock when a
    // single-flight fix lets just one thread open).
    reg.set_open_barrier(Arc::new(|| thread::sleep(Duration::from_millis(40))));

    let start = Arc::new(Barrier::new(M));
    let handles: Vec<_> = (0..M)
        .map(|_| {
            let reg = Arc::clone(&reg);
            let schema = schema.clone();
            let dir = table_dir.clone();
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                reg.warm_readers(&key(), ddl(), &schema, &dir, None, &CancelFlag::new())
                    .expect("concurrent warm")
                    .readers
                    .len()
            })
        })
        .collect();
    for h in handles {
        assert_eq!(h.join().expect("thread"), 2, "each caller gets both gens");
    }

    // The cache is correct either way (2 distinct generations); the BUG is the
    // redundant parse work.
    assert_eq!(
        reg.debug_distinct_gen_count(&key()),
        2,
        "two generations cached"
    );
    assert_eq!(
        reg.metrics().snapshot().reader_opens,
        2,
        "M={M} concurrent misses for one key must coalesce onto ONE rebuild — \
         exactly #generations (2) full Index.db parses, not ×M (issue #2383)"
    );
    drop(temp);
}

/// **Fix C — cancel granularity inside the parse.** Cancelling DURING a large
/// `Index.db` parse must abort promptly with `WarmError::Cancelled`, not run the
/// whole O(entries) parse to completion. A single big generation (150k partitions)
/// makes the parse dominate wall time; the canceller waits until the open has
/// begun (the open barrier fired), then — after a margin that guarantees the
/// registry's coarse pre-open cancel check has already passed — trips the flag
/// mid-parse.
///
/// RED on current main: neither `parse_all_partition_keys_with_summary` nor
/// `SSTableReader::open` polls the cancel flag, so once past the coarse
/// between-open check the parse runs to completion and `warm_readers` returns
/// `Ok`. The field showed workers spinning in this exact loop AFTER a client kill.
#[test]
fn cancel_during_large_index_parse_aborts_promptly() {
    let schema = simple_schema();
    let (_temp, table_dir) = build_big_single_gen(&schema, 150_000);

    let reg = WarmTableRegistry::new();
    let cancel = CancelFlag::new();

    // Signal when the (single) open has started; the open barrier fires at the top
    // of the open loop, immediately BEFORE the registry's coarse pre-open cancel
    // check — so the canceller waits for it, then adds a margin so that coarse
    // check has certainly passed and we are now inside the parse.
    let open_started = Arc::new(AtomicBool::new(false));
    {
        let flag = Arc::clone(&open_started);
        reg.set_open_barrier(Arc::new(move || flag.store(true, Ordering::SeqCst)));
    }

    let canceller = {
        let cancel = cancel.clone();
        let open_started = Arc::clone(&open_started);
        thread::spawn(move || {
            while !open_started.load(Ordering::SeqCst) {
                thread::yield_now();
            }
            // Margin: let the coarse pre-open cancel check run first, so this trip
            // lands strictly DURING the parse (never at the coarse boundary, which
            // the unfixed code already honors).
            thread::sleep(Duration::from_millis(30));
            cancel.cancel();
        })
    };

    let res = reg.warm_readers(&key(), ddl(), &schema, &table_dir, None, &cancel);
    canceller.join().expect("canceller");

    // RED today: the parse ignores the mid-parse cancel and returns Ok. GREEN once
    // the parse loop polls the cancel flag (fix C) and returns Cancelled promptly.
    assert!(
        matches!(res, Err(WarmError::Cancelled)),
        "a cancel tripped DURING a large Index.db parse must abort promptly with \
         Cancelled, got {:?} (issue #2383 fix C)",
        res.map(|w| (w.outcome, w.readers.len()))
    );
}
