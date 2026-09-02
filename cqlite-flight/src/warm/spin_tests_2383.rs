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
///
/// Strips the sibling `-Summary.db` before returning (issue #2412 re-anchor,
/// coordinator-flagged regression): `SSTableReader::open` now defers the whole
/// `Index.db` parse when a usable `Summary.db` is present (`open_lazy`, design
/// §A) — for that shape `warm_readers`' OPEN call no longer performs the O(N)
/// synchronous parse this repro targets AT ALL, so a cancel arriving during open
/// has nothing left to interrupt (the property genuinely improved, not broke).
/// The counted, cancel-aware FellBack path (`open_with_summary_cancellable`,
/// §A1) is UNCHANGED and is exactly what fires when `Summary.db` is absent — so
/// stripping it restores this repro's original at-open eager-parse scenario
/// exactly, on the surviving code path. See also
/// `ensure_materialized_cancel_mid_parse_aborts_promptly`
/// (`index_reader/lazy.rs`) for the OTHER surviving big-parse site (the deferred
/// materialize a Summary-usable reader's full/compaction scan still triggers).
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
    let data_files: Vec<_> = std::fs::read_dir(&table_dir)
        .expect("table dir")
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|f| f.ends_with("-Data.db"))
        })
        .collect();
    assert_eq!(
        data_files.len(),
        1,
        "the cancel repro needs exactly ONE big generation (found {})",
        data_files.len()
    );
    // Force the FellBack eager-parse-at-open path (issue #2412 §A1): delete the
    // sibling Summary.db so `summary_usable == false` and `load_index_reader`
    // takes `open_with_summary_cancellable` (still eager, still cancel-aware
    // every `CANCEL_POLL_INTERVAL` entries) instead of `open_lazy`.
    let data_name = data_files[0].file_name();
    let data_name = data_name.to_str().expect("utf8 filename");
    let base = data_name
        .strip_suffix("-Data.db")
        .expect("writer-produced Data.db filename");
    let summary_path = table_dir.join(format!("{base}-Summary.db"));
    assert!(
        summary_path.exists(),
        "fixture precondition: the writer must emit a Summary.db to strip"
    );
    std::fs::remove_file(&summary_path).expect("strip Summary.db to force the FellBack path");
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
    reg.warm_readers(&key(), ddl(), &schema, None, &snap1, Some("snap1"), &cancel)
        .expect("first snapshot warms");
    let opens_after_first = reg.metrics().snapshot().reader_opens;
    assert!(opens_after_first >= 2, "both generations parsed cold");

    // Connector clears query N's snapshot; the live inodes in table_dir persist.
    std::fs::remove_dir_all(&snap1).expect("clear query-N snapshot dir");

    // Query N+1: a NEW snapshot dir over the SAME inodes (hardlinks).
    let snap2 = make_snapshot(&table_dir, "snap2");
    let w2 = reg
        .warm_readers(&key(), ddl(), &schema, None, &snap2, Some("snap2"), &cancel)
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
    // The rebind is COUNTED (issue #2356 §D closure probe): both generations
    // were rebound to the fresh snap2 hardlinks, distinguishing this from a
    // pure warm hit (which would leave `rebind_hits` at 0).
    assert!(
        reg.metrics().snapshot().rebind_hits >= 2,
        "both generations must be counted as rebinds, got {}",
        reg.metrics().snapshot().rebind_hits
    );
}

/// **Warm-hit work is scale-free (flight-warm-snapshot-closure spec).** A warm hit
/// over a LARGE-partition-count generation costs ZERO reader opens and ZERO rebinds
/// — the warm-hit work probe is bounded by the warm-set DIFF (here: nothing
/// changed), never by partition count. Pairs with the small-generation warm-hit
/// tests (e.g. `service::do_get_second_request_is_a_warm_hit_with_zero_reader_opens`)
/// to show independence from partition count: both are 0.
#[test]
fn warm_hit_over_large_generation_is_scale_free() {
    let schema = simple_schema();
    // Many partitions in ONE generation so a cold parse is genuinely large.
    let (_temp, table_dir) = build_big_single_gen(&schema, 3_000);

    let reg = WarmTableRegistry::new();
    let cancel = CancelFlag::new();

    // Cold warm: opens (parses) the one big generation.
    reg.warm_readers(&key(), ddl(), &schema, None, &table_dir, None, &cancel)
        .expect("cold warm of the big generation");
    let after_cold = reg.metrics().snapshot();
    assert!(
        after_cold.reader_opens >= 1,
        "cold build parsed the generation"
    );

    // Repeat over the SAME stable path: a pure warm hit — zero further work.
    reg.warm_readers(&key(), ddl(), &schema, None, &table_dir, None, &cancel)
        .expect("warm hit");
    let after_hit = reg.metrics().snapshot();
    assert_eq!(
        after_hit.reader_opens, after_cold.reader_opens,
        "the warm hit opened ZERO further readers regardless of partition count"
    );
    assert_eq!(
        after_hit.rebind_hits, after_cold.rebind_hits,
        "a stable-path warm hit performs ZERO rebinds regardless of partition count"
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
                reg.warm_readers(&key(), ddl(), &schema, None, &dir, None, &CancelFlag::new())
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
/// whole O(entries) parse to completion. A single big generation (150k
/// partitions) keeps that parse the dominant cost of the open being aborted.
///
/// Positioning is STRUCTURAL, not calibrated (issue #3940). The cancel is
/// tripped by the OPENING thread itself, from the pre-parse barrier inside the
/// coalesced real-open closure — downstream of `rebuild`'s pre-rebuild and
/// pre-open cancel checks, of `open_added`'s per-iteration check and of the
/// coalescer's follower poll — so no cancel gate but the parse's own
/// `ScanCancel` polling can observe it. The predecessor of this test slept
/// `baseline / 20` on a second thread, where `baseline` was a separately-timed
/// uncancelled warm of the same fixture; that compares two DIFFERENT scheduling
/// windows and failed the `flight-tests` gate under concurrent-gate CPU
/// contention (the sleep outlived the parse, so the open returned `Ok`). There
/// is now no clock, no sleep, no calibration pass and no canceller thread. The
/// exact discrimination boundary — including what this positioning does NOT
/// cover — is stated at the assertion site.
///
/// RED pre-#2383-fix-C: neither `parse_all_partition_keys_with_summary` nor
/// `SSTableReader::open` polled the cancel flag, so once past the coarse
/// between-open check the parse ran to completion and `warm_readers` returned
/// `Ok`. The field showed workers spinning in this exact loop AFTER a client kill.
///
/// Re-anchored (issue #2412, coordinator-flagged regression): this test drove
/// the OPEN call's eager Index.db parse directly. Since #2412 Stage 2, BIG open
/// defers that parse (`open_lazy`) whenever a usable `Summary.db` is present —
/// the common/field shape — so `warm_readers`' open no longer performs the O(N)
/// synchronous work this repro cancels mid-flight (open is now O(summary), by
/// design). `build_big_single_gen` now strips the sibling `-Summary.db`, forcing
/// the STILL-eager, STILL-cancel-aware FellBack path
/// (`open_with_summary_cancellable`, §A1) — the surviving at-open big-parse site
/// — so this exact scenario (cancel lands DURING open's Index.db parse, aborts
/// promptly) is re-verified on the code that still runs it. The lazy
/// `ensure_materialized` deferred-parse site (the one a Summary-usable reader's
/// full/compaction scan still triggers) is covered by a companion test,
/// `ensure_materialized_cancel_mid_parse_aborts_promptly`
/// (`cqlite-core/src/storage/sstable/index_reader/lazy.rs`).
#[test]
fn cancel_during_large_index_parse_aborts_promptly() {
    let schema = simple_schema();
    let (_temp, table_dir) = build_big_single_gen(&schema, 150_000);

    let reg = WarmTableRegistry::new();
    let cancel = CancelFlag::new();

    // Position the cancel STRUCTURALLY, not temporally (issue #3940).
    //
    // The pre-parse barrier fires on the opening thread INSIDE the coalesced
    // real-open closure: downstream of `rebuild`'s pre-rebuild and pre-open
    // cancel checks, downstream of `open_added`'s per-iteration check, and
    // downstream of the coalescer's follower-wait poll (this call is the LEADER,
    // whose path has no cancel gate of its own). Tripping the flag from the
    // barrier therefore puts the cancellation in front of the reader open+parse
    // with NO other observer left between the two.
    {
        let cancel = cancel.clone();
        reg.set_open_parse_barrier(Arc::new(move || cancel.cancel()));
    }

    let res = reg.warm_readers(&key(), ddl(), &schema, None, &table_dir, None, &cancel);

    // WHAT REPLACED THE 1/20th CALIBRATED MARGIN, AND WHY IT IS CONTENTION-IMMUNE
    // (issue #3940).
    //
    // This assertion was always `matches!(res, Err(WarmError::Cancelled))` — never
    // a timing assert. The wall clock was only ever a POSITIONING DEVICE: a
    // canceller thread slept `baseline / 20` (a separately-measured uncancelled
    // warm) so its trip would land past the coarse pre-open check and inside the
    // parse. That compares TWO scheduling windows — the calibration warm and the
    // timed warm — and under concurrent-gate CPU contention they see different CPU
    // availability, so `margin` came out longer than the timed parse and the
    // cancel arrived AFTER the open had already returned `Ok`. Hence the
    // `flight-tests` flake.
    //
    // Positioning is now structural: the cancel is tripped BY the opening thread
    // itself, at a fixed point in the call graph, before the parse begins. There
    // is no clock, no sleep, no calibration run and no second thread — so there
    // are no two scheduling windows to compare and nothing for CPU contention to
    // skew. The ordering the old margin tried to buy probabilistically is now an
    // invariant of the control flow.
    //
    // WHAT THIS DISCRIMINATES: the `CancelFlag` -> `ScanCancel` bridge
    // (`open_added`) and the Index.db open/parse's cancel awareness (#2383 fix C,
    // `IndexReader::open_with_summary_cancellable` ->
    // `parse_index_data_cancellable`) surfacing `Error::Cancelled` BY VARIANT, and
    // `rebuild` mapping it to `WarmError::Cancelled` instead of a fail-closed
    // retain. Verified by positive control: with fix C's parse-side cancel polling
    // removed, this assertion FAILS with `res == Ok`.
    //
    // WHAT IT DOES NOT DISCRIMINATE, stated because a silent narrowing is
    // indistinguishable from coverage: a cancel positioned at the open boundary is
    // caught by fix C's coarse PRE-PARSE check, which is behaviourally identical
    // to the entry-loop's own poll at entry 0 — so removing ONLY the in-loop
    // `entry_index % CANCEL_POLL_INTERVAL` poll leaves this test GREEN. Landing a
    // cancel strictly BETWEEN two parsed entries is not reachable from this crate
    // without a clock (the loop lives in cqlite-core and exposes no progress
    // signal), so that stride's own coverage belongs to a cqlite-core-local test
    // beside the loop, not here. Trading a probabilistic in-loop landing for a
    // deterministic pre-parse one is the deliberate choice of #3940: the old test
    // did not reliably land in the loop either, it merely failed loudly when it
    // missed.
    assert!(
        matches!(res, Err(WarmError::Cancelled)),
        "a cancel tripped on the opening thread immediately before a large \
         Index.db open+parse must abort with Cancelled, got {:?} (issue #2383 \
         fix C; positioning per issue #3940)",
        res.map(|w| (w.outcome, w.readers.len()))
    );
}

/// **Blocker 1 (post-review, roborev job 1653)** — the coalescer's `Weak` hit
/// must NOT serve a reader whose backing path is DEAD, even when that reader is
/// kept alive purely by an IN-FLIGHT holder OUTSIDE the registry's own cache.
///
/// Scenario (the exact #2352 ENOENT class, one step removed): reader `R` for
/// table A opens from a per-query snapshot dir; A's generation is then
/// LRU-EVICTED from the registry's cache under budget pressure while a prior
/// request still holds `R` alive via its `WarmSet` `Arc` clone (`held` below);
/// A's snapshot dir is then cleared by the connector. `registry::rebuild`'s
/// rebind pass (fix B) only walks the CURRENTLY CACHED reader set — since A's
/// generation is no longer cached, rebind can never see `R`, let alone repoint
/// its path. The coalescer's OWN `Done(Weak)` slot for `R`'s generation
/// identity, however, is untouched by cache eviction (it lives in a SEPARATE
/// map), so the ONLY thing standing between the next query and re-serving `R`'s
/// dead path is the coalescer's own path-liveness gate.
///
/// RED without the blocker-1 fix: the coalescer upgrades its `Weak` to `Some(R)`
/// and returns it immediately WITHOUT checking `R`'s (now dead) backing path.
/// GREEN with the fix: the gate rejects the dead-path hit and falls through to
/// `do_open` from the live `entry.path`, so the served reader always backs a
/// live inode and decodes correctly.
#[test]
fn evicted_but_inflight_reader_is_not_served_with_dead_path() {
    let schema = simple_schema();
    let (_t_a, _d_a, dir_a) = build_sstables(&schema, vec![vec![write_row(1, "a", 1, 100)]]);
    let (_t_b, _d_b, dir_b) = build_sstables(&schema, vec![vec![write_row(2, "b", 2, 100)]]);

    let key_a = TableKey::new(KS, "blocker1_victim");
    let key_b = TableKey::new(KS, "blocker1_pressure");

    // Budget = exactly ONE generation's footprint, so warming B evicts A.
    let probe = WarmTableRegistry::new();
    probe
        .warm_readers(
            &key_a,
            ddl(),
            &schema,
            None,
            &dir_a,
            None,
            &CancelFlag::new(),
        )
        .expect("probe warm");
    let one_gen = probe.debug_used_bytes();
    assert!(one_gen > 0, "a generation's footprint is non-zero");

    let reg = WarmTableRegistry::with_budget(one_gen);
    let cancel = CancelFlag::new();

    // Warm A from a per-query snapshot dir: reader R's `file_path` lives inside
    // snap1. Keep the returned `Arc` alive for the WHOLE test — the "in-flight
    // holder outside the registry's cache" this bug depends on.
    let snap1 = make_snapshot(&dir_a, "snap1");
    let w1 = reg
        .warm_readers(&key_a, ddl(), &schema, None, &snap1, Some("snap1"), &cancel)
        .expect("warm A from snap1");
    assert_eq!(w1.readers.len(), 1, "table A is a single generation");
    let held: Vec<Arc<SSTableReader>> = w1.readers.clone();
    let held_ptr = Arc::as_ptr(&held[0]);

    // Warm B: pushes `used_bytes` over budget, evicting A's generation from the
    // registry's OWN cache — but R stays alive via `held` above.
    reg.warm_readers(&key_b, ddl(), &schema, None, &dir_b, None, &cancel)
        .expect("warm B evicts A");
    assert_eq!(
        reg.debug_reader_count(&key_a),
        0,
        "A's generation was evicted from the registry's own cache (bug precondition)"
    );

    // The connector clears query N's snapshot; R's (no-longer-cached-by-the-
    // registry) `file_path` is now dead. R itself stays alive only via `held`.
    std::fs::remove_dir_all(&snap1).expect("clear query-N snapshot dir");

    // Query N+1: a NEW snapshot dir over the SAME inodes. Since A was evicted,
    // this is a genuine registry miss — `rebuild`'s rebind pass never sees R (it
    // isn't in the cached set), so the ONLY protection against re-serving R's
    // dead path is the coalescer's own path-liveness gate (blocker 1).
    let snap2 = make_snapshot(&dir_a, "snap2");
    let w2 = reg
        .warm_readers(&key_a, ddl(), &schema, None, &snap2, Some("snap2"), &cancel)
        .expect("re-warm A after eviction + snapshot teardown");
    assert_eq!(w2.readers.len(), 1, "table A is still a single generation");

    // The served reader must back a LIVE path — never R's stale snap1 path.
    assert!(
        std::fs::metadata(w2.readers[0].file_path()).is_ok(),
        "must never serve a dead-path reader from the coalescer, got {}",
        w2.readers[0].file_path().display()
    );
    // Must NOT be R itself (the stale-path reader) — the coalescer must have
    // fallen through to a fresh open from the live path.
    assert_ne!(
        Arc::as_ptr(&w2.readers[0]) as *const (),
        held_ptr as *const (),
        "the coalescer must not hand back R's dead-path reader (issue #2383 blocker 1)"
    );
    // Row correctness through the same merge path do_get drives.
    assert_eq!(
        decode_names(&schema, w2.readers),
        vec!["a".to_string()],
        "the re-warmed set decodes correctly from the live inodes (no ENOENT)"
    );
    drop(held);
}
