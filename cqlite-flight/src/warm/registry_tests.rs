//! Registry integration tests over REAL in-process SSTables (issue #2310).
//!
//! These prove the spec requirements the registry owns end to end: the
//! generation-identity key across snapshot dirs (Req 1), fail-closed rebuild
//! (Req 4), LRU + removed-on-disk eviction (Req 5), and the #2345 UDT-registry
//! guard. Loaded via `#[path]` from `registry.rs` to keep that file within the
//! campsite threshold. Plain `#[test]`s: `build_sstables` drives its own runtime,
//! and `warm_readers` builds its own — nesting a `#[tokio::test]` runtime would
//! panic.

use std::path::Path;
use std::sync::Arc;

use cqlite_core::schema::TableSchema;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::storage::write_engine::Mutation;

use crate::cancel::CancelFlag;
use crate::producer::MergeProducer;
use crate::testutil::{
    build_sstables, make_snapshot, simple_schema, write_row, KS, SIMPLE_DDL, TBL,
};
use crate::warm::{RefreshOutcome, TableKey, WarmError, WarmTableRegistry};

fn key() -> TableKey {
    TableKey::new(KS, TBL)
}

fn ddl() -> u64 {
    crate::warm::ddl_hash(SIMPLE_DDL)
}

/// Decode a warm reader set into its sorted `name` column values — proves ROW
/// CORRECTNESS (not just that a reader opened) by driving the SAME merge path
/// `do_get` uses ([`MergeProducer::produce_streaming_from_readers_to_vec`]).
fn decode_names(schema: &TableSchema, readers: Vec<Arc<SSTableReader>>) -> Vec<String> {
    use arrow::array::{Array, StringArray};
    let producer = MergeProducer::new(schema.clone(), 1024).expect("producer");
    let batches = producer
        .produce_streaming_from_readers_to_vec(readers, &CancelFlag::new())
        .expect("decode readers");
    let mut out = Vec::new();
    for b in &batches {
        let names = b
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..names.len() {
            out.push(names.value(i).to_string());
        }
    }
    out.sort();
    out
}

/// Append one more SSTable generation into an existing live table dir (a fresh
/// write-engine flush pointed at the SAME data root).
fn append_gen(table_dir: &Path, schema: &TableSchema, rows: Vec<Mutation>) {
    use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
    let data_dir = table_dir.parent().unwrap().parent().unwrap().to_path_buf();
    let wal = table_dir.join(".wal_append_reg");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let config = WriteEngineConfig::new(data_dir, wal, schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in rows {
        engine.write(m).expect("write");
    }
    rt.block_on(engine.flush()).expect("flush").expect("info");
}

/// Requirement 1 (generation-identity key): the SAME inodes reached through TWO
/// different snapshot hardlink dirs resolve to ONE warm entry — the second is a
/// warm HIT with zero further reader opens. A path key would miss here.
#[test]
fn cross_snapshot_dirs_share_one_warm_entry() {
    let schema = simple_schema();
    let (_temp, _data, table_dir) = build_sstables(
        &schema,
        vec![
            vec![write_row(1, "a", 1, 100)],
            vec![write_row(2, "b", 2, 100)],
        ],
    );
    // Two per-query snapshots over the SAME underlying inodes (hardlinks).
    let snap1 = make_snapshot(&table_dir, "snap1");
    let snap2 = make_snapshot(&table_dir, "snap2");

    let reg = WarmTableRegistry::new();
    let cancel = CancelFlag::new();

    let w1 = reg
        .warm_readers(&key(), ddl(), &schema, &snap1, Some("snap1"), &cancel)
        .expect("first snapshot warms");
    assert_eq!(w1.outcome, RefreshOutcome::RebuiltDelta, "first is a build");
    let opens_after_first = reg.metrics().snapshot().reader_opens;
    assert!(opens_after_first >= 2, "both generations opened cold");

    let w2 = reg
        .warm_readers(&key(), ddl(), &schema, &snap2, Some("snap2"), &cancel)
        .expect("second snapshot (different dir, same inodes)");
    assert_eq!(
        w2.outcome,
        RefreshOutcome::Unchanged,
        "same inodes via a different snapshot dir → warm hit, not a path miss"
    );
    let m = reg.metrics().snapshot();
    assert_eq!(m.hits, 1, "the second snapshot request is a warm hit");
    assert_eq!(
        m.reader_opens, opens_after_first,
        "a cross-snapshot warm hit opens ZERO further readers"
    );
    assert_eq!(w2.readers.len(), w1.readers.len(), "same reader set");
}

/// Regression (issue #2352): the connector creates a FRESH per-query snapshot
/// dir and CLEARS the prior one after each query (`SnapshotManager.clearSnapshot`).
/// The warm cache keys on inode identity, so query N+1's new snapshot dir (same
/// inodes) is a set-match hit — but the cached reader re-opens its `Data.db`
/// LAZILY by the PATH it was opened from (query N's snapshot dir), which the
/// connector has since deleted. A full-scan through the warm readers then fails
/// with `No such file or directory (os error 2)` mid-merge — the 9 nightly
/// Flight↔Trino E2E failures.
///
/// RED on unfixed code: the second `warm_readers` returns a silent `Unchanged`
/// hit carrying readers whose backing path (the deleted `snap1`) no longer
/// resolves, so `decode_names` (the same streaming-merge path `do_get` drives)
/// panics with the ENOENT. GREEN after the fix: a dead cached path forces an
/// authoritative rebuild from the current LIVE dir (`snap2`), the scan reads the
/// live inodes, and the rows decode correctly — counted as a refresh
/// (`RebuiltDelta`), never a stale hit.
#[test]
fn warm_hit_after_snapshot_teardown_rebuilds_instead_of_enoent() {
    let schema = simple_schema();
    let (_temp, _data, table_dir) = build_sstables(
        &schema,
        vec![
            vec![write_row(1, "a", 1, 100)],
            vec![write_row(2, "b", 2, 100)],
        ],
    );

    // Query N: a per-query snapshot dir; warm the cache from it (readers open
    // with their `file_path` inside `snap1`).
    let snap1 = make_snapshot(&table_dir, "snap1");
    let reg = WarmTableRegistry::new();
    let cancel = CancelFlag::new();
    let w1 = reg
        .warm_readers(&key(), ddl(), &schema, &snap1, Some("snap1"), &cancel)
        .expect("first snapshot warms");
    assert_eq!(w1.outcome, RefreshOutcome::RebuiltDelta, "first is a build");
    assert_eq!(
        decode_names(&schema, w1.readers),
        vec!["a".to_string(), "b".to_string()],
        "first query decodes correctly from the live snapshot dir"
    );

    // Connector clears the query-N snapshot; the LIVE inodes in `table_dir`
    // persist. The cached readers' `file_path`s are now dead paths.
    std::fs::remove_dir_all(&snap1).expect("clear the query-N snapshot dir");

    // Query N+1: a NEW per-query snapshot dir over the SAME inodes. Inode-keyed
    // identity makes this a set-match — but a dead cached path must NOT be served.
    let snap2 = make_snapshot(&table_dir, "snap2");
    let w2 = reg
        .warm_readers(&key(), ddl(), &schema, &snap2, Some("snap2"), &cancel)
        .expect("second snapshot request after the first was cleared");

    // The fix: a dead cached path is a rebuild (a refresh outcome), never a
    // silent stale hit.
    assert_eq!(
        w2.outcome,
        RefreshOutcome::RebuiltDelta,
        "a cleared cached snapshot path forces a rebuild from the current live \
         dir, not a stale warm hit (issue #2352)"
    );
    // Every returned reader must have a LIVE backing path (rebuilt from snap2),
    // so a full-scan re-open cannot ENOENT.
    for r in &w2.readers {
        assert!(
            std::fs::metadata(r.file_path()).is_ok(),
            "warm reader must back a live path after teardown, got dead {}",
            r.file_path().display()
        );
    }
    // THE regression assertion: streaming the warm readers (the exact path
    // `do_get` drives) no longer ENOENTs and returns the correct rows. On unfixed
    // code the stale `Unchanged` hit above carries readers pointing at the
    // deleted `snap1`, so this `decode_names` panics with
    // `No such file or directory (os error 2)` mid-merge — the nightly signature.
    assert_eq!(
        decode_names(&schema, w2.readers),
        vec!["a".to_string(), "b".to_string()],
        "streaming the rebuilt warm readers reads the live inodes correctly \
         (no ENOENT mid-merge)"
    );
}

/// Warm/cold UDT-registry PARITY (spec non-goal: warm is a parse-cost change
/// only, never a read-semantics change; #2349): the warm path must open readers
/// with the SAME UDT-registry posture as the cold path. The cold Flight path
/// (`KWayMerger::new_cancellable`) opens readers with `udt_registry = None`, so a
/// warm reader must ALSO have no registry. Both are `has_udt_registry() == false`
/// today; they flip together when #2349 wires a real registry into both paths.
#[test]
fn warm_and_cold_reader_udt_posture_identical() {
    use cqlite_core::{Config, Platform};

    let schema = simple_schema();
    let (_temp, _data, table_dir) = build_sstables(&schema, vec![vec![write_row(1, "a", 1, 100)]]);

    // WARM posture: readers handed out by the registry.
    let reg = WarmTableRegistry::new();
    let w = reg
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &CancelFlag::new())
        .expect("warms");
    assert!(!w.readers.is_empty(), "opened at least one reader");
    for r in &w.readers {
        assert!(
            !r.has_udt_registry(),
            "a warm reader must match the cold path's no-UDT-registry posture (#2349)"
        );
    }

    // COLD posture: open a reader exactly as `KWayMerger::new_cancellable` does
    // (plain `SSTableReader::open`, never `set_udt_registry`).
    let data_db = std::fs::read_dir(&table_dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .expect("a Data.db exists");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let config = Config::default();
    let cold = rt.block_on(async {
        let platform = Arc::new(Platform::new(&config).await.unwrap());
        SSTableReader::open(&data_db, &config, platform)
            .await
            .unwrap()
    });
    assert_eq!(
        cold.has_udt_registry(),
        w.readers[0].has_udt_registry(),
        "warm and cold readers must share one UDT-registry posture (parity, #2349)"
    );
    assert!(
        !cold.has_udt_registry(),
        "the cold path opens readers without a UDT registry"
    );
}

/// Fix A (concurrent same-key rebuild race): two concurrent MISSES for ONE table
/// each open the SAME added generations OUTSIDE the swap lock; the second swap
/// must NOT keep both copies. A test-only rendezvous holds both threads past the
/// probe+open, before EITHER swaps, so the race is deterministic. Asserts exactly
/// ONE cached `WarmReader` per generation and `used_bytes` == the single-threaded
/// accounted footprint (fails against the pre-fix no-dedup swap: 4 readers, 2x
/// bytes).
#[test]
fn concurrent_same_key_rebuild_dedups_readers_and_bytes() {
    use std::sync::Barrier;
    use std::thread;

    let schema = simple_schema();
    let (temp, _data, table_dir) = build_sstables(
        &schema,
        vec![
            vec![write_row(1, "a", 1, 100)],
            vec![write_row(2, "b", 2, 100)],
        ],
    );

    // Reference: a clean single-threaded warm of the SAME dir → the correct
    // reader count (2) and accounted footprint.
    let reference = WarmTableRegistry::new();
    reference
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &CancelFlag::new())
        .expect("reference warm");
    let ref_used = reference.debug_used_bytes();
    let ref_count = reference.debug_reader_count(&key());
    assert_eq!(ref_count, 2, "reference caches two generations");
    assert!(ref_used > 0, "reference footprint is non-zero");

    // Race two concurrent same-key misses on one shared registry.
    let reg = Arc::new(WarmTableRegistry::new());
    let barrier = Arc::new(Barrier::new(2));
    {
        let b = Arc::clone(&barrier);
        reg.set_swap_barrier(Arc::new(move || {
            b.wait();
        }));
    }

    let handles: Vec<_> = (0..2)
        .map(|_| {
            let reg = Arc::clone(&reg);
            let schema = schema.clone();
            let dir = table_dir.clone();
            thread::spawn(move || {
                reg.warm_readers(&key(), ddl(), &schema, &dir, None, &CancelFlag::new())
                    .expect("concurrent warm")
                    .readers
                    .len()
            })
        })
        .collect();
    for h in handles {
        let n = h.join().expect("thread");
        assert_eq!(
            n, 2,
            "each request returns exactly two generations' readers"
        );
    }

    assert_eq!(
        reg.debug_reader_count(&key()),
        2,
        "no duplicate WarmReader survives the concurrent double-swap"
    );
    assert_eq!(
        reg.debug_distinct_gen_count(&key()),
        2,
        "exactly one reader per distinct generation"
    );
    assert_eq!(
        reg.debug_used_bytes(),
        ref_used,
        "used_bytes matches the single-threaded footprint (no double-count drift)"
    );
    drop(temp);
}

/// Epoch guard (issue #2310, roborev 1639): a SLOW rebuild ("A") whose own
/// disk-probe ran BEFORE a second generation landed — so A's `current_set`
/// only knows about gen1 — must NOT be allowed to reach the swap after a
/// FASTER, newer rebuild ("B", probed AFTER gen2 landed) already installed
/// `{gen1, gen2}`. Without the epoch guard, A's stale `current_set = {gen1}`
/// would make the ORIGINAL swap logic treat B's freshly-installed gen2 as
/// "removed" (not in A's `current_set`) and EVICT it — silently losing a live
/// generation. `open_barrier` pauses A (by thread name) right after its OWN
/// probe/before it opens gen1, so B can land gen2 on disk and fully complete
/// its rebuild+swap before A is released to attempt its (now stale) swap.
#[test]
fn slow_rebuild_does_not_overwrite_a_faster_newer_swap() {
    use std::sync::Barrier;
    use std::thread;

    let schema = simple_schema();
    let (temp, _data, table_dir) = build_sstables(&schema, vec![vec![write_row(1, "a", 1, 100)]]);
    let reg = Arc::new(WarmTableRegistry::new());

    let arrived = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    {
        let arrived = Arc::clone(&arrived);
        let resume = Arc::clone(&resume);
        reg.set_open_barrier(Arc::new(move || {
            // Only the slow-A thread pauses here; B's calls to the same global
            // hook no-op and proceed immediately.
            if thread::current().name() == Some("slow-A") {
                arrived.wait();
                resume.wait();
            }
        }));
    }

    let a_reg = Arc::clone(&reg);
    let a_schema = schema.clone();
    let a_dir = table_dir.clone();
    let a_handle = thread::Builder::new()
        .name("slow-A".to_string())
        .spawn(move || {
            a_reg.warm_readers(&key(), ddl(), &a_schema, &a_dir, None, &CancelFlag::new())
        })
        .expect("spawn slow-A");

    // Block until A has probed (sees ONLY gen1) and paused before opening it.
    arrived.wait();

    // gen2 lands on disk AFTER A's probe. B (this thread) probes, opens, and
    // swaps to completion BEFORE A is released — B's install is the "newer"
    // state A's stale probe never saw.
    append_gen(&table_dir, &schema, vec![write_row(2, "b", 2, 100)]);
    let b_result = reg
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &CancelFlag::new())
        .expect("fast rebuild B installs the newer set");
    assert_eq!(b_result.readers.len(), 2, "B installs both generations");

    // Release A: it opens its own (already-stale) gen1 copy and attempts to
    // swap against its stale current_set={gen1}.
    resume.wait();
    let a_result = a_handle
        .join()
        .expect("thread")
        .expect("A's rebuild completes (discarded, not erroring)");

    assert_eq!(
        a_result.readers.len(),
        2,
        "A must be served the CURRENT (B's) fresher two-generation set, not \
         have evicted gen2 based on its own stale one-generation probe"
    );
    assert_eq!(
        a_result.outcome,
        RefreshOutcome::FailClosedRetained,
        "the discarded-stale-rebuild path is folded into FailClosedRetained \
         (adjudicated, documented in warm/metrics.rs)"
    );

    // No reader churn: A must be served the IDENTICAL (B's) Arc<SSTableReader>s,
    // never a freshly-reopened duplicate for either generation.
    let mut a_ptrs: Vec<*const SSTableReader> = a_result.readers.iter().map(Arc::as_ptr).collect();
    let mut b_ptrs: Vec<*const SSTableReader> = b_result.readers.iter().map(Arc::as_ptr).collect();
    a_ptrs.sort();
    b_ptrs.sort();
    assert_eq!(
        a_ptrs, b_ptrs,
        "A's served readers must be the SAME Arcs as B's installed ones (no churn)"
    );

    assert_eq!(
        reg.debug_reader_count(&key()),
        2,
        "the final cached set is B's two generations — gen2 was never evicted"
    );
    drop(temp);
}

/// Requirement 4 (fail-closed rebuild): a newly-added generation that cannot be
/// opened returns the typed error and leaves the previously warm set fully
/// intact — no partial view.
#[test]
fn fail_closed_rebuild_retains_prior_warm_set() {
    let schema = simple_schema();
    let (_temp, _data, table_dir) = build_sstables(&schema, vec![vec![write_row(1, "a", 1, 100)]]);
    let reg = WarmTableRegistry::new();
    let cancel = CancelFlag::new();

    // Warm the valid set.
    let w1 = reg
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &cancel)
        .expect("warms the valid generation");
    let opens_after_first = reg.metrics().snapshot().reader_opens;

    // Add a CORRUPT generation on disk: clone the valid gen-1 components to a new
    // generation, then corrupt its `Statistics.db` — a #1626 hard-fail on open,
    // exactly the fail-closed rebuild scenario (design Test strategy).
    for entry in std::fs::read_dir(&table_dir).unwrap().flatten() {
        let name = entry.file_name();
        let name = name.to_str().unwrap();
        if let Some(suffix) = name.strip_prefix("nb-1-big-") {
            std::fs::copy(entry.path(), table_dir.join(format!("nb-999-big-{suffix}"))).unwrap();
        }
    }
    std::fs::write(
        table_dir.join("nb-999-big-Statistics.db"),
        b"corrupt statistics",
    )
    .unwrap();

    let err = reg
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &cancel)
        .expect_err("a corrupt added generation must fail the rebuild");
    assert!(
        matches!(err, WarmError::Open { .. }),
        "fail-closed rebuild surfaces the typed Open error, got {err:?}"
    );
    let m = reg.metrics().snapshot();
    assert_eq!(
        m.refresh_fail_closed_retained, 1,
        "the fail-closed retention outcome is recorded"
    );

    // The previously warm set is still intact: remove the corrupt file and prove
    // the original generation still serves as a warm hit (its parsed state was
    // never dropped by the failed rebuild).
    std::fs::remove_file(table_dir.join("nb-999-big-Data.db")).unwrap();
    let w2 = reg
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &cancel)
        .expect("prior set still serves");
    assert_eq!(
        w2.outcome,
        RefreshOutcome::Unchanged,
        "prior set intact → hit"
    );
    assert_eq!(
        reg.metrics().snapshot().reader_opens,
        opens_after_first,
        "the failed rebuild opened no reader that survived; the retained set needs none"
    );
    let _ = w1;
}

/// Requirement 5 (removed-on-disk): a generation that a rebuild finds gone is
/// evicted immediately (recorded as an evict), and the result reflects the
/// remaining set.
#[test]
fn removed_generation_is_evicted_immediately() {
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
    let w1 = reg
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &cancel)
        .expect("warms two generations");
    assert_eq!(w1.readers.len(), 2);

    // Delete the OLDEST generation's Data.db (generation 1 — the first flush).
    let victim = std::fs::read_dir(&table_dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("nb-1-") && n.ends_with("-Data.db"))
        })
        .expect("gen-1 Data.db present");
    std::fs::remove_file(&victim).unwrap();

    let w2 = reg
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &cancel)
        .expect("rebuild drops the removed generation");
    assert_eq!(w2.readers.len(), 1, "the removed generation is gone");
    assert!(
        reg.metrics().snapshot().evicts >= 1,
        "the removed-on-disk generation is evicted immediately"
    );
}

/// Requirement 5 (LRU byte budget + hardening): warming distinct tables past a
/// budget that holds ONE generation evicts the least-recently-used entry;
/// `used_bytes` stays within budget after the eviction; and the evicted entry
/// re-parses to CORRECT rows (not merely "an open succeeded") on its next request.
#[test]
fn lru_evicts_when_over_budget() {
    let schema = simple_schema();
    // Two separate tables, each one SSTable. `warm_readers` keys per TableKey.
    let (_t_a, _d_a, dir_a) = build_sstables(&schema, vec![vec![write_row(1, "a", 1, 100)]]);
    let (_t_b, _d_b, dir_b) = build_sstables(&schema, vec![vec![write_row(1, "b", 1, 100)]]);

    let key_a = TableKey::new(KS, "table_a");
    let key_b = TableKey::new(KS, "table_b");

    // Budget = table B's single-generation footprint (measured on a throwaway
    // registry): B fits, but A+B together force A out.
    let probe = WarmTableRegistry::new();
    probe
        .warm_readers(&key_b, ddl(), &schema, &dir_b, None, &CancelFlag::new())
        .expect("probe warm");
    let one_gen = probe.debug_used_bytes();
    assert!(one_gen > 0, "a generation's footprint is non-zero");

    let reg = WarmTableRegistry::with_budget(one_gen);
    let cancel = CancelFlag::new();

    reg.warm_readers(&key_a, ddl(), &schema, &dir_a, None, &cancel)
        .expect("warm A");
    reg.warm_readers(&key_b, ddl(), &schema, &dir_b, None, &cancel)
        .expect("warm B evicts A");
    assert!(
        reg.metrics().snapshot().evicts >= 1,
        "warming B past the one-generation budget evicts LRU entry A"
    );
    assert!(
        reg.debug_used_bytes() <= one_gen,
        "used_bytes stays within budget after eviction (got {})",
        reg.debug_used_bytes()
    );

    // A must re-parse (a fresh miss + open) since it was evicted, and its rows
    // must be CORRECT — the re-opened reader decodes partition id=1 → name "a".
    let opens_before = reg.metrics().snapshot().reader_opens;
    let wa = reg
        .warm_readers(&key_a, ddl(), &schema, &dir_a, None, &cancel)
        .expect("A re-parses after eviction");
    assert_eq!(
        wa.outcome,
        RefreshOutcome::RebuiltDelta,
        "an evicted entry is a miss on its next request"
    );
    assert!(
        reg.metrics().snapshot().reader_opens > opens_before,
        "re-warming an evicted entry re-opens its reader"
    );
    assert_eq!(
        decode_names(&schema, wa.readers),
        vec!["a".to_string()],
        "the re-parsed entry decodes its CORRECT rows, not just an open"
    );
}

/// Requirement 3 (snapshot manifest fast path), corrected for issue #2352: a
/// byte-identical snapshot `manifest.json` serves the cached set WITHOUT relisting
/// the directory — proven by planting a THIRD `Data.db` on disk that the manifest
/// does NOT list. The fast path (trusting the manifest) serves exactly the two
/// cached readers and never `read_dir`s the third file; an authoritative relist
/// would enumerate three generations and rebuild to a different set.
///
/// The pre-#2352 version proved "no relist" by DELETING every `Data.db` after
/// warming and asserting the cached readers were still served. That encoded the
/// exact ENOENT regression #2352 fixes: a warm hit must NEVER serve a reader whose
/// backing `Data.db` has been cleared (a full-scan would re-open the dead path and
/// fail mid-merge). So the two real files are kept LIVE here — a dead-path cached
/// set now rebuilds from the current live dir instead of being served (see
/// `warm_hit_after_snapshot_teardown_rebuilds_instead_of_enoent`).
#[test]
fn manifest_fast_path_serves_cached_without_relisting() {
    let schema = simple_schema();
    let (_temp, _data, table_dir) = build_sstables(
        &schema,
        vec![
            vec![write_row(1, "a", 1, 100)],
            vec![write_row(2, "b", 2, 100)],
        ],
    );
    let snap = make_snapshot(&table_dir, "snap1");
    std::fs::write(
        snap.join("manifest.json"),
        br#"{"files":["nb-1-big-Data.db","nb-2-big-Data.db"]}"#,
    )
    .unwrap();

    let reg = WarmTableRegistry::new();
    let cancel = CancelFlag::new();
    let w1 = reg
        .warm_readers(&key(), ddl(), &schema, &snap, Some("snap1"), &cancel)
        .expect("warm snapshot");
    assert_eq!(w1.readers.len(), 2, "both generations warmed");
    let opens = reg.metrics().snapshot().reader_opens;

    // Plant a THIRD Data.db the manifest does NOT list, keeping the two real files
    // LIVE. The manifest fast path (byte-identical manifest) must ignore it (no
    // read_dir); an authoritative relist would enumerate 3 generations and rebuild.
    std::fs::write(snap.join("nb-3-big-Data.db"), b"not-a-real-sstable").unwrap();

    let w2 = reg
        .warm_readers(&key(), ddl(), &schema, &snap, Some("snap1"), &cancel)
        .expect("manifest fast path serves cached");
    assert_eq!(
        w2.outcome,
        RefreshOutcome::Unchanged,
        "identical manifest → warm hit via the fast path (a relist would have \
         seen 3 generations and NOT reported Unchanged)"
    );
    assert_eq!(
        w2.readers.len(),
        2,
        "the two cached readers are served — the planted third file was never relisted"
    );
    assert_eq!(
        reg.metrics().snapshot().reader_opens,
        opens,
        "the fast path opened zero further readers"
    );
}

/// Requirement 4 (in-flight isolation): a warm set handed to a request is an
/// `Arc` snapshot; a concurrent rebuild that swaps in a new generation does NOT
/// change what the held set yields. The held (pre-swap) set decodes its original
/// row; the NEXT lookup sees the post-swap set.
#[test]
fn held_warm_set_is_isolated_from_a_rebuild_swap() {
    let schema = simple_schema();
    let (_temp, _data, table_dir) = build_sstables(&schema, vec![vec![write_row(1, "a", 1, 100)]]);
    let reg = WarmTableRegistry::new();
    let cancel = CancelFlag::new();
    let w1 = reg
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &cancel)
        .expect("warm gen-1");
    // Hold the pre-swap Arc set (as an in-flight stream would).
    let held = w1.readers.clone();

    // Add a second generation and rebuild+swap the warm set.
    append_gen(&table_dir, &schema, vec![write_row(2, "b", 2, 200)]);
    let w2 = reg
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &cancel)
        .expect("rebuild adds gen-2");
    assert_eq!(
        w2.readers.len(),
        2,
        "the NEXT lookup sees the post-swap set"
    );

    // The HELD set still yields exactly its pre-swap row — isolated from the swap.
    assert_eq!(
        decode_names(&schema, held),
        vec!["a".to_string()],
        "the held set yields its pre-swap rows"
    );
    assert_eq!(
        decode_names(&schema, w2.readers),
        vec!["a".to_string(), "b".to_string()],
        "the post-swap lookup yields both generations"
    );
}

/// Requirement 7 (mid-rebuild cancellation): cancelling BETWEEN added-generation
/// opens surfaces the `Cancelled` variant, leaves the prior warm set fully intact,
/// and performs NO partial swap (the partially-opened readers are discarded, never
/// installed).
#[test]
fn mid_rebuild_cancellation_leaves_prior_set_intact() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let schema = simple_schema();
    let (_temp, _data, table_dir) = build_sstables(&schema, vec![vec![write_row(1, "a", 1, 100)]]);
    let reg = WarmTableRegistry::new();
    let cancel = CancelFlag::new();
    // Warm gen-1 (the prior set).
    reg.warm_readers(&key(), ddl(), &schema, &table_dir, None, &cancel)
        .expect("warm gen-1");

    // Add TWO more generations so the rebuild opens multiple.
    append_gen(&table_dir, &schema, vec![write_row(2, "b", 2, 200)]);
    append_gen(&table_dir, &schema, vec![write_row(3, "c", 3, 300)]);

    // Trip the cancel flag BETWEEN added-generation opens: the per-open barrier
    // fires at the top of each open iteration; cancel once one gen has opened.
    let calls = Arc::new(AtomicU64::new(0));
    let cancel_hook = cancel.clone();
    let calls_hook = Arc::clone(&calls);
    reg.set_open_barrier(Arc::new(move || {
        if calls_hook.fetch_add(1, Ordering::SeqCst) >= 1 {
            cancel_hook.cancel();
        }
    }));

    let err = reg
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &cancel)
        .expect_err("a mid-rebuild cancellation must abort");
    assert!(matches!(err, WarmError::Cancelled), "got {err:?}");
    assert!(
        calls.load(Ordering::SeqCst) >= 2,
        "the rebuild opened at least one gen before cancelling"
    );
    assert_eq!(
        reg.debug_reader_count(&key()),
        1,
        "no partial swap: only the prior gen-1 remains cached"
    );
}

/// Requirement 7 (cancellation): a pre-cancelled warm lookup does ZERO probe /
/// rebuild work and returns the distinct `Cancelled` variant.
#[test]
fn pre_cancelled_warm_lookup_does_zero_work() {
    let schema = simple_schema();
    let (_temp, _data, table_dir) = build_sstables(&schema, vec![vec![write_row(1, "a", 1, 100)]]);
    let reg = WarmTableRegistry::new();
    let cancel = CancelFlag::new();
    cancel.cancel();
    let err = reg
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &cancel)
        .expect_err("a pre-cancelled lookup must not work");
    assert!(matches!(err, WarmError::Cancelled), "got {err:?}");
    let m = reg.metrics().snapshot();
    assert_eq!(m.reader_opens, 0, "zero readers opened");
    assert_eq!(m.misses + m.hits, 0, "no build, no hit");
}

/// A live-mode second request over an unchanged generation set is a warm hit
/// (Requirement 2 at the registry level) — the authoritative listing matched.
#[test]
fn unchanged_live_set_is_a_warm_hit() {
    let schema = simple_schema();
    let (_temp, _data, table_dir) = build_sstables(&schema, vec![vec![write_row(1, "a", 1, 100)]]);
    let reg = WarmTableRegistry::new();
    let cancel = CancelFlag::new();
    let _ = reg
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &cancel)
        .expect("first");
    let opens = reg.metrics().snapshot().reader_opens;
    let w2 = reg
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &cancel)
        .expect("second");
    assert_eq!(w2.outcome, RefreshOutcome::Unchanged);
    assert_eq!(
        reg.metrics().snapshot().reader_opens,
        opens,
        "unchanged live set → warm hit → zero further opens"
    );
    // Keep the reader set usable (Arc clones).
    let _keep: Vec<Arc<_>> = w2.readers;
}
