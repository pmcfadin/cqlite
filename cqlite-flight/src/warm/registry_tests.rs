//! Registry integration tests over REAL in-process SSTables (issue #2310).
//!
//! These prove the spec requirements the registry owns end to end: the
//! generation-identity key across snapshot dirs (Req 1), fail-closed rebuild
//! (Req 4), LRU + removed-on-disk eviction (Req 5), and the #2345 UDT-registry
//! guard. Loaded via `#[path]` from `registry.rs` to keep that file within the
//! campsite threshold. Plain `#[test]`s: `build_sstables` drives its own runtime,
//! and `warm_readers` builds its own — nesting a `#[tokio::test]` runtime would
//! panic.

use std::sync::Arc;

use crate::cancel::CancelFlag;
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

/// The #2345 UDT-registry guard: every reader the registry hands out was opened
/// WITH its UDT registry resolved (never the #1234 silent-`Blob` trap).
#[test]
fn warm_readers_are_udt_registry_aware() {
    let schema = simple_schema();
    let (_temp, _data, table_dir) = build_sstables(&schema, vec![vec![write_row(1, "a", 1, 100)]]);
    let reg = WarmTableRegistry::new();
    let w = reg
        .warm_readers(&key(), ddl(), &schema, &table_dir, None, &CancelFlag::new())
        .expect("warms");
    assert!(!w.readers.is_empty(), "opened at least one reader");
    for r in &w.readers {
        assert!(
            r.has_udt_registry(),
            "a warm reader must carry a UDT registry before it is shared (#2345)"
        );
    }
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

/// Requirement 5 (LRU byte budget): warming distinct tables past a tiny budget
/// evicts the least-recently-used entry; an evicted entry re-parses on its next
/// request (a fresh miss/open).
#[test]
fn lru_evicts_when_over_budget() {
    let schema = simple_schema();
    // Two separate tables, each one SSTable. `warm_readers` keys per TableKey.
    let (_t_a, _d_a, dir_a) = build_sstables(&schema, vec![vec![write_row(1, "a", 1, 100)]]);
    let (_t_b, _d_b, dir_b) = build_sstables(&schema, vec![vec![write_row(1, "b", 1, 100)]]);

    // Budget = 1 byte: any second generation forces the first out.
    let reg = WarmTableRegistry::with_budget(1);
    let cancel = CancelFlag::new();
    let key_a = TableKey::new(KS, "table_a");
    let key_b = TableKey::new(KS, "table_b");

    reg.warm_readers(&key_a, ddl(), &schema, &dir_a, None, &cancel)
        .expect("warm A");
    reg.warm_readers(&key_b, ddl(), &schema, &dir_b, None, &cancel)
        .expect("warm B evicts A");
    assert!(
        reg.metrics().snapshot().evicts >= 1,
        "warming B over a 1-byte budget evicts LRU entry A"
    );

    // A must re-parse (a fresh miss + open) since it was evicted.
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
