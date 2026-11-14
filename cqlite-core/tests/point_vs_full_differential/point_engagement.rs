//! Issue #4255: prove the POINT leg of the point-vs-full differential really is a
//! cold, partition-targeted lookup — not the full scan's path, nor a replay of
//! entries the full leg left in a process-global cache.
//!
//! Two properties the equality assertion alone cannot see (both pass when the
//! "point" leg quietly served the same bytes the full leg did):
//!
//! 1. **Engagement.** `ReadPathMode::Point` fails closed when a query CANNOT be
//!    point-served, but nothing asserted the point path actually SERVED each probe.
//!    [`engagement_violation`] requires `metadata.access_path` to be targeted
//!    (the `read_path_forcing_e2e.rs` pattern) and names the table + key on failure.
//! 2. **Cold start.** `GlobalKeyOffsetCache` is a process-wide static keyed by
//!    on-disk generation identity, so a warm entry would let the point leg skip
//!    the Index.db / BTI trie descent it exists to stress. [`point_leg_cold`]
//!    invalidates the cache before every point run and returns the cache-counter
//!    delta of that run, so a test can PROVE the first probe was a MISS.
//!
//! The counters are process-global, so every test in this target that opens a
//! `Database` is `#[serial]` (a concurrent sibling would perturb the deltas).

use serial_test::serial;

use cqlite_core::config::ReadPathMode;
use cqlite_core::query::result::QueryResult;
use cqlite_core::storage::cache::GlobalKeyOffsetCache;
use cqlite_core::Database;

use super::{describe_search, open_db, pin_read_clock, schema_path, sstables_root_for_table};

/// Process-global key-offset-cache counters at one instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CacheCounters {
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
}

impl CacheCounters {
    pub(super) fn now() -> Self {
        let c = GlobalKeyOffsetCache::global();
        Self {
            hits: c.hit_count(),
            misses: c.miss_count(),
            entries: c.len(),
        }
    }

    /// `(hits, misses, entries gained)` since `earlier` (saturating: an
    /// invalidation between the two snapshots can only shrink `entries`).
    pub(super) fn since(self, earlier: Self) -> (u64, u64, usize) {
        (
            self.hits.saturating_sub(earlier.hits),
            self.misses.saturating_sub(earlier.misses),
            self.entries.saturating_sub(earlier.entries),
        )
    }
}

/// `Err` naming the query (table + key) unless the query was served by a
/// partition-TARGETED access path. A missing `access_path` is also a violation: a
/// result that cannot say how it was served proves nothing.
pub(super) fn engagement_violation(query: &str, result: &QueryResult) -> Result<(), String> {
    match result.metadata.access_path.as_ref() {
        Some(path) if path.is_targeted() && !path.is_full_scan() => Ok(()),
        Some(path) => Err(format!(
            "ENGAGEMENT FAIL: the point leg did not take a targeted path for `{query}` \
             (access_path = {path:?}) — the differential would be comparing the full path \
             against itself"
        )),
        None => Err(format!(
            "ENGAGEMENT FAIL: no access_path recorded for the point-leg query `{query}`"
        )),
    }
}

/// Run `query` on the point DB COLD: the process-global key-offset cache is emptied
/// first (so nothing the full leg or an earlier probe left can serve it), and the
/// cache-counter delta of the run is returned alongside the result. The engagement
/// assertion is applied here, so every point leg is checked at one choke point.
pub(super) async fn point_leg_cold(
    point_db: &Database,
    query: &str,
) -> Result<(QueryResult, (u64, u64, usize)), String> {
    GlobalKeyOffsetCache::global().invalidate_all();
    let before = CacheCounters::now();
    let result = point_db
        .execute(query)
        .await
        .map_err(|e| format!("point path failed for `{query}`: {e}"))?;
    let delta = CacheCounters::now().since(before);
    engagement_violation(query, &result)?;
    Ok((result, delta))
}

/// The first point probe of a case must start with a key-offset-cache MISS: after
/// [`point_leg_cold`]'s invalidation, a probe that saw zero misses was served from
/// somewhere other than a cold index descent (a warm hit, or no cache consultation
/// at all, in which case "cold" is unproven). Hits AFTER that first miss are
/// legitimate and expected: the BTI path consults the cache for the prune and again
/// for the seek, and the first consult has just populated it. `delta` is
/// `(hits, misses, entries gained)` from [`point_leg_cold`].
pub(super) fn cold_miss_violation(
    label: &str,
    query: &str,
    delta: (u64, u64, usize),
) -> Result<(), String> {
    let (hits, misses, entries) = delta;
    if misses >= 1 && entries >= 1 {
        return Ok(());
    }
    Err(format!(
        "COLD-START FAIL: {label}: the first point probe `{query}` saw {hits} hit(s), \
         {misses} miss(es), {entries} new entr(ies) in the key-offset cache — expected >=1 \
         miss and >=1 populated entry after invalidation"
    ))
}

// ---------------------------------------------------------------------------
// Seeded checks: the assertions above CAN fail (a guard whose failing branch is
// never exercised is indistinguishable from one that cannot fire — #3220).
// ---------------------------------------------------------------------------

#[test]
fn cold_miss_violation_fires_on_a_warm_hit_and_on_no_cache_traffic() {
    assert!(cold_miss_violation("t", "q", (0, 1, 1)).is_ok());
    assert!(
        cold_miss_violation("t", "q", (1, 1, 1)).is_ok(),
        "a BTI probe hits on its own second consult after the first miss populated it"
    );
    assert!(
        cold_miss_violation("t", "q", (1, 0, 0)).is_err(),
        "a pure hit means warm"
    );
    assert!(
        cold_miss_violation("t", "q", (0, 0, 0)).is_err(),
        "no miss means the probe never consulted the cache, so cold is unproven"
    );
    assert!(
        cold_miss_violation("t", "q", (0, 1, 0)).is_err(),
        "a miss that populated no entry never proved the cold descent filled the cache"
    );
}

/// A probe forced through a NON-targeted shape (the FULL path over a
/// `WHERE pk = <k>` query) must red the engagement assertion, naming the table and
/// key. Uses a real committed fixture, so it exercises the real `access_path`
/// plumbing, not a stub.
#[tokio::test]
#[serial]
async fn engagement_assertion_reds_a_probe_served_by_the_full_path() {
    const KS: &str = "test_tomb";
    let case = super::CORPUS
        .iter()
        .find(|c| c.keyspace == KS)
        .expect("corpus has a test_tomb case");
    let root = sstables_root_for_table(case.keyspace, case.table)
        .unwrap_or_else(|| panic!("{}", describe_search(case.keyspace, case.table)));
    let schema = schema_path(case.schema).expect("committed schema");
    let _clock = pin_read_clock();

    let full_db = open_db(&root, &schema, case.keyspace, ReadPathMode::Full)
        .await
        .expect("open full db");
    let point_db = open_db(&root, &schema, case.keyspace, ReadPathMode::Point)
        .await
        .expect("open point db");
    let keys = super::discover_pk_ints(&full_db, case.keyspace, case.table, case.pk_column)
        .await
        .expect("discovery");
    let key = *keys.first().expect("fixture has a live partition");
    let query = format!(
        "SELECT * FROM {}.{} WHERE {} = {key}",
        case.keyspace, case.table, case.pk_column
    );

    // Control: the real point leg satisfies the assertion.
    let (_, _) = point_leg_cold(&point_db, &query)
        .await
        .expect("a genuine point probe is targeted");

    // Seeded: the same query served by the FULL path must be rejected, by name.
    let full_result = full_db.execute(&query).await.expect("full SELECT");
    let err = engagement_violation(&query, &full_result)
        .expect_err("a full-path result must fail the engagement assertion");
    assert!(
        err.contains("ENGAGEMENT FAIL")
            && err.contains(case.table)
            && err.contains(&format!("= {key}")),
        "failure must name the table and key, got: {err}"
    );
}
