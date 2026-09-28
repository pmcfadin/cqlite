//! Issue #4309 — raw SSTable view physical-dump-parity sweep: `test_tomb/*`.
//!
//! This is one third of the sweep #4222 shipped without (its task 6.3,
//! "Physical-dump-parity sweep test", was never built, so the spec's own
//! "physical dump parity is the correctness oracle" requirement had no
//! satisfying test). The shared harness, the full column-contract checklist
//! and every declared gap live in
//! `cqlite-core/tests/support/raw_view_parity.rs` — read that module doc
//! first; it is the authority for what this lane compares and what it
//! deliberately does not.
//!
//! # Fixtures — ALL NINE `test_tomb` tables
//! (`test-data/schemas/tombstone-parity.cql`, Cassandra 5.0 `nb` BIG, LZ4)
//!
//! | Table | What it contributes to the sweep |
//! |---|---|
//! | `gc_before_boundary` | TTL'd cells whose `localDeletionTime` lands on / past a documented boundary — `<col>_ttl`, `<col>_local_deletion_time`, `row_ttl`, `row_liveness_expires_at`, plus a no-TTL negative control row |
//! | `tombstone_histogram` | row + cell tombstones at `gc_grace_seconds = 0` |
//! | `skipped_partition_delete` | a cross-generation partition tombstone AND a tombstone-only partition (gen-2 has ZERO rows) |
//! | `resurrection_gc0` | gen-2 row/cell/partition deletes shadowing gen-1 at `gc_grace = 0` |
//! | `resurrection_gc_positive` | the same shapes with tombstones retained |
//! | `dropped_regular_col` | a per-cell dropped regular column across two generations |
//! | `dropped_static_col` | a dropped STATIC column — exercises the `static_block` modelling |
//! | `static_with_tombstones` | a live static cell beside row / cell / RANGE tombstones in one partition |
//! | `wide_range_tombstone` | ~3 000 physical rows in ONE partition with three range-tombstone pairs at index-block edges |
//!
//! # Fixture discipline (#3220/#3121)
//!
//! `static_with_tombstones`'s `Data.db` IS git-committed, so its case is
//! `must_run` and PANICS when absent. The other eight are fetch-only (the
//! repo commits only their JSONL/`.txt`/`.crc32` sidecars), so each SKIPs
//! cleanly on its own when no candidate root carries its bytes —
//! `CQLITE_REQUIRE_FIXTURES=1` (#972) turns that SKIP into a panic. Roots are
//! resolved PER TABLE and every case asserts on its own; there is no
//! suite-wide `assert!(ran > 0)`.
//!
//! # What a green gate does and does not certify
//!
//! `raw_view_parity.rs`'s module doc has the full statement (roborev finding
//! R1, issue #4309): the full gate's `core-tests` component runs WITHOUT
//! `CQLITE_REQUIRE_FIXTURES=1`, so on a box without the fetched corpus every
//! fetch-only case here SKIPs and certifies nothing. Cite a strict-mode run
//! against a fetched corpus, not a gate PASS, when claiming this family was
//! swept. The gate-wiring remedy is issue #4311.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/raw_view_parity.rs"]
mod raw_view_parity;

use raw_view_parity::{assert_raw_view_matches_golden, Discipline, FixtureSpec};

/// Every `test_tomb` table shares one schema, one partition-key shape and —
/// with a single git-committed exception — one fixture discipline.
const fn tomb(table: &'static str, discipline: Discipline) -> FixtureSpec {
    FixtureSpec {
        keyspace: "test_tomb",
        table,
        schema_file: "tombstone-parity.cql",
        partition_key_columns: &["pk"],
        discipline,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_before_boundary_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&tomb("gc_before_boundary", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "cell_timestamp",
            "cell_ttl",
            "cell_local_deletion_time",
            "row_timestamp",
            "row_ttl",
            "row_liveness_expires_at",
            "entry:row",
        ]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tombstone_histogram_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&tomb("tombstone_histogram", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "cell_timestamp",
            "cell_tombstone",
            "cell_local_deletion_time",
            "row_timestamp",
            "row_tombstone",
            "row_local_deletion_time",
            "row_deletion_timestamp",
            "entry:row",
        ]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skipped_partition_delete_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&tomb("skipped_partition_delete", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "cell_timestamp",
            "row_timestamp",
            "partition_deletion_time",
            "partition_deletion_timestamp",
            "entry:partition_deletion",
            "shape:multi_generation",
        ]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resurrection_gc0_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&tomb("resurrection_gc0", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "cell_timestamp",
            "cell_tombstone",
            "cell_local_deletion_time",
            "row_timestamp",
            "row_tombstone",
            "row_local_deletion_time",
            "row_deletion_timestamp",
            "partition_deletion_time",
            "partition_deletion_timestamp",
            "entry:partition_deletion",
            "shape:multi_generation",
        ]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resurrection_gc_positive_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&tomb("resurrection_gc_positive", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "cell_timestamp",
            "cell_tombstone",
            "cell_local_deletion_time",
            "row_timestamp",
            "row_tombstone",
            "row_local_deletion_time",
            "row_deletion_timestamp",
            "partition_deletion_time",
            "partition_deletion_timestamp",
            "entry:partition_deletion",
            "shape:multi_generation",
        ]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_regular_col_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&tomb("dropped_regular_col", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "cell_timestamp",
            "row_timestamp",
            "entry:row",
            "shape:multi_generation",
        ]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_static_col_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&tomb("dropped_static_col", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "cell_timestamp",
            "row_timestamp",
            "entry:static_block",
            "shape:multi_generation",
        ]);
}

/// `must_run`: this fixture's `Data.db` is git-committed (issue #3121), so
/// "not found" means a broken checkout, never an unfetched corpus.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_with_tombstones_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&tomb("static_with_tombstones", Discipline::GitCommitted))
        .await
        .require_observed(&[
            "cell_timestamp",
            "cell_tombstone",
            "cell_local_deletion_time",
            "row_timestamp",
            "row_tombstone",
            "row_local_deletion_time",
            "row_deletion_timestamp",
            "bound_inclusive",
            "range_deletion_time",
            "range_deletion_timestamp",
            "entry:static_block",
            "entry:range_tombstone_bound",
        ]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wide_range_tombstone_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&tomb("wide_range_tombstone", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "cell_timestamp",
            "row_timestamp",
            "bound_inclusive",
            "range_deletion_time",
            "range_deletion_timestamp",
            "entry:range_tombstone_bound",
        ]);
}
