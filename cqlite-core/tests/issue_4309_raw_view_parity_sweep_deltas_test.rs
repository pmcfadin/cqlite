//! Issue #4309 — raw SSTable view physical-dump-parity sweep: `test_deltas/*`.
//!
//! One third of the sweep #4222 shipped without (its task 6.3 was never
//! built). The shared harness, the full column-contract checklist and every
//! declared gap live in `cqlite-core/tests/support/raw_view_parity.rs` — read
//! that module doc first; it is the authority for what this lane compares and
//! what it deliberately does not.
//!
//! # Fixtures — ALL NINE `test_deltas` tables
//! (`test-data/schemas/deltas.cql`, Cassandra 5.0 `nb` BIG, LZ4)
//!
//! | Table | What it contributes to the sweep |
//! |---|---|
//! | `cell_tombstones` | `<col>_tombstone` / `<col>_local_deletion_time` on individually deleted cells |
//! | `row_tombstones` | `row_tombstone` / `row_local_deletion_time` / `row_deletion_timestamp` |
//! | `range_tombstones` | multi-column clustering: a PREFIX bound (`ck2` unspecified ⇒ absent, never fabricated) and MIXED open/closed inclusivity |
//! | `partition_tombstones` | `partition_deletion_time` / `_timestamp` on tombstone-only partitions beside live ones |
//! | `ttl_cells` | `<col>_ttl` / `row_ttl` / `row_liveness_expires_at`, with a no-TTL partition (pk=10) as the negative control |
//! | `static_with_rows` | a `static_block` per partition, including one partition (pk=99) that is static-ONLY |
//! | `collection_ops` | the `<col>_complex_deletion{,_time,_timestamp}` trio on SET / LIST / MAP columns, with per-column markers written at different times |
//! | `partial_updates` | UPDATE-only rows with NO row liveness — `row_timestamp` must be ABSENT while each cell carries its own write time |
//! | `adjacent_ranges` | `range_tombstone_boundary` markers: ONE golden entry closing a range and opening the next at the same clustering position, with DIFFERENT deletion timestamps per side |
//!
//! # Fixture discipline (#3220/#3121)
//!
//! `static_with_rows`'s `Data.db` IS git-committed, so its case is `must_run`
//! and PANICS when absent. The other eight are fetch-only, so each SKIPs
//! cleanly on its own; `CQLITE_REQUIRE_FIXTURES=1` (#972) turns that SKIP into
//! a panic. Roots are resolved PER TABLE and every case asserts on its own.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/raw_view_parity.rs"]
mod raw_view_parity;

use raw_view_parity::{assert_raw_view_matches_golden, Discipline, FixtureSpec};

const fn deltas(table: &'static str, discipline: Discipline) -> FixtureSpec {
    FixtureSpec {
        keyspace: "test_deltas",
        table,
        schema_file: "deltas.cql",
        partition_key_columns: &["pk"],
        discipline,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cell_tombstones_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&deltas("cell_tombstones", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "cell_timestamp",
            "cell_tombstone",
            "cell_local_deletion_time",
            "row_timestamp",
            "entry:row",
        ]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_tombstones_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&deltas("row_tombstones", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "cell_timestamp",
            "row_timestamp",
            "row_tombstone",
            "row_local_deletion_time",
            "row_deletion_timestamp",
            "entry:row",
        ]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn range_tombstones_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&deltas("range_tombstones", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "cell_timestamp",
            "row_timestamp",
            "bound_inclusive",
            "range_deletion_time",
            "range_deletion_timestamp",
            "entry:range_tombstone_bound",
            "shape:prefix_bound",
        ]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_tombstones_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&deltas("partition_tombstones", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "cell_timestamp",
            "row_timestamp",
            "partition_deletion_time",
            "partition_deletion_timestamp",
            "entry:partition_deletion",
        ]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ttl_cells_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&deltas("ttl_cells", Discipline::FetchOnly))
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

/// `must_run`: this fixture's `Data.db` is git-committed (issue #3121).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_with_rows_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&deltas("static_with_rows", Discipline::GitCommitted))
        .await
        .require_observed(&["cell_timestamp", "row_timestamp", "entry:static_block"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collection_ops_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&deltas("collection_ops", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "complex_deletion",
            "complex_deletion_time",
            "complex_deletion_timestamp",
            "row_timestamp",
            "entry:row",
        ]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_updates_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&deltas("partial_updates", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "cell_timestamp",
            "row_timestamp",
            "shape:row_update_without_liveness",
        ]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adjacent_ranges_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&deltas("adjacent_ranges", Discipline::FetchOnly))
        .await
        .require_observed(&[
            "cell_timestamp",
            "row_timestamp",
            "bound_inclusive",
            "range_deletion_time",
            "range_deletion_timestamp",
            "entry:range_tombstone_bound",
            "entry:range_tombstone_boundary",
        ]);
}
