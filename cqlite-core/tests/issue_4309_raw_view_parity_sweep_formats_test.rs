//! Issue #4309 — raw SSTable view physical-dump-parity sweep: the FORMAT and
//! COMPRESSION axes the `test_tomb`/`test_deltas` families cannot reach.
//!
//! One third of the sweep #4222 shipped without (its task 6.3 was never
//! built). The shared harness, the full column-contract checklist and every
//! declared gap live in `cqlite-core/tests/support/raw_view_parity.rs` — read
//! that module doc first; it is the authority for what this lane compares and
//! what it deliberately does not.
//!
//! # Fixtures
//!
//! | Fixture | Axis it adds |
//! |---|---|
//! | `test_da.wide_table` (`wide-table-bti.cql`) | the **BTI (`da`)** format — `raw_view/point.rs` resolves a partition by TRIE DESCENT here, not by the BIG index; 3 partitions × 300 clustering rows |
//! | `test_comp.lz4_table` (`compression-parity.cql`) | a **COMPRESSED** (`LZ4Compressor`, 16 KiB chunks) BIG table read through chunk stitching — 600 rows in one partition |
//! | `test_comp.uncompressed_table` (same schema) | the **UNCOMPRESSED** BIG table (`compression = {'enabled': false}`, so NO `CompressionInfo.db`; its only checksum sidecar is `Digest.crc32` — this fixture ships no `CRC.db`) whose compaction-stream path `issue_4222_raw_view_uncompressed_failclosed_test.rs` investigated — every write time must be the real on-disk one, never `from_legacy_value`'s fabricated zero |
//! | `test_compactionparity.live_clustering` (`compaction-parity.cql`) | the compaction-parity corpus the raw view's JOIN-substitute correlation lane uses, swept end-to-end; also the only fixture whose partition-key column is not named `pk` |
//!
//! # Fixture discipline (#3220/#3121)
//!
//! All four `Data.db` binaries are **git-committed**, so every case here is
//! `must_run`: absence means a broken checkout, never an unfetched corpus, and
//! the harness PANICS rather than skipping. Roots are still resolved PER TABLE
//! (`sstables_root_for_table`), never by keyspace.
//!
//! # What a green gate certifies HERE — this lane is the exception
//!
//! Unlike the `tomb` and `deltas` lanes, **every case in this file is
//! `Discipline::GitCommitted`**, so the full gate's `core-tests` component
//! certifies all four WITHOUT a fetched corpus, and
//! `CQLITE_REQUIRE_FIXTURES=1` changes nothing here — there is no fetch-only
//! case for it to promote. A green gate IS sufficient evidence that this
//! family was swept.
//!
//! The strict-mode caveat (roborev finding R1, issue #4309; full statement in
//! `raw_view_parity.rs`'s module doc) is scoped to the two lanes that DO have
//! fetch-only cases — 16 of the 22 cases across `tomb` and `deltas`. Do not
//! restate it here: applied to this lane it is not merely redundant but
//! FALSE, and it would tell a reader to discount a gate PASS for the one
//! family the gate fully covers. The gate-wiring remedy for the other two
//! lanes is issue #4311.

#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

#[path = "support/raw_view_parity.rs"]
mod raw_view_parity;

use raw_view_parity::{assert_raw_view_matches_golden, Discipline, FixtureSpec};

/// The BTI (`da`) axis: the point-read producer resolves each partition by
/// trie descent rather than through a BIG `Index.db`, so a mapping defect
/// reachable only on that path would be invisible to every other lane.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bti_wide_table_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&FixtureSpec {
        keyspace: "test_da",
        table: "wide_table",
        schema_file: "wide-table-bti.cql",
        partition_key_columns: &["pk"],
        discipline: Discipline::GitCommitted,
    })
    .await
    .require_observed(&["cell_timestamp", "row_timestamp", "entry:row"]);
}

/// The COMPRESSED-BIG axis: a chunk-stitched read must surface the same
/// physical metadata as an uncompressed one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lz4_table_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&FixtureSpec {
        keyspace: "test_comp",
        table: "lz4_table",
        schema_file: "compression-parity.cql",
        partition_key_columns: &["pk"],
        discipline: Discipline::GitCommitted,
    })
    .await
    .require_observed(&["cell_timestamp", "row_timestamp", "entry:row"]);
}

/// The UNCOMPRESSED-BIG axis: the compaction stream has a non-stitching
/// fallback that would collapse every live cell's write timestamp to a
/// fabricated zero (issue #28). Every `<col>_timestamp` here is compared
/// against the golden byte-exact, so that fallback cannot pass unnoticed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncompressed_table_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&FixtureSpec {
        keyspace: "test_comp",
        table: "uncompressed_table",
        schema_file: "compression-parity.cql",
        partition_key_columns: &["pk"],
        discipline: Discipline::GitCommitted,
    })
    .await
    .require_observed(&["cell_timestamp", "row_timestamp", "entry:row"]);
}

/// The compaction-parity corpus, and the sweep's only non-`pk` partition-key
/// column name (`id`) — so the harness's key handling is exercised rather
/// than assumed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_clustering_matches_the_sstabledump_golden() {
    assert_raw_view_matches_golden(&FixtureSpec {
        keyspace: "test_compactionparity",
        table: "live_clustering",
        schema_file: "compaction-parity.cql",
        partition_key_columns: &["id"],
        discipline: Discipline::GitCommitted,
    })
    .await
    .require_observed(&["cell_timestamp", "row_timestamp", "entry:row"]);
}
