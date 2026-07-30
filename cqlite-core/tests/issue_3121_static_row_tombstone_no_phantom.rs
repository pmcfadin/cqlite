//! Issue #3121 — a ROW-DELETED clustering row in a STATIC-bearing partition must
//! never resurface as a phantom row carrying the partition's static value.
//!
//! ## The property
//!
//! `test_tomb.static_with_tombstones` (real Apache Cassandra 5.0 `nb`, single
//! flush, `test-data/schemas/tombstone-parity.cql` Table 8) holds ONE partition
//! (`pk=1`) with a live static cell (`stat_col='surviving_static'`) plus:
//!
//! | ck | on-disk shape                                              | in a SELECT   |
//! |----|------------------------------------------------------------|---------------|
//! |  1 | live row, `row_col='row_1'`                                 | present       |
//! |  2 | **pure ROW DELETION, ZERO cells**                           | **ABSENT**    |
//! |  3 | live row marker whose only regular cell is a cell tombstone | present, NULL |
//! |4,5 | covered by a range-tombstone bound pair (no row bodies)     | absent        |
//! |  6 | live row, `row_col='row_6'`                                 | present       |
//!
//! So `SELECT pk, ck, stat_col, row_col ... WHERE pk = 1` returns EXACTLY
//! `ck = 1, 3, 6` — every one of them carrying the live static value — and NEVER
//! a `ck = 2`.
//!
//! ## Cassandra authority (pinned `cassandra-5.0.8` tag — never a working tree)
//!
//! A static cell can NEVER confer liveness on a clustering row, because the two
//! travel on separate channels:
//!
//! * `db/rows/BaseRowIterator.java` — `public Row staticRow()` delivers the static
//!   row OUT OF BAND; it is not one of the iterator's `Unfiltered`s. So a
//!   clustering row's own `Row` object never contains a static cell.
//! * `db/transform/Filter.java` — `applyToStatic(Row)` and
//!   `applyToRow(row) { return row.purge(DeletionPurger.PURGE_ALL, nowInSec,
//!   enforceStrictLiveness); }` are SEPARATE transformations. A clustering row is
//!   purged from ITSELF and dropped when `purge` returns `null`.
//! * `db/rows/BTreeRow.java` — `purge` → `update(info, deletion, newTree)` returns
//!   `null` when `info.isEmpty() && deletion.isLive() && BTree.isEmpty(newTree)`:
//!   the row's OWN primary-key liveness, OWN deletion, OWN cell btree.
//! * `cql3/statements/SelectStatement.java` — `processPartition()` branches on
//!   `!partition.hasNext()` (clustering rows only) while still reading
//!   `partition.staticRow()`, which is only coherent because the static row is not
//!   counted as a clustering row.
//!
//! Verified against the same fixture with Cassandra 5.0.8's own `sstabledump`
//! golden (`nb-1-big-Data.db.jsonl`, committed next to the fixture): `ck=2` is a
//! `row` with `"deletion_info"` and an EMPTY `cells` array.
//!
//! ## Why THREE tests (the real content of this lane, issue #3121 AC3)
//!
//! The rule is implemented ONCE, in `build_display_row_read_path` (decide the
//! row-tombstone question over the row's OWN cells, THEN inject statics), but it
//! is CALLED from three independent decode sites, each behind its own
//! `shadow.is_some()` gate:
//!
//! | # | site (`…/reader/parsing/row_decoder/`) | decoder entry point                 | public surface that selects it |
//! |---|---------------------------------------|-------------------------------------|--------------------------------|
//! | A | `block_emit.rs`                       | `parse_block_emit_with_metadata`    | `SELECT …, WRITETIME(col) …` (a `ProjectionFlags::include_cell_metadata` projection routes to `stitch_and_parse_all_chunks_with_metadata` → `parse_block_with_cell_metadata`) |
//! | B | `block_emit_windowed.rs`              | `parse_block_emit_windowed`         | a partition-targeted `SELECT … WHERE pk = 1` (BIG promoted-index point read, `data_access/big_promoted.rs`) |
//! | C | `timestamp_policy.rs`                 | `TimestampPolicy::on_data_row`      | `Database::execute_streaming` (batched streaming scan → `run_scan_stream_windowed` → `parse_one_partition_with_timestamps`) |
//!
//! Before this lane only site B was exercised by any static + row-tombstone read,
//! so a regression at A or C would have shipped undetected. Each test below is
//! bound to exactly ONE site, and each was VERIFIED to fail when that site's gate
//! is seeded to the old inject-then-decide arm (issue #3121 D3).
//!
//! Run:
//! ```bash
//! env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
//!   cargo test -p cqlite-core --features cli-helpers \
//!   --test issue_3121_static_row_tombstone_no_phantom
//! ```

// `tombstones` deliberately surfaces tombstone rows to the user-facing SELECT
// path (it is the tombstone-inspection build, see `issue_1085_…_parity`), which
// changes the expected result set of every query here; gate it out so these
// assertions only run on the build whose SELECT semantics they describe.
#![cfg(all(
    feature = "state_machine",
    feature = "cli-helpers",
    not(feature = "tombstones")
))]

use std::path::{Path, PathBuf};

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::query::result::{QueryResult, StreamingConfig};
use cqlite_core::types::Value;
use cqlite_core::Database;

// ===========================================================================
// Fixture contract
// ===========================================================================

const KEYSPACE: &str = "test_tomb";
const KEYSPACE_FILTER: &str = "/test_tomb/";
const TABLE: &str = "test_tomb.static_with_tombstones";
/// The fixture directory (Cassandra-assigned table UUID) — pinned so a rename or
/// a regenerated fixture is a loud miss rather than a silent skip.
const FIXTURE_DIR: &str = "static_with_tombstones-4cdb9780702011f1b8f419c9a388d558";
const DATA_DB: &str = "nb-1-big-Data.db";

/// The live static value written by `tombstone-parity.cql` Table 8.
const STATIC_VALUE: &str = "surviving_static";

/// The clustering keys a Cassandra `SELECT` returns for `pk = 1`, IN ORDER.
/// `ck = 2` (the pure row deletion) and `ck = 4,5` (range-tombstoned) are absent.
const EXPECTED_CK: [i32; 3] = [1, 3, 6];

/// The row-deleted clustering key that must NEVER appear.
const PHANTOM_CK: i32 = 2;

/// The clustering key whose only regular cell is a cell tombstone, so `row_col`
/// reads as NULL while the row itself stays present.
const DELETED_CELL_CK: i32 = 3;

// ===========================================================================
// Pinned read clock (never wall-clock)
// ===========================================================================

const TTL_NOW_OVERRIDE_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

/// A PINNED reconciliation instant, far past every `localDeletionTime` in the
/// fixture, so no assertion here can depend on the wall clock.
const PINNED_NOW_SECS: i64 = 1_800_000_000;

/// Pin the read-time clock for this test binary.
///
/// Deliberately SET-ONLY and never removed: `std::env::set_var` is
/// process-global, and every test in this file pins the SAME value, so
/// concurrent tests cannot observe a different clock than they installed. A
/// set/remove pair around each query WOULD race (test threads run in parallel by
/// default) — that is the wall-clock-race class the doctrine calls out.
fn pin_read_clock() {
    std::env::set_var(TTL_NOW_OVERRIDE_ENV, PINNED_NOW_SECS.to_string());
}

// ===========================================================================
// Fixture discovery — SKIP when absent, FAIL LOUDLY when present
// ===========================================================================

/// `true` when `CQLITE_REQUIRE_FIXTURES` is truthy (issue #972 strict mode): every
/// would-be SKIP becomes a PANIC so a CI lane cannot false-pass on missing data.
fn require_fixtures_strict() -> bool {
    matches!(
        std::env::var("CQLITE_REQUIRE_FIXTURES").as_deref(),
        Ok("1") | Ok("true")
    )
}

/// `Some(sstables_root)` when the real fixture binary is on disk.
///
/// Returns `None` — the ONLY sanctioned skip — when the corpus was never fetched:
/// `CQLITE_DATASETS_ROOT` unset, or the whole `test_tomb` keyspace absent. If the
/// keyspace IS present but this pinned fixture (or its `Data.db`) is not, that is a
/// renamed/regenerated/partial corpus rather than an unfetched one, and it PANICS —
/// otherwise a fixture rename would silently turn all three assertions into no-ops.
/// Once the fixture is present, every assertion below is mandatory and a
/// short/empty read FAILS.
fn fixture_root() -> Option<PathBuf> {
    let Ok(datasets_root) = std::env::var("CQLITE_DATASETS_ROOT") else {
        skip_or_panic("CQLITE_DATASETS_ROOT is unset");
        return None;
    };
    let root = PathBuf::from(datasets_root).join("sstables");
    let keyspace_dir = root.join(KEYSPACE);
    if !keyspace_dir.is_dir() {
        skip_or_panic(&format!(
            "keyspace directory absent ({})",
            keyspace_dir.display()
        ));
        return None;
    }
    // Corpus IS fetched: from here a missing fixture is a hard error, never a skip.
    let data_db = keyspace_dir.join(FIXTURE_DIR).join(DATA_DB);
    assert!(
        data_db.is_file(),
        "keyspace {KEYSPACE} is present but the pinned fixture binary is missing: {} — \
         a renamed/regenerated fixture must FAIL here, not silently skip (issue #3121)",
        data_db.display()
    );
    Some(root)
}

/// Skip cleanly (default) or PANIC (`CQLITE_REQUIRE_FIXTURES=1`).
fn skip_or_panic(reason: &str) {
    if require_fixtures_strict() {
        panic!(
            "CQLITE_REQUIRE_FIXTURES=1 but the issue #3121 fixture is unavailable — \
             {reason}; fetch it with `bash test-data/scripts/fetch-datasets.sh`"
        );
    }
    eprintln!(
        "[SKIP] issue #3121: {reason}; fetch the corpus with \
         `bash test-data/scripts/fetch-datasets.sh`"
    );
}

fn schema_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a parent directory")
        .join("test-data")
        .join("schemas")
        .join("tombstone-parity.cql")
}

async fn open_fixture_db(sstables_root: &Path) -> Database {
    let schema = schema_path();
    assert!(
        schema.is_file(),
        "committed schema missing: {}",
        schema.display()
    );
    let result = ingest(IngestionConfig {
        schema_paths: vec![schema],
        data_dir: sstables_root.to_path_buf(),
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(KEYSPACE_FILTER.to_string()),
    })
    .await
    .unwrap_or_else(|e| panic!("ingestion of {} failed: {e}", sstables_root.display()));
    assert!(
        result.schema_load_result.schemas_loaded > 0,
        "no schemas loaded from tombstone-parity.cql — the fixture is present, \
         so this is a real failure, not a skip"
    );
    result.database
}

// ===========================================================================
// The shared assertion (one place, so all three sites are held to it)
// ===========================================================================

/// One decoded result row, reduced to the three facts under test.
struct Observed {
    ck: i32,
    stat_col: Value,
    row_col: Value,
}

fn observe(values: &std::collections::HashMap<std::sync::Arc<str>, Value>) -> Observed {
    let ck = match values.get("ck") {
        Some(Value::Integer(i)) => *i,
        other => panic!("row is missing an INT clustering key `ck`: {other:?}"),
    };
    Observed {
        ck,
        stat_col: values.get("stat_col").cloned().unwrap_or(Value::Null),
        row_col: values.get("row_col").cloned().unwrap_or(Value::Null),
    }
}

/// A user-facing `SELECT` renders a deleted cell as NULL: `Value::Tombstone` is
/// the read path's carrier for "this cell is deleted", which every output writer
/// turns into a null (e.g. `cqlite-cli/src/output/json.rs` maps
/// `Value::Tombstone(_) => JsonValue::Null`). Accept either representation.
fn is_null_valued(v: &Value) -> bool {
    matches!(v, Value::Null | Value::Tombstone(_))
}

/// Assert the FULL Cassandra-semantics contract for `pk = 1` on one decode site.
///
/// `site` names the production site under test so a failure identifies which of
/// the three gates regressed.
fn assert_pk1_contract(site: &str, observed: &[Observed]) {
    let cks: Vec<i32> = observed.iter().map(|o| o.ck).collect();

    // Anti-vacuity: the fixture IS present (checked in `fixture_root`), so a
    // zero-row or short read is a FAILURE, never a pass.
    assert!(
        !observed.is_empty(),
        "{site}: read returned ZERO rows for a fixture that is present on disk — \
         0-rows-when-present is a failure, not a skip"
    );

    // The phantom row itself: the pure row deletion at ck=2 must be ABSENT.
    assert!(
        !cks.contains(&PHANTOM_CK),
        "{site}: row-deleted ck={PHANTOM_CK} resurfaced as a phantom row \
         (issue #3121) — a static cell must never confer liveness on a \
         row-tombstoned clustering row (Cassandra 5.0.8 BTreeRow.purge / \
         transform.Filter.applyToRow). Observed ck sequence: {cks:?}"
    );

    // Exact result set, in clustering order.
    assert_eq!(
        cks,
        EXPECTED_CK.to_vec(),
        "{site}: result set diverges from Cassandra 5.0.8 for {TABLE} pk=1"
    );

    for o in observed {
        // AC4: statics must still reach the rows that DO survive — deciding the
        // row-tombstone question first must not cost the survivors their static.
        assert_eq!(
            o.stat_col,
            Value::text(STATIC_VALUE),
            "{site}: ck={} lost the live static value; suppressing the phantom row \
             must not stop statics from projecting onto surviving rows (#3121 AC4)",
            o.ck
        );

        if o.ck == DELETED_CELL_CK {
            assert!(
                is_null_valued(&o.row_col),
                "{site}: ck={DELETED_CELL_CK}'s only regular cell is a CELL \
                 tombstone, so row_col must read NULL; got {:?}",
                o.row_col
            );
        } else {
            assert_eq!(
                o.row_col,
                Value::text(format!("row_{}", o.ck)),
                "{site}: ck={} lost its live row_col value",
                o.ck
            );
        }
    }
}

fn observed_from(result: &QueryResult) -> Vec<Observed> {
    result.rows.iter().map(|r| observe(&r.values)).collect()
}

// ===========================================================================
// Site A — `block_emit.rs` (`parse_block_emit_with_metadata`)
// ===========================================================================

/// A `WRITETIME(col)` projection sets `ProjectionFlags::include_cell_metadata`,
/// which routes the read to `stitch_and_parse_all_chunks_with_metadata` →
/// `V5CompressedLegacyParser::parse_block_with_cell_metadata` →
/// `parse_block_emit_with_metadata`, i.e. the `block_emit.rs` gate — a decode
/// site NO prior static + row-tombstone test reached (issue #3121 AC3).
///
/// Seeded-divergence verified (D3): forcing that site's `shadow.is_some()` arm to
/// the old inject-then-decide order makes THIS test fail with `ck=2` present.
#[tokio::test]
async fn phantom_row_absent_on_cell_metadata_projection_site_block_emit() {
    let Some(root) = fixture_root() else { return };
    pin_read_clock();
    let db = open_fixture_db(&root).await;

    let result = db
        .execute(&format!(
            "SELECT pk, ck, stat_col, row_col, WRITETIME(row_col) FROM {TABLE} WHERE pk = 1"
        ))
        .await
        .expect("WRITETIME projection SELECT failed");

    assert_pk1_contract(
        "block_emit.rs (cell-metadata projection)",
        &observed_from(&result),
    );
}

// ===========================================================================
// Site B — `block_emit_windowed.rs` (`parse_block_emit_windowed`)
// ===========================================================================

/// A partition-targeted `SELECT … WHERE pk = 1` is served by the BIG
/// promoted-index point read (`data_access/big_promoted.rs`), which decodes the
/// partition body through `parse_block_emit_windowed` — the `block_emit_windowed.rs`
/// gate.
///
/// Seeded-divergence verified (D3): forcing that site's arm to the old
/// inject-then-decide order makes THIS test fail with `ck=2` present.
#[tokio::test]
async fn phantom_row_absent_on_point_read_site_block_emit_windowed() {
    let Some(root) = fixture_root() else { return };
    pin_read_clock();
    let db = open_fixture_db(&root).await;

    let result = db
        .execute(&format!(
            "SELECT pk, ck, stat_col, row_col FROM {TABLE} WHERE pk = 1"
        ))
        .await
        .expect("partition-targeted SELECT failed");

    assert_pk1_contract(
        "block_emit_windowed.rs (point read)",
        &observed_from(&result),
    );
}

// ===========================================================================
// Site C — `timestamp_policy.rs` (`TimestampPolicy::on_data_row`)
// ===========================================================================

/// `Database::execute_streaming` drives the batched streaming scan
/// (`scan_stream_batched` → `run_scan_stream_windowed` →
/// `parse_one_partition_with_timestamps`), whose per-row policy is
/// `TimestampPolicy::on_data_row` — the `timestamp_policy.rs` gate, the third
/// decode site and likewise previously unexercised for this shape.
///
/// The fixture is LZ4-compressed, so `requires_chunk_stitching()` holds and the
/// windowed streaming driver (not the block-by-block arm) is selected.
///
/// Seeded-divergence verified (D3): forcing that site's arm to the old
/// inject-then-decide order makes THIS test fail with `ck=2` present.
#[tokio::test]
async fn phantom_row_absent_on_streaming_scan_site_timestamp_policy() {
    let Some(root) = fixture_root() else { return };
    pin_read_clock();
    let db = open_fixture_db(&root).await;

    let mut iter = db
        .execute_streaming(
            &format!("SELECT pk, ck, stat_col, row_col FROM {TABLE}"),
            StreamingConfig::default(),
        )
        .await
        .expect("streaming SELECT failed");

    let mut observed = Vec::new();
    while let Some(row) = iter.next_async().await {
        let row = row.expect("streaming row decode failed");
        observed.push(observe(&row.values));
    }

    // The fixture holds exactly ONE partition (pk=1), so the unrestricted
    // streaming scan's result set IS the pk=1 result set.
    assert_pk1_contract("timestamp_policy.rs (streaming scan)", &observed);
}
