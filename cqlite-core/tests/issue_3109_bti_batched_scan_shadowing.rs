//! Issue #3109: the BATCHED streaming surface must apply read shadowing to BTI
//! (`da`) readers, exactly as the per-row surface does.
//!
//! `SSTableReader::run_scan_stream_batched` had no `bti_partitions_db.is_some()`
//! dispatch, unlike `scan` and `run_scan_stream`. A `da` reader therefore fell into
//! the non-stitching block loop, which decodes through
//! `parse_block_entries_at_now` → the `V5UncompressedOA` STATE MACHINE — a decoder
//! that takes neither `read_shadowing` nor a caller-pinned `now_secs`, so both are
//! silently dropped (`parsing/block_entries.rs`, the "KNOWN FAIL-OPEN SEAM"
//! comment). Net effect: a BTI table streamed through the batched surface was read
//! UNSHADOWED — TTL-expired rows (and, for a fixture that had them,
//! partition/range tombstones) were surfaced where the per-row surface hides them.
//! This is the #1577 class: per-surface decode-posture divergence.
//!
//! The same hole existed on the SEQUENTIAL walk (`sequential_scan`): every caller
//! but one returns early for BTI, and the exception — `iterate_all_partitions`,
//! whose two index branches are both gated on `bti_partitions_db.is_none()` — fell
//! through to the state machine and failed outright. All four surfaces are asserted
//! to agree here, so the next one added has to join the agreement or fail.
//!
//! # Oracles, and how `now` is pinned
//!
//! Both fixtures are REAL Cassandra 5.0.2 BTI (`da`) SSTables with committed
//! sstabledump goldens; the goldens are re-read here (never hardcoded blind) so a
//! corpus regeneration fails loudly instead of silently weakening the test.
//!
//! * **`ttl_shadowing_is_applied_at_a_caller_pinned_read_clock`** — the TTL arm.
//!   `test_da.ttl_table`'s golden records both rows written with `"ttl": 86400`
//!   and `"expires_at": "2026-06-11T16:17:37Z"`, so Cassandra's own `SELECT`
//!   semantics are unambiguous: live before that instant, gone after it. `now` is
//!   pinned through the EXPLICIT, non-debug-gated `now_secs` parameter of
//!   [`SSTableReader::open_query_row_stream`], whose full-ring arm
//!   (`token_bound = None`) drives exactly the batched surface this issue fixed
//!   (`scan_stream_batched_admitted` → `run_scan_stream_batched` → the BTI
//!   dispatch). An explicit parameter is deterministic in a `--release` test build
//!   too; the ambient `CQLITE_TTL_NOW_OVERRIDE_SECS` seam is `debug_assertions`-only
//!   and would be IGNORED there, silently falling back to the wall clock and turning
//!   the "before expiry" phase into a time bomb that fires the moment the fixture's
//!   TTL elapses (it already has).
//!
//! * **`all_bti_scan_surfaces_agree_row_for_row`** — the cross-surface arm, which
//!   needs no clock pin at all: "every surface returns the same rows" is true under
//!   ANY clock. Its non-vacuous fixture is `test_da.simple_table` (no TTL, so its
//!   rows are live forever); `ttl_table` is then checked for agreement only, with no
//!   count pinned, so it cannot rot as its TTL elapses.
//!
//! Requires the gitignored `Data.db` binaries; SKIPs (never passes with zero rows)
//! when absent. `CQLITE_DATASETS_ROOT` is honored first, else the in-repo
//! `test-data/datasets` corpus is used, so a plain local `cargo test` still runs
//! this — the only direct regression test for the #3109 dispatch.

#![cfg(feature = "state_machine")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{parse_cql_schema, TableSchema};
use cqlite_core::storage::scan_cancel::ScanCancel;
use cqlite_core::storage::sstable::reader::QueryRowBatch;
use cqlite_core::storage::sstable::{SSTableManager, SSTableReader};
use cqlite_core::types::{ScanRow, TableId};
use cqlite_core::{Config, RowKey};

const KEYSPACE: &str = "test_da";
const TTL_TABLE: &str = "ttl_table";
const LIVE_TABLE: &str = "simple_table";
const SSTABLE_PREFIX: &str = "da-2-bti";

/// The TTL fixture's expiry instant, as recorded by Cassandra's own sstabledump in
/// the committed golden. Asserted against the golden below, so a regenerated corpus
/// fails loudly rather than quietly invalidating the two pins.
const GOLDEN_EXPIRES_AT: &str = "2026-06-11T16:17:37Z";
const GOLDEN_EXPIRES_AT_EPOCH: i64 = 1_781_194_657;

/// 2026-06-11T00:00:00Z — strictly BEFORE the expiry, and after every write in the
/// fixture (`tstamp` 2026-06-10T16:17:37Z): every row is live.
const NOW_BEFORE_EXPIRY: i64 = 1_781_136_000;
/// 2026-07-02T00:00:00Z — the pin the sibling query-semantics cases use, well AFTER
/// the expiry: every row is expired.
const NOW_AFTER_EXPIRY: i64 = 1_782_950_400;

/// Repo root = the parent of this crate's manifest dir (`<repo>/cqlite-core`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cqlite-core has a parent repo dir")
        .to_path_buf()
}

/// The `<table>-<cfid>` generation dir, requiring a real `Data.db` so a JSONL-only
/// checkout SKIPs rather than passing with zero rows.
///
/// `CQLITE_DATASETS_ROOT` is preferred (a worktree has no gitignored binaries and
/// must point at the main checkout), with the in-repo corpus as a fallback so a
/// plain local `cargo test` does not silently skip the only direct regression test
/// for this fix.
fn fixture_dir(table: &str) -> Option<PathBuf> {
    let roots = std::env::var("CQLITE_DATASETS_ROOT")
        .ok()
        .map(PathBuf::from)
        .into_iter()
        .chain(std::iter::once(repo_root().join("test-data/datasets")));
    for root in roots {
        let keyspace_dir = root.join("sstables").join(KEYSPACE);
        let Ok(entries) = std::fs::read_dir(&keyspace_dir) else {
            continue;
        };
        let mut candidates: Vec<PathBuf> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(&format!("{table}-")))
                    .unwrap_or(false)
            })
            .collect();
        candidates.sort();
        if let Some(dir) = candidates
            .into_iter()
            .find(|p| p.join(format!("{SSTABLE_PREFIX}-Data.db")).is_file())
        {
            return Some(dir);
        }
    }
    None
}

/// Number of physical ROWS the committed sstabledump golden records.
///
/// Counts `"type": "row"` entries, NOT JSONL lines: a line is a PARTITION, and the
/// surfaces this is compared against count ROWS. They coincide only while every
/// partition in the fixture holds exactly one row — a regenerated corpus with a
/// multi-row partition would otherwise silently assert the wrong number. Same
/// counting rule as `query_semantics_oracle_parity.rs`.
///
/// The oracle is Cassandra's own dump of Cassandra-written bytes — never CQLite
/// output (#3042).
fn golden_row_count(dir: &Path) -> usize {
    let golden = dir.join(format!("{SSTABLE_PREFIX}-Data.db.jsonl"));
    let text = std::fs::read_to_string(&golden)
        .unwrap_or_else(|e| panic!("read golden {}: {e}", golden.display()));
    let rows = text.matches("\"type\": \"row\"").count();
    assert!(
        rows > 0,
        "golden {} must record at least one physical row (no vacuous pass)",
        golden.display()
    );
    rows
}

/// [`golden_row_count`] for the TTL fixture, additionally asserting the TTL/expiry
/// facts the two `now` pins are derived from.
fn golden_ttl_row_count(dir: &Path) -> usize {
    let golden = dir.join(format!("{SSTABLE_PREFIX}-Data.db.jsonl"));
    let text = std::fs::read_to_string(&golden)
        .unwrap_or_else(|e| panic!("read golden {}: {e}", golden.display()));
    for (i, line) in text.lines().filter(|l| !l.trim().is_empty()).enumerate() {
        assert!(
            line.contains("\"ttl\":") && line.contains(GOLDEN_EXPIRES_AT),
            "golden partition {i} must carry the TTL/expiry this test's pins are \
             derived from (expires_at {GOLDEN_EXPIRES_AT}); the corpus was \
             regenerated — re-derive NOW_BEFORE_EXPIRY / NOW_AFTER_EXPIRY from the \
             new golden. Line: {line}"
        );
    }
    assert!(
        NOW_BEFORE_EXPIRY < GOLDEN_EXPIRES_AT_EPOCH && GOLDEN_EXPIRES_AT_EPOCH < NOW_AFTER_EXPIRY,
        "the two pins must straddle the golden's expiry instant"
    );
    golden_row_count(dir)
}

/// Schemas for the two `test_da` fixtures (match `test-data/schemas/da-test.cql`).
fn table_schema(table: &str) -> TableSchema {
    let cql = match table {
        TTL_TABLE => format!(
            "CREATE TABLE {KEYSPACE}.{TTL_TABLE} (\
                 id UUID PRIMARY KEY, data TEXT, expiring_value INT);"
        ),
        LIVE_TABLE => format!(
            "CREATE TABLE {KEYSPACE}.{LIVE_TABLE} (\
                 id UUID PRIMARY KEY, name TEXT, age INT, salary BIGINT, \
                 active BOOLEAN, created TIMESTAMP);"
        ),
        other => panic!("no schema for test_da.{other}"),
    };
    parse_cql_schema(&cql).expect("parse test_da schema")
}

async fn open_manager(keyspace_dir: &Path) -> SSTableManager {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    SSTableManager::new(
        keyspace_dir,
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("open SSTableManager")
}

async fn open_reader(data_db: &Path) -> Arc<SSTableReader> {
    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let reader = SSTableReader::open(data_db, &config, platform)
        .await
        .expect("open BTI SSTable reader");
    assert!(
        reader.is_bti(),
        "fixture {} must open as a BTI (`da`) reader, else the #3109 dispatch is \
         never exercised",
        data_db.display()
    );
    Arc::new(reader)
}

type Entry = (Vec<u8>, ScanRow);

fn snap(key: RowKey, row: ScanRow) -> Entry {
    (key.as_bytes().to_vec(), row)
}

/// Drive the batched BTI surface at an EXPLICITLY pinned read clock.
///
/// [`SSTableReader::open_query_row_stream`] with `token_bound = None` takes the
/// full-ring arm, which is `scan_stream_batched_admitted(.., Some(now_secs))` —
/// i.e. `run_scan_stream_batched`, whose BTI dispatch #3109 added. `now_secs` is a
/// plain function parameter, honored identically in debug and release builds.
fn batched_rows_at_pinned_now(reader: Arc<SSTableReader>, schema: &TableSchema, now: i64) -> usize {
    let mut stream = reader
        .open_query_row_stream(schema.clone(), None, now, ScanCancel::new())
        .expect("open_query_row_stream");
    let mut rows = 0usize;
    while let Some(msg) = stream.next_batch() {
        match msg.expect("query row batch Ok") {
            QueryRowBatch::Rows(batch) => rows += batch.len(),
            QueryRowBatch::Unsupported => panic!(
                "the full-ring query row stream must serve this BTI reader through the \
                 batched scan surface; `Unsupported` means the pinned-clock arm under \
                 test was never driven (the assertions below would be vacuous)"
            ),
        }
    }
    rows
}

/// Issue #3109 / #1741: the batched streaming surface applies read shadowing to a
/// BTI (`da`) reader at the clock its CALLER pinned — live rows before the
/// fixture's TTL expiry, nothing after it.
///
/// Pre-#3109 this surface routed `da` readers into the block loop, whose
/// `V5UncompressedOA` state machine drops BOTH `read_shadowing` and `now_secs`: the
/// expired phase surfaced rows that `scan` hides (or failed outright on this
/// schema-required fixture), and the live phase could not honor the pin at all.
///
/// Both phases are pinned through an explicit parameter, so this test is
/// deterministic in a `--release` build too — it can never decay into reading the
/// wall clock.
#[tokio::test]
async fn ttl_shadowing_is_applied_at_a_caller_pinned_read_clock() {
    let Some(gen_dir) = fixture_dir(TTL_TABLE) else {
        eprintln!(
            "SKIP issue #3109: {KEYSPACE}/{TTL_TABLE} Data.db absent (set \
             CQLITE_DATASETS_ROOT and fetch the datasets)"
        );
        return;
    };
    let physical_rows = golden_ttl_row_count(&gen_dir);
    let schema = table_schema(TTL_TABLE);
    let data_db = gen_dir.join(format!("{SSTABLE_PREFIX}-Data.db"));

    // Non-vacuous arm: at a pinned `now` BEFORE the expiry every physical row is
    // live, so no surface can pass by returning nothing.
    let live = batched_rows_at_pinned_now(open_reader(&data_db).await, &schema, NOW_BEFORE_EXPIRY);
    assert_eq!(
        live, physical_rows,
        "at a pinned now BEFORE expiry ({NOW_BEFORE_EXPIRY}) the batched BTI surface \
         must return all {physical_rows} physical rows the golden records"
    );

    // Shadowing arm: at a pinned `now` AFTER the expiry every row is TTL-expired
    // and a `SELECT` returns NOTHING. This is what the pre-#3109 batched surface got
    // wrong.
    let expired =
        batched_rows_at_pinned_now(open_reader(&data_db).await, &schema, NOW_AFTER_EXPIRY);
    assert_eq!(
        expired, 0,
        "at a pinned now AFTER expiry ({NOW_AFTER_EXPIRY}) the batched BTI surface must \
         hide every TTL-expired row; got {expired} (issue #3109)"
    );
}

async fn collect_per_row(
    manager: &SSTableManager,
    table_id: &TableId,
    schema: &TableSchema,
) -> Vec<Entry> {
    let mut rx = manager
        .scan_stream(table_id, None, None, Some(schema), 256)
        .await
        .expect("scan_stream opens");
    let mut out = Vec::new();
    while let Some(item) = rx.recv().await {
        let (k, v) = item.expect("per-row item Ok");
        out.push(snap(k, v));
    }
    out
}

async fn collect_batched(
    manager: &SSTableManager,
    table_id: &TableId,
    schema: &TableSchema,
) -> Vec<Entry> {
    let mut rx = manager
        .scan_stream_batched(table_id, None, None, Some(schema), 256)
        .await
        .expect("scan_stream_batched opens");
    let mut out = Vec::new();
    while let Some(item) = rx.recv().await {
        for (k, v) in item.expect("batch item Ok") {
            out.push(snap(k, v));
        }
    }
    out
}

/// The MATERIALIZING surface (`SSTableReader::scan`'s BTI branch, reached through
/// the manager) — the reference the two streaming surfaces must reproduce.
async fn collect_scan(
    manager: &SSTableManager,
    table_id: &TableId,
    schema: &TableSchema,
) -> Vec<Entry> {
    manager
        .scan(table_id, None, None, None, Some(schema))
        .await
        .expect("scan succeeds")
        .into_iter()
        .map(|(k, v)| snap(k, v))
        .collect()
}

/// The SEQUENTIAL walk (`SSTableReader::sequential_scan`), reached through
/// `iterate_all_partitions` — the one caller of that walk whose BTI readers are NOT
/// intercepted by an earlier dispatch (both of its index branches are gated on
/// `bti_partitions_db.is_none()`), so it is where a missing dispatch on the walk
/// itself shows up. It resolves its own schema from the SSTable header/registry,
/// so it takes no `schema` argument.
async fn collect_sequential(data_db: &Path) -> Vec<Entry> {
    open_reader(data_db)
        .await
        .iterate_all_partitions()
        .await
        .expect("iterate_all_partitions succeeds")
        .into_iter()
        .map(|(k, v)| snap(k, v))
        .collect()
}

/// Every surface's rows for one table, in one place so a new surface is added to
/// the agreement assertion rather than left to drift (#1577 class).
struct Surfaces {
    scan: Vec<Entry>,
    per_row: Vec<Entry>,
    batched: Vec<Entry>,
    sequential: Vec<Entry>,
}

impl Surfaces {
    /// Assert all surfaces returned the same rows in the same order, and return
    /// that agreed row count.
    fn assert_agree(&self, table: &str) -> usize {
        for (name, rows) in [
            ("per-row", &self.per_row),
            ("batched", &self.batched),
            ("sequential", &self.sequential),
        ] {
            assert_eq!(
                rows.len(),
                self.scan.len(),
                "{table}: the {name} streaming surface returned {} rows but the \
                 materializing `scan` surface returned {} — the BTI decode posture \
                 diverges between surfaces (#3109 / #1577 class)",
                rows.len(),
                self.scan.len()
            );
            for (i, (got, want)) in rows.iter().zip(self.scan.iter()).enumerate() {
                assert_eq!(
                    got.0, want.0,
                    "{table}: {name} row {i}: key mismatch vs scan"
                );
                assert_eq!(
                    got.1, want.1,
                    "{table}: {name} row {i}: value mismatch vs scan"
                );
            }
        }
        self.scan.len()
    }
}

async fn surfaces_for(gen_dir: &Path, table: &str) -> Surfaces {
    let keyspace_dir = gen_dir.parent().expect("generation dir has a parent");
    let data_db = gen_dir.join(format!("{SSTABLE_PREFIX}-Data.db"));
    let schema = table_schema(table);
    let table_id = TableId::from(format!("{KEYSPACE}.{table}").as_str());
    let manager = open_manager(keyspace_dir).await;
    Surfaces {
        scan: collect_scan(&manager, &table_id, &schema).await,
        per_row: collect_per_row(&manager, &table_id, &schema).await,
        batched: collect_batched(&manager, &table_id, &schema).await,
        sequential: collect_sequential(&data_db).await,
    }
}

/// Issue #3109: all four scan surfaces decode a BTI (`da`) reader through the same
/// authoritative trie walk, so they return the same rows in the same order.
///
/// Clock-independent BY CONSTRUCTION — "every surface agrees" is true under any
/// `now`, so nothing here can rot with the wall clock:
///
/// * `simple_table` carries NO TTL, so its rows are live forever and the agreement
///   is permanently non-vacuous (a strictly positive count, byte-compared
///   row-for-row). Pre-#3109 both the batched and the sequential surface took the
///   `V5UncompressedOA` state machine here instead of the trie walk.
/// * `ttl_table` is then checked for AGREEMENT ONLY, with no count pinned: whatever
///   the ambient clock decides about its expiry, no surface may surface a row
///   another one hides. The exact counts for that fixture are pinned separately, at
///   an explicitly pinned clock, by
///   `ttl_shadowing_is_applied_at_a_caller_pinned_read_clock`.
#[tokio::test]
async fn all_bti_scan_surfaces_agree_row_for_row() {
    let Some(live_dir) = fixture_dir(LIVE_TABLE) else {
        eprintln!(
            "SKIP issue #3109: {KEYSPACE}/{LIVE_TABLE} Data.db absent (set \
             CQLITE_DATASETS_ROOT and fetch the datasets)"
        );
        return;
    };
    let physical_rows = golden_row_count(&live_dir);
    let agreed = surfaces_for(&live_dir, LIVE_TABLE)
        .await
        .assert_agree(LIVE_TABLE);
    assert_eq!(
        agreed, physical_rows,
        "the TTL-free {LIVE_TABLE} fixture must decode to all {physical_rows} physical \
         rows the golden records on every surface (non-vacuous arm)"
    );

    // Same agreement on the TTL fixture — agreement only, no count: this arm must
    // stay correct whether or not the fixture's TTL has elapsed.
    if let Some(ttl_dir) = fixture_dir(TTL_TABLE) {
        surfaces_for(&ttl_dir, TTL_TABLE)
            .await
            .assert_agree(TTL_TABLE);
    }
}
