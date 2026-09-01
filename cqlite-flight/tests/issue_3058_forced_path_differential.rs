//! Issue #3058 — the FORCED-PATH DIFFERENTIAL: the same bytes, the same ticket,
//! a PINNED `now`, both arms, byte-identical result sets.
//!
//! This is the primary proof for the one gap code-reading cannot settle (design
//! §gap (b)): the k-way merge arm decides row visibility with `entry_to_row`'s
//! `has_live_data_cell` / row-marker liveness rule (`producer.rs`, issues
//! #2374/#2789), while the single-generation fast arm lets the decoder's own
//! `PartitionShadow` + `build_row_from_scan_cached` suppression decide. Only a
//! differential over the SAME bytes can show the two agree.
//!
//! Every case runs `do_get` twice — once under `CQLITE_FLIGHT_MERGE_PATH=merge`
//! and once under `=bypass` — and asserts the two runs return identical rows,
//! identical column values, and identical ORDER. Each pair also asserts, on the
//! `read_path_probe` markers, that the two runs genuinely took DIFFERENT arms:
//! without that, a differential that silently ran the same arm twice would be a
//! vacuous pass.
//!
//! Covered shapes:
//!   * real Cassandra `nb` fixtures from the query-semantics oracle
//!     (`test_compaction_tombstone_ttl`): range tombstone, row deletion,
//!     expired-TTL cell + expired row-liveness marker, surviving live rows;
//!   * STATIC columns (issues #3095 / #3140): #3095 retired "any static column
//!     refuses the fast arm" and #3140 retired the follow-on "…and it declares a
//!     deletion" guard, so a static-bearing table takes the fast arm either way —
//!     pinned here by `cassandra/static_clustering_shape(declared static, both arms)`
//!     and by the deletion-bearing `statics/select-star`, and end to end by
//!     `issue_3095_flight_static_columns.rs`. What still fails closed is a static
//!     column present ON DISK that the ticket DDL does not declare
//!     (`BypassReason::StaticColumns`, the `@stale-ddl` case);
//!   * a CQLite-written fixture carrying a partition deletion, a range tombstone,
//!     a row deletion, a SIMPLE CELL TOMBSTONE (issue #3094), an expired-TTL cell
//!     and a live-TTL cell, read at TWO pinned `now` values so the pinned clock
//!     itself is pinned;
//!   * feature parity on the fast arm: predicate pushdown, projection, a token
//!     range, and a `max_batch_bytes`-capped stream.
//!
//! ## Isolation
//!
//! `CQLITE_FLIGHT_MERGE_PATH`, `CQLITE_TTL_NOW_OVERRIDE_SECS` and the probe
//! counters are all PROCESS-GLOBAL, so this file holds exactly ONE `#[test]`
//! that runs every case sequentially — the same discipline
//! `query_semantics_flight_parity.rs` uses. Add a case to the list, not a second
//! `#[test]`. That convention is additionally ENFORCED by `PROBE_LOCK` (the same
//! guard the sibling `issue_3058_bypass_path_taken.rs` holds), so a `#[test]`
//! added here later cannot race the env window by accident.
//!
//! ## Residual (stated, not hidden)
//!
//! The gap-(b) shape with NO liveness marker AT ALL — a clustering row written by
//! `UPDATE t SET v=? WHERE pk=? AND ck=?` — is covered here only in its
//! EXPIRED-marker form (`ttl_expired_live`, whose surviving row's visibility comes
//! from a live data cell rather than a live marker, asserted under `SELECT *` AND
//! a PK-only projection). No committed fixture contains a marker-less clustering
//! row, and CQLite's own writer cannot produce one (`merge_row_group` derives
//! liveness from any mutation that writes cells), so that exact byte shape needs a
//! Cassandra-generated fixture — recorded as owed, not silently claimed.
//!
//! ## Fixture contract
//!
//! Dataset-backed cases SKIP cleanly when the fetched corpus is absent, UNLESS
//! `CQLITE_REQUIRE_FIXTURES=1` (which the gate sets), where an absent fixture is
//! a hard failure. The CQLite-written cases always run. A case that returns ZERO
//! rows where rows are expected is a failure, never a vacuous pass.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::storage::read_path_probe::ReadPathProbe;
use cqlite_core::storage::sstable::reader::SSTableReader;
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use cqlite_flight::bypass::{bypass_reason, BypassReason, ForcedMergePath, MERGE_PATH_ENV};
use cqlite_flight::service::CqliteFlightService;

mod differential_fixtures;
use differential_fixtures::*;

/// Serializes the process-global env + probe window (see the module doc).
static PROBE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Debug-only reader seam pinning the read-time TTL clock (see `now_clock.rs`).
const TTL_NOW_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

/// One row rendered as an ordered `column -> value` map, so a mismatch prints a
/// readable diff and every column participates in the comparison.
type Row = BTreeMap<String, String>;

fn require_fixtures() -> bool {
    std::env::var("CQLITE_REQUIRE_FIXTURES")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent")
        .to_path_buf()
}

/// The fetched dataset root (`CQLITE_DATASETS_ROOT`, else the in-repo default).
fn datasets_root() -> PathBuf {
    std::env::var("CQLITE_DATASETS_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| repo_root().join("test-data/datasets"))
}

/// Resolve `<sstables>/<keyspace>/<table>-<uuid>/`, whose Cassandra-assigned
/// UUID suffix is discovered from the directory listing (never guessed).
fn fixture_dir(keyspace: &str, table: &str) -> Option<PathBuf> {
    let ks_dir = datasets_root().join("sstables").join(keyspace);
    let prefix = format!("{table}-");
    std::fs::read_dir(&ks_dir)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&prefix))
        })
}

/// Whether the fixture's `Data.db` binaries were actually fetched.
fn fixture_data_present(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
}

fn ticket_json(keyspace: &str, table: &str, ddl: &str) -> serde_json::Value {
    serde_json::json!({ "keyspace": keyspace, "table": table, "ddl": ddl })
}

/// Drain `do_get` into ordered rows (the emit ORDER is part of the comparison)
/// plus each emitted batch's row count, so a batching-budget case can compare
/// the batch BOUNDARIES across arms too.
// arrow-flight's `FlightError` Err type has a framework-fixed large size; boxing
// it (clippy's suggestion) would break the flight decoder stream API (#2856).
#[allow(clippy::result_large_err)]
async fn do_get_rows_and_batches(
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
) -> (Vec<Row>, Vec<usize>) {
    let bytes = serde_json::to_vec(ticket).expect("ticket json");
    let resp = svc
        .do_get(Request::new(Ticket::new(bytes)))
        .await
        .expect("do_get")
        .into_inner();
    let mapped = resp.map(|r| r.map_err(|e| FlightError::ExternalError(Box::new(e))));
    let mut stream = FlightRecordBatchStream::new_from_flight_data(mapped);
    let mut rows = Vec::new();
    let mut sizes = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("record batch");
        sizes.push(batch.num_rows());
        push_rows(&batch, &mut rows);
    }
    (rows, sizes)
}

/// Render every column of every row through Arrow's own display formatter, so
/// the comparison covers VALUES (and nullness) for every CQL type without this
/// test hardcoding a per-type downcast.
fn push_rows(batch: &RecordBatch, out: &mut Vec<Row>) {
    let schema = batch.schema();
    let formatters: Vec<_> = batch
        .columns()
        .iter()
        .map(|c| {
            arrow::util::display::ArrayFormatter::try_new(
                c.as_ref(),
                &arrow::util::display::FormatOptions::default(),
            )
            .expect("array formatter")
        })
        .collect();
    for r in 0..batch.num_rows() {
        let mut row = Row::new();
        for (c, field) in schema.fields().iter().enumerate() {
            let rendered = if batch.column(c).is_null(r) {
                "<null>".to_string()
            } else {
                formatters[c].value(r).to_string()
            };
            row.insert(field.name().clone(), rendered);
        }
        out.push(row);
    }
}

/// Drain `do_get` capturing a terminal error INSTEAD of panicking, so a case
/// whose (unchanged, pre-existing) behaviour is a hard error on both arms can be
/// compared arm-vs-arm too — e.g. a composite-keyed collection of frozen UDTs,
/// which #2339 fails closed on. Returns `Err(status message)`.
// arrow-flight's `FlightError` Err type has a framework-fixed large size; boxing
// it (clippy's suggestion) would break the flight decoder stream API (#2856).
#[allow(clippy::result_large_err)]
async fn do_get_outcome(
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
) -> Result<Vec<Row>, String> {
    let bytes = serde_json::to_vec(ticket).expect("ticket json");
    let resp = match svc.do_get(Request::new(Ticket::new(bytes))).await {
        Ok(r) => r.into_inner(),
        Err(status) => return Err(format!("do_get rpc: {}", status.message())),
    };
    let mapped = resp.map(|r| r.map_err(|e| FlightError::ExternalError(Box::new(e))));
    let mut stream = FlightRecordBatchStream::new_from_flight_data(mapped);
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        match batch {
            Ok(batch) => push_rows(&batch, &mut rows),
            Err(e) => return Err(format!("stream: {e}")),
        }
    }
    Ok(rows)
}

/// Run `ticket` under a forced arm, returning its rows, batch sizes and the
/// probe delta that proves which arm actually ran.
async fn run_forced(
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
    arm: &str,
) -> (Vec<Row>, Vec<usize>, ReadPathProbe) {
    std::env::set_var(MERGE_PATH_ENV, arm);
    let before = ReadPathProbe::snapshot();
    let (rows, sizes) = do_get_rows_and_batches(svc, ticket).await;
    let delta = ReadPathProbe::snapshot().delta_since(&before);
    std::env::remove_var(MERGE_PATH_ENV);
    (rows, sizes, delta)
}

/// The differential itself: run `ticket` on BOTH arms at `pinned_now` and assert
/// identical rows/values/order — and that the arms really differed.
///
/// `expect_rows` is the minimum number of rows the case must return; a case that
/// silently returns nothing cannot pass (anti-vacuity).
async fn assert_arms_agree(
    label: &str,
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
    pinned_now: i64,
    expect_rows: usize,
    failures: &mut Vec<String>,
) -> Option<Vec<Row>> {
    std::env::set_var(TTL_NOW_ENV, pinned_now.to_string());
    let (merge_rows, merge_sizes, merge_delta) = run_forced(svc, ticket, "merge").await;
    let (bypass_rows, bypass_sizes, bypass_delta) = run_forced(svc, ticket, "bypass").await;
    std::env::remove_var(TTL_NOW_ENV);

    if merge_rows != bypass_rows {
        failures.push(format!(
            "case {label}: FORCED-PATH DIFFERENTIAL MISMATCH at pinned now {pinned_now}\n  \
             merge  ({} rows): {:#?}\n  bypass ({} rows): {:#?}",
            merge_rows.len(),
            merge_rows,
            bypass_rows.len(),
            bypass_rows,
        ));
        return None;
    }
    if merge_rows.len() < expect_rows {
        failures.push(format!(
            "case {label}: expected at least {expect_rows} rows on BOTH arms, got {} \
             — a zero/short result is a failure, never a vacuous pass",
            merge_rows.len()
        ));
        return None;
    }
    if merge_delta.mergers_built == 0 || merge_delta.reconcile_entries == 0 {
        failures.push(format!(
            "case {label}: the forced-merge run did not actually take the merge arm \
             (mergers={}, reconciles={}) — the differential would be vacuous",
            merge_delta.mergers_built, merge_delta.reconcile_entries
        ));
        return None;
    }
    if bypass_delta.mergers_built != 0 || bypass_delta.reconcile_entries != 0 {
        failures.push(format!(
            "case {label}: the forced-bypass run still merged (mergers={}, \
             reconciles={}) — the differential compared the same arm twice",
            bypass_delta.mergers_built, bypass_delta.reconcile_entries
        ));
        return None;
    }
    // Spec R3 on EVERY differential case, both shapes: the bypass leg must build
    // ZERO per-row `CellWriteMetadata` maps. Asserted here rather than only in the
    // dedicated path-taken tests so each case — full-ring, token-bound, projected,
    // byte-capped, UDT-bearing — carries its own pin.
    if bypass_delta.cell_metadata_maps != 0 {
        failures.push(format!(
            "case {label}: the forced-bypass run built {} per-row CellWriteMetadata \
             map(s) — spec R3 requires zero on the fast path",
            bypass_delta.cell_metadata_maps
        ));
        return None;
    }
    // The BATCH BOUNDARIES, not just the totals: comparing sums could never fail
    // independently of the row-set equality asserted above, which made every
    // batching claim in this file vacuous (roborev). Both arms run the SAME drive
    // loop and the same `BatchByteCap`, so for an identical row set the boundaries
    // must be identical too — a divergence means one arm's batching diverged.
    if merge_sizes != bypass_sizes {
        failures.push(format!(
            "case {label}: batch BOUNDARIES differ: merge {merge_sizes:?} vs bypass \
             {bypass_sizes:?} (the row sets are identical, so the batching diverged)"
        ));
        return None;
    }
    eprintln!(
        "PASS {label} — {} rows identical on both arms (pinned now {pinned_now})",
        merge_rows.len()
    );
    Some(bypass_rows)
}

/// A schema the predicate REFUSES (a static column — declared OR found in the
/// SSTable's own serialization header — or a multi-cell column whose two arms
/// disagree) must fall back to the merge arm under BOTH forced values and behave
/// IDENTICALLY: the fast path cannot change those results because it is never
/// taken for them (fail-closed; see `BypassReason::StaticColumns` /
/// `BypassReason::MulticellArmDivergence`).
///
/// "Identically" includes an identical FAILURE: `#2339` makes the merge arm fail
/// closed on a composite-keyed collection, and preserving that (rather than
/// silently serving rows at one generation and erroring at two) is exactly the
/// point of the guard. `expect_error` records which of the two outcomes is
/// today's behaviour, so if #2339 is later fixed this test says so instead of
/// quietly changing meaning.
async fn assert_refused_schema_unchanged(
    label: &str,
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
    pinned_now: i64,
    expect_error: Option<&str>,
    failures: &mut Vec<String>,
) {
    std::env::set_var(TTL_NOW_ENV, pinned_now.to_string());
    std::env::set_var(MERGE_PATH_ENV, "merge");
    let merge_before = ReadPathProbe::snapshot();
    let merge_outcome = do_get_outcome(svc, ticket).await;
    let merge_delta = ReadPathProbe::snapshot().delta_since(&merge_before);
    std::env::set_var(MERGE_PATH_ENV, "bypass");
    let bypass_before = ReadPathProbe::snapshot();
    let bypass_outcome = do_get_outcome(svc, ticket).await;
    let bypass_delta = ReadPathProbe::snapshot().delta_since(&bypass_before);
    std::env::remove_var(MERGE_PATH_ENV);
    std::env::remove_var(TTL_NOW_ENV);

    if bypass_delta.mergers_built == 0 {
        failures.push(format!(
            "case {label}: a schema the predicate refuses must fall back to the \
             merge arm even under CQLITE_FLIGHT_MERGE_PATH=bypass (mergers=0) — \
             the fast arm must NOT be taken"
        ));
    }
    if merge_delta.mergers_built == 0 {
        failures.push(format!("case {label}: the forced-merge run did not merge"));
    }
    if merge_outcome != bypass_outcome {
        failures.push(format!(
            "case {label}: the refused schema must behave identically under both \
             forced values\n  merge: {merge_outcome:#?}\n  bypass: {bypass_outcome:#?}"
        ));
        return;
    }
    match (&merge_outcome, expect_error) {
        (Err(msg), Some(must_name)) if msg.contains(must_name) => eprintln!(
            "PASS {label} — refused schema took the merge arm on both forced values; \
             behaviour unchanged (still the pre-existing error naming \
             '{must_name}')"
        ),
        (Err(msg), Some(must_name)) => failures.push(format!(
            "case {label}: both arms failed, but NOT with the expected condition — \
             the error must name '{must_name}', so an unrelated identical failure \
             cannot pass this case. Got: {msg}"
        )),
        (Ok(rows), None) if !rows.is_empty() => eprintln!(
            "PASS {label} — refused schema took the merge arm on both forced values \
             ({} rows, unchanged)",
            rows.len()
        ),
        (Ok(rows), None) => failures.push(format!(
            "case {label}: the refused schema returned NO rows ({}) — a vacuous pass",
            rows.len()
        )),
        (Ok(_), Some(must_name)) => failures.push(format!(
            "case {label}: expected today's pre-existing ERROR naming '{must_name}' \
             on both arms, but the request SUCCEEDED — if #2339 was fixed, update \
             this case"
        )),
        (Err(msg), None) => failures.push(format!(
            "case {label}: expected rows on both arms but both failed: {msg}"
        )),
    }
}

// ---------------------------------------------------------------------------
// CQLite-written tombstone/TTL fixture
// ---------------------------------------------------------------------------

/// The whole differential (one test — see the module doc's isolation note).
#[tokio::test]
async fn forced_path_differential_agrees_on_every_shape() {
    let _guard = PROBE_LOCK.lock().await;
    let mut failures: Vec<String> = Vec::new();

    // ---- CQLite-written tombstone/TTL/range/partition shapes ---------------
    let (_temp, shapes_dir) = build_shapes_fixture().await;
    let svc = CqliteFlightService::new(shapes_dir.clone(), 8192);
    let full = ticket_json(KS, TBL, DDL);

    // (1) Every reconciliation shape, at a `now` BEFORE the TTL cell expires.
    let before = assert_arms_agree(
        "shapes/select-star@before-expiry",
        &svc,
        &full,
        NOW_BEFORE_EXPIRY,
        1,
        &mut failures,
    )
    .await;
    // (2) The SAME bytes at a `now` AFTER it expires — pinning that the pinned
    //     clock reaches the fast path's shadow decoder rather than the wall clock.
    let after = assert_arms_agree(
        "shapes/select-star@after-expiry",
        &svc,
        &full,
        NOW_AFTER_EXPIRY,
        1,
        &mut failures,
    )
    .await;
    if let (Some(before), Some(after)) = (before.as_ref(), after.as_ref()) {
        let w_before = cell(before, "ck", "2", "w");
        let w_after = cell(after, "ck", "2", "w");
        if w_before.as_deref() == Some("<null>") || w_before.is_none() {
            failures.push(format!(
                "the live-TTL cell must be PRESENT at the earlier pinned now, got {w_before:?}"
            ));
        }
        if w_after.as_deref() != Some("<null>") {
            failures.push(format!(
                "the TTL cell must be EXPIRED at the later pinned now (the fast path \
                 read the request's now, not the wall clock), got {w_after:?}"
            ));
        }
        // Row/partition/range/cell tombstone shapes, asserted on the bypass output
        // (already proven identical to the merge arm above).
        assert_semantics(after, &mut failures);
    }

    // (3) Predicate pushdown + projection + a token range on the fast arm.
    let mut projected = ticket_json(KS, TBL, DDL);
    projected["columns"] = serde_json::json!(["pk", "ck"]);
    projected["predicates"] =
        serde_json::json!([{ "column": "v", "op": "Equal", "value": "live" }]);
    let _ = assert_arms_agree(
        "shapes/predicate+projection",
        &svc,
        &projected,
        NOW_BEFORE_EXPIRY,
        1,
        &mut failures,
    )
    .await;

    // A token range derived from the fixture's REAL tokens: `(t1 - 1, t1]` holds
    // exactly partition pk=1, whose three surviving clustering rows give the case
    // a non-zero floor. Deriving the range (rather than guessing a half-ring)
    // is what stops this from passing vacuously when the range holds nothing —
    // and `assert_arms_agree` additionally requires the merge run to have really
    // merged and the bypass run to have really bypassed.
    let t1 = cassandra_murmur3_token(&1_i32.to_be_bytes());
    let mut token_scoped = ticket_json(KS, TBL, DDL);
    token_scoped["token_start"] = serde_json::json!(t1.saturating_sub(1));
    token_scoped["token_end"] = serde_json::json!(t1);
    let _ = assert_arms_agree(
        "shapes/token-range(single partition)",
        &svc,
        &token_scoped,
        NOW_BEFORE_EXPIRY,
        3,
        &mut failures,
    )
    .await;

    // (4) A byte-capped stream. The cap must be genuinely EXERCISED (more than one
    //     batch, and strictly more batches than the same query uncapped) and the
    //     boundaries must be identical on both arms — `assert_arms_agree` now
    //     compares the boundaries, not just the totals.
    //
    //     `TINY_BATCH_BYTES` is below one row's estimated Arrow payload, so the
    //     dual row-cap/byte-cap boundary cuts on EVERY row: with a cap of 512 the
    //     whole 4-row result fitted in one batch and nothing about batching was
    //     observable at all. The cap's BYTE semantics (no batch exceeds the
    //     budget) are pinned by `tests/issue_2825_max_batch_bytes_e2e.rs`, which
    //     now runs over the fast arm; what this case owns is arm-EQUIVALENCE.
    const TINY_BATCH_BYTES: usize = 16;
    let capped =
        CqliteFlightService::new(shapes_dir.clone(), 8192).with_max_batch_bytes(TINY_BATCH_BYTES);
    let _ = assert_arms_agree(
        "shapes/max-batch-bytes",
        &capped,
        &full,
        NOW_BEFORE_EXPIRY,
        1,
        &mut failures,
    )
    .await;
    std::env::set_var(TTL_NOW_ENV, NOW_BEFORE_EXPIRY.to_string());
    let (capped_rows, capped_sizes, capped_delta) = run_forced(&capped, &full, "bypass").await;
    let uncapped = CqliteFlightService::new(shapes_dir.clone(), 8192);
    let (uncapped_rows, uncapped_sizes, _) = run_forced(&uncapped, &full, "bypass").await;
    std::env::remove_var(TTL_NOW_ENV);
    if capped_delta.mergers_built != 0 {
        failures.push("the byte-capped run must be on the fast arm".to_string());
    }
    if capped_sizes.len() <= 1 {
        failures.push(format!(
            "byte-cap: the cap was NOT exercised — the whole result came back in \
             {} batch(es) ({capped_sizes:?}), so this case would pass even if \
             max_batch_bytes were ignored entirely",
            capped_sizes.len()
        ));
    }
    if capped_sizes.len() <= uncapped_sizes.len() {
        failures.push(format!(
            "byte-cap: a {TINY_BATCH_BYTES}-byte cap must cut into MORE batches than \
             the same query uncapped (capped {capped_sizes:?} vs uncapped \
             {uncapped_sizes:?}) — the cap is not governing the fast arm"
        ));
    }
    if capped_sizes.iter().any(|n| *n != 1) {
        failures.push(format!(
            "byte-cap: with a cap below one row's payload every batch must carry \
             exactly one row, got {capped_sizes:?}"
        ));
    }
    if capped_rows != uncapped_rows {
        failures.push(
            "byte-cap: capping changes only the batch boundaries, never the rows".to_string(),
        );
    }

    // ---- V5_0Uncompressed-classified clustered fixture (issue #3097) --------
    // The merge arm's NON-stitching Summary-guided enumeration
    // (`stream_all_partitions_for_query` → `stream_partitions_summary_guided`,
    // via `from_readers::drive_query_stream`) must decode with the CALLER's
    // authoritative ticket schema, so the clustering column `ck` is POPULATED —
    // not decoded under the reader header schema's placeholder name and surfaced
    // as NULL. Before #3097 the merge arm passed `None` here and this exact
    // `SELECT *` returned `ck = null` while the bypass arm returned `ck`. This
    // case runs both arms over the SAME bytes at a PINNED `now`, asserts they
    // agree, and additionally pins that `ck` is non-null on BOTH — the difference
    // the differential row-equality alone would miss if both arms regressed
    // together.
    let (_utemp, uncomp_dir) = build_uncomp_clustered_fixture().await;
    let usvc = CqliteFlightService::new(uncomp_dir, 8192);
    let uticket = ticket_json(KS, UNCOMP_TBL, UNCOMP_DDL);
    let uncomp_rows = assert_arms_agree(
        "clustered_uncomp/select-star(#3097 merge-arm caller schema)",
        &usvc,
        &uticket,
        NOW_BEFORE_EXPIRY,
        400,
        &mut failures,
    )
    .await;
    if let Some(rows) = uncomp_rows.as_ref() {
        let ck_nulls = rows
            .iter()
            .filter(|r| r.get("ck").map(String::as_str) == Some("<null>"))
            .count();
        if ck_nulls != 0 {
            failures.push(format!(
                "clustered_uncomp: {ck_nulls} row(s) surfaced `ck` as NULL — the merge \
                 arm must decode the clustering column under the caller's schema name \
                 (issue #3097), never the reader header's placeholder"
            ));
        }
        // Spot-check a concrete value: partition `i` was written with `ck = i`.
        if !rows
            .iter()
            .any(|r| r.get("ck").map(String::as_str) == Some("0"))
        {
            failures.push(
                "clustered_uncomp: expected a row with ck=0 (the first partition's \
                 clustering value), but none surfaced"
                    .to_string(),
            );
        }
    }

    // ---- Static columns: BOTH ARMS (issues #3095 / #3140) -------------------
    // #3095 retired the "any static column refuses the fast arm" exclusion, and #3140
    // has now retired the follow-on guard that still refused a static-bearing SSTable
    // declaring a DELETION (`BypassReason::StaticColumnsWithDeletions`). That guard
    // existed because a simple CELL tombstone diverged between the arms — the merge arm
    // dropped it (column reads null, Cassandra's answer) while the fast arm surfaced a
    // raw `Value::Tombstone` the Arrow encoder rejected. PR #3122 fixed the fast arm at
    // its source (`row_decoder`'s `PartitionShadow::cell_tombstone_dropped`), so this
    // fixture — which contains a row deletion — is now SERVED by the fast arm and is an
    // ordinary both-arms differential. `assert_arms_agree` requires the bypass leg to
    // build ZERO mergers, so a silent relapse to the merge arm fails the case.
    //
    // SCOPE, so this is not mistaken for a Cassandra oracle: the fixture is
    // CQLITE-WRITTEN, so its ROW CONTENT proves nothing about Cassandra (it is
    // invariant to a uniform error and subject to the write-side #1074 — #3042). What
    // it proves is that both FORCED values return the same rows and that the fast arm
    // is genuinely taken. The Cassandra-parity oracle is
    // `cqlite-flight/tests/issue_3095_flight_static_columns.rs`, on Cassandra-written
    // fixtures including a static-ONLY partition and the deletion-bearing
    // `test_tomb.static_with_tombstones`.
    let (_stemp, statics_dir) = build_statics_fixture().await;
    let ssvc = CqliteFlightService::new(statics_dir, 8192);
    let sticket = ticket_json(KS, STATIC_TBL, STATIC_DDL);
    // Not just "agreed": pin the SHAPE both arms return, so an agreed-but-wrong result
    // set (a phantom `ck = null` row alongside the real rows, or a dropped rowless
    // partition) cannot pass. Expected per Cassandra's `processPartition()`: pk=1's two
    // clustering rows carrying `s1`; ONE row for the static-only pk=2; ONE row for
    // pk=3, whose only clustering row was deleted.
    if let Some(rows) = assert_arms_agree(
        "statics/select-star",
        &ssvc,
        &sticket,
        NOW_BEFORE_EXPIRY,
        4,
        &mut failures,
    )
    .await
    {
        let mut shape: Vec<(String, String, String)> = rows
            .iter()
            .map(|r| {
                let get = |c: &str| r.get(c).cloned().unwrap_or_else(|| "<missing>".into());
                (get("pk"), get("ck"), get("s"))
            })
            .collect();
        shape.sort();
        let expected: Vec<(String, String, String)> = vec![
            ("1".into(), "1".into(), "s1".into()),
            ("1".into(), "2".into(), "s1".into()),
            ("2".into(), "<null>".into(), "s2-only".into()),
            ("3".into(), "<null>".into(), "s3-rows-deleted".into()),
        ];
        if shape != expected {
            failures.push(format!(
                "statics/select-star: the (pk, ck, s) shape is not Cassandra's — \
                 expected {expected:#?}, got {shape:#?}"
            ));
        }
    }

    // ---- The on-disk static branch, asserted DIRECTLY on the predicate ------
    assert_undeclared_static_column_is_refused_on_disk(&mut failures).await;

    // ---- Real Cassandra fixtures ------------------------------------------
    for case in dataset_cases() {
        let Some(dir) = fixture_dir(case.keyspace, case.table) else {
            handle_missing(&case, "fixture dir absent", &mut failures);
            continue;
        };
        if !fixture_data_present(&dir) {
            handle_missing(&case, "Data.db not fetched", &mut failures);
            continue;
        }
        let root = datasets_root().join("sstables");
        let svc = CqliteFlightService::new(root, 8192);
        let mut ticket = ticket_json(case.keyspace, case.table, &case.ddl);
        if !case.columns.is_empty() {
            ticket["columns"] = serde_json::json!(case.columns);
        }
        // A token range derived from the fixture's REAL partition token drives the
        // TOKEN-BOUND bypass arm (the Summary-guided walk the Trino connector
        // uses, and the only arm that overrides the reader-derived schema with the
        // ticket DDL) over real Cassandra `nb` bytes. A single `int` partition key
        // is stored as its 4-byte big-endian value, which is what Cassandra hashes.
        if let Some(pk) = case.token_of_int_pk {
            let t = cassandra_murmur3_token(&pk.to_be_bytes());
            ticket["token_start"] = serde_json::json!(t.saturating_sub(1));
            ticket["token_end"] = serde_json::json!(t);
        }
        if case.refuses_fast_arm {
            assert_refused_schema_unchanged(
                case.label,
                &svc,
                &ticket,
                case.pinned_now,
                case.refused_error_substr,
                &mut failures,
            )
            .await;
            continue;
        }
        let _ = assert_arms_agree(
            case.label,
            &svc,
            &ticket,
            case.pinned_now,
            case.min_rows,
            &mut failures,
        )
        .await;

        // The gap-(b) shape read under a PK-ONLY projection too: a row whose
        // visibility comes from a live data cell rather than a liveness marker
        // must survive a projection that drops that very cell.
        if case.pk_only_projection.is_empty() {
            continue;
        }
        let mut pk_only = ticket.clone();
        pk_only["columns"] = serde_json::json!(case.pk_only_projection);
        let _ = assert_arms_agree(
            case.pk_only_label,
            &svc,
            &pk_only,
            case.pinned_now,
            case.min_rows,
            &mut failures,
        )
        .await;
    }

    assert!(
        failures.is_empty(),
        "forced-path differential failures:\n{}",
        failures.join("\n\n")
    );
}

/// Prove the ON-DISK static branch of the predicate — not the schema branch —
/// is what refuses a stale-DDL request (roborev/C).
///
/// The chain asserted here, all on real Cassandra bytes
/// (`test_writeparity.static_clustering_shape`, whose serialization header reads
/// `StaticColumns: sdata:…UTF8Type`):
/// 1. the ticket DDL (which OMITS `sdata`) parses to a schema declaring NO static
///    column — so the schema-side check (`bypass.rs`'s `schema.columns … is_static`)
///    provably CANNOT fire;
/// 2. the reader's own header DOES report `["sdata"]`, and reports it as KNOWN;
/// 3. `bypass_reason` nevertheless returns `StaticColumns`.
///
/// (1) + (3) together establish that the refusal came from the on-disk branch,
/// which is otherwise unexercised: every other static test declares the column in
/// its DDL and refuses one check earlier.
async fn assert_undeclared_static_column_is_refused_on_disk(failures: &mut Vec<String>) {
    const STALE_DDL: &str =
        "CREATE TABLE static_clustering_shape (id int, ck int, rdata text, PRIMARY KEY (id, ck))";
    let Some(dir) = fixture_dir("test_writeparity", "static_clustering_shape") else {
        let msg = "on-disk static check: fixture dir absent".to_string();
        if require_fixtures() {
            failures.push(format!("REQUIRE_FIXTURES: {msg}"));
        } else {
            eprintln!("SKIP {msg}");
        }
        return;
    };
    let Some(data_db) = std::fs::read_dir(&dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
    else {
        let msg = "on-disk static check: Data.db not fetched".to_string();
        if require_fixtures() {
            failures.push(format!("REQUIRE_FIXTURES: {msg}"));
        } else {
            eprintln!("SKIP {msg}");
        }
        return;
    };

    let schema = match cqlite_core::schema::parse_cql_schema(STALE_DDL) {
        Ok(s) => s,
        Err(e) => {
            failures.push(format!(
                "on-disk static check: stale DDL did not parse: {e}"
            ));
            return;
        }
    };
    if schema.columns.iter().any(|c| c.is_static) {
        failures.push(
            "on-disk static check: the stale DDL must declare NO static column, or the \
             schema-side check would refuse first and this case would prove nothing"
                .to_string(),
        );
        return;
    }

    let config = cqlite_core::Config::default();
    let platform = match cqlite_core::Platform::new(&config).await {
        Ok(p) => std::sync::Arc::new(p),
        Err(e) => {
            failures.push(format!("on-disk static check: platform: {e}"));
            return;
        }
    };
    let reader = match SSTableReader::open(&data_db, &config, platform).await {
        Ok(r) => std::sync::Arc::new(r),
        Err(e) => {
            failures.push(format!("on-disk static check: reader open: {e}"));
            return;
        }
    };

    let on_disk = reader.on_disk_static_columns();
    if on_disk != vec!["sdata".to_string()] {
        failures.push(format!(
            "on-disk static check: the serialization header must report the file's \
             STATIC column (expected [\"sdata\"], got {on_disk:?}) — if this is empty \
             the guard is a silent no-op"
        ));
        return;
    }
    if !reader.static_columns_are_known() {
        failures.push(
            "on-disk static check: the header was parsed, so the static question must \
             report itself as KNOWN"
                .to_string(),
        );
        return;
    }

    let reason = bypass_reason(
        std::slice::from_ref(&reader),
        &schema,
        ForcedMergePath::Auto,
        false,
        None,
    );
    if reason != BypassReason::StaticColumns {
        failures.push(format!(
            "on-disk static check: a request whose DDL omits the file's static column \
             must be refused BY THE ON-DISK BRANCH; got {reason:?}"
        ));
        return;
    }
    eprintln!(
        "PASS on-disk static check — DDL declares no static column, header reports \
         {on_disk:?}, predicate refuses with StaticColumns (the on-disk branch)"
    );
}

/// A dataset-backed differential case.
struct DatasetCase {
    label: &'static str,
    pk_only_label: &'static str,
    keyspace: &'static str,
    table: &'static str,
    ddl: String,
    pinned_now: i64,
    min_rows: usize,
    pk_only_projection: Vec<&'static str>,
    /// Optional projection for the MAIN case (empty = `SELECT *`).
    columns: Vec<&'static str>,
    /// `true` when the bypass predicate REFUSES this schema (a static column or a
    /// composite-keyed collection): the case asserts the fail-closed fallback —
    /// both forced values take the merge arm and behave identically — instead of
    /// an arm-vs-arm row differential.
    refuses_fast_arm: bool,
    /// For a refused schema: `Some(substring)` when today's (unchanged) behaviour
    /// on BOTH arms is a hard error, naming the CONDITION the error must mention
    /// (so an unrelated identical failure cannot pass); `None` when it is rows.
    refused_error_substr: Option<&'static str>,
    /// When `Some(pk)`, the ticket carries a token range derived from that `int`
    /// partition key's REAL Murmur3 token, so the case exercises the TOKEN-BOUND
    /// bypass arm (`stream_partitions_summary_guided`) rather than the full-ring
    /// one. Without it a dataset case only ever covers `drive_full_scan_rows`.
    token_of_int_pk: Option<i32>,
}

/// The pinned `now` the query-semantics oracle records for the
/// `test_compaction_tombstone_ttl` fixtures.
const ORACLE_PINNED_NOW: i64 = 1_782_950_400;

fn dataset_cases() -> Vec<DatasetCase> {
    let tombstone_ddl = "CREATE TABLE test_compaction_tombstone_ttl.{TBL} \
         (id int, ck int, v text, PRIMARY KEY (id, ck))";
    vec![
        DatasetCase {
            label: "cassandra/rt_cross_gen",
            pk_only_label: "cassandra/rt_cross_gen@pk-only",
            keyspace: "test_compaction_tombstone_ttl",
            table: "rt_cross_gen",
            ddl: tombstone_ddl.replace("{TBL}", "rt_cross_gen"),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 2,
            pk_only_projection: vec!["id", "ck"],
            refuses_fast_arm: false,
            refused_error_substr: None,
            columns: vec![],
            token_of_int_pk: None,
        },
        DatasetCase {
            label: "cassandra/ttl_expired_live",
            pk_only_label: "cassandra/ttl_expired_live@pk-only",
            keyspace: "test_compaction_tombstone_ttl",
            table: "ttl_expired_live",
            ddl: tombstone_ddl.replace("{TBL}", "ttl_expired_live"),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 1,
            pk_only_projection: vec!["id", "ck"],
            refuses_fast_arm: false,
            refused_error_substr: None,
            columns: vec![],
            token_of_int_pk: None,
        },
        // TOKEN-BOUND variants of the two single-partition fixtures (roborev,
        // issue #3058): without these, EVERY real-Cassandra case built a ticket
        // with no token range, so all of them exercised only the full-ring arm
        // (`drive_full_scan_rows`) and the Summary-guided arm the Trino connector
        // actually drives had NO arm-vs-arm proof over real `nb` bytes. That arm
        // is also the only one that overrides the reader-derived schema with the
        // ticket DDL (`caller_schema`, which exists because `nb` headers carry no
        // embedded schema — #3097), so it is exactly where a schema-resolution
        // divergence would surface.
        DatasetCase {
            label: "cassandra/rt_cross_gen@token-bound",
            pk_only_label: "cassandra/rt_cross_gen@token-bound+pk-only",
            keyspace: "test_compaction_tombstone_ttl",
            table: "rt_cross_gen",
            ddl: tombstone_ddl.replace("{TBL}", "rt_cross_gen"),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 2,
            pk_only_projection: vec!["id", "ck"],
            refuses_fast_arm: false,
            refused_error_substr: None,
            columns: vec![],
            // Every surviving row of this fixture lives in partition id = 1.
            token_of_int_pk: Some(1),
        },
        DatasetCase {
            label: "cassandra/ttl_expired_live@token-bound",
            pk_only_label: "cassandra/ttl_expired_live@token-bound+pk-only",
            keyspace: "test_compaction_tombstone_ttl",
            table: "ttl_expired_live",
            ddl: tombstone_ddl.replace("{TBL}", "ttl_expired_live"),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 1,
            pk_only_projection: vec!["id", "ck"],
            refuses_fast_arm: false,
            refused_error_substr: None,
            columns: vec![],
            token_of_int_pk: Some(1),
        },
        DatasetCase {
            label: "cassandra/shadow_row_delete",
            pk_only_label: "cassandra/shadow_row_delete@pk-only",
            keyspace: "test_compaction_tombstone_ttl",
            table: "shadow_row_delete",
            ddl: tombstone_ddl.replace("{TBL}", "shadow_row_delete"),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 3,
            pk_only_projection: vec!["id", "ck"],
            refuses_fast_arm: false,
            refused_error_substr: None,
            columns: vec![],
            token_of_int_pk: None,
        },
        // REAL Cassandra static shape: `sdata` was written by
        // `UPDATE static_clustering_shape SET sdata='static-val' WHERE id=1`
        // alongside an INSERTed clustering row. Issue #3095: with both arms now
        // implementing Cassandra's `processPartition()` static semantics, a ticket
        // DDL that DECLARES `sdata static` is servable by the fast arm, so this is
        // an arm differential rather than a fail-closed fallback. (Its
        // Cassandra-parity assertion — 1 row, static value present, no `ck = null`
        // row — lives in `issue_3095_flight_static_columns.rs`.)
        // Spec R1/R6 + issue #2339 AC2: this table declares
        // `contacts set<frozen<contact_info>>` (a composite SET ELEMENT, whose
        // element type is a NESTED UDT) and
        // `emergency_contacts map<text, frozen<contact_info>>` (a scalar-keyed map
        // with a composite VALUE). The merge arm's reassembler used to FAIL CLOSED
        // on `contacts`, so `SELECT *` here ERRORED at two generations and
        // SUCCEEDED at one, and this case could only be asserted as a fail-closed
        // FALLBACK. #2339 decodes the composite element structurally on the merge
        // arm, so the case is now an ordinary UNPROJECTED (`SELECT *`) ARM
        // DIFFERENTIAL over real Cassandra bytes: the ticket DDL carries the two
        // `CREATE TYPE`s, so the element type resolves, the predicate selects the
        // fast arm, and `assert_arms_agree` compares every column of every row
        // (and FAILS unless the bypass leg shows mergers_built == 0, so "the fast
        // arm was really taken" is asserted, not assumed).
        DatasetCase {
            label: "cassandra/collections_with_udts(composite set element, SELECT *)",
            pk_only_label: "cassandra/collections_with_udts@pk-only",
            keyspace: "test_collections",
            table: "collections_with_udts",
            ddl: [
                "CREATE TABLE collections_with_udts (user_id uuid PRIMARY KEY, addresses list<frozen<address_type>>, contacts set<frozen<contact_info>>, locations_visited map<date, frozen<address_type>>, emergency_contacts map<text, frozen<contact_info>>);",
                "CREATE TYPE address_type (street text, city text, state text, zip_code text, country text);",
                "CREATE TYPE contact_info (email text, phone text, address frozen<address_type>);",
            ]
            .join(" "),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 1,
            pk_only_projection: vec!["user_id"],
            refuses_fast_arm: false,
            refused_error_substr: None,
            columns: vec![],
            token_of_int_pk: None,
        },
        // Spec R7's "a frozen UDT inside a collection still decodes structurally"
        // scenario, over the SAME real bytes (roborev/C): the composite-keyed
        // guard is schema-WIDE, so the case above (which declares the sibling
        // `contacts set<frozen<contact_info>>`) can no longer reach the fast arm
        // under any projection. This case therefore declares a ticket DDL with
        // ONLY the non-refused columns — `addresses list<frozen<address_type>>`
        // and `locations_visited map<date, frozen<address_type>>` (a scalar-keyed
        // map) — which is legitimate because the CALLER schema IS the ticket DDL
        // and undeclared on-disk columns are tolerated by the reassembler
        // (`read_assembly.rs`). The predicate therefore selects the fast arm and
        // the `frozen<UDT>`-inside-a-`list` Struct decode is compared arm-vs-arm
        // again. `assert_arms_agree` FAILS the case unless the bypass leg shows
        // mergers_built == 0 AND reconcile_entries == 0, so "the fast arm was
        // really taken" is asserted, not assumed.
        DatasetCase {
            label: "cassandra/collections_with_udts@udt-in-collection",
            pk_only_label: "cassandra/collections_with_udts@udt-in-collection+pk-only",
            keyspace: "test_collections",
            table: "collections_with_udts",
            ddl: [
                "CREATE TABLE collections_with_udts (user_id uuid PRIMARY KEY, addresses list<frozen<address_type>>, locations_visited map<date, frozen<address_type>>);",
                "CREATE TYPE address_type (street text, city text, state text, zip_code text, country text);",
            ]
            .join(" "),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 1,
            pk_only_projection: vec!["user_id"],
            refuses_fast_arm: false,
            refused_error_substr: None,
            columns: vec!["user_id", "addresses"],
            token_of_int_pk: None,
        },
        // Issue #2339: NESTED FROZEN COLLECTIONS in element/value position, on real
        // Cassandra bytes. `s_map_vals set<frozen<map<text,int>>>` is a composite
        // SET ELEMENT whose element is a frozen COLLECTION (not a UDT), so it needs
        // no `CREATE TYPE` to resolve — which means #2339's narrowing lets the
        // predicate SELECT the fast arm for this schema. This case is what proves
        // that is safe: both arms must return identical rows for all three of
        // `m_list_vals map<text, frozen<list<int>>>` (scalar key, frozen-collection
        // VALUE), `l_set_vals list<frozen<set<text>>>` (a list, position-keyed) and
        // `s_map_vals` (the composite element), UNPROJECTED. `assert_arms_agree`
        // FAILS unless the bypass leg shows mergers_built == 0, so "the fast arm was
        // really taken" is asserted, not assumed.
        DatasetCase {
            label: "cassandra/cx_nested_frozen_collections(nested frozen collections, SELECT *)",
            pk_only_label: "cassandra/cx_nested_frozen_collections@pk-only",
            keyspace: "test_types",
            table: "cx_nested_frozen_collections",
            ddl: "CREATE TABLE cx_nested_frozen_collections (pk int, ck int, m_list_vals map<text, frozen<list<int>>>, l_set_vals list<frozen<set<text>>>, s_map_vals set<frozen<map<text,int>>>, PRIMARY KEY (pk, ck));".to_string(),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 1,
            pk_only_projection: vec!["pk", "ck"],
            refuses_fast_arm: false,
            refused_error_substr: None,
            columns: vec![],
            token_of_int_pk: None,
        },
        // The NON-FROZEN (multicell) UDT divergence, on real Cassandra bytes
        // (roborev): `mp person_type` is multicell, so the merge arm's
        // `assemble_complex` `_` fall-through keeps only the LAST element's scalar
        // while the single-generation decoder assembles the whole `Value::Udt`
        // (#927/#1081). The predicate REFUSES such a schema, so this pins that the
        // fast arm is not taken and behaviour is exactly today's.
        //
        // Issue #2339 (roborev F1) changed WHAT "today's behaviour" is, and this
        // case is the harness saying so rather than quietly changing meaning. The
        // service now resolves each column's `cql_type` under the TICKET's keyspace
        // (it previously resolved under the `"default"` placeholder and therefore
        // resolved NOTHING), so `mp` is metadata-correct — an Arrow `Struct` — and
        // the typed UDT builder consequently FAILS CLOSED on the fall-through's
        // `Boolean(true)` (the `active` field, the last element's scalar) instead of
        // formatting it into the opaque `Utf8` column an unresolved `Custom` used to
        // produce. So the pre-existing #927/#1081 divergence surfaces as an error
        // instead of a silently-wrong value: still IDENTICAL on both arms, which is
        // what this case exists to assert, and the fail-closed direction. Assembling
        // a multicell UDT on the merge arm remains #927/#1081's work; when that
        // lands, this case reverts to `refused_error_substr: None`.
        DatasetCase {
            label: "cassandra/cx_multicell_udt_collection_paths(fail-closed non-frozen UDT)",
            pk_only_label: "cassandra/cx_multicell_udt_collection_paths@pk-only",
            keyspace: "test_types",
            table: "cx_multicell_udt_collection_paths",
            ddl: [
                "CREATE TABLE cx_multicell_udt_collection_paths (pk int, ck int, mp person_type, ml list<text>, PRIMARY KEY (pk, ck));",
                "CREATE TYPE person_type (first_name text, last_name text, age int, active boolean);",
            ]
            .join(" "),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 1,
            pk_only_projection: vec![],
            refuses_fast_arm: true,
            // The typed-UDT Arrow builder's fail-closed message
            // (`arrow_builders_nested::build_typed_udt_array`): a resolved UDT
            // column may only carry a `Value::Udt` or null, so the merge arm's
            // last-element scalar is refused rather than coerced. Naming the
            // substring keeps an UNRELATED identical failure from passing this case.
            refused_error_substr: Some("expected Udt value"),
            columns: vec![],
            token_of_int_pk: None,
        },
        // The STALE-DDL case (roborev/C): this fixture's serialization header
        // declares `sdata` STATIC, but this ticket DDL OMITS it — so the
        // schema-side static check CANNOT fire and only the ON-DISK header check
        // can refuse the fast path. That is the exact scenario the on-disk check
        // exists for (a DDL predating an `ALTER TABLE ADD … STATIC`), and without
        // this case that branch is dead in test terms. `assert_undeclared_static_
        // column_is_refused_on_disk` below additionally asserts, directly on the
        // predicate, that the refusal comes from the on-disk branch.
        DatasetCase {
            label: "cassandra/static_clustering_shape@stale-ddl(on-disk static)",
            pk_only_label: "cassandra/static_clustering_shape@stale-ddl+pk-only",
            keyspace: "test_writeparity",
            table: "static_clustering_shape",
            // NOTE: `sdata text static` is deliberately ABSENT.
            ddl: "CREATE TABLE static_clustering_shape (id int, ck int, rdata text, PRIMARY KEY (id, ck))"
                .to_string(),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 1,
            pk_only_projection: vec![],
            refuses_fast_arm: true,
            refused_error_substr: None,
            columns: vec![],
            token_of_int_pk: None,
        },
        DatasetCase {
            label: "cassandra/static_clustering_shape(declared static, both arms)",
            pk_only_label: "cassandra/static_clustering_shape@pk-only",
            keyspace: "test_writeparity",
            table: "static_clustering_shape",
            ddl: "CREATE TABLE test_writeparity.static_clustering_shape \
                  (id int, ck int, sdata text static, rdata text, PRIMARY KEY (id, ck))"
                .to_string(),
            pinned_now: ORACLE_PINNED_NOW,
            min_rows: 1,
            pk_only_projection: vec![],
            refuses_fast_arm: false,
            refused_error_substr: None,
            columns: vec![],
            token_of_int_pk: None,
        },
    ]
}

fn handle_missing(case: &DatasetCase, why: &str, failures: &mut Vec<String>) {
    let msg = format!("case {}: {why}", case.label);
    if require_fixtures() {
        failures.push(format!("REQUIRE_FIXTURES: {msg}"));
    } else {
        eprintln!("SKIP {msg}");
    }
}

/// The value of `column` in the row whose `key_col` renders as `key_val`.
fn cell(rows: &[Row], key_col: &str, key_val: &str, column: &str) -> Option<String> {
    rows.iter()
        .find(|r| r.get(key_col).map(String::as_str) == Some(key_val))
        .and_then(|r| r.get(column).cloned())
}

/// The tombstone semantics the fixture encodes, asserted on the (already
/// arm-identical) output so a shared misreading of the fixture cannot pass.
fn assert_semantics(rows: &[Row], failures: &mut Vec<String>) {
    let pks: Vec<&str> = rows
        .iter()
        .filter_map(|r| r.get("pk").map(String::as_str))
        .collect();
    if pks.contains(&"3") {
        failures.push("a partition-deleted partition must not surface any row".into());
    }
    let cks: Vec<&str> = rows
        .iter()
        .filter_map(|r| r.get("ck").map(String::as_str))
        .collect();
    if cks.contains(&"4") {
        failures.push("a row-deleted row must not surface".into());
    }
    if cks.contains(&"10") || cks.contains(&"11") {
        failures.push("range-tombstoned rows must not surface".into());
    }
    if !cks.contains(&"99") {
        failures.push("the row outside the range tombstone must survive".into());
    }
    if cell(rows, "ck", "3", "v").as_deref() != Some("two-column") {
        failures.push("a live multi-column row must surface both of its cells".into());
    }
    if cell(rows, "ck", "3", "w").as_deref() != Some("also-live") {
        failures.push("a live multi-column row must surface both of its cells".into());
    }
    // Issue #3094: the SIMPLE CELL TOMBSTONE row. The row itself survives (its
    // liveness marker and its `v` cell are live), the deleted `w` reads NULL, and
    // it never surfaces a raw tombstone value.
    if !cks.contains(&"5") {
        failures.push(
            "the row whose `w` was deleted by a cell tombstone must still surface \
             (its liveness marker and `v` cell are live) — issue #3094"
                .into(),
        );
    }
    if cell(rows, "ck", "5", "v").as_deref() != Some("cell-tomb-row") {
        failures.push(format!(
            "the cell-tombstone row's sibling `v` must stay live, got {:?} (#3094)",
            cell(rows, "ck", "5", "v")
        ));
    }
    match cell(rows, "ck", "5", "w").as_deref() {
        Some("<null>") => {}
        other => failures.push(format!(
            "a DELETED cell must read as NULL, got {other:?} — a raw \
             `Value::Tombstone` here is what used to fail the whole `do_get` stream \
             in the Arrow encoder (issue #3094)"
        )),
    }
}
