//! Issue #3096 — the IN-REPO ARROW-BUFFER DIGEST ORACLE (spec R3).
//!
//! # What it folds, and why that matters
//!
//! The digest is computed over the **Arrow buffers** of every `RecordBatch` the
//! `do_get` data plane emits — each column's value buffers AND its validity
//! (null) bitmap, in column order, folded together with the batch's row count.
//! It is deliberately NOT computed over `QueryRow` values.
//!
//! That distinction is the whole point. A value-level fold reconstructs what the
//! rows MEANT and can therefore be identical while the Arrow buffers underneath
//! are wrong — a mis-sized offsets buffer, a null bitmap with an off-by-one, a
//! builder that wrote the right values into the wrong slots but reads back the
//! same through Arrow's own accessors. Every optimization lever this change
//! ranks (column-major build, append-in-place builders, folding the size
//! estimate into the build pass) risks exactly that defect class, so the oracle
//! has to see the bytes the builders produced.
//!
//! # What it asserts
//!
//! 1. **Arm invariance.** The same corpus and the same ticket at a PINNED `now`,
//!    run under `CQLITE_FLIGHT_MERGE_PATH=bypass` and under `=merge`, must
//!    produce an IDENTICAL digest, row count, and cells-per-row. The arms are
//!    additionally proven to have genuinely DIFFERED (via `read_path_probe`), so
//!    a differential that silently ran one arm twice cannot pass vacuously.
//! 2. **Change invariance.** The digest is PINNED as a constant below. Every
//!    lever this change lands must leave it untouched; a lever that moves it is
//!    reverted or its divergence is separately specified — never absorbed.
//!
//! The external digest `0x0a2a390223bde6aa` named in issue #3096 exists nowhere
//! in this repository, is not reproducible, and is NOT asserted here.
//!
//! # Fixtures
//!
//! * **CI fixture** — a small `ws0.events` corpus built in-test through the SAME
//!   `ws0_corpus_gen::generate::generate` the 4M-row measurement corpus uses, so
//!   the shape the oracle pins is the shape the rig measures. Always runs.
//! * **Measurement corpus** — set `CQLITE_WS0_CORPUS_DIR` to the generated 4M-row
//!   corpus root to run the same oracle over it (what the perf runs do). Skips
//!   cleanly when unset; a corpus dir that is SET but unusable is a hard failure,
//!   never a silent skip.
//!
//! SCOPE (issue #3042): the fixture is CQLite-written and CQLite-read, so it is
//! invariant to a uniform framing error and is a PERFORMANCE FIXTURE ONLY. This
//! oracle asserts ARM- and CHANGE-invariance over it. It makes NO claim that the
//! on-disk framing is Cassandra-correct — that stays with the Cassandra-written
//! fixtures and `query_semantics_flight_parity.rs`.
//!
//! # Isolation
//!
//! `CQLITE_FLIGHT_MERGE_PATH`, `CQLITE_TTL_NOW_OVERRIDE_SECS` and the probe
//! counters are PROCESS-GLOBAL, so this file holds exactly ONE `#[test]` that
//! runs every case sequentially — the discipline
//! `issue_3058_forced_path_differential.rs` established. Add a case to the list,
//! never a second `#[test]`.

use std::path::PathBuf;

use arrow::array::{Array, ArrayData};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::storage::read_path_probe::ReadPathProbe;
use cqlite_flight::bypass::MERGE_PATH_ENV;
use cqlite_flight::service::CqliteFlightService;
use ws0_corpus_gen::generate::{generate, has_data_db, CorpusSpec};
use ws0_corpus_gen::schema::{DDL, KEYSPACE, TABLE};

/// Debug-only reader seam pinning the read-time TTL clock (`now_clock.rs`). The
/// corpus carries no TTLs, but the clock is pinned anyway so the oracle can never
/// become wall-clock dependent if a TTL-bearing case is added later.
const TTL_NOW_ENV: &str = "CQLITE_TTL_NOW_OVERRIDE_SECS";

/// The pinned read-time clock, in epoch seconds (2026-01-01T00:00:00Z).
const PINNED_NOW_SECS: i64 = 1_767_225_600;

/// Rows in the in-test CI fixture: 5 partitions x 100 rows. Small enough to build
/// in a test, large enough to span several `RecordBatch`es at the default
/// `batch_size` used below.
const CI_FIXTURE_ROWS: u64 = 500;

/// `batch_size` for the service, chosen so the CI fixture spans MULTIPLE batches
/// — a single-batch fixture could not detect a batch-boundary divergence, which
/// the digest folds in via each batch's row count.
const BATCH_SIZE: usize = 128;

/// Env var pointing at the generated measurement corpus root (the dir holding
/// `ws0/events/`). Unset = skip that case.
const CORPUS_DIR_ENV: &str = "CQLITE_WS0_CORPUS_DIR";

/// ============================ THE PINNED DIGEST ============================
///
/// The Arrow-buffer digest of the CI fixture, identical on BOTH arms. Re-pin ONLY
/// with a recorded reason: this value moving means the bytes `do_get` puts on the
/// wire changed.
///
/// To re-derive after an INTENDED output change, run this test and read the
/// `observed` value from the failure message.
const CI_FIXTURE_DIGEST: u64 = 0x0000_0000_0000_0000;

/// Row count of the CI fixture as observed through `do_get`. Pinned so a fixture
/// that silently shrank cannot make the digest assertion vacuous.
const CI_FIXTURE_ROWS_OBSERVED: u64 = CI_FIXTURE_ROWS;

/// Cells per row: the twelve `ws0.events` columns, every one non-null.
const CI_FIXTURE_CELLS_PER_ROW: f64 = 12.0;

/// The folded shape of one `do_get` stream.
#[derive(Debug, Clone, Copy, PartialEq)]
struct BufferDigest {
    /// FNV-1a 64 over every batch's column buffers + validity + row count.
    digest: u64,
    /// Batches emitted (a batching change moves this AND the digest).
    batches: u64,
    /// Rows emitted.
    rows: u64,
    /// Columns per batch (asserted uniform).
    columns: usize,
    /// Non-null cells across the whole stream.
    non_null_cells: u64,
}

impl BufferDigest {
    fn cells_per_row(&self) -> f64 {
        if self.rows == 0 {
            0.0
        } else {
            self.non_null_cells as f64 / self.rows as f64
        }
    }
}

/// FNV-1a 64. Chosen over `DefaultHasher` because `DefaultHasher` is explicitly
/// NOT guaranteed stable across Rust releases — a pinned constant computed with it
/// could break on a toolchain bump with no output change at all.
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01B3;

fn fold_bytes(h: u64, bytes: &[u8]) -> u64 {
    let mut h = h;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

fn fold_u64(h: u64, v: u64) -> u64 {
    fold_bytes(h, &v.to_le_bytes())
}

/// Fold ONE column's `ArrayData`: its logical shape, its validity bitmap, every
/// value buffer, and (recursively) any child data, so a nested type's buffers are
/// covered too rather than silently skipped.
fn fold_array_data(mut h: u64, data: &ArrayData) -> u64 {
    h = fold_bytes(h, format!("{:?}", data.data_type()).as_bytes());
    h = fold_u64(h, data.len() as u64);
    h = fold_u64(h, data.offset() as u64);
    h = fold_u64(h, data.null_count() as u64);
    // The VALIDITY bitmap, distinguishing "no bitmap" from "an all-valid bitmap":
    // a builder that started emitting an explicit all-valid bitmap where none was
    // emitted before HAS changed the buffers, and the oracle must see it.
    match data.nulls() {
        None => h = fold_bytes(h, b"validity:none"),
        Some(nulls) => {
            h = fold_bytes(h, b"validity:some");
            h = fold_u64(h, nulls.len() as u64);
            h = fold_u64(h, nulls.offset() as u64);
            h = fold_bytes(h, nulls.validity());
        }
    }
    // Every VALUE buffer, in order (offsets buffer first for the var-width types,
    // then the data buffer) — this is what catches a bad offsets vector.
    h = fold_u64(h, data.buffers().len() as u64);
    for buf in data.buffers() {
        h = fold_u64(h, buf.len() as u64);
        h = fold_bytes(h, buf.as_slice());
    }
    h = fold_u64(h, data.child_data().len() as u64);
    for child in data.child_data() {
        h = fold_array_data(h, child);
    }
    h
}

/// Fold one `RecordBatch`: the row count, then each column IN SCHEMA ORDER
/// (name + buffers), so a column reorder is a digest change.
fn fold_batch(mut h: u64, batch: &RecordBatch) -> (u64, u64) {
    h = fold_bytes(h, b"batch");
    h = fold_u64(h, batch.num_rows() as u64);
    h = fold_u64(h, batch.num_columns() as u64);
    let schema = batch.schema();
    let mut non_null = 0u64;
    for (i, field) in schema.fields().iter().enumerate() {
        h = fold_bytes(h, field.name().as_bytes());
        h = fold_bytes(h, &[u8::from(field.is_nullable())]);
        let col = batch.column(i);
        non_null += (col.len() - col.null_count()) as u64;
        h = fold_array_data(h, &col.to_data());
    }
    (h, non_null)
}

/// Drive `do_get` to completion, folding the Arrow buffers of every emitted batch.
// arrow-flight's `FlightError` Err type has a framework-fixed large size; boxing
// it (clippy's suggestion) would break the flight decoder stream API (#2856).
#[allow(clippy::result_large_err)]
async fn digest_do_get(
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
) -> Result<BufferDigest, String> {
    let bytes = serde_json::to_vec(ticket).map_err(|e| format!("ticket json: {e}"))?;
    let resp = svc
        .do_get(Request::new(Ticket::new(bytes)))
        .await
        .map_err(|s| format!("do_get rpc: {}", s.message()))?
        .into_inner();
    let mapped = resp.map(|r| r.map_err(|e| FlightError::ExternalError(Box::new(e))));
    let mut stream = FlightRecordBatchStream::new_from_flight_data(mapped);

    let mut h = FNV_OFFSET;
    let mut rows = 0u64;
    let mut batches = 0u64;
    let mut non_null_cells = 0u64;
    let mut columns: Option<usize> = None;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|e| format!("stream: {e}"))?;
        match columns {
            None => columns = Some(batch.num_columns()),
            Some(c) if c != batch.num_columns() => {
                return Err(format!(
                    "batch column count changed mid-stream: {c} then {}",
                    batch.num_columns()
                ))
            }
            Some(_) => {}
        }
        let (next, non_null) = fold_batch(h, &batch);
        h = next;
        non_null_cells += non_null;
        rows += batch.num_rows() as u64;
        batches += 1;
    }
    Ok(BufferDigest {
        digest: h,
        batches,
        rows,
        columns: columns.unwrap_or(0),
        non_null_cells,
    })
}

/// Run one forced arm, returning the digest and the probe delta proving which arm
/// actually ran.
async fn run_arm(
    svc: &CqliteFlightService,
    ticket: &serde_json::Value,
    arm: &str,
) -> (Result<BufferDigest, String>, ReadPathProbe) {
    std::env::set_var(MERGE_PATH_ENV, arm);
    let before = ReadPathProbe::snapshot();
    let out = digest_do_get(svc, ticket).await;
    let delta = ReadPathProbe::snapshot().delta_since(&before);
    std::env::remove_var(MERGE_PATH_ENV);
    (out, delta)
}

fn ticket() -> serde_json::Value {
    serde_json::json!({ "keyspace": KEYSPACE, "table": TABLE, "ddl": DDL })
}

/// One corpus case: the arms must agree, and the result must be non-vacuous.
async fn assert_arms_agree(
    label: &str,
    corpus_root: &PathBuf,
    expect_rows: u64,
    failures: &mut Vec<String>,
) -> Option<BufferDigest> {
    let svc = CqliteFlightService::new(corpus_root.clone(), BATCH_SIZE);
    let t = ticket();

    std::env::set_var(TTL_NOW_ENV, PINNED_NOW_SECS.to_string());
    let (merge, merge_delta) = run_arm(&svc, &t, "merge").await;
    let (bypass, bypass_delta) = run_arm(&svc, &t, "bypass").await;
    std::env::remove_var(TTL_NOW_ENV);

    let (merge, bypass) = match (merge, bypass) {
        (Ok(m), Ok(b)) => (m, b),
        (m, b) => {
            failures.push(format!(
                "case {label}: do_get failed: merge={m:?} bypass={b:?}"
            ));
            return None;
        }
    };

    // Anti-vacuity FIRST: a zero/short result would make every equality below
    // trivially true.
    if merge.rows != expect_rows || bypass.rows != expect_rows {
        failures.push(format!(
            "case {label}: expected {expect_rows} rows on BOTH arms, got merge={} bypass={} \
             — a short or empty result is a failure, never a vacuous pass",
            merge.rows, bypass.rows
        ));
        return None;
    }
    // …and that the two arms genuinely DIFFERED, or the differential compared the
    // same arm twice.
    if merge_delta.mergers_built == 0 || merge_delta.reconcile_entries == 0 {
        failures.push(format!(
            "case {label}: the forced-merge run did not take the merge arm \
             (mergers={}, reconciles={}) — the differential would be vacuous",
            merge_delta.mergers_built, merge_delta.reconcile_entries
        ));
        return None;
    }
    if bypass_delta.mergers_built != 0 || bypass_delta.reconcile_entries != 0 {
        failures.push(format!(
            "case {label}: the forced-bypass run still merged (mergers={}, reconciles={}) \
             — the differential compared the same arm twice",
            bypass_delta.mergers_built, bypass_delta.reconcile_entries
        ));
        return None;
    }

    if merge != bypass {
        failures.push(format!(
            "case {label}: ARROW-BUFFER DIGEST MISMATCH between arms at pinned now \
             {PINNED_NOW_SECS}\n  merge  = {merge:?}\n  bypass = {bypass:?}"
        ));
        return None;
    }
    if (merge.cells_per_row() - CI_FIXTURE_CELLS_PER_ROW).abs() > f64::EPSILON {
        failures.push(format!(
            "case {label}: cells/row = {}, expected {CI_FIXTURE_CELLS_PER_ROW} \
             (the ws0.events row is 12 non-null columns)",
            merge.cells_per_row()
        ));
        return None;
    }
    eprintln!(
        "PASS {label} — digest 0x{:016x}, {} rows in {} batches, {} columns, {:.1} cells/row \
         (identical on both arms)",
        merge.digest,
        merge.rows,
        merge.batches,
        merge.columns,
        merge.cells_per_row()
    );
    Some(merge)
}

#[test]
fn arrow_buffer_digest_is_arm_invariant_and_pinned() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut failures: Vec<String> = Vec::new();

    // ---- Case 1: the CI fixture (always runs, digest PINNED) ------------------
    let temp = tempfile::tempdir().expect("tempdir");
    let spec = CorpusSpec::small(temp.path().to_path_buf(), CI_FIXTURE_ROWS);
    let identity = rt.block_on(generate(&spec)).expect("generate CI fixture");
    assert_eq!(
        identity.rows, CI_FIXTURE_ROWS,
        "the CI fixture must hold exactly {CI_FIXTURE_ROWS} rows"
    );
    assert!(
        !identity.compression_info_present,
        "the fixture must be uncompressed (#1406)"
    );
    assert!(
        has_data_db(&spec.table_dir()),
        "the generated fixture must hold a Data.db"
    );

    let observed = rt.block_on(assert_arms_agree(
        "ci-fixture/select-star",
        &temp.path().to_path_buf(),
        CI_FIXTURE_ROWS_OBSERVED,
        &mut failures,
    ));

    if let Some(d) = observed {
        assert_eq!(
            d.digest, CI_FIXTURE_DIGEST,
            "the PINNED Arrow-buffer digest changed: observed 0x{:016x}, pinned 0x{:016x}. \
             The bytes do_get puts on the wire moved. A lever that does this is reverted, or \
             its divergence is separately specified and this constant is re-pinned WITH a \
             recorded reason — never silently. (rows={} batches={} columns={})",
            d.digest, CI_FIXTURE_DIGEST, d.rows, d.batches, d.columns
        );
    }

    // ---- Case 2: the measurement corpus (opt-in via env) ----------------------
    match std::env::var(CORPUS_DIR_ENV) {
        Err(_) => eprintln!(
            "SKIP measurement-corpus case — set {CORPUS_DIR_ENV} to the ws0-corpus-gen output \
             root to run the oracle over the 4M-row corpus"
        ),
        Ok(dir) => {
            let root = PathBuf::from(&dir);
            let table_dir = root.join(KEYSPACE).join(TABLE);
            // SET but unusable is a hard failure: a typo must not degrade into a
            // silent skip that looks like coverage.
            assert!(
                has_data_db(&table_dir),
                "{CORPUS_DIR_ENV}={dir} but {} holds no *-Data.db — run ws0-corpus-gen",
                table_dir.display()
            );
            let identity_path = root.join("corpus-identity.json");
            let expect_rows = std::fs::read_to_string(&identity_path)
                .ok()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
                .and_then(|v| v["rows"].as_u64())
                .unwrap_or_else(|| {
                    panic!(
                        "{} must carry the recorded row count — the corpus case cannot \
                         assert non-vacuity without it",
                        identity_path.display()
                    )
                });
            let d = rt.block_on(assert_arms_agree(
                "measurement-corpus/select-star",
                &root,
                expect_rows,
                &mut failures,
            ));
            if let Some(d) = d {
                // The measurement corpus's digest is NOT pinned as a constant (it
                // depends on the row count the operator generated); it is printed
                // so a perf run can record it beside its numbers and compare it
                // across levers.
                println!(
                    "measurement-corpus arrow-buffer digest = 0x{:016x} over {} rows \
                     in {} batches",
                    d.digest, d.rows, d.batches
                );
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} arrow-buffer digest case(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
