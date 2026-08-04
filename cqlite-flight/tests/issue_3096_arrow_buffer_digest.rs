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
//! # TWO TAPS, TWO PINNED DIGESTS — deliberately not one (roborev finding 1)
//!
//! The same fold runs at two DIFFERENT points of the data plane, and each has a
//! digest of its own:
//!
//! * **Producer tap** ([`producer_digest`]) — the `RecordBatch`es the Arrow
//!   builders hand to the egress sink, folded BEFORE `streaming.rs::encode_do_get`
//!   ever sees them. This is the tap that makes "a defect in the Arrow builders is
//!   visible" TRUE: no IPC serialization, no client-side decode, so nothing can
//!   normalize a buffer representation on the way past.
//! * **Wire tap** ([`digest_do_get`]) — the batches a Flight CLIENT decodes off
//!   `do_get`, i.e. after IPC serialization and reconstruction. This one is
//!   NOT redundant and is NOT collapsed into the producer tap: it is what
//!   actually observes bypass-vs-merge invariance END TO END, through the exact
//!   surface a Trino/Arrow client consumes.
//!
//! Keeping both is the point. The producer tap alone would stop watching the
//! served surface; the wire tap alone cannot see a builder defect that the IPC
//! round trip happens to normalize away. Neither substitutes for the other, so
//! both are pinned separately below.
//!
//! # What it asserts
//!
//! 1. **Arm invariance.** The same corpus and the same ticket at a PINNED `now`,
//!    run under `CQLITE_FLIGHT_MERGE_PATH=bypass` and under `=merge`, must
//!    produce an IDENTICAL digest, row count, and cells-per-row — AT BOTH TAPS.
//!    The arms are additionally proven to have genuinely DIFFERED (via
//!    `read_path_probe`, checked per tap), so a differential that silently ran one
//!    arm twice cannot pass vacuously.
//! 2. **Cross-tap census agreement.** The two taps see different BYTES (an IPC
//!    round trip may legitimately re-lay-out a buffer) but must agree on the
//!    logical census — rows, columns, batch count, and non-null cells. A
//!    disagreement there is a real defect in the round trip, not a
//!    representation difference.
//! 3. **Change invariance.** BOTH digests are PINNED as constants below. Every
//!    lever this change lands must leave them untouched; a lever that moves one is
//!    reverted or its divergence is separately specified — never absorbed.
//!
//! The external digest `0x0a2a390223bde6aa` named in issue #3096 exists nowhere
//! in this repository, is not reproducible, and is NOT asserted here.
//!
//! # Fixtures
//!
//! * **CI fixture** — a small `ws0.events` corpus built in-test by the
//!   self-contained fixture builder in `tests/support/ws0_fixture.rs`. Needs no
//!   fetched dataset and no external corpus, so it ALWAYS runs, and the pinned
//!   digest below is its digest.
//! * **Measurement corpus** — set `CQLITE_WS0_CORPUS_DIR` to a generated 4M-row
//!   corpus root to run the same oracle over it (what a perf run does). The
//!   generator + driver scripts that PRODUCE such a corpus are re-anchored to
//!   issue #3272 and are not in this change, so this arm skips cleanly with an
//!   explicit reason when the env var is unset. A corpus dir that is SET but
//!   unusable stays a hard failure, never a silent skip.
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

use std::path::{Path, PathBuf};

use arrow::array::{Array, ArrayData};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::schema::{parse_cql_schema, udt_registry_from_cql};
use cqlite_core::storage::read_path_probe::ReadPathProbe;
use cqlite_flight::bypass::MERGE_PATH_ENV;
use cqlite_flight::cancel::CancelFlag;
use cqlite_flight::filter::ScanSpec;
use cqlite_flight::producer::MergeProducer;
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::ticket::FlightTicket;
use cqlite_flight::warm::{ddl_hash, TableKey, WarmTableRegistry};
#[path = "support/ws0_fixture.rs"]
mod ws0_fixture;
use ws0_fixture::{
    assert_ddl_matches_the_committed_pin, generate, has_data_db, CorpusSpec, DDL, KEYSPACE, TABLE,
};

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

/// ============================ THE PINNED DIGESTS ============================
///
/// # Re-pin log
///
/// A pinned oracle whose value changes without a written reason is
/// indistinguishable from one that was quietly adjusted to pass. Every change to
/// either constant is therefore recorded here, old → new, with its reason and its
/// commit. The same table is mirrored in
/// `docs/reports/ws0-3096-artifacts/digest-oracle-repin.md`.
///
/// | Digest | Old | New | Reason | Commit |
/// |---|---|---|---|---|
/// | wire | `0xd0014e42e893f87f` | (unchanged in this commit) | — | — |
/// | producer | (did not exist) | `0xd0014e42e893f87f` | NEW TAP: roborev finding 1 — the pre-existing digest hashed batches only AFTER Flight IPC serialization and client-side decoding, so an Arrow-builder defect the round trip normalized was invisible. This tap folds the producer's `RecordBatch`es BEFORE `encode_do_get`. | (this commit) |
///
/// **Measured on introduction:** over the null-free fixture the producer digest
/// came out EQUAL to the wire digest (`0xd0014e42e893f87f`) — for this shape the
/// IPC round trip is byte-preserving, so the pre-existing wire digest did happen
/// to reflect the builders' output. That was a COINCIDENCE of the shape (no
/// validity bitmaps to normalize, no sliced/offset buffers), not a property of the
/// round trip, and it is exactly what the wire tap could not tell anyone. The
/// producer tap removes the dependence on it.
///
/// To re-derive after an INTENDED output change, run this test and read the
/// `observed` value from the failure message.
///
/// ---------------------------------------------------------------------------
/// The **WIRE** digest: the batches a Flight client decodes off `do_get`,
/// identical on BOTH arms. This value moving means the bytes `do_get` puts on the
/// wire changed.
///
/// PINNED 2026-08-03 on the Phase-0 (pre-lever) binary, over the 500-row
/// `ws0.events` CI fixture at `BATCH_SIZE` = 128: 4 batches (128/128/128/116),
/// 12 columns, 12.0 cells/row, identical under `bypass` and `merge`.
const CI_FIXTURE_WIRE_DIGEST: u64 = 0xd001_4e42_e893_f87f;

/// The **PRODUCER** digest: the same fold over the `RecordBatch`es the Arrow
/// builders produced, taken BEFORE `encode_do_get`. This value moving means the
/// builders' output changed — which is the signal the wire digest alone could not
/// give, because an IPC round trip can normalize a buffer representation.
///
/// It is NOT expected to equal the wire digest: the two fold different (though
/// census-equal) byte layouts of the same rows.
const CI_FIXTURE_PRODUCER_DIGEST: u64 = 0xd001_4e42_e893_f87f;

/// Row count of the CI fixture as observed through `do_get`. Pinned so a fixture
/// that silently shrank cannot make the digest assertion vacuous.
const CI_FIXTURE_ROWS_OBSERVED: u64 = CI_FIXTURE_ROWS;

/// NON-NULL cells the whole stream must carry, pinned as an EXACT integer rather
/// than a float cells-per-row ratio: an integer census cannot be satisfied
/// approximately, and it is the currency a validity-bitmap change moves.
///
/// `CI_FIXTURE_ROWS` x 12 columns, every cell present.
const CI_FIXTURE_NON_NULL_CELLS: u64 = CI_FIXTURE_ROWS * 12;

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

/// Fold the PRODUCER's `RecordBatch`es — the Arrow builders' own output, taken
/// BEFORE `streaming.rs::encode_do_get` (roborev finding 1).
///
/// # Why this is the production path and not a lookalike
///
/// [`MergeProducer::produce_streaming_from_readers_to_vec`] is a public wrapper
/// around `produce_streaming_from_readers`, the EXACT function `do_get`'s row
/// route drives over its warm reader set — including the
/// `bypass_reason(.., ForcedMergePath::from_env(), ..)` arm decision, so this tap
/// honors `CQLITE_FLIGHT_MERGE_PATH` just as the wire tap does. Every input is
/// built the way `service.rs::build_producer` / `do_get_resolve` build it:
///
/// * schema — `parse_cql_schema(DDL)`, the same parse the service performs on the
///   ticket's DDL (never the in-code `ws0_events_schema()`, which would be a
///   second source of truth);
/// * scan spec — `ScanSpec::from_ticket`, so a projection/predicate/token bound
///   would reach the producer identically;
/// * UDT registry — `udt_registry_from_cql(DDL, keyspace)` (a no-op for this
///   DDL, which declares no `CREATE TYPE`, but wired so it cannot silently
///   diverge);
/// * byte cap — the service's own configured `max_batch_bytes`;
/// * readers — `WarmTableRegistry::warm_readers`, the same registry call.
fn producer_digest(svc: &CqliteFlightService, corpus_root: &Path) -> Result<BufferDigest, String> {
    let ticket: FlightTicket =
        serde_json::from_value(ticket()).map_err(|e| format!("ticket decode: {e}"))?;
    let schema = parse_cql_schema(&ticket.ddl).map_err(|e| format!("parse ddl: {e}"))?;
    let spec = ScanSpec::from_ticket(&ticket, &schema).map_err(|e| format!("scan spec: {e}"))?;
    let registry = udt_registry_from_cql(&ticket.ddl, &ticket.keyspace);
    let producer = MergeProducer::with_spec(schema.clone(), BATCH_SIZE, spec)
        .map_err(|e| format!("producer: {e}"))?
        .with_max_batch_bytes(svc.max_batch_bytes())
        .with_udt_registry(registry.clone());

    let table_dir = corpus_root.join(&ticket.keyspace).join(&ticket.table);
    let warm = WarmTableRegistry::new();
    let set = warm
        .warm_readers(
            &TableKey::new(&ticket.keyspace, &ticket.table),
            ddl_hash(&ticket.ddl),
            &schema,
            Some(&registry),
            &table_dir,
            ticket.snapshot.as_deref(),
            &CancelFlag::new(),
        )
        .map_err(|e| format!("warm readers: {e}"))?;
    if set.readers.is_empty() {
        return Err(format!(
            "the warm reader set over {} is EMPTY — a 0-batch producer digest would \
             be vacuous",
            table_dir.display()
        ));
    }

    let batches = producer
        .produce_streaming_from_readers_to_vec(set.readers.clone(), &CancelFlag::new())
        .map_err(|e| format!("produce: {e}"))?;

    let mut h = FNV_OFFSET;
    let mut rows = 0u64;
    let mut non_null_cells = 0u64;
    let mut columns: Option<usize> = None;
    for batch in &batches {
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
        let (next, non_null) = fold_batch(h, batch);
        h = next;
        non_null_cells += non_null;
        rows += batch.num_rows() as u64;
    }
    Ok(BufferDigest {
        digest: h,
        batches: batches.len() as u64,
        rows,
        columns: columns.unwrap_or(0),
        non_null_cells,
    })
}

/// One forced arm, observed at BOTH taps.
#[derive(Debug)]
struct ArmObservation {
    /// The producer tap (pre-`encode_do_get`).
    producer: BufferDigest,
    /// The wire tap (post-IPC, client-decoded).
    wire: BufferDigest,
    /// Probe delta across the PRODUCER run, proving which arm it took.
    producer_probe: ReadPathProbe,
    /// Probe delta across the WIRE run, proving which arm it took.
    wire_probe: ReadPathProbe,
}

/// Run `f` with the read-path probe sampled either side of it.
fn probed<T>(f: impl FnOnce() -> T) -> (T, ReadPathProbe) {
    let before = ReadPathProbe::snapshot();
    let out = f();
    (out, ReadPathProbe::snapshot().delta_since(&before))
}

/// Run one forced arm at both taps. `MERGE_PATH_ENV` is process-global, so it is
/// set once and covers BOTH runs — the producer tap and the wire tap therefore
/// observe the SAME arm, which is what makes their census comparison meaningful.
///
/// SYNCHRONOUS, and it owns the `block_on` for the wire tap rather than being
/// driven from inside one. `WarmTableRegistry::warm_readers` builds its own
/// runtime internally (`warm/rebuild.rs`), so the producer tap MUST be called
/// from a thread with no active runtime — calling it inside `rt.block_on` panics
/// with "Cannot start a runtime from within a runtime". Hence: producer tap on the
/// bare test thread, wire tap inside its own `block_on`, both under one arm.
fn run_arm(
    rt: &tokio::runtime::Runtime,
    svc: &CqliteFlightService,
    corpus_root: &Path,
    ticket: &serde_json::Value,
    arm: &str,
) -> Result<ArmObservation, String> {
    std::env::set_var(MERGE_PATH_ENV, arm);
    let (producer, producer_probe) = probed(|| producer_digest(svc, corpus_root));
    let (wire, wire_probe) = probed(|| rt.block_on(digest_do_get(svc, ticket)));
    std::env::remove_var(MERGE_PATH_ENV);
    Ok(ArmObservation {
        producer: producer.map_err(|e| format!("producer tap: {e}"))?,
        wire: wire.map_err(|e| format!("wire tap: {e}"))?,
        producer_probe,
        wire_probe,
    })
}

fn ticket() -> serde_json::Value {
    serde_json::json!({ "keyspace": KEYSPACE, "table": TABLE, "ddl": DDL })
}

/// The probe delta a forced arm MUST show, or the differential compared the same
/// arm twice. Returns an error string naming the tap and the arm on violation.
fn assert_arm_taken(
    label: &str,
    tap: &str,
    arm: &str,
    delta: &ReadPathProbe,
) -> Result<(), String> {
    match arm {
        "merge" => {
            if delta.mergers_built == 0 || delta.reconcile_entries == 0 {
                return Err(format!(
                    "case {label}: the forced-merge run did not take the merge arm at the \
                     {tap} tap (mergers={}, reconciles={}) — the differential would be vacuous",
                    delta.mergers_built, delta.reconcile_entries
                ));
            }
        }
        "bypass" => {
            if delta.mergers_built != 0 || delta.reconcile_entries != 0 {
                return Err(format!(
                    "case {label}: the forced-bypass run still merged at the {tap} tap \
                     (mergers={}, reconciles={}) — the differential compared the same arm twice",
                    delta.mergers_built, delta.reconcile_entries
                ));
            }
        }
        other => return Err(format!("case {label}: unknown arm {other:?}")),
    }
    Ok(())
}

/// Both taps of one corpus case, once the arms have been proven to agree.
#[derive(Debug, Clone, Copy)]
struct CaseDigests {
    /// The arm-invariant PRODUCER digest (pre-`encode_do_get`).
    producer: BufferDigest,
    /// The arm-invariant WIRE digest (post-IPC).
    wire: BufferDigest,
}

/// One corpus case: at BOTH taps the arms must agree, the two taps must agree on
/// the logical census, and the result must be non-vacuous.
fn assert_arms_agree(
    rt: &tokio::runtime::Runtime,
    label: &str,
    corpus_root: &Path,
    expect_rows: u64,
    failures: &mut Vec<String>,
) -> Option<CaseDigests> {
    let svc = CqliteFlightService::new(corpus_root.to_path_buf(), BATCH_SIZE);
    let t = ticket();

    std::env::set_var(TTL_NOW_ENV, PINNED_NOW_SECS.to_string());
    let merge = run_arm(rt, &svc, corpus_root, &t, "merge");
    let bypass = run_arm(rt, &svc, corpus_root, &t, "bypass");
    std::env::remove_var(TTL_NOW_ENV);

    let (merge, bypass) = match (merge, bypass) {
        (Ok(m), Ok(b)) => (m, b),
        (m, b) => {
            failures.push(format!(
                "case {label}: a tap failed: merge={m:?} bypass={b:?}"
            ));
            return None;
        }
    };

    // Anti-vacuity FIRST: a zero/short result would make every equality below
    // trivially true. Checked at BOTH taps on BOTH arms.
    for (arm, obs) in [("merge", &merge), ("bypass", &bypass)] {
        for (tap, d) in [("producer", &obs.producer), ("wire", &obs.wire)] {
            if d.rows != expect_rows {
                failures.push(format!(
                    "case {label}: expected {expect_rows} rows at the {tap} tap on the {arm} \
                     arm, got {} — a short or empty result is a failure, never a vacuous pass",
                    d.rows
                ));
                return None;
            }
        }
    }
    // …and that the two arms genuinely DIFFERED, at each tap independently: a tap
    // that ignored the forced arm would compare the same arm twice.
    for (arm, obs) in [("merge", &merge), ("bypass", &bypass)] {
        for (tap, delta) in [("producer", &obs.producer_probe), ("wire", &obs.wire_probe)] {
            if let Err(e) = assert_arm_taken(label, tap, arm, delta) {
                failures.push(e);
                return None;
            }
        }
    }

    // Arm invariance, per tap.
    if merge.producer != bypass.producer {
        failures.push(format!(
            "case {label}: PRODUCER-TAP ARROW-BUFFER DIGEST MISMATCH between arms at pinned \
             now {PINNED_NOW_SECS}\n  merge  = {:?}\n  bypass = {:?}",
            merge.producer, bypass.producer
        ));
        return None;
    }
    if merge.wire != bypass.wire {
        failures.push(format!(
            "case {label}: WIRE-TAP ARROW-BUFFER DIGEST MISMATCH between arms at pinned now \
             {PINNED_NOW_SECS}\n  merge  = {:?}\n  bypass = {:?}",
            merge.wire, bypass.wire
        ));
        return None;
    }

    let producer = merge.producer;
    let wire = merge.wire;

    // Cross-tap census agreement: the BYTES may legitimately differ (an IPC round
    // trip can re-lay-out a buffer — the very reason both taps exist), but the
    // rows, columns, batching and null census may not.
    if (
        producer.rows,
        producer.columns,
        producer.batches,
        producer.non_null_cells,
    ) != (wire.rows, wire.columns, wire.batches, wire.non_null_cells)
    {
        failures.push(format!(
            "case {label}: the two taps disagree on the LOGICAL CENSUS — the IPC round trip \
             changed rows/columns/batches/non-null cells, which is a defect, not a \
             representation difference\n  producer = {producer:?}\n  wire     = {wire:?}"
        ));
        return None;
    }

    if producer.non_null_cells != wire.non_null_cells {
        failures.push(format!(
            "case {label}: non-null cell counts differ between taps: producer={} wire={}",
            producer.non_null_cells, wire.non_null_cells
        ));
        return None;
    }
    eprintln!(
        "PASS {label} — producer digest 0x{:016x}, wire digest 0x{:016x}; {} rows in {} \
         batches, {} columns, {} non-null cells ({:.2} cells/row) — each digest identical on \
         both arms",
        producer.digest,
        wire.digest,
        wire.rows,
        wire.batches,
        wire.columns,
        wire.non_null_cells,
        wire.cells_per_row()
    );
    Some(CaseDigests { producer, wire })
}

#[test]
fn arrow_buffer_digest_is_arm_invariant_and_pinned() {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut failures: Vec<String> = Vec::new();

    // ---- Case 1: the CI fixture (always runs, digest PINNED) ------------------
    // The fixture's schema must still be the committed `ws0.events` DDL: a drifted
    // DDL would move the pinned digest for a reason that has nothing to do with the
    // encode path.
    assert_ddl_matches_the_committed_pin();
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

    let observed = assert_arms_agree(
        &rt,
        "ci-fixture/select-star",
        temp.path(),
        CI_FIXTURE_ROWS_OBSERVED,
        &mut failures,
    );

    if let Some(d) = observed {
        // The absolute census pin. `assert_arms_agree` only proved the two taps
        // agree WITH EACH OTHER; this is what proves they agree with the fixture.
        assert_eq!(
            d.wire.non_null_cells,
            CI_FIXTURE_NON_NULL_CELLS,
            "non-null cell census moved: observed {}, pinned {CI_FIXTURE_NON_NULL_CELLS} \
             ({:.2} cells/row over {} rows)",
            d.wire.non_null_cells,
            d.wire.cells_per_row(),
            d.wire.rows
        );
        assert_eq!(
            d.wire.digest, CI_FIXTURE_WIRE_DIGEST,
            "the PINNED WIRE Arrow-buffer digest changed: observed 0x{:016x}, pinned 0x{:016x}. \
             The bytes do_get puts on the wire moved. A lever that does this is reverted, or \
             its divergence is separately specified and this constant is re-pinned WITH a \
             recorded reason — never silently. (rows={} batches={} columns={})",
            d.wire.digest, CI_FIXTURE_WIRE_DIGEST, d.wire.rows, d.wire.batches, d.wire.columns
        );
        assert_eq!(
            d.producer.digest,
            CI_FIXTURE_PRODUCER_DIGEST,
            "the PINNED PRODUCER Arrow-buffer digest changed: observed 0x{:016x}, pinned \
             0x{:016x}. The Arrow BUILDERS' output moved — this is the tap the wire digest \
             cannot see, because an IPC round trip can normalize a buffer representation. Same \
             rule: revert the lever, or re-pin WITH a recorded reason. (rows={} batches={} \
             columns={})",
            d.producer.digest,
            CI_FIXTURE_PRODUCER_DIGEST,
            d.producer.rows,
            d.producer.batches,
            d.producer.columns
        );
        // NOT asserted either way, deliberately. Equal digests would mean the IPC
        // round trip is byte-preserving for this shape (legitimate); unequal means
        // it re-laid-out a buffer (also legitimate, and the reason the producer tap
        // exists). Reported so a change in that relationship is visible in the log
        // without pinning a property neither Arrow nor Flight guarantees.
        eprintln!(
            "taps {}: producer 0x{:016x} vs wire 0x{:016x}",
            if d.producer.digest == d.wire.digest {
                "fold IDENTICAL bytes (the IPC round trip preserved the layout)"
            } else {
                "fold DIFFERENT bytes (the IPC round trip re-laid-out at least one buffer)"
            },
            d.producer.digest,
            d.wire.digest
        );
    }

    // ---- Case 2: the measurement corpus (opt-in via env) ----------------------
    match std::env::var(CORPUS_DIR_ENV) {
        Err(_) => eprintln!(
            "SKIP measurement-corpus case — {CORPUS_DIR_ENV} is unset. This arm runs the \
             oracle over a generated 4M-row corpus; the corpus GENERATOR and the perf \
             driver scripts are re-anchored to issue #3272 and are not part of this \
             change, so point {CORPUS_DIR_ENV} at a corpus root produced there to enable \
             it. The CI-fixture case above is unaffected and always runs."
        ),
        Ok(dir) => {
            let root = PathBuf::from(&dir);
            let table_dir = root.join(KEYSPACE).join(TABLE);
            // SET but unusable is a hard failure: a typo must not degrade into a
            // silent skip that looks like coverage.
            assert!(
                has_data_db(&table_dir),
                "{CORPUS_DIR_ENV}={dir} but {} holds no *-Data.db — generate the corpus \
                 with the rig re-anchored to issue #3272, or unset {CORPUS_DIR_ENV}",
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
            let d = assert_arms_agree(
                &rt,
                "measurement-corpus/select-star",
                &root,
                expect_rows,
                &mut failures,
            );
            if let Some(d) = d {
                // The measurement corpus's digest is NOT pinned as a constant (it
                // depends on the row count the operator generated); it is printed
                // so a perf run can record it beside its numbers and compare it
                // across levers.
                println!(
                    "measurement-corpus arrow-buffer digests = producer 0x{:016x} / wire \
                     0x{:016x} over {} rows in {} batches",
                    d.producer.digest, d.wire.digest, d.wire.rows, d.wire.batches
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
