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
//! # The validity bitmaps are EXERCISED, not merely folded (roborev finding 2)
//!
//! Folding a validity bitmap proves nothing if no cell is ever null: every bitmap
//! is then absent or all-set, and a misplaced validity bit has nothing to
//! misplace. The fixture therefore carries `NullPlan::Pinned` — a deterministic
//! absent-cell pattern (see `tests/support/ws0_fixture.rs`) placed so that a
//! shifted bit MOVES the digest rather than landing in bitmap padding:
//!
//! * `metric_a` is null on a stride of 8 WITHIN each partition, and partitions
//!   enter the stream 100 rows apart (100 ≡ 4 mod 8). Its nulls therefore occupy
//!   **bit 0 of a byte** (byte-aligned) AND **bit 4 of a byte** (a non-boundary
//!   offset) — both, in the SAME column.
//! * `region` (var-width, so its nulls must also agree with an offsets buffer),
//!   `payload` (wide var-width, stride coprime with 8) and `device_id`
//!   (fixed-size-binary, nulled at each partition's TAIL — for the final
//!   partition, the last VALID bit of the final batch's last bitmap byte, right up
//!   against the padding) widen the shape.
//!
//! None of this is asserted by narration: [`ValidityCoverage`] MEASURES where the
//! nulls landed in the emitted batches and the test fails if the byte-aligned
//! case, the non-boundary case, the multi-batch spread or the exact per-column
//! null census is missing. [`assert_fold_detects_a_shifted_validity_bit`]
//! separately proves the fold itself is position-sensitive.
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

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use arrow::array::{Array, ArrayData, Int32Array};
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
    assert_ddl_matches_the_committed_pin, generate, has_data_db, CorpusSpec, NullPlan, DDL,
    KEYSPACE, NON_KEY_COLUMNS, TABLE,
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
/// | producer | (did not exist) | `0xd0014e42e893f87f` | NEW TAP (roborev finding 1): the pre-existing digest hashed batches only AFTER Flight IPC serialization and client-side decoding, so an Arrow-builder defect the round trip normalized was invisible. This tap folds the producer's `RecordBatch`es BEFORE `encode_do_get`. | `fcd96ca` |
/// | wire | `0xd0014e42e893f87f` | `0xe6eccf8a9ffbca11` | THE FIXTURE GAINED DETERMINISTIC NULLS (roborev finding 2): all twelve cells were non-null, so no validity bitmap ever had content and a misplaced validity bit had nothing to misplace. The fixture now carries `NullPlan::Pinned` (150 absent cells over 500 rows), so the hashed data genuinely differs. | (stamped in the next commit) |
/// | producer | `0xd0014e42e893f87f` | `0xe6eccf8a9ffbca11` | Same cause: the same fixture, now null-bearing, folded at the producer tap. | (stamped in the next commit) |
///
/// **The two taps currently fold byte-IDENTICAL input**, both before and after the
/// nulls were added — for this shape the Flight IPC round trip preserves the
/// buffer layout, so the wire digest did happen to reflect the builders' output.
/// That is a COINCIDENCE of the shape, not a property of the round trip, and it is
/// exactly what the wire tap alone could never tell anyone. The producer tap
/// removes the dependence on it; the relationship is reported per run, never
/// asserted (see the `taps …` log line).
///
/// To re-derive after an INTENDED output change, run this test and read the
/// `observed` value from the failure message.
///
/// ---------------------------------------------------------------------------
/// The **WIRE** digest: the batches a Flight client decodes off `do_get`,
/// identical on BOTH arms. This value moving means the bytes `do_get` puts on the
/// wire changed.
///
/// PINNED 2026-08-04 on the Phase-0 (pre-lever) binary, over the 500-row
/// `ws0.events` CI fixture under `NullPlan::Pinned` at `BATCH_SIZE` = 128: 4
/// batches (128/128/128/116), 12 columns, 5,850 non-null + 150 null cells
/// (11.70 cells/row), identical under `bypass` and `merge`.
const CI_FIXTURE_WIRE_DIGEST: u64 = 0xe6ec_cf8a_9ffb_ca11;

/// The **PRODUCER** digest: the same fold over the `RecordBatch`es the Arrow
/// builders produced, taken BEFORE `encode_do_get`. This value moving means the
/// builders' output changed — which is the signal the wire digest alone could not
/// give, because an IPC round trip can normalize a buffer representation.
///
/// It is not REQUIRED to differ from the wire digest (and currently does not — see
/// the re-pin log above); it is required to be observed independently of it.
const CI_FIXTURE_PRODUCER_DIGEST: u64 = 0xe6ec_cf8a_9ffb_ca11;

/// Row count of the CI fixture as observed through `do_get`. Pinned so a fixture
/// that silently shrank cannot make the digest assertion vacuous.
const CI_FIXTURE_ROWS_OBSERVED: u64 = CI_FIXTURE_ROWS;

/// Rows per partition in the CI fixture. Load-bearing for the null plan: it is
/// ≡ 4 (mod 8), which is what makes a stride-8 rule inside a partition land on
/// both a byte-aligned and a non-byte-aligned validity bit across partitions.
const CI_FIXTURE_ROWS_PER_PARTITION: u64 = 100;

/// Partitions in the CI fixture.
const CI_FIXTURE_PARTITIONS: u64 = CI_FIXTURE_ROWS / CI_FIXTURE_ROWS_PER_PARTITION;

/// The EXACT per-column null census the `NullPlan::Pinned` fixture must produce,
/// derived from the plan's rules over `CI_FIXTURE_ROWS_PER_PARTITION` = 100 rows
/// in each of `CI_FIXTURE_PARTITIONS` = 5 partitions:
///
/// | Column | Rule | Rows per partition | Total |
/// |---|---|---|---|
/// | `metric_a` | `r % 8 == 0` | 13 (`0,8,…,96`) | 65 |
/// | `region` | `r % 8 == 3` | 13 (`3,11,…,99`) | 65 |
/// | `payload` | `r % 40 == 17` | 3 (`17,57,97`) | 15 |
/// | `device_id` | partition tail (`r == 99`) | 1 | 5 |
///
/// Pinned as an exact integer census per column, not a total: a total could stay
/// right while the plan moved nulls from one column to another.
const CI_FIXTURE_NULLS_PER_COLUMN: [(&str, u64); 4] = [
    ("device_id", 5),
    ("metric_a", 65),
    ("payload", 15),
    ("region", 65),
];

/// Total null cells across the stream: the sum of [`CI_FIXTURE_NULLS_PER_COLUMN`].
const CI_FIXTURE_NULL_CELLS: u64 = 65 + 65 + 15 + 5;

/// NON-NULL cells the whole stream must carry, pinned as an EXACT integer rather
/// than a float cells-per-row ratio: an integer census cannot be satisfied
/// approximately, and it is the currency a validity-bitmap change moves.
///
/// `CI_FIXTURE_ROWS` x 12 columns, less the nulls the plan removed.
const CI_FIXTURE_NON_NULL_CELLS: u64 = CI_FIXTURE_ROWS * 12 - CI_FIXTURE_NULL_CELLS;

/// The column whose nulls must be proven to occupy BOTH a byte-aligned validity
/// bit and a non-boundary one. Named as a constant so the assertion and the
/// failure message can never name different columns.
const BOUNDARY_COVERAGE_COLUMN: &str = "metric_a";

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

/// WHERE the nulls actually landed, measured from the emitted batches.
///
/// This is what stops the validity-bitmap claim from being narration (roborev
/// finding 2). Every field is observed, never assumed, so a fixture that silently
/// lost its nulls — or moved them all into bitmap padding — fails instead of
/// quietly reducing the oracle to the value-buffer-only oracle it used to be.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ValidityCoverage {
    /// Null cells per column name, across the whole stream.
    nulls_per_column: BTreeMap<String, u64>,
    /// Per column, the distinct `bit_index % 8` offsets its nulls occupied inside
    /// their batch's validity bitmap. `0` means a byte-ALIGNED null; anything else
    /// is a non-boundary offset.
    bit_offsets_per_column: BTreeMap<String, BTreeSet<u32>>,
    /// Per column, the batch indices in which at least one null appeared — so
    /// "nulls only in the final batch's tail" is detectable.
    batches_with_nulls_per_column: BTreeMap<String, BTreeSet<u64>>,
}

impl ValidityCoverage {
    /// Record one batch's nulls. `batch_index` is the batch's position in the
    /// stream, counted from 0.
    fn record(&mut self, batch_index: u64, batch: &RecordBatch) {
        let schema = batch.schema();
        for (i, field) in schema.fields().iter().enumerate() {
            let col = batch.column(i);
            if col.null_count() == 0 {
                continue;
            }
            let data = col.to_data();
            // The validity BIT index is the array's own buffer offset plus the row
            // — not the row alone: a sliced array's bitmap starts mid-byte, and
            // reporting `row % 8` there would name the wrong bit.
            let base = data.offset();
            for row in 0..col.len() {
                if !col.is_null(row) {
                    continue;
                }
                let bit = base + row;
                *self
                    .nulls_per_column
                    .entry(field.name().clone())
                    .or_insert(0) += 1;
                self.bit_offsets_per_column
                    .entry(field.name().clone())
                    .or_default()
                    .insert((bit % 8) as u32);
                self.batches_with_nulls_per_column
                    .entry(field.name().clone())
                    .or_default()
                    .insert(batch_index);
            }
        }
    }

    /// Total nulls observed across every column.
    fn total_nulls(&self) -> u64 {
        self.nulls_per_column.values().sum()
    }
}

/// One tap's complete observation: the folded digest plus the measured null
/// placement. Compared as a whole between arms, so the nulls must land in the
/// SAME positions on the bypass arm and the merge arm.
#[derive(Debug, Clone, PartialEq)]
struct StreamFold {
    /// The folded Arrow-buffer digest and its census.
    digest: BufferDigest,
    /// Where the nulls landed.
    coverage: ValidityCoverage,
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
) -> Result<StreamFold, String> {
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
    let mut coverage = ValidityCoverage::default();
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
        coverage.record(batches, &batch);
        let (next, non_null) = fold_batch(h, &batch);
        h = next;
        non_null_cells += non_null;
        rows += batch.num_rows() as u64;
        batches += 1;
    }
    Ok(StreamFold {
        digest: BufferDigest {
            digest: h,
            batches,
            rows,
            columns: columns.unwrap_or(0),
            non_null_cells,
        },
        coverage,
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
fn producer_digest(svc: &CqliteFlightService, corpus_root: &Path) -> Result<StreamFold, String> {
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
    let mut coverage = ValidityCoverage::default();
    for (index, batch) in batches.iter().enumerate() {
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
        coverage.record(index as u64, batch);
        let (next, non_null) = fold_batch(h, batch);
        h = next;
        non_null_cells += non_null;
        rows += batch.num_rows() as u64;
    }
    Ok(StreamFold {
        digest: BufferDigest {
            digest: h,
            batches: batches.len() as u64,
            rows,
            columns: columns.unwrap_or(0),
            non_null_cells,
        },
        coverage,
    })
}

/// One forced arm, observed at BOTH taps.
#[derive(Debug)]
struct ArmObservation {
    /// The producer tap (pre-`encode_do_get`).
    producer: StreamFold,
    /// The wire tap (post-IPC, client-decoded).
    wire: StreamFold,
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
#[derive(Debug, Clone)]
struct CaseDigests {
    /// The arm-invariant PRODUCER observation (pre-`encode_do_get`).
    producer: StreamFold,
    /// The arm-invariant WIRE observation (post-IPC).
    wire: StreamFold,
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
        for (tap, f) in [("producer", &obs.producer), ("wire", &obs.wire)] {
            if f.digest.rows != expect_rows {
                failures.push(format!(
                    "case {label}: expected {expect_rows} rows at the {tap} tap on the {arm} \
                     arm, got {} — a short or empty result is a failure, never a vacuous pass",
                    f.digest.rows
                ));
                return None;
            }
            // A tap that observed NO nulls has reduced this oracle to the
            // value-buffer-only oracle roborev finding 2 flagged: every validity
            // bitmap would be absent or all-set, so a misplaced validity bit would
            // have nothing to misplace.
            if f.coverage.total_nulls() == 0 {
                failures.push(format!(
                    "case {label}: the {tap} tap on the {arm} arm observed ZERO null cells — \
                     the fixture's null plan did not reach the Arrow layer, so the validity \
                     bitmaps carry no content and this oracle proves nothing about them"
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
        producer.digest.rows,
        producer.digest.columns,
        producer.digest.batches,
        producer.digest.non_null_cells,
    ) != (
        wire.digest.rows,
        wire.digest.columns,
        wire.digest.batches,
        wire.digest.non_null_cells,
    ) {
        failures.push(format!(
            "case {label}: the two taps disagree on the LOGICAL CENSUS — the IPC round trip \
             changed rows/columns/batches/non-null cells, which is a defect, not a \
             representation difference\n  producer = {:?}\n  wire     = {:?}",
            producer.digest, wire.digest
        ));
        return None;
    }

    // And on WHERE the nulls landed. A round trip that preserved the null COUNT
    // but moved a validity bit is exactly the defect class this oracle exists for.
    if producer.coverage != wire.coverage {
        failures.push(format!(
            "case {label}: the two taps disagree on NULL PLACEMENT — the IPC round trip moved \
             at least one validity bit\n  producer = {:?}\n  wire     = {:?}",
            producer.coverage, wire.coverage
        ));
        return None;
    }
    eprintln!(
        "PASS {label} — producer digest 0x{:016x}, wire digest 0x{:016x}; {} rows in {} \
         batches, {} columns, {} non-null cells + {} nulls ({:.2} cells/row) — each digest \
         identical on both arms\n  null placement: {:?}\n  null bit offsets (mod 8): {:?}\n  \
         batches holding nulls: {:?}",
        producer.digest.digest,
        wire.digest.digest,
        wire.digest.rows,
        wire.digest.batches,
        wire.digest.columns,
        wire.digest.non_null_cells,
        wire.coverage.total_nulls(),
        wire.digest.cells_per_row(),
        wire.coverage.nulls_per_column,
        wire.coverage.bit_offsets_per_column,
        wire.coverage.batches_with_nulls_per_column,
    );
    Some(CaseDigests { producer, wire })
}

/// Prove [`fold_array_data`] actually MOVES when a single validity bit moves.
///
/// Without this, every null-coverage assertion in this file could pass over a fold
/// that ignored the bitmap — the fixture would carry nulls, the coverage would
/// report them, and the digest would still be blind to their placement. Two
/// shifts are checked: one WITHIN a bitmap byte, and one ACROSS a byte boundary
/// (the case a naive byte-wise fold could miss).
fn assert_fold_detects_a_shifted_validity_bit() {
    // 16 rows = exactly two bitmap bytes, so index 7 -> 8 crosses a byte boundary.
    let rows = 16usize;
    let with_null_at = |null_at: usize| -> u64 {
        let values: Vec<Option<i32>> = (0..rows)
            .map(|i| if i == null_at { None } else { Some(i as i32) })
            .collect();
        let array = Int32Array::from(values);
        fold_array_data(FNV_OFFSET, &array.to_data())
    };

    // Same byte (bit 3 -> bit 4).
    assert_ne!(
        with_null_at(3),
        with_null_at(4),
        "the fold did NOT change when a null moved from row 3 to row 4 (same bitmap \
         byte) — it is blind to validity-bit POSITION, so every null-coverage \
         assertion in this file would be vacuous"
    );
    // Across a byte boundary (bit 7 -> bit 8, i.e. byte 0 -> byte 1).
    assert_ne!(
        with_null_at(7),
        with_null_at(8),
        "the fold did NOT change when a null moved from row 7 to row 8 (across a \
         bitmap byte boundary) — it is blind to validity-bit position"
    );
    // And a null must be distinguishable from no null at all.
    assert_ne!(
        with_null_at(rows),
        with_null_at(0),
        "the fold did NOT change between an all-valid array and one with a null at \
         row 0"
    );
}

/// Assert the MEASURED null placement carries the coverage roborev finding 2 asked
/// for: the exact per-column census, a byte-ALIGNED null and a NON-BOUNDARY null
/// in the same column, and nulls spread beyond the final batch's tail.
fn assert_validity_coverage(tap: &str, coverage: &ValidityCoverage) {
    let expected: BTreeMap<String, u64> = CI_FIXTURE_NULLS_PER_COLUMN
        .iter()
        .map(|(name, n)| ((*name).to_string(), *n))
        .collect();
    assert_eq!(
        coverage.nulls_per_column, expected,
        "{tap} tap: per-column null census moved. A total that still adds up while \
         nulls moved between columns is exactly what this per-column pin exists to \
         catch."
    );
    assert_eq!(
        coverage.total_nulls(),
        CI_FIXTURE_NULL_CELLS,
        "{tap} tap: total null count moved"
    );

    let offsets = coverage
        .bit_offsets_per_column
        .get(BOUNDARY_COVERAGE_COLUMN)
        .unwrap_or_else(|| {
            panic!(
                "{tap} tap: column {BOUNDARY_COVERAGE_COLUMN} has no nulls at all, so it \
                 cannot carry the byte-boundary coverage the null plan is built around"
            )
        });
    assert!(
        offsets.contains(&0),
        "{tap} tap: no null in {BOUNDARY_COVERAGE_COLUMN} landed on bit 0 of a bitmap byte \
         (observed offsets {offsets:?}) — the byte-ALIGNED case is unexercised"
    );
    assert!(
        offsets.iter().any(|o| *o != 0),
        "{tap} tap: every null in {BOUNDARY_COVERAGE_COLUMN} landed on a byte boundary \
         (observed offsets {offsets:?}) — the NON-BOUNDARY case is unexercised, and a \
         validity bit misplaced within a byte would be invisible"
    );

    // Not tail-only: at least one null strictly before the LAST batch, and at
    // least one inside it. Nulls confined to an all-set-except-the-tail position
    // prove little, and a null adjacent to the final byte's padding bits is the
    // adversarial case.
    let batches = coverage
        .batches_with_nulls_per_column
        .get(BOUNDARY_COVERAGE_COLUMN)
        .unwrap_or_else(|| panic!("{tap} tap: {BOUNDARY_COVERAGE_COLUMN} nulls in no batch"));
    let last = coverage
        .batches_with_nulls_per_column
        .values()
        .flat_map(|s| s.iter().copied())
        .max()
        .unwrap_or(0);
    assert!(
        batches.iter().any(|b| *b < last),
        "{tap} tap: every {BOUNDARY_COVERAGE_COLUMN} null is in the final batch \
         ({batches:?}, last={last}) — nulls only in a trailing position prove little"
    );
    assert!(
        batches.contains(&last),
        "{tap} tap: no {BOUNDARY_COVERAGE_COLUMN} null in the final batch ({batches:?}, \
         last={last}) — the bits adjacent to the final bitmap byte's padding are \
         unexercised"
    );
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
    // Prove the FOLD is position-sensitive before trusting it to police null
    // placement: an insensitive fold would make every coverage assertion below
    // pass while observing nothing (roborev finding 2).
    assert_fold_detects_a_shifted_validity_bit();

    let temp = tempfile::tempdir().expect("tempdir");
    // `NullPlan::Pinned` — the fixture carries deterministic absent cells so the
    // validity bitmaps have CONTENT (roborev finding 2). The default
    // (`NullPlan::None`) is left to `issue_3096_framing_subphase.rs`, whose fixture
    // bytes stay unchanged.
    let spec = CorpusSpec::small(temp.path().to_path_buf(), CI_FIXTURE_ROWS)
        .with_null_plan(NullPlan::Pinned);
    assert_eq!(
        spec.rows_per_partition, CI_FIXTURE_ROWS_PER_PARTITION,
        "the null plan's byte-boundary coverage depends on rows-per-partition being \
         {CI_FIXTURE_ROWS_PER_PARTITION} (≡ 4 mod 8)"
    );
    let identity = rt.block_on(generate(&spec)).expect("generate CI fixture");
    assert_eq!(
        identity.rows, CI_FIXTURE_ROWS,
        "the CI fixture must hold exactly {CI_FIXTURE_ROWS} rows"
    );
    assert_eq!(
        identity.partitions, CI_FIXTURE_PARTITIONS,
        "the null census below is derived over {CI_FIXTURE_PARTITIONS} partitions"
    );
    // MEASURED on the WRITE side, so the two sides of the fixture agree in the same
    // currency: the writer dropped exactly the cells the Arrow layer must report as
    // null, and neither number is assumed.
    assert_eq!(
        identity.cells_absent, CI_FIXTURE_NULL_CELLS,
        "the null plan dropped {} non-key cells, expected {CI_FIXTURE_NULL_CELLS}",
        identity.cells_absent
    );
    assert_eq!(
        identity.cells_written,
        CI_FIXTURE_ROWS * NON_KEY_COLUMNS - CI_FIXTURE_NULL_CELLS,
        "written non-key cell count disagrees with the plan"
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
            d.wire.digest.non_null_cells,
            CI_FIXTURE_NON_NULL_CELLS,
            "non-null cell census moved: observed {}, pinned {CI_FIXTURE_NON_NULL_CELLS} \
             ({:.2} cells/row over {} rows)",
            d.wire.digest.non_null_cells,
            d.wire.digest.cells_per_row(),
            d.wire.digest.rows
        );
        // The validity bitmaps carry the CONTENT they claim to, in the POSITIONS
        // that make a misplaced bit detectable. Asserted from the measured
        // coverage, at both taps.
        for (tap, fold) in [("producer", &d.producer), ("wire", &d.wire)] {
            assert_validity_coverage(tap, &fold.coverage);
        }
        assert_eq!(
            d.wire.digest.digest,
            CI_FIXTURE_WIRE_DIGEST,
            "the PINNED WIRE Arrow-buffer digest changed: observed 0x{:016x}, pinned 0x{:016x}. \
             The bytes do_get puts on the wire moved. A lever that does this is reverted, or \
             its divergence is separately specified and this constant is re-pinned WITH a \
             recorded reason — never silently. (rows={} batches={} columns={})",
            d.wire.digest.digest,
            CI_FIXTURE_WIRE_DIGEST,
            d.wire.digest.rows,
            d.wire.digest.batches,
            d.wire.digest.columns
        );
        assert_eq!(
            d.producer.digest.digest,
            CI_FIXTURE_PRODUCER_DIGEST,
            "the PINNED PRODUCER Arrow-buffer digest changed: observed 0x{:016x}, pinned \
             0x{:016x}. The Arrow BUILDERS' output moved — this is the tap the wire digest \
             cannot see, because an IPC round trip can normalize a buffer representation. Same \
             rule: revert the lever, or re-pin WITH a recorded reason. (rows={} batches={} \
             columns={})",
            d.producer.digest.digest,
            CI_FIXTURE_PRODUCER_DIGEST,
            d.producer.digest.rows,
            d.producer.digest.batches,
            d.producer.digest.columns
        );
        // NOT asserted either way, deliberately. Equal digests would mean the IPC
        // round trip is byte-preserving for this shape (legitimate); unequal means
        // it re-laid-out a buffer (also legitimate, and the reason the producer tap
        // exists). Reported so a change in that relationship is visible in the log
        // without pinning a property neither Arrow nor Flight guarantees.
        eprintln!(
            "taps {}: producer 0x{:016x} vs wire 0x{:016x}",
            if d.producer.digest.digest == d.wire.digest.digest {
                "fold IDENTICAL bytes (the IPC round trip preserved the layout)"
            } else {
                "fold DIFFERENT bytes (the IPC round trip re-laid-out at least one buffer)"
            },
            d.producer.digest.digest,
            d.wire.digest.digest
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
                    d.producer.digest.digest,
                    d.wire.digest.digest,
                    d.wire.digest.rows,
                    d.wire.digest.batches
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
