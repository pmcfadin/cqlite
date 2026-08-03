//! Byte-bounded Arrow egress batches — the dual row-cap / byte-cap batch
//! boundary (issue #2825, T4/M11).
//!
//! # The problem
//!
//! Before this module both egress build sites finished a record batch on **row
//! count** alone (`buffer.len() >= batch_size`). A batch's byte size was
//! therefore `batch_size × row_width` — an *unbounded* function of schema shape:
//! the same code path that produces a ~192 KiB batch for a two-column keyvalue
//! table produces a 512 MiB batch for a table with a 64 KiB blob column. The
//! ratified B4 budget (≤16Mi per-query working set at concurrency 1) cannot be
//! held by a bound stated in rows.
//!
//! # The mechanism
//!
//! [`BatchByteCap`] is a running accumulator: each candidate row's
//! [`estimate_arrow_row_bytes`] width is tested against the accumulator with
//! [`cut_before`](BatchByteCap::cut_before) **before** the row joins the buffer,
//! and the producer flushes when EITHER the row-cap or this byte-cap trips —
//! whichever comes first. The decision is made before `rows_to_record_batch`
//! allocates anything: building a batch to discover it is oversized is a report,
//! not a cap, and `RecordBatch::get_array_memory_size()` is only readable after
//! every value has been copied.
//!
//! # Currency: payload bytes, and the published capacity conversion
//!
//! The cap is normatively denominated in Arrow **payload** bytes (the sum of
//! buffer lengths — `cqlite_core::export::arrow_payload_bytes`). It is NOT
//! denominated in `get_array_memory_size()`, which reports buffer **capacity**:
//! the construction path (`StringArray::from` / `BinaryArray::from`) grows
//! `MutableBuffer` by power-of-two doubling from zero, so reported memory runs
//! up to ~2× payload (measured 1.72–1.80× on realistic shapes against arrow 53).
//! Capacity is a property of an allocator's growth policy, not of the data: it
//! is not computable before the batch exists and it is non-monotonic in row
//! count, so it cannot be the trigger.
//!
//! Consumers that must budget in capacity currency convert with the published
//! constants:
//!
//! ```text
//! worst_case_get_array_memory_size
//!     <= BATCH_BYTES_CAPACITY_FACTOR * max(cap, widest_row_payload)
//!        + BATCH_BYTES_PER_COLUMN_SLACK * n_array_nodes
//! ```
//!
//! # The `max(cap, widest_row_payload)` term is not a fudge
//!
//! One row cannot be split across two Arrow batches, so a schema whose single
//! widest row exceeds the whole cap has an **inherent, unbounded** overshoot:
//! that row leaves as a one-row batch of its own natural size (the alternative
//! is dropping it or stalling). The cap therefore bounds a batch at
//! `max(cap, widest_row_payload)` payload bytes — which reduces to plain `cap`
//! for every schema whose widest row fits, and the overshoot is a property of
//! the DATA, not slack in the mechanism.
//!
//! Nothing in the conversion path imposes a smaller per-cell ceiling that would
//! bound the term for us: `arrow_convert.rs`'s `checked_value_bytes` guard
//! rejects only a *cumulative* `Utf8`/`Binary` column length above
//! `i32::MAX` (2 GiB — the 32-bit Arrow offset limit) and returns an error
//! rather than clamping, so the honest per-row ceiling is that same ~2 GiB.
//!
//! The mechanism itself never overshoots: the boundary is **test-then-push**
//! (below), so a batch whose rows all fit is cut BEFORE the row that would
//! cross, never after it.
//!
//! # What this bounds, and how it composes with the per-stream ceiling
//!
//! **Bounded here: ONE batch.** At the 4 MiB default, over a schema whose rows
//! fit the cap, an emitted batch is ≤4 MiB of payload and therefore
//! ≤`2 × 4 MiB + 2 KiB × n_array_nodes = 8 MiB + ~2n KiB` of capacity — see
//! [`worst_case_batch_capacity_bytes`], and add the wider row's bytes for a
//! deployment whose rows can individually exceed the cap. (The `+ 2 KiB × nodes`
//! term is not decoration: on a tiny batch the fixed per-array-node allocations
//! ARE the whole reported size — see [`BATCH_BYTES_PER_COLUMN_SLACK`].)
//!
//! **Bounded next door: per-stream egress RESIDENCY.** Issue #2821 delivered
//! `cqlite_flight::egress_credit` — a per-stream in-flight ceiling denominated in
//! **capacity** bytes (`--max-inflight-egress-bytes`, default
//! `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` = 12 MiB), enforced by reserving credit
//! BEFORE each batch is materialized and releasing it when the batch has left the
//! stream. So `do_get` is no longer merely count-bounded: the
//! `get_array_memory_size()` reading `streaming.rs` takes is still fed to metrics,
//! but the reservation path now makes a real backpressure decision in the same
//! currency.
//!
//! **The delivered composition, in capacity currency:**
//!
//! ```text
//! peak SERVER-SIDE in-flight egress capacity
//!     <= max(ceiling, one maximum batch)
//!      = max(12 MiB, 2 * 4 MiB + 2 KiB * nodes)
//!      = 12 MiB   <=  16 MiB (B4 at concurrency 1)
//! ```
//!
//! Both sides of that `<=` are governed egress capacity. The producer's row
//! buffer and the encoder's queued `FlightData` are further server-side terms
//! that live in the remaining B4 headroom and are NOT deducted here, so this is
//! not a total per-query working-set bound (roborev job 12 F3).
//!
//! **Read "SERVER-SIDE" literally.** The governed quantity is the capacity bytes
//! the SERVER holds on the egress path — rows being materialized, batches queued
//! in the `do_get` channel, and yielded batches the consumer has not yet dropped.
//! It is NOT a bound on total resident bytes including consumer-held batches: a
//! batch a client retains after receiving it is the client's memory, which the
//! server can neither free nor reuse, so the governor stops charging for it (that
//! release is `MeteredDoGetStream::open_safety_valve`, and it is also what stops
//! a retaining consumer from wedging the stream). A consumer that accumulates
//! every batch it is handed is bounded by its OWN budget, not by this figure.
//!
//! The ceiling deliberately sits ABOVE one maximum batch: a reservation is taken
//! at the FULL published worst case before the batch exists, so a ceiling that
//! merely equals it would clamp every byte-cap-cut batch to the whole pool and run
//! the stream lock-step — see `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES`.
//!
//! It is a `max`, not the `ceiling + one maximum batch` sum this module's
//! pre-#2821 text projected: reserve-before-materialize removed the additive term
//! (which existed only because a parked producer could hold a materialized but
//! UNCHARGED batch). The `max` survives because a batch larger than the whole
//! ceiling is clamped to the pool total and is then the only thing resident.
//! (The naive `4 + 8 = 12 MiB` reading of the task framing mixes payload and
//! capacity: a 4 MiB *payload* cap is an 8 MiB *capacity* batch.)
//!
//! # Liveness
//!
//! Test-then-push: the crossing row's width is tested against the accumulator
//! FIRST, and the batch is cut only when the buffer is **non-empty** and adding
//! the row would take it past the cap. An empty buffer always accepts the row,
//! however wide — so a single row wider than the whole cap is delivered as a
//! one-row batch, never dropped and never a stall. Caps of `0` and `1`
//! therefore degrade to one row per batch rather than hanging — the same
//! *outcome* `batch_size.max(1)` gives the row-cap, reached by the ordering rule
//! instead of by clamping the operator's configured value.

use cqlite_core::export::estimate_arrow_row_bytes;
use cqlite_core::query::{ColumnInfo, QueryRow};

/// Default per-batch Arrow **payload** byte cap: 4 MiB.
///
/// Chosen so the row-cap still trips first on every narrow shape measured in
/// this tree, i.e. `batch_size × narrow_row_bytes < cap`:
///
/// | narrow shape | bytes/row | full 8192-row batch | headroom |
/// |---|---:|---:|---:|
/// | `issue_1494` fixture (`k{i:06}`/`v{i}`) | ~20 | ~192 KiB | ~22× |
/// | `many_partition_fixture` (`int`/`text`/`int`) | ~13 | ~107 KiB | ~39× |
/// | field model (`phase1-5-transport-ingest.md:195`) | ~180 | 1.47 MB | ~2.9× |
/// | the contested 300 B/row figure | 300 | 2.34 MiB | 1.7× |
///
/// So the byte-cap is a no-op on the narrow path — no throughput regression —
/// and binds only where a batch would otherwise be unbounded. Note that at the
/// pessimistic 300 B/row the *capacity* reading of a full narrow batch is
/// already 4,227,256 B, above 4 MiB: precisely why the cap must be
/// payload-denominated.
///
/// # Do NOT lower this to "align" it with the encoder's wire target
///
/// Recorded here because this constant is where a batch-bytes tuner looks first
/// (issue #3096, lever 4). Two reasons, both binding:
///
/// * It is the **caller-facing byte-bounded batching contract** (`--max-batch-bytes`,
///   spec R6): no emitted batch may exceed a caller-supplied cap. Lowering the
///   default changes observable default batching for every caller that never set
///   one.
/// * **The narrow-shape headroom table above is derived from the 4 MiB value.**
///   Halving it moves where the byte-cap starts binding on the ~300 B/row shape,
///   so the table would have to be re-derived to buy a framing effect that
///   belongs on the other side of the mismatch.
///
/// The encoder's side is [`FLIGHT_DATA_SIZE_TARGET_BYTES`] — a *wire-side
/// capacity* target, a different governor in a different currency, bounded by
/// [`GRPC_DEFAULT_MAX_MESSAGE_BYTES`] **less the reserved per-message framing
/// overhead** ([`FLIGHT_DATA_RESERVED_CEILING_BYTES`]). Fix the mismatch there.
pub const DEFAULT_MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;

/// Environment variable backing `--max-batch-bytes`.
pub const ENV_MAX_BATCH_BYTES: &str = "CQLITE_MAX_BATCH_BYTES";

/// The default gRPC **maximum inbound message size**: 4 MiB.
///
/// **This is the interop ceiling every wire-side size decision in this module is
/// measured against, and it is named rather than narrated so the next person to
/// tune batch bytes finds it by READING THE CODE instead of by breaking a
/// client** (issue #3096).
///
/// # Which consumers actually refuse here — read from primary sources
///
/// Verified against the pinned dependency sources during the #3096 review,
/// because the pre-review text in this module got the attribution WRONG:
///
/// * **tonic 0.12.3 and 0.13.1** — `src/codec/mod.rs:100`:
///   `const DEFAULT_MAX_RECV_MESSAGE_SIZE: usize = 4 * 1024 * 1024;`, the value
///   `decode.rs:186` falls back to when `max_decoding_message_size` is unset. So
///   **every** tonic gRPC client refuses a larger message with no configuration
///   on our side — including this tree's OWN load generator
///   (`tools/flight-loadgen/src/client.rs` builds a raw
///   `FlightServiceClient<Channel>` and sets no limit), i.e. the client the
///   #3096 throughput numbers were measured with.
/// * **grpc-java 1.79.0** (the version `trino-connector/build.gradle.kts` drags
///   in via `grpc-netty`) — `io/grpc/internal/GrpcUtil.java:212`:
///   `public static final int DEFAULT_MAX_MESSAGE_SIZE = 4 * 1024 * 1024;`, the
///   default of a channel built straight from `ManagedChannelBuilder`.
///
/// # Which consumers do NOT — the correction (#3096 review)
///
/// **arrow-java's Flight client RAISES grpc-java's default to
/// `Integer.MAX_VALUE`**, so a Trino/JDBC consumer does NOT refuse at 4 MiB. At
/// arrow-java v19.0.0 (pinned by `trino-connector/build.gradle.kts`):
/// `flight/flight-core/src/main/java/org/apache/arrow/flight/grpc/NettyClientBuilder.java:56`
/// declares `protected int maxInboundMessageSize = Integer.MAX_VALUE;` and
/// applies it to the channel at `:228`; `FlightServer.java:76` mirrors it with
/// `static final int MAX_GRPC_MESSAGE_SIZE = Integer.MAX_VALUE`. Every
/// `FlightClient.builder(..).build()` — which is exactly what
/// `CqliteFlightClient.connect` calls — inherits that, and the Flight-SQL JDBC
/// driver constructs the same `NettyClientBuilder` directly
/// (`ArrowFlightSqlClientHandler.java:1037`) without setting the limit.
///
/// That is why `docs/research/phase2-verify-transport.md` records "Flight
/// defaults `maxInboundMessageSize` high" — a statement about the **Java Flight
/// client**, now scoped as such in that document, and NOT a statement that no
/// gRPC ceiling binds this server. The claim this module used to make — that a
/// 4 MiB body is what "a Trino/JDBC consumer will refuse at" — is the one that
/// was wrong and is gone.
///
/// **The ceiling is still binding**, for the tonic and raw-grpc-java consumers
/// above, so a wire-side target must stay under it with room to spare.
/// arrow-flight itself defaults its own re-slicing target BELOW this (2 MiB) and
/// says why: "this value would normally be 4MB, but the size calculation is
/// somewhat inexact" (`arrow-flight-53.4.1/src/encode.rs:164-166`).
///
/// A `FlightData` body is not the whole gRPC message — the IPC `data_header`,
/// `app_metadata` and protobuf framing ride on top — so a body *at* this value
/// is already over it. That gap is reserved explicitly:
/// [`FLIGHT_FRAMING_OVERHEAD_BYTES`] and [`FLIGHT_DATA_RESERVED_CEILING_BYTES`].
pub(crate) const GRPC_DEFAULT_MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

/// Upper bound on the per-message **NON-BODY** bytes of a `FlightData` gRPC
/// message: the Arrow IPC `data_header` flatbuffer, `app_metadata`, the IPC
/// per-buffer alignment padding, and the protobuf field tags/length varints
/// (issue #3096 review).
///
/// **Why this constant exists.** `max_flight_data_size` governs the `data_body`
/// only, while [`GRPC_DEFAULT_MAX_MESSAGE_BYTES`] governs the whole serialized
/// message. A target set EQUAL to the ceiling therefore permits exactly the
/// message the ceiling forbids — the defect this constant was introduced to
/// close. The reserve is subtracted from the ceiling BEFORE the target is
/// derived, so the arithmetic, not a comment, is what keeps the message legal.
///
/// **Why 64 KiB is a bound, not a guess.** The `data_header` is a
/// `RecordBatch` flatbuffer: a fixed ~200 B plus ~16 B per Arrow buffer and per
/// field node. 64 KiB therefore covers a schema with roughly 2,000 array nodes —
/// two orders of magnitude past the widest table shape in this tree's corpora
/// (`ws0.events` has 12 columns / ~25 buffers, i.e. well under 1 KiB of header).
/// `app_metadata` is empty on this path (`streaming.rs encode_do_get` sets
/// none), IPC alignment padding is ≤7 B per buffer at arrow-rs's 8-byte default,
/// and protobuf framing is tens of bytes. A schema wide enough to exceed this
/// would also be spending its whole target on header rather than data, which the
/// [`FLIGHT_DATA_SIZE_INEXACTNESS_MARGIN_BYTES`] residual absorbs to ~4,000
/// nodes.
pub(crate) const FLIGHT_FRAMING_OVERHEAD_BYTES: usize = 64 * 1024;

/// A second, equal-sized margin below [`FLIGHT_DATA_RESERVED_CEILING_BYTES`] for
/// the encoder's OWN documented imprecision (issue #3096 review).
///
/// arrow-flight sizes a split by summing `get_buffer_memory_size()` (buffer
/// CAPACITY) and then cuts the batch into `ceil(size / target)` **row-uniform**
/// slices (`arrow-flight-53.4.1/src/encode.rs:613-637`). Two consequences it
/// documents as "the size calculation is somewhat inexact":
///
/// * capacity is an over-estimate of the realized IPC body for an
///   allocator-grown buffer, and an EXACT estimate for a `Vec`-backed one — so
///   the safety a capacity-denominated target buys shrinks to zero as the
///   capacity/payload ratio approaches 1.0 (measured 1.001x on a 512 x 8 KiB
///   `Binary` shape — see [`BATCH_BYTES_CAPACITY_FACTOR`]);
/// * the split is row-uniform, so a batch whose row WIDTHS are skewed can put
///   more than `size / n_slices` bytes into one slice. This residual is upstream
///   and is bounded by the producer's own cap, not by the target — a caller that
///   raises `--max-batch-bytes` far above the default and feeds it width-skewed
///   rows can still frame a slice above this ceiling, and no value of the target
///   prevents that. Stated, not papered over.
pub(crate) const FLIGHT_DATA_SIZE_INEXACTNESS_MARGIN_BYTES: usize = 64 * 1024;

/// The largest `data_body` this server designs to put on the wire:
/// [`GRPC_DEFAULT_MAX_MESSAGE_BYTES`] minus the reserved per-message framing
/// overhead (issue #3096 review).
///
/// A `FlightData` whose body is at or below this value serializes to a gRPC
/// message inside the 4 MiB default ceiling. It is the bound the framing tests
/// assert EVERY emitted body against, including at a capacity/payload ratio of
/// ~1.0 where the encoder's capacity-denominated target has no slack of its own.
pub(crate) const FLIGHT_DATA_RESERVED_CEILING_BYTES: usize =
    GRPC_DEFAULT_MAX_MESSAGE_BYTES - FLIGHT_FRAMING_OVERHEAD_BYTES;

/// The flight-data re-slicing target that was **measured and REJECTED**: 8 MiB
/// (issue #3096, lever 4).
///
/// Recorded as a named constant, not as prose in a report, because "just raise
/// the target until batches stop being split" is the obvious move and it is
/// **wire-unsafe**. See [`MEASURED_DATA_BODY_BYTES_AT_REJECTED_TARGET`] for the
/// number that rejects it and [`MEASURED_FLIGHT_DATA_MESSAGES`] for what it buys.
/// An unbounded target measures the same as this one on the #3096 corpus: one
/// message per producer batch.
pub(crate) const REJECTED_FLIGHT_DATA_SIZE_TARGET_BYTES: usize = 8 * 1024 * 1024;

/// Rows the framing measurements in this module were taken over (issue #3096,
/// `ws0.events` corpus, `--batch-size 8192`).
pub(crate) const MEASURED_FRAMING_ROWS: usize = 400_000;

/// Realized Arrow **payload** bytes per row of the #3096 `ws0.events` corpus, as
/// measured — the width that turns [`MEASURED_FLIGHT_DATA_MESSAGES`] into a
/// per-message body size. (At this width the 4 MiB payload cap binds at 5,645
/// rows/batch, before the 8192-row cap.)
pub(crate) const MEASURED_ARROW_PAYLOAD_BYTES_PER_ROW: usize = 701;

/// `FlightData` **data** messages measured for [`MEASURED_FRAMING_ROWS`] rows at
/// three encoder targets: `(target_bytes, messages)`.
///
/// | target | messages | mean `data_body` | verdict |
/// |---|--:|--:|---|
/// | 2 MiB — arrow-flight 53.4.1's own default | 331 | ~847 KB | inherited by accident; four ~1,411-row slices per batch **plus a degenerate 1-row tail** |
/// | 4 MiB — [`SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES`] | 189 | **~1.49 MB** | **SUPERSEDED** — equalled [`GRPC_DEFAULT_MAX_MESSAGE_BYTES`], so a ratio-~1.0 batch frames bodies AT the ceiling |
/// | 8 MiB — [`REJECTED_FLIGHT_DATA_SIZE_TARGET_BYTES`] | 72 | **3.90 MB** | **REJECTED** — sits on [`GRPC_DEFAULT_MAX_MESSAGE_BYTES`] |
///
/// The 2 MiB body figure is derived from the row width, not separately measured;
/// the 4 MiB and 8 MiB ones are the measured means recorded below.
///
/// **This table is measurement HISTORY, not the shipped design point.** The
/// shipped target is [`FLIGHT_DATA_SIZE_TARGET_BYTES`], which is deliberately
/// NOT any of these three: it sits just under
/// [`FLIGHT_DATA_RESERVED_CEILING_BYTES`]. Its wire safety is asserted directly
/// (every emitted body under that ceiling, at a capacity/payload ratio of ~1.0)
/// by `streaming_framing_tests.rs` rather than resting on a mean, and its
/// corpus-shaped framing is measured in-process by the same tests. See
/// `docs/reports/ws0-3096-artifacts/abc-interleaved-2026-08-03.md` for the
/// throughput re-measurement at the shipped target.
pub(crate) const MEASURED_FLIGHT_DATA_MESSAGES: [(usize, usize); 3] = [
    (2 * 1024 * 1024, 331),
    (SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES, 189),
    (REJECTED_FLIGHT_DATA_SIZE_TARGET_BYTES, 72),
];

/// The flight-data re-slicing target lever 4 first shipped and the #3096 review
/// **SUPERSEDED**: 4 MiB — exactly [`GRPC_DEFAULT_MAX_MESSAGE_BYTES`].
///
/// **Why it had to go:** it permitted precisely the message size this module
/// declares unsafe. `split_batch_for_grpc_response` sizes each slice UP TO the
/// target, so with the target set at the ceiling the encoder's design point is a
/// `data_body` AT the ceiling — while the gRPC message is that body PLUS the IPC
/// `data_header` and the protobuf framing. Pre-lever-4 the same batch split at
/// 2 MiB and could not get near it, so this was an interop REGRESSION introduced
/// by the lever whose own rationale rejected 8 MiB for that exact break.
///
/// **Measured, at exactly the strength the measurement supports.** Perturbing the
/// target back to this value and running
/// `streaming_framing_tests::a_ratio_one_binary_batch_frames_every_body_under_the_reserved_ceiling`
/// frames a 511 x 8 KiB `Binary` batch (capacity/payload ratio 1.0, payload
/// 4,188,160 B — just under `DEFAULT_MAX_BATCH_BYTES`) as **ONE message with a
/// 4,188,224-byte body**: `6,080 bytes` — 0.15% — below tonic's hard 4,194,304
/// refusal threshold, *before* any header is added. So the delivered position was
/// not "always breaks"; it was "one `data_header` away from breaking", and it
/// crosses for real on any of:
///
/// * a schema whose `data_header` exceeds that margin (~380 Arrow buffers at
///   ~16 B each — a wide analytics table);
/// * a batch with row-WIDTH skew, where the row-uniform split puts more than
///   `capacity / n_slices` bytes in one slice (see
///   [`FLIGHT_DATA_SIZE_INEXACTNESS_MARGIN_BYTES`]);
/// * any future `app_metadata` on this path.
///
/// A 0.15% margin against a hard interop ceiling, reached by accident and guarded
/// by a `<=`, is not a design point. Reserving the framing overhead explicitly is.
///
/// Retained as a named constant for two reasons: the 189-message/1.49 MB
/// measurement recorded above was taken AT this target and is still a real
/// measurement, and "just round the target up to the ceiling" is the mistake that
/// was actually made once and must fail the build if it is made again.
pub(crate) const SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES: usize = 4 * 1024 * 1024;

/// Mean `data_body` bytes per `FlightData` message measured at
/// [`REJECTED_FLIGHT_DATA_SIZE_TARGET_BYTES`]: **3.90 MB**.
///
/// **This single number is the whole rejection.** 3.90 MB of body, plus IPC
/// metadata and protobuf framing, sits on the 4 MiB
/// [`GRPC_DEFAULT_MAX_MESSAGE_BYTES`] ceiling — so an 8 MiB target trades a
/// framing micro-optimization for a **tonic/grpc-java interop break** (the
/// consumers that really do refuse there; see
/// [`GRPC_DEFAULT_MAX_MESSAGE_BYTES`] for who does and does not, from primary
/// sources). The framing win it buys (72 messages instead of 189) is real and is
/// not worth that.
pub(crate) const MEASURED_DATA_BODY_BYTES_AT_REJECTED_TARGET: usize = 3_900_000;

/// Mean `data_body` bytes per `FlightData` message measured at
/// [`SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES`]: **~1.49 MB**.
///
/// Note what this figure does and does NOT establish. It is the mean over the
/// `ws0.events` corpus, whose capacity/payload ratio is ~1.85 — so the target's
/// capacity-denominated headroom hid the hazard. It is **not** a bound: on a
/// ratio-~1.0 shape the same target frames bodies at the target itself. That is
/// why the shipped target is derived from
/// [`FLIGHT_DATA_RESERVED_CEILING_BYTES`] and asserted as a bound in the framing
/// tests, instead of being justified by this mean.
pub(crate) const MEASURED_DATA_BODY_BYTES_AT_SUPERSEDED_TARGET: usize = 1_490_000;

/// The arrow-flight encoder's own batch **re-slicing** target, in Arrow buffer
/// **CAPACITY** bytes (issue #3096, lever 4).
///
/// # Two different governors, two different currencies
///
/// [`DEFAULT_MAX_BATCH_BYTES`] is a **producer-side PAYLOAD cap**: the sum of
/// Arrow buffer *lengths* in one emitted batch. `FlightDataEncoderBuilder`'s
/// `max_flight_data_size` is a **wire-side CAPACITY target**: `split_batch_for_
/// grpc_response` sums each column's `get_buffer_memory_size()` (buffer
/// *capacity*) and zero-copy-slices the batch into
/// `ceil(capacity / target)` pieces, each of which is framed as its own
/// `FlightData` message. Capacity runs up to [`BATCH_BYTES_CAPACITY_FACTOR`]
/// (2×) the payload, so the two numbers are NOT comparable directly — which is
/// exactly how they drifted.
///
/// # What was wrong, measured
///
/// arrow-flight 53.4.1's default target is `GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES`
/// = 2 MiB — HALF this tree's 4 MiB payload cap, so **every** batch the producer
/// had already cut to its own cap was re-sliced by the encoder. Measured over
/// the issue #3096 `ws0.events` corpus (701 B/row realized Arrow payload,
/// `--batch-size 8192`, so the byte cap binds at 5,645 rows/batch), 400,000 rows
/// produced **331** `FlightData` messages at the 2 MiB default: four ~1,411-row
/// slices per batch plus a degenerate **1-row tail** (integer truncation in
/// `rows_per_batch = num_rows / n_batches`), i.e. one in five messages carried a
/// single row.
///
/// # Why the ceiling MINUS a reserve, and never the ceiling itself
///
/// The target is derived, not chosen:
///
/// ```text
/// FLIGHT_DATA_SIZE_TARGET_BYTES
///   = GRPC_DEFAULT_MAX_MESSAGE_BYTES        4,194,304   the interop ceiling
///   - FLIGHT_FRAMING_OVERHEAD_BYTES            65,536   IPC data_header + protobuf framing
///   - FLIGHT_DATA_SIZE_INEXACTNESS_MARGIN_BYTES 65,536   the encoder's own imprecision
///   = 4,063,232                                          (3.875 MiB)
/// ```
///
/// **The first version of this lever set the target EQUAL to the ceiling
/// (4 MiB), and that was an interop regression** — see
/// [`SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES`] for the concrete break (a
/// ratio-~1.0 `Binary` shape under a raised `--max-batch-bytes` framing ~4 MiB
/// bodies) and [`FLIGHT_FRAMING_OVERHEAD_BYTES`] for why a body AT the ceiling is
/// already over it. Both facts were stated in this module at the time; only the
/// arithmetic was missing, which is why the reserve is now subtracted in code and
/// both compile-time guards below are STRICT.
///
/// **Why not 3 MiB, or 2 MiB?** Wire safety is satisfied by any target at or
/// below [`FLIGHT_DATA_RESERVED_CEILING_BYTES`]; below that, every byte of extra
/// margin is paid for in framing. arrow-flight's split count is
/// `ceil(batch_capacity / target)`, so the corpus's ~7.4 MiB-capacity batches
/// cross from 2 slices to 3 as soon as the target drops below ~3.7 MiB — the
/// whole of lever 4's measured framing effect. 3.875 MiB is the largest target
/// that keeps the reserve strictly satisfied, so it buys the framing win at no
/// cost in wire safety.
///
/// # Why NOT "large enough to stop splitting"
///
/// The obvious move — raise the target until a full-cap batch is never split —
/// is **wire-unsafe** and is deliberately rejected. Measured on the same corpus,
/// a target of [`REJECTED_FLIGHT_DATA_SIZE_TARGET_BYTES`] (8 MiB — or unbounded)
/// does collapse the run to one message per producer batch (72 messages for
/// 400,000 rows), but those messages carry
/// [`MEASURED_DATA_BODY_BYTES_AT_REJECTED_TARGET`] — **3.90 MB** — of `data_body`
/// each. That sits on [`GRPC_DEFAULT_MAX_MESSAGE_BYTES`], the 4 MiB ceiling both
/// tonic and raw grpc-java apply by default, once IPC and protobuf overhead is
/// added, so an 8 MiB target would trade a framing micro-optimization for an
/// interop break with every tonic consumer — including this tree's own
/// `flight-loadgen`.
///
/// # What this deliberately does NOT change
///
/// [`DEFAULT_MAX_BATCH_BYTES`] is untouched. It is the caller-facing,
/// byte-bounded batching contract (`--max-batch-bytes`): no emitted batch may
/// exceed a caller-supplied cap, and the narrow-shape headroom table above is
/// derived from the 4 MiB value. Lowering the default to "align" would change
/// observable default batching for every caller that never set one — including
/// where the byte cap starts binding on the ~300 B/row shape — to buy the same
/// framing effect. Fixing the encoder's side of the mismatch leaves the public
/// contract exactly as published. A caller who raises `--max-batch-bytes` above
/// 4 MiB still gets its larger batches; the encoder simply re-slices them for
/// the wire, which is what a wire-side target is for — and, since this target is
/// now strictly under the reserved ceiling, those re-slices are legal gRPC
/// messages instead of ~4 MiB bodies.
pub(crate) const FLIGHT_DATA_SIZE_TARGET_BYTES: usize =
    FLIGHT_DATA_RESERVED_CEILING_BYTES - FLIGHT_DATA_SIZE_INEXACTNESS_MARGIN_BYTES;

// ---------------------------------------------------------------------------
// The wire-size rejections, enforced at COMPILE TIME (issue #3096, tightened in
// its review).
//
// The reasoning above is only a landmine-remover if raising the target trips
// something. These guards make "just raise it until batches stop splitting"
// fail the BUILD rather than a tonic/grpc-java client, and keep the recorded
// measurements internally consistent so one of them cannot be edited alone.
//
// The FIRST version of these guards used `<=` against the raw ceiling and so
// ADMITTED exactly the 4 MiB target this module declared unsafe. Every ceiling
// comparison here is now STRICT and takes the reserved framing overhead into
// account, which is what makes the guard say something the prose did not already
// say.
//
// Proven non-vacuous by perturbation: setting FLIGHT_DATA_SIZE_TARGET_BYTES to
// 4 MiB (the superseded value) or 8 MiB fails `cargo build -p cqlite-flight`
// with `error[E0080]` quoting the interop-break message below.
// ---------------------------------------------------------------------------

/// A body plus its framing must fit the ceiling with room to spare — STRICTLY.
/// This is the guard that the superseded 4 MiB target would have failed.
const _: () = assert!(
    FLIGHT_DATA_SIZE_TARGET_BYTES + FLIGHT_FRAMING_OVERHEAD_BYTES < GRPC_DEFAULT_MAX_MESSAGE_BYTES,
    "FLIGHT_DATA_SIZE_TARGET_BYTES + FLIGHT_FRAMING_OVERHEAD_BYTES is not STRICTLY \
     below GRPC_DEFAULT_MAX_MESSAGE_BYTES (4 MiB — tonic's DEFAULT_MAX_RECV_MESSAGE_SIZE \
     and raw grpc-java's DEFAULT_MAX_MESSAGE_SIZE). The encoder sizes each slice UP TO \
     the target and the IPC data_header + protobuf framing ride on top, so a target at \
     (or over) the ceiling frames messages the default tonic client refuses — see \
     SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES, REJECTED_FLIGHT_DATA_SIZE_TARGET_BYTES \
     and MEASURED_DATA_BODY_BYTES_AT_REJECTED_TARGET (3.90 MB bodies measured at 8 MiB)."
);

/// The reserve must be REAL: a zero reserve turns the strict guard above back
/// into the `<=`-against-the-ceiling form that admitted the superseded target.
const _: () = assert!(
    FLIGHT_FRAMING_OVERHEAD_BYTES > 0
        && FLIGHT_DATA_SIZE_INEXACTNESS_MARGIN_BYTES > 0
        && FLIGHT_DATA_RESERVED_CEILING_BYTES < GRPC_DEFAULT_MAX_MESSAGE_BYTES
        && FLIGHT_DATA_SIZE_TARGET_BYTES < FLIGHT_DATA_RESERVED_CEILING_BYTES,
    "the framing reserve has been zeroed or inverted: the target must sit strictly \
     under FLIGHT_DATA_RESERVED_CEILING_BYTES, which must sit strictly under \
     GRPC_DEFAULT_MAX_MESSAGE_BYTES"
);

/// The target must stay STRICTLY under the producer's own payload cap, so a
/// full-cap batch can never leave as ONE message whose body is the whole cap.
/// (Also asserted from the test side —
/// `streaming_framing_tests::the_target_sits_between_the_arrow_default_and_the_producer_payload_cap`.)
const _: () = assert!(
    FLIGHT_DATA_SIZE_TARGET_BYTES < DEFAULT_MAX_BATCH_BYTES,
    "FLIGHT_DATA_SIZE_TARGET_BYTES is not STRICTLY below DEFAULT_MAX_BATCH_BYTES: at \
     equality a batch cut to the producer's payload cap can be framed as a SINGLE \
     FlightData message carrying that whole cap as its body, which is the interop \
     break this module rejects"
);

/// The superseded target is the value the strict guard above now forbids — if it
/// ever becomes admissible again, the record of why it was withdrawn is stale.
const _: () = assert!(
    SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES + FLIGHT_FRAMING_OVERHEAD_BYTES
        > GRPC_DEFAULT_MAX_MESSAGE_BYTES,
    "SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES no longer describes a withdrawn target: \
     it now fits the ceiling once framing overhead is added, so either the ceiling or \
     the reserve moved and the withdrawal must be re-derived"
);

/// The rejected target is over the ceiling, and its measured bodies sat ON it.
const _: () = assert!(
    REJECTED_FLIGHT_DATA_SIZE_TARGET_BYTES > GRPC_DEFAULT_MAX_MESSAGE_BYTES
        && MEASURED_DATA_BODY_BYTES_AT_REJECTED_TARGET * 10 > GRPC_DEFAULT_MAX_MESSAGE_BYTES * 9,
    "the recorded 8 MiB rejection no longer states a rejection: either the target \
     is no longer above the gRPC ceiling, or its measured data_body no longer sits \
     within 10% of it"
);

/// The superseded target's measured bodies sat well inside the ceiling — which is
/// exactly why the mean could not carry the wire-safety claim (the hazard shape is
/// a ratio-~1.0 batch, not the corpus mean).
const _: () = assert!(
    MEASURED_DATA_BODY_BYTES_AT_SUPERSEDED_TARGET * 2 < GRPC_DEFAULT_MAX_MESSAGE_BYTES,
    "the superseded target's measured data_body is no longer recorded as comfortably \
     inside the gRPC ceiling — re-measure before quoting it"
);

/// The recorded framing table, the recorded corpus width and the recorded body
/// sizes must agree: `rows x bytes_per_row / messages` is the mean body size, so
/// editing one figure without re-measuring the others fails the build.
const _: () = {
    let total_payload = MEASURED_FRAMING_ROWS * MEASURED_ARROW_PAYLOAD_BYTES_PER_ROW;
    let superseded = total_payload / MEASURED_FLIGHT_DATA_MESSAGES[1].1;
    let rejected = total_payload / MEASURED_FLIGHT_DATA_MESSAGES[2].1;
    assert!(
        superseded * 100 < MEASURED_DATA_BODY_BYTES_AT_SUPERSEDED_TARGET * 105
            && superseded * 105 > MEASURED_DATA_BODY_BYTES_AT_SUPERSEDED_TARGET * 100,
        "MEASURED_FLIGHT_DATA_MESSAGES's superseded-target message count is not within \
         5% of MEASURED_DATA_BODY_BYTES_AT_SUPERSEDED_TARGET at the recorded row width"
    );
    assert!(
        rejected * 100 < MEASURED_DATA_BODY_BYTES_AT_REJECTED_TARGET * 105
            && rejected * 105 > MEASURED_DATA_BODY_BYTES_AT_REJECTED_TARGET * 100,
        "MEASURED_FLIGHT_DATA_MESSAGES's rejected-target message count is not within \
         5% of MEASURED_DATA_BODY_BYTES_AT_REJECTED_TARGET at the recorded row width"
    );
    assert!(
        MEASURED_FLIGHT_DATA_MESSAGES[0].0 < MEASURED_FLIGHT_DATA_MESSAGES[1].0
            && MEASURED_FLIGHT_DATA_MESSAGES[1].0 == SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES
            && MEASURED_FLIGHT_DATA_MESSAGES[2].0 == REJECTED_FLIGHT_DATA_SIZE_TARGET_BYTES
            && MEASURED_FLIGHT_DATA_MESSAGES[0].1 > MEASURED_FLIGHT_DATA_MESSAGES[1].1
            && MEASURED_FLIGHT_DATA_MESSAGES[1].1 > MEASURED_FLIGHT_DATA_MESSAGES[2].1,
        "MEASURED_FLIGHT_DATA_MESSAGES must stay ordered by target with strictly \
         falling message counts, and its middle/last targets must be the superseded and \
         rejected constants"
    );
};

/// Worst-case ratio of a batch's `get_array_memory_size()` (buffer **capacity**)
/// to its payload bytes (buffer **lengths**).
///
/// `MutableBuffer::reserve` grows to `max(round_upto_multiple_of_64(required),
/// capacity * 2)` — power-of-two doubling from zero — so a payload landing just
/// past a power of two reports up to ~2× that payload. Measured against this
/// tree's arrow 53: 1.001× (512 × 8 KiB binary), 1.280× (100 × 64 KiB binary),
/// 1.445× (8192 × 180 B binary), 1.720× (8192 × 300 B binary), 1.779× (8192 ×
/// 290 B string), 1.801× (8192 × 20 B string). `2` is the bound, not the typical.
///
/// Published so a consumer — notably issue #2821's per-stream in-flight ceiling
/// — can convert this change's payload guarantee into the capacity currency it
/// meters, with no undocumented fudge factor.
pub const BATCH_BYTES_CAPACITY_FACTOR: usize = 2;

/// Fixed capacity slack allowed **per Arrow array node**, on top of
/// [`BATCH_BYTES_CAPACITY_FACTOR`] × cap.
///
/// Every Arrow array carries fixed allocations that do not scale with the
/// payload (the `ArrayData`/`Buffer` structs themselves, a 64-byte-aligned
/// minimum allocation per buffer, the validity buffer, an empty-array's offsets
/// buffer — and, dominating all of them, the **1 KiB default values buffer**
/// `GenericStringBuilder`/`GenericBinaryBuilder` allocate, which every `Utf8` or
/// `Binary` column carries however few bytes it holds). On a batch that is mostly
/// one wide column these round to nothing; on a batch of tiny rows they are the
/// whole reported size, so a capacity bound stated purely as a multiple of the
/// payload would be wrong for that shape.
///
/// **2048, corrected from 1024 (issue #2932, found in the #2821 review).** With
/// 1024 this function was NOT an upper bound for text/blob schemas. 1024 was
/// under the real
/// fixed cost of the commonest node there is: a `Utf8`/`Binary` array built by
/// `export::arrow_convert` reports **1208 B** at any length from 0 up (arrow 53 —
/// 1024 values buffer + 64 offsets + the struct overhead). A two-`text`-column
/// batch of three short rows therefore reports 2416 B against a `2 × payload +
/// 1024 × 2` = 2186 B bound — which #2821's fail-closed reservation turned from a
/// silently-loose doc claim into a terminal `do_get` error.
///
/// **Enforcement, precisely.** Two tests, and the claim is exactly what they
/// assert — no more:
///
/// * `batch_bytes_tests::the_capacity_bound_holds_for_tiny_batches` pins the six
///   hand-written shapes including the exact two-`text`-column regression.
/// * `batch_bytes_tests::the_capacity_bound_holds_over_the_shared_shape_corpus`
///   asserts `get_array_memory_size() <= worst_case_batch_capacity_bytes(Σ
///   estimate, nodes, 0)` over EVERY shape in the SHARED corpus
///   (`cqlite_core::export::arrow_shape_corpus` — the same shapes the estimator's
///   conservatism contract is validated against), each at full row count AND
///   truncated to one row. That reaches `FixedSizeBinary(16)` (uuid/timeuuid),
///   boolean/decimal/varint/timestamp/date/time/counter, tuple and UDT
///   (`Struct`), `set`, 8-deep nesting, `frozen`, and the `cql_type = None` flat
///   dispatch arms that route through different builders.
///
/// Measured worst case across that corpus is **1188 B per node** (a one-row
/// `Utf8` batch), so 2048 carries 860 B of margin; the guard FAILS at 1024. The
/// cost is 1 KiB more reservation per array node — 3 KiB on a three-node schema
/// against a multi-MiB batch.
///
/// Denominated in array NODES, not output columns: a flat scalar column is one
/// node, but a `list<text>` column is two (the `ListArray` and its `Utf8`
/// child) and a `map<text,text>` column is four (map, entries struct, key
/// `Utf8`, value `Utf8`). Callers with a flat schema pass the column count;
/// callers with nested columns must count the child arrays too, or the slack
/// term under-states their fixed allocations. (At the 4 MiB default the
/// `2 × cap` term dominates by three orders of magnitude either way; the
/// distinction bites only for a tiny cap over a deeply nested schema.)
pub const BATCH_BYTES_PER_COLUMN_SLACK: usize = 2048;

/// Whether the caller should finish the current batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShouldFlush {
    /// Keep accumulating: neither cap has been reached.
    No,
    /// The byte-cap has been reached — finish this batch now.
    Yes,
}

impl ShouldFlush {
    /// `true` when the batch must be finished.
    #[inline]
    pub fn is_yes(self) -> bool {
        matches!(self, ShouldFlush::Yes)
    }
}

/// Running per-batch payload-byte accumulator implementing the byte half of the
/// dual row-cap / byte-cap boundary.
///
/// Shared by BOTH egress build sites (`producer.rs`'s partition-at-a-time merge
/// loop and `producer_stream.rs`'s row-granular loop) so the boundary rule is
/// defined once. A cap wired into only one path would leave the other unbounded.
#[derive(Debug, Clone)]
pub struct BatchByteCap {
    /// Configured payload-byte ceiling for one batch.
    cap: usize,
    /// Estimated payload bytes accumulated since the last [`Self::reset`].
    accumulated: usize,
    /// Rows accumulated since the last [`Self::reset`] — the liveness guard that
    /// makes the one-row floor unconditional.
    rows: usize,
}

impl BatchByteCap {
    /// Build an accumulator enforcing `cap` payload bytes per batch.
    ///
    /// `cap` is used exactly as given, including `0` and `1`: the test-then-push
    /// rule makes those degrade to one row per batch rather than hang, so no
    /// clamp is needed (and clamping would silently misreport the operator's
    /// configuration). `usize::MAX` effectively disables the byte-cap, leaving
    /// the row-cap as the sole boundary.
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            accumulated: 0,
            rows: 0,
        }
    }

    /// The configured payload-byte ceiling.
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Estimated payload bytes accumulated in the current batch.
    pub fn accumulated(&self) -> usize {
        self.accumulated
    }

    /// Rows accumulated into the current batch since the last [`Self::reset`].
    /// A [`ShouldFlush::Yes`] can only ever be reported with this at 1 or more —
    /// the one-row floor.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// **Test-then-push**: must the currently buffered rows be finished BEFORE a
    /// row of `width` payload bytes is appended?
    ///
    /// Answering before the row joins the buffer is what bounds a batch at `cap`
    /// rather than at `cap - 1 + width_of_crossing_row`: the batch is cut on the
    /// row that WOULD cross, so the crossing row starts the next batch instead
    /// of overshooting this one (issue #2825 review B1).
    ///
    /// The one-row floor is the `self.rows > 0` conjunct: an **empty buffer
    /// always accepts the row**, however wide, so a row wider than the entire
    /// cap leaves as a one-row batch and can never trigger a flush of nothing
    /// (which would loop without progress). Caps of `0` and `1` therefore
    /// degrade to one row per batch rather than hanging.
    ///
    /// Saturating: a fail-closed `usize::MAX` width (a pathological value, see
    /// `estimate_arrow_row_bytes`) compares at the ceiling instead of wrapping.
    pub fn cut_before(&self, width: usize) -> ShouldFlush {
        if self.rows > 0 && self.accumulated.saturating_add(width) > self.cap {
            ShouldFlush::Yes
        } else {
            ShouldFlush::No
        }
    }

    /// Account for one row of `width` payload bytes that has just been appended
    /// to the buffer. Call AFTER [`Self::cut_before`] has been honoured.
    ///
    /// `width` is saturating-added, so a `usize::MAX` estimate pins the
    /// accumulator at the ceiling rather than wrapping.
    pub fn accumulate(&mut self, width: usize) {
        self.accumulated = self.accumulated.saturating_add(width);
        self.rows = self.rows.saturating_add(1);
    }

    /// Estimate `row`'s Arrow payload width for the projected `columns` — the
    /// quantity both [`Self::cut_before`] and [`Self::accumulate`] take, computed
    /// once per row by the caller so it is never estimated twice.
    pub fn row_width(columns: &[ColumnInfo], row: &QueryRow) -> usize {
        estimate_arrow_row_bytes(columns, row)
    }

    /// Clear the accumulator for the next batch. Called wherever the buffer is
    /// flushed, so the running estimate always describes exactly the rows
    /// currently buffered — the whole buffer is never re-measured per push.
    pub fn reset(&mut self) {
        self.accumulated = 0;
        self.rows = 0;
    }
}

/// Split an already-materialized row slice into contiguous groups, each ending
/// where the dual row-cap / byte-cap boundary falls.
///
/// Used by the aggregate route (issue #841), which folds rows into accumulator
/// state and then materializes one PARTIAL row per `GROUP BY` group in one go —
/// it never passes through the incremental buffer, so it needs the boundary
/// applied after the fact. The row path uses [`BatchByteCap`] directly.
///
/// Never yields an empty group: the same test-then-push rule applies, so a
/// single over-cap row becomes a one-row group. An empty input yields no groups.
pub fn split_rows_into_batches<'a>(
    columns: &[ColumnInfo],
    rows: &'a [QueryRow],
    max_rows: usize,
    cap: usize,
) -> Vec<&'a [QueryRow]> {
    let max_rows = max_rows.max(1);
    let mut groups = Vec::new();
    let mut byte_cap = BatchByteCap::new(cap);
    let mut start = 0usize;
    for (i, row) in rows.iter().enumerate() {
        let width = BatchByteCap::row_width(columns, row);
        // Cut BEFORE the crossing row, so the group that ends here holds only
        // rows that fit — the same rule the two incremental producers apply.
        // `cut_before` is `No` while the group is empty, so `start < i` holds
        // here and the pushed group is never empty.
        if byte_cap.cut_before(width).is_yes() {
            groups.push(&rows[start..i]);
            start = i;
            byte_cap.reset();
        }
        byte_cap.accumulate(width);
        if i + 1 - start >= max_rows {
            groups.push(&rows[start..=i]);
            start = i + 1;
            byte_cap.reset();
        }
    }
    if start < rows.len() {
        groups.push(&rows[start..]);
    }
    groups
}

/// Worst-case resident size, in `get_array_memory_size()` (capacity) bytes, of
/// ONE emitted batch produced under `cap` over a schema whose widest single row
/// contributes `widest_row_payload` payload bytes, with `n_array_nodes` Arrow
/// array nodes (see [`BATCH_BYTES_PER_COLUMN_SLACK`]).
///
/// Derived from the published constants alone:
///
/// ```text
/// BATCH_BYTES_CAPACITY_FACTOR * max(cap, widest_row_payload)
///     + BATCH_BYTES_PER_COLUMN_SLACK * n_array_nodes
/// ```
///
/// This is the quantity issue #2821's per-stream ceiling composes with to state
/// its delivered `max(ceiling, one maximum batch)` bound against B4's ≤16Mi, and
/// the exact conversion `cqlite_flight::egress_credit` uses to turn a payload
/// estimate into the pre-materialization capacity reservation — see the module
/// documentation.
///
/// The `max(..)` term is honest, not slack. The boundary is test-then-push, so a
/// batch is cut BEFORE the row that would cross the cap — but a row cannot be
/// split across Arrow batches, so a row wider than the whole cap is emitted
/// alone at its own natural width. Callers whose rows are known to fit the cap
/// pass `0` (or any value ≤ `cap`) and get the familiar
/// `FACTOR * cap + slack` bound; callers that cannot rule out a wider row must
/// state that row's payload here. Nothing downstream clamps it —
/// `arrow_convert.rs`'s `checked_value_bytes` guard only *rejects* a cumulative
/// column length above `i32::MAX`, so ~2 GiB is the only structural ceiling.
///
/// Saturating: an operator-configured `usize::MAX` cap reports `usize::MAX`
/// rather than wrapping.
pub fn worst_case_batch_capacity_bytes(
    cap: usize,
    n_array_nodes: usize,
    widest_row_payload: usize,
) -> usize {
    cap.max(widest_row_payload)
        .saturating_mul(BATCH_BYTES_CAPACITY_FACTOR)
        .saturating_add(BATCH_BYTES_PER_COLUMN_SLACK.saturating_mul(n_array_nodes))
}

#[cfg(test)]
#[path = "batch_bytes_tests.rs"]
mod batch_bytes_tests;
