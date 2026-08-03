//! The wire-side `FlightData` **framing** governor — the arrow-flight encoder's
//! re-slicing target and the gRPC message ceiling it must stay under (issue
//! #3096, lever 4; tightened in that issue's review).
//!
//! # Why this is its OWN module
//!
//! `batch_bytes` governs the **producer-side PAYLOAD cap**: how many Arrow buffer
//! *length* bytes may accumulate in one emitted `RecordBatch`
//! (`DEFAULT_MAX_BATCH_BYTES`, the caller-facing `--max-batch-bytes` contract).
//! This module governs the **wire-side CAPACITY target**: how large a
//! `FlightData` message the arrow-flight encoder may frame, bounded by the gRPC
//! maximum inbound message size every default tonic client applies.
//!
//! Two governors, two currencies, two failure modes — and conflating them is
//! exactly how the target came to sit ON the interop ceiling. They are separated
//! here so the distinction is structural rather than a paragraph
//! (campsite rule / epic #1116: `batch_bytes.rs` was pushed past its threshold by
//! the review's additions, and this is the responsibility boundary to split on).
//!
//! # What lives here
//!
//! * [`GRPC_DEFAULT_MAX_MESSAGE_BYTES`] — the interop ceiling, with the primary
//!   sources for who does and does not refuse there.
//! * [`FLIGHT_FRAMING_OVERHEAD_BYTES`] / [`FLIGHT_DATA_SIZE_INEXACTNESS_MARGIN_BYTES`]
//!   / [`FLIGHT_DATA_RESERVED_CEILING_BYTES`] — the reserve that makes a body-sized
//!   target a legal MESSAGE.
//! * [`FLIGHT_DATA_SIZE_TARGET_BYTES`] — the shipped, DERIVED target.
//! * [`SUPERSEDED_FLIGHT_DATA_SIZE_TARGET_BYTES`] /
//!   [`REJECTED_FLIGHT_DATA_SIZE_TARGET_BYTES`] and the `MEASURED_*` figures — the
//!   record of what was tried, measured, and withdrawn.
//! * the compile-time guards that make raising the target fail the BUILD.
//!
//! Behaviour tests for all of it drive the real `encode_do_get`:
//! `streaming_framing_tests.rs`.

use super::batch_bytes::DEFAULT_MAX_BATCH_BYTES;

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
///   `Binary` shape — see [`super::batch_bytes::BATCH_BYTES_CAPACITY_FACTOR`]);
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
/// It is the bound `wire_partition::guard_message_within_ceiling` asserts every
/// emitted message's **FULL SERIALIZED SIZE** against — body plus `data_header`
/// plus `app_metadata` plus protobuf framing — and the bound the framing tests
/// assert in that same currency, including at a capacity/payload ratio of ~1.0
/// where the encoder's capacity-denominated target has no slack of its own.
///
/// Because the guard measures the whole message, this ceiling no longer has to be
/// read as "a body this large leaves enough room for the rest": the 65,536-byte
/// gap below [`GRPC_DEFAULT_MAX_MESSAGE_BYTES`] is now pure headroom under the raw
/// limit, and [`FLIGHT_FRAMING_OVERHEAD_BYTES`]'s remaining job is sizing
/// [`FLIGHT_DATA_SIZE_TARGET_BYTES`] so the pre-encode partition leaves room for a
/// header rather than hitting the guard.
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
/// # This is a WIRE-SAFETY governor. It is NOT a throughput lever.
///
/// Stated first because the throughput claim it originally shipped with does not
/// hold. Re-measured at THIS target over 8 interleaved rounds / 3 arms / 24 runs
/// against the same corpus and pinning
/// (`docs/reports/ws0-3096-artifacts/abc-interleaved-2026-08-03.md` §10), stating
/// the target instead of inheriting arrow-flight's 2 MiB default moves `do_get`
/// throughput by a paired within-round median of **−72 rows/s (−0.03%)**, 4 of 8
/// rounds positive, against per-arm spreads of 5.5–9.8% — i.e. **zero**. The
/// **+4,817 rows/s / +2.3%** recorded when this lever landed was measured at the
/// SUPERSEDED 4 MiB target and did not reproduce. cycles/row does fall by a median
/// ~137 (~0.6%) with rows/s unmoved, which spec R1 explicitly forbids reporting as
/// a gain.
///
/// What this constant IS for: keeping every `data_body` under
/// [`FLIGHT_DATA_RESERVED_CEILING_BYTES`] even at a capacity/payload ratio of ~1.0
/// (asserted over the real `encode_do_get` in `streaming_framing_tests.rs`), and
/// stating the producer cap in the encoder's currency so framing is deliberate
/// rather than inherited.
///
/// # Two different governors, two different currencies
///
/// [`DEFAULT_MAX_BATCH_BYTES`] is a **producer-side PAYLOAD cap**: the sum of
/// Arrow buffer *lengths* in one emitted batch. `FlightDataEncoderBuilder`'s
/// `max_flight_data_size` is a **wire-side CAPACITY target**: `split_batch_for_
/// grpc_response` sums each column's `get_buffer_memory_size()` (buffer
/// *capacity*) and zero-copy-slices the batch into
/// `ceil(capacity / target)` pieces, each of which is framed as its own
/// `FlightData` message. Capacity runs up to [`super::batch_bytes::BATCH_BYTES_CAPACITY_FACTOR`]
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
