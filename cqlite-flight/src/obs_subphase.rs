//! In-`stream` data-plane SUB-PHASE value table + per-request emitter (issue
//! #2819).
//!
//! A sibling of [`crate::obs`] (campsite rule, epic #1116), re-exported through it
//! (`pub use crate::obs_subphase::…` in `obs.rs`) so call sites keep the stable
//! `crate::obs::{...}` path. This module owns:
//!
//! * the six bounded `cqlite.rpc.phase` VALUES that decompose the `stream`
//!   phase (`stream_cold_fault`, `stream_decompress`, `stream_merge`,
//!   `stream_encode`, `stream_encode_framing`, `stream_grpc_write`), and
//! * [`StreamSubPhaseEmitter`], the RAII that flushes the per-request accumulator
//!   (five `AtomicU64` nanos counters filled on the concurrent pipeline threads
//!   via `cqlite_core::observability::stream_subphase` — cold-fault/decompress on
//!   the per-SSTable PRODUCER thread(s), and merge/encode/gRPC-write all on the
//!   merge-consumer thread — `ChannelSink::emit` runs there, not a separate egress
//!   thread) into exactly one `cqlite.rpc.phase.duration` sample per sub-phase
//!   that recorded time, once at stream teardown.
//!
//! No new metric name or attribute key is introduced (Non-goal #1): the samples
//! ride the EXISTING `cqlite.rpc.phase.duration` histogram and the EXISTING
//! `cqlite.rpc.phase` attribute. The values are emitted ONLY on the `do_get`
//! method and ONLY on `phase.duration` (never on `cqlite.rpc.phase.active`, and
//! never added to the top-level `RPC_PHASES` ordered cursor). `stream_grpc_write`
//! is client-paced (egress channel park/wake), not server cost — flagged as such
//! in the operator-doc catalog annotation.

use std::sync::Arc;

use cqlite_core::observability::StreamSubPhaseTimings;
use cqlite_core::observability::{self as obs, catalog, AttrValue, StreamSubPhase};

/// See the module docs. Cold body-chunk page-in (cold-IO latency), producer thread.
pub const PHASE_STREAM_COLD_FAULT: &str = "stream_cold_fault";
/// LZ4 chunk decompression, producer thread.
pub const PHASE_STREAM_DECOMPRESS: &str = "stream_decompress";
/// k-way merge + reconcile + row materialize, merge-consumer thread.
pub const PHASE_STREAM_MERGE: &str = "stream_merge";
/// Arrow `RecordBatch` ARRAY BUILD, merge-consumer thread. Does NOT cover the
/// arrow-flight encoder stage — see [`PHASE_STREAM_ENCODE_FRAMING`].
pub const PHASE_STREAM_ENCODE: &str = "stream_encode";
/// Arrow-flight IPC FRAMING of an already-built `RecordBatch` (dictionary
/// hydration + encoder-target re-slicing + IPC serialization), recorded on the
/// ASYNC gRPC task that polls the response stream (issue #3096).
pub const PHASE_STREAM_ENCODE_FRAMING: &str = "stream_encode_framing";
/// Egress channel `reserve()`/send incl. backpressure park, on the merge-consumer
/// thread (`ChannelSink::emit`, not a separate egress thread) — CLIENT-PACED.
pub const PHASE_STREAM_GRPC_WRITE: &str = "stream_grpc_write";

/// The closed set of in-`stream` sub-phase `(StreamSubPhase, value)` pairs, in the
/// fixed order the teardown emitter walks them.
const STREAM_SUBPHASES: [(StreamSubPhase, &str); 6] = [
    (StreamSubPhase::ColdFault, PHASE_STREAM_COLD_FAULT),
    (StreamSubPhase::Decompress, PHASE_STREAM_DECOMPRESS),
    (StreamSubPhase::Merge, PHASE_STREAM_MERGE),
    (StreamSubPhase::Encode, PHASE_STREAM_ENCODE),
    (StreamSubPhase::EncodeFraming, PHASE_STREAM_ENCODE_FRAMING),
    (StreamSubPhase::GrpcWrite, PHASE_STREAM_GRPC_WRITE),
];

/// Emit one `cqlite.rpc.phase.duration` sample per in-`stream` sub-phase that
/// accumulated any wall time (issue #2819), tagged with the `do_get` method and
/// the bounded `cqlite.rpc.phase = stream_*` value. Bounded to ≤6 samples per RPC
/// (one per sub-phase that recorded time), emitted ONCE at stream teardown — never
/// once per row/chunk. A sub-phase that recorded nothing emits no sample (never a
/// fabricated zero), matching `PhaseTimer`'s "a phase never entered records none"
/// invariant. The sub-phases run on concurrent pipeline threads and OVERLAP in
/// wall-clock, so they are NOT expected to sum to the `stream` phase duration.
fn emit_stream_subphase_samples(timings: &StreamSubPhaseTimings) {
    let method = (catalog::attr::RPC_METHOD, AttrValue::StaticStr("do_get"));
    for (phase, value) in STREAM_SUBPHASES {
        let nanos = timings.nanos(phase);
        if nanos == 0 {
            continue;
        }
        let seconds = nanos as f64 / 1_000_000_000.0;
        obs::record_histogram(
            catalog::RPC_PHASE_DURATION,
            seconds,
            &[
                method.clone(),
                (catalog::attr::RPC_PHASE, AttrValue::StaticStr(value)),
            ],
        );
    }
}

/// RAII emitter that flushes the per-request in-`stream` sub-phase samples exactly
/// once at teardown (issue #2819). Constructed inside the merge closure alongside
/// the `PhaseTimer`; its [`Drop`] emits the accumulated sub-phase histogram
/// samples on EVERY exit path (normal completion, error, cancel, panic), mirroring
/// `PhaseTimer`'s own drop-driven emission — so a stalled or errored `do_get` still
/// records whatever sub-phase time it accrued.
///
/// It re-`enter()`s the `flight.do_get` RPC span around the emission loop (issue
/// #2819 L5) — mirroring `PhaseTimer::record_current` — so the five `stream_*`
/// samples carry the SAME span/exemplar association as the top-level phase samples
/// on that instrument, and an operator correlating a slow trace sees the sub-phase
/// breakdown, not just the top-level phases.
///
/// The span is passed IN by the caller (`spawn_streaming`), captured on the async
/// task BEFORE the `spawn_blocking`. It must NOT be captured here via
/// `Span::current()`: this emitter is constructed ON the `spawn_blocking` thread,
/// which tokio does NOT reach the caller's span into, so `Span::current()` here is
/// the EMPTY span (the roborev L5 non-functional-fix cause).
pub struct StreamSubPhaseEmitter {
    timings: Arc<StreamSubPhaseTimings>,
    span: tracing::Span,
}

impl StreamSubPhaseEmitter {
    /// Wrap the per-request accumulator so its samples are emitted at drop, under
    /// `rpc_span` — the `flight.do_get` span the caller captured on the async task
    /// (NOT `Span::current()` on this blocking thread, which is empty).
    pub fn new(rpc_span: tracing::Span, timings: Arc<StreamSubPhaseTimings>) -> Self {
        Self {
            timings,
            span: rpc_span,
        }
    }
}

impl Drop for StreamSubPhaseEmitter {
    fn drop(&mut self) {
        // `Drop` runs exactly once, so the emission is unconditional (no
        // "already-emitted" guard is reachable). A sub-phase that accumulated no
        // time emits no sample (`emit_stream_subphase_samples` skips zero buckets).
        // Re-enter the captured RPC span so the samples attach to it (L5).
        let _entered = self.span.enter();
        emit_stream_subphase_samples(&self.timings);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subphase_values_are_the_bounded_closed_set() {
        // The value table must stay a closed 6-value set of `stream_*` labels
        // (never a ticket/key/query value) and align 1:1 with the StreamSubPhase
        // variants the core seam records into.
        assert_eq!(STREAM_SUBPHASES.len(), 6);
        for (_, v) in STREAM_SUBPHASES {
            assert!(v.starts_with("stream_"), "{v} must be a stream_* sub-phase");
        }
        // Each value appears exactly once, and each variant is mapped exactly once
        // — a copy/paste that mapped two variants to the same label would silently
        // merge two buckets into one.
        let mut values: Vec<&str> = STREAM_SUBPHASES.iter().map(|(_, v)| *v).collect();
        values.sort_unstable();
        values.dedup();
        assert_eq!(
            values.len(),
            STREAM_SUBPHASES.len(),
            "duplicate phase value"
        );
        // The framing bucket is the one issue #3096 added; assert it explicitly so
        // a revert that drops it fails here rather than silently restoring the
        // attribution blind spot.
        assert!(
            STREAM_SUBPHASES
                .iter()
                .any(|(p, v)| *p == StreamSubPhase::EncodeFraming
                    && *v == PHASE_STREAM_ENCODE_FRAMING),
            "the IPC-framing sub-phase must stay in the emitted set (issue #3096)"
        );
    }

    #[test]
    fn emitter_is_callable_without_a_meter() {
        // The emitter must drive its emission without panicking in any build
        // (a no-op when the core observability feature is off).
        let t = Arc::new(StreamSubPhaseTimings::default());
        t.add_nanos(StreamSubPhase::Merge, 1_000);
        let e = StreamSubPhaseEmitter::new(tracing::Span::none(), t);
        drop(e);
    }

    /// Issue #2819 (roborev L5): the emitter must carry the LIVE `flight.do_get`
    /// span the caller captured on the async task — NOT an empty span. With a real
    /// subscriber installed (so spans are enabled), constructing the emitter with
    /// the ambient span must store a NON-disabled span equal to it. This FAILS if a
    /// regression reverts to `Span::current()` inside the `spawn_blocking` closure
    /// (which is the empty/disabled span there).
    #[test]
    fn emitter_carries_the_live_rpc_span() {
        use tracing_subscriber::prelude::*;
        let subscriber = tracing_subscriber::registry().with(tracing_subscriber::fmt::layer());
        tracing::subscriber::with_default(subscriber, || {
            let rpc = tracing::info_span!("flight.do_get");
            let _g = rpc.enter();
            // Capture the ambient span exactly as `spawn_streaming` does, then hand
            // it to the emitter (as the blocking closure would).
            let captured = tracing::Span::current();
            assert!(
                !captured.is_disabled(),
                "the ambient flight.do_get span must be enabled under a subscriber"
            );
            let e =
                StreamSubPhaseEmitter::new(captured, Arc::new(StreamSubPhaseTimings::default()));
            assert!(
                !e.span.is_disabled(),
                "emitter must hold a NON-disabled (live) span, not the empty span a \
                 spawn_blocking `Span::current()` would yield"
            );
            assert_eq!(
                e.span.id(),
                rpc.id(),
                "emitter must carry the SAME flight.do_get span the samples correlate to"
            );
        });
    }
}
