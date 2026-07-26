//! In-`stream` data-plane SUB-PHASE value table + per-request emitter (issue
//! #2819).
//!
//! A sibling of [`crate::obs`] (campsite rule, epic #1116), re-exported through it
//! (`pub use crate::obs_subphase::…` in `obs.rs`) so call sites keep the stable
//! `crate::obs::{...}` path. This module owns:
//!
//! * the five bounded `cqlite.rpc.phase` VALUES that decompose the `stream`
//!   phase (`stream_cold_fault`, `stream_decompress`, `stream_merge`,
//!   `stream_encode`, `stream_grpc_write`), and
//! * [`StreamSubPhaseEmitter`], the RAII that flushes the per-request accumulator
//!   (five `AtomicU64` nanos counters filled on the concurrent pipeline threads
//!   via `cqlite_core::observability::stream_subphase` — cold-fault/decompress on
//!   the per-SSTable PRODUCER thread, merge/encode on the merge consumer thread,
//!   gRPC-write on the egress thread) into exactly one `cqlite.rpc.phase.duration`
//!   sample per sub-phase that recorded time, once at stream teardown.
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
/// k-way merge + reconcile + row materialize, merge consumer thread.
pub const PHASE_STREAM_MERGE: &str = "stream_merge";
/// Arrow `RecordBatch` encode, merge consumer thread.
pub const PHASE_STREAM_ENCODE: &str = "stream_encode";
/// Egress channel `reserve()`/send incl. backpressure park, egress thread —
/// CLIENT-PACED.
pub const PHASE_STREAM_GRPC_WRITE: &str = "stream_grpc_write";

/// The closed set of in-`stream` sub-phase `(StreamSubPhase, value)` pairs, in the
/// fixed order the teardown emitter walks them.
const STREAM_SUBPHASES: [(StreamSubPhase, &str); 5] = [
    (StreamSubPhase::ColdFault, PHASE_STREAM_COLD_FAULT),
    (StreamSubPhase::Decompress, PHASE_STREAM_DECOMPRESS),
    (StreamSubPhase::Merge, PHASE_STREAM_MERGE),
    (StreamSubPhase::Encode, PHASE_STREAM_ENCODE),
    (StreamSubPhase::GrpcWrite, PHASE_STREAM_GRPC_WRITE),
];

/// Emit one `cqlite.rpc.phase.duration` sample per in-`stream` sub-phase that
/// accumulated any wall time (issue #2819), tagged with the `do_get` method and
/// the bounded `cqlite.rpc.phase = stream_*` value. Bounded to ≤5 samples per RPC
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
pub struct StreamSubPhaseEmitter {
    timings: Arc<StreamSubPhaseTimings>,
}

impl StreamSubPhaseEmitter {
    /// Wrap the per-request accumulator so its samples are emitted at drop.
    pub fn new(timings: Arc<StreamSubPhaseTimings>) -> Self {
        Self { timings }
    }
}

impl Drop for StreamSubPhaseEmitter {
    fn drop(&mut self) {
        // `Drop` runs exactly once, so the emission is unconditional (no
        // "already-emitted" guard is reachable). A sub-phase that accumulated no
        // time emits no sample (`emit_stream_subphase_samples` skips zero buckets).
        emit_stream_subphase_samples(&self.timings);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subphase_values_are_the_bounded_closed_set() {
        // The value table must stay a closed 5-value set of `stream_*` labels
        // (never a ticket/key/query value) and align 1:1 with the StreamSubPhase
        // variants the core seam records into.
        assert_eq!(STREAM_SUBPHASES.len(), 5);
        for (_, v) in STREAM_SUBPHASES {
            assert!(v.starts_with("stream_"), "{v} must be a stream_* sub-phase");
        }
    }

    #[test]
    fn emitter_is_callable_without_a_meter() {
        // The emitter must drive its emission without panicking in any build
        // (a no-op when the core observability feature is off).
        let t = Arc::new(StreamSubPhaseTimings::default());
        t.add_nanos(StreamSubPhase::Merge, 1_000);
        let e = StreamSubPhaseEmitter::new(t);
        drop(e);
    }
}
