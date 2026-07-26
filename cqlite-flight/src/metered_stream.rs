//! The accounting + cancellation + credit-release wrapper around the `do_get`
//! batch stream.
//!
//! Split out of `streaming.rs` (campsite rule, epic #1116) when issue #2821
//! added the deferred egress-credit slot.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::task::{Context, Poll};

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow_flight::error::FlightError;
use futures::Stream;
use tokio::sync::mpsc;
use tonic::Status;
use tracing::Span;

use crate::cancel::CancelGuard;
use crate::egress_credit::{CreditedBatch, EgressPermit};
use crate::obs::RpcMetrics;
use crate::producer::ProducerError;
use crate::streaming::StreamProbe;

/// Minimal [`Stream`] adapter over a bounded [`mpsc::Receiver`] (the crate has no
/// `tokio-stream` dependency; `poll_recv` is all we need).
pub(crate) struct ReceiverStream<T> {
    pub(crate) rx: mpsc::Receiver<T>,
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.rx.poll_recv(cx)
    }
}

/// Egress credit for a batch that has been handed downstream but whose Arrow
/// data may still be referenced by the consumer (issue #2821).
///
/// The permit is released when — and only when — nothing downstream references
/// the batch's arrays any more, observed through a `Weak` handle per column
/// rather than inferred from the consumer's polling discipline. That distinction
/// is the whole point:
///
/// * Releasing at the top of every `poll_next` (the obvious implementation) is
///   correct ONLY for a consumer that drops batch N before asking for N+1. That
///   happens to be exactly what `FlightDataEncoder` does (arrow-flight 53.4.1,
///   `encode.rs:400-436`: it polls `inner` only once its `FlightData` queue is
///   empty, and `encode_batch` consumes and drops the `RecordBatch`) — but
///   `MeteredDoGetStream` is `pub(crate)` and polled directly in tests, and a
///   speculative poll (a `select!` arm, `futures::poll!`) from a consumer still
///   holding the batch would return the credit for resident data and silently
///   void the published bound.
/// * Releasing only when the NEXT batch is *yielded* is the other obvious
///   implementation, and it DEADLOCKS: the deferred permit can hold the whole
///   pool while the producer parks reserving the next batch and the consumer
///   waits for that batch.
///
/// Keying on `Weak::strong_count() == 0` satisfies both: the production encoder
/// has dropped the batch by the time it re-polls, so credit is returned exactly
/// as promptly as before, while a consumer that is still holding the data keeps
/// paying for it (no voided bound). Conservative in the safe direction — a
/// consumer that retains a single column keeps the credit charged, because that
/// column's buffers are genuinely still resident.
///
/// # It cannot wedge the stream: the safety valve
///
/// Keying on the data's liveness moves the dependence from the consumer's POLL
/// discipline to its DROP discipline, and a consumer that retains batch N while
/// awaiting N+1 would otherwise hang: the deferred permit holds the credit, the
/// producer parks in `EgressCredit::reserve`, and the batch the consumer is
/// waiting for can never be built. `MeteredDoGetStream::open_safety_valve`
/// closes that cycle — see it for the predicate and for why releasing the credit
/// there is CORRECT rather than a concession.
///
/// No consumer behaviour can wedge this stream.
struct DeferredCredit {
    /// The held credit. The permit's `Drop` IS the release, so removing it from
    /// `deferred` (or clearing the vec) returns the bytes to the pool — there is
    /// deliberately no second, hand-audited release path that could drift from
    /// the charged amount. Its `charged_bytes()` is READ (never written) by
    /// [`MeteredDoGetStream::open_safety_valve`] to test whether the whole pool
    /// is held by consumer-retained batches.
    permit: EgressPermit,
    /// One weak handle per column of the yielded batch. Empty for a zero-column
    /// batch, which holds no buffers and is therefore released immediately.
    columns: Vec<Weak<dyn Array>>,
}

impl DeferredCredit {
    fn new(batch: &RecordBatch, permit: EgressPermit) -> Self {
        Self {
            permit,
            columns: batch.columns().iter().map(Arc::downgrade).collect(),
        }
    }

    /// `true` once nothing downstream holds any of the batch's arrays.
    fn is_dropped_downstream(&self) -> bool {
        self.columns.iter().all(|col| col.strong_count() == 0)
    }
}

/// Accounting + cancellation wrapper around the `do_get` batch stream.
///
/// Accumulates rows/bytes per batch as they pass toward the encoder and attributes
/// them to [`RpcMetrics`] at stream end — a fully-consumed stream records the same
/// totals the collect path would; a stream dropped early (client disconnect)
/// attributes exactly the emitted prefix. It also owns the merge's [`CancelGuard`]
/// so dropping the response stream cancels the merge (issue #1476 / #1473), and
/// it is the DRAIN SIDE of the egress credit governor (issue #2821): the permit
/// that rode through the channel with each batch is held here until the consumer
/// comes back for the next one.
pub(crate) struct MeteredDoGetStream {
    inner: Pin<Box<dyn Stream<Item = Result<CreditedBatch, ProducerError>> + Send + 'static>>,
    /// Taken (and dropped, emitting terminal metrics) exactly once at finalization.
    metrics: Option<RpcMetrics>,
    /// Cancels the merge on drop unless disarmed; `None` for the aggregate route
    /// (already materialized — nothing to cancel).
    guard: Option<CancelGuard>,
    /// Aborts the detached merge `spawn_blocking` task on Drop (issue #2264),
    /// defense-in-depth beyond the cancellation-aware send + `CancelGuard`. `None`
    /// for the aggregate route (already materialized — nothing to abort).
    abort: Option<tokio::task::AbortHandle>,
    /// The shared cancel flag's async cancellation future (issue #2680 / P0 #2782
    /// server hardening). Polled in `poll_next` ONLY when the inner stream would
    /// otherwise park, so a cancellation tripped from ANY source — a half-closed
    /// peer, a transport reset, or the drop guard — terminates egress at the next
    /// poll instead of waiting for the merge to notice between steps. `None` for
    /// the aggregate route (already materialized — nothing to interrupt). This is
    /// defense-in-depth: it never fires on the normal delivery path (the inner
    /// poll is biased first), and never regresses the existing drop-driven cancel.
    cancelled: Option<Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>>,
    /// DEFERRED egress credit for batches already handed downstream (issue
    /// #2821).
    ///
    /// This stream is constructed UPSTREAM of `FlightDataEncoderBuilder`, and the
    /// encoder holds the `RecordBatch` we hand it while it encodes. Releasing the
    /// credit at the instant we yield would leave that batch resident with its
    /// credit already returned — reintroducing on the consumer side exactly the
    /// uncharged-resident-batch class that reserve-before-materialize eliminated
    /// on the producer side, and making the true bound
    /// `max(ceiling, one maximum batch) + one maximum batch`.
    ///
    /// So each permit is parked here and reaped at the top of a later
    /// `poll_next`, once [`DeferredCredit::is_dropped_downstream`] shows the
    /// consumer has released the data (see [`DeferredCredit`] for why the release
    /// point is keyed on the data's liveness rather than on a poll). Against the
    /// production encoder this reaps on the very next poll — identical timing to
    /// a release at the top of `poll_next`, with none of its dependence on
    /// consumer discipline.
    ///
    /// A `Vec` rather than a single slot: a consumer that holds several yielded
    /// batches at once keeps paying for all of them. It cannot grow without
    /// bound — unreleased credit stops the producer, which is precisely the
    /// intended backpressure.
    ///
    /// Cleared on every terminal arm and by `Drop`, so a client disconnect
    /// returns the credit. **Do not "simplify" this into release-on-yield.**
    deferred: Vec<DeferredCredit>,
    /// Owned future resolving the next time the producer PARKS on the exhausted
    /// credit pool (issue #2821 review R1). Polled only in the `Pending` arm,
    /// exactly like [`Self::cancelled`], and re-armed after each resolution.
    ///
    /// Registering this before returning `Pending` is what makes the safety
    /// valve race-free: without it a producer that parks in the window between
    /// the wedge check and the `Pending` return would never wake the stream
    /// again. Unconditional — an ungoverned (unbounded/inert) stream simply
    /// never signals, and the valve's predicate never holds for it.
    park_signal: Pin<Box<dyn Future<Output = ()> + Send>>,
    probe: StreamProbe,
    rows: u64,
    bytes: u64,
    errored: bool,
    /// The `flight.do_get` RPC span, captured via [`Span::current`] at
    /// construction time — while still executing inside the `do_get` wrapper's
    /// `.instrument(span)`-wrapped future, so it correctly resolves to that RPC
    /// span. Re-entered around every `poll_next` and around `Drop`'s finalize
    /// (issue #1476 roborev F2): once `do_get` itself returns the `Response`,
    /// later polls of THIS stream happen entirely outside the instrumented
    /// future, so `record_status_error`'s `Span::current()`-based OTel error
    /// marking would otherwise land on no span (or a stale, unrelated one) —
    /// this makes mid-stream error/cancellation observability land on the same
    /// span as an eager-setup error always did.
    span: Span,
}

impl MeteredDoGetStream {
    pub(crate) fn new(
        inner: Pin<Box<dyn Stream<Item = Result<CreditedBatch, ProducerError>> + Send + 'static>>,
        metrics: RpcMetrics,
        guard: Option<CancelGuard>,
        probe: StreamProbe,
        abort: Option<tokio::task::AbortHandle>,
        cancelled: Option<Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>>,
    ) -> Self {
        let park_signal = probe.egress.parked_owned();
        Self {
            inner,
            metrics: Some(metrics),
            guard,
            abort,
            cancelled,
            deferred: Vec::new(),
            park_signal,
            probe,
            rows: 0,
            bytes: 0,
            errored: false,
            span: Span::current(),
        }
    }

    /// Attribute the accumulated rows/bytes to metrics (and the probe) and record
    /// the terminal RPC status. Idempotent: `metrics` is taken exactly once, so a
    /// normal end followed by drop (or vice versa) records only once.
    fn finalize(&mut self, ok: bool) {
        if let Some(mut metrics) = self.metrics.take() {
            // Issue #2162: rows/bytes were already attributed INCREMENTALLY per
            // batch (`record_batch_progress`), so `finalize` no longer re-adds the
            // accumulated totals — it only sets the terminal OK/error status and
            // drops `metrics`, which emits the terminal RPC request/duration
            // counters. The probe records the emitted-prefix totals for tests.
            if ok {
                metrics.ok();
            }
            self.probe.store_terminal_totals(self.rows, self.bytes);
            // Dropping `metrics` here emits the terminal RPC counters/histogram.
        }
    }

    /// Return the credit of every deferred batch the consumer has finished with.
    ///
    /// Run at the TOP of `poll_next`, before the inner stream is polled: a
    /// producer parked on an exhausted pool must see the credit of the batches
    /// the consumer already dropped, or a one-batch-deep pool would deadlock.
    /// Batches the consumer still holds stay charged — including across a poll
    /// that returns `Pending`.
    fn reap_deferred(&mut self) {
        self.deferred.retain(|d| !d.is_dropped_downstream());
    }

    /// **The safety valve**: force-release deferred permits OLDEST-FIRST — as
    /// many as the parked reservation needs and no more — when the stream is
    /// wedged, so no consumer behaviour can hang `do_get`.
    ///
    /// Run ONLY from the `Poll::Pending` arm, after `reap_deferred` and after the
    /// inner stream has declined to yield. The wedge predicate is the conjunction
    /// of three facts, each observed rather than assumed:
    ///
    /// 1. **The channel is empty.** The inner poll returned `Pending`, so no
    ///    batch is on its way to this stream.
    /// 2. **The producer is parked on the credit pool.**
    ///    [`EgressObservation::parked_now`] is a GAUGE maintained by an RAII
    ///    guard around the semaphore await, so it counts producers blocked *right
    ///    now*, not parks that have since been satisfied or abandoned.
    /// 3. **Every charged byte is held by a deferred (consumer-retained) batch.**
    ///    Summing the deferred permits' own `charged_bytes` and comparing against
    ///    the pool's total charge means the ONLY way to free credit is for the
    ///    consumer to drop a batch — which it may never do.
    ///
    /// In that state the cycle is closed: the producer waits on credit, the
    /// credit waits on the consumer, and the consumer waits on the producer.
    /// Releasing ENOUGH credit breaks it. The producer is woken by the semaphore,
    /// builds the next batch, and its send wakes this stream through the receiver
    /// waker registered by the `Pending` poll — so no extra wakeup is needed
    /// here.
    ///
    /// # Why the release is sized, and why ONE permit is not enough
    ///
    /// A single firing must restore progress on its own. This arm returns
    /// `Pending` without arranging to be re-entered, and by construction nothing
    /// else will re-enter it: the producer is parked (so it emits no wakeup), the
    /// channel is empty (so it emits none either), and the consumer is awaiting
    /// the batch the parked producer cannot build. So a valve that released one
    /// permit and returned `Pending` would WEDGE the stream outright whenever the
    /// oldest deferred permit charges less than the parked reservation needs —
    /// i.e. whenever the batches have non-uniform capacities (roborev job 12 F1).
    ///
    /// The release is therefore sized against observed facts rather than assumed
    /// to be enough: [`EgressObservation::parked_want_bytes`] is what the parked
    /// reservation is asking the pool for, and
    /// `pool_total_bytes - charged_bytes` is what the pool can already give it.
    /// Deferred permits are released oldest-first until the second reaches the
    /// first, and NOT ONE FURTHER — the loop exits the instant the producer can
    /// proceed, so a wedge costs the minimum credit that restores progress and
    /// never a blanket drain of the deferred slot (which would silently loosen
    /// the bound the valve exists to protect). The parked reservation is clamped
    /// to the pool total before it parks, so the target is always reachable; the
    /// loop is bounded by the deferred slot regardless.
    ///
    /// **Why releasing is correct, not a compromise.** The published bound
    /// governs **server-side residency** — the bytes the SERVER holds and can
    /// still act on. A batch the consumer has taken and is retaining is the
    /// consumer's memory; the governor charging for it would be metering
    /// something it no longer controls, and doing so is precisely what closes the
    /// cycle above. So the valve does not loosen the bound: it restores the
    /// bound's actual subject. The reframing is stated in
    /// [`crate::egress_credit::DEFAULT_MAX_INFLIGHT_EGRESS_BYTES`].
    ///
    /// Oldest first: the batch that has been out longest is the one least likely
    /// to still be in genuine use. Every permit released is counted
    /// ([`EgressObservation::safety_valve_releases`]), so "the valve fires on the
    /// normal path" is a test-detectable regression rather than a silent
    /// loosening — against a consumer that drops batch N before N+1 exists
    /// (including `FlightDataEncoder`) it never fires at all.
    fn open_safety_valve(&mut self) {
        if self.deferred.is_empty() {
            return;
        }
        let obs = &self.probe.egress;
        // `Acquire`: this also makes the matching `parked_want_bytes` visible.
        if obs.parked_now() == 0 {
            return;
        }
        let charged = obs.charged_bytes();
        let deferred_charged = self
            .deferred
            .iter()
            .fold(0u64, |acc, d| acc.saturating_add(d.permit.charged_bytes()));
        if deferred_charged == 0 || deferred_charged < charged {
            // Someone other than a consumer-retained batch holds credit (a
            // queued batch, an in-flight reservation): the producer's park will
            // clear on its own, so this is backpressure, not a wedge.
            return;
        }
        // How much the parked reservation needs, and how much the pool can
        // already give it. `free` is tracked incrementally from the permits this
        // loop actually returns rather than re-read from the pool, so a producer
        // concurrently charging against the pool cannot make the loop release
        // MORE than the wedge requires.
        let want = obs.parked_want_bytes();
        let mut free = obs.pool_total_bytes().saturating_sub(charged);
        let mut released = 0u64;
        while free < want && !self.deferred.is_empty() {
            // Dropping the permit IS the release (see `DeferredCredit::permit`).
            let oldest = self.deferred.remove(0);
            free = free.saturating_add(oldest.permit.charged_bytes());
            drop(oldest);
            obs.record_safety_valve_release();
            released = released.saturating_add(1);
        }
        if released > 0 {
            tracing::debug!(
                released,
                deferred_remaining = self.deferred.len(),
                want_bytes = want,
                "egress safety valve: released consumer-retained batches' credit to unwedge the \
                 stream (the consumer is holding yielded batches while awaiting the next one; \
                 those bytes are consumer-side residency, outside the server-side bound)"
            );
        }
    }

    /// The merge is finished (completed or errored); disarm so we do not cancel a
    /// flag whose task has already exited.
    fn disarm_guard(&mut self) {
        if let Some(guard) = self.guard.as_mut() {
            guard.disarm();
        }
    }
}

impl Stream for MeteredDoGetStream {
    type Item = Result<RecordBatch, FlightError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // Roborev F2: re-enter the captured `do_get` span so any observability
        // recorded during this poll (error marking, span-current lookups)
        // attributes to the RPC span, not whatever happens to be ambient on the
        // task currently driving this stream. Cloned first (cheap — `Span` is an
        // `Arc` handle) so `Entered`'s borrow doesn't pin `this` immutably while
        // the match arms below need `&mut this`.
        let span = this.span.clone();
        let _entered = span.enter();
        // Issue #2821: return the credit of every previously-yielded batch the
        // consumer has finished with, BEFORE polling the inner stream — a
        // producer parked on the pool must be able to make progress on this
        // poll. A batch the consumer is still holding keeps its credit, even if
        // this poll goes on to return `Pending`. See `DeferredCredit`.
        this.reap_deferred();
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(credited))) => {
                let (batch, permit) = credited.split();
                // Issue #2162: attribute this batch's rows/bytes as an INCREMENTAL
                // counter delta right now, as it passes toward the client, instead
                // of a single aggregate emission at stream end. One emission per
                // batch, never per row. The running total (`this.rows`/`this.bytes`)
                // is still accumulated for the terminal probe/parity bookkeeping;
                // `finish` no longer re-emits the counters, so the monotonic total
                // over a fully-drained stream is byte-identical to the pre-#2162
                // single emission.
                let batch_rows = batch.num_rows() as u64;
                let batch_bytes = batch.get_array_memory_size() as u64;
                this.rows = this.rows.saturating_add(batch_rows);
                this.bytes = this.bytes.saturating_add(batch_bytes);
                if let Some(metrics) = this.metrics.as_mut() {
                    metrics.record_batch_progress(batch_rows, batch_bytes);
                }
                // Feature-independent progress seam (no-op OTel notwithstanding):
                // publish the running rows and bump the per-batch emission count so
                // a slow-consumer test can observe forward progress mid-stream.
                this.probe.record_batch_emitted(this.rows);
                // Hold this batch's credit until the consumer drops the batch.
                this.deferred.push(DeferredCredit::new(&batch, permit));
                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(err))) => {
                // Terminal: nothing further will be yielded, so release every
                // deferred permit here rather than waiting for `Drop`.
                this.deferred.clear();
                this.errored = true;
                this.disarm_guard();
                this.finalize(false);
                // Issue #2681: stamp the abort reason from the typed producer
                // error VARIANT (a cooperative cancel → client_cancel; a
                // merge/convert/predicate/discovery/panic fault → internal),
                // never inferred from the resulting gRPC code. The high-cardinality
                // ticket/split identity is not threaded into the merge task, so the
                // event carries an empty context here (the metric attribution — the
                // load-bearing signal — is fully stamped).
                let reason = crate::service::producer_error_abort_reason(&err);
                let status = Status::from(err);
                // Roborev B2: pre-change `do_get` ran `record_status_error` for
                // EVERY error (service.rs `finish`); a mid-stream error must hit the
                // same error-observability path, not just the per-RPC OK/error flag.
                crate::obs::record_do_get_abort(&status, reason, crate::obs::AbortContext::empty());
                this.probe.record_error();
                Poll::Ready(Some(Err(FlightError::ExternalError(Box::new(status)))))
            }
            Poll::Ready(None) => {
                // Terminal: no further batch can be produced, so no credit needs
                // to stay charged (see the error arm).
                this.deferred.clear();
                this.disarm_guard();
                this.finalize(!this.errored);
                Poll::Ready(None)
            }
            Poll::Pending => {
                // The inner stream (bounded channel) has no batch ready. Race the
                // shared cancel flag (issue #2680 / P0 #2782 server hardening): if a
                // cancellation has been requested from ANY source — a half-closed
                // peer, a transport reset, or the response-stream drop guard — end
                // egress NOW rather than parking until the merge notices it between
                // steps. Polled ONLY here (the delivery arm above already returned),
                // so the normal fast path is untouched. On cancel, terminate the
                // stream cleanly (`None`): the emitted prefix is finalized and the
                // merge is stopped via the guard/abort — no truncated result is
                // presented as complete because the client is already departing.
                if let Some(cancelled) = this.cancelled.as_mut() {
                    if cancelled.as_mut().poll(cx).is_ready() {
                        // Terminal (see the error arm).
                        this.deferred.clear();
                        this.disarm_guard();
                        this.finalize(!this.errored);
                        return Poll::Ready(None);
                    }
                }
                // Issue #2821 review R1 — the SAFETY VALVE. Register for the
                // producer's next park BEFORE testing the wedge predicate, so a
                // park that happens after the test still wakes this stream (a
                // `Notify` permit is stored when no waiter is registered, so a
                // park on either side of the window is caught exactly once).
                // Without this registration the valve would lose a race it can
                // never recover from: nothing else will ever poll a stream whose
                // consumer is waiting on the batch the wedged producer cannot
                // build.
                //
                // Loop because a resolved signal must be RE-ARMED and the
                // replacement polled, or the replacement never registers. At most
                // two iterations in practice — `Notify` stores a single permit —
                // and the trip count is bounded structurally rather than assumed.
                for _ in 0..4 {
                    if this.park_signal.as_mut().poll(cx).is_pending() {
                        break;
                    }
                    this.park_signal = this.probe.egress.parked_owned();
                }
                this.open_safety_valve();
                Poll::Pending
            }
        }
    }
}

impl Drop for MeteredDoGetStream {
    fn drop(&mut self) {
        // Roborev F2: enter the RPC span so a drop-triggered finalize (early
        // client disconnect) still attributes under `flight.do_get` (cloned
        // first for the same reason as `poll_next` — avoid pinning `self`
        // immutably while `finalize` needs `&mut self`).
        let span = self.span.clone();
        let _entered = span.enter();

        // Issue #2821: return every deferred batch's egress credit explicitly, so
        // release on a client disconnect is stated at the site rather than
        // resting on field-drop order. Dropping `inner` then drops every batch
        // still queued in the channel, each releasing its own permit — RAII, so
        // no termination path can strand credit.
        self.deferred.clear();

        // Roborev round 4: `self.metrics` is still `Some` here ONLY when this
        // stream is being dropped BEFORE it ever reached its own terminal poll
        // (normal end via `Poll::Ready(None)`, or an error via
        // `Poll::Ready(Some(Err(_)))`) — i.e. an unclean early drop, the client
        // disconnecting mid-stream. Both of those poll_next arms call
        // `finalize`, which `take()`s `metrics`, so a stream that already ended
        // (successfully or with a recorded error) always has `metrics == None`
        // by the time `Drop` runs — this check can never double-record.
        //
        // Pre-change, a disconnect surfaced as `aborted` through the handler's
        // `Err` path and hit `record_status_error` there; the streaming rewrite
        // must not let that vanish from the flight error-rate signal / RPC-span
        // error marking just because the failure now arrives via `Drop` instead
        // of a returned `Err`.
        if self.metrics.is_some() {
            let status =
                Status::aborted("do_get stream dropped before completion (client disconnected)");
            // Issue #2681: a stream dropped before completion IS a client
            // disconnect — stamp `client_cancel` at the site (a benign, expected
            // terminal state), never inferred from the `Aborted` code.
            crate::obs::record_do_get_abort(
                &status,
                crate::obs::AbortReason::ClientCancel,
                crate::obs::AbortContext::empty(),
            );
            self.probe.record_error();
        }

        // Early drop (client disconnect): attribute the emitted prefix, then the
        // still-armed guard cancels the merge (dropped as a struct field right
        // after this fn body runs). `finalize` is idempotent, so a stream that
        // already ended normally records nothing more here.
        self.finalize(false);

        // Belt-and-suspenders (issue #2264): abort the detached merge task at its
        // next await point. The cancellation-aware send is what actually unparks a
        // producer blocked in the full channel (abort cannot interrupt a blocking
        // `reserve`/send synchronously); this covers a task that has moved past its
        // send but not yet observed the between-step cancel poll. Harmless for a
        // task that already completed (abort of a finished task is a no-op).
        if let Some(abort) = self.abort.take() {
            abort.abort();
        }
    }
}
