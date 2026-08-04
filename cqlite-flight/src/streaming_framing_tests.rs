//! Teardown-ORDER guard for `FramingTimedStream` (issue #3096, roborev).
//!
//! # The invariant
//!
//! The IPC-framing sub-phase sample must be flushed while its PARENT stream is
//! still alive — i.e. `FramingTimedStream::_emitter` must drop BEFORE `inner` (the
//! metered/encoded response stream, whose own `Drop` finalizes the RPC's row/byte
//! accounting and records a mid-stream disconnect). That is the #2819 roborev-B1
//! ordering rule applied one level down: a sub-phase sample emitted after its
//! parent's teardown is a sample an end-of-stream metrics scrape can miss, and the
//! framing attribution is exactly what #3096 exists to make readable.
//!
//! Rust drops struct fields in DECLARATION order, so the invariant is carried by
//! nothing but the order of two lines. This module observes it instead of trusting
//! it: both events land in ONE ordered sink and the order is asserted.
//!
//! # Why the span-enter is a sound proxy for "the sample was emitted"
//!
//! `StreamSubPhaseEmitter::drop` re-enters the captured `flight.do_get` span and
//! THEN emits (see `obs_subphase.rs`), so a recorded enter means that `Drop` body
//! ran. Drops are sequential on one thread — the body runs to completion before the
//! next field's `Drop` starts — so "enter observed before the inner drop" implies
//! "emission completed before the inner drop". The proxy is used because the
//! emission itself lands in a metrics pipeline that is a no-op in the default
//! (meter-off) build, whereas `Span::enter` is observable in EVERY build, so this
//! guard runs on the plain `cargo test -p cqlite-flight --lib` path.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow_flight::FlightData;
use futures::Stream;
use tonic::Status;
use tracing_subscriber::layer::Context as LayerContext;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

use cqlite_core::observability::{StreamSubPhase, StreamSubPhaseTimings};

use super::FramingTimedStream;

/// The RPC span the emitter re-enters; also the name filtered on below.
const RPC_SPAN: &str = "flight.do_get";
/// Recorded when `StreamSubPhaseEmitter::drop` enters the RPC span to emit.
const FRAMING_EMITTED: &str = "framing-emitted";
/// Recorded when the wrapped parent stream is dropped.
const INNER_DROPPED: &str = "inner-dropped";

/// One ordered event log shared by the emitter observer and the inner stream.
#[derive(Clone, Default)]
struct OrderSink(Arc<Mutex<Vec<&'static str>>>);

impl OrderSink {
    fn push(&self, event: &'static str) {
        self.0.lock().expect("order sink not poisoned").push(event);
    }

    fn events(&self) -> Vec<&'static str> {
        self.0.lock().expect("order sink not poisoned").clone()
    }
}

/// Stands in for the encoded/metered parent stream: yields end-of-stream and
/// records its own drop into the shared sink.
struct DropRecordingStream {
    sink: OrderSink,
}

impl Stream for DropRecordingStream {
    type Item = Result<FlightData, Status>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

impl Drop for DropRecordingStream {
    fn drop(&mut self) {
        self.sink.push(INNER_DROPPED);
    }
}

/// Records every `flight.do_get` span ENTER into the shared sink — the emitter's
/// `Drop` is the only thing that enters it in this test.
struct SpanEnterRecorder {
    sink: OrderSink,
}

impl<S> tracing_subscriber::Layer<S> for SpanEnterRecorder
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_enter(&self, id: &tracing::span::Id, ctx: LayerContext<'_, S>) {
        if ctx.span(id).map(|s| s.name()) == Some(RPC_SPAN) {
            self.sink.push(FRAMING_EMITTED);
        }
    }
}

/// Issue #3096 (roborev): the framing sample must be emitted BEFORE the wrapped
/// parent stream is dropped. This FAILS (observed order `[inner-dropped,
/// framing-emitted]`) if `FramingTimedStream` ever declares `inner` ahead of
/// `_emitter` again, or grows an explicit `Drop` that emits after releasing
/// `inner`.
#[test]
fn framing_sample_is_emitted_before_the_wrapped_stream_is_dropped() {
    let sink = OrderSink::default();
    let subscriber = tracing_subscriber::registry().with(SpanEnterRecorder { sink: sink.clone() });
    tracing::subscriber::with_default(subscriber, || {
        let rpc_span = tracing::info_span!(RPC_SPAN);
        assert!(
            !rpc_span.is_disabled(),
            "the RPC span must be enabled, or the emitter's enter is unobservable \
             and this guard would pass vacuously"
        );
        let timings = Arc::new(StreamSubPhaseTimings::default());
        // Pre-seed the framing bucket: a zero bucket emits no sample at all, so a
        // guard over an empty accumulator would prove nothing about the emission.
        timings.add_nanos(StreamSubPhase::EncodeFraming, 4_096);

        let mut stream = FramingTimedStream {
            _emitter: crate::obs::StreamSubPhaseEmitter::new(rpc_span, timings.clone()),
            inner: Box::pin(DropRecordingStream { sink: sink.clone() }),
            timings,
        };
        // Teardown happens after the LAST poll in production; poll to end-of-stream
        // first so this exercises that path rather than an unpolled construction.
        assert!(
            futures::executor::block_on(futures::StreamExt::next(&mut stream)).is_none(),
            "the stand-in inner stream yields end-of-stream"
        );
        assert!(
            sink.events().is_empty(),
            "polling must record neither event; got {:?}",
            sink.events()
        );

        drop(stream);
    });

    assert_eq!(
        sink.events(),
        vec![FRAMING_EMITTED, INNER_DROPPED],
        "the framing sub-phase sample must be emitted while the wrapped parent \
         stream is still alive (fields drop in DECLARATION order, so `_emitter` \
         must stay declared before `inner`)"
    );
}
