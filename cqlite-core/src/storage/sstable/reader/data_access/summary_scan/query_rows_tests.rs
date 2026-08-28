//! Unit tests for the single-generation query ROW stream (`query_rows.rs`).
//!
//! Split out of `query_rows.rs` to keep that file under the campsite-rule size
//! limit (epic #1116). Included via
//! `#[cfg(test)] #[path = "query_rows_tests.rs"] mod tests;`, so these tests are
//! a CHILD module of `query_rows` and reach its private protocol items
//! (`QueryRowMsg`, `BatchSink`, `panicked_producer_error`, and
//! `QueryRowStream`'s fields) directly.

use super::*;

/// Build a `QueryRowStream` around a raw channel so the CONSUMER half of the
/// #3106 protocol can be asserted in isolation, including the case a real
/// producer cannot be made to reach on demand (a sender dropped with no
/// terminal message at all). The end-to-end proof over a real walk with a
/// real panicking producer lives in `query_rows_panic_tests`.
fn stream_over(rx: Receiver<QueryRowMsg>) -> QueryRowStream {
    QueryRowStream {
        rx,
        child_cancel: ScanCancel::new(),
        terminated: false,
    }
}

/// Issue #3106: a producer that drops its sender WITHOUT the terminal `Done`
/// sentinel — exactly what an unwinding producer thread leaves behind — must
/// yield an ERROR, never the clean `None` that made a truncated result set
/// look like a finished scan.
#[test]
fn a_disconnect_without_the_done_sentinel_is_an_error_not_a_clean_end_of_stream() {
    let (tx, rx) = sync_channel::<QueryRowMsg>(QUERY_ROWS_CHANNEL_BATCHES);
    let mut stream = stream_over(rx);

    // One batch is delivered, then the producer "dies" mid-stream.
    tx.send(QueryRowMsg::Item(QueryRowBatch::Rows(vec![(
        RowKey::new(vec![1]),
        ScanRow::Row(Vec::new()),
    )])))
    .expect("send batch");
    drop(tx);

    assert!(
        matches!(
            stream.next_batch(),
            Some(Ok(QueryRowBatch::Rows(rows))) if rows.len() == 1
        ),
        "the batch already produced is still delivered"
    );
    let err = stream
        .next_batch()
        .expect("a dead producer must NOT report a clean end of stream")
        .expect_err("a dead producer must be an error");
    let msg = err.to_string();
    assert!(
        msg.contains("WITHOUT its terminal") && msg.contains("TRUNCATED"),
        "the error must name the violated protocol and the truncation, got: {msg}"
    );
    assert!(
        stream.next_batch().is_none(),
        "after the terminal error the stream is finished, not endlessly erroring"
    );
}

/// The sentinel is what makes a clean end of stream OBSERVED rather than
/// inferred: `Done` (and only `Done`) yields `None`, and the subsequent
/// disconnect stays `None`.
#[test]
fn the_done_sentinel_is_the_only_clean_end_of_stream() {
    let (tx, rx) = sync_channel::<QueryRowMsg>(QUERY_ROWS_CHANNEL_BATCHES);
    let mut stream = stream_over(rx);
    tx.send(QueryRowMsg::Done).expect("send Done");
    assert!(
        stream.next_batch().is_none(),
        "the explicit terminator is a clean end of stream"
    );
    drop(tx);
    assert!(
        stream.next_batch().is_none(),
        "the disconnect AFTER a Done sentinel stays a clean end of stream"
    );
}

/// A terminal [`QueryRowMsg::Failed`] (the walk failed, or its panic was caught
/// and forwarded) also terminates the protocol, so the following disconnect must
/// not turn into a second, spurious dead-producer error that would mask the real
/// one. `Failed` is a DISTINCT variant, so "this message is terminal" is
/// structural: an `Item` cannot carry an error at all.
#[test]
fn a_terminal_error_terminates_the_protocol() {
    let (tx, rx) = sync_channel::<QueryRowMsg>(QUERY_ROWS_CHANNEL_BATCHES);
    let mut stream = stream_over(rx);
    tx.send(QueryRowMsg::Failed(Error::internal("walk failed")))
        .expect("send error");
    drop(tx);
    let msg = stream
        .next_batch()
        .expect("the terminal error is delivered")
        .expect_err("it is an error")
        .to_string();
    assert!(
        msg.contains("walk failed"),
        "the real cause survives: {msg}"
    );
    assert!(
        stream.next_batch().is_none(),
        "the disconnect after a terminal error is not a second error"
    );
}

/// A caught producer panic is forwarded with its MESSAGE, so the client gets
/// an informative failure instead of a generic "the producer died".
#[test]
fn a_caught_panic_is_forwarded_with_its_message() {
    let payload: Box<dyn std::any::Any + Send> = Box::new(String::from("decode blew up"));
    let msg = panicked_producer_error(payload.as_ref()).to_string();
    assert!(
        msg.contains("PANICKED") && msg.contains("decode blew up"),
        "the panic message must reach the caller, got: {msg}"
    );
    let static_payload: Box<dyn std::any::Any + Send> = Box::new("static blew up");
    assert!(
        panicked_producer_error(static_payload.as_ref())
            .to_string()
            .contains("static blew up"),
        "a &'static str payload (a bare panic!(\"…\")) is carried too"
    );
    let opaque: Box<dyn std::any::Any + Send> = Box::new(7u8);
    assert!(
        panicked_producer_error(opaque.as_ref())
            .to_string()
            .contains("non-string panic payload"),
        "an unrecognized payload degrades to a named placeholder, not silence"
    );
}

/// Roborev (issue #3058): the "`FellBack` is pre-emit only" contract is
/// ENFORCED, not assumed. Rows already handed to the sink (including an
/// under-full, still-buffered batch) must turn a second walk into a hard
/// corruption error rather than a silently duplicated result set.
#[test]
fn a_post_emit_fallback_fails_closed_instead_of_duplicating_rows() {
    assert!(
        assert_nothing_emitted(0, "before the full-index fallback walk").is_ok(),
        "the pre-emit case is the normal fallback and must proceed"
    );
    let err = assert_nothing_emitted(1, "before the full-index fallback walk")
        .expect_err("a post-emit fallback must fail closed");
    let msg = err.to_string();
    assert!(
        msg.contains("FellBack AFTER emitting"),
        "the error must name the violated contract, got: {msg}"
    );
    assert!(
        assert_nothing_emitted(127, "before reporting Unsupported").is_err(),
        "a partially-filled batch (< QUERY_ROWS_PER_BATCH) still counts as emitted"
    );
}

/// Roborev (issue #3058): a CALLER cancellation must reach the TOKEN-BOUNDED
/// walk too, at a batch boundary — not only once the consumer notices and
/// drops the stream (which on a wide partition is a whole batch later). The
/// caller's own flag must still be left un-cancelled (only the child is ever
/// cancelled), or the merge-arm fallback would be poisoned again.
#[test]
fn a_caller_cancellation_breaks_the_token_bound_sink_at_a_batch_boundary() {
    let caller = ScanCancel::new();
    let child = ScanCancel::new();
    let bridge = CancelBridge {
        caller: caller.clone(),
        child: child.clone(),
    };
    let (tx, rx) = sync_channel::<QueryRowMsg>(QUERY_ROWS_CHANNEL_BATCHES);
    let mut fault = ProducerFault::default();
    let mut sink = BatchSink::new(&tx, &bridge, &mut fault);
    let row = || (RowKey::new(vec![1]), ScanRow::Row(Vec::new()));

    // A full batch with no cancellation flows through.
    for _ in 0..QUERY_ROWS_PER_BATCH {
        assert!(matches!(
            sink.push(row()).expect("push"),
            ControlFlow::Continue(())
        ));
    }
    assert!(
        matches!(rx.try_recv(), Ok(QueryRowMsg::Item(QueryRowBatch::Rows(b))) if b.len() == QUERY_ROWS_PER_BATCH),
        "the first batch was handed to the consumer"
    );

    // Now the caller cancels. The next batch boundary must BREAK the walk.
    caller.cancel();
    let mut broke = false;
    for _ in 0..QUERY_ROWS_PER_BATCH {
        if matches!(sink.push(row()).expect("push"), ControlFlow::Break(())) {
            broke = true;
            break;
        }
    }
    assert!(
        broke,
        "a caller cancellation must stop the token-bounded walk at the batch \
         boundary, not only when the consumer drops the stream"
    );
    assert!(
        child.is_cancelled(),
        "the cancellation is propagated into the child so the walk's own poll \
         aborts it promptly too"
    );
    assert!(
        matches!(bridge.caller_result(), Err(Error::Cancelled)),
        "a cancelled scan terminates with Cancelled, never a clean short stream"
    );
}

/// The bridge is ONE-WAY: a caller cancellation stops the walk (via the
/// child), but nothing this stream does may cancel the caller's flag — the
/// fallback to the k-way merge arm depends on getting it back un-cancelled.
#[test]
fn the_cancel_bridge_is_one_way() {
    let caller = ScanCancel::new();
    let bridge = CancelBridge {
        caller: caller.clone(),
        child: ScanCancel::new(),
    };
    assert!(!bridge.poll_caller(), "no cancellation yet");
    bridge.child().cancel();
    assert!(
        !caller.is_cancelled(),
        "cancelling the CHILD must never reach the caller's flag"
    );
    assert!(!bridge.poll_caller(), "still no caller cancellation");

    let caller2 = ScanCancel::new();
    let bridge2 = CancelBridge {
        caller: caller2.clone(),
        child: ScanCancel::new(),
    };
    caller2.cancel();
    assert!(
        bridge2.poll_caller(),
        "a caller cancellation stops the scan"
    );
    assert!(
        bridge2.child().is_cancelled(),
        "and is propagated into the child so the walk aborts promptly"
    );
}

/// The exported read-ahead bounds must stay DERIVED from the buffer sizes that
/// actually run — never a hand-maintained literal (issue #3384).
///
/// This is the regression pin for the whole point of the constants: an integration
/// test sizes its fixture as a multiple of [`QUERY_ROWS_MAX_READ_AHEAD`] so
/// "the abandoned walk stopped early" is structural rather than scheduling luck.
/// Were a buffer sizing change to leave the constant behind, that test would
/// silently go back to asserting a coin flip, so each term is pinned here against
/// the sizing it comes from.
#[test]
fn the_exported_read_ahead_bounds_are_derived_from_the_real_buffer_sizes() {
    // The batch size here is the MAXIMUM of the two arms', not either arm's own
    // (roborev, issue #3384): the token-bounded arm re-chunks to
    // QUERY_ROWS_PER_BATCH, but the full-ring arm forwards the inner stream's
    // BATCH_EMIT_ROWS-capped batches verbatim, and a bound that must hold on both
    // has to assume the larger. Using QUERY_ROWS_PER_BATCH here is what made the
    // exported bound understate the full-ring arm by a factor of two.
    let max_handoff_batch = BATCH_EMIT_ROWS.max(QUERY_ROWS_PER_BATCH);
    assert_eq!(
        QUERY_ROWS_MAX_RESIDENT_ROWS,
        max_handoff_batch * (QUERY_ROWS_CHANNEL_BATCHES + 1),
        "the handoff-channel bound is channel-resident batches + the parked send, \
         each sized by the LARGER arm's batch"
    );
    assert!(
        max_handoff_batch >= BATCH_EMIT_ROWS,
        "the full-ring arm forwards BATCH_EMIT_ROWS-sized batches unchanged, so the \
         handoff term can never be smaller than one of them"
    );
    assert_eq!(
        QUERY_ROWS_FULL_SCAN_BUFFER_ROWS,
        QUERY_ROWS_PER_BATCH * QUERY_ROWS_CHANNEL_BATCHES,
        "the full-ring arm's inner buffer_size must stay the value \
         `drive_full_scan_rows` passes"
    );
    assert_eq!(
        QUERY_ROWS_MAX_READ_AHEAD,
        QUERY_ROWS_MAX_RESIDENT_ROWS
            + batched_channel_capacity(QUERY_ROWS_FULL_SCAN_BUFFER_ROWS) * BATCH_EMIT_ROWS
            + BATCH_EMIT_ROWS
            + MAX_INFLIGHT_BATCH_ROWS,
        "the walk's read-ahead is the SUM of every bounded buffer between disk \
         and consumer; see the constant's doc for the four terms"
    );
    // The walk's read-ahead STRICTLY exceeds the handoff channel's own bound —
    // the property that makes using the wrong one of the two a real error rather
    // than a stylistic one.
    assert!(
        QUERY_ROWS_MAX_READ_AHEAD > QUERY_ROWS_MAX_RESIDENT_ROWS,
        "read-ahead {QUERY_ROWS_MAX_READ_AHEAD} must exceed the handoff \
         channel's {QUERY_ROWS_MAX_RESIDENT_ROWS}"
    );
}

/// The channel sizing helper the read-ahead bound is derived from is the SAME
/// definition the batched scan stream's constructor uses, so it must behave like
/// `ceil` with a floor of one batch — a zero-capacity `mpsc::channel` would panic
/// at the constructor, and a capacity that over-states the real channel would
/// over-state the read-ahead bound.
#[test]
fn the_batched_channel_capacity_helper_is_ceil_with_a_one_batch_floor() {
    assert_eq!(batched_channel_capacity(0), 1, "never zero-capacity");
    assert_eq!(batched_channel_capacity(1), 1);
    assert_eq!(batched_channel_capacity(BATCH_EMIT_ROWS), 1);
    assert_eq!(
        batched_channel_capacity(BATCH_EMIT_ROWS + 1),
        2,
        "ceil, not floor"
    );
    assert_eq!(batched_channel_capacity(4 * BATCH_EMIT_ROWS), 4);
}
