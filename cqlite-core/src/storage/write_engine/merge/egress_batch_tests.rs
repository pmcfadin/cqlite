//! Unit pins for the batched egress fan-in (issue #2820) — the constants, the
//! ROWS→MESSAGES capacity conversion, the resident-rows bound, the ramp, and the
//! producer-side accumulator's send/accounting contract.
//!
//! A `*_tests.rs` sibling of `egress_batch.rs` per the #1116/#1135 campsite rule.
//! Also the new home of the channel-capacity constant pin moved out of the
//! ~12.5k-line `merge/mod.rs`.
//!
//! What these can and cannot see: they drive the REAL `EgressBatcher` over a real
//! `sync_channel`, so the send-count/accounting behaviour asserted here is the
//! production one. They do NOT prove the batcher is WIRED into the producer
//! threads — that is `egress_wiring_tests.rs` (per-source channel capacities) and
//! `tests/issue_2820_merge_fanin_batch.rs` (end-to-end send counts + row parity
//! over real SSTables).

use std::sync::atomic::{AtomicI64, Ordering};

use super::*;
use crate::storage::write_engine::merge::model::RowData;
use crate::storage::write_engine::merge::MergeEntry;
use crate::storage::write_engine::mutation::DecoratedKey;

fn entry(n: i64) -> MergeEntry {
    MergeEntry::new(
        0,
        DecoratedKey::new(n, n.to_be_bytes().to_vec()),
        None,
        100 + n,
        RowData::Live { cells: vec![] },
    )
}

/// The row budget stays 256 and stays a ROW budget (issue #2820 design item 1).
///
/// Moved verbatim in intent from `merge/mod.rs::streaming_tests`: it checks only
/// the constant's value — it does NOT prove an end-to-end memory bound. The
/// end-to-end bound (the producer streaming its source one partition at a time,
/// issue #827) is asserted by `tests/test_issue_827_merge_streaming_memory.rs`.
#[test]
fn the_row_budget_is_256_and_the_batch_size_is_256() {
    assert_eq!(
        super::super::STREAMING_CHANNEL_CAPACITY,
        256,
        "the per-source ROW budget is the documented 256; `egress_budget`'s whole \
         vocabulary (EGRESS_ROW_BUDGET, MIN_CAP, MAX_CAP) is derived from it in ROWS"
    );
    assert_eq!(BATCH_EMIT_ROWS_MERGE, 256);
    assert_eq!(
        FIRST_BATCH_EMIT_ROWS, 1,
        "the first batch of a run must be one row, so batching NEVER delays the \
         first row (issue #2820 design item 7)"
    );
    assert_eq!(
        MIN_MSG_CAP, 2,
        "a capacity-1 channel kills producer/consumer overlap"
    );
}

/// The ROWS→MESSAGES conversion (issue #2820 design item 2) — the single most
/// consequential line of the change.
///
/// The failure it exists to catch is NOT arithmetic: it is passing the ROW budget
/// straight to `sync_channel`, which would budget 256 BATCHES = 65_536 entries
/// per source, per merge — a 256x resident-row blow-up. So the pin is stated as
/// the ROWS the resulting capacity can hold, not just as a quotient.
#[test]
fn a_row_budget_converts_to_a_message_capacity_that_holds_about_that_many_rows() {
    // Default budget: 256 rows -> 1 full batch, floored at MIN_MSG_CAP.
    assert_eq!(
        message_capacity_for_rows(super::super::STREAMING_CHANNEL_CAPACITY),
        MIN_MSG_CAP
    );
    // div_ceil, so a partial batch still gets a slot.
    assert_eq!(message_capacity_for_rows(BATCH_EMIT_ROWS_MERGE * 4), 4);
    assert_eq!(message_capacity_for_rows(BATCH_EMIT_ROWS_MERGE * 4 + 1), 5);
    // Every input is floored at MIN_MSG_CAP: a tiny (or zero) row budget must
    // never produce a 0-capacity channel, which would wedge the producer on its
    // first send, nor a 1-capacity one, which serialises producer and consumer.
    for rows in [0usize, 1, 8, 64, 255] {
        assert_eq!(
            message_capacity_for_rows(rows),
            MIN_MSG_CAP,
            "row budget {rows} must floor at MIN_MSG_CAP"
        );
    }
    // The anti-blow-up statement: the message capacity must NEVER be the row
    // budget itself once that budget exceeds a couple of batches.
    let big = BATCH_EMIT_ROWS_MERGE * 16;
    assert!(
        message_capacity_for_rows(big) < big,
        "a ROW budget passed through as a MESSAGE capacity is the 256x resident-row \
         blow-up this conversion exists to prevent"
    );
}

/// The exported resident-rows bound (issue #2820 design item 3), mirroring
/// `scan_stream_windowed::MAX_INFLIGHT_BATCH_ROWS`: channel-resident + consumer-held
/// + producer-blocked-in-send, each up to one full batch.
#[test]
fn the_resident_rows_bound_is_channel_plus_two_batches() {
    let msg_cap = message_capacity_for_rows(super::super::STREAMING_CHANNEL_CAPACITY);
    assert_eq!(msg_cap, 2, "default row budget -> 2 messages");
    assert_eq!(
        max_inflight_rows(msg_cap),
        (msg_cap + 2) * BATCH_EMIT_ROWS_MERGE
    );
    // The worked example of record: 1024 rows = 4x the pre-#2820 flat 256.
    assert_eq!(max_inflight_rows(msg_cap), 1024);
    assert_eq!(
        max_inflight_rows(msg_cap),
        4 * super::super::STREAMING_CHANNEL_CAPACITY
    );
    // Monotone in the message capacity, so a shrunken adaptive budget can only
    // lower the bound (never raise it).
    for msg in 2..8 {
        assert!(max_inflight_rows(msg) < max_inflight_rows(msg + 1));
    }
}

/// The ramp (issue #2820 design item 7): the first flush is one row, the limit
/// doubles per flush, and it saturates at `BATCH_EMIT_ROWS_MERGE` — so a cold
/// producer parks in `send` holding FAR fewer rows than the saturated worst case,
/// which is why `rows_in_full_channel` exists alongside `max_inflight_rows`.
#[test]
fn the_ramp_doubles_from_one_row_and_saturates() {
    let mut limit = FIRST_BATCH_EMIT_ROWS;
    let mut seen = vec![limit];
    for _ in 0..12 {
        limit = next_batch_limit(limit);
        seen.push(limit);
    }
    assert_eq!(&seen[..5], &[1, 2, 4, 8, 16]);
    assert_eq!(
        *seen.last().expect("non-empty"),
        BATCH_EMIT_ROWS_MERGE,
        "the ramp must saturate at the full batch size, never grow past it"
    );

    assert_eq!(
        rows_in_full_channel(2),
        3,
        "1 + 2 rows for a 2-message channel"
    );
    assert_eq!(rows_in_full_channel(4), 15, "1 + 2 + 4 + 8");
    // Strictly below the saturated bound for every capacity: a fixture that
    // derived "the producer is blocked" from `max_inflight_rows` would wait for
    // rows that, from a cold start, can never be sent.
    for msg in 2..10 {
        assert!(
            rows_in_full_channel(msg) < max_inflight_rows(msg),
            "cold-start fill must be below the saturated bound at msg_cap {msg}"
        );
    }
}

/// The exact-message oracle the send-count test relies on
/// ([`EgressBatchProbe::expected_messages`]) must agree with what the REAL
/// batcher sends, for row counts on both sides of the ramp — otherwise the
/// end-to-end oracle would be pinning a formula against itself.
#[test]
fn expected_messages_matches_the_real_batcher() {
    let probe = merge_egress_batch_probe();
    for rows in [0usize, 1, 2, 3, 7, 255, 256, 257, 999, 1024] {
        // Capacity generous enough that no send ever blocks (this test drives the
        // batcher on ONE thread, so a full channel would deadlock).
        let (tx, rx) = std::sync::mpsc::sync_channel(rows + 4);
        let local_sent = AtomicI64::new(0);
        let mut batcher = EgressBatcher::new(&tx, &local_sent);
        for n in 0..rows {
            assert!(
                matches!(batcher.push(entry(n as i64)), ControlFlow::Continue(())),
                "a live channel must never break"
            );
        }
        // The pre-terminator tail flush the producer thread bodies perform.
        let _ = batcher.flush();
        drop(tx);

        let mut messages = 0u64;
        let mut entries = 0usize;
        let mut biggest = 0usize;
        let mut tokens = Vec::new();
        while let Ok(msg) = rx.recv() {
            let MergeMsg::Batch(batch) = msg else {
                panic!("the batcher sends only DATA batches");
            };
            assert!(!batch.is_empty(), "an empty batch must never be sent");
            messages += 1;
            entries += batch.len();
            biggest = biggest.max(batch.len());
            tokens.extend(batch.iter().map(|e| e.key.token));
        }
        assert_eq!(entries, rows, "every pushed row must be sent exactly once");
        assert_eq!(
            tokens,
            (0..rows as i64).collect::<Vec<_>>(),
            "batching must preserve scan ORDER, with no duplicate or dropped row"
        );
        assert!(
            biggest <= BATCH_EMIT_ROWS_MERGE,
            "no batch ({biggest}) may exceed BATCH_EMIT_ROWS_MERGE ({BATCH_EMIT_ROWS_MERGE})"
        );
        assert_eq!(
            messages,
            probe.expected_messages(rows as u64),
            "the send-count oracle's formula must match the real batcher for {rows} rows"
        );
        assert_eq!(
            local_sent.load(Ordering::SeqCst),
            rows as i64,
            "the adapter's own sent-count is in ENTRIES (issue #2419), so it must \
             track rows — not messages, which would make the Drop reconcile \
             residual leak `entries - messages` on every cancelled merge"
        );
    }
}

/// A sub-batch result set is delivered in FULL and immediately (issue #2820
/// design item 7), with NO wall-clock assertion: the oracle is in ROWS.
///
/// Three rows never reach the ramp's second limit, so pure batching would leave
/// all three sitting in the accumulator — the regression #1143 recorded and
/// #1592 re-learned. Here the FIRST row must be sendable before any further row
/// is pushed, and the tail must be flushed before the terminator.
#[test]
fn a_sub_batch_result_set_emits_its_first_row_immediately_and_loses_none() {
    let (tx, rx) = std::sync::mpsc::sync_channel(8);
    let local_sent = AtomicI64::new(0);
    let mut batcher = EgressBatcher::new(&tx, &local_sent);

    // ONE row pushed, and it is already on the channel: first-row latency is not
    // gated on a full batch.
    let _ = batcher.push(entry(0));
    let first = rx
        .try_recv()
        .expect("the first row must be sent immediately");
    let MergeMsg::Batch(batch) = first else {
        panic!("DATA batch expected")
    };
    assert_eq!(batch.len(), 1, "the ramp's first batch is one row");

    // Two more rows: still under the next limits, so they wait for the tail flush.
    let _ = batcher.push(entry(1));
    let _ = batcher.push(entry(2));
    let _ = batcher.flush();
    drop(tx);

    let mut rows = vec![batch[0].key.token];
    while let Ok(MergeMsg::Batch(batch)) = rx.recv() {
        rows.extend(batch.iter().map(|e| e.key.token));
    }
    assert_eq!(
        rows,
        vec![0, 1, 2],
        "a 3-row run must deliver all 3 rows, in order — the tail flush before the \
         terminator is what makes a sub-batch result set whole"
    );
}

/// A dropped consumer makes the next flush report `Break` (the producer's
/// stop signal), exactly as the pre-batching per-row `send` failure did.
#[test]
fn a_dropped_consumer_breaks_the_walk() {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let local_sent = AtomicI64::new(0);
    let mut batcher = EgressBatcher::new(&tx, &local_sent);
    drop(rx);
    assert!(
        matches!(batcher.push(entry(0)), ControlFlow::Break(())),
        "a send into a closed channel must Break so the walk stops"
    );
    assert_eq!(
        local_sent.load(Ordering::SeqCst),
        0,
        "a FAILED send must not be accounted as sent — that residual would leak the \
         shared egress-depth gauge upward permanently"
    );
}

/// The probe's counters move per BATCH and per ENTRY respectively, and the peak
/// tracks the largest batch — the observability the end-to-end oracle reads.
#[test]
fn the_probe_counts_messages_entries_and_the_peak_batch() {
    let before = merge_egress_batch_probe();
    const ROWS: usize = 600;
    let (tx, rx) = std::sync::mpsc::sync_channel(ROWS);
    let local_sent = AtomicI64::new(0);
    let mut batcher = EgressBatcher::new(&tx, &local_sent);
    for n in 0..ROWS {
        let _ = batcher.push(entry(n as i64));
    }
    let _ = batcher.flush();
    drop(tx);
    let drained: usize = std::iter::from_fn(|| rx.recv().ok())
        .map(|msg| msg.tracked_entries())
        .sum();
    assert_eq!(drained, ROWS);

    let after = merge_egress_batch_probe();
    // The counters are process-global and monotonic, so a concurrent merge in this
    // binary can only ADD to both deltas — never make a batched run look per-row.
    let messages = after.messages_sent - before.messages_sent;
    let entries = after.entries_sent - before.entries_sent;
    assert!(
        entries >= ROWS as u64,
        "entries_sent must count ROWS (got {entries} for {ROWS})"
    );
    assert!(
        messages >= before.expected_messages(ROWS as u64),
        "messages_sent must count BATCHES"
    );
    assert!(
        messages < entries,
        "600 rows must cost fewer than 600 messages — that ratio IS the fan-in \
         amortisation (messages={messages}, entries={entries})"
    );
    assert!(
        after.peak_batch_rows >= BATCH_EMIT_ROWS_MERGE,
        "a 600-row run saturates the ramp, so some batch must be full (peak={})",
        after.peak_batch_rows
    );
    assert!(
        after.peak_batch_rows <= BATCH_EMIT_ROWS_MERGE,
        "no batch may exceed the cap (peak={})",
        after.peak_batch_rows
    );
}
