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

/// A row carrying `payload` bytes of blob, the shape the `#827` merge memory
/// fixture writes (48 KiB per row) — the case a ROW-count bound cannot bound.
fn fat_entry(n: i64, payload: usize) -> MergeEntry {
    use crate::storage::write_engine::merge::model::CellData;
    use crate::types::Value;
    MergeEntry::new(
        0,
        DecoratedKey::new(n, n.to_be_bytes().to_vec()),
        None,
        100 + n,
        RowData::Live {
            cells: vec![CellData::new(
                "payload".to_string(),
                Value::Blob(vec![0x5a; payload].into()),
                100 + n,
            )],
        },
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
/// + producer-blocked-in-send, each up to one batch ceiling.
#[test]
fn the_resident_rows_bound_is_channel_plus_two_batches() {
    const DEFAULT: usize = super::super::STREAMING_CHANNEL_CAPACITY;
    let msg_cap = message_capacity_for_rows(DEFAULT);
    assert_eq!(msg_cap, 2, "default row budget -> 2 messages");
    assert_eq!(batch_limit_ceiling(DEFAULT), BATCH_EMIT_ROWS_MERGE);
    assert_eq!(
        max_inflight_rows(DEFAULT),
        (msg_cap + 2) * batch_limit_ceiling(DEFAULT)
    );
    // The worked example of record: 1024 rows = 4x the pre-#2820 flat 256, and
    // unchanged by the ceiling fix AT THE DEFAULT.
    assert_eq!(max_inflight_rows(DEFAULT), 1024);
    assert_eq!(max_inflight_rows(DEFAULT), 4 * DEFAULT);
    // The GAUGE's ceiling is the channel-resident half only — strictly below the
    // in-flight bound by the consumer-held + producer-parked batches. Documenting
    // the gauge with the wrong one of the two overstates it 2x.
    assert_eq!(
        rows_resident_in_channel(DEFAULT),
        msg_cap * BATCH_EMIT_ROWS_MERGE
    );
    assert_eq!(rows_resident_in_channel(DEFAULT), 2 * DEFAULT);
    assert!(rows_resident_in_channel(DEFAULT) < max_inflight_rows(DEFAULT));
}

/// THE regression this round exists for (issue #2820 review round 2, roborev job
/// 215): the resident-row bounds must SHRINK as `egress_budget`'s adaptive
/// per-channel row capacity shrinks.
///
/// Keyed on capacities the PRODUCTION budget can actually return
/// (`egress_budget::capacity_for(active_merges)`), never on a synthetic `msg_cap`
/// sweep. That distinction IS the defect: `message_capacity_for_rows` floors at
/// `MIN_MSG_CAP = 2` for every value in the reachable range `[MIN_CAP, MAX_CAP]`
/// (because `div_ceil(BATCH_EMIT_ROWS_MERGE)` is 1 there), so the previous
/// version of this pin — `for msg in 2..8` — asserted monotonicity over five
/// message capacities of which FOUR are unreachable in production. It read as
/// covering the shrunken-budget case while proving nothing about it, and the
/// bound it was guarding was in fact a CONSTANT 1024 at every real setting.
#[test]
fn resident_bounds_shrink_as_the_adaptive_row_capacity_shrinks() {
    use super::super::egress_budget;

    // Reachable capacities, taken from the production budget function itself.
    let caps: Vec<usize> = [1usize, 2, 4, 8, 16, 64, 256, 4096]
        .iter()
        .map(|&active| egress_budget::capacity_for(active))
        .collect();
    let solo = caps[0];
    let squeezed = *caps.last().expect("non-empty");
    assert_eq!(solo, super::super::STREAMING_CHANNEL_CAPACITY);
    assert!(
        squeezed < solo,
        "premise: the budget must actually shrink under concurrency \
         (solo={solo}, squeezed={squeezed}) — otherwise this test is vacuous"
    );

    // Every bound is a fixed MULTIPLE of the row capacity over this whole range:
    // 2x resident-in-channel, 4x in-flight. NOT a constant.
    for &rows_cap in &caps {
        assert_eq!(
            message_capacity_for_rows(rows_cap),
            MIN_MSG_CAP,
            "the premise of the defect: msg_cap is floored for every reachable \
             rows_cap ({rows_cap}), so the batch CEILING is what must scale"
        );
        assert_eq!(batch_limit_ceiling(rows_cap), rows_cap);
        assert_eq!(rows_resident_in_channel(rows_cap), 2 * rows_cap);
        assert_eq!(
            max_inflight_rows(rows_cap),
            4 * rows_cap,
            "in-flight rows must be 4x the ADAPTIVE capacity at every setting — a \
             constant here means egress_channel_capacity_for has no effect on \
             resident memory (rows_cap={rows_cap})"
        );
    }

    // And strictly monotone across the reachable range, in the direction that
    // matters: a smaller budget can only LOWER the bound.
    let mut sorted = caps.clone();
    sorted.sort_unstable();
    sorted.dedup();
    for pair in sorted.windows(2) {
        assert!(
            max_inflight_rows(pair[0]) < max_inflight_rows(pair[1]),
            "in-flight bound must rise with the row capacity ({} -> {})",
            pair[0],
            pair[1]
        );
        assert!(rows_resident_in_channel(pair[0]) < rows_resident_in_channel(pair[1]));
    }

    // The worked numbers of record at the floor: 8 rows/channel -> 32 in flight,
    // NOT the 1024 a capacity-independent ceiling produced (a 128x regression).
    assert_eq!(egress_budget::min_cap(), 8, "premise: the shipped floor");
    assert_eq!(max_inflight_rows(8), 32);
    assert_eq!(rows_resident_in_channel(8), 16);
}

/// The ramp (issue #2820 design item 7): the first flush is one row, the limit
/// doubles per flush, and it saturates at `BATCH_EMIT_ROWS_MERGE` — so a cold
/// producer parks in `send` holding FAR fewer rows than the saturated worst case,
/// which is why `rows_in_full_channel` exists alongside `max_inflight_rows`.
#[test]
fn the_ramp_doubles_from_one_row_and_saturates() {
    const DEFAULT: usize = super::super::STREAMING_CHANNEL_CAPACITY;
    let ceiling = batch_limit_ceiling(DEFAULT);
    let mut limit = FIRST_BATCH_EMIT_ROWS;
    let mut seen = vec![limit];
    for _ in 0..12 {
        limit = next_batch_limit(limit, ceiling);
        seen.push(limit);
    }
    assert_eq!(&seen[..5], &[1, 2, 4, 8, 16]);
    assert_eq!(
        *seen.last().expect("non-empty"),
        BATCH_EMIT_ROWS_MERGE,
        "the ramp must saturate at the full batch size, never grow past it"
    );
    // A THROTTLED run's ramp saturates at its own row capacity, not at the global
    // constant — the ceiling fix of review round 2.
    let mut low = FIRST_BATCH_EMIT_ROWS;
    for _ in 0..12 {
        low = next_batch_limit(low, batch_limit_ceiling(8));
    }
    assert_eq!(
        low, 8,
        "a rows_cap=8 run must never assemble a 256-row batch"
    );

    // Cold-start CHANNEL fill (the default channel is 2 messages, so 1 + 2).
    assert_eq!(rows_in_full_channel(DEFAULT), 3, "ramp 1 + 2 over 2 slots");
    assert_eq!(
        rows_in_full_channel(BATCH_EMIT_ROWS_MERGE * 4),
        15,
        "1 + 2 + 4 + 8 over a 4-message channel"
    );
    // Strictly below the saturated bound for every reachable capacity: a fixture
    // that derived "the producer is blocked" from `max_inflight_rows` would wait
    // for rows that, from a cold start, can never be sent.
    for rows_cap in [8usize, 16, 64, 128, 256] {
        assert!(
            rows_in_full_channel(rows_cap) < max_inflight_rows(rows_cap),
            "cold-start fill must be below the saturated bound at rows_cap {rows_cap}"
        );
        // And the PARKING threshold adds the batch the producer still owns — the
        // term every backpressure fixture writes by hand, now derivable.
        let probe = merge_egress_batch_probe();
        assert_eq!(
            probe.rows_that_park_the_producer(rows_cap),
            rows_in_full_channel(rows_cap) + batch_limit_ceiling(rows_cap) + 1,
            "the parking threshold is the channel fill PLUS one in-flight batch"
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
        let mut batcher =
            EgressBatcher::new(&tx, &local_sent, super::super::STREAMING_CHANNEL_CAPACITY);
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
    let mut batcher =
        EgressBatcher::new(&tx, &local_sent, super::super::STREAMING_CHANNEL_CAPACITY);

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
    let mut batcher =
        EgressBatcher::new(&tx, &local_sent, super::super::STREAMING_CHANNEL_CAPACITY);
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
    let mut batcher =
        EgressBatcher::new(&tx, &local_sent, super::super::STREAMING_CHANNEL_CAPACITY);
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

/// THE byte bound (issue #2820 review round 2, BLOCKER 2): a ROW count is not a
/// memory bound, so the accumulator flushes on the BYTE budget when rows are
/// large — and this pin RUNS in a normal lane (`--lite`'s scoped tests, the
/// gate's `write-tests`), unlike the `dhat-heap`-gated `#827` fixture, which no
/// gate component executes.
///
/// The property is stated in the direction that fails on a regression: with
/// 48 KiB rows (the `#827` fixture's shape) the byte budget must trip FIRST, so
/// no batch may reach the 256-row ceiling and the per-batch payload stays near
/// `BATCH_EMIT_BYTES_MERGE` instead of `256 × 48 KiB ≈ 12 MiB`.
#[test]
fn the_byte_budget_flushes_large_rows_long_before_the_row_ceiling() {
    const PAYLOAD: usize = 48 * 1024;
    const ROWS: usize = 200;
    let probe = merge_egress_batch_probe();
    assert_eq!(probe.batch_emit_bytes, BATCH_EMIT_BYTES_MERGE);

    // The row bound ALONE would allow this many 48 KiB rows per batch.
    let rows_per_batch_if_row_bounded =
        batch_limit_ceiling(super::super::STREAMING_CHANNEL_CAPACITY);
    assert_eq!(rows_per_batch_if_row_bounded, 256);

    let (tx, rx) = std::sync::mpsc::sync_channel(ROWS + 4);
    let local_sent = AtomicI64::new(0);
    let mut batcher =
        EgressBatcher::new(&tx, &local_sent, super::super::STREAMING_CHANNEL_CAPACITY);
    for n in 0..ROWS {
        assert!(matches!(
            batcher.push(fat_entry(n as i64, PAYLOAD)),
            ControlFlow::Continue(())
        ));
    }
    let _ = batcher.flush();
    drop(tx);

    let mut biggest_rows = 0usize;
    let mut biggest_bytes = 0usize;
    let mut total_rows = 0usize;
    let mut messages = 0usize;
    while let Ok(msg) = rx.recv() {
        let MergeMsg::Batch(batch) = msg else {
            panic!("the batcher sends only DATA batches");
        };
        let bytes: usize = batch
            .iter()
            .map(super::super::RunReader::estimate_entry_size)
            .sum();
        biggest_rows = biggest_rows.max(batch.len());
        biggest_bytes = biggest_bytes.max(bytes);
        total_rows += batch.len();
        messages += 1;
    }

    assert_eq!(total_rows, ROWS, "no row may be dropped by the byte flush");
    assert!(
        biggest_rows < rows_per_batch_if_row_bounded,
        "the BYTE budget must trip first for {PAYLOAD}-byte rows: biggest batch \
         {biggest_rows} rows vs the row ceiling {rows_per_batch_if_row_bounded}"
    );
    // The honest bound: the threshold is checked AFTER the push, so a batch may
    // reach the budget plus ONE entry — never a multiple of it.
    let one_row = super::super::RunReader::estimate_entry_size(&fat_entry(0, PAYLOAD));
    assert!(
        biggest_bytes <= BATCH_EMIT_BYTES_MERGE + one_row,
        "a batch ({biggest_bytes} B) may exceed the budget by at most one row \
         ({one_row} B), never by a multiple"
    );
    // Non-vacuity: the run really did produce multi-row batches (a per-row send
    // would satisfy every bound above while losing the whole optimisation).
    assert!(
        messages < ROWS,
        "the byte bound must still BATCH: {messages} messages for {ROWS} rows"
    );
    assert!(
        biggest_rows > 1,
        "some batch must carry more than one 48 KiB row (biggest={biggest_rows})"
    );
    // And the sizing statement the module doc makes: in-flight BYTES, not rows,
    // are what bound memory for this shape.
    let in_flight = probe.max_inflight_bytes(super::super::STREAMING_CHANNEL_CAPACITY, one_row);
    assert!(
        in_flight < 8 * 1024 * 1024,
        "per-source in-flight bytes must stay a low-single-digit-MB term \
         (got {in_flight}); the row bound alone would allow \
         {} B",
        max_inflight_rows(super::super::STREAMING_CHANNEL_CAPACITY) * one_row
    );
    assert!(
        max_inflight_rows(super::super::STREAMING_CHANNEL_CAPACITY) * one_row > 32 * 1024 * 1024,
        "premise: the ROW bound alone really is tens of MB per source for this \
         row size — otherwise this test proves nothing"
    );
}

/// A THROTTLED run assembles smaller batches, end to end through the real
/// accumulator (issue #2820 review round 2): the `rows_cap` the constructor takes
/// must actually bound the batch, not merely be recorded.
#[test]
fn a_throttled_row_capacity_caps_the_real_batch_size() {
    const ROWS_CAP: usize = 8;
    const ROWS: usize = 200;
    let (tx, rx) = std::sync::mpsc::sync_channel(ROWS + 4);
    let local_sent = AtomicI64::new(0);
    let mut batcher = EgressBatcher::new(&tx, &local_sent, ROWS_CAP);
    for n in 0..ROWS {
        let _ = batcher.push(entry(n as i64));
    }
    let _ = batcher.flush();
    drop(tx);

    let mut biggest = 0usize;
    let mut total = 0usize;
    let mut tokens = Vec::new();
    while let Ok(MergeMsg::Batch(batch)) = rx.recv() {
        biggest = biggest.max(batch.len());
        total += batch.len();
        tokens.extend(batch.iter().map(|e| e.key.token));
    }
    assert_eq!(total, ROWS, "throttling must not drop rows");
    assert_eq!(
        tokens,
        (0..ROWS as i64).collect::<Vec<_>>(),
        "throttling must not reorder rows"
    );
    assert_eq!(
        biggest, ROWS_CAP,
        "a rows_cap={ROWS_CAP} run must saturate at {ROWS_CAP}-row batches, not at \
         BATCH_EMIT_ROWS_MERGE ({BATCH_EMIT_ROWS_MERGE}) — that constant ceiling IS \
         the defect roborev job 215 found"
    );
    let probe = merge_egress_batch_probe();
    assert_eq!(
        probe.expected_messages_at(ROWS_CAP, ROWS as u64),
        ROWS as u64 / ROWS_CAP as u64 + 3,
        "the ramp at a throttled ceiling: 1+2+4 then 8s (oracle sanity)"
    );
}

// ---------------------------------------------------------------------------
// Issue #2820, roborev round 2 (FINDING 1): the byte budget must bound NESTED
// values too.
//
// The estimator these batches are measured with used to end in
// `_ => 32, // Default estimate for complex types`, so a `List`/`Map`/`Set`/
// `Tuple`/`Udt`/`Frozen`/`Json` row of ANY size counted as ~32 bytes: 256 large
// nested rows summed to a few KiB, never reached the 1 MiB budget, and the batch
// ran to the ROW ceiling instead. Every assert below that the batch stayed under
// the row ceiling is therefore RED under the old flat-32 estimate and GREEN with
// the exhaustive one — the same control shape as the 775-vs-11 send-count pin.
//
// Deliberately NOT covered by the pre-existing 48 KiB-`Blob` byte-bound test:
// a flat `Blob` was sized correctly all along, which is exactly why the flat
// fixture (and the dhat measurement built on it) could not see this.
// ---------------------------------------------------------------------------

/// ~`payload` bytes of nested value under one cell, in each of the shapes the
/// flat-32 wildcard used to swallow.
#[derive(Clone, Copy, Debug)]
enum NestedShape {
    List,
    Map,
    Udt,
    FrozenList,
}

/// One cell whose value carries `chunks × chunk` bytes nested inside `shape`.
fn nested_value(shape: NestedShape, chunks: usize, chunk: usize) -> crate::types::Value {
    use crate::types::{UdtField, UdtValue, Value};
    let blob = || Value::Blob(vec![0x5a; chunk].into());
    match shape {
        NestedShape::List => Value::List((0..chunks).map(|_| blob()).collect()),
        NestedShape::Map => Value::Map(
            (0..chunks)
                .map(|i| (Value::text(format!("k{i}")), blob()))
                .collect(),
        ),
        NestedShape::Udt => Value::Udt(Box::new(UdtValue {
            type_name: "big".to_string(),
            keyspace: "ks".to_string(),
            fields: (0..chunks)
                .map(|i| UdtField {
                    name: format!("f{i}"),
                    value: Some(blob()),
                })
                .collect(),
        })),
        NestedShape::FrozenList => {
            Value::Frozen(Box::new(Value::List((0..chunks).map(|_| blob()).collect())))
        }
    }
}

fn nested_entry(n: i64, shape: NestedShape, chunks: usize, chunk: usize) -> MergeEntry {
    use crate::storage::write_engine::merge::model::CellData;
    MergeEntry::new(
        0,
        DecoratedKey::new(n, n.to_be_bytes().to_vec()),
        None,
        100 + n,
        RowData::Live {
            cells: vec![CellData::new(
                "nested".to_string(),
                nested_value(shape, chunks, chunk),
                100 + n,
            )],
        },
    )
}

/// The estimate for a nested row must track the bytes it actually carries.
///
/// The tight, direct form of the finding: under the flat-32 wildcard every one
/// of these rows estimated at a couple of hundred bytes regardless of payload.
#[test]
fn a_nested_row_is_estimated_at_the_bytes_it_carries() {
    const CHUNKS: usize = 16;
    const CHUNK: usize = 4096;
    const CARRIED: usize = CHUNKS * CHUNK; // 64 KiB

    for shape in [
        NestedShape::List,
        NestedShape::Map,
        NestedShape::Udt,
        NestedShape::FrozenList,
    ] {
        let size =
            super::super::RunReader::estimate_entry_size(&nested_entry(0, shape, CHUNKS, CHUNK));
        assert!(
            size >= CARRIED,
            "{shape:?}: a row carrying {CARRIED} B of nested blob estimated at \
             only {size} B — the byte budget is bypassable for this shape"
        );
        // Sanity in the other direction: the estimate is an approximation, not a
        // multiple — container overhead must not dominate the payload.
        assert!(
            size < 4 * CARRIED,
            "{shape:?}: estimate {size} B is more than 4x the {CARRIED} B carried; \
             container overhead has swamped the payload"
        );
    }
}

/// A batch of large NESTED rows must flush on BYTES, not run to the row ceiling.
///
/// RED under the flat-32 estimate: 512 rows × ~200 estimated bytes never reaches
/// 1 MiB, so `biggest_rows` was the full 256-row ceiling for every shape.
#[test]
fn the_byte_bound_trips_for_large_nested_rows_not_the_row_ceiling() {
    const ROWS: usize = 512;
    const CHUNKS: usize = 16;
    const CHUNK: usize = 4096; // 64 KiB carried per row
    let row_ceiling = batch_limit_ceiling(super::super::STREAMING_CHANNEL_CAPACITY);
    assert_eq!(row_ceiling, 256, "premise: the ROW ceiling is 256");

    for shape in [
        NestedShape::List,
        NestedShape::Map,
        NestedShape::Udt,
        NestedShape::FrozenList,
    ] {
        // Capacity ROWS + 4: this test measures the BATCHER's flush decisions, so
        // it must never block on the channel.
        let (tx, rx) = std::sync::mpsc::sync_channel(ROWS + 4);
        let local_sent = AtomicI64::new(0);
        let mut batcher =
            EgressBatcher::new(&tx, &local_sent, super::super::STREAMING_CHANNEL_CAPACITY);
        for n in 0..ROWS {
            assert!(matches!(
                batcher.push(nested_entry(n as i64, shape, CHUNKS, CHUNK)),
                ControlFlow::Continue(())
            ));
        }
        let _ = batcher.flush();
        drop(tx);

        let mut biggest_rows = 0usize;
        let mut biggest_bytes = 0usize;
        let mut total_rows = 0usize;
        let mut messages = 0usize;
        while let Ok(msg) = rx.recv() {
            let MergeMsg::Batch(batch) = msg else {
                panic!("the batcher sends only DATA batches");
            };
            let bytes: usize = batch
                .iter()
                .map(super::super::RunReader::estimate_entry_size)
                .sum();
            biggest_rows = biggest_rows.max(batch.len());
            biggest_bytes = biggest_bytes.max(bytes);
            total_rows += batch.len();
            messages += 1;
        }

        assert_eq!(
            total_rows, ROWS,
            "{shape:?}: no row may be dropped by the byte flush"
        );
        // THE guard. Under `_ => 32` this is exactly `256 < 256` and FAILS.
        assert!(
            biggest_rows < row_ceiling,
            "{shape:?}: the BYTE budget must trip before the {row_ceiling}-row \
             ceiling for 64 KiB nested rows (biggest batch {biggest_rows} rows)"
        );
        let one_row =
            super::super::RunReader::estimate_entry_size(&nested_entry(0, shape, CHUNKS, CHUNK));
        assert!(
            biggest_bytes <= BATCH_EMIT_BYTES_MERGE + one_row,
            "{shape:?}: a batch ({biggest_bytes} B) may exceed the budget by at \
             most one row ({one_row} B), never by a multiple"
        );
        // Non-vacuity: it must still BATCH (a per-row send satisfies every bound
        // above while losing the whole optimisation).
        assert!(
            messages < ROWS,
            "{shape:?}: the byte bound must still batch — {messages} messages for \
             {ROWS} rows"
        );
        assert!(
            biggest_rows > 1,
            "{shape:?}: some batch must carry more than one 64 KiB nested row \
             (biggest={biggest_rows})"
        );
    }
}

/// A pathologically WIDE value fails CLOSED (`usize::MAX`), never permissively.
///
/// The node budget bounds worst-case work; when it is exhausted the size is
/// unknown, and an unknown size must take the FAIL-CLOSED branch (an immediate
/// flush) rather than a small guess. The guard is checked BEFORE enqueuing, so
/// the traversal never grows a worklist proportional to the element count.
#[test]
fn a_value_past_the_node_budget_fails_closed_rather_than_undercounting() {
    use crate::storage::write_engine::merge::model::CellData;
    use crate::types::Value;
    let cap = super::super::entry_size::MAX_ESTIMATE_NODES;
    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(0, vec![0]),
        None,
        100,
        RowData::Live {
            cells: vec![CellData::new(
                "wide".to_string(),
                Value::List(vec![Value::Null; cap + 1]),
                100,
            )],
        },
    );
    assert_eq!(
        super::super::RunReader::estimate_entry_size(&entry),
        usize::MAX,
        "a value past the {cap}-node budget must fail CLOSED so the batcher \
         flushes immediately and read-ahead stops"
    );
}

// ---------------------------------------------------------------------------
// Issue #2820, roborev round 4 (FINDING 2) — and the CLASS behind it.
//
// The class: *a container's ELEMENT ALLOCATION is not counted, only its header
// and its element PAYLOADS.* This is the THIRD instance found in
// `estimate_entry_size` (after the `_ => 32` wildcard and `range_deletion`'s
// uncounted bounds), so the tests below pin the two shapes that maximise it:
//
//   * a WIDE SCALAR clustering key — `Vec<(String, Value)>`, whose element slot
//     is `size_of::<(String, Value)>()` = 24 + `size_of::<Value>()` bytes, i.e.
//     ~8x the 8-byte payload of a `BigInt` component. `add_clustering_key`
//     counted the 24-byte Vec HEADER plus each name/value payload and never the
//     array those elements live in, so a row with many small clustering
//     components underestimated by roughly the whole array.
//   * a range-`Tombstone` VALUE whose `RowKey` bounds (`Arc<[u8]>`) carry
//     unbounded bytes the flat `size_of::<TombstoneInfo>() + 16` arm ignored.
//
// Both bypass BOTH consumers of this estimate (the 1 MiB batch budget and
// `RunReader::refill_buffer`'s read-ahead limit), which is why they are pinned
// against the estimator directly rather than only through a batch.
// ---------------------------------------------------------------------------

/// Bytes of `Vec<(String, Value)>` element array for `n` clustering components.
#[cfg(test)]
fn clustering_element_array(n: usize) -> usize {
    use crate::types::Value;
    n * std::mem::size_of::<(String, Value)>()
}

/// An entry whose clustering key has `components` small SCALAR columns.
fn wide_clustering_entry(n: i64, components: usize) -> MergeEntry {
    use crate::storage::write_engine::mutation::ClusteringKey;
    use crate::types::Value;
    MergeEntry::new(
        0,
        DecoratedKey::new(n, n.to_be_bytes().to_vec()),
        Some(ClusteringKey::new(
            (0..components)
                .map(|i| (format!("c{i}"), Value::BigInt(i as i64)))
                .collect(),
        )),
        100 + n,
        RowData::Live { cells: vec![] },
    )
}

/// A wide SCALAR clustering key is estimated at the array it allocates.
///
/// RED before the fix: 64 components estimated at ~1 KiB (header + 64 × (name +
/// 8 B payload)) against a 64 × `size_of::<(String, Value)>()` = 4 KiB element
/// array — the array itself was invisible to both budgets.
#[test]
fn a_wide_scalar_clustering_key_is_estimated_at_the_array_it_allocates() {
    const COMPONENTS: usize = 64;
    let array = clustering_element_array(COMPONENTS);
    let size = super::super::RunReader::estimate_entry_size(&wide_clustering_entry(0, COMPONENTS));
    assert!(
        size >= std::mem::size_of::<MergeEntry>() + array,
        "a {COMPONENTS}-component clustering key allocates a {array} B element \
         array; estimated at only {size} B — both the batch byte budget and \
         read-ahead are bypassable for wide scalar clustering keys"
    );
}

/// ...and the estimate must GROW with the array, not just clear one threshold.
///
/// Constant-independent form of the same finding: widening the key by `D`
/// components must add at least `D` element slots. Under the old arithmetic it
/// added only `D × (name.len() + 8)`.
#[test]
fn widening_a_scalar_clustering_key_grows_the_estimate_by_its_element_slots() {
    const NARROW: usize = 8;
    const WIDE: usize = 64;
    let narrow = super::super::RunReader::estimate_entry_size(&wide_clustering_entry(0, NARROW));
    let wide = super::super::RunReader::estimate_entry_size(&wide_clustering_entry(0, WIDE));
    let delta = clustering_element_array(WIDE) - clustering_element_array(NARROW);
    assert!(
        wide - narrow >= delta,
        "widening {NARROW}→{WIDE} components adds a {delta} B element array, but \
         the estimate grew only {} B ({narrow} → {wide})",
        wide - narrow
    );
}

/// A batch of wide-clustering-key rows must flush on BYTES, not the row ceiling.
///
/// The wiring half: the undercount is only a defect because these two budgets
/// consume it. `COMPONENTS` is chosen so a 256-row batch of these rows stays
/// UNDER 1 MiB at the OLD arithmetic (~3.3 KiB/row: header + name + 8 B payload)
/// and well over it once the ~16 KiB/row element array is counted — so this
/// assert is RED before the fix and GREEN after, rather than tripping either way.
#[test]
fn the_byte_bound_trips_for_wide_clustering_keys_not_the_row_ceiling() {
    const ROWS: usize = 512;
    const COMPONENTS: usize = 250;
    let row_ceiling = batch_limit_ceiling(super::super::STREAMING_CHANNEL_CAPACITY);
    let (tx, rx) = std::sync::mpsc::sync_channel(ROWS + 4);
    let local_sent = AtomicI64::new(0);
    let mut batcher =
        EgressBatcher::new(&tx, &local_sent, super::super::STREAMING_CHANNEL_CAPACITY);
    for n in 0..ROWS {
        assert!(matches!(
            batcher.push(wide_clustering_entry(n as i64, COMPONENTS)),
            ControlFlow::Continue(())
        ));
    }
    let _ = batcher.flush();
    drop(tx);

    let mut biggest_rows = 0usize;
    let mut total_rows = 0usize;
    while let Ok(MergeMsg::Batch(batch)) = rx.recv() {
        biggest_rows = biggest_rows.max(batch.len());
        total_rows += batch.len();
    }
    assert_eq!(total_rows, ROWS, "no row may be dropped by the byte flush");
    assert!(
        biggest_rows < row_ceiling,
        "the BYTE budget must trip before the {row_ceiling}-row ceiling for rows \
         carrying a {COMPONENTS}-component clustering key (biggest batch \
         {biggest_rows} rows)"
    );
}

/// A range-`Tombstone` value's `RowKey` bounds are counted, not flat-16'd.
///
/// RED before the fix: `Value::Tombstone` added `size_of::<TombstoneInfo>() + 16`
/// however many bytes its `range_start`/`range_end` `Arc<[u8]>` carried.
#[test]
fn a_tombstone_values_range_bounds_are_counted() {
    use crate::storage::write_engine::merge::model::CellData;
    use crate::types::{RowKey, TombstoneInfo, TombstoneType, Value};
    const BOUND: usize = 32 * 1024;
    let tombstone = Value::Tombstone(Box::new(TombstoneInfo {
        deletion_time: 100,
        tombstone_type: TombstoneType::RangeTombstone,
        local_deletion_time: 1,
        ttl: None,
        range_start: Some(RowKey::new(vec![0x11; BOUND])),
        range_end: Some(RowKey::new(vec![0x22; BOUND])),
    }));
    let entry = MergeEntry::new(
        0,
        DecoratedKey::new(0, vec![0]),
        None,
        100,
        RowData::Live {
            cells: vec![CellData::new("rt".to_string(), tombstone, 100)],
        },
    );
    let size = super::super::RunReader::estimate_entry_size(&entry);
    assert!(
        size >= 2 * BOUND,
        "a tombstone carrying {} B of range-bound bytes estimated at only \
         {size} B — the bound bytes are invisible to both budgets",
        2 * BOUND
    );
}
