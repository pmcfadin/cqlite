//! Issue #2419 (WS2), C-audit Req 3 — the `cqlite.merge.egress_channel_depth`
//! gauge.
//!
//! Sibling of `issue_2316_producer_gauge.rs` (same capture infra, same
//! backed-up-merge fixture shape). The unit test in
//! `storage::write_engine::merge::channel_depth` pins the depth arithmetic
//! against a PRIVATE atomic via `adjust()` (deliberately, to stay immune to the
//! #2451 flake class — see that module's tests) — but that means nothing
//! public-surface would fail if the PRODUCTION `channel_depth::sent_n()` /
//! `received_n()` call sites (`egress_batch::EgressBatcher::flush`,
//! `SSTableRowIteratorAdapter::next`) were ever unwired, since the private-atomic
//! pin never touches them. This test closes that gap: it drives a REAL k-way
//! merge whose per-input channels back up past capacity (so `sent()` must fire
//! to observe a positive reading), then drains it to completion (so
//! `received()` must fire to observe the return to baseline) — all through the
//! actual OTel capture surface, never a private/injected atomic.
//!
//! Runs only under `observability-testing` (the in-memory metric capture fixture):
//!   cargo test -p cqlite-core --features observability-testing \
//!     --test issue_2419_egress_depth_gauge

#![cfg(all(feature = "write-support", feature = "observability-testing"))]

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use cqlite_core::observability::{catalog, testing};
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::writer::SSTableWriter;
use cqlite_core::storage::write_engine::merge::compute_baseline_min;
use cqlite_core::storage::write_engine::{
    CellOperation, KWayMerger, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use tempfile::TempDir;

/// Rows per input SSTable, DERIVED from the shipped batching constants (issue
/// #2820) rather than a literal: everything a full egress channel holds from a
/// cold start (`rows_in_full_channel` — the ramp means the first batches are not
/// full ones), plus the full batch the producer then blocks trying to hand over,
/// plus one row it cannot even accumulate. That is the smallest fixture for which
/// every producer is GUARANTEED to be parked in `send` with nothing received.
///
/// A literal would silently rot: pre-#2820 "> 256" meant "past a 256-ENTRY
/// channel", and the channel is now bounded in MESSAGES. The historical 400-row
/// floor is kept so the fixture also stays a genuinely multi-partition scan.
fn rows_per_input() -> i32 {
    let probe = cqlite_core::storage::write_engine::merge::merge_egress_batch_probe();
    let rows_cap = cqlite_core::storage::write_engine::merge::egress_channel_capacity_for(
        cqlite_core::storage::write_engine::merge::active_merge_count() + 1,
    );
    let needed = probe.rows_in_full_channel(rows_cap) + probe.batch_emit_rows + 1;
    needed.max(400) as i32
}
const NUM_INPUTS: usize = 4;

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: "egress_ks".to_string(),
        table: "items".to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "val".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn write_row(id: i32, val: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new("egress_ks", "items"),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "val".to_string(),
            value: Value::text(val.to_string()),
        }],
        ts,
        None,
    )
}

fn collect_inputs(dir: &std::path::Path, out: &mut Vec<(u64, PathBuf)>, depth: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if name.starts_with("nb-") && name.ends_with("-big-Data.db") {
            let base = name.trim_end_matches("-Data.db");
            if !path.with_file_name(format!("{base}-TOC.txt")).exists() {
                continue;
            }
            let generation = name
                .strip_prefix("nb-")
                .and_then(|s| s.split("-big-").next())
                .and_then(|g| g.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((generation, path));
        } else if depth > 0 && path.is_dir() {
            collect_inputs(&path, out, depth - 1);
        }
    }
}

/// Build `NUM_INPUTS` REAL nb SSTables (each `rows_per_input()` live rows over a
/// disjoint partition range). Never empty.
fn build_inputs() -> (TempDir, Vec<PathBuf>, TableSchema) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("driver runtime");
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("inputs");
    let wal_dir = temp.path().join("wal");
    let schema = make_schema();

    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");
    for input in 0..NUM_INPUTS {
        let base = input as i32 * rows_per_input();
        for r in 0..rows_per_input() {
            engine
                .write(write_row(
                    base + r,
                    &format!("v-{input}-{r}"),
                    100 + input as i64,
                ))
                .expect("write row");
        }
        rt.block_on(engine.flush())
            .expect("flush")
            .expect("flush info");
    }
    rt.block_on(engine.close()).expect("close engine");

    let mut found = Vec::new();
    collect_inputs(&data_dir, &mut found, 8);
    found.sort_by_key(|b| std::cmp::Reverse(b.0));
    let inputs: Vec<PathBuf> = found.into_iter().map(|(_, p)| p).collect();
    assert!(
        inputs.len() >= NUM_INPUTS,
        "expected >= {NUM_INPUTS} real inputs, got {}",
        inputs.len()
    );
    drop(rt);
    (temp, inputs, schema)
}

/// Sum every accumulated `cqlite.merge.egress_channel_depth` data point in one
/// captured snapshot (there may be several from repeated `flush_and_collect`
/// calls without an intervening `reset` — see the polling loops below).
fn depth_points(snap: &testing::CapturedMetrics) -> Vec<f64> {
    snap.entries()
        .iter()
        .filter(|e| e.name == catalog::MERGE_EGRESS_CHANNEL_DEPTH)
        .flat_map(|e| e.points.iter().map(|p| p.value))
        .collect()
}

/// The egress-depth gauge RISES to reflect real bounded-channel backpressure
/// while a k-way merge holds M producers backed up past capacity, and RETURNS
/// to baseline once the merge is drained to completion — driven ENTIRELY
/// through the production `channel_depth::sent_n()` / `received_n()` call sites
/// (issue #2419 C-audit Req 3): a lower-bound/positive-observation style on the
/// GAUGE throughout (never an exact-equality assertion against the shared
/// global), so this stays immune to the #2451 flake class even though the gauge
/// is a process-wide atomic.
///
/// Issue #2820 adds exact-equality assertions on the FAN-IN PROBE deltas
/// (`merge_egress_batch_probe`) — the entries-vs-messages discriminator for the
/// gauge's declared `{entry}` unit. Those are safe for the SAME reason the
/// adaptive-capacity derivation below is exact and NOT for a weaker one: this
/// file holds ONE test, so no concurrent merge in this binary can contribute to
/// the delta, and the counters are monotonic (a hypothetical concurrent merge
/// could only ADD to both sides, which a strict equality catches loudly rather
/// than passing vacuously). The GAUGE assertions stay threshold-based.
#[test]
fn egress_depth_gauge_rises_and_returns_to_baseline() {
    let capture = testing::metrics_capture();
    capture.reset();

    let (_temp, inputs, schema) = build_inputs();
    let (baseline_ts, baseline_ldt, baseline_ttl) = compute_baseline_min(&inputs);
    let out = TempDir::new().expect("out tempdir");

    // Construct the merger WITHOUT stepping: `KWayMerger::new` does not seed its
    // heap ("populated on first step"), so every producer races ahead filling
    // its OWN egress channel and blocks on `send` once full — none received yet.
    // Issue #2765: that channel's capacity is now ADAPTIVE — up to 256, but
    // `clamp(EGRESS_ROW_BUDGET / active_merges, MIN_CAP, 256)` under concurrent
    // merges — so the theoretical ceiling is `NUM_INPUTS * per_channel_cap`, NOT
    // a hard-coded `NUM_INPUTS * 256`. The threshold below is derived from the
    // LIVE adaptive capacity (a conservative fraction of that ceiling, never an
    // exact target) so a future concurrent merge in this binary shrinks the cap
    // WITHOUT pushing the threshold out of reach (the pre-#2765 fixed-256
    // assumption would 10s-timeout in that case).
    // Sample the live active-merge count BEFORE and AFTER construction and derive
    // the per-channel capacity from the HIGHER count (→ the LOWER capacity).
    // `max(before+1, after)` (this merger counts itself via the `+1`) is a safe
    // lower bound on the true snapshot GIVEN this single-test binary has no
    // ambient merges, and stays conservative under monotonically-RISING
    // concurrency. (It is NOT a universal bound: an ambient merge that both
    // starts AND finishes between the two reads would make the true snapshot
    // `before+2` while this derives `before+1` → an over-estimate. That cannot
    // happen here — one test per binary — so the derivation is exact.)
    let before = cqlite_core::storage::write_engine::merge::active_merge_count();
    // Issue #2820: the fan-in probe baseline, read before any producer starts.
    let probe_before = cqlite_core::storage::write_engine::merge::merge_egress_batch_probe();
    let merger = KWayMerger::new(inputs, &schema).expect("KWayMerger::new");
    let after = cqlite_core::storage::write_engine::merge::active_merge_count();
    let per_channel_cap = cqlite_core::storage::write_engine::merge::egress_channel_capacity_for(
        (before + 1).max(after),
    );
    // Issue #2820: the channel carries BATCHES, and the batch-size ramp starts at
    // ONE row — so a producer whose consumer never steps parks in `send` holding
    // `rows_in_full_channel(per_channel_cap)` rows, NOT `per_channel_cap` rows.
    // Deriving the threshold from the ROW capacity (as this did pre-#2820) would
    // wait for rows the producer structurally cannot send from a cold start, and
    // deadline out. The gauge's UNIT is unchanged (entries) — only how many
    // entries a full channel holds changed.
    let rows_when_backed_up = cqlite_core::storage::write_engine::merge::merge_egress_batch_probe()
        .rows_in_full_channel(per_channel_cap);
    assert!(
        rows_when_backed_up > 0,
        "a full channel must hold at least one row, else this test is vacuous"
    );

    // MID-MERGE: poll (bounded, fail-loud) until the gauge POSITIVELY records a
    // reading proving multiple channels are genuinely backed up concurrently —
    // never inferred from an absent/stale window. If `channel_depth::sent_n()`
    // were removed from `EgressBatcher::flush`, this loop would exhaust its deadline and
    // fail explicitly. Half the adaptive ceiling (`NUM_INPUTS * cap`) requires
    // more than one full channel's worth (for NUM_INPUTS >= 2), so it still
    // proves CONCURRENT multi-channel backpressure, adaptively.
    // The FULL steady state, not half of it (issue #2820): nothing is received
    // until the merge is stepped, so every one of the `NUM_INPUTS` producers parks
    // in `send` holding exactly `rows_when_backed_up` rows — a deterministic level,
    // not a probabilistic peak. Requiring the whole figure is what makes this poll
    // DISCRIMINATE the gauge's UNIT: if either accounting site ever counted
    // MESSAGES instead of ENTRIES the gauge would top out at
    // `NUM_INPUTS * message_capacity` (8 at the shipped defaults, vs 12 entries),
    // this loop would exhaust its deadline, and the assertion below would name the
    // unit as the cause.
    let backed_up_entries = NUM_INPUTS * rows_when_backed_up;
    let backpressure_threshold = backed_up_entries as f64;
    let mid_deadline = Instant::now() + Duration::from_secs(10);
    let mut mid_reached = false;
    let mut mid_last_seen: Option<f64> = None;
    let mut mid_unit: Option<String> = None;
    while Instant::now() < mid_deadline {
        let snap = capture.flush_and_collect();
        for entry in snap
            .entries()
            .iter()
            .filter(|e| e.name == catalog::MERGE_EGRESS_CHANNEL_DEPTH)
        {
            mid_unit = Some(entry.unit.clone());
        }
        for v in depth_points(&snap) {
            mid_last_seen = Some(v);
            if v >= backpressure_threshold {
                mid_reached = true;
            }
        }
        if mid_reached {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        mid_reached,
        "gauge should POSITIVELY record a backed-up reading >= {backpressure_threshold} \
         while {NUM_INPUTS} producers are blocked on a full channel (never inferred \
         from an absent/stale window); last observed value: {:?}. Since issue #2820 \
         the channel carries BATCHES: a reading stuck near \
         NUM_INPUTS * message_capacity instead means an accounting site is counting \
         MESSAGES, while this gauge's unit is ENTRIES (see channel_depth's module \
         doc — an asymmetry there drives the reconcile residual negative, where the \
         `> 0` guard and `max(0)` floor hide it forever)",
        mid_last_seen
    );
    assert_eq!(
        mid_unit.as_deref(),
        Some(catalog::unit::ENTRIES),
        "gauge must carry the {{entry}} unit"
    );

    // Issue #2820, the other half of that unit claim: the gauge's DECLARED unit is
    // `{entry}`, so its VALUE must track entries. Read the fan-in probe at the same
    // backed-up steady state and pin both figures explicitly, so the two can never
    // be quietly swapped: the producers sent `backed_up_entries` ENTRIES in
    // `NUM_INPUTS * message_capacity` MESSAGES, and the reading above matched the
    // former.
    let batched = cqlite_core::storage::write_engine::merge::merge_egress_batch_probe();
    let entries_sent = batched.entries_sent - probe_before.entries_sent;
    let messages_sent = batched.messages_sent - probe_before.messages_sent;
    assert_eq!(
        entries_sent, backed_up_entries as u64,
        "each of the {NUM_INPUTS} parked producers must have sent exactly \
         {rows_when_backed_up} entries (a full channel from a cold start)"
    );
    assert_eq!(
        messages_sent,
        (NUM_INPUTS * batched.message_capacity_for_rows(per_channel_cap)) as u64,
        "…in exactly one message per BATCH — which is strictly fewer than the \
         entries above, and is why the gauge value must not be derived from it"
    );
    assert!(
        messages_sent < entries_sent,
        "the fixture must produce multi-row batches, or 'the gauge counts entries, \
         not messages' is vacuous (entries={entries_sent}, messages={messages_sent})"
    );

    // Reset HERE — BEFORE the drain — so the fresh delta window opens strictly
    // before any entry is received (mirrors `issue_2316_producer_gauge.rs`'s
    // identical ordering rationale).
    capture.reset();

    // Drain the merge to completion: every producer's remaining entries are
    // received (channel_depth::received_n() fires per BATCH, by its entry count, via
    // `SSTableRowIteratorAdapter::next`), so the gauge should settle back to 0
    // through completely ordinary consumption — no cancellation/reconcile
    // needed for this scenario.
    let mut writer =
        SSTableWriter::new(out.path().to_path_buf(), 1, &schema).expect("SSTableWriter::new");
    writer.pre_seed_encoding_baselines(baseline_ts, baseline_ldt, baseline_ttl);
    let stats = merger.merge(&mut writer).expect("merge into writer");
    let finish_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("finish runtime");
    finish_rt.block_on(writer.finish()).expect("writer finish");
    assert!(
        stats.output_rows >= (NUM_INPUTS as u64 * rows_per_input() as u64),
        "merge should emit all input rows; got {}",
        stats.output_rows
    );

    // AFTER: poll (bounded, fail-loud) for a POSITIVE observation of 0.0 — never
    // inferred from an absent/un-updated window (see `issue_2316_producer_gauge.rs`'s
    // identical DELTA-temporality caveat: an absent metric defaults to 0.0 in
    // `counter_sum`, which would pass vacuously whether `received()` fired or is
    // entirely broken). If `channel_depth::received_n()` were removed, the gauge
    // would stay pinned at its backed-up level forever and this loop would
    // exhaust its deadline and fail explicitly.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut observed_zero_record = false;
    let mut last_seen: Option<f64> = None;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
        let snap = capture.flush_and_collect();
        let matches = depth_points(&snap);
        if let Some(&v) = matches.last() {
            last_seen = Some(v);
        }
        if matches.contains(&0.0) {
            observed_zero_record = true;
            break;
        }
    }
    assert!(
        observed_zero_record,
        "gauge must POSITIVELY record a value of 0 after the merge fully drains \
         (never inferred from an absent/un-updated window); last observed value: {:?}",
        last_seen
    );
}
