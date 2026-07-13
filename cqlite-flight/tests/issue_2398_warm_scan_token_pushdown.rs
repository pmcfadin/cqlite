//! Issue #2398 — reproduction: a warm token-range `do_get` scan reads + parses
//! EVERY partition body in the SSTable, a fixed O(all partitions) per-query cost
//! independent of the split range width and of `LIMIT`.
//!
//! ## What the field saw
//!
//! Round-9/10 (#2367): warm `LIMIT 5` scans took ~7-9s — a fixed multi-second
//! per-query setup, NOT proportional to the returned row count, with
//! `index_parses_total` FLAT (so it is neither the #2383 rebuild storm nor the
//! #2385 cold index parse). The latency also VARIED with the split (6.3-7.4s),
//! the signature of a cost that depends on WHERE in the ring the split falls.
//!
//! ## Root cause (this test's evidence)
//!
//! The per-SSTable partition enumeration (the streaming full-index walk for the
//! uncompressed `V5_0Uncompressed` field path — `stream_all_partitions_via_full_index`,
//! issue #2361 — and the chunk-stitching walk `drain_compaction_window` for the
//! `nb` path) walks EVERY partition from the ring start and reads + parses each
//! `Data.db` body. The split's token-range filter is applied only DOWNSTREAM at
//! the consumer (`MergeProducer::drive_merge`'s `token.contains`). So a split
//! whose `(start, end]` range covers a single partition still pays to read +
//! parse the whole SSTable.
//!
//! `work_counters::stream_walk_partitions_parsed()` counts partition BODIES a scan
//! decoded. This test drives a warm token-range scan whose range holds exactly ONE
//! partition and shows the server decoded ALL `N` bodies to answer it — the
//! scale-free mechanism the field 7s reflects at ~1.9M partitions.
//!
//! ## Flip when the fix lands (issue #2398 acceptance criterion 2)
//!
//! The fix is to push the token range INTO the walk (binary-search the
//! token-ordered index entries and walk only the in-range slice). When it lands,
//! the single-partition-range assertion below MUST become `walked <= small bound`
//! (a few, for binary-search boundary slack) instead of `== N`, and the
//! `>` comparison flips. See the issue for the fix plan; this test is its pinned
//! reproduction.
//!
//! ## Warm small-LIMIT latency target (issue #2398 acceptance criterion 3)
//!
//! Warm point-reads already land at ~2.1s ≈ the Trino/JDBC transport floor
//! (bounded index lookup). A warm small-`LIMIT` scan should approach that same
//! floor: its server-side work must be O(in-range partitions + LIMIT), bounded
//! near the transport floor (target: within ~1-2x the warm point-read, i.e. low
//! single-digit seconds and NOT growing with the SSTable's total partition
//! count), NOT the current O(all partitions in every overlapping SSTable) that
//! makes it a fixed 7-9s at field scale. The `stream_walk_partitions_parsed`
//! counter is the scale-free proxy for that target: for a narrow split it must
//! track the in-range slice, not the whole SSTable.
//!
//! ## Isolation
//!
//! The work counter is a PROCESS-GLOBAL atomic and the scan runs on a spawned
//! producer thread, so an exact count is only meaningful with no sibling `do_get`
//! in flight. This file holds EXACTLY ONE `#[test]` — one file = one binary = one
//! process (per-test process isolation under nextest, the gate default; no sibling
//! threads under plain `cargo test`). Add a sibling FILE, not a second `#[test]`.

use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::storage::sstable::work_counters;
use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_core::util::cassandra_murmur3::cassandra_murmur3_token;
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::test_fixtures as fx;

/// Partitions written into the single SSTable. Enough that "walk them all" is
/// unmistakably distinct from "walk the one in range".
const N: usize = 40;

/// Build ONE `nb-big` SSTable holding `N` single-`text`-PK partitions (keys
/// `k000000..`). One flush → one SSTable the warm `do_get` merge scans.
fn build_single_sstable() -> (tempfile::TempDir, PathBuf) {
    let schema = fx::keyvalue_schema();
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).expect("engine");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for i in 0..N {
        engine
            .write(fx::keyvalue_write(&key_for(i), &format!("v{i}")))
            .expect("write");
    }
    rt.block_on(engine.flush())
        .expect("flush")
        .expect("flush info");
    (temp, data_dir)
}

fn key_for(i: usize) -> String {
    format!("k{i:06}")
}

/// The Murmur3 token of partition `i`. A single-component `text` partition key is
/// stored on disk as its raw value bytes, which is exactly what the scan hashes.
fn token_for(i: usize) -> i64 {
    cassandra_murmur3_token(key_for(i).as_bytes())
}

/// A token-range `do_get` ticket over `(start, end]`.
fn token_range_ticket(start: i64, end: i64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": fx::KEYVALUE_KS,
        "table": fx::KEYVALUE_TBL,
        "ddl": fx::KEYVALUE_DDL,
        "token_start": start,
        "token_end": end,
    }))
    .unwrap()
}

/// A full-scan ticket (no token range).
fn full_scan_ticket() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": fx::KEYVALUE_KS,
        "table": fx::KEYVALUE_TBL,
        "ddl": fx::KEYVALUE_DDL,
    }))
    .unwrap()
}

/// Drive the in-process `do_get` handler and fully drain the decoded stream,
/// returning the total row count.
async fn do_get_rows(svc: &CqliteFlightService, ticket: Vec<u8>) -> usize {
    let resp = svc
        .do_get(Request::new(Ticket::new(ticket)))
        .await
        .expect("do_get");
    let stream = resp
        .into_inner()
        .map(|r| r.map_err(|s| FlightError::ExternalError(Box::new(s))));
    let mut rb = FlightRecordBatchStream::new_from_flight_data(stream);
    let mut rows = 0usize;
    while let Some(batch) = rb.next().await {
        let batch: RecordBatch = batch.expect("decode batch");
        rows += batch.num_rows();
    }
    rows
}

#[test]
fn warm_token_range_scan_reads_the_whole_sstable_for_one_partition() {
    let (_temp, data_dir) = build_single_sstable();
    let svc = CqliteFlightService::new(data_dir, 1024);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Pick an INTERIOR partition (roborev 1692, low): the ring MINIMUM's
    // predecessor bound (`min_token - 1`) collapses to `min_token` itself when
    // `min_token == i64::MIN`, and `token_in_half_open_range` treats an equal
    // `(start, end]` pair as the FULL ring (not empty) — so anchoring on the
    // lowest token risks silently widening the "one partition" range to a full
    // scan instead of narrowing it. An interior token's own predecessor in the
    // SORTED token order is always a strictly smaller, distinct i64 (no
    // wraparound edge), so `(predecessor, target]` is unconditionally a genuine
    // one-partition half-open range.
    let mut tokens: Vec<(usize, i64)> = (0..N).map(|i| (i, token_for(i))).collect();
    tokens.sort_by_key(|(_, t)| *t);
    let mid = tokens.len() / 2;
    let (target_idx, target_token) = tokens[mid];
    let predecessor_token = tokens[mid - 1].1;
    assert!(
        predecessor_token < target_token,
        "fixture must have a unique target token (got {target_token} == predecessor \
         {predecessor_token})"
    );
    let start = predecessor_token;
    let end = target_token;

    rt.block_on(async {
        // Warm the reader (first request opens + parses the index; the #2310 warm
        // path the field runs). Its walk parses are discarded by the reset below.
        let _ = do_get_rows(&svc, full_scan_ticket()).await;

        // ---- A warm token-range scan whose range holds exactly ONE partition. --
        work_counters::reset();
        let rows = do_get_rows(&svc, token_range_ticket(start, end)).await;
        let walked = work_counters::stream_walk_partitions_parsed() as usize;

        // Correctness: exactly the one in-range partition's row is returned.
        assert_eq!(
            rows, 1,
            "the (predecessor, target] range must return exactly the target \
             partition (idx {target_idx})"
        );

        // Control: a full scan's whole-SSTable body count, so the comparison below
        // is against a measured baseline, not a hard-coded N.
        work_counters::reset();
        let all = do_get_rows(&svc, full_scan_ticket()).await;
        let full_walked = work_counters::stream_walk_partitions_parsed() as usize;
        assert_eq!(all, N, "the full scan returns every partition");
        assert_eq!(
            full_walked, N,
            "a full (no token range) scan walks every partition body"
        );

        // THE evidence (issue #2398): answering a ONE-partition token range cost
        // the SAME whole-SSTable body-parse work as the full scan — the token
        // filter runs only at the consumer, so the walk reads every partition
        // regardless of the split range. This is the fixed per-query setup the
        // field saw as a multi-second cost independent of the result size.
        //
        // FLIP WHEN THE FIX LANDS: once the token range is pushed into the walk
        // (binary-search the token-ordered index, walk only the in-range slice),
        // change this to `assert!(walked <= 4, ...)` — the walk will touch only the
        // in-range partition (+ small boundary slack), NOT all {N}.
        assert_eq!(
            walked, full_walked,
            "issue #2398: a single-partition token range parsed {walked} partition \
             bodies — the SAME as the whole-SSTable full scan ({full_walked}) — \
             because the token filter is applied only downstream at the consumer, \
             not pushed into the per-SSTable walk. This is the fixed warm-scan \
             setup cost independent of the split range and of LIMIT."
        );
    });
}
