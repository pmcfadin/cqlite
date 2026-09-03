//! Issue #2383 — E2E resolve-phase CPU-spin pin over a real `do_get`.
//!
//! The round-8 field failure: a `do_get` against a snapshot with 2 BIG-nb
//! SSTables re-parsed the FULL `Index.db` for the SAME generation repeatedly
//! (debug log showed 8× "Parsed 1586932 partition entries" for one logical
//! query), pinning tokio workers in the O(entries) parse loop so LIMIT/`count(*)`/
//! point-reads all hung. The Trino connector takes a FRESH per-query Sidecar
//! snapshot and CLEARS the prior one; the warm cache keys on inode-stable
//! generation identity, so query N+1's new snapshot dir (same inodes, new path)
//! is a set-match — but current main finds the CACHED reader paths dead (query
//! N's cleared snapshot) and fully RE-OPENS every generation, re-parsing the
//! whole index each time.
//!
//! This drives that through the REAL `FlightService::do_get` and reads back the
//! authoritative `cqlite.sstable.index_parses_total` counter (one increment per
//! full `Index.db` parse, emitted from the core parse loop regardless of call
//! path) ALONGSIDE the warm registry's `reader_opens` work-done counter. A
//! correct read path rebinds a same-inode generation to its LIVE path with ZERO
//! FURTHER reader opens; a broken one re-opens #generations — the spin.
//!
//! Re-anchored for issue #2412 (lazy Summary-guided BIG open): a cold `do_get`
//! over a usable `Summary.db` now performs ZERO full `Index.db` parses at open
//! (deferred), so `index_parses_total` alone no longer distinguishes "rebound"
//! from "re-opened" for this fixture shape — see the test's own doc for the
//! `reader_opens`-based re-anchor.
//!
//! Separate integration-test process (roborev #2163 precedent): the OTel capture
//! harness installs a PROCESS-GLOBAL meter provider, so it must not share
//! cqlite-flight's parallel `--lib` unit-test binary. Fixtures are built directly
//! via `cqlite_core::storage::write_engine` (the crate `testutil` is
//! `#[cfg(test)]`-gated and invisible to an external integration binary).
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --features observability-testing \
//!   --test issue_2383_resolve_spin_test
//! ```

#![cfg(feature = "observability-testing")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::observability::{catalog, testing};
use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_flight::service::CqliteFlightService;

const KS: &str = "spin_ks";
const TBL: &str = "items";
const DDL: &str = "CREATE TABLE spin_ks.items (id int PRIMARY KEY, name text, score int)";

fn simple_schema() -> TableSchema {
    let col = |name: &str, ty: &str, nullable: bool| Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    };
    TableSchema {
        keyspace: KS.into(),
        table: TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            col("id", "int", false),
            col("name", "text", true),
            col("score", "int", true),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn write_row(id: i32, name: &str, score: i32) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![
            CellOperation::Write {
                column: "name".into(),
                value: Value::text(name),
            },
            CellOperation::Write {
                column: "score".into(),
                value: Value::Integer(score),
            },
        ],
        100,
        None,
    )
}

/// Flush a TWO-generation fixture (two separate flushes → two `nb-*-big`
/// SSTables) so the merge spans ≥2 Index.db files. Returns the temp dir (keep
/// alive), the data root, and the table dir.
fn build_two_gen_fixture() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let schema = simple_schema();
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
    let mut engine = WriteEngine::new(config).expect("engine");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for i in 0..8 {
        engine.write(write_row(i, "a", i)).expect("write");
    }
    rt.block_on(engine.flush()).expect("flush 1").expect("info");
    for i in 8..16 {
        engine.write(write_row(i, "b", i)).expect("write");
    }
    rt.block_on(engine.flush()).expect("flush 2").expect("info");
    let table_dir = data_dir.join(KS).join(TBL);
    let gens = std::fs::read_dir(&table_dir)
        .unwrap()
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .count();
    assert_eq!(gens, 2, "fixture must hold exactly two generations");
    (temp, data_dir, table_dir)
}

/// Sidecar-style snapshot: HARDLINK every component file into
/// `table_dir/snapshots/<name>/` (same inodes as the live SSTables, a new path).
fn make_snapshot(table_dir: &Path, name: &str) -> PathBuf {
    let snap = table_dir.join("snapshots").join(name);
    std::fs::create_dir_all(&snap).unwrap();
    for entry in std::fs::read_dir(table_dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_file() {
            let dest = snap.join(entry.file_name());
            std::fs::hard_link(&path, &dest)
                .or_else(|_| std::fs::copy(&path, &dest).map(|_| ()))
                .unwrap();
        }
    }
    snap
}

fn ticket_bytes(snapshot: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": KS,
        "table": TBL,
        "ddl": DDL,
        "snapshot": snapshot,
    }))
    .unwrap()
}

/// Run one `do_get` in-process and fully drain its stream (so the whole merge
/// completes and every parse has fired).
async fn do_get_drain(svc: &CqliteFlightService, ticket: Vec<u8>) -> usize {
    let resp = svc
        .do_get(Request::new(Ticket::new(ticket)))
        .await
        .expect("do_get");
    let mut stream = resp.into_inner();
    let mut msgs = 0usize;
    while let Some(item) = stream.next().await {
        item.expect("stream item ok");
        msgs += 1;
    }
    msgs
}

/// **Resolve-phase spin pin (issue #2383), re-anchored for issue #2412.** Two
/// `do_get`s against the SAME underlying generations reached through TWO
/// per-query snapshot dirs (query N's is cleared before query N+1 stages a
/// fresh same-inode dir, exactly as the Trino connector's `clearSnapshot`
/// does). The SECOND must serve the same inodes via a REBIND — zero further
/// reader OPENS — not a full re-open of every generation (the field's O(entries
/// × opens) amplification, seen as 8× re-parses for one logical query).
///
/// Re-anchored (issue #2412, coordinator-flagged regression class — same root
/// cause as `spin_tests_2383::cancel_at_large_index_parse_entry_aborts_promptly`):
/// this test originally asserted query 1 (cold) full-parses `Index.db` `>= 2`
/// (once per generation) via `index_parses_total`. Since #2412 Stage 2, BIG
/// open defers that parse (`open_lazy`) whenever a usable `Summary.db` is
/// present — the common/field shape this fixture produces — so a cold `do_get`
/// now performs ZERO full parses at open (spec Requirement 7's "cold = 0 full
/// Index.db parses", a strict IMPROVEMENT on the pre-#2412 baseline this test
/// measured). `index_parses_total` therefore no longer distinguishes "rebound
/// in place" from "fully re-opened" for this shape (both are now cheap/parse-
/// free) — the REBIND-vs-reopen property #2383 fix B protects is asserted
/// instead via `reader_opens` (the warm registry's real-open work-done counter,
/// `#2383`/`#2310`), which is exactly what distinguishes them: a rebind costs
/// ZERO opens, a full re-open costs one PER generation.
///
/// RED pre-#2383-fix-B: the cached readers' snap1 paths were dead after
/// teardown, so the warm rebuild RE-OPENED both generations from snap2 —
/// `reader_opens` for query 2 == 2 (one per generation), instead of the
/// rebind's 0.
#[test]
fn do_get_reparses_index_on_every_snapshot_swap() {
    // Install the process-global in-memory meter BEFORE any parse in this process.
    let mc = testing::metrics_capture();

    let (_temp, data_dir, table_dir) = build_two_gen_fixture();
    let svc = CqliteFlightService::new(data_dir, 8192);
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Query N: warm from snap1 (readers open with file_path inside snap1).
    let snap1 = make_snapshot(&table_dir, "snap1");
    mc.reset();
    let msgs1 = rt.block_on(do_get_drain(&svc, ticket_bytes("snap1")));
    let q1_parses = mc
        .flush_and_collect()
        .counter_sum(catalog::INDEX_PARSES_TOTAL);
    let opens_after_q1 = svc.warm_metrics().reader_opens;
    assert!(msgs1 > 0, "query 1 must stream at least a schema message");
    // Spec Requirement 7 (issue #2412): a cold open over a usable Summary.db
    // performs ZERO full Index.db parses (open is lazy, deferred to first
    // materializing use — never triggered on this streaming query path).
    assert_eq!(
        q1_parses, 0.0,
        "cold query 1 must full-parse Index.db ZERO times over a usable \
         Summary.db (issue #2412 lazy open); got {q1_parses}"
    );
    assert_eq!(
        opens_after_q1, 2,
        "cold query 1 must open exactly the two generations once each"
    );

    // Connector clears query N's snapshot; the live inodes in table_dir persist.
    std::fs::remove_dir_all(&snap1).expect("clear query-N snapshot dir");

    // Query N+1: a NEW snapshot dir over the SAME inodes (hardlinks). The on-disk
    // dir is what the "snap2" ticket resolves; the returned path is not read here.
    let _snap2 = make_snapshot(&table_dir, "snap2");
    mc.reset();
    let msgs2 = rt.block_on(do_get_drain(&svc, ticket_bytes("snap2")));
    let q2_parses = mc
        .flush_and_collect()
        .counter_sum(catalog::INDEX_PARSES_TOTAL);
    let opens_after_q2 = svc.warm_metrics().reader_opens;
    assert!(msgs2 > 0, "query 2 must stream at least a schema message");

    // Full parses stay zero (nothing to re-parse either way for this shape).
    assert_eq!(
        q2_parses, 0.0,
        "a second do_get over the SAME generations must still perform zero \
         full Index.db parses; got {q2_parses}"
    );
    // THE anti-spin assertion (re-anchored): the second query over the same
    // inodes must REBIND — zero FURTHER reader opens — not fully re-open every
    // generation. RED pre-#2383-fix-B: `opens_after_q2` would climb by 2 (a
    // full re-open of both generations from snap2's dead-path fallback).
    assert_eq!(
        opens_after_q2, opens_after_q1,
        "a second do_get over the SAME generations (new same-inode snapshot dir) \
         must REBIND with zero further reader opens, not re-open #generations \
         (issue #2383 resolve-phase spin); opens climbed from {opens_after_q1} \
         to {opens_after_q2}"
    );
}
