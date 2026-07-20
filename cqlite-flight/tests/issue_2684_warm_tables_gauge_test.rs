//! Issue #2684 — `cqlite.flight.warm_tables` proven bidirectional through the
//! REAL `do_get` handler path (the public Flight surface).
//!
//! The gauge is atomic-backed at the `WarmTableRegistry` mutation sites and read
//! back here via the feature-independent `cqlite_flight::warm::warm_table_count`
//! level reader (no OTel stack needed — mirrors the #2419 blocking-task level
//! reader). A first `do_get` on a previously-unseen table triggers a rebuild
//! INSERT → the level rises; retiring the table's on-disk SSTables and issuing a
//! second `do_get` triggers a rebuild that drops the retired generation → the
//! level falls back.
//!
//! ## Isolation requirement
//!
//! `warm_table_count()` reads a PROCESS-GLOBAL atomic. An exact up/down read is
//! only meaningful with no sibling `do_get` in flight, so this file holds
//! EXACTLY ONE `#[test]` — one file = one binary = one process (matching the
//! `issue_2370_gauge_readback_test.rs` precedent). Do not add a second `#[test]`
//! here; add a sibling FILE instead.
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --test issue_2684_warm_tables_gauge_test
//! ```

use std::path::Path;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::Request;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::warm::warm_table_count;

const KS: &str = "warm_gauge_ks";
const TBL: &str = "items";
const DDL: &str = "CREATE TABLE warm_gauge_ks.items (id int PRIMARY KEY, name text)";

fn schema() -> TableSchema {
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
        columns: vec![col("id", "int", false), col("name", "text", true)],
        comments: std::collections::HashMap::new(),
        dropped_columns: std::collections::HashMap::new(),
    }
}

fn write_row(id: i32, name: &str) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![CellOperation::Write {
            column: "name".into(),
            value: Value::text(name),
        }],
        100,
        None,
    )
}

/// Flush a small single-SSTable fixture and return (temp, data_dir, table_dir).
fn build_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let temp = tempfile::TempDir::new().unwrap();
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema());
    let mut engine = WriteEngine::new(config).expect("engine");
    for i in 1..=6 {
        engine.write(write_row(i, &format!("n{i}"))).expect("write");
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(engine.flush()).expect("flush").expect("info");
    (temp, data_dir)
}

fn ticket_bytes() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": KS,
        "table": TBL,
        "ddl": DDL,
    }))
    .unwrap()
}

/// Delete every `*-Data.db` under `table_dir` so the next `warm_readers` rebuild
/// finds the generation set empty and retires the warm entry.
fn retire_sstables(table_dir: &Path) {
    for entry in std::fs::read_dir(table_dir)
        .expect("read table dir")
        .flatten()
    {
        let name = entry.file_name();
        if name.to_str().is_some_and(|n| n.ends_with("-Data.db")) {
            std::fs::remove_file(entry.path()).expect("remove data.db");
        }
    }
}

/// Drain a `do_get` for `ticket` to completion (or map the terminal error).
async fn drive_do_get(svc: &CqliteFlightService, ticket: Vec<u8>) -> Result<(), tonic::Status> {
    let resp = svc.do_get(Request::new(Ticket::new(ticket))).await?;
    let mut stream = resp.into_inner();
    while let Some(item) = stream.next().await {
        item?;
    }
    Ok(())
}

#[test]
fn warm_tables_gauge_rises_on_first_serve_and_falls_on_retirement() {
    let (_temp, data_dir) = build_fixture();
    let table_dir = data_dir.join(KS).join(TBL);
    let svc = CqliteFlightService::new(data_dir, 4);

    // The gauge is a process-global level; read the baseline explicitly so the
    // assertions are baseline-relative and never assume a clean slate.
    let baseline = warm_table_count();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // UP: a real do_get on a previously-unseen table warms it (rebuild insert).
        drive_do_get(&svc, ticket_bytes())
            .await
            .expect("first do_get warms the table");
        let after_warm = warm_table_count();
        assert_eq!(
            after_warm,
            baseline + 1,
            "warm_tables must rise by one after the first serve of a previously-unseen table"
        );

        // DOWN: retire the table's SSTables on disk, then a second do_get rebuilds
        // to an empty generation set → the table leaves the live warm set.
        retire_sstables(&table_dir);
        // The retired-table do_get returns zero rows (or a benign terminal status);
        // either way the rebuild retires the warm entry and drops the gauge.
        let _ = drive_do_get(&svc, ticket_bytes()).await;
        let after_retire = warm_table_count();
        assert!(
            after_retire < after_warm,
            "warm_tables must fall after the table's generations are retired \
             (was {after_warm}, now {after_retire})"
        );
        assert_eq!(
            after_retire, baseline,
            "warm_tables returns to its baseline once the retired table leaves the warm set"
        );
    });
}
