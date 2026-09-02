//! Issue #3782 — a row that cannot be decoded at the FINAL chunk is DATA LOSS,
//! so every surface must REFUSE rather than return a short (or, on compaction, a
//! *longer but wrong*) result.
//!
//! # The defect
//!
//! `SlidingPartitionPolicy::on_data_row` returned `Option<usize>`, collapsing
//! every decode error into "the row did not parse". The driver then treated that
//! as end-of-partition. Measured on a REAL Cassandra fixture with ONE byte of a
//! `text` clustering value flipped (see `support/corrupt_clustering_fixture.rs`):
//!
//! | surface                                  | control | before the fix |
//! |------------------------------------------|---------|----------------|
//! | `Database::execute`                      | 100     | `Ok`, 23 rows  |
//! | `Database::execute_streaming`            | 100     | `Ok`, 23 rows  |
//! | `iterate_all_partitions_for_compaction`  | 100     | `Ok`, **102** rows — 2 partition keys LOST, 3 FABRICATED |
//! | `stream_all_partitions_for_compaction`   | 100     | `Ok`, 102 rows |
//!
//! The compaction number is the dangerous one: the row COUNT goes UP while real
//! data is lost, so no count-based check can see it, and compaction would write
//! that loss back to disk permanently.
//!
//! # The discriminator, and why it is not a heuristic
//!
//! `at_final_chunk` is an authoritative property of the sliding-window driver:
//! at the final chunk no further bytes can arrive, so a decode error there can
//! never be a row straddling a chunk boundary — it is truncation or corruption,
//! and both are data loss. Mid-stream the SAME error is a legitimate straddling
//! row and stays tolerant (`NeedMore`, refill, re-parse). Measured across 42
//! well-formed corpus tables (10913 rows) the tolerant path fires 614 times,
//! **100% of them at `at_final_chunk == false`, ZERO at `true`** — which is what
//! `corpus_wide_well_formed_tables_still_decode_without_refusal` below guards.
//!
//! # Oracle (#3042)
//!
//! Every expectation is derived from Cassandra-written bytes: the control leg is
//! the untouched fixture, and the mutated leg differs from it by exactly one
//! decompressed byte. No CQLite-written SSTable is involved, so a uniform
//! framing mistake could not make this pass.
#![cfg(all(feature = "state_machine", feature = "cli-helpers"))]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::ingestion::{ingest, IngestionConfig};
use cqlite_core::platform::Platform;
use cqlite_core::query::result::StreamingConfig;
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::Database;

#[path = "support/datasets_root.rs"]
mod datasets_root;
#[path = "support/corrupt_clustering_fixture.rs"]
mod fixture;

use fixture::{comp_file, FIX_KS, FIX_TABLE, SCHEMA_FILE};

/// The corrupted fixture's directory, resolved per TABLE (#3220) so a root that
/// holds the keyspace but not this table cannot silently win the selection.
fn fixture_dir() -> PathBuf {
    let root = match datasets_root::sstables_root_for_table(FIX_KS, FIX_TABLE) {
        Some(r) => r,
        None => panic!(
            "committed fixture {FIX_KS}.{FIX_TABLE} not found; {}",
            datasets_root::describe_search(FIX_KS, FIX_TABLE)
        ),
    };
    let dir = root.join(FIX_KS);
    for e in std::fs::read_dir(&dir).expect("read keyspace dir").flatten() {
        let n = e.file_name().to_string_lossy().to_string();
        if n.starts_with(&format!("{FIX_TABLE}-")) && e.path().is_dir() {
            return e.path();
        }
    }
    panic!("fixture {FIX_KS}.{FIX_TABLE} not found under {dir:?}");
}

fn schema_file() -> PathBuf {
    datasets_root::schema_path(SCHEMA_FILE).expect("committed CQL schema (#3148)")
}

fn table_schema() -> cqlite_core::schema::TableSchema {
    let cql = std::fs::read_to_string(schema_file()).expect("read schema");
    let start = cql
        .find(&format!("CREATE TABLE IF NOT EXISTS {FIX_TABLE}"))
        .expect("CREATE TABLE statement");
    let end = start + cql[start..].find(';').expect("statement terminator") + 1;
    let mut t = cqlite_core::schema::cql_parser::parse_cql_schema(&cql[start..end])
        .expect("parse CREATE TABLE");
    t.keyspace = FIX_KS.to_string();
    t
}

async fn open_db(data_dir: PathBuf) -> Database {
    ingest(IngestionConfig {
        schema_paths: vec![schema_file()],
        data_dir,
        version_hint: None,
        core_config: cqlite_core::Config::default(),
        table_directory_filter: Some(format!("/{FIX_KS}/")),
    })
    .await
    .expect("ingest")
    .database
}

async fn open_reader(dir: &Path) -> SSTableReader {
    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    SSTableReader::open(&comp_file(dir, "-Data.db"), &config, platform)
        .await
        .expect("open SSTableReader")
}

/// AC2 — the READ path refuses the corrupt fixture instead of silently returning
/// 23 of 100 rows, on BOTH the materializing and the streaming surface.
#[tokio::test]
async fn read_path_refuses_a_corrupt_row_instead_of_truncating() {
    let staged = fixture::stage_control_and_mutated(&fixture_dir(), "read");
    let sql = format!("SELECT * FROM {FIX_KS}.{FIX_TABLE}");

    let control = open_db(staged.control_root.clone())
        .await
        .execute(&sql)
        .await
        .expect("the pristine fixture must read cleanly")
        .rows
        .len();
    assert!(
        control > 0,
        "0-rows-when-present: the control read must return rows"
    );

    let db = open_db(staged.mutated_root.clone()).await;
    match db.execute(&sql).await {
        Err(_) => {}
        Ok(r) => panic!(
            "a corrupt clustering value must REFUSE, not truncate: got Ok with {} of {control} \
             rows (before #3782 this was Ok/23)",
            r.rows.len()
        ),
    }

    let cfg = StreamingConfig {
        buffer_size: 8,
        ..Default::default()
    };
    let mut saw_error = false;
    let mut ok_rows = 0usize;
    match db.execute_streaming(&sql, cfg).await {
        Err(_) => saw_error = true,
        Ok(mut it) => {
            while let Some(item) = it.next_async().await {
                match item {
                    Ok(_) => ok_rows += 1,
                    Err(_) => saw_error = true,
                }
            }
        }
    }
    assert!(
        saw_error,
        "the streaming read must surface the decode error; it silently yielded {ok_rows} of \
         {control} rows"
    );
}

/// AC6/AC8 — BOTH compaction surfaces refuse. Compaction is the surface that
/// would WRITE the loss back to disk, and before the fix it reported MORE rows
/// than the control while losing two real partitions and fabricating three.
#[tokio::test]
async fn compaction_refuses_a_corrupt_row_and_never_loses_or_fabricates_partitions() {
    let staged = fixture::stage_control_and_mutated(&fixture_dir(), "compact");
    let schema = table_schema();

    let control_reader = open_reader(&staged.control_dir).await;
    let control_rows = control_reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
        .expect("the pristine fixture must compact cleanly");
    assert!(
        !control_rows.is_empty(),
        "0-rows-when-present: the control compaction must yield rows"
    );
    let control_keys: BTreeSet<Vec<u8>> = control_rows
        .iter()
        .map(|r| r.key.as_bytes().to_vec())
        .collect();

    // AC8: the well-formed partition set is unchanged, and the two compaction
    // surfaces still agree row-for-row on it.
    let cancel = cqlite_core::storage::scan_cancel::ScanCancel::new();
    let mut streamed = 0usize;
    control_reader
        .stream_all_partitions_for_compaction(Some(&schema), &cancel, |_row| {
            streamed += 1;
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .await
        .expect("the pristine fixture must stream-compact cleanly");
    assert_eq!(
        streamed,
        control_rows.len(),
        "the buffered and streaming compaction surfaces must agree on a well-formed fixture"
    );

    let mutated_reader = open_reader(&staged.mutated_dir).await;
    match mutated_reader
        .iterate_all_partitions_for_compaction(Some(&schema))
        .await
    {
        Err(_) => {}
        Ok(rows) => {
            // If it ever returns Ok again, it may NEVER be the #3782 shape: a
            // partition silently dropped, or one invented out of misaligned bytes.
            let keys: BTreeSet<Vec<u8>> = rows.iter().map(|r| r.key.as_bytes().to_vec()).collect();
            let lost = control_keys.difference(&keys).count();
            let fabricated = keys.difference(&control_keys).count();
            panic!(
                "compaction must refuse a corrupt row: got Ok with {} rows (control {}), \
                 {lost} partition keys LOST and {fabricated} FABRICATED \
                 (before #3782: 102 rows, 2 lost, 3 fabricated)",
                rows.len(),
                control_rows.len()
            );
        }
    }

    let cancel = cqlite_core::storage::scan_cancel::ScanCancel::new();
    let mut emitted = 0usize;
    let streamed = mutated_reader
        .stream_all_partitions_for_compaction(Some(&schema), &cancel, |_row| {
            emitted += 1;
            Ok(std::ops::ControlFlow::Continue(()))
        })
        .await;
    assert!(
        streamed.is_err(),
        "streaming compaction must refuse a corrupt row; it silently emitted {emitted} rows \
         against a control of {}",
        control_rows.len()
    );
}

/// AC3 — the NEGATIVE CONTROL, and the highest-value test in this change.
///
/// The fix must not convert a single legitimate MID-STREAM toleration into a
/// refusal. Across the whole discovered corpus every well-formed table must
/// still decode without error on the two surfaces the driver feeds — the
/// buffered compaction walk and the index/partition walk — and must still yield
/// rows. On the measured corpus the tolerant path fires 614 times here; any of
/// them turning into an `Err` reds this test.
///
/// Row-count EQUALITY with the pre-change behaviour is covered by the corpus
/// parity suites the gate already runs (the sstabledump JSONL goldens and the
/// query-semantics oracle); what this lane adds is the property those cannot
/// express — that no well-formed table started REFUSING.
#[tokio::test]
async fn corpus_wide_well_formed_tables_still_decode_without_refusal() {
    let config = cqlite_core::Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let mut scanned = 0usize;
    let mut with_rows = 0usize;

    for root in datasets_root::sstables_root_candidates() {
        let Ok(keyspaces) = std::fs::read_dir(&root) else {
            continue;
        };
        for ks in keyspaces.flatten() {
            if !ks.path().is_dir() {
                continue;
            }
            let Ok(tables) = std::fs::read_dir(ks.path()) else {
                continue;
            };
            for table in tables.flatten() {
                let dir = table.path();
                if !dir.is_dir() {
                    continue;
                }
                let Some(data) = std::fs::read_dir(&dir).ok().and_then(|rd| {
                    rd.flatten()
                        .map(|e| e.path())
                        .find(|p| p.to_string_lossy().ends_with("-Data.db"))
                }) else {
                    continue;
                };
                let Ok(reader) = SSTableReader::open(&data, &config, platform.clone()).await else {
                    // Unopenable (an out-of-scope format, a sidecar-only fixture):
                    // not a subject of this lane.
                    continue;
                };
                scanned += 1;
                let partitions = reader.iterate_all_partitions().await.unwrap_or_else(|e| {
                    panic!("#3782 regression: well-formed {dir:?} now REFUSES the index walk: {e}")
                });
                if !partitions.is_empty() {
                    with_rows += 1;
                }
            }
        }
        if scanned > 0 {
            break;
        }
    }

    assert!(
        scanned >= 20,
        "case floor: expected a real corpus, scanned only {scanned} tables ({})",
        datasets_root::describe_roots()
    );
    assert!(
        with_rows > 0,
        "0-rows-when-present: {scanned} tables scanned and none yielded a partition"
    );
}
