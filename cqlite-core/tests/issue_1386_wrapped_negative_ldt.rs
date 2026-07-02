//! Issue #1386: wrapped / negative `localDeletionTime` through the compaction
//! purge sites.
//!
//! ## The invariant under test
//!
//! An on-disk `localDeletionTime` (LDT) is an UNSIGNED 32-bit GC-clock second
//! count. Cassandra extended the epoch past year-2038 by treating the field as
//! `u32` (`hasUIntDeletionTime`, BigFormat.java:409): a value in `[2^31, 2^32)`
//! is a FAR-FUTURE instant, even though its `i32` bit pattern is NEGATIVE. The
//! three gc-grace purge sites in `ReconcileState::purge_gc_grace`
//! (`reconcile.rs`) MUST reinterpret the bits unsigned before comparing to
//! `gcBefore`:
//!
//! ```text
//! i64::from(ldt as u32) < gc_before     // reconcile.rs:470 (cell tombstones)
//! i64::from(self.row_del_ldt as u32) < gc_before  // reconcile.rs:507 (row)
//! i64::from(cd.local_deletion_time as u32) < gc_before  // reconcile.rs:521 (complex)
//! ```
//!
//! A regression to a SIGNED compare (`i64::from(ldt) < gc_before`) at ANY of the
//! three sites would treat a far-future wrapped LDT (a negative `i32`) as
//! ANCIENT — strictly below any realistic `gcBefore` — and PURGE a tombstone that
//! is not yet expired. If that tombstone was shadowing older, covered cells in
//! another SSTable, dropping it RESURRECTS the deleted data. This is tested on
//! the READ path only in `issue_655_oa_read_gates.rs`; here we pin it through the
//! real WRITE / compaction purge path.
//!
//! ## What each test proves (real `compact_sstables`)
//!
//! Two input SSTables are compacted with `purge_safe = true` (major compaction,
//! overlap gate is +inf) and a NORMAL `gcBefore` (~year 2027, far below `2^31`):
//!
//!   * an OLDER SSTable holds a live cell under `(pk, ck)` at a LOW timestamp;
//!   * a NEWER SSTable holds a tombstone (cell / row / complex-deletion) covering
//!     that cell, stamped with a far-future WRAPPED LDT at a HIGHER timestamp.
//!
//! After compaction we read the output back through the compaction reader
//! (`iterate_all_partitions_for_compaction`) and assert:
//!   1. the far-future tombstone is RETAINED (present) — NOT purged;
//!   2. it still SHADOWS the covered older live cell (the older value is absent);
//!   3. its LDT survives verbatim as the wrapped `as u32 as i32` bit pattern.
//!
//! Because the tombstone is retained and shadowing is intact, NO gc/overlap-safe
//! purge occurred for it — the behavioral equivalent of "`PurgeCounts` zero
//! purges" (the private `PurgeCounts` is not observable from an integration
//! test, so retention-with-shadowing is the authoritative public-surface proxy).
//!
//! ## Boundary
//!
//! `wrapped_ldt_at_i32_min_*` pins the exact wrap boundary: `LDT == 2^31`
//! (`i32::MIN` as a bit pattern). Under a signed compare `i32::MIN` is the MOST
//! negative value and would be purged first; under the correct unsigned compare
//! it is `2_147_483_648` seconds — far future — and is retained.
//!
//! ## Documented divergence
//!
//! CQLite SILENTLY reinterprets the LDT bits unsigned (year-2106 semantics) and
//! never marks the value "suspect"; Cassandra 5.0's `Cell`/`LivenessInfo` path
//! additionally treats an out-of-range deletion time as suspicious (logging /
//! validation). CQLite's recorded posture is: reinterpret-unsigned, do NOT
//! suspect-mark. See the doc comment on
//! [`ReconcileState::purge_gc_grace`](../../src/storage/write_engine/merge/reconcile.rs).

#![cfg(all(feature = "write-support", feature = "cli-helpers"))]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cqlite_core::platform::Platform;
use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::sstable::reader::{CompactionRowData, SSTableReader};
use cqlite_core::storage::write_engine::merge::compact_sstables;
use cqlite_core::storage::write_engine::{
    CellOperation, ClusteringKey, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::{TombstoneType, Value};
use cqlite_core::Config;
use tempfile::TempDir;

const KEYSPACE: &str = "wrap_ks";
const TABLE: &str = "items";

// ===========================================================================
// LDT bit-pattern constants (the crux of the issue)
// ===========================================================================

/// A far-future wrapped LDT: unsigned `2^31 + 5 = 2_147_483_653` seconds
/// (≈ January 2038), whose `i32` bit pattern is NEGATIVE.
const WRAPPED_LDT_UNSIGNED: i64 = 2_147_483_653; // 2^31 + 5
/// The same value as it is carried on disk / in the mutation API: a negative
/// `i32` (`(2_147_483_653u32) as i32 == -2_147_483_643`).
const WRAPPED_LDT_I32: i32 = WRAPPED_LDT_UNSIGNED as u32 as i32;

/// The exact wrap boundary: unsigned `2^31 = 2_147_483_648`, whose `i32` bit
/// pattern is `i32::MIN` (the most-negative value — purged FIRST under a signed
/// compare, retained under the correct unsigned compare).
const BOUNDARY_LDT_UNSIGNED: i64 = 2_147_483_648; // 2^31
const BOUNDARY_LDT_I32: i32 = BOUNDARY_LDT_UNSIGNED as u32 as i32; // == i32::MIN

/// A NORMAL gc cutoff (~ year 2027): far below `2^31`, so a correct unsigned
/// compare keeps every wrapped far-future tombstone WITHIN grace (retained). A
/// signed regression would see the negative wrapped LDT as `< gcBefore` and
/// purge it.
const GC_BEFORE_SECS: i64 = 1_800_000_000;
/// `now` is far in the future so the live keep-alive cells are never
/// TTL-expired; the tombstones under test carry no TTL.
const NOW_SECS: i64 = 4_000_000_000;

// ===========================================================================
// Schema
// ===========================================================================

/// PK=id(int), CK=ck(int), simple columns `name`(text) + `score`(int), and a
/// non-frozen `tags`(set<text>) complex column for the complex-deletion site.
fn schema() -> TableSchema {
    TableSchema {
        keyspace: KEYSPACE.to_string(),
        table: TABLE.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order: ClusteringOrder::Asc,
        }],
        columns: vec![
            col("id", "int"),
            col("ck", "int"),
            col("name", "text"),
            col("score", "int"),
            col("tags", "set<text>"),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

fn col(name: &str, ty: &str) -> Column {
    Column {
        name: name.to_string(),
        data_type: ty.to_string(),
        nullable: true,
        default: None,
        is_static: false,
    }
}

// ===========================================================================
// Runtime + write / discover / compact / read helpers
// ===========================================================================

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

/// Flush a batch of mutations into ONE input SSTable rooted at `dir`.
fn flush_batch(dir: &Path, muts: Vec<Mutation>) {
    let data_dir = dir.join("data");
    let wal_dir = dir.join("wal");
    let config = WriteEngineConfig::new(data_dir, wal_dir, schema());
    let mut engine = WriteEngine::new(config).expect("engine");
    for m in muts {
        engine.write(m).expect("write mutation");
    }
    let r = rt();
    r.block_on(engine.flush()).expect("flush").expect("info");
    r.block_on(engine.close()).expect("close engine");
}

/// Discover every `nb-*-big-Data.db` under `dir`.
fn discover_inputs(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    walk(&p, out);
                } else if p
                    .file_name()
                    .and_then(|s| s.to_str())
                    .is_some_and(|n| n.ends_with("-Data.db") && !n.starts_with("._"))
                {
                    out.push(p);
                }
            }
        }
    }
    walk(dir, &mut out);
    out
}

/// Run a MAJOR compaction (`purge_safe = true`, overlap gate = +inf) over
/// `inputs` with the pinned normal `gcBefore` / `now`. Returns the output
/// Data.db path.
fn compact_major(inputs: Vec<PathBuf>, out_dir: &Path) -> PathBuf {
    let report = rt()
        .block_on(compact_sstables(
            inputs,
            out_dir,
            &schema(),
            1386,
            Some(GC_BEFORE_SECS),
            Some(NOW_SECS),
            true,
        ))
        .expect("compaction must succeed (no panic on a wrapped/negative LDT)");
    report.output.data_path
}

/// Every `CompactionRow` of a compacted output, read back through the compaction
/// read path (surfaces cell tombstones, row tombstones, and complex deletions).
fn read_compaction_rows(
    data_path: &Path,
) -> Vec<cqlite_core::storage::sstable::reader::CompactionRow> {
    rt().block_on(async {
        let mut config = Config::default();
        config.storage.use_mmap = false;
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let reader = SSTableReader::open(data_path, &config, platform)
            .await
            .expect("open compacted output reader");
        let schema = schema();
        reader
            .iterate_all_partitions_for_compaction(Some(&schema))
            .await
            .expect("iterate compaction rows")
    })
}

// ===========================================================================
// Mutation builders
// ===========================================================================

/// An OLDER live row: `name`=`old_value` + `score` at a LOW timestamp. This is
/// the covered data a surviving tombstone must keep SHADOWED.
fn write_older_live_row(ck: i32, old_value: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KEYSPACE, TABLE),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![
            CellOperation::Write {
                column: "name".to_string(),
                value: Value::Text(old_value.to_string()),
            },
            CellOperation::Write {
                column: "score".to_string(),
                value: Value::Integer(7),
            },
        ],
        ts,
        None,
    )
}

/// An OLDER live row holding a non-frozen `tags` set element, so a newer complex
/// deletion at a higher `markedForDeleteAt` shadows it.
fn write_older_tags_row(ck: i32, element: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KEYSPACE, TABLE),
        PartitionKey::single("id", Value::Integer(1)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "tags".to_string(),
            value: Value::Set(vec![Value::Text(element.to_string())]),
        }],
        ts,
        None,
    )
}

// ===========================================================================
// Criterion 1 + 2 (cell-tombstone site) — reconcile.rs:470
// ===========================================================================

/// Far-future wrapped cell tombstone (LDT bit pattern `2^31 + 5`) covering an
/// older live `name` cell: RETAINED under a normal `gcBefore`, still shadows the
/// older value, LDT survives as the wrapped bit pattern, no panic.
#[test]
fn wrapped_ldt_cell_tombstone_retained_and_shadows() {
    let temp = TempDir::new().unwrap();
    let older = temp.path().join("older");
    let newer = temp.path().join("newer");
    let out = temp.path().join("out");

    // Older SSTable: live name="secret" at ts=100.
    flush_batch(&older, vec![write_older_live_row(0, "secret", 100)]);

    // Newer SSTable: cell tombstone for `name` at ts=300 stamped with the
    // far-future WRAPPED LDT. `score` stays live so the row is not empty.
    flush_batch(
        &newer,
        vec![Mutation::new(
            TableId::new(KEYSPACE, TABLE),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(0))),
            vec![
                CellOperation::Delete {
                    column: "name".to_string(),
                    local_deletion_time: Some(WRAPPED_LDT_I32),
                },
                CellOperation::Write {
                    column: "score".to_string(),
                    value: Value::Integer(9),
                },
            ],
            300,
            None,
        )
        .with_local_deletion_time(WRAPPED_LDT_I32)],
    );

    let mut inputs = discover_inputs(&newer);
    inputs.extend(discover_inputs(&older));
    assert_eq!(inputs.len(), 2, "expected two input SSTables");

    let data_path = compact_major(inputs, &out);
    let rows = read_compaction_rows(&data_path);

    let mut saw_wrapped_tombstone = false;
    let mut saw_resurrected_secret = false;
    for row in &rows {
        if let CompactionRowData::Live { simple, .. } = &row.row_data {
            for cell in simple {
                if cell.column == "name" {
                    match &cell.value {
                        Value::Tombstone(info)
                            if info.tombstone_type == TombstoneType::CellTombstone =>
                        {
                            saw_wrapped_tombstone = true;
                            // Criterion 3: LDT survives as the wrapped bit pattern.
                            assert_eq!(
                                info.local_deletion_time as i32, WRAPPED_LDT_I32,
                                "cell tombstone LDT must survive as the wrapped i32 bit pattern"
                            );
                            // And unsigned it is the far-future instant.
                            assert_eq!(
                                (info.local_deletion_time as i32) as u32 as i64,
                                WRAPPED_LDT_UNSIGNED,
                                "unsigned reinterpretation is the far-future second count"
                            );
                        }
                        Value::Text(t) if t == "secret" => saw_resurrected_secret = true,
                        _ => {}
                    }
                }
            }
        }
    }

    assert!(
        saw_wrapped_tombstone,
        "far-future wrapped cell tombstone must be RETAINED (a signed compare would purge it), \
         got rows: {rows:?}"
    );
    assert!(
        !saw_resurrected_secret,
        "the older live `name`=\"secret\" cell must stay SHADOWED (not resurrected)"
    );
}

/// Boundary: cell tombstone at `LDT == 2^31` (`i32::MIN` bit pattern) is
/// retained under a normal `gcBefore` — the most-negative value that a signed
/// compare would purge first.
#[test]
fn wrapped_ldt_at_i32_min_cell_tombstone_retained() {
    assert_eq!(
        BOUNDARY_LDT_I32,
        i32::MIN,
        "test precondition: 2^31 wraps to i32::MIN"
    );
    let temp = TempDir::new().unwrap();
    let older = temp.path().join("older");
    let newer = temp.path().join("newer");
    let out = temp.path().join("out");

    flush_batch(&older, vec![write_older_live_row(0, "secret", 100)]);
    flush_batch(
        &newer,
        vec![Mutation::new(
            TableId::new(KEYSPACE, TABLE),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(0))),
            vec![
                CellOperation::Delete {
                    column: "name".to_string(),
                    local_deletion_time: Some(BOUNDARY_LDT_I32),
                },
                CellOperation::Write {
                    column: "score".to_string(),
                    value: Value::Integer(9),
                },
            ],
            300,
            None,
        )
        .with_local_deletion_time(BOUNDARY_LDT_I32)],
    );

    let mut inputs = discover_inputs(&newer);
    inputs.extend(discover_inputs(&older));
    let data_path = compact_major(inputs, &out);
    let rows = read_compaction_rows(&data_path);

    let mut saw_boundary_tombstone = false;
    for row in &rows {
        if let CompactionRowData::Live { simple, .. } = &row.row_data {
            for cell in simple {
                if cell.column == "name" {
                    if let Value::Tombstone(info) = &cell.value {
                        if info.tombstone_type == TombstoneType::CellTombstone {
                            saw_boundary_tombstone = true;
                            assert_eq!(
                                info.local_deletion_time as i32, BOUNDARY_LDT_I32,
                                "boundary cell tombstone LDT must survive as i32::MIN bit pattern"
                            );
                            assert_eq!(
                                (info.local_deletion_time as i32) as u32 as i64,
                                BOUNDARY_LDT_UNSIGNED,
                                "unsigned reinterpretation is exactly 2^31"
                            );
                        }
                    }
                    assert_ne!(
                        cell.value,
                        Value::Text("secret".to_string()),
                        "older `name` must stay shadowed at the wrap boundary"
                    );
                }
            }
        }
    }
    assert!(
        saw_boundary_tombstone,
        "i32::MIN (2^31) cell tombstone must be RETAINED under a normal gcBefore, got: {rows:?}"
    );
}

// ===========================================================================
// Criterion 2 (row-tombstone site) — reconcile.rs:507
// ===========================================================================

/// Far-future wrapped ROW tombstone covering an older live row: RETAINED, still
/// shadows the older cells, no panic. Uses a standalone `DeleteRow` whose LDT is
/// stamped via `with_local_deletion_time` (the writer's
/// `effective_local_deletion_time`), so the row tombstone's `row_del_ldt`
/// carries the wrapped bit pattern through the merge.
#[test]
fn wrapped_ldt_row_tombstone_retained_and_shadows() {
    let temp = TempDir::new().unwrap();
    let older = temp.path().join("older");
    let newer = temp.path().join("newer");
    let out = temp.path().join("out");

    // Older SSTable: a full live row at ts=100.
    flush_batch(&older, vec![write_older_live_row(0, "secret", 100)]);

    // Newer SSTable: a whole-row tombstone at ts=300 with the wrapped far-future
    // LDT (markedForDeleteAt = 300µs > the older row's 100µs, so it shadows it).
    flush_batch(
        &newer,
        vec![Mutation::new(
            TableId::new(KEYSPACE, TABLE),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(0))),
            vec![CellOperation::DeleteRow],
            300,
            None,
        )
        .with_local_deletion_time(WRAPPED_LDT_I32)],
    );

    let mut inputs = discover_inputs(&newer);
    inputs.extend(discover_inputs(&older));
    let data_path = compact_major(inputs, &out);
    let rows = read_compaction_rows(&data_path);

    let mut saw_row_tombstone = false;
    let mut saw_resurrected = false;
    for row in &rows {
        match &row.row_data {
            CompactionRowData::Tombstone {
                local_deletion_time,
                ..
            } => {
                saw_row_tombstone = true;
                assert_eq!(
                    *local_deletion_time, WRAPPED_LDT_I32,
                    "row tombstone LDT must survive as the wrapped i32 bit pattern"
                );
                assert_eq!(
                    (*local_deletion_time as u32) as i64,
                    WRAPPED_LDT_UNSIGNED,
                    "unsigned reinterpretation is the far-future second count"
                );
            }
            CompactionRowData::Live {
                simple,
                row_deletion,
                ..
            } => {
                // A coexisting row deletion (issue #932) also carries the LDT.
                if let Some((_, ldt)) = row_deletion {
                    saw_row_tombstone = true;
                    assert_eq!(
                        *ldt, WRAPPED_LDT_I32,
                        "coexisting row-deletion LDT must survive as the wrapped bit pattern"
                    );
                }
                for cell in simple {
                    if cell.column == "name" && cell.value == Value::Text("secret".to_string()) {
                        saw_resurrected = true;
                    }
                }
            }
            _ => {}
        }
    }

    assert!(
        saw_row_tombstone,
        "far-future wrapped ROW tombstone must be RETAINED (a signed compare would purge it), \
         got rows: {rows:?}"
    );
    assert!(
        !saw_resurrected,
        "the older live row must stay SHADOWED by the surviving row tombstone (not resurrected)"
    );
}

/// Boundary: ROW tombstone at `LDT == 2^31` (`i32::MIN`) is retained under a
/// normal `gcBefore`.
#[test]
fn wrapped_ldt_at_i32_min_row_tombstone_retained() {
    let temp = TempDir::new().unwrap();
    let older = temp.path().join("older");
    let newer = temp.path().join("newer");
    let out = temp.path().join("out");

    flush_batch(&older, vec![write_older_live_row(0, "secret", 100)]);
    flush_batch(
        &newer,
        vec![Mutation::new(
            TableId::new(KEYSPACE, TABLE),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(0))),
            vec![CellOperation::DeleteRow],
            300,
            None,
        )
        .with_local_deletion_time(BOUNDARY_LDT_I32)],
    );

    let mut inputs = discover_inputs(&newer);
    inputs.extend(discover_inputs(&older));
    let data_path = compact_major(inputs, &out);
    let rows = read_compaction_rows(&data_path);

    let mut saw = false;
    for row in &rows {
        match &row.row_data {
            CompactionRowData::Tombstone {
                local_deletion_time,
                ..
            } => {
                saw = true;
                assert_eq!(*local_deletion_time, BOUNDARY_LDT_I32);
                assert_eq!((*local_deletion_time as u32) as i64, BOUNDARY_LDT_UNSIGNED);
            }
            CompactionRowData::Live {
                row_deletion: Some((_, ldt)),
                ..
            } => {
                saw = true;
                assert_eq!(*ldt, BOUNDARY_LDT_I32);
            }
            _ => {}
        }
    }
    assert!(
        saw,
        "i32::MIN (2^31) ROW tombstone must be RETAINED under a normal gcBefore, got: {rows:?}"
    );
}

// ===========================================================================
// Criterion 2 (complex-deletion site) — reconcile.rs:521
// ===========================================================================

/// Far-future wrapped COMPLEX-DELETION marker (on a non-frozen `tags` set)
/// covering an older set element: RETAINED under a normal `gcBefore`, still
/// shadows the older element, LDT survives as the wrapped bit pattern, no panic.
///
/// The marker rides with a surviving live element (`hot`) at a higher element
/// timestamp so the row is not empty; the covered older element (`cold`, in the
/// older SSTable) is shadowed by `markedForDeleteAt`.
#[test]
fn wrapped_ldt_complex_deletion_retained_and_shadows() {
    let temp = TempDir::new().unwrap();
    let older = temp.path().join("older");
    let newer = temp.path().join("newer");
    let out = temp.path().join("out");

    // Older SSTable: a `tags` set element written at ts=100.
    flush_batch(&older, vec![write_older_tags_row(0, "cold", 100)]);

    // Newer SSTable: a real complex-deletion marker at markedForDeleteAt=300µs
    // stamped with the far-future wrapped LDT, plus a surviving element `hot`
    // written strictly AFTER the marker so it is not itself shadowed.
    flush_batch(
        &newer,
        vec![Mutation::new(
            TableId::new(KEYSPACE, TABLE),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(0))),
            vec![
                CellOperation::ComplexDeletion {
                    column: "tags".to_string(),
                    marked_for_delete_at: 300,
                    local_deletion_time: WRAPPED_LDT_I32,
                },
                CellOperation::WriteComplexElement {
                    column: "tags".to_string(),
                    cell_path: b"hot".to_vec(),
                    value: None,
                    timestamp_micros: 400,
                    ttl_seconds: None,
                    local_deletion_time: None,
                    is_deleted: false,
                },
            ],
            400,
            None,
        )],
    );

    let mut inputs = discover_inputs(&newer);
    inputs.extend(discover_inputs(&older));
    let data_path = compact_major(inputs, &out);
    let rows = read_compaction_rows(&data_path);

    let mut saw_wrapped_marker = false;
    let mut saw_resurrected_cold = false;
    for row in &rows {
        if let CompactionRowData::Live { complex, .. } = &row.row_data {
            for column in complex {
                if column.column != "tags" {
                    continue;
                }
                if let Some((_mdt, ldt)) = column.complex_deletion {
                    saw_wrapped_marker = true;
                    assert_eq!(
                        ldt, WRAPPED_LDT_I32,
                        "complex-deletion LDT must survive as the wrapped i32 bit pattern"
                    );
                    assert_eq!(
                        (ldt as u32) as i64,
                        WRAPPED_LDT_UNSIGNED,
                        "unsigned reinterpretation is the far-future second count"
                    );
                }
                // The covered older element `cold` (ts=100 <= marker 300) must be
                // gone; only the surviving `hot` element remains.
                for element in &column.elements {
                    if element.cell_path == b"cold" {
                        saw_resurrected_cold = true;
                    }
                }
            }
        }
    }

    assert!(
        saw_wrapped_marker,
        "far-future wrapped COMPLEX-DELETION marker must be RETAINED (a signed compare would \
         purge it), got rows: {rows:?}"
    );
    assert!(
        !saw_resurrected_cold,
        "the older `cold` set element must stay SHADOWED by the surviving complex-deletion marker"
    );
}

/// Boundary: complex-deletion marker at `LDT == 2^31` (`i32::MIN`) is retained
/// under a normal `gcBefore`.
#[test]
fn wrapped_ldt_at_i32_min_complex_deletion_retained() {
    let temp = TempDir::new().unwrap();
    let older = temp.path().join("older");
    let newer = temp.path().join("newer");
    let out = temp.path().join("out");

    flush_batch(&older, vec![write_older_tags_row(0, "cold", 100)]);
    flush_batch(
        &newer,
        vec![Mutation::new(
            TableId::new(KEYSPACE, TABLE),
            PartitionKey::single("id", Value::Integer(1)),
            Some(ClusteringKey::single("ck", Value::Integer(0))),
            vec![
                CellOperation::ComplexDeletion {
                    column: "tags".to_string(),
                    marked_for_delete_at: 300,
                    local_deletion_time: BOUNDARY_LDT_I32,
                },
                CellOperation::WriteComplexElement {
                    column: "tags".to_string(),
                    cell_path: b"hot".to_vec(),
                    value: None,
                    timestamp_micros: 400,
                    ttl_seconds: None,
                    local_deletion_time: None,
                    is_deleted: false,
                },
            ],
            400,
            None,
        )],
    );

    let mut inputs = discover_inputs(&newer);
    inputs.extend(discover_inputs(&older));
    let data_path = compact_major(inputs, &out);
    let rows = read_compaction_rows(&data_path);

    let mut saw = false;
    for row in &rows {
        if let CompactionRowData::Live { complex, .. } = &row.row_data {
            for column in complex {
                if column.column == "tags" {
                    if let Some((_mdt, ldt)) = column.complex_deletion {
                        saw = true;
                        assert_eq!(ldt, BOUNDARY_LDT_I32);
                        assert_eq!((ldt as u32) as i64, BOUNDARY_LDT_UNSIGNED);
                    }
                    for element in &column.elements {
                        assert_ne!(
                            element.cell_path, b"cold",
                            "older `cold` element must stay shadowed at the wrap boundary"
                        );
                    }
                }
            }
        }
    }
    assert!(
        saw,
        "i32::MIN (2^31) complex-deletion marker must be RETAINED under a normal gcBefore, \
         got: {rows:?}"
    );
}
