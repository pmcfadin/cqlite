//! Issue #933: apply range tombstones during compaction END-TO-END.
//!
//! Validates the full pipeline that #933 wired together:
//!   1. **Reader** surfaces on-disk range-tombstone bound markers (previously
//!      SKIPPED) through the compaction read contract, pairing start/end bounds
//!      (incl. open-ended Bottom/Top) into complete ranges.
//!   2. **Merge** shadows the cells a range tombstone covers (schema-aware,
//!      honoring DESC) and re-emits the surviving marker.
//!   3. **Writer** persists the surviving marker to the output SSTable.
//!
//! The key safety property (roborev #959 High #2): shadowing covered cells WITHOUT
//! persisting the marker would RESURRECT rows from a non-compacted SSTable. We
//! prove the marker persists by RE-COMPACTING the output against a fresh SSTable
//! that re-introduces the covered rows at an older timestamp — they must stay
//! shadowed.
//!
//! These tests do not require external Cassandra fixtures: the CQLite writer emits
//! the same on-disk range-tombstone bound markers Cassandra does (IS_MARKER +
//! ClusteringBoundOrBoundary), and the reader/merge consume them.

#![cfg(feature = "write-support")]

use std::path::{Path, PathBuf};

use cqlite_core::schema::{ClusteringColumn, ClusteringOrder, Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::merge::{compact_sstables, KWayMerger, MergeStep, RowData};
use cqlite_core::storage::write_engine::mutation::{
    CellOperation, ClusteringBound, ClusteringKey, Mutation, PartitionKey, RangeTombstone, TableId,
};
use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_core::types::Value;
use tempfile::TempDir;

const KS: &str = "rt_ks";
const TBL: &str = "rt_items";

fn schema(order: ClusteringOrder) -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
        partition_keys: vec![KeyColumn {
            name: "id".to_string(),
            data_type: "int".to_string(),
            position: 0,
        }],
        clustering_keys: vec![ClusteringColumn {
            name: "ck".to_string(),
            data_type: "int".to_string(),
            position: 0,
            order,
        }],
        columns: vec![
            Column {
                name: "id".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "ck".to_string(),
                data_type: "int".to_string(),
                nullable: false,
                default: None,
                is_static: false,
            },
            Column {
                name: "name".to_string(),
                data_type: "text".to_string(),
                nullable: true,
                default: None,
                is_static: false,
            },
        ],
        comments: Default::default(),
        dropped_columns: Default::default(),
    }
}

fn write_row(id: i32, ck: i32, name: &str, ts: i64) -> Mutation {
    Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        Some(ClusteringKey::single("ck", Value::Integer(ck))),
        vec![CellOperation::Write {
            column: "name".to_string(),
            value: Value::Text(name.to_string()),
        }],
        ts,
        None,
    )
}

/// A range-tombstone mutation (no row content) for partition `id`.
fn range_delete(id: i32, start: ClusteringBound, end: ClusteringBound, ts: i64) -> Mutation {
    let mut m = Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(id)),
        None,
        vec![],
        ts,
        None,
    );
    m.range_tombstones.push(RangeTombstone {
        start,
        end,
        deletion_time: ts,
        // A within-grace LDT (far future) so gc-grace never purges the marker in
        // these tests.
        local_deletion_time: 2_000_000_000,
    });
    m
}

fn incl(ck: i32) -> ClusteringBound {
    ClusteringBound::Inclusive(ClusteringKey::single("ck", Value::Integer(ck)))
}

/// Flush a batch of mutations into its own SSTable generation and return the
/// engine so subsequent flushes produce distinct generations.
fn flush_batch(engine: &mut WriteEngine, rt: &tokio::runtime::Runtime, muts: Vec<Mutation>) {
    for m in muts {
        engine.write(m).expect("write");
    }
    rt.block_on(engine.flush())
        .expect("flush")
        .expect("sstable info");
}

fn discover_inputs(dir: &Path) -> Vec<PathBuf> {
    fn collect(dir: &Path, out: &mut Vec<(u64, PathBuf)>, depth: usize) {
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
                collect(&path, out, depth - 1);
            }
        }
    }
    let mut found = Vec::new();
    collect(dir, &mut found, 8);
    // newest generation first (run index 0 = newest)
    found.sort_by(|a, b| b.0.cmp(&a.0));
    found.into_iter().map(|(_, p)| p).collect()
}

/// What a read-back of one or more SSTables yields through the compaction read
/// path: the surviving `(id, ck)` live rows and the surviving range markers.
struct ReadBack {
    live_rows: Vec<(i32, i32)>,
    markers: Vec<(i32, RangeTombstone)>,
}

fn read_back(inputs: Vec<PathBuf>, schema: &TableSchema) -> ReadBack {
    let mut merger = KWayMerger::new(inputs, schema).expect("KWayMerger::new");
    let mut live_rows = Vec::new();
    let mut markers = Vec::new();
    loop {
        match merger.step().expect("merger step") {
            MergeStep::Complete => break,
            MergeStep::Partition { rows, .. } => {
                for entry in rows {
                    let id = decode_id(&entry.key.key, schema);
                    if let Some(rt) = &entry.range_deletion {
                        markers.push((id, rt.clone()));
                        continue;
                    }
                    if let RowData::Live { cells } = &entry.row_data {
                        // A genuine data row has at least one non-key cell.
                        let has_data = cells.iter().any(|c| c.column != "ck" && c.column != "id");
                        if has_data {
                            if let Some(ck) = entry.clustering_key.as_ref().and_then(ck_value) {
                                live_rows.push((id, ck));
                            }
                        }
                    }
                }
            }
        }
    }
    live_rows.sort_unstable();
    ReadBack { live_rows, markers }
}

fn decode_id(key_bytes: &[u8], schema: &TableSchema) -> i32 {
    let pk = PartitionKey::from_bytes(key_bytes, schema).expect("decode pk");
    match &pk.columns[0].1 {
        Value::Integer(n) => *n,
        other => panic!("unexpected pk value {other:?}"),
    }
}

fn ck_value(ck: &ClusteringKey) -> Option<i32> {
    match ck.columns.first().map(|(_, v)| v) {
        Some(Value::Integer(n)) => Some(*n),
        _ => None,
    }
}

fn compact(inputs: Vec<PathBuf>, out_dir: &Path, schema: &TableSchema, generation: u64) -> PathBuf {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let report = rt
        .block_on(compact_sstables(
            inputs, out_dir, schema, generation, None, None, /* purge_safe */ true,
        ))
        .expect("compaction");
    report.output.data_path
}

/// Core deliverable: a bounded range tombstone `[1, 3]` from a SEPARATE SSTable
/// shadows the covered rows during compaction AND the marker is persisted.
#[test]
fn bounded_range_tombstone_shadows_and_persists() {
    let schema = schema(ClusteringOrder::Asc);
    let temp = TempDir::new().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let data_dir = temp.path().join("inputs");
    let mut engine = WriteEngine::new(WriteEngineConfig::new(
        data_dir.clone(),
        temp.path().join("wal"),
        schema.clone(),
    ))
    .unwrap();

    // SSTable A: rows ck 0..=4 at ts=100.
    flush_batch(
        &mut engine,
        &rt,
        (0..=4)
            .map(|ck| write_row(1, ck, &format!("v{ck}"), 100))
            .collect(),
    );
    // SSTable B: range tombstone covering [1, 3] at ts=200.
    flush_batch(
        &mut engine,
        &rt,
        vec![range_delete(1, incl(1), incl(3), 200)],
    );
    rt.block_on(engine.close()).unwrap();

    let inputs = discover_inputs(&data_dir);
    assert!(
        inputs.len() >= 2,
        "expected >= 2 input generations, got {}",
        inputs.len()
    );

    // Compact A + B.
    let out_dir = temp.path().join("out1");
    let output = compact(inputs, &out_dir, &schema, 1001);

    let rb = read_back(vec![output.clone()], &schema);

    // ck 1,2,3 covered and suppressed; ck 0,4 survive.
    assert_eq!(
        rb.live_rows,
        vec![(1, 0), (1, 4)],
        "only ck 0 and 4 survive the [1,3] range tombstone"
    );

    // The surviving marker is persisted to the output (writer→reader round-trip).
    assert_eq!(rb.markers.len(), 1, "exactly one range marker persisted");
    let (mid, rtomb) = &rb.markers[0];
    assert_eq!(*mid, 1);
    assert_eq!(rtomb.deletion_time, 200);
    assert!(
        matches!(&rtomb.start, ClusteringBound::Inclusive(k) if ck_value(k) == Some(1)),
        "start bound is INCL(1), got {:?}",
        rtomb.start
    );
    assert!(
        matches!(&rtomb.end, ClusteringBound::Inclusive(k) if ck_value(k) == Some(3)),
        "end bound is INCL(3), got {:?}",
        rtomb.end
    );

    // Cross-SSTable safety: a fresh, NON-compacted SSTable re-introduces the
    // covered rows at an OLDER timestamp. Re-compacting output1 + C must keep them
    // shadowed (the persisted marker covers them) — proving we did not silently
    // drop cells without persisting the shadowing marker.
    let data_dir2 = temp.path().join("inputs2");
    let mut engine2 = WriteEngine::new(WriteEngineConfig::new(
        data_dir2.clone(),
        temp.path().join("wal2"),
        schema.clone(),
    ))
    .unwrap();
    flush_batch(
        &mut engine2,
        &rt,
        vec![
            write_row(1, 1, "resurrect-1", 100),
            write_row(1, 2, "resurrect-2", 100),
            write_row(1, 3, "resurrect-3", 100),
        ],
    );
    rt.block_on(engine2.close()).unwrap();
    let c = discover_inputs(&data_dir2);
    assert_eq!(c.len(), 1);

    // output1 is the newest generation (1001) so it must sort first.
    let mut inputs2 = vec![output];
    inputs2.extend(c);
    let out_dir2 = temp.path().join("out2");
    let output2 = compact(inputs2, &out_dir2, &schema, 1002);

    let rb2 = read_back(vec![output2], &schema);
    assert_eq!(
        rb2.live_rows,
        vec![(1, 0), (1, 4)],
        "covered rows from the non-compacted SSTable stay shadowed by the persisted marker"
    );
}

/// Open-ended ranges: `[2, Top]` (open-to-top) and `[Bottom, 1]` (open-from-bottom)
/// round-trip through compaction and shadow the correct rows.
#[test]
fn open_ended_range_tombstones() {
    let schema = schema(ClusteringOrder::Asc);
    let temp = TempDir::new().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let data_dir = temp.path().join("inputs");
    let mut engine = WriteEngine::new(WriteEngineConfig::new(
        data_dir.clone(),
        temp.path().join("wal"),
        schema.clone(),
    ))
    .unwrap();

    // SSTable A: id=2 ck 0..=3, id=3 ck 0..=3.
    let mut rows = Vec::new();
    for ck in 0..=3 {
        rows.push(write_row(2, ck, &format!("p2-{ck}"), 100));
        rows.push(write_row(3, ck, &format!("p3-{ck}"), 100));
    }
    flush_batch(&mut engine, &rt, rows);

    // SSTable B: id=2 open-to-top [2, Top]; id=3 open-from-bottom [Bottom, 1].
    flush_batch(
        &mut engine,
        &rt,
        vec![
            range_delete(2, incl(2), ClusteringBound::Top, 200),
            range_delete(3, ClusteringBound::Bottom, incl(1), 200),
        ],
    );
    rt.block_on(engine.close()).unwrap();

    let inputs = discover_inputs(&data_dir);
    let out_dir = temp.path().join("out");
    let output = compact(inputs, &out_dir, &schema, 2001);
    let rb = read_back(vec![output], &schema);

    // id=2: [2, Top] removes ck 2,3 → ck 0,1 survive.
    // id=3: [Bottom, 1] removes ck 0,1 → ck 2,3 survive.
    assert_eq!(
        rb.live_rows,
        vec![(2, 0), (2, 1), (3, 2), (3, 3)],
        "open-to-top and open-from-bottom ranges shadow the correct rows"
    );

    // Both markers persist with their open bounds intact.
    let mut p2 = None;
    let mut p3 = None;
    for (id, m) in &rb.markers {
        match id {
            2 => p2 = Some(m.clone()),
            3 => p3 = Some(m.clone()),
            other => panic!("unexpected marker partition {other}"),
        }
    }
    let p2 = p2.expect("id=2 marker persisted");
    assert!(
        matches!(p2.end, ClusteringBound::Top),
        "id=2 end is Top, got {:?}",
        p2.end
    );
    assert!(
        matches!(&p2.start, ClusteringBound::Inclusive(k) if ck_value(k) == Some(2)),
        "id=2 start is INCL(2), got {:?}",
        p2.start
    );

    let p3 = p3.expect("id=3 marker persisted");
    assert!(
        matches!(p3.start, ClusteringBound::Bottom),
        "id=3 start is Bottom, got {:?}",
        p3.start
    );
    assert!(
        matches!(&p3.end, ClusteringBound::Inclusive(k) if ck_value(k) == Some(1)),
        "id=3 end is INCL(1), got {:?}",
        p3.end
    );
}

/// A cell strictly NEWER than the range tombstone survives (per-cell shadow
/// boundary `<=`), while older cells in the covered range are suppressed.
#[test]
fn newer_cell_survives_range_tombstone() {
    let schema = schema(ClusteringOrder::Asc);
    let temp = TempDir::new().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let data_dir = temp.path().join("inputs");
    let mut engine = WriteEngine::new(WriteEngineConfig::new(
        data_dir.clone(),
        temp.path().join("wal"),
        schema.clone(),
    ))
    .unwrap();

    // A: ck 1 (ts=100, older), ck 2 (ts=100, older), ck 3 (ts=300, NEWER).
    flush_batch(
        &mut engine,
        &rt,
        vec![
            write_row(5, 1, "old-1", 100),
            write_row(5, 2, "old-2", 100),
            write_row(5, 3, "new-3", 300),
        ],
    );
    // B: range [1, 3] at ts=200.
    flush_batch(
        &mut engine,
        &rt,
        vec![range_delete(5, incl(1), incl(3), 200)],
    );
    rt.block_on(engine.close()).unwrap();

    let inputs = discover_inputs(&data_dir);
    let out_dir = temp.path().join("out");
    let output = compact(inputs, &out_dir, &schema, 3001);
    let rb = read_back(vec![output], &schema);

    // ck 1,2 (ts=100 <= 200) shadowed; ck 3 (ts=300 > 200) survives.
    assert_eq!(
        rb.live_rows,
        vec![(5, 3)],
        "only the row written strictly after the range deletion survives"
    );
}

/// DESC clustering order: the schema-aware bound comparison must honor the
/// reversed order so a `[1, 3]` range still covers ck 1,2,3 (issue #933 / roborev
/// #959 Medium #3 — comparing with `ClusteringKey::compare`, not schema-agnostic
/// `cmp`).
#[test]
fn desc_clustering_order_honored() {
    let schema = schema(ClusteringOrder::Desc);
    let temp = TempDir::new().unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let data_dir = temp.path().join("inputs");
    let mut engine = WriteEngine::new(WriteEngineConfig::new(
        data_dir.clone(),
        temp.path().join("wal"),
        schema.clone(),
    ))
    .unwrap();

    flush_batch(
        &mut engine,
        &rt,
        (0..=4)
            .map(|ck| write_row(9, ck, &format!("d{ck}"), 100))
            .collect(),
    );
    // Under DESC the on-disk clustering order is 4,3,2,1,0, so the range deleting
    // ck ∈ {1,2,3} has its START bound at the value that sorts FIRST (3) and its
    // END bound at the value that sorts LAST (1) — exactly how Cassandra
    // normalizes a slice on a reversed column. Coverage must be computed
    // schema-aware (DESC), so ck 1,2,3 are covered and ck 0,4 survive.
    flush_batch(
        &mut engine,
        &rt,
        vec![range_delete(9, incl(3), incl(1), 200)],
    );
    rt.block_on(engine.close()).unwrap();

    let inputs = discover_inputs(&data_dir);
    let out_dir = temp.path().join("out");
    let output = compact(inputs, &out_dir, &schema, 4001);
    let rb = read_back(vec![output], &schema);

    assert_eq!(
        rb.live_rows,
        vec![(9, 0), (9, 4)],
        "DESC range [1,3] still covers ck 1,2,3 (schema-aware bound comparison)"
    );
}
